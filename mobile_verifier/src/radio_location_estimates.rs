use std::str::FromStr;

use crate::{heartbeats::HbType, sp_boosted_rewards_bans::BannedRadios, Settings};
use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    radio_location_estimates::{Entity, RadioLocationEstimate, RadioLocationEstimatesReq},
    radio_location_estimates_ingest_report::RadioLocationEstimatesIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    verified_radio_location_estimates::VerifiedRadioLocationEstimatesReport,
    FileStore, FileType,
};
use futures::{StreamExt, TryStreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        RadioLocationEstimatesVerificationStatus, VerifiedRadioLocationEstimatesReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Pool, Postgres, Row, Transaction};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

const CONFIDENCE_THRESHOLD: Decimal = dec!(0.75);

pub struct RadioLocationEstimatesDaemon<AV> {
    pool: Pool<Postgres>,
    authorization_verifier: AV,
    reports_receiver: Receiver<FileInfoStream<RadioLocationEstimatesIngestReport>>,
    verified_report_sink: FileSinkClient<VerifiedRadioLocationEstimatesReportV1>,
}

impl<AV> RadioLocationEstimatesDaemon<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
{
    pub fn new(
        pool: Pool<Postgres>,
        authorization_verifier: AV,
        reports_receiver: Receiver<FileInfoStream<RadioLocationEstimatesIngestReport>>,
        verified_report_sink: FileSinkClient<VerifiedRadioLocationEstimatesReportV1>,
    ) -> Self {
        Self {
            pool,
            authorization_verifier,
            reports_receiver,
            verified_report_sink,
        }
    }

    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: AV,
    ) -> anyhow::Result<impl ManagedTask> {
        let (reports_receiver, reports_receiver_server) =
            file_source::continuous_source::<RadioLocationEstimatesIngestReport, _>()
                .state(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::RadioLocationEstimatesIngestReport.to_string())
                .create()
                .await?;

        let (verified_report_sink, verified_report_sink_server) =
            VerifiedRadioLocationEstimatesReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let task = Self::new(
            pool,
            authorization_verifier,
            reports_receiver,
            verified_report_sink,
        );

        Ok(TaskManager::builder()
            .add_task(reports_receiver_server)
            .add_task(verified_report_sink_server)
            .add_task(task)
            .build())
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("Starting sme deamon");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => {
                    tracing::info!("sme deamon shutting down");
                    break;
                }
                Some(file) = self.reports_receiver.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<RadioLocationEstimatesIngestReport>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "Processing Radio Location Estimates file {}",
            file_info_stream.file_info.key
        );

        let mut transaction = self.pool.begin().await?;

        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(
                transaction,
                |mut transaction, report: RadioLocationEstimatesIngestReport| async move {
                    let verified_report_status = self.verify_report(&report.report).await;

                    if verified_report_status == RadioLocationEstimatesVerificationStatus::Valid {
                        save_to_db(&report, &mut transaction).await?;

                        // Once they are saved to DB should we directly write to ban table?
                        // maybe_ban_radios(&report, &mut transaction).await?;
                    }

                    let verified_report_proto: VerifiedRadioLocationEstimatesReportV1 =
                        VerifiedRadioLocationEstimatesReport {
                            report,
                            status: verified_report_status,
                            timestamp: Utc::now(),
                        }
                        .into();

                    self.verified_report_sink
                        .write(
                            verified_report_proto,
                            &[("report_status", verified_report_status.as_str_name())],
                        )
                        .await?;

                    Ok(transaction)
                },
            )
            .await?
            .commit()
            .await?;

        self.verified_report_sink.commit().await?;

        Ok(())
    }

    async fn verify_report(
        &self,
        req: &RadioLocationEstimatesReq,
    ) -> RadioLocationEstimatesVerificationStatus {
        if !self.verify_known_carrier_key(&req.carrier_key).await {
            return RadioLocationEstimatesVerificationStatus::InvalidKey;
        }

        RadioLocationEstimatesVerificationStatus::Valid
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        self.authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
            .unwrap_or_default()
    }
}

impl<AV> ManagedTask for RadioLocationEstimatesDaemon<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

async fn save_to_db(
    report: &RadioLocationEstimatesIngestReport,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    let estimates = &report.report.estimates;
    let entity = &report.report.entity;
    let received_timestamp = report.received_timestamp;
    for estimate in estimates {
        insert_estimate(entity, received_timestamp, estimate, exec).await?;
    }
    invalidate_old_estimates(entity, received_timestamp, exec).await?;

    Ok(())
}

async fn invalidate_old_estimates(
    entity: &Entity,
    timestamp: DateTime<Utc>,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE radio_location_estimates
        SET invalided_at = now()
        WHERE radio_key = $1
            AND received_timestamp < $2;
        "#,
    )
    .bind(entity.to_string())
    .bind(timestamp)
    .execute(exec)
    .await?;

    Ok(())
}

async fn insert_estimate(
    entity: &Entity,
    received_timestamp: DateTime<Utc>,
    estimate: &RadioLocationEstimate,
    exec: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    let radius = estimate.radius;
    let lat = estimate.lat;
    let long = estimate.long;
    let hashed_key = hash_key(entity, received_timestamp, radius, lat, long);

    sqlx::query(
        r#"
        INSERT INTO radio_location_estimates (hashed_key, radio_type, radio_key, received_timestamp, radius, lat, long, confidence)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (hashed_key) DO NOTHING
        "#,
    )
    .bind(hashed_key)
    .bind(entity_to_radio_type(entity))
    .bind(entity.to_string())
    .bind(received_timestamp)
    .bind(estimate.radius)
    .bind(lat)
    .bind(long)
    .bind(estimate.confidence)
    .execute(exec)
    .await?;

    Ok(())
}

pub fn hash_key(
    entity: &Entity,
    timestamp: DateTime<Utc>,
    radius: Decimal,
    lat: Decimal,
    long: Decimal,
) -> String {
    let key = format!("{}{}{}{}{}", entity, timestamp, radius, lat, long);

    let mut hasher = Sha256::new();
    hasher.update(key);
    let hashed_key = hasher.finalize();
    hex::encode(hashed_key)
}

pub async fn clear_invalided(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    timestamp: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        DELETE FROM radio_location_estimates
        WHERE invalided_at IS NOT NULL
            AND invalided_at < $1
        "#,
    )
    .bind(timestamp)
    .execute(&mut *tx)
    .await?;
    Ok(())
}

// This is wrong should be a get estimates but will fix later
pub async fn get_banned_radios(pool: &PgPool) -> anyhow::Result<BannedRadios> {
    // TODO: Do we still want to ban any radio that is NOT in this table?
    // Might be multiple per radio
    // check assertion in circle as well
    sqlx::query(
        r#"
            SELECT radio_type, radio_key
            FROM radio_location_estimates
            WHERE confidence < $1
                AND invalided_at IS NULL
        "#,
    )
    .bind(CONFIDENCE_THRESHOLD)
    .fetch(pool)
    .map_err(anyhow::Error::from)
    .try_fold(BannedRadios::default(), |mut set, row| async move {
        let radio_type = row.get::<HbType, &str>("radio_type");
        let radio_key = row.get::<String, &str>("radio_key");
        match radio_type {
            HbType::Wifi => set.insert_wifi(PublicKeyBinary::from_str(&radio_key)?),
            HbType::Cbrs => set.insert_cbrs(radio_key),
        };

        Ok(set)
    })
    .await
}

fn entity_to_radio_type(entity: &Entity) -> HbType {
    match entity {
        Entity::CbrsId(_) => HbType::Cbrs,
        Entity::WifiPubKey(_) => HbType::Wifi,
    }
}
