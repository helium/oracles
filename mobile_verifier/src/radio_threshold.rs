use chrono::{DateTime, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    mobile_radio_invalidated_threshold::{
        InvalidatedRadioThresholdIngestReport, InvalidatedRadioThresholdReportReq,
        VerifiedInvalidatedRadioThresholdIngestReport,
    },
    mobile_radio_threshold::{
        RadioThresholdIngestReport, RadioThresholdReportReq, VerifiedRadioThresholdIngestReport,
    },
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore, FileType,
};
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        InvalidatedRadioThresholdReportVerificationStatus, RadioThresholdReportVerificationStatus,
        VerifiedInvalidatedRadioThresholdIngestReportV1, VerifiedRadioThresholdIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Pool, Postgres, Transaction};
use std::{collections::HashSet, ops::Range};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

use crate::Settings;

pub struct RadioThresholdIngestor<AV> {
    pool: PgPool,
    reports_receiver: Receiver<FileInfoStream<RadioThresholdIngestReport>>,
    invalid_reports_receiver: Receiver<FileInfoStream<InvalidatedRadioThresholdIngestReport>>,
    verified_report_sink: FileSinkClient<VerifiedRadioThresholdIngestReportV1>,
    verified_invalid_report_sink: FileSinkClient<VerifiedInvalidatedRadioThresholdIngestReportV1>,
    authorization_verifier: AV,
}

impl<AV> ManagedTask for RadioThresholdIngestor<AV>
where
    AV: AuthorizationVerifier,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result }),
        )
    }
}

impl<AV> RadioThresholdIngestor<AV>
where
    AV: AuthorizationVerifier,
{
    pub async fn create_managed_task(
        pool: Pool<Postgres>,
        settings: &Settings,
        file_upload: FileUpload,
        file_store: FileStore,
        authorization_verifier: AV,
    ) -> anyhow::Result<impl ManagedTask> {
        let (verified_radio_threshold, verified_radio_threshold_server) =
            VerifiedRadioThresholdIngestReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (verified_invalidated_radio_threshold, verified_invalidated_radio_threshold_server) =
            VerifiedInvalidatedRadioThresholdIngestReportV1::file_sink(
                settings.store_base_path(),
                file_upload.clone(),
                FileSinkCommitStrategy::Manual,
                FileSinkRollTime::Default,
                env!("CARGO_PKG_NAME"),
            )
            .await?;

        let (radio_threshold_ingest, radio_threshold_ingest_server) =
            file_source::continuous_source()
                .state(pool.clone())
                .store(file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::RadioThresholdIngestReport.to_string())
                .create()
                .await?;

        // invalidated radio threshold reports
        let (invalidated_radio_threshold_ingest, invalidated_radio_threshold_ingest_server) =
            file_source::continuous_source()
                .state(pool.clone())
                .store(file_store.clone())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .prefix(FileType::InvalidatedRadioThresholdIngestReport.to_string())
                .create()
                .await?;

        let radio_threshold_ingestor = RadioThresholdIngestor::new(
            pool.clone(),
            radio_threshold_ingest,
            invalidated_radio_threshold_ingest,
            verified_radio_threshold,
            verified_invalidated_radio_threshold,
            authorization_verifier,
        );

        Ok(TaskManager::builder()
            .add_task(verified_radio_threshold_server)
            .add_task(verified_invalidated_radio_threshold_server)
            .add_task(radio_threshold_ingest_server)
            .add_task(invalidated_radio_threshold_ingest_server)
            .add_task(radio_threshold_ingestor)
            .build())
    }

    pub fn new(
        pool: sqlx::Pool<Postgres>,
        reports_receiver: Receiver<FileInfoStream<RadioThresholdIngestReport>>,
        invalid_reports_receiver: Receiver<FileInfoStream<InvalidatedRadioThresholdIngestReport>>,
        verified_report_sink: FileSinkClient<VerifiedRadioThresholdIngestReportV1>,
        verified_invalid_report_sink: FileSinkClient<
            VerifiedInvalidatedRadioThresholdIngestReportV1,
        >,
        authorization_verifier: AV,
    ) -> Self {
        Self {
            pool,
            reports_receiver,
            invalid_reports_receiver,
            verified_report_sink,
            verified_invalid_report_sink,
            authorization_verifier,
        }
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting radio threshold ingestor");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                Some(file) = self.reports_receiver.recv() => {
                    self.process_file(file).await?;
                }
                Some(file) = self.invalid_reports_receiver.recv() => {
                    self.process_invalid_file(file).await?;
                }
            }
        }
        tracing::info!("stopping radio threshold ingestor");
        Ok(())
    }

    async fn process_file(
        &self,
        file_info_stream: FileInfoStream<RadioThresholdIngestReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, ingest_report| async move {
                // verify the report
                let verified_report_status = self.verify_report(&ingest_report.report).await;

                // if the report is valid then save to the db
                // and thus available to the rewarder
                match verified_report_status {
                    RadioThresholdReportVerificationStatus::ThresholdReportStatusValid
                    | RadioThresholdReportVerificationStatus::ThresholdReportStatusLegacyValid => {
                        save(&ingest_report, &mut transaction).await?;
                    }
                    _ => {}
                }

                // write out paper trail of verified report, valid or invalid
                let verified_report_proto: VerifiedRadioThresholdIngestReportV1 =
                    VerifiedRadioThresholdIngestReport {
                        report: ingest_report,
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
            })
            .await?
            .commit()
            .await?;
        self.verified_report_sink.commit().await?;
        Ok(())
    }

    async fn process_invalid_file(
        &self,
        file_info_stream: FileInfoStream<InvalidatedRadioThresholdIngestReport>,
    ) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        file_info_stream
            .into_stream(&mut transaction)
            .await?
            .map(anyhow::Ok)
            .try_fold(transaction, |mut transaction, ingest_report| async move {
                // verify the report
                let verified_report_status = self.verify_invalid_report(&ingest_report.report).await;

                // if the report is valid then delete the thresholds from the DB
                if verified_report_status == InvalidatedRadioThresholdReportVerificationStatus::InvalidatedThresholdReportStatusValid {
                     delete(&ingest_report, &mut transaction).await?;
                }

                // write out paper trail of verified report, valid or invalid
                let verified_report_proto: VerifiedInvalidatedRadioThresholdIngestReportV1 =
                    VerifiedInvalidatedRadioThresholdIngestReport {
                        report: ingest_report,
                        status: verified_report_status,
                        timestamp: Utc::now(),
                    }
                        .into();
                self.verified_invalid_report_sink
                    .write(
                        verified_report_proto,
                        &[("report_status", verified_report_status.as_str_name())],
                    )
                    .await?;
                Ok(transaction)
            })
            .await?
            .commit()
            .await?;
        self.verified_invalid_report_sink.commit().await?;
        Ok(())
    }

    async fn verify_report(
        &self,
        report: &RadioThresholdReportReq,
    ) -> RadioThresholdReportVerificationStatus {
        self.do_report_verifications(report).await
    }

    async fn verify_invalid_report(
        &self,
        report: &InvalidatedRadioThresholdReportReq,
    ) -> InvalidatedRadioThresholdReportVerificationStatus {
        if !self.verify_known_carrier_key(&report.carrier_pub_key).await {
            return InvalidatedRadioThresholdReportVerificationStatus::InvalidatedThresholdReportStatusInvalidCarrierKey;
        };
        InvalidatedRadioThresholdReportVerificationStatus::InvalidatedThresholdReportStatusValid
    }

    async fn do_report_verifications(
        &self,
        report: &RadioThresholdReportReq,
    ) -> RadioThresholdReportVerificationStatus {
        if !self.verify_known_carrier_key(&report.carrier_pub_key).await {
            return RadioThresholdReportVerificationStatus::ThresholdReportStatusInvalidCarrierKey;
        };
        RadioThresholdReportVerificationStatus::ThresholdReportStatusValid
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        self.authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
            .unwrap_or_default()
    }
}

pub async fn save(
    ingest_report: &RadioThresholdIngestReport,
    db: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
            INSERT INTO radio_threshold (
            hotspot_pubkey,
            bytes_threshold,
            subscriber_threshold,
            threshold_timestamp,
            threshold_met,
            recv_timestamp)
            VALUES ($1, $2, $3, $4, true, $5)
            ON CONFLICT (hotspot_pubkey)
            DO UPDATE SET
            bytes_threshold = EXCLUDED.bytes_threshold,
            subscriber_threshold = EXCLUDED.subscriber_threshold,
            threshold_timestamp = EXCLUDED.threshold_timestamp,
            recv_timestamp = EXCLUDED.recv_timestamp,
            updated_at = NOW()
            "#,
    )
    .bind(ingest_report.report.hotspot_pubkey.to_string())
    .bind(ingest_report.report.bytes_threshold as i64)
    .bind(ingest_report.report.subscriber_threshold as i32)
    .bind(ingest_report.report.threshold_timestamp)
    .bind(ingest_report.received_timestamp)
    .execute(db)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct VerifiedRadioThresholds {
    gateways: HashSet<PublicKeyBinary>,
}

impl VerifiedRadioThresholds {
    pub fn insert(&mut self, hotspot_key: PublicKeyBinary) {
        self.gateways.insert(hotspot_key);
    }

    pub fn is_verified(&self, key: PublicKeyBinary) -> bool {
        self.gateways.contains(&key)
    }
}

pub async fn verified_radio_thresholds(
    pool: &sqlx::Pool<Postgres>,
    reward_period: &Range<DateTime<Utc>>,
) -> Result<VerifiedRadioThresholds, sqlx::Error> {
    let gateways = sqlx::query_scalar::<_, PublicKeyBinary>(
        "SELECT hotspot_pubkey FROM radio_threshold WHERE threshold_timestamp < $1",
    )
    .bind(reward_period.end)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect::<HashSet<_>>();

    Ok(VerifiedRadioThresholds { gateways })
}

pub async fn delete(
    ingest_report: &InvalidatedRadioThresholdIngestReport,
    db: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
            DELETE FROM radio_threshold WHERE hotspot_pubkey = $1
        "#,
    )
    .bind(ingest_report.report.hotspot_pubkey.to_string())
    .execute(&mut *db)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use file_store::mobile_radio_threshold::{RadioThresholdIngestReport, RadioThresholdReportReq};
    use helium_crypto::{KeyTag, Keypair};
    use rand::rngs::OsRng;
    use sqlx::{prelude::FromRow, Row};

    fn generate_keypair() -> Keypair {
        Keypair::generate(KeyTag::default(), &mut OsRng)
    }

    #[sqlx::test]
    async fn test_save_radio_threshold(pool: PgPool) {
        let now = Utc::now();
        let hotspot_keypair = generate_keypair();
        let hotspot_pubkey = PublicKeyBinary::from(hotspot_keypair.public_key().to_owned());
        let carrier_keypair = generate_keypair();
        let carrier_pubkey = PublicKeyBinary::from(carrier_keypair.public_key().to_owned());

        let report = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey.clone(),
            carrier_pub_key: carrier_pubkey,
            bytes_threshold: 1000,
            subscriber_threshold: 50,
            threshold_timestamp: now,
        };

        let ingest_report = RadioThresholdIngestReport {
            received_timestamp: now,
            report,
        };

        let mut transaction = pool.begin().await.unwrap();
        save(&ingest_report, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        #[derive(Debug, FromRow, PartialEq)]
        struct TestRadioThreshold {
            hotspot_pubkey: PublicKeyBinary,
            bytes_threshold: i64,
            subscriber_threshold: i32,
            threshold_met: bool,
            threshold_timestamp: DateTime<Utc>,
        }

        let result = sqlx::query_as::<_, TestRadioThreshold>(
            r#"
            SELECT
                hotspot_pubkey,
                bytes_threshold,
                subscriber_threshold,
                threshold_timestamp,
                threshold_met
            FROM radio_threshold
            WHERE hotspot_pubkey = $1
            "#,
        )
        .bind(&hotspot_pubkey)
        .fetch_one(&pool)
        .await
        .unwrap();

        pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
            use chrono::{Duration, DurationRound};
            ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
        }

        assert_eq!(
            result,
            TestRadioThreshold {
                hotspot_pubkey,
                bytes_threshold: 1000,
                subscriber_threshold: 50,
                threshold_met: true,
                threshold_timestamp: nanos_trunc(now)
            }
        );
    }

    #[sqlx::test]
    async fn test_save_radio_threshold_update_existing(pool: PgPool) {
        let now = Utc::now();
        let hotspot_keypair = generate_keypair();
        let hotspot_pubkey = PublicKeyBinary::from(hotspot_keypair.public_key().to_owned());
        let carrier_keypair = generate_keypair();
        let carrier_pubkey = PublicKeyBinary::from(carrier_keypair.public_key().to_owned());

        let initial_report = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey.clone(),
            carrier_pub_key: carrier_pubkey.clone(),
            bytes_threshold: 1000,
            subscriber_threshold: 50,
            threshold_timestamp: now,
        };

        let initial_ingest_report = RadioThresholdIngestReport {
            received_timestamp: now,
            report: initial_report,
        };

        // Save initial report
        let mut transaction = pool.begin().await.unwrap();
        save(&initial_ingest_report, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        // Create an updated report with different values
        let updated_now = Utc::now();
        let updated_report = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey.clone(),
            carrier_pub_key: carrier_pubkey,
            bytes_threshold: 2000,     // Changed value
            subscriber_threshold: 100, // Changed value
            threshold_timestamp: updated_now,
        };

        let updated_ingest_report = RadioThresholdIngestReport {
            received_timestamp: updated_now,
            report: updated_report,
        };

        // Save updated report - should update the existing record
        let mut transaction = pool.begin().await.unwrap();
        save(&updated_ingest_report, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        #[derive(Debug, FromRow, PartialEq)]
        struct TestRadioThreshold {
            hotspot_pubkey: PublicKeyBinary,
            bytes_threshold: i64,
            subscriber_threshold: i32,
            threshold_met: bool,
            threshold_timestamp: DateTime<Utc>,
        }

        let result = sqlx::query_as::<_, TestRadioThreshold>(
            r#"
            SELECT
                hotspot_pubkey,
                bytes_threshold,
                subscriber_threshold,
                threshold_timestamp,
                threshold_met
            FROM radio_threshold
            WHERE hotspot_pubkey = $1
            "#,
        )
        .bind(&hotspot_pubkey)
        .fetch_one(&pool)
        .await
        .unwrap();

        pub fn nanos_trunc(ts: DateTime<Utc>) -> DateTime<Utc> {
            use chrono::{Duration, DurationRound};
            ts.duration_trunc(Duration::nanoseconds(1000)).unwrap()
        }

        assert_eq!(
            result,
            TestRadioThreshold {
                hotspot_pubkey,
                bytes_threshold: 2000,
                subscriber_threshold: 100,
                threshold_met: true,
                threshold_timestamp: nanos_trunc(updated_now)
            }
        );
    }

    #[sqlx::test]
    async fn test_delete_radio_threshold(pool: PgPool) {
        let now = Utc::now();
        let hotspot_keypair = generate_keypair();
        let hotspot_pubkey = PublicKeyBinary::from(hotspot_keypair.public_key().to_owned());
        let hotspot_keypair_2 = generate_keypair();
        let hotspot_pubkey_2 = PublicKeyBinary::from(hotspot_keypair_2.public_key().to_owned());
        let carrier_keypair = generate_keypair();
        let carrier_pubkey = PublicKeyBinary::from(carrier_keypair.public_key().to_owned());

        let report = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey.clone(),
            carrier_pub_key: carrier_pubkey.clone(),
            bytes_threshold: 1000,
            subscriber_threshold: 50,
            threshold_timestamp: now,
        };

        let ingest_report = RadioThresholdIngestReport {
            received_timestamp: now,
            report,
        };

        let report_2 = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey_2.clone(),
            carrier_pub_key: carrier_pubkey.clone(),
            bytes_threshold: 1000,
            subscriber_threshold: 50,
            threshold_timestamp: now,
        };

        let ingest_report_2 = RadioThresholdIngestReport {
            received_timestamp: now,
            report: report_2,
        };

        // Save the radio threshold
        let mut transaction = pool.begin().await.unwrap();
        save(&ingest_report, &mut transaction).await.unwrap();
        save(&ingest_report_2, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        // Verify the record exists
        let count_before = sqlx::query(
            r#"
        SELECT COUNT(*) as count
        FROM radio_threshold
        "#,
        )
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("count");

        assert_eq!(count_before, 2, "Records should exist before deletion");

        // Create an invalidated radio threshold report
        let invalidation_report = InvalidatedRadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey.clone(),
            carrier_pub_key: carrier_pubkey,
            reason: 0.try_into().unwrap(),
            timestamp: now,
        };

        let invalidated_ingest_report = InvalidatedRadioThresholdIngestReport {
            received_timestamp: Utc::now(),
            report: invalidation_report,
        };

        // Delete the radio threshold
        let mut transaction = pool.begin().await.unwrap();
        delete(&invalidated_ingest_report, &mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        // Verify the record (2) still exists
        let count_after = sqlx::query(
            r#"
        SELECT COUNT(*) as count
        FROM radio_threshold
        WHERE hotspot_pubkey = $1
        "#,
        )
        .bind(hotspot_pubkey_2.to_string())
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("count");

        assert_eq!(count_after, 1, "Record (2) should still exists");

        // Verify the record (1) was deleted
        let count_after = sqlx::query(
            r#"
        SELECT COUNT(*) as count
        FROM radio_threshold
        WHERE hotspot_pubkey = $1
        "#,
        )
        .bind(hotspot_pubkey.to_string())
        .fetch_one(&pool)
        .await
        .unwrap()
        .get::<i64, _>("count");

        assert_eq!(count_after, 0, "Record (1) should be deleted");
    }

    #[sqlx::test]
    async fn test_verified_radio_thresholds(pool: PgPool) {
        // Setup: Create radio thresholds with different timestamps
        let now = Utc::now();

        // Create two hotspot keypairs
        let hotspot_keypair1 = generate_keypair();
        let hotspot_pubkey1 = PublicKeyBinary::from(hotspot_keypair1.public_key().to_owned());

        let hotspot_keypair2 = generate_keypair();
        let hotspot_pubkey2 = PublicKeyBinary::from(hotspot_keypair2.public_key().to_owned());

        let carrier_keypair = generate_keypair();
        let carrier_pubkey = PublicKeyBinary::from(carrier_keypair.public_key().to_owned());

        // Create radio threshold reports with different timestamps
        // Hotspot 1 - before the reward period end (should be verified)
        let report1 = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey1.clone(),
            carrier_pub_key: carrier_pubkey.clone(),
            bytes_threshold: 1000,
            subscriber_threshold: 50,
            threshold_timestamp: now - chrono::Duration::hours(5), // Before the reward period end
        };

        // Hotspot 2 - after the reward period end (should not be verified)
        let report2 = RadioThresholdReportReq {
            hotspot_pubkey: hotspot_pubkey2.clone(),
            carrier_pub_key: carrier_pubkey.clone(),
            bytes_threshold: 2000,
            subscriber_threshold: 100,
            threshold_timestamp: now + chrono::Duration::hours(2), // After the reward period end
        };

        // Create and save ingest reports
        let ingest_report1 = RadioThresholdIngestReport {
            received_timestamp: now,
            report: report1,
        };

        let ingest_report2 = RadioThresholdIngestReport {
            received_timestamp: now,
            report: report2,
        };

        // Save both reports
        let mut transaction = pool.begin().await.unwrap();
        save(&ingest_report1, &mut transaction).await.unwrap();
        save(&ingest_report2, &mut transaction).await.unwrap();
        transaction.commit().await.unwrap();

        // Define reward period for testing
        let reward_period = (now - chrono::Duration::hours(24))..now;

        // Call the function under test
        let verified_thresholds = verified_radio_thresholds(&pool, &reward_period)
            .await
            .unwrap();

        // Verify results
        assert!(
            verified_thresholds.is_verified(hotspot_pubkey1.clone()),
            "Hotspot 1 should be verified (threshold timestamp before period end)"
        );

        assert!(
            !verified_thresholds.is_verified(hotspot_pubkey2.clone()),
            "Hotspot 2 should not be verified (threshold timestamp after period end)"
        );

        // Verify total count
        let count = verified_thresholds.gateways.len();
        assert_eq!(count, 1, "Should have exactly 1 verified hotspot");
    }
}
