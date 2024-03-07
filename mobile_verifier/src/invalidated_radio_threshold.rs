use chrono::Utc;
use file_store::{
    file_info_poller::FileInfoStream,
    file_sink::FileSinkClient,
    mobile_radio_invalidated_threshold::{
        InvalidatedRadioThresholdIngestReport, InvalidatedRadioThresholdReportReq,
        VerifiedInvalidatedRadioThresholdIngestReport,
    },
};
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        InvalidatedRadioThresholdReportVerificationStatus,
        VerifiedInvalidatedRadioThresholdIngestReportV1,
    },
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{PgPool, Postgres, Transaction};
use task_manager::ManagedTask;
use tokio::sync::mpsc::Receiver;

pub struct InvalidatedRadioThresholdIngestor<AV> {
    pool: PgPool,
    reports_receiver: Receiver<FileInfoStream<InvalidatedRadioThresholdIngestReport>>,
    verified_report_sink: FileSinkClient,
    authorization_verifier: AV,
}

impl<AV> ManagedTask for InvalidatedRadioThresholdIngestor<AV>
where
    AV: AuthorizationVerifier + Send + Sync + 'static,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures_util::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<AV> InvalidatedRadioThresholdIngestor<AV>
where
    AV: AuthorizationVerifier,
{
    pub fn new(
        pool: sqlx::Pool<Postgres>,
        reports_receiver: Receiver<FileInfoStream<InvalidatedRadioThresholdIngestReport>>,
        verified_report_sink: FileSinkClient,
        authorization_verifier: AV,
    ) -> Self {
        Self {
            pool,
            reports_receiver,
            verified_report_sink,
            authorization_verifier,
        }
    }

    async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting invalidated radio threshold ingestor");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                Some(file) = self.reports_receiver.recv() => {
                    self.process_file(file).await?;
                }
            }
        }
        tracing::info!("stopping invalidated radio threshold ingestor");
        Ok(())
    }

    async fn process_file(
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
                let verified_report_status = self.verify_report(&ingest_report.report).await;

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

    async fn verify_report(
        &self,
        report: &InvalidatedRadioThresholdReportReq,
    ) -> InvalidatedRadioThresholdReportVerificationStatus {
        if !self.verify_known_carrier_key(&report.carrier_pub_key).await {
            return InvalidatedRadioThresholdReportVerificationStatus::InvalidatedThresholdReportStatusInvalidCarrierKey;
        };
        InvalidatedRadioThresholdReportVerificationStatus::InvalidatedThresholdReportStatusValid
    }

    async fn verify_known_carrier_key(&self, public_key: &PublicKeyBinary) -> bool {
        match self
            .authorization_verifier
            .verify_authorized_key(public_key, NetworkKeyRole::MobileCarrier)
            .await
        {
            Ok(res) => res,
            Err(_err) => false,
        }
    }
}

pub async fn delete(
    ingest_report: &InvalidatedRadioThresholdIngestReport,
    db: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
            DELETE FROM radio_threshold
            WHERE hotspot_pubkey = $1 AND (cbsd_id is null or cbsd_id = $2)
        "#,
    )
    .bind(ingest_report.report.hotspot_pubkey.to_string())
    .bind(ingest_report.report.cbsd_id.clone())
    .execute(&mut *db)
    .await?;
    Ok(())
}
