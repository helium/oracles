use crate::Settings;
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source,
    file_upload::FileUpload,
    radio_location_estimates_ingest_report::RadioLocationEstimatesIngestReport,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore, FileType,
};
use helium_proto::services::poc_mobile::{
    RadioLocationEstimatesIngestReportV1, VerifiedRadioLocationEstimatesReportV1,
};
use mobile_config::client::authorization_client::AuthorizationVerifier;
use sqlx::{Pool, Postgres};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::mpsc::Receiver;

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
        authorization_verifier: AV,
        file_store: FileStore,
        file_upload: FileUpload,
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
                Some(_file) = self.reports_receiver.recv() => {
                    // self.process_file(file).await?;
                }
            }
        }
        Ok(())
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
