use crate::{
    balances::BalanceCache,
    burner::Burner,
    settings::Settings,
    verifier::{CachedOrgClient, ConfigServer, Verifier},
};
use anyhow::{bail, Result};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkBuilder,
    file_sink::FileSinkClient,
    file_source, file_upload,
    iot_packet::PacketRouterPacketReport,
    FileStore, FileType,
};
use futures_util::TryFutureExt;
use iot_config::client::OrgClient;
use solana::{balance_monitor::BalanceMonitor, SolanaRpc};
use sqlx::{Pool, Postgres};
use std::{sync::Arc, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tokio::sync::{mpsc::Receiver, Mutex};

struct Daemon {
    pool: Pool<Postgres>,
    verifier: Verifier<BalanceCache<Option<Arc<SolanaRpc>>>, Arc<Mutex<CachedOrgClient>>>,
    report_files: Receiver<FileInfoStream<PacketRouterPacketReport>>,
    valid_packets: FileSinkClient,
    invalid_packets: FileSinkClient,
    minimum_allowed_balance: u64,
}

impl ManagedTask for Daemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl Daemon {
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        tracing::info!("starting daemon");
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                file = self.report_files.recv() => {
                    if let Some(file) = file {
                        self.handle_file(file).await?
                    } else {
                        bail!("Report file stream was dropped")
                    }
                }

            }
        }
        tracing::info!("stopping daemon");
        Ok(())
    }

    async fn handle_file(
        &mut self,
        report_file: FileInfoStream<PacketRouterPacketReport>,
    ) -> Result<()> {
        tracing::info!(file = %report_file.file_info, "Verifying file");

        let mut transaction = self.pool.begin().await?;
        let reports = report_file.into_stream(&mut transaction).await?;

        self.verifier
            .verify(
                self.minimum_allowed_balance,
                &mut transaction,
                reports,
                &self.valid_packets,
                &self.invalid_packets,
            )
            .await?;
        transaction.commit().await?;
        self.valid_packets.commit().await?;
        self.invalid_packets.commit().await?;

        Ok(())
    }
}

#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result<()> {
        poc_metrics::start_metrics(&settings.metrics)?;

        // Set up the postgres pool:
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        let solana = if settings.enable_solana_integration {
            let Some(ref solana_settings) = settings.solana else {
                bail!("Missing solana section in settings");
            };
            // Set up the solana RpcClient:
            Some(SolanaRpc::new(solana_settings).await?)
        } else {
            None
        };

        let sol_balance_monitor = BalanceMonitor::new(env!("CARGO_PKG_NAME"), solana.clone())?;

        // Set up the balance cache:
        let balances = BalanceCache::new(&mut pool.clone(), solana.clone()).await?;

        // Set up the balance burner:
        let burner = Burner::new(
            pool.clone(),
            &balances,
            settings.burn_period,
            solana.clone(),
        );

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;

        let store_base_path = std::path::Path::new(&settings.cache);

        // Verified packets:
        let (valid_packets, valid_packets_server) = FileSinkBuilder::new_tm(
            FileType::IotValidPacket,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_valid_packets"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let (invalid_packets, invalid_packets_server) = FileSinkBuilder::new_tm(
            FileType::InvalidPacket,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_invalid_packets"),
        )
        .file_upload(Some(file_upload.clone()))
        .auto_commit(false)
        .create()
        .await?;

        let org_client = Arc::new(Mutex::new(CachedOrgClient::new(OrgClient::from_settings(
            &settings.iot_config_client,
        )?)));

        let file_store = FileStore::from_settings(&settings.ingest).await?;

        let (report_files, report_files_server) =
            file_source::continuous_source::<PacketRouterPacketReport>()
                .db(pool.clone())
                .store(file_store)
                .lookback(LookbackBehavior::StartAfter(settings.start_after()))
                .file_type(FileType::IotPacketReport)
                .create()?;

        let balance_store = balances.balances();
        let verifier_daemon = Daemon {
            pool,
            report_files,
            valid_packets,
            invalid_packets,
            verifier: Verifier {
                debiter: balances,
                config_server: org_client.clone(),
            },
            minimum_allowed_balance: settings.minimum_allowed_balance,
        };

        // Run the services:
        let minimum_allowed_balance = settings.minimum_allowed_balance;
        let monitor_funds_period = settings.monitor_funds_period;

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(valid_packets_server)
            .add_task(invalid_packets_server)
            .add_task(move |shutdown| {
                org_client
                    .monitor_funds(
                        solana,
                        balance_store,
                        minimum_allowed_balance,
                        Duration::from_secs(60 * monitor_funds_period),
                        shutdown,
                    )
                    .map_err(anyhow::Error::from)
            })
            .add_task(verifier_daemon)
            .add_task(sol_balance_monitor)
            .add_task(burner)
            .add_task(report_files_server)
            .start()
            .await
    }
}
