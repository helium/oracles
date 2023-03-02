use crate::{
    balances::BalanceCache,
    burner::Burner,
    settings::Settings,
    verifier::{CachedOrgClient, Verifier},
};
use anyhow::{bail, Error, Result};
use chrono::{TimeZone, Utc};
use file_store::{
    file_info_poller::{FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    file_source, file_upload,
    iot_packet::PacketRouterPacketReport,
    FileSinkBuilder, FileStore, FileType,
};
use futures_util::TryFutureExt;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::read_keypair_file};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

struct Daemon {
    pool: Pool<Postgres>,
    verifier: Verifier<BalanceCache, CachedOrgClient>,
    report_files: Receiver<FileInfoStream<PacketRouterPacketReport>>,
    valid_packets: FileSinkClient,
    invalid_packets: FileSinkClient,
}

impl Daemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<()> {
        loop {
            tokio::select! {
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

        Ok(())
    }

    async fn handle_file(
        &mut self,
        report_file: FileInfoStream<PacketRouterPacketReport>,
    ) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        let reports = report_file.into_stream(&mut transaction).await?;

        self.verifier
            .verify(
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

pub async fn run_daemon(settings: &Settings) -> Result<()> {
    poc_metrics::install_metrics();

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // Set up the postgres pool:
    let (pool, db_handle) = settings
        .database
        .connect(env!("CARGO_PKG_NAME"), shutdown_listener.clone())
        .await?;
    sqlx::migrate!().run(&pool).await?;

    // Set up the solana RpcClient:
    let rpc_client = Arc::new(RpcClient::new(settings.solana_rpc.clone()));

    let (sub_dao, _) = Pubkey::find_program_address(
        &["sub_dao".as_bytes(), settings.dnt_mint()?.as_ref()],
        &helium_sub_daos::ID,
    );

    // Set up the balance cache:
    let balances = BalanceCache::new(&pool, sub_dao, rpc_client.clone()).await?;

    // Set up the balance burner:
    let burn_keypair = match read_keypair_file(&settings.burn_keypair) {
        Ok(kp) => kp,
        Err(e) => bail!("Failed to read keypair file ({})", e),
    };
    let burner = Burner::new(settings, &pool, &balances, rpc_client, burn_keypair).await?;

    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = std::path::Path::new(&settings.cache);

    // Verified packets:
    let (valid_packets, mut valid_packets_server) = FileSinkBuilder::new(
        FileType::IotValidPacket,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_valid_packets"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .auto_commit(false)
    .create()
    .await?;

    let (invalid_packets, mut invalid_packets_server) = FileSinkBuilder::new(
        FileType::InvalidPacket,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_invalid_packets"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .auto_commit(false)
    .create()
    .await?;

    let org_client = settings.connect_org();

    let file_store = FileStore::from_settings(&settings.ingest).await?;

    let (report_files, source_join_handle) =
        file_source::continuous_source::<PacketRouterPacketReport>()
            .db(pool.clone())
            .store(file_store)
            .lookback(LookbackBehavior::StartAfter(
                Utc.timestamp_millis_opt(0).unwrap(),
            ))
            .file_type(FileType::IotPacketReport)
            .build()?
            .start(shutdown_listener.clone())
            .await?;

    let config_keypair = settings.config_keypair()?;
    let verifier_daemon = Daemon {
        pool,
        report_files,
        valid_packets,
        invalid_packets,
        verifier: Verifier {
            debiter: balances,
            config_server: CachedOrgClient::new(org_client, config_keypair),
        },
    };

    // Run the services:
    tokio::try_join!(
        db_handle.map_err(Error::from),
        burner.run(&shutdown_listener).map_err(Error::from),
        file_upload.run(&shutdown_listener).map_err(Error::from),
        verifier_daemon.run(&shutdown_listener).map_err(Error::from),
        valid_packets_server
            .run(&shutdown_listener)
            .map_err(Error::from),
        invalid_packets_server
            .run(&shutdown_listener)
            .map_err(Error::from),
        source_join_handle.map_err(Error::from),
    )?;

    Ok(())
}
