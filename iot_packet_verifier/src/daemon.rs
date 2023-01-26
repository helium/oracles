use crate::{balances::Balances, burner::Burner, ingest, settings::Settings, verifier::Verifier};
use anyhow::{bail, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use db_store::meta;
use file_store::{file_upload, FileSinkBuilder, FileStore, FileType};
use futures::StreamExt;
use futures_util::TryFutureExt;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::read_keypair_file};
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::time;

struct Daemon {
    pool: Pool<Postgres>,
    verifier: Verifier,
    file_store: FileStore,
}

const POLL_TIME: time::Duration = time::Duration::from_secs(10);

impl Daemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<()> {
        let mut timer = time::interval(POLL_TIME);
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let mut last_verified: NaiveDateTime =
            meta::fetch(&self.pool, "last_verified_report").await?;

        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = timer.tick() => {
                    last_verified = self.handle_tick(last_verified.clone()).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_tick(&mut self, mut last_verified: NaiveDateTime) -> Result<NaiveDateTime> {
        let mut reports = self
            .file_store
            .list(
                FileType::IotPacketReport,
                DateTime::from_utc(last_verified, Utc),
                None,
            )
            .boxed();

        while let Some(report) = reports.next().await.transpose()? {
            last_verified = report.timestamp.naive_utc();
            meta::store(&self.pool, "last_verified_report", last_verified).await?;
            self.verifier
                .verify(
                    &self.pool,
                    ingest::ingest_reports(&self.file_store, report).await?,
                )
                .await?;
        }

        Ok(last_verified)
    }
}

pub async fn run_daemon(settings: &Settings) -> Result<()> {
    poc_metrics::install_metrics();

    // Set up the postgres pool:
    let pool = settings.database.connect(10).await?;
    sqlx::migrate!().run(&pool).await?;

    // Set up the solana RpcClient:
    let rpc_client = Arc::new(RpcClient::new(settings.solana_rpc.clone()));

    let (sub_dao, _) = Pubkey::find_program_address(
        &["sub_dao".as_bytes(), settings.dnt_mint()?.as_ref()],
        &helium_sub_daos::ID,
    );

    // Set up the balance tracker:
    let balances = Balances::new(&pool, &sub_dao, rpc_client.clone()).await?;

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
        FileType::ValidPacket,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_valid_packets"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let (invalid_packets, mut invalid_packets_server) = FileSinkBuilder::new(
        FileType::InvalidPacket,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_invalid_packets"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let org_client = settings.org.connect_org();

    let file_store = FileStore::from_settings(&settings.ingest).await?;

    // Set up the verifier Daemon:
    let config_keypair = settings.config_keypair()?;
    let verifier_daemon = Daemon {
        pool,
        file_store,
        verifier: Verifier {
            keypair: config_keypair,
            balances,
            org_client,
            sub_dao,
            valid_packets,
            invalid_packets,
        },
    };

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // Run the services:
    tokio::try_join!(
        burner.run(&shutdown_listener).map_err(Error::from),
        file_upload.run(&shutdown_listener).map_err(Error::from),
        verifier_daemon.run(&shutdown_listener).map_err(Error::from),
        valid_packets_server
            .run(&shutdown_listener)
            .map_err(Error::from),
        invalid_packets_server
            .run(&shutdown_listener)
            .map_err(Error::from),
    )?;

    Ok(())
}
