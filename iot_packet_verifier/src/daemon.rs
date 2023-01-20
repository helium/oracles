use crate::{balances::Balances, burner::Burner, ingest, settings::Settings};
use anyhow::{Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use db_store::meta;
use file_store::{file_sink, file_sink_write, file_upload, FileStore, FileType};
use futures::StreamExt;
use futures_util::TryFutureExt;
use helium_crypto::{PublicKey, PublicKeyBinary};
use helium_proto::services::packet_verifier::ValidPacket;
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tokio::time;

struct Daemon {
    pool: Pool<Postgres>,
    file_store: FileStore,
    balances: Balances,
    valid_packets_tx: file_sink::MessageSender,
    invalid_packets_tx: file_sink::MessageSender,
}

const POLL_TIME: time::Duration = time::Duration::from_secs(10);

impl Daemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<()> {
        let mut timer = time::interval(POLL_TIME);
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        let mut last_verified: NaiveDateTime =
            meta::fetch(&self.pool, "last_verified_packet").await?;

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
        let reports =
            ingest::ingest_reports(&self.file_store, DateTime::from_utc(last_verified, Utc));

        tokio::pin!(reports);

        while let Some(report) = reports.next().await {
            let report_timestamp =
                NaiveDateTime::from_timestamp_millis(report.gateway_timestamp_ms as i64).unwrap();

            // Since the report timestamp will always be behind the timestamp for the while,
            // the first few reports we see upon restarting will have already been processed.
            if report_timestamp <= last_verified {
                continue;
            }

            let debit_amount = payload_size_to_dc(report.payload_size as u64);
            let Ok(gateway) = PublicKey::from_bytes(&report.gateway) else {
                tracing::error!("Invalid gateway address: {:?}", report.gateway);
                continue;
            };

            // TODO: Use transactions and write manifests
            if self
                .balances
                .debit_if_sufficient(&gateway, debit_amount)
                .await?
            {
                // Add the amount burned into the pending burns table
                sqlx::query(
                    r#"
                    INSERT INTO pending_burns (gateway, amount)
                    VALUES ($1, $2)
                    ON CONFLICT (gateway) DO UPDATE SET
                    amount = pending_burns.amount + $2
                    "#,
                )
                .bind(PublicKeyBinary::from(gateway.clone()))
                .bind(debit_amount as i64)
                .fetch_one(&self.pool)
                .await?;

                file_sink_write!(
                    "valid_packet",
                    &self.valid_packets_tx,
                    ValidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway,
                        payload_hash: report.payload_hash,
                    }
                )
                .await?;
            } else {
                file_sink_write!(
                    "invalid_packet",
                    &self.invalid_packets_tx,
                    ValidPacket {
                        payload_size: report.payload_size,
                        gateway: report.gateway,
                        payload_hash: report.payload_hash,
                    }
                )
                .await?;
            }

            meta::store(&self.pool, "last_verified", report_timestamp).await?;
            last_verified = report_timestamp;
        }

        Ok(last_verified)
    }
}

pub fn payload_size_to_dc(payload_size: u64) -> u64 {
    payload_size.min(24) / 24
}

pub async fn run_daemon(settings: &Settings) -> Result<()> {
    poc_metrics::install_metrics();

    // Set up the postgres pool:
    let pool = settings.database.connect(10).await?;
    sqlx::migrate!().run(&pool).await?;

    // Set up the solana RpcClient:
    let rpc_client = Arc::new(RpcClient::new(settings.solana_rpc.clone()));

    // Set up the balance tracker:
    let balances = Balances::new(&pool, rpc_client.clone()).await?;

    // Set up the balance burner:
    let burner = Burner::new(&pool, rpc_client, &balances, settings.program_id.clone()).await?;

    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = std::path::Path::new(&settings.cache);

    // Verified packets:
    let (valid_packets_tx, valid_packets_rx) = file_sink::message_channel(50);
    let mut valid_packets =
        file_sink::FileSinkBuilder::new(FileType::ValidPacket, store_base_path, valid_packets_rx)
            .deposits(Some(file_upload_tx.clone()))
            .create()
            .await?;

    let (invalid_packets_tx, invalid_packets_rx) = file_sink::message_channel(50);
    let mut invalid_packets = file_sink::FileSinkBuilder::new(
        FileType::InvalidPacket,
        store_base_path,
        invalid_packets_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let file_store = FileStore::from_settings(&settings.ingest).await?;

    // Set up the verifier Daemon:
    let verifier_daemon = Daemon {
        pool,
        file_store,
        balances,
        valid_packets_tx,
        invalid_packets_tx,
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
        valid_packets.run(&shutdown_listener).map_err(Error::from),
        invalid_packets.run(&shutdown_listener).map_err(Error::from),
    )?;

    Ok(())
}
