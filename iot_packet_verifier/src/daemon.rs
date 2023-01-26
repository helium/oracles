use crate::{balances::Balances, burner::Burner, ingest, settings::Settings};
use anyhow::{bail, Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use db_store::meta;
use file_store::{file_sink::FileSinkClient, file_upload, FileSinkBuilder, FileStore, FileType};
use futures::StreamExt;
use futures_util::TryFutureExt;
use helium_crypto::{Keypair, PublicKeyBinary};
use helium_proto::services::{
    iot_config::{org_client::OrgClient, OrgGetReqV1},
    packet_verifier::{InvalidPacket, ValidPacket},
    Channel,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::read_keypair_file};
use sqlx::{Pool, Postgres};
use std::{collections::HashMap, sync::Arc};
use tokio::time;

struct Daemon {
    pool: Pool<Postgres>,
    keypair: Keypair,
    file_store: FileStore,
    balances: Balances,
    org_client: OrgClient<Channel>,
    sub_dao: Pubkey,
    valid_packets: FileSinkClient,
    invalid_packets: FileSinkClient,
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

        let mut org_cache = HashMap::<u64, PublicKeyBinary>::new();

        while let Some(report) = reports.next().await {
            let report_timestamp =
                NaiveDateTime::from_timestamp_millis(report.gateway_timestamp_ms as i64).unwrap();

            // Since the report timestamp will always be behind the timestamp for the while,
            // the first few reports we see upon restarting will have already been processed.
            if report_timestamp <= last_verified {
                continue;
            }

            let debit_amount = payload_size_to_dc(report.payload_size as u64);

            if !org_cache.contains_key(&report.oui) {
                let req = OrgGetReqV1 { oui: report.oui };
                let pubkey = PublicKeyBinary::from(
                    self.org_client
                        .get(req)
                        .await?
                        .into_inner()
                        .org
                        .unwrap()
                        .owner,
                );
                org_cache.insert(report.oui, pubkey);
            }

            let payer = org_cache.get(&report.oui).unwrap();
            let sufficiency = self
                .balances
                .debit_if_sufficient(&self.sub_dao, payer, debit_amount)
                .await?;

            // TODO: Use transactions and write manifests
            if sufficiency.is_sufficient() {
                // Add the amount burned into the pending burns table
                sqlx::query(
                    r#"
                    INSERT INTO pending_burns (payer, amount, last_burn)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (payer) DO UPDATE SET
                    amount = pending_burns.amount + $2
                    "#,
                )
                .bind(payer)
                .bind(debit_amount as i64)
                .bind(Utc::now().naive_utc())
                .fetch_one(&self.pool)
                .await?;

                self.valid_packets
                    .write(
                        ValidPacket {
                            payload_size: report.payload_size,
                            gateway: report.gateway,
                            payload_hash: report.payload_hash,
                        },
                        [],
                    )
                    .await?;
            } else {
                self.invalid_packets
                    .write(
                        InvalidPacket {
                            payload_size: report.payload_size,
                            gateway: report.gateway,
                            payload_hash: report.payload_hash,
                        },
                        [],
                    )
                    .await?;
            }

            sufficiency
                .configure_org(&mut self.org_client, &self.keypair, report.oui)
                .await?;

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

    let (sub_dao, _) = Pubkey::find_program_address(
        &["sub_dao".as_bytes(), settings.dnt_mint.as_ref()],
        &helium_sub_daos::ID,
    );

    // Set up the balance tracker:
    let balances = Balances::new(&pool, &sub_dao, rpc_client.clone()).await?;

    // Set up the balance burner:
    let Ok(burn_keypair) = read_keypair_file(&settings.burn_keypair) else {
        bail!("Failed to read keypair file ({})", settings.burn_keypair.display());
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
        keypair: config_keypair,
        file_store,
        balances,
        org_client,
        sub_dao,
        valid_packets,
        invalid_packets,
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
