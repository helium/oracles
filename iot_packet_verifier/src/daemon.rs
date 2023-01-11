use crate::balances::Balances;
use crate::ingest;
use crate::settings::Settings;
use crate::verifier::Verifier;
use anyhow::{Error, Result};
use chrono::{Duration, TimeZone, Utc};
use db_store::meta;
use file_store::{file_sink, file_sink_write, file_upload, FileStore, FileType};
use futures::StreamExt;
use futures_util::TryFutureExt;
use node_follower::follower_service::FollowerService;
use sqlx::{Pool, Postgres};
use tokio::time;

struct Daemon {
    pool: Pool<Postgres>,
    file_store: FileStore,
    balances: Balances,
    verified_packets_tx: file_sink::MessageSender,
}

const POLL_TIME: time::Duration = time::Duration::from_secs(303);

impl Daemon {
    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<()> {
        let mut timer = time::interval(POLL_TIME);
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = timer.tick() => self.handle_tick().await?,
            }
        }

        Ok(())
    }

    async fn handle_tick(&mut self) -> Result<()> {
        let last_verified =
            Utc.timestamp(meta::fetch(&self.pool, "last_verified_end_time").await?, 0);
        let next_verified = Utc::now() - Duration::minutes(10);

        if last_verified >= next_verified {
            return Ok(());
        }

        let verified_packets = Verifier::new(ingest::ingest_reports(
            &self.file_store,
            &(last_verified..next_verified),
        ))
        .verify_stream(&self.balances);

        tokio::pin!(verified_packets);

        while let Some(verified_packet) = verified_packets.next().await {
            if !verified_packet.is_valid {
                // TODO: If we encounter a packet that is not valid, disable the
                // oui via the config server
                todo!();
            }

            file_sink_write!(
                "verified_packet",
                &self.verified_packets_tx,
                verified_packet
            )
            .await?;
        }

        meta::store(
            &self.pool,
            "last_verified_end_time",
            next_verified.timestamp(),
        )
        .await?;

        Ok(())
    }
}

pub async fn run_daemon(settings: &Settings) -> Result<()> {
    poc_metrics::install_metrics();

    // Set up the BalanceFollower:
    let mut follower = FollowerService::from_settings(&settings.follower);
    let balances = Balances::new(todo!(), todo!());

    // Set up the verifier Daemon:
    let pool = settings.database.connect(10).await?;
    sqlx::migrate!().run(&pool).await?;

    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = std::path::Path::new(&settings.cache);

    // Verified packets:
    let (verified_packets_tx, verified_packets_rx) = file_sink::message_channel(50);
    let mut verified_packets = file_sink::FileSinkBuilder::new(
        FileType::VerifiedPacket,
        store_base_path,
        verified_packets_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .create()
    .await?;

    let file_store = FileStore::from_settings(&settings.ingest).await?;

    let verifier_daemon = Daemon {
        pool,
        file_store,
        balances,
        verified_packets_tx,
    };

    let (shutdown_trigger, shutdown_listener) = triggered::trigger();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_trigger.trigger()
    });

    // Run the services:
    tokio::try_join!(
        file_upload.run(&shutdown_listener).map_err(Error::from),
        verifier_daemon.run(&shutdown_listener).map_err(Error::from),
        verified_packets
            .run(&shutdown_listener)
            .map_err(Error::from),
    )?;

    Ok(())
}
