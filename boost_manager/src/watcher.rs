use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::file_sink::FileSinkClient;
use file_store::traits::TimestampEncode;
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_proto::BoostedHexUpdateV1 as BoostedHexUpdateProto;
use mobile_config::{
    boosted_hex_info::BoostedHexes, client::hex_boosting_client::HexBoostingInfoResolver,
};
use sqlx::{PgExecutor, Pool, Postgres};
use task_manager::ManagedTask;
use tokio::time;

const POLL_TIME: time::Duration = time::Duration::from_secs(60 * 30);
const LAST_PROCESSED_TIMESTAMP_KEY: &str = "last_processed_hex_boosting_info";

pub struct Watcher {
    pub pool: Pool<Postgres>,
    pub hex_boosting_client: Arc<dyn HexBoostingInfoResolver>,
    pub file_sink: FileSinkClient<BoostedHexUpdateProto>,
}

impl ManagedTask for Watcher {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl Watcher {
    pub async fn new(
        pool: Pool<Postgres>,
        file_sink: FileSinkClient<BoostedHexUpdateProto>,
        hex_boosting_client: Arc<dyn HexBoostingInfoResolver>,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            file_sink,
            hex_boosting_client,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting Watcher");
        let mut timer = time::interval(POLL_TIME);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = timer.tick() => {
                    match self.handle_tick().await {
                        Ok(()) => (),
                        Err(err) => {
                            tracing::error!("fatal Watcher error: {err:?}");
                        }
                    }
                }
            }
        }
        tracing::info!("stopping Watcher");
        Ok(())
    }

    pub async fn handle_tick(&mut self) -> Result<()> {
        let now = Utc::now();
        // get the last time we processed hex boosting info
        let last_processed_ts = fetch_last_processed_timestamp(&self.pool).await?;

        // get modified hex info from mobile config
        let boosted_hexes =
            BoostedHexes::get_modified(self.hex_boosting_client.clone(), last_processed_ts).await?;
        tracing::info!(
            "modified hexes count since {}: {} ",
            last_processed_ts,
            boosted_hexes.hexes.len()
        );
        for info in boosted_hexes.hexes.values() {
            let proto: BoostedHexUpdateProto = BoostedHexUpdateProto {
                timestamp: now.encode_timestamp(),
                update: Some(info.clone().try_into()?),
            };
            self.file_sink.write(proto, []).await?.await??;
        }
        self.file_sink.commit().await?;
        save_last_processed_timestamp(&self.pool, &now).await?;
        Ok(())
    }
}

pub async fn fetch_last_processed_timestamp(
    db: impl PgExecutor<'_>,
) -> db_store::Result<DateTime<Utc>> {
    Utc.timestamp_opt(meta::fetch(db, LAST_PROCESSED_TIMESTAMP_KEY).await?, 0)
        .single()
        .ok_or(db_store::Error::DecodeError)
}

pub async fn save_last_processed_timestamp(
    db: impl PgExecutor<'_>,
    value: &DateTime<Utc>,
) -> db_store::Result<()> {
    meta::store(db, LAST_PROCESSED_TIMESTAMP_KEY, value.timestamp()).await
}
