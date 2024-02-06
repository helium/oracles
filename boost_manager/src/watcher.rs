use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use db_store::meta;
use file_store::file_sink::FileSinkClient;
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_proto::BoostedHexUpdateV1 as BoostedHexUpdateProto;
use file_store::traits::TimestampEncode;
use mobile_config::{
    boosted_hex_info::BoostedHexes,
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
};
use sqlx::{PgExecutor, Pool, Postgres};
use task_manager::ManagedTask;
use tokio::time;

const POLL_TIME: time::Duration = time::Duration::from_secs(60 * 30);
const LAST_PROCESSED_TIMESTAMP_KEY: &str = "last_processed_hex_boosting_info";

pub struct Watcher<A> {
    pub pool: Pool<Postgres>,
    pub hex_boosting_client: A,
    pub file_sink: FileSinkClient,
}

impl<A> ManagedTask for Watcher<A>
where
    A: HexBoostingInfoResolver<Error = ClientError>,
{
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

impl<A> Watcher<A>
where
    A: HexBoostingInfoResolver<Error = ClientError>,
{
    pub async fn new(
        pool: Pool<Postgres>,
        file_sink: FileSinkClient,
        hex_boosting_client: A,
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
                        Ok(()) => {
                            tracing::info!("successfully processed hex boosting info");
                        }
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
            BoostedHexes::get_modified(&self.hex_boosting_client, last_processed_ts).await?;
        tracing::info!("modified boosted_hexes: {:?}", boosted_hexes);

        for (location, info) in &boosted_hexes.hexes {
            tracing::info!("location: {}, info: {:?}", location, info);
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
