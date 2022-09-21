use crate::{env_var, error::Result, subnetwork_rewards::SubnetworkRewards};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use helium_proto::services::{follower, Channel, Endpoint, Uri};
use poc_store::{FileStore, MetaValue};
use sqlx::{Pool, Postgres};
use tokio::{select, time::sleep};

pub struct Server {
    pub input_store: FileStore,
    pub output_store: FileStore,
    pub follower_client: follower::Client<Channel>,
    pub last_reward_end_time: MetaValue<i64>,
    pub lookup_delay: i64,
    pub pool: Pool<Postgres>,
}

pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

/// Default hours to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 3;

impl Server {
    pub async fn new(pool: Pool<Postgres>) -> Result<Self> {
        Ok(Self {
            input_store: FileStore::from_env_with_prefix("INPUT").await?,
            output_store: FileStore::from_env_with_prefix("OUTPUT").await?,
            follower_client: follower::Client::new(
                Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(RPC_TIMEOUT)
                    .connect_lazy(),
            ),
            last_reward_end_time: MetaValue::<i64>::fetch_or_insert_with(
                &pool,
                "last_reward_end_time",
                || env_var("LAST_REWARD_END_TIME", 0).unwrap(),
            )
            .await?,
            lookup_delay: env_var("LOOKUP_DELAY", DEFAULT_LOOKUP_DELAY)?,
            pool,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let Self {
            input_store,
            output_store,
            follower_client,
            mut last_reward_end_time,
            lookup_delay,
            pool,
        } = self;

        loop {
            let (start, stop) = get_time_range(*last_reward_end_time.value(), lookup_delay);
            SubnetworkRewards::from_period(&input_store, follower_client.clone(), start, stop)
                .await?
                .write(&output_store)
                .await?;
            last_reward_end_time.update(&pool, stop.timestamp()).await?;
            select! {
                _ = sleep(std::time::Duration::from_secs(lookup_delay as u64 * 60 * 60)) => continue,
                _ = shutdown.clone() => break,
            }
        }

        Ok(())
    }
}

pub fn get_time_range(
    last_reward_end_time: i64, // DateTime<Utc>,
    lookup_delay: i64,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc =
        DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(last_reward_end_time, 0), Utc);
    let now = Utc::now();
    let stop_utc = now - Duration::hours(lookup_delay);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}
