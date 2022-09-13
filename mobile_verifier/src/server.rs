use crate::{env_var, error::Result, subnetwork_rewards::SubnetworkRewards};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use helium_proto::services::{follower, Channel, Endpoint, Uri};
use poc_store::FileStore;
use tokio::{select, time::sleep};

pub struct Server {
    pub input_store: FileStore,
    pub output_store: FileStore,
    pub follower_client: follower::Client<Channel>,
    pub last_reward_end_time: i64,
    pub lookup_delay: i64,
}

pub const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

/// Default hours to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 3;

impl Server {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            input_store: FileStore::from_env_with_prefix("INPUT").await?,
            output_store: FileStore::from_env_with_prefix("OUTPUT").await?,
            follower_client: follower::Client::new(
                Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(RPC_TIMEOUT)
                    .connect_lazy(),
            ),
            last_reward_end_time: env_var("LAST_REWARD_END_TIME", 0)?,
            lookup_delay: env_var("LOOKUP_DELAY", DEFAULT_LOOKUP_DELAY)?,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let Self {
            input_store,
            output_store,
            follower_client,
            last_reward_end_time,
            lookup_delay,
        } = self;

        let mut last_reward_end_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(last_reward_end_time, 0), Utc);
        loop {
            let (start, stop) = get_time_range(last_reward_end_time, lookup_delay);
            let _ = SubnetworkRewards::from_period(
                &input_store,
                &output_store,
                follower_client.clone(),
                start,
                stop,
            );
            last_reward_end_time = stop;
            select! {
                _ = sleep(std::time::Duration::from_secs(lookup_delay as u64 * 60 * 60)) => continue,
                _ = shutdown.clone() => break,
            }
        }

        Ok(())
    }
}

pub fn get_time_range(
    after_utc: DateTime<Utc>,
    lookup_delay: i64,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let stop_utc = now - Duration::hours(lookup_delay);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}
