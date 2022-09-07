use crate::{env_var, error::Result, subnetwork_rewards::SubnetworkRewards};
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use helium_proto::{
    follower_client::FollowerClient,
    services::{Channel, Endpoint, Uri},
};
use poc_store::FileStore;
use tokio::{select, time::sleep};

pub struct Server {
    pub input_store: FileStore,
    pub output_store: FileStore,
    pub follower_client: FollowerClient<Channel>,
    pub last_reward_end_time: i64,
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

/// Default hours to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 24;

impl Server {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            input_store: FileStore::from_env_with_prefix("INPUT").await?,
            output_store: FileStore::from_env_with_prefix("OUTPUT").await?,
            follower_client: FollowerClient::new(
                Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                    .connect_timeout(CONNECT_TIMEOUT)
                    .timeout(RPC_TIMEOUT)
                    .connect_lazy(),
            ),
            last_reward_end_time: env_var("LAST_REWARD_END_TIME", 0)?,
        })
    }

    pub async fn run(self, shutdown: triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        let Self {
            input_store,
            output_store,
            follower_client,
            last_reward_end_time,
        } = self;

        let mut last_reward_end_time =
            DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(last_reward_end_time, 0), Utc);
        loop {
            let (start, stop) = get_time_range(last_reward_end_time);
            let _ = SubnetworkRewards::from_period(
                &input_store,
                &output_store,
                follower_client.clone(),
                start,
                stop,
            );
            last_reward_end_time = stop;
            // Sleep for 20 hours
            select! {
                _ = sleep(std::time::Duration::from_secs(DEFAULT_LOOKUP_DELAY as u64 * 60 * 60)) => continue,
                _ = shutdown.clone() => break,
            }
        }

        Ok(())
    }
}

pub fn get_time_range(after_utc: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let now = Utc::now();
    let stop_utc = now - Duration::hours(DEFAULT_LOOKUP_DELAY);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}
