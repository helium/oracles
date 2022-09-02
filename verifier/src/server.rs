use crate::{
    env_var,
    error::Result,
    subnetwork_rewards::{SubnetworkRewards, DEFAULT_LOOKUP_DELAY},
};
use helium_proto::{
    follower_client::FollowerClient,
    services::{Channel, Endpoint, Uri},
};
use poc_store::FileStore;
use tokio::{select, time::sleep};

pub struct Server {
    input_store: FileStore,
    output_store: FileStore,
    follower_client: FollowerClient<Channel>,
    last_reward_end_time: i64,
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const DEFAULT_URI: &str = "http://127.0.0.1:8080";

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

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("Starting verifier service");

        // TODO: Update last reward end time
        loop {
            let _ = SubnetworkRewards::from_last_reward_end_time(
                &self.input_store,
                &self.output_store,
                self.follower_client.clone(),
                self.last_reward_end_time,
            );
            // Sleep for 20 hours
            select! {
                _ = sleep(std::time::Duration::from_secs(DEFAULT_LOOKUP_DELAY as u64 * 60 * 60)) => continue,
                _ = shutdown.clone() => break,
            }
        }

        Ok(())
    }
}
