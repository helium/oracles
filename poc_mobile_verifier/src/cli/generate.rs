use crate::{
    env_var,
    server::{CONNECT_TIMEOUT, DEFAULT_URI, RPC_TIMEOUT},
    subnetwork_rewards::SubnetworkRewards,
    Result,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::FileStore;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Endpoint, Uri};
use serde_json::json;

/// Verify the shares for a given time range
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    after: NaiveDateTime,
    #[clap(long)]
    before: NaiveDateTime,
    #[clap(long)]
    input_bucket: String,
}

impl Cmd {
    pub async fn run(self) -> Result {
        let Self {
            after,
            before,
            input_bucket,
        } = self;

        tracing::info!(
            "Verifying shares from bucket {input_bucket} within the following time range: {after} to {before}"
        );

        let input_store = FileStore::new(None, "us-west-2", input_bucket).await?;

        let follower_service = follower::Client::new(
            Endpoint::from(env_var("FOLLOWER_URI", Uri::from_static(DEFAULT_URI))?)
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(RPC_TIMEOUT)
                .connect_lazy(),
        );

        let rewards = SubnetworkRewards::from_period(
            &input_store,
            follower_service,
            DateTime::from_utc(after, Utc),
            DateTime::from_utc(before, Utc),
        )
        .await?;

        let total_rewards = rewards
            .rewards
            .iter()
            .fold(0, |acc, reward| acc + reward.amount);
        let rewards: Vec<(PublicKey, u64)> = rewards
            .rewards
            .iter()
            .map(|r| {
                (
                    PublicKey::try_from(r.account.as_slice()).expect("unable to get public key"),
                    r.amount,
                )
            })
            .collect();

        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "rewards": rewards,
                "total_rewards": total_rewards,
            }))
            .unwrap()
        );

        Ok(())
    }
}
