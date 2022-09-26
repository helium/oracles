use crate::{
    cli::print_json, follower::FollowerService, subnetwork_rewards::SubnetworkRewards, Result,
};
use chrono::NaiveDateTime;
use file_store::{datetime_from_naive, FileStore};
use helium_crypto::PublicKey;
use helium_proto::SubnetworkReward as ProtoSubnetworkReward;
use serde_json::json;

/// Generate poc rewards
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required start time to look for (inclusive)
    #[clap(long)]
    after: NaiveDateTime,
    /// Required before time to look for (inclusive)
    #[clap(long)]
    before: NaiveDateTime,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let mut follower_service = FollowerService::from_env()?;

        SubnetworkRewards::from_period(
            store,
            &mut follower_service,
            datetime_from_naive(self.after),
            datetime_from_naive(self.before),
        )
        .await?
        .map_or_else(
            || Ok(()),
            |r| {
                let proto_rewards: Vec<ProtoSubnetworkReward> = r.into();

                let total_rewards = proto_rewards
                    .iter()
                    .fold(0, |acc, reward| acc + reward.amount);

                // Convert Vec<u8> pubkeys to b58
                let rewards: Vec<(PublicKey, u64)> = proto_rewards
                    .iter()
                    .map(|r| {
                        (
                            PublicKey::try_from(r.account.as_slice())
                                .expect("unable to get public key"),
                            r.amount,
                        )
                    })
                    .collect();
                print_json(&json!({ "rewards": rewards, "total_rewards": total_rewards }))
            },
        )
    }
}
