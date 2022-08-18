use crate::{
    cli::print_json, follower::FollowerService, subnetwork_rewards::SubnetworkRewards, Result,
};
use chrono::NaiveDateTime;
use helium_proto::SubnetworkReward as ProtoSubnetworkReward;
use poc_store::{datetime_from_naive, FileStore};
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
        let follower_service = FollowerService::from_env()?;

        let rewards = SubnetworkRewards::from_period(
            store,
            follower_service,
            datetime_from_naive(self.after),
            datetime_from_naive(self.before),
        )
        .await?;

        let json = match rewards {
            None => {
                json!({ "rewards": "null", "total": 0 })
            }
            Some(r) => {
                let proto_rewards: Vec<ProtoSubnetworkReward> = r.clone().into();
                let total = proto_rewards
                    .iter()
                    .fold(0, |acc, reward| acc + reward.amount);
                json!({ "rewards": r, "total": total })
            }
        };
        print_json(&json)
    }
}
