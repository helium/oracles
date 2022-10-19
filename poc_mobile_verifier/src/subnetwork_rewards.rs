use crate::{
    error::Result,
    heartbeats::Heartbeat,
    reward_share::{OwnerEmissions, OwnerResolver},
};
use chrono::{DateTime, Utc};
use file_store::file_sink;
use helium_crypto::PublicKey;
use helium_proto::services::{follower, Channel};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::ops::Range;
use tokio::sync::oneshot;

mod proto {
    pub use helium_proto::services::poc_mobile::*;
    pub use helium_proto::{SubnetworkReward, SubnetworkRewards};
}

pub struct SubnetworkRewards {
    pub epoch: Range<DateTime<Utc>>,
    pub rewards: Vec<proto::SubnetworkReward>,
}

impl SubnetworkRewards {
    pub async fn from_epoch(
        mut follower_service: follower::Client<Channel>,
        epoch: &Range<DateTime<Utc>>,
        heartbeats: &Vec<Heartbeat>,
    ) -> Result<Self> {
        // Gather hotspot shares
        let mut hotspot_shares = HashMap::<PublicKey, Decimal>::new();
        for heartbeat in heartbeats {
            *hotspot_shares.entry(heartbeat.hotspot_key.clone()).or_default() +=
                heartbeat.reward_weight;
        }

        let (owner_shares, _missing_owner_shares) =
            follower_service.owner_shares(hotspot_shares).await?;

        let owner_emissions =
            OwnerEmissions::new(owner_shares, epoch.start, epoch.end - epoch.start);

        let mut rewards = owner_emissions
            .into_inner()
            .into_iter()
            .map(|(owner, amt)| proto::SubnetworkReward {
                account: owner.to_vec(),
                amount: u64::from(amt),
            })
            .collect::<Vec<_>>();

        rewards.sort_by(|a, b| {
            a.account
                .cmp(&b.account)
                .then_with(|| a.amount.cmp(&b.amount))
        });

        Ok(Self {
            epoch: epoch.clone(),
            rewards,
        })
    }

    pub async fn write(
        self,
        subnet_rewards_tx: &file_sink::MessageSender,
    ) -> file_store::Result<oneshot::Receiver<file_store::Result>> {
        file_sink::write(
            subnet_rewards_tx,
            proto::SubnetworkRewards {
                start_epoch: self.epoch.start.timestamp() as u64,
                end_epoch: self.epoch.end.timestamp() as u64,
                rewards: self.rewards,
            },
        )
        .await
    }
}
