use crate::{
    error::Result,
    heartbeats::Heartbeats,
    reward_share::{OwnerEmissions, OwnerResolver},
    speedtests::SpeedtestAverages,
};
use chrono::{DateTime, Utc};
use file_store::file_sink;
use helium_crypto::PublicKey;
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
        mut follower_service: impl OwnerResolver,
        epoch: &Range<DateTime<Utc>>,
        heartbeats: &Heartbeats,
        // TODO: Use speedtests as part of the reward calculation
        _speedtests: &SpeedtestAverages,
    ) -> Result<Self> {
        // Gather hotspot shares
        let mut hotspot_shares = HashMap::<PublicKey, Decimal>::new();
        for heartbeat in heartbeats.into_iter() {
            *hotspot_shares
                .entry(heartbeat.hotspot_key.clone())
                .or_default() += heartbeat.reward_weight;
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cell_type::CellType, heartbeats::Heartbeats, reward_share::OwnerResolver, shares::Share,
    };
    use chrono::{Duration, Utc};
    use helium_proto::services::poc_mobile::ShareValidity;
    use std::collections::HashMap;

    struct MapResolver {
        owners: HashMap<PublicKey, PublicKey>,
    }

    #[async_trait::async_trait]
    impl OwnerResolver for MapResolver {
        async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>> {
            Ok(self.owners.get(address).cloned())
        }
    }

    #[tokio::test]
    async fn test_single_owner_multiple_hotspots() {
        let g1: PublicKey = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
            .parse()
            .expect("unable to construct pubkey");
        let g2: PublicKey = "118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc"
            .parse()
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let ct1 = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let ct2 = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");

        let owner1: PublicKey = "1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r"
            .parse()
            .expect("unable to get test pubkey");
        let owner2: PublicKey = "1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd"
            .parse()
            .expect("unable to get pubkey");

        let mut owners = HashMap::new();
        owners.insert(g1.clone(), owner1.clone());
        owners.insert(g2.clone(), owner2.clone());

        let resolver = MapResolver { owners };

        let now = Utc::now();
        let timestamp = now.naive_utc();

        let shares = vec![
            Share {
                cbsd_id: c1.clone(),
                pub_key: g1.clone(),
                reward_weight: ct1.reward_weight(),
                cell_type: Some(ct1),
                validity: ShareValidity::Valid,
                timestamp,
            },
            Share {
                cbsd_id: c2.clone(),
                pub_key: g1.clone(),
                reward_weight: ct2.reward_weight(),
                cell_type: Some(ct2),
                validity: ShareValidity::Valid,
                timestamp,
            },
            Share {
                cbsd_id: c1.clone(),
                pub_key: g2.clone(),
                reward_weight: ct1.reward_weight(),
                cell_type: Some(ct1),
                validity: ShareValidity::Valid,
                timestamp,
            },
            Share {
                cbsd_id: c1.clone(),
                pub_key: g2.clone(),
                reward_weight: ct1.reward_weight(),
                cell_type: Some(ct1),
                validity: ShareValidity::Valid,
                timestamp,
            },
        ];

        let heartbeats: Heartbeats = shares.into_iter().collect();

        let owner_rewards: HashMap<PublicKey, _> = SubnetworkRewards::from_epoch(
            resolver,
            &(now..(now + Duration::hours(24))),
            heartbeats,
        )
        .await
        .expect("Could not generate rewards")
        .rewards
        .into_iter()
        .map(|p| (PublicKey::try_from(p.account).unwrap(), p.amount))
        .collect();

        // The owner with two hotspots gets more rewards
        assert!(owner_rewards.get(&owner1).unwrap() > owner_rewards.get(&owner2).unwrap());
    }
}
