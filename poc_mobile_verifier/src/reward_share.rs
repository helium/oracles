use crate::{
    heartbeats::Heartbeats,
    mobile::Mobile,
    speedtests::{Average, SpeedtestAverages},
    Result,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1},
    poc_mobile as proto, Channel,
};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::collections::HashMap;
use std::ops::Range;

pub struct RadioShare {
    hotspot_key: PublicKey,
    cbsd_id: String,
    amount: Decimal,
}

#[derive(Default)]
pub struct OwnerShares {
    shares: HashMap<PublicKey, Vec<RadioShare>>,
}

impl OwnerShares {
    pub fn total_shares(&self) -> Decimal {
        self.shares
            .iter()
            .fold(Decimal::ZERO, |sum, (_, radio_shares)| {
                sum + radio_shares
                    .iter()
                    .fold(Decimal::ZERO, |sum, radio_share| sum + radio_share.amount)
            })
    }

    pub fn into_radio_shares(
        self,
        epoch: &Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = proto::RadioRewardShare> {
        let total_shares = self.total_shares();
        let total_rewards = get_scheduled_tokens(epoch.start, epoch.end - epoch.start).unwrap();
        let rewards_per_share = total_rewards / total_shares;
        self.shares
            .into_iter()
            .map(move |(owner_key, radio_shares)| {
                let owner_key = owner_key.clone();
                radio_shares
                    .into_iter()
                    .map(move |radio_share| proto::RadioRewardShare {
                        owner_key: owner_key.to_vec(),
                        hotspot_key: radio_share.hotspot_key.to_vec(),
                        cbsd_id: radio_share.cbsd_id,
                        amount: u64::from(Mobile::from(rewards_per_share * radio_share.amount)),
                        start_epoch: todo!(),
                        end_epoch: todo!(),
                    })
            })
            .flatten()
    }
}

// 100M genesis rewards per day
const GENESIS_REWARDS_PER_DAY: i64 = 100_000_000;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

/*
impl OwnerEmissions {
    pub fn new(owner_shares: OwnerShares, start: DateTime<Utc>, duration: Duration) -> Self {
        let mut owner_emissions = HashMap::new();
        let total_shares: Decimal = owner_shares.values().sum();
        if total_shares > dec!(0) {
            if let Some(actual_emissions) = get_scheduled_tokens(start, duration) {
                let emissions_per_share = actual_emissions / total_shares;
                for (owner, share) in owner_shares {
                    owner_emissions.insert(owner, Mobile::from(share * emissions_per_share));
                }
            }
        }
        OwnerEmissions(owner_emissions)
    }

    #[allow(dead_code)]
    pub fn total_emissions(&self) -> Mobile {
        Mobile::from(
            self.0
                .values()
                .fold(dec!(0), |acc, amt| acc + amt.into_inner()),
        )
    }

    pub fn into_inner(self) -> HashMap<PublicKey, Mobile> {
        self.0
    }
}
*/

pub fn get_scheduled_tokens(start: DateTime<Utc>, duration: Duration) -> Option<Decimal> {
    if *GENESIS_START <= start {
        // Get tokens from start - duration
        Some(
            (Decimal::from(GENESIS_REWARDS_PER_DAY)
                / Decimal::from(Duration::hours(24).num_seconds()))
                * Decimal::from(duration.num_seconds()),
        )
    } else {
        None
    }
}

#[async_trait::async_trait]
pub trait OwnerResolver: Send {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>>;

    async fn owner_shares(
        &mut self,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<OwnerShares> {
        let mut owner_shares = OwnerShares::default();
        for heartbeat in heartbeats.into_iter() {
            if let Some(owner) = self.resolve_owner(&heartbeat.hotspot_key).await? {
                let speedmultiplier = speedtests
                    .get_average(&heartbeat.hotspot_key)
                    .as_ref()
                    .map_or(dec!(0.0), Average::reward_multiplier);
                owner_shares
                    .shares
                    .entry(owner)
                    .or_default()
                    .push(RadioShare {
                        hotspot_key: heartbeat.hotspot_key,
                        cbsd_id: heartbeat.cbsd_id,
                        amount: heartbeat.reward_weight * speedmultiplier,
                    })
            }
        }
        Ok(owner_shares)
    }
    /*
        async fn owner_shares(
            &mut self,
            hotspot_shares: HotspotShares,
        ) -> Result<(OwnerShares, MissingOwnerShares)> {
            let mut owner_shares = OwnerShares::new();
            let mut missing_owner_shares = MissingOwnerShares::new();
            for (hotspot, share) in hotspot_shares {
                if let Some(owner) = self.resolve_owner(&hotspot).await? {
                    *owner_shares.entry(owner).or_default() += share;
                } else {
                    *missing_owner_shares.entry(hotspot).or_default() += share;
                }
            }
            Ok((owner_shares, missing_owner_shares))
    }
        */
}

#[async_trait::async_trait]
impl OwnerResolver for follower::Client<Channel> {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.find_gateway(req).await?.into_inner();

        if let Some(GatewayResult::Info(gateway_info)) = res.result {
            if let Ok(pub_key) = PublicKey::try_from(gateway_info.owner) {
                return Ok(Some(pub_key));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::cell_type::CellType;
    use async_trait::async_trait;
    use helium_crypto::PublicKey;
    use std::str::FromStr;

    use super::*;

    struct FixedOwnerResolver {
        owner: PublicKey,
    }

    #[async_trait]
    impl OwnerResolver for FixedOwnerResolver {
        async fn resolve_owner(&mut self, _address: &PublicKey) -> Result<Option<PublicKey>> {
            Ok(Some(self.owner.clone()))
        }
    }

    #[tokio::test]
    async fn test_rewards() {
        // SercommIndoor
        let g1 = PublicKey::from_str("11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL")
            .expect("unable to construct pubkey");
        // Nova430I
        let g2 = PublicKey::from_str("118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc")
            .expect("unable to construct pubkey");
        // SercommOutdoor
        let g3 = PublicKey::from_str("112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ")
            .expect("unable to construct pubkey");
        // Nova436H
        let g4 = PublicKey::from_str("11k712d9dSb8CAujzS4PdC7Hi8EEBZWsSnt4Zr1hgke4e1Efiag")
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c3 = "P27-SCO4255PA102206DPT000207".to_string();
        let c4 = "2AG32MBS3100196N1202000240215KY0184".to_string();

        let ct1 = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let ct2 = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");
        let ct3 = CellType::from_cbsd_id(&c3).expect("unable to get cell_type");
        let ct4 = CellType::from_cbsd_id(&c4).expect("unable to get cell_type");

        let mut shares = HotspotShares::new();
        shares.insert(g1, ct1.reward_weight());
        shares.insert(g2, ct2.reward_weight());
        shares.insert(g3, ct3.reward_weight());
        shares.insert(g4, ct4.reward_weight());

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let (owner_shares, _missing_owner_shares) = owner_resolver
            .owner_shares(shares)
            .await
            .expect("unable to get owner_shares");

        let start = Utc::now();
        let duration = chrono::Duration::hours(24);
        let owner_emissions = OwnerEmissions::new(owner_shares, start, duration);
        let total_owner_emissions = owner_emissions.total_emissions();

        // 100M in bones
        assert_eq!(10000000000000000, u64::from(total_owner_emissions));
    }
}
