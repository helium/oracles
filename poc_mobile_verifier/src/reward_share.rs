use chrono::{DateTime, Duration, TimeZone, Utc};
use helium_proto::services::{
    follower::{self, FollowerGatewayReqV1},
    Channel,
};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::{mobile::Mobile, Result};
use helium_crypto::PublicKey;
use std::collections::HashMap;

/// Map from gw_public_key to accumulated_reward_weight (decimal)
pub type HotspotShares = HashMap<PublicKey, Decimal>;

/// Map from owner_public_key to accumulated_reward_weight (decimal)
pub type OwnerShares = HashMap<PublicKey, Decimal>;

/// Map from gw_public_key (without owners) to accumulated_reward_weight (decimal)
pub type MissingOwnerShares = HashMap<PublicKey, Decimal>;

/// Map from owner_public_key to accumulated_rewards (mobile)
#[derive(Debug, Clone, Serialize)]
pub struct OwnerEmissions(HashMap<PublicKey, Mobile>);

// 100M genesis rewards per day
const GENESIS_REWARDS_PER_DAY: i64 = 100_000_000;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

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
pub trait OwnerResolver {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>>;

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
}

#[async_trait::async_trait]
impl OwnerResolver for follower::Client<Channel> {
    async fn resolve_owner(&mut self, address: &PublicKey) -> Result<Option<PublicKey>> {
        let req = FollowerGatewayReqV1 {
            address: address.to_vec(),
        };
        let res = self.find_gateway(req).await?.into_inner();

        if let Ok(pub_key) = PublicKey::try_from(res.owner) {
            return Ok(Some(pub_key));
        }

        Ok(None)
    }
}
