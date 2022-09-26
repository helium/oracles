use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::BytesMutStream;
use futures::stream::StreamExt;
use helium_proto::{
    services::poc_mobile::CellHeartbeatReqV1, Message, SubnetworkReward as ProtoSubnetworkReward,
};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::{
    subnetwork_rewards::SubnetworkRewards, traits::OwnerResolver, CellType, Mobile, Result,
};
use helium_crypto::PublicKey;
use std::collections::HashMap;

/// Map from cbsd_id to share
pub type Shares = HashMap<String, Share>;

/// Map from cell_type to accumulated_reward_weight (decimal)
pub type CellShares = HashMap<CellType, Decimal>;

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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn total_emissions(&self) -> Mobile {
        Mobile::from(
            self.0
                .values()
                .fold(dec!(0), |acc, amt| acc + amt.get_decimal()),
        )
    }
}

impl From<OwnerEmissions> for SubnetworkRewards {
    fn from(owner_emissions: OwnerEmissions) -> SubnetworkRewards {
        let unsorted_rewards = owner_emissions
            .0
            .into_iter()
            .map(|(owner, amt)| ProtoSubnetworkReward {
                account: owner.to_vec(),
                amount: u64::from(amt),
            })
            .collect();
        SubnetworkRewards(unsorted_rewards)
    }
}

#[derive(Debug, Serialize)]
pub struct Share {
    pub timestamp: u64,
    pub pub_key: PublicKey,
    pub weight: Decimal,
    pub cell_type: CellType,
}

impl Share {
    pub fn new(timestamp: u64, pub_key: PublicKey, weight: Decimal, cell_type: CellType) -> Self {
        Self {
            timestamp,
            pub_key,
            weight,
            cell_type,
        }
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

pub fn cell_shares(shares: &Shares) -> CellShares {
    let mut cell_shares = CellShares::new();
    for share in shares.values() {
        *cell_shares.entry(share.cell_type).or_default() += share.weight;
    }
    cell_shares
}

pub fn hotspot_shares(shares: &Shares) -> HotspotShares {
    let mut hotspot_shares = HotspotShares::new();
    for share in shares.values() {
        *hotspot_shares.entry(share.pub_key.clone()).or_default() += share.weight;
    }
    hotspot_shares
}

pub async fn owner_shares<F>(
    owner_resolver: &mut F,
    hotspot_shares: HotspotShares,
) -> Result<(OwnerShares, MissingOwnerShares)>
where
    F: OwnerResolver,
{
    let mut owner_shares = OwnerShares::new();
    let mut missing_owner_shares = MissingOwnerShares::new();
    for (hotspot, share) in hotspot_shares {
        if let Some(owner) = owner_resolver.resolve_owner(&hotspot).await? {
            *owner_shares.entry(owner).or_default() += share;
        } else {
            *missing_owner_shares.entry(hotspot).or_default() += share;
        }
    }
    Ok((owner_shares, missing_owner_shares))
}

pub async fn gather_shares(
    stream: &mut BytesMutStream,
    after_utc: u64,
    before_utc: u64,
) -> Result<Shares> {
    let mut shares = Shares::new();

    while let Some(Ok(msg)) = stream.next().await {
        // NOTE: This will early exit with an error if we fail to decode
        let CellHeartbeatReqV1 {
            pub_key,
            cbsd_id,
            timestamp,
            operation_mode,
            ..
        } = CellHeartbeatReqV1::decode(msg)?;

        if !operation_mode || timestamp < after_utc || timestamp >= before_utc {
            continue;
        }

        if let Some(cell_type) = CellType::from_cbsd_id(&cbsd_id) {
            if let Ok(gw_pubkey) = PublicKey::try_from(pub_key.as_slice()) {
                let share = Share::new(timestamp, gw_pubkey, cell_type.reward_weight(), cell_type);

                if shares
                    .get(&cbsd_id)
                    .map_or(false, |found_share| found_share.timestamp > timestamp)
                {
                    continue;
                }
                shares.insert(cbsd_id, share);
            }
        }
    }

    Ok(shares)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Duration;

    #[test]
    fn emissions_per_second() {
        let scheduled_tokens = get_scheduled_tokens(Utc::now(), Duration::seconds(1))
            .expect("unable to get scheduled_tokens");
        assert_eq!(
            Mobile::from(dec!(1157.40740741)),
            Mobile::from(scheduled_tokens)
        );
    }
}
