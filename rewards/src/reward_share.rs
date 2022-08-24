use chrono::{DateTime, Duration, Utc};
use futures::stream::StreamExt;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use poc_store::BytesMutStream;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::{
    emissions::get_scheduled_tokens, traits::OwnerResolver, CellType, Mobile, PublicKey, Result,
};
use std::collections::HashMap;

// key: cbsd_id, val: share
pub type Shares = HashMap<String, Share>;

// key: cell_type, val: total_accumulated_shares_for_this_cell_type
pub type CellShares = HashMap<CellType, Decimal>;

// key: gw_pubkey, val: total_accumulated_shares_for_this_hotspot
pub type HotspotShares = HashMap<PublicKey, Decimal>;

// key: owner_pubkey, val: total_accumulated_shares_for_this_owner
pub type OwnerShares = HashMap<PublicKey, Decimal>;

// key: owner_pubkey, val: owner_reward
pub type OwnerEmissions = HashMap<PublicKey, Mobile>;

// key: gw_pubkey, val: total_accumulated_shares_for_this_hotspot
pub type MissingOwnerShares = HashMap<PublicKey, Decimal>;

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

pub fn cell_shares(shares: &Shares) -> CellShares {
    let mut cell_shares = CellShares::new();
    for share in shares.values() {
        *cell_shares.entry(share.cell_type).or_insert(dec!(0)) += share.weight;
    }
    cell_shares
}

pub fn hotspot_shares(shares: &Shares) -> HotspotShares {
    let mut hotspot_shares = HotspotShares::new();
    for share in shares.values() {
        *hotspot_shares
            .entry(share.pub_key.clone())
            .or_insert(dec!(0)) += share.weight;
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
            *owner_shares.entry(owner).or_insert(dec!(0)) += share;
        } else {
            *missing_owner_shares.entry(hotspot).or_insert(dec!(0)) += share;
        }
    }
    Ok((owner_shares, missing_owner_shares))
}

pub fn owner_emissions(
    owner_shares: OwnerShares,
    start: DateTime<Utc>,
    duration: Duration,
) -> OwnerEmissions {
    let mut owner_emissions = OwnerEmissions::new();
    let total_shares: Decimal = owner_shares.values().sum();
    if let Some(actual_emissions) = get_scheduled_tokens(start, duration) {
        let emissions_per_share = actual_emissions / total_shares;
        for (owner, share) in owner_shares {
            owner_emissions.insert(owner, Mobile::from(share * emissions_per_share));
        }
    }
    owner_emissions
}

pub async fn gather_shares(
    stream: &mut BytesMutStream,
    after_utc: u64,
    before_utc: u64,
) -> Result<Shares> {
    let mut shares = Shares::new();

    while let Some(Ok(msg)) = stream.next().await {
        let CellHeartbeatReqV1 {
            pub_key,
            cbsd_id,
            timestamp,
            ..
        } = CellHeartbeatReqV1::decode(msg)?;

        if timestamp < after_utc || timestamp >= before_utc {
            continue;
        }

        if let Some(cell_type) = CellType::from_cbsd_id(&cbsd_id) {
            if let Ok(gw_pubkey) = PublicKey::try_from(pub_key) {
                let share = Share::new(timestamp, gw_pubkey, cell_type.reward_weight(), cell_type);
                shares.insert(cbsd_id, share);
            }
        }
    }

    Ok(shares)
}
