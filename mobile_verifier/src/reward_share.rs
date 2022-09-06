use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::stream::StreamExt;
use helium_proto::{
    follower_client::FollowerClient,
    services::{
        poc_mobile::{CellHeartbeatReqV1, SpeedtestReqV1},
        Channel,
    },
    FollowerGatewayReqV1, Message, SubnetworkReward as ProtoSubnetworkReward,
};
use lazy_static::lazy_static;
use poc_store::BytesMutStream;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::{
    cell_type::CellType,
    mobile::Mobile,
    reward_speed_share::{
        InvalidSpeedShare, InvalidSpeedShares, SpeedShare, SpeedShareMovingAvgs, SpeedShares,
    },
    subnetwork_rewards::SubnetworkRewards,
    Result,
};
use helium_crypto::PublicKey;
use std::collections::HashMap;

/// Map from cbsd_id to share
pub type Shares = HashMap<String, Share>;

/// Map from cbsd_id to invalid share
pub type InvalidShares = Vec<InvalidShare>;

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
                .fold(dec!(0), |acc, amt| acc + amt.into_inner()),
        )
    }

    pub fn into_inner(self) -> HashMap<PublicKey, Mobile> {
        self.0
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

#[derive(Clone, Debug, Serialize)]
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

pub struct InvalidShare {
    pub cbsd_id: String,
    pub timestamp: u64,
    pub pub_key: Vec<u8>,
    pub weight: Decimal,
    pub cell_type: CellType,
    // TODO: Probably better to make this an enum or something.
    pub invalid_reason: &'static str,
}

pub fn cell_shares(shares: &Shares) -> CellShares {
    let mut cell_shares = CellShares::new();
    for share in shares.values() {
        *cell_shares.entry(share.cell_type).or_default() += share.weight;
    }
    cell_shares
}

pub fn hotspot_shares(
    shares: &Shares,
    speed_shares_moving_avg: &SpeedShareMovingAvgs,
) -> HotspotShares {
    let mut hotspot_shares = HotspotShares::new();
    for share in shares.values() {
        speed_shares_moving_avg.get(&share.pub_key).map_or_else(
            || (),
            |moving_avg| {
                if moving_avg.is_valid {
                    *hotspot_shares.entry(share.pub_key.clone()).or_default() += share.weight;
                }
            },
        )
    }
    hotspot_shares
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
impl OwnerResolver for FollowerClient<Channel> {
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

pub struct GatheredShares {
    pub shares: Shares,
    pub invalid_shares: InvalidShares,
    pub speed_shares: SpeedShares,
    pub speed_shares_moving_avg: SpeedShareMovingAvgs,
    pub invalid_speed_shares: InvalidSpeedShares,
}

impl GatheredShares {
    pub async fn from_stream(
        stream: &mut BytesMutStream,
        after_utc: u64,
        before_utc: u64,
    ) -> Result<Self> {
        let mut shares = Self {
            shares: Shares::new(),
            invalid_shares: InvalidShares::new(),
            speed_shares: SpeedShares::new(),
            speed_shares_moving_avg: SpeedShareMovingAvgs::default(),
            invalid_speed_shares: InvalidSpeedShares::new(),
        };

        while let Some(Ok(msg)) = stream.next().await {
            // NOTE: This will early exit with an error if we fail to decode

            if let Ok(cell_heartbeat_req_v1) = CellHeartbeatReqV1::decode(msg.clone()) {
                shares.gather_heartbeat(cell_heartbeat_req_v1, after_utc, before_utc)
            } else if let Ok(speedtest_req_v1) = SpeedtestReqV1::decode(msg) {
                shares.gather_speedtest(speedtest_req_v1, after_utc, before_utc)
            } else {
                continue;
            }
        }
        Ok(shares)
    }

    fn gather_speedtest(
        &mut self,
        speedtest_req_v1: SpeedtestReqV1,
        after_utc: u64,
        before_utc: u64,
    ) {
        let SpeedtestReqV1 {
            pub_key: st_pub_key,
            timestamp: st_timestamp,
            upload_speed: st_upload_speed,
            download_speed: st_download_speed,
            latency: st_latency,
            ..
        } = speedtest_req_v1;

        if st_timestamp < after_utc || st_timestamp >= before_utc {
            self.invalid_speed_shares.push(InvalidSpeedShare {
                pub_key: st_pub_key,
                timestamp: st_timestamp,
                upload_speed: st_upload_speed,
                download_speed: st_download_speed,
                latency: st_latency,
                invalid_reason: "outside of time range",
            });
            return;
        }

        if let Ok(gw_public_key) = PublicKey::try_from(st_pub_key.as_ref()) {
            let share = SpeedShare::new(
                gw_public_key.clone(),
                st_timestamp,
                st_upload_speed,
                st_download_speed,
                st_latency,
            );

            self.speed_shares
                .entry(gw_public_key)
                .or_default()
                .push(share);
            self.speed_shares_moving_avg.update(&self.speed_shares)
        }
    }

    fn gather_heartbeat(
        &mut self,
        cell_heartbeat_req_v1: CellHeartbeatReqV1,
        after_utc: u64,
        before_utc: u64,
    ) {
        let CellHeartbeatReqV1 {
            pub_key: hb_pub_key,
            cbsd_id: hb_cbsd_id,
            timestamp: hb_timestamp,
            ..
        } = cell_heartbeat_req_v1;

        if let Some(cell_type) = CellType::from_cbsd_id(&hb_cbsd_id) {
            // TODO: Will only get inserted in invalid if cbsd_id is a valid cell_type
            if hb_timestamp < after_utc || hb_timestamp >= before_utc {
                self.invalid_shares.push(InvalidShare {
                    cbsd_id: hb_cbsd_id,
                    pub_key: hb_pub_key,
                    timestamp: hb_timestamp,
                    weight: cell_type.reward_weight(),
                    cell_type,
                    invalid_reason: "outside of time range",
                });
                return;
            }

            if let Ok(gw_pubkey) = PublicKey::try_from(hb_pub_key.as_ref()) {
                let share = Share::new(
                    hb_timestamp,
                    gw_pubkey,
                    cell_type.reward_weight(),
                    cell_type,
                );

                if self
                    .shares
                    .get(&hb_cbsd_id)
                    .map_or(false, |found_share| found_share.timestamp > hb_timestamp)
                {
                    return;
                }
                self.shares.insert(hb_cbsd_id, share);
            }
        }
    }
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
