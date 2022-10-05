use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::{
    heartbeat::{CellHeartbeat, CellHeartbeatIngestReport},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    traits::MsgDecode,
    BytesMutStream,
};
use futures::stream::StreamExt;
use helium_proto::services::{
    follower::{self, FollowerGatewayReqV1},
    poc_mobile::{Share as ShareProto, ShareValidity},
    Channel,
};
use lazy_static::lazy_static;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;

use crate::{
    cell_type::CellType,
    mobile::Mobile,
    reward_speed_share::{InvalidSpeedShares, SpeedShare, SpeedShareMovingAvgs, SpeedShares},
    Result,
};
use helium_crypto::PublicKey;
use std::collections::HashMap;

/// Map from cbsd_id to share
pub type Shares = HashMap<String, Share>;

/// Map from cbsd_id to invalid share
pub type InvalidShares = Vec<ShareProto>;

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

#[derive(Clone, Debug, Serialize)]
pub struct Share {
    pub timestamp: DateTime<Utc>,
    pub pub_key: PublicKey,
    pub weight: Decimal,
    pub cell_type: CellType,
    pub validity: ShareValidity,
}

impl Share {
    pub fn new(
        timestamp: DateTime<Utc>,
        pub_key: PublicKey,
        weight: Decimal,
        cell_type: CellType,
        validity: ShareValidity,
    ) -> Self {
        Self {
            timestamp,
            pub_key,
            weight,
            cell_type,
            validity,
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

#[derive(Debug, Clone, Serialize)]
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
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
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

            if let Ok(cell_heartbeat_ingest_report) = CellHeartbeatIngestReport::decode(msg.clone())
            {
                shares.gather_heartbeat(cell_heartbeat_ingest_report.report, after_utc, before_utc)
            } else if let Ok(speedtest_ingest_report) = CellSpeedtestIngestReport::decode(msg) {
                shares.gather_speedtest(speedtest_ingest_report.report, after_utc, before_utc)
            } else {
                continue;
            }
        }
        Ok(shares)
    }

    fn gather_speedtest(
        &mut self,
        speedtest: CellSpeedtest,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) {
        // TODO: Better error handling for bad public keys
        let mut speed_share = match SpeedShare::try_from(speedtest) {
            Ok(share) => share,
            Err(_) => return,
        };

        if speed_share.timestamp < after_utc || speed_share.timestamp >= before_utc {
            speed_share.validity = ShareValidity::HeartbeatOutsideRange;
            self.invalid_speed_shares.push(speed_share);
        } else {
            self.speed_shares
                .entry(speed_share.pub_key.clone())
                .or_default()
                .push(speed_share);
            self.speed_shares_moving_avg.update(&self.speed_shares)
        }
    }

    fn gather_heartbeat(
        &mut self,
        cell_heartbeat: CellHeartbeat,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) {
        let CellHeartbeat {
            pubkey: hb_pub_key,
            cbsd_id: hb_cbsd_id,
            timestamp: hb_timestamp,
            ..
        } = cell_heartbeat;

        if let Some(cell_type) = CellType::from_cbsd_id(&hb_cbsd_id) {
            // TODO: Will only get inserted in invalid if cbsd_id is a valid cell_type
            if hb_timestamp < after_utc || hb_timestamp >= before_utc {
                self.invalid_shares.push(ShareProto {
                    cbsd_id: hb_cbsd_id,
                    timestamp: hb_timestamp.timestamp() as u64,
                    pub_key: hb_pub_key.to_vec(),
                    weight: crate::bones_to_u64(cell_type.reward_weight()),
                    cell_type: cell_type as i32,
                    validity: ShareValidity::HeartbeatOutsideRange as i32,
                });
                return;
            }

            let share = Share::new(
                hb_timestamp,
                hb_pub_key,
                cell_type.reward_weight(),
                cell_type,
                ShareValidity::Valid,
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
