use crate::{
    heartbeats::Heartbeats,
    speedtests::{Average, SpeedtestAverages},
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::traits::TimestampEncode;
use helium_crypto::PublicKey;
use helium_proto::services::{
    follower::{self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1},
    poc_mobile as proto, Channel,
};
use lazy_static::lazy_static;
use rust_decimal::prelude::*;
use std::collections::HashMap;
use std::ops::Range;

pub struct RadioShare {
    hotspot_key: PublicKey,
    cbsd_id: String,
    amount: Decimal,
}

#[derive(Default)]
pub struct RadioShares {
    shares: Vec<RadioShare>,
}

impl RadioShares {
    pub fn push(&mut self, share: RadioShare) {
        self.shares.push(share);
    }

    pub fn total_shares(&self) -> Decimal {
        self.shares
            .iter()
            .fold(Decimal::ZERO, |sum, radio_share| sum + radio_share.amount)
    }

    pub fn into_iter(self) -> impl Iterator<Item = RadioShare> {
        self.shares.into_iter()
    }
}

#[derive(Default)]
pub struct OwnerShares {
    pub shares: HashMap<PublicKey, RadioShares>,
}

const REWARDS_PER_SHARE_PREC: u32 = 9;
const MOBILE_SCALE: u32 = 100_000_000;

impl OwnerShares {
    pub async fn aggregate(
        resolver: &mut impl OwnerResolver,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<Self, ResolveError> {
        let mut owner_shares = Self::default();
        for heartbeat in heartbeats.into_iter() {
            if let Some(owner) = resolver.resolve_owner(&heartbeat.hotspot_key).await? {
                let speedmultiplier = speedtests
                    .get_average(&heartbeat.hotspot_key)
                    .as_ref()
                    .map_or(Decimal::ZERO, Average::reward_multiplier);
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

    pub fn total_shares(&self) -> Decimal {
        self.shares
            .iter()
            .fold(Decimal::ZERO, |sum, (_, radio_shares)| {
                sum + radio_shares.total_shares()
            })
    }

    pub fn into_radio_shares(
        self,
        epoch: &'_ Range<DateTime<Utc>>,
    ) -> Result<impl Iterator<Item = proto::RadioRewardShare> + '_, ClockError> {
        let total_shares = self.total_shares();
        let total_rewards = get_scheduled_tokens(epoch.start, epoch.end - epoch.start)?;
        let rewards_per_share = (total_rewards / total_shares)
            .round_dp_with_strategy(REWARDS_PER_SHARE_PREC, RoundingStrategy::ToPositiveInfinity);
        Ok(self
            .shares
            .into_iter()
            .flat_map(move |(owner_key, radio_shares)| {
                radio_shares
                    .into_iter()
                    .map(move |radio_share| proto::RadioRewardShare {
                        owner_key: owner_key.to_vec(),
                        hotspot_key: radio_share.hotspot_key.to_vec(),
                        cbsd_id: radio_share.cbsd_id,
                        amount: {
                            let rewards = rewards_per_share * radio_share.amount;
                            let rewards = (rewards * Decimal::from(MOBILE_SCALE))
                                .round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero);
                            rewards.to_u64().unwrap_or(0)
                        },
                        start_epoch: epoch.start.encode_timestamp(),
                        end_epoch: epoch.end.encode_timestamp(),
                    })
            }))
    }
}

// 100M genesis rewards per day
const GENESIS_REWARDS_PER_DAY: i64 = 100_000_000;

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> = Utc.ymd(2022, 7, 11).and_hms(0, 0, 0);
}

#[derive(thiserror::Error, Debug)]
#[error("clock is set before the genesis start")]
pub struct ClockError;

pub fn get_scheduled_tokens(
    start: DateTime<Utc>,
    duration: Duration,
) -> Result<Decimal, ClockError> {
    (*GENESIS_START <= start)
        .then(|| {
            (Decimal::from(GENESIS_REWARDS_PER_DAY)
                / Decimal::from(Duration::hours(24).num_seconds()))
                * Decimal::from(duration.num_seconds())
        })
        .ok_or(ClockError)
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct ResolveError(#[from] tonic::Status);

#[async_trait::async_trait]
pub trait OwnerResolver: Send {
    async fn resolve_owner(
        &mut self,
        address: &PublicKey,
    ) -> Result<Option<PublicKey>, ResolveError>;
}

#[async_trait::async_trait]
impl OwnerResolver for follower::Client<Channel> {
    async fn resolve_owner(
        &mut self,
        address: &PublicKey,
    ) -> Result<Option<PublicKey>, ResolveError> {
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
    use super::*;
    use crate::{
        cell_type::CellType,
        heartbeats::{Heartbeat, Heartbeats},
        speedtests::{Speedtest, SpeedtestAverages},
    };
    use chrono::{Duration, NaiveDateTime, Utc};
    use helium_proto::services::poc_mobile::HeartbeatValidity;
    use std::collections::{HashMap, VecDeque};

    struct MapResolver {
        owners: HashMap<PublicKey, PublicKey>,
    }

    #[async_trait::async_trait]
    impl OwnerResolver for MapResolver {
        async fn resolve_owner(
            &mut self,
            address: &PublicKey,
        ) -> Result<Option<PublicKey>, ResolveError> {
            Ok(self.owners.get(address).cloned())
        }
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
    }

    fn cell_type_weight(cbsd_id: &String) -> Decimal {
        CellType::from_cbsd_id(cbsd_id)
            .expect("unable to get cell_type")
            .reward_weight()
    }

    fn acceptable_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(10),
            download_speed: bytes_per_s(100),
            latency: 25,
        }
    }

    fn degraded_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(5),
            download_speed: bytes_per_s(60),
            latency: 60,
        }
    }

    fn failed_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(1),
            download_speed: bytes_per_s(20),
            latency: 110,
        }
    }

    fn poor_speedtest(timestamp: NaiveDateTime) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(2),
            download_speed: bytes_per_s(40),
            latency: 90,
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

        let owner1: PublicKey = "1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r"
            .parse()
            .expect("unable to get test pubkey");
        let owner2: PublicKey = "1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd"
            .parse()
            .expect("unable to get pubkey");

        let mut owners = HashMap::new();
        owners.insert(g1.clone(), owner1.clone());
        owners.insert(g2.clone(), owner2.clone());

        let mut resolver = MapResolver { owners };

        let now = Utc::now();
        let timestamp = now.naive_utc();

        let heartbeats = vec![
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c2.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c2),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
        ];

        let heartbeats: Heartbeats = heartbeats.into_iter().collect();

        let last_timestamp = timestamp - Duration::hours(12);
        let g1_speedtests = vec![
            acceptable_speedtest(last_timestamp),
            acceptable_speedtest(timestamp),
        ];
        let g2_speedtests = vec![
            acceptable_speedtest(last_timestamp),
            acceptable_speedtest(timestamp),
        ];
        let mut speedtests = HashMap::new();
        speedtests.insert(g1, VecDeque::from(g1_speedtests));
        speedtests.insert(g2, VecDeque::from(g2_speedtests));
        let speedtest_avgs = SpeedtestAverages { speedtests };

        let owner_rewards = OwnerShares::aggregate(&mut resolver, heartbeats, speedtest_avgs)
            .await
            .expect("Could not generate rewards");

        // The owner with two hotspots gets more rewards
        assert!(
            owner_rewards
                .shares
                .get(&owner1)
                .expect("Could not fetch owner1 shares")
                .total_shares()
                > owner_rewards
                    .shares
                    .get(&owner2)
                    .expect("Could not fetch owner2 shares")
                    .total_shares()
        );
    }

    #[tokio::test]
    async fn reward_shares_with_speed_multiplier() {
        // init hotspots
        let owner1: PublicKey = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKey = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        let owner3: PublicKey = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed owner3 parse");
        let owner4: PublicKey = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed owner4 parse");

        // init hotspots
        let gw1: PublicKey = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKey = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw3: PublicKey = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw4: PublicKey = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");
        let gw5: PublicKey = "112j1iw1sV2B2Tz2DxPSeum9Cmc5kMKNdDTDg1zDRsdwuvZueq3B"
            .parse()
            .expect("failed gw5 parse");
        let gw6: PublicKey = "11fCasUk9XvU15ktsMMH64J9E7XuqQ2L5FJPv8HZMCDG6kdZ3SC"
            .parse()
            .expect("failed gw6 parse");
        let gw7: PublicKey = "11HdwRpQDrYM7LJtRGSzRF3vY2iwuumx1Z2MUhBYAVTwZdSh6Bi"
            .parse()
            .expect("failed gw7 parse");
        let gw8: PublicKey = "112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ"
            .parse()
            .expect("failed gw8 parse");

        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner1.clone());
        owners.insert(gw3.clone(), owner1.clone());
        owners.insert(gw4.clone(), owner2.clone());
        owners.insert(gw5.clone(), owner2.clone());
        owners.insert(gw6.clone(), owner3.clone());
        owners.insert(gw7.clone(), owner3.clone());
        owners.insert(gw8.clone(), owner4.clone());
        let mut resolver = MapResolver { owners };

        // init cells and cell_types
        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c3 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c4 = "2AG32PBS3101S1202000464223GY0154".to_string();
        let c5 = "P27-SCE4255W2107CW5000016".to_string();
        let c6 = "2AG32PBS3101S1202000464223GY0155".to_string();
        let c7 = "2AG32PBS3101S1202000464223GY0156".to_string();
        let c8 = "P27-SCE4255W2107CW5000017".to_string();
        let c9 = "P27-SCE4255W2107CW5000018".to_string();
        let c10 = "P27-SCE4255W2107CW5000019".to_string();
        let c11 = "P27-SCE4255W2107CW5000020".to_string();
        let c12 = "P27-SCE4255W2107CW5000021".to_string();
        let c13 = "P27-SCE4255W2107CW5000022".to_string();
        let c14 = "2AG32PBS3101S1202000464223GY0157".to_string();

        let now = Utc::now();
        let timestamp = (now - Duration::minutes(20)).naive_utc();

        // setup heartbeats
        let heartbeats = vec![
            Heartbeat {
                cbsd_id: c1.clone(),
                hotspot_key: gw1.clone(),
                reward_weight: cell_type_weight(&c1),
                validity: HeartbeatValidity::NotOperational,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c2.clone(),
                hotspot_key: gw2.clone(),
                reward_weight: cell_type_weight(&c2),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c3.clone(),
                hotspot_key: gw2.clone(),
                reward_weight: cell_type_weight(&c3),
                validity: HeartbeatValidity::NotOperational,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c4.clone(),
                hotspot_key: gw3.clone(),
                reward_weight: cell_type_weight(&c4),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c5.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c5),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c6.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c6),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c7.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c7),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c8.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c8),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c9.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c9),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c10.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c10),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c11.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c11),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c12.clone(),
                hotspot_key: gw5.clone(),
                reward_weight: cell_type_weight(&c12),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c13.clone(),
                hotspot_key: gw6.clone(),
                reward_weight: cell_type_weight(&c13),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
            Heartbeat {
                cbsd_id: c14.clone(),
                hotspot_key: gw7.clone(),
                reward_weight: cell_type_weight(&c14),
                validity: HeartbeatValidity::Valid,
                timestamp,
            },
        ];
        let heartbeats: Heartbeats = heartbeats.into_iter().collect();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw3_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw4_speedtests = vec![
            acceptable_speedtest(last_speedtest),
            acceptable_speedtest(timestamp),
        ];
        let gw5_speedtests = vec![
            degraded_speedtest(last_speedtest),
            degraded_speedtest(timestamp),
        ];
        let gw6_speedtests = vec![
            failed_speedtest(last_speedtest),
            failed_speedtest(timestamp),
        ];
        let gw7_speedtests = vec![poor_speedtest(last_speedtest), poor_speedtest(timestamp)];
        let mut speedtests = HashMap::new();
        speedtests.insert(gw1, VecDeque::from(gw1_speedtests));
        speedtests.insert(gw2, VecDeque::from(gw2_speedtests));
        speedtests.insert(gw3, VecDeque::from(gw3_speedtests));
        speedtests.insert(gw4, VecDeque::from(gw4_speedtests));
        speedtests.insert(gw5, VecDeque::from(gw5_speedtests));
        speedtests.insert(gw6, VecDeque::from(gw6_speedtests));
        speedtests.insert(gw7, VecDeque::from(gw7_speedtests));
        let speedtest_avgs = SpeedtestAverages { speedtests };

        // calculate the rewards for the sample group
        let mut owner_rewards = HashMap::<PublicKey, u64>::new();
        for radio_share in OwnerShares::aggregate(&mut resolver, heartbeats, speedtest_avgs)
            .await
            .expect("Could not generate rewards")
            .into_radio_shares(&((now - Duration::hours(1))..now))
            .expect("Clock is out of sync")
        {
            *owner_rewards
                .entry(PublicKey::try_from(radio_share.owner_key).expect("Invalid public key"))
                .or_default() += radio_share.amount;
        }

        assert_eq!(
            *owner_rewards
                .get(&owner1)
                .expect("Could not fetch owner1 rewards"),
            99_715_099_715_100
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            299_145_299_145_301
        );
        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            17_806_267_806_268
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let mut total = 0;
        for val in owner_rewards.values() {
            total += *val
        }

        assert_eq!(total, 416_666_666_666_669); // total emissions for 1 hour
    }
}
