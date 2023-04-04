use crate::{
    heartbeats::Heartbeats,
    speedtests::{Average, SpeedtestAverages},
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::{mobile_transfer::ValidDataTransferSession, traits::TimestampEncode};
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    follower::{self, follower_gateway_resp_v1::Result as GatewayResult, FollowerGatewayReqV1},
    poc_mobile as proto, Channel,
};
use lazy_static::lazy_static;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::ops::Range;

pub struct TransferRewards {
    scale: Decimal,
    rewards: HashMap<PublicKeyBinary, Decimal>,
    pub remaining_rewards: Decimal,
}

impl TransferRewards {
    pub fn empty(epoch: &Range<DateTime<Utc>>) -> Self {
        Self {
            scale: Decimal::ONE,
            rewards: HashMap::new(),
            remaining_rewards: get_scheduled_tokens(epoch.start, epoch.end - epoch.start).unwrap(),
        }
    }

    pub fn scale(&self) -> Decimal {
        self.scale
    }

    pub fn reward(&self, hotspot: &PublicKeyBinary) -> Decimal {
        self.rewards.get(hotspot).copied().unwrap_or(Decimal::ZERO) * self.scale
    }

    pub async fn from_transfer_sessions(
        mobile_price: Decimal,
        transfer_sessions: impl Stream<Item = ValidDataTransferSession>,
        epoch: &Range<DateTime<Utc>>,
    ) -> Result<Self, ClockError> {
        tokio::pin!(transfer_sessions);

        let mut data_transfer_reward_sum = Decimal::ZERO;
        let rewards = transfer_sessions
            // Accumulate bytes per hotspot
            .fold(
                HashMap::<PublicKeyBinary, Decimal>::new(),
                |mut entries, session| async move {
                    *entries.entry(session.pub_key).or_default() +=
                        Decimal::from(bytes_to_dc(session.download_bytes + session.upload_bytes));
                    entries
                },
            )
            .await
            .into_iter()
            // Calculate rewards per hotspot
            .map(|(pub_key, dc_amount)| {
                let mobiles = dc_to_mobile_bones(dc_amount, mobile_price);
                data_transfer_reward_sum += mobiles;
                (pub_key, mobiles)
            })
            .collect();

        let total_rewards = get_scheduled_tokens(epoch.start, epoch.end - epoch.start)?;

        // Determine if we need to scale the rewards given for data transfer rewards.
        // Ideally this should never happen, but if the total number of data transfer rewards
        // is greater than (at the time of writing) 80% of the total pool, we need to scale
        // the rewards given for data transfer.
        //
        // If we find that total data_transfer reward sum is greater than 80%, we use the
        // following math to calculate the scale:
        //
        // [ scale * data_transfer_reward_sum ] / total_rewards = 0.8
        //
        //   therefore:
        //
        // scale = [ 0.8 * total_rewards ] / data_transfer_reward_sum
        //
        let (scale, remaining_rewards) =
            if data_transfer_reward_sum / total_rewards > *MAX_DATA_TRANSFER_REWARDS_PERCENT {
                (
                    *MAX_DATA_TRANSFER_REWARDS_PERCENT * total_rewards / data_transfer_reward_sum,
                    total_rewards * (dec!(1.0) - *MAX_DATA_TRANSFER_REWARDS_PERCENT),
                )
            } else {
                (Decimal::ONE, total_rewards - data_transfer_reward_sum)
            };

        Ok(Self {
            scale,
            rewards,
            remaining_rewards,
        })
    }
}

const BYTES_PER_DC: u64 = 66;

fn bytes_to_dc(bytes: u64) -> u64 {
    let bytes = bytes.max(BYTES_PER_DC);
    (bytes + BYTES_PER_DC - 1) / BYTES_PER_DC
}

lazy_static! {
    static ref MAX_DATA_TRANSFER_REWARDS_PERCENT: Decimal = dec!(0.8);
    static ref DC_USD_PRICE: Decimal = dec!(0.00000003);
}

const DEFAULT_PREC: u32 = 15;

/// Returns the equivalent amount of Mobile bones for a specified amount of Data Credits
pub fn dc_to_mobile_bones(dc_amount: Decimal, mobile_price: Decimal) -> Decimal {
    let dc_in_usd = dc_amount * *DC_USD_PRICE;
    (dc_in_usd / mobile_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

pub struct RadioShare {
    hotspot_key: PublicKeyBinary,
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
pub struct RewardShares {
    pub shares: HashMap<PublicKeyBinary, RadioShares>,
}

impl RewardShares {
    pub async fn aggregate(
        resolver: &mut impl OwnerResolver,
        heartbeats: Heartbeats,
        speedtests: SpeedtestAverages,
    ) -> Result<Self, ResolveError> {
        let mut owner_shares = Self::default();
        for heartbeat in heartbeats.into_rewardables() {
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

    pub fn into_radio_shares<'a>(
        self,
        transfer_rewards: &'a TransferRewards,
        epoch: &'a Range<DateTime<Utc>>,
    ) -> Result<impl Iterator<Item = proto::RadioRewardShare> + 'a, ClockError> {
        let total_shares = self.total_shares();
        let total_rewards = transfer_rewards.remaining_rewards;
        let rewards_per_share = total_rewards / total_shares;
        Ok(self
            .shares
            .into_iter()
            .flat_map(move |(owner_key, radio_shares)| {
                radio_shares
                    .into_iter()
                    .map(move |radio_share| proto::RadioRewardShare {
                        owner_key: owner_key.clone().into(),
                        cbsd_id: radio_share.cbsd_id,
                        amount: {
                            let rewards = rewards_per_share * radio_share.amount
                                + transfer_rewards.reward(&radio_share.hotspot_key);
                            let rewards =
                                rewards.round_dp_with_strategy(0, RoundingStrategy::ToZero);
                            rewards.to_u64().unwrap_or(0)
                        },
                        start_epoch: epoch.start.encode_timestamp(),
                        end_epoch: epoch.end.encode_timestamp(),
                        hotspot_key: radio_share.hotspot_key.into(),
                    })
                    .filter(|radio_share| radio_share.amount > 0)
            }))
    }
}

lazy_static! {
    static ref GENESIS_START: DateTime<Utc> =
        Utc.with_ymd_and_hms(2022, 7, 11, 0, 0, 0).single().unwrap();

    // 100M genesis rewards per day
    static ref GENESIS_REWARDS_PER_DAY: Decimal = dec!(100_000_000) * dec!(1_000_000);
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
            (*GENESIS_REWARDS_PER_DAY / Decimal::from(Duration::hours(24).num_seconds()))
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
        address: &PublicKeyBinary,
    ) -> Result<Option<PublicKeyBinary>, ResolveError>;
}

#[async_trait::async_trait]
impl OwnerResolver for follower::Client<Channel> {
    async fn resolve_owner(
        &mut self,
        address: &PublicKeyBinary,
    ) -> Result<Option<PublicKeyBinary>, ResolveError> {
        let req = FollowerGatewayReqV1 {
            address: address.clone().into(),
        };
        let res = self.find_gateway(req).await?.into_inner();

        if let Some(GatewayResult::Info(gateway_info)) = res.result {
            if let Ok(pub_key) = PublicKeyBinary::try_from(gateway_info.owner) {
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
    use futures::stream;
    use helium_proto::services::poc_mobile::HeartbeatValidity;
    use std::collections::{HashMap, VecDeque};

    #[tokio::test]
    async fn transfer_reward_amount() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let payer: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed payer parse");

        let data_transfer_sessions = stream::iter(vec![ValidDataTransferSession {
            pub_key: owner.clone(),
            payer,
            upload_bytes: 66,
            download_bytes: 1,
            num_dcs: 0, // Not used
            first_timestamp: DateTime::default(),
            last_timestamp: DateTime::default(),
        }]);

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;
        let total_rewards = get_scheduled_tokens(epoch.start, epoch.end - epoch.start).unwrap();
        println!("total rewards for epoch: {total_rewards}");
        // confirm our hourly rewards add up to expected 24hr amount
        // total_rewards will be in bones
        assert_eq!(
            total_rewards / dec!(1_000_000) * dec!(24),
            dec!(100_000_000)
        );

        let data_transfer_rewards =
            TransferRewards::from_transfer_sessions(dec!(1.0), data_transfer_sessions, &epoch)
                .await
                .expect("Could not fetch data transfer sessions");

        assert_eq!(data_transfer_rewards.reward(&owner), dec!(0.00000006));
        assert_eq!(data_transfer_rewards.scale, dec!(1.0));
        assert_eq!(
            data_transfer_rewards.remaining_rewards,
            total_rewards - (data_transfer_rewards.reward(&owner) * data_transfer_rewards.scale)
        );
    }

    #[tokio::test]
    async fn transfer_reward_scale() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let payer: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed payer parse");

        // Just an absurdly large amount of DC
        let mut transfer_sessions = Vec::new();
        for _ in 0..1_000_000 {
            transfer_sessions.push(ValidDataTransferSession {
                pub_key: owner.clone(),
                payer: payer.clone(),
                upload_bytes: 66 * 80_000_000 * 95_000_000,
                download_bytes: 0,
                num_dcs: 0, // Not used
                first_timestamp: DateTime::default(),
                last_timestamp: DateTime::default(),
            });
        }

        let data_transfer_sessions = stream::iter(transfer_sessions);

        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;

        let data_transfer_rewards =
            TransferRewards::from_transfer_sessions(dec!(1.0), data_transfer_sessions, &epoch)
                .await
                .expect("Could not fetch data transfer sessions");

        assert_eq!(
            data_transfer_rewards.remaining_rewards,
            dec!(20_000_000) * dec!(1_000_000)
        );
        assert_eq!(
            // Rewards are automatically scaled
            data_transfer_rewards.reward(&owner),
            dec!(80_000_000) * dec!(1_000_000)
        );
    }

    struct MapResolver {
        owners: HashMap<PublicKeyBinary, PublicKeyBinary>,
    }

    #[async_trait::async_trait]
    impl OwnerResolver for MapResolver {
        async fn resolve_owner(
            &mut self,
            address: &PublicKeyBinary,
        ) -> Result<Option<PublicKeyBinary>, ResolveError> {
            Ok(self.owners.get(address).cloned())
        }
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
    }

    fn cell_type_weight(cbsd_id: &str) -> Decimal {
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
        let g1: PublicKeyBinary = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
            .parse()
            .expect("unable to construct pubkey");
        let g2: PublicKeyBinary = "118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc"
            .parse()
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let owner1: PublicKeyBinary = "1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r"
            .parse()
            .expect("unable to get test pubkey");
        let owner2: PublicKeyBinary = "1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd"
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

        let mut heartbeats: Heartbeats = heartbeats.into_iter().collect();

        // Set all of the hours seen to true
        for (_, hb) in heartbeats.heartbeats.iter_mut() {
            hb.hours_seen = [true; 24];
        }

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

        let owner_rewards = RewardShares::aggregate(&mut resolver, heartbeats, speedtest_avgs)
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
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        let owner3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed owner3 parse");
        let owner4: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed owner4 parse");

        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw4: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");
        let gw5: PublicKeyBinary = "112j1iw1sV2B2Tz2DxPSeum9Cmc5kMKNdDTDg1zDRsdwuvZueq3B"
            .parse()
            .expect("failed gw5 parse");
        let gw6: PublicKeyBinary = "11fCasUk9XvU15ktsMMH64J9E7XuqQ2L5FJPv8HZMCDG6kdZ3SC"
            .parse()
            .expect("failed gw6 parse");
        let gw7: PublicKeyBinary = "11HdwRpQDrYM7LJtRGSzRF3vY2iwuumx1Z2MUhBYAVTwZdSh6Bi"
            .parse()
            .expect("failed gw7 parse");
        let gw8: PublicKeyBinary = "112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ"
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
        let mut heartbeats: Heartbeats = heartbeats.into_iter().collect();

        // Set all of the hours seen to true
        for (_, hb) in heartbeats.heartbeats.iter_mut() {
            hb.hours_seen = [true; 24];
        }

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
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let epoch = (now - Duration::hours(1))..now;
        let transfer_rewards = TransferRewards::empty(&epoch);
        for radio_share in RewardShares::aggregate(&mut resolver, heartbeats, speedtest_avgs)
            .await
            .expect("Could not generate rewards")
            .into_radio_shares(&transfer_rewards, &epoch)
            .expect("Clock is out of sync")
        {
            *owner_rewards
                .entry(PublicKeyBinary::from(radio_share.owner_key))
                .or_default() += radio_share.amount;
        }

        assert_eq!(
            *owner_rewards
                .get(&owner1)
                .expect("Could not fetch owner1 rewards"),
            997_150_997_150
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            2_991_452_991_450
        );
        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            178_062_678_062
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let mut total = 0;
        for val in owner_rewards.values() {
            total += *val
        }

        assert_eq!(total, 4_166_666_666_662); // total emissions for 1 hour
    }

    #[tokio::test]
    async fn dont_write_zero_rewards() {
        use rust_decimal_macros::dec;

        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");

        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c3 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let mut shares = HashMap::new();

        shares.insert(
            owner1.clone(),
            RadioShares {
                shares: vec![RadioShare {
                    hotspot_key: gw1,
                    cbsd_id: c1,
                    amount: dec!(10.0),
                }],
            },
        );
        shares.insert(
            owner2,
            RadioShares {
                shares: vec![
                    RadioShare {
                        hotspot_key: gw2.clone(),
                        cbsd_id: c2,
                        amount: dec!(-1.0),
                    },
                    RadioShare {
                        hotspot_key: gw2,
                        cbsd_id: c3,
                        amount: dec!(0.0),
                    },
                ],
            },
        );

        let now = Utc::now();
        // We should never see any radio shares from owner2, since all of them are
        // less than or equal to zero.
        let owner_shares = RewardShares { shares };
        let epoch = now - Duration::hours(1)..now;
        let transfer_rewards = TransferRewards::empty(&epoch);
        for reward in owner_shares
            .into_radio_shares(&transfer_rewards, &epoch)
            .expect("Could not convert to radio shares")
        {
            let actual_owner = PublicKeyBinary::from(reward.owner_key);
            assert_eq!(actual_owner, owner1);
        }
    }
}
