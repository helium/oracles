use crate::{
    data_session::HotspotMap,
    heartbeats::HeartbeatReward,
    speedtests::{Average, SpeedtestAverages},
    subscriber_location::SubscriberValidatedLocations,
};

use chrono::{DateTime, Duration, Utc};
use file_store::traits::TimestampEncode;
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use helium_proto::services::poc_mobile::mobile_reward_share::Reward as ProtoReward;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::ops::Range;

/// Total tokens emissions pool per 365 days
const TOTAL_EMISSIONS_POOL: Decimal = dec!(30_000_000_000_000_000);

/// Maximum amount of the total emissions pool allocated for data transfer
/// rewards
const MAX_DATA_TRANSFER_REWARDS_PERCENT: Decimal = dec!(0.4);

/// The fixed price of a mobile data credit
const DC_USD_PRICE: Decimal = dec!(0.00001);

/// Default precision used for rounding
const DEFAULT_PREC: u32 = 15;

// Percent of total emissions allocated for mapper rewards
const MAPPERS_REWARDS_PERCENT: Decimal = dec!(0.2);

/// shares of the mappers pool allocated per eligble subscriber for discovery mapping
const DISCOVERY_MAPPING_SHARES: Decimal = dec!(30);

pub struct TransferRewards {
    reward_scale: Decimal,
    rewards: HashMap<PublicKeyBinary, Decimal>,
    reward_sum: Decimal,
}

impl TransferRewards {
    pub fn reward_scale(&self) -> Decimal {
        self.reward_scale
    }

    pub fn reward_sum(&self) -> Decimal {
        self.reward_sum
    }

    #[cfg(test)]
    fn reward(&self, hotspot: &PublicKeyBinary) -> Decimal {
        self.rewards.get(hotspot).copied().unwrap_or(Decimal::ZERO) * self.reward_scale
    }

    pub async fn from_transfer_sessions(
        mobile_bone_price: Decimal,
        transfer_sessions: HotspotMap,
        hotspots: &PocShares,
        epoch: &Range<DateTime<Utc>>,
    ) -> Self {
        let mut reward_sum = Decimal::ZERO;
        let rewards = transfer_sessions
            .into_iter()
            .filter(|(pub_key, _)| hotspots.is_valid(pub_key))
            // Calculate rewards per hotspot
            .map(|(pub_key, dc_amount)| {
                let bones = dc_to_mobile_bones(Decimal::from(dc_amount), mobile_bone_price);
                reward_sum += bones;
                (pub_key, bones)
            })
            .collect();

        let duration = epoch.end - epoch.start;
        let total_emissions_pool = get_total_scheduled_tokens(duration);

        // Determine if we need to scale the rewards given for data transfer rewards.
        // Ideally this should never happen, but if the total number of data transfer rewards
        // is greater than (at the time of writing) 40% of the total pool, we need to scale
        // the rewards given for data transfer.
        //
        // If we find that total data_transfer reward sum is greater than 40%, we use the
        // following math to calculate the scale:
        //
        // [ scale * data_transfer_reward_sum ] / total_emissions_pool = 0.4
        //
        //   therefore:
        //
        // scale = [ 0.4 * total_emissions_pool ] / data_transfer_reward_sum
        //
        let reward_scale = if reward_sum / total_emissions_pool > MAX_DATA_TRANSFER_REWARDS_PERCENT
        {
            MAX_DATA_TRANSFER_REWARDS_PERCENT * total_emissions_pool / reward_sum
        } else {
            Decimal::ONE
        };

        Self {
            reward_scale,
            rewards,
            reward_sum: reward_sum * reward_scale,
        }
    }

    pub fn into_rewards(
        self,
        epoch: &'_ Range<DateTime<Utc>>,
    ) -> impl Iterator<Item = proto::MobileRewardShare> + '_ {
        let Self {
            reward_scale,
            rewards,
            ..
        } = self;
        let start_period = epoch.start.encode_timestamp();
        let end_period = epoch.end.encode_timestamp();
        rewards
            .into_iter()
            .map(move |(hotspot_key, reward)| proto::MobileRewardShare {
                start_period,
                end_period,
                reward: Some(proto::mobile_reward_share::Reward::GatewayReward(
                    proto::GatewayReward {
                        hotspot_key: hotspot_key.into(),
                        dc_transfer_reward: (reward * reward_scale)
                            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                            .to_u64()
                            .unwrap_or(0),
                    },
                )),
            })
    }
}

#[derive(Default)]
pub struct MapperShares {
    pub discovery_mapping_shares: SubscriberValidatedLocations,
}

impl MapperShares {
    pub fn new(discovery_mapping_shares: SubscriberValidatedLocations) -> Self {
        Self {
            discovery_mapping_shares,
        }
    }

    pub fn rewards_per_share(
        &self,
        reward_period: &'_ Range<DateTime<Utc>>,
    ) -> anyhow::Result<Decimal> {
        // note: currently rewards_per_share calculation only takes into
        // consideration discovery mapping shares
        // in the future it will also need to take into account
        // verification mapping shares
        let duration: Duration = reward_period.end - reward_period.start;
        let total_mappers_pool = get_scheduled_tokens_for_mappers(duration);

        // the number of subscribers eligible for discovery location rewards hihofe
        let discovery_mappers_count = Decimal::from(self.discovery_mapping_shares.len());

        // calculate the total eligible mapping shares for the epoch
        // this could be simplified as every subscriber is awarded the same share
        // however the fuction is setup to allow the verification mapper shares to be easily
        // added without impacting code structure ( the per share value for those will be different )
        let total_mapper_shares = discovery_mappers_count * DISCOVERY_MAPPING_SHARES;
        let res = total_mappers_pool
            .checked_div(total_mapper_shares)
            .unwrap_or(Decimal::ZERO);
        Ok(res)
    }

    pub fn into_subscriber_rewards(
        self,
        reward_period: &'_ Range<DateTime<Utc>>,
        reward_per_share: Decimal,
    ) -> impl Iterator<Item = proto::MobileRewardShare> + '_ {
        self.discovery_mapping_shares
            .into_iter()
            .map(move |subscriber_id| proto::SubscriberReward {
                subscriber_id,
                discovery_location_amount: (DISCOVERY_MAPPING_SHARES * reward_per_share)
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
                    .unwrap_or(0),
            })
            .filter(|subscriber_reward| subscriber_reward.discovery_location_amount > 0)
            .map(|subscriber_reward| proto::MobileRewardShare {
                start_period: reward_period.start.encode_timestamp(),
                end_period: reward_period.end.encode_timestamp(),
                reward: Some(ProtoReward::SubscriberReward(subscriber_reward)),
            })
    }
}

/// Returns the equivalent amount of Mobile bones for a specified amount of Data Credits
pub fn dc_to_mobile_bones(dc_amount: Decimal, mobile_bone_price: Decimal) -> Decimal {
    let dc_in_usd = dc_amount * DC_USD_PRICE;
    (dc_in_usd / mobile_bone_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

#[derive(Default)]
pub struct RadioShares {
    radio_shares: HashMap<String, Decimal>,
}

impl RadioShares {
    fn total_shares(&self) -> Decimal {
        self.radio_shares
            .values()
            .fold(Decimal::ZERO, |sum, amount| sum + amount)
    }
}

#[derive(Default)]
pub struct PocShares {
    pub hotspot_shares: HashMap<PublicKeyBinary, RadioShares>,
}

impl PocShares {
    pub async fn aggregate(
        heartbeats: impl Stream<Item = Result<HeartbeatReward, sqlx::Error>>,
        speedtests: SpeedtestAverages,
    ) -> Result<Self, sqlx::Error> {
        let mut poc_shares = Self::default();
        let mut heartbeats = std::pin::pin!(heartbeats);
        while let Some(heartbeat) = heartbeats.next().await.transpose()? {
            let speedmultiplier = speedtests
                .get_average(&heartbeat.hotspot_key)
                .as_ref()
                .map_or(Decimal::ZERO, Average::reward_multiplier);
            *poc_shares
                .hotspot_shares
                .entry(heartbeat.hotspot_key)
                .or_default()
                .radio_shares
                .entry(heartbeat.cbsd_id)
                .or_default() += heartbeat.reward_weight * speedmultiplier;
        }
        Ok(poc_shares)
    }

    pub fn is_valid(&self, hotspot: &PublicKeyBinary) -> bool {
        if let Some(shares) = self.hotspot_shares.get(hotspot) {
            !shares.total_shares().is_zero()
        } else {
            false
        }
    }

    pub fn total_shares(&self) -> Decimal {
        self.hotspot_shares
            .values()
            .fold(Decimal::ZERO, |sum, radio_shares| {
                sum + radio_shares.total_shares()
            })
    }

    pub fn into_rewards(
        self,
        transfer_rewards_sum: Decimal,
        epoch: &'_ Range<DateTime<Utc>>,
    ) -> Option<impl Iterator<Item = proto::MobileRewardShare> + '_> {
        let total_shares = self.total_shares();
        let available_poc_rewards =
            get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start) - transfer_rewards_sum;
        if let Some(poc_rewards_per_share) = available_poc_rewards.checked_div(total_shares) {
            let start_period = epoch.start.encode_timestamp();
            let end_period = epoch.end.encode_timestamp();
            Some(
                self.hotspot_shares
                    .into_iter()
                    .flat_map(move |(hotspot_key, RadioShares { radio_shares })| {
                        radio_shares.into_iter().map(move |(cbsd_id, amount)| {
                            let poc_reward = poc_rewards_per_share * amount;
                            let hotspot_key: Vec<u8> = hotspot_key.clone().into();
                            proto::MobileRewardShare {
                                start_period,
                                end_period,
                                reward: Some(proto::mobile_reward_share::Reward::RadioReward(
                                    proto::RadioReward {
                                        hotspot_key,
                                        cbsd_id,
                                        poc_reward: poc_reward
                                            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                                            .to_u64()
                                            .unwrap_or(0),
                                        ..Default::default()
                                    },
                                )),
                            }
                        })
                    })
                    .filter(|mobile_reward| match mobile_reward.reward {
                        Some(proto::mobile_reward_share::Reward::RadioReward(ref radio_reward)) => {
                            radio_reward.poc_reward > 0
                        }
                        _ => false,
                    }),
            )
        } else {
            None
        }
    }
}

pub fn get_total_scheduled_tokens(duration: Duration) -> Decimal {
    (TOTAL_EMISSIONS_POOL / dec!(365) / Decimal::from(Duration::hours(24).num_seconds()))
        * Decimal::from(duration.num_seconds())
}

pub fn get_scheduled_tokens_for_poc_and_dc(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * dec!(0.6)
}

pub fn get_scheduled_tokens_for_mappers(duration: Duration) -> Decimal {
    get_total_scheduled_tokens(duration) * MAPPERS_REWARDS_PERCENT
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        cell_type::CellType,
        data_session,
        data_session::HotspotDataSession,
        heartbeats::HeartbeatReward,
        speedtests::{Speedtest, SpeedtestAverages},
        subscriber_location::SubscriberValidatedLocations,
    };
    use chrono::{Duration, Utc};
    use futures::stream;
    use helium_proto::services::poc_mobile::mobile_reward_share::Reward as MobileReward;
    use prost::Message;
    use std::collections::{HashMap, VecDeque};

    fn valid_shares() -> RadioShares {
        let mut radio_shares: HashMap<String, Decimal> = Default::default();
        radio_shares.insert(String::new(), Decimal::ONE);
        RadioShares { radio_shares }
    }

    #[test]
    fn bytes_to_bones() {
        assert_eq!(
            dc_to_mobile_bones(Decimal::from(1), dec!(1.0)),
            dec!(0.00001)
        );
        assert_eq!(
            dc_to_mobile_bones(Decimal::from(2), dec!(1.0)),
            dec!(0.00002)
        );
    }

    #[tokio::test]
    async fn discover_mapping_amount() {
        // test based on example defined at https://github.com/helium/oracles/issues/422
        // NOTE: the example defined above lists values in mobile tokens, whereas
        //       this test uses mobile bones

        const NUM_SUBSCRIBERS: u64 = 10_000;

        // simulate 10k subscriber location shares
        let mut location_shares = SubscriberValidatedLocations::new();
        for n in 0..NUM_SUBSCRIBERS {
            location_shares.push(n.encode_to_vec());
        }

        // calculate discovery mapping rewards for a 24hr period
        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;

        // translate location shares into discovery mapping shares
        let mapping_shares = MapperShares::new(location_shares);
        let rewards_per_share = mapping_shares.rewards_per_share(&epoch).unwrap();

        // verify total rewards for the epoch
        let total_epoch_rewards = get_total_scheduled_tokens(epoch.end - epoch.start)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(82_191_780_821_917, total_epoch_rewards);

        // verify total rewards allocated to mappers the epoch
        let total_mapper_rewards = get_scheduled_tokens_for_mappers(epoch.end - epoch.start)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(16_438_356_164_383, total_mapper_rewards);

        let expected_reward_per_subscriber = total_mapper_rewards / NUM_SUBSCRIBERS;

        // get the summed rewards allocated to subscribers for discovery location
        let mut total_discovery_mapping_rewards = 0_u64;
        for subscriber_share in mapping_shares.into_subscriber_rewards(&epoch, rewards_per_share) {
            if let Some(MobileReward::SubscriberReward(r)) = subscriber_share.reward {
                total_discovery_mapping_rewards += r.discovery_location_amount;
                assert_eq!(expected_reward_per_subscriber, r.discovery_location_amount);
            }
        }

        // verify the total rewards awared for discovery mapping
        assert_eq!(16_438_356_160_000, total_discovery_mapping_rewards);

        // the sum of rewards distributed should not exceed the epoch amount
        // but due to rounding whilst going to u64 for each subscriber,
        // we will be some bones short of the full epoch amount
        // the difference in bones cannot be more than the total number of subscribers ( 10 k)
        let diff = total_mapper_rewards - total_discovery_mapping_rewards;
        assert!(diff < NUM_SUBSCRIBERS);
    }

    #[tokio::test]
    async fn transfer_reward_amount() {
        let owner: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner parse");
        let payer: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed payer parse");

        let data_transfer_session = HotspotDataSession {
            pub_key: owner.clone(),
            payer,
            upload_bytes: 0,   // Unused
            download_bytes: 0, // Unused
            num_dcs: 2,
            received_timestamp: DateTime::default(),
        };

        let mut data_transfer_map = HotspotMap::new();
        data_transfer_map.insert(
            data_transfer_session.pub_key,
            data_transfer_session.num_dcs as u64,
        );

        let mut hotspot_shares = HashMap::default();
        hotspot_shares.insert(owner.clone(), valid_shares());
        let poc_shares = PocShares { hotspot_shares };

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;
        let total_rewards = get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start);

        // confirm our hourly rewards add up to expected 24hr amount
        // total_rewards will be in bones
        assert_eq!(
            (total_rewards / dec!(1_000_000) * dec!(24)).trunc(),
            dec!(49_315_068)
        );

        let data_transfer_rewards = TransferRewards::from_transfer_sessions(
            dec!(1.0),
            data_transfer_map,
            &poc_shares,
            &epoch,
        )
        .await;

        assert_eq!(data_transfer_rewards.reward(&owner), dec!(0.00002));
        assert_eq!(data_transfer_rewards.reward_scale(), dec!(1.0));
        let available_poc_rewards = get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start)
            - data_transfer_rewards.reward_sum;
        assert_eq!(
            available_poc_rewards,
            total_rewards
                - (data_transfer_rewards.reward(&owner) * data_transfer_rewards.reward_scale())
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

        let mut transfer_sessions = Vec::new();
        // Just an absurdly large amount of DC
        for _ in 0..3_003 {
            transfer_sessions.push(Ok(HotspotDataSession {
                pub_key: owner.clone(),
                payer: payer.clone(),
                upload_bytes: 0,
                download_bytes: 0,
                num_dcs: 2222222222222222,
                received_timestamp: DateTime::default(),
            }));
        }
        let data_transfer_sessions = stream::iter(transfer_sessions);
        let aggregated_data_transfer_sessions =
            data_session::data_sessions_to_dc(data_transfer_sessions)
                .await
                .unwrap();

        let now = Utc::now();
        let epoch = (now - Duration::hours(24))..now;

        let mut hotspot_shares = HashMap::default();
        hotspot_shares.insert(owner.clone(), valid_shares());
        let poc_shares = PocShares { hotspot_shares };

        let data_transfer_rewards = TransferRewards::from_transfer_sessions(
            dec!(1.0),
            aggregated_data_transfer_sessions,
            &poc_shares,
            &epoch,
        )
        .await;

        // We have constructed the data transfer in such a way that they easily exceed the maximum
        // allotted reward amount for data transfer, which is 40% of the daily tokens. We check to
        // ensure that amount of tokens remaining for POC is no less than 20% of the rewards allocated
        // for POC and data transfer (which is 60% of the daily total emissions).
        let available_poc_rewards = get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start)
            - data_transfer_rewards.reward_sum;
        assert_eq!(available_poc_rewards.trunc(), dec!(16_438_356_164_383));
        assert_eq!(
            // Rewards are automatically scaled
            data_transfer_rewards.reward(&owner).trunc(),
            dec!(32_876_712_328_767)
        );
        assert_eq!(data_transfer_rewards.reward_scale().round_dp(1), dec!(0.5));
    }

    fn bytes_per_s(mbps: i64) -> i64 {
        mbps * 125000
    }

    fn cell_type_weight(cbsd_id: &str) -> Decimal {
        CellType::from_cbsd_id(cbsd_id)
            .expect("unable to get cell_type")
            .reward_weight()
    }

    fn acceptable_speedtest(timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(10),
            download_speed: bytes_per_s(100),
            latency: 25,
        }
    }

    fn degraded_speedtest(timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(5),
            download_speed: bytes_per_s(60),
            latency: 60,
        }
    }

    fn failed_speedtest(timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(1),
            download_speed: bytes_per_s(20),
            latency: 110,
        }
    }

    fn poor_speedtest(timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            timestamp,
            upload_speed: bytes_per_s(2),
            download_speed: bytes_per_s(40),
            latency: 90,
        }
    }

    #[tokio::test]
    async fn test_radio_weights() {
        let g1: PublicKeyBinary = "11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL"
            .parse()
            .expect("unable to construct pubkey");
        let g2: PublicKeyBinary = "118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc"
            .parse()
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let timestamp = Utc::now();

        let heartbeats = vec![
            HeartbeatReward {
                cbsd_id: c1.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c1),
            },
            HeartbeatReward {
                cbsd_id: c2.clone(),
                hotspot_key: g1.clone(),
                reward_weight: cell_type_weight(&c2),
            },
            HeartbeatReward {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
            },
            HeartbeatReward {
                cbsd_id: c1.clone(),
                hotspot_key: g2.clone(),
                reward_weight: cell_type_weight(&c1),
            },
        ];

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
        speedtests.insert(g1.clone(), VecDeque::from(g1_speedtests));
        speedtests.insert(g2.clone(), VecDeque::from(g2_speedtests));
        let speedtest_avgs = SpeedtestAverages { speedtests };

        let rewards = PocShares::aggregate(stream::iter(heartbeats).map(Ok), speedtest_avgs)
            .await
            .unwrap();

        // The owner with two hotspots gets more rewards
        assert!(
            rewards
                .hotspot_shares
                .get(&g1)
                .expect("Could not fetch gateway1 shares")
                .total_shares()
                > rewards
                    .hotspot_shares
                    .get(&g2)
                    .expect("Could not fetch gateway2 shares")
                    .total_shares()
        );
    }

    #[tokio::test]
    async fn reward_shares_with_speed_multiplier() {
        // init owners
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

        // init cells and cell_types
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
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
        let timestamp = now - Duration::minutes(20);

        // setup heartbeats
        let heartbeats = vec![
            HeartbeatReward {
                cbsd_id: c2.clone(),
                hotspot_key: gw2.clone(),
                reward_weight: cell_type_weight(&c2),
            },
            HeartbeatReward {
                cbsd_id: c4.clone(),
                hotspot_key: gw3.clone(),
                reward_weight: cell_type_weight(&c4),
            },
            HeartbeatReward {
                cbsd_id: c5.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c5),
            },
            HeartbeatReward {
                cbsd_id: c6.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c6),
            },
            HeartbeatReward {
                cbsd_id: c7.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c7),
            },
            HeartbeatReward {
                cbsd_id: c8.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c8),
            },
            HeartbeatReward {
                cbsd_id: c9.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c9),
            },
            HeartbeatReward {
                cbsd_id: c10.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c10),
            },
            HeartbeatReward {
                cbsd_id: c11.clone(),
                hotspot_key: gw4.clone(),
                reward_weight: cell_type_weight(&c11),
            },
            HeartbeatReward {
                cbsd_id: c12.clone(),
                hotspot_key: gw5.clone(),
                reward_weight: cell_type_weight(&c12),
            },
            HeartbeatReward {
                cbsd_id: c13.clone(),
                hotspot_key: gw6.clone(),
                reward_weight: cell_type_weight(&c13),
            },
            HeartbeatReward {
                cbsd_id: c14.clone(),
                hotspot_key: gw7.clone(),
                reward_weight: cell_type_weight(&c14),
            },
        ];

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
        for mobile_reward in PocShares::aggregate(stream::iter(heartbeats).map(Ok), speedtest_avgs)
            .await
            .unwrap()
            .into_rewards(Decimal::ZERO, &epoch)
            .unwrap()
        {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            *owner_rewards.entry(owner).or_default() += radio_reward.poc_reward;
        }

        assert_eq!(
            *owner_rewards
                .get(&owner1)
                .expect("Could not fetch owner1 rewards"),
            491_745_697_224
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            1_475_237_091_670
        );

        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            87_811_731_647
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let mut total = 0;
        for val in owner_rewards.values() {
            total += *val
        }

        assert_eq!(total, 2_054_794_520_541); // total emissions for 1 hour
    }

    #[tokio::test]
    async fn dont_write_zero_rewards() {
        use rust_decimal_macros::dec;

        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "P27-SCE4255W2107CW5000015".to_string();
        let c3 = "2AG32PBS3101S1202000464223GY0153".to_string();

        let mut hotspot_shares = HashMap::new();

        hotspot_shares.insert(
            gw1.clone(),
            RadioShares {
                radio_shares: vec![(c1, dec!(10.0))].into_iter().collect(),
            },
        );
        hotspot_shares.insert(
            gw2,
            RadioShares {
                radio_shares: vec![(c2, dec!(-1.0)), (c3, dec!(0.0))]
                    .into_iter()
                    .collect(),
            },
        );

        let now = Utc::now();
        // We should never see any radio shares from owner2, since all of them are
        // less than or equal to zero.
        let owner_shares = PocShares { hotspot_shares };
        let epoch = now - Duration::hours(1)..now;
        let expected_hotspot = gw1;
        for mobile_reward in owner_shares.into_rewards(Decimal::ZERO, &epoch).unwrap() {
            let radio_reward = match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::RadioReward(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let actual_hotspot = PublicKeyBinary::from(radio_reward.hotspot_key);
            assert_eq!(actual_hotspot, expected_hotspot);
        }
    }

    #[tokio::test]
    async fn skip_empty_radio_rewards() {
        let owner_shares = PocShares {
            hotspot_shares: HashMap::new(),
        };

        let now = Utc::now();
        let epoch = now - Duration::hours(1)..now;

        assert!(owner_shares.into_rewards(Decimal::ZERO, &epoch).is_none());
    }
}
