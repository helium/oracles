use crate::{
    data_session::HotspotMap,
    heartbeats::HeartbeatReward,
    speedtests_average::{SpeedtestAverage, SpeedtestAverages},
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

/// Total tokens emissions pool per 365 days or 366 days for a leap year
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

/// shares of the mappers pool allocated per eligible subscriber for discovery mapping
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

        // the number of subscribers eligible for discovery location rewards
        let discovery_mappers_count = Decimal::from(self.discovery_mapping_shares.len());

        // calculate the total eligible mapping shares for the epoch
        // this could be simplified as every subscriber is awarded the same share
        // however the function is setup to allow the verification mapper shares to be easily
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
    radio_shares: HashMap<Option<String>, Decimal>,
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
        heartbeat_rewards: impl Stream<Item = Result<HeartbeatReward, sqlx::Error>>,
        speedtest_averages: &SpeedtestAverages,
        reward_wifi_hbs: bool,
    ) -> anyhow::Result<Self> {
        let mut poc_shares = Self::default();
        let mut heartbeat_rewards = std::pin::pin!(heartbeat_rewards);
        while let Some(heartbeat_reward) = heartbeat_rewards.next().await.transpose()? {
            let speedmultiplier = speedtest_averages
                .get_average(&heartbeat_reward.hotspot_key)
                .as_ref()
                .map_or(Decimal::ZERO, SpeedtestAverage::reward_multiplier);
            *poc_shares
                .hotspot_shares
                .entry(heartbeat_reward.hotspot_key.clone())
                .or_default()
                .radio_shares
                .entry(heartbeat_reward.cbsd_id.clone())
                .or_default() += heartbeat_reward.reward_weight(reward_wifi_hbs) * speedmultiplier;
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
                                        cbsd_id: cbsd_id.unwrap_or(String::new()),
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
    (TOTAL_EMISSIONS_POOL / dec!(366) / Decimal::from(Duration::hours(24).num_seconds()))
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
        heartbeats::{HeartbeatReward, HeartbeatRow},
        speedtests::Speedtest,
        subscriber_location::SubscriberValidatedLocations,
    };
    use chrono::{Duration, Utc};
    use file_store::speedtest::CellSpeedtest;
    use futures::stream;
    use helium_proto::services::poc_mobile::mobile_reward_share::Reward as MobileReward;
    use prost::Message;
    use std::collections::HashMap;

    fn valid_shares() -> RadioShares {
        let mut radio_shares: HashMap<Option<String>, Decimal> = Default::default();
        radio_shares.insert(Some(String::new()), Decimal::ONE);
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
        assert_eq!(81_967_213_114_754, total_epoch_rewards);

        // verify total rewards allocated to mappers the epoch
        let total_mapper_rewards = get_scheduled_tokens_for_mappers(epoch.end - epoch.start)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(16_393_442_622_950, total_mapper_rewards);

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
        assert_eq!(16_393_442_620_000, total_discovery_mapping_rewards);

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
            dec!(49_180_327)
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
        assert_eq!(available_poc_rewards.trunc(), dec!(16_393_442_622_950));
        assert_eq!(
            // Rewards are automatically scaled
            data_transfer_rewards.reward(&owner).trunc(),
            dec!(32_786_885_245_901)
        );
        assert_eq!(data_transfer_rewards.reward_scale().round_dp(1), dec!(0.5));
    }

    fn bytes_per_s(mbps: u64) -> u64 {
        mbps * 125000
    }

    fn acceptable_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(10),
                download_speed: bytes_per_s(100),
                latency: 25,
                serial: "".to_string(),
            },
        }
    }

    fn degraded_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(5),
                download_speed: bytes_per_s(60),
                latency: 60,
                serial: "".to_string(),
            },
        }
    }

    fn failed_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(1),
                download_speed: bytes_per_s(20),
                latency: 110,
                serial: "".to_string(),
            },
        }
    }

    fn poor_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(2),
                download_speed: bytes_per_s(40),
                latency: 90,
                serial: "".to_string(),
            },
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
        let g3: PublicKeyBinary = "112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf"
            .parse()
            .expect("unable to construct pubkey");
        let g4: PublicKeyBinary = "11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo"
            .parse()
            .expect("unable to construct pubkey");
        let g5: PublicKeyBinary = "113HRxtzxFbFUjDEJJpyeMRZRtdAW38LAUnB5mshRwi6jt7uFbt"
            .parse()
            .expect("unable to construct pubkey");

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c1ct = CellType::from_cbsd_id(&c1).expect("unable to get cell_type");
        let c2ct = CellType::from_cbsd_id(&c2).expect("unable to get cell_type");

        let g3ct = CellType::NovaGenericWifiIndoor;
        let g4ct = CellType::NovaGenericWifiIndoor;
        let g5ct = CellType::NovaGenericWifiIndoor;

        let timestamp = Utc::now();

        let heartbeat_keys = vec![
            HeartbeatRow {
                cbsd_id: Some(c1.clone()),
                hotspot_key: g1.clone(),
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: g1.clone(),
                cell_type: c2ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c1.clone()),
                hotspot_key: g2.clone(),
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c1.clone()),
                hotspot_key: g2.clone(),
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g3.clone(),
                cell_type: g3ct,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g4.clone(),
                cell_type: g4ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g5.clone(),
                cell_type: g5ct,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(100000),
            },
        ];
        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(HeartbeatReward::from)
            .collect();

        let last_timestamp = timestamp - Duration::hours(12);
        let g1_speedtests = vec![
            acceptable_speedtest(g1.clone(), last_timestamp),
            acceptable_speedtest(g1.clone(), timestamp),
        ];
        let g2_speedtests = vec![
            acceptable_speedtest(g2.clone(), last_timestamp),
            acceptable_speedtest(g2.clone(), timestamp),
        ];
        let g3_speedtests = vec![
            acceptable_speedtest(g3.clone(), last_timestamp),
            acceptable_speedtest(g3.clone(), timestamp),
        ];
        let g4_speedtests = vec![
            acceptable_speedtest(g4.clone(), last_timestamp),
            acceptable_speedtest(g4.clone(), timestamp),
        ];
        let g5_speedtests = vec![
            acceptable_speedtest(g5.clone(), last_timestamp),
            acceptable_speedtest(g5.clone(), timestamp),
        ];
        let g1_average = SpeedtestAverage::from(&g1_speedtests);
        let g2_average = SpeedtestAverage::from(&g2_speedtests);
        let g3_average = SpeedtestAverage::from(&g3_speedtests);
        let g4_average = SpeedtestAverage::from(&g4_speedtests);
        let g5_average = SpeedtestAverage::from(&g5_speedtests);
        let mut averages = HashMap::new();
        averages.insert(g1.clone(), g1_average);
        averages.insert(g2.clone(), g2_average);
        averages.insert(g3.clone(), g3_average);
        averages.insert(g4.clone(), g4_average);
        averages.insert(g5.clone(), g5_average);
        let speedtest_avgs = SpeedtestAverages { averages };

        let rewards = PocShares::aggregate(
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            true,
        )
        .await
        .unwrap();

        let gw1_shares = rewards
            .hotspot_shares
            .get(&g1)
            .expect("Could not fetch gateway1 shares")
            .total_shares();
        let gw2_shares = rewards
            .hotspot_shares
            .get(&g2)
            .expect("Could not fetch gateway1 shares")
            .total_shares();
        let gw3_shares = rewards
            .hotspot_shares
            .get(&g3)
            .expect("Could not fetch gateway3 shares")
            .total_shares();
        let gw4_shares = rewards
            .hotspot_shares
            .get(&g4)
            .expect("Could not fetch gateway4 shares")
            .total_shares();
        let gw5_shares = rewards
            .hotspot_shares
            .get(&g5)
            .expect("Could not fetch gateway5 shares")
            .total_shares();

        // The owner with two hotspots gets more rewards
        assert_eq!(gw1_shares, dec!(3.50));
        assert_eq!(gw2_shares, dec!(2.00));
        assert!(gw1_shares > gw2_shares);

        // gw3 has wifi HBs and has location validation timestamp
        // gets the full 0.4 reward weight
        assert_eq!(gw3_shares, dec!(0.40));
        // gw4 has wifi HBs and DOES NOT have a location validation timestamp
        // gets 0.25 of the full reward weight
        assert_eq!(gw4_shares, dec!(0.1));
        // gw4 has wifi HBs and does have a location validation timestamp
        // but the HB distance is too far from the asserted location
        // gets 0.25 of the full reward weight
        assert_eq!(gw5_shares, dec!(0.1));
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
        let owner5: PublicKeyBinary = "112bUGwooPd1dCDd3h3yZwskjxCzBsQNKeaJTuUF4hSgYedcsFa9"
            .parse()
            .expect("failed owner5 parse");
        let owner6: PublicKeyBinary = "112WqD16uH8GLmCMhyRUrp6Rw5MTELzBdx7pSepySYUoSjixQoxJ"
            .parse()
            .expect("failed owner6 parse");

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
        // include a couple of wifi spots in the mix
        let gw9: PublicKeyBinary = "112bUuQaE7j73THS9ABShHGokm46Miip9L361FSyWv7zSYn8hZWf"
            .parse()
            .expect("failed gw9 parse");
        let gw10: PublicKeyBinary = "11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo"
            .parse()
            .expect("failed gw10 parse");

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
        owners.insert(gw9.clone(), owner5.clone());
        owners.insert(gw10.clone(), owner6.clone());

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
        let heartbeat_keys = vec![
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c4.clone()),
                hotspot_key: gw3.clone(),
                cell_type: CellType::from_cbsd_id(&c4).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c5.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c5).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c6.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c6).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c7.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c7).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c8.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c8).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c9.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c9).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c10.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c10).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c11.clone()),
                hotspot_key: gw4.clone(),
                cell_type: CellType::from_cbsd_id(&c11).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c12.clone()),
                hotspot_key: gw5.clone(),
                cell_type: CellType::from_cbsd_id(&c12).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c13.clone()),
                hotspot_key: gw6.clone(),
                cell_type: CellType::from_cbsd_id(&c13).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c14.clone()),
                hotspot_key: gw7.clone(),
                cell_type: CellType::from_cbsd_id(&c14).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw9.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw10.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
        ];

        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(HeartbeatReward::from)
            .collect();

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw1_speedtests = vec![
            acceptable_speedtest(gw1.clone(), last_speedtest),
            acceptable_speedtest(gw1.clone(), timestamp),
        ];
        let gw2_speedtests = vec![
            acceptable_speedtest(gw2.clone(), last_speedtest),
            acceptable_speedtest(gw2.clone(), timestamp),
        ];
        let gw3_speedtests = vec![
            acceptable_speedtest(gw3.clone(), last_speedtest),
            acceptable_speedtest(gw3.clone(), timestamp),
        ];
        let gw4_speedtests = vec![
            acceptable_speedtest(gw4.clone(), last_speedtest),
            acceptable_speedtest(gw4.clone(), timestamp),
        ];
        let gw5_speedtests = vec![
            degraded_speedtest(gw5.clone(), last_speedtest),
            degraded_speedtest(gw5.clone(), timestamp),
        ];
        let gw6_speedtests = vec![
            failed_speedtest(gw6.clone(), last_speedtest),
            failed_speedtest(gw6.clone(), timestamp),
        ];
        let gw7_speedtests = vec![
            poor_speedtest(gw7.clone(), last_speedtest),
            poor_speedtest(gw7.clone(), timestamp),
        ];
        let gw9_speedtests = vec![
            poor_speedtest(gw9.clone(), last_speedtest),
            poor_speedtest(gw9.clone(), timestamp),
        ];
        let gw10_speedtests = vec![
            poor_speedtest(gw10.clone(), last_speedtest),
            poor_speedtest(gw10.clone(), timestamp),
        ];

        let gw1_average = SpeedtestAverage::from(&gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(&gw2_speedtests);
        let gw3_average = SpeedtestAverage::from(&gw3_speedtests);
        let gw4_average = SpeedtestAverage::from(&gw4_speedtests);
        let gw5_average = SpeedtestAverage::from(&gw5_speedtests);
        let gw6_average = SpeedtestAverage::from(&gw6_speedtests);
        let gw7_average = SpeedtestAverage::from(&gw7_speedtests);
        let gw9_average = SpeedtestAverage::from(&gw9_speedtests);
        let gw10_average = SpeedtestAverage::from(&gw10_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);
        averages.insert(gw3.clone(), gw3_average);
        averages.insert(gw4.clone(), gw4_average);
        averages.insert(gw5.clone(), gw5_average);
        averages.insert(gw6.clone(), gw6_average);
        averages.insert(gw7.clone(), gw7_average);
        averages.insert(gw9.clone(), gw9_average);
        averages.insert(gw10.clone(), gw10_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        // calculate the rewards for the sample group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let epoch = (now - Duration::hours(1))..now;
        for mobile_reward in PocShares::aggregate(
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            true,
        )
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
            486_246_179_493
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            1_458_738_538_478
        );

        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            86_829_674_909
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let owner5_reward = *owner_rewards
            .get(&owner5)
            .expect("Could not fetch owner5 rewards");
        assert_eq!(owner5_reward, 13_892_747_985);

        let owner6_reward = *owner_rewards
            .get(&owner6)
            .expect("Could not fetch owner6 rewards");
        assert_eq!(owner6_reward, 3_473_186_996);

        // confirm owner 6 reward is 0.25 of owner 5's reward
        // this is due to owner 6's hotspot not having a validation location timestamp
        // and thus its reward scale is reduced
        assert_eq!((owner5_reward as f64 * 0.25) as u64, owner6_reward);

        let mut total = 0;
        for val in owner_rewards.values() {
            total += *val
        }

        assert_eq!(total, 2_049_180_327_861); // total emissions for 1 hour
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
                radio_shares: vec![(Some(c1), dec!(10.0))].into_iter().collect(),
            },
        );
        hotspot_shares.insert(
            gw2,
            RadioShares {
                radio_shares: vec![(Some(c2), dec!(-1.0)), (Some(c3), dec!(0.0))]
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
