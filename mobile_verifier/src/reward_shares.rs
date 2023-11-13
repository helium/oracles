use crate::{
    coverage::{CoverageReward, CoveredHexStream, CoveredHexes},
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
use std::{collections::HashMap, ops::Range};
use uuid::Uuid;

/// Total tokens emissions pool per 365 days or 366 days for a leap year
const TOTAL_EMISSIONS_POOL: Decimal = dec!(30_000_000_000_000_000);

/// Maximum amount of the total emissions pool allocated for data transfer
/// rewards
const MAX_DATA_TRANSFER_REWARDS_PERCENT: Decimal = dec!(0.4);

/// The fixed price of a mobile data credit
const DC_USD_PRICE: Decimal = dec!(0.00001);

/// Default precision used for rounding
const DEFAULT_PREC: u32 = 15;

/// Percent of total emissions allocated for mapper rewards
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
        epoch: &Range<DateTime<Utc>>,
    ) -> Self {
        let mut reward_sum = Decimal::ZERO;
        let rewards = transfer_sessions
            .into_iter()
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
            .filter(|mobile_reward| match mobile_reward.reward {
                Some(proto::mobile_reward_share::Reward::GatewayReward(ref gateway_reward)) => {
                    gateway_reward.dc_transfer_reward > 0
                }
                _ => false,
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

#[derive(Debug)]
struct RadioPoints {
    heartbeat_multiplier: Decimal,
    coverage_object: Uuid,
    seniority: DateTime<Utc>,
    points: Decimal,
}

impl RadioPoints {
    fn new(heartbeat_multiplier: Decimal, coverage_object: Uuid, seniority: DateTime<Utc>) -> Self {
        Self {
            heartbeat_multiplier,
            seniority,
            coverage_object,
            points: Decimal::ZERO,
        }
    }

    fn points(&self) -> Decimal {
        (self.heartbeat_multiplier * self.points).max(Decimal::ZERO)
    }
}

#[derive(Debug, Default)]
struct HotspotPoints {
    /// Points are multiplied by the multiplier to get shares.
    /// Multiplier should never be zero.
    speedtest_multiplier: Decimal,
    radio_points: HashMap<Option<String>, RadioPoints>,
}

impl HotspotPoints {
    pub fn new(speedtest_multiplier: Decimal) -> Self {
        Self {
            speedtest_multiplier,
            radio_points: HashMap::new(),
        }
    }
}

impl HotspotPoints {
    pub fn total_points(&self) -> Decimal {
        self.speedtest_multiplier
            * self
                .radio_points
                .values()
                .fold(Decimal::ZERO, |sum, radio| sum + radio.points())
    }
}

#[derive(Debug)]
pub struct CoveragePoints {
    coverage_points: HashMap<PublicKeyBinary, HotspotPoints>,
}

impl CoveragePoints {
    pub async fn aggregate_points(
        hex_streams: &impl CoveredHexStream,
        heartbeats: impl Stream<Item = Result<HeartbeatReward, sqlx::Error>>,
        speedtests: &SpeedtestAverages,
        period_end: DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        let mut heartbeats = std::pin::pin!(heartbeats);
        let mut covered_hexes = CoveredHexes::default();
        let mut coverage_points = HashMap::new();
        while let Some(heartbeat) = heartbeats.next().await.transpose()? {
            let speedtest_multiplier = speedtests
                .get_average(&heartbeat.hotspot_key)
                .as_ref()
                .map_or(Decimal::ZERO, SpeedtestAverage::reward_multiplier);

            if speedtest_multiplier.is_zero() {
                continue;
            }

            let seniority = hex_streams
                .fetch_seniority(heartbeat.key(), period_end)
                .await?;
            let covered_hex_stream = hex_streams
                .covered_hex_stream(heartbeat.key(), &heartbeat.coverage_object, &seniority)
                .await?;
            covered_hexes
                .aggregate_coverage(&heartbeat.hotspot_key, covered_hex_stream)
                .await?;
            let opt_cbsd_id = heartbeat.key().to_owned().into_cbsd_id();
            coverage_points
                .entry(heartbeat.hotspot_key)
                .or_insert_with(|| HotspotPoints::new(speedtest_multiplier))
                .radio_points
                .insert(
                    opt_cbsd_id,
                    RadioPoints::new(
                        heartbeat.reward_weight,
                        heartbeat.coverage_object,
                        seniority.seniority_ts,
                    ),
                );
        }

        for CoverageReward {
            radio_key,
            points,
            hotspot,
        } in covered_hexes.into_coverage_rewards()
        {
            // Guaranteed that points contains the given hotspot.
            coverage_points
                .get_mut(&hotspot)
                .unwrap()
                .radio_points
                .get_mut(&radio_key.into_cbsd_id())
                .unwrap()
                .points += points;
        }

        Ok(Self { coverage_points })
    }

    /*
    pub fn is_valid(&self, hotspot: &PublicKeyBinary) -> bool {
        if let Some(coverage_points) = self.coverage_points.get(hotspot) {
            !coverage_points.total_points().is_zero()
        } else {
            false
        }
    }
     */

    /// Only used for testing
    pub fn hotspot_points(&self, hotspot: &PublicKeyBinary) -> Decimal {
        self.coverage_points
            .get(hotspot)
            .map(HotspotPoints::total_points)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn total_shares(&self) -> Decimal {
        self.coverage_points
            .values()
            .fold(Decimal::ZERO, |sum, radio_points| {
                sum + radio_points.total_points()
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
        available_poc_rewards
            .checked_div(total_shares)
            .map(|poc_rewards_per_share| {
                let start_period = epoch.start.encode_timestamp();
                let end_period = epoch.end.encode_timestamp();
                self.coverage_points
                    .into_iter()
                    .flat_map(move |(hotspot_key, hotspot_points)| {
                        radio_points_into_rewards(
                            hotspot_key,
                            start_period,
                            end_period,
                            poc_rewards_per_share,
                            hotspot_points.speedtest_multiplier,
                            hotspot_points.radio_points.into_iter(),
                        )
                    })
                    .filter(|mobile_reward| match mobile_reward.reward {
                        Some(proto::mobile_reward_share::Reward::RadioReward(ref radio_reward)) => {
                            radio_reward.poc_reward > 0
                        }
                        _ => false,
                    })
            })
    }
}

fn radio_points_into_rewards(
    hotspot_key: PublicKeyBinary,
    start_period: u64,
    end_period: u64,
    poc_rewards_per_share: Decimal,
    speedtest_multiplier: Decimal,
    radio_points: impl Iterator<Item = (Option<String>, RadioPoints)>,
) -> impl Iterator<Item = proto::MobileRewardShare> {
    radio_points.map(move |(cbsd_id, radio_points)| {
        new_radio_reward(
            cbsd_id,
            &hotspot_key,
            start_period,
            end_period,
            poc_rewards_per_share,
            speedtest_multiplier,
            radio_points,
        )
    })
}

fn new_radio_reward(
    cbsd_id: Option<String>,
    hotspot_key: &PublicKeyBinary,
    start_period: u64,
    end_period: u64,
    poc_rewards_per_share: Decimal,
    speedtest_multiplier: Decimal,
    radio_points: RadioPoints,
) -> proto::MobileRewardShare {
    let poc_reward = poc_rewards_per_share
        * speedtest_multiplier
        * radio_points.heartbeat_multiplier
        * radio_points.points;
    let hotspot_key: Vec<u8> = hotspot_key.clone().into();
    let cbsd_id = cbsd_id.unwrap_or_default();
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
                coverage_points: radio_points.points.to_u64().unwrap_or(0),
                seniority_timestamp: radio_points.seniority.encode_timestamp(),
                coverage_object: Vec::from(radio_points.coverage_object.into_bytes()),
                ..Default::default()
            },
        )),
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
        coverage::{CoveredHexStream, HexCoverage, Seniority},
        data_session,
        data_session::HotspotDataSession,
        heartbeats::{HeartbeatReward, HeartbeatRow, KeyType, OwnedKeyType},
        speedtests::Speedtest,
        speedtests_average::SpeedtestAverage,
        subscriber_location::SubscriberValidatedLocations,
    };
    use chrono::{Duration, Utc};
    use file_store::speedtest::CellSpeedtest;
    use futures::stream::{self, BoxStream};
    use helium_proto::services::poc_mobile::mobile_reward_share::Reward as MobileReward;
    use prost::Message;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn ensure_correct_conversion_of_bytes_to_bones() {
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

    /// Test to ensure that the correct data transfer amount is rewarded.
    #[tokio::test]
    async fn ensure_data_correct_transfer_reward_amount() {
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

        let now = Utc::now();
        let epoch = (now - Duration::hours(1))..now;
        let total_rewards = get_scheduled_tokens_for_poc_and_dc(epoch.end - epoch.start);

        // confirm our hourly rewards add up to expected 24hr amount
        // total_rewards will be in bones
        assert_eq!(
            (total_rewards / dec!(1_000_000) * dec!(24)).trunc(),
            dec!(49_180_327)
        );

        let data_transfer_rewards =
            TransferRewards::from_transfer_sessions(dec!(1.0), data_transfer_map, &epoch).await;

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

    /// Test to ensure that excess transfer rewards are properly scaled down.
    #[tokio::test]
    async fn ensure_excess_transfer_rewards_scale() {
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

        let data_transfer_rewards = TransferRewards::from_transfer_sessions(
            dec!(1.0),
            aggregated_data_transfer_sessions,
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

    #[async_trait::async_trait]
    impl CoveredHexStream for HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> {
        async fn covered_hex_stream<'a>(
            &'a self,
            key: KeyType<'a>,
            coverage_obj: &'a Uuid,
            _seniority: &'a Seniority,
        ) -> Result<BoxStream<'a, Result<HexCoverage, sqlx::Error>>, sqlx::Error> {
            Ok(
                stream::iter(self.get(&(key.to_owned(), *coverage_obj)).unwrap().clone())
                    .map(Ok)
                    .boxed(),
            )
        }
        async fn fetch_seniority(
            &self,
            _key: KeyType<'_>,
            _period_end: DateTime<Utc>,
        ) -> Result<Seniority, sqlx::Error> {
            Ok(Seniority {
                uuid: Uuid::new_v4(),
                seniority_ts: DateTime::default(),
                last_heartbeat: DateTime::default(),
                inserted_at: DateTime::default(),
                update_reason: 0,
            })
        }
    }

    /// Test to ensure that a hotspot with radios that have higher heartbeat multipliers
    /// will receive more rewards than a hotspot with a lower heartbeat multiplier.
    #[tokio::test]
    async fn ensure_correct_radio_weights() {
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

        let max_asserted_distance_deviation: u32 = 300;

        let c1 = "P27-SCE4255W2107CW5000014".to_string();
        let c2 = "2AG32PBS3101S1202000464223GY0153".to_string();
        let c3 = "P27-SCE4255W2107CW5000016".to_string();
        let c4 = "P27-SCE4255W2107CW5000018".to_string();

        let cov_obj_1 = Uuid::new_v4();
        let cov_obj_2 = Uuid::new_v4();
        let cov_obj_3 = Uuid::new_v4();
        let cov_obj_4 = Uuid::new_v4();
        let cov_obj_5 = Uuid::new_v4();
        let cov_obj_6 = Uuid::new_v4();
        let cov_obj_7 = Uuid::new_v4();

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
                coverage_object: cov_obj_1,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: g1.clone(),
                coverage_object: cov_obj_2,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: c2ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c3.clone()),
                hotspot_key: g2.clone(),
                coverage_object: cov_obj_3,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c4.clone()),
                hotspot_key: g2.clone(),
                coverage_object: cov_obj_4,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: c1ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g3.clone(),
                coverage_object: cov_obj_5,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: g3ct,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g4.clone(),
                coverage_object: cov_obj_6,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: g4ct,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: g5.clone(),
                coverage_object: cov_obj_7,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: g5ct,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(100000),
            },
        ];
        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(|row| HeartbeatReward::from_heartbeat_row(row, max_asserted_distance_deviation))
            .collect();

        let mut hex_coverage = HashMap::new();
        hex_coverage.insert(
            (OwnedKeyType::from(c1.clone()), cov_obj_1),
            simple_hex_coverage(&c1, 0x8a1fb46692dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), cov_obj_2),
            simple_hex_coverage(&c2, 0x8a1fb46522dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c3.clone()), cov_obj_3),
            simple_hex_coverage(&c3, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c4.clone()), cov_obj_4),
            simple_hex_coverage(&c4, 0x8a1fb46632dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(g3.clone()), cov_obj_5),
            simple_hex_coverage(&g3, 0x8a1fb46662dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(g4.clone()), cov_obj_6),
            simple_hex_coverage(&g4, 0x8a1fb46522dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(g5.clone()), cov_obj_7),
            simple_hex_coverage(&g5, 0x8a1fb46682dffff),
        );

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

        let rewards = CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            // Field isn't used:
            DateTime::<Utc>::MIN_UTC,
        )
        .await
        .unwrap();

        let gw1_shares = rewards
            .coverage_points
            .get(&g1)
            .expect("Could not fetch gateway1 shares")
            .total_points();
        let gw2_shares = rewards
            .coverage_points
            .get(&g2)
            .expect("Could not fetch gateway1 shares")
            .total_points();
        let gw3_shares = rewards
            .coverage_points
            .get(&g3)
            .expect("Could not fetch gateway3 shares")
            .total_points();
        let gw4_shares = rewards
            .coverage_points
            .get(&g4)
            .expect("Could not fetch gateway4 shares")
            .total_points();
        let gw5_shares = rewards
            .coverage_points
            .get(&g5)
            .expect("Could not fetch gateway5 shares")
            .total_points();

        // For the following assertions, we multiply each expected points
        // by four, as that is the amount of coverage points given to an outdoor
        // radio with a low signal level.

        // The owner with two hotspots gets more rewards
        assert_eq!(gw1_shares, dec!(3.50) * dec!(4));
        assert_eq!(gw2_shares, dec!(2.00) * dec!(4));
        assert!(gw1_shares > gw2_shares);

        // gw3 has wifi HBs and has location validation timestamp
        // gets the full 0.4 reward weight
        assert_eq!(gw3_shares, dec!(0.40) * dec!(4));
        // gw4 has wifi HBs and DOES NOT have a location validation timestamp
        // gets 0.25 of the full reward weight
        assert_eq!(gw4_shares, dec!(0.1) * dec!(4));
        // gw4 has wifi HBs and does have a location validation timestamp
        // but the HB distance is too far from the asserted location
        // gets 0.25 of the full reward weight
        assert_eq!(gw5_shares, dec!(0.1) * dec!(4));
    }

    fn simple_hex_coverage<'a>(key: impl Into<KeyType<'a>>, hex: u64) -> Vec<HexCoverage> {
        let key = key.into();
        let radio_key = key.to_owned();
        vec![HexCoverage {
            uuid: Uuid::new_v4(),
            hex: hex as i64,
            indoor: false,
            radio_key,
            signal_level: crate::coverage::SignalLevel::Low,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
            inserted_at: DateTime::<Utc>::MIN_UTC,
        }]
    }

    /// Test to ensure that different speedtest averages correctly afferct reward shares.
    #[tokio::test]
    async fn ensure_speedtest_averages_affect_reward_shares() {
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
        let owner7: PublicKeyBinary = "112WnYhq4qX3wdw6JTZT3w3A9FNGxeescJwJffcBN5jiZvovWRkQ"
            .parse()
            .expect("failed owner7 parse");

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
        let gw11: PublicKeyBinary = "112WnYhq4qX3wdw6JTZT3w3A9FNGxeescJwJffcBN5jiZvovWRkQ"
            .parse()
            .expect("failed gw11 parse");

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
        owners.insert(gw11.clone(), owner7.clone());

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

        let cov_obj_2 = Uuid::new_v4();
        let cov_obj_4 = Uuid::new_v4();
        let cov_obj_5 = Uuid::new_v4();
        let cov_obj_6 = Uuid::new_v4();
        let cov_obj_7 = Uuid::new_v4();
        let cov_obj_8 = Uuid::new_v4();
        let cov_obj_9 = Uuid::new_v4();
        let cov_obj_10 = Uuid::new_v4();
        let cov_obj_11 = Uuid::new_v4();
        let cov_obj_12 = Uuid::new_v4();
        let cov_obj_13 = Uuid::new_v4();
        let cov_obj_14 = Uuid::new_v4();
        let cov_obj_15 = Uuid::new_v4();
        let cov_obj_16 = Uuid::new_v4();
        let cov_obj_17 = Uuid::new_v4();

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);
        let max_asserted_distance_deviation: u32 = 300;

        // setup heartbeats
        let heartbeat_keys = vec![
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                coverage_object: cov_obj_2,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c4.clone()),
                hotspot_key: gw3.clone(),
                coverage_object: cov_obj_4,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c4).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c5.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_5,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c5).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c6.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_6,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c6).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c7.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_7,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c7).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c8.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_8,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c8).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c9.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_9,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c9).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c10.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_10,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c10).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c11.clone()),
                hotspot_key: gw4.clone(),
                coverage_object: cov_obj_11,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c11).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c12.clone()),
                hotspot_key: gw5.clone(),
                coverage_object: cov_obj_12,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c12).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c13.clone()),
                hotspot_key: gw6.clone(),
                coverage_object: cov_obj_13,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c13).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: Some(c14.clone()),
                hotspot_key: gw7.clone(),
                coverage_object: cov_obj_14,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c14).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw9.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_15,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw10.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_16,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw11.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: cov_obj_17,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(10000),
            },
        ];

        // Setup hex coverages
        let mut hex_coverage = HashMap::new();
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), cov_obj_2),
            simple_hex_coverage(&c2, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c4.clone()), cov_obj_4),
            simple_hex_coverage(&c4, 0x8a1fb46632dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c5.clone()), cov_obj_5),
            simple_hex_coverage(&c5, 0x8a1fb46642dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c6.clone()), cov_obj_6),
            simple_hex_coverage(&c6, 0x8a1fb46652dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c7.clone()), cov_obj_7),
            simple_hex_coverage(&c7, 0x8a1fb46662dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c8.clone()), cov_obj_8),
            simple_hex_coverage(&c8, 0x8a1fb46522dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c9.clone()), cov_obj_9),
            simple_hex_coverage(&c9, 0x8a1fb46682dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c10.clone()), cov_obj_10),
            simple_hex_coverage(&c10, 0x8a1fb46692dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c11.clone()), cov_obj_11),
            simple_hex_coverage(&c11, 0x8a1fb466a2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c12.clone()), cov_obj_12),
            simple_hex_coverage(&c12, 0x8a1fb466b2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c13.clone()), cov_obj_13),
            simple_hex_coverage(&c13, 0x8a1fb466c2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c14.clone()), cov_obj_14),
            simple_hex_coverage(&c14, 0x8a1fb466d2dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw9.clone()), cov_obj_15),
            simple_hex_coverage(&gw9, 0x8c2681a30641dff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw10.clone()), cov_obj_16),
            simple_hex_coverage(&gw10, 0x8c2681a3065d3ff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw11.clone()), cov_obj_17),
            simple_hex_coverage(&gw11, 0x8c2681a306607ff),
        );

        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(|row| HeartbeatReward::from_heartbeat_row(row, max_asserted_distance_deviation))
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
            acceptable_speedtest(gw9.clone(), last_speedtest),
            acceptable_speedtest(gw9.clone(), timestamp),
        ];
        let gw10_speedtests = vec![
            acceptable_speedtest(gw10.clone(), last_speedtest),
            acceptable_speedtest(gw10.clone(), timestamp),
        ];
        let gw11_speedtests = vec![
            acceptable_speedtest(gw11.clone(), last_speedtest),
            acceptable_speedtest(gw11.clone(), timestamp),
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
        let gw11_average = SpeedtestAverage::from(&gw11_speedtests);
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
        averages.insert(gw11.clone(), gw11_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        // calculate the rewards for the sample group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let epoch = (now - Duration::hours(1))..now;
        for mobile_reward in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            // Field isn't used:
            DateTime::<Utc>::MIN_UTC,
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
            471_075_937_440
        );
        assert_eq!(
            *owner_rewards
                .get(&owner2)
                .expect("Could not fetch owner2 rewards"),
            1_413_227_812_320
        );
        assert_eq!(
            *owner_rewards
                .get(&owner3)
                .expect("Could not fetch owner3 rewards"),
            84_120_703_114
        );
        assert_eq!(owner_rewards.get(&owner4), None);

        let owner5_reward = *owner_rewards
            .get(&owner5)
            .expect("Could not fetch owner5 rewards");
        assert_eq!(owner5_reward, 53_837_249_993);

        let owner6_reward = *owner_rewards
            .get(&owner6)
            .expect("Could not fetch owner6 rewards");
        assert_eq!(owner6_reward, 13_459_312_498);

        // confirm owner 6 reward is 0.25 of owner 5's reward
        // this is due to owner 6's hotspot not having a validation location timestamp
        // and thus its reward scale is reduced
        assert_eq!((owner5_reward as f64 * 0.25) as u64, owner6_reward);

        let owner7_reward = *owner_rewards
            .get(&owner6)
            .expect("Could not fetch owner7 rewards");
        assert_eq!(owner7_reward, 13_459_312_498);

        // confirm owner 7 reward is 0.25 of owner 5's reward
        // owner 7's hotspot does have a validation location timestamp
        // but its distance beyond the asserted location is too high
        // and thus its reward scale is reduced
        assert_eq!((owner5_reward as f64 * 0.25) as u64, owner7_reward);

        // total emissions for 1 hour
        let expected_total_rewards = get_scheduled_tokens_for_poc_and_dc(Duration::hours(1))
            .to_u64()
            .unwrap();
        // the emissions actually distributed for the hour
        let mut distributed_total_rewards = 0;
        for val in owner_rewards.values() {
            distributed_total_rewards += *val
        }
        assert_eq!(distributed_total_rewards, 2_049_180_327_863);

        let diff = expected_total_rewards - distributed_total_rewards;
        // the sum of rewards distributed should not exceed the epoch amount
        // but due to rounding whilst going to u64 when computing rewards,
        // is permitted to be a few bones less
        assert_eq!(diff, 5);
    }

    #[tokio::test]
    async fn full_wifi_indoor_vs_sercomm_indoor_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner2.clone());

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);
        let max_asserted_distance_deviation: u32 = 300;

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // init cells and cell_types
        let c2 = "P27-SCE4255W".to_string(); // sercom indoor

        // setup heartbeats
        let heartbeat_keys = vec![
            // add wifi indoor HB
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: g1_cov_obj,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1),
            },
            // add sercomm indoor HB
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                coverage_object: g2_cov_obj,
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
        ];

        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(|row| HeartbeatReward::from_heartbeat_row(row, max_asserted_distance_deviation))
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

        let gw1_average = SpeedtestAverage::from(&gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(&gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };
        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), g2_cov_obj),
            simple_hex_coverage(&c2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        for mobile_reward in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            DateTime::<Utc>::MIN_UTC,
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

        // wifi
        let owner1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");
        assert_eq!(owner1_reward, 585_480_093_676);

        //sercomm
        let owner2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");
        assert_eq!(owner2_reward, 1_463_700_234_192);

        // confirm owner 1 reward is 0.4 of owner 2's reward
        // owner 1 is a wifi indoor with a distance_to_asserted < max
        // and so gets the full reward scale of 0.4
        // owner 2 is a cbrs sercomm indoor which has a reward scale of 1.0
        assert_eq!(owner1_reward, (owner2_reward as f64 * 0.4) as u64);
    }

    #[tokio::test]
    async fn reduced_wifi_indoor_vs_sercomm_indoor_reward_shares() {
        // init owners
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        // init hotspots
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        owners.insert(gw2.clone(), owner2.clone());

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);
        let max_asserted_distance_deviation: u32 = 300;

        // init cells and cell_types
        let c2 = "P27-SCE4255W".to_string(); // sercom indoor

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // setup heartbeats
        let heartbeat_keys = vec![
            // add wifi  indoor HB
            // with distance to asserted > than max allowed
            // this results in reward scale dropping to 0.25
            HeartbeatRow {
                cbsd_id: None,
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiIndoor,
                coverage_object: g1_cov_obj,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                location_validation_timestamp: Some(timestamp),
                distance_to_asserted: Some(1000),
            },
            // add sercomm indoor HB
            HeartbeatRow {
                cbsd_id: Some(c2.clone()),
                hotspot_key: gw2.clone(),
                coverage_object: g2_cov_obj,
                latest_timestamp: DateTime::<Utc>::MIN_UTC,
                cell_type: CellType::from_cbsd_id(&c2).unwrap(),
                location_validation_timestamp: None,
                distance_to_asserted: Some(1),
            },
        ];

        let heartbeat_rewards: Vec<HeartbeatReward> = heartbeat_keys
            .into_iter()
            .map(|row| HeartbeatReward::from_heartbeat_row(row, max_asserted_distance_deviation))
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

        let gw1_average = SpeedtestAverage::from(&gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(&gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(c2.clone()), g2_cov_obj),
            simple_hex_coverage(&c2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        for mobile_reward in CoveragePoints::aggregate_points(
            &hex_coverage,
            stream::iter(heartbeat_rewards).map(Ok),
            &speedtest_avgs,
            DateTime::<Utc>::MIN_UTC,
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

        // wifi
        let owner1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");
        assert_eq!(owner1_reward, 186_289_120_715);

        //sercomm
        let owner2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");
        assert_eq!(owner2_reward, 1_862_891_207_153);

        // confirm owner 1 reward is 0.1 of owner 2's reward
        // owner 1 is a wifi indoor with a distance_to_asserted > max
        // and so gets the reduced reward scale of 0.1 ( radio reward scale of 0.4 * location scale of 0.25)
        // owner 2 is a cbrs sercomm indoor which has a reward scale of 1.0
        assert_eq!(owner1_reward, (owner2_reward as f64 * 0.1) as u64);
    }

    /// Test to ensure that rewards that are zeroed are not written out.
    #[tokio::test]
    async fn ensure_zeroed_rewards_are_not_written() {
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

        let mut coverage_points = HashMap::new();

        coverage_points.insert(
            gw1.clone(),
            HotspotPoints {
                speedtest_multiplier: dec!(1.0),
                radio_points: vec![(
                    Some(c1),
                    RadioPoints {
                        heartbeat_multiplier: dec!(1.0),
                        seniority: DateTime::default(),
                        coverage_object: Uuid::new_v4(),
                        points: dec!(10.0),
                    },
                )]
                .into_iter()
                .collect(),
            },
        );
        coverage_points.insert(
            gw2,
            HotspotPoints {
                speedtest_multiplier: dec!(1.0),
                radio_points: vec![
                    (
                        Some(c2),
                        RadioPoints {
                            heartbeat_multiplier: dec!(1.0),
                            seniority: DateTime::default(),
                            coverage_object: Uuid::new_v4(),
                            points: dec!(-1.0),
                        },
                    ),
                    (
                        Some(c3),
                        RadioPoints {
                            heartbeat_multiplier: dec!(1.0),
                            points: dec!(0.0),
                            seniority: DateTime::default(),
                            coverage_object: Uuid::new_v4(),
                        },
                    ),
                ]
                .into_iter()
                .collect(),
            },
        );

        let now = Utc::now();
        // We should never see any radio shares from owner2, since all of them are
        // less than or equal to zero.
        let coverage_points = CoveragePoints { coverage_points };
        let epoch = now - Duration::hours(1)..now;
        let expected_hotspot = gw1;
        for mobile_reward in coverage_points.into_rewards(Decimal::ZERO, &epoch).unwrap() {
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
        let coverage_points = CoveragePoints {
            coverage_points: HashMap::new(),
        };

        let now = Utc::now();
        let epoch = now - Duration::hours(1)..now;

        assert!(coverage_points
            .into_rewards(Decimal::ZERO, &epoch)
            .is_none());
    }
}
