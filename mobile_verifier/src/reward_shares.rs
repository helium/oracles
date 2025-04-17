use crate::{
    banning::BannedRadios,
    coverage::CoveredHexStream,
    data_session::HotspotMap,
    heartbeats::HeartbeatReward,
    rewarder::boosted_hex_eligibility::BoostedHexEligibility,
    seniority::Seniority,
    speedtests_average::SpeedtestAverages,
    subscriber_mapping_activity::SubscriberMappingShares,
    unique_connections::{self, UniqueConnectionCounts},
    PriceInfo,
};
use chrono::{DateTime, Utc};
use coverage_point_calculator::{
    BytesPs, LocationTrust, OracleBoostingStatus, SPBoostedRewardEligibility, Speedtest,
    SpeedtestTier,
};
use file_store::traits::TimestampEncode;
use futures::{Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    poc_mobile as proto, poc_mobile::mobile_reward_share::Reward as ProtoReward,
};
use mobile_config::{boosted_hex_info::BoostedHexes, sub_dao_epoch_reward_info::EpochRewardInfo};
use radio_reward_v2::{RadioRewardV2Ext, ToProtoDecimal};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::{collections::HashMap, ops::Range};
use uuid::Uuid;

mod radio_reward_v2;

/// Maximum amount of the total emissions pool allocated for data transfer
/// rewards
const MAX_DATA_TRANSFER_REWARDS_PERCENT: Decimal = dec!(0.4);

/// Percentage of total emissions pool allocated for proof of coverage
const POC_REWARDS_PERCENT: Decimal = dec!(0.1);

/// Percentage of total emissions pool allocated for boosted proof of coverage
const BOOSTED_POC_REWARDS_PERCENT: Decimal = dec!(0.1);

/// The fixed price of a mobile data credit
const DC_USD_PRICE: Decimal = dec!(0.00001);

/// Default precision used for rounding
pub const DEFAULT_PREC: u32 = 15;

/// Percent of total emissions allocated for mapper rewards
const MAPPERS_REWARDS_PERCENT: Decimal = dec!(0.2);

// Percent of total emissions allocated for service provider rewards
const SERVICE_PROVIDER_PERCENT: Decimal = dec!(0.1);

// Percent of total emissions allocated for oracles
const ORACLES_PERCENT: Decimal = dec!(0.04);

#[derive(Debug)]
pub struct TransferRewards {
    reward_scale: Decimal,
    rewards: HashMap<PublicKeyBinary, TransferReward>,
    reward_sum: Decimal,
    price_info: PriceInfo,
}

#[derive(Copy, Clone, Debug)]
pub struct TransferReward {
    bones: Decimal,
    bytes_rewarded: u64,
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
        self.rewards
            .get(hotspot)
            .copied()
            .map(|x| x.bones)
            .unwrap_or(Decimal::ZERO)
            * self.reward_scale
    }

    pub fn total(&self) -> Decimal {
        self.rewards
            .values()
            .map(|v| v.bones * self.reward_scale)
            .sum()
    }

    pub async fn from_transfer_sessions(
        price_info: PriceInfo,
        transfer_sessions: HotspotMap,
        reward_shares: &DataTransferAndPocAllocatedRewardBuckets,
    ) -> Self {
        let mut reward_sum = Decimal::ZERO;
        let rewards = transfer_sessions
            .into_iter()
            // Calculate rewards per hotspot
            .map(|(pub_key, rewardable)| {
                let bones = dc_to_hnt_bones(
                    Decimal::from(rewardable.rewardable_dc),
                    price_info.price_per_bone,
                );
                reward_sum += bones;
                (
                    pub_key,
                    TransferReward {
                        bones,
                        bytes_rewarded: rewardable.rewardable_bytes,
                    },
                )
            })
            .collect();

        // If we find that total data_transfer reward sum is greater than the
        // allocated reward shares for data transfer, we recalculate the rewards
        // per share to make them fit.
        let reward_scale = if reward_sum > reward_shares.data_transfer {
            reward_shares.data_transfer / reward_sum
        } else {
            Decimal::ONE
        };

        Self {
            reward_scale,
            rewards,
            reward_sum: reward_sum * reward_scale,
            price_info,
        }
    }

    pub fn into_rewards(
        self,
        reward_info: &'_ EpochRewardInfo,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_ {
        let Self {
            reward_scale,
            rewards,
            ..
        } = self;
        let start_period = reward_info.epoch_period.start.encode_timestamp();
        let end_period = reward_info.epoch_period.end.encode_timestamp();
        let price = self.price_info.price_in_bones;

        rewards
            .into_iter()
            .map(move |(hotspot_key, reward)| {
                let dc_transfer_reward = (reward.bones * reward_scale)
                    .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                    .to_u64()
                    .unwrap_or(0);
                (
                    dc_transfer_reward,
                    proto::MobileRewardShare {
                        start_period,
                        end_period,
                        reward: Some(proto::mobile_reward_share::Reward::GatewayReward(
                            proto::GatewayReward {
                                hotspot_key: hotspot_key.into(),
                                dc_transfer_reward,
                                rewardable_bytes: reward.bytes_rewarded,
                                price,
                            },
                        )),
                    },
                )
            })
            .filter(|(dc_transfer_reward, _mobile_reward)| *dc_transfer_reward > 0)
    }
}

#[derive(Default)]
pub struct MapperShares {
    mapping_activity_shares: Vec<SubscriberMappingShares>,
}

impl MapperShares {
    pub fn new(mapping_activity_shares: Vec<SubscriberMappingShares>) -> Self {
        Self {
            mapping_activity_shares,
        }
    }

    pub fn rewards_per_share(&self, total_mappers_pool: Decimal) -> anyhow::Result<Decimal> {
        let total_shares = self
            .mapping_activity_shares
            .iter()
            .map(|mas| Decimal::from(mas.discovery_reward_shares + mas.verification_reward_shares))
            .sum();

        let res = total_mappers_pool
            .checked_div(total_shares)
            .unwrap_or(Decimal::ZERO);

        Ok(res)
    }

    pub fn into_subscriber_rewards(
        self,
        reward_period: &Range<DateTime<Utc>>,
        reward_per_share: Decimal,
    ) -> impl Iterator<Item = (u64, proto::MobileRewardShare)> + '_ {
        self.mapping_activity_shares.into_iter().map(move |mas| {
            let discovery_location_amount = (Decimal::from(mas.discovery_reward_shares)
                * reward_per_share)
                .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                .to_u64()
                .unwrap_or_default();

            let verification_mapping_amount = (Decimal::from(mas.verification_reward_shares)
                * reward_per_share)
                .round_dp_with_strategy(0, RoundingStrategy::ToZero)
                .to_u64()
                .unwrap_or_default();

            (
                discovery_location_amount + verification_mapping_amount,
                proto::MobileRewardShare {
                    start_period: reward_period.start.encode_timestamp(),
                    end_period: reward_period.end.encode_timestamp(),
                    reward: Some(ProtoReward::SubscriberReward(proto::SubscriberReward {
                        subscriber_id: mas.subscriber_id,
                        discovery_location_amount,
                        verification_mapping_amount,
                    })),
                },
            )
        })
    }
}

/// Returns the equivalent amount of Hnt bones for a specified amount of Data Credits
pub fn dc_to_hnt_bones(dc_amount: Decimal, hnt_bone_price: Decimal) -> Decimal {
    let dc_in_usd = dc_amount * DC_USD_PRICE;
    (dc_in_usd / hnt_bone_price)
        .round_dp_with_strategy(DEFAULT_PREC, RoundingStrategy::ToPositiveInfinity)
}

pub fn coverage_point_to_mobile_reward_share(
    coverage_points: coverage_point_calculator::CoveragePoints,
    reward_period: &Range<DateTime<Utc>>,
    radio_id: &RadioId,
    poc_reward: u64,
    rewards_per_share: CalculatedPocRewardShares,
    seniority_timestamp: DateTime<Utc>,
    coverage_object_uuid: Uuid,
) -> (proto::MobileRewardShare, proto::MobileRewardShare) {
    let hotspot_key = radio_id.clone();

    let boosted_hexes = coverage_points
        .covered_hexes
        .iter()
        .filter(|hex| hex.boosted_multiplier.is_some_and(|boost| boost > dec!(1)))
        .map(|covered_hex| proto::BoostedHex {
            location: covered_hex.hex.into_raw(),
            multiplier: covered_hex.boosted_multiplier.unwrap().to_u32().unwrap(),
        })
        .collect();

    let to_proto_value = |value: Decimal| (value * dec!(1000)).to_u32().unwrap_or_default();
    let location_trust_score_multiplier = to_proto_value(coverage_points.location_trust_multiplier);
    let speedtest_multiplier = to_proto_value(coverage_points.speedtest_multiplier);

    let coverage_points_v1 = coverage_points
        .coverage_points_v1()
        .to_u64()
        .unwrap_or_default();

    let coverage_object = Vec::from(coverage_object_uuid.into_bytes());

    let radio_reward_v1 = proto::mobile_reward_share::Reward::RadioReward(proto::RadioReward {
        hotspot_key: hotspot_key.clone().into(),
        cbsd_id: String::default(),
        poc_reward,
        coverage_points: coverage_points_v1,
        seniority_timestamp: seniority_timestamp.encode_timestamp(),
        coverage_object: coverage_object.clone(),
        location_trust_score_multiplier,
        speedtest_multiplier,
        boosted_hexes,
        ..Default::default()
    });

    let radio_reward_v2 = proto::mobile_reward_share::Reward::RadioRewardV2(proto::RadioRewardV2 {
        hotspot_key: hotspot_key.into(),
        cbsd_id: String::default(),
        base_coverage_points_sum: Some(coverage_points.coverage_points.base.proto_decimal()),
        boosted_coverage_points_sum: Some(coverage_points.coverage_points.boosted.proto_decimal()),
        base_reward_shares: Some(coverage_points.total_base_shares().proto_decimal()),
        boosted_reward_shares: Some(coverage_points.total_boosted_shares().proto_decimal()),
        base_poc_reward: rewards_per_share.base_poc_reward(&coverage_points),
        boosted_poc_reward: rewards_per_share.boosted_poc_reward(&coverage_points),
        seniority_timestamp: seniority_timestamp.encode_timestamp(),
        coverage_object,
        location_trust_scores: coverage_points.proto_location_trust_scores(),
        location_trust_score_multiplier: Some(
            coverage_points.location_trust_multiplier.proto_decimal(),
        ),
        speedtests: coverage_points.proto_speedtests(),
        speedtest_multiplier: Some(coverage_points.speedtest_multiplier.proto_decimal()),
        sp_boosted_hex_status: coverage_points.proto_sp_boosted_hex_status().into(),
        oracle_boosted_hex_status: coverage_points.proto_oracle_boosted_hex_status().into(),
        covered_hexes: coverage_points.proto_covered_hexes(),
        speedtest_average: Some(coverage_points.proto_speedtest_avg()),
    });

    let base = proto::MobileRewardShare {
        start_period: reward_period.start.encode_timestamp(),
        end_period: reward_period.end.encode_timestamp(),
        reward: None,
    };

    (
        proto::MobileRewardShare {
            reward: Some(radio_reward_v1),
            ..base.clone()
        },
        proto::MobileRewardShare {
            reward: Some(radio_reward_v2),
            ..base
        },
    )
}

type RadioId = PublicKeyBinary;

#[derive(Debug, Clone)]
struct RadioInfo {
    radio_type: coverage_point_calculator::RadioType,
    coverage_obj_uuid: Uuid,
    seniority: Seniority,
    trust_scores: Vec<coverage_point_calculator::LocationTrust>,
    sp_boosted_reward_eligibility: SPBoostedRewardEligibility,
    speedtests: Vec<coverage_point_calculator::Speedtest>,
    oracle_boosting_status: OracleBoostingStatus,
}

#[derive(Debug)]
pub struct CoverageShares {
    coverage_map: coverage_map::CoverageMap,
    radio_infos: HashMap<RadioId, RadioInfo>,
}

impl CoverageShares {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        hex_streams: &impl CoveredHexStream,
        heartbeats: impl Stream<Item = Result<HeartbeatReward, sqlx::Error>>,
        speedtest_averages: &SpeedtestAverages,
        boosted_hexes: &BoostedHexes,
        boosted_hex_eligibility: &BoostedHexEligibility,
        banned_radios: &BannedRadios,
        unique_connections: &UniqueConnectionCounts,
        reward_period: &Range<DateTime<Utc>>,
    ) -> anyhow::Result<Self> {
        let mut radio_infos: HashMap<RadioId, RadioInfo> = HashMap::new();
        let mut coverage_map_builder = coverage_map::CoverageMapBuilder::default();

        // The heartbearts query is written in a way that each radio is iterated a single time.
        let mut heartbeats = std::pin::pin!(heartbeats);
        while let Some(heartbeat) = heartbeats.next().await.transpose()? {
            let pubkey = heartbeat.hotspot_key.clone();
            let heartbeat_key = heartbeat.key();
            let key = pubkey.clone();

            if banned_radios.is_poc_banned(&pubkey) {
                tracing::trace!(%pubkey, "ignoring POC banned radio");
                continue;
            }

            let seniority = hex_streams
                .fetch_seniority(heartbeat_key, reward_period.end)
                .await?;

            let trust_scores: Vec<LocationTrust> = heartbeat
                .iter_distances_and_scores()
                .map(|(distance, trust_score)| LocationTrust {
                    meters_to_asserted: distance as u32,
                    trust_score,
                })
                .collect();

            let speedtests = match speedtest_averages.get_average(&pubkey) {
                Some(avg) => avg.speedtests,
                None => vec![],
            };
            let speedtests: Vec<Speedtest> = speedtests
                .iter()
                .map(|test| Speedtest {
                    upload_speed: BytesPs::new(test.report.upload_speed),
                    download_speed: BytesPs::new(test.report.download_speed),
                    latency_millis: test.report.latency,
                    timestamp: test.report.timestamp,
                })
                .collect();

            let (is_indoor, covered_hexes) = {
                let mut is_indoor = false;

                let covered_hexes_stream = hex_streams
                    .covered_hex_stream(heartbeat_key, &heartbeat.coverage_object, &seniority)
                    .await?;

                let mut covered_hexes = vec![];
                let mut covered_hexes_stream = std::pin::pin!(covered_hexes_stream);
                while let Some(hex_coverage) = covered_hexes_stream.next().await.transpose()? {
                    is_indoor = hex_coverage.indoor;
                    covered_hexes.push(coverage_map::UnrankedCoverage {
                        location: hex_coverage.hex,
                        signal_power: hex_coverage.signal_power,
                        signal_level: hex_coverage.signal_level.into(),
                        assignments: hex_coverage.assignments,
                    });
                }
                (is_indoor, covered_hexes)
            };

            use coverage_point_calculator::RadioType;
            let radio_type = match is_indoor {
                true => RadioType::IndoorWifi,
                false => RadioType::OutdoorWifi,
            };

            let oracle_boosting_status =
                if unique_connections::is_qualified(unique_connections, &pubkey) {
                    OracleBoostingStatus::Qualified
                } else if banned_radios.is_sp_banned(&pubkey) {
                    OracleBoostingStatus::Banned
                } else {
                    OracleBoostingStatus::Eligible
                };

            let sp_boosted_reward_eligibility =
                boosted_hex_eligibility.eligibility(pubkey.clone(), &covered_hexes);

            if eligible_for_coverage_map(oracle_boosting_status, &speedtests, &trust_scores) {
                coverage_map_builder.insert_coverage_object(coverage_map::CoverageObject {
                    indoor: is_indoor,
                    hotspot_key: pubkey.into(),
                    seniority_timestamp: seniority.seniority_ts,
                    coverage: covered_hexes,
                });
            }

            radio_infos.insert(
                key,
                RadioInfo {
                    radio_type,
                    coverage_obj_uuid: heartbeat.coverage_object,
                    seniority,
                    trust_scores,
                    sp_boosted_reward_eligibility,
                    speedtests,
                    oracle_boosting_status,
                },
            );
        }

        let coverage_map = coverage_map_builder.build(boosted_hexes, reward_period.start);

        Ok(Self {
            coverage_map,
            radio_infos,
        })
    }

    fn coverage_points(
        &self,
        radio_id: &RadioId,
    ) -> anyhow::Result<coverage_point_calculator::CoveragePoints> {
        let radio_info = self.radio_infos.get(radio_id).unwrap();

        let hexes = {
            let pubkey = radio_id;
            let ranked_coverage = self.coverage_map.get_wifi_coverage(pubkey.as_ref());
            ranked_coverage.to_vec()
        };

        let coverage_points = coverage_point_calculator::CoveragePoints::new(
            radio_info.radio_type,
            radio_info.sp_boosted_reward_eligibility,
            radio_info.speedtests.clone(),
            radio_info.trust_scores.clone(),
            hexes,
            radio_info.oracle_boosting_status,
        )?;

        Ok(coverage_points)
    }

    pub fn into_rewards(
        self,
        reward_shares: DataTransferAndPocAllocatedRewardBuckets,
        reward_period: &Range<DateTime<Utc>>,
    ) -> Option<(
        CalculatedPocRewardShares,
        impl Iterator<Item = (u64, proto::MobileRewardShare, proto::MobileRewardShare)> + '_,
    )> {
        struct ProcessedRadio {
            radio_id: RadioId,
            points: coverage_point_calculator::CoveragePoints,
            seniority: Seniority,
            coverage_obj_uuid: Uuid,
        }

        let mut processed_radios = vec![];
        for (radio_id, radio_info) in self.radio_infos.iter() {
            let points = match self.coverage_points(radio_id) {
                Ok(points) => points,
                Err(err) => {
                    tracing::error!(
                        pubkey = radio_id.to_string(),
                        ?err,
                        "could not reward radio"
                    );
                    continue;
                }
            };

            processed_radios.push(ProcessedRadio {
                radio_id: radio_id.clone(),
                points,
                seniority: radio_info.seniority.clone(),
                coverage_obj_uuid: radio_info.coverage_obj_uuid,
            });
        }

        let Some(rewards_per_share) = CalculatedPocRewardShares::new(
            reward_shares,
            processed_radios.iter().map(|radio| &radio.points),
        ) else {
            tracing::info!(?reward_period, "could not calculate reward shares");
            return None;
        };

        Some((
            rewards_per_share,
            processed_radios
                .into_iter()
                .map(move |radio| {
                    let ProcessedRadio {
                        radio_id,
                        points,
                        seniority,
                        coverage_obj_uuid,
                    } = radio;

                    let poc_reward = rewards_per_share.poc_reward(&points);
                    let (mobile_reward_v1, mobile_reward_v2) =
                        coverage_point_to_mobile_reward_share(
                            points,
                            reward_period,
                            &radio_id,
                            poc_reward,
                            rewards_per_share,
                            seniority.seniority_ts,
                            coverage_obj_uuid,
                        );
                    (poc_reward, mobile_reward_v1, mobile_reward_v2)
                })
                .filter(|(poc_reward, _mobile_reward_v1, _mobile_reward_v2)| *poc_reward > 0),
        ))
    }

    /// Only used for testing
    pub fn test_hotspot_reward_shares(&self, hotspot: &RadioId) -> Decimal {
        self.coverage_points(hotspot)
            .expect("reward shares for hotspot")
            .total_shares()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DataTransferAndPocAllocatedRewardBuckets {
    pub data_transfer: Decimal,
    pub poc: Decimal,
    pub boosted_poc: Decimal,
}

impl DataTransferAndPocAllocatedRewardBuckets {
    pub fn new(total_emission_pool: Decimal) -> Self {
        Self {
            data_transfer: total_emission_pool * MAX_DATA_TRANSFER_REWARDS_PERCENT,
            poc: total_emission_pool * POC_REWARDS_PERCENT,
            boosted_poc: total_emission_pool * BOOSTED_POC_REWARDS_PERCENT,
        }
    }

    pub fn total_poc(&self) -> Decimal {
        self.poc + self.boosted_poc
    }

    /// Rewards left over from Data Transfer rewards go into the POC pool. They
    /// do not get considered for boosted POC rewards if the boost shares
    /// surpasses the 10% allocation.
    pub fn handle_unallocated_data_transfer(&mut self, unallocated_data_transfer: Decimal) {
        self.poc += unallocated_data_transfer;
    }
}

/// Given the rewards allocated for Proof of Coverage and all the radios
/// participating in an epoch, we can calculate the rewards per share for a
/// radio.
///
/// Reference:
/// HIP 122: Amend Service Provider Hex Boosting
/// https://github.com/helium/HIP/blob/main/0122-amend-service-provider-hex-boosting.md
#[derive(Copy, Clone, Debug, Default)]
pub struct CalculatedPocRewardShares {
    pub(crate) normal: Decimal,
    pub(crate) boost: Decimal,
}

impl CalculatedPocRewardShares {
    fn new<'a>(
        allocated_rewards: DataTransferAndPocAllocatedRewardBuckets,
        radios: impl Iterator<Item = &'a coverage_point_calculator::CoveragePoints>,
    ) -> Option<Self> {
        let (total_points, boost_points, poc_points) = radios.fold(
            (dec!(0), dec!(0), dec!(0)),
            |(total, boosted, poc), radio| {
                (
                    total + radio.total_shares(),
                    boosted + radio.total_boosted_shares(),
                    poc + radio.total_base_shares(),
                )
            },
        );

        if total_points.is_zero() {
            return None;
        }

        let shares_per_point = allocated_rewards.total_poc() / total_points;
        let boost_within_limit = allocated_rewards.boosted_poc >= shares_per_point * boost_points;

        if boost_within_limit {
            Some(CalculatedPocRewardShares {
                normal: shares_per_point,
                boost: shares_per_point,
            })
        } else {
            // Over boosted reward limit, need to calculate 2 share ratios
            let normal = allocated_rewards.poc / poc_points;
            let boost = allocated_rewards.boosted_poc / boost_points;

            Some(CalculatedPocRewardShares { normal, boost })
        }
    }

    fn base_poc_reward(&self, points: &coverage_point_calculator::CoveragePoints) -> u64 {
        (self.normal * points.total_base_shares())
            .to_u64()
            .unwrap_or_default()
    }

    fn boosted_poc_reward(&self, points: &coverage_point_calculator::CoveragePoints) -> u64 {
        (self.boost * points.total_boosted_shares())
            .to_u64()
            .unwrap_or_default()
    }

    fn poc_reward(&self, points: &coverage_point_calculator::CoveragePoints) -> u64 {
        self.base_poc_reward(points) + self.boosted_poc_reward(points)
    }
}

pub fn get_scheduled_tokens_for_poc(total_emission_pool: Decimal) -> Decimal {
    let poc_percent =
        MAX_DATA_TRANSFER_REWARDS_PERCENT + POC_REWARDS_PERCENT + BOOSTED_POC_REWARDS_PERCENT;
    total_emission_pool * poc_percent
}

pub fn get_scheduled_tokens_for_mappers(total_emission_pool: Decimal) -> Decimal {
    total_emission_pool * MAPPERS_REWARDS_PERCENT
}

pub fn get_scheduled_tokens_for_service_providers(total_emission_pool: Decimal) -> Decimal {
    total_emission_pool * SERVICE_PROVIDER_PERCENT
}

pub fn get_scheduled_tokens_for_oracles(total_emission_pool: Decimal) -> Decimal {
    total_emission_pool * ORACLES_PERCENT
}

fn eligible_for_coverage_map(
    oracle_boosting_status: OracleBoostingStatus,
    speedtests: &[Speedtest],
    trust_scores: &[LocationTrust],
) -> bool {
    if oracle_boosting_status == OracleBoostingStatus::Banned {
        return false;
    }

    let avg_speedtest = Speedtest::avg(speedtests);
    if avg_speedtest.tier() == SpeedtestTier::Fail {
        return false;
    }

    match coverage_point_calculator::location::multiplier(trust_scores) {
        Ok(multiplier) => {
            if multiplier <= dec!(0.0) {
                return false;
            }
        }
        Err(e) => {
            tracing::error!(?e, "multiplier calculation failed");
            return false;
        }
    }
    true
}

#[cfg(test)]
mod test {

    use super::*;
    use hex_assignments::{assignment::HexAssignments, Assignment};

    use crate::{
        cell_type::CellType,
        coverage::{CoveredHexStream, HexCoverage},
        data_session::{self, HotspotDataSession, HotspotReward},
        heartbeats::{HeartbeatReward, KeyType, OwnedKeyType},
        reward_shares,
        service_provider::{
            self, ServiceProviderDCSessions, ServiceProviderPromotions, ServiceProviderRewardInfos,
        },
        speedtests::Speedtest,
        speedtests_average::SpeedtestAverage,
    };
    use chrono::{Duration, Utc};
    use file_store::speedtest::CellSpeedtest;
    use futures::stream::{self, BoxStream};
    use helium_lib::token::Token;
    use helium_proto::{
        services::poc_mobile::mobile_reward_share::Reward as MobileReward, ServiceProvider,
    };
    use hextree::Cell;
    use prost::Message;
    use std::collections::HashMap;
    use uuid::Uuid;

    const EPOCH_ADDRESS: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
    const SUB_DAO_ADDRESS: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

    const EMISSIONS_POOL_IN_BONES_24_HOURS: u64 = 82_191_780_821_917;
    const EMISSIONS_POOL_IN_BONES_1_HOUR: u64 = 3_424_657_534_247;

    fn new_poc_only(total_emission_pool: Decimal) -> DataTransferAndPocAllocatedRewardBuckets {
        let poc = total_emission_pool * POC_REWARDS_PERCENT;
        let data_transfer = total_emission_pool * MAX_DATA_TRANSFER_REWARDS_PERCENT;

        DataTransferAndPocAllocatedRewardBuckets {
            data_transfer: dec!(0),
            poc: poc + data_transfer,
            boosted_poc: total_emission_pool * BOOSTED_POC_REWARDS_PERCENT,
        }
    }

    fn hex_assignments_mock() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::A,
            urbanized: Assignment::A,
            landtype: Assignment::A,
            service_provider_override: Assignment::C,
        }
    }

    fn bad_hex_assignments_mock() -> HexAssignments {
        HexAssignments {
            footfall: Assignment::C,
            urbanized: Assignment::C,
            landtype: Assignment::C,
            service_provider_override: Assignment::C,
        }
    }

    #[test]
    fn ensure_correct_conversion_of_bytes_to_bones() {
        assert_eq!(dc_to_hnt_bones(Decimal::from(1), dec!(1.0)), dec!(0.00001));
        assert_eq!(dc_to_hnt_bones(Decimal::from(2), dec!(1.0)), dec!(0.00002));
    }

    fn hnt_bones_to_dc(hnt_bones_amount: Decimal, hnt_bones_price: Decimal) -> Decimal {
        let hnt_value = hnt_bones_amount * hnt_bones_price;
        (hnt_value / DC_USD_PRICE).round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity)
    }

    fn rewards_info_1_hour() -> EpochRewardInfo {
        let now = Utc::now();
        let epoch_duration = Duration::hours(1);
        EpochRewardInfo {
            epoch_day: 1,
            epoch_address: EPOCH_ADDRESS.into(),
            sub_dao_address: SUB_DAO_ADDRESS.into(),
            epoch_period: (now - epoch_duration)..now,
            epoch_emissions: Decimal::from(EMISSIONS_POOL_IN_BONES_1_HOUR),
            rewards_issued_at: now,
        }
    }

    fn rewards_info_24_hours() -> EpochRewardInfo {
        let now = Utc::now();
        let epoch_duration = Duration::hours(1);
        EpochRewardInfo {
            epoch_day: 1,
            epoch_address: EPOCH_ADDRESS.into(),
            sub_dao_address: SUB_DAO_ADDRESS.into(),
            epoch_period: (now - epoch_duration)..now,
            epoch_emissions: Decimal::from(EMISSIONS_POOL_IN_BONES_24_HOURS),
            rewards_issued_at: now,
        }
    }

    fn default_price_info() -> PriceInfo {
        PriceInfo::new(10000000000000000, Token::Hnt.decimals())
    }

    #[test]
    fn test_poc_scheduled_tokens() {
        let v = get_scheduled_tokens_for_poc(dec!(100));
        assert_eq!(dec!(60), v, "poc gets 60%");
    }

    #[test]
    fn test_mappers_scheduled_tokens() {
        let v = get_scheduled_tokens_for_mappers(dec!(100));
        assert_eq!(dec!(20), v, "mappers get 20%");
    }

    #[test]
    fn test_service_provider_scheduled_tokens() {
        let v = get_scheduled_tokens_for_service_providers(dec!(100));
        assert_eq!(dec!(10), v, "service providers get 10%");
    }

    #[test]
    fn test_oracles_scheduled_tokens() {
        let v = get_scheduled_tokens_for_oracles(dec!(100));
        assert_eq!(dec!(4), v, "oracles get 4%");
    }

    #[test]
    fn test_price_conversion() {
        let token = Token::Hnt;
        let hnt_dollar_price = dec!(1.0);
        let hnt_price_from_pricer = 100000000_u64;
        let hnt_dollar_bone_price = dec!(0.00000001);

        let hnt_price = PriceInfo::new(hnt_price_from_pricer, token.decimals());

        assert_eq!(hnt_dollar_bone_price, hnt_price.price_per_bone);
        assert_eq!(hnt_price_from_pricer, hnt_price.price_in_bones);
        assert_eq!(hnt_dollar_price, hnt_price.price_per_token);
    }

    #[tokio::test]
    async fn subscriber_rewards() {
        const NUM_SUBSCRIBERS: u64 = 10_000;

        let mut mapping_activity_shares = Vec::new();
        for n in 0..NUM_SUBSCRIBERS {
            mapping_activity_shares.push(SubscriberMappingShares {
                subscriber_id: n.encode_to_vec(),
                discovery_reward_shares: 30,
                verification_reward_shares: 30,
            })
        }

        // set our rewards info
        let rewards_info = rewards_info_24_hours();

        // translate location shares into shares
        let shares = MapperShares::new(mapping_activity_shares);
        let total_mappers_pool =
            reward_shares::get_scheduled_tokens_for_mappers(rewards_info.epoch_emissions);
        let rewards_per_share = shares.rewards_per_share(total_mappers_pool).unwrap();

        // verify total rewards allocated to mappers the epoch
        let total_mapper_rewards = get_scheduled_tokens_for_mappers(rewards_info.epoch_emissions)
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(16_438_356_164_383, total_mapper_rewards);

        let expected_reward_per_subscriber = total_mapper_rewards / NUM_SUBSCRIBERS;

        // get the summed rewards allocated to subscribers for discovery location
        let mut allocated_mapper_rewards = 0_u64;
        for (reward_amount, subscriber_share) in
            shares.into_subscriber_rewards(&rewards_info.epoch_period, rewards_per_share)
        {
            if let Some(MobileReward::SubscriberReward(r)) = subscriber_share.reward {
                assert_eq!(
                    expected_reward_per_subscriber,
                    r.discovery_location_amount + r.verification_mapping_amount
                );
                assert_eq!(
                    reward_amount,
                    r.discovery_location_amount + r.verification_mapping_amount
                );
                // These are the same because we gave `total_reward_points: 30,` for each
                // VerifiedMappingEventShares which is the same amount as discovery mapping
                assert_eq!(821_917_808, r.discovery_location_amount);
                assert_eq!(821_917_808, r.verification_mapping_amount);
                allocated_mapper_rewards += reward_amount;
            }
        }

        // verify the total rewards awarded for discovery mapping
        assert_eq!(16_438_356_160_000, allocated_mapper_rewards);

        // confirm the unallocated service provider reward amounts
        // this should not be more than the total number of subscribers ( 10 k)
        // as we can at max drop one bone per subscriber due to rounding
        let unallocated_mapper_reward_amount = total_mapper_rewards - allocated_mapper_rewards;
        assert_eq!(unallocated_mapper_reward_amount, 4383);
        assert!(unallocated_mapper_reward_amount < NUM_SUBSCRIBERS);
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
            rewardable_bytes: 0,
            num_dcs: 2,
            received_timestamp: DateTime::default(),
            burn_timestamp: Utc::now(),
        };

        let mut data_transfer_map = HotspotMap::new();
        data_transfer_map.insert(
            data_transfer_session.pub_key,
            HotspotReward {
                rewardable_bytes: 0, // Not used
                rewardable_dc: data_transfer_session.num_dcs as u64,
            },
        );

        let rewards_info = rewards_info_1_hour();

        let total_rewards = get_scheduled_tokens_for_poc(rewards_info.epoch_emissions);

        // confirm our hourly rewards add up to expected 24hr amount
        // total_rewards will be in bones
        assert_eq!(
            (total_rewards / dec!(1_000_000) * dec!(24)).trunc(),
            dec!(49_315_068)
        );

        let reward_shares =
            DataTransferAndPocAllocatedRewardBuckets::new(rewards_info.epoch_emissions);

        let price_info = default_price_info();
        assert_eq!(price_info.price_per_token, dec!(100000000));
        assert_eq!(price_info.price_per_bone, dec!(1));

        let data_transfer_rewards =
            TransferRewards::from_transfer_sessions(price_info, data_transfer_map, &reward_shares)
                .await;

        assert_eq!(data_transfer_rewards.reward(&owner), dec!(0.00002));
        assert_eq!(data_transfer_rewards.reward_scale(), dec!(1.0));

        let available_poc_rewards = get_scheduled_tokens_for_poc(rewards_info.epoch_emissions)
            - data_transfer_rewards.reward_sum;

        assert_eq!(
            available_poc_rewards,
            (total_rewards
                - (data_transfer_rewards.reward(&owner) * data_transfer_rewards.reward_scale()))
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
                rewardable_bytes: 0,
                num_dcs: 2222222222222222,
                received_timestamp: DateTime::default(),
                burn_timestamp: Utc::now(),
            }));
        }
        let data_transfer_sessions = stream::iter(transfer_sessions);
        let aggregated_data_transfer_sessions =
            data_session::data_sessions_to_dc(data_transfer_sessions)
                .await
                .unwrap();

        // set our rewards info
        let rewards_info = rewards_info_24_hours();

        let price_info = default_price_info();
        assert_eq!(price_info.price_per_token, dec!(100000000));
        assert_eq!(price_info.price_per_bone, dec!(1));

        let reward_shares =
            DataTransferAndPocAllocatedRewardBuckets::new(rewards_info.epoch_emissions);

        let data_transfer_rewards = TransferRewards::from_transfer_sessions(
            price_info,
            aggregated_data_transfer_sessions,
            &reward_shares,
        )
        .await;

        // We have constructed the data transfer in such a way that they easily exceed the maximum
        // allotted reward amount for data transfer, which is 40% of the daily tokens. We check to
        // ensure that amount of tokens remaining for POC is no less than 20% of the rewards allocated
        // for POC and data transfer (which is 60% of the daily total emissions).
        let available_poc_rewards = get_scheduled_tokens_for_poc(rewards_info.epoch_emissions)
            - data_transfer_rewards.reward_sum;
        assert_eq!(available_poc_rewards.trunc(), dec!(16_438_356_164_383));
        assert_eq!(
            // Rewards are automatically scaled
            data_transfer_rewards.reward(&owner).trunc(),
            dec!(32_876_712_328_766)
        );
        assert_eq!(data_transfer_rewards.reward_scale().round_dp(1), dec!(0.5));
    }

    fn bytes_per_s(mbps: u64) -> u64 {
        mbps * 125000
    }

    fn good_speedtest(pubkey: PublicKeyBinary, timestamp: DateTime<Utc>) -> Speedtest {
        Speedtest {
            report: CellSpeedtest {
                pubkey,
                timestamp,
                upload_speed: bytes_per_s(15),
                download_speed: bytes_per_s(100),
                latency: 1,
                serial: "".to_string(),
            },
        }
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

    fn simple_hex_coverage<'a>(key: impl Into<KeyType<'a>>, hex: u64) -> Vec<HexCoverage> {
        let key = key.into();
        let radio_key = key.to_owned();
        let hex = hex.try_into().expect("valid h3 cell");

        vec![HexCoverage {
            uuid: Uuid::new_v4(),
            hex,
            indoor: true,
            radio_key,
            signal_level: crate::coverage::SignalLevel::Low,
            signal_power: 0,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
            inserted_at: DateTime::<Utc>::MIN_UTC,
            assignments: hex_assignments_mock(),
        }]
    }

    fn bad_hex_coverage<'a>(key: impl Into<KeyType<'a>>, hex: u64) -> Vec<HexCoverage> {
        let key = key.into();
        let radio_key = key.to_owned();
        let hex = hex.try_into().expect("valid h3 cell");

        vec![HexCoverage {
            uuid: Uuid::new_v4(),
            hex,
            indoor: true,
            radio_key,
            signal_level: crate::coverage::SignalLevel::Low,
            signal_power: 0,
            coverage_claim_time: DateTime::<Utc>::MIN_UTC,
            inserted_at: DateTime::<Utc>::MIN_UTC,
            assignments: bad_hex_assignments_mock(),
        }]
    }

    #[tokio::test]
    async fn check_speedtest_avg_in_radio_reward_v2() {
        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let gw1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let mut owners = HashMap::new();
        owners.insert(gw1.clone(), owner1.clone());
        let cov_obj_1 = Uuid::new_v4();

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        let heartbeat_rewards = vec![HeartbeatReward {
            hotspot_key: gw1.clone(),
            coverage_object: cov_obj_1,
            cell_type: CellType::NovaGenericWifiOutdoor,
            distances_to_asserted: None,
            trust_score_multipliers: vec![dec!(1.0)],
        }]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        let mut hex_coverage = HashMap::new();
        hex_coverage.insert(
            (OwnedKeyType::Wifi(gw1.clone()), cov_obj_1),
            simple_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );

        let st1 = Speedtest {
            report: CellSpeedtest {
                pubkey: gw1.clone(),
                timestamp,
                upload_speed: bytes_per_s(10),
                download_speed: bytes_per_s(100),
                latency: 50,
                serial: "".to_string(),
            },
        };
        let st2 = Speedtest {
            report: CellSpeedtest {
                pubkey: gw1.clone(),
                timestamp,
                upload_speed: bytes_per_s(20),
                download_speed: bytes_per_s(200),
                latency: 100,
                serial: "".to_string(),
            },
        };

        let gw1_speedtests = vec![st1, st2];

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        let rewards_info = rewards_info_24_hours();

        let reward_shares = new_poc_only(rewards_info.epoch_emissions);

        let (_reward_amount, _mobile_reward_v1, mobile_reward_v2) = CoverageShares::new(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &BoostedHexEligibility::default(),
            &BannedRadios::default(),
            &UniqueConnectionCounts::default(),
            &rewards_info.epoch_period,
        )
        .await
        .unwrap()
        .into_rewards(reward_shares, &rewards_info.epoch_period)
        .unwrap()
        .1
        .next()
        .unwrap();

        let radio_reward = match mobile_reward_v2.reward {
            Some(MobileReward::RadioRewardV2(radio_reward)) => radio_reward,
            _ => unreachable!(),
        };
        let speedtest_avg = radio_reward.speedtest_average.unwrap();
        assert_eq!(speedtest_avg.upload_speed_bps, bytes_per_s(15));
        assert_eq!(speedtest_avg.download_speed_bps, bytes_per_s(150));
        assert_eq!(speedtest_avg.latency_ms, 75);
    }

    /// Test to ensure that different speedtest averages correctly afferct reward shares.
    #[tokio::test]
    async fn ensure_speedtest_averages_affect_reward_shares() {
        // https://github.com/helium/HIP/blob/main/0119-closing-gaming-loopholes-within-the-mobile-network.md#maximum-asserted-distance-difference
        // Scenario:
        // All gateways are indoors.
        // owner 1: (x1)
        //  gw10(location_trust_score_multipliers: [1], speedtest_multipliers: [1])
        // owner 2: (x1.5)
        //  gw20(location_trust_score_multipliers: [1, 0.25, 0.25], speedtest_multipliers: [1])=0.5
        //  gw21(location_trust_score_multipliers: [1], speedtest_multipliers: [1])
        // owner 3: (x0.75)
        //  gw30(location_trust_score_multipliers: [1], , speedtest_multipliers: [0.75])

        let owner1: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed owner1 parse");
        let owner2: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed owner2 parse");
        let owner3: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed owner3 parse");

        let gw10: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .expect("failed gw1 parse");
        let gw20: PublicKeyBinary = "11sctWiP9r5wDJVuDe1Th4XSL2vaawaLLSQF8f8iokAoMAJHxqp"
            .parse()
            .expect("failed gw2 parse");
        let gw21: PublicKeyBinary = "112DJZiXvZ8FduiWrEi8siE3wJX6hpRjjtwbavyXUDkgutEUSLAE"
            .parse()
            .expect("failed gw3 parse");
        let gw30: PublicKeyBinary = "112p1GbUtRLyfFaJr1XF8fH7yz9cSZ4exbrSpVDeu67DeGb31QUL"
            .parse()
            .expect("failed gw4 parse");

        // link gws to owners
        let mut owners = HashMap::new();
        owners.insert(gw10.clone(), owner1.clone());
        owners.insert(gw20.clone(), owner2.clone());
        owners.insert(gw21.clone(), owner2.clone());
        owners.insert(gw30.clone(), owner3.clone());

        let cov_obj_10 = Uuid::new_v4();
        let cov_obj_20 = Uuid::new_v4();
        let cov_obj_21 = Uuid::new_v4();
        let cov_obj_30 = Uuid::new_v4();

        let now = Utc::now();
        let timestamp = now - Duration::minutes(20);

        let heartbeat_rewards = vec![
            HeartbeatReward {
                hotspot_key: gw10.clone(),
                coverage_object: cov_obj_10,
                cell_type: CellType::NovaGenericWifiIndoor,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                hotspot_key: gw20.clone(),
                coverage_object: cov_obj_20,
                cell_type: CellType::NovaGenericWifiIndoor,
                distances_to_asserted: Some(vec![0, 250, 250]),
                trust_score_multipliers: vec![dec!(1.0), dec!(0.25), dec!(0.25)],
            },
            HeartbeatReward {
                hotspot_key: gw21.clone(),
                coverage_object: cov_obj_21,
                cell_type: CellType::NovaGenericWifiIndoor,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            HeartbeatReward {
                hotspot_key: gw30.clone(),
                coverage_object: cov_obj_30,
                cell_type: CellType::NovaGenericWifiIndoor,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

        // Setup hex coverages
        let mut hex_coverage = HashMap::new();
        hex_coverage.insert(
            (OwnedKeyType::Wifi(gw10.clone()), cov_obj_10),
            simple_hex_coverage(&gw10, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw20.clone()), cov_obj_20),
            simple_hex_coverage(&gw20, 0x8a1fb46632dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw21.clone()), cov_obj_21),
            simple_hex_coverage(&gw21, 0x8a1fb46642dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw30.clone()), cov_obj_30),
            simple_hex_coverage(&gw30, 0x8a1fb46652dffff),
        );

        // setup speedtests
        let last_speedtest = timestamp - Duration::hours(12);
        let gw10_speedtests = vec![
            good_speedtest(gw10.clone(), last_speedtest),
            good_speedtest(gw10.clone(), timestamp),
        ];
        let gw20_speedtests = vec![
            good_speedtest(gw20.clone(), last_speedtest),
            good_speedtest(gw20.clone(), timestamp),
        ];

        let gw21_speedtests = vec![
            good_speedtest(gw21.clone(), last_speedtest),
            good_speedtest(gw21.clone(), timestamp),
        ];

        let gw30_speedtests = vec![
            good_speedtest(gw30.clone(), last_speedtest),
            degraded_speedtest(gw30.clone(), timestamp),
        ];

        let gw10_average = SpeedtestAverage::from(gw10_speedtests);
        let gw20_average = SpeedtestAverage::from(gw20_speedtests);
        let gw21_average = SpeedtestAverage::from(gw21_speedtests);
        let gw30_average = SpeedtestAverage::from(gw30_speedtests);

        let mut averages = HashMap::new();
        averages.insert(gw10.clone(), gw10_average);
        averages.insert(gw20.clone(), gw20_average);
        averages.insert(gw21.clone(), gw21_average);
        averages.insert(gw30.clone(), gw30_average);

        let speedtest_avgs = SpeedtestAverages { averages };

        // calculate the rewards for the sample group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();

        let rewards_info = rewards_info_1_hour();

        let reward_shares = new_poc_only(rewards_info.epoch_emissions);

        let mut allocated_poc_rewards = 0_u64;

        let epoch = (now - Duration::hours(1))..now;
        for (reward_amount, _mobile_reward_v1, mobile_reward_v2) in CoverageShares::new(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &BoostedHexEligibility::default(),
            &BannedRadios::default(),
            &UniqueConnectionCounts::default(),
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(reward_shares, &rewards_info.epoch_period)
        .unwrap()
        .1
        {
            let radio_reward = match mobile_reward_v2.reward {
                Some(MobileReward::RadioRewardV2(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            let base = radio_reward.base_poc_reward;
            let boosted = radio_reward.boosted_poc_reward;
            let poc_reward = base + boosted;
            assert_eq!(reward_amount, poc_reward);

            allocated_poc_rewards += reward_amount;
            *owner_rewards.entry(owner).or_default() += poc_reward;
        }

        let owner_1_reward = *owner_rewards
            .get(&owner1)
            .expect("Could not fetch owner1 rewards");

        let owner_2_reward = *owner_rewards
            .get(&owner2)
            .expect("Could not fetch owner2 rewards");

        let owner_3_reward = *owner_rewards
            .get(&owner3)
            .expect("Could not fetch owner3 rewards");

        assert_eq!((owner_1_reward as f64 * 1.5) as u64, owner_2_reward);
        assert_eq!((owner_1_reward as f64 * 0.75) as u64, owner_3_reward);

        let expected_unallocated = 3;
        assert_eq!(
            allocated_poc_rewards,
            reward_shares.total_poc().to_u64().unwrap() - expected_unallocated
        );
    }

    #[tokio::test]
    async fn qualified_wifi_exempt_from_oracle_boosting_bad() {
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

        let g1_cov_obj = Uuid::new_v4();
        let g2_cov_obj = Uuid::new_v4();

        // setup heartbeats
        let heartbeat_rewards = vec![
            // add qualified wifi indoor HB
            HeartbeatReward {
                hotspot_key: gw1.clone(),
                cell_type: CellType::NovaGenericWifiOutdoor,
                coverage_object: g1_cov_obj,
                distances_to_asserted: Some(vec![0]),
                trust_score_multipliers: vec![dec!(1.0)],
            },
            // add unqualified wifi indoor HB
            HeartbeatReward {
                hotspot_key: gw2.clone(),
                cell_type: CellType::NovaGenericWifiOutdoor,
                coverage_object: g2_cov_obj,
                distances_to_asserted: None,
                trust_score_multipliers: vec![dec!(1.0)],
            },
        ]
        .into_iter()
        .map(Ok)
        .collect::<Vec<Result<HeartbeatReward, _>>>();

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

        let gw1_average = SpeedtestAverage::from(gw1_speedtests);
        let gw2_average = SpeedtestAverage::from(gw2_speedtests);
        let mut averages = HashMap::new();
        averages.insert(gw1.clone(), gw1_average);
        averages.insert(gw2.clone(), gw2_average);

        let speedtest_avgs = SpeedtestAverages { averages };
        let mut hex_coverage: HashMap<(OwnedKeyType, Uuid), Vec<HexCoverage>> = Default::default();
        hex_coverage.insert(
            (OwnedKeyType::from(gw1.clone()), g1_cov_obj),
            bad_hex_coverage(&gw1, 0x8a1fb46622dffff),
        );
        hex_coverage.insert(
            (OwnedKeyType::from(gw2.clone()), g2_cov_obj),
            bad_hex_coverage(&gw2, 0x8a1fb46642dffff),
        );

        // calculate the rewards for the group
        let mut owner_rewards = HashMap::<PublicKeyBinary, u64>::new();
        let duration = Duration::hours(1);
        let epoch = (now - duration)..now;
        let rewards_info = rewards_info_1_hour();

        let reward_shares = new_poc_only(rewards_info.epoch_emissions);
        let unique_connection_counts = HashMap::from([(gw1.clone(), 42)]);
        for (_reward_amount, _mobile_reward_v1, mobile_reward_v2) in CoverageShares::new(
            &hex_coverage,
            stream::iter(heartbeat_rewards),
            &speedtest_avgs,
            &BoostedHexes::default(),
            &BoostedHexEligibility::default(),
            &BannedRadios::default(),
            &unique_connection_counts,
            &epoch,
        )
        .await
        .unwrap()
        .into_rewards(reward_shares, &epoch)
        .unwrap()
        .1
        {
            let radio_reward = match mobile_reward_v2.reward {
                Some(MobileReward::RadioRewardV2(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let owner = owners
                .get(&PublicKeyBinary::from(radio_reward.hotspot_key))
                .expect("Could not find owner")
                .clone();

            let base = radio_reward.base_poc_reward;
            let boosted = radio_reward.boosted_poc_reward;
            *owner_rewards.entry(owner).or_default() += base + boosted;
        }

        // qualified wifi
        let owner1_reward = owner_rewards.get(&owner1);
        assert!(owner1_reward.is_some());

        // unqualified wifi
        let owner2_reward = owner_rewards.get(&owner2);
        assert!(owner2_reward.is_none());
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

        let now = Utc::now();
        // We should never see any radio shares from owner2, since all of them are
        // less than or equal to zero.
        let rewards_info = rewards_info_1_hour();

        let uuid_1 = Uuid::new_v4();
        let uuid_2 = Uuid::new_v4();

        let mut coverage_map = coverage_map::CoverageMapBuilder::default();
        coverage_map.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key: gw1.clone().into(),
            seniority_timestamp: now,
            coverage: vec![coverage_map::UnrankedCoverage {
                location: Cell::from_raw(0x8c2681a3064dbff).expect("valid h3 cell"),
                signal_power: 42,
                signal_level: coverage_map::SignalLevel::High,
                assignments: hex_assignments_mock(),
            }],
        });
        coverage_map.insert_coverage_object(coverage_map::CoverageObject {
            indoor: true,
            hotspot_key: gw2.clone().into(),
            seniority_timestamp: now,
            coverage: vec![coverage_map::UnrankedCoverage {
                location: Cell::from_raw(0x8c2681a3064ddff).expect("valid h3 cell"),
                signal_power: 42,
                signal_level: coverage_map::SignalLevel::High,
                assignments: hex_assignments_mock(),
            }],
        });
        let coverage_map =
            coverage_map.build(&BoostedHexes::default(), rewards_info.epoch_period.start);

        let mut radio_infos = HashMap::new();
        radio_infos.insert(
            gw1.clone(),
            RadioInfo {
                radio_type: coverage_point_calculator::RadioType::IndoorWifi,
                coverage_obj_uuid: uuid_1,
                trust_scores: vec![coverage_point_calculator::LocationTrust {
                    meters_to_asserted: 0,
                    trust_score: dec!(1),
                }],
                seniority: Seniority {
                    uuid: Uuid::new_v4(),
                    seniority_ts: now,
                    last_heartbeat: now,
                    inserted_at: now,
                    update_reason: 0,
                },
                sp_boosted_reward_eligibility: SPBoostedRewardEligibility::Eligible,
                speedtests: vec![
                    coverage_point_calculator::Speedtest {
                        upload_speed: coverage_point_calculator::BytesPs::new(100_000_000),
                        download_speed: coverage_point_calculator::BytesPs::new(100_000_000),
                        latency_millis: 10,
                        timestamp: now,
                    },
                    coverage_point_calculator::Speedtest {
                        upload_speed: coverage_point_calculator::BytesPs::new(100_000_000),
                        download_speed: coverage_point_calculator::BytesPs::new(100_000_000),
                        latency_millis: 10,
                        timestamp: now,
                    },
                ],
                oracle_boosting_status: OracleBoostingStatus::Eligible,
            },
        );
        radio_infos.insert(
            gw2.clone(),
            RadioInfo {
                radio_type: coverage_point_calculator::RadioType::IndoorWifi,
                coverage_obj_uuid: uuid_2,
                trust_scores: vec![coverage_point_calculator::LocationTrust {
                    meters_to_asserted: 0,
                    trust_score: dec!(1),
                }],
                seniority: Seniority {
                    uuid: Uuid::new_v4(),
                    seniority_ts: now,
                    last_heartbeat: now,
                    inserted_at: now,
                    update_reason: 0,
                },
                sp_boosted_reward_eligibility: SPBoostedRewardEligibility::Eligible,
                speedtests: vec![],
                oracle_boosting_status: OracleBoostingStatus::Eligible,
            },
        );

        let coverage_shares = CoverageShares {
            coverage_map,
            radio_infos,
        };

        let reward_shares = new_poc_only(rewards_info.epoch_emissions);
        // gw2 does not have enough speedtests for a multiplier
        let expected_hotspot = gw1;
        for (_reward_amount, _mobile_reward_v1, mobile_reward_v2) in coverage_shares
            .into_rewards(reward_shares, &rewards_info.epoch_period)
            .expect("rewards output")
            .1
        {
            let radio_reward = match mobile_reward_v2.reward {
                Some(MobileReward::RadioRewardV2(radio_reward)) => radio_reward,
                _ => unreachable!(),
            };
            let actual_hotspot = PublicKeyBinary::from(radio_reward.hotspot_key);
            assert_eq!(actual_hotspot, expected_hotspot);
        }
    }

    #[tokio::test]
    async fn skip_empty_radio_rewards() {
        let rewards_info = rewards_info_1_hour();
        let coverage_shares = CoverageShares {
            coverage_map: coverage_map::CoverageMapBuilder::default()
                .build(&BoostedHexes::default(), rewards_info.epoch_period.start),
            radio_infos: HashMap::new(),
        };

        let reward_shares = new_poc_only(rewards_info.epoch_emissions);
        assert!(coverage_shares
            .into_rewards(reward_shares, &rewards_info.epoch_period)
            .is_none());
    }

    #[test]
    fn service_provider_reward_amounts() {
        let hnt_bone_price = dec!(0.00001);

        let sp1 = ServiceProvider::HeliumMobile;

        let rewards_info = rewards_info_1_hour();

        let total_sp_rewards = service_provider::get_scheduled_tokens(rewards_info.epoch_emissions);
        let sp_reward_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(sp1, dec!(1000))]),
            ServiceProviderPromotions::default(),
            total_sp_rewards,
            hnt_bone_price,
            rewards_info.clone(),
        );

        let mut sp_rewards = HashMap::<i32, u64>::new();
        let mut allocated_sp_rewards = 0_u64;

        for (reward_amount, reward) in sp_reward_infos.iter_rewards() {
            if let Some(MobileReward::ServiceProviderReward(r)) = reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        assert_eq!(sp1_reward_amount, 999);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards - Decimal::from(allocated_sp_rewards))
            .round_dp_with_strategy(0, RoundingStrategy::ToZero)
            .to_u64()
            .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 342_465_752_425);
    }

    #[test]
    fn service_provider_reward_amounts_capped() {
        let hnt_bone_price = dec!(1.0);
        let sp1 = ServiceProvider::HeliumMobile;

        let rewards_info = rewards_info_1_hour();

        let total_sp_rewards_in_bones = dec!(1_0000_0000);
        let total_rewards_value_in_dc = hnt_bones_to_dc(total_sp_rewards_in_bones, hnt_bone_price);

        let sp_reward_infos = ServiceProviderRewardInfos::new(
            // force the service provider to have spend more DC than total rewardable
            ServiceProviderDCSessions::from([(sp1, total_rewards_value_in_dc * dec!(2.0))]),
            ServiceProviderPromotions::default(),
            total_sp_rewards_in_bones,
            hnt_bone_price,
            rewards_info.clone(),
        );

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;

        for (reward_amount, reward) in sp_reward_infos.iter_rewards() {
            if let Some(MobileReward::ServiceProviderReward(r)) = reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");

        assert_eq!(Decimal::from(sp1_reward_amount), total_sp_rewards_in_bones);
        assert_eq!(sp1_reward_amount, 1_0000_0000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 0);
    }

    #[test]
    fn service_provider_reward_hip87_ex1() {
        // price from hip example and converted to bones
        let hnt_bone_price = dec!(0.0001) / dec!(1_0000_0000);
        let sp1 = ServiceProvider::HeliumMobile;

        let rewards_info = rewards_info_1_hour();

        let total_sp_rewards_in_bones = dec!(500_000_000) * dec!(1_0000_0000);

        let sp_reward_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(sp1, dec!(1_0000_0000))]),
            ServiceProviderPromotions::default(),
            total_sp_rewards_in_bones,
            hnt_bone_price,
            rewards_info.clone(),
        );

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;

        for (reward_amount, reward) in sp_reward_infos.iter_rewards() {
            if let Some(MobileReward::ServiceProviderReward(r)) = reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount_in_bones = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        // assert expected value in bones
        assert_eq!(sp1_reward_amount_in_bones, 10_000_000 * 1_0000_0000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 49_000_000_000_000_000);
    }

    #[test]
    fn service_provider_reward_hip87_ex2() {
        // price from hip example and converted to bones
        let hnt_bone_price = dec!(0.0001) / dec!(1_0000_0000);
        let sp1 = ServiceProvider::HeliumMobile;

        let rewards_info = rewards_info_1_hour();
        let total_sp_rewards_in_bones = dec!(500_000_000) * dec!(1_0000_0000);

        let sp_reward_infos = ServiceProviderRewardInfos::new(
            ServiceProviderDCSessions::from([(sp1, dec!(100_000_000_000))]),
            ServiceProviderPromotions::default(),
            total_sp_rewards_in_bones,
            hnt_bone_price,
            rewards_info.clone(),
        );

        let mut sp_rewards = HashMap::new();
        let mut allocated_sp_rewards = 0_u64;

        for (reward_amount, reward) in sp_reward_infos.iter_rewards() {
            if let Some(MobileReward::ServiceProviderReward(r)) = reward.reward {
                sp_rewards.insert(r.service_provider_id, r.amount);
                assert_eq!(reward_amount, r.amount);
                allocated_sp_rewards += reward_amount;
            }
        }

        let sp1_reward_amount_in_bones = *sp_rewards
            .get(&(sp1 as i32))
            .expect("Could not fetch sp1 shares");
        // assert expected value in bones
        assert_eq!(sp1_reward_amount_in_bones, 500_000_000 * 1_0000_0000);

        // confirm the unallocated service provider reward amounts
        let unallocated_sp_reward_amount = (total_sp_rewards_in_bones
            - Decimal::from(allocated_sp_rewards))
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_u64()
        .unwrap_or(0);
        assert_eq!(unallocated_sp_reward_amount, 0);
    }
}
