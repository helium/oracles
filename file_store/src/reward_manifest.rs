use crate::{error::DecodeError, traits::MsgDecode, Error};
use chrono::{DateTime, TimeZone, Utc};
use helium_proto as proto;
use rust_decimal::Decimal;

#[derive(Clone, Debug)]
pub struct RewardManifest {
    pub written_files: Vec<String>,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
    pub reward_data: Option<RewardData>,
}

#[derive(Clone, Debug)]
pub enum RewardData {
    MobileRewardData {
        poc_bones_per_coverage_point: Decimal,
        boosted_poc_bones_per_coverage_point: Decimal,
    },
    IotRewardData {
        poc_bones_per_beacon_reward_share: Decimal,
        poc_bones_per_witness_reward_share: Decimal,
        dc_bones_per_share: Decimal,
    },
}

impl MsgDecode for RewardManifest {
    type Msg = proto::RewardManifest;
}

impl TryFrom<proto::RewardManifest> for RewardManifest {
    type Error = Error;

    fn try_from(value: proto::RewardManifest) -> Result<Self, Self::Error> {
        Ok(RewardManifest {
            written_files: value.written_files,
            start_timestamp: Utc
                .timestamp_opt(value.start_timestamp as i64, 0)
                .single()
                .ok_or(Error::Decode(DecodeError::InvalidTimestamp(
                    value.start_timestamp,
                )))?,
            end_timestamp: Utc
                .timestamp_opt(value.end_timestamp as i64, 0)
                .single()
                .ok_or(Error::Decode(DecodeError::InvalidTimestamp(
                    value.end_timestamp,
                )))?,
            reward_data: match value.reward_data {
                Some(proto::reward_manifest::RewardData::MobileRewardData(reward_data)) => {
                    Some(RewardData::MobileRewardData {
                        poc_bones_per_coverage_point: reward_data
                            .poc_bones_per_reward_share
                            .ok_or(DecodeError::empty_field("poc_bones_per_coverage_point"))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                        boosted_poc_bones_per_coverage_point: reward_data
                            .boosted_poc_bones_per_reward_share
                            .ok_or(DecodeError::empty_field(
                                "boosted_poc_bones_per_coverage_point",
                            ))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                    })
                }
                Some(proto::reward_manifest::RewardData::IotRewardData(reward_data)) => {
                    Some(RewardData::IotRewardData {
                        poc_bones_per_beacon_reward_share: reward_data
                            .poc_bones_per_beacon_reward_share
                            .ok_or(DecodeError::empty_field(
                                "poc_bones_per_beacon_reward_share",
                            ))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                        poc_bones_per_witness_reward_share: reward_data
                            .poc_bones_per_witness_reward_share
                            .ok_or(DecodeError::empty_field(
                                "poc_bones_per_witness_reward_share",
                            ))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                        dc_bones_per_share: reward_data
                            .dc_bones_per_share
                            .ok_or(DecodeError::empty_field("dc_bones_per_share"))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                    })
                }
                None => None,
            },
        })
    }
}
