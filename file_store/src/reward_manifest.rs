use crate::{error::DecodeError, traits::MsgDecode, Error};
use chrono::{DateTime, TimeZone, Utc};
use helium_proto as proto;
use helium_proto::{IotRewardToken, MobileRewardToken};
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct RewardManifest {
    pub written_files: Vec<String>,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: DateTime<Utc>,
    pub reward_data: Option<RewardData>,
    pub epoch: u64,
    pub price: u64,
}

#[derive(Clone, Debug, Serialize)]
pub enum RewardData {
    MobileRewardData {
        poc_bones_per_reward_share: Decimal,
        boosted_poc_bones_per_reward_share: Decimal,
        token: MobileRewardToken,
    },
    IotRewardData {
        poc_bones_per_beacon_reward_share: Decimal,
        poc_bones_per_witness_reward_share: Decimal,
        dc_bones_per_share: Decimal,
        token: IotRewardToken,
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
            epoch: value.epoch,
            price: value.price,
            reward_data: match value.reward_data {
                Some(proto::reward_manifest::RewardData::MobileRewardData(reward_data)) => {
                    let token = MobileRewardToken::try_from(reward_data.token).map_err(|_| {
                        DecodeError::unsupported_token_type(
                            "mobile_reward_manifest",
                            reward_data.token,
                        )
                    })?;
                    Some(RewardData::MobileRewardData {
                        poc_bones_per_reward_share: reward_data
                            .poc_bones_per_reward_share
                            .ok_or(DecodeError::empty_field("poc_bones_per_reward_share"))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                        boosted_poc_bones_per_reward_share: reward_data
                            .boosted_poc_bones_per_reward_share
                            .ok_or(DecodeError::empty_field(
                                "boosted_poc_bones_per_reward_share",
                            ))?
                            .value
                            .parse()
                            .map_err(DecodeError::from)?,
                        token,
                    })
                }
                Some(proto::reward_manifest::RewardData::IotRewardData(reward_data)) => {
                    let token = IotRewardToken::try_from(reward_data.token).map_err(|_| {
                        DecodeError::unsupported_token_type(
                            "iot_reward_manifest",
                            reward_data.token,
                        )
                    })?;
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
                        token,
                    })
                }
                None => None,
            },
        })
    }
}
