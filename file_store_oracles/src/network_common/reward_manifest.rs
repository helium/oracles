use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError};
use helium_proto as proto;
use helium_proto::{IotRewardToken, MobileRewardToken};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::prost_enum;

#[derive(thiserror::Error, Debug)]
pub enum RewardManifestError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported mobile token type: {0}")]
    MobileTokenType(prost::UnknownEnumValue),

    #[error("unsupported iot token type: {0}")]
    IotTokenType(prost::UnknownEnumValue),

    #[error("error parsing decimal: {0}")]
    Decimal(#[from] rust_decimal::Error),
}

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
    type Error = RewardManifestError;

    fn try_from(value: proto::RewardManifest) -> Result<Self, Self::Error> {
        Ok(RewardManifest {
            written_files: value.written_files,
            start_timestamp: value.start_timestamp.to_timestamp()?,
            end_timestamp: value.end_timestamp.to_timestamp()?,
            epoch: value.epoch,
            price: value.price,
            reward_data: match value.reward_data {
                Some(proto::reward_manifest::RewardData::MobileRewardData(reward_data)) => {
                    Some(RewardData::MobileRewardData {
                        poc_bones_per_reward_share: reward_data
                            .poc_bones_per_reward_share
                            .ok_or(RewardManifestError::MissingField(
                                "mobile_reward_data.poc_bones_per_reward_share",
                            ))?
                            .value
                            .parse()?,
                        boosted_poc_bones_per_reward_share: deprecated_decimal(
                            reward_data.boosted_poc_bones_per_reward_share,
                        ),
                        token: prost_enum(reward_data.token, RewardManifestError::MobileTokenType)?,
                    })
                }
                Some(proto::reward_manifest::RewardData::IotRewardData(reward_data)) => {
                    Some(RewardData::IotRewardData {
                        poc_bones_per_beacon_reward_share: reward_data
                            .poc_bones_per_beacon_reward_share
                            .ok_or(RewardManifestError::MissingField(
                                "iot_reward_data.poc_bones_per_beacon_reward_share",
                            ))?
                            .value
                            .parse()?,
                        poc_bones_per_witness_reward_share: reward_data
                            .poc_bones_per_witness_reward_share
                            .ok_or(RewardManifestError::MissingField(
                                "iot_reward_data.poc_bones_per_witness_reward_share",
                            ))?
                            .value
                            .parse()?,
                        dc_bones_per_share: reward_data
                            .dc_bones_per_share
                            .ok_or(RewardManifestError::MissingField(
                                "iot_reward_data.dc_bones_per_share",
                            ))?
                            .value
                            .parse()?,
                        token: prost_enum(reward_data.token, RewardManifestError::IotTokenType)?,
                    })
                }
                None => None,
            },
        })
    }
}

// Hex boosting is not longer active. Output protos write None to boosted values
// instead of helium_proto::Decimal { value: "0".to_string() }.
// We want to keep backwards compatibility for reading protos, but we no longer
// want to err on boosted fields not being provided a value.
fn deprecated_decimal(input: Option<helium_proto::Decimal>) -> Decimal {
    let Some(helium_proto::Decimal { value }) = input else {
        return Decimal::ZERO;
    };

    value.parse().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use rust_decimal::dec;

    use super::*;

    #[test]
    fn deprecated_decimal_parse() {
        let empty = helium_proto::Decimal {
            value: "".to_string(),
        };
        let value = helium_proto::Decimal {
            value: "1.23".to_string(),
        };
        let bad = helium_proto::Decimal {
            value: "bad".to_string(),
        };

        assert_eq!(deprecated_decimal(None), Decimal::ZERO);
        assert_eq!(deprecated_decimal(Some(empty)), Decimal::ZERO);
        assert_eq!(deprecated_decimal(Some(value)), dec!(1.23));
        assert_eq!(deprecated_decimal(Some(bad)), Decimal::ZERO);
    }
}
