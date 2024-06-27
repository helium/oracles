use chrono::{DateTime, TimeZone, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile as proto;
use serde::{Deserialize, Serialize};

use crate::{error::DecodeError, traits::MsgDecode, Error};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceProviderBoostedRewardBannedRadio {
    pub pubkey: PublicKeyBinary,
    pub key_type: KeyType,
    pub reason: proto::service_provider_boosted_rewards_banned_radio_req_v1::SpBoostedRewardsBannedRadioReason,
    pub until: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KeyType {
    CbsdId(String),
    HotspotKey(PublicKeyBinary),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceProviderBoostedRewardBannedRadioIngest {
    pub received_timestamp: DateTime<Utc>,
    pub report: ServiceProviderBoostedRewardBannedRadio,
}

impl MsgDecode for ServiceProviderBoostedRewardBannedRadio {
    type Msg = proto::ServiceProviderBoostedRewardsBannedRadioReqV1;
}

impl MsgDecode for ServiceProviderBoostedRewardBannedRadioIngest {
    type Msg = proto::ServiceProviderBoostedRewardsBannedRadioIngestReportV1;
}

impl TryFrom<proto::ServiceProviderBoostedRewardsBannedRadioReqV1>
    for ServiceProviderBoostedRewardBannedRadio
{
    type Error = Error;

    fn try_from(
        value: proto::ServiceProviderBoostedRewardsBannedRadioReqV1,
    ) -> Result<Self, Self::Error> {
        let reason = value.reason();

        Ok(Self {
            pubkey: value.pub_key.into(),
            key_type: match value.key_type {
                Some(proto::service_provider_boosted_rewards_banned_radio_req_v1::KeyType::CbsdId(cbsd_id)) => KeyType::CbsdId(cbsd_id),
                Some(proto::service_provider_boosted_rewards_banned_radio_req_v1::KeyType::HotspotKey(bytes)) => KeyType::HotspotKey(bytes.into()),
                None => return Err(Error::NotFound("key_type".to_string())),
            },
            reason,
            until: Utc.timestamp_opt(value.until as i64, 0).single().ok_or_else(|| DecodeError::invalid_timestamp(value.until))?,
        })
    }
}

impl TryFrom<proto::ServiceProviderBoostedRewardsBannedRadioIngestReportV1>
    for ServiceProviderBoostedRewardBannedRadioIngest
{
    type Error = Error;

    fn try_from(
        value: proto::ServiceProviderBoostedRewardsBannedRadioIngestReportV1,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: Utc
                .timestamp_millis_opt(value.received_timestamp as i64)
                .single()
                .ok_or_else(|| DecodeError::invalid_timestamp(value.received_timestamp))?,
            report: value
                .report
                .ok_or_else(|| Error::not_found("report not found"))?
                .try_into()?,
        })
    }
}
