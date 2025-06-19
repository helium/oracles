use std::convert::TryFrom;

use crate::{
    error::DecodeError,
    traits::{MsgDecode, MsgTimestamp, TimestampDecode, TimestampEncode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use h3o::CellIndex;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    HexUsageStatsIngestReportV1, HexUsageStatsReqV1, RadioUsageStatsIngestReportV1,
    RadioUsageStatsReqV1,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct HexUsageStatsReq {
    pub hex: CellIndex,
    pub service_provider_user_count: u64,
    pub disco_mapping_user_count: u64,
    pub offload_user_count: u64,
    pub service_provider_transfer_bytes: u64,
    pub offload_transfer_bytes: u64,
    pub epoch_start_timestamp: DateTime<Utc>,
    pub epoch_end_timestamp: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioUsageStatsReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub service_provider_user_count: u64,
    pub disco_mapping_user_count: u64,
    pub offload_user_count: u64,
    pub service_provider_transfer_bytes: u64,
    pub offload_transfer_bytes: u64,
    pub epoch_start_timestamp: DateTime<Utc>,
    pub epoch_end_timestamp: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

impl MsgDecode for HexUsageStatsReq {
    type Msg = HexUsageStatsReqV1;
}

impl MsgDecode for RadioUsageStatsReq {
    type Msg = RadioUsageStatsReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HexUsageStatsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for HexUsageStatsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioUsageStatsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for RadioUsageStatsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<HexUsageStatsReqV1> for HexUsageStatsReq {
    type Error = Error;
    fn try_from(v: HexUsageStatsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let epoch_start_timestamp = v.epoch_start_timestamp.to_timestamp_millis()?;
        let epoch_end_timestamp = v.epoch_end_timestamp.to_timestamp_millis()?;
        let hex = CellIndex::try_from(v.hex).map_err(|_| {
            DecodeError::FileStreamTryDecode(format!("invalid CellIndex {}", v.hex))
        })?;
        Ok(Self {
            hex,
            service_provider_user_count: v.service_provider_user_count,
            disco_mapping_user_count: v.disco_mapping_user_count,
            offload_user_count: v.offload_user_count,
            service_provider_transfer_bytes: v.service_provider_transfer_bytes,
            offload_transfer_bytes: v.offload_transfer_bytes,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<HexUsageStatsReq> for HexUsageStatsReqV1 {
    fn from(v: HexUsageStatsReq) -> Self {
        let timestamp = v.timestamp();
        let epoch_start_timestamp = v.epoch_start_timestamp.encode_timestamp_millis();
        let epoch_end_timestamp = v.epoch_end_timestamp.encode_timestamp_millis();

        HexUsageStatsReqV1 {
            hex: v.hex.into(),
            service_provider_user_count: v.service_provider_user_count,
            disco_mapping_user_count: v.disco_mapping_user_count,
            offload_user_count: v.offload_user_count,
            service_provider_transfer_bytes: v.service_provider_transfer_bytes,
            offload_transfer_bytes: v.offload_transfer_bytes,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<RadioUsageStatsReqV1> for RadioUsageStatsReq {
    type Error = Error;
    fn try_from(v: RadioUsageStatsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let epoch_start_timestamp = v.epoch_start_timestamp.to_timestamp_millis()?;
        let epoch_end_timestamp = v.epoch_end_timestamp.to_timestamp_millis()?;
        Ok(Self {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            service_provider_user_count: v.service_provider_user_count,
            disco_mapping_user_count: v.disco_mapping_user_count,
            offload_user_count: v.offload_user_count,
            service_provider_transfer_bytes: v.service_provider_transfer_bytes,
            offload_transfer_bytes: v.offload_transfer_bytes,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<RadioUsageStatsReq> for RadioUsageStatsReqV1 {
    fn from(v: RadioUsageStatsReq) -> Self {
        let timestamp = v.timestamp();
        let epoch_start_timestamp = v.epoch_start_timestamp.encode_timestamp_millis();
        let epoch_end_timestamp = v.epoch_end_timestamp.encode_timestamp_millis();

        RadioUsageStatsReqV1 {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            cbsd_id: String::default(),
            service_provider_user_count: v.service_provider_user_count,
            disco_mapping_user_count: v.disco_mapping_user_count,
            offload_user_count: v.offload_user_count,
            service_provider_transfer_bytes: v.service_provider_transfer_bytes,
            offload_transfer_bytes: v.offload_transfer_bytes,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct HexUsageCountsIngestReport {
    pub report: HexUsageStatsReq,
    pub received_timestamp: DateTime<Utc>,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioUsageCountsIngestReport {
    pub report: RadioUsageStatsReq,
    pub received_timestamp: DateTime<Utc>,
}

impl MsgDecode for HexUsageCountsIngestReport {
    type Msg = HexUsageStatsIngestReportV1;
}

impl MsgDecode for RadioUsageCountsIngestReport {
    type Msg = RadioUsageStatsIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HexUsageStatsIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for HexUsageCountsIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioUsageStatsIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for RadioUsageCountsIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<HexUsageStatsIngestReportV1> for HexUsageCountsIngestReport {
    type Error = Error;
    fn try_from(v: HexUsageStatsIngestReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| Error::not_found("ingest HexUsageStatsIngestReport report"))?
                .try_into()?,
            received_timestamp: v.timestamp()?,
        })
    }
}

impl From<HexUsageCountsIngestReport> for HexUsageStatsIngestReportV1 {
    fn from(v: HexUsageCountsIngestReport) -> Self {
        let received_timestamp = v.received_timestamp;
        let report: HexUsageStatsReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp: received_timestamp.encode_timestamp_millis(),
        }
    }
}

impl TryFrom<RadioUsageStatsIngestReportV1> for RadioUsageCountsIngestReport {
    type Error = Error;
    fn try_from(v: RadioUsageStatsIngestReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| Error::not_found("ingest RadioUsageCountsIngestReport report"))?
                .try_into()?,
            received_timestamp: v.timestamp()?,
        })
    }
}

impl From<RadioUsageCountsIngestReport> for RadioUsageStatsIngestReportV1 {
    fn from(v: RadioUsageCountsIngestReport) -> Self {
        let received_timestamp = v.received_timestamp;
        let report: RadioUsageStatsReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp: received_timestamp.encode_timestamp(),
        }
    }
}
