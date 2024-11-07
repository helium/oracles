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
    HexUsageCountsIngestReportV1, HexUsageCountsReqV1, RadioUsageCountsIngestReportV1,
    RadioUsageCountsReqV1,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct HexUsageCountsReq {
    pub hex: CellIndex,
    pub service_provider_subscriber_count: u64,
    pub disco_mapping_count: u64,
    pub offload_count: u64,
    pub epoch_start_timestamp: DateTime<Utc>,
    pub epoch_end_timestamp: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioUsageCountsReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub cbsd_id: String,
    pub service_provider_subscriber_count: u64,
    pub disco_mapping_count: u64,
    pub offload_count: u64,
    pub service_provider_transfer_bytes: u64,
    pub offload_transfer_bytes: u64,
    pub epoch_start_timestamp: DateTime<Utc>,
    pub epoch_end_timestamp: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

impl MsgDecode for HexUsageCountsReq {
    type Msg = HexUsageCountsReqV1;
}

impl MsgDecode for RadioUsageCountsReq {
    type Msg = RadioUsageCountsReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HexUsageCountsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for HexUsageCountsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioUsageCountsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for RadioUsageCountsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<HexUsageCountsReqV1> for HexUsageCountsReq {
    type Error = Error;
    fn try_from(v: HexUsageCountsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let epoch_start_timestamp = v.epoch_start_timestamp.to_timestamp_millis()?;
        let epoch_end_timestamp = v.epoch_end_timestamp.to_timestamp_millis()?;
        let hex = CellIndex::try_from(v.hex).map_err(|_| {
            DecodeError::FileStreamTryDecode(format!("invalid CellIndex {}", v.hex))
        })?;
        Ok(Self {
            hex,
            service_provider_subscriber_count: v.service_provider_subscriber_count,
            disco_mapping_count: v.disco_mapping_count,
            offload_count: v.offload_count,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<HexUsageCountsReq> for HexUsageCountsReqV1 {
    fn from(v: HexUsageCountsReq) -> Self {
        let timestamp = v.timestamp();
        let epoch_start_timestamp = v.epoch_start_timestamp.encode_timestamp_millis();
        let epoch_end_timestamp = v.epoch_end_timestamp.encode_timestamp_millis();

        HexUsageCountsReqV1 {
            hex: v.hex.into(),
            service_provider_subscriber_count: v.service_provider_subscriber_count,
            disco_mapping_count: v.disco_mapping_count,
            offload_count: v.offload_count,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<RadioUsageCountsReqV1> for RadioUsageCountsReq {
    type Error = Error;
    fn try_from(v: RadioUsageCountsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let epoch_start_timestamp = v.epoch_start_timestamp.to_timestamp_millis()?;
        let epoch_end_timestamp = v.epoch_end_timestamp.to_timestamp_millis()?;
        Ok(Self {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            cbsd_id: v.cbsd_id,
            service_provider_subscriber_count: v.service_provider_subscriber_count,
            disco_mapping_count: v.disco_mapping_count,
            offload_count: v.offload_count,
            service_provider_transfer_bytes: v.service_provider_transfer_bytes,
            offload_transfer_bytes: v.offload_transfer_bytes,
            epoch_start_timestamp,
            epoch_end_timestamp,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<RadioUsageCountsReq> for RadioUsageCountsReqV1 {
    fn from(v: RadioUsageCountsReq) -> Self {
        let timestamp = v.timestamp();
        let epoch_start_timestamp = v.epoch_start_timestamp.encode_timestamp_millis();
        let epoch_end_timestamp = v.epoch_end_timestamp.encode_timestamp_millis();

        RadioUsageCountsReqV1 {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            cbsd_id: v.cbsd_id,
            service_provider_subscriber_count: v.service_provider_subscriber_count,
            disco_mapping_count: v.disco_mapping_count,
            offload_count: v.offload_count,
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
    pub report: HexUsageCountsReq,
    pub received_timestamp: DateTime<Utc>,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct RadioUsageCountsIngestReport {
    pub report: RadioUsageCountsReq,
    pub received_timestamp: DateTime<Utc>,
}

impl MsgDecode for HexUsageCountsIngestReport {
    type Msg = HexUsageCountsIngestReportV1;
}

impl MsgDecode for RadioUsageCountsIngestReport {
    type Msg = RadioUsageCountsIngestReportV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HexUsageCountsIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for HexUsageCountsIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for RadioUsageCountsIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for RadioUsageCountsIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp()
    }
}

impl TryFrom<HexUsageCountsIngestReportV1> for HexUsageCountsIngestReport {
    type Error = Error;
    fn try_from(v: HexUsageCountsIngestReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| Error::not_found("ingest HexUsageCountsIngestReport report"))?
                .try_into()?,
            received_timestamp: v.timestamp()?,
        })
    }
}

impl From<HexUsageCountsIngestReport> for HexUsageCountsIngestReportV1 {
    fn from(v: HexUsageCountsIngestReport) -> Self {
        let received_timestamp = v.received_timestamp;
        let report: HexUsageCountsReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp: received_timestamp.encode_timestamp(),
        }
    }
}

impl TryFrom<RadioUsageCountsIngestReportV1> for RadioUsageCountsIngestReport {
    type Error = Error;
    fn try_from(v: RadioUsageCountsIngestReportV1) -> Result<Self> {
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

impl From<RadioUsageCountsIngestReport> for RadioUsageCountsIngestReportV1 {
    fn from(v: RadioUsageCountsIngestReport) -> Self {
        let received_timestamp = v.received_timestamp;
        let report: RadioUsageCountsReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp: received_timestamp.encode_timestamp(),
        }
    }
}
