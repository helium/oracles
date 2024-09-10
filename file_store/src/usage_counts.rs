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
    HexUsageCountsIngestReportV1, HexUsageCountsReqV1, HotspotUsageCountsIngestReportV1,
    HotspotUsageCountsReqV1,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct HexUsageCountsReq {
    pub hex: CellIndex,
    pub helium_mobile_subscriber_avg_count: u64,
    pub helium_mobile_disco_mapping_avg_count: u64,
    pub offload_avg_count: u64,
    pub tmo_cell_avg_count: u64,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct HotspotUsageCountsReq {
    pub hotspot_pubkey: PublicKeyBinary,
    pub cbsd_id: String,
    pub helium_mobile_subscriber_avg_count: u64,
    pub helium_mobile_disco_mapping_avg_count: u64,
    pub offload_avg_count: u64,
    pub timestamp: DateTime<Utc>,
    pub carrier_mapping_key: PublicKeyBinary,
}

impl MsgDecode for HexUsageCountsReq {
    type Msg = HexUsageCountsReqV1;
}

impl MsgDecode for HotspotUsageCountsReq {
    type Msg = HotspotUsageCountsReqV1;
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HexUsageCountsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for HexUsageCountsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgTimestamp<Result<DateTime<Utc>>> for HotspotUsageCountsReqV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for HotspotUsageCountsReq {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl TryFrom<HexUsageCountsReqV1> for HexUsageCountsReq {
    type Error = Error;
    fn try_from(v: HexUsageCountsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        let hex = CellIndex::try_from(v.hex).map_err(|_| {
            DecodeError::FileStreamTryDecode(format!("invalid CellIndex {}", v.hex))
        })?;
        Ok(Self {
            hex,
            helium_mobile_subscriber_avg_count: v.helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count: v.helium_mobile_disco_mapping_avg_count,
            offload_avg_count: v.offload_avg_count,
            tmo_cell_avg_count: v.tmo_cell_avg_count,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<HexUsageCountsReq> for HexUsageCountsReqV1 {
    fn from(v: HexUsageCountsReq) -> Self {
        let timestamp = v.timestamp();
        HexUsageCountsReqV1 {
            hex: v.hex.into(),
            helium_mobile_subscriber_avg_count: v.helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count: v.helium_mobile_disco_mapping_avg_count,
            offload_avg_count: v.offload_avg_count,
            tmo_cell_avg_count: v.tmo_cell_avg_count,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
            signature: vec![],
        }
    }
}

impl TryFrom<HotspotUsageCountsReqV1> for HotspotUsageCountsReq {
    type Error = Error;
    fn try_from(v: HotspotUsageCountsReqV1) -> Result<Self> {
        let timestamp = v.timestamp()?;
        Ok(Self {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            cbsd_id: v.cbsd_id,
            helium_mobile_subscriber_avg_count: v.helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count: v.helium_mobile_disco_mapping_avg_count,
            offload_avg_count: v.offload_avg_count,
            timestamp,
            carrier_mapping_key: v.carrier_mapping_key.into(),
        })
    }
}

impl From<HotspotUsageCountsReq> for HotspotUsageCountsReqV1 {
    fn from(v: HotspotUsageCountsReq) -> Self {
        let timestamp = v.timestamp();
        HotspotUsageCountsReqV1 {
            hotspot_pubkey: v.hotspot_pubkey.into(),
            cbsd_id: v.cbsd_id,
            helium_mobile_subscriber_avg_count: v.helium_mobile_subscriber_avg_count,
            helium_mobile_disco_mapping_avg_count: v.helium_mobile_disco_mapping_avg_count,
            offload_avg_count: v.offload_avg_count,
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
pub struct HotspotUsageCountsIngestReport {
    pub report: HotspotUsageCountsReq,
    pub received_timestamp: DateTime<Utc>,
}

impl MsgDecode for HexUsageCountsIngestReport {
    type Msg = HexUsageCountsIngestReportV1;
}

impl MsgDecode for HotspotUsageCountsIngestReport {
    type Msg = HotspotUsageCountsIngestReportV1;
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

impl MsgTimestamp<Result<DateTime<Utc>>> for HotspotUsageCountsIngestReportV1 {
    fn timestamp(&self) -> Result<DateTime<Utc>> {
        self.received_timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for HotspotUsageCountsIngestReport {
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

impl TryFrom<HotspotUsageCountsIngestReportV1> for HotspotUsageCountsIngestReport {
    type Error = Error;
    fn try_from(v: HotspotUsageCountsIngestReportV1) -> Result<Self> {
        Ok(Self {
            report: v
                .clone()
                .report
                .ok_or_else(|| Error::not_found("ingest HotspotUsageCountsIngestReport report"))?
                .try_into()?,
            received_timestamp: v.timestamp()?,
        })
    }
}

impl From<HotspotUsageCountsIngestReport> for HotspotUsageCountsIngestReportV1 {
    fn from(v: HotspotUsageCountsIngestReport) -> Self {
        let received_timestamp = v.received_timestamp;
        let report: HotspotUsageCountsReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp: received_timestamp.encode_timestamp(),
        }
    }
}
