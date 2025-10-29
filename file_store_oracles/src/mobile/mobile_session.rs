use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    invalid_data_transfer_ingest_report_v1::DataTransferIngestReportStatus,
    verified_data_transfer_ingest_report_v1, CarrierIdV2,
    DataTransferEvent as DataTransferEventProto, DataTransferRadioAccessTechnology,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1, InvalidDataTransferIngestReportV1,
    VerifiedDataTransferIngestReportV1,
};
use serde::Serialize;

use crate::{prost_enum, traits::MsgTimestamp};

#[derive(thiserror::Error, Debug)]
pub enum DataTransferError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported status reason: {0}")]
    StatusReason(prost::UnknownEnumValue),

    #[error("unsupported carrier id v2: {0}")]
    CarrierIdV2(prost::UnknownEnumValue),

    #[error("unsupported radio access technology: {0}")]
    RadioAccessTechnology(prost::UnknownEnumValue),
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferSessionIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: DataTransferSessionReq,
}

impl MsgDecode for DataTransferSessionIngestReport {
    type Msg = DataTransferSessionIngestReportV1;
}

impl MsgTimestamp<TimestampDecodeResult> for DataTransferSessionIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.received_timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for DataTransferSessionIngestReport {
    fn timestamp(&self) -> u64 {
        self.received_timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<DataTransferSessionIngestReportV1> for DataTransferSessionIngestReport {
    type Error = DataTransferError;

    fn try_from(v: DataTransferSessionIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(DataTransferError::MissingField(
                    "data_transfer_session_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<DataTransferSessionIngestReport> for DataTransferSessionIngestReportV1 {
    fn from(v: DataTransferSessionIngestReport) -> Self {
        let received_timestamp = v.timestamp();
        let report: DataTransferSessionReqV1 = v.report.into();
        Self {
            report: Some(report),
            received_timestamp,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct InvalidDataTransferIngestReport {
    pub report: DataTransferSessionIngestReport,
    pub reason: DataTransferIngestReportStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgDecode for InvalidDataTransferIngestReport {
    type Msg = InvalidDataTransferIngestReportV1;
}

impl MsgTimestamp<TimestampDecodeResult> for InvalidDataTransferIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgTimestamp<u64> for InvalidDataTransferIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl TryFrom<InvalidDataTransferIngestReportV1> for InvalidDataTransferIngestReport {
    type Error = DataTransferError;

    fn try_from(v: InvalidDataTransferIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(DataTransferError::MissingField(
                    "invalid_data_transfer_ingest_report.report",
                ))?
                .try_into()?,
            reason: prost_enum(v.reason, DataTransferError::StatusReason)?,
        })
    }
}

impl From<InvalidDataTransferIngestReport> for InvalidDataTransferIngestReportV1 {
    fn from(v: InvalidDataTransferIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: DataTransferSessionIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            reason: v.reason as i32,
            timestamp,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct VerifiedDataTransferIngestReport {
    pub report: DataTransferSessionIngestReport,
    pub status: verified_data_transfer_ingest_report_v1::ReportStatus,
    pub timestamp: DateTime<Utc>,
}

impl MsgTimestamp<u64> for VerifiedDataTransferIngestReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for VerifiedDataTransferIngestReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp_millis()
    }
}

impl MsgDecode for VerifiedDataTransferIngestReport {
    type Msg = VerifiedDataTransferIngestReportV1;
}

impl TryFrom<VerifiedDataTransferIngestReportV1> for VerifiedDataTransferIngestReport {
    type Error = DataTransferError;

    fn try_from(v: VerifiedDataTransferIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            status: prost_enum(v.status, DataTransferError::StatusReason)?,
            timestamp: v.timestamp()?,
            report: v
                .report
                .ok_or(DataTransferError::MissingField(
                    "verified_data_transfer_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl From<VerifiedDataTransferIngestReport> for VerifiedDataTransferIngestReportV1 {
    fn from(v: VerifiedDataTransferIngestReport) -> Self {
        let timestamp = v.timestamp();
        let report: DataTransferSessionIngestReportV1 = v.report.into();
        Self {
            report: Some(report),
            status: v.status as i32,
            timestamp,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferEvent {
    pub pub_key: PublicKeyBinary,
    pub upload_bytes: u64,
    pub download_bytes: u64,
    pub radio_access_technology: DataTransferRadioAccessTechnology,
    pub event_id: String,
    pub payer: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    pub signature: Vec<u8>,
}

impl MsgTimestamp<TimestampDecodeResult> for DataTransferEventProto {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl MsgTimestamp<u64> for DataTransferEvent {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp()
    }
}

impl MsgDecode for DataTransferEvent {
    type Msg = DataTransferEventProto;
}

impl TryFrom<DataTransferEventProto> for DataTransferEvent {
    type Error = DataTransferError;

    fn try_from(v: DataTransferEventProto) -> Result<Self, Self::Error> {
        let timestamp = v.timestamp()?;

        Ok(Self {
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            radio_access_technology: prost_enum(
                v.radio_access_technology,
                DataTransferError::RadioAccessTechnology,
            )?,
            event_id: v.event_id,
            payer: v.payer.into(),
            timestamp,
            signature: v.signature,
        })
    }
}

impl From<DataTransferEvent> for DataTransferEventProto {
    fn from(v: DataTransferEvent) -> Self {
        let timestamp = v.timestamp();
        Self {
            pub_key: v.pub_key.into(),
            upload_bytes: v.upload_bytes,
            download_bytes: v.download_bytes,
            radio_access_technology: v.radio_access_technology as i32,
            event_id: v.event_id,
            payer: v.payer.into(),
            timestamp,
            signature: v.signature,
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct DataTransferSessionReq {
    pub data_transfer_usage: DataTransferEvent,
    pub rewardable_bytes: u64,
    pub pub_key: PublicKeyBinary,
    pub signature: Vec<u8>,
    pub carrier_id: CarrierIdV2,
    pub sampling: bool,
}

impl MsgDecode for DataTransferSessionReq {
    type Msg = DataTransferSessionReqV1;
}

impl TryFrom<DataTransferSessionReqV1> for DataTransferSessionReq {
    type Error = DataTransferError;

    fn try_from(v: DataTransferSessionReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            rewardable_bytes: v.rewardable_bytes,
            data_transfer_usage: v
                .data_transfer_usage
                .ok_or(DataTransferError::MissingField(
                    "data_transfer_session_req.data_transfer_usage",
                ))?
                .try_into()?,
            signature: v.signature,
            pub_key: v.pub_key.into(),
            carrier_id: prost_enum(v.carrier_id_v2, DataTransferError::CarrierIdV2)?,
            sampling: v.sampling,
        })
    }
}

#[allow(deprecated)]
impl From<DataTransferSessionReq> for DataTransferSessionReqV1 {
    fn from(v: DataTransferSessionReq) -> Self {
        let report: DataTransferEventProto = v.data_transfer_usage.into();
        Self {
            data_transfer_usage: Some(report),
            rewardable_bytes: v.rewardable_bytes,
            pub_key: v.pub_key.into(),
            signature: v.signature,
            carrier_id_v2: v.carrier_id.into(),
            sampling: v.sampling,
            ..Default::default()
        }
    }
}
