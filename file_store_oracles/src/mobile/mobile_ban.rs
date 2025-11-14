use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError, TimestampEncode};
use helium_crypto::PublicKeyBinary;

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        ban_req_v1::BanAction, BanDetailsV1, BanIngestReportV1, BanReason, BanReqV1, BanRespV1,
        BanType, UnbanDetailsV1, VerifiedBanIngestReportStatus, VerifiedBanIngestReportV1,
    };
}

// Re-export proto enums
pub use proto::{BanReason, VerifiedBanIngestReportStatus};

use crate::prost_enum;

#[derive(thiserror::Error, Debug)]
pub enum BanReportError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported status: {0}")]
    Status(prost::UnknownEnumValue),

    #[error("unsupported ban type: {0}")]
    BanType(prost::UnknownEnumValue),

    #[error("unsupported reason: {0}")]
    Reason(prost::UnknownEnumValue),
}

#[derive(Clone)]
pub struct VerifiedBanReport {
    pub verified_timestamp: DateTime<Utc>,
    pub report: BanReport,
    pub status: proto::VerifiedBanIngestReportStatus,
}

#[derive(Clone)]
pub struct BanReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: BanRequest,
}

#[derive(Clone)]
pub struct BanRequest {
    pub hotspot_pubkey: PublicKeyBinary,
    pub timestamp: DateTime<Utc>,
    pub ban_pubkey: PublicKeyBinary,
    pub signature: Vec<u8>,
    pub ban_action: BanAction,
}

#[derive(Clone)]
pub struct BanDetails {
    pub hotspot_serial: String,
    pub message: String,
    pub reason: proto::BanReason,
    pub ban_type: BanType,
    pub expiration_timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct UnbanDetails {
    pub hotspot_serial: String,
    pub message: String,
}

#[derive(Clone)]
pub enum BanAction {
    Ban(BanDetails),
    Unban(UnbanDetails),
}

// non proto BanType provided for pulling values out of DB with FromStr
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BanType {
    All,
    Poc,
    Data,
}

impl MsgDecode for BanReport {
    type Msg = proto::BanIngestReportV1;
}

impl MsgDecode for VerifiedBanReport {
    type Msg = proto::VerifiedBanIngestReportV1;
}

// === Conversion :: proto -> struct

impl TryFrom<proto::VerifiedBanIngestReportV1> for VerifiedBanReport {
    type Error = BanReportError;

    fn try_from(value: proto::VerifiedBanIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            verified_timestamp: value.verified_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or(BanReportError::MissingField("verified_ban_report.report"))?
                .try_into()?,
            status: prost_enum(value.status, BanReportError::Status)?,
        })
    }
}

impl TryFrom<proto::BanIngestReportV1> for BanReport {
    type Error = BanReportError;

    fn try_from(value: proto::BanIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: value.received_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or(BanReportError::MissingField("ban_report.report"))?
                .try_into()?,
        })
    }
}

impl TryFrom<proto::BanReqV1> for BanRequest {
    type Error = BanReportError;

    fn try_from(value: proto::BanReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            hotspot_pubkey: value.hotspot_pubkey.into(),
            timestamp: value.timestamp_ms.to_timestamp_millis()?,
            ban_pubkey: value.ban_pubkey.into(),
            signature: value.signature,
            ban_action: value.ban_action.try_into()?,
        })
    }
}

impl TryFrom<Option<proto::BanAction>> for BanAction {
    type Error = BanReportError;

    fn try_from(value: Option<proto::BanAction>) -> Result<Self, Self::Error> {
        match value {
            Some(action) => Ok(action.try_into()?),
            None => Err(BanReportError::MissingField("ban_action")),
        }
    }
}

impl TryFrom<proto::BanAction> for BanAction {
    type Error = BanReportError;
    fn try_from(value: proto::BanAction) -> Result<Self, Self::Error> {
        let action = match value {
            proto::BanAction::Ban(details) => Self::Ban(details.try_into()?),
            proto::BanAction::Unban(details) => Self::Unban(details.into()),
        };
        Ok(action)
    }
}

impl TryFrom<proto::BanDetailsV1> for BanDetails {
    type Error = BanReportError;

    fn try_from(value: proto::BanDetailsV1) -> Result<Self, Self::Error> {
        let expiration_timestamp = match value.expiration_timestamp_ms {
            0 => None,
            val => Some(val.to_timestamp_millis()?),
        };
        Ok(Self {
            reason: prost_enum(value.reason, BanReportError::Reason)?,
            ban_type: prost_enum(value.ban_type, BanReportError::BanType)?,
            hotspot_serial: value.hotspot_serial,
            message: value.message,
            expiration_timestamp,
        })
    }
}

impl From<proto::UnbanDetailsV1> for UnbanDetails {
    fn from(value: proto::UnbanDetailsV1) -> Self {
        Self {
            hotspot_serial: value.hotspot_serial,
            message: value.message,
        }
    }
}

// Helper to use map_enum that goes through the proto type into our own.
impl TryFrom<i32> for BanType {
    type Error = prost::UnknownEnumValue;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        let val = proto::BanType::try_from(value)?;
        Ok(val.into())
    }
}

impl From<proto::BanType> for BanType {
    fn from(value: proto::BanType) -> Self {
        match value {
            proto::BanType::All => Self::All,
            proto::BanType::Poc => Self::Poc,
            proto::BanType::Data => Self::Data,
        }
    }
}

// === Conversion :: struct -> proto

impl From<VerifiedBanReport> for proto::VerifiedBanIngestReportV1 {
    fn from(value: VerifiedBanReport) -> Self {
        Self {
            verified_timestamp_ms: value.verified_timestamp.encode_timestamp_millis(),
            report: Some(value.report.into()),
            status: value.status.into(),
        }
    }
}

impl From<BanReport> for proto::BanIngestReportV1 {
    fn from(value: BanReport) -> Self {
        Self {
            received_timestamp_ms: value.received_timestamp.encode_timestamp_millis(),
            report: Some(value.report.into()),
        }
    }
}

impl From<BanRequest> for proto::BanReqV1 {
    fn from(value: BanRequest) -> Self {
        Self {
            hotspot_pubkey: value.hotspot_pubkey.into(),
            timestamp_ms: value.timestamp.encode_timestamp_millis(),
            ban_pubkey: value.ban_pubkey.into(),
            signature: value.signature,
            ban_action: Some(value.ban_action.into()),
        }
    }
}

impl From<BanAction> for proto::BanAction {
    fn from(value: BanAction) -> Self {
        match value {
            BanAction::Ban(details) => proto::BanAction::Ban(details.into()),
            BanAction::Unban(details) => proto::BanAction::Unban(details.into()),
        }
    }
}

impl From<BanDetails> for proto::BanDetailsV1 {
    fn from(value: BanDetails) -> Self {
        Self {
            hotspot_serial: value.hotspot_serial,
            message: value.message,
            reason: value.reason.into(),
            ban_type: value.ban_type.into(),
            expiration_timestamp_ms: value
                .expiration_timestamp
                .map_or(0, |ts| ts.encode_timestamp_millis()),
        }
    }
}

impl From<UnbanDetails> for proto::UnbanDetailsV1 {
    fn from(value: UnbanDetails) -> Self {
        Self {
            hotspot_serial: value.hotspot_serial,
            message: value.message,
        }
    }
}

impl From<BanType> for proto::BanType {
    fn from(value: BanType) -> Self {
        match value {
            BanType::All => Self::All,
            BanType::Poc => Self::Poc,
            BanType::Data => Self::Data,
        }
    }
}

impl From<BanType> for i32 {
    fn from(value: BanType) -> Self {
        proto::BanType::from(value).into()
    }
}

// === Helpers

impl VerifiedBanReport {
    pub fn is_valid(&self) -> bool {
        matches!(self.status, proto::VerifiedBanIngestReportStatus::Valid)
    }

    pub fn hotspot_pubkey(&self) -> &PublicKeyBinary {
        &self.report.report.hotspot_pubkey
    }
}

impl BanType {
    pub fn as_str_name(&self) -> &'static str {
        proto::BanType::from(*self).as_str_name()
    }
}
