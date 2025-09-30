use std::str::FromStr;

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;

use crate::{
    error::DecodeError,
    file_info_poller::{
        FileInfoPollerServer, FileInfoPollerState, FileInfoStream, LookbackBehavior,
    },
    file_sink::FileSinkClient,
    traits::{FileSinkWriteExt, MsgDecode, TimestampDecode, TimestampEncode},
    Error, FileSink,
};

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        ban_req_v1::BanAction, BanDetailsV1, BanIngestReportV1, BanReason, BanReqV1, BanRespV1,
        BanType, UnbanDetailsV1, VerifiedBanIngestReportStatus, VerifiedBanIngestReportV1,
    };
}

// Re-export proto enums
pub use proto::{BanReason, VerifiedBanIngestReportStatus};

pub type BanReportStream = FileInfoStream<BanReport>;
pub type BanReportSource = tokio::sync::mpsc::Receiver<BanReportStream>;

pub type VerifiedBanReportSink = FileSinkClient<proto::VerifiedBanIngestReportV1>;
pub type VerifiedBanReportStream = FileInfoStream<VerifiedBanReport>;
pub type VerifiedBanReportSource = tokio::sync::mpsc::Receiver<VerifiedBanReportStream>;

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
    type Error = Error;

    fn try_from(value: proto::VerifiedBanIngestReportV1) -> Result<Self, Self::Error> {
        let status = value.status();
        Ok(Self {
            verified_timestamp: value.verified_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or_else(|| Error::not_found("verified ban report missing"))?
                .try_into()?,
            status,
        })
    }
}

impl TryFrom<proto::BanIngestReportV1> for BanReport {
    type Error = Error;

    fn try_from(value: proto::BanIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: value.received_timestamp_ms.to_timestamp_millis()?,
            report: value
                .report
                .ok_or_else(|| Error::not_found("ban report missing"))?
                .try_into()?,
        })
    }
}

impl TryFrom<proto::BanReqV1> for BanRequest {
    type Error = Error;

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
    type Error = Error;

    fn try_from(value: Option<proto::BanAction>) -> Result<Self, Self::Error> {
        match value {
            Some(action) => Ok(action.try_into()?),
            None => Err(DecodeError::empty_field("ban_action")),
        }
    }
}

impl TryFrom<proto::BanAction> for BanAction {
    type Error = Error;
    fn try_from(value: proto::BanAction) -> Result<Self, Self::Error> {
        let action = match value {
            proto::BanAction::Ban(details) => Self::Ban(details.try_into()?),
            proto::BanAction::Unban(details) => Self::Unban(details.into()),
        };
        Ok(action)
    }
}

impl TryFrom<proto::BanDetailsV1> for BanDetails {
    type Error = Error;

    fn try_from(value: proto::BanDetailsV1) -> Result<Self, Self::Error> {
        let reason = value.reason();
        let ban_type = value.ban_type().into();
        let expiration_timestamp = match value.expiration_timestamp_ms {
            0 => None,
            val => Some(val.to_timestamp_millis()?),
        };
        Ok(Self {
            reason,
            ban_type,
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

pub async fn verified_report_sink(
    target_path: &std::path::Path,
    file_upload: crate::file_upload::FileUpload,
    commit_strategy: crate::traits::FileSinkCommitStrategy,
    roll_time: crate::traits::FileSinkRollTime,
    metric_prefix: &str,
) -> crate::Result<(
    VerifiedBanReportSink,
    FileSink<proto::VerifiedBanIngestReportV1>,
)> {
    proto::VerifiedBanIngestReportV1::file_sink(
        target_path,
        file_upload,
        commit_strategy,
        roll_time,
        metric_prefix,
    )
    .await
}

pub async fn report_source<State: FileInfoPollerState>(
    pool: State,
    client: crate::Client,
    bucket: String,
    start_after: DateTime<Utc>,
) -> crate::Result<(BanReportSource, FileInfoPollerServer<BanReport, State>)> {
    crate::file_source::continuous_source()
        .state(pool)
        .file_store(client, bucket)
        .lookback(LookbackBehavior::StartAfter(start_after))
        .prefix(crate::FileType::MobileBanReport.to_string())
        .create()
        .await
}

pub async fn verified_report_source<State: FileInfoPollerState>(
    pool: State,
    client: crate::Client,
    bucket: String,
    start_after: DateTime<Utc>,
) -> crate::Result<(
    VerifiedBanReportSource,
    FileInfoPollerServer<VerifiedBanReport, State>,
)> {
    crate::file_source::continuous_source()
        .state(pool)
        .file_store(client, bucket)
        .lookback(LookbackBehavior::StartAfter(start_after))
        .prefix(crate::FileType::VerifiedMobileBanReport.to_string())
        .create()
        .await
}

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

#[derive(Debug, thiserror::Error)]
#[error("invalid ban type string: {0}")]
pub struct BanTypeParseError(String);

impl FromStr for BanType {
    type Err = BanTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        proto::BanType::from_str_name(s)
            .ok_or_else(|| BanTypeParseError(s.to_string()))
            .map(BanType::from)
    }
}
