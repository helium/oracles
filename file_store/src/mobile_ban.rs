use std::str::FromStr;

use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;

use crate::{
    error::DecodeError,
    file_info_poller::{FileInfoPollerServer, FileInfoStream, LookbackBehavior},
    file_sink::FileSinkClient,
    traits::{FileSinkWriteExt, MsgDecode, TimestampDecode, TimestampEncode},
    Error, FileSink, FileStore,
};

pub mod proto {
    pub use helium_proto::services::poc_mobile::{
        ban_req_v1::BanAction, BanDetailsV1, BanIngestReportV1, BanReason, BanReqV1, BanRespV1,
        BanType, UnbanDetailsV1, VerifiedBanIngestReportStatus, VerifiedBanIngestReportV1,
    };
}

pub use proto::VerifiedBanIngestReportStatus;

pub type BanReportStream = FileInfoStream<BanReport>;
pub type BanReportSource = tokio::sync::mpsc::Receiver<BanReportStream>;
pub type VerifiedBanReportSink = FileSinkClient<proto::VerifiedBanIngestReportV1>;

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
    pub hotspot_serial: String,
    pub sent_timestamp: DateTime<Utc>,
    pub ban_key: PublicKeyBinary,
    pub signature: Vec<u8>,
    pub ban_action: BanAction,
}

#[derive(Clone)]
pub enum BanAction {
    Ban {
        notes: String,
        reason: proto::BanReason,
        ban_type: BanType,
        expiration_timestamp: Option<DateTime<Utc>>,
    },
    UnBan {
        notes: String,
    },
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BanType {
    All,
    Poc,
    Data,
}

impl MsgDecode for BanReport {
    type Msg = proto::BanIngestReportV1;
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
                .ok_or_else(|| Error::not_found("verified  ban report missing"))?
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
                .ok_or_else(|| Error::not_found(" ban report missing"))?
                .try_into()?,
        })
    }
}

impl TryFrom<proto::BanReqV1> for BanRequest {
    type Error = Error;

    fn try_from(value: proto::BanReqV1) -> Result<Self, Self::Error> {
        Ok(Self {
            hotspot_pubkey: value.hotspot_pubkey.into(),
            hotspot_serial: value.hotspot_serial,
            sent_timestamp: value.sent_timestamp_ms.to_timestamp_millis()?,
            ban_key: value.ban_key.into(),
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
            proto::BanAction::Ban(details) => Self::Ban {
                reason: details.reason(),
                ban_type: details.ban_type().into(),
                notes: details.notes,
                expiration_timestamp: match details.expiration_timestamp_ms {
                    0 => None,
                    val => Some(val.to_timestamp_millis()?),
                },
            },
            proto::BanAction::Unban(details) => Self::UnBan {
                notes: details.notes,
            },
        };
        Ok(action)
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
            hotspot_serial: value.hotspot_serial,
            sent_timestamp_ms: value.sent_timestamp.encode_timestamp_millis(),
            ban_key: value.ban_key.into(),
            signature: value.signature,
            ban_action: Some(value.ban_action.into()),
        }
    }
}

impl From<BanAction> for proto::BanAction {
    fn from(value: BanAction) -> Self {
        match value {
            BanAction::Ban {
                notes,
                reason,
                ban_type,
                expiration_timestamp,
            } => proto::BanAction::Ban(proto::BanDetailsV1 {
                notes,
                reason: reason.into(),
                ban_type: ban_type.into(),
                expiration_timestamp_ms: match expiration_timestamp {
                    Some(ts) => ts.encode_timestamp_millis(),
                    None => 0,
                },
            }),
            BanAction::UnBan { notes } => proto::BanAction::Unban(proto::UnbanDetailsV1 { notes }),
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

pub async fn report_source(
    pool: PgPool,
    file_store: FileStore,
    start_after: DateTime<Utc>,
) -> crate::Result<(BanReportSource, FileInfoPollerServer<BanReport>)> {
    crate::file_source::continuous_source()
        .state(pool)
        .store(file_store)
        .lookback(LookbackBehavior::StartAfter(start_after))
        .prefix(crate::FileType::MobileBanReport.to_string())
        .create()
        .await
}

impl VerifiedBanReport {
    pub fn is_verified(&self) -> bool {
        matches!(self.status, proto::VerifiedBanIngestReportStatus::Valid)
    }

    pub fn hotspot_pubkey(&self) -> &PublicKeyBinary {
        &self.report.report.hotspot_pubkey
    }
}

impl BanType {
    pub fn as_str_name(&self) -> &'static str {
        proto::BanType::from(self.clone()).as_str_name()
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
