use crate::{error::DecodeError, traits::TimestampDecode, Error, Result};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;
use std::{fmt, io, os::unix::fs::MetadataExt, path::Path, str::FromStr};

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub key: String,
    pub file_type: FileType,
    pub timestamp: DateTime<Utc>,
    pub size: usize,
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.gz)?").unwrap();
}

impl FromStr for FileInfo {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| DecodeError::file_info("failed to decode file info"))?;
        let file_type = FileType::from_str(&cap[1])?;
        let timestamp = u64::from_str(&cap[2])
            .map_err(|_| DecodeError::file_info("failed to decode timestamp"))?
            .to_timestamp_millis()?;
        Ok(Self {
            key,
            file_type,
            timestamp,
            size: 0,
        })
    }
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AsRef<str> for FileInfo {
    fn as_ref(&self) -> &str {
        &self.key
    }
}

impl From<FileInfo> for String {
    fn from(v: FileInfo) -> Self {
        v.key
    }
}

impl From<(FileType, DateTime<Utc>)> for FileInfo {
    fn from(v: (FileType, DateTime<Utc>)) -> Self {
        Self {
            key: format!("{}.{}.gz", &v.0, v.1.timestamp_millis()),
            file_type: v.0,
            timestamp: v.1,
            size: 0,
        }
    }
}

impl TryFrom<&aws_sdk_s3::model::Object> for FileInfo {
    type Error = Error;
    fn try_from(value: &aws_sdk_s3::model::Object) -> Result<Self> {
        let size = value.size() as usize;
        let key = value
            .key
            .as_ref()
            .ok_or_else(|| Error::not_found("no file name found"))?;
        let mut info = Self::from_str(key)?;
        info.size = size;
        Ok(info)
    }
}

impl TryFrom<&Path> for FileInfo {
    type Error = Error;
    fn try_from(value: &Path) -> Result<Self> {
        let mut info = Self::from_str(&value.to_string_lossy())?;
        info.size = value.metadata()?.size() as usize;
        Ok(info)
    }
}

impl FileInfo {
    pub fn matches(str: &str) -> bool {
        RE.is_match(str)
    }
}

pub const CELL_HEARTBEAT: &str = "cell_heartbeat";
pub const CELL_SPEEDTEST: &str = "cell_speedtest";
pub const CELL_HEARTBEAT_INGEST_REPORT: &str = "heartbeat_report";
pub const CELL_SPEEDTEST_INGEST_REPORT: &str = "speedtest_report";
pub const ENTROPY: &str = "entropy";
pub const SUBNETWORK_REWARDS: &str = "subnetwork_rewards";
pub const ENTROPY_REPORT: &str = "entropy_report";
pub const IOT_BEACON_INGEST_REPORT: &str = "iot_beacon_ingest_report";
pub const IOT_WITNESS_INGEST_REPORT: &str = "iot_witness_ingest_report";
pub const IOT_POC: &str = "iot_poc";
pub const IOT_INVALID_BEACON_REPORT: &str = "iot_invalid_beacon";
pub const IOT_INVALID_WITNESS_REPORT: &str = "iot_invalid_witness";
pub const SPEEDTEST_AVG: &str = "speedtest_avg";
pub const VALIDATED_HEARTBEAT: &str = "validated_heartbeat";
pub const SIGNED_POC_RECEIPT_TXN: &str = "signed_poc_receipt_txn";
pub const RADIO_REWARD_SHARE: &str = "radio_reward_share";
pub const REWARD_MANIFEST: &str = "reward_manifest";
pub const IOT_PACKET_REPORT: &str = "packetreport";
pub const IOT_VALID_PACKET: &str = "iot_valid_packet";
pub const INVALID_PACKET: &str = "invalid_packet";
pub const IOT_REWARD_SHARE: &str = "iot_reward_share";
pub const DATA_TRANSFER_SESSION_INGEST_REPORT: &str = "data_transfer_session_ingest_report";
pub const VALID_DATA_TRANSFER_SESSION: &str = "valid_data_transfer_session";
pub const PRICE_REPORT: &str = "price_report";
pub const MOBILE_REWARD_SHARE: &str = "mobile_reward_share";

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Copy, strum::EnumCount)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    CellHeartbeat = 0,
    CellSpeedtest = 1,
    Entropy = 2,
    SubnetworkRewards = 3,
    CellHeartbeatIngestReport,
    CellSpeedtestIngestReport,
    EntropyReport,
    IotBeaconIngestReport,
    IotWitnessIngestReport,
    IotPoc,
    IotInvalidBeaconReport,
    IotInvalidWitnessReport,
    SpeedtestAvg,
    ValidatedHeartbeat,
    SignedPocReceiptTxn,
    RadioRewardShare,
    RewardManifest,
    IotPacketReport,
    IotValidPacket,
    InvalidPacket,
    IotRewardShare,
    DataTransferSessionIngestReport,
    ValidDataTransferSession,
    PriceReport,
    MobileRewardShare,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::CellHeartbeat => CELL_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
            Self::CellHeartbeatIngestReport => CELL_HEARTBEAT_INGEST_REPORT,
            Self::CellSpeedtestIngestReport => CELL_SPEEDTEST_INGEST_REPORT,
            Self::Entropy => ENTROPY,
            Self::SubnetworkRewards => SUBNETWORK_REWARDS,
            Self::EntropyReport => ENTROPY_REPORT,
            Self::IotBeaconIngestReport => IOT_BEACON_INGEST_REPORT,
            Self::IotWitnessIngestReport => IOT_WITNESS_INGEST_REPORT,
            Self::IotPoc => IOT_POC,
            Self::IotInvalidBeaconReport => IOT_INVALID_BEACON_REPORT,
            Self::IotInvalidWitnessReport => IOT_INVALID_WITNESS_REPORT,
            Self::SpeedtestAvg => SPEEDTEST_AVG,
            Self::ValidatedHeartbeat => VALIDATED_HEARTBEAT,
            Self::SignedPocReceiptTxn => SIGNED_POC_RECEIPT_TXN,
            Self::RadioRewardShare => RADIO_REWARD_SHARE,
            Self::RewardManifest => REWARD_MANIFEST,
            Self::IotPacketReport => IOT_PACKET_REPORT,
            Self::IotValidPacket => IOT_VALID_PACKET,
            Self::InvalidPacket => INVALID_PACKET,
            Self::IotRewardShare => IOT_REWARD_SHARE,
            Self::DataTransferSessionIngestReport => DATA_TRANSFER_SESSION_INGEST_REPORT,
            Self::ValidDataTransferSession => VALID_DATA_TRANSFER_SESSION,
            Self::PriceReport => PRICE_REPORT,
            Self::MobileRewardShare => MOBILE_REWARD_SHARE,
        };
        f.write_str(s)
    }
}

impl FileType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::CellHeartbeat => CELL_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
            Self::CellHeartbeatIngestReport => CELL_HEARTBEAT_INGEST_REPORT,
            Self::CellSpeedtestIngestReport => CELL_SPEEDTEST_INGEST_REPORT,
            Self::Entropy => ENTROPY,
            Self::SubnetworkRewards => SUBNETWORK_REWARDS,
            Self::EntropyReport => ENTROPY_REPORT,
            Self::IotBeaconIngestReport => IOT_BEACON_INGEST_REPORT,
            Self::IotWitnessIngestReport => IOT_WITNESS_INGEST_REPORT,
            Self::IotPoc => IOT_POC,
            Self::IotInvalidBeaconReport => IOT_INVALID_BEACON_REPORT,
            Self::IotInvalidWitnessReport => IOT_INVALID_WITNESS_REPORT,
            Self::SpeedtestAvg => SPEEDTEST_AVG,
            Self::ValidatedHeartbeat => VALIDATED_HEARTBEAT,
            Self::SignedPocReceiptTxn => SIGNED_POC_RECEIPT_TXN,
            Self::RadioRewardShare => RADIO_REWARD_SHARE,
            Self::RewardManifest => REWARD_MANIFEST,
            Self::IotPacketReport => IOT_PACKET_REPORT,
            Self::IotValidPacket => IOT_VALID_PACKET,
            Self::InvalidPacket => INVALID_PACKET,
            Self::IotRewardShare => IOT_REWARD_SHARE,
            Self::DataTransferSessionIngestReport => DATA_TRANSFER_SESSION_INGEST_REPORT,
            Self::ValidDataTransferSession => VALID_DATA_TRANSFER_SESSION,
            Self::PriceReport => PRICE_REPORT,
            Self::MobileRewardShare => MOBILE_REWARD_SHARE,
        }
    }
}

impl FromStr for FileType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let result = match s {
            CELL_HEARTBEAT => Self::CellHeartbeat,
            CELL_SPEEDTEST => Self::CellSpeedtest,
            CELL_HEARTBEAT_INGEST_REPORT => Self::CellHeartbeatIngestReport,
            CELL_SPEEDTEST_INGEST_REPORT => Self::CellSpeedtestIngestReport,
            ENTROPY => Self::Entropy,
            SUBNETWORK_REWARDS => Self::SubnetworkRewards,
            ENTROPY_REPORT => Self::EntropyReport,
            IOT_BEACON_INGEST_REPORT => Self::IotBeaconIngestReport,
            IOT_WITNESS_INGEST_REPORT => Self::IotWitnessIngestReport,
            IOT_POC => Self::IotPoc,
            IOT_INVALID_BEACON_REPORT => Self::IotInvalidBeaconReport,
            IOT_INVALID_WITNESS_REPORT => Self::IotInvalidWitnessReport,
            SPEEDTEST_AVG => Self::SpeedtestAvg,
            VALIDATED_HEARTBEAT => Self::ValidatedHeartbeat,
            SIGNED_POC_RECEIPT_TXN => Self::SignedPocReceiptTxn,
            RADIO_REWARD_SHARE => Self::RadioRewardShare,
            REWARD_MANIFEST => Self::RewardManifest,
            IOT_PACKET_REPORT => Self::IotPacketReport,
            IOT_VALID_PACKET => Self::IotValidPacket,
            INVALID_PACKET => Self::InvalidPacket,
            IOT_REWARD_SHARE => Self::IotRewardShare,
            DATA_TRANSFER_SESSION_INGEST_REPORT => Self::DataTransferSessionIngestReport,
            VALID_DATA_TRANSFER_SESSION => Self::ValidDataTransferSession,
            PRICE_REPORT => Self::PriceReport,
            MOBILE_REWARD_SHARE => Self::MobileRewardShare,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        };
        Ok(result)
    }
}
