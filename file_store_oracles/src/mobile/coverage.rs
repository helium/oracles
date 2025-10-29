use chrono::{DateTime, Utc};
use file_store::traits::{MsgDecode, TimestampDecode, TimestampDecodeError};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    coverage_object_req_v1, CoverageObjectIngestReportV1, CoverageObjectReqV1,
    RadioHexSignalLevel as RadioHexSignalLevelProto, SignalLevel,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::prost_enum;

#[derive(thiserror::Error, Debug)]
pub enum CoverageError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("invalid cell index: {0}")]
    InvalidCellIndex(#[from] h3o::error::InvalidCellIndex),

    #[error("uuid: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("unsupported keytype CbsdId")]
    UnsupportedCsbdId,

    #[error("missing key_type")]
    MissingKeyType,

    #[error("missing field: {0}")]
    MissingField(&'static str),

    #[error("unsupported signal level: {0}")]
    SignalLevel(prost::UnknownEnumValue),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioHexSignalLevel {
    pub location: h3o::CellIndex,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
}

#[derive(Debug, Clone)]
pub enum KeyType {
    HotspotKey(PublicKeyBinary),
}

impl From<KeyType> for coverage_object_req_v1::KeyType {
    fn from(kt: KeyType) -> Self {
        match kt {
            KeyType::HotspotKey(key) => coverage_object_req_v1::KeyType::HotspotKey(key.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoverageObject {
    pub pub_key: PublicKeyBinary,
    pub uuid: Uuid,
    pub key_type: KeyType,
    pub coverage_claim_time: DateTime<Utc>,
    pub coverage: Vec<RadioHexSignalLevel>,
    pub indoor: bool,
    pub trust_score: u32,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct CoverageObjectIngestReport {
    pub received_timestamp: DateTime<Utc>,
    pub report: CoverageObject,
}

impl MsgDecode for CoverageObject {
    type Msg = CoverageObjectReqV1;
}

impl MsgDecode for CoverageObjectIngestReport {
    type Msg = CoverageObjectIngestReportV1;
}

impl TryFrom<CoverageObjectIngestReportV1> for CoverageObjectIngestReport {
    type Error = CoverageError;

    fn try_from(v: CoverageObjectIngestReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            received_timestamp: v.received_timestamp.to_timestamp_millis()?,
            report: v
                .report
                .ok_or(CoverageError::MissingField(
                    "coverage_object_ingest_report.report",
                ))?
                .try_into()?,
        })
    }
}

impl TryFrom<CoverageObjectReqV1> for CoverageObject {
    type Error = CoverageError;

    fn try_from(v: CoverageObjectReqV1) -> Result<Self, Self::Error> {
        let coverage: Result<Vec<RadioHexSignalLevel>, CoverageError> = v
            .coverage
            .into_iter()
            .map(RadioHexSignalLevel::try_from)
            .collect();
        Ok(Self {
            pub_key: v.pub_key.into(),
            uuid: Uuid::from_slice(&v.uuid)?,
            key_type: match v.key_type {
                Some(coverage_object_req_v1::KeyType::HotspotKey(key)) => {
                    KeyType::HotspotKey(key.into())
                }
                Some(coverage_object_req_v1::KeyType::CbsdId(_id)) => {
                    return Err(CoverageError::UnsupportedCsbdId);
                }
                None => return Err(CoverageError::MissingKeyType),
            },
            coverage_claim_time: v.coverage_claim_time.to_timestamp()?,
            coverage: coverage?,
            indoor: v.indoor,
            trust_score: v.trust_score,
            signature: v.signature,
        })
    }
}

impl TryFrom<RadioHexSignalLevelProto> for RadioHexSignalLevel {
    type Error = CoverageError;

    fn try_from(v: RadioHexSignalLevelProto) -> Result<Self, Self::Error> {
        Ok(Self {
            signal_level: prost_enum(v.signal_level, CoverageError::SignalLevel)?,
            signal_power: v.signal_power,
            location: v.location.parse()?,
        })
    }
}

impl From<RadioHexSignalLevel> for RadioHexSignalLevelProto {
    fn from(rhsl: RadioHexSignalLevel) -> RadioHexSignalLevelProto {
        RadioHexSignalLevelProto {
            signal_level: rhsl.signal_level as i32,
            signal_power: rhsl.signal_power,
            location: rhsl.location.to_string(),
        }
    }
}
