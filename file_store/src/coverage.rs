use crate::{
    error::DecodeError,
    traits::{MsgDecode, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    coverage_object_req_v1, CoverageObjectIngestReportV1, CoverageObjectReqV1,
    RadioHexSignalLevel as RadioHexSignalLevelProto, SignalLevel,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioHexSignalLevel {
    pub location: h3o::CellIndex,
    pub signal_level: SignalLevel,
    pub signal_power: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KeyType {
    CbsdId(String),
    HotspotKey(PublicKeyBinary),
}

impl From<KeyType> for coverage_object_req_v1::KeyType {
    fn from(kt: KeyType) -> Self {
        match kt {
            KeyType::CbsdId(id) => coverage_object_req_v1::KeyType::CbsdId(id),
            KeyType::HotspotKey(key) => coverage_object_req_v1::KeyType::HotspotKey(key.into()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    type Error = Error;

    fn try_from(v: CoverageObjectIngestReportV1) -> Result<Self> {
        Ok(Self {
            received_timestamp: v.received_timestamp.to_timestamp_millis()?,
            report: v
                .report
                .ok_or_else(|| Error::not_found("ingest coverage object report"))?
                .try_into()?,
        })
    }
}

impl TryFrom<CoverageObjectReqV1> for CoverageObject {
    type Error = Error;

    fn try_from(v: CoverageObjectReqV1) -> Result<Self> {
        let coverage: Result<Vec<RadioHexSignalLevel>> = v
            .coverage
            .into_iter()
            .map(RadioHexSignalLevel::try_from)
            .collect();
        Ok(Self {
            pub_key: v.pub_key.into(),
            uuid: Uuid::from_slice(&v.uuid).map_err(DecodeError::from)?,
            key_type: match v.key_type {
                Some(coverage_object_req_v1::KeyType::CbsdId(id)) => KeyType::CbsdId(id),
                Some(coverage_object_req_v1::KeyType::HotspotKey(key)) => {
                    KeyType::HotspotKey(key.into())
                }
                None => return Err(Error::NotFound("key_type".to_string())),
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
    type Error = Error;

    fn try_from(v: RadioHexSignalLevelProto) -> Result<Self> {
        Ok(Self {
            signal_level: SignalLevel::try_from(v.signal_level).map_err(|_| {
                DecodeError::unsupported_signal_level("coverage_object_req_v1", v.signal_level)
            })?,
            signal_power: v.signal_power,
            location: v.location.parse().map_err(DecodeError::from)?,
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
