use crate::{
    error::DecodeError,
    traits::{MsgDecode, TimestampDecode},
    Error, Result,
};
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    CoverageObjectIngestReportV1, CoverageObjectReqV1,
    RadioHexSignalLevel as RadioHexSignalLevelProto, SignalLevel,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RadioHexSignalLevel {
    pub location: h3o::CellIndex,
    pub signal_level: SignalLevel,
    pub signal_power: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CoverageObject {
    pub pub_key: PublicKeyBinary,
    pub uuid: Uuid,
    pub cbsd_id: String,
    pub coverage_claim_time: DateTime<Utc>,
    pub indoor: bool,
    pub coverage: Vec<RadioHexSignalLevel>,
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
            cbsd_id: v.cbsd_id,
            coverage_claim_time: v.coverage_claim_time.to_timestamp()?,
            indoor: v.indoor,
            coverage: coverage?,
        })
    }
}

impl TryFrom<RadioHexSignalLevelProto> for RadioHexSignalLevel {
    type Error = Error;

    fn try_from(v: RadioHexSignalLevelProto) -> Result<Self> {
        Ok(Self {
            signal_level: SignalLevel::from_i32(v.signal_level).ok_or_else(|| {
                DecodeError::unsupported_signal_level("coverage_object_req_v1", v.signal_level)
            })?,
            signal_power: v.signal_power,
            location: v.location.parse().map_err(DecodeError::from)?,
        })
    }
}

impl Into<RadioHexSignalLevelProto> for RadioHexSignalLevel {
    fn into(self) -> RadioHexSignalLevelProto {
        RadioHexSignalLevelProto {
            signal_level: self.signal_level as i32,
            signal_power: self.signal_power,
            location: self.location.to_string(),
        }
    }
}
