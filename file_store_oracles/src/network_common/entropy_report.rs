use chrono::{DateTime, Utc};
use file_store::traits::{
    MsgDecode, TimestampDecode, TimestampDecodeError, TimestampDecodeResult, TimestampEncode,
};
use helium_proto::EntropyReportV1;
use serde::Serialize;

use crate::traits::MsgTimestamp;

#[derive(thiserror::Error, Debug)]
pub enum EntropyReportError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),
}

#[derive(Serialize, Clone, Debug)]
pub struct EntropyReport {
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
    pub version: u32,
}

impl MsgTimestamp<u64> for EntropyReport {
    fn timestamp(&self) -> u64 {
        self.timestamp.encode_timestamp_millis()
    }
}

impl MsgTimestamp<TimestampDecodeResult> for EntropyReportV1 {
    fn timestamp(&self) -> TimestampDecodeResult {
        self.timestamp.to_timestamp()
    }
}

impl MsgDecode for EntropyReport {
    type Msg = EntropyReportV1;
}

impl TryFrom<EntropyReportV1> for EntropyReport {
    type Error = EntropyReportError;

    fn try_from(v: EntropyReportV1) -> Result<Self, Self::Error> {
        let timestamp = v.timestamp()?;
        Ok(Self {
            data: v.data,
            version: v.version,
            timestamp,
        })
    }
}
