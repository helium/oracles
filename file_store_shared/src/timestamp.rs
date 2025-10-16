use crate::{error::DecodeError, Result};
use chrono::{DateTime, TimeZone, Utc};

pub trait TimestampDecode {
    fn to_timestamp(self) -> Result<DateTime<Utc>>;
    fn to_timestamp_millis(self) -> Result<DateTime<Utc>>;
    fn to_timestamp_nanos(self) -> Result<DateTime<Utc>>;
}

impl TimestampDecode for u64 {
    fn to_timestamp(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Utc.timestamp_opt(decoded, 0)
            .single()
            .ok_or_else(|| DecodeError::invalid_timestamp(self))
    }

    fn to_timestamp_millis(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Utc.timestamp_millis_opt(decoded)
            .single()
            .ok_or_else(|| DecodeError::invalid_timestamp(self))
    }

    fn to_timestamp_nanos(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Ok(Utc.timestamp_nanos(decoded))
    }
}

pub trait TimestampEncode {
    fn encode_timestamp(&self) -> u64;
    fn encode_timestamp_millis(&self) -> u64;
    fn encode_timestamp_nanos(&self) -> u64;
}

impl TimestampEncode for DateTime<Utc> {
    fn encode_timestamp(&self) -> u64 {
        self.timestamp() as u64
    }

    fn encode_timestamp_millis(&self) -> u64 {
        self.timestamp_millis() as u64
    }

    fn encode_timestamp_nanos(&self) -> u64 {
        self.timestamp_nanos_opt()
            .expect("value can not be represented in a timestamp with nanosecond precision.")
            as u64
    }
}
