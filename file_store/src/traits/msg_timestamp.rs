use crate::{error::DecodeError, Result};
use chrono::{DateTime, TimeZone, Utc};

pub trait MsgTimestamp {
    fn to_timestamp(self) -> Result<DateTime<Utc>>;
    fn to_timestamp_millis(self) -> Result<DateTime<Utc>>;
    fn to_timestamp_nanos(self) -> Result<DateTime<Utc>>;
}

impl MsgTimestamp for u64 {
    fn to_timestamp(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Ok(Utc.timestamp(decoded, 0))
    }

    fn to_timestamp_millis(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Ok(Utc.timestamp_millis(decoded))
    }

    fn to_timestamp_nanos(self) -> Result<DateTime<Utc>> {
        let decoded = i64::try_from(self).map_err(DecodeError::from)?;
        Ok(Utc.timestamp_nanos(decoded))
    }
}
