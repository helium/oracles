use chrono::{DateTime, TimeZone, Utc};

// This trait is not being provided by file-store proper
// because it increaseses the chance of running into orphan rules.
//
// A common use is implementing MsgTimestamp for both the prost struct and
// domain struct to ensure the same field is being referenced with the same
// time unit. This becomes not possible if file-store provides the trait. The
// implementer wouldn't own the trait and a high potential they wouldn't own
// the proto struct definition and would be unable to provide half of the
// benefit this trait provides.
//
// If you really want it, here's the trait in it's entirety with some examples,
// put it in your own project.
//
// pub trait MsgTimestamp<R> {
//     fn timestamp(&self) -> R;
// }

// impl MsgTimestamp<TimestampDecodeResult> for ProtoStruct {
//     fn timestamp(&self) -> TimestampDecodeResult {
//         self.received_timestamp.encode_timestamp_millis()
//     }
// }

// impl MsgTimestamp<u64> for DomainStruct {
//     fn timestamp(&self) -> u64 {
//         self.received_timestamp.to_timestamp_millis()
//     }
// }

pub type TimestampDecodeResult = Result<DateTime<Utc>, TimestampDecodeError>;

#[derive(thiserror::Error, Debug)]
pub enum TimestampDecodeError {
    #[error("integer conversion error")]
    FromInt(#[from] std::num::TryFromIntError),

    #[error("invalid seconds: {0}")]
    InvalidSeconds(u64),

    #[error("invalid millis: {0}")]
    InvalidMillis(u64),
}

pub trait TimestampDecode {
    fn to_timestamp(self) -> TimestampDecodeResult;
    fn to_timestamp_millis(self) -> TimestampDecodeResult;
    fn to_timestamp_nanos(self) -> TimestampDecodeResult;
}

impl TimestampDecode for u64 {
    fn to_timestamp(self) -> TimestampDecodeResult {
        let decoded = i64::try_from(self)?;
        Utc.timestamp_opt(decoded, 0)
            .single()
            .ok_or(TimestampDecodeError::InvalidSeconds(self))
    }

    fn to_timestamp_millis(self) -> TimestampDecodeResult {
        let decoded = i64::try_from(self)?;
        Utc.timestamp_millis_opt(decoded)
            .single()
            .ok_or(TimestampDecodeError::InvalidMillis(self))
    }

    fn to_timestamp_nanos(self) -> TimestampDecodeResult {
        let decoded = i64::try_from(self)?;
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
