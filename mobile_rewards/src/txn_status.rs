use crate::{Error, Result};
use helium_proto::services::transaction::TxnStatus as ProtoTxnStatus;
use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnStatus(pub ProtoTxnStatus);

impl From<ProtoTxnStatus> for TxnStatus {
    fn from(status: ProtoTxnStatus) -> Self {
        Self(status)
    }
}

impl Display for TxnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str_name())
    }
}

impl TryFrom<i32> for TxnStatus {
    type Error = Error;
    fn try_from(value: i32) -> Result<Self> {
        match ProtoTxnStatus::from_i32(value) {
            Some(v) => Ok(Self::from(v)),
            None => Err(Error::NotFound(format!("unknown value {value}"))),
        }
    }
}

impl From<TxnStatus> for i32 {
    fn from(status: TxnStatus) -> i32 {
        status.0 as i32
    }
}
