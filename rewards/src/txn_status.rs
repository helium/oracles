use crate::{Error, Result};
use helium_proto::TxnStatus as ProtoTxnStatus;
use std::fmt::Display;

#[derive(Clone, Debug, PartialEq)]
pub struct TxnStatus(pub ProtoTxnStatus);

impl From<ProtoTxnStatus> for TxnStatus {
    fn from(status: ProtoTxnStatus) -> Self {
        Self(status)
    }
}

impl Display for TxnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ProtoTxnStatus::Pending => write!(f, "{:?}", "pending"),
            ProtoTxnStatus::NotFound => write!(f, "{:?}", "not_found"),
        }
    }
}

impl TryFrom<i32> for TxnStatus {
    type Error = Error;
    fn try_from(value: i32) -> Result<Self> {
        match value {
            0 => Ok(TxnStatus::from(ProtoTxnStatus::Pending)),
            1 => Ok(TxnStatus::from(ProtoTxnStatus::NotFound)),
            v => Err(Error::NotFound(format!("unknown value {}", v))),
        }
    }
}

impl From<TxnStatus> for i32 {
    fn from(status: TxnStatus) -> i32 {
        match status.0 {
            ProtoTxnStatus::Pending => 0,
            ProtoTxnStatus::NotFound => 1,
        }
    }
}
