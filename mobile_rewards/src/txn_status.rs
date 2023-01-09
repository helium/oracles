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

#[derive(thiserror::Error, Debug)]
#[error("no such txn status: {0}")]
pub struct NoSuchTxnStatus(i32);

impl TryFrom<i32> for TxnStatus {
    type Error = NoSuchTxnStatus;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match ProtoTxnStatus::from_i32(value) {
            Some(v) => Ok(Self::from(v)),
            None => Err(NoSuchTxnStatus(value)),
        }
    }
}

impl From<TxnStatus> for i32 {
    fn from(status: TxnStatus) -> i32 {
        status.0 as i32
    }
}
