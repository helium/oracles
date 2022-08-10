use crate::{Error, Result};
use std::fmt::Display;

#[derive(Clone)]
pub struct BlockchainTokenTypeV1(pub helium_proto::BlockchainTokenTypeV1);

impl From<helium_proto::BlockchainTokenTypeV1> for BlockchainTokenTypeV1 {
    fn from(tt: helium_proto::BlockchainTokenTypeV1) -> Self {
        Self(tt)
    }
}

impl Display for BlockchainTokenTypeV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            helium_proto::BlockchainTokenTypeV1::Hnt => write!(f, "{:?}", "Hnt"),
            helium_proto::BlockchainTokenTypeV1::Hst => write!(f, "{:?}", "Hst"),
            helium_proto::BlockchainTokenTypeV1::Mobile => write!(f, "{:?}", "Mobile"),
            helium_proto::BlockchainTokenTypeV1::Iot => write!(f, "{:?}", "Iot"),
        }
    }
}

impl TryFrom<i32> for BlockchainTokenTypeV1 {
    type Error = Error;
    fn try_from(value: i32) -> Result<Self> {
        match value {
            0 => Ok(BlockchainTokenTypeV1::from(
                helium_proto::BlockchainTokenTypeV1::Hnt,
            )),
            1 => Ok(BlockchainTokenTypeV1::from(
                helium_proto::BlockchainTokenTypeV1::Hst,
            )),
            2 => Ok(BlockchainTokenTypeV1::from(
                helium_proto::BlockchainTokenTypeV1::Mobile,
            )),
            3 => Ok(BlockchainTokenTypeV1::from(
                helium_proto::BlockchainTokenTypeV1::Iot,
            )),
            v => Err(Error::NotFound(format!("unknown value {}", v))),
        }
    }
}

impl From<BlockchainTokenTypeV1> for i32 {
    fn from(tt: BlockchainTokenTypeV1) -> i32 {
        match tt.0 {
            helium_proto::BlockchainTokenTypeV1::Hnt => 0,
            helium_proto::BlockchainTokenTypeV1::Hst => 1,
            helium_proto::BlockchainTokenTypeV1::Mobile => 2,
            helium_proto::BlockchainTokenTypeV1::Iot => 3,
        }
    }
}
