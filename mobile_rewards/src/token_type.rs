use std::fmt::Display;

#[derive(Clone, Debug)]
pub struct BlockchainTokenTypeV1(pub helium_proto::BlockchainTokenTypeV1);

impl From<helium_proto::BlockchainTokenTypeV1> for BlockchainTokenTypeV1 {
    fn from(tt: helium_proto::BlockchainTokenTypeV1) -> Self {
        Self(tt)
    }
}

impl Display for BlockchainTokenTypeV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str_name())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("{0} is not a blockchain token type")]
pub struct NoSuchBlockchainTokenType(i32);

impl TryFrom<i32> for BlockchainTokenTypeV1 {
    type Error = NoSuchBlockchainTokenType;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match helium_proto::BlockchainTokenTypeV1::from_i32(value) {
            Some(v) => Ok(Self::from(v)),
            None => Err(NoSuchBlockchainTokenType(value)),
        }
    }
}

impl From<BlockchainTokenTypeV1> for i32 {
    fn from(tt: BlockchainTokenTypeV1) -> i32 {
        tt.0 as i32
    }
}
