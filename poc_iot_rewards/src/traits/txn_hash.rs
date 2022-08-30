use crate::Result;
use helium_proto::{BlockchainTxnSubnetworkRewardsV1, Message};
use sha2::{Digest, Sha256};

pub trait TxnHash: Message + std::clone::Clone {
    fn hash(&self) -> Result<Vec<u8>>
    where
        Self: std::marker::Sized;
}

macro_rules! impl_hash {
    ($txn_type:ty, $( $sig: ident ),+ ) => {
        impl TxnHash for $txn_type {
            fn hash(&self) -> Result<Vec<u8>> {
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                Ok(Sha256::digest(&txn.encode_to_vec()).to_vec())
            }
        }
    }
}

impl_hash!(BlockchainTxnSubnetworkRewardsV1, reward_server_signature);
