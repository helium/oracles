use crate::{Keypair, Result, PublicKey};
use helium_proto::{
    BlockchainTxnSubnetworkRewardsV1, Message
};
use helium_crypto::Verify;

pub trait TxnSign: Message + std::clone::Clone {
    fn sign(&self, keypair: &Keypair) -> Result<Vec<u8>>
    where
        Self: std::marker::Sized;
    fn verify(&self, pubkey: &PublicKey, signature: &[u8]) -> Result;
}

macro_rules! impl_sign {
    ($txn_type:ty, $( $sig: ident ),+ ) => {
        impl TxnSign for $txn_type {
            fn sign(&self, keypair: &Keypair) -> Result<Vec<u8>> {
                let mut buf = vec![];
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                txn.encode(& mut buf)?;
                keypair.sign(&buf)
            }

            fn verify(&self, pubkey: &PublicKey, signature: &[u8]) -> Result {
                let mut buf = vec![];
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                txn.encode(& mut buf)?;
                pubkey.verify(&buf, &signature).map_err(|err| err.into())
            }
        }
    }
}

impl_sign!(BlockchainTxnSubnetworkRewardsV1, reward_server_signature);
