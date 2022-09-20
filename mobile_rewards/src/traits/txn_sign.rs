use crate::{Keypair, Result};
use helium_crypto::{PublicKey, Verify};
use helium_proto::{BlockchainTxnSubnetworkRewardsV1, Message};

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
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                keypair.sign(&txn.encode_to_vec())
            }

            fn verify(&self, pubkey: &PublicKey, signature: &[u8]) -> Result {
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                pubkey.verify(&txn.encode_to_vec(), &signature).map_err(|err| err.into())
            }
        }
    }
}

impl_sign!(BlockchainTxnSubnetworkRewardsV1, reward_server_signature);
