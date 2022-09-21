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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        subnetwork_reward::sorted_rewards,
        token_type::BlockchainTokenTypeV1,
        traits::{TxnSign, B64},
        Keypair,
    };
    use helium_crypto::PublicKey;
    use helium_proto::SubnetworkReward;
    use std::str::FromStr;

    #[test]
    fn roundtrip() {
        let mut rewards: Vec<SubnetworkReward> = vec![];
        let owner1 = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .unwrap()
            .to_vec();

        let owner2 = PublicKey::from_str("1ZY8nysZz485qaaFEjCdBvduuUV326AZ5zeNFieZg3KrmR2VuPv")
            .unwrap()
            .to_vec();

        let r1 = SubnetworkReward {
            account: owner1,
            amount: 100,
        };
        let r2 = SubnetworkReward {
            account: owner2,
            amount: 200,
        };
        rewards.push(r1);
        rewards.push(r2);
        rewards = sorted_rewards(rewards);

        let mut txn = BlockchainTxnSubnetworkRewardsV1 {
            rewards,
            token_type: BlockchainTokenTypeV1::from(helium_proto::BlockchainTokenTypeV1::Mobile)
                .into(),
            end_epoch: 2000,
            start_epoch: 1000,
            reward_server_signature: vec![],
        };

        let keypair_b64 = "EeNwbGXheUq4frT05EJwMtvGuz8zHyajOaN2h5yz5M9A58pZdf9bLayp8Ex6x0BkGxREleQnTNwOTyT2vPL0i1_nyll1_1strKnwTHrHQGQbFESV5CdM3A5PJPa88vSLXw";
        let kp = Keypair::try_from(
            Vec::from_b64_url(keypair_b64)
                .expect("unable to get raw keypair")
                .as_ref(),
        )
        .expect("unable to get keypair");

        let sig = txn.sign(&kp).expect("unable to sign txn");
        txn.reward_server_signature = sig.clone();

        // Check that we can verify this signature
        assert!(txn.verify(&kp.public_key(), &sig).is_ok());

        let txn_hash = txn.hash().expect("unable to hash");
        let txn_hash_b64url = txn_hash.to_b64_url().expect("unable to b64url enc");

        // This hash is taken from blockchain-node by constructing the exact same txn
        assert_eq!(
            txn_hash_b64url,
            "e5AfNbDmdTUAe6jb8uVtYqXFyfdouKcdeWw2iW13n9c"
        );
    }
}
