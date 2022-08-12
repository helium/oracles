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
                let mut buf = vec![];
                let mut txn = self.clone();
                $(txn.$sig = vec![];)+
                txn.encode(& mut buf)?;
                Ok(Sha256::digest(&buf).to_vec())
            }
        }
    }
}

impl_hash!(BlockchainTxnSubnetworkRewardsV1, reward_server_signature);

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        subnetwork_reward::sorted_rewards, token_type::BlockchainTokenTypeV1, traits::b64::B64,
        PublicKey,
    };
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

        let txn = BlockchainTxnSubnetworkRewardsV1 {
            rewards,
            token_type: BlockchainTokenTypeV1::from(helium_proto::BlockchainTokenTypeV1::Mobile)
                .into(),
            end_epoch: 1660254319,
            start_epoch: 1660252975,
            reward_server_signature: vec![],
        };

        // This hash is taken from blockchain-node by constructing the exact same txn
        assert_eq!(
            txn.hash().unwrap().to_b64_url().unwrap(),
            "ut-0ubcZLZwKH1l_rQfQCBm_frdV2T6DERvbqv7h9mA"
        );
    }
}
