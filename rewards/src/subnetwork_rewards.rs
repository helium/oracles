use crate::{
    cli::print_json,
    datetime_from_epoch,
    follower::FollowerService,
    reward_share::{cell_shares, gather_shares},
    subnetwork_reward::sorted_rewards,
    token_type::BlockchainTokenTypeV1,
    traits::{TxnHash, TxnSign, B64},
    Error, Keypair, Result,
};
use chrono::{DateTime, Duration, Utc};
use futures::stream::{self, StreamExt};
use helium_proto::{
    BlockchainTokenTypeV1 as ProtoTokenType, BlockchainTxnSubnetworkRewardsV1,
    SubnetworkReward as ProtoSubnetworkReward,
};
use poc_store::{FileStore, FileType};
use serde::Serialize;
use serde_json::json;
use std::cmp::min;

// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;
// minimum number of heartbeats to consider for rewarding
pub const MIN_PER_CELL_TYPE_HEARTBEATS: u64 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct SubnetworkRewards(Vec<ProtoSubnetworkReward>);

impl SubnetworkRewards {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub async fn from_period(
        store: FileStore,
        follower_service: FollowerService,
        after_utc: DateTime<Utc>,
        before_utc: DateTime<Utc>,
    ) -> Result<Option<Self>> {
        if let Some(rewards) = get_rewards(store, follower_service, after_utc, before_utc).await? {
            return Ok(Some(Self(rewards)));
        }
        Ok(None)
    }

    pub async fn from_last_reward_end_time(
        store: FileStore,
        follower_service: FollowerService,
        last_reward_end_time: i64,
    ) -> Result<Option<Self>> {
        let (after_utc, before_utc) = get_time_range(last_reward_end_time);
        if let Some(rewards) = get_rewards(store, follower_service, after_utc, before_utc).await? {
            return Ok(Some(Self(rewards)));
        }
        Ok(None)
    }
}

impl From<SubnetworkRewards> for Vec<ProtoSubnetworkReward> {
    fn from(subnetwork_rewards: SubnetworkRewards) -> Self {
        sorted_rewards(subnetwork_rewards.0)
    }
}

pub fn construct_txn(
    keypair: &Keypair,
    rewards: SubnetworkRewards,
    start_epoch: i64,
    end_epoch: i64,
) -> Result<(BlockchainTxnSubnetworkRewardsV1, String)> {
    let mut txn = BlockchainTxnSubnetworkRewardsV1 {
        rewards: rewards.into(),
        token_type: BlockchainTokenTypeV1::from(ProtoTokenType::Mobile).into(),
        start_epoch: start_epoch as u64,
        end_epoch: end_epoch as u64,
        reward_server_signature: vec![],
    };
    txn.reward_server_signature = txn.sign(keypair)?;

    let txn_hash = txn.hash()?;
    let txn_hash_str = txn_hash.to_b64_url()?;
    Ok((txn, txn_hash_str))
}

async fn get_rewards(
    store: FileStore,
    mut _follower_service: FollowerService,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<Option<Vec<ProtoSubnetworkReward>>> {
    if before_utc <= after_utc {
        tracing::error!(
            "cannot reward future period, before: {:?}, after: {:?}",
            before_utc,
            after_utc
        );
        return Err(Error::NotFound("cannot reward future".to_string()));
    }

    let file_list = store
        .list_all(FileType::CellHeartbeat, after_utc, before_utc)
        .await?;

    print_json(&json!({ "file_list": file_list }))?;

    metrics::histogram!("reward_server_processed_files", file_list.len() as f64);
    let mut stream = store.source(stream::iter(file_list).map(Ok).boxed());

    let shares = gather_shares(
        &mut stream,
        after_utc.timestamp() as u64,
        before_utc.timestamp() as u64,
    )
    .await?;

    print_json(&json!({ "shares": shares }))?;

    let cell_shares = cell_shares(shares);

    print_json(&json!({ "cell_shares": cell_shares }))?;

    Ok(None)
}

pub fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = datetime_from_epoch(last_reward_end_time);
    let now = Utc::now();
    let stop_utc = now - Duration::minutes(DEFAULT_LOOKUP_DELAY);
    let start_utc = min(after_utc, stop_utc);
    (start_utc, stop_utc)
}

#[cfg(test)]
mod test {
    use crate::Mobile;
    use async_trait::async_trait;
    use rust_decimal_macros::dec;
    use std::str::FromStr;

    use super::*;

    struct FixedOwnerResolver {
        owner: PublicKey,
    }

    #[async_trait]
    impl OwnerResolver for FixedOwnerResolver {
        async fn resolve_owner(&mut self, _address: &PublicKey) -> Result<Option<PublicKey>> {
            Ok(Some(self.owner.clone()))
        }
    }

    #[tokio::test]
    async fn check_rewards() {
        // SercommIndoor
        let g1 = PublicKey::from_str("11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL")
            .expect("unable to construct pubkey")
            .to_vec();
        // Nova430I
        let g2 = PublicKey::from_str("118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc")
            .expect("unable to construct pubkey")
            .to_vec();
        // SercommOutdoor
        let g3 = PublicKey::from_str("112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ")
            .expect("unable to construct pubkey")
            .to_vec();
        // Nova436H
        let g4 = PublicKey::from_str("11k712d9dSb8CAujzS4PdC7Hi8EEBZWsSnt4Zr1hgke4e1Efiag")
            .expect("unable to construct pubkey")
            .to_vec();

        let mut c1 = CbsdCounter::new();
        c1.insert("P27-SCE4255W2107CW5000014".to_string(), 4);
        let mut c2 = CbsdCounter::new();
        c2.insert("2AG32PBS3101S1202000464223GY0153".to_string(), 5);
        let mut c3 = CbsdCounter::new();
        c3.insert("P27-SCO4255PA102206DPT000207".to_string(), 6);
        let mut c4 = CbsdCounter::new();
        c4.insert("2AG32MBS3100196N1202000240215KY0184".to_string(), 3);

        let mut counter = Counter::new();
        counter.insert(g1, c1);
        counter.insert(g2, c2);
        counter.insert(g3, c3);
        counter.insert(g4, c4);

        let mut expected_model: Model = HashMap::new();
        expected_model.insert(CellType::SercommIndoor, 1);
        expected_model.insert(CellType::Nova436H, 1);
        expected_model.insert(CellType::SercommOutdoor, 1);
        expected_model.insert(CellType::Nova430I, 1);

        let generated_model = generate_model(&counter);
        assert_eq!(generated_model, expected_model);

        let mut expected_emitted: Emission = HashMap::new();
        expected_emitted.insert(
            CellType::SercommIndoor,
            Mobile::from(dec!(10000000.00000000)),
        );
        expected_emitted.insert(
            CellType::SercommOutdoor,
            Mobile::from(dec!(25000000.00000000)),
        );
        expected_emitted.insert(CellType::Nova430I, Mobile::from(dec!(25000000.00000000)));
        expected_emitted.insert(CellType::Neutrino430, Mobile::from(dec!(0.00000000)));
        expected_emitted.insert(CellType::Nova436H, Mobile::from(dec!(40000000.00000000)));

        let after_utc = Utc::now();
        // let before_utc = after_utc - Duration::hours(24);
        let emitted =
            get_emissions_per_model(&generated_model, after_utc, Duration::hours(24)).unwrap();
        assert_eq!(emitted, expected_emitted);

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let owner_rewards =
            OwnerRewards::new(&mut owner_resolver, counter, generated_model, emitted)
                .await
                .expect("unable to create owner rewards");

        let rewards: Vec<ProtoSubnetworkReward> = SubnetworkRewards::from(owner_rewards).into();
        assert_eq!(1, rewards.len()); // there is only one fixed owner

        let tot_rewards = rewards.iter().fold(0, |acc, reward| acc + reward.amount);

        // 100M in bones
        assert_eq!(10000000000000000, tot_rewards);

        let keypair_b64 = "EeNwbGXheUq4frT05EJwMtvGuz8zHyajOaN2h5yz5M9A58pZdf9bLayp8Ex6x0BkGxREleQnTNwOTyT2vPL0i1_nyll1_1strKnwTHrHQGQbFESV5CdM3A5PJPa88vSLXw";
        let kp = Keypair::try_from(
            Vec::from_b64_url(keypair_b64)
                .expect("unable to get raw keypair")
                .as_ref(),
        )
        .expect("unable to get keypair");
        let (_txn, txn_hash_str) = construct_txn(&kp, SubnetworkRewards(rewards), 1000, 1010)
            .expect("unable to construct txn");

        // This is taken from a blockchain-node, constructing the exact same txn
        assert_eq!(
            "hpnFwsGQQohIlAYGyaWJ-j3Hs8kIPeskh09qEkoL3I4".to_string(),
            txn_hash_str
        );
    }
}
