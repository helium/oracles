use crate::{
    datetime_from_epoch,
    emissions::{get_emissions_per_model, Emission, Model},
    follower::FollowerService,
    subnetwork_reward::sorted_rewards,
    token_type::BlockchainTokenTypeV1,
    traits::{OwnerResolver, TxnHash, TxnSign, B64},
    CellType, Error, Keypair, Mobile, PublicKey, Result,
};
use chrono::{DateTime, Duration, Utc};
use futures::stream::StreamExt;
use helium_proto::{
    services::poc_mobile::CellHeartbeatReqV1, BlockchainTokenTypeV1 as ProtoTokenType,
    BlockchainTxnSubnetworkRewardsV1, Message, SubnetworkReward as ProtoSubnetworkReward,
};
use poc_store::{
    file_source::{store_source, ByteStream},
    FileStore, FileType,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{cmp::min, collections::HashMap};

// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;
// minimum number of heartbeats to consider for rewarding
pub const MIN_PER_CELL_TYPE_HEARTBEATS: u64 = 1;

// key: cbsd_id, val: # of heartbeats
pub type CbsdCounter = HashMap<String, u64>;
// key: gateway_pubkeybin, val: CbsdCounter
pub type Counter = HashMap<Vec<u8>, CbsdCounter>;

#[derive(Debug, Clone)]
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
    ) -> Result<Self> {
        let rewards = get_rewards(store, follower_service, after_utc, before_utc).await?;
        Ok(Self(rewards))
    }

    pub async fn from_last_reward_end_time(
        store: FileStore,
        follower_service: FollowerService,
        last_reward_end_time: i64,
    ) -> Result<Self> {
        let (after_utc, before_utc) = get_time_range(last_reward_end_time);
        let rewards = get_rewards(store, follower_service, after_utc, before_utc).await?;
        Ok(Self(rewards))
    }
}

impl From<SubnetworkRewards> for Vec<ProtoSubnetworkReward> {
    fn from(subnetwork_rewards: SubnetworkRewards) -> Self {
        subnetwork_rewards.0
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
    mut follower_service: FollowerService,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<Vec<ProtoSubnetworkReward>> {
    if before_utc <= after_utc {
        tracing::error!(
            "cannot reward future period, before: {:?}, after: {:?}",
            before_utc,
            after_utc
        );
        return Err(Error::NotFound("cannot reward future".to_string()));
    }

    let file_list = store
        .list(
            "poc5g-ingest",
            FileType::CellHeartbeat,
            after_utc,
            before_utc,
        )
        .await?;
    let mut stream = store_source(store, "poc5g-ingest", file_list);
    let counter = count_heartbeats(&mut stream).await?;
    let model = generate_model(&counter);
    let emitted = get_emissions_per_model(&model, after_utc, before_utc - after_utc);
    let rewards = construct_rewards(&mut follower_service, counter, model, emitted).await?;
    Ok(rewards)
}

async fn count_heartbeats(stream: &mut ByteStream) -> Result<Counter> {
    // count heartbeats for this input stream
    let mut counter: Counter = HashMap::new();
    while let Some(Ok(msg)) = stream.next().await {
        let CellHeartbeatReqV1 {
            pub_key, cbsd_id, ..
        } = CellHeartbeatReqV1::decode(msg)?;
        // Think of Counter more like a 2-dimensional sparse matrix and less like
        // a hashmap of hashmaps.
        let count = counter
            .entry(pub_key)
            .or_insert_with(HashMap::new)
            .entry(cbsd_id)
            .or_insert(0);
        *count += 1;
    }
    Ok(counter)
}

pub fn generate_model(counter: &Counter) -> Model {
    // how many total mobile each cell_type needs to get
    // (cell_type, mobile), (...)...
    let mut model: HashMap<CellType, u64> = HashMap::new();
    for (cbsd_id, single_hotspot_count) in counter
        .iter()
        .flat_map(|(_gw_pubkey_bin, sub_map)| sub_map.iter())
    {
        if let Some(ct) = CellType::from_cbsd_id(cbsd_id) {
            let count = model.entry(ct).or_insert(0);
            // This cell type only gets added to the model if it has more than MIN_PER_CELL_TYPE_HEARTBEATS
            if *single_hotspot_count >= MIN_PER_CELL_TYPE_HEARTBEATS {
                *count += 1
            }
        }
    }
    model
}

pub fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = datetime_from_epoch(last_reward_end_time);
    let now = Utc::now();
    let stop_utc = now - Duration::minutes(DEFAULT_LOOKUP_DELAY);
    let start_utc = min(after_utc, stop_utc);
    (start_utc, stop_utc)
}

pub async fn construct_rewards<F>(
    owner_resolver: &mut F,
    counter: Counter,
    model: Model,
    emitted: Emission,
) -> Result<Vec<ProtoSubnetworkReward>>
where
    F: OwnerResolver,
{
    let mut rewards: Vec<ProtoSubnetworkReward> = Vec::with_capacity(counter.len());

    for (gw_pubkey_bin, per_cell_cnt) in counter.into_iter() {
        if let Ok(gw_pubkey) = PublicKey::try_from(gw_pubkey_bin) {
            let owner = owner_resolver.resolve_owner(&gw_pubkey).await?;
            if owner.is_none() {
                continue;
            }

            let mut reward_acc = dec!(0);

            for (cbsd_id, cnt) in per_cell_cnt {
                if cnt < MIN_PER_CELL_TYPE_HEARTBEATS {
                    continue;
                }

                let cell_type = if let Some(cell_type) = CellType::from_cbsd_id(&cbsd_id) {
                    cell_type
                } else {
                    continue;
                };

                if let (Some(total_count), Some(total_reward)) =
                    (model.get(&cell_type), emitted.get(&cell_type))
                {
                    let amt = total_reward.get_decimal() / Decimal::from(*total_count);
                    reward_acc += amt;
                }
            }
            rewards.push(ProtoSubnetworkReward {
                account: owner.unwrap().to_vec(),
                amount: u64::from(Mobile::from(reward_acc)),
            });
        }
    }
    Ok(sorted_rewards(rewards))
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
    #[ignore = "credentials required"]
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
        let emitted = get_emissions_per_model(&generated_model, after_utc, Duration::hours(24));
        assert_eq!(emitted, expected_emitted);

        let test_owner = PublicKey::from_str("1ay5TAKuQDjLS6VTpoWU51p3ik3Sif1b3DWRstErqkXFJ4zuG7r")
            .expect("unable to get test pubkey");
        let mut owner_resolver = FixedOwnerResolver { owner: test_owner };

        let rewards = construct_rewards(&mut owner_resolver, counter, generated_model, emitted)
            .await
            .expect("unable to construct rewards");
        assert_eq!(4, rewards.len());

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
            "d3VgXagj8fn-iLPqFW5JSWtUu7O9RV0Uce31jmviXs0".to_string(),
            txn_hash_str
        );
    }
}
