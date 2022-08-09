use crate::{
    datetime_from_epoch,
    emissions::{self, Emission},
    follower::{FollowerService, Meta},
    pending_txn::PendingTxn,
    CellType, ConsensusTxnTrigger, PublicKey, Result,
};
use chrono::{DateTime, Duration, Utc};
use emissions::{get_emissions_per_model, Model};
use futures::stream::StreamExt;
use helium_proto::{
    services::poc_mobile::CellHeartbeatReqV1, BlockchainTokenTypeV1,
    BlockchainTxnSubnetworkRewardsV1, Message, SubnetworkReward,
};
use poc_store::{
    file_source::{store_source, Stream},
    FileStore, FileType,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sqlx::{Pool, Postgres};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::broadcast;

// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;
// minimum number of heartbeats to consider for rewarding
pub const MIN_PER_CELL_TYPE_HEARTBEATS: u64 = 3;

//                         cbsd_id num_heartbeats
type CbsdCounter = HashMap<String, u64>;
type Counter = HashMap<Vec<u8>, CbsdCounter>;
type Rewards = Vec<SubnetworkReward>;

pub struct Server {
    trigger_receiver: broadcast::Receiver<ConsensusTxnTrigger>,
    pool: Pool<Postgres>,
}

impl Server {
    pub async fn new(
        pool: Pool<Postgres>,
        trigger_receiver: broadcast::Receiver<ConsensusTxnTrigger>,
    ) -> Result<Self> {
        let result = Self {
            pool,
            trigger_receiver,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting rewards server");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping rewards server");
                return Ok(());
            }
            tokio::select! {
                _ = shutdown.clone() => (),
                trigger = self.trigger_receiver.recv() => {
                    if let Ok(trigger) = trigger {
                        if self.handle_trigger(trigger).await.is_err() {
                            tracing::error!("failed to handle trigger!")
                        }
                    } else {
                        tracing::error!("failed to recv trigger!")
                    }
                }
            }
        }
    }

    pub async fn handle_trigger(&mut self, trigger: ConsensusTxnTrigger) -> Result {
        tracing::info!("chain trigger received {:#?}", trigger);

        match PendingTxn::get_all_failed_pending_txns(&self.pool).await {
            Ok(Some(failed_pending_txns)) => {
                tracing::error!("found failed_pending_txns {:#?}", failed_pending_txns)
            }
            Err(_) => {
                tracing::error!("unable to get failed_pending_txns!")
            }
            Ok(None) => match Meta::last_reward_end_time(&self.pool).await {
                Err(_) => {
                    tracing::error!("unable to get failed_pending_txns!")
                }
                Ok(None) => {
                    let kv = handle_first_reward(&self.pool, &trigger).await;
                    tracing::info!("inserted kv: {:#?}", kv);
                }
                Ok(Some(last_reward_end_time)) => {
                    tracing::info!("found last_reward_end_time: {:#?}", last_reward_end_time);
                    let (after_utc, before_utc) = get_time_range(last_reward_end_time);

                    if before_utc <= after_utc {
                        tracing::error!("cannot reward future stuff");
                        return Ok(());
                    }

                    match FileStore::from_env().await {
                        Err(_) => {
                            tracing::error!("unable to make file store")
                        }
                        Ok(store) => {
                            tracing::info!(
                                "searching for files after: {:?} - before: {:?}",
                                after_utc,
                                before_utc
                            );
                            let _ = handle_files(store, after_utc, before_utc).await;
                        }
                    }
                }
            },
        }
        Ok(())
    }
}

async fn count_heartbeats(stream: &mut Stream) -> Result<Counter> {
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

pub fn generate_model(counter: &Counter) -> Result<Model> {
    // how many total mobile each cell_type needs to get
    // (cell_type, mobile), (...)...
    let mut model: HashMap<CellType, u64> = HashMap::new();
    for (cbsd_id, single_hotspot_count) in counter
        .iter()
        .flat_map(|(_gw_pubkey_bin, sub_map)| sub_map.iter())
    {
        if let Ok(ct) = CellType::from_str(cbsd_id) {
            let count = model.entry(ct).or_insert(0);
            // This cell type only gets added to the model if it has more than MIN_PER_CELL_TYPE_HEARTBEATS
            if *single_hotspot_count > MIN_PER_CELL_TYPE_HEARTBEATS {
                *count += 1
            }
        }
    }
    Ok(model)
}

async fn handle_first_reward(
    pool: &Pool<Postgres>,
    trigger: &ConsensusTxnTrigger,
) -> Result<Option<Meta>> {
    tracing::info!("no last_reward_end_time found, just insert trigger block_timestamp");

    let kv = Meta::insert_kv(
        pool,
        "last_reward_end_time",
        &trigger.block_timestamp.to_string(),
    )
    .await?;
    Ok(kv)
}

fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    (
        datetime_from_epoch(last_reward_end_time),
        Utc::now() - Duration::minutes(DEFAULT_LOOKUP_DELAY),
    )
}

async fn handle_files(
    store: FileStore,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result {
    match store
        .list(
            "poc5g-ingest",
            Some(FileType::CellHeartbeat),
            Some(after_utc),
            Some(before_utc),
        )
        .await
    {
        Err(_) => {
            tracing::error!("unable to get file list");
        }
        Ok(None) => {
            tracing::info!("0 files found");
        }
        Ok(Some(file_list)) => {
            tracing::info!("found {:?} files", file_list.len());
            let mut stream = store_source(store, "poc5g-ingest", file_list);
            let counter = count_heartbeats(&mut stream).await?;

            if let Ok(model) = generate_model(&counter) {
                let emitted = get_emissions_per_model(&model, after_utc, before_utc - after_utc);
                tracing::info!("emitted: {:#?}", emitted);

                match construct_rewards(counter, &model, &emitted).await? {
                    Some(rewards) => {
                        let txn = bare_txn(rewards, after_utc, before_utc).await?;
                        // TODO: sign this transaction with the reward server secret key
                        // - submit it to the follower
                        // - insert in the pending_txn tbl
                        tracing::info!("txn: {:#?}", txn)
                    }
                    None => {
                        tracing::error!("unable to construct rewards!");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn bare_txn(
    rewards: Rewards,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result<BlockchainTxnSubnetworkRewardsV1> {
    Ok(BlockchainTxnSubnetworkRewardsV1 {
        rewards,
        token_type: token_type_to_int(BlockchainTokenTypeV1::Mobile),
        start_epoch: after_utc.timestamp() as u64,
        end_epoch: before_utc.timestamp() as u64,
        reward_server_signature: vec![],
    })
}

async fn construct_rewards(
    counter: Counter,
    model: &Model,
    emitted: &Emission,
) -> Result<Option<Rewards>> {
    let mut follower_service = FollowerService::from_env()?;
    let mut rewards: Vec<SubnetworkReward> = Vec::with_capacity(counter.len());

    for (gw_pubkey_bin, per_cell_cnt) in counter.into_iter() {
        if let Ok(gw_pubkey) = PublicKey::try_from(gw_pubkey_bin.as_ref()) {
            let owner = follower_service.find_gateway(&gw_pubkey).await?.owner;
            // This seems necessary because some owner keys apparently
            // don't cleanly convert to PublicKey, even though the
            // owner_pubkey isn't actually used!
            if PublicKey::try_from((&owner).as_ref()).is_err() {
                continue;
            }

            let mut reward_acc = 0;

            for (cbsd_id, cnt) in per_cell_cnt {
                if cnt < MIN_PER_CELL_TYPE_HEARTBEATS {
                    continue;
                }

                let cell_type = CellType::from_str(&cbsd_id)?;

                if let (Some(total_count), Some(total_reward)) =
                    (model.get(&cell_type), emitted.get(&cell_type))
                {
                    if let Some(amt) =
                        (total_reward.get_decimal() / Decimal::from(*total_count)).to_u64()
                    {
                        reward_acc += amt;
                    }
                }
            }
            rewards.push(SubnetworkReward {
                account: owner,
                amount: reward_acc,
            });
        }
    }
    if rewards.is_empty() {
        Ok(None)
    } else {
        Ok(Some(rewards))
    }
}

fn token_type_to_int(tt: BlockchainTokenTypeV1) -> i32 {
    match tt {
        BlockchainTokenTypeV1::Hnt => 0,
        BlockchainTokenTypeV1::Hst => 1,
        BlockchainTokenTypeV1::Mobile => 2,
        BlockchainTokenTypeV1::Iot => 3,
    }
}

#[cfg(test)]
mod test {
    use crate::Mobile;
    use rust_decimal_macros::dec;

    use super::*;

    #[tokio::test]
    async fn check_rewards() {
        // SercommIndoor
        let g1 = PublicKey::from_str("11eX55faMbqZB7jzN4p67m6w7ScPMH6ubnvCjCPLh72J49PaJEL")
            .unwrap()
            .to_vec();
        // Nova430I
        let g2 = PublicKey::from_str("118SPA16MX8WrUKcuXxsg6SH8u5dWszAySiUAJX6tTVoQVy7nWc")
            .unwrap()
            .to_vec();
        // SercommOutdoor
        let g3 = PublicKey::from_str("112qDCKek7fePg6wTpEnbLp3uD7TTn8MBH7PGKtmAaUcG1vKQ9eZ")
            .unwrap()
            .to_vec();
        // Nova436H
        let g4 = PublicKey::from_str("11k712d9dSb8CAujzS4PdC7Hi8EEBZWsSnt4Zr1hgke4e1Efiag")
            .unwrap()
            .to_vec();

        let mut c1 = CbsdCounter::new();
        c1.insert("P27-SCE4255W2107CW5000014".to_string(), 4);
        let mut c2 = CbsdCounter::new();
        c2.insert("2AG32PBS3101S1202000464223GY0153".to_string(), 5);
        let mut c3 = CbsdCounter::new();
        c3.insert("P27-SCO4255PA102206DPT000207".to_string(), 6);
        let mut c4 = CbsdCounter::new();
        c4.insert("2AG32MBS3100196N1202000240215KY0184".to_string(), 5);

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

        let generated_model = generate_model(&counter).unwrap();
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
        let emitted = get_emissions_per_model(&generated_model, after_utc, Duration::hours(24));
        assert_eq!(emitted, expected_emitted);

        let rewards = construct_rewards(counter, &generated_model, &emitted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(4, rewards.len());

        let tot_rewards = rewards.iter().fold(0, |acc, reward| acc + reward.amount);
        assert_eq!(100_000_000, tot_rewards);

        // TODO cross check individual owner rewards
        // TODO: Bug, we are likely having duplicates in rewards
    }
}
