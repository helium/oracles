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

// key: <pubkey, cbsd_id>, val: # of heartbeats
type Counter = HashMap<(Vec<u8>, String), u64>;
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
        let heartbeat_req = CellHeartbeatReqV1::decode(msg)?;
        let count = counter
            .entry((heartbeat_req.pub_key, heartbeat_req.cbsd_id))
            .or_insert(0);
        if *count <= 3 {
            *count += 1;
        }
    }
    Ok(counter)
}

pub fn generate_model(counter: &Counter) -> Result<Model> {
    // how many total mobile each cell_type needs to get
    // (cell_type, mobile), (...)...
    let mut model: HashMap<CellType, u64> = HashMap::new();
    for ((_gw_pubkey_bin, cbsd_id), _heartbeats) in counter.iter() {
        if let Ok(ct) = CellType::from_str(cbsd_id) {
            let count = model.entry(ct).or_insert(0);
            *count += 1
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

                match construct_rewards(&counter, &model, &emitted).await? {
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
    counter: &Counter,
    model: &Model,
    emitted: &Emission,
) -> Result<Option<Rewards>> {
    let mut follower_service = FollowerService::from_env()?;

    let mut rewards: Vec<SubnetworkReward> = vec![];

    for ((gw_pubkey_bin, cbsd_id), _) in counter.iter() {
        if let Ok(gw_pubkey) = PublicKey::try_from(gw_pubkey_bin.as_ref()) {
            let gw_resp = follower_service.find_gateway(&gw_pubkey).await?;
            let ct = CellType::from_str(cbsd_id)?;
            let owner = gw_resp.owner;

            // This seems necessary because some owner keys apparently don't cleanly
            // convert to PublicKey, even though the owner_pubkey isn't actually used!
            if let Ok(_owner_pubkey) = PublicKey::try_from(owner.as_ref()) {
                match model.get(&ct) {
                    None => (),
                    Some(total_count) => match emitted.get(&ct) {
                        None => (),
                        Some(total_reward) => {
                            if let Some(amt) =
                                (total_reward.get_decimal() / Decimal::from(*total_count)).to_u64()
                            {
                                let reward = SubnetworkReward {
                                    account: owner,
                                    amount: amt,
                                };
                                rewards.push(reward);
                            }
                        }
                    },
                }
            }
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

// #[cfg(test)]
// mod test {
//     use super::*;
//
//     #[test]
//     fn check_rewards() {
//         let o1 =
//             PublicKey::from_str("112fBdq2Hk4iFTi5JCWZ6mTdp4mb7piVBwcMcRyN7br7VPRhhHa").unwrap();
//         let o2 =
//             PublicKey::from_str("11Uy5F7mgouEegZkgCwpWDXFupCCihw63ozzrFF8wX8QiHEJD1v").unwrap();
//         let amt = 50;
//         let r1 = SubnetworkReward {
//             account: o1.to_vec(),
//             amount: amt,
//         };
//         let r2 = SubnetworkReward {
//             account: o2.to_vec(),
//             amount: amt,
//         };
//
//         // assert_eq!(expected, output);
//     }
// }
