use crate::{
    datetime_from_epoch, emissions, follower::Meta, pending_txn::PendingTxn, CellType,
    ConsensusTxnTrigger, Result,
};
use chrono::{DateTime, Duration, Utc};
use emissions::{get_emissions_per_model, Model};
use futures::stream::StreamExt;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use poc_store::{
    file_source::{store_source, Stream},
    FileStore, FileType,
};
use sqlx::{Pool, Postgres};
use std::{collections::HashMap, str::FromStr};
use tokio::sync::broadcast;

// default minutes to delay lookup from now
pub const DEFAULT_LOOKUP_DELAY: i64 = 30;

// key: <pubkey, cbsd_id>, val: # of heartbeats
type Counter = HashMap<(Vec<u8>, String), u64>;

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
                let emitted = get_emissions_per_model(model, after_utc, before_utc - after_utc);
                tracing::info!("emitted: {:#?}", emitted);
            }
        }
    }
    Ok(())
}
