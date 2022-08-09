use crate::CellType;
use crate::{
    datetime_from_epoch, emissions, follower::Meta, pending_txn::PendingTxn, ConsensusTxnTrigger,
    Result,
};
use chrono::{Duration, Utc};
use futures::stream::StreamExt;
use helium_proto::{services::poc_mobile::CellHeartbeatReqV1, Message};
use poc_store::{file_source::store_source, FileStore, FileType};
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::broadcast;

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
        // Trigger received
        tracing::info!("chain trigger received {:#?}", trigger);

        // Check pending txns table for pending failures, abort if failed (TBD)
        if let Ok(failed_pending_txns) = PendingTxn::get_all_failed_pending_txns(&self.pool).await {
            if failed_pending_txns.is_empty() {
                tracing::info!("all pending txns clear, continue");

                // Retrieve last reward cycle end time from follower_meta table, if none, continue (we just started)
                if let Ok(Some(last_reward_end_time)) = Meta::last_reward_end_time(&self.pool).await
                {
                    tracing::info!("found last_reward_end_time: {:#?}", last_reward_end_time);

                    let after_utc = datetime_from_epoch(last_reward_end_time);
                    let mut before_utc = after_utc;

                    // Fetch files from file_store from last_time to last_time + epoch
                    if let Ok(store) = FileStore::from_env().await {
                        // before = last_reward_end_time + 30 minutes
                        // loop till before > now - stop
                        loop {
                            let before = before_utc + Duration::minutes(30);
                            if before > Utc::now() {
                                break;
                            }
                            before_utc = before
                        }

                        tracing::info!(
                            "searching for files after: {:?} - before: {:?}",
                            after_utc,
                            before_utc
                        );

                        // only reward if hotspot (celltype) appears 3+ times in an epoch

                        if let Ok(file_list) = store
                            .list(
                                "poc5g-ingest",
                                Some(FileType::CellHeartbeat),
                                Some(after_utc),
                                Some(before_utc),
                            )
                            .await
                        {
                            if file_list.is_empty() {
                                // No rewards to issue because we couldn't find any matching
                                // files pertaining to this reward cycle
                                tracing::info!("0 files found!")
                            } else {
                                tracing::info!("found {:?} files", file_list.len());
                                let mut stream = store_source(store, "poc5g-ingest", file_list);

                                // key: <pubkey, cbsd_id>, val: # of heartbeats
                                let mut counter: HashMap<(Vec<u8>, String), u64> = HashMap::new();

                                while let Some(Ok(msg)) = stream.next().await {
                                    let heartbeat_req = CellHeartbeatReqV1::decode(msg)?;
                                    let count = counter
                                        .entry((heartbeat_req.pub_key, heartbeat_req.cbsd_id))
                                        .or_insert(0);
                                    *count += 1;
                                }

                                // filter out any <pubkey, celltype> < 3
                                counter.retain(|_, v| *v >= 3);

                                // let mut follower_service = FollowerService::from_env()?;

                                // how many total mobile each cell_type needs to get
                                // (cell_type, mobile), (...)...
                                let mut models: HashMap<CellType, u64> = HashMap::new();

                                for ((_, cbsd_id), _v) in counter.iter() {
                                    if let Ok(ct) = CellType::from_str(cbsd_id) {
                                        let count = models.entry(ct).or_insert(0);
                                        *count += 1
                                    }
                                }
                                tracing::info!("models: {:#?}", models);

                                let emitted = emissions::get_emissions_per_model(models, after_utc);
                                tracing::info!("emitted: {:#?}", emitted);

                                // - construct pending reward txn, store in pending table
                            }
                        }
                    }
                } else {
                    tracing::info!(
                        "no last_reward_end_time found, just insert trigger block_timestamp"
                    );

                    let kv = Meta::insert_kv(
                        &self.pool,
                        "last_reward_end_time",
                        &trigger.block_timestamp.to_string(),
                    )
                    .await?;
                    tracing::info!("inserted kv: {:#?}", kv);
                }
            } else {
                // Abort the entire process (for now)
                tracing::error!("found failed_pending_txns {:#?}", failed_pending_txns);
            }
        } else {
            tracing::error!("unable to get failed_pending_txns!")
        }

        Ok(())
    }
}
