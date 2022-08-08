use crate::{
    datetime_from_epoch, follower::Meta, pending_txn::PendingTxn, ConsensusTxnTrigger, Result,
};
use futures_util::stream::StreamExt;
use poc_store::{file_source::store_source, FileStore, FileType};
use sqlx::{Pool, Postgres};
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

                    // Fetch files from file_store from last_time to last_time + epoch
                    if let Ok(store) = FileStore::from_env().await {
                        tracing::info!(
                            "searching for files after: {:?} - before: {:?}",
                            datetime_from_epoch(last_reward_end_time),
                            datetime_from_epoch(trigger.block_timestamp as i64)
                        );

                        // before = last_reward_end_time + 30 minutes
                        //
                        // loop till
                        // before > now - stop
                        //
                        // only reward if hotspot (celltype) appears 3+ times in an epoch

                        if let Ok(file_list) = store
                            .list(
                                "poc5g-ingest",
                                Some(FileType::CellHeartbeat),
                                Some(datetime_from_epoch(last_reward_end_time)),
                                Some(datetime_from_epoch(trigger.block_timestamp as i64)),
                            )
                            .await
                        {
                            if file_list.is_empty() {
                                // No rewards to issue because we couldn't find any matching
                                // files pertaining to this reward cycle
                                tracing::info!("0 files found!")
                            } else {
                                tracing::info!("found {:?} files", file_list.len());
                                let stream = store_source(store, "poc5g-ingest", file_list);
                                tracing::info!("count: {:?}", stream.count().await)

                                // - use file_multi_source to read heartbeats
                                // - look up hotspot owner for rewarded hotspot
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
