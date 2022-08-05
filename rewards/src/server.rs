use crate::{follower::FollowerMeta, pending_txn::PendingTxn, ConsensusTxnTrigger, Result};
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
                            tracing::error!("Failed to handle trigger!")
                        }
                    } else {
                        tracing::error!("Failed to recv trigger!")
                    }
                }
            }
        }
    }

    pub async fn handle_trigger(&mut self, trigger: ConsensusTxnTrigger) -> Result {
        // Trigger received
        // - check pending txns table for pending failures, abort if failed (TBD)
        // - retrieve last reward cycle end time from follower_meta table, if none, continue (we just started)
        // - fetch files from file_store from last_time to last_time + epoch
        // - use file_multi_source to read heartbeats
        // - look up hotspot owner for rewarded hotspot
        // - construct pending reward txn, store in pending table
        // - submit pending_txn to blockchain-node
        // - use node's txn follower to detect cleared txns and update pending table

        tracing::info!("chain trigger received {:#?}", trigger);

        if let Ok(failed_pending_txns) = PendingTxn::get_all_failed_pending_txns(&self.pool).await {
            if failed_pending_txns.is_empty() {
                tracing::info!("all pending txns clear, continue");

                if let Ok(Some(last_reward_end_time)) =
                    FollowerMeta::get(&self.pool, "last_reward_end_time").await
                {
                    tracing::info!("found last_reward_end_time: {:#?}", last_reward_end_time);
                    // figure out next epoch + files corresponding to it...
                } else {
                    tracing::info!(
                        "no last_reward_end_time found, maybe we just started, continue"
                    );
                    // not sure how to proceed for the first time...
                }

                // let kv = FollowerMeta::insert_kv(
                //     &self.pool,
                //     "last_reward_end_time",
                //     &trigger.block_timestamp.to_string(),
                // )
                // .await?;
                //
                // tracing::info!("kv: {:#?}", kv);
            } else {
                // abort the entire process (for now)
                panic!("found failed_pending_txns {:#?}", failed_pending_txns);
            }
        } else {
            tracing::error!("unable to get failed_pending_txns!")
        }

        Ok(())
    }
}
