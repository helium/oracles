use crate::{pending_txn::PendingTxn, ConsensusTxnTrigger, PublicKey, Result};
use sqlx::{Pool, Postgres};
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

        // TODO: remove and construct an actual pending txn
        let pending_txn = PendingTxn::new(
            PublicKey::from_str("112fBdq2Hk4iFTi5JCWZ6mTdp4mb7piVBwcMcRyN7br7VPRhhHa").unwrap(),
            "hash".to_string(),
        )
        .await;
        tracing::info!("pending_txn {:#?}", pending_txn);

        if pending_txn.insert_into(&self.pool).await.is_err() {
            tracing::error!("Failed to insert pending txn")
        }

        tracing::info!("chain trigger received {:#?}", trigger);
        Ok(())
    }
}
