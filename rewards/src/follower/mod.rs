pub mod client;
pub mod meta;
pub use client::FollowerService;
pub use meta::Meta;

use crate::{
    datetime_from_epoch, env_var,
    pending_txn::{PendingTxn, Status},
    traits::b64::B64,
    transaction::client::TransactionService,
    txn_status::TxnStatus,
    ConsensusTxnTrigger, Error, Result,
};
use chrono::{Duration, Utc};
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    FollowerTxnStreamRespV1, TxnQueryRespV1, TxnStatus as ProtoTxnStatus,
};
use sqlx::{Pool, Postgres};
use std::str;
use tokio::{sync::broadcast, time};
use tonic::Streaming;

/// First block that 5G hotspots were introduced (FreedomFi)
pub const DEFAULT_START_BLOCK: i64 = 995041;
pub const DEFAULT_START_REWARD_BLOCK: i64 = 1477650;

pub const TXN_TYPES: &[&str] = &[
    "blockchain_txn_consensus_group_v1",
    "blockchain_txn_subnetwork_rewards_v1",
];

pub struct Follower {
    pool: Pool<Postgres>,
    pub service: FollowerService,
    pub txn_service: TransactionService,
    start_block: i64,
    start_reward_block: i64,
    trigger: broadcast::Sender<ConsensusTxnTrigger>,
}

impl Follower {
    pub async fn new(
        pool: Pool<Postgres>,
        trigger: broadcast::Sender<ConsensusTxnTrigger>,
    ) -> Result<Self> {
        let start_block = env_var("FOLLOWER_START_BLOCK", DEFAULT_START_BLOCK)?;
        let start_reward_block = env_var("REWARD_START_BLOCK", DEFAULT_START_REWARD_BLOCK)?;
        let service = FollowerService::from_env()?;
        Ok(Self {
            service,
            pool,
            start_block,
            start_reward_block,
            trigger,
            txn_service: TransactionService::from_env()?,
        })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting follower");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping follower");
                return Ok(());
            }
            let height = Meta::last_height(&self.pool, self.start_block).await? as u64;

            if Meta::last_reward_height(&self.pool).await?.is_none() {
                Meta::insert_kv(
                    &self.pool,
                    "last_reward_height",
                    &self.start_reward_block.to_string(),
                )
                .await?;
            };

            tracing::info!("connecting to txn stream at height {height}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = self.service.txn_stream(height, &[], TXN_TYPES) => match stream_result {
                    Ok(txn_stream) => {
                        tracing::info!("connected to txn stream");
                        self.run_with_txn_stream(txn_stream, shutdown.clone()).await?
                    }
                    Err(err) => {
                        tracing::warn!("failed to connect to txn stream: {err}");
                        self.reconnect_wait(shutdown.clone()).await
                    }
                }
            }
        }
    }

    async fn reconnect_wait(&mut self, shutdown: triggered::Listener) {
        let timer = time::sleep(time::Duration::from_secs(5));
        tokio::select! {
            _ = timer => (),
            _ = shutdown => (),
        }
    }

    async fn run_with_txn_stream(
        &mut self,
        mut txn_stream: Streaming<FollowerTxnStreamRespV1>,
        shutdown: triggered::Listener,
    ) -> Result {
        loop {
            tokio::select! {
                msg = txn_stream.message() => match msg {
                    Ok(Some(txn)) => {
                        let height = txn.height as i64;
                        self.process_txn_entry(txn).await?;
                        Meta::update(&self.pool, "last_height", &height.to_string()).await?;
                    }
                    Ok(None) => {
                        tracing::warn!("txn stream disconnected");
                        return Ok(());
                    }
                    Err(err) => {
                        tracing::warn!("txn stream error {err:?}");
                        return Ok(());
                    }
                },
                _ = shutdown.clone() => return Ok(())
            }
        }
    }

    async fn process_txn_entry(&mut self, entry: FollowerTxnStreamRespV1) -> Result {
        let txn = match entry.txn {
            Some(BlockchainTxn { txn: Some(ref txn) }) => txn,
            _ => {
                tracing::warn!("ignoring missing txn in stream");
                return Ok(());
            }
        };
        match txn {
            Txn::SubnetworkRewards(txn) => self.process_subnet_rewards(&entry, txn).await,
            Txn::ConsensusGroup(_) => self.process_consensus_group(&entry).await,
            _ => Ok(()),
        }
    }

    async fn process_subnet_rewards(
        &mut self,
        envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnSubnetworkRewardsV1,
    ) -> Result {
        if txn.token_type() != BlockchainTokenTypeV1::Mobile {
            return Ok(());
        }

        let txn_ht = &envelope.height;
        let txn_hash = &envelope.txn_hash.to_b64_url()?;
        let txn_ts = envelope.timestamp;
        match PendingTxn::update(
            &self.pool,
            txn_hash,
            Status::Cleared,
            datetime_from_epoch(txn_ts as i64),
        )
        .await
        {
            Ok(()) => {
                // TODO: Don't do two separate queries
                Meta::update(&self.pool, "last_reward_height", &txn_ht.to_string()).await?;
                Meta::update(&self.pool, "last_reward_end_time", &txn_ts.to_string()).await?;
                Ok(())
            }
            // we got a subnetwork reward but don't have a pending txn in our db,
            // it may have been submitted externally, ignore and just bump the last_reward_height
            // in our meta table
            Err(Error::NotFound(_)) => {
                tracing::warn!(
                    "ignore but bump last_reward_height and last_reward_end_time in meta!"
                );
                // TODO: Don't do two separate queries
                Meta::update(&self.pool, "last_reward_height", &txn_ht.to_string()).await?;
                Meta::update(&self.pool, "last_reward_end_time", &txn_ts.to_string()).await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn process_consensus_group(&mut self, envelope: &FollowerTxnStreamRespV1) -> Result {
        let ht = envelope.height;
        let ts = envelope.timestamp;
        tracing::info!(
            "processing consensus group at {height} with timestamp: {ts}",
            height = ht,
            ts = ts
        );
        match self.trigger.send(ConsensusTxnTrigger::new(ht, ts)) {
            Ok(_) => {
                // lookup non-cleared pending_txn
                // mark pending as failed if txn_mgr in bnode says its failed
                match PendingTxn::list(&self.pool, Status::Pending).await {
                    Ok(pending_txns) => {
                        let mut failed_hashes: Vec<String> = Vec::new();
                        for txn in pending_txns {
                            let submitted_at = txn.submitted_at()?;
                            let created_at = txn.created_at()?;
                            let txn_key = txn.pending_key()?;
                            match self.txn_service.query(&txn_key).await {
                                Ok(TxnQueryRespV1 { status, .. }) => {
                                    if TxnStatus::try_from(status)?
                                        == TxnStatus::from(ProtoTxnStatus::NotFound)
                                        && (Utc::now() - submitted_at) > Duration::minutes(30)
                                    {
                                        failed_hashes.push(txn.hash)
                                    }
                                }
                                Err(_) => {
                                    tracing::error!("failed to retrieve txn {created_at} status")
                                }
                            }
                        }
                        let failed_count = failed_hashes.len();
                        if failed_count > 0 {
                            match PendingTxn::update_all(
                                &self.pool,
                                failed_hashes,
                                Status::Failed,
                                Utc::now(),
                            )
                            .await
                            {
                                Ok(()) => {
                                    tracing::info!("successfully failed {failed_count} txns")
                                }
                                Err(_) => {
                                    tracing::error!("unable to update failed txns")
                                }
                            }
                        }

                        return Ok(());
                    }
                    Err(_) => {
                        tracing::error!("unable to retrieve outstanding pending txns")
                    }
                }

                Ok(())
            }
            Err(_) => {
                tracing::error!("failed to send reward trigger");
                Ok(())
            }
        }
    }
}
