use crate::{
    datetime_from_epoch, env_var,
    follower::{FollowerService, Meta},
    keypair::Keypair,
    pending_txn::{PendingTxn, Status},
    subnetwork_rewards::{construct_txn, SubnetworkRewards},
    traits::B64,
    transaction::TransactionService,
    txn_status::TxnStatus,
    Error, Result,
};
use chrono::{Duration, Utc};
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    FollowerTxnStreamRespV1, TxnQueryRespV1, TxnStatus as ProtoTxnStatus,
};
use poc_store::FileStore;
use sqlx::{Pool, Postgres};
use tokio::time;
use tonic::Streaming;

/// First block that 5G hotspots were introduced (FreedomFi)
pub const DEFAULT_START_BLOCK: i64 = 995041;
pub const DEFAULT_START_REWARD_BLOCK: i64 = 1477650;

const RECONNECT_WAIT_SECS: u64 = 5;

pub const TXN_TYPES: &[&str] = &[
    "blockchain_txn_consensus_group_v1",
    "blockchain_txn_subnetwork_rewards_v1",
];

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Keypair,
    follower_service: FollowerService,
    txn_service: TransactionService,
    start_block: i64,
    start_reward_block: i64,
}

impl Server {
    pub async fn new(pool: Pool<Postgres>, keypair: Keypair) -> Result<Self> {
        let result = Self {
            pool,
            keypair,
            follower_service: FollowerService::from_env()?,
            txn_service: TransactionService::from_env()?,
            start_block: env_var("FOLLOWER_START_BLOCK", DEFAULT_START_BLOCK)?,
            start_reward_block: env_var("REWARD_START_BLOCK", DEFAULT_START_REWARD_BLOCK)?,
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

            let height = Meta::last_height(&self.pool, self.start_block).await? as u64;

            if Meta::last_reward_height(&self.pool).await?.is_none() {
                Meta::insert_kv(
                    &self.pool,
                    "last_reward_height",
                    &self.start_reward_block.to_string(),
                )
                .await?;
            };

            tracing::info!("connecting to blockchain txn stream at height {height}");
            tokio::select! {
                _ = shutdown.clone() => (),
                // trigger = self.trigger_receiver.recv() => {
                //     if let Ok(trigger) = trigger {
                //         if self.handle_trigger(trigger).await.is_err() {
                //             tracing::error!("failed to handle trigger!")
                //         }
                //     } else {
                //         tracing::error!("failed to recv trigger!")
                //     }
                // }
                stream_result = self.follower_service.txn_stream(height, &[], TXN_TYPES) => match stream_result {
                    Ok(txn_stream) => {
                        tracing::info!("connected to txn stream");
                        self.run_with_txn_stream(txn_stream, shutdown.clone()).await?
                    }
                    Err(err) => {
                        tracing::warn!("failed to connec to txn stream: {err}");
                        self.reconnect_wait(shutdown.clone()).await
                    }
                }
            }
        }
    }

    async fn reconnect_wait(&mut self, shutdown: triggered::Listener) {
        let timer = time::sleep(time::Duration::from_secs(RECONNECT_WAIT_SECS));
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
                        Meta::update(&self.pool, "last_height", height.to_string()).await?;
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

        let txn_ht = envelope.height;
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
                Meta::update_all(
                    &self.pool,
                    &[
                        ("last_reward_height", txn_ht.to_string()),
                        ("last_reward_end_time", txn_ts.to_string()),
                    ],
                )
                .await
            }
            // we got a subnetwork reward but don't have a pending txn in our db,
            // it may have been submitted externally, ignore and just bump the last_reward_height
            // in our meta table
            Err(Error::NotFound(_)) => {
                tracing::warn!(
                    "ignore but bump last_reward_height and last_reward_end_time in meta!"
                );
                Meta::update_all(
                    &self.pool,
                    &[
                        ("last_reward_height", txn_ht.to_string()),
                        ("last_reward_end_time", txn_ts.to_string()),
                    ],
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn process_consensus_group(&mut self, envelope: &FollowerTxnStreamRespV1) -> Result {
        let ht = envelope.height;
        let ts = envelope.timestamp;
        tracing::info!("processing consensus group at {ht} with timestamp: {ts}");

        self.handle_rewards(ht, ts).await?;

        self.handle_failed().await?;

        Ok(())
    }

    async fn handle_failed(&mut self) -> Result {
        if let Ok(pending_txns) = PendingTxn::list(&self.pool, Status::Pending).await {
            let mut failed_hashes: Vec<String> = Vec::new();
            for txn in pending_txns {
                let submitted_at = txn.submitted_at()?;
                let created_at = txn.created_at()?;
                let txn_key = txn.pending_key()?;
                match self.txn_service.query(&txn_key).await {
                    Ok(TxnQueryRespV1 { status, .. }) => {
                        if TxnStatus::try_from(status)? == TxnStatus::from(ProtoTxnStatus::NotFound)
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
                match PendingTxn::update_all(&self.pool, failed_hashes, Status::Failed, Utc::now())
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
        } else {
            tracing::error!("unable to retrieve outstanding pending txns");
        }
        Ok(())
    }

    async fn handle_rewards(&mut self, block_height: u64, block_timestamp: u64) -> Result {
        tracing::info!("chain consensus group trigger received");

        match PendingTxn::list(&self.pool, Status::Failed).await {
            Ok(failed_pending_txns) if !failed_pending_txns.is_empty() => {
                tracing::error!("found failed_pending_txns {:#?}", failed_pending_txns)
            }
            Err(_) => {
                tracing::error!("unable to list failed_pending_txns!")
            }
            Ok(_) => match Meta::last_reward_end_time(&self.pool).await {
                Err(_) => {
                    tracing::error!("unable to get failed_pending_txns!")
                }
                Ok(None) => {
                    let kv = handle_first_reward(&self.pool, block_timestamp).await;
                    tracing::info!("inserted kv: {:#?}", kv);
                }
                Ok(Some(last_reward_end_time)) => {
                    tracing::info!("found last_reward_end_time: {:#?}", last_reward_end_time);

                    let store = FileStore::from_env().await?;
                    let rewards = SubnetworkRewards::from_last_reward_end_time(
                        store,
                        self.follower_service.clone(),
                        last_reward_end_time,
                    )
                    .await?;

                    match Meta::last_reward_height(&self.pool).await? {
                        None => {
                            tracing::error!("cannot continue, no known last_reward_height!")
                        }
                        Some(last_reward_height) => {
                            let _ = &self
                                .issue_rewards(rewards, last_reward_height + 1, block_height as i64)
                                .await;
                        }
                    }
                }
            },
        }
        Ok(())
    }

    async fn issue_rewards(
        &mut self,
        rewards: SubnetworkRewards,
        start_epoch: i64,
        end_epoch: i64,
    ) -> Result {
        if rewards.is_empty() {
            tracing::info!("nothing to reward");
            return Ok(());
        }

        let (txn, txn_hash_str) = construct_txn(&self.keypair, rewards, start_epoch, end_epoch)?;
        // insert in the pending_txn tbl (status: created)
        let pt = PendingTxn::insert_new(&self.pool, txn_hash_str.clone()).await?;
        tracing::info!("inserted pending_txn: {:?}", pt);

        // submit the txn
        if let Ok(_resp) = self
            .txn_service
            .submit(
                BlockchainTxn {
                    txn: Some(Txn::SubnetworkRewards(txn)),
                },
                &pt.pending_key()?,
            )
            .await
        {
            PendingTxn::update(&self.pool, &txn_hash_str, Status::Pending, Utc::now()).await?;
        }
        Ok(())
    }
}

async fn handle_first_reward(pool: &Pool<Postgres>, block_timestamp: u64) -> Result<Meta> {
    tracing::info!("no last_reward_end_time found, just insert trigger block_timestamp");

    Meta::insert_kv(pool, "last_reward_end_time", &block_timestamp.to_string()).await
}
