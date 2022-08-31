use crate::{
    datetime_from_epoch, env_var,
    follower::FollowerService,
    keypair::Keypair,
    meta::Meta,
    pending_txn::{PendingTxn, Status},
    subnetwork_rewards::{construct_txn, get_time_range, RewardPeriod, SubnetworkRewards},
    traits::B64,
    transaction::TransactionService,
    txn_status::TxnStatus,
    Error, Result,
};
use chrono::{Duration, Utc};
use futures::try_join;
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    FollowerTxnStreamRespV1, Message, TxnQueryRespV1, TxnStatus as ProtoTxnStatus,
};
use poc_metrics::record_duration;
use poc_store::FileStore;
use sqlx::{Pool, Postgres};
use tokio::time;
use tonic::Streaming;

pub const DEFAULT_START_REWARD_BLOCK: i64 = 1477650;

const RECONNECT_WAIT_SECS: u64 = 5;
const DEFAULT_TRIGGER_INTERVAL_SECS: u64 = 900; // 15 min
const DEFAULT_REWARD_INTERVAL_SECS: i64 = 86400; // 24 hours
const STALE_PENDING_TIMEOUT_SECS: i64 = 1800; // 30 min

pub const TXN_TYPES: &[&str] = &["blockchain_txn_subnetwork_rewards_v1"];

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Keypair,
    follower_service: FollowerService,
    txn_service: TransactionService,
    start_reward_block: i64,
    trigger_interval: time::Duration,
    reward_interval: Duration,
}

impl Server {
    pub async fn new(pool: Pool<Postgres>, keypair: Keypair) -> Result<Self> {
        let result = Self {
            pool,
            keypair,
            follower_service: FollowerService::from_env()?,
            txn_service: TransactionService::from_env()?,
            start_reward_block: env_var("FOLLOWER_START_BLOCK", DEFAULT_START_REWARD_BLOCK)?,
            trigger_interval: time::Duration::from_secs(env_var(
                "TRIGGER_INTERVAL",
                DEFAULT_TRIGGER_INTERVAL_SECS,
            )?),
            reward_interval: Duration::seconds(env_var(
                "REWARD_INTERVAL",
                DEFAULT_REWARD_INTERVAL_SECS,
            )?),
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting rewards server");

        // If the rewards server has restarted, check for and process any
        // PendingTxns that may have been created but failed to previoiusly send
        self.process_prior_created_txns().await?;

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping rewards server");
                return Ok(());
            }

            let follow_start = match Meta::last_follower_height(&self.pool).await? {
                None => {
                    let start_reward_block = &self.start_reward_block;
                    Meta::insert_kv(
                        &self.pool,
                        "last_follower_height",
                        &start_reward_block.to_string(),
                    )
                    .await?;
                    *start_reward_block
                }
                Some(last_follower_height) => last_follower_height,
            } as u64;

            tracing::info!("connecting to blockchain txn stream at height {follow_start}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = self.follower_service.txn_stream(follow_start, &[], TXN_TYPES) => match stream_result {
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
        let mut trigger_timer = time::interval(self.trigger_interval);

        loop {
            tokio::select! {
                msg = txn_stream.message() => match msg {
                    Ok(Some(txn)) => {
                        record_duration!(
                            "reward_server_process_txn_entry_duration",
                            self.process_txn_entry(txn).await?
                        )
                    }
                    Ok(None) => {
                        tracing::warn!("txn stream disconnected");
                        return Ok(());
                    }
                    Err(err) => {
                        metrics::increment_counter!("reward_server_txn_stream_error_count");
                        tracing::warn!("txn stream error {err:?}");
                        return Ok(());
                    }
                },
                _ = trigger_timer.tick() => {
                    self.process_clock_tick().await?
                }
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

        let last_follower_height = txn.end_epoch;
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
                Meta::update(
                    &self.pool,
                    "last_follower_height",
                    last_follower_height.to_string(),
                )
                .await
            }
            // we got a subnetwork reward but don't have a pending txn in our db,
            // it may have been submitted externally, ignore and just bump the last_follower_height
            // in our meta table
            Err(Error::NotFound(_)) => {
                tracing::warn!("ignore but bump last_follower_height to {last_follower_height:?}!");
                Meta::update(
                    &self.pool,
                    "last_follower_height",
                    last_follower_height.to_string(),
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn process_clock_tick(&mut self) -> Result {
        let now = Utc::now();
        tracing::info!("processing clock tick at {}", now.to_string());

        // Early exit if we found failed pending txns
        match self.check_pending().await {
            Ok(_) => {
                tracing::info!("no failed pending transactions");
            }
            Err(err) => {
                tracing::error!("pending transactions check failed with error: {err:?}");
                return Err(err);
            }
        }

        match Meta::last_reward_end_time(&self.pool).await? {
            Some(last_reward_time) => {
                tracing::info!("found last_reward_end_time: {:#?}", last_reward_time);
                let (start_utc, end_utc) = get_time_range(last_reward_time);
                if end_utc - start_utc > self.reward_interval {
                    // Handle rewards if we pass our duration
                    match self
                        .handle_rewards(last_reward_time, end_utc.timestamp())
                        .await
                    {
                        Ok(_) => tracing::info!("successfully emitted mobile rewards"),
                        Err(err) => {
                            tracing::error!("rewards emissions failed with error: {err:?}");
                            return Err(err);
                        }
                    }
                }
            }
            None => {
                tracing::info!(
                    "no last_reward_end_time found, inserting current timestamp {}",
                    now.timestamp()
                );
                Meta::update(&self.pool, "last_reward_end_time", now.to_string()).await?;
            }
        }
        Ok(())
    }

    async fn check_pending(&mut self) -> Result {
        let pending_txns = PendingTxn::list(&self.pool, Status::Pending).await?;
        let mut failed_hashes: Vec<String> = Vec::new();
        for txn in pending_txns {
            let submitted_at = txn.submitted_at()?;
            let created_at = txn.created_at()?;
            let txn_key = txn.pending_key()?;
            match self.txn_service.query(&txn_key).await {
                Ok(TxnQueryRespV1 { status, .. }) => {
                    if TxnStatus::try_from(status)? == TxnStatus::from(ProtoTxnStatus::NotFound)
                        && (Utc::now() - submitted_at)
                            > Duration::seconds(STALE_PENDING_TIMEOUT_SECS)
                    {
                        failed_hashes.push(txn.hash)
                    }
                }
                Err(_) => {
                    tracing::error!("failed to retrieve txn {created_at} status")
                }
            }
        }
        if !failed_hashes.is_empty() {
            PendingTxn::update_all(&self.pool, failed_hashes, Status::Failed, Utc::now()).await?
        }
        let failed_pending_txns = PendingTxn::list(&self.pool, Status::Failed).await?;
        if !failed_pending_txns.is_empty() {
            tracing::error!("found failed_pending_txns {:#?}", failed_pending_txns);
            return Err(Error::TransactionError(
                "failed transactions in pending".to_string(),
            ));
        }
        Ok(())
    }

    async fn handle_rewards(&mut self, last_reward_time: i64, end_utc: i64) -> Result {
        tracing::info!("triggering rewards emissions");

        let store = FileStore::from_env().await?;
        let rewards = SubnetworkRewards::from_last_reward_end_time(
            store,
            self.follower_service.clone(),
            last_reward_time,
        );

        if let Ok(reward_period) = self.follower_service.reward_period().await {
            if let Some(r) = rewards.await? {
                let pool = &self.pool.clone();
                let issue_rewards = self.issue_rewards(r, reward_period);
                let update_end_time =
                    Meta::update(pool, "last_reward_end_time", end_utc.to_string());
                try_join!(issue_rewards, update_end_time)?;
            } else {
                tracing::error!("cannot continue; unable to determine reward period!");
                return Err(Error::NotFound("invalid reward period".to_string()));
            }
        }
        Ok(())
    }

    async fn issue_rewards(
        &mut self,
        rewards: SubnetworkRewards,
        reward_period: RewardPeriod,
    ) -> Result {
        if rewards.is_empty() {
            tracing::info!("nothing to reward");
            return Ok(());
        }

        let (txn, txn_hash_str) = construct_txn(&self.keypair, rewards, reward_period)?;

        // binary encode the txn for storage
        let txn_encoded = txn.encode_to_vec();

        // insert in the pending_txn tbl (status: created)
        let pt = PendingTxn::insert_new(&self.pool, &txn_hash_str, txn_encoded).await?;
        tracing::info!("inserted pending_txn with hash: {txn_hash_str}");

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

    async fn process_prior_created_txns(&mut self) -> Result {
        tracing::info!("processing any unsubmitted reward txns from previous run");

        let created_txns = PendingTxn::list(&self.pool, Status::Created).await?;
        if !created_txns.is_empty() {
            for pending_txn in created_txns {
                let txn = Message::decode(pending_txn.txn_bin.as_ref())?;
                if let Ok(_resp) = self
                    .txn_service
                    .submit(
                        BlockchainTxn {
                            txn: Some(Txn::SubnetworkRewards(txn)),
                        },
                        &pending_txn.pending_key()?,
                    )
                    .await
                {
                    PendingTxn::update(&self.pool, &pending_txn.hash, Status::Pending, Utc::now())
                        .await?;
                }
            }
        }
        Ok(())
    }
}
