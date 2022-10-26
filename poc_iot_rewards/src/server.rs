use crate::{
    datetime_from_epoch,
    error::DecodeError,
    keypair::Keypair,
    pending_txn::{PendingTxn, Status},
    traits::{TxnHash, TxnSign, B64},
    transaction::TransactionService,
    txn_status::TxnStatus,
    Error, Result,
};
use chrono::{DateTime, Duration, Utc};
use db_store::MetaValue;
use file_store::FileStore;
use helium_proto::{
    blockchain_txn::Txn,
    services::{
        follower::{
            self, FollowerSubnetworkLastRewardHeightReqV1, FollowerTxnStreamReqV1,
            FollowerTxnStreamRespV1,
        },
        transaction::{TxnQueryRespV1, TxnStatus as TxnStatusProto},
        Channel, Endpoint,
    },
    BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1, Message,
    SubnetworkReward,
};
use http::Uri;
use poc_metrics::record_duration;
use sqlx::{Pool, Postgres};
use std::{env, ops::Range, time::Duration as StdDuration};
use tokio::time;
use tonic::Streaming;

const DEFAULT_START_REWARD_BLOCK: i64 = 1_500_000;
const DEFAULT_REWARD_INTERVAL_SECS: i64 = 1800; // 30 min
const RECONNECT_WAIT_SECS: i64 = 5;
const DEFAULT_TRIGGER_INTERVAL_SECS: i64 = 900; // 15 min
const STALE_PENDING_TIMEOUT_SECS: i64 = 1800; // 30 min
const DEFAULT_LOOKUP_DELAY_SECS: i64 = 1800; // 30 min
const CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const RPC_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const DEFAULT_URI: &str = "http://0.0.0.0:8080";

const TXN_TYPES: &[&str] = &["blockchain_txn_subnetwork_rewards_v1"];

pub struct Server {
    keypair: Keypair,
    follower_client: follower::Client<Channel>,
    pool: Pool<Postgres>,
    reward_interval: Duration,
    start_reward_block: i64,
    trigger_interval: Duration,
    txn_service: TransactionService,
    verifier_store: FileStore,
}

impl Server {
    pub async fn new(pool: Pool<Postgres>, keypair: Keypair) -> Result<Self> {
        let result = Self {
            pool,
            keypair,
            follower_client: new_follower_from_env()?,
            reward_interval: Duration::seconds(
                env::var("REWARD_INTERVAL")
                    .unwrap_or_else(|_| DEFAULT_REWARD_INTERVAL_SECS.to_string())
                    .parse()
                    .map_err(DecodeError::from)?,
            ),
            start_reward_block: env::var("FOLLOWER_START_BLOCK")
                .unwrap_or_else(|_| DEFAULT_START_REWARD_BLOCK.to_string())
                .parse()
                .map_err(DecodeError::from)?,
            trigger_interval: Duration::seconds(
                env::var("TRIGGER_INTERVAL")
                    .unwrap_or_else(|_| DEFAULT_TRIGGER_INTERVAL_SECS.to_string())
                    .parse()
                    .map_err(DecodeError::from)?,
            ),
            txn_service: TransactionService::from_env()?,
            verifier_store: FileStore::from_env().await?,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting iot-reward server");

        // Handle any rewards created but not submitted on a previous run
        self.process_prior_created_txns().await?;

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping iot-reward server");
                return Ok(());
            }

            let follow_start =
                *MetaValue::<i64>::fetch_or_insert_with(&self.pool, "last_follower_height", || {
                    self.start_reward_block
                })
                .await?
                .value() as u64;

            tracing::info!("connecting to blockchain txn stream at height {follow_start}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = txn_stream(&mut self.follower_client, follow_start, &[], TXN_TYPES) => match stream_result {
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
        let timer = time::sleep(
            Duration::seconds(RECONNECT_WAIT_SECS)
                .to_std()
                .expect("valid interval in seconds"),
        );
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
        let mut trigger_timer = time::interval(
            self.trigger_interval
                .to_std()
                .expect("valid interval in seconds"),
        );

        loop {
            tokio::select! {
                msg = txn_stream.message() => match msg {
                    Ok(Some(txn)) => {
                        tracing::info!("txn received from stream");
                        self.process_txn_entry(txn).await?
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
            _ => Ok(()),
        }
    }

    async fn process_subnet_rewards(
        &mut self,
        envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnSubnetworkRewardsV1,
    ) -> Result {
        if txn.token_type() != BlockchainTokenTypeV1::Iot {
            return Ok(());
        }

        let txn_hash = &envelope.txn_hash.to_b64_url()?;
        let txn_ts = envelope.timestamp;
        PendingTxn::update(
            &self.pool,
            txn_hash,
            Status::Cleared,
            datetime_from_epoch(txn_ts as i64),
        )
        .await
    }

    async fn process_clock_tick(&mut self) -> Result {
        let now = Utc::now();
        let reward_period = reward_period(&mut self.follower_client).await?;
        let current_height = reward_period.end;
        tracing::info!(
            "processing clock tick at height {} with time {}",
            current_height,
            now.to_string()
        );

        update_last_follower_height(&self.pool, current_height).await?;

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

        let last_reward_time =
            MetaValue::<i64>::fetch_or_insert_with(&self.pool, "last_reward_end_time", || {
                let starting_ts = now.timestamp();
                tracing::info!(
                    "no last_reward_end_time found, inserting current timestamp {starting_ts}"
                );
                starting_ts
            })
            .await?;

        tracing::info!(
            "found last_reward_end_time: {:#?}",
            *last_reward_time.value()
        );

        let (start_utc, end_utc) = get_time_range(*last_reward_time.value());
        if end_utc - start_utc > self.reward_interval {
            // Handle rewards if we've passed our duration
            record_duration!(
                "reward_server_emission_duration",
                self.handle_rewards(reward_period, last_reward_time, start_utc..end_utc)
                    .await?
            )
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
                    if TxnStatus::try_from(status)? == TxnStatus::from(TxnStatusProto::NotFound)
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
                "failed txns in pending".to_string(),
            ));
        }
        Ok(())
    }

    async fn handle_rewards(
        &mut self,
        reward_period: Range<u64>,
        mut last_reward_end_time: MetaValue<i64>,
        time_range: Range<DateTime<Utc>>,
    ) -> Result {
        tracing::info!("triggering rewards emission");

        // Grab the appropriate files from the verifier store for iot

        let rewards = vec![];

        // Iterate over the results coming from the store and construct txn based on the rewardable shares computed per owner

        if !rewards.is_empty() {
            let pool = self.pool.clone();

            // Submit the rewards txn
            self.issue_rewards(rewards, reward_period).await?;

            // Update the meta record for a successful submission
            last_reward_end_time
                .update(&pool, time_range.end.timestamp())
                .await?;
        } else {
            tracing::error!("cannot continue; unable to determine reward period!");
            return Err(Error::NotFound("invalid reward period".to_string()));
        }
        Ok(())
    }

    async fn issue_rewards(
        &mut self,
        rewards: Vec<SubnetworkReward>,
        reward_period: Range<u64>,
    ) -> Result {
        if rewards.is_empty() {
            tracing::info!("nothing to reward");
            return Ok(());
        }

        let (txn, txn_hash_str) = construct_txn(&self.keypair, rewards, reward_period)?;

        // binary encode txn for storage
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
            metrics::increment_counter!("reward_server_emission");
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

async fn update_last_follower_height(pool: &Pool<Postgres>, last_height: u64) -> Result {
    metrics::gauge!("reward_server_last_reward_height", last_height as f64);
    MetaValue::new("last_follower_height", last_height)
        .insert(pool)
        .await?;
    Ok(())
}

fn new_follower_from_env() -> Result<follower::Client<Channel>> {
    let uri: Uri = env::var("FOLLOWER_URI")
        .unwrap_or_else(|_| DEFAULT_URI.to_string())
        .parse()?;
    let channel = Endpoint::from(uri)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect_lazy();
    Ok(follower::Client::new(channel))
}

async fn txn_stream<T>(
    client: &mut follower::Client<Channel>,
    height: u64,
    txn_hash: &[u8],
    txn_types: &[T],
) -> Result<Streaming<FollowerTxnStreamRespV1>>
where
    T: ToString,
{
    let req = FollowerTxnStreamReqV1 {
        height,
        txn_hash: txn_hash.to_vec(),
        txn_types: txn_types.iter().map(|e| e.to_string()).collect(),
    };
    let res = client.txn_stream(req).await?.into_inner();
    Ok(res)
}

async fn reward_period(client: &mut follower::Client<Channel>) -> Result<Range<u64>> {
    let req = FollowerSubnetworkLastRewardHeightReqV1 {
        token_type: BlockchainTokenTypeV1::Iot.into(),
    };
    let res = client
        .subnetwork_last_reward_height(req)
        .await?
        .into_inner();

    Ok(res.reward_height + 1..res.height)
}

fn get_time_range(last_reward_end_time: i64) -> (DateTime<Utc>, DateTime<Utc>) {
    let after_utc = datetime_from_epoch(last_reward_end_time);
    let now = Utc::now();
    let stop_utc = now - Duration::seconds(DEFAULT_LOOKUP_DELAY_SECS);
    let start_utc = after_utc.min(stop_utc);
    (start_utc, stop_utc)
}

fn construct_txn(
    keypair: &Keypair,
    rewards: Vec<SubnetworkReward>,
    period: Range<u64>,
) -> Result<(BlockchainTxnSubnetworkRewardsV1, String)> {
    let mut txn = BlockchainTxnSubnetworkRewardsV1 {
        rewards,
        token_type: BlockchainTokenTypeV1::Iot.into(),
        start_epoch: period.start,
        end_epoch: period.end,
        reward_server_signature: vec![],
    };
    txn.reward_server_signature = txn.sign(keypair)?;
    let hash_str = txn.hash().and_then(|hash| hash.to_b64_url())?;
    Ok((txn, hash_str))
}
