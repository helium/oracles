use crate::{
    pending_txn::{PendingTxn, Status},
    traits::B64,
    txn_status::TxnStatus,
    Error, Result, Settings,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{traits::TimestampDecode, FileStore, FileType};
use futures::{stream, StreamExt};
use helium_crypto::{Keypair, Sign};
use helium_proto::{
    blockchain_txn::Txn,
    services::{
        follower::{
            self, FollowerSubnetworkLastRewardHeightReqV1, FollowerTxnStreamReqV1,
            FollowerTxnStreamRespV1,
        },
        transaction::{TxnQueryRespV1, TxnStatus as ProtoTxnStatus},
        Channel,
    },
    BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1, Message,
    SubnetworkReward, SubnetworkRewards,
};
use node_follower::txn_service::TransactionService;
use poc_metrics::record_duration;
use sha2::{Digest, Sha256};
use sqlx::{Pool, Postgres};
use std::ops::Range;
use tokio::time;
use tonic::Streaming;

pub const DEFAULT_START_REWARD_BLOCK: i64 = 1477650;

const RECONNECT_WAIT_SECS: i64 = 5;
const STALE_PENDING_TIMEOUT_SECS: i64 = 1800; // 30 min

pub const TXN_TYPES: &[&str] = &["blockchain_txn_subnetwork_rewards_v1"];

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Keypair,
    follower_client: follower::Client<Channel>,
    txn_service: TransactionService,
    last_follower_height: MetaValue<i64>,
    trigger_interval: Duration,
    reward_interval: Duration,
    verifier_store: FileStore,
}

impl Server {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(10).await?;
        let keypair = settings.keypair()?;
        let start_reward_block = settings
            .follower
            .block
            .unwrap_or(DEFAULT_START_REWARD_BLOCK);
        Ok(Self {
            keypair,
            follower_client: settings.follower.connect_follower()?,
            txn_service: TransactionService::from_settings(&settings.transactions)?,
            trigger_interval: Duration::seconds(settings.trigger),
            last_follower_height: MetaValue::<i64>::fetch_or_insert_with(
                &pool,
                "last_follower_height",
                || start_reward_block,
            )
            .await?,
            reward_interval: Duration::seconds(settings.rewards),
            verifier_store: FileStore::from_settings(&settings.verifier).await?,
            pool,
        })
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

            let curr_height = *self.last_follower_height.value() as u64;
            tracing::info!("connecting to blockchain txn stream at height {curr_height}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = txn_stream(&mut self.follower_client, curr_height, &[], TXN_TYPES) => match stream_result {
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

        let txn_hash = &envelope.txn_hash.to_b64_url()?;
        let txn_ts = envelope.timestamp.to_timestamp()?;
        PendingTxn::update(&self.pool, txn_hash, Status::Cleared, txn_ts).await
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

        self.last_follower_height
            .update(&self.pool, current_height as i64)
            .await?;

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

        let (start_utc, end_utc) = get_time_range(*last_reward_time.value(), Utc::now());
        if end_utc - start_utc > self.reward_interval {
            // Handle rewards if we pass our duration
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

    async fn handle_rewards(
        &mut self,
        reward_period: Range<u64>,
        mut last_reward_end_time: MetaValue<i64>,
        time_range: Range<DateTime<Utc>>,
    ) -> Result {
        tracing::info!("triggering rewards emissions");

        let file_list = self
            .verifier_store
            .list_all(
                FileType::SubnetworkRewards,
                time_range.start,
                time_range.end,
            )
            .await?;

        // TODO: We need a better pattern for this:
        let mut stream = self
            .verifier_store
            .source(stream::iter(file_list).map(Ok).boxed());
        let mut rewards = vec![];
        while let Some(Ok(msg)) = stream.next().await {
            let subnet_rewards = SubnetworkRewards::decode(msg);
            if let Ok(subnet_rewards) = subnet_rewards {
                // We only care that the verified epoch ends in our rewards epoch,
                // so that we include verified epochs that straddle two rewards epochs.
                if time_range.contains(&Utc.timestamp(subnet_rewards.end_epoch as i64, 0)) {
                    rewards.extend(subnet_rewards.rewards);
                }
            }
        }

        if !rewards.is_empty() {
            self.issue_rewards(rewards, reward_period).await?;
            last_reward_end_time
                .update(&self.pool, time_range.end.timestamp())
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
    let res = client
        .subnetwork_last_reward_height(FollowerSubnetworkLastRewardHeightReqV1 {
            token_type: BlockchainTokenTypeV1::Mobile as i32,
        })
        .await?
        .into_inner();

    Ok(res.reward_height + 1..res.height)
}

pub fn get_time_range(
    last_reward_end_time: i64,
    now: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let last_timestamp_utc = Utc.timestamp(last_reward_end_time, 0);
    (last_timestamp_utc.min(now), now)
}

pub fn construct_txn(
    keypair: &Keypair,
    rewards: Vec<SubnetworkReward>,
    period: Range<u64>,
) -> Result<(BlockchainTxnSubnetworkRewardsV1, String)> {
    let mut txn = BlockchainTxnSubnetworkRewardsV1 {
        rewards,
        token_type: BlockchainTokenTypeV1::Mobile as i32,
        start_epoch: period.start,
        end_epoch: period.end,
        reward_server_signature: vec![],
    };
    txn.reward_server_signature = sign_txn(&txn, keypair)?;
    let hash = hash_txn_b64_url(&txn);
    Ok((txn, hash))
}

fn hash_txn_b64_url(txn: &BlockchainTxnSubnetworkRewardsV1) -> String {
    let mut txn = txn.clone();
    txn.reward_server_signature = vec![];
    let digest = Sha256::digest(txn.encode_to_vec()).to_vec();
    base64::encode_config(digest, base64::URL_SAFE_NO_PAD)
}

fn sign_txn(txn: &BlockchainTxnSubnetworkRewardsV1, keypair: &Keypair) -> Result<Vec<u8>> {
    let mut txn = txn.clone();
    txn.reward_server_signature = vec![];
    Ok(keypair.sign(&txn.encode_to_vec())?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_time_range_returns_24_hour_period() {
        let last_timestamp = Utc.ymd(2022, 11, 1).and_hms(0, 0, 0).timestamp();
        let now = Utc.ymd(2022, 11, 2).and_hms(0, 0, 0);
        let (start, stop) = get_time_range(last_timestamp, now);

        assert_eq!(start, Utc.ymd(2022, 11, 1).and_hms(0, 0, 0));
        assert_eq!(stop, now);
    }

    #[test]
    fn get_time_range_will_handle_last_timestamp_being_after_now() {
        let last_timestamp = Utc.ymd(2022, 11, 2).and_hms(0, 0, 1).timestamp();
        let now = Utc.ymd(2022, 11, 2).and_hms(0, 0, 0);
        let (start, stop) = get_time_range(last_timestamp, now);

        assert_eq!(start, now);
        assert_eq!(stop, now);
    }
}
