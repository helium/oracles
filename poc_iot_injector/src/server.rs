use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Settings, LOADER_WORKERS,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use db_store::MetaValue;
use file_store::{traits::TimestampDecode, FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_crypto::Keypair;
use node_follower::txn_service::TransactionService;
use prost::bytes::BytesMut;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::{task::JoinSet, time};

const MAX_CONCURRENT_SUBMISSIONS: usize = 16;

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Arc<Keypair>,
    txn_service: Option<TransactionService>,
    iot_verifier_store: FileStore,
    last_poc_submission_ts: MetaValue<i64>,
    tick_time: StdDuration,
    submission_offset: ChronoDuration,
    settings: Settings,
}

#[derive(thiserror::Error, Debug)]
pub enum NewServerError {
    #[error("database error: {0}")]
    DatabaseError(#[from] db_store::Error),
    #[error("keypair error: {0}")]
    KeypairError(#[from] Box<helium_crypto::Error>),
    #[error("file store error: {0}")]
    FileStoreError(#[from] file_store::Error),
}

impl Server {
    pub async fn new(settings: &Settings) -> Result<Self, NewServerError> {
        let pool = settings.database.connect(10).await?;
        let keypair = settings.keypair()?;
        let tick_time = settings.trigger_interval();
        let submission_offset = settings.submission_offset();

        // Check meta for last_poc_submission_ts, if not found, use the env var and insert it
        let last_poc_submission_ts =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_poc_submission_ts", || {
                settings.last_poc_submission
            })
            .await?;

        let result = Self {
            pool: pool.clone(),
            settings: settings.clone(),
            keypair: Arc::new(keypair),
            tick_time,
            submission_offset,
            // Only create txn_service if do_submission is true
            txn_service: settings
                .do_submission
                .then_some(TransactionService::from_settings(&settings.transactions)),
            iot_verifier_store: FileStore::from_settings(&settings.verifier).await?,
            last_poc_submission_ts,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting poc-iot-injector server");
        let mut poc_iot_timer = time::interval(self.tick_time);
        poc_iot_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = poc_iot_timer.tick() => match self.handle_poc_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal poc_iot_injector error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_poc_tick(&mut self) -> anyhow::Result<()> {
        let after_utc = (*self.last_poc_submission_ts.value() as u64).to_timestamp_millis()?;

        let now = Utc::now();
        let before_utc = now
            .checked_sub_signed(self.submission_offset)
            .unwrap_or(now);

        tracing::info!(
            "handling poc_tick, after_utc: {:?}, before_utc: {:?}",
            after_utc,
            before_utc
        );

        submit_txns(
            &mut self.txn_service,
            &mut self.iot_verifier_store,
            self.keypair.clone(),
            after_utc,
            before_utc,
            self.settings.clone(),
        )
        .await?;

        tracing::info!("updating last_poc_submission_ts to {:?}", before_utc);
        // NOTE: All the poc_receipt txns for the corresponding after-before period will be
        // submitted above, may take a while to do that but we need to
        // update_last_poc_submission_ts once we're done doing it. This should ensure that we have
        // at least submitted all the receipts we could for that time period and will look at the
        // next incoming reports in the next poc_iot_timer tick.
        self.last_poc_submission_ts
            .update(&self.pool, before_utc.timestamp_millis())
            .await?;

        Ok(())
    }
}

async fn submit_txns(
    txn_service: &mut Option<TransactionService>,
    store: &mut FileStore,
    keypair: Arc<Keypair>,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
    settings: Settings,
) -> anyhow::Result<()> {
    let file_list = store
        .list_all(FileType::IotPoc, after_utc, before_utc)
        .await?;

    let mut stream =
        store.source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed());

    let mut set = JoinSet::new();

    while let Some(msg) = stream.next().await {
        match msg {
            Err(err) => tracing::warn!("skipping entry in stream: {err:?}"),
            Ok(buf) => {
                let shared_key = keypair.clone();
                let mut shared_txn_service = txn_service.clone();

                tracing::info!("Spawning submission tasks...");
                set.spawn(async move {
                    let _ = process_submission(
                        buf,
                        shared_key,
                        settings.do_submission,
                        settings.max_witnesses_per_receipt,
                        &mut shared_txn_service,
                    )
                    .await;
                });

                if set.len() > MAX_CONCURRENT_SUBMISSIONS {
                    tracing::info!("Processing {MAX_CONCURRENT_SUBMISSIONS} submissions");
                    set.join_next().await;
                }
            }
        }
    }

    // Make sure the tasks are finished to completion even when we run out of stream items
    while !set.is_empty() {
        set.join_next().await;
    }

    Ok(())
}

async fn process_submission(
    buf: BytesMut,
    shared_key: Arc<Keypair>,
    do_submission: bool,
    max_witnesses_per_receipt: u64,
    txn_service: &mut Option<TransactionService>,
) -> anyhow::Result<()> {
    match handle_report_msg(buf, shared_key, max_witnesses_per_receipt) {
        Ok(txn_details) => {
            tracing::debug!("txn_details: {:?}", txn_details);
            if do_submission {
                tracing::info!("submitting txn: {:?}", txn_details.hash_b64_url);
                match handle_txn_submission(txn_details.clone(), txn_service).await {
                    Ok(_) => {
                        tracing::info!("txn sent: {:?}", txn_details.hash_b64_url);
                        metrics::increment_counter!("poc_injector_receipt_count");
                    }
                    Err(err) => {
                        tracing::warn!("unable to submit_txn, {err:?}")
                    }
                }
            } else {
                tracing::info!("not submitting txn: {:?}", txn_details.hash_b64_url);
                metrics::increment_counter!("poc_injector_receipt_count");
            }
        }
        Err(err) => tracing::warn!("unable to construct txn_details, {err:?}"),
    }
    Ok(())
}

async fn handle_txn_submission(
    txn_details: TxnDetails,
    txn_service: &mut Option<TransactionService>,
) -> anyhow::Result<()> {
    match txn_service {
        Some(txn_service) => {
            txn_service
                .submit_random(txn_details.txn, &txn_details.hash)
                .await?;
            Ok(())
        }
        None => Ok(()),
    }
}
