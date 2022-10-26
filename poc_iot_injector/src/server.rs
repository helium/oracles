use crate::{keypair::Keypair, receipt_txn::handle_report_msg, Result, Settings};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_proto::{blockchain_txn::Txn, BlockchainTxn};
use node_follower::txn_service::TransactionService;
use sqlx::{Pool, Postgres};
use std::time::Duration as StdDuration;
use std::{env, sync::Arc};
use tokio::time;

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Arc<Keypair>,
    txn_service: TransactionService,
    iot_verifier_store: FileStore,
    last_poc_submission_ts: MetaValue<i64>,
    tick_time: StdDuration,
}

impl Server {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let pool = settings.database.connect(10).await?;
        let keypair = settings.keypair()?;
        let tick_time = settings.trigger_interval();
        let last_poc_submission_ts = settings.last_poc_submission;

        // Check meta for last_poc_submission_ts, if not found, use the env var and insert it
        let last_poc_submission_ts =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_reward_end_time", || {
                env_last_poc_submission_ts
            })
            .await?;

        let result = Self {
            pool: pool.clone(),
            keypair: Arc::new(keypair),
            tick_time,
            txn_service: TransactionService::from_settings(&settings.transactions)?,
            iot_verifier_store: FileStore::from_settings(&settings.verifier).await?,
            last_poc_submission_ts,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
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

    async fn handle_poc_tick(&mut self) -> Result {
        let after_utc = Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(
            *self.last_poc_submission_ts.value(),
            0,
        ));
        let before_utc = Utc::now();

        submit_txns(
            &mut self.txn_service,
            &mut self.iot_verifier_store,
            self.keypair.clone(),
            after_utc,
            before_utc,
        )
        .await?;

        // NOTE: All the poc_receipt txns for the corresponding after-before period will be
        // submitted above, may take a while to do that but we need to
        // update_last_poc_submission_ts once we're done doing it. This should ensure that we have
        // at least submitted all the receipts we could for that time period and will look at the
        // next incoming reports in the next poc_iot_timer tick.
        self.last_poc_submission_ts
            .update(&self.pool, before_utc.timestamp())
            .await?;

        Ok(())
    }
}

async fn submit_txns(
    txn_service: &mut TransactionService,
    store: &mut FileStore,
    keypair: Arc<Keypair>,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
) -> Result {
    let file_list = store
        .list_all(FileType::LoraValidPoc, after_utc, before_utc)
        .await?;

    let before_ts = before_utc.timestamp_millis();

    store
        .source_unordered(LOADER_WORKERS, stream::iter(file_list).map(Ok).boxed())
        .for_each_concurrent(STORE_WORKERS, |msg| {
            let mut shared_txn_service = txn_service.clone();
            let shared_key = keypair.clone();
            async move {
                match msg {
                    Ok(m) => {
                        handle_txn_submission(m, shared_key, &mut shared_txn_service, before_ts)
                            .await
                    }
                    Err(e) => tracing::error!("unable to process msg due to: {:?}", e),
                }
            }
        })
        .await;
    Ok(())
}

async fn handle_txn_submission(
    msg: prost::bytes::BytesMut,
    shared_key: Arc<Keypair>,
    txn_service: &mut TransactionService,
    before_ts: i64,
) {
    if let Ok(Some((txn, hash, hash_b64_url))) = handle_report_msg(msg, shared_key, before_ts) {
        if txn_service
            .submit(
                BlockchainTxn {
                    txn: Some(Txn::PocReceiptsV2(txn)),
                },
                &hash,
            )
            .await
            .is_ok()
        {
            tracing::debug!("txn submitted successfully, hash: {:?}", hash_b64_url);
        } else {
            tracing::warn!("txn submission failed!, hash: {:?}", hash_b64_url);
        }
    }
}
