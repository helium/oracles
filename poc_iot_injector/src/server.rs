use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Settings, LOADER_WORKERS, STORE_WORKERS,
};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use db_store::MetaValue;
use file_store::{file_sink, file_sink_write, FileStore, FileType};
use futures::stream::{self, StreamExt};
use helium_crypto::Keypair;
use node_follower::txn_service::TransactionService;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::time;

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Arc<Keypair>,
    txn_service: Option<TransactionService>,
    iot_verifier_store: FileStore,
    last_poc_submission_ts: MetaValue<i64>,
    tick_time: StdDuration,
    receipt_sender: file_sink::MessageSender,
    do_submission: bool,
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
    pub async fn new(
        settings: &Settings,
        receipt_sender: file_sink::MessageSender,
    ) -> Result<Self, NewServerError> {
        let pool = settings.database.connect(10).await?;
        let keypair = settings.keypair()?;
        let tick_time = settings.trigger_interval();
        let do_submission = settings.do_submission;

        // Check meta for last_poc_submission_ts, if not found, use the env var and insert it
        let last_poc_submission_ts =
            MetaValue::<i64>::fetch_or_insert_with(&pool, "last_reward_end_time", || {
                settings.last_poc_submission
            })
            .await?;

        let result = Self {
            pool: pool.clone(),
            keypair: Arc::new(keypair),
            tick_time,
            // Only create txn_service if do_submission is true
            txn_service: do_submission
                .then_some(TransactionService::from_settings(&settings.transactions)),
            iot_verifier_store: FileStore::from_settings(&settings.verifier).await?,
            last_poc_submission_ts,
            receipt_sender,
            do_submission,
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
        let after_utc = Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(
            *self.last_poc_submission_ts.value(),
            0,
        ));
        let before_utc = Utc::now();
        tracing::info!(
            "handling poc_tick, after_utc: {:?}, before_utc: {:?}",
            after_utc,
            before_utc
        );

        submit_txns(
            &mut self.txn_service,
            &mut self.iot_verifier_store,
            self.keypair.clone(),
            &self.receipt_sender,
            after_utc,
            before_utc,
            self.do_submission,
        )
        .await?;

        tracing::info!("updating last_poc_submission_ts to {:?}", before_utc);
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
    txn_service: &mut Option<TransactionService>,
    store: &mut FileStore,
    keypair: Arc<Keypair>,
    receipt_sender: &file_sink::MessageSender,
    after_utc: DateTime<Utc>,
    before_utc: DateTime<Utc>,
    do_submission: bool,
) -> anyhow::Result<()> {
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
                    Err(err) => tracing::warn!("skipping entry in stream: {err:?}"),
                    Ok(buf) => match handle_report_msg(buf, shared_key, before_ts) {
                        Ok(txn_details) => {
                            tracing::debug!("txn_details: {:?}", txn_details);
                            if do_submission {
                                tracing::info!("submitting txn: {:?}", txn_details.hash_b64_url);
                                if let Err(err) = handle_txn_submission(
                                    txn_details.clone(),
                                    &mut shared_txn_service,
                                )
                                .await
                                {
                                    tracing::warn!("unable to submit_txn, {err:?}");
                                } else {
                                    tracing::info!("storing txn: {:?}", txn_details.hash_b64_url);
                                    metrics::increment_counter!("poc_injector_receipt_count");
                                    let _ = file_sink_write!(
                                        "signed_poc_receipt_txn",
                                        receipt_sender,
                                        txn_details.txn
                                    )
                                    .await;
                                }
                            } else {
                                tracing::info!("only storing txn: {:?}", txn_details.hash_b64_url);
                                metrics::increment_counter!("poc_injector_receipt_count");
                                let _ = file_sink_write!(
                                    "signed_poc_receipt_txn",
                                    receipt_sender,
                                    txn_details.txn
                                )
                                .await;
                            }
                        }
                        Err(err) => tracing::warn!("unable to construct txn_details, {err:?}"),
                    },
                }
            }
        })
        .await;
    Ok(())
}

#[derive(thiserror::Error, Debug)]
#[error("txn submission failed: {0}")]
pub struct HandleTxnSubmissionError(pub String);

async fn handle_txn_submission(
    txn_details: TxnDetails,
    txn_service: &mut Option<TransactionService>,
) -> Result<(), HandleTxnSubmissionError> {
    match txn_service {
        Some(txn_service) => {
            if txn_service
                .submit(txn_details.txn, &txn_details.hash)
                .await
                .is_ok()
            {
                tracing::debug!(
                    "txn submitted successfully, hash: {:?}",
                    txn_details.hash_b64_url
                );
                Ok(())
            } else {
                tracing::warn!(
                    "txn submission failed!, hash: {:?}",
                    txn_details.hash_b64_url
                );
                Err(HandleTxnSubmissionError(txn_details.hash_b64_url))
            }
        }
        None => Ok(()),
    }
}
