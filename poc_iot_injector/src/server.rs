use crate::{
    receipt_txn::{handle_report_msg, TxnDetails},
    Settings,
};
use file_store::{file_info_poller::FileInfoStream, iot_valid_poc::IotPoc};
use futures::stream::StreamExt;
use governor::{Jitter, Quota, RateLimiter};
use helium_crypto::Keypair;
use node_follower::txn_service::TransactionService;
use sqlx::{Pool, Postgres};
use std::{num::NonZeroU32, sync::Arc, time::Duration as StdDuration};
use tokio::{sync::mpsc::Receiver, task::JoinSet};

const MAX_CONCURRENT_SUBMISSIONS: usize = 16;

pub struct Server {
    pool: Pool<Postgres>,
    keypair: Arc<Keypair>,
    txn_service: Option<TransactionService>,
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

#[derive(thiserror::Error, Debug)]
pub enum SubmissionError {
    #[error("rate limit not set")]
    RateLimitNotSet,
}

impl Server {
    pub async fn new(settings: &Settings) -> Result<Self, NewServerError> {
        let pool = settings.database.connect(10).await?;
        let keypair = settings.keypair()?;

        let result = Self {
            pool,
            settings: settings.clone(),
            keypair: Arc::new(keypair),
            // Only create txn_service if do_submission is true
            txn_service: settings
                .do_submission
                .then_some(TransactionService::from_settings(&settings.transactions)),
        };
        Ok(result)
    }

    pub async fn run(
        &mut self,
        shutdown: &triggered::Listener,
        mut receiver: Receiver<FileInfoStream<IotPoc>>,
    ) -> anyhow::Result<()> {
        tracing::info!("starting poc-iot-injector server");
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                msg = receiver.recv() => if let Some(stream) =  msg {
                    self.submit_txns(stream).await?;
                }
            }
        }
        Ok(())
    }

    async fn submit_txns(&self, file_info_stream: FileInfoStream<IotPoc>) -> anyhow::Result<()> {
        let mut transaction = self.pool.begin().await?;
        let mut stream = file_info_stream.into_stream(&mut transaction).await?;

        let max_txns_per_min = NonZeroU32::new(self.settings.max_txns_per_min)
            .ok_or(SubmissionError::RateLimitNotSet)?;
        let bucket = Arc::new(RateLimiter::direct(Quota::per_minute(max_txns_per_min)));

        // To ensure that multiple tasks don't wake up at the exact same time
        let jitter = Jitter::up_to(StdDuration::from_secs(5));

        let mut set = JoinSet::new();

        while let Some(iot_poc) = stream.next().await {
            let bucket = bucket.clone();

            let shared_key = self.keypair.clone();
            let mut shared_txn_service = self.txn_service.clone();
            let do_submission = self.settings.do_submission;
            let max_witnesses_per_receipt = self.settings.max_witnesses_per_receipt;

            tracing::info!("Spawning submission tasks...");
            set.spawn(async move {
                bucket.until_ready_with_jitter(jitter).await;

                let _ = process_submission(
                    iot_poc,
                    shared_key,
                    do_submission,
                    max_witnesses_per_receipt,
                    &mut shared_txn_service,
                )
                .await;
            });

            if set.len() > MAX_CONCURRENT_SUBMISSIONS {
                tracing::info!("Processing {MAX_CONCURRENT_SUBMISSIONS} submissions");
                set.join_next().await;
            }
        }

        // Make sure the tasks are finished to completion even when we run out of stream items
        while !set.is_empty() {
            set.join_next().await;
        }

        transaction.commit().await?;

        Ok(())
    }
}

async fn process_submission(
    iot_poc: IotPoc,
    shared_key: Arc<Keypair>,
    do_submission: bool,
    max_witnesses_per_receipt: u64,
    txn_service: &mut Option<TransactionService>,
) -> anyhow::Result<()> {
    match handle_report_msg(iot_poc, shared_key, max_witnesses_per_receipt) {
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
