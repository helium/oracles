use crate::db::{self, TxnRow};
use anyhow::Result;
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana::{start_boost::SolanaNetwork, GetSignature};
use sqlx::{Pool, Postgres};
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

pub struct Updater<S> {
    pool: Pool<Postgres>,
    chain_enabled: bool,
    interval: Duration,
    batch_size: usize,
    pub solana: S,
}

impl<S> ManagedTask for Updater<S>
where
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));
        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl<S> Updater<S>
where
    S: SolanaNetwork,
{
    pub fn new(
        pool: Pool<Postgres>,
        chain_enabled: bool,
        interval: Duration,
        batch_size: usize,
        solana: S,
    ) -> Result<Self> {
        Ok(Self {
            pool,
            chain_enabled,
            interval,
            batch_size,
            solana,
        })
    }

    pub async fn run(self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!("starting Updater");
        // on startup if there are activations in the DB with 'queued' status and WITH a txn id then
        // it suggests we crashed out early of the updater tick
        // after we started the solana activation flow
        // we need to check if these txns are onchain and if not then null the txn id
        // if they are on chain then update their status to success
        let txns_ids_to_verify = db::get_txns_ids_to_verify(&self.pool).await?;
        tracing::warn!(
            "checking {} txn_ids on chain status",
            txns_ids_to_verify.len()
        );

        if !txns_ids_to_verify.is_empty() {
            // If we have activations that we need to verify, wait one minute to ensure
            // every transaction has been confirmed on chain
            tracing::info!("We have pending txn_id's to verify, sleeping for one minute to given them time to appear on-chain");
            tokio::time::sleep(Duration::from_secs(60)).await;
            for p in txns_ids_to_verify {
                self.confirm_txn(&p).await?
            }
        }

        let mut timer = time::interval(self.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = timer.tick() => {
                    if self.chain_enabled {
                         self.process_activations().await?
                     }
                     else {
                         tracing::info!("processing of activations is disabled, skipping...")}
                     }
            }
        }
        tracing::info!("stopping Updater");
        Ok(())
    }

    pub async fn process_activations(&self) -> Result<()> {
        self.check_failed_activations().await?;
        // get the batch of queued activations to update on chain
        let activations = db::get_queued_batch(&self.pool).await?;

        if activations.is_empty() {
            tracing::info!("no activations in queue");
            return Ok(());
        }

        let activations_count = activations.len() as u64;
        tracing::info!(
            num_of_activations = activations_count,
            "processing activations,"
        );

        // slice the activations up into batches of N activations and submit to solana
        for batch in activations.chunks(self.batch_size) {
            let batch_size = batch.len();

            // get a list of all the activations DB ids which form part of the batch
            let ids: Vec<u64> = batch.iter().map(|sp| sp.location).collect();

            let solana_txn = self.solana.make_start_boost_transaction(batch).await?;
            let transaction_id = solana_txn.get_signature().to_string();
            // update the batch in the db with the txn id
            db::save_batch_txn_id(&self.pool, &transaction_id, &ids).await?;

            // if activations were processed successfully then
            // update their status in the DB to success
            // if not processed successfully then bump their retry count
            // if retry count is below max retries then the activations
            // will be retried next tick
            match self.solana.submit_transaction(&solana_txn).await {
                Ok(()) => {
                    self.handle_submit_txn_success(&ids, batch_size, activations_count)
                        .await?;
                }
                Err(e) => {
                    tracing::warn!("submit txn failed, error: {}", e);
                    self.handle_submit_txn_failure(&ids, batch_size).await?;
                }
            };
        }
        Ok(())
    }

    async fn check_failed_activations(&self) -> Result<()> {
        let num_marked_failed = db::update_failed_activations(&self.pool).await?;
        metrics::counter!("failed_activations", num_marked_failed);
        let total_failed_count = db::get_failed_activations_count(&self.pool).await?;
        metrics::gauge!("db_failed_row_count", total_failed_count as f64);
        if total_failed_count > 0 {
            tracing::warn!("{} failed status activations ", total_failed_count);
        };
        Ok(())
    }

    async fn handle_submit_txn_success(
        &self,
        ids: &[u64],
        batch_size: usize,
        summed_activations_count: u64,
    ) -> Result<()> {
        tracing::info!("processed batch of {} activations successfully", batch_size);
        metrics::counter!("success_activations", summed_activations_count);
        db::update_success_batch(&self.pool, ids).await?;
        Ok(())
    }

    async fn handle_submit_txn_failure(&self, ids: &[u64], batch_size: usize) -> Result<()> {
        tracing::info!(
            "failed to process batch of {} activations, retrying next tick",
            batch_size
        );
        db::update_failed_batch(&self.pool, ids).await?;
        Ok(())
    }

    async fn confirm_txn<'a>(&self, txn_row: &TxnRow) -> Result<()> {
        if self.solana.confirm_transaction(&txn_row.txn_id).await? {
            tracing::info!("txn_id {} confirmed on chain, updated db", txn_row.txn_id);
            db::update_verified_txns_onchain(&self.pool, &txn_row.txn_id).await?
        } else {
            tracing::info!(
                "txn_id {} confirmed NOT on chain, updated db and requeued activations",
                txn_row.txn_id
            );
            db::update_verified_txns_not_onchain(&self.pool, &txn_row.txn_id).await?
        }
        Ok(())
    }
}
