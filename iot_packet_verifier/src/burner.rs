use crate::{
    balances::{BalanceCache, BalanceStore},
    pending::{
        confirm_pending_txns, Burn, ConfirmPendingError, PendingTables, PendingTablesTransaction,
    },
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKeyBinary;
use solana::{burn::SolanaNetwork, sender, SolanaRpcError};
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};
use tracing::Instrument;

pub struct Burner<P, S> {
    pending_tables: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

impl<P, S> ManagedTask for Burner<P, S>
where
    P: PendingTables,
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError {
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Solana error: {0}")]
    SolanaError(#[from] SolanaRpcError),
    #[error("Confirm pending transaction error: {0}")]
    ConfirmPendingError(#[from] ConfirmPendingError),
}

impl<P, S> Burner<P, S> {
    pub fn new(
        pending_tables: P,
        balances: &BalanceCache<S>,
        burn_period: Duration,
        solana: S,
    ) -> Self {
        Self {
            pending_tables,
            balances: balances.balances(),
            burn_period,
            solana,
        }
    }
}

impl<P, S> Burner<P, S>
where
    P: PendingTables + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<(), BurnError> {
        tracing::info!("Starting burner");
        let mut burn_timer = time::interval(self.burn_period);
        burn_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = burn_timer.tick() => {
                    match self.burn().await {
                        Ok(()) => continue,
                        Err(err) => {
                            tracing::error!("Error while burning data credits: {err}");
                            confirm_pending_txns(&self.pending_tables, &self.solana, &self.balances).await?;
                        }
                    }
                }
            }
        }
        tracing::info!("Stopping burner");
        Ok(())
    }

    pub async fn burn(&mut self) -> Result<(), BurnError> {
        // There should only be a single pending txn at a time
        let pending_txns = self.pending_tables.fetch_all_pending_txns().await?;
        if !pending_txns.is_empty() {
            tracing::info!(pending_txns = pending_txns.len(), "skipping burn");
            return Ok(());
        }

        // Fetch the next payer and amount that should be burn. If no such burn
        // exists, perform no action.
        let Some(Burn { payer, amount }) = self.pending_tables.fetch_next_burn().await? else {
            tracing::info!("no pending burns");
            return Ok(());
        };

        tracing::info!(%amount, %payer, "Burning DC");

        let txn = self
            .solana
            .make_burn_transaction(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        let store = BurnTxnStore::new(
            self.pending_tables.clone(),
            self.balances.clone(),
            payer.clone(),
            amount,
        );

        let burn_span = tracing::info_span!("burn_txn", %payer, amount);
        self.solana
            .submit_transaction(&txn, &store)
            .map_err(BurnError::SolanaError)
            .instrument(burn_span)
            .await
    }
}

pub struct BurnTxnStore<PT> {
    pool: PT,
    balances: BalanceStore,
    payer: PublicKeyBinary,
    amount: u64,
}

impl<PT: PendingTables + Clone> BurnTxnStore<PT> {
    pub fn new(pool: PT, balances: BalanceStore, payer: PublicKeyBinary, amount: u64) -> Self {
        Self {
            pool,
            balances,
            payer,
            amount,
        }
    }
}

#[async_trait::async_trait]
impl<PT: PendingTables> sender::TxnStore for BurnTxnStore<PT> {
    async fn on_prepared(&self, txn: &solana::Transaction) -> sender::SenderResult<()> {
        tracing::info!("txn prepared");

        let signature = txn.get_signature();
        let add_pending = self
            .pool
            .add_pending_transaction(&self.payer, self.amount, signature);

        let Ok(()) = add_pending.await else {
            tracing::error!("failed to add pending transcation");
            return Err(sender::SenderError::preparation(
                "could not add pending transaction",
            ));
        };

        Ok(())
    }

    async fn on_finalized(&self, txn: &solana::Transaction) {
        tracing::info!("txn finalized");

        let Ok(mut db_txn) = self.pool.begin().await else {
            tracing::error!("failed to start finalized txn db transaction");
            return;
        };

        let signature = txn.get_signature();
        let Ok(()) = db_txn.remove_pending_transaction(signature).await else {
            tracing::error!("failed to remove pending");
            return;
        };

        let Ok(()) = db_txn
            .subtract_burned_amount(&self.payer, self.amount)
            .await
        else {
            tracing::error!("failed to subtract burned amount");
            return;
        };

        // Subtract balances from map before submitted db txn
        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(&self.payer).unwrap();
        // Reduce the pending burn amount and the payer's balance by the amount we've burned
        payer_account.burned = payer_account.burned.saturating_sub(self.amount);
        payer_account.balance = payer_account.balance.saturating_sub(self.amount);

        let Ok(()) = db_txn.commit().await else {
            tracing::error!("failed to commit finalized txn db transaction");
            return;
        };

        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "true"
        )
        .increment(self.amount);
    }

    async fn on_error(&self, txn: &solana::Transaction, err: sender::SenderError) {
        tracing::warn!(?err, "txn failed");
        let Ok(mut db_txn) = self.pool.begin().await else {
            tracing::error!("failed to start error transaction");
            return;
        };

        let signature = txn.get_signature();
        let Ok(()) = db_txn.remove_pending_transaction(signature).await else {
            tracing::error!("failed to remove pending transaction on error");
            return;
        };

        let Ok(()) = db_txn.commit().await else {
            tracing::error!("failed to commit on error transaction");
            return;
        };

        metrics::counter!(
            "burned",
            "payer" => self.payer.to_string(),
            "success" => "false"
        )
        .increment(self.amount);
    }
}
