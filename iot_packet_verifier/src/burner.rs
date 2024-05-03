use crate::{
    balances::{BalanceCache, BalanceStore},
    pending::{
        confirm_pending_txns, Burn, ConfirmPendingError, PendingTables, PendingTablesTransaction,
    },
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana::{burn::SolanaNetwork, GetSignature};
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

pub struct Burner<P, S> {
    pending_tables: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

impl<P, S> ManagedTask for Burner<P, S>
where
    P: PendingTables + Send + Sync + 'static,
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
pub enum BurnError<S> {
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Solana error: {0}")]
    SolanaError(S),
    #[error("Confirm pending transaction error: {0}")]
    ConfirmPendingError(#[from] ConfirmPendingError<S>),
}

impl<P, S> Burner<P, S> {
    pub fn new(pending_tables: P, balances: &BalanceCache<S>, burn_period: u64, solana: S) -> Self {
        Self {
            pending_tables,
            balances: balances.balances(),
            burn_period: Duration::from_secs(60 * burn_period),
            solana,
        }
    }
}

impl<P, S> Burner<P, S>
where
    P: PendingTables + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<(), BurnError<S::Error>> {
        tracing::info!("Starting burner");
        let mut burn_timer = time::interval(self.burn_period);
        burn_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            #[rustfmt::skip]
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

    pub async fn burn(&mut self) -> Result<(), BurnError<S::Error>> {
        // Fetch the next payer and amount that should be burn. If no such burn
        // exists, perform no action.
        let Some(Burn { payer, amount }) = self.pending_tables.fetch_next_burn().await? else {
            return Ok(());
        };

        tracing::info!(%amount, %payer, "Burning DC");

        // Create a burn transaction and execute it:
        let txn = self
            .solana
            .make_burn_transaction(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;
        self.pending_tables
            .add_pending_transaction(&payer, amount, txn.get_signature())
            .await?;
        self.solana
            .submit_transaction(&txn)
            .await
            .map_err(BurnError::SolanaError)?;

        // Removing the pending transaction and subtract the burn amount
        // now that we have confirmation that the burn transaction is confirmed
        // on chain:
        let mut pending_tables_txn = self.pending_tables.begin().await?;
        pending_tables_txn
            .remove_pending_transaction(txn.get_signature())
            .await?;
        pending_tables_txn
            .subtract_burned_amount(&payer, amount)
            .await?;
        pending_tables_txn.commit().await?;

        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(&payer).unwrap();
        // Reduce the pending burn amount and the payer's balance by the amount
        // we've burned.
        payer_account.burned = payer_account.burned.saturating_sub(amount);
        payer_account.balance = payer_account.balance.saturating_sub(amount);

        metrics::counter!("burned", "payer" => payer.to_string()).increment(amount);

        Ok(())
    }
}
