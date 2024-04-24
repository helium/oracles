use crate::{
    balances::{BalanceCache, BalanceStore},
    pending::{
        confirm_pending_txns, Burn, ConfirmPendingError, PendingTables, PendingTablesTransaction,
    },
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKeyBinary;
use solana::{burn::SolanaNetwork, GetSignature, IsErrorBlockhashNotFound};
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

pub struct Burner<P, S> {
    pending_tables: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
    retry_delay: Duration,
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
    #[error("Custom error: {0}")]
    CustomError(#[from] anyhow::Error),
}

impl<P, S> Burner<P, S> {
    pub fn new(
        pending_tables: P,
        balances: &BalanceCache<S>,
        burn_period: Duration,
        solana: S,
        retry_delay: Duration,
    ) -> Self {
        Self {
            pending_tables,
            balances: balances.balances(),
            burn_period,
            solana,
            retry_delay,
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
        let mut signed_txn = self.sign_and_prep_txn(&txn, &payer, amount).await?;

        // handle retries, if we encounter a blockhash not found error
        // resign the txn with the latest blockhash before next retry attempt
        let mut attempt = 1;
        const MAX_ATTEMPTS: u32 = 10;
        loop {
            match self.solana.submit_transaction(&signed_txn).await {
                Ok(_) => {
                    tracing::info!(%payer, %amount, "Burned DC");
                    self.handle_burn_success(signed_txn, &payer, amount).await?;
                    break;
                }
                Err(err) if err.is_error_blockhash_not_found() && attempt < MAX_ATTEMPTS => {
                    tracing::error!(%payer, %amount, "block hash not found..possibly stale block hash, resigning txn and retrying");
                    let mut pending_tables_txn = self.pending_tables.begin().await?;
                    pending_tables_txn
                        .remove_pending_transaction(txn.get_signature())
                        .await?;
                    pending_tables_txn.commit().await?;
                    attempt += 1;
                    signed_txn = self.sign_and_prep_txn(&txn, &payer, amount).await?;
                    continue;
                }
                Err(_) if attempt < MAX_ATTEMPTS => {
                    attempt += 1;
                    tokio::time::sleep(self.retry_delay * attempt).await;
                    continue;
                }
                Err(err) => {
                    Err(BurnError::SolanaError(err))?;
                }
            }
        }
        Ok(())
    }

    async fn sign_and_prep_txn(
        &self,
        txn: &S::Transaction,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> anyhow::Result<S::Transaction> {
        let signed_txn = self.solana.sign_transaction(txn).await?;
        self.pending_tables
            .add_pending_transaction(payer, amount, signed_txn.get_signature())
            .await?;
        Ok(signed_txn)
    }

    async fn handle_burn_success(
        &self,
        txn: S::Transaction,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), BurnError<S::Error>> {
        // Removing the pending transaction and subtract the burn amount
        // now that we have confirmation that the burn transaction is confirmed
        // on chain:
        let mut pending_tables_txn = self.pending_tables.begin().await?;
        pending_tables_txn
            .remove_pending_transaction(txn.get_signature())
            .await?;
        pending_tables_txn
            .subtract_burned_amount(payer, amount)
            .await?;
        pending_tables_txn.commit().await?;

        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(payer).unwrap();
        // Reduce the pending burn amount and the payer's balance by the amount
        // we've burned.
        payer_account.burned = payer_account.burned.saturating_sub(amount);
        payer_account.balance = payer_account.balance.saturating_sub(amount);

        metrics::counter!("burned", "payer" => payer.to_string()).increment(amount);

        Ok(())
    }
}
