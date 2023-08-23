use crate::{
    balances::{BalanceCache, BalanceStore},
    pending_burns::{Burn, PendingBurns},
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKeyBinary;
use solana::SolanaNetwork;
use solana_sdk::signature::ParseSignatureError;
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::time::{self, MissedTickBehavior};

pub struct Burner<P, S> {
    pending_burns: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

impl<P, S> ManagedTask for Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
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
pub enum BurnError<P, S> {
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Sql error: {0}")]
    SqlError(P),
    #[error("Solana error: {0}")]
    SolanaError(S),
    #[error("Parse signature error: {0}")]
    ParseSignatureError(#[from] ParseSignatureError),
}

impl<P, S> Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub fn new(pending_burns: P, balances: &BalanceCache<S>, burn_period: u64, solana: S) -> Self {
        Self {
            pending_burns,
            balances: balances.balances(),
            burn_period: Duration::from_secs(60 * burn_period),
            solana,
        }
    }

    pub async fn run(
        mut self,
        shutdown: triggered::Listener,
    ) -> Result<(), BurnError<P::Error, S::Error>> {
        tracing::info!("Starting burner");

        self.recover_attempted_burns().await?;

        let mut burn_timer = time::interval(self.burn_period);
        burn_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = burn_timer.tick() =>
                    if let Err(err) = self.burn().await {
                        tracing::error!("Failed to burn: {err:?}");
                    }
            }
        }

        tracing::info!("Stopping burner");
        Ok(())
    }

    pub async fn recover_attempted_burns(&mut self) -> Result<(), BurnError<P::Error, S::Error>> {
        tracing::info!("Attempting to recover attempted burns");

        for attempted_burn in self
            .pending_burns
            .fetch_incomplete_burns()
            .await
            .map_err(BurnError::SqlError)?
        {
            let amount = attempted_burn.amount();
            let since = attempted_burn.latest_transaction_signature()?;
            tracing::info!(%since, %amount, "Checking chain for burn transaction");
            if self
                .solana
                .has_burn_transaction(&attempted_burn.payer, amount, &since)
                .await
                .map_err(BurnError::SolanaError)?
            {
                tracing::info!("Found a matching transaction. Removing burn attempt");
                self.pending_burns
                    .complete_burn_attempt(&attempted_burn.payer, amount)
                    .await
                    .map_err(BurnError::SqlError)?;
                // Reset the payer's balance
                self.adjust_payer_balance(&attempted_burn.payer, amount)
                    .await
                    .map_err(BurnError::SolanaError)?;
            } else {
                tracing::info!("No matching transactions found");
            }
        }

        self.pending_burns
            .remove_incomplete_burns()
            .await
            .map_err(BurnError::SqlError)?;

        Ok(())
    }

    pub async fn adjust_payer_balance(
        &self,
        payer: &PublicKeyBinary,
        burned_amount: u64,
    ) -> Result<(), S::Error> {
        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(payer).unwrap();
        payer_account.burned -= burned_amount;
        // Reset the balance of the payer:
        payer_account.balance = self.solana.payer_balance(payer).await?;
        Ok(())
    }

    pub async fn burn(&mut self) -> Result<(), BurnError<P::Error, S::Error>> {
        // Create burn transaction and execute it:

        let Some(Burn { payer, amount }) = self.pending_burns.fetch_next().await
            .map_err(BurnError::SqlError)? else {
            return Ok(());
        };

        tracing::info!(%amount, %payer, "Burning DC");

        let amount = amount as u64;
        let latest_transaction = self
            .solana
            .latest_transaction()
            .await
            .map_err(BurnError::SolanaError)?;

        self.pending_burns
            .begin_burn_attempt(&payer, amount, &latest_transaction.to_string())
            .await
            .map_err(BurnError::SqlError)?;

        self.solana
            .burn_data_credits(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        self.pending_burns
            .complete_burn_attempt(&payer, amount)
            .await
            .map_err(BurnError::SqlError)?;

        self.adjust_payer_balance(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        metrics::counter!("burned", amount, "payer" => payer.to_string());

        Ok(())
    }
}
