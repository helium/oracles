use crate::{
    balances::{BalanceCache, BalanceStore},
    pending_burns::{Burn, PendingBurns},
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana::SolanaNetwork;
use std::time::Duration;
use task_manager::ManagedTask;
use tokio::task;
use tokio_util::sync::CancellationToken;

pub struct Burner<P, S> {
    pending_burns: P,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError<P, S> {
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Sql error: {0}")]
    SqlError(P),
    #[error("Solana error: {0}")]
    SolanaError(S),
}

impl<P, S> Burner<P, S> {
    pub fn new(pending_burns: P, balances: &BalanceCache<S>, burn_period: u64, solana: S) -> Self {
        Self {
            pending_burns,
            balances: balances.balances(),
            burn_period: Duration::from_secs(60 * burn_period),
            solana,
        }
    }
}

impl<P, S> ManagedTask for Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
    S: SolanaNetwork,
{
    fn start_task(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(token).map_err(anyhow::Error::from))
    }
}

impl<P, S> Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn run(
        mut self,
        token: CancellationToken,
    ) -> Result<(), BurnError<P::Error, S::Error>> {
        let burn_service = task::spawn(async move {
            loop {
                self.burn().await?;
                tokio::time::sleep(self.burn_period).await;
            }
        });

        tokio::select! {
            _ = token.cancelled() => Ok(()),
            service_result = burn_service => service_result?,
        }
    }

    pub async fn burn(&mut self) -> Result<(), BurnError<P::Error, S::Error>> {
        // Create burn transaction and execute it:

        let Some(Burn { payer, amount }) = self.pending_burns.fetch_next().await
            .map_err(BurnError::SqlError)? else {
            return Ok(());
        };

        tracing::info!(%amount, %payer, "Burning DC");

        let amount = amount as u64;

        self.solana
            .burn_data_credits(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        // Now that we have successfully executed the burn and are no long in
        // sync land, we can remove the amount burned.
        self.pending_burns
            .subtract_burned_amount(&payer, amount)
            .await
            .map_err(BurnError::SqlError)?;

        let mut balance_lock = self.balances.lock().await;
        let balances = balance_lock.get_mut(&payer).unwrap();
        balances.burned -= amount;
        // Zero the balance in order to force a reset:
        balances.balance = 0;

        metrics::counter!("burned", amount, "payer" => payer.to_string());

        Ok(())
    }
}
