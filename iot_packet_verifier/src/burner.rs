use crate::{
    balances::{BalanceCache, BalanceStore},
    pending_burns::{Burn, PendingBurns},
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana::SolanaNetwork;
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

impl<P, S> Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn run(
        mut self,
        shutdown: triggered::Listener,
    ) -> Result<(), BurnError<P::Error, S::Error>> {
        tracing::info!("starting burner");
        let mut burn_timer = time::interval(self.burn_period);
        burn_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = burn_timer.tick() =>
                    match self.burn().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("error whilst handling denylist tick: {err:?}");
                    }
                }
            }
        }
        tracing::info!("stopping burner");
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

        self.solana
            .burn_data_credits(&payer, amount)
            .await
            .map_err(BurnError::SolanaError)?;

        // Now that we have successfully executed the burn and are no longer in
        // sync land, we can remove the amount burned:
        self.pending_burns
            .subtract_burned_amount(&payer, amount)
            .await
            .map_err(BurnError::SqlError)?;

        let mut balance_lock = self.balances.lock().await;
        let payer_account = balance_lock.get_mut(&payer).unwrap();
        payer_account.burned -= amount;
        // Reset the balance of the payer:
        payer_account.balance = self
            .solana
            .payer_balance(&payer)
            .await
            .map_err(BurnError::SolanaError)?;

        metrics::counter!("burned", amount, "payer" => payer.to_string());

        Ok(())
    }
}
