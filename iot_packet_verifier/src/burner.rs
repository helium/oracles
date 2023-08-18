use crate::{
    balances::{BalanceCache, BalanceStore},
    pending_burns::{Burn, PendingBurns},
};
use solana::SolanaNetwork;
use std::time::Duration;
use tokio::task;

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

impl<P, S> Burner<P, S>
where
    P: PendingBurns + Send + Sync + 'static,
    S: SolanaNetwork,
{
    pub async fn new(
        mut pending_burns: P,
        balances: &BalanceCache<S>,
        burn_period: u64,
        solana: S,
    ) -> Result<Self, BurnError<P::Error, S::Error>> {
        // Recover from failed burn storage
        for saved_balance in pending_burns
            .fetch_saved_balances()
            .await
            .map_err(BurnError::SqlError)?
        {
            let curr_balance = solana
                .payer_balance(&saved_balance.payer)
                .await
                .map_err(BurnError::SolanaError)?;
            // If the balance we have saved is greater than the current balance,
            // we must have burned and failed to reduce the pending burns table.
            let burned = saved_balance.amount() - curr_balance;
            pending_burns
                .subtract_burned_amount(&saved_balance.payer, burned)
                .await
                .map_err(BurnError::SqlError)?
        }

        Ok(Self {
            pending_burns,
            balances: balances.balances(),
            burn_period: Duration::from_secs(60 * burn_period),
            solana,
        })
    }

    pub async fn run(
        mut self,
        shutdown: &triggered::Listener,
    ) -> Result<(), BurnError<P::Error, S::Error>> {
        let burn_service = task::spawn(async move {
            loop {
                if let Err(e) = self.burn().await {
                    tracing::error!("Failed to burn: {e:?}");
                }
                tokio::time::sleep(self.burn_period).await;
            }
        });

        tokio::select! {
            _ = shutdown.clone() => Ok(()),
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

        self.pending_burns
            .save_balance(
                &payer,
                self.solana
                    .payer_balance(&payer)
                    .await
                    .map_err(BurnError::SolanaError)?,
            )
            .await
            .map_err(BurnError::SqlError)?;

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
