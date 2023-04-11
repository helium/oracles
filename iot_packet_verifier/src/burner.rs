use crate::{
    balances::{BalanceCache, BalanceStore},
    solana::SolanaNetwork,
};
use chrono::Utc;
use helium_crypto::PublicKeyBinary;
use sqlx::{FromRow, Pool, Postgres};
use std::time::Duration;
use tokio::task;

pub struct Burner<S> {
    pool: Pool<Postgres>,
    balances: BalanceStore,
    burn_period: Duration,
    solana: S,
}

#[derive(thiserror::Error, Debug)]
pub enum BurnError<E> {
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Solana error: {0}")]
    SolanaError(E),
}

const BURN_THRESHOLD: i64 = 10_000;

impl<S> Burner<S>
where
    S: SolanaNetwork,
{
    pub async fn new(
        pool: &Pool<Postgres>,
        balances: &BalanceCache<S>,
        burn_period: u64,
        solana: S,
    ) -> Result<Self, BurnError<S::Error>> {
        Ok(Self {
            pool: pool.clone(),
            balances: balances.balances(),
            burn_period: Duration::from_secs(60 * burn_period),
            solana,
        })
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> Result<(), BurnError<S::Error>> {
        let burn_service = task::spawn(async move {
            loop {
                self.burn().await?;
                tokio::time::sleep(self.burn_period).await;
            }
        });

        tokio::select! {
            _ = shutdown.clone() => Ok(()),
            service_result = burn_service => service_result?,
        }
    }

    pub async fn burn(&self) -> Result<(), BurnError<S::Error>> {
        // Create burn transaction and execute it:

        let Some(Burn { payer, amount, id }): Option<Burn> =
            sqlx::query_as("SELECT * FROM pending_burns WHERE amount >= $1 ORDER BY last_burn ASC")
                .bind(BURN_THRESHOLD)
                .fetch_optional(&self.pool)
            .await? else {
                return Ok(());
            };

        self.solana
            .burn_data_credits(&payer, amount as u64)
            .await
            .map_err(BurnError::SolanaError)?;

        // Now that we have successfully executed the burn and are no long in
        // sync land, we can remove the amount burned.
        sqlx::query(
            r#"
            UPDATE pending_burns SET
              amount = amount - $1,
              last_burn = $2
            WHERE id = $3
            "#,
        )
        .bind(amount)
        .bind(Utc::now().naive_utc())
        .bind(id)
        .execute(&self.pool)
        .await?;

        let mut balance_lock = self.balances.lock().await;
        let balances = balance_lock.get_mut(&payer).unwrap();
        balances.burned -= amount as u64;
        // Zero the balance in order to force a reset:
        balances.balance = 0;

        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub id: i32,
    pub payer: PublicKeyBinary,
    pub amount: i64,
}
