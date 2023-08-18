use async_trait::async_trait;
use chrono::Utc;
use futures::{stream, Stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use sqlx::{FromRow, Pool, Postgres, Transaction};
use std::{collections::HashMap, convert::Infallible, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

#[async_trait]
pub trait PendingBurns {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>>;

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error>;

    async fn fetch_saved_balances(&mut self) -> Result<Vec<SavedBalance>, Self::Error>;

    async fn save_balance(
        &mut self,
        payer: &PublicKeyBinary,
        balance: u64,
    ) -> Result<(), Self::Error>;

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error>;

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error>;
}

const BURN_THRESHOLD: i64 = 10_000;

#[async_trait]
impl PendingBurns for Pool<Postgres> {
    type Error = sqlx::Error;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        sqlx::query_as("SELECT * FROM pending_burns").fetch(&*self)
    }

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error> {
        sqlx::query_as("SELECT * FROM pending_burns WHERE amount >= $1 ORDER BY last_burn ASC")
            .bind(BURN_THRESHOLD)
            .fetch_optional(&*self)
            .await
    }

    async fn fetch_saved_balances(&mut self) -> Result<Vec<SavedBalance>, Self::Error> {
        sqlx::query_as("SELECT * FROM saved_balances")
            .fetch_all(&*self)
            .await
    }

    async fn save_balance(
        &mut self,
        payer: &PublicKeyBinary,
        balance: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query("INSERT INTO saved_balances (payer, balance) VALUES ($1, $2)")
            .bind(payer)
            .bind(balance as i64)
            .execute(&*self)
            .await?;
        Ok(())
    }

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            UPDATE pending_burns SET
              amount = amount - $1,
              last_burn = $2
            WHERE payer = $3;
            "#,
        )
        .bind(amount as i64)
        .bind(Utc::now().naive_utc())
        .bind(payer)
        .execute(&*self)
        .await?;

        sqlx::query("DELETE FROM saved_balances WHERE payer = $1")
            .bind(payer)
            .execute(&*self)
            .await?;

        Ok(())
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_burns (payer, amount, last_burn)
            VALUES ($1, $2, $3)
            ON CONFLICT (payer) DO UPDATE SET
            amount = pending_burns.amount + $2
            RETURNING *
            "#,
        )
        .bind(payer)
        .bind(amount as i64)
        .bind(Utc::now().naive_utc())
        .fetch_one(&*self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl PendingBurns for &'_ mut Transaction<'_, Postgres> {
    type Error = sqlx::Error;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        sqlx::query_as("SELECT * FROM pending_burns").fetch(&mut **self)
    }

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error> {
        sqlx::query_as("SELECT * FROM pending_burns WHERE amount >= $1 ORDER BY last_burn ASC")
            .bind(BURN_THRESHOLD)
            .fetch_optional(&mut **self)
            .await
    }

    async fn fetch_saved_balances(&mut self) -> Result<Vec<SavedBalance>, Self::Error> {
        sqlx::query_as("SELECT * FROM saved_balances")
            .fetch_all(&mut **self)
            .await
    }

    async fn save_balance(
        &mut self,
        payer: &PublicKeyBinary,
        balance: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query("INSERT INTO saved_balances (payer, balance) VALUES ($1, $2)")
            .bind(payer)
            .bind(balance as i64)
            .execute(&mut **self)
            .await?;
        Ok(())
    }

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            UPDATE pending_burns SET
              amount = amount - $1,
              last_burn = $2
            WHERE payer = $3
            "#,
        )
        .bind(amount as i64)
        .bind(Utc::now().naive_utc())
        .bind(payer)
        .execute(&mut **self)
        .await?;

        sqlx::query("DELETE FROM saved_balances WHERE payer = $1")
            .bind(payer)
            .execute(&mut **self)
            .await?;

        Ok(())
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_burns (payer, amount, last_burn)
            VALUES ($1, $2, $3)
            ON CONFLICT (payer) DO UPDATE SET
            amount = pending_burns.amount + $2
            RETURNING *
            "#,
        )
        .bind(payer)
        .bind(amount as i64)
        .bind(Utc::now().naive_utc())
        .fetch_one(&mut **self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl PendingBurns for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    type Error = Infallible;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        stream::iter(
            self.lock()
                .await
                .clone()
                .into_iter()
                .map(|(payer, amount)| {
                    Ok(Burn {
                        payer,
                        amount: amount as i64,
                    })
                }),
        )
        .boxed()
    }

    async fn fetch_next(&mut self) -> Result<Option<Burn>, Self::Error> {
        Ok(self
            .lock()
            .await
            .iter()
            .max_by_key(|(_, amount)| **amount)
            .map(|(payer, amount)| Burn {
                payer: payer.clone(),
                amount: *amount as i64,
            }))
    }

    async fn fetch_saved_balances(&mut self) -> Result<Vec<SavedBalance>, Self::Error> {
        Ok(Vec::new())
    }

    async fn save_balance(
        &mut self,
        _payer: &PublicKeyBinary,
        _balance: u64,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        let mut map = self.lock().await;
        let balance = map.get_mut(payer).unwrap();
        *balance -= amount;
        Ok(())
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        let mut map = self.lock().await;
        *map.entry(payer.clone()).or_default() += amount;
        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub payer: PublicKeyBinary,
    pub amount: i64,
}

#[derive(FromRow)]
pub struct SavedBalance {
    pub payer: PublicKeyBinary,
    amount: i64,
}

impl SavedBalance {
    pub fn amount(&self) -> u64 {
        self.amount as u64
    }
}
