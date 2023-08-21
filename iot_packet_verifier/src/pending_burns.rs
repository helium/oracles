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

    async fn fetch_incomplete_burns(&mut self) -> Result<Vec<BurnAttempt>, Self::Error>;

    async fn remove_incomplete_burns(&mut self) -> Result<(), Self::Error>;

    async fn begin_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
        latest_transaction_signature: &str,
    ) -> Result<(), Self::Error>;

    async fn complete_burn_attempt(
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

    async fn fetch_incomplete_burns(&mut self) -> Result<Vec<BurnAttempt>, Self::Error> {
        sqlx::query_as("SELECT * FROM attempted_burns")
            .fetch_all(&*self)
            .await
    }

    async fn remove_incomplete_burns(&mut self) -> Result<(), Self::Error> {
        sqlx::query("TRUNCATE TABLE attempted_burns")
            .execute(&*self)
            .await?;
        Ok(())
    }

    async fn begin_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
        latest_transaction_signature: &str,
    ) -> Result<(), Self::Error> {
        sqlx::query("INSERT INTO attempted_burns (payer, amount, latest_transaction_signature) VALUES ($1, $2, $3)")
            .bind(payer)
            .bind(amount as i64)
            .bind(latest_transaction_signature)
            .execute(&*self)
            .await?;
        Ok(())
    }

    async fn complete_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        let mut transaction = self.begin().await?;

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
        .execute(&mut transaction)
        .await?;

        sqlx::query("DELETE FROM attempted_burns WHERE payer = $1")
            .bind(payer)
            .execute(&mut transaction)
            .await?;

        transaction.commit().await?;

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

    async fn fetch_incomplete_burns(&mut self) -> Result<Vec<BurnAttempt>, Self::Error> {
        sqlx::query_as("SELECT * FROM attempted_burns")
            .fetch_all(&mut **self)
            .await
    }

    async fn remove_incomplete_burns(&mut self) -> Result<(), Self::Error> {
        sqlx::query("TRUNCATE TABLE attempted_burns")
            .execute(&mut **self)
            .await?;
        Ok(())
    }

    async fn begin_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
        latest_transaction_signature: &str,
    ) -> Result<(), Self::Error> {
        sqlx::query("INSERT INTO attempted_burns (payer, amount, latest_transaction_signature) VALUES ($1, $2, $3)")
            .bind(payer)
            .bind(amount as i64)
            .bind(latest_transaction_signature)
            .execute(&mut **self)
            .await?;
        Ok(())
    }

    async fn complete_burn_attempt(
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

        sqlx::query("DELETE FROM attempted_burns WHERE payer = $1")
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

#[derive(Clone)]
pub struct MockPendingBurns {
    pub pending_burns: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
    pub burn_attempts: Arc<Mutex<HashMap<PublicKeyBinary, BurnAttempt>>>,
}

#[async_trait]
impl PendingBurns for MockPendingBurns {
    type Error = Infallible;

    async fn fetch_all<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<Burn, Self::Error>> + Send + 'a>> {
        stream::iter(
            self.pending_burns
                .lock()
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
            .pending_burns
            .lock()
            .await
            .iter()
            .max_by_key(|(_, amount)| **amount)
            .map(|(payer, amount)| Burn {
                payer: payer.clone(),
                amount: *amount as i64,
            }))
    }

    async fn fetch_incomplete_burns(&mut self) -> Result<Vec<BurnAttempt>, Self::Error> {
        Ok(self.burn_attempts.lock().await.values().cloned().collect())
    }

    async fn remove_incomplete_burns(&mut self) -> Result<(), Self::Error> {
        self.burn_attempts.lock().await.clear();
        Ok(())
    }

    async fn begin_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
        latest_transaction_signature: &str,
    ) -> Result<(), Self::Error> {
        self.burn_attempts.lock().await.insert(
            payer.clone(),
            BurnAttempt {
                payer: payer.clone(),
                amount: amount as i64,
                latest_transaction_signature: latest_transaction_signature.to_string(),
            },
        );
        Ok(())
    }

    async fn complete_burn_attempt(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        self.burn_attempts.lock().await.remove(payer);
        let mut map = self.pending_burns.lock().await;
        let balance = map.get_mut(payer).unwrap();
        *balance -= amount;
        Ok(())
    }

    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), Self::Error> {
        let mut map = self.pending_burns.lock().await;
        *map.entry(payer.clone()).or_default() += amount;
        Ok(())
    }
}

#[derive(FromRow, Debug)]
pub struct Burn {
    pub payer: PublicKeyBinary,
    pub amount: i64,
}

#[derive(Clone, FromRow, Debug)]
pub struct BurnAttempt {
    pub payer: PublicKeyBinary,
    amount: i64,
    latest_transaction_signature: String,
}

impl BurnAttempt {
    pub fn amount(&self) -> u64 {
        self.amount as u64
    }

    pub fn latest_transaction_signature(
        &self,
    ) -> Result<solana_sdk::signature::Signature, solana_sdk::signature::ParseSignatureError> {
        self.latest_transaction_signature.parse()
    }
}
