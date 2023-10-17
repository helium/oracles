use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helium_crypto::PublicKeyBinary;
use solana_sdk::signature::Signature;
use sqlx::{postgres::PgRow, FromRow, PgPool, Postgres, Row, Transaction};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// To avoid excessive burn transaction (which cost us money), we institute a minimum
/// amount of Data Credits accounted for before we burn from a payer:
const BURN_THRESHOLD: i64 = 10_000;

#[async_trait]
pub trait AddPendingBurn {
    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error>;
}

#[async_trait]
pub trait PendingTables {
    type Transaction<'a>: PendingTablesTransaction<'a> + Send + Sync
    where
        Self: 'a;

    async fn fetch_next_burn(&self) -> Result<Option<Burn>, sqlx::Error>;

    async fn fetch_all_pending_burns(&self) -> Result<Vec<Burn>, sqlx::Error>;

    async fn fetch_all_pending_txns(&self) -> Result<Vec<PendingTxn>, sqlx::Error>;

    async fn submit_txn(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error>;

    async fn begin<'a>(&'a self) -> Result<Self::Transaction<'a>, sqlx::Error>;
}

#[async_trait]
pub trait PendingTablesTransaction<'a> {
    async fn confirm_txn(&mut self, signature: &Signature) -> Result<(), sqlx::Error>;

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error>;

    async fn commit(self) -> Result<(), sqlx::Error>;
}

#[async_trait]
impl PendingTables for PgPool {
    type Transaction<'a> = Transaction<'a, Postgres>;

    async fn begin<'a>(&'a self) -> Result<Self::Transaction<'a>, sqlx::Error> {
        self.begin().await
    }

    async fn fetch_next_burn(&self) -> Result<Option<Burn>, sqlx::Error> {
        sqlx::query_as(
            "SELECT * FROM pending_burns WHERE amount >= $1 ORDER BY last_burn ASC LIMIT 1",
        )
        .bind(BURN_THRESHOLD)
        .fetch_optional(self)
        .await
    }

    async fn fetch_all_pending_burns(&self) -> Result<Vec<Burn>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM pending_burns")
            .fetch_all(self)
            .await
    }

    async fn fetch_all_pending_txns(&self) -> Result<Vec<PendingTxn>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM pending_txns")
            .fetch_all(self)
            .await
    }

    async fn submit_txn(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_txns (signature, payer, amount, time_of_submission)
            VALUES ($1 $2, $3, $4)
            "#,
        )
        .bind(&signature.to_string())
        .bind(payer)
        .bind(amount as i64)
        .bind(Utc::now())
        .execute(self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl<'a> AddPendingBurn for &'_ mut Transaction<'a, Postgres> {
    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_burns (payer, amount, last_burn)
            VALUES ($1, $2, $3)
            ON CONFLICT (payer) DO UPDATE SET
            amount = pending_burns.amount + $2
            "#,
        )
        .bind(payer)
        .bind(amount as i64)
        .bind(Utc::now())
        .execute(&mut **self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl<'a> PendingTablesTransaction<'a> for Transaction<'a, Postgres> {
    async fn confirm_txn(&mut self, signature: &Signature) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM pending_txns WHERE signature = $1")
            .bind(&signature.to_string())
            .execute(self)
            .await?;
        Ok(())
    }

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            UPDATE pending_burns SET
              amount = amount - $1,
              last_burn = $2
            WHERE
              payer = $3
            "#,
        )
        .bind(amount as i64)
        .bind(Utc::now())
        .bind(payer)
        .execute(&mut *self)
        .await?;
        Ok(())
    }

    async fn commit(self) -> Result<(), sqlx::Error> {
        self.commit().await
    }
}

#[derive(Debug)]
pub struct Burn {
    pub payer: PublicKeyBinary,
    pub amount: u64,
}

impl FromRow<'_, PgRow> for Burn {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            payer: row.try_get("payer")?,
            amount: row.try_get::<i64, _>("amount")? as u64,
        })
    }
}

pub struct PendingTxn {
    pub signature: Signature,
    pub payer: PublicKeyBinary,
    pub amount: u64,
    pub time_of_submission: DateTime<Utc>,
}

impl FromRow<'_, PgRow> for PendingTxn {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            payer: row.try_get("payer")?,
            amount: row.try_get::<i64, _>("amount")? as u64,
            time_of_submission: row.try_get("time_of_submission")?,
            signature: row
                .try_get::<String, _>("signature")?
                .parse()
                .map_err(|e| sqlx::Error::ColumnDecode {
                    index: "signature".to_string(),
                    source: Box::new(e),
                })?,
        })
    }
}

#[async_trait]
impl AddPendingBurn for Arc<Mutex<HashMap<PublicKeyBinary, u64>>> {
    async fn add_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error> {
	let mut map = self.lock().await;
        *map.entry(payer.clone()).or_default() += amount;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockPendingTxn {
    payer: PublicKeyBinary,
    amount: u64,
    time_of_submission: DateTime<Utc>,
}

#[derive(Default, Clone)]
pub struct MockPendingTables {
    pub pending_txns: Arc<Mutex<HashMap<Signature, MockPendingTxn>>>,
    pub pending_burns: Arc<Mutex<HashMap<PublicKeyBinary, u64>>>,
}

#[async_trait]
impl PendingTables for MockPendingTables {
    type Transaction<'a> = &'a MockPendingTables;

    async fn fetch_next_burn(&self) -> Result<Option<Burn>, sqlx::Error> {
        Ok(self
            .pending_burns
            .lock()
            .await
            .iter()
            .max_by_key(|(_, amount)| **amount)
            .map(|(payer, &amount)| Burn {
                payer: payer.clone(),
                amount,
            }))
    }

    async fn fetch_all_pending_burns(&self) -> Result<Vec<Burn>, sqlx::Error> {
        Ok(self
            .pending_burns
            .lock()
            .await
            .clone()
            .into_iter()
            .map(|(payer, amount)| Burn { payer, amount })
            .collect())
    }

    async fn fetch_all_pending_txns(&self) -> Result<Vec<PendingTxn>, sqlx::Error> {
        Ok(self
            .pending_txns
            .lock()
            .await
            .clone()
            .into_iter()
            .map(|(signature, mock)| PendingTxn {
                signature,
                payer: mock.payer,
                amount: mock.amount,
                time_of_submission: mock.time_of_submission,
            })
            .collect())
    }

    async fn submit_txn(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
        self.pending_txns.lock().await.insert(
            signature.clone(),
            MockPendingTxn {
                payer: payer.clone(),
                amount,
                time_of_submission: Utc::now(),
            },
        );
        Ok(())
    }

    async fn begin<'a>(&'a self) -> Result<Self::Transaction<'a>, sqlx::Error> {
        Ok(self)
    }
}

#[async_trait]
impl<'a> PendingTablesTransaction<'a> for &'a MockPendingTables {
    async fn confirm_txn(&mut self, signature: &Signature) -> Result<(), sqlx::Error> {
        self.pending_txns.lock().await.remove(signature);
        Ok(())
    }

    async fn subtract_burned_amount(
        &mut self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<(), sqlx::Error> {
        let mut map = self.pending_burns.lock().await;
        let balance = map.get_mut(payer).unwrap();
        *balance -= amount;
        Ok(())
    }

    async fn commit(self) -> Result<(), sqlx::Error> {
        Ok(())
    }
}
