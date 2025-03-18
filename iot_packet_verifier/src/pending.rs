use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use solana::{burn::SolanaNetwork, SolanaRpcError};
use solana_sdk::signature::Signature;
use sqlx::{postgres::PgRow, FromRow, PgPool, Postgres, Row, Transaction};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use crate::balances::BalanceStore;

/// To avoid excessive burn transaction (which cost us money), we institute a minimum
/// amount of Data Credits accounted for before we burn from a payer:
pub const BURN_THRESHOLD: i64 = 10_000;

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

    async fn add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
        self.do_add_pending_transaction(payer, amount, signature, Utc::now())
            .await
    }

    async fn do_add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
        time_of_submission: DateTime<Utc>,
    ) -> Result<(), sqlx::Error>;

    async fn begin<'a>(&'a self) -> Result<Self::Transaction<'a>, sqlx::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum ConfirmPendingError {
    #[error("Sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("Chrono error: {0}")]
    ChronoError(#[from] chrono::OutOfRangeError),
    #[error("Solana error: {0}")]
    SolanaError(#[from] SolanaRpcError),
}

pub async fn confirm_pending_txns<S>(
    pending_tables: &impl PendingTables,
    solana: &S,
    balances: &BalanceStore,
) -> Result<(), ConfirmPendingError>
where
    S: SolanaNetwork,
{
    // Fetch all pending transactions and confirm them:
    let pending = pending_tables.fetch_all_pending_txns().await?;
    for pending in pending {
        // Sleep for at least a minute since the time of submission to
        // give the transaction plenty of time to be confirmed:
        let time_since_submission = Utc::now() - pending.time_of_submission;
        if Duration::minutes(1) > time_since_submission {
            tokio::time::sleep((Duration::minutes(1) - time_since_submission).to_std()?).await;
        }

        let mut txn = pending_tables.begin().await?;
        // Remove the pending transaction from the pending transaction table
        // regardless of whether or not it has been confirmed:
        txn.remove_pending_transaction(&pending.signature).await?;
        // Check if the transaction has been confirmed. If it has, remove the
        // amount from the pending burns table:
        if solana
            .confirm_transaction(&pending.signature)
            .await
            .map_err(ConfirmPendingError::SolanaError)?
        {
            txn.subtract_burned_amount(&pending.payer, pending.amount)
                .await?;
            let mut balance_lock = balances.lock().await;
            let payer_account = balance_lock.get_mut(&pending.payer).unwrap();
            payer_account.burned = payer_account.burned.saturating_sub(pending.amount);
            payer_account.balance = payer_account.balance.saturating_sub(pending.amount);
        }
        // Commit our work:
        txn.commit().await?;
    }

    Ok(())
}

#[async_trait]
pub trait PendingTablesTransaction<'a> {
    async fn remove_pending_transaction(
        &mut self,
        signature: &Signature,
    ) -> Result<(), sqlx::Error>;

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

    async fn do_add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
        time_of_submission: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO pending_txns (signature, payer, amount, time_of_submission)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(signature.to_string())
        .bind(payer)
        .bind(amount as i64)
        .bind(time_of_submission)
        .execute(self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl AddPendingBurn for Transaction<'_, Postgres> {
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
    async fn remove_pending_transaction(
        &mut self,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM pending_txns WHERE signature = $1")
            .bind(signature.to_string())
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
