use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use solana::burn::SolanaNetwork;
use solana_sdk::signature::Signature;
use sqlx::{postgres::PgRow, FromRow, PgPool, Postgres, Row, Transaction};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use crate::balances::BalanceStore;

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

    async fn add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error>;

    async fn begin<'a>(&'a self) -> Result<Self::Transaction<'a>, sqlx::Error>;
}

#[derive(thiserror::Error, Debug)]
pub enum ConfirmPendingError<S> {
    #[error("Sqlx error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("Chrono error: {0}")]
    ChronoError(#[from] chrono::OutOfRangeError),
    #[error("Solana error: {0}")]
    SolanaError(S),
}

pub async fn confirm_pending_txns<S>(
    pending_tables: &impl PendingTables,
    solana: &S,
    balances: &BalanceStore,
) -> Result<(), ConfirmPendingError<S::Error>>
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

    async fn add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
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
        .bind(Utc::now())
        .execute(self)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl AddPendingBurn for &'_ mut Transaction<'_, Postgres> {
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

    async fn add_pending_transaction(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
        self.pending_txns.lock().await.insert(
            *signature,
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
    async fn remove_pending_transaction(
        &mut self,
        signature: &Signature,
    ) -> Result<(), sqlx::Error> {
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

#[cfg(test)]
mod test {
    use crate::balances::PayerAccount;

    use super::*;
    use std::collections::HashSet;

    struct MockConfirmed(HashSet<Signature>);

    #[async_trait]
    impl SolanaNetwork for MockConfirmed {
        type Error = std::convert::Infallible;
        type Transaction = Signature;

        #[allow(clippy::diverging_sub_expression)]
        async fn payer_balance(&self, _payer: &PublicKeyBinary) -> Result<u64, Self::Error> {
            unreachable!()
        }

        #[allow(clippy::diverging_sub_expression)]
        async fn make_burn_transaction(
            &self,
            _payer: &PublicKeyBinary,
            _amount: u64,
        ) -> Result<Self::Transaction, Self::Error> {
            unreachable!()
        }

        #[allow(clippy::diverging_sub_expression)]
        async fn submit_transaction(
            &self,
            _transaction: &Self::Transaction,
        ) -> Result<(), Self::Error> {
            unreachable!()
        }

        async fn confirm_transaction(&self, txn: &Signature) -> Result<bool, Self::Error> {
            Ok(self.0.contains(txn))
        }
    }

    #[tokio::test]
    async fn test_confirm_pending_txns() {
        let confirmed = Signature::new_unique();
        let unconfirmed = Signature::new_unique();
        let payer: PublicKeyBinary = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6"
            .parse()
            .unwrap();
        let mut pending_txns = HashMap::new();
        const CONFIRMED_BURN_AMOUNT: u64 = 7;
        const UNCONFIRMED_BURN_AMOUNT: u64 = 11;
        pending_txns.insert(
            confirmed,
            MockPendingTxn {
                payer: payer.clone(),
                amount: CONFIRMED_BURN_AMOUNT,
                time_of_submission: Utc::now() - Duration::minutes(1),
            },
        );
        pending_txns.insert(
            unconfirmed,
            MockPendingTxn {
                payer: payer.clone(),
                amount: UNCONFIRMED_BURN_AMOUNT,
                time_of_submission: Utc::now() - Duration::minutes(1),
            },
        );
        let mut balances = HashMap::new();
        balances.insert(
            payer.clone(),
            PayerAccount {
                balance: CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
                burned: CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
            },
        );
        let mut pending_burns = HashMap::new();
        pending_burns.insert(
            payer.clone(),
            CONFIRMED_BURN_AMOUNT + UNCONFIRMED_BURN_AMOUNT,
        );
        let pending_txns = Arc::new(Mutex::new(pending_txns));
        let pending_burns = Arc::new(Mutex::new(pending_burns));
        let pending_tables = MockPendingTables {
            pending_txns,
            pending_burns,
        };
        let mut confirmed_txns = HashSet::new();
        confirmed_txns.insert(confirmed);
        let confirmed = MockConfirmed(confirmed_txns);
        // Confirm and resolve transactions:
        confirm_pending_txns(&pending_tables, &confirmed, &Arc::new(Mutex::new(balances)))
            .await
            .unwrap();
        // The amount left in the pending burns table should only be the unconfirmed
        // burn amount:
        assert_eq!(
            *pending_tables
                .pending_burns
                .lock()
                .await
                .get(&payer)
                .unwrap(),
            UNCONFIRMED_BURN_AMOUNT,
        );
    }
}
