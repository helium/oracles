use crate::{
    pending::{Burn, PendingTables},
    verifier::Debiter,
};
use solana::{burn::SolanaNetwork, SolanaRpcError};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

/// Caches balances fetched from the solana chain and debits made by the
/// packet verifier.
pub struct BalanceCache<S> {
    escrow_accounts: BalanceStore,
    solana: S,
}

pub type BalanceStore = Arc<Mutex<HashMap<String, EscrowAccount>>>;

impl<S> BalanceCache<S>
where
    S: SolanaNetwork,
{
    /// Fetch all of the current balances that have been actively burned so that
    /// we have an accurate cache.
    pub async fn new(pending_tables: &impl PendingTables, solana: S) -> anyhow::Result<Self> {
        let mut balances = HashMap::new();

        for Burn {
            escrow_key,
            amount: burn_amount,
        } in pending_tables.fetch_all_pending_burns().await?
        {
            // Look up the current balance of the escrow_account
            let balance = solana.escrow_account_balance(&escrow_key).await?;
            balances.insert(
                escrow_key,
                EscrowAccount {
                    burned: burn_amount,
                    balance,
                },
            );
        }

        Ok(Self {
            escrow_accounts: Arc::new(Mutex::new(balances)),
            solana,
        })
    }
}

impl<S> BalanceCache<S> {
    pub fn balances(&self) -> BalanceStore {
        self.escrow_accounts.clone()
    }
}

#[async_trait::async_trait]
impl<S> Debiter for BalanceCache<S>
where
    S: SolanaNetwork,
{
    /// Debits the balance from the cache, returning the remaining balance as an
    /// option if there was enough and none otherwise.
    async fn debit_if_sufficient(
        &self,
        escrow_key: &String,
        amount: u64,
        trigger_balance_check_threshold: u64,
    ) -> Result<Option<u64>, SolanaRpcError> {
        let mut escrow_accounts = self.escrow_accounts.lock().await;

        // Fetch the balance if we haven't seen the escrow_account before
        if let Entry::Vacant(escrow_account) = escrow_accounts.entry(escrow_key.clone()) {
            let escrow_account = escrow_account.insert(EscrowAccount::new(
                self.solana.escrow_account_balance(escrow_key).await?,
            ));
            return Ok((escrow_account.balance >= amount).then(|| {
                escrow_account.burned += amount;
                escrow_account.balance - amount
            }));
        }

        let escrow_account = escrow_accounts.get_mut(escrow_key).unwrap();
        match escrow_account
            .balance
            .checked_sub(amount + escrow_account.burned)
        {
            Some(remaining_balance) => {
                if remaining_balance < trigger_balance_check_threshold {
                    escrow_account.balance = self.solana.escrow_account_balance(escrow_key).await?;
                }
                escrow_account.burned += amount;
                Ok(Some(escrow_account.balance - escrow_account.burned))
            }
            None => Ok(None),
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct EscrowAccount {
    pub balance: u64,
    pub burned: u64,
}

impl EscrowAccount {
    pub fn new(balance: u64) -> Self {
        Self { balance, burned: 0 }
    }
}
