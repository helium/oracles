use crate::{
    pending_burns::{Burn, PendingBurns},
    solana::SolanaNetwork,
    verifier::Debiter,
};
use futures_util::StreamExt;
use helium_crypto::PublicKeyBinary;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

/// Caches balances fetched from the solana chain and debits made by the
/// packet verifier.
pub struct BalanceCache<S> {
    balances: BalanceStore,
    solana: S,
}

pub type BalanceStore = Arc<Mutex<HashMap<PublicKeyBinary, Balance>>>;

impl<S> BalanceCache<S>
where
    S: SolanaNetwork,
{
    /// Fetch all of the current balances that have been actively burned so that
    /// we have an accurate cache.
    pub async fn new<P>(pending_burns: &mut P, solana: S) -> anyhow::Result<Self>
    where
        P: PendingBurns,
    {
        let mut balances = HashMap::new();
        let mut burns = pending_burns.fetch_all().await;

        while let Some(Burn {
            payer,
            amount: burn_amount,
            ..
        }) = burns.next().await.transpose()?
        {
            // Look up the current balance of the payer
            let balance = solana.payer_balance(&payer).await?;
            balances.insert(
                payer,
                Balance {
                    burned: burn_amount as u64,
                    balance,
                },
            );
        }

        Ok(Self {
            balances: Arc::new(Mutex::new(balances)),
            solana,
        })
    }
}

impl<S> BalanceCache<S> {
    pub fn balances(&self) -> BalanceStore {
        self.balances.clone()
    }
}

#[async_trait::async_trait]
impl<S> Debiter for BalanceCache<S>
where
    S: SolanaNetwork,
{
    type Error = S::Error;

    /// Debits the balance from the cache, returning true if there was enough
    /// balance and false otherwise.
    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
    ) -> Result<bool, S::Error> {
        let mut balances = self.balances.lock().await;

        let mut balance = if !balances.contains_key(payer) {
            let new_balance = self.solana.payer_balance(payer).await?;
            balances.insert(payer.clone(), Balance::new(new_balance));
            balances.get_mut(payer).unwrap()
        } else {
            let mut balance = balances.get_mut(payer).unwrap();

            // If the balance is not sufficient, check to see if it has been increased
            if balance.balance < amount + balance.burned {
                balance.balance = self.solana.payer_balance(payer).await?;
            }

            balance
        };

        let sufficient = balance.balance >= amount + balance.burned;

        if sufficient {
            balance.burned += amount;
        }

        Ok(sufficient)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Balance {
    pub balance: u64,
    pub burned: u64,
}

impl Balance {
    pub fn new(balance: u64) -> Self {
        Self { balance, burned: 0 }
    }
}
