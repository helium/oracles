use crate::{
    pending_burns::{Burn, PendingBurns},
    verifier::Debiter,
};
use futures_util::StreamExt;
use helium_crypto::PublicKeyBinary;
use solana::SolanaNetwork;
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

/// Caches balances fetched from the solana chain and debits made by the
/// packet verifier.
pub struct BalanceCache<S> {
    payer_accounts: BalanceStore,
    solana: S,
}

pub type BalanceStore = Arc<Mutex<HashMap<PublicKeyBinary, PayerAccount>>>;

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
                PayerAccount {
                    burned: burn_amount as u64,
                    balance,
                },
            );
        }

        Ok(Self {
            payer_accounts: Arc::new(Mutex::new(balances)),
            solana,
        })
    }
}

impl<S> BalanceCache<S> {
    pub fn balances(&self) -> BalanceStore {
        self.payer_accounts.clone()
    }
}

#[async_trait::async_trait]
impl<S> Debiter for BalanceCache<S>
where
    S: SolanaNetwork,
{
    type Error = S::Error;

    /// Debits the balance from the cache, returning the remaining balance as an
    /// option if there was enough and none otherwise.
    async fn debit_if_sufficient(
        &self,
        payer: &PublicKeyBinary,
        amount: u64,
        trigger_balance_check_threshold: u64,
    ) -> Result<Option<u64>, S::Error> {
        let mut payer_accounts = self.payer_accounts.lock().await;

        // Fetch the balance if we haven't seen the payer before
        if let Entry::Vacant(payer_account) = payer_accounts.entry(payer.clone()) {
            let payer_account =
                payer_account.insert(PayerAccount::new(self.solana.payer_balance(payer).await?));
            return Ok((payer_account.balance >= amount).then(|| {
                payer_account.burned += amount;
                payer_account.balance - amount
            }));
        }

        let payer_account = payer_accounts.get_mut(payer).unwrap();
        match payer_account
            .balance
            .checked_sub(amount + payer_account.burned)
        {
            Some(remaining_balance) => {
                if remaining_balance < trigger_balance_check_threshold {
                    payer_account.balance = self.solana.payer_balance(payer).await?;
                }
                payer_account.burned += amount;
                Ok(Some(payer_account.balance - payer_account.burned))
            }
            None => Ok(None),
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct PayerAccount {
    pub balance: u64,
    pub burned: u64,
}

impl PayerAccount {
    pub fn new(balance: u64) -> Self {
        Self { balance, burned: 0 }
    }
}
