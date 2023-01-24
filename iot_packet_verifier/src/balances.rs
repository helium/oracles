use crate::{burner::Burn, pdas};
use anchor_lang::AccountDeserialize;
use data_credits::DelegatedDataCreditsV0;
use futures_util::StreamExt;
use helium_crypto::PublicKey;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::program_pack::Pack;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Balances {
    pub provider: Arc<RpcClient>,
    pub balances: Arc<Mutex<HashMap<PublicKey, Balance>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum DebitError {
    #[error("Sql error: {0}")]
    SqlError(#[from] sqlx::Error),
    #[error("Solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("Solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
}

impl Balances {
    /// Fetch all of the current balances that have been actively burned so that
    /// we have an accurate cache.
    pub async fn new(pool: &Pool<Postgres>, provider: Arc<RpcClient>) -> Result<Self, DebitError> {
        let mut burns = sqlx::query_as("SELECT * FROM pending_burns").fetch(pool);

        let mut balances = HashMap::new();

        while let Some(Burn {
            payer,
            amount: burn_amount,
            ..
        }) = burns.next().await.transpose()?
        {
            // Look up the current balance of the payer
            let payer = PublicKey::try_from(payer).unwrap();
            let balance = payer_balance(provider.as_ref(), &payer).await?;
            balances.insert(
                payer,
                Balance {
                    burned: burn_amount as u64,
                    balance,
                },
            );
        }

        Ok(Self {
            provider,
            balances: Arc::new(Mutex::new(balances)),
        })
    }

    pub fn balances(&self) -> Arc<Mutex<HashMap<PublicKey, Balance>>> {
        self.balances.clone()
    }

    /// Debits the balance from the cache, returning true if there was enough
    /// balance and false otherwise.
    pub async fn debit_if_sufficient(
        &self,
        payer: &PublicKey,
        amount: u64,
    ) -> Result<bool, DebitError> {
        let mut balances = self.balances.lock().await;

        let mut balance = if !balances.contains_key(payer) {
            let new_balance = payer_balance(self.provider.as_ref(), payer).await?;
            balances.insert(payer.clone(), Balance::new(new_balance));
            balances.get_mut(&payer).unwrap()
        } else {
            let mut balance = balances.get_mut(payer).unwrap();

            // If the balance is not sufficient, check to see if it has been increased
            if balance.balance < amount + balance.burned {
                balance.balance = payer_balance(self.provider.as_ref(), payer).await?;
            }

            balance
        };

        let sufficient = if balance.balance >= amount + balance.burned {
            balance.burned += amount;
            true
        } else {
            false
        };

        Ok(sufficient)
    }
}

pub async fn payer_balance(provider: &RpcClient, payer: &PublicKey) -> Result<u64, DebitError> {
    let ddc_key = pdas::delegated_data_credits(payer);
    let account_data = provider.get_account_data(&ddc_key).await?;
    let mut account_data = account_data.as_ref();
    let ddc = DelegatedDataCreditsV0::try_deserialize(&mut account_data)?;
    let account_data = provider.get_account_data(&ddc.escrow_account).await?;
    let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;
    Ok(account_layout.amount)
}

pub struct Balance {
    pub balance: u64,
    pub burned: u64,
}

impl Balance {
    pub fn new(balance: u64) -> Self {
        Self { balance, burned: 0 }
    }
}
