use crate::pdas;
use anchor_lang::AccountDeserialize;
use data_credits::DelegatedDataCreditsV0;
use helium_crypto::PublicKey;
use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_sdk::program_pack::Pack;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Balances {
    pub provider: Arc<RpcClient>,
    pub balances: Arc<Mutex<HashMap<PublicKey, Balance>>>,
}

#[derive(thiserror::Error, Debug)]
pub enum DebitError {
    #[error("solana rpc error: {0}")]
    RpcClientError(#[from] ClientError),
    #[error("anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    #[error("solana program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
}

impl Balances {
    pub fn new(provider: Arc<RpcClient>) -> Self {
        Self {
            provider,
            balances: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub fn balances(&self) -> Arc<Mutex<HashMap<PublicKey, Balance>>> {
        self.balances.clone()
    }

    /// Debits the balance from the cache, returning true if there was enough
    /// balance and false otherwise.
    pub async fn debit_if_sufficient(
        &self,
        gateway: &PublicKey,
        amount: u64,
    ) -> Result<bool, DebitError> {
        let mut balances = self.balances.lock().await;

        let mut balance = if !balances.contains_key(gateway) {
            let new_balance = self.gateway_balance(gateway).await?;
            balances.insert(gateway.clone(), Balance::new(new_balance));
            balances.get_mut(&gateway).unwrap()
        } else {
            let mut balance = balances.get_mut(gateway).unwrap();

            // If the balance is not sufficient, check to see if it has been increased
            if balance.curr_balance < amount {
                let new_balance = self.gateway_balance(gateway).await?;
                if new_balance > balance.last_recorded_balance {
                    balance.curr_balance += new_balance - balance.last_recorded_balance;
                    balance.last_recorded_balance = new_balance;
                }
            }

            balance
        };

        let sufficient = if balance.curr_balance >= amount {
            balance.curr_balance -= amount;
            true
        } else {
            false
        };

        Ok(sufficient)
    }

    pub async fn gateway_balance(&self, gateway: &PublicKey) -> Result<u64, DebitError> {
        let ddc_key = pdas::delegated_data_credits(gateway);
        let account_data = self.provider.get_account_data(&ddc_key).await?;
        let mut account_data = account_data.as_ref();
        let ddc = DelegatedDataCreditsV0::try_deserialize(&mut account_data)?;
        let account_data = self.provider.get_account_data(&ddc.escrow_account).await?;
        let account_layout = spl_token::state::Account::unpack(account_data.as_slice())?;
        Ok(account_layout.amount)
    }
}

pub struct Balance {
    last_recorded_balance: u64,
    curr_balance: u64,
}

impl Balance {
    pub fn new(balance: u64) -> Self {
        Self {
            last_recorded_balance: balance,
            curr_balance: balance,
        }
    }
}
