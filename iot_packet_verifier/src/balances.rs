use helium_proto::services::follower;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::Streaming;

pub type Oui = u64;

pub struct Balance {
    prev_update_balance: u64,
    curr_balance: u64,
}

impl Balance {
    fn new(balance: u64) -> Self {
        Self {
            prev_update_balance: balance,
            curr_balance: balance,
        }
    }

    fn update_balance(&mut self, new_balance: u64) {
        if self.prev_update_balance < new_balance {
            self.curr_balance += new_balance - self.prev_update_balance;
        }
        self.prev_update_balance = new_balance;
    }
}

#[derive(Clone)]
pub struct Balances {
    balances: Arc<RwLock<HashMap<Oui, Mutex<Balance>>>>,
}

impl Balances {
    /// Debits the balance if it is sufficient, and returns false if the
    /// balance is insufficient.
    pub async fn debit_balance_if_sufficient(&self, oui: &Oui, value: u64) -> bool {
        if let Some(balance) = self.balances.read().await.get(oui) {
            let mut balance = balance.lock().await;
            if balance.curr_balance >= value {
                balance.curr_balance -= value;
                return true;
            }
        }
        false
    }
}

pub struct BalanceFollower {
    updates: Streaming<follower::FollowerOuiBalanceUpdateRespV1>,
    balances: Balances,
}

#[derive(thiserror::Error, Debug)]
pub enum FollowerError {
    #[error("Tonic error: {0}")]
    TonicError(#[from] tonic::Status),
    #[error("Stream closed unexpectedly")]
    StreamClosed,
}

impl BalanceFollower {
    pub async fn new(
        mut stream: Streaming<follower::FollowerOuiBalanceUpdateRespV1>,
    ) -> Result<Self, FollowerError> {
        // Get the first set of balances:
        let mut balances = HashMap::new();
        let first = stream.message().await?.ok_or(FollowerError::StreamClosed)?;
        for balance in first.updated_balances {
            balances.insert(balance.oui, Mutex::new(Balance::new(balance.balance)));
        }

        Ok(Self {
            updates: stream,
            balances: Balances {
                balances: Arc::new(RwLock::new(balances)),
            },
        })
    }

    pub fn balances(&self) -> Balances {
        self.balances.clone()
    }

    pub async fn run(mut self, shutdown: &triggered::Listener) -> Result<(), FollowerError> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                update_result = self.update() => update_result?,
            }
        }

        Ok(())
    }

    async fn update(&mut self) -> Result<(), FollowerError> {
        let next_update = self
            .updates
            .message()
            .await?
            .ok_or(FollowerError::StreamClosed)?;

        // Lock all of the balances until they have all been updated.
        let mut balances = self.balances.balances.write().await;
        for update in next_update.updated_balances {
            if balances.contains_key(&update.oui) {
                balances
                    .get(&update.oui)
                    .unwrap()
                    .lock()
                    .await
                    .update_balance(update.balance);
            } else {
                balances.insert(update.oui, Mutex::new(Balance::new(update.balance)));
            }
        }

        Ok(())
    }
}
