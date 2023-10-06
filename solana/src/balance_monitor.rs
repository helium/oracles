use crate::{SolanaRpc, SolanaRpcError};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use task_manager::ManagedTask;

// Check balance every 12 hours
const DURATION: Duration = Duration::from_secs(43_200);

pub enum BalanceMonitor {
    Solana(Arc<SolanaRpc>, Vec<Pubkey>),
    Noop,
}

impl BalanceMonitor {
    pub fn new(
        solana: Option<Arc<SolanaRpc>>,
        mut additional_pubkeys: Vec<Pubkey>,
    ) -> Result<Self, Box<SolanaRpcError>> {
        match solana {
            None => Ok(BalanceMonitor::Noop),
            Some(rpc_client) => {
                let Ok(keypair) = Keypair::from_bytes(&rpc_client.keypair) else {
                    tracing::error!("sol monitor: keypair failed to deserialize");
                    return Err(Box::new(SolanaRpcError::InvalidKeypair));
                };

                additional_pubkeys.push(keypair.pubkey());
                Ok(BalanceMonitor::Solana(rpc_client, additional_pubkeys))
            }
        }
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        match self {
            Self::Noop => Ok(()),
            Self::Solana(solana, pubkeys) => {
                run(solana, pubkeys, shutdown).await;
                Ok(())
            }
        }
    }
}

impl ManagedTask for BalanceMonitor {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

async fn run(solana: Arc<SolanaRpc>, service_pubkeys: Vec<Pubkey>, shutdown: triggered::Listener) {
    tracing::info!("starting sol monitor");

    let mut trigger = tokio::time::interval(DURATION);

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => break,
            _ = trigger.tick() => {
                for service_pubkey in service_pubkeys.iter() {
                    match solana.provider.get_balance(service_pubkey).await {
                        Ok(balance) => metrics::gauge!("sol-balance", balance as f64, "pubkey" => service_pubkey.to_string()),
                        Err(err) => tracing::error!("sol monitor: failed to get account balance: {:?}", err.kind()),
                    }
                }
            }
        }
    }
    tracing::info!("stopping sol monitor")
}
