use crate::{SolanaRpc, SolanaRpcError};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use task_manager::ManagedTask;

// Check balance every 12 hours
const DURATION: Duration = Duration::from_secs(43_200);

pub enum BalanceMonitor {
    Solana(String, Arc<SolanaRpc>, Pubkey),
    Noop,
}

impl BalanceMonitor {
    pub fn new(
        app_account: &str,
        solana: Option<Arc<SolanaRpc>>,
    ) -> Result<Self, Box<SolanaRpcError>> {
        match solana {
            None => Ok(BalanceMonitor::Noop),
            Some(rpc_client) => {
                let Ok(keypair) = Keypair::from_bytes(&rpc_client.keypair) else {
                    tracing::error!("sol monitor: keypair failed to deserialize");
                    return Err(Box::new(SolanaRpcError::InvalidKeypair))
                };
                let app_metric_name = format!("{app_account}-sol-balance");

                Ok(BalanceMonitor::Solana(
                    app_metric_name,
                    rpc_client,
                    keypair.pubkey(),
                ))
            }
        }
    }

    pub async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        match self {
            Self::Noop => Ok(()),
            Self::Solana(metric, solana, pubkey) => {
                run(metric, solana, pubkey, shutdown).await;
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

async fn run(
    metric_name: String,
    solana: Arc<SolanaRpc>,
    service_pubkey: Pubkey,
    shutdown: triggered::Listener,
) {
    tracing::info!("starting sol monitor");

    let mut trigger = tokio::time::interval(DURATION);

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => break,
            _ = trigger.tick() => {
                match solana.provider.get_balance(&service_pubkey).await {
                    Ok(balance) => metrics::gauge!(metric_name.clone(), balance as f64),
                    Err(err) => tracing::error!("sol monitor: failed to get account balance: {:?}", err.kind()),
                }
            }
        }
    }
    tracing::info!("stopping sol monitor")
}
