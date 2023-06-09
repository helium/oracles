use crate::{SolanaRpc, SolanaRpcError};
use futures::{future::LocalBoxFuture, TryFutureExt};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};
use task_manager::ManagedTask;
use tokio_util::sync::CancellationToken;

// Check balance every 12 hours
const DURATION: Duration = Duration::from_secs(43_200);

pub enum BalanceMonitor {
    Solana(String, Arc<SolanaRpc>, Pubkey),
    Noop,
}

impl BalanceMonitor {
    pub fn new(app_account: &str, solana: Option<Arc<SolanaRpc>>) -> Result<Self, SolanaRpcError> {
        match solana {
            None => Ok(BalanceMonitor::Noop),
            Some(rpc_client) => {
                let Ok(keypair) = Keypair::from_bytes(&rpc_client.keypair) else {
                    tracing::error!("sol monitor: keypair failed to deserialize");
                    return Err(SolanaRpcError::InvalidKeypair)
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
}

impl ManagedTask for BalanceMonitor {
    fn start_task(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        match *self {
            Self::Noop => Box::pin(async move { Ok(()) }),
            Self::Solana(metric, solana, pubkey) => {
                let handle = tokio::spawn(run(metric, solana, pubkey, token));

                Box::pin(handle.map_err(anyhow::Error::from))
            }
        }
    }
}

async fn run(
    metric_name: String,
    solana: Arc<SolanaRpc>,
    service_pubkey: Pubkey,
    token: CancellationToken,
) {
    let mut trigger = tokio::time::interval(DURATION);

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("sol monitor: shutting down");
                break
            }
            _ = trigger.tick() => {
                match solana.provider.get_balance(&service_pubkey).await {
                    Ok(balance) => metrics::gauge!(metric_name.clone(), balance as f64),
                    Err(err) => tracing::error!("sol monitor: failed to get account balance: {:?}", err.kind()),
                }
            }
        }
    }
}
