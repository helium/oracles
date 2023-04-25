use crate::{SolanaRpc, SolanaRpcError};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::{sync::Arc, time::Duration};

// Check balance every 12 hours
const DURATION: Duration = Duration::from_secs(43_200);

pub async fn start(
    app_account: &str,
    solana: Option<Arc<SolanaRpc>>,
    shutdown: triggered::Listener,
) -> Result<futures::future::BoxFuture<'static, Result<(), tokio::task::JoinError>>, SolanaRpcError>
{
    Ok(match solana {
        None => Box::pin(async move { Ok(()) }),
        Some(rpc_client) => {
            let Ok(keypair) = Keypair::from_bytes(&rpc_client.keypair) else {
                tracing::error!("sol monitor: keypair failed to deserialize");
                return Err(SolanaRpcError::InvalidKeypair)
            };
            let app_metric_name = format!("{app_account}-sol-balance");
            let handle = tokio::spawn(async move {
                run(app_metric_name, rpc_client, keypair.pubkey(), shutdown).await
            });
            Box::pin(async move { handle.await })
        }
    })
}

async fn run(
    metric_name: String,
    solana: Arc<SolanaRpc>,
    service_pubkey: Pubkey,
    shutdown: triggered::Listener,
) {
    let mut trigger = tokio::time::interval(DURATION);

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => {
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
