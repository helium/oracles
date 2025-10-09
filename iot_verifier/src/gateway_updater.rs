//
// responsible for updating the gateway info cache at a set interval
//

use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use iot_config::client::ClientError;
use iot_config::{client::Gateways, gateway_info::GatewayInfo};
use std::{collections::HashMap, time::Duration};
use task_manager::ManagedTask;
use tokio::sync::watch;
use tokio::time;

pub type GatewayMap = HashMap<PublicKeyBinary, GatewayInfo>;
pub type MessageSender = watch::Sender<GatewayMap>;
pub type MessageReceiver = watch::Receiver<GatewayMap>;

pub struct GatewayUpdater<G> {
    gateways: G,
    refresh_interval: Duration,
    sender: MessageSender,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayUpdaterError {
    #[error("error querying gateway api")]
    GatewayApiError(ClientError),
    #[error("error sending on channel")]
    SendError(#[from] watch::error::SendError<GatewayMap>),
}

impl<G> ManagedTask for GatewayUpdater<G>
where
    G: Gateways,
{
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl<G> GatewayUpdater<G>
where
    G: Gateways,
{
    pub async fn new(
        refresh_interval: Duration,
        mut gateways: G,
    ) -> Result<(MessageReceiver, Self), GatewayUpdaterError> {
        let gateway_map = refresh_gateways(&mut gateways).await?;
        let (sender, receiver) = watch::channel(gateway_map);
        Ok((
            receiver,
            Self {
                gateways,
                refresh_interval,
                sender,
            },
        ))
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting gateway_updater");
        let mut trigger_timer = time::interval(self.refresh_interval);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = trigger_timer.tick() => self.handle_refresh_tick().await?,
            }
        }
        tracing::info!("stopping gateway_updater");
        Ok(())
    }

    async fn handle_refresh_tick(&mut self) -> Result<(), GatewayUpdaterError> {
        tracing::info!("handling refresh tick");
        let updated_gateway_map = refresh_gateways(&mut self.gateways).await?;
        let gateway_count = updated_gateway_map.len();
        if gateway_count > 0 {
            tracing::info!("completed refreshing gateways, total gateways: {gateway_count}");
            self.sender.send(updated_gateway_map)?;
        } else {
            tracing::warn!("failed to refresh gateways, empty map...");
        }
        Ok(())
    }
}

pub async fn refresh_gateways<G>(gateways: &mut G) -> Result<GatewayMap, GatewayUpdaterError>
where
    G: Gateways,
{
    tracing::info!("refreshing gateways");
    let mut gateway_map = GatewayMap::new();
    let mut gw_stream = gateways
        .stream_gateways_info()
        .await
        .map_err(GatewayUpdaterError::GatewayApiError)?;
    while let Some(gateway_info) = gw_stream.next().await {
        gateway_map.insert(gateway_info.address.clone(), gateway_info);
    }
    Ok(gateway_map)
}
