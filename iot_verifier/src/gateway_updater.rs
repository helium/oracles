use crate::Settings;
use chrono::Duration;
use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use iot_config::{
    client::{Client as IotConfigClient, ClientError as IotConfigClientError},
    gateway_info::{GatewayInfo, GatewayInfoResolver},
};
use std::collections::HashMap;
use tokio::sync::watch;
use tokio::time;

pub type GatewayMap = HashMap<PublicKeyBinary, GatewayInfo>;
pub type MessageSender = watch::Sender<GatewayMap>;
pub type MessageReceiver = watch::Receiver<GatewayMap>;

pub struct GatewayUpdater {
    iot_config_client: IotConfigClient,
    refresh_interval: Duration,
    pub receiver: MessageReceiver,
    sender: MessageSender,
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayUpdaterError {
    #[error("gateway not found: {0}")]
    GatewayNotFound(PublicKeyBinary),
    #[error("error querying iot config service")]
    IotConfigClient(#[from] IotConfigClientError),
}

impl GatewayUpdater {
    pub async fn from_settings(
        settings: &Settings,
        iot_config_client: IotConfigClient,
    ) -> Result<Self, GatewayUpdaterError> {
        let gateway_map = refresh_gateways(iot_config_client.clone()).await?;
        let (sender, receiver) = watch::channel(gateway_map);
        Ok(Self {
            iot_config_client,
            refresh_interval: settings.gateway_refresh_interval(),
            receiver,
            sender,
        })
    }

    pub async fn run(&self, shutdown: &triggered::Listener) -> Result<(), GatewayUpdaterError> {
        tracing::info!("starting gateway_updater");

        let mut trigger_timer = time::interval(
            self.refresh_interval
                .to_std()
                .expect("valid interval in seconds"),
        );

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping gateway_updater");
                return Ok(());
            }

            tokio::select! {
                _ = trigger_timer.tick() => self.handle_refresh_tick().await?,
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }

    async fn handle_refresh_tick(&self) -> Result<(), GatewayUpdaterError> {
        tracing::info!("handling refresh tick");
        let updated_gateway_map = refresh_gateways(self.iot_config_client.clone()).await?;
        _ = self.sender.send(updated_gateway_map);
        Ok(())
    }
}

pub async fn refresh_gateways(
    iot_config_client: IotConfigClient,
) -> Result<GatewayMap, GatewayUpdaterError> {
    tracing::info!("refreshing gateways");
    let mut gateways = GatewayMap::new();
    let mut gw_stream = iot_config_client.clone().stream_gateways_info().await?;
    while let Some(gateway_info) = gw_stream.next().await {
        gateways.insert(gateway_info.address.clone(), gateway_info);
    }
    let gateway_count = gateways.len();
    tracing::info!("completed refreshing gateways, total gateways: {gateway_count}");
    Ok(gateways)
}
