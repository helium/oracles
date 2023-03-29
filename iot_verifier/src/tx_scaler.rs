use crate::{
    hex_density::{compute_hex_density_map, GlobalHexMap, HexDensityMap, SharedHexDensityMap},
    last_beacon::LastBeacon,
    Settings,
};
use chrono::{DateTime, Duration, Utc};
use futures::stream::StreamExt;
use iot_config_client::iot_config_client::{IotConfigClient, IotConfigClientError};

use sqlx::PgPool;
use std::collections::HashMap;
use tokio::time;

// The number in minutes within which the gateway has registered a beacon
// to the oracle for inclusion in transmit scaling density calculations
const HIP_17_INTERACTIVITY_LIMIT: i64 = 3600;

pub struct Server {
    iot_config_client: IotConfigClient,
    hex_density_map: SharedHexDensityMap,
    pool: PgPool,
    trigger_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum TxScalerError {
    #[error("error querying iot config service")]
    IotConfigClient(#[from] IotConfigClientError),
    #[error("tx scaler db connect error")]
    DbConnect(#[from] db_store::Error),
    #[error("txn scaler error retrieving recent activity")]
    RecentActivity(#[from] sqlx::Error),
}

impl Server {
    pub async fn from_settings(
        settings: &Settings,
        pool: PgPool,
        iot_config_client: IotConfigClient,
    ) -> Result<Self, TxScalerError> {
        let mut server = Self {
            iot_config_client,
            hex_density_map: SharedHexDensityMap::new(),
            pool,
            trigger_interval: Duration::seconds(settings.transmit_scale_interval),
        };

        server.refresh_scaling_map().await?;

        Ok(server)
    }

    pub fn hex_density_map(&self) -> impl HexDensityMap {
        self.hex_density_map.clone()
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result<(), TxScalerError> {
        tracing::info!("density_scaler: starting transmit scaler process");

        let mut trigger_timer = time::interval(
            self.trigger_interval
                .to_std()
                .expect("valid interval in seconds"),
        );

        loop {
            if shutdown.is_triggered() {
                tracing::info!("density_scaler: stopping transmit scaler");
                return Ok(());
            }

            tokio::select! {
                _ = trigger_timer.tick() => self.refresh_scaling_map().await?,
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn refresh_scaling_map(&mut self) -> Result<(), TxScalerError> {
        let refresh_start = Utc::now();
        tracing::info!("density_scaler: generating hex scaling map, starting at {refresh_start:?}");
        let mut global_map = GlobalHexMap::new();
        let active_gateways = self
            .gateways_recent_activity(refresh_start)
            .await
            .map_err(sqlx::Error::from)?;
        let mut gw_stream = self.iot_config_client.gateway_stream().await?;
        while let Some(gateway_info) = gw_stream.next().await {
            if let Some(h3index) = gateway_info.location {
                if active_gateways.contains_key(&gateway_info.address.as_ref().to_vec()) {
                    global_map.increment_unclipped(h3index)
                }
            }
        }
        global_map.reduce_global();
        let new_map = compute_hex_density_map(&global_map);
        tracing::info!(
            "density_scaler: scaling factor map entries: {}",
            new_map.len()
        );
        self.hex_density_map.swap(new_map).await;
        tracing::info!(
            "density_scaler: generating hex scaling map, completed at {:?}",
            Utc::now()
        );
        Ok(())
    }

    async fn gateways_recent_activity(
        &self,
        now: DateTime<Utc>,
    ) -> Result<HashMap<Vec<u8>, DateTime<Utc>>, sqlx::Error> {
        let interactivity_deadline = now - Duration::minutes(HIP_17_INTERACTIVITY_LIMIT);
        Ok(
            LastBeacon::get_all_since(interactivity_deadline, &self.pool)
                .await?
                .into_iter()
                .map(|beacon| (beacon.id, beacon.timestamp))
                .collect::<HashMap<Vec<u8>, DateTime<Utc>>>(),
        )
    }
}
