use crate::{
    hex::{compute_hex_density_map, GlobalHexMap, HexDensityMap, SharedHexDensityMap},
    Result, Settings,
};
use chrono::{Duration, Utc};
use futures::stream::StreamExt;
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfo};
use rust_decimal::Decimal;
use tokio::time;

pub struct Server {
    hex_density_map: SharedHexDensityMap,
    follower: FollowerService,
    trigger_interval: Duration,
}

impl Server {
    pub async fn from_settings(settings: Settings) -> Result<Self> {
        let mut server = Self {
            hex_density_map: SharedHexDensityMap::new(),
            follower: FollowerService::from_settings(&settings.follower)?,
            trigger_interval: Duration::seconds(settings.trigger),
        };

        server.refresh_scaling_map().await?;

        Ok(server)
    }

    pub fn hex_density_map(&self) -> impl HexDensityMap {
        self.hex_density_map.clone()
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting density scaler process");

        let mut trigger_timer = time::interval(
            self.trigger_interval
                .to_std()
                .expect("valid interval in seconds"),
        );

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping density scaler");
                return Ok(());
            }

            tokio::select! {
                _ = trigger_timer.tick() => self.refresh_scaling_map().await?,
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn refresh_scaling_map(&mut self) -> Result {
        tracing::info!("generating hex scaling map : starting {:?}", Utc::now());
        let mut global_map = GlobalHexMap::new();
        let mut gw_stream = self.follower.active_gateways().await?;
        while let Some(GatewayInfo { location, .. }) = gw_stream.next().await {
            if let Some(h3index) = location {
                global_map.increment_unclipped(h3index)
            }
        }
        global_map.reduce_global();
        let new_map = compute_hex_density_map(&global_map);

        tracing::info!("populated hex density map with {} keys", new_map.len());
        let sample: Vec<(String, Decimal)> = new_map.clone().into_iter().take(3).collect::<Vec<(String, Decimal)>>();
        for (location, scale) in sample {
            tracing::info!("Scaling factor for sample location:  {:?}, {:?}", location, scale);
        };
        self.hex_density_map.swap(new_map).await;
        tracing::info!("completed hex scaling map : completed {:?}", Utc::now());
        Ok(())
    }
}
