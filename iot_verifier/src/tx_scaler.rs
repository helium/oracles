use crate::{
    hex_density::{compute_hex_density_map, GlobalHexMap, HexDensityMap, SharedHexDensityMap},
    Settings,
};
use chrono::{DateTime, Duration, Utc};
use futures::stream::StreamExt;
use helium_crypto::PublicKeyBinary;
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfo};
use sqlx::PgPool;
use tokio::time;

// The number in minutes within which the gateway has registered a beacon
// to the oracle for inclusion in transmit scaling density calculations
const HIP_17_INTERACTIVITY_LIMIT: i64 = 3600;

pub struct Server {
    follower: FollowerService,
    hex_density_map: SharedHexDensityMap,
    pool: PgPool,
    trigger_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum TxScalerError {
    #[error("error querying blockchain node")]
    NodeFollower(#[from] node_follower::Error),
    #[error("tx scaler db connect error")]
    DbConnect(#[from] db_store::Error),
    #[error("txn scaler error retrieving recent activity")]
    RecentActivity(#[from] sqlx::Error),
}

impl Server {
    pub async fn from_settings(settings: &Settings) -> Result<Self, TxScalerError> {
        let mut server = Self {
            follower: FollowerService::from_settings(&settings.follower),
            hex_density_map: SharedHexDensityMap::new(),
            pool: settings.database.connect(2).await?,
            trigger_interval: Duration::seconds(settings.transmit_scale_interval),
        };

        server.refresh_scaling_map().await?;

        Ok(server)
    }

    pub fn hex_density_map(&self) -> impl HexDensityMap {
        self.hex_density_map.clone()
    }

    pub async fn run(&mut self, shutdown: &triggered::Listener) -> Result<(), TxScalerError> {
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

    pub async fn refresh_scaling_map(&mut self) -> Result<(), TxScalerError> {
        let refresh_start = Utc::now();
        tracing::info!("generating hex scaling map : starting {refresh_start:?}");
        let mut global_map = GlobalHexMap::new();
        let active_gateways = self
            .gateways_recent_activity(refresh_start)
            .await
            .map_err(sqlx::Error::from)?;
        let mut gw_stream = self.follower.active_gateways().await?;
        while let Some(GatewayInfo {
            location, address, ..
        }) = gw_stream.next().await
        {
            if let Some(h3index) = location {
                if active_gateways.contains(&address.into()) {
                    global_map.increment_unclipped(h3index)
                }
            }
        }
        global_map.reduce_global();
        let new_map = compute_hex_density_map(&global_map);
        tracing::info!("scaling factor map entries: {}", new_map.len());
        self.hex_density_map.swap(new_map).await;
        tracing::info!("completed hex scaling map : completed {:?}", Utc::now());
        Ok(())
    }

    async fn gateways_recent_activity(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<PublicKeyBinary>, sqlx::Error> {
        let interactivity_deadline = now - Duration::minutes(HIP_17_INTERACTIVITY_LIMIT);
        sqlx::query_scalar::<_, PublicKeyBinary>(
            r#"
            select id from last_beacon where timestamp >= $1
            "#,
        )
        .bind(interactivity_deadline)
        .fetch_all(&self.pool)
        .await
    }
}
