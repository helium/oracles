use crate::{
    gateway_updater::MessageReceiver,
    hex_density::{compute_hex_density_map, GlobalHexMap, HexDensityMap},
    last_beacon::LastBeacon,
};
use chrono::{DateTime, Duration, Utc};
use futures::future::LocalBoxFuture;
use helium_crypto::PublicKeyBinary;
use sqlx::PgPool;
use std::collections::HashMap;
use task_manager::ManagedTask;

// The number in minutes within which the gateway has registered a beacon
// to the oracle for inclusion in transmit scaling density calculations
const HIP_17_INTERACTIVITY_LIMIT: i64 = 3600;

pub struct Server {
    pub hex_density_map: HexDensityMap,
    pool: PgPool,
    refresh_offset: Duration,
    gateway_cache_receiver: MessageReceiver,
}

#[derive(Debug, thiserror::Error)]
pub enum TxScalerError {
    #[error("tx scaler db connect error")]
    DbConnect(#[from] db_store::Error),
    #[error("txn scaler error retrieving recent activity")]
    RecentActivity(#[from] sqlx::Error),
}

impl ManagedTask for Server {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl Server {
    pub async fn new(
        refresh_offset: Duration,
        pool: PgPool,
        gateway_cache_receiver: MessageReceiver,
    ) -> anyhow::Result<Self> {
        let mut server = Self {
            hex_density_map: HexDensityMap::new(),
            pool,
            refresh_offset,
            gateway_cache_receiver,
        };

        server.refresh_scaling_map().await?;

        Ok(server)
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        tracing::info!("starting tx scaler process");

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = self.gateway_cache_receiver.changed() => self.refresh_scaling_map().await?,
            }
        }

        tracing::info!("stopping tx scaler process");
        Ok(())
    }

    pub async fn refresh_scaling_map(&mut self) -> anyhow::Result<()> {
        let refresh_start = Utc::now() - self.refresh_offset;
        tracing::info!("density_scaler: generating hex scaling map, starting at {refresh_start:?}");
        let mut global_map = GlobalHexMap::new();
        let active_gateways = self.gateways_recent_activity(refresh_start).await?;
        for pubkey in active_gateways.keys() {
            if let Some(gateway_info) = self.gateway_cache_receiver.borrow().get(pubkey) {
                if let Some(metadata) = &gateway_info.metadata {
                    global_map.increment_unclipped(metadata.location)
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
    ) -> anyhow::Result<HashMap<PublicKeyBinary, DateTime<Utc>>> {
        let interactivity_deadline = now - Duration::minutes(HIP_17_INTERACTIVITY_LIMIT);
        Ok(
            LastBeacon::get_all_since(&self.pool, interactivity_deadline)
                .await?
                .into_iter()
                .map(|beacon| (beacon.id, beacon.timestamp))
                .collect::<HashMap<PublicKeyBinary, DateTime<Utc>>>(),
        )
    }
}
