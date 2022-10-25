use crate::{
    error::DecodeError,
    hex::{compute_scaling_map, GlobalHexMap, ScalingMap},
    query::{QueryMsg, QueryReceiver},
    Result,
};
use chrono::Duration;
use futures::stream::StreamExt;
use node_follower::{follower_service::FollowerService, gateway_resp::GatewayInfo};
use std::env;
use tokio::time;

const DEFAULT_TRIGGER_INTERVAL_SECS: i64 = 1800; // 30 min

pub struct Server {
    scaling_map: ScalingMap,
    follower: FollowerService,
    trigger_interval: Duration,
}

impl Server {
    pub fn new(follower: FollowerService) -> Result<Self> {
        let result = Self {
            scaling_map: ScalingMap::new(),
            follower,
            trigger_interval: Duration::seconds(
                env::var("DENSITY_TRIGGER_INTERVAL_SECS")
                    .unwrap_or_else(|_| DEFAULT_TRIGGER_INTERVAL_SECS.to_string())
                    .parse()
                    .map_err(DecodeError::from)?,
            ),
        };
        Ok(result)
    }

    pub async fn run(
        &mut self,
        mut queries: QueryReceiver,
        shutdown: triggered::Listener,
    ) -> Result {
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
                query = queries.recv() => match query {
                    Some(QueryMsg{hex, response: tx}) => {
                        let resp = self
                                   .scaling_map
                                   .get(&hex)
                                   .map(|scale| scale.to_owned());
                        tx.send(resp)
                    },
                    None => {
                        tracing::warn!("query channel closed");
                        return Ok(())
                    }
                },
                _ = trigger_timer.tick() => self.refresh_scaling_map().await?,
                _ = shutdown.clone() => return Ok(()),
            }
        }
    }

    pub async fn refresh_scaling_map(&mut self) -> Result {
        let mut global_map = GlobalHexMap::new();
        let mut gw_stream = self.follower.active_gateways().await?;
        while let Some(GatewayInfo { location, .. }) = gw_stream.next().await {
            if let Some(h3index) = location {
                global_map.increment_unclipped(h3index)
            }
        }
        global_map.reduce_global();
        let mut scaling_map = ScalingMap::new();
        compute_scaling_map(&global_map, &mut scaling_map);
        self.scaling_map = scaling_map;
        Ok(())
    }
}
