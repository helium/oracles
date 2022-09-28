use crate::{
    error::DecodeError,
    hex::{compute_scaling_map, GlobalHexMap, ScalingMap},
    query::{QueryMsg, QueryReceiver},
    Result,
};
use chrono::Duration;
use helium_proto::services::{
    follower::{
        self, FollowerGatewayRespV1, FollowerGatewayStreamReqV1, FollowerGatewayStreamRespV1,
    },
    Channel, Endpoint,
};
use http::Uri;
use std::{env, time::Duration as StdDuration};
use tokio::time;
use tonic::Streaming;

const DEFAULT_TRIGGER_INTERVAL_SECS: i64 = 1800; // 30 min
const CONNECT_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const RPC_TIMEOUT: StdDuration = StdDuration::from_secs(5);
const DEFAULT_URI: &str = "http://127.0.0.1:8080";
const DEFAULT_STREAM_BATCH_SIZE: u32 = 1000;

pub struct Server {
    scaling_map: ScalingMap,
    follower_client: follower::Client<Channel>,
    trigger_interval: Duration,
}

impl Server {
    pub fn new() -> Result<Self> {
        let result = Self {
            scaling_map: ScalingMap::new(),
            follower_client: new_follower_from_env()?,
            trigger_interval: Duration::seconds(
                env::var("TRIGGER_INTERVAL_SECS")
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
        let mut gw_stream = active_gateways(&mut self.follower_client).await?;
        while let Some(FollowerGatewayStreamRespV1 { gateways }) = gw_stream.message().await? {
            for FollowerGatewayRespV1 { location, .. } in gateways {
                if let Ok(h3index) = location.parse::<u64>() {
                    global_map.increment_unclipped(h3index)
                }
            }
        }
        global_map.reduce_global();
        let mut scaling_map = ScalingMap::new();
        compute_scaling_map(&global_map, &mut scaling_map);
        self.scaling_map = scaling_map;
        Ok(())
    }
}

fn new_follower_from_env() -> Result<follower::Client<Channel>> {
    let uri: Uri = env::var("FOLLOWER_URI")
        .unwrap_or_else(|_| DEFAULT_URI.to_string())
        .parse()?;
    let channel = Endpoint::from(uri)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect_lazy();
    Ok(follower::Client::new(channel))
}

async fn active_gateways(
    client: &mut follower::Client<Channel>,
) -> Result<Streaming<FollowerGatewayStreamRespV1>> {
    let req = FollowerGatewayStreamReqV1 {
        batch_size: env::var("GW_STREAM_BATCH_SIZE")
            .unwrap_or_else(|_| DEFAULT_STREAM_BATCH_SIZE.to_string())
            .parse()
            .map_err(DecodeError::from)?,
    };
    let res = client.active_gateways(req).await?.into_inner();
    Ok(res)
}
