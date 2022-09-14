// TODO: Remove
#![allow(dead_code, unused)]

use crate::{follower::FollowerService, keypair::Keypair, Result};
// use helium_proto::{
//     blockchain_txn::Txn,
//     BlockchainTxn, Message, BlockchainTxnPOCReceiptsV2
// };
// use poc_metrics::record_duration;
// use poc_store::FileStore;
use tokio::time;

// 30 mins
pub const POC_IOT_TICK_TIME: time::Duration = time::Duration::from_secs(1800);

pub struct Server {
    keypair: Keypair,
    follower_service: FollowerService,
}

impl Server {
    pub async fn new(keypair: Keypair) -> Result<Self> {
        let result = Self {
            keypair,
            follower_service: FollowerService::from_env()?,
        };
        Ok(result)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting poc-iot-injector server");
        let mut poc_iot_timer = time::interval(POC_IOT_TICK_TIME);
        poc_iot_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = poc_iot_timer.tick() => match self.handle_poc_tick().await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal poc_iot_injector error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_poc_tick(&mut self) -> Result {
        tracing::debug!("handle_poc_tick");
        // TODO:
        // - Lookup verifier s3 files
        // - Construct a poc receipt v2 transaction
        // - Submit it to the follower
        Ok(())
    }
}
