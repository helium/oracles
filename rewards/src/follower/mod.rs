pub mod client;
pub mod meta;
pub use client::FollowerService;
pub use meta::Meta;

use crate::{
    env_var,
    gateway::Gateway,
    pending_txn::PendingTxn,
    ConsensusTxnTrigger, Error, PublicKey, Result
};
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    FollowerTxnStreamRespV1,
};
use sqlx::{Pool, Postgres};
use std::str;
use tokio::{sync::broadcast, time};
use tonic::Streaming;

/// First block that 5G hotspots were introduced (FreedomFi)
pub const DEFAULT_START_BLOCK: i64 = 995041;

pub const TXN_TYPES: &[&str] = &[
    "blockchain_txn_transfer_hotspot_v1",
    "blockchain_txn_transfer_hotspot_v2",
    "blockchain_txn_consensus_group_v1",
    "blockchain_txn_subnetwork_rewards_v1",
];

pub struct Follower {
    pool: Pool<Postgres>,
    service: FollowerService,
    start_block: i64,
    trigger: broadcast::Sender<ConsensusTxnTrigger>,
}

impl Follower {
    pub async fn new(
        pool: Pool<Postgres>,
        trigger: broadcast::Sender<ConsensusTxnTrigger>,
    ) -> Result<Self> {
        let start_block = env_var("FOLLOWER_START_BLOCK", DEFAULT_START_BLOCK)?;
        let service = FollowerService::from_env()?;
        Ok(Self {
            service,
            pool,
            start_block,
            trigger,
        })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting follower");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping follower");
                return Ok(());
            }
            let height = Meta::last_height(&self.pool, self.start_block).await? as u64;
            tracing::info!("connecting to txn stream at height {height}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = self.service.txn_stream(Some(height), &[], TXN_TYPES) => match stream_result {
                    Ok(txn_stream) => {
                        tracing::info!("connected to txn stream");
                        self.run_with_txn_stream(txn_stream, shutdown.clone()).await?
                    }
                    Err(err) => {
                        tracing::warn!("failed to connect to txn stream: {err}");
                        self.reconnect_wait(shutdown.clone()).await
                    }
                }
            }
        }
    }

    async fn reconnect_wait(&mut self, shutdown: triggered::Listener) {
        let timer = time::sleep(time::Duration::from_secs(5));
        tokio::select! {
            _ = timer => (),
            _ = shutdown => (),
        }
    }

    async fn run_with_txn_stream(
        &mut self,
        mut txn_stream: Streaming<FollowerTxnStreamRespV1>,
        shutdown: triggered::Listener,
    ) -> Result {
        loop {
            tokio::select! {
                msg = txn_stream.message() => match msg {
                    Ok(Some(txn)) => {
                        let height = txn.height as i64;
                        self.process_txn_entry(txn).await?;
                        Meta::update_last_height(&self.pool, height).await?;
                    }
                    Ok(None) => {
                        tracing::warn!("txn stream disconnected");
                        return Ok(());
                    }
                    Err(err) => {
                        tracing::warn!("txn stream error {err:?}");
                        return Ok(());
                    }
                },
                _ = shutdown.clone() => return Ok(())
            }
        }
    }

    async fn process_txn_entry(&mut self, entry: FollowerTxnStreamRespV1) -> Result {
        let txn = match entry.txn {
            Some(BlockchainTxn { txn: Some(ref txn) }) => txn,
            _ => {
                tracing::warn!("ignoring missing txn in stream");
                return Ok(());
            }
        };
        match txn {
            Txn::TransferHotspot(txn) => {
                self.process_transfer_gateway(txn.gateway.as_ref(), txn.buyer.as_ref())
                    .await
            }
            Txn::TransferHotspotV2(txn) => {
                self.process_transfer_gateway(txn.gateway.as_ref(), txn.new_owner.as_ref())
                    .await
            }
            Txn::SubnetworkRewards(txn) => self.process_subnet_rewards(&entry, txn).await,
            Txn::ConsensusGroup(_) => self.process_consensus_group(&entry).await,
            _ => Ok(()),
        }
    }

    async fn process_transfer_gateway(&mut self, gateway: &[u8], owner: &[u8]) -> Result {
        let gateway = PublicKey::try_from(gateway)?;
        let owner = PublicKey::try_from(owner)?;
        tracing::info!("processing transfer hotspot for {gateway} to {owner}");
        match Gateway::update_owner(&self.pool, &gateway, &owner).await {
            Ok(()) => Ok(()),
            Err(Error::NotFound(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn process_subnet_rewards(
        &mut self,
        envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnSubnetworkRewardsV1,
    ) -> Result {
        if txn.token_type() != BlockchainTokenTypeV1::Mobile {
            return Ok(());
        }

        let txn_hash = str::from_utf8(&envelope.txn_hash).unwrap();
        match PendingTxn::mark_txn_cleared(&self.pool, txn_hash).await {
            Ok(()) => Ok(()),
            // should we explicitly handle NotFound differently here?
            Err(err) => Err(err),
        }
    }

    async fn process_consensus_group(&mut self, envelope: &FollowerTxnStreamRespV1) -> Result {
        let ht = envelope.height;
        let ts = envelope.timestamp;
        tracing::info!(
            "processing consensus group at {height} with timestamp: {ts}",
            height = ht,
            ts = ts
        );
        match self.trigger.send(ConsensusTxnTrigger::new(ht, ts)) {
            Ok(_) => {
                // lookup non-cleared pending_txn
                // mark pending as failed if txn_mgr in bnode says its failed
                match PendingTxn::get_all_pending_txns(&self.pool).await {
                    Ok(Some(_pending_txns)) => {
                        // do stuff to lookup and update results
                        return Ok(())
                    }
                    Ok(None) => {
                        tracing::info!("no pending txns waiting")
                    }
                    Err(_) => {
                        tracing::error!("unable to retrieve outstanding pending txns")
                    }
                }

                Ok(())
            }
            Err(_) => {
                tracing::error!("failed to send reward trigger");
                Ok(())
            }
        }
    }
}
