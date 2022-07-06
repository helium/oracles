pub mod client;

use crate::{api::gateway::Gateway, Maker, PublicKey, Result};
use client::FollowerService;
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnAddGatewayV1,
    BlockchainTxnSubnetworkRewardsV1, FollowerTxnStreamRespV1,
};
use http::Uri;
use sqlx::{Pool, Postgres};
use tokio::time;
use tonic::Streaming;

pub const START_BLOCK: i64 = 995041;
pub const TXN_TYPES: &[&'static str] = &[
    "blockchain_txn_add_gateway_v1",
    "blockchain_txn_consensus_group_v1",
    "blockchain_txn_subnetwork_rewards_v1",
];

pub struct Follower {
    pool: Pool<Postgres>,
    service: FollowerService,
}

impl Follower {
    pub fn new(uri: Uri, pool: Pool<Postgres>) -> Result<Self> {
        let service = FollowerService::new(uri)?;
        Ok(Self { service, pool })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting follower");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping follower");
                return Ok(());
            }
            let height = self.get_gateway_height().await? as u64;
            tracing::info!("connecting to txn stream at height {height}");
            tokio::select! {
                _ = shutdown.clone() => (),
                stream_result = self.service.txn_stream(None, &[], TXN_TYPES) => match stream_result {
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

    async fn get_gateway_height(&mut self) -> Result<i64> {
        Gateway::max_height(&self.pool, START_BLOCK).await
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
                    Ok(Some(txn)) => self.process_txn_entry(txn).await?,
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
            Txn::AddGateway(txn) => self.process_add_gateway(&entry, txn).await,
            Txn::SubnetworkRewards(txn) => self.process_subnet_rewards(&entry, txn).await,
            Txn::ConsensusGroup(_) => self.process_consensus_group(&entry).await,
            _ => Ok(()),
        }
    }

    async fn process_add_gateway(
        &mut self,
        envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnAddGatewayV1,
    ) -> Result {
        tracing::info!(
            "processing {} add gw {} payer {}",
            envelope.height,
            PublicKey::try_from(txn.gateway.as_ref())?,
            PublicKey::try_from(txn.payer.as_ref())?
        );
        let gateway =
            Gateway::from_txn(envelope.height, envelope.timestamp, &envelope.txn_hash, txn)?;
        let makers = Maker::list(&self.pool).await?;
        if makers.iter().any(|m| m.pubkey == gateway.payer) {
            let inserted = gateway.insert_into(&self.pool).await?;
            tracing::info!(
                "inserted gateway: {inserted} maker: {maker}",
                maker = gateway.payer
            );
        }
        Ok(())
    }

    async fn process_subnet_rewards(
        &mut self,
        _envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnSubnetworkRewardsV1,
    ) -> Result {
        if txn.token_type() != BlockchainTokenTypeV1::Mobile {
            return Ok(());
        }
        Ok(())
    }

    async fn process_consensus_group(&mut self, _envelope: &FollowerTxnStreamRespV1) -> Result {
        Ok(())
    }
}
