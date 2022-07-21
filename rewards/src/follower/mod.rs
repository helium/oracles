pub mod client;
pub use client::FollowerService;

use crate::{env_var, gateway::Gateway, Error, PublicKey, Result, Trigger};
use helium_proto::{
    blockchain_txn::Txn, BlockchainTokenTypeV1, BlockchainTxn, BlockchainTxnSubnetworkRewardsV1,
    FollowerTxnStreamRespV1,
};
use sqlx::{Pool, Postgres};
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
    trigger: broadcast::Sender<Trigger>,
}

impl Follower {
    pub async fn new(pool: Pool<Postgres>, trigger: broadcast::Sender<Trigger>) -> Result<Self> {
        let start_block = env_var("FOLLOWER_START_BLOCK", DEFAULT_START_BLOCK)?;
        let service = FollowerService::from_env()?;
        Ok(Self {
            service,
            pool,
            start_block,
            trigger,
        })
    }

    async fn last_height<'c, E>(executor: E, start_block: i64) -> Result<i64>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let height = sqlx::query_scalar::<_, String>(
            r#"
            select value from follower_meta
            where key = 'last_height'
            "#,
        )
        .fetch_optional(executor)
        .await?
        .and_then(|v| v.parse::<i64>().map_or_else(|_| None, Some))
        .unwrap_or(start_block);
        Ok(height)
    }

    async fn update_last_height<'c, E>(executor: E, height: i64) -> Result
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let _ = sqlx::query(
            r#"
            insert into follower_meta (key, value)
            values ('last_height', $1)
            on conflict (key) do update set
                value = EXCLUDED.value
            "#,
        )
        .bind(height.to_string())
        .execute(executor)
        .await?;
        Ok(())
    }

    pub async fn run(&mut self, shutdown: triggered::Listener) -> Result {
        tracing::info!("starting follower");

        loop {
            if shutdown.is_triggered() {
                tracing::info!("stopping follower");
                return Ok(());
            }
            let height = Self::last_height(&self.pool, self.start_block).await? as u64;
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
                        Self::update_last_height(&self.pool, height).await?;
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
        _envelope: &FollowerTxnStreamRespV1,
        txn: &BlockchainTxnSubnetworkRewardsV1,
    ) -> Result {
        if txn.token_type() != BlockchainTokenTypeV1::Mobile {
            return Ok(());
        }
        Ok(())
    }

    async fn process_consensus_group(&mut self, envelope: &FollowerTxnStreamRespV1) -> Result {
        tracing::info!(
            "processing consensus group at {height}",
            height = envelope.height
        );
        match self.trigger.send(Trigger::new(envelope.height)) {
            Ok(_) => Ok(()),
            Err(_) => {
                tracing::error!("failed to send reward trigger");
                Ok(())
            }
        }
    }
}
