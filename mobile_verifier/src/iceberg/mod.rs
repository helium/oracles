use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, BranchTransaction, IntoBoxedDataWriter};
use serde::Serialize;

pub mod gateway_reward;
pub mod heartbeat;
pub mod radio_reward;
pub mod radio_reward_covered_hex;
pub mod service_provider_reward;
pub mod unallocated_reward;

pub use heartbeat::IcebergHeartbeat;

pub const NAMESPACE: &str = "poc";
pub const REWARDS_NAMESPACE: &str = "rewards";

pub type HeartbeatWriter = BoxedDataWriter<IcebergHeartbeat>;
pub type HeartbeatTransaction = BranchTransaction<IcebergHeartbeat>;

pub async fn get_writer(settings: &helium_iceberg::Settings) -> anyhow::Result<HeartbeatWriter> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let writer = catalog
        .create_table_if_not_exists(heartbeat::table_definition()?)
        .await?;

    Ok(writer.boxed())
}

pub struct RewardWriters {
    proof_of_coverage: BoxedDataWriter<radio_reward::IcebergRadioReward>,
    covered_hexes: BoxedDataWriter<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    data_transfer: BoxedDataWriter<gateway_reward::IcebergGatewayReward>,
    service_provider: BoxedDataWriter<service_provider_reward::IcebergServiceProviderReward>,
    unallocated: BoxedDataWriter<unallocated_reward::IcebergUnallocatedReward>,
}

pub struct RewardTransactions {
    pub proof_of_coverage: BranchTransaction<radio_reward::IcebergRadioReward>,
    pub covered_hexes: BranchTransaction<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    pub data_transfer: BranchTransaction<gateway_reward::IcebergGatewayReward>,
    pub service_provider: BranchTransaction<service_provider_reward::IcebergServiceProviderReward>,
    pub unallocated: BranchTransaction<unallocated_reward::IcebergUnallocatedReward>,
}

impl RewardWriters {
    pub async fn begin(&self, wap_id: &str) -> anyhow::Result<RewardTransactions> {
        Ok(RewardTransactions {
            proof_of_coverage: self.proof_of_coverage.begin(wap_id).await?,
            covered_hexes: self.covered_hexes.begin(wap_id).await?,
            data_transfer: self.data_transfer.begin(wap_id).await?,
            service_provider: self.service_provider.begin(wap_id).await?,
            unallocated: self.unallocated.begin(wap_id).await?,
        })
    }
}

impl RewardTransactions {
    pub async fn publish(self) -> anyhow::Result<()> {
        self.proof_of_coverage
            .publish()
            .await
            .context("publishing proof_of_coverage")?;
        self.covered_hexes
            .publish()
            .await
            .context("publishing covered_hexes")?;
        self.data_transfer
            .publish()
            .await
            .context("publishing data_transfer")?;
        self.service_provider
            .publish()
            .await
            .context("publishing service_provider")?;
        self.unallocated
            .publish()
            .await
            .context("publishing unallocated")?;
        Ok(())
    }
}

pub async fn get_reward_writers(
    settings: &helium_iceberg::Settings,
) -> anyhow::Result<RewardWriters> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog
        .create_namespace_if_not_exists(REWARDS_NAMESPACE)
        .await?;

    let proof_of_coverage = catalog
        .create_table_if_not_exists(radio_reward::table_definition()?)
        .await?;
    let covered_hexes = catalog
        .create_table_if_not_exists(radio_reward_covered_hex::table_definition()?)
        .await?;
    let data_transfer = catalog
        .create_table_if_not_exists(gateway_reward::table_definition()?)
        .await?;
    let service_provider = catalog
        .create_table_if_not_exists(service_provider_reward::table_definition()?)
        .await?;
    let unallocated = catalog
        .create_table_if_not_exists(unallocated_reward::table_definition()?)
        .await?;

    Ok(RewardWriters {
        proof_of_coverage: proof_of_coverage.boxed(),
        covered_hexes: covered_hexes.boxed(),
        data_transfer: data_transfer.boxed(),
        service_provider: service_provider.boxed(),
        unallocated: unallocated.boxed(),
    })
}

pub async fn maybe_begin<T: Serialize + Send + 'static>(
    writer: Option<&BoxedDataWriter<T>>,
    wap_id: &str,
) -> anyhow::Result<Option<BranchTransaction<T>>> {
    let Some(data_writer) = writer else {
        return Ok(None);
    };

    let txn = data_writer.begin(wap_id).await?;
    Ok(Some(txn))
}

pub async fn maybe_publish<T: Serialize + Send + 'static>(
    txn: Option<BranchTransaction<T>>,
) -> anyhow::Result<()> {
    if let Some(txn) = txn {
        txn.publish().await.context("publishing")?;
    }
    Ok(())
}
