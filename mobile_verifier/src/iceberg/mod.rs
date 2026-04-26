use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

pub mod gateway_reward;
pub mod heartbeat;
pub mod radio_reward;
pub mod radio_reward_covered_hex;
pub mod service_provider_reward;
pub mod speedtest;
pub mod unallocated_reward;
pub mod unique_connections;

pub use heartbeat::IcebergHeartbeat;
pub use speedtest::IcebergSpeedtest;
pub use unique_connections::IcebergUniqueConnections;

pub const NAMESPACE: &str = "poc";
pub const REWARDS_NAMESPACE: &str = "rewards";

pub type HeartbeatWriter = BoxedDataWriter<IcebergHeartbeat>;
pub type SpeedtestWriter = BoxedDataWriter<IcebergSpeedtest>;
pub type UniqueConnectionsWriter = BoxedDataWriter<IcebergUniqueConnections>;

pub struct PocWriters {
    pub heartbeat: Option<HeartbeatWriter>,
    pub speedtest: Option<SpeedtestWriter>,
    pub unique_connections: Option<UniqueConnectionsWriter>,
}

impl PocWriters {
    pub fn noop() -> Self {
        Self {
            heartbeat: None,
            speedtest: None,
            unique_connections: None,
        }
    }

    pub async fn from_settings(settings: &helium_iceberg::Settings) -> anyhow::Result<Self> {
        tracing::info!("iceberg settings provided, connecting...");
        let catalog = settings.connect().await.context("connecting to catalog")?;
        catalog.create_namespace_if_not_exists(NAMESPACE).await?;

        let heartbeat = catalog
            .create_table_if_not_exists(heartbeat::table_definition()?)
            .await?
            .boxed();
        let speedtest = catalog
            .create_table_if_not_exists(speedtest::table_definition()?)
            .await?
            .boxed();
        let unique_connections = catalog
            .create_table_if_not_exists(unique_connections::table_definition()?)
            .await?
            .boxed();

        Ok(Self {
            heartbeat: Some(heartbeat),
            speedtest: Some(speedtest),
            unique_connections: Some(unique_connections),
        })
    }
}

pub struct RewardWriters {
    proof_of_coverage: BoxedDataWriter<radio_reward::IcebergRadioReward>,
    covered_hexes: BoxedDataWriter<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    data_transfer: BoxedDataWriter<gateway_reward::IcebergGatewayReward>,
    service_provider: BoxedDataWriter<service_provider_reward::IcebergServiceProviderReward>,
    unallocated: BoxedDataWriter<unallocated_reward::IcebergUnallocatedReward>,
}

impl RewardWriters {
    pub async fn write_proof_of_coverage(
        &self,
        id: &str,
        rows: Vec<radio_reward::IcebergRadioReward>,
    ) -> anyhow::Result<()> {
        self.proof_of_coverage
            .write_idempotent(id, rows)
            .await
            .context("writing proof_of_coverage")
    }

    pub async fn write_covered_hexes(
        &self,
        id: &str,
        rows: Vec<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    ) -> anyhow::Result<()> {
        self.covered_hexes
            .write_idempotent(id, rows)
            .await
            .context("writing covered_hexes")
    }

    pub async fn write_data_transfer(
        &self,
        id: &str,
        rows: Vec<gateway_reward::IcebergGatewayReward>,
    ) -> anyhow::Result<()> {
        self.data_transfer
            .write_idempotent(id, rows)
            .await
            .context("writing data_transfer")
    }

    pub async fn write_service_provider(
        &self,
        id: &str,
        rows: Vec<service_provider_reward::IcebergServiceProviderReward>,
    ) -> anyhow::Result<()> {
        self.service_provider
            .write_idempotent(id, rows)
            .await
            .context("writing service_provider")
    }

    pub async fn write_unallocated(
        &self,
        id: &str,
        rows: Vec<unallocated_reward::IcebergUnallocatedReward>,
    ) -> anyhow::Result<()> {
        self.unallocated
            .write_idempotent(id, rows)
            .await
            .context("writing unallocated")
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

pub async fn maybe_write_idempotent<T: Serialize + Send + 'static>(
    writer: Option<&BoxedDataWriter<T>>,
    id: &str,
    records: Vec<T>,
) -> anyhow::Result<()> {
    if let Some(data_writer) = writer {
        data_writer
            .write_idempotent(id, records)
            .await
            .context("writing idempotent")?;
    }
    Ok(())
}
