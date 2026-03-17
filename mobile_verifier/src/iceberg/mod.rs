use anyhow::Context;
use chrono::{DateTime, FixedOffset};
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

pub type HeartbeatWriter = BoxedDataWriter<IcebergHeartbeat>;
pub type HeartbeatTransaction = BranchTransaction<IcebergHeartbeat>;

pub(crate) fn proto_decimal_to_f64(d: &Option<helium_proto::Decimal>) -> f64 {
    d.as_ref()
        .and_then(|d| d.value.parse::<f64>().ok())
        .unwrap_or_default()
}

pub(crate) fn timestamp_to_dt(ts: u64) -> DateTime<FixedOffset> {
    DateTime::from_timestamp(ts as i64, 0)
        .unwrap_or_default()
        .fixed_offset()
}

pub async fn get_writer(settings: &helium_iceberg::Settings) -> anyhow::Result<HeartbeatWriter> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let writer = catalog
        .create_table_if_not_exists(heartbeat::table_definition()?)
        .await?;

    Ok(writer.boxed())
}

pub struct RewardWriters {
    radio_rewards: BoxedDataWriter<radio_reward::IcebergRadioReward>,
    radio_reward_covered_hexes:
        BoxedDataWriter<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    gateway_rewards: BoxedDataWriter<gateway_reward::IcebergGatewayReward>,
    service_provider_rewards:
        BoxedDataWriter<service_provider_reward::IcebergServiceProviderReward>,
    unallocated_rewards: BoxedDataWriter<unallocated_reward::IcebergUnallocatedReward>,
}

pub struct RewardTransactions {
    pub radio_rewards: BranchTransaction<radio_reward::IcebergRadioReward>,
    pub radio_reward_covered_hexes:
        BranchTransaction<radio_reward_covered_hex::IcebergRadioRewardCoveredHex>,
    pub gateway_rewards: BranchTransaction<gateway_reward::IcebergGatewayReward>,
    pub service_provider_rewards:
        BranchTransaction<service_provider_reward::IcebergServiceProviderReward>,
    pub unallocated_rewards: BranchTransaction<unallocated_reward::IcebergUnallocatedReward>,
}

impl RewardWriters {
    pub async fn begin(&self, wap_id: &str) -> anyhow::Result<RewardTransactions> {
        Ok(RewardTransactions {
            radio_rewards: self.radio_rewards.begin(wap_id).await?,
            radio_reward_covered_hexes: self.radio_reward_covered_hexes.begin(wap_id).await?,
            gateway_rewards: self.gateway_rewards.begin(wap_id).await?,
            service_provider_rewards: self.service_provider_rewards.begin(wap_id).await?,
            unallocated_rewards: self.unallocated_rewards.begin(wap_id).await?,
        })
    }
}

impl RewardTransactions {
    pub async fn publish(self) -> anyhow::Result<()> {
        self.radio_rewards
            .publish()
            .await
            .context("publishing radio_rewards")?;
        self.radio_reward_covered_hexes
            .publish()
            .await
            .context("publishing radio_reward_covered_hexes")?;
        self.gateway_rewards
            .publish()
            .await
            .context("publishing gateway_rewards")?;
        self.service_provider_rewards
            .publish()
            .await
            .context("publishing service_provider_rewards")?;
        self.unallocated_rewards
            .publish()
            .await
            .context("publishing unallocated_rewards")?;
        Ok(())
    }
}

pub async fn get_reward_writers(
    settings: &helium_iceberg::Settings,
) -> anyhow::Result<RewardWriters> {
    let catalog = settings.connect().await.context("connecting to catalog")?;

    catalog.create_namespace_if_not_exists(NAMESPACE).await?;

    let radio_rewards = catalog
        .create_table_if_not_exists(radio_reward::table_definition()?)
        .await?;
    let radio_reward_covered_hexes = catalog
        .create_table_if_not_exists(radio_reward_covered_hex::table_definition()?)
        .await?;
    let gateway_rewards = catalog
        .create_table_if_not_exists(gateway_reward::table_definition()?)
        .await?;
    let service_provider_rewards = catalog
        .create_table_if_not_exists(service_provider_reward::table_definition()?)
        .await?;
    let unallocated_rewards = catalog
        .create_table_if_not_exists(unallocated_reward::table_definition()?)
        .await?;

    Ok(RewardWriters {
        radio_rewards: radio_rewards.boxed(),
        radio_reward_covered_hexes: radio_reward_covered_hexes.boxed(),
        gateway_rewards: gateway_rewards.boxed(),
        service_provider_rewards: service_provider_rewards.boxed(),
        unallocated_rewards: unallocated_rewards.boxed(),
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
