use anyhow::Context;
use helium_iceberg::{BoxedDataWriter, IntoBoxedDataWriter};
use serde::Serialize;

pub mod ban;
pub mod burned_session;
pub mod gateway_reward;
pub mod heartbeat;
pub mod invalid_ban;
pub mod invalid_heartbeat;
pub mod invalid_speedtest;
pub mod invalid_speedtest_avg;
pub mod service_provider_reward;
pub mod speedtest;
pub mod speedtest_avg;
pub mod unallocated_reward;
pub mod valid_invalid;

pub use ban::IcebergBan;
pub use heartbeat::IcebergHeartbeat;
pub use invalid_ban::IcebergInvalidBan;
pub use invalid_heartbeat::IcebergInvalidHeartbeat;
pub use invalid_speedtest::IcebergInvalidSpeedtest;
pub use invalid_speedtest_avg::IcebergInvalidSpeedtestAvg;
pub use speedtest::IcebergSpeedtest;
pub use speedtest_avg::IcebergSpeedtestAvg;
pub use valid_invalid::ValidInvalidWriter;

pub const NAMESPACE: &str = "poc";
pub const REWARDS_NAMESPACE: &str = "rewards";

/// Column appended to every `invalid_*` table, recording why a record was
/// rejected (a validity/status enum's string name).
pub const REASON_COLUMN: &str = "reason";

/// Strip a protobuf enum's `as_str_name()` prefix so a stored reason reads
/// `gateway_not_found` rather than `heartbeat_validity_gateway_not_found`.
/// Falls back to the full name if the prefix isn't present.
pub(crate) fn reason_without_prefix(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_string()
}

// POC tables write accepted records to `poc.<table>` and rejected records to a
// sibling `poc.invalid_<table>` (same schema plus a `reason` column). See
// `helium_iceberg::ValidInvalidWriter`.
pub type BanWriter = ValidInvalidWriter<IcebergBan, IcebergInvalidBan>;
pub type HeartbeatWriter = ValidInvalidWriter<IcebergHeartbeat, IcebergInvalidHeartbeat>;
pub type SpeedtestWriter = ValidInvalidWriter<IcebergSpeedtest, IcebergInvalidSpeedtest>;
pub type SpeedtestAvgWriter = ValidInvalidWriter<IcebergSpeedtestAvg, IcebergInvalidSpeedtestAvg>;

pub struct PocWriters {
    pub ban: Option<BanWriter>,
    pub heartbeat: Option<HeartbeatWriter>,
    pub speedtest: Option<SpeedtestWriter>,
    pub speedtest_avg: Option<SpeedtestAvgWriter>,
}

impl PocWriters {
    pub fn noop() -> Self {
        Self {
            ban: None,
            heartbeat: None,
            speedtest: None,
            speedtest_avg: None,
        }
    }

    pub async fn from_settings(settings: &helium_iceberg::Settings) -> anyhow::Result<Self> {
        tracing::info!("iceberg settings provided, connecting...");
        let catalog = settings.connect().await.context("connecting to catalog")?;
        catalog.create_namespace_if_not_exists(NAMESPACE).await?;

        // Each writer creates its valid table plus the sibling `invalid_*`
        // table (same schema + a `reason` column; see the `invalid_*` modules).
        let ban = ValidInvalidWriter::create(
            &catalog,
            ban::table_definition()?,
            invalid_ban::table_definition()?,
        )
        .await?;
        let heartbeat = ValidInvalidWriter::create(
            &catalog,
            heartbeat::table_definition()?,
            invalid_heartbeat::table_definition()?,
        )
        .await?;
        let speedtest = ValidInvalidWriter::create(
            &catalog,
            speedtest::table_definition()?,
            invalid_speedtest::table_definition()?,
        )
        .await?;
        let speedtest_avg = ValidInvalidWriter::create(
            &catalog,
            speedtest_avg::table_definition()?,
            invalid_speedtest_avg::table_definition()?,
        )
        .await?;

        Ok(Self {
            ban: Some(ban),
            heartbeat: Some(heartbeat),
            speedtest: Some(speedtest),
            speedtest_avg: Some(speedtest_avg),
        })
    }
}

pub struct RewardWriters {
    data_transfer: BoxedDataWriter<gateway_reward::IcebergGatewayReward>,
    service_provider: BoxedDataWriter<service_provider_reward::IcebergServiceProviderReward>,
    unallocated: BoxedDataWriter<unallocated_reward::IcebergUnallocatedReward>,
}

impl RewardWriters {
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
