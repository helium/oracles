use chrono::{DateTime, Duration, Utc};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_iceberg_oracles::rewards::{
    self, IcebergGatewayReward, IcebergServiceProviderReward, IcebergUnallocatedReward,
};
use mobile_verifier::rewarder::EpochRewardInfo;
use mobile_verifier::{DeviceType, GatewayResolution, GatewayResolver, PriceInfo};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana::Token;
use tonic::async_trait;

pub const EPOCH_ADDRESS: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
pub const SUB_DAO_ADDRESS: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

pub const EMISSIONS_POOL_IN_BONES_24_HOURS: u64 = 82_191_780_821_917;

#[derive(Debug, Copy, Clone)]
pub struct GatewayClientAllOwnersValid;

#[async_trait]
impl GatewayResolver for GatewayClientAllOwnersValid {
    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
        _gateway_query_timestamp: &DateTime<Utc>,
    ) -> anyhow::Result<GatewayResolution> {
        Ok(GatewayResolution::AssertedLocation(
            0x8c2681a3064d9ff,
            DeviceType::WifiIndoor,
        ))
    }
}

pub fn reward_info_24_hours() -> EpochRewardInfo {
    let now = Utc::now();
    let epoch_duration = Duration::hours(24);
    EpochRewardInfo {
        epoch_day: 1,
        epoch_address: EPOCH_ADDRESS.into(),
        sub_dao_address: SUB_DAO_ADDRESS.into(),
        epoch_period: (now - epoch_duration)..now,
        epoch_emissions: Decimal::from(EMISSIONS_POOL_IN_BONES_24_HOURS),
        // 6% is carved out for veHNT delegators on-chain; the rewarder
        // distributes the remaining ~94%.
        hnt_rewards_issued: Decimal::from(
            EMISSIONS_POOL_IN_BONES_24_HOURS - EMISSIONS_POOL_IN_BONES_24_HOURS * 6 / 100,
        ),
        delegation_rewards_issued: Decimal::from(EMISSIONS_POOL_IN_BONES_24_HOURS * 6 / 100),
        rewards_issued_at: now,
    }
}

pub fn default_price_info() -> PriceInfo {
    let token = Token::Hnt;
    let price_info = PriceInfo::new(1000000000000, token.decimals());
    assert_eq!(price_info.price_per_bone, dec!(0.0001));
    price_info
}

/// Every reward row the rewarder wrote to the harness's `rewards` tables.
#[derive(Default, Debug)]
pub struct WrittenRewards {
    pub gateway_rewards: Vec<IcebergGatewayReward>,
    pub sp_rewards: Vec<IcebergServiceProviderReward>,
    pub unallocated: Vec<IcebergUnallocatedReward>,
}

impl WrittenRewards {
    pub fn dc_transfer_sum(&self) -> u64 {
        self.gateway_rewards
            .iter()
            .map(|r| r.dc_transfer_reward)
            .sum()
    }

    pub fn unallocated_sum(&self) -> u64 {
        self.unallocated.iter().map(|r| r.amount).sum()
    }
}

pub async fn written_rewards(harness: &IcebergTestHarness) -> anyhow::Result<WrittenRewards> {
    async fn select_all<T>(harness: &IcebergTestHarness, table: &str) -> anyhow::Result<Vec<T>>
    where
        T: trino_rust_client::Trino + 'static,
        for<'de> T: serde::Deserialize<'de> + serde::Serialize,
    {
        let rows = harness
            .trino()
            .get_all::<T>(format!("SELECT * FROM {}.{table}", rewards::NAMESPACE))
            .await?;
        Ok(rows.into_vec())
    }

    Ok(WrittenRewards {
        gateway_rewards: select_all(harness, rewards::data_transfer::TABLE_NAME).await?,
        sp_rewards: select_all(harness, rewards::service_provider::TABLE_NAME).await?,
        unallocated: select_all(harness, rewards::unallocated::TABLE_NAME).await?,
    })
}

pub async fn setup_iceberg() -> anyhow::Result<IcebergTestHarness> {
    let harness = IcebergTestHarness::new_with_tables([
        mobile_verifier::iceberg::ban::table_definition()?,
        helium_iceberg_oracles::data_transfer::burned_session::table_definition()?,
        mobile_verifier::iceberg::heartbeat::table_definition()?,
        mobile_verifier::iceberg::speedtest::table_definition()?,
        mobile_verifier::iceberg::speedtest_avg::table_definition()?,
    ])
    .await?;
    Ok(harness)
}

/// Reward-table writers backed by the test harness's catalog.
///
/// Iceberg is a hard requirement of the service, so the reward path takes its
/// writers unconditionally; tests supply real ones rather than a stub.
pub async fn reward_writers(
    harness: &IcebergTestHarness,
) -> anyhow::Result<mobile_verifier::iceberg::RewardWriters> {
    mobile_verifier::iceberg::get_reward_writers(&harness.as_settings()).await
}

/// PoC-table writers (heartbeat, speedtest, speedtest average, ban) backed by
/// the test harness's catalog.
pub async fn poc_writers(
    harness: &IcebergTestHarness,
) -> anyhow::Result<mobile_verifier::iceberg::PocWriters> {
    mobile_verifier::iceberg::PocWriters::from_settings(&harness.as_settings()).await
}
