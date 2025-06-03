use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_sink::{FileSinkClient, Message as SinkMessage},
    traits::{MsgBytes, TimestampEncode},
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward as MobileReward, radio_reward_v2, GatewayReward, MobileRewardShare,
    OracleBoostingHexAssignment, OracleBoostingReportV1, PromotionReward, RadioReward,
    RadioRewardV2, ServiceProviderReward, SpeedtestAvg, SubscriberReward, UnallocatedReward,
};
use hex_assignments::{Assignment, HexAssignment, HexBoostDataAssignmentsExt};
use hextree::Cell;
use mobile_config::{
    boosted_hex_info::{BoostedHexInfo, BoostedHexInfoStream},
    client::sub_dao_client::SubDaoEpochRewardInfoResolver,
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
    sub_dao_epoch_reward_info::EpochRewardInfo,
};
use mobile_verifier::{
    boosting_oracles::AssignedCoverageObjects, GatewayResolution, GatewayResolver, PriceInfo,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use solana::Token;
use sqlx::PgPool;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};
use tokio::{sync::RwLock, time::Timeout};
use tonic::async_trait;

pub const EPOCH_ADDRESS: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
pub const SUB_DAO_ADDRESS: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";

pub const EMISSIONS_POOL_IN_BONES_24_HOURS: u64 = 82_191_780_821_917;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MockHexBoostingClient {
    boosted_hexes: Vec<BoostedHexInfo>,
}

#[derive(Debug, Clone)]
pub struct MockSubDaoRewardsClient {
    info: Option<EpochRewardInfo>,
}

impl MockHexBoostingClient {
    pub fn new(boosted_hexes: Vec<BoostedHexInfo>) -> Self {
        Self { boosted_hexes }
    }
}

#[async_trait::async_trait]
impl HexBoostingInfoResolver for MockHexBoostingClient {
    async fn stream_boosted_hexes_info(&mut self) -> Result<BoostedHexInfoStream, ClientError> {
        Ok(stream::iter(self.boosted_hexes.clone()).boxed())
    }

    async fn stream_modified_boosted_hexes_info(
        &mut self,
        _timestamp: DateTime<Utc>,
    ) -> Result<BoostedHexInfoStream, ClientError> {
        Ok(stream::iter(self.boosted_hexes.clone()).boxed())
    }
}

#[async_trait::async_trait]
impl SubDaoEpochRewardInfoResolver for MockSubDaoRewardsClient {
    async fn resolve_info(
        &self,
        _sub_dao: &str,
        _epoch: u64,
    ) -> Result<Option<EpochRewardInfo>, ClientError> {
        Ok(self.info.clone())
    }
}

pub trait RadioRewardV2Ext {
    fn boosted_hexes(&self) -> Vec<radio_reward_v2::CoveredHex>;
    fn nth_boosted_hex(&self, index: usize) -> radio_reward_v2::CoveredHex;
    fn boosted_hexes_len(&self) -> usize;
    fn total_poc_reward(&self) -> u64;
    fn total_coverage_points(&self) -> u64;
}

impl RadioRewardV2Ext for RadioRewardV2 {
    fn boosted_hexes(&self) -> Vec<radio_reward_v2::CoveredHex> {
        self.covered_hexes.to_vec()
    }

    fn boosted_hexes_len(&self) -> usize {
        self.covered_hexes
            .iter()
            .filter(|hex| hex.boosted_multiplier > 0)
            .collect::<Vec<_>>()
            .len()
    }

    fn nth_boosted_hex(&self, index: usize) -> radio_reward_v2::CoveredHex {
        self.covered_hexes
            .iter()
            .filter(|hex| hex.boosted_multiplier > 0)
            .cloned()
            .collect::<Vec<_>>()
            .get(index)
            .unwrap_or_else(|| panic!("expected {index} in boosted_hexes"))
            .clone()
    }

    fn total_poc_reward(&self) -> u64 {
        self.base_poc_reward + self.boosted_poc_reward
    }

    fn total_coverage_points(&self) -> u64 {
        let base = self.base_coverage_points_sum.clone().unwrap_or_default();
        let boosted = self.boosted_coverage_points_sum.clone().unwrap_or_default();

        let base = Decimal::from_str(&base.value).expect("decoding base cp");
        let boosted = Decimal::from_str(&boosted.value).expect("decoding boosted cp");

        (base + boosted).to_u64().unwrap()
    }
}

pub struct MockHexBoostDataAssignment {
    footfall: Assignment,
    urbanized: Assignment,
    landtype: Assignment,
    service_provider_override: Assignment,
}

pub fn mock_hex_boost_data_default() -> impl HexBoostDataAssignmentsExt {
    MockHexBoostDataAssignment {
        footfall: Assignment::A,
        landtype: Assignment::A,
        urbanized: Assignment::A,
        service_provider_override: Assignment::C,
    }
}

pub fn mock_hex_boost_data_bad() -> impl HexBoostDataAssignmentsExt {
    MockHexBoostDataAssignment {
        footfall: Assignment::C,
        landtype: Assignment::C,
        urbanized: Assignment::C,
        service_provider_override: Assignment::C,
    }
}

impl HexBoostDataAssignmentsExt for MockHexBoostDataAssignment {
    fn footfall_assignment(&self, _cell: Cell) -> anyhow::Result<Assignment> {
        Ok(self.footfall)
    }

    fn landtype_assignment(&self, _cell: Cell) -> anyhow::Result<Assignment> {
        Ok(self.landtype)
    }

    fn urbanization_assignment(&self, _cell: Cell) -> anyhow::Result<Assignment> {
        Ok(self.urbanized)
    }

    fn service_provider_override_assignment(&self, _cell: Cell) -> anyhow::Result<Assignment> {
        Ok(self.service_provider_override)
    }
}

#[derive(Default)]
pub struct MockHexBoostDataColl {
    pub footfall: HashMap<Cell, Assignment>,
    pub urbanized: HashMap<Cell, Assignment>,
    pub landtype: HashMap<Cell, Assignment>,
    pub service_provider_override: HashSet<Cell>,
}

impl HexBoostDataAssignmentsExt for MockHexBoostDataColl {
    fn footfall_assignment(&self, cell: Cell) -> anyhow::Result<Assignment> {
        self.footfall.assignment(cell)
    }

    fn landtype_assignment(&self, cell: Cell) -> anyhow::Result<Assignment> {
        self.landtype.assignment(cell)
    }

    fn urbanization_assignment(&self, cell: Cell) -> anyhow::Result<Assignment> {
        self.urbanized.assignment(cell)
    }

    fn service_provider_override_assignment(&self, cell: Cell) -> anyhow::Result<Assignment> {
        self.service_provider_override.assignment(cell)
    }
}

pub async fn set_unassigned_oracle_boosting_assignments(
    pool: &PgPool,
    data_sets: &impl HexBoostDataAssignmentsExt,
) -> anyhow::Result<Vec<OracleBoostingReportV1>> {
    let mut tx = pool.begin().await?;

    let assigned_coverage_objs = AssignedCoverageObjects::assign_hex_stream(
        dataset_downloader::db::fetch_hexes_with_null_assignments(&mut tx),
        data_sets,
    )
    .await?;
    let timestamp = Utc::now().encode_timestamp();
    let mut output = Vec::new();
    for (uuid, hexes) in assigned_coverage_objs.coverage_objs.iter() {
        let assignments: Vec<_> = hexes
            .iter()
            .map(|hex| {
                let location = format!("{:x}", hex.hex);
                let assignment_multiplier = (hex.assignments.boosting_multiplier() * dec!(1000))
                    .to_u32()
                    .unwrap_or(0);
                OracleBoostingHexAssignment {
                    location,
                    urbanized: hex.assignments.urbanized.into(),
                    footfall: hex.assignments.footfall.into(),
                    landtype: hex.assignments.landtype.into(),
                    service_provider_override: hex.assignments.service_provider_override.into(),
                    assignment_multiplier,
                }
            })
            .collect();
        output.push(OracleBoostingReportV1 {
            coverage_object: Vec::from(uuid.into_bytes()),
            assignments,
            timestamp,
        });
    }

    assigned_coverage_objs.save(&mut tx).await?;
    tx.commit().await?;
    Ok(output)
}

#[derive(Debug, Copy, Clone)]
pub struct GatewayClientAllOwnersValid;

#[async_trait]
impl GatewayResolver for GatewayClientAllOwnersValid {
    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
    ) -> Result<GatewayResolution, ClientError> {
        Ok(GatewayResolution::AssertedLocation(0x8c2681a3064d9ff))
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
        rewards_issued_at: now,
    }
}

pub fn default_price_info() -> PriceInfo {
    let token = Token::Hnt;
    let price_info = PriceInfo::new(1000000000000, token.decimals());
    assert_eq!(price_info.price_per_token, dec!(10000));
    assert_eq!(price_info.price_per_bone, dec!(0.0001));
    price_info
}

// Non-blocking version is file sink testing.
// Requires the FileSinkClient to be dropped when all writing is done, or panic!.
pub fn create_file_sink<T: Send + Sync + 'static>() -> (FileSinkClient<T>, FileSinkReceiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(999);
    (
        FileSinkClient {
            sender: tx,
            metric: "metric".into(),
        },
        FileSinkReceiver::new(rx),
    )
}

#[derive(Debug)]
pub struct FileSinkReceiver<T> {
    msgs: Arc<RwLock<Vec<T>>>,
    channel_closed: Arc<tokio::sync::Notify>,
}

#[derive(Default, Debug)]
pub struct MobileRewardShareMessages {
    pub gateway_rewards: Vec<GatewayReward>,
    pub radio_rewards: Vec<RadioReward>,
    pub subscriber_rewards: Vec<SubscriberReward>,
    pub sp_rewards: Vec<ServiceProviderReward>,
    pub unallocated: Vec<UnallocatedReward>,
    pub radio_reward_v2s: Vec<RadioRewardV2>,
    pub promotion_rewards: Vec<PromotionReward>,
}

impl MobileRewardShareMessages {
    fn insert(&mut self, item: MobileReward) {
        match item {
            MobileReward::GatewayReward(inner) => self.gateway_rewards.push(inner),
            MobileReward::RadioReward(inner) => self.radio_rewards.push(inner),
            MobileReward::SubscriberReward(inner) => self.subscriber_rewards.push(inner),
            MobileReward::ServiceProviderReward(inner) => self.sp_rewards.push(inner),
            MobileReward::UnallocatedReward(inner) => self.unallocated.push(inner),
            MobileReward::RadioRewardV2(inner) => self.radio_reward_v2s.push(inner),
            MobileReward::PromotionReward(inner) => self.promotion_rewards.push(inner),
        }
    }

    pub fn unallocated_amount_or_default(&self) -> u64 {
        self.unallocated
            .iter()
            .map(|reward| reward.amount)
            .sum::<u64>()
    }

    pub fn total_poc_rewards(&self) -> u64 {
        self.radio_reward_v2s
            .iter()
            .map(|reward| reward.total_poc_reward())
            .sum()
    }

    pub fn total_sub_discovery_amount(&self) -> u64 {
        self.subscriber_rewards
            .iter()
            .map(|reward| reward.discovery_location_amount)
            .sum()
    }
}

trait TestTimeoutExt<T>
where
    Self: Sized,
{
    fn timeout_2_secs(self) -> Timeout<Self>;
}

// Add ability to timeout all Futures after 2 seconds
impl<F: std::future::Future> TestTimeoutExt<F> for F {
    fn timeout_2_secs(self) -> Timeout<Self> {
        tokio::time::timeout(Duration::seconds(2).to_std().unwrap(), self)
    }
}

impl FileSinkReceiver<MobileRewardShare> {
    pub async fn finish(self) -> anyhow::Result<MobileRewardShareMessages> {
        // make sure channel is closed and done being written to
        if let Err(err) = self.channel_closed.notified().timeout_2_secs().await {
            panic!("file sink receiver channel was never closed: {err:?}");
        }

        let lock = Arc::try_unwrap(self.msgs).expect("no locks on messages");
        let msgs = lock.into_inner();

        let mut output = MobileRewardShareMessages::default();

        for msg in msgs {
            match msg.reward {
                Some(item) => output.insert(item),
                None => panic!("something went wrong"),
            };
        }

        Ok(output)
    }
}

impl FileSinkReceiver<SpeedtestAvg> {
    pub async fn finish(self) -> anyhow::Result<Vec<SpeedtestAvg>> {
        // make sure the channel is closed and done being written to
        if let Err(err) = self.channel_closed.notified().timeout_2_secs().await {
            panic!("file sink receiver channel was never closed: {err:?}");
        }

        let lock = Arc::try_unwrap(self.msgs).expect("no locks on messages");
        let msgs = lock.into_inner();

        Ok(msgs)
    }
}

impl<T: Send + Sync + 'static> FileSinkReceiver<T> {
    fn new(mut receiver: tokio::sync::mpsc::Receiver<SinkMessage<T>>) -> Self {
        let channel_closed = Arc::new(tokio::sync::Notify::new());
        let closer = channel_closed.clone();

        let msgs = Arc::new(RwLock::new(vec![]));
        let inner_msgs = msgs.clone();

        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg {
                    SinkMessage::Data(sender, msg) => {
                        sender.send(Ok(())).expect("ack file data");
                        inner_msgs.write().await.push(msg);
                    }
                    SinkMessage::Commit(_sender) => (),
                    SinkMessage::Rollback(_sender) => todo!(),
                }
            }
            closer.notify_one();
        });

        Self {
            msgs,
            channel_closed,
        }
    }
}

// Allows converting from a Vec<T> to HashMap<String, T>
//
// This trait assumes there will not be multiple entries
// in the Vec for a given String.
pub trait AsStringKeyedMap<V> {
    fn as_keyed_map(&self) -> HashMap<String, V>
    where
        Self: Sized;
}

pub trait AsStringKeyedMapKey {
    fn key(&self) -> String;
}

impl AsStringKeyedMapKey for RadioRewardV2 {
    fn key(&self) -> String {
        PublicKeyBinary::from(self.hotspot_key.to_vec()).to_string()
    }
}

impl AsStringKeyedMapKey for SubscriberReward {
    fn key(&self) -> String {
        use helium_proto::Message;
        String::decode(self.subscriber_id.as_bytes()).expect("decode subscriber id")
    }
}

impl AsStringKeyedMapKey for PromotionReward {
    fn key(&self) -> String {
        self.entity.to_owned()
    }
}

impl<V: AsStringKeyedMapKey + Clone> AsStringKeyedMap<V> for Vec<V> {
    fn as_keyed_map(&self) -> HashMap<String, V>
    where
        Self: Sized,
    {
        let mut map = HashMap::new();
        for item in self {
            let key = item.key();
            if map.contains_key(&key) {
                panic!("Duplicate string key found: {}", key);
            }
            map.insert(key, item.clone());
        }
        map
    }
}
