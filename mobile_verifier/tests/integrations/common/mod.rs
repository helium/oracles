use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_sink::{FileSinkClient, Message as SinkMessage},
    traits::TimestampEncode,
};
use futures::{stream, StreamExt};
use helium_crypto::PublicKeyBinary;
use helium_lib::keypair::Pubkey;
use helium_lib::token::Token;
use helium_proto::services::{
    mobile_config::NetworkKeyRole,
    poc_mobile::{
        mobile_reward_share::Reward as MobileReward, radio_reward_v2, GatewayReward,
        MobileRewardShare, OracleBoostingHexAssignment, OracleBoostingReportV1, PromotionReward,
        RadioReward, RadioRewardV2, ServiceProviderReward, SpeedtestAvg, SubscriberReward,
        UnallocatedReward,
    },
};
use hex_assignments::{Assignment, HexAssignment, HexBoostData};
use mobile_config::{
    boosted_hex_info::{BoostedHexInfo, BoostedHexInfoStream},
    client::sub_dao_client::SubDaoEpochRewardInfoResolver,
    client::{
        authorization_client::AuthorizationVerifier, entity_client::EntityVerifier,
        hex_boosting_client::HexBoostingInfoResolver, ClientError,
    },
    sub_dao_epoch_reward_info::EpochRewardInfo,
};
use mobile_verifier::{
    boosting_oracles::AssignedCoverageObjects, GatewayResolution, GatewayResolver, PriceInfo,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::{collections::HashMap, str::FromStr};
use tokio::{sync::mpsc::error::TryRecvError, time::timeout};
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
    type Error = ClientError;

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
    type Error = ClientError;

    async fn resolve_info(
        &self,
        _sub_dao: &Pubkey,
        _epoch: u64,
    ) -> Result<Option<EpochRewardInfo>, Self::Error> {
        Ok(self.info.clone())
    }
}
pub struct MockFileSinkReceiver<T> {
    pub receiver: tokio::sync::mpsc::Receiver<SinkMessage<T>>,
}

impl<T: std::fmt::Debug> MockFileSinkReceiver<T> {
    pub async fn receive(&mut self, caller: &str) -> Option<T> {
        match timeout(seconds(2), self.receiver.recv()).await {
            Ok(Some(SinkMessage::Data(on_write_tx, msg))) => {
                let _ = on_write_tx.send(Ok(()));
                Some(msg)
            }
            Ok(None) => None,
            Err(e) => panic!("{caller}: timeout while waiting for message1 {:?}", e),
            Ok(Some(unexpected_msg)) => {
                println!("{caller}: ignoring unexpected msg {:?}", unexpected_msg);
                None
            }
        }
    }

    pub fn assert_no_messages(&mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }
}

impl MockFileSinkReceiver<SpeedtestAvg> {
    pub async fn get_all_speedtest_avgs(&mut self) -> Vec<SpeedtestAvg> {
        let mut messages = vec![];
        while let Ok(SinkMessage::Data(on_write_tx, msg)) = self.receiver.try_recv() {
            let _ = on_write_tx.send(Ok(()));
            messages.push(msg);
        }
        messages
    }
}

impl MockFileSinkReceiver<MobileRewardShare> {
    pub async fn receive_radio_reward_v1(&mut self) -> RadioReward {
        match self.receive("receive_radio_reward_v1").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::RadioReward(r)) => r,
                err => panic!("failed to get radio reward: {err:?}"),
            },
            None => panic!("failed to receive radio reward"),
        }
    }

    pub async fn receive_radio_reward(&mut self) -> RadioRewardV2 {
        // NOTE(mj): When v1 rewards stop being written, remove this receiver
        // and the comparison.
        let radio_reward_v1 = self.receive_radio_reward_v1().await;
        match self.receive("receive_radio_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::RadioRewardV2(reward)) => {
                    assert_eq!(
                        reward.total_poc_reward(),
                        radio_reward_v1.poc_reward,
                        "mismatch in poc rewards between v1 and v2"
                    );
                    reward
                }
                err => panic!("failed to get radio reward: {err:?}"),
            },
            None => panic!("failed to receive radio reward"),
        }
    }

    pub async fn receive_gateway_reward(&mut self) -> GatewayReward {
        match self.receive("receive_gateway_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::GatewayReward(r)) => r,
                _ => panic!("failed to get gateway reward"),
            },
            None => panic!("failed to receive gateway reward"),
        }
    }

    pub async fn receive_service_provider_reward(&mut self) -> ServiceProviderReward {
        match self.receive("receive_service_provider_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::ServiceProviderReward(r)) => r,
                _ => panic!("failed to get service provider reward"),
            },
            None => panic!("failed to receive service provider reward"),
        }
    }

    pub async fn receive_subscriber_reward(&mut self) -> SubscriberReward {
        match self.receive("receive_subscriber_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::SubscriberReward(r)) => r,
                _ => panic!("failed to get subscriber reward"),
            },
            None => panic!("failed to receive subscriber reward"),
        }
    }

    pub async fn receive_promotion_reward(&mut self) -> PromotionReward {
        match self.receive("receive_promotion_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::PromotionReward(r)) => r,
                _ => panic!("failed to get promotion reward"),
            },
            None => panic!("failed to receive promotion reward"),
        }
    }

    pub async fn receive_unallocated_reward(&mut self) -> UnallocatedReward {
        match self.receive("receive_unallocated_reward").await {
            Some(mobile_reward) => match mobile_reward.reward {
                Some(MobileReward::UnallocatedReward(r)) => r,
                _ => panic!("failed to get unallocated reward"),
            },
            None => panic!("failed to receive unallocated reward"),
        }
    }
}

pub fn create_file_sink<T>() -> (FileSinkClient<T>, MockFileSinkReceiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    (
        FileSinkClient {
            sender: tx,
            metric: "metric".into(),
        },
        MockFileSinkReceiver { receiver: rx },
    )
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

pub fn seconds(s: u64) -> std::time::Duration {
    std::time::Duration::from_secs(s)
}

pub fn mock_hex_boost_data_default() -> HexBoostData<Assignment, Assignment, Assignment> {
    HexBoostData::builder()
        .urbanization(Assignment::A)
        .footfall(Assignment::A)
        .landtype(Assignment::A)
        .build()
        .unwrap()
}

pub fn mock_hex_boost_data_bad() -> HexBoostData<Assignment, Assignment, Assignment> {
    HexBoostData::builder()
        .urbanization(Assignment::C)
        .footfall(Assignment::C)
        .landtype(Assignment::C)
        .build()
        .unwrap()
}

type MockAssignmentMap = HashMap<hextree::Cell, Assignment>;

#[allow(dead_code)]
pub fn mock_hex_boost_data(
    footfall: MockAssignmentMap,
    urbanized: MockAssignmentMap,
    landtype: MockAssignmentMap,
) -> HexBoostData<MockAssignmentMap, MockAssignmentMap, MockAssignmentMap> {
    HexBoostData::builder()
        .footfall(footfall)
        .urbanization(urbanized)
        .landtype(landtype)
        .build()
        .unwrap()
}

pub async fn set_unassigned_oracle_boosting_assignments(
    pool: &PgPool,
    data_sets: &HexBoostData<impl HexAssignment, impl HexAssignment, impl HexAssignment>,
) -> anyhow::Result<Vec<OracleBoostingReportV1>> {
    let assigned_coverage_objs = AssignedCoverageObjects::assign_hex_stream(
        mobile_verifier::boosting_oracles::data_sets::db::fetch_hexes_with_null_assignments(pool),
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
    assigned_coverage_objs.save(pool).await?;
    Ok(output)
}

#[derive(Clone)]
pub struct MockAuthorizationClient {}

impl MockAuthorizationClient {
    pub fn new() -> MockAuthorizationClient {
        Self {}
    }
}

#[async_trait]
impl AuthorizationVerifier for MockAuthorizationClient {
    type Error = ClientError;

    async fn verify_authorized_key(
        &self,
        _pubkey: &PublicKeyBinary,
        _role: NetworkKeyRole,
    ) -> Result<bool, ClientError> {
        Ok(true)
    }
}

#[derive(Clone)]
pub struct MockEntityClient {}

impl MockEntityClient {
    pub fn new() -> MockEntityClient {
        Self {}
    }
}

#[async_trait]
impl EntityVerifier for MockEntityClient {
    type Error = ClientError;

    async fn verify_rewardable_entity(&self, _entity_id: &[u8]) -> Result<bool, ClientError> {
        Ok(true)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct GatewayClientAllOwnersValid;

#[async_trait]
impl GatewayResolver for GatewayClientAllOwnersValid {
    type Error = std::convert::Infallible;

    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
    ) -> Result<GatewayResolution, Self::Error> {
        Ok(GatewayResolution::AssertedLocation(0x8c2681a3064d9ff))
    }
}

pub fn default_rewards_info(total_emissions: u64, epoch_duration: Duration) -> EpochRewardInfo {
    let now = Utc::now();
    EpochRewardInfo {
        epoch_day: 1,
        epoch_address: EPOCH_ADDRESS.into(),
        sub_dao_address: SUB_DAO_ADDRESS.into(),
        epoch_period: (now - epoch_duration)..now,
        epoch_emissions: Decimal::from(total_emissions),
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
