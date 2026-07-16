use chrono::{DateTime, Duration, Utc};
use file_store::file_sink::{FileSinkClient, Message as SinkMessage};
use helium_crypto::PublicKeyBinary;
use helium_iceberg::IcebergTestHarness;
use helium_proto::services::poc_mobile::{
    mobile_reward_share::Reward as MobileReward, GatewayReward, MobileRewardShare, PromotionReward,
    RadioReward, RadioRewardV2, ServiceProviderReward, SpeedtestAvg, SubscriberReward,
    UnallocatedReward,
};
use mobile_config::gateway::service::info::DeviceType;
use mobile_config::{client::ClientError, sub_dao_epoch_reward_info::EpochRewardInfo};
use mobile_verifier::{GatewayResolution, GatewayResolver, PriceInfo};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana::Token;
use std::sync::Arc;
use tokio::{sync::RwLock, time::Timeout};
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
    ) -> Result<GatewayResolution, ClientError> {
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
                        // Mirror the real file sink: the write is recorded regardless of
                        // whether the caller is still awaiting its ack. `write_all` keeps
                        // only the last receiver, so earlier acks have no receiver — that
                        // is expected, not an error.
                        let _ = sender.send(Ok(()));
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
