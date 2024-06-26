use chrono::{DateTime, Utc};
use file_store::{
    file_sink::{FileSinkClient, Message as SinkMessage},
    traits::TimestampEncode,
};
use futures::{stream, StreamExt};
use helium_proto::{
    services::poc_mobile::{
        mobile_reward_share::Reward as MobileReward, GatewayReward, MobileRewardShare,
        OracleBoostingHexAssignment, OracleBoostingReportV1, RadioReward, ServiceProviderReward,
        SpeedtestAvg, SubscriberReward, UnallocatedReward,
    },
    Message,
};
use hex_assignments::{Assignment, HexAssignment, HexBoostData};
use mobile_config::{
    boosted_hex_info::{BoostedHexInfo, BoostedHexInfoStream},
    client::{hex_boosting_client::HexBoostingInfoResolver, ClientError},
};

use mobile_verifier::boosting_oracles::AssignedCoverageObjects;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::collections::HashMap;
use tokio::{sync::mpsc::error::TryRecvError, time::timeout};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MockHexBoostingClient {
    boosted_hexes: Vec<BoostedHexInfo>,
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

pub struct MockFileSinkReceiver {
    pub receiver: tokio::sync::mpsc::Receiver<SinkMessage>,
}

impl MockFileSinkReceiver {
    pub async fn receive(&mut self) -> Option<Vec<u8>> {
        match timeout(seconds(2), self.receiver.recv()).await {
            Ok(Some(SinkMessage::Data(on_write_tx, msg))) => {
                let _ = on_write_tx.send(Ok(()));
                Some(msg)
            }
            Ok(None) => None,
            Err(e) => panic!("timeout while waiting for message1 {:?}", e),
            Ok(Some(unexpected_msg)) => {
                println!("ignoring unexpected msg {:?}", unexpected_msg);
                None
            }
        }
    }

    pub async fn get_all(&mut self) -> Vec<Vec<u8>> {
        let mut buf = Vec::new();
        while let Ok(SinkMessage::Data(on_write_tx, msg)) = self.receiver.try_recv() {
            let _ = on_write_tx.send(Ok(()));
            buf.push(msg);
        }
        buf
    }

    pub fn assert_no_messages(&mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }

    pub async fn get_all_speedtest_avgs(&mut self) -> Vec<SpeedtestAvg> {
        self.get_all()
            .await
            .into_iter()
            .map(|bytes| {
                SpeedtestAvg::decode(bytes.as_slice()).expect("Not a valid speedtest average")
            })
            .collect()
    }

    pub async fn receive_radio_reward(&mut self) -> RadioReward {
        match self.receive().await {
            Some(bytes) => {
                let mobile_reward = MobileRewardShare::decode(bytes.as_slice())
                    .expect("failed to decode expected radio reward");
                println!("mobile_reward: {:?}", mobile_reward);
                match mobile_reward.reward {
                    Some(MobileReward::RadioReward(r)) => r,
                    _ => panic!("failed to get radio reward"),
                }
            }
            None => panic!("failed to receive radio reward"),
        }
    }

    pub async fn receive_gateway_reward(&mut self) -> GatewayReward {
        match self.receive().await {
            Some(bytes) => {
                let mobile_reward = MobileRewardShare::decode(bytes.as_slice())
                    .expect("failed to decode expected gateway reward");
                println!("mobile_reward: {:?}", mobile_reward);
                match mobile_reward.reward {
                    Some(MobileReward::GatewayReward(r)) => r,
                    _ => panic!("failed to get gateway reward"),
                }
            }
            None => panic!("failed to receive gateway reward"),
        }
    }

    pub async fn receive_service_provider_reward(&mut self) -> ServiceProviderReward {
        match self.receive().await {
            Some(bytes) => {
                let mobile_reward = MobileRewardShare::decode(bytes.as_slice())
                    .expect("failed to decode expected service provider reward");
                println!("mobile_reward: {:?}", mobile_reward);
                match mobile_reward.reward {
                    Some(MobileReward::ServiceProviderReward(r)) => r,
                    _ => panic!("failed to get service provider reward"),
                }
            }
            None => panic!("failed to receive service provider reward"),
        }
    }

    pub async fn receive_subscriber_reward(&mut self) -> SubscriberReward {
        match self.receive().await {
            Some(bytes) => {
                let mobile_reward = MobileRewardShare::decode(bytes.as_slice())
                    .expect("failed to decode expected subscriber reward");
                println!("mobile_reward: {:?}", mobile_reward);
                match mobile_reward.reward {
                    Some(MobileReward::SubscriberReward(r)) => r,
                    _ => panic!("failed to get subscriber reward"),
                }
            }
            None => panic!("failed to receive subscriber reward"),
        }
    }

    pub async fn receive_unallocated_reward(&mut self) -> UnallocatedReward {
        match self.receive().await {
            Some(bytes) => {
                let mobile_reward = MobileRewardShare::decode(bytes.as_slice())
                    .expect("failed to decode expected unallocated reward");
                println!("mobile_reward: {:?}", mobile_reward);
                match mobile_reward.reward {
                    Some(MobileReward::UnallocatedReward(r)) => r,
                    _ => panic!("failed to get unallocated reward"),
                }
            }
            None => panic!("failed to receive unallocated reward"),
        }
    }
}

pub fn create_file_sink() -> (FileSinkClient, MockFileSinkReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    (
        FileSinkClient {
            sender: tx,
            metric: "metric",
        },
        MockFileSinkReceiver { receiver: rx },
    )
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
