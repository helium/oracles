use blake3::hash;
use chrono::{DateTime, Duration, Utc};
use file_store::{
    file_sink::{FileSinkClient, Message as SinkMessage},
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_witness_report::{IotWitnessIngestReport, IotWitnessReport},
    traits::{IngestId, MsgTimestamp},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{
        iot_reward_share::Reward as IotReward, GatewayReward, IotRewardShare,
        LoraBeaconIngestReportV1, LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1, LoraPocV1,
        LoraWitnessIngestReportV1, OperationalReward, UnallocatedReward,
    },
    DataRate, Region as ProtoRegion,
};
use iot_config::{
    client::RegionParamsInfo,
    gateway_info::{GatewayInfo, GatewayMetadata},
};
use iot_verifier::{
    entropy::Entropy,
    last_beacon_reciprocity::LastBeaconReciprocity,
    last_witness::LastWitness,
    poc_report::{InsertBindings, IotStatus, Report, ReportType},
    PriceInfo,
};

use helium_lib::token::Token;
use iot_config::sub_dao_epoch_reward_info::EpochRewardInfo;
use prost::Message;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::{PgPool, Postgres, Transaction};
use std::{self, ops::DerefMut, str::FromStr};
use tokio::{sync::mpsc::error::TryRecvError, sync::Mutex, time::timeout};

pub const EPOCH_ADDRESS: &str = "112E7TxoNHV46M6tiPA8N1MkeMeQxc9ztb4JQLXBVAAUfq1kJLoF";
pub const SUB_DAO_ADDRESS: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
pub const EMISSIONS_POOL_IN_BONES_24_HOURS: u64 = 89_041_095_890_411;

pub fn rewards_info_24_hours() -> EpochRewardInfo {
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
    let price_info = PriceInfo::new(1, token.decimals());
    assert_eq!(price_info.price_per_token, dec!(0.00000001));
    assert_eq!(price_info.price_per_bone, dec!(0.0000000000000001));
    price_info
}

pub fn create_file_sink<T: file_store::traits::MsgBytes>(
) -> (FileSinkClient<T>, MockFileSinkReceiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(10);

    (
        FileSinkClient::new(tx, "metric"),
        MockFileSinkReceiver { receiver: rx },
    )
}

pub struct MockFileSinkReceiver<T> {
    pub receiver: tokio::sync::mpsc::Receiver<SinkMessage<T>>,
}

impl<T: std::fmt::Debug> MockFileSinkReceiver<T> {
    pub async fn receive(&mut self) -> Option<T> {
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

    pub fn assert_no_messages(&mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }
}

impl MockFileSinkReceiver<LoraPocV1> {
    pub async fn receive_valid_poc(&mut self) -> LoraPocV1 {
        match self.receive().await {
            Some(msg) => msg,
            None => panic!("failed to receive valid poc"),
        }
    }
}
impl MockFileSinkReceiver<LoraInvalidBeaconReportV1> {
    pub async fn receive_invalid_beacon(&mut self) -> LoraInvalidBeaconReportV1 {
        match self.receive().await {
            Some(msg) => msg,
            None => panic!("failed to receive invalid beacon"),
        }
    }
}

impl MockFileSinkReceiver<LoraInvalidWitnessReportV1> {
    pub async fn receive_invalid_witness(&mut self) -> LoraInvalidWitnessReportV1 {
        match self.receive().await {
            Some(msg) => msg,
            None => panic!("failed to receive invalid witness"),
        }
    }
}

impl MockFileSinkReceiver<IotRewardShare> {
    pub async fn receive_gateway_reward(&mut self) -> GatewayReward {
        match self.receive().await {
            Some(iot_reward) => {
                println!("iot_reward: {:?}", iot_reward);
                match iot_reward.reward {
                    Some(IotReward::GatewayReward(r)) => r,
                    _ => panic!("failed to get gateway reward"),
                }
            }
            None => panic!("failed to receive gateway reward"),
        }
    }

    pub async fn receive_operational_reward(&mut self) -> OperationalReward {
        match self.receive().await {
            Some(iot_reward) => {
                println!("iot_reward: {:?}", iot_reward);
                match iot_reward.reward {
                    Some(IotReward::OperationalReward(r)) => r,
                    _ => panic!("failed to get operational reward"),
                }
            }
            None => panic!("failed to receive operational reward"),
        }
    }

    pub async fn receive_unallocated_reward(&mut self) -> UnallocatedReward {
        match self.receive().await {
            Some(iot_reward) => {
                println!("iot_reward: {:?}", iot_reward);
                match iot_reward.reward {
                    Some(IotReward::UnallocatedReward(r)) => r,
                    _ => panic!("failed to get unallocated reward"),
                }
            }
            None => panic!("failed to receive unallocated reward"),
        }
    }
}

fn seconds(s: u64) -> std::time::Duration {
    std::time::Duration::from_secs(s)
}

pub fn create_valid_beacon_report(
    pubkey: &str,
    received_timestamp: DateTime<Utc>,
) -> IotBeaconIngestReport {
    beacon_report_to_ingest_report(
        IotBeaconReport {
            pub_key: PublicKeyBinary::from_str(pubkey).unwrap(),
            local_entropy: LOCAL_ENTROPY.to_vec(),
            remote_entropy: REMOTE_ENTROPY.to_vec(),
            data: POC_DATA.to_vec(),
            frequency: 867900000,
            channel: 0,
            datarate: DataRate::Sf12bw125,
            tx_power: 8,
            timestamp: received_timestamp,
            signature: vec![],
            tmst: 0,
        },
        received_timestamp,
    )
}

pub fn create_valid_witness_report(
    pubkey: &str,
    received_timestamp: DateTime<Utc>,
) -> IotWitnessIngestReport {
    witness_report_to_ingest_report(
        IotWitnessReport {
            pub_key: PublicKeyBinary::from_str(pubkey).unwrap(),
            data: POC_DATA.to_vec(),
            timestamp: Utc::now(),
            tmst: 0,
            signal: -1080,
            snr: 35,
            frequency: 867900032,
            datarate: DataRate::Sf12bw125,
            signature: vec![],
        },
        received_timestamp,
    )
}

pub fn beacon_report_to_ingest_report(
    report: IotBeaconReport,
    received_timestamp: DateTime<Utc>,
) -> IotBeaconIngestReport {
    IotBeaconIngestReport {
        received_timestamp,
        report,
    }
}

pub fn witness_report_to_ingest_report(
    report: IotWitnessReport,
    received_timestamp: DateTime<Utc>,
) -> IotWitnessIngestReport {
    IotWitnessIngestReport {
        received_timestamp,
        report,
    }
}

pub async fn inject_beacon_report(
    pool: PgPool,
    beacon: IotBeaconIngestReport,
) -> anyhow::Result<()> {
    let mut inserts = Vec::<InsertBindings>::new();
    let lora_beacon: LoraBeaconIngestReportV1 = LoraBeaconIngestReportV1 {
        received_timestamp: beacon.timestamp(),
        report: Some(beacon.report.clone().into()),
    };
    let mut buf = vec![];
    lora_beacon.encode(&mut buf)?;
    let packet_data = beacon.report.data.clone();
    let binding = InsertBindings {
        id: beacon.ingest_id(),
        remote_entropy: beacon.report.remote_entropy.clone(),
        packet_data,
        buf,
        received_ts: beacon.received_timestamp,
        report_type: ReportType::Beacon,
        status: IotStatus::Ready,
    };
    inserts.push(binding);
    let txn = Mutex::new(pool.begin().await?);
    Report::bulk_insert(txn.lock().await.deref_mut(), inserts).await?;
    txn.into_inner().commit().await?;
    Ok(())
}

pub async fn inject_invalid_beacon_report(
    pool: PgPool,
    beacon: IotBeaconIngestReport,
) -> anyhow::Result<()> {
    let mut inserts = Vec::<InsertBindings>::new();
    let lora_beacon: LoraBeaconIngestReportV1 = LoraBeaconIngestReportV1 {
        received_timestamp: beacon.timestamp(),
        report: Some(beacon.report.clone().into()),
    };
    let mut buf = vec![];
    lora_beacon.encode(&mut buf)?;
    let packet_data = beacon.report.data.clone();
    let binding = InsertBindings {
        id: beacon.ingest_id(),
        remote_entropy: beacon.report.remote_entropy.clone(),
        packet_data,
        buf: buf[1..50].to_vec(), // corrupt this to force report to be invalid
        received_ts: beacon.received_timestamp,
        report_type: ReportType::Beacon,
        status: IotStatus::Ready,
    };
    inserts.push(binding);
    let txn = Mutex::new(pool.begin().await?);
    Report::bulk_insert(txn.lock().await.deref_mut(), inserts).await?;
    txn.into_inner().commit().await?;
    Ok(())
}

pub async fn inject_witness_report(
    pool: PgPool,
    witness: IotWitnessIngestReport,
) -> anyhow::Result<()> {
    let mut inserts = Vec::<InsertBindings>::new();
    let lora_witness: LoraWitnessIngestReportV1 = LoraWitnessIngestReportV1 {
        received_timestamp: witness.timestamp(),
        report: Some(witness.report.clone().into()),
    };
    let mut buf = vec![];
    lora_witness.encode(&mut buf)?;
    let packet_data = witness.report.data.clone();
    let binding = InsertBindings {
        id: witness.ingest_id(),
        remote_entropy: Vec::<u8>::with_capacity(0),
        packet_data,
        buf,
        received_ts: witness.received_timestamp,
        report_type: ReportType::Witness,
        status: IotStatus::Ready,
    };
    inserts.push(binding);
    let mut txn = pool.begin().await?;
    Report::bulk_insert(&mut txn, inserts).await?;
    txn.commit().await?;
    Ok(())
}

pub async fn inject_entropy_report(pool: PgPool, ts: DateTime<Utc>) -> anyhow::Result<()> {
    let data = REMOTE_ENTROPY.to_vec();
    let id = hash(&data).as_bytes().to_vec();
    let mut txn = pool.begin().await?;
    Entropy::insert_into(&mut txn, &id, &data, &ts, 0).await?;
    txn.commit().await?;
    Ok(())
}

pub async fn inject_last_beacon(
    txn: &mut Transaction<'_, Postgres>,
    gateway: PublicKeyBinary,
    ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    LastBeaconReciprocity::update_last_timestamp(&mut *txn, &gateway, ts).await
}

pub async fn inject_last_witness(
    txn: &mut Transaction<'_, Postgres>,
    gateway: PublicKeyBinary,
    ts: DateTime<Utc>,
) -> anyhow::Result<()> {
    LastWitness::update_last_timestamp(&mut *txn, &gateway, ts).await
}

pub fn valid_gateway() -> GatewayInfo {
    GatewayInfo {
        address: PublicKeyBinary::from_str(BEACONER1).unwrap(),
        metadata: None,
        is_full_hotspot: true,
    }
}

pub fn valid_gateway_stream() -> Vec<GatewayInfo> {
    vec![
        GatewayInfo {
            address: PublicKeyBinary::from_str(BEACONER1).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 631615575095659519,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(BEACONER2).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 631615575095659519,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(BEACONER3).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 631615575095659519,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(BEACONER4).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 631615575095659519,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(BEACONER5).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 627111975465463807,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(WITNESS1).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 627111975465463807,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(WITNESS2).unwrap(),
            metadata: Some(GatewayMetadata {
                location: 631615575095659519,
                elevation: 0,
                gain: 20,
                region: ProtoRegion::Eu868,
            }),
            is_full_hotspot: true,
        },
        GatewayInfo {
            address: PublicKeyBinary::from_str(NO_METADATA_GATEWAY1).unwrap(),
            metadata: None,
            is_full_hotspot: true,
        },
    ]
}

pub fn valid_region_params() -> RegionParamsInfo {
    let region_params =
        beacon::RegionParams::from_bytes(ProtoRegion::Eu868.into(), 60, EU868_PARAMS, 0)
            .expect("region params");
    RegionParamsInfo {
        region: ProtoRegion::Eu868,
        region_params: region_params.params,
    }
}

#[allow(dead_code)]
pub const BEACONER1: &str = "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6";
pub const BEACONER2: &str = "11z69eJ3czc92k6snrfR9ek7g2uRWXosFbnG9v4bXgwhfUCivUo";
pub const BEACONER3: &str = "1ZPNnNd9k5qiQXXigKifQpCPiy5HTbszQDSyLM56ywk7ihNRvt6";
pub const BEACONER4: &str = "1ZAxCrEsigGVbLUM37Jki6p88kyZ5NVqjVC6oHSbqu49t7bQDym";
pub const BEACONER5: &str = "112BwpY6ARmnMsPZE9iBauh6EJVDvH7MimZtvWnd99nXmmGcKeMD";

pub const WITNESS1: &str = "13ABbtvMrRK8jgYrT3h6Y9Zu44nS6829kzsamiQn9Eefeu3VAZs";
pub const WITNESS2: &str = "112e5E4NCpZ88ivqoXeyWwiVCC4mJFv4kMPowycNMXjoDRSP6ZnS";

pub const UNKNOWN_GATEWAY1: &str = "1YiZUsuCwxE7xyxjke1ogehv5WSuYZ9o7uM2ZKvRpytyqb8Be63";
pub const NO_METADATA_GATEWAY1: &str = "1YpopKVbRDELWGR3nMd1MAU8a5GxP1uQSDj9AeXHEi3fHSsWGRi";

pub const DENIED_PUBKEY1: &str = "112bUGwooPd1dCDd3h3yZwskjxCzBsQNKeaJTuUF4hSgYedcsFa9";

pub const LOCAL_ENTROPY: [u8; 4] = [233, 70, 25, 176];
pub const REMOTE_ENTROPY: [u8; 32] = [
    182, 170, 63, 128, 217, 53, 95, 19, 157, 153, 134, 38, 184, 209, 255, 23, 118, 205, 163, 106,
    225, 26, 16, 0, 106, 141, 81, 101, 70, 39, 107, 9,
];
pub const POC_DATA: [u8; 51] = [
    28, 153, 18, 65, 96, 232, 59, 146, 134, 125, 99, 12, 175, 76, 158, 210, 28, 253, 146, 59, 187,
    203, 122, 146, 49, 241, 156, 148, 74, 246, 68, 17, 8, 212, 48, 6, 152, 58, 221, 158, 186, 101,
    37, 59, 135, 126, 18, 72, 244, 65, 174,
];

pub const ENTROPY_TIMESTAMP: i64 = 1677163710000;

const EU868_PARAMS: &[u8] = &[
    10, 35, 8, 224, 202, 187, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10,
    5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 229, 199, 157, 3, 16, 200, 208,
    7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1,
    10, 35, 8, 224, 255, 211, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10,
    5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 154, 224, 157, 3, 16, 200, 208,
    7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1,
    10, 35, 8, 224, 180, 236, 157, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10,
    5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 207, 248, 157, 3, 16, 200, 208,
    7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1,
    10, 35, 8, 224, 233, 132, 158, 3, 16, 200, 208, 7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10,
    5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1, 10, 35, 8, 160, 132, 145, 158, 3, 16, 200, 208,
    7, 24, 161, 1, 34, 20, 10, 4, 8, 6, 16, 65, 10, 5, 8, 3, 16, 129, 1, 10, 5, 8, 2, 16, 238, 1,
];
