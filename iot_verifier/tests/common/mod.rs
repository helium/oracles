use blake3::hash;
use chrono::{DateTime, Utc};
use file_store::{
    file_sink::{FileSinkClient, Message as SinkMessage},
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_witness_report::{IotWitnessIngestReport, IotWitnessReport},
    traits::{IngestId, MsgTimestamp},
};
use helium_crypto::PublicKeyBinary;
use helium_proto::{
    services::poc_lora::{
        LoraBeaconIngestReportV1, LoraInvalidBeaconReportV1, LoraInvalidWitnessReportV1, LoraPocV1,
        LoraWitnessIngestReportV1,
    },
    DataRate, Region as ProtoRegion,
};
use iot_config::{
    client::RegionParamsInfo,
    gateway_info::{GatewayInfo, GatewayMetadata},
};
use iot_verifier::{
    entropy::Entropy,
    poc_report::{InsertBindings, IotStatus, Report, ReportType},
};

use prost::Message;
use sqlx::PgPool;
use std::{self, ops::DerefMut, str::FromStr};
use tokio::{sync::mpsc::error::TryRecvError, sync::Mutex, time::timeout};

pub fn create_file_sink() -> (FileSinkClient, MockFileSinkReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(5);
    (
        FileSinkClient::new(tx, "metric"),
        MockFileSinkReceiver { receiver: rx },
    )
}

pub struct MockFileSinkReceiver {
    pub receiver: tokio::sync::mpsc::Receiver<SinkMessage>,
}

#[allow(dead_code)]
impl MockFileSinkReceiver {
    pub async fn receive(&mut self) -> SinkMessage {
        match timeout(seconds(2), self.receiver.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => panic!("server closed connection while waiting for message"),
            Err(_) => panic!("timeout while waiting for message"),
        }
    }

    pub fn assert_no_messages(mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }

    pub async fn receive_valid_poc(&mut self) -> LoraPocV1 {
        match self.receive().await {
            SinkMessage::Data(_, bytes) => {
                LoraPocV1::decode(bytes.as_slice()).expect("decode beacon report")
            }
            _ => panic!("invalid beacon message"),
        }
    }

    pub async fn receive_invalid_beacon(&mut self) -> LoraInvalidBeaconReportV1 {
        match self.receive().await {
            SinkMessage::Data(_, bytes) => LoraInvalidBeaconReportV1::decode(bytes.as_slice())
                .expect("decode invalid beacon report"),
            _ => panic!("invalid beacon message"),
        }
    }

    pub async fn receive_invalid_witness(&mut self) -> LoraInvalidWitnessReportV1 {
        match self.receive().await {
            SinkMessage::Data(_, bytes) => LoraInvalidWitnessReportV1::decode(bytes.as_slice())
                .expect("decode invalid witness report"),
            _ => panic!("invalid witness message"),
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

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn beacon_report_to_ingest_report(
    report: IotBeaconReport,
    received_timestamp: DateTime<Utc>,
) -> IotBeaconIngestReport {
    IotBeaconIngestReport {
        received_timestamp,
        report,
    }
}

#[allow(dead_code)]
pub fn witness_report_to_ingest_report(
    report: IotWitnessReport,
    received_timestamp: DateTime<Utc>,
) -> IotWitnessIngestReport {
    IotWitnessIngestReport {
        received_timestamp,
        report,
    }
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
pub async fn inject_entropy_report(pool: PgPool, ts: DateTime<Utc>) -> anyhow::Result<()> {
    let data = REMOTE_ENTROPY.to_vec();
    let id = hash(&data).as_bytes().to_vec();
    let mut txn = pool.begin().await?;
    Entropy::insert_into(&mut txn, &id, &data, &ts, 0).await?;
    txn.commit().await?;
    Ok(())
}

#[allow(dead_code)]
pub fn valid_gateway() -> GatewayInfo {
    GatewayInfo {
        address: PublicKeyBinary::from_str(BEACONER1).unwrap(),
        metadata: None,
        is_full_hotspot: true,
    }
}

#[allow(dead_code)]
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
            address: PublicKeyBinary::from_str(NO_METADATA_GATEWAY1).unwrap(),
            metadata: None,
            is_full_hotspot: true,
        },
    ]
}

#[allow(dead_code)]
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

pub const WITNESS1: &str = "13ABbtvMrRK8jgYrT3h6Y9Zu44nS6829kzsamiQn9Eefeu3VAZs";
#[allow(dead_code)]
pub const UNKNOWN_GATEWAY1: &str = "1YiZUsuCwxE7xyxjke1ogehv5WSuYZ9o7uM2ZKvRpytyqb8Be63";
pub const NO_METADATA_GATEWAY1: &str = "1YpopKVbRDELWGR3nMd1MAU8a5GxP1uQSDj9AeXHEi3fHSsWGRi";

#[allow(dead_code)]
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
