use crate::common;
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{
    file_info_poller::FileInfoStream,
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    FileInfo,
};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::DeviceType as MobileDeviceType, poc_mobile::SpeedtestAvgValidity,
};
use mobile_config::{
    client::gateway_client::GatewayInfoResolver,
    gateway_info::{DeviceType, GatewayInfo, GatewayInfoStream},
};
use mobile_verifier::speedtests::SpeedtestDaemon;
use sqlx::{Pool, Postgres};

#[derive(thiserror::Error, Debug)]
enum MockError {}

#[derive(Clone)]
struct MockGatewayInfoResolver {}

#[async_trait::async_trait]
impl GatewayInfoResolver for MockGatewayInfoResolver {
    type Error = MockError;

    async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
    ) -> Result<Option<GatewayInfo>, Self::Error> {
        Ok(Some(GatewayInfo {
            address: address.clone(),
            metadata: None,
            device_type: DeviceType::Cbrs,
            created_at: None,
            refreshed_at: None,
        }))
    }

    async fn stream_gateways_info(
        &mut self,
        _device_types: &[MobileDeviceType],
    ) -> Result<GatewayInfoStream, Self::Error> {
        todo!()
    }
}

#[sqlx::test]
async fn speedtests_average_should_only_include_last_48_hours(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, mut speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let stream = file_info_stream(vec![
        speedtest(&hotspot, "2024-01-01 01:00:00", 0, 101, 11),
        speedtest(&hotspot, "2024-01-02 01:00:00", 0, 99, 9),
        speedtest(&hotspot, "2024-01-03 01:00:00", 0, 101, 11),
        speedtest(&hotspot, "2024-01-04 01:00:00", 10, 100, 10),
        speedtest(&hotspot, "2024-01-05 01:00:00", 10, 100, 10),
        speedtest(&hotspot, "2024-01-06 01:00:00", 10, 100, 10),
    ]);

    assert!(daemon.process_file(stream).await.is_ok());

    let avgs = speedtest_avg_receiver.get_all_speedtest_avgs().await;

    assert_eq!(6, avgs.len());
    assert_eq!(SpeedtestAvgValidity::TooFewSamples, avgs[0].validity());
    assert_eq!(1.0, avgs[5].reward_multiplier);

    Ok(())
}

fn file_info_stream(
    speedtests: Vec<CellSpeedtestIngestReport>,
) -> FileInfoStream<CellSpeedtestIngestReport> {
    let file_info = FileInfo {
        key: "key".to_string(),
        prefix: "prefix".to_string(),
        timestamp: Utc::now(),
        size: 0,
    };

    FileInfoStream::new("default".to_string(), file_info, speedtests)
}

fn speedtest(
    pubkey: &PublicKeyBinary,
    ts: &str,
    u: u64,
    d: u64,
    l: u32,
) -> CellSpeedtestIngestReport {
    CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: pubkey.clone(),
            serial: "".to_string(),
            timestamp: parse_dt(ts),
            upload_speed: mbps(u),
            download_speed: mbps(d),
            latency: l,
        },
    }
}

fn parse_dt(dt: &str) -> DateTime<Utc> {
    NaiveDateTime::parse_from_str(dt, "%Y-%m-%d %H:%M:%S")
        .expect("unable_to_parse")
        .and_utc()
}

fn mbps(mbps: u64) -> u64 {
    mbps * 125000
}
