use crate::common;
use chrono::{DateTime, NaiveDateTime, Utc};
use coverage_point_calculator::speedtest::BYTES_PER_MEGABIT;
use file_store::{file_info_poller::FileInfoStream, FileInfo};
use file_store_oracles::speedtest::{CellSpeedtest, CellSpeedtestIngestReport};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::{
    mobile_config::DeviceType as MobileDeviceType,
    poc_mobile::{SpeedtestAvgValidity, SpeedtestVerificationResult as SpeedtestResult},
};
use mobile_config::{
    client::ClientError,
    gateway::{
        client::GatewayInfoResolver,
        service::info::{DeviceType, GatewayInfo, GatewayInfoStream},
    },
};
use mobile_verifier::speedtests::SpeedtestDaemon;
use sqlx::{Pool, Postgres};

#[derive(Clone)]
struct MockGatewayInfoResolver {}

#[async_trait::async_trait]
impl GatewayInfoResolver for MockGatewayInfoResolver {
    async fn resolve_gateway_info(
        &self,
        address: &PublicKeyBinary,
        _gateway_query_timestamp: &DateTime<Utc>,
    ) -> Result<Option<GatewayInfo>, ClientError> {
        Ok(Some(GatewayInfo {
            address: address.clone(),
            metadata: None,
            device_type: DeviceType::Cbrs,
            created_at: None,
            refreshed_at: None,
            updated_at: None,
        }))
    }

    async fn stream_gateways_info(
        &mut self,
        _device_types: &[MobileDeviceType],
    ) -> Result<GatewayInfoStream, ClientError> {
        todo!()
    }
}

#[sqlx::test]
async fn speedtests_average_should_only_include_last_48_hours(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

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

    // Drop the daemon when it's done running to close the channel
    {
        let daemon = SpeedtestDaemon::new(
            pool,
            gateway_info_resolver,
            rx,
            speedtest_avg_client,
            verified_client,
        );

        daemon.process_file(stream).await?;
    }

    let avgs = speedtest_avg_receiver.finish().await?;

    assert_eq!(6, avgs.len());
    assert_eq!(SpeedtestAvgValidity::TooFewSamples, avgs[0].validity());
    assert_eq!(1.0, avgs[5].reward_multiplier);

    Ok(())
}

#[sqlx::test]
async fn speedtest_upload_exceeds_300megabits_ps_limit(pool: Pool<Postgres>) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, _speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: hotspot.clone(),
            serial: "test-serial".to_string(),
            timestamp: Utc::now(),
            upload_speed: mbps(400),   // exceeds limit
            download_speed: mbps(100), // within limit
            latency: 10,
        },
    };

    let result = daemon
        .validate_speedtest(&speedtest_report)
        .await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_download_exceeds_300_megabits_ps_limit(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, _speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    // Create speedtest with download speed > 300Mbits
    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: hotspot.clone(),
            serial: "test-serial".to_string(),
            timestamp: Utc::now(),
            upload_speed: mbps(50),    // within limit
            download_speed: mbps(350), // exceeds limit
            latency: 10,
        },
    };

    let result = daemon
        .validate_speedtest(&speedtest_report)
        .await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_both_speeds_exceed_300_megabits_ps_limit(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, _speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    // Create speedtest with both speeds > 300Mbits
    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: hotspot.clone(),
            serial: "test-serial".to_string(),
            timestamp: Utc::now(),
            upload_speed: mbps(400),   // exceeds limit
            download_speed: mbps(350), // exceeds limit
            latency: 10,
        },
    };

    let result = daemon
        .validate_speedtest(&speedtest_report)
        .await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_within_300_megabits_ps_limit_should_be_valid(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, _speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    // Create speedtest with both speeds within 300Mbits limit
    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: hotspot.clone(),
            serial: "test-serial".to_string(),
            timestamp: Utc::now(),
            upload_speed: mbps(100),   // within limit
            download_speed: mbps(200), // within limit
            latency: 10,
        },
    };

    let result = daemon
        .validate_speedtest(&speedtest_report)
        .await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValid);

    Ok(())
}

#[sqlx::test]
async fn speedtest_exactly_300_megabits_ps_limit_should_be_valid(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, _speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let daemon = SpeedtestDaemon::new(
        pool,
        gateway_info_resolver,
        rx,
        speedtest_avg_client,
        verified_client,
    );

    // Create speedtest with speeds exactly at 300Mbits limit
    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: Utc::now(),
        report: CellSpeedtest {
            pubkey: hotspot.clone(),
            serial: "test-serial".to_string(),
            timestamp: Utc::now(),
            upload_speed: mbps(300),   // should be valid
            download_speed: mbps(300), // should be valid
            latency: 10,
        },
    };

    let result = daemon
        .validate_speedtest(&speedtest_report)
        .await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValid);

    Ok(())
}

#[sqlx::test]
async fn invalid_speedtests_should_not_affect_average(pool: Pool<Postgres>) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (speedtest_avg_client, speedtest_avg_receiver) = common::create_file_sink();
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    // Create a mix of valid and invalid speedtests
    // Valid speedtests will average to "Poor" tier (0.25 multiplier)
    // Invalid speedtests have very high speeds that would result in "Good" tier if included
    let speedtests = vec![
        // Valid speedtest - should be included in average
        // Upload: 3 Mbps = Poor tier (≥2 but <5)
        // Download: 35 Mbps = Poor tier (≥30 but <50)
        // Latency: 80ms = Poor tier (≥75 but <100)
        CellSpeedtestIngestReport {
            received_timestamp: Utc::now(),
            report: CellSpeedtest {
                pubkey: hotspot.clone(),
                serial: "test-serial-1".to_string(),
                timestamp: parse_dt("2024-01-01 01:00:00"),
                upload_speed: mbps(3),    // Poor tier
                download_speed: mbps(35), // Poor tier
                latency: 80,              // Poor tier
            },
        },
        // Invalid speedtest - upload exceeds 300Mbits, should NOT be included
        // If included, this would push toward Good tier due to very high speeds
        CellSpeedtestIngestReport {
            received_timestamp: Utc::now(),
            report: CellSpeedtest {
                pubkey: hotspot.clone(),
                serial: "test-serial-2".to_string(),
                timestamp: parse_dt("2024-01-01 02:00:00"),
                upload_speed: mbps(900),   // Invalid (way above limit)
                download_speed: mbps(150), // Good tier
                latency: 20,               // Good tier
            },
        },
        // Another valid speedtest - should be included in average
        // Upload: 4 Mbps = Poor tier (≥2 but <5)
        // Download: 40 Mbps = Poor tier (≥30 but <50)
        // Latency: 90ms = Poor tier (≥75 but <100)
        CellSpeedtestIngestReport {
            received_timestamp: Utc::now(),
            report: CellSpeedtest {
                pubkey: hotspot.clone(),
                serial: "test-serial-3".to_string(),
                timestamp: parse_dt("2024-01-01 03:00:00"),
                upload_speed: mbps(4),    // Poor tier
                download_speed: mbps(40), // Poor tier
                latency: 90,              // Poor tier
            },
        },
        // Invalid speedtest - download exceeds 300Mbits, should NOT be included
        // If included, this would push toward Good tier due to very high speeds
        CellSpeedtestIngestReport {
            received_timestamp: Utc::now(),
            report: CellSpeedtest {
                pubkey: hotspot.clone(),
                serial: "test-serial-4".to_string(),
                timestamp: parse_dt("2024-01-01 04:00:00"),
                upload_speed: mbps(15),    // 15 Mbps - would be Good tier
                download_speed: mbps(900), // invalid (way above limit)
                latency: 30,               // Would be Good tier
            },
        },
    ];

    let stream = file_info_stream(speedtests);

    // Drop the daemon when it's done running to close the channel
    {
        let daemon = SpeedtestDaemon::new(
            pool,
            gateway_info_resolver,
            rx,
            speedtest_avg_client,
            verified_client,
        );

        daemon.process_file(stream).await?;
    }

    let avgs = speedtest_avg_receiver.finish().await?;

    // Should have 2 average entries (one for each valid speedtest)
    // Invalid speedtests with speeds > 300Mbits should NOT generate averages
    assert_eq!(
        2,
        avgs.len(),
        "Only valid speedtests should generate averages"
    );

    // Verify the averages only include valid speedtests
    // Expected averages based on the two valid speedtests:
    // Upload: (3 Mbps + 4 Mbps) / 2 = 3.5 Mbps = Poor tier
    // Download: (35 Mbps + 40 Mbps) / 2 = 37.5 Mbps = Poor tier
    // Latency: (80ms + 90ms) / 2 = 85ms = Poor tier
    // Result: Poor tier = 0.25 multiplier
    let expected_upload_avg = mbps(3) + mbps(4); // Sum before division in the code
    let expected_upload_avg = expected_upload_avg / 2; // (3.5 Mbps)
    let expected_download_avg = mbps(35) + mbps(40); // Sum before division in the code
    let expected_download_avg = expected_download_avg / 2; // (37.5 Mbps)

    // Check the last average (which includes both valid speedtests)
    let last_avg = &avgs[1];
    assert_eq!(
        expected_upload_avg, last_avg.upload_speed_avg_bps,
        "Upload average should only include valid speedtests"
    );
    assert_eq!(
        expected_download_avg, last_avg.download_speed_avg_bps,
        "Download average should only include valid speedtests"
    );

    // Most importantly: verify that the reward multiplier is 0.25 (Poor tier)
    // If invalid speedtests were included, the multiplier would be much higher
    assert_eq!(
        0.25, last_avg.reward_multiplier,
        "Reward multiplier should be 0.25 (Poor tier) based only on valid speedtests"
    );

    Ok(())
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
    mbps * BYTES_PER_MEGABIT
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
