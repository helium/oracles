use crate::common;
use chrono::{DateTime, NaiveDateTime, Utc};
use file_store::{file_info_poller::FileInfoStream, FileInfo};
use file_store_oracles::speedtest::{CellSpeedtest, CellSpeedtestIngestReport};
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::SpeedtestVerificationResult as SpeedtestResult;
use mobile_verifier::speedtests::{SpeedtestDaemon, BYTES_PER_MEGABIT};
use mobile_verifier::{iceberg, GatewayResolution, GatewayResolver};
use sqlx::{Pool, Postgres};

#[derive(Clone)]
struct MockGatewayInfoResolver {}

#[async_trait::async_trait]
impl GatewayResolver for MockGatewayInfoResolver {
    async fn resolve_gateway(
        &self,
        _address: &PublicKeyBinary,
        _gateway_query_timestamp: &DateTime<Utc>,
    ) -> anyhow::Result<GatewayResolution> {
        // On-chain, non-data-only: speedtest verification treats this as a valid
        // device type (the previous mock returned a CBRS gateway).
        Ok(GatewayResolution::GatewayNotAsserted)
    }
}

#[sqlx::test]
async fn speedtest_upload_exceeds_300megabits_ps_limit(pool: Pool<Postgres>) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        gateway_info_resolver,
        rx,
        verified_client,
        poc_writers.speedtest,
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

    let result = daemon.validate_speedtest(&speedtest_report).await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_download_exceeds_300_megabits_ps_limit(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        gateway_info_resolver,
        rx,
        verified_client,
        poc_writers.speedtest,
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

    let result = daemon.validate_speedtest(&speedtest_report).await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_both_speeds_exceed_300_megabits_ps_limit(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        gateway_info_resolver,
        rx,
        verified_client,
        poc_writers.speedtest,
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

    let result = daemon.validate_speedtest(&speedtest_report).await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValueOutOfBounds);

    Ok(())
}

#[sqlx::test]
async fn speedtest_within_300_megabits_ps_limit_should_be_valid(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        gateway_info_resolver,
        rx,
        verified_client,
        poc_writers.speedtest,
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

    let result = daemon.validate_speedtest(&speedtest_report).await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValid);

    Ok(())
}

#[sqlx::test]
async fn speedtest_exactly_300_megabits_ps_limit_should_be_valid(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let gateway_info_resolver = MockGatewayInfoResolver {};
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        gateway_info_resolver,
        rx,
        verified_client,
        poc_writers.speedtest,
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

    let result = daemon.validate_speedtest(&speedtest_report).await?;
    assert_eq!(result, SpeedtestResult::SpeedtestValid);

    Ok(())
}

#[sqlx::test]
async fn only_valid_speedtests_reach_the_speedtests_table(
    pool: Pool<Postgres>,
) -> anyhow::Result<()> {
    // `hotspots.speedtests` is the only input to the dbt `speedtest_averages`
    // model, so a rejected report has to stay out of it or it would be averaged.
    let (_tx, rx) = tokio::sync::mpsc::channel(2);
    let (verified_client, _verified_receiver) = common::create_file_sink();

    let hotspot: PublicKeyBinary =
        "112NqN2WWMwtK29PMzRby62fDydBJfsCLkCAf392stdok48ovNT6".parse()?;

    let stream = file_info_stream(vec![
        speedtest(&hotspot, "2024-01-01 01:00:00", 3, 35, 80),
        // Upload exceeds the 300 Mbps bound: rejected.
        speedtest(&hotspot, "2024-01-01 02:00:00", 900, 150, 20),
        speedtest(&hotspot, "2024-01-01 03:00:00", 4, 40, 90),
        // Download exceeds the 300 Mbps bound: rejected.
        speedtest(&hotspot, "2024-01-01 04:00:00", 15, 900, 30),
    ]);

    let harness = common::setup_iceberg().await?;
    let poc_writers = common::poc_writers(&harness).await?;

    let daemon = SpeedtestDaemon::new(
        pool.clone(),
        MockGatewayInfoResolver {},
        rx,
        verified_client,
        poc_writers.speedtest,
    );
    daemon.process_file(stream).await?;

    let mut stored = iceberg::speedtest::get_all(harness.trino()).await?;
    stored.sort_by_key(|s| s.timestamp);
    let timestamps: Vec<_> = stored.iter().map(|s| s.timestamp.to_utc()).collect();
    assert_eq!(
        vec![
            parse_dt("2024-01-01 01:00:00"),
            parse_dt("2024-01-01 03:00:00")
        ],
        timestamps
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
