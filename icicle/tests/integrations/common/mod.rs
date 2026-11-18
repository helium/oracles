//! Test environment for the routes: a private Iceberg catalog, a private
//! bucket, and private spool and state directories per test.
//!
//! # One runtime for every test
//!
//! The route connects to its bucket from `BucketSettings`, and `file_store`
//! caches the S3 client it builds, keyed by endpoint and credentials, for the
//! life of the process. Every test here uses the same RustFS, so they all get
//! the same cached client, and a client is tied to the Tokio runtime that
//! built it. Under `cargo test`, each `#[tokio::test]` has its own runtime,
//! so the second test would get a client whose runtime is gone. The tests
//! therefore share one runtime through [`run_test`].

use chrono::{DateTime, Duration, Utc};
use file_store::{aws_local::AwsLocal, BucketSettings};
use glacon_helium::{
    task_manager::ManagedTask, FileStoreInputSettings, IcebergOutputSettings, RouteSettings,
};
use helium_iceberg::IcebergTestHarness;
use helium_proto::services::chain_rewardable_entities::{
    HeliumPubKey, MobileHotspotChangeReportV1, MobileHotspotChangeReqV1, MobileHotspotChangeV1,
    MobileHotspotDeviceType, MobileHotspotMetadata, SolanaPubKey,
};
use icicle::{data::mobile_hotspot_history, settings::FreshStateSetting};
use std::{collections::BTreeMap, future::Future, sync::LazyLock};
use trino_rust_client::Trino;

/// Region and credentials `AwsLocal` uses for its buckets.
const S3_REGION: &str = "us-east-1";
const S3_CREDENTIAL: &str = "admin";

/// Polaris answers 500 when several catalogs are created at once.
const CATALOG_ATTEMPTS: u32 = 5;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("test runtime")
});

/// Run one test on the runtime every test shares. See the module docs.
pub fn run_test<F>(test: F) -> anyhow::Result<()>
where
    F: Future<Output = anyhow::Result<()>>,
{
    RUNTIME.block_on(test)
}

pub struct Harness {
    pub iceberg: IcebergTestHarness,
    bucket: AwsLocal,
    spool_root: tempfile::TempDir,
    state_root: tempfile::TempDir,
}

impl Harness {
    pub async fn new() -> anyhow::Result<Self> {
        let mut attempt = 0;
        let iceberg = loop {
            attempt += 1;
            match IcebergTestHarness::new().await {
                Ok(iceberg) => break iceberg,
                Err(err) if attempt < CATALOG_ATTEMPTS => {
                    tracing::warn!(attempt, %err, "creating test catalog failed, retrying");
                    tokio::time::sleep(humantime::parse_duration("250ms")? * attempt).await;
                }
                Err(err) => return Err(err.into()),
            }
        };

        let bucket = AwsLocal::new().await;
        bucket.create_bucket().await?;

        Ok(Self {
            iceberg,
            bucket,
            spool_root: tempfile::tempdir()?,
            state_root: tempfile::tempdir()?,
        })
    }

    /// Upload one gzipped, length-delimited
    /// `mobile_hotspot_change_report.<epoch_ms>.gz` file.
    pub async fn put_file(
        &self,
        timestamp: DateTime<Utc>,
        reports: Vec<MobileHotspotChangeReportV1>,
    ) -> anyhow::Result<String> {
        Ok(self
            .bucket
            .put_protos_at_time(mobile_hotspot_history::FILE_PREFIX, reports, timestamp)
            .await?)
    }

    /// The route's settings as deployed -- the `chain` input from the epoch,
    /// the default `chain.mobile_hotspot_history` table -- pointed at this
    /// test's bucket, catalog and directories. The input polls every 100ms
    /// and stops after 2s without new files, so the route ends on its own.
    pub fn settings(&self) -> anyhow::Result<mobile_hotspot_history::Settings> {
        let input = FileStoreInputSettings {
            bucket: BucketSettings {
                bucket: self.bucket.bucket().to_string(),
                settings: file_store::Settings {
                    region: Some(S3_REGION.to_string()),
                    endpoint: Some(self.bucket.endpoint().to_string()),
                    access_key_id: Some(S3_CREDENTIAL.to_string()),
                    secret_access_key: Some(S3_CREDENTIAL.to_string()),
                },
            },
            start_after: DateTime::UNIX_EPOCH,
            poll_duration: Some(humantime::parse_duration("100ms")?),
            offset: None,
            idle_timeout: Some(humantime::parse_duration("2s")?),
        };

        Ok(mobile_hotspot_history::Settings {
            // Stable for the life of the harness, so the route can be
            // restarted on it.
            route: RouteSettings::new(self.state_root.path().join(mobile_hotspot_history::ROUTE)),
            inputs: BTreeMap::from([(mobile_hotspot_history::INPUT.to_string(), input)]),
            table: Default::default(),
            output: IcebergOutputSettings {
                catalog: self.iceberg.as_settings(),
                spool_dir: self.spool_root.path().to_path_buf(),
                max_batch_rows: 10_000,
                batch_timeout: humantime::parse_duration("1s")?,
            },
            fresh_state: FreshStateSetting::Refuse,
        })
    }

    /// Run the real route, as `icicle server` builds it, until its input idles
    /// out.
    pub async fn run_route(&self) -> anyhow::Result<()> {
        run(mobile_hotspot_history::route(&self.settings()?)?).await
    }

    /// Delete the bucket: `AwsLocal` cannot clean up on drop.
    pub async fn cleanup(self) -> anyhow::Result<()> {
        self.bucket.cleanup().await?;
        Ok(())
    }
}

/// Run a route until its inputs idle out, with a hard timeout.
async fn run(task: impl ManagedTask + 'static) -> anyhow::Result<()> {
    let (_trigger, shutdown) = triggered::trigger();
    tokio::time::timeout(
        humantime::parse_duration("2 minutes")?,
        Box::new(task).start_task(shutdown),
    )
    .await??;
    Ok(())
}

// ---------------------------------------------------------------- fixtures

pub fn minutes_ago(minutes: i64) -> DateTime<Utc> {
    Utc::now() - Duration::minutes(minutes)
}

/// A report for the hotspot whose key bytes are all `key_byte`, asserted at
/// `asserted_hex` (empty for an unasserted hotspot).
pub fn report(key_byte: u8, asserted_hex: &str, at: DateTime<Utc>) -> MobileHotspotChangeReportV1 {
    MobileHotspotChangeReportV1 {
        received_timestamp_ms: at.timestamp_millis() as u64,
        report: Some(MobileHotspotChangeReqV1 {
            change: Some(MobileHotspotChangeV1 {
                block: 42,
                timestamp_seconds: at.timestamp() as u64,
                pub_key: Some(HeliumPubKey {
                    value: vec![key_byte; 33],
                }),
                asset: Some(SolanaPubKey {
                    value: vec![key_byte; 32],
                }),
                metadata: Some(MobileHotspotMetadata {
                    serial_number: format!("SN-{key_byte}"),
                    device_type: MobileHotspotDeviceType::WifiIndoor as i32,
                    asserted_hex: asserted_hex.to_string(),
                    azimuth: 180,
                }),
            }),
            signer: "test-signer".to_string(),
            signature: vec![],
        }),
    }
}

// ---------------------------------------------------------------- reading

/// One row of the table, the columns the tests assert on.
#[derive(Debug, serde::Deserialize, serde::Serialize, Trino)]
pub struct HistoryRow {
    pub file_ts: i64,
    pub record_index: i64,
    pub received_ms: i64,
    pub block: i64,
    pub timestamp_s: i64,
    pub pub_key: String,
    pub animal_name: Option<String>,
    pub asset: String,
    pub serial_number: Option<String>,
    pub device_type: String,
    pub asserted_hex: Option<String>,
    pub asserted_lat: Option<f64>,
    pub asserted_lon: Option<f64>,
    pub azimuth: Option<i32>,
    pub signer: String,
}

pub fn table() -> String {
    format!(
        "{}.{}",
        mobile_hotspot_history::DEFAULT_NAMESPACE,
        mobile_hotspot_history::DEFAULT_TABLE_NAME
    )
}

/// Every row, in the order it was written.
pub async fn rows(h: &Harness) -> anyhow::Result<Vec<HistoryRow>> {
    Ok(h.iceberg
        .trino()
        .get_all::<HistoryRow>(format!(
            // Milliseconds, exactly: `CAST(to_unixtime(..) AS BIGINT)` goes
            // through a double and rounds a sub-second timestamp.
            "SELECT file_ts, record_index, \
                    date_diff('millisecond', from_unixtime(0), received_timestamp) AS received_ms, \
                    block, \
                    date_diff('second', from_unixtime(0), timestamp) AS timestamp_s, \
                    pub_key, animal_name, asset, serial_number, device_type, asserted_hex, \
                    asserted_lat, asserted_lon, azimuth, signer \
             FROM {} ORDER BY file_ts, record_index",
            table()
        ))
        .await?
        .into_vec())
}
