use file_store::{traits::MsgDecode, Client, FileInfoStream, Settings};
use file_store_oracles::{
    iot_beacon_report::IotBeaconIngestReport,
    iot_valid_poc::IotPoc,
    iot_witness_report::IotWitnessIngestReport,
    mobile_radio_invalidated_threshold::VerifiedInvalidatedRadioThresholdIngestReport,
    mobile_radio_threshold::VerifiedRadioThresholdIngestReport,
    speedtest::{cli::SpeedtestAverage, CellSpeedtest},
    FileType,
};

use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::{stream::TryStreamExt, StreamExt};
use helium_crypto::PublicKey;
use serde::{ser::SerializeSeq, Serialize, Serializer};
use std::{
    io,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::fs;

/// Commands on remote buckets
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Configuration file to use.
    #[clap(short = 'c')]
    config: PathBuf,
    #[clap(subcommand)]
    cmd: BucketCmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum BucketCmd {
    Ls(List),
    Rm(Remove),
    Put(Put),
    Get(Get),
    Locate(Locate),
}

impl Cmd {
    pub async fn run(&self) -> anyhow::Result<()> {
        let settings = Settings::new(&self.config)?;
        self.cmd.run(&settings).await
    }
}

impl BucketCmd {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        match self {
            Self::Ls(cmd) => cmd.run(settings).await,
            Self::Rm(cmd) => cmd.run(settings).await,
            Self::Put(cmd) => cmd.run(settings).await,
            Self::Get(cmd) => cmd.run(settings).await,
            Self::Locate(cmd) => cmd.run(settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct FileFilter {
    /// Optional start time to look for (inclusive). Defaults to the oldest
    /// timestamp in the bucket.
    #[clap(long)]
    pub after: Option<NaiveDateTime>,
    /// Optional end time to look for (exclusive). Defaults to the latest
    /// available timestamp in the bucket.
    #[clap(long)]
    pub before: Option<NaiveDateTime>,
    /// The file type prefix to search for
    #[clap(long)]
    pub prefix: String,
}

impl FileFilter {
    fn list(&self, client: &Client, bucket: &str) -> FileInfoStream {
        file_store::list_files(
            client,
            bucket,
            &self.prefix,
            self.after.as_ref().map(|dt| Utc.from_utc_datetime(dt)),
            self.before.as_ref().map(|dt| Utc.from_utc_datetime(dt)),
        )
    }
}

/// List keys in a given bucket
#[derive(Debug, clap::Args)]
pub struct List {
    #[clap(short)]
    bucket: String,
    #[clap(flatten)]
    filter: FileFilter,
}

impl List {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let client = settings.connect().await;
        let mut file_infos = self.filter.list(&client, &self.bucket);
        let mut ser = serde_json::Serializer::new(io::stdout());
        let mut seq = ser.serialize_seq(None)?;
        while let Some(info) = file_infos.try_next().await? {
            seq.serialize_element(&info)?;
        }
        seq.end()?;
        Ok(())
    }
}

/// Put one or more files in a given bucket
#[derive(Debug, clap::Args)]
pub struct Put {
    #[clap(short)]
    bucket: String,
    /// The files to upload to the bucket
    files: Vec<PathBuf>,
}

impl Put {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let client = settings.connect().await;
        for file in self.files.iter() {
            file_store::put_file(&client, &self.bucket, file).await?;
        }
        Ok(())
    }
}

/// Remove one or more keys from a given bucket
#[derive(Debug, clap::Args)]
pub struct Remove {
    /// The bucket to remove from
    bucket: String,
    /// The keys in the bucket to remove
    keys: Vec<String>,
}

impl Remove {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let client = settings.connect().await;
        for key in self.keys.iter() {
            file_store::remove_file(&client, &self.bucket, key).await?;
        }
        Ok(())
    }
}

/// Get one or more files from a given bucket to a given folder
#[derive(Debug, clap::Args)]
pub struct Get {
    bucket: String,
    /// The target folder to download files to
    dest: PathBuf,
    #[clap(flatten)]
    filter: FileFilter,
}

impl Get {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let client = settings.connect().await;
        let file_infos = self.filter.list(&client, &self.bucket);
        file_infos
            .map_ok(|info| (client.clone(), self.bucket.clone(), info))
            .try_for_each_concurrent(5, |(client, bucket, info)| async move {
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.dest.join(Path::new(&info.key)))
                    .await?;

                let stream = file_store::get_raw_file(&client, &bucket, &info.key).await?;
                let mut reader = tokio::io::BufReader::new(stream.into_async_read());
                tokio::io::copy(&mut reader, &mut file).await?;

                Ok(())
            })
            .await?;
        Ok(())
    }
}

/// Locate specific records in a time range
#[derive(Debug, clap::Args)]
pub struct Locate {
    bucket: String,
    gateway: PublicKey,

    #[clap(flatten)]
    filter: FileFilter,
}

impl Locate {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        let client = settings.connect().await;
        let file_infos = self.filter.list(&client, &self.bucket);

        let mut events = file_store::source_files(&client, &self.bucket, file_infos);

        let mut ser = serde_json::Serializer::new(io::stdout());
        let mut seq = ser.serialize_seq(None)?;

        while let Some(buf) = events.next().await {
            let buf = buf?;
            if let Ok(Some(val)) = locate(&self.filter.prefix, &self.gateway, &buf) {
                seq.serialize_element(&val)?
            }
        }

        seq.end()?;
        Ok(())
    }
}

fn locate(
    prefix: &str,
    gateway: &PublicKey,
    buf: &[u8],
) -> anyhow::Result<Option<serde_json::Value>> {
    let pub_key = gateway.to_vec();

    match FileType::from_str(prefix)? {
        FileType::SpeedtestAvg => SpeedtestAverage::decode_to_value_if(buf, pub_key),
        FileType::CellSpeedtest => CellSpeedtest::decode_to_value_if(buf, pub_key),
        FileType::IotBeaconIngestReport => IotBeaconIngestReport::decode_to_value_if(buf, pub_key),
        FileType::IotWitnessIngestReport => {
            IotWitnessIngestReport::decode_to_value_if(buf, pub_key)
        }
        FileType::VerifiedRadioThresholdIngestReport => {
            VerifiedRadioThresholdIngestReport::decode_to_value_if(buf, pub_key)
        }
        FileType::VerifiedInvalidatedRadioThresholdIngestReport => {
            VerifiedInvalidatedRadioThresholdIngestReport::decode_to_value_if(buf, pub_key)
        }

        FileType::IotPoc => IotPoc::decode_to_value_if(buf, pub_key),
        _ => Ok(None),
    }
}

trait ToValue {
    fn decode_to_value_if(
        buf: &[u8],
        gateway: Vec<u8>,
    ) -> anyhow::Result<Option<serde_json::Value>>;
}

macro_rules! impl_to_value {
    ($type:ty, $($path:tt)+) => {
        impl ToValue for $type {
            fn decode_to_value_if(buf: &[u8], gateway: Vec<u8>) -> anyhow::Result<Option<serde_json::Value>> {
                let event = Self::decode(buf).map_err(anyhow::Error::from)?;

                if event.$($path)+.as_ref() == gateway {
                    let val = event.serialize(serde_json::value::Serializer)?;
                    Ok(Some(val))
                } else {
                    Ok(None)
                }
            }
        }
    };
}

impl_to_value!(CellSpeedtest, pubkey);
impl_to_value!(IotBeaconIngestReport, report.pub_key);
impl_to_value!(IotWitnessIngestReport, report.pub_key);
impl_to_value!(IotPoc, beacon_report.report.pub_key);
impl_to_value!(SpeedtestAverage, pub_key);
impl_to_value!(
    VerifiedRadioThresholdIngestReport,
    report.report.hotspot_pubkey
);
impl_to_value!(
    VerifiedInvalidatedRadioThresholdIngestReport,
    report.report.hotspot_pubkey
);
