use crate::{
    datetime_from_naive, heartbeat::CellHeartbeat, lora_beacon_report::LoraBeaconIngestReport,
    lora_valid_poc::LoraValidPoc, lora_witness_report::LoraWitnessIngestReport,
    speedtest::CellSpeedtest, traits::MsgDecode, Error, FileInfoStream, FileStore, FileType,
    Result,
};
use chrono::NaiveDateTime;
use futures::{stream::TryStreamExt, StreamExt, TryFutureExt};
use helium_crypto::PublicKey;
use serde::{ser::SerializeSeq, Serializer};
use std::{
    io,
    path::{Path, PathBuf},
};
use tokio::fs;

/// Commands on remote buckets
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: BucketCmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum BucketCmd {
    Ls(List),
    Rm(Remove),
    Put(Put),
    Get(Get),
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

impl BucketCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::Ls(cmd) => cmd.run().await,
            Self::Rm(cmd) => cmd.run().await,
            Self::Put(cmd) => cmd.run().await,
            Self::Get(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, clap::Args)]
struct FileFilter {
    /// Optional start time to look for (inclusive). Defaults to the oldest
    /// timestamp in the bucket.
    #[clap(long)]
    after: Option<NaiveDateTime>,
    /// Optional end time to look for (exclusive). Defaults to the latest
    /// available timestamp in the bucket.
    #[clap(long)]
    before: Option<NaiveDateTime>,
    /// The file type to search for
    #[clap(long)]
    file_type: FileType,
}

impl FileFilter {
    fn list(&self, store: &FileStore) -> FileInfoStream {
        store.list(
            self.file_type,
            self.after.map(datetime_from_naive),
            self.before.map(datetime_from_naive),
        )
    }
}

/// List keys in a given bucket
#[derive(Debug, clap::Args)]
pub struct List {
    #[clap(flatten)]
    filter: FileFilter,
}

impl List {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let mut file_infos = self.filter.list(&store);
        let mut ser = serde_json::Serializer::new(io::stdout());
        let mut seq = ser.serialize_seq(None)?;
        while let Some(info) = file_infos.try_next().await? {
            seq.serialize_element(&info)?;
        }
        seq.end()?;
        Ok(())
        // print_json(&file_infos)
    }
}

/// Put one or more files in a given bucket
#[derive(Debug, clap::Args)]
pub struct Put {
    /// The files to upload to the bucket
    files: Vec<PathBuf>,
}

impl Put {
    pub async fn run(&self) -> Result {
        let file_store = FileStore::from_env().await?;
        for file in self.files.iter() {
            file_store.put(file).await?;
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
    pub async fn run(&self) -> Result {
        let file_store = FileStore::from_env().await?;
        for key in self.keys.iter() {
            file_store.remove(key).await?;
        }
        Ok(())
    }
}

/// Get one or more files from a given bucket to a given folder
#[derive(Debug, clap::Args)]
pub struct Get {
    /// The target folder to download files to
    dest: PathBuf,
    #[clap(flatten)]
    filter: FileFilter,
}

impl Get {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let file_infos = self.filter.list(&store);
        file_infos
            .map_ok(|info| (store.clone(), info))
            .try_for_each_concurrent(5, |(store, info)| async move {
                fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&self.dest.join(Path::new(&info.key)))
                    .map_err(Error::from)
                    .and_then(|mut file| {
                        store.get(&info.key).and_then(|stream| async move {
                            let mut reader = tokio_util::io::StreamReader::new(stream);
                            tokio::io::copy(&mut reader, &mut file)
                                .map_err(Error::from)
                                .await
                        })
                    })
                    .map_ok(|_| ())
                    .await
            })
            .await?;
        Ok(())
    }
}

/// Locate specific records in a time range
#[derive(Debug, clap::Args)]
pub struct Locate {
    gateway: PublicKey,

    #[clap(flatten)]
    filter: FileFilter,
}

impl Locate {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let file_infos = self.filter.list(&store);
        let file_type = self.filter.file_type;
        let gateway = &self.gateway.clone();
        let mut events = store
            .source(file_infos)
            .map_ok(|buf| (buf, gateway))
            .try_filter_map(|(buf, gateway)| async move { locate(file_type, gateway, &buf) })
            .boxed();
        let mut ser = serde_json::Serializer::new(io::stdout());
        let mut seq = ser.serialize_seq(None)?;
        while let Some(event) = events.try_next().await? {
            seq.serialize_element(&event)?;
        }
        seq.end()?;
        Ok(())
    }
}

fn locate(
    file_type: FileType,
    gateway: &PublicKey,
    buf: &[u8],
) -> Result<Option<serde_json::Value>> {
    match file_type {
        FileType::CellHeartbeat => {
            CellHeartbeat::decode(buf).and_then(|event| event.to_value_if(gateway))
        }
        FileType::CellSpeedtest => {
            CellSpeedtest::decode(buf).and_then(|event| event.to_value_if(gateway))
        }
        FileType::LoraBeaconIngestReport => {
            LoraBeaconIngestReport::decode(buf).and_then(|event| event.to_value_if(gateway))
        }
        FileType::LoraWitnessIngestReport => {
            LoraWitnessIngestReport::decode(buf).and_then(|event| event.to_value_if(gateway))
        }
        FileType::LoraValidPoc => {
            LoraValidPoc::decode(buf).and_then(|event| event.to_value_if(gateway))
        }
        FileType::Entropy => Ok(None),
        _ => Ok(None),
    }
}

trait ToValue {
    fn to_value(self) -> Result<serde_json::Value>
    where
        Self: serde::Serialize;

    fn to_value_if(self, gateway: &PublicKey) -> Result<Option<serde_json::Value>>
    where
        Self: Gateway,
        Self: serde::Serialize + Sized,
    {
        (self.pubkey() == gateway)
            .then(|| self.to_value())
            .transpose()
    }
}

trait Gateway {
    fn pubkey(&self) -> &PublicKey;
}

impl<T> ToValue for T
where
    T: serde::Serialize,
{
    fn to_value(self) -> Result<serde_json::Value>
    where
        Self: serde::Serialize,
    {
        self.serialize(serde_json::value::Serializer)
            .map_err(Error::from)
    }
}

impl Gateway for CellHeartbeat {
    fn pubkey(&self) -> &PublicKey {
        &self.pubkey
    }
}

impl Gateway for CellSpeedtest {
    fn pubkey(&self) -> &PublicKey {
        &self.pubkey
    }
}

impl Gateway for LoraBeaconIngestReport {
    fn pubkey(&self) -> &PublicKey {
        &self.report.pub_key
    }
}

impl Gateway for LoraWitnessIngestReport {
    fn pubkey(&self) -> &PublicKey {
        &self.report.pub_key
    }
}

impl Gateway for LoraValidPoc {
    fn pubkey(&self) -> &PublicKey {
        &self.beacon_report.report.pub_key
    }
}
