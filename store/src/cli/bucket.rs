use crate::{cli::print_json, datetime_from_naive, Error, FileInfo, FileStore, FileType, Result};
use chrono::NaiveDateTime;
use futures::{
    stream::{self, StreamExt},
    TryFutureExt,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
    /// The (optional) file type to search for
    #[clap(long)]
    file_type: Option<FileType>,
}

impl FileFilter {
    async fn list(&self, store: &FileStore, bucket: &str) -> Result<Vec<FileInfo>> {
        store
            .list(
                bucket,
                self.file_type,
                self.after.map(datetime_from_naive),
                self.before.map(datetime_from_naive),
            )
            .await
    }
}

/// List keys in a given bucket
#[derive(Debug, clap::Args)]
pub struct List {
    bucket: String,
    #[clap(flatten)]
    filter: FileFilter,
}

impl List {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let file_infos = self.filter.list(&store, &self.bucket).await?;
        print_json(&file_infos)
    }
}

/// Put one or more files in a given bucket
#[derive(Debug, clap::Args)]
pub struct Put {
    /// The bucket to put files into
    bucket: String,
    /// The files to upload to the bucket
    files: Vec<PathBuf>,
}

impl Put {
    pub async fn run(&self) -> Result {
        let file_store = FileStore::from_env().await?;
        for file in self.files.iter() {
            file_store.put(&self.bucket, file).await?;
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
            file_store.remove(&self.bucket, key).await?;
        }
        Ok(())
    }
}

/// Get one or more files from a given bucket to a given folder
#[derive(Debug, clap::Args)]
pub struct Get {
    /// The bucket to fetch files from
    bucket: String,
    /// The target folder to download files to
    dest: PathBuf,
    #[clap(flatten)]
    filter: FileFilter,
}

impl Get {
    pub async fn run(&self) -> Result {
        let store = FileStore::from_env().await?;
        let file_infos = self.filter.list(&store, &self.bucket).await?;
        let results = Arc::new(Mutex::new(Vec::with_capacity(file_infos.len())));
        stream::iter(&file_infos)
            .map(|info| (store.clone(), info, results.clone()))
            .for_each_concurrent(5, |(store, info, results)| async move {
                let result = fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&self.dest.join(Path::new(&info.key)))
                    .map_err(Error::from)
                    .and_then(|mut file| {
                        store
                            .get(&self.bucket, &info.key)
                            .and_then(|mut stream| async move {
                                tokio::io::copy(&mut stream, &mut file)
                                    .map_err(Error::from)
                                    .await
                            })
                    })
                    .await;
                {
                    let mut results = results.lock().unwrap();
                    results.push((info, result));
                }
            })
            .await;
        let results = results.lock().unwrap();
        let errors: Vec<serde_json::Value> = results
            .iter()
            .filter_map(|(info, result)| {
                result.as_ref().err().map(|err| {
                    json!({
                        "key": info.key,
                        "error": format!("{err:?}"),
                    })
                })
            })
            .collect();
        let success = results.iter().filter(|(_, result)| result.is_ok()).count();
        let json = json!({
            "count": file_infos.len(),
            "sucess": success,
            "errors": errors,
        });
        print_json(&json)
    }
}
