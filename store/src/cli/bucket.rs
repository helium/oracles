use crate::{cli::print_json, datetime_from_naive, FileStore, FileType, Result};
use chrono::NaiveDateTime;
use std::path::PathBuf;

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
        }
    }
}

/// List keys in a given bucket
#[derive(Debug, clap::Args)]
pub struct List {
    bucket: String,
    #[clap(long)]
    after: Option<NaiveDateTime>,
    #[clap(long)]
    before: Option<NaiveDateTime>,
    #[clap(long)]
    file_type: Option<FileType>,
}

impl List {
    pub async fn run(&self) -> Result {
        let file_store = FileStore::from_env().await?;
        let file_infos = file_store
            .list(
                &self.bucket,
                self.file_type.clone(),
                self.after.map(datetime_from_naive),
                self.before.map(datetime_from_naive),
            )
            .await?;
        print_json(&file_infos)
    }
}

/// Put one or more files in a given bucket
#[derive(Debug, clap::Args)]
pub struct Put {
    bucket: String,
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
    bucket: String,
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
