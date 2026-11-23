//! The command line: `icicle -c <settings.toml> server`, the shape the
//! workspace `Dockerfile` runs every oracle with.

pub mod server;

use crate::{metrics, Settings};
use anyhow::Context;
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(about = "Routes chain change reports from S3 into Iceberg", version)]
pub struct Cli {
    /// Path to the settings file. Every key can also be given as an
    /// `ICICLE__*` environment variable.
    #[clap(short = 'c', long)]
    pub config: Option<PathBuf>,

    #[clap(subcommand)]
    pub cmd: Cmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    /// Run the configured routes until shut down.
    Server(server::Server),
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        let settings = Settings::new(self.config.as_ref()).context("loading settings")?;
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        // Secrets (catalog credential and token, S3 and bucket keys) are
        // `skip_serializing` in helium_iceberg and file_store.
        tracing::info!("Settings: {}", serde_json::to_string_pretty(&settings)?);
        metrics::start(&settings.metrics)?;

        match self.cmd {
            Cmd::Server(cmd) => cmd.run(&settings).await,
        }
    }
}
