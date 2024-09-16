use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use file_store::file_upload::FileUpload;
use helium_proto::ServiceProvider;
use humantime_serde::re::humantime::format_duration;
use promotion_fund::{make_promotion_fund_file_sink, settings::Settings, state::State};
use solana::carrier::SolanaRpc;
use task_manager::TaskManager;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(short, long)]
    config: Option<PathBuf>,
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Fetch current values from Solana and output a file to S3
    ///
    /// A file will be output regardless of how recently another file was
    /// written to S3.
    WriteSolana,
    /// Print the current values from S3
    PrintS3,
    /// Check Solana for new values every `solana_check_interval`
    ///
    /// When the values from Solana do not match the latest values in S3, a new
    /// S3 file will be output.
    Server,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let settings = Settings::new(cli.config).context("reading settings")?;
    poc_metrics::start_metrics(&settings.metrics)?;

    match cli.cmd {
        Cmd::WriteSolana => write_solana(&settings).await?,
        Cmd::PrintS3 => print_s3(&settings).await?,
        Cmd::Server => run_server(&settings).await?,
    };

    Ok(())
}

async fn run_server(settings: &Settings) -> Result<()> {
    let (upload, upload_server) = FileUpload::from_settings_tm(&settings.file_store_output).await?;
    let (promotion_funds_sink, promotion_fund_server) =
        make_promotion_fund_file_sink(settings, upload).await?;

    let s3_current = State::fetch_s3_bps(&settings.file_store_output).await?;
    let solana_client = SolanaRpc::new(&settings.solana).context("making solana client")?;
    let check_timer = tokio::time::interval(settings.solana_check_interval);
    let state = State::new(
        s3_current,
        solana_client,
        promotion_funds_sink,
        Some(check_timer),
    );

    tracing::info!(
        check_interval = %format_duration(settings.solana_check_interval),
        metrics = %settings.metrics.endpoint,
        "starting promotion_fund server"
    );

    TaskManager::builder()
        .add_task(upload_server)
        .add_task(promotion_fund_server)
        .add_task(state)
        .build()
        .start()
        .await
}

async fn print_s3(settings: &Settings) -> Result<()> {
    let s3_current = State::fetch_s3_bps(&settings.file_store_output).await?;
    if s3_current.is_empty() {
        tracing::warn!("nothing read from s3");
    }
    for (sp_int, bps) in s3_current.iter() {
        let sp = ServiceProvider::try_from(*sp_int);
        tracing::info!(?sp, bps);
    }

    Ok(())
}

async fn write_solana(settings: &Settings) -> Result<()> {
    let (trigger, listener) = triggered::trigger();
    let (upload, upload_server) = FileUpload::from_settings_tm(&settings.file_store_output).await?;
    let (promotion_funds_sink, promotion_fund_server) =
        make_promotion_fund_file_sink(settings, upload).await?;

    let handle = tokio::spawn(async move {
        tokio::try_join!(
            upload_server.run(listener.clone()),
            promotion_fund_server.run(listener)
        )
    });

    let solana = SolanaRpc::new(&settings.solana).context("making solana client")?;
    let mut state = State::new(Default::default(), solana, promotion_funds_sink, None);
    state.handle_tick().await?;
    // allow time for the upload to s3
    tokio::time::sleep(Duration::from_secs(5)).await;

    trigger.trigger();
    if let Err(err) = handle.await {
        tracing::warn!(?err, "something went wrong");
        return Err(anyhow::Error::from(err));
    }

    Ok(())
}
