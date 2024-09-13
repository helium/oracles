use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use helium_proto::ServiceProvider;
use humantime_serde::re::humantime::format_duration;
use promotion_fund::{
    fetch_s3_bps, s3,
    settings::Settings,
    state::{State, StateEvent},
};
use solana::carrier::SolanaRpc;
use tokio::{signal::unix, task::JoinHandle};

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
    FetchSolana,
    /// Print the current values from S3
    ///
    /// Uses the lookback duration from the settings file to show what a Server
    /// started right now would use as it's current values.
    PrintS3,
    /// A single run of the Server
    ///
    /// Fetch and compare S3 to Solana, outputting a file if necessary.
    Compare,
    /// Check Solana for new values every `solana_check_interval`
    ///
    /// When the values from Solana do not match the latest values in S3, or the
    /// lookback time does not return any files, a new S3 file will be output.
    Server,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let settings = Settings::new(cli.config).context("reading settings")?;
    poc_metrics::start_metrics(&settings.metrics)?;

    let (trigger, listener) = triggered::trigger();
    let join_handle = match cli.cmd {
        Cmd::FetchSolana => fetch_solana(&settings, listener).await?,
        Cmd::PrintS3 => fetch_s3(&settings, listener).await?,
        Cmd::Compare => compare_s3_and_solana(&settings, listener).await?,
        Cmd::Server => run_server(&settings, listener).await?,
    };

    trigger.trigger();
    if let Err(err) = join_handle.await {
        tracing::warn!(?err, "something went wrong shutting down");
    }

    Ok(())
}

async fn run_server(
    settings: &Settings,
    listener: triggered::Listener,
) -> Result<JoinHandle<anyhow::Result<()>>> {
    let (mut file_poller, source_handle) =
        s3::start_file_source(settings, listener.clone()).await?;
    let (file_sink, sink_handle) = s3::start_file_sink(settings, listener.clone()).await?;

    // FIXME: Not sure where to put this.
    let mut terminate_signal = unix::signal(unix::SignalKind::terminate())?;

    let s3_current = fetch_s3_bps(&mut file_poller).await?;
    let solana_client = SolanaRpc::new(&settings.solana).context("making solana client")?;
    let mut check_timer = tokio::time::interval(settings.solana_check_interval);
    let mut state = State::new(s3_current, solana_client, file_sink);

    tracing::info!(
        lookback = %format_duration(settings.s3_lookback_duration),
        check_interval = %format_duration(settings.solana_check_interval),
        metrics = %settings.metrics.endpoint,
        "starting promotion_fund server"
    );

    loop {
        let msg = tokio::select! {
            _ = terminate_signal.recv() => StateEvent::Shutdown,
            _ = tokio::signal::ctrl_c() => StateEvent::Shutdown,
            _ = listener.clone() => StateEvent::Shutdown,
            _ = check_timer.tick() => StateEvent::Tick,
            file_info = file_poller.recv() => match file_info {
                Some(file_info) => StateEvent::NewS3(file_info),
                None => StateEvent::Shutdown
            }
        };
        tracing::debug!(?msg);

        match msg {
            StateEvent::Shutdown => break,
            StateEvent::Tick => state.handle_tick().await?,
            StateEvent::NewS3(file_info) => state.handle_new_s3(file_info).await?,
        }
    }

    let join_handle = tokio::spawn(async move {
        if let Err(err) = tokio::try_join!(sink_handle, source_handle) {
            tracing::warn!(?err, "something went wrong");
            return Err(anyhow::Error::from(err));
        };
        Ok(())
    });
    Ok(join_handle)
}

async fn compare_s3_and_solana(
    settings: &Settings,
    listener: triggered::Listener,
) -> Result<JoinHandle<anyhow::Result<()>>> {
    let solana_client = SolanaRpc::new(&settings.solana).context("making solana client")?;
    let (sink, sink_handle) = s3::start_file_sink(settings, listener.clone()).await?;
    let (mut file_poller, source_handle) = s3::start_file_source(settings, listener).await?;

    let s3_current = fetch_s3_bps(&mut file_poller).await?;
    let state = State::new(s3_current, solana_client, sink);
    state.handle_tick().await?;
    // Allow time for s3 upload
    tokio::time::sleep(Duration::from_secs(5)).await;

    let join_handle = tokio::spawn(async move {
        if let Err(err) = tokio::try_join!(sink_handle, source_handle) {
            tracing::warn!(?err, "something went wrong");
            return Err(anyhow::Error::from(err));
        };
        Ok(())
    });

    Ok(join_handle)
}

async fn fetch_s3(
    settings: &Settings,
    listener: triggered::Listener,
) -> Result<JoinHandle<anyhow::Result<()>>> {
    let (mut file_poller, source_handle) = s3::start_file_source(settings, listener)
        .await
        .context("starting file source")?;

    let s3_current = fetch_s3_bps(&mut file_poller).await?;
    if s3_current.is_empty() {
        tracing::warn!(
            lookback_duration = ?settings.s3_lookback_duration,
            "nothing read from s3, maybe increase lookback duration..."
        );
    }
    for (sp_int, bps) in s3_current.iter() {
        let sp = ServiceProvider::try_from(*sp_int);
        tracing::info!(?sp, bps);
    }

    Ok(source_handle)
}

async fn fetch_solana(
    settings: &Settings,
    listener: triggered::Listener,
) -> Result<JoinHandle<anyhow::Result<()>>> {
    let (sink, sink_handle) = s3::start_file_sink(settings, listener).await?;

    let solana = SolanaRpc::new(&settings.solana).context("making solana client")?;
    let state = State::new_without_s3(solana, sink);
    state.handle_tick().await?;
    // allow time for the upload to s3
    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(sink_handle)
}
