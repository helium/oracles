//! `mobile-verifier-compare` — Postgres ↔ Trino reward-query parity CLI for
//! `mobile_verifier`. Each subcommand calls the same SQL function the
//! production reward pipeline runs (via path-dep on `mobile-verifier`),
//! issues the equivalent query against the Iceberg `poc.*`/`rewards.*`
//! tables via Trino, and prints a side-by-side diff.
//!
//! `seed` bootstraps the local docker-compose stack — creates the per-app
//! Postgres databases, runs sqlx migrations, creates the Iceberg tables in
//! Trino, and populates fixture data across four buckets (MATCH / MISMATCH /
//! PG_ONLY / TRINO_ONLY) so the other subcommands have something to diff.
//!
//! Run with no `-c` flag for the seed path (docker-compose defaults baked
//! into clap); supply `-c settings.toml` for everything else.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::ops::Range;
use std::path::PathBuf;

mod data_sessions;
mod diff;
mod heartbeats;
mod payer_burns;
mod pending_burns;
mod seed;
mod settings;
mod speedtests;

pub use settings::Settings;

/// Top-level args shared across every comparison subcommand.
#[derive(Debug, clap::Args)]
pub struct CommonArgs {
    /// Start of the epoch to compare (RFC 3339).
    #[clap(long)]
    pub start: DateTime<Utc>,
    /// End of the epoch to compare (RFC 3339). Exclusive.
    #[clap(long)]
    pub end: DateTime<Utc>,
    /// Print every row, not just mismatches.
    #[clap(long)]
    pub show_all: bool,
    /// Cap the rendered table to N rows (mismatches first). 0 = unlimited.
    #[clap(long, default_value = "0")]
    pub limit: usize,
}

impl CommonArgs {
    pub fn epoch(&self) -> Range<DateTime<Utc>> {
        self.start..self.end
    }
}

#[derive(clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Postgres ↔ Trino reward-query parity diff + local seed bootstrapper")]
struct Cli {
    /// Optional settings file for the diff subcommands. Required for every
    /// subcommand except `seed`, which takes its endpoints via flags.
    #[clap(short = 'c')]
    config: Option<PathBuf>,
    #[clap(subcommand)]
    sub: Sub,
}

#[derive(Debug, clap::Subcommand)]
enum Sub {
    /// Compare HeartbeatReward::validated (PG `wifi_heartbeats` valid_radios.sql)
    /// against an equivalent query over `poc.heartbeats` in Trino.
    Heartbeats(HeartbeatsCmd),
    /// Compare aggregate_epoch_speedtests (PG `speedtests`) against `poc.speedtests`.
    Speedtests(SpeedtestsCmd),
    /// Compare get_latest_speedtests_for_pubkey for a single hotspot.
    LatestSpeedtests(LatestSpeedtestsCmd),
    /// Compare aggregate_hotspot_data_sessions_to_dc (PG hotspot_data_transfer_sessions)
    /// against `rewards.data_transfer` aggregated over the same window.
    DataSessions(DataSessionsCmd),
    /// Compare sum_data_sessions_to_dc_by_payer (PG by payer) against `rewards.data_transfer`.
    PayerBurns(PayerBurnsCmd),
    /// Compare mobile_packet_verifier pending_burns initialize totals (per payer)
    /// against `rewards.data_transfer`. Requires --mpv-database-url.
    PendingBurns(PendingBurnsCmd),
    /// Run every comparison sequentially.
    All(AllCmd),
    /// Bootstrap local Postgres + Iceberg with fixture data so the other
    /// subcommands have something meaningful to diff. Targets the
    /// docker-compose stack by default.
    Seed(seed::Cmd),
}

#[derive(Debug, clap::Args)]
struct HeartbeatsCmd {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct SpeedtestsCmd {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct LatestSpeedtestsCmd {
    /// Hotspot pubkey (base58) to fetch latest speedtests for.
    #[clap(long)]
    pubkey: String,
    /// Anchor timestamp; the query pulls the latest N speedtests ending at this
    /// time (RFC 3339). Defaults to --end.
    #[clap(long)]
    anchor: Option<DateTime<Utc>>,
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct DataSessionsCmd {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct PayerBurnsCmd {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct PendingBurnsCmd {
    /// Postgres URL for the mobile_packet_verifier database (initialize is a
    /// whole-table aggregate so no epoch filter applies). Example:
    /// postgres://user:pass@host:5432/mobile_packet_verifier
    #[clap(long)]
    mpv_database_url: String,
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Debug, clap::Args)]
struct AllCmd {
    /// Optional MPV database URL. If omitted the pending-burns comparison is skipped.
    #[clap(long)]
    mpv_database_url: Option<String>,
    #[clap(flatten)]
    common: CommonArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Seed is config-free: it bootstraps the very databases the settings
    // file would otherwise point at. Everything else needs `-c <toml>`.
    if let Sub::Seed(cmd) = cli.sub {
        custom_tracing::init(
            "mobile_verifier_compare=info".to_string(),
            Default::default(),
        )
        .await?;
        return seed::run(cmd).await;
    }

    let settings = Settings::new(cli.config)?;
    custom_tracing::init(settings.log.clone(), Default::default()).await?;

    let trino =
        trino_client::Client::from_settings(&settings.trino).context("building Trino client")?;
    let pg = settings
        .database
        .connect("mobile-verifier-compare")
        .await
        .context("connecting to mobile_verifier Postgres")?;

    match cli.sub {
        Sub::Heartbeats(c) => heartbeats::run(&pg, &trino, &c.common).await,
        Sub::Speedtests(c) => speedtests::run(&pg, &trino, &c.common).await,
        Sub::LatestSpeedtests(c) => {
            speedtests::run_latest(&pg, &trino, &c.pubkey, c.anchor, &c.common).await
        }
        Sub::DataSessions(c) => data_sessions::run(&pg, &trino, &c.common).await,
        Sub::PayerBurns(c) => payer_burns::run(&pg, &trino, &c.common).await,
        Sub::PendingBurns(c) => {
            let mpv_pg = sqlx::PgPool::connect(&c.mpv_database_url)
                .await
                .context("connecting to mobile_packet_verifier Postgres")?;
            pending_burns::run(&mpv_pg, &trino, &c.common).await
        }
        Sub::All(c) => {
            heartbeats::run(&pg, &trino, &c.common).await?;
            speedtests::run(&pg, &trino, &c.common).await?;
            data_sessions::run(&pg, &trino, &c.common).await?;
            payer_burns::run(&pg, &trino, &c.common).await?;
            if let Some(url) = c.mpv_database_url.as_deref() {
                let mpv_pg = sqlx::PgPool::connect(url)
                    .await
                    .context("connecting to mobile_packet_verifier Postgres")?;
                pending_burns::run(&mpv_pg, &trino, &c.common).await?;
            } else {
                println!("[pending-burns] skipped (no --mpv-database-url)");
            }
            Ok(())
        }
        Sub::Seed(_) => unreachable!("Seed is dispatched at the top of main"),
    }
}
