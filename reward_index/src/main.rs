use anyhow::Result;
use clap::Parser;
use file_store::{
    file_info_poller::LookbackBehavior, file_source, reward_manifest::RewardManifest, FileStore,
    FileType,
};
use reward_index::{db, settings::Settings, telemetry, Indexer};
use std::path::PathBuf;
use task_manager::TaskManager;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Indexer")]
pub struct Cli {
    /// Optional configuration file to use. If present the toml file at the
    /// given path will be loaded. Environemnt variables can override the
    /// settins in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        let settings = Settings::new(self.config)?;
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Server),
    /// Escrow Duration related commands
    Escrow {
        #[clap(subcommand)]
        cmd: EscrowCmds,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum EscrowCmds {
    /// Take all known radios in the rewards table and give them an escrow duration of 0 (zero) days.
    Migrate {
        /// Date on which to expire the grandfathered escrow_duration of 0 days.
        #[arg(long)]
        expires_on: chrono::NaiveDate,
    },
    Get {
        #[arg(short, long)]
        address: String,
    },
    Set {
        #[arg(long)]
        address: String,
        #[arg(long)]
        days: u32,
        #[arg(long)]
        expires_on: Option<chrono::NaiveDate>,
    },
}

impl EscrowCmds {
    async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(settings.mode, reward_index::settings::Mode::Iot),
            "migration not available for iot"
        );

        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        match self {
            EscrowCmds::Migrate { expires_on } => {
                let migrated_addresses =
                    db::escrow_duration::migrate_known_radios(&pool, expires_on).await?;
                tracing::info!(migrated_addresses, "done");
            }
            EscrowCmds::Get { address } => {
                let duration = db::escrow_duration::get(&pool, &address).await?;
                match duration {
                    Some((days, Some(expiration))) => {
                        println!("{address} has an escrow period of {days} days, expring on {expiration}");
                    }
                    Some((days, None)) => {
                        println!("{address} has an escrow period of {days} days, forever");
                    }
                    None => {
                        println!(
                            "{address} uses the default expiration date: {} days",
                            settings.escrow.default_days
                        );
                    }
                }
            }
            EscrowCmds::Set {
                address,
                days,
                expires_on,
            } => {
                let _inserted =
                    db::escrow_duration::insert(&pool, &address, days, expires_on).await?;

                match expires_on {
                    Some(expiration) => {
                        println!("{address} now has escrow period of {days} days, expiring on {expiration}");
                    }
                    None => {
                        println!("{address} now has escrow period of {days} days, forever");
                    }
                }
            }
        }

        Ok(())
    }
}

impl Cmd {
    pub async fn run(self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
            Self::Escrow { cmd } => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let app_name = format!("{}_{}", settings.mode, env!("CARGO_PKG_NAME"));
        let pool = settings.database.connect(&app_name).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let file_store = FileStore::from_settings(&settings.verifier).await?;
        let (receiver, server) = file_source::continuous_source::<RewardManifest, _>()
            .state(pool.clone())
            .store(file_store.clone())
            .prefix(FileType::RewardManifest.to_string())
            .lookback(LookbackBehavior::StartAfter(settings.start_after))
            .poll_duration(settings.interval)
            .offset(settings.interval * 2)
            .create()
            .await?;

        // Reward server
        let indexer = Indexer::from_settings(settings, pool, file_store, receiver).await?;

        TaskManager::builder()
            .add_task(server)
            .add_task(indexer)
            .build()
            .start()
            .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
