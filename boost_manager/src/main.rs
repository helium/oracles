use anyhow::{bail, Result};
use boost_manager::{
    activator::Activator, purger::Purger, settings::Settings, telemetry, updater::Updater,
    watcher::Watcher,
};
use clap::Parser;
use file_store::{
    file_info_poller::LookbackBehavior, file_source, file_upload, reward_manifest::RewardManifest,
    traits::FileSinkWriteExt, FileStore, FileType,
};
use helium_proto::BoostedHexUpdateV1;
use mobile_config::client::hex_boosting_client::HexBoostingClient;
use solana::start_boost::SolanaRpc;
use std::{
    path::{self, PathBuf},
    time::Duration,
};
use task_manager::TaskManager;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Boost Manager")]
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
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Server(Server),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Server {}

impl Server {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;

        // Install the prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Set up the solana network:
        let solana = if settings.enable_solana_integration {
            let Some(ref solana_settings) = settings.solana else {
                bail!("Missing solana section in settings");
            };
            // Set up the solana RpcClient:
            Some(SolanaRpc::new(solana_settings).await?)
        } else {
            None
        };

        // Create database pool and run migrations
        let pool = settings.database.connect(env!("CARGO_PKG_NAME")).await?;
        sqlx::migrate!().run(&pool).await?;

        telemetry::initialize(&pool).await?;

        let hex_boosting_client = HexBoostingClient::from_settings(&settings.mobile_config_client)?;

        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings.output).await?;
        let store_base_path = path::Path::new(&settings.cache);

        let reward_check_interval = settings.reward_check_interval;

        // setup the received for the rewards manifest files
        let file_store = FileStore::from_settings(&settings.verifier).await?;
        let (manifest_receiver, manifest_server) =
            file_source::continuous_source::<RewardManifest, _>()
                .state(pool.clone())
                .store(file_store)
                .prefix(FileType::RewardManifest.to_string())
                .lookback(LookbackBehavior::StartAfter(settings.start_after))
                .poll_duration(reward_check_interval)
                .offset(reward_check_interval * 2)
                .create()
                .await?;

        // setup the writer for our updated hexes
        let (updated_hexes_sink, updated_hexes_sink_server) = BoostedHexUpdateV1::file_sink(
            store_base_path,
            file_upload.clone(),
            Some(Duration::from_secs(5 * 60)),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

        // The server to monitor rewards and activate any newly seen boosted hexes
        let verifier_store = FileStore::from_settings(&settings.verifier).await?;
        let activator = Activator::new(
            pool.clone(),
            manifest_receiver,
            hex_boosting_client.clone(),
            verifier_store,
        )
        .await?;

        let watcher = Watcher::new(pool.clone(), updated_hexes_sink, hex_boosting_client).await?;

        let updater = Updater::new(
            pool.clone(),
            settings.enable_solana_integration,
            settings.activation_check_interval,
            settings.txn_batch_size(),
            solana,
        )?;

        let purger = Purger::new(pool.clone(), settings.retention_period);

        TaskManager::builder()
            .add_task(file_upload_server)
            .add_task(manifest_server)
            .add_task(updated_hexes_sink_server)
            .add_task(activator)
            .add_task(watcher)
            .add_task(updater)
            .add_task(purger)
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
