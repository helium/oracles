use anyhow::{Error, Result};
use clap::Parser;
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_proto::services::mobile_config::{
    AdminServer, AuthorizationServer, CarrierServiceServer, EntityServer, GatewayServer,
    HexBoostingServer,
};
use mobile_config::{
    admin_service::AdminService, authorization_service::AuthorizationService,
    carrier_service::CarrierService, entity_service::EntityService,
    gateway_service::GatewayService, hex_boosting_service::HexBoostingService, key_cache::KeyCache,
    settings::Settings,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use task_manager::{ManagedTask, TaskManager};
use tonic::transport;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Config Service")]
pub struct Cli {
    /// Optional configuration file to use. If present, the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
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
    Server(Daemon),
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result<()> {
        match self {
            Self::Server(cmd) => cmd.run(&settings).await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Daemon;

impl Daemon {
    pub async fn run(&self, settings: &Settings) -> Result<()> {
        custom_tracing::init(settings.log.clone(), settings.tracing_cfg_file.clone()).await?;

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;

        // Create database pool
        let pool = settings.database.connect("mobile-config-store").await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let metadata_pool = settings.metadata.connect("mobile-config-metadata").await?;

        let (key_cache_updater, key_cache) = KeyCache::from_settings(settings, &pool).await?;

        let admin_svc =
            AdminService::new(settings, key_cache.clone(), key_cache_updater, pool.clone())?;
        let gateway_svc = GatewayService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );
        let auth_svc = AuthorizationService::new(key_cache.clone(), settings.signing_keypair()?);
        let entity_svc = EntityService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );
        let carrier_svc =
            CarrierService::new(key_cache.clone(), pool.clone(), settings.signing_keypair()?);

        let hex_boosting_svc = HexBoostingService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair()?,
        );

        let listen_addr = settings.listen;
        let grpc_server = GrpcServer {
            listen_addr,
            admin_svc,
            gateway_svc,
            auth_svc,
            entity_svc,
            carrier_svc,
            hex_boosting_svc,
        };

        TaskManager::builder()
            .add_task(grpc_server)
            .build()
            .start()
            .await
    }
}

pub struct GrpcServer {
    listen_addr: SocketAddr,
    admin_svc: AdminService,
    gateway_svc: GatewayService,
    auth_svc: AuthorizationService,
    entity_svc: EntityService,
    carrier_svc: CarrierService,
    hex_boosting_svc: HexBoostingService,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            transport::Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(250)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .layer(custom_tracing::grpc_layer::new_with_span(make_span))
                .add_service(AdminServer::new(self.admin_svc))
                .add_service(GatewayServer::new(self.gateway_svc))
                .add_service(AuthorizationServer::new(self.auth_svc))
                .add_service(EntityServer::new(self.entity_svc))
                .add_service(CarrierServiceServer::new(self.carrier_svc))
                .add_service(HexBoostingServer::new(self.hex_boosting_svc))
                .serve_with_shutdown(self.listen_addr, shutdown)
                .map_err(Error::from)
                .await
        })
    }
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(
        "tracing",
        pub_key = tracing::field::Empty,
        signer = tracing::field::Empty,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.run().await
}
