use std::sync::Arc;

use crate::gateway::tracker::Tracker;
use crate::grpc_server::GrpcServer;
use crate::sub_dao_service::SubDaoService;
use crate::{
    admin::AuthCache, admin_service::AdminService, db_cleaner::DbCleaner,
    gateway::service::GatewayService, org, org_service::OrgService, region_map::RegionMapReader,
    route_service::RouteService, settings::Settings, telemetry,
};
use task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Daemon;

impl Daemon {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;

        tracing::info!("Starting IoT Config Daemon with {:?}", settings);

        // Install prometheus metrics exporter
        poc_metrics::start_metrics(&settings.metrics)?;
        telemetry::initialize();

        // Create database pool
        let pool = settings.database.connect("iot-config-store").await?;
        sqlx::migrate!().run(&pool).await?;

        // Create on-chain metadata pool
        let metadata_pool = settings.metadata.connect("iot-config-metadata").await?;

        let (auth_updater, auth_cache) = AuthCache::new(settings.admin_pubkey()?, &pool).await?;
        let (region_updater, region_map) = RegionMapReader::new(&pool).await?;
        let (delegate_key_updater, delegate_key_cache) = org::delegate_keys_cache(&pool).await?;

        let signing_keypair = Arc::new(settings.signing_keypair()?);

        let gateway_svc = GatewayService::new(
            signing_keypair.clone(),
            pool.clone(),
            region_map.clone(),
            auth_cache.clone(),
            delegate_key_cache,
        )?;

        let route_svc =
            RouteService::new(signing_keypair.clone(), auth_cache.clone(), pool.clone());

        let org_svc = OrgService::new(
            signing_keypair.clone(),
            auth_cache.clone(),
            pool.clone(),
            route_svc.clone_update_channel(),
            delegate_key_updater,
        )?;

        let admin_svc = AdminService::new(
            settings,
            auth_cache.clone(),
            auth_updater,
            pool.clone(),
            region_map.clone(),
            region_updater,
        )?;

        let subdao_svc = SubDaoService::new(settings, auth_cache, metadata_pool.clone())?;

        let listen_addr = settings.listen;
        let pubkey = settings
            .signing_keypair()
            .map(|keypair| keypair.public_key().to_string())?;
        tracing::debug!("listening on {listen_addr}");
        tracing::debug!("signing as {pubkey}");

        let tracker = Tracker::new(
            pool.clone(),
            metadata_pool.clone(),
            settings.gateway_tracker_interval,
        );

        let grpc_server = GrpcServer::new(
            listen_addr,
            gateway_svc,
            route_svc,
            org_svc,
            admin_svc,
            subdao_svc,
        );

        let db_cleaner = DbCleaner::new(pool.clone(), settings.deleted_entry_retention);

        TaskManager::builder()
            .add_task(tracker)
            .add_task(grpc_server)
            .add_task(db_cleaner)
            .build()
            .start()
            .await?;
        Ok(())
    }
}
