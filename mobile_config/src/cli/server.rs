use crate::{
    admin_service::AdminService,
    authorization_service::AuthorizationService,
    carrier_service::CarrierService,
    entity_service::EntityService,
    gateway::{service::GatewayService, tracker::Tracker},
    grpc_server::GrpcServer,
    hex_boosting_service::HexBoostingService,
    key_cache::KeyCache,
    settings::Settings,
    sub_dao_service::SubDaoService,
};

use task_manager::TaskManager;

#[derive(Debug, clap::Args)]
pub struct Server;

impl Server {
    pub async fn run(&self, settings: &Settings) -> anyhow::Result<()> {
        custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;
        tracing::info!("Settings: {}", serde_json::to_string_pretty(settings)?);

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
            pool.clone(),
            settings.signing_keypair.clone(),
        );
        let auth_svc =
            AuthorizationService::new(key_cache.clone(), settings.signing_keypair.clone());
        let entity_svc = EntityService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair.clone(),
        );
        let carrier_svc = CarrierService::new(
            key_cache.clone(),
            pool.clone(),
            metadata_pool.clone(),
            settings.signing_keypair.clone(),
        );

        let hex_boosting_svc = HexBoostingService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair.clone(),
            settings.boosted_hex_activation_cutoff,
        );

        let sub_dao_svc = SubDaoService::new(
            key_cache.clone(),
            metadata_pool.clone(),
            settings.signing_keypair.clone(),
        );

        let listen_addr = settings.listen;
        let grpc_server = GrpcServer::new(
            listen_addr,
            admin_svc,
            gateway_svc,
            auth_svc,
            entity_svc,
            carrier_svc,
            hex_boosting_svc,
            sub_dao_svc,
        );

        TaskManager::builder()
            .add_task(grpc_server)
            .add_task(Tracker::new(
                pool.clone(),
                metadata_pool.clone(),
                settings.gateway_tracker_interval,
            ))
            .build()
            .start()
            .await
    }
}
