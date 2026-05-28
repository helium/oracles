use crate::{
    admin_service::AdminService,
    authorization_service::AuthorizationService,
    carrier_service::CarrierService,
    entity_service::EntityService,
    gateway::{
        hotspot_change_stream::{self, HotspotChangeDaemon, MobileHotspotChange},
        ownership_change_stream::{self, OwnershipChange, OwnershipChangeDaemon},
        service::GatewayService,
        tracker::Tracker,
    },
    grpc_server::GrpcServer,
    hex_boosting_service::HexBoostingService,
    key_cache::KeyCache,
    settings::Settings,
    sub_dao_service::SubDaoService,
};

use file_store::file_source;
use file_store_oracles::FileType;
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

        let ingest_bucket = settings.ingest.connect().await;

        let (hotspot_change_rx, hotspot_change_server) =
            file_source::continuous_source::<MobileHotspotChange, _, _>()
                .state(pool.clone())
                .bucket_client(ingest_bucket.clone())
                .lookback_start_after(settings.gateway_stream_start_after)
                .prefix(FileType::MobileHotspotChangeReport.to_str())
                .process_name(hotspot_change_stream::PROCESS_NAME.to_string())
                .create()
                .await?;

        let (ownership_change_rx, ownership_change_server) =
            file_source::continuous_source::<OwnershipChange, _, _>()
                .state(pool.clone())
                .bucket_client(ingest_bucket)
                .lookback_start_after(settings.gateway_stream_start_after)
                .prefix(FileType::EntityOwnershipChangeReport.to_str())
                .process_name(ownership_change_stream::PROCESS_NAME.to_string())
                .create()
                .await?;

        let hotspot_change_daemon =
            HotspotChangeDaemon::new(pool.clone(), metadata_pool.clone(), hotspot_change_rx);
        let ownership_change_daemon = OwnershipChangeDaemon::new(pool.clone(), ownership_change_rx);

        TaskManager::builder()
            .add_task(grpc_server)
            .add_task(hotspot_change_server)
            .add_task(ownership_change_server)
            .add_task(task_manager::channel_consumer(hotspot_change_daemon))
            .add_task(task_manager::channel_consumer(ownership_change_daemon))
            .add_task(task_manager::periodic(Tracker::new(
                pool.clone(),
                metadata_pool.clone(),
                settings.gateway_tracker_interval,
            )))
            .build()
            .start()
            .await?;
        Ok(())
    }
}
