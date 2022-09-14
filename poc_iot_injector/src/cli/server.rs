use crate::{keypair::load_from_file, mk_db_pool, server::Server, Result};
use tokio::signal;

/// Start rewards server
#[derive(Debug, clap::Args)]
pub struct Cmd {}

impl Cmd {
    pub async fn run(&self) -> Result {
        // Install the prometheus metrics exporter
        poc_metrics::install_metrics();

        // Create database pool
        let pool = mk_db_pool(10).await?;
        sqlx::migrate!().run(&pool).await?;

        // Configure shutdown trigger
        let (shutdown_trigger, shutdown_listener) = triggered::trigger();
        tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            shutdown_trigger.trigger()
        });

        // injector server keypair from env
        let poc_injector_kp_path = std::env::var("POC_IOT_INJECTOR_KEYPAIR")
            .unwrap_or_else(|_| String::from("/tmp/poc_iot_injector_keypair"));
        let poc_iot_injector_keypair = load_from_file(&poc_injector_kp_path)?;

        // poc_iot_injector server
        let mut poc_iot_injector_server = Server::new(pool, poc_iot_injector_keypair).await?;

        poc_iot_injector_server
            .run(shutdown_listener.clone())
            .await?;
        Ok(())
    }
}
