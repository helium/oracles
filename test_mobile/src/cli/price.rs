use anyhow::Result;
use file_store::{file_sink, file_upload, FileType};
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use std::{
    path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Generate Mobile price report
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(short, long, default_value = "1000000")]
    pub price: u64,
}

impl Cmd {
    pub async fn run(self) -> Result<()> {
        let settings = file_store::Settings {
            bucket: "mobile-price".to_string(),
            endpoint: Some("http://localhost:4566".to_string()),
            region: "us-east-1".to_string(),
            access_key_id: None,
            secret_access_key: None,
        };

        let (shutdown_trigger1, shutdown_listener1) = triggered::trigger();

        // Initialize uploader
        let (file_upload, file_upload_server) =
            file_upload::FileUpload::from_settings_tm(&settings).await?;

        let file_upload_thread = tokio::spawn(async move {
            file_upload_server
                .run(shutdown_listener1)
                .await
                .expect("failed to complete file_upload_server");
        });

        let store_base_path = path::Path::new(".");

        let (price_sink, price_sink_server) = file_sink::FileSinkBuilder::new(
            FileType::PriceReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_report_submission"),
        )
        .auto_commit(false)
        .roll_time(Duration::from_millis(100))
        .create()
        .await?;

        let (shutdown_trigger2, shutdown_listener2) = triggered::trigger();
        let price_sink_thread = tokio::spawn(async move {
            price_sink_server
                .run(shutdown_listener2)
                .await
                .expect("failed to complete price_sink_server");
        });

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let price_report = PriceReportV1 {
            price: 1000000,
            timestamp: now as u64,
            token_type: BlockchainTokenTypeV1::Mobile.into(),
        };

        price_sink.write(price_report, []).await?;

        let price_sink_rcv = price_sink.commit().await.expect("commit failed");
        let _ = price_sink_rcv
            .await
            .expect("commit didn't complete completed");

        let _ = tokio::time::sleep(Duration::from_secs(1)).await;

        shutdown_trigger1.trigger();
        shutdown_trigger2.trigger();
        file_upload_thread
            .await
            .expect("file_upload_thread did not complete");
        price_sink_thread
            .await
            .expect("price_sink_thread did not complete");

        Ok(())
    }
}
