use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use file_store::FileType;
use helium_proto::{BlockchainTokenTypeV1, Message, PriceReportV1};
use tokio::{fs::File, io::AsyncWriteExt};

/// Generate Mobile price report
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(short, long, default_value = "1000000")]
    pub price: u64,
}

impl Cmd {
    pub async fn run(self) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let file_name = format!("{}.{now}.gz", FileType::PriceReport.to_str());
        let file = File::create(file_name).await?;

        let mobile_price_report = PriceReportV1 {
            price: self.price,
            timestamp: now as u64,
            token_type: BlockchainTokenTypeV1::Mobile.into(),
        };
        let encoded_mpr = PriceReportV1::encode_to_vec(&mobile_price_report);

        let mut writer = async_compression::tokio::write::GzipEncoder::new(file);
        writer.write_all(&encoded_mpr).await?;
        writer.shutdown().await?;

        Ok(())
    }
}
