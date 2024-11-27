use crate::{service_provider, Settings};
use anyhow::Result;
use chrono::{DateTime, Utc};
use mobile_config::client::CarrierServiceClient;

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(long)]
    start: Option<DateTime<Utc>>,
}

impl Cmd {
    pub async fn run(self, settings: &Settings) -> Result<()> {
        let epoch_start = match self.start {
            Some(dt) => dt,
            None => Utc::now(),
        };

        let carrier_client = CarrierServiceClient::from_settings(&settings.config_client)?;
        let promos = service_provider::get_promotions(carrier_client, &epoch_start).await?;

        println!("Promotions as of {epoch_start}");
        for sp in promos.into_proto() {
            println!("Service Provider: {:?}", sp.service_provider());
            println!("  incentive_escrow_bps: {:?}", sp.incentive_escrow_fund_bps);
            println!("  Promotions: ({})", sp.promotions.len());
            for promo in sp.promotions {
                let start = DateTime::from_timestamp(promo.start_ts as i64, 0).unwrap();
                let end = DateTime::from_timestamp(promo.end_ts as i64, 0).unwrap();
                let duration = humantime::format_duration((end - start).to_std()?);
                println!("    name: {}", promo.entity);
                println!("    duration: {duration} ({start:?} -> {end:?})",);
                println!("    shares: {}", promo.shares);
            }
        }

        Ok(())
    }
}
