use crate::{metrics::Metrics, Settings};
use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, TimeZone, Utc};
use file_store::file_sink;
use futures::TryFutureExt;
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use task_manager::ManagedTask;
use tokio::{fs, time};

const HNT_DECIMALS: i32 = 8;
const LATEST_PRICE_FILE: &str = "hnt.latest";
const TOKEN: &str = "hnt";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Price {
    timestamp: DateTime<Utc>,
    price: u64,
}

impl Price {
    fn new(timestamp: DateTime<Utc>, price: u64) -> Self {
        Self { timestamp, price }
    }
}

pub struct PriceGenerator {
    http: reqwest::Client,
    source_url: String,
    interval_duration: std::time::Duration,
    last_price_opt: Option<Price>,
    default_price: Option<u64>,
    stale_price_duration: Duration,
    latest_price_file: PathBuf,
    file_sink: file_sink::FileSinkClient<PriceReportV1>,
}

impl ManagedTask for PriceGenerator {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl From<&Price> for PriceReportV1 {
    fn from(value: &Price) -> Self {
        Self {
            timestamp: value.timestamp.timestamp() as u64,
            price: value.price,
            token_type: BlockchainTokenTypeV1::Hnt.into(),
        }
    }
}

impl TryFrom<PriceReportV1> for Price {
    type Error = Error;

    fn try_from(value: PriceReportV1) -> Result<Self, Self::Error> {
        let tt: BlockchainTokenTypeV1 = BlockchainTokenTypeV1::try_from(value.token_type)
            .map_err(|_| anyhow!("unsupported token type: {:?}", value.token_type))?;
        if tt != BlockchainTokenTypeV1::Hnt {
            anyhow::bail!("expected HNT price report, got {tt:?}");
        }
        Ok(Self {
            timestamp: Utc
                .timestamp_opt(value.timestamp as i64, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid timestamp"))?,
            price: value.price,
        })
    }
}

#[derive(Debug, Deserialize)]
struct HermesResponse {
    parsed: Vec<HermesParsedPrice>,
}

#[derive(Debug, Deserialize)]
struct HermesParsedPrice {
    price: HermesPrice,
}

#[derive(Debug, Deserialize)]
struct HermesPrice {
    price: String,
    expo: i32,
    publish_time: i64,
}

impl PriceGenerator {
    pub async fn new(
        settings: &Settings,
        file_sink: file_sink::FileSinkClient<PriceReportV1>,
    ) -> Result<Self> {
        Ok(Self {
            last_price_opt: None,
            http: reqwest::Client::new(),
            source_url: settings.source.clone(),
            default_price: settings.default_price,
            interval_duration: settings.interval,
            stale_price_duration: settings.stale_price_duration,
            latest_price_file: settings.cache.join(LATEST_PRICE_FILE),
            file_sink,
        })
    }

    pub async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!(token = TOKEN, "starting price generator");

        let mut trigger = time::interval(self.interval_duration);
        self.last_price_opt = self.read_price_file().await;

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = trigger.tick() => {
                    match self.default_price {
                        Some(price) => {
                            let price = Price::new(Utc::now(), price);
                            self.write_price_to_sink(&price).await?;
                        }
                        None => {
                            self.retrieve_and_update_price().await?;
                        }
                    }
                }
            }
        }

        tracing::info!(token = TOKEN, "stopping price generator");
        Ok(())
    }

    async fn write_price_to_sink(&self, price: &Price) -> Result<()> {
        let price_report = PriceReportV1::from(price);
        tracing::info!(token = TOKEN, price.price, "updating price");
        self.file_sink.write(price_report, []).await?;

        Ok(())
    }

    async fn retrieve_and_update_price(&mut self) -> Result<()> {
        let price_opt = match self.fetch_hermes_price().await {
            Ok(new_price) => {
                self.last_price_opt = Some(new_price.clone());
                self.write_price_file(&new_price).await;

                Metrics::update("price_update_counter".to_string(), new_price.price as f64);

                Some(new_price)
            }
            Err(err) => {
                tracing::error!(token = TOKEN, ?err, "error in retrieving new price");

                match &self.last_price_opt {
                    Some(old_price) if self.is_valid(old_price) => {
                        Metrics::update("price_stale_counter".to_string(), old_price.price as f64);

                        Some(Price::new(Utc::now(), old_price.price))
                    }
                    Some(_old_price) => {
                        tracing::warn!(token = TOKEN, "stale price is too old, discarding");
                        self.last_price_opt = None;
                        None
                    }
                    None => None,
                }
            }
        };

        if let Some(price) = price_opt {
            self.write_price_to_sink(&price).await?;
        }

        Ok(())
    }

    async fn fetch_hermes_price(&self) -> Result<Price> {
        let response: HermesResponse = self
            .http
            .get(&self.source_url)
            .send()
            .and_then(|r| async { r.error_for_status() })
            .and_then(|r| r.json::<HermesResponse>())
            .await?;

        let parsed = response
            .parsed
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("hermes response had no parsed price entries"))?;

        let raw_price = parsed
            .price
            .price
            .parse::<Decimal>()
            .map_err(|err| anyhow!("failed to parse price {:?}: {err}", parsed.price.price))?;

        let scale = HNT_DECIMALS + parsed.price.expo;
        let scaled = if scale >= 0 {
            raw_price * Decimal::from(10_u64.pow(scale as u32))
        } else {
            raw_price / Decimal::from(10_u64.pow((-scale) as u32))
        };
        let price: u64 = scaled.try_into()?;

        let timestamp = Utc
            .timestamp_opt(parsed.price.publish_time, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid publish_time {}", parsed.price.publish_time))?;

        tracing::debug!(token = TOKEN, price, "retrieved price from hermes");
        Ok(Price::new(timestamp, price))
    }

    fn is_valid(&self, price: &Price) -> bool {
        price.timestamp > Utc::now() - self.stale_price_duration
    }

    async fn read_price_file(&self) -> Option<Price> {
        fs::read_to_string(&self.latest_price_file)
            .map_err(|err| format!("{err:?}"))
            .and_then(|contents| async move {
                serde_json::from_str::<Price>(&contents).map_err(|err| format!("{err:?}"))
            })
            .await
            .map_err(|err| {
                tracing::warn!(token = TOKEN, ?err, "unable to read latest price file");
                err
            })
            .ok()
    }

    async fn write_price_file(&self, price: &Price) {
        let result = async { serde_json::to_string_pretty(price) }
            .map_err(|err| format!("{err:?}"))
            .and_then(|json| {
                fs::write(&self.latest_price_file, json).map_err(|err| format!("{err:?}"))
            })
            .await;

        match result {
            Ok(_) => (),
            Err(err) => {
                tracing::warn!(token = TOKEN, ?err, "unable to save latest price file");
            }
        }
    }
}
