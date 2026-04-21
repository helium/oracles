use crate::{hermes, metrics::Metrics, Settings};
use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeZone, Utc};
use file_store::file_sink;
use futures::TryFutureExt;
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use task_manager::ManagedTask;
use tokio::{fs, time};

const LATEST_PRICE_FILE: &str = "hnt.latest";
const TOKEN: &str = "hnt";
const MAX_PRICE_AGE: chrono::Duration = chrono::Duration::minutes(10);

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
        let parsed = hermes::fetch(&self.http, &self.source_url).await?;
        let price = parsed.price.scaled_u64()?;
        let timestamp = Utc
            .timestamp_opt(parsed.price.publish_time, 0)
            .single()
            .ok_or_else(|| anyhow!("invalid publish_time {}", parsed.price.publish_time))?;

        let age = Utc::now() - timestamp;
        if age > MAX_PRICE_AGE {
            anyhow::bail!(
                "hermes price is {}s old, exceeds max age of {}s",
                age.num_seconds(),
                MAX_PRICE_AGE.as_seconds_f32()
            );
        }

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
