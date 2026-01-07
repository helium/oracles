use crate::{metrics::Metrics, Settings};
use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, TimeZone, Utc};
use file_store::file_sink;
use futures::TryFutureExt;
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana::{RpcClient, Token};
use std::{path::PathBuf, time::Duration};
use task_manager::ManagedTask;
use tokio::{fs, time};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Price {
    timestamp: DateTime<Utc>,
    price: u64,
    token: Token,
}

impl Price {
    fn new(timestamp: DateTime<Utc>, price: u64, token: Token) -> Self {
        Self {
            timestamp,
            price,
            token,
        }
    }
}

pub struct PriceGenerator {
    token: Token,
    client: RpcClient,
    interval_duration: std::time::Duration,
    last_price_opt: Option<Price>,
    default_price: Option<u64>,
    stale_price_duration: Duration,
    latest_price_file: PathBuf,
    file_sink: file_sink::FileSinkClient<PriceReportV1>,
}

impl AsRef<RpcClient> for PriceGenerator {
    fn as_ref(&self) -> &RpcClient {
        &self.client
    }
}

impl ManagedTask for PriceGenerator {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(self.run(shutdown))
    }
}

impl TryFrom<&Price> for PriceReportV1 {
    type Error = Error;

    fn try_from(value: &Price) -> anyhow::Result<Self> {
        Ok(Self {
            timestamp: value.timestamp.timestamp() as u64,
            price: value.price,
            token_type: match value.token {
                Token::Hnt => BlockchainTokenTypeV1::Hnt.into(),
                Token::Mobile => BlockchainTokenTypeV1::Mobile.into(),
                Token::Iot => BlockchainTokenTypeV1::Iot.into(),
                _ => anyhow::bail!("Invalid token type"),
            },
        })
    }
}

impl TryFrom<PriceReportV1> for Price {
    type Error = Error;

    fn try_from(value: PriceReportV1) -> Result<Self, Self::Error> {
        let tt: BlockchainTokenTypeV1 = BlockchainTokenTypeV1::try_from(value.token_type)
            .map_err(|_| anyhow!("unsupported token type: {:?}", value.token_type))?;
        Ok(Self {
            timestamp: Utc
                .timestamp_opt(value.timestamp as i64, 0)
                .single()
                .ok_or_else(|| anyhow!("invalid timestamp"))?,
            price: value.price,
            token: match tt {
                BlockchainTokenTypeV1::Hnt => Token::Hnt,
                BlockchainTokenTypeV1::Mobile => Token::Mobile,
                BlockchainTokenTypeV1::Iot => Token::Iot,
                _ => anyhow::bail!("Invalid token type"),
            },
        })
    }
}

impl PriceGenerator {
    pub async fn new(
        settings: &Settings,
        token: Token,
        default_price: Option<u64>,
        file_sink: file_sink::FileSinkClient<PriceReportV1>,
    ) -> Result<Self> {
        let client = RpcClient::new(settings.source.clone());
        Ok(Self {
            last_price_opt: None,
            token,
            client,
            default_price,
            interval_duration: settings.interval,
            stale_price_duration: settings.stale_price_duration,
            latest_price_file: settings.cache.join(format!("{token:?}.latest")),
            file_sink,
        })
    }

    pub async fn run(mut self, mut shutdown: triggered::Listener) -> Result<()> {
        tracing::info!(token = %self.token, "starting price generator");

        let mut trigger = time::interval(self.interval_duration);
        self.last_price_opt = self.read_price_file().await;

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = trigger.tick() => {
                    match self.default_price {
                        Some(price) => {
                            let price = Price::new(Utc::now(), price, self.token);
                            self.write_price_to_sink(&price).await?;
                        }
                        None => {
                            self.retrieve_and_update_price().await?;
                        }
                    }
                }
            }
        }

        tracing::info!(token = %self.token, "stopping price generator");
        Ok(())
    }

    async fn write_price_to_sink(&self, price: &Price) -> Result<()> {
        let price_report = PriceReportV1::try_from(price)?;
        tracing::info!(token = %self.token, %price.price, "updating price");
        self.file_sink.write(price_report, []).await?;

        Ok(())
    }

    async fn retrieve_and_update_price(&mut self) -> Result<()> {
        let price_opt = match self.get_pyth_price().await {
            Ok(new_price) => {
                self.last_price_opt = Some(new_price.clone());
                self.write_price_file(&new_price).await;

                Metrics::update(
                    "price_update_counter".to_string(),
                    self.token,
                    new_price.price as f64,
                );

                Some(new_price)
            }
            Err(err) => {
                tracing::error!(token = %self.token,?err,"error in retrieving new price");

                match &self.last_price_opt {
                    Some(old_price) if self.is_valid(old_price) => {
                        Metrics::update(
                            "price_stale_counter".to_string(),
                            self.token,
                            old_price.price as f64,
                        );

                        Some(Price::new(Utc::now(), old_price.price, old_price.token))
                    }
                    Some(_old_price) => {
                        tracing::warn!(token = %self.token, "stale price is too old, discarding");
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

    async fn get_pyth_price(&self) -> Result<Price> {
        solana::token::price::get(self, self.token)
            .await
            .map_err(anyhow::Error::from)
            .and_then(|p| {
                tracing::debug!(token = %self.token, %p.price, "retrieved price from chain");
                (p.price * Decimal::from(10_u64.pow(self.token.decimals() as u32)))
                    .try_into()
                    .map(|price| Price::new(p.timestamp, price, self.token))
                    .map_err(anyhow::Error::from)
            })
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
                tracing::warn!(token = %self.token, ?err, "unable to read latest price file");
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
                tracing::warn!(token = %self.token, ?err, "unable to save latest price file");
            }
        }
    }
}
