use crate::{metrics::Metrics, Settings};
use anyhow::{anyhow, bail, Error, Result};
use chrono::{DateTime, TimeZone, Utc};
use file_store::file_sink;
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_anchor_gen::anchor_lang::AccountDeserialize;
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use pyth_solana_receiver_sdk::price_update::{FeedId, PriceUpdateV2};
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey as SolPubkey;
use std::{cmp::Ordering, path::PathBuf, str::FromStr, time::Duration};
use task_manager::ManagedTask;
use tokio::{fs, time};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Price {
    timestamp: DateTime<Utc>,
    price: u64,
    token_type: BlockchainTokenTypeV1,
}

impl Price {
    fn new(timestamp: DateTime<Utc>, price: u64, token_type: BlockchainTokenTypeV1) -> Self {
        Self {
            timestamp,
            price,
            token_type,
        }
    }
}

pub struct PriceGenerator {
    token_type: BlockchainTokenTypeV1,
    client: RpcClient,
    interval_duration: std::time::Duration,
    last_price_opt: Option<Price>,
    key: Option<SolPubkey>,
    feed_id: Option<FeedId>,
    default_price: Option<u64>,
    stale_price_duration: Duration,
    latest_price_file: PathBuf,
    file_sink: Option<file_sink::FileSinkClient>,
    pyth_price_interval: std::time::Duration,
}

impl ManagedTask for PriceGenerator {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl From<Price> for PriceReportV1 {
    fn from(value: Price) -> Self {
        Self {
            timestamp: value.timestamp.timestamp() as u64,
            price: value.price,
            token_type: value.token_type.into(),
        }
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
            token_type: tt,
        })
    }
}

impl PriceGenerator {
    pub async fn new(
        settings: &Settings,
        token_type: BlockchainTokenTypeV1,
        file_sink: file_sink::FileSinkClient,
    ) -> Result<Self> {
        let client = RpcClient::new(settings.source.clone());
        Ok(Self {
            last_price_opt: None,
            token_type,
            client,
            key: settings.price_key(token_type)?,
            feed_id: settings.price_feed_id(token_type)?,
            default_price: settings.default_price(token_type)?,
            interval_duration: settings.interval,
            stale_price_duration: settings.stale_price_duration,
            latest_price_file: PathBuf::from_str(&settings.cache)?
                .join(format!("{token_type:?}.latest")),
            file_sink: Some(file_sink),
            pyth_price_interval: settings.pyth_price_interval,
        })
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        match (
            self.key,
            self.feed_id,
            self.default_price,
            self.file_sink.clone(),
        ) {
            (Some(key), Some(feed_id), _, Some(file_sink)) => {
                self.run_with_key(key, feed_id, file_sink, &shutdown).await
            }
            (None, None, Some(defaut_price), Some(file_sink)) => {
                self.run_with_default(defaut_price, file_sink, &shutdown)
                    .await
            }
            _ => {
                tracing::warn!(
                    "stopping price generator for {:?}, not configured",
                    self.token_type
                );
                Ok(())
            }
        }
    }

    async fn run_with_default(
        &mut self,
        default_price: u64,
        file_sink: file_sink::FileSinkClient,
        shutdown: &triggered::Listener,
    ) -> Result<()> {
        tracing::info!(
            "starting default price generator for {:?}, using price {default_price}",
            self.token_type
        );
        let mut trigger = time::interval(self.interval_duration);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = trigger.tick() => {
                    let price = Price::new(Utc::now(), default_price, self.token_type);
                    let price_report = PriceReportV1::from(price);
                    tracing::info!("updating {:?} with default price: {}", self.token_type, default_price);
                    file_sink.write(price_report, []).await?;
                }
            }
        }

        tracing::info!("stopping default price generator for {:?}", self.token_type);
        Ok(())
    }

    async fn run_with_key(
        &mut self,
        key: SolPubkey,
        feed_id: FeedId,
        file_sink: file_sink::FileSinkClient,
        shutdown: &triggered::Listener,
    ) -> Result<()> {
        tracing::info!("starting price generator for {:?}", self.token_type);
        let mut trigger = time::interval(self.interval_duration);
        self.last_price_opt = self.read_price_file().await;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = trigger.tick() => self.handle(&key, &feed_id, &file_sink).await?,
            }
        }

        tracing::info!("stopping price generator for {:?}", self.token_type);
        Ok(())
    }

    async fn handle(
        &mut self,
        key: &SolPubkey,
        feed_id: &FeedId,
        file_sink: &file_sink::FileSinkClient,
    ) -> Result<()> {
        let price_opt = match self.get_pyth_price(key, feed_id).await {
            Ok(new_price) => {
                tracing::info!(
                    "updating price for {:?} to {}",
                    self.token_type,
                    new_price.price
                );
                self.last_price_opt = Some(new_price.clone());
                self.write_price_file(&new_price).await;

                Metrics::update(
                    "price_update_counter".to_string(),
                    self.token_type,
                    new_price.price as f64,
                );

                Some(new_price)
            }
            Err(err) => {
                tracing::error!(
                    "error in retrieving new price for {:?}: {err:?}",
                    self.token_type
                );

                match &self.last_price_opt {
                    Some(old_price) if self.is_valid(old_price) => {
                        Metrics::update(
                            "price_stale_counter".to_string(),
                            self.token_type,
                            old_price.price as f64,
                        );

                        Some(Price::new(
                            Utc::now(),
                            old_price.price,
                            old_price.token_type,
                        ))
                    }
                    Some(_old_price) => {
                        tracing::warn!(
                            "stale price for {:?} is too old, discarding",
                            self.token_type
                        );
                        self.last_price_opt = None;
                        None
                    }
                    None => None,
                }
            }
        };

        if let Some(price) = price_opt {
            let price_report = PriceReportV1::from(price);
            tracing::debug!("price_report: {:?}", price_report);
            file_sink.write(price_report, []).await?;
        }

        Ok(())
    }

    async fn get_pyth_price(&self, price_key: &SolPubkey, feed_id: &FeedId) -> Result<Price> {
        let account = self.client.get_account(price_key).await?;
        let PriceUpdateV2 { price_message, .. } =
            PriceUpdateV2::try_deserialize(&mut account.data.as_slice())?;

        if price_message.feed_id != *feed_id {
            bail!("Mismatched feed id");
        }

        if price_message
            .publish_time
            .saturating_add(self.pyth_price_interval.as_secs() as i64)
            < Utc::now().timestamp()
        {
            bail!("Price is too old");
        }

        if price_message.ema_price < 0 {
            bail!("Price is less than zero");
        }

        // Remove the confidence interval from the price to get the most optimistic price:
        let optimistic_price = price_message.ema_price as u64 + price_message.ema_conf * 2;

        // We want the price to have a resulting exponent of 10^-6
        // I don't think it's possible for pyth to give us anything other than -8, but we make
        // this robust just in case:
        let exp = price_message.exponent + 6;
        let adjusted_optimistic_price = match exp.cmp(&0) {
            Ordering::Less => optimistic_price / 10_u64.pow(exp.unsigned_abs()),
            Ordering::Greater => optimistic_price * 10_u64.pow(exp as u32),
            _ => optimistic_price,
        };

        Ok(Price::new(
            DateTime::from_timestamp(price_message.publish_time, 0).ok_or_else(|| {
                anyhow!(
                    "Invalid publish time for price: {}",
                    price_message.publish_time
                )
            })?,
            adjusted_optimistic_price,
            self.token_type,
        ))
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
                tracing::warn!(token = ?self.token_type, "unable to read latest price file due to {err}");
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
                tracing::warn!(token = ?self.token_type, "unable to save latest price file due to {err}");
            }
        }
    }
}
