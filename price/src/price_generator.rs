use crate::{metrics::Metrics, PriceError, Settings};
use anyhow::Result;
use chrono::Utc;
use file_store::file_sink;
use helium_proto::{BlockchainTokenTypeV1, PriceReportV1};
use pyth_sdk_solana::load_price_feed_from_account;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey as SolPubkey;
use tokio::time;

#[derive(Debug, Clone, Serialize)]
pub struct Price {
    pub timestamp: i64,
    pub price: u64,
    pub token_type: BlockchainTokenTypeV1,
}

impl Price {
    pub fn new(timestamp: i64, price: u64, token_type: BlockchainTokenTypeV1) -> Self {
        Self {
            timestamp,
            price,
            token_type,
        }
    }
}

pub struct PriceGenerator {
    settings: Settings,
    client: RpcClient,
    price: Option<Price>,
}

impl From<Price> for PriceReportV1 {
    fn from(value: Price) -> Self {
        Self {
            timestamp: value.timestamp as u64,
            price: value.price,
            token_type: value.token_type.into(),
        }
    }
}

impl TryFrom<PriceReportV1> for Price {
    type Error = PriceError;

    fn try_from(value: PriceReportV1) -> Result<Self, Self::Error> {
        let tt: BlockchainTokenTypeV1 = BlockchainTokenTypeV1::from_i32(value.token_type)
            .ok_or(PriceError::UnsupportedTokenType(value.token_type))?;
        Ok(Self {
            timestamp: value.timestamp as i64,
            price: value.price,
            token_type: tt,
        })
    }
}

impl PriceGenerator {
    pub async fn new(settings: &Settings, token_type: BlockchainTokenTypeV1) -> Result<Self> {
        let client = RpcClient::new(&settings.source);
        let price = match settings.price_key(token_type) {
            None => {
                let timestamp = Utc::now().timestamp();
                match settings.default_price(token_type) {
                    None => {
                        tracing::warn!(
                            "no price_key and no default_price in settings for {:?}",
                            token_type
                        );
                        None
                    }
                    Some(p) => Some(Price::new(timestamp, p, token_type)),
                }
            }
            Some(price_key) => {
                let rpc_price = get_price(&client, &price_key, settings.age, token_type).await?;
                Some(rpc_price)
            }
        };
        Ok(Self {
            settings: settings.clone(),
            client,
            price,
        })
    }

    pub async fn run(
        &mut self,
        file_sink: file_sink::FileSinkClient,
        shutdown: &triggered::Listener,
        token_type: BlockchainTokenTypeV1,
    ) -> anyhow::Result<()> {
        tracing::info!("started price generator for: {:?}", token_type);
        let mut price_timer = time::interval(self.settings.interval().to_std()?);
        price_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            if shutdown.is_triggered() {
                break;
            }
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = price_timer.tick() => match self.handle_price_tick(&file_sink, token_type).await {
                    Ok(()) => (),
                    Err(err) => {
                        tracing::error!("fatal price generator error: {err:?}");
                        return Err(err)
                    }
                }
            }
        }
        tracing::info!("stopping price generator for {:?}", token_type);
        Ok(())
    }

    async fn handle_price_tick(
        &mut self,
        file_sink: &file_sink::FileSinkClient,
        token_type: BlockchainTokenTypeV1,
    ) -> anyhow::Result<()> {
        let timestamp = Utc::now().timestamp();
        if let Some(price_key) = &self.settings.price_key(token_type) {
            let new_price = self.maybe_new_price(price_key, token_type).await;

            match new_price {
                None => Ok(()),
                Some(mut new_price) => {
                    // Just set the timestamp to latest for the "new" price
                    new_price.timestamp = timestamp;

                    tracing::info!(
                        "updating price {} for {:?} at {:?}",
                        new_price.price.to_string(),
                        token_type,
                        timestamp
                    );

                    let price_report = PriceReportV1::from(new_price.clone());
                    tracing::debug!("price_report: {:?}", price_report);

                    // Set this as the last price for generator
                    self.price = Some(new_price);
                    tracing::debug!("setting new price for generator: {:?}", self.price);

                    file_sink.write(price_report, []).await?;

                    Ok(())
                }
            }
        } else {
            // The following is mostly to accomodate testing when we don't have a price_key but
            // have a default price value set in settings
            match &self.settings.default_price(token_type) {
                None => {
                    // nothing to do
                    tracing::info!("no price_key and no default_price for {:?}", token_type);
                    Ok(())
                }
                Some(p) => {
                    // We have a default price in settings
                    tracing::info!("no price_key for {:?}, default_price: {:?}", token_type, p);
                    let price = Price::new(timestamp, *p, token_type);
                    let price_report = PriceReportV1::from(price.clone());
                    self.price = Some(price);
                    file_sink.write(price_report, []).await?;
                    Ok(())
                }
            }
        }
    }

    /// Try to get a new price if we can via RPC, otherwise return old price
    pub async fn maybe_new_price(
        &self,
        price_key: &SolPubkey,
        token_type: BlockchainTokenTypeV1,
    ) -> Option<Price> {
        match get_price(&self.client, price_key, self.settings.age, token_type).await {
            Ok(price) => {
                Metrics::update(
                    "price_update_counter".to_string(),
                    token_type,
                    price.price as f64,
                );
                Some(price)
            }
            Err(err) => match self.price.clone() {
                None => {
                    tracing::warn!("no known price for {:?}", token_type);
                    None
                }
                Some(old_price) => {
                    Metrics::update(
                        "price_stale_counter".to_string(),
                        token_type,
                        old_price.price as f64,
                    );
                    tracing::warn!("rpc failure: {err:?}, reusing old price {:?}", old_price);
                    Some(old_price)
                }
            },
        }
    }
}

pub async fn get_price(
    client: &RpcClient,
    price_key: &SolPubkey,
    age: u64,
    token_type: BlockchainTokenTypeV1,
) -> Result<Price> {
    let mut price_account = client.get_account(price_key)?;
    tracing::debug!("price_account: {:?}", price_account);

    let current_time = Utc::now().timestamp();
    let price_feed = load_price_feed_from_account(price_key, &mut price_account)?;
    tracing::debug!("price_feed: {:?}", price_feed);

    match price_feed.get_price_no_older_than(current_time, age) {
        None => {
            tracing::error!("unable to fetch price at {:?}", current_time);
            Err(PriceError::UnableToFetch.into())
        }
        Some(feed_price) => {
            let value = feed_price.price;
            tracing::info!(
                "got price {:?} via rpc for {:?} at {:?}",
                value,
                token_type,
                current_time
            );
            Ok(Price::new(current_time, value as u64, token_type))
        }
    }
}
