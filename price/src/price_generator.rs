use crate::{PriceError, Settings};
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
    price: Price,
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
            None => Price::new(0, 0, token_type),
            Some(price_key) => get_price(&client, &price_key, settings.age, token_type).await?,
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
        if let Some(price_key) = &self.settings.price_key(token_type) {
            // Try to get a new price if we can, otherwise reuse the old price we already have.
            let mut new_price =
                match get_price(&self.client, price_key, self.settings.age, token_type).await {
                    Ok(price) => price,
                    Err(err) => {
                        let old_price = self.price.clone();
                        tracing::warn!("rpc failure: {err:?}, reusing old price {:?}", old_price);
                        old_price
                    }
                };

            // Just set the timestamp to latest for the "new" price
            let timestamp = Utc::now().timestamp();
            new_price.timestamp = timestamp;

            tracing::info!(
                "updating price {} for {:?} at {:?}",
                new_price.price.to_string(),
                token_type,
                timestamp
            );

            let price_report = PriceReportV1::from(new_price.clone());
            tracing::debug!("price_report: {:?}", price_report);

            file_sink.write(price_report, []).await?;

            Ok(())
        } else {
            tracing::warn!("no price key for {:?}", token_type);
            Ok(())
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
