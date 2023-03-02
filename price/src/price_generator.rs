use crate::{PriceError, Settings};
use anyhow::Result;
use chrono::Utc;
use file_store::file_sink;
use helium_proto::{services::price_oracle::PriceOracleReportV1, BlockchainTokenTypeV1};
use pyth_sdk_solana::load_price_feed_from_account;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey as SolPubkey;
use tokio::{sync::watch, time};

pub type MessageSender = watch::Sender<Price>;
pub type MessageReceiver = watch::Receiver<Price>;

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
    pub receiver: MessageReceiver,
    pub sender: MessageSender,
}

impl From<Price> for PriceOracleReportV1 {
    fn from(value: Price) -> Self {
        Self {
            timestamp: value.timestamp as u64,
            price: value.price,
            token_type: value.token_type.into(),
        }
    }
}

impl TryFrom<PriceOracleReportV1> for Price {
    type Error = PriceError;

    fn try_from(value: PriceOracleReportV1) -> Result<Self, Self::Error> {
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
        let client = RpcClient::new(&settings.rpc_endpoint);
        let price = match settings.price_key(token_type) {
            None => Price::new(0, 0, token_type),
            Some(price_key) => get_price(&client, &price_key, settings.age, token_type).await?,
        };
        let (sender, receiver) = watch::channel(price);
        Ok(Self {
            settings: settings.clone(),
            client,
            receiver,
            sender,
        })
    }

    pub fn receiver(&self) -> MessageReceiver {
        self.receiver.clone()
    }

    pub async fn run(
        &mut self,
        file_sink: file_sink::FileSinkClient,
        shutdown: &triggered::Listener,
        token_type: BlockchainTokenTypeV1,
    ) -> anyhow::Result<()> {
        tracing::info!("started price generator for: {:?}", token_type);
        let mut price_timer = time::interval(self.settings.tick_interval().to_std()?);
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
        tracing::info!("stopping price generator");
        Ok(())
    }

    async fn handle_price_tick(
        &mut self,
        file_sink: &file_sink::FileSinkClient,
        token_type: BlockchainTokenTypeV1,
    ) -> anyhow::Result<()> {
        if let Some(price_key) = &self.settings.price_key(token_type) {
            let new_price =
                match get_price(&self.client, price_key, self.settings.age, token_type).await {
                    Ok(price) => price.price,
                    Err(err) => {
                        tracing::warn!("failed to get price: {err:?}");
                        self.receiver.borrow().price
                    }
                };
            let timestamp = Utc::now().timestamp();

            self.sender.send_modify(|price| {
                price.price = new_price;
                price.timestamp = timestamp;
            });

            let price = &*self.receiver.borrow();
            tracing::info!(
                "updating price: {} for {:?}, at: {}",
                price.price.to_string(),
                token_type,
                price.timestamp
            );

            let price_report = PriceOracleReportV1::from(price.clone());
            tracing::info!("price_report: {:?}", price_report);

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
        Some(p) => {
            tracing::info!("got price {:?} at {:?}", p, current_time);
            Ok(Price {
                price: p.price as u64,
                timestamp: current_time,
                token_type,
            })
        }
    }
}
