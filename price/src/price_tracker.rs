use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, Duration, TimeZone, Utc};
use file_store::{FileInfo, FileStore, FileType};
use futures::stream::{StreamExt, TryStreamExt};
use helium_proto::{BlockchainTokenTypeV1, Message, PriceReportV1};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
struct Price {
    price: u64,
    timestamp: DateTime<Utc>,
}

impl TryFrom<&PriceReportV1> for Price {
    type Error = anyhow::Error;

    fn try_from(value: &PriceReportV1) -> Result<Self, Self::Error> {
        Ok(Self {
            price: value.price,
            timestamp: Utc
                .timestamp_opt(value.timestamp as i64, 0)
                .single()
                .ok_or_else(|| anyhow!("Invalid timestamp: {}", value.timestamp))?,
        })
    }
}

type Prices = HashMap<BlockchainTokenTypeV1, Price>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    price_duration_minutes: u64,
    file_store: file_store::Settings,
}

impl Settings {
    fn price_duration(&self) -> Duration {
        Duration::minutes(self.price_duration_minutes as i64)
    }
}

#[derive(Clone)]
pub struct PriceTracker {
    price_duration: Duration,
    task_killer: mpsc::Sender<String>,
    price_receiver: watch::Receiver<Prices>,
}

impl PriceTracker {
    pub async fn start(
        settings: &Settings,
        shutdown: triggered::Listener,
    ) -> Result<(Self, impl std::future::Future<Output = Result<()>>)> {
        let file_store = FileStore::from_settings(&settings.file_store).await?;
        let (price_sender, price_receiver) = watch::channel(Prices::new());
        let (task_kill_sender, task_kill_receiver) = mpsc::channel(1);
        let initial_timestamp =
            calculate_initial_prices(&file_store, settings.price_duration(), &price_sender).await?;

        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            run(
                file_store,
                task_kill_receiver,
                price_sender,
                initial_timestamp,
                shutdown_clone,
            )
            .await
        });

        let tracker = Self {
            price_duration: settings.price_duration(),
            task_killer: task_kill_sender,
            price_receiver,
        };

        Ok((tracker, async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(Error::from(err)),
            }
        }))
    }

    pub async fn price(&self, token_type: &BlockchainTokenTypeV1) -> Result<u64> {
        let result = self
            .price_receiver
            .borrow()
            .get(token_type)
            .ok_or_else(|| anyhow!("price not available"))
            .and_then(|price| {
                if price.timestamp > Utc::now() - self.price_duration {
                    Ok(price.price)
                } else {
                    Err(anyhow!(
                        "price too old, price timestamp: {}",
                        price.timestamp
                    ))
                }
            });

        if let Err(error) = &result {
            self.task_killer.send(error.to_string()).await?;
        }

        result
    }
}

async fn run(
    file_store: FileStore,
    mut task_killer: mpsc::Receiver<String>,
    price_sender: watch::Sender<Prices>,
    mut after: DateTime<Utc>,
    shutdown: triggered::Listener,
) -> Result<()> {
    let mut trigger = tokio::time::interval(std::time::Duration::from_secs(30));

    loop {
        let shutdown = shutdown.clone();

        tokio::select! {
            _ = shutdown => {
                tracing::info!("PriceTracker: shutting down");
                break;
            }
            _ = trigger.tick() => {
                let timestamp = process_files(&file_store, &price_sender, after).await?;
                after = timestamp.unwrap_or(after);
            }
            msg = task_killer.recv() => if let Some(string) = msg {
                    return Err(anyhow!(string));
            }
        }
    }

    Ok(())
}

async fn calculate_initial_prices(
    file_store: &FileStore,
    price_duration: Duration,
    sender: &watch::Sender<Prices>,
) -> Result<DateTime<Utc>> {
    tracing::debug!("PriceTracker: Updating initial prices");
    process_files(file_store, sender, Utc::now() - price_duration)
        .await?
        .ok_or_else(|| anyhow!("price not available"))
}

async fn process_files(
    file_store: &FileStore,
    sender: &watch::Sender<Prices>,
    after: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    file_store
        .list(FileType::PriceReport, after, None)
        .map_err(Error::from)
        .and_then(|file| process_file(file_store, file, sender))
        .try_fold(None, |_old, ts| async move { Ok(Some(ts)) })
        .await
}

async fn process_file(
    file_store: &FileStore,
    file: FileInfo,
    sender: &watch::Sender<Prices>,
) -> Result<DateTime<Utc>> {
    tracing::debug!("PriceTracker: processing pricing report file {}", file.key);
    let timestamp = file.timestamp;

    file_store
        .stream_file(file)
        .await?
        .map_err(Error::from)
        .and_then(|buf| async { PriceReportV1::decode(buf).map_err(Error::from) })
        .and_then(|report| async move {
            Price::try_from(&report).map(|price| (report.token_type(), price))
        })
        .map_err(|err| {
            tracing::warn!("PriceTracker: skipping price report due to error {err:?}");
            err
        })
        .filter_map(|result| async { result.ok() })
        .for_each(|(token_type, price)| async move {
            sender.send_modify(|prices| {
                prices.insert(token_type, price);
            });
        })
        .await;

    Ok(timestamp)
}
