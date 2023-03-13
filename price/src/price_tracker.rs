use anyhow::{anyhow, Error, Result};
use chrono::{DateTime, Duration, Utc};
use file_store::{FileInfo, FileStore, FileType, Settings};
use futures::stream::{StreamExt, TryStreamExt};
use helium_proto::{BlockchainTokenTypeV1, Message, PriceReportV1};
use std::collections::HashMap;
use tokio::sync::watch;

type Prices = HashMap<BlockchainTokenTypeV1, u64>;

#[derive(Clone)]
pub struct PriceTracker {
    receiver: watch::Receiver<Prices>,
}

impl PriceTracker {
    pub async fn start(
        settings: &Settings,
        shutdown: triggered::Listener,
    ) -> Result<(Self, impl std::future::Future<Output = Result<()>>)> {
        let file_store = FileStore::from_settings(settings).await?;
        let (sender, receiver) = watch::channel(Prices::new());
        let initial_timestamp = calculate_initial_prices(&file_store, &sender).await?;

        let handle =
            tokio::spawn(async move { run(file_store, sender, initial_timestamp, shutdown).await });

        Ok((PriceTracker { receiver }, async move {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err),
                Err(err) => Err(Error::from(err)),
            }
        }))
    }

    pub async fn price(&self, token_type: &BlockchainTokenTypeV1) -> Result<u64> {
        self.receiver
            .borrow()
            .get(token_type)
            .cloned()
            .ok_or_else(|| anyhow!("price not available"))
    }
}

async fn run(
    file_store: FileStore,
    sender: watch::Sender<Prices>,
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
                let timestamp = process_files(&file_store, &sender, after).await?;
                after = timestamp.unwrap_or(after);
            }
        }
    }

    Ok(())
}

async fn calculate_initial_prices(
    file_store: &FileStore,
    sender: &watch::Sender<Prices>,
) -> Result<DateTime<Utc>> {
    tracing::debug!("PriceTracker: Updating initial prices");
    for duration in [Duration::minutes(10), Duration::hours(4)] {
        let timestamp = process_files(file_store, sender, Utc::now() - duration).await?;

        if timestamp.is_some() {
            return Ok(timestamp.unwrap());
        }
    }

    Err(anyhow!("price not available"))
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
        .map_err(|err| {
            tracing::warn!("PriceTracker: skipping price report due to error {err:?}");
            err
        })
        .filter_map(|result| async { result.ok() })
        .for_each(|report| async move {
            sender.send_if_modified(
                |prices: &mut Prices| match prices.get(&report.token_type()) {
                    Some(price) if price == &report.price => false,
                    _ => {
                        prices.insert(report.token_type(), report.price);
                        true
                    }
                },
            );
        })
        .await;

    Ok(timestamp)
}
