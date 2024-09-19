use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use chrono::Utc;
use file_store::{
    file_info_poller::{FileInfoPollerParser, ProstFileInfoPollerParser},
    file_sink::FileSinkClient,
    FileStore, FileType,
};
use futures::TryFutureExt;
use helium_proto::{IntoEnumIterator, ServiceProvider, ServiceProviderPromotionFundV1};
use solana::carrier::SolanaRpc;
use task_manager::ManagedTask;
use tokio::time::{self, Interval};

use crate::{compare_s3_and_solana_values, settings::Settings, Action, S3Value, SolanaValue};

const PROMOTION_FUND_LAST_SOLANA_FETCH_TIME: &str = "promotion_fund_last_solana_fetch_time";

pub struct Daemon {
    s3_current: S3Value,
    solana_client: SolanaRpc,
    file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
    solana_check_interval: Interval,
}

impl ManagedTask for Daemon {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> futures::future::LocalBoxFuture<'static, anyhow::Result<()>> {
        let handle = tokio::spawn(self.run(shutdown));

        Box::pin(
            handle
                .map_err(anyhow::Error::from)
                .and_then(|result| async move { result.map_err(anyhow::Error::from) }),
        )
    }
}

impl Daemon {
    pub fn new(
        s3_current: S3Value,
        solana_client: SolanaRpc,
        file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
        solana_check_interval: Option<Interval>,
    ) -> Self {
        Self {
            s3_current,
            solana_client,
            file_sink,
            solana_check_interval: solana_check_interval.unwrap_or(time::interval(Duration::MAX)),
        }
    }

    pub async fn from_settings(
        settings: &Settings,
        file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
    ) -> anyhow::Result<Self> {
        let s3_current = fetch_s3_bps(&settings.file_store_output).await?;
        let solana_client = SolanaRpc::new(&settings.solana).context("making solana client")?;
        let check_timer = tokio::time::interval(settings.solana_check_interval);

        Ok(Self::new(
            s3_current,
            solana_client,
            file_sink,
            Some(check_timer),
        ))
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.clone() => break,
                _ = self.solana_check_interval.tick() => self.handle_tick().await?
            }
        }

        Ok(())
    }

    pub async fn handle_tick(&mut self) -> Result<()> {
        let solana_current = match fetch_solana_bps(&self.solana_client).await {
            Ok(solana_current) => {
                metrics::gauge!(PROMOTION_FUND_LAST_SOLANA_FETCH_TIME)
                    .set(Utc::now().timestamp() as f64);
                solana_current
            }
            Err(err) => {
                tracing::error!(?err, "failed to get bps from solana");
                return Ok(());
            }
        };

        let action = compare_s3_and_solana_values(&self.s3_current, &solana_current);
        match action {
            Action::Noop => tracing::info!("nothing to do"),
            Action::Write => {
                tracing::info!(items = solana_current.len(), "writing new file");
                self.store_solana_values(&solana_current);
                write_protos(&self.file_sink, solana_current).await?;
            }
        }

        Ok(())
    }

    fn store_solana_values(&mut self, promo_funds: &[ServiceProviderPromotionFundV1]) {
        self.s3_current.clear();

        for promo_fund_v1 in promo_funds {
            self.s3_current
                .insert(promo_fund_v1.service_provider, promo_fund_v1.bps);
        }
    }
}

pub async fn fetch_s3_bps(settings: &file_store::Settings) -> anyhow::Result<S3Value> {
    let file_store = FileStore::from_settings(settings).await?;
    let mut results = HashMap::new();

    let all = file_store
        .list_all(FileType::ServiceProviderPromotionFund.to_str(), None, None)
        .await?;

    if let Some(last) = all.last() {
        let byte_stream = file_store.get_raw(&last.key).await?;
        let data: Vec<ServiceProviderPromotionFundV1> =
            ProstFileInfoPollerParser.parse(byte_stream).await?;
        for sp_promo_fund in data {
            results.insert(sp_promo_fund.service_provider, sp_promo_fund.bps);
        }
    }

    Ok(results)
}

pub async fn fetch_solana_bps(client: &SolanaRpc) -> anyhow::Result<SolanaValue> {
    let mut results = Vec::new();
    for service_provider in ServiceProvider::iter() {
        let bps = client
            .fetch_incentive_escrow_fund_bps(&service_provider.to_string())
            .await
            .with_context(|| format!("fetching solana bps for {service_provider:?}"))?;

        let proto = ServiceProviderPromotionFundV1 {
            timestamp: Utc::now().timestamp_millis() as u64,
            service_provider: service_provider.into(),
            bps: bps as u32,
        };
        results.push(proto);
    }

    Ok(results)
}

pub async fn write_protos(
    file_sink: &FileSinkClient<ServiceProviderPromotionFundV1>,
    promo_funds: Vec<ServiceProviderPromotionFundV1>,
) -> anyhow::Result<()> {
    for proto in promo_funds {
        file_sink.write(proto, []).await?.await??;
    }
    file_sink.commit().await?.await??;
    Ok(())
}
