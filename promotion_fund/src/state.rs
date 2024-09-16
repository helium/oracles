use std::time::Duration;

use anyhow::Result;
use file_store::file_sink::FileSinkClient;
use futures::TryFutureExt;
use helium_proto::ServiceProviderPromotionFundV1;
use solana::carrier::SolanaRpc;
use task_manager::ManagedTask;
use tokio::time::{self, Interval};

use crate::{compare_s3_and_solana_values, fetch_s3_bps, fetch_solana_bps, Action, S3Value};

pub struct State {
    s3_current: S3Value,
    solana_client: SolanaRpc,
    file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
    solana_check_interval: Interval,
}

impl ManagedTask for State {
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

impl State {
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

    pub async fn fetch_s3_bps(settings: &file_store::Settings) -> anyhow::Result<S3Value> {
        let file_store = file_store::FileStore::from_settings(settings).await?;
        fetch_s3_bps(file_store).await
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
            Ok(solana_current) => solana_current,
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
                self.write_protos(solana_current).await?;
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

    async fn write_protos(
        &self,
        promo_funds: Vec<ServiceProviderPromotionFundV1>,
    ) -> anyhow::Result<()> {
        for proto in promo_funds {
            self.file_sink.write(proto, []).await?.await??;
        }
        self.file_sink.commit().await?.await??;
        Ok(())
    }
}
