use anyhow::Result;
use file_store::{
    file_info_poller::{FileInfoStream, NoState},
    file_sink::FileSinkClient,
};
use futures::StreamExt;
use helium_proto::ServiceProviderPromotionFundV1;
use solana::carrier::SolanaRpc;

use crate::{compare_s3_and_solana_values, fetch_solana_bps, s3, Action, S3Value};

#[derive(Debug)]
pub enum StateEvent {
    Shutdown,
    Tick,
    NewS3(FileInfoStream<ServiceProviderPromotionFundV1>),
}

pub struct State {
    s3_current: S3Value,
    solana_client: SolanaRpc,
    file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
}

impl State {
    pub fn new(
        s3_current: S3Value,
        solana_client: SolanaRpc,
        file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
    ) -> Self {
        Self {
            s3_current,
            solana_client,
            file_sink,
        }
    }

    pub fn new_without_s3(
        solana_client: SolanaRpc,
        file_sink: FileSinkClient<ServiceProviderPromotionFundV1>,
    ) -> Self {
        Self {
            s3_current: Default::default(),
            solana_client,
            file_sink,
        }
    }

    pub async fn handle_tick(&self) -> Result<()> {
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
                s3::write_protos(solana_current, &self.file_sink).await?;
            }
        }

        Ok(())
    }

    pub async fn handle_new_s3(
        &mut self,
        file_info: FileInfoStream<ServiceProviderPromotionFundV1>,
    ) -> Result<()> {
        self.s3_current.clear();

        let mut file_stream = file_info.into_stream(NoState).await?;
        while let Some(promo_fund_v1) = file_stream.next().await {
            self.s3_current
                .insert(promo_fund_v1.service_provider, promo_fund_v1.bps);
        }

        Ok(())
    }
}
