use std::collections::HashMap;

use anyhow::Context;
use chrono::Utc;
use file_store::{
    file_info_poller::{FileInfoPollerParser, ProstFileInfoPollerParser},
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileSink, FileStore, FileType,
};
use helium_proto::{IntoEnumIterator, ServiceProvider, ServiceProviderPromotionFundV1};
use settings::Settings;
use solana::carrier::{SolanaNetwork, SolanaRpc};

pub mod settings;
pub mod state;

type ServiceProviderInt = i32;
type BasisPoints = u32;

type S3Value = HashMap<ServiceProviderInt, BasisPoints>;
type SolanaValue = Vec<ServiceProviderPromotionFundV1>;

#[derive(Debug, PartialEq)]
pub enum Action {
    Write,
    Noop,
}

fn compare_s3_and_solana_values(s3_current: &S3Value, solana_current: &SolanaValue) -> Action {
    for sp_fund in solana_current {
        // A Service Provider missing from the S3 file only
        // matters if their Solana BPS is >0.
        let s3_bps = s3_current.get(&sp_fund.service_provider).unwrap_or(&0);
        if s3_bps != &sp_fund.bps {
            return Action::Write;
        }
    }

    Action::Noop
}

async fn fetch_s3_bps(file_store: FileStore) -> anyhow::Result<S3Value> {
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

async fn fetch_solana_bps(client: &SolanaRpc) -> anyhow::Result<SolanaValue> {
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

pub async fn make_promotion_fund_file_sink(
    settings: &Settings,
    upload: FileUpload,
) -> anyhow::Result<(
    FileSinkClient<ServiceProviderPromotionFundV1>,
    FileSink<ServiceProviderPromotionFundV1>,
)> {
    let (sink, sink_server) = ServiceProviderPromotionFundV1::file_sink(
        &settings.file_sink_cache,
        upload,
        FileSinkCommitStrategy::Manual,
        FileSinkRollTime::Default,
        env!("CARGO_PKG_NAME"),
    )
    .await?;
    Ok((sink, sink_server))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_when_nothing_in_s3_or_solana() {
        let action = compare_s3_and_solana_values(&HashMap::new(), &vec![]);
        assert_eq!(Action::Noop, action);
    }

    #[test]
    fn noop_when_new_solana_value_is_zero() {
        let s3_promos = HashMap::from_iter([]);
        let solana_promos = vec![ServiceProviderPromotionFundV1 {
            timestamp: 0,
            service_provider: ServiceProvider::HeliumMobile as i32,
            bps: 0,
        }];

        let action = compare_s3_and_solana_values(&s3_promos, &solana_promos);
        assert_eq!(Action::Noop, action);
    }

    #[test]
    fn noop_when_values_are_the_same() {
        let s3_promos = HashMap::from_iter([(ServiceProvider::HeliumMobile as i32, 1)]);
        let solana_promos = vec![ServiceProviderPromotionFundV1 {
            timestamp: 0,
            service_provider: ServiceProvider::HeliumMobile as i32,
            bps: 1,
        }];

        let action = compare_s3_and_solana_values(&s3_promos, &solana_promos);
        assert_eq!(Action::Noop, action);
    }

    #[test]
    fn write_when_new_values_from_solana() {
        let sp_promos = vec![ServiceProviderPromotionFundV1 {
            timestamp: 0,
            service_provider: ServiceProvider::HeliumMobile as i32,
            bps: 1,
        }];

        let action = compare_s3_and_solana_values(&HashMap::new(), &sp_promos);
        assert_eq!(Action::Write, action);
    }

    #[test]
    fn write_when_solana_differs_from_s3() {
        let s3_promos = HashMap::from_iter([(ServiceProvider::HeliumMobile as i32, 1)]);
        let solana_promos = vec![ServiceProviderPromotionFundV1 {
            timestamp: 0,
            service_provider: ServiceProvider::HeliumMobile as i32,
            bps: 2,
        }];

        let action = compare_s3_and_solana_values(&s3_promos, &solana_promos);
        assert_eq!(Action::Write, action);
    }

    #[test]
    fn all_items_written_when_one_is_different() {
        let s3_promos = HashMap::from_iter([(ServiceProvider::HeliumMobile as i32, 1), (1, 1)]);
        let solana_promos = vec![
            ServiceProviderPromotionFundV1 {
                timestamp: 0,
                service_provider: ServiceProvider::HeliumMobile as i32,
                bps: 2,
            },
            ServiceProviderPromotionFundV1 {
                timestamp: 0,
                service_provider: 1,
                bps: 1,
            },
        ];

        let action = compare_s3_and_solana_values(&s3_promos, &solana_promos);
        assert_eq!(Action::Write, action);
    }
}
