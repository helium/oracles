use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use chrono::Utc;
use file_store::file_info_poller::{self, FileInfoStream};
use futures::StreamExt;
use helium_proto::{IntoEnumIterator, ServiceProvider, ServiceProviderPromotionFundV1};
use solana::carrier::{SolanaNetwork, SolanaRpc};
use tokio::{sync::mpsc::Receiver, time::timeout};

pub mod s3;
pub mod settings;
pub mod state;

type ServiceProviderInt = i32;
type BasisPoints = u32;

type S3Receiver = Receiver<FileInfoStream<ServiceProviderPromotionFundV1>>;
type S3Value = HashMap<ServiceProviderInt, BasisPoints>;
type SolanaValue = Vec<ServiceProviderPromotionFundV1>;

#[derive(Debug, PartialEq)]
pub enum Action {
    Write,
    Noop,
}

pub fn compare_s3_and_solana_values(s3_current: &S3Value, solana_current: &SolanaValue) -> Action {
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

pub async fn fetch_s3_bps(s3_receiver: &mut S3Receiver) -> anyhow::Result<S3Value> {
    // Files are read from oldest to newest. When more than one file is
    // within the lookback time the lastest value will always be returned
    // for a service provider.
    let mut results = HashMap::new();
    let wait = Duration::from_secs(2);

    while let Ok(Some(file_info)) = timeout(wait, s3_receiver.recv()).await {
        let mut stream = file_info.into_stream(file_info_poller::NoState).await?;
        while let Some(sp_promo_fund_v1) = stream.next().await {
            results.insert(sp_promo_fund_v1.service_provider, sp_promo_fund_v1.bps);
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
