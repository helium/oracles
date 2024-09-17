use std::collections::HashMap;

use file_store::{
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileSink,
};
use helium_proto::ServiceProviderPromotionFundV1;
use settings::Settings;

pub mod daemon;
pub mod settings;

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
    use helium_proto::ServiceProvider;

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
