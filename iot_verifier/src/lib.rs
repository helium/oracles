extern crate tls_init;

pub mod entropy;
pub mod entropy_loader;
pub mod gateway_cache;
pub mod gateway_updater;
pub mod hex_density;
pub mod last_beacon;
pub mod last_beacon_reciprocity;
pub mod last_witness;
pub mod loader;
pub mod meta;
pub mod packet_loader;
pub mod poc;
pub mod poc_report;
pub mod purger;
pub mod region_cache;
pub mod reward_share;
pub mod rewarder;
pub mod runner;
mod settings;
pub mod telemetry;
pub mod tx_scaler;
pub mod witness_updater;

use blake3::Hasher;
use chrono::{DateTime, Utc};
use file_store::traits::TimestampEncode;
use file_store_oracles::{
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_witness_report::{IotWitnessIngestReport, IotWitnessReport},
};
use rust_decimal::Decimal;
pub use settings::Settings;
use solana::SolPubkey;

#[derive(Clone, Debug)]
pub struct PriceInfo {
    pub price_in_bones: u64,
    pub price_per_token: Decimal,
    pub price_per_bone: Decimal,
    pub decimals: u8,
}

impl PriceInfo {
    pub fn new(price_in_bones: u64, decimals: u8) -> Self {
        let price_per_token =
            Decimal::from(price_in_bones) / Decimal::from(10_u64.pow(decimals as u32));
        let price_per_bone = price_per_token / Decimal::from(10_u64.pow(decimals as u32));
        Self {
            price_in_bones,
            price_per_token,
            price_per_bone,
            decimals,
        }
    }
}

pub fn resolve_subdao_pubkey() -> SolPubkey {
    solana::SubDao::Iot.key()
}

#[cfg(test)]
tls_init::include_tls_tests!();

pub trait IngestId {
    fn ingest_id(&self) -> Vec<u8>;
}

pub trait ReportId {
    fn report_id(&self, received_ts: DateTime<Utc>) -> Vec<u8>;
}

macro_rules! impl_ingest_id {
    ($report_type:ty) => {
        impl IngestId for $report_type {
            fn ingest_id(&self) -> Vec<u8> {
                let mut hasher = Hasher::new();
                hasher.update(&self.report.data);
                hasher.update(
                    &self
                        .received_timestamp
                        .encode_timestamp_millis()
                        .to_be_bytes(),
                );
                hasher.update(self.report.pub_key.as_ref());
                hasher.finalize().as_bytes().to_vec()
            }
        }
    };
}

macro_rules! impl_report_id {
    ($report_type:ty) => {
        impl ReportId for $report_type {
            fn report_id(&self, received_ts: DateTime<Utc>) -> Vec<u8> {
                let mut hasher = Hasher::new();
                hasher.update(&self.data);
                hasher.update(&received_ts.encode_timestamp_millis().to_be_bytes());
                hasher.update(self.pub_key.as_ref());
                hasher.finalize().as_bytes().to_vec()
            }
        }
    };
}

impl_ingest_id!(IotBeaconIngestReport);
impl_ingest_id!(IotWitnessIngestReport);
impl_report_id!(IotBeaconReport);
impl_report_id!(IotWitnessReport);
