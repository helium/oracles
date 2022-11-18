use crate::{
    lora_beacon_report::{LoraBeaconIngestReport, LoraBeaconReport},
    lora_witness_report::{LoraWitnessIngestReport, LoraWitnessReport},
};
use blake3::hash;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
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
                let mut id: Vec<u8> = self.report.data.clone();
                let mut public_key = self.report.pub_key.to_vec();
                id.append(&mut self.received_timestamp.to_string().as_bytes().to_vec());
                id.append(&mut public_key);
                blake3::hash(&id).as_bytes().to_vec()
                // Sha256::digest(&id).to_vec()
            }
        }
    };
}

macro_rules! impl_report_id {
    ($report_type:ty) => {
        impl ReportId for $report_type {
            fn report_id(&self, received_ts: DateTime<Utc>) -> Vec<u8> {
                let mut id: Vec<u8> = self.data.clone();
                let mut public_key = self.pub_key.to_vec();
                id.append(&mut received_ts.to_string().as_bytes().to_vec());
                id.append(&mut public_key);
                blake3::hash(&id).as_bytes().to_vec()
                // Sha256::digest(&id).to_vec()
            }
        }
    };
}

impl_ingest_id!(LoraBeaconIngestReport);
impl_ingest_id!(LoraWitnessIngestReport);
impl_report_id!(LoraBeaconReport);
impl_report_id!(LoraWitnessReport);
