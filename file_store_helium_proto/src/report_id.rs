use crate::{
    iot_beacon_report::{IotBeaconIngestReport, IotBeaconReport},
    iot_witness_report::{IotWitnessIngestReport, IotWitnessReport},
    traits::TimestampEncode,
};
use blake3::Hasher;
use chrono::{DateTime, Utc};
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
