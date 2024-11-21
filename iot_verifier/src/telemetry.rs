use std::cell::RefCell;

use crate::{poc_report::Report, rewarder};
use chrono::{DateTime, Utc};
use iot_config::EpochInfo;
use sqlx::{Pool, Postgres};

const PACKET_COUNTER: &str = concat!(env!("CARGO_PKG_NAME"), "_", "packet");
const NON_REWARDABLE_PACKET_COUNTER: &str =
    concat!(env!("CARGO_PKG_NAME"), "_", "non_rewardable_packet");
const LOADER_BEACON_COUNTER: &str = concat!(env!("CARGO_PKG_NAME"), "_", "loader_beacon");
const LOADER_WITNESS_COUNTER: &str = concat!(env!("CARGO_PKG_NAME"), "_", "loader_witness");
const LOADER_DROPPED_BEACON_COUNTER: &str = concat!(env!("CARGO_PKG_NAME"), "_", "dropped_beacon");
const LOADER_DROPPED_WITNESS_COUNTER: &str =
    concat!(env!("CARGO_PKG_NAME"), "_", "dropped_witness");
const BEACON_GUAGE: &str = concat!(env!("CARGO_PKG_NAME"), "_", "num_beacons");
const INVALID_WITNESS_COUNTER: &str =
    concat!(env!("CARGO_PKG_NAME"), "_", "invalid_witness_report");
const LAST_REWARDED_END_TIME: &str = "last_rewarded_end_time";

pub async fn initialize(db: &Pool<Postgres>) -> anyhow::Result<()> {
    let next_reward_epoch = rewarder::next_reward_epoch(db).await?;
    let epoch_period: EpochInfo = next_reward_epoch.into();
    last_rewarded_end_time(epoch_period.period.start);
    num_beacons(Report::count_all_beacons(db).await?);
    Ok(())
}

pub fn count_packets(count: u64) {
    metrics::counter!(PACKET_COUNTER).increment(count);
}

pub fn count_non_rewardable_packets(count: u64) {
    metrics::counter!(NON_REWARDABLE_PACKET_COUNTER).increment(count);
}

pub fn count_loader_beacons(count: u64) {
    metrics::counter!(LOADER_BEACON_COUNTER).increment(count);
}

pub fn count_loader_witnesses(count: u64) {
    metrics::counter!(LOADER_WITNESS_COUNTER).increment(count);
}

pub fn count_loader_dropped_beacons(count: u64, labels: &[(&'static str, &'static str)]) {
    metrics::counter!(LOADER_DROPPED_BEACON_COUNTER, labels).increment(count);
}

pub fn count_loader_dropped_witnesses(count: u64, labels: &[(&'static str, &'static str)]) {
    metrics::counter!(LOADER_DROPPED_WITNESS_COUNTER, labels).increment(count);
}

pub fn num_beacons(count: u64) {
    metrics::gauge!(BEACON_GUAGE).set(count as f64);
}

pub fn increment_num_beacons_by(count: u64) {
    metrics::gauge!(BEACON_GUAGE).increment(count as f64);
}

pub fn decrement_num_beacons() {
    metrics::gauge!(BEACON_GUAGE).decrement(1.0)
}

pub fn increment_invalid_witnesses(labels: &[(&'static str, &'static str)]) {
    metrics::counter!(INVALID_WITNESS_COUNTER, labels).increment(1);
}

pub fn last_rewarded_end_time(datetime: DateTime<Utc>) {
    metrics::gauge!(LAST_REWARDED_END_TIME).set(datetime.timestamp() as f64);
}

#[derive(Default)]
pub struct LoaderMetricTracker {
    beacons: RefCell<u64>,
    beacons_denied: RefCell<u64>,
    beacons_unknown: RefCell<u64>,
    witnesses: RefCell<u64>,
    witnesses_no_beacon: RefCell<u64>,
    witnesses_denied: RefCell<u64>,
    witnesses_unknown: RefCell<u64>,
    packets: RefCell<u64>,
    non_rewardable_packets: RefCell<u64>,
}

impl LoaderMetricTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_packets(&self) {
        *self.packets.borrow_mut() += 1;
    }

    pub fn increment_non_rewardable_packets(&self) {
        *self.non_rewardable_packets.borrow_mut() += 1;
    }

    pub fn increment_beacons(&self) {
        *self.beacons.borrow_mut() += 1;
    }

    pub fn increment_beacons_denied(&self) {
        *self.beacons_denied.borrow_mut() += 1;
    }

    pub fn increment_beacons_unknown(&self) {
        *self.beacons_unknown.borrow_mut() += 1;
    }

    pub fn increment_witnesses(&self) {
        *self.witnesses.borrow_mut() += 1;
    }

    pub fn increment_witnesses_no_beacon(&self) {
        *self.witnesses_no_beacon.borrow_mut() += 1;
    }

    pub fn increment_witnesses_denied(&self) {
        *self.witnesses_denied.borrow_mut() += 1;
    }

    pub fn increment_witnesses_unknown(&self) {
        *self.witnesses_unknown.borrow_mut() += 1;
    }

    pub fn record_metrics(self) {
        let beacons = self.beacons.into_inner();
        let beacons_denied = self.beacons_denied.into_inner();
        let beacons_unknown = self.beacons_unknown.into_inner();

        let witnesses = self.witnesses.into_inner();
        let witnesses_no_beacon = self.witnesses_no_beacon.into_inner();
        let witnesses_denied = self.witnesses_denied.into_inner();
        let witnesses_unknown = self.witnesses_unknown.into_inner();

        let packets = self.packets.into_inner();
        let non_rewardable_packets = self.non_rewardable_packets.into_inner();

        if packets > 0 {
            count_packets(packets);
        }

        if non_rewardable_packets > 0 {
            count_non_rewardable_packets(packets);
        }

        if beacons > 0 {
            count_loader_beacons(beacons);
            increment_num_beacons_by(beacons);
        }

        if beacons_denied > 0 {
            count_loader_dropped_beacons(beacons_denied, &[("status", "ok"), ("reason", "denied")]);
        }

        if beacons_unknown > 0 {
            count_loader_dropped_beacons(
                beacons_unknown,
                &[("status", "ok"), ("reason", "gateway_not_found")],
            );
        }

        if witnesses > 0 {
            count_loader_witnesses(witnesses);
        }

        if witnesses_no_beacon > 0 {
            count_loader_dropped_witnesses(
                witnesses_no_beacon,
                &[("status", "ok"), ("reason", "no_associated_beacon_data")],
            );
        }

        if witnesses_denied > 0 {
            count_loader_dropped_witnesses(
                witnesses_denied,
                &[("status", "ok"), ("reason", "denied")],
            );
        }

        if witnesses_unknown > 0 {
            count_loader_dropped_witnesses(
                witnesses_unknown,
                &[("status", "ok"), ("reason", "gateway_not_found")],
            );
        }
    }
}
