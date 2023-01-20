use std::cell::RefCell;

const LOADER_BEACON_COUNTER: &str = "oracles_iot_verifier_loader_beacon";
const LOADER_WITNESS_COUNTER: &str = "oracles_iot_verifier_loader_witness";
const LOADER_DROPPED_BEACON_COUNTER: &str = "iot_verifier_dropped_beacon";
const LOADER_DROPPED_WITNESS_COUNTER: &str = "iot_verifier_dropped_witness";
const INVALID_WITNESS_COUNTER: &str = "iot_verifier_invalid_witness_report";
const BEACON_GUAGE: &str = "oracles_iot_verifier_num_beacons";

pub struct Metrics;

impl Metrics {
    pub fn count_loader_beacons(count: u64) {
        metrics::counter!(LOADER_BEACON_COUNTER, count);
    }

    pub fn count_loader_witnesses(count: u64) {
        metrics::counter!(LOADER_WITNESS_COUNTER, count);
    }

    pub fn count_loader_dropped_beacons(count: u64, labels: &[(&'static str, &'static str)]) {
        metrics::counter!(LOADER_DROPPED_BEACON_COUNTER, count, labels);
    }

    pub fn count_loader_dropped_witnesses(count: u64, labels: &[(&'static str, &'static str)]) {
        metrics::counter!(LOADER_DROPPED_WITNESS_COUNTER, count, labels);
    }

    pub fn count_loader_invalid_witnesses(count: u64, labels: &[(&'static str, &'static str)]) {
        metrics::counter!(INVALID_WITNESS_COUNTER, count, labels);
    }

    pub fn num_beacons(count: u64) {
        metrics::gauge!(BEACON_GUAGE, count as f64);
    }

    pub fn increment_num_beacons_by(count: u64) {
        metrics::increment_gauge!(BEACON_GUAGE, count as f64);
    }

    pub fn decrement_num_beacons() {
        metrics::decrement_gauge!(BEACON_GUAGE, 1.0)
    }
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
}

impl LoaderMetricTracker {
    pub fn new() -> Self {
        Self::default()
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

        if beacons > 0 {
            Metrics::count_loader_beacons(beacons);
            Metrics::increment_num_beacons_by(beacons);
        }

        if beacons_denied > 0 {
            Metrics::count_loader_dropped_beacons(
                beacons_denied,
                &[("status", "ok"), ("reason", "denied")],
            );
        }

        if beacons_unknown > 0 {
            Metrics::count_loader_dropped_beacons(
                beacons_unknown,
                &[("status", "ok"), ("reason", "gateway not found")],
            );
        }

        if witnesses > 0 {
            Metrics::count_loader_witnesses(witnesses);
        }

        if witnesses_no_beacon > 0 {
            Metrics::count_loader_invalid_witnesses(
                witnesses_no_beacon,
                &[("status", "ok"), ("reason", "no_associated_beacon_data")],
            );
        }

        if witnesses_denied > 0 {
            Metrics::count_loader_dropped_witnesses(
                witnesses_denied,
                &[("status", "ok"), ("reason", "denied")],
            );
        }

        if witnesses_unknown > 0 {
            Metrics::count_loader_dropped_witnesses(
                witnesses_unknown,
                &[("status", "ok"), ("reason", "gateway not found")],
            );
        }
    }
}
