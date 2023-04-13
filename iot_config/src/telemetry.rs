const RPC_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-request");
const STREAM_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-stream");
const REGION_HEX_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "region-hexes");
const REGION_LOOKUP_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "region-lookup");
const SKF_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "skfs-added");
const SKF_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "skfs-removed");
const EUI_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "euis-added");
const EUI_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "euis-removed");
const DEVADDR_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "devaddrs-added");
const DEVADDR_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "devaddrs-removed");

pub fn count_request(service: &'static str, rpc: &'static str) {
    metrics::increment_counter!(RPC_METRIC, "service" => service, "rpc" => rpc);
}

pub fn gauge_hexes(cells: usize) {
    metrics::gauge!(REGION_HEX_METRIC, cells as f64);
}

pub fn count_region_lookup(
    default_region: helium_proto::Region,
    reported_region: helium_proto::Region,
) {
    metrics::increment_counter!(
        REGION_LOOKUP_METRIC,
        // per metrics docs, &str should be preferred for performance; should the regions be
        // mapped through a match of region => &'static str of the name?
        "default_region" => default_region.to_string(), "reported_region" => reported_region.to_string()
    );
}

pub fn count_skf_updates(adds: usize, removes: usize) {
    metrics::counter!(SKF_ADD_COUNT_METRIC, adds as u64);
    metrics::counter!(SKF_REMOVE_COUNT_METRIC, removes as u64);
}

pub fn count_eui_updates(adds: usize, removes: usize) {
    metrics::counter!(EUI_ADD_COUNT_METRIC, adds as u64);
    metrics::counter!(EUI_REMOVE_COUNT_METRIC, removes as u64);
}

pub fn count_devaddr_updates(adds: usize, removes: usize) {
    metrics::counter!(DEVADDR_ADD_COUNT_METRIC, adds as u64);
    metrics::counter!(DEVADDR_REMOVE_COUNT_METRIC, removes as u64);
}

pub fn stream_subscribe(stream: &'static str) {
    metrics::increment_gauge!(STREAM_METRIC, 1.0, "stream" => stream);
}

pub fn stream_unsubscribe(stream: &'static str) {
    metrics::decrement_gauge!(STREAM_METRIC, 1.0, "stream" => stream);
}
