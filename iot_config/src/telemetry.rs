const RPC_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-request");
const STREAM_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-stream");
const STREAM_THROTTLE_COUNT_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "grpc-stream-throttle");
const REGION_HEX_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "region-hexes");
const REGION_LOOKUP_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "region-lookup");
const SKF_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "skfs-added");
const SKF_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "skfs-removed");
const EUI_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "euis-added");
const EUI_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "euis-removed");
const DEVADDR_ADD_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "devaddrs-added");
const DEVADDR_REMOVE_COUNT_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "devaddrs-removed");
const GATEWAY_CHAIN_LOOKUP_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "gateway-info-lookup");
const GATEWAY_CHAIN_LOOKUP_DURATION_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "gateway-info-lookup-duration");

const EPOCH_CHAIN_LOOKUP_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "epoch-chain-lookup");

pub fn initialize() {
    metrics::gauge!(STREAM_METRIC).set(0.0);
}

pub fn count_request(service: &'static str, rpc: &'static str) {
    metrics::counter!(RPC_METRIC, "service" => service, "rpc" => rpc).increment(1);
}

pub fn count_gateway_info_lookup(result: &'static str) {
    metrics::counter!(GATEWAY_CHAIN_LOOKUP_METRIC, "result" => result).increment(1);
}

pub fn gauge_hexes(cells: usize) {
    metrics::gauge!(REGION_HEX_METRIC).set(cells as f64);
}

pub fn count_region_lookup(
    default_region: helium_proto::Region,
    reported_region: Option<helium_proto::Region>,
) {
    let reported_region =
        reported_region.map_or_else(|| "LOOKUP_FAILED".to_string(), |region| region.to_string());
    metrics::counter!(
        REGION_LOOKUP_METRIC,
        // per metrics docs, &str should be preferred for performance; should the regions be
        // mapped through a match of region => &'static str of the name?
        "default_region" => default_region.to_string(), "reported_region" => reported_region
    )
    .increment(1);
}

pub fn duration_gateway_info_lookup(start: std::time::Instant) {
    metrics::histogram!(GATEWAY_CHAIN_LOOKUP_DURATION_METRIC).record(start.elapsed());
}

pub fn count_skf_updates(adds: usize, removes: usize) {
    metrics::counter!(SKF_ADD_COUNT_METRIC).increment(adds as u64);
    metrics::counter!(SKF_REMOVE_COUNT_METRIC).increment(removes as u64);
}

pub fn count_eui_updates(adds: usize, removes: usize) {
    metrics::counter!(EUI_ADD_COUNT_METRIC).increment(adds as u64);
    metrics::counter!(EUI_REMOVE_COUNT_METRIC).increment(removes as u64);
}

pub fn count_devaddr_updates(adds: usize, removes: usize) {
    metrics::counter!(DEVADDR_ADD_COUNT_METRIC).increment(adds as u64);
    metrics::counter!(DEVADDR_REMOVE_COUNT_METRIC).increment(removes as u64);
}

pub fn count_epoch_chain_lookup(result: &'static str) {
    metrics::counter!(EPOCH_CHAIN_LOOKUP_METRIC, "result" => result).increment(1);
}

pub fn route_stream_subscribe() {
    metrics::gauge!(STREAM_METRIC).increment(1.0);
}

pub fn route_stream_unsubscribe() {
    metrics::gauge!(STREAM_METRIC).decrement(1.0);
}

pub fn route_stream_throttle() {
    metrics::counter!(STREAM_THROTTLE_COUNT_METRIC).increment(1);
}
