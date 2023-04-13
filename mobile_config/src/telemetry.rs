const RPC_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-request");

pub fn count_request(service: &'static str, rpc: &'static str) {
    metrics::increment_counter!(RPC_METRIC, "service" => service, "rpc" => rpc);
}
