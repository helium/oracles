const RPC_METRIC: &str = concat!(env!("CARGO_PKG_NAME"), "-", "grpc-request");
const GATEWAY_CHAIN_LOOKUP_METRIC: &str =
    concat!(env!("CARGO_PKG_NAME"), "-", "gateway-chain-lookup");

pub fn count_request(service: &'static str, rpc: &'static str) {
    metrics::counter!(RPC_METRIC, "service" => service, "rpc" => rpc).increment(1);
}

pub fn count_gateway_chain_lookup(result: &'static str) {
    metrics::counter!(GATEWAY_CHAIN_LOOKUP_METRIC, "result" => result).increment(1);
}
