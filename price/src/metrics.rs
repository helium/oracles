use helium_proto::BlockchainTokenTypeV1;

const PRICE_UPDATE_COUNTER: &str = concat!(env!("CARGO_PKG_NAME"), "_", "price_update_counter");
const PRICE_GAUGE: &str = concat!(env!("CARGO_PKG_NAME"), "_", "price_gauge");

pub struct Metrics;

impl Metrics {
    pub fn update(token_type: BlockchainTokenTypeV1, price: f64) {
        increment_counter(token_type);
        set_gauge(token_type, price)
    }
}

fn increment_counter(token_type: BlockchainTokenTypeV1) {
    metrics::increment_counter!(PRICE_UPDATE_COUNTER, "token_type" => token_type.as_str_name());
}

fn set_gauge(token_type: BlockchainTokenTypeV1, value: f64) {
    metrics::gauge!(PRICE_GAUGE, value, "token_type" => token_type.as_str_name());
}
