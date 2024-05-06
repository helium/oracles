use helium_proto::BlockchainTokenTypeV1;

const PRICE_GAUGE: &str = concat!(env!("CARGO_PKG_NAME"), "_", "price_gauge");

pub struct Metrics;

impl Metrics {
    pub fn update(counter: String, token_type: BlockchainTokenTypeV1, price: f64) {
        increment_counter(counter, token_type);
        set_gauge(token_type, price)
    }
}

fn increment_counter(counter: String, token_type: BlockchainTokenTypeV1) {
    metrics::counter!(counter, "token_type" => token_type.as_str_name()).increment(1);
}

fn set_gauge(token_type: BlockchainTokenTypeV1, value: f64) {
    metrics::gauge!(PRICE_GAUGE, "token_type" => token_type.as_str_name()).set(value);
}
