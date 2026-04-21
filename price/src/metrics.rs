const PRICE_GAUGE: &str = concat!(env!("CARGO_PKG_NAME"), "_", "price_gauge");
const TOKEN_TYPE: &str = "hnt";

pub struct Metrics;

impl Metrics {
    pub fn update(counter: String, price: f64) {
        increment_counter(counter);
        set_gauge(price);
    }
}

fn increment_counter(counter: String) {
    metrics::counter!(counter, "token_type" => TOKEN_TYPE).increment(1);
}

fn set_gauge(value: f64) {
    metrics::gauge!(PRICE_GAUGE, "token_type" => TOKEN_TYPE).set(value);
}
