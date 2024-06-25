use helium_lib::token::Token;

const PRICE_GAUGE: &str = concat!(env!("CARGO_PKG_NAME"), "_", "price_gauge");

pub struct Metrics;

impl Metrics {
    pub fn update(counter: String, token: Token, price: f64) {
        increment_counter(counter, token);
        set_gauge(token, price)
    }
}

fn increment_counter(counter: String, token: Token) {
    metrics::counter!(counter, "token_type" => token.to_string()).increment(1);
}

fn set_gauge(token: Token, value: f64) {
    metrics::gauge!(PRICE_GAUGE, "token_type" => token.to_string()).set(value);
}
