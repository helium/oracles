const PENDING_TXNS_COUNTER: &str = "mobile_rewards_pending_txns";
const CLEARED_TXNS_COUNTER: &str = "mobile_rewards_cleared_txns";
const TXN_STREAM_ERROR_COUNTER: &str = "mobile_rewards_txn_stream_error_count";

pub fn register_metrics() {
    metrics::register_counter!(PENDING_TXNS_COUNTER);
    metrics::register_counter!(CLEARED_TXNS_COUNTER);
    metrics::register_counter!(TXN_STREAM_ERROR_COUNTER);
}

pub fn increment_pending_txns() {
    metrics::increment_counter!(PENDING_TXNS_COUNTER);
}

pub fn increment_cleared_txns() {
    metrics::increment_counter!(CLEARED_TXNS_COUNTER);
}

pub fn increment_txn_stream_errors() {
    metrics::increment_counter!(TXN_STREAM_ERROR_COUNTER);
}
