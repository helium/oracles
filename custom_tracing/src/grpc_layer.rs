use tower_http::{
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tower_layer::Layer;
use tracing::Level;

pub fn new<T>() -> impl Layer<T> {
    TraceLayer::new_for_grpc()
        .make_span_with(make_span)
        .on_response(
            DefaultOnResponse::new()
                .level(Level::DEBUG)
                .latency_unit(LatencyUnit::Micros),
        )
        .on_failure(
            DefaultOnFailure::new()
                .level(Level::WARN)
                .latency_unit(LatencyUnit::Micros),
        );
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(
        "tracing",
        pub_key = tracing::field::Empty,
        subscriber_id = tracing::field::Empty,
    )
}
