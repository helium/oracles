use axum::{body::Body, http::Request};
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{Level, Span};

pub fn new_with_span(
    make_span: fn(&Request<Body>) -> Span,
) -> TraceLayer<
    SharedClassifier<ServerErrorsAsFailures>,
    for<'a> fn(&'a axum::http::Request<axum::body::Body>) -> Span,
> {
    TraceLayer::new_for_http()
        .make_span_with(make_span)
        .on_response(
            DefaultOnResponse::new()
                .level(Level::DEBUG)
                .latency_unit(LatencyUnit::Millis),
        )
        .on_failure(
            DefaultOnFailure::new()
                .level(Level::WARN)
                .latency_unit(LatencyUnit::Millis),
        )
}
