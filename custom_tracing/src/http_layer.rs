use axum::{
    body::Body,
    http::{header, Request},
};
use tower_http::{
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tower_layer::Layer;
use tracing::Level;

pub fn new<T>() -> impl Layer<T> {
    TraceLayer::new_for_http()
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
        )
}

fn make_span(request: &Request<Body>) -> tracing::Span {
    let user_agent = request
        .headers()
        .get(header::USER_AGENT)
        .map(|value| value.to_str().unwrap_or("unknown"));

    let app_version = request
        .headers()
        .get("X-App-Version")
        .map(|value| value.to_str().unwrap_or("unknown"));

    tracing::info_span!(
        "tracing",
        user_agent,
        app_version,
        uri = %request.uri(),
        solana_addr = tracing::field::Empty,
        subscriber = tracing::field::Empty,
    )
}
