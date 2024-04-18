use helium_proto::services::Body;
use http::request::Request;
use tower_http::{
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tower_layer::Layer;
use tracing::{Level, Span};

pub fn new<T>(make_span: &dyn Fn(&Request<Body>) -> Span) -> impl Layer<T> {
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
