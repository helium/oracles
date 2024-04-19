use helium_proto::services::Body;
use http::request::Request;
use tower_http::{
    classify::{GrpcErrorsAsFailures, SharedClassifier},
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{Level, Span};

pub fn new(
    make_span: fn(&Request<Body>) -> Span,
) -> TraceLayer<SharedClassifier<GrpcErrorsAsFailures>, for<'a> fn(&'a http::Request<Body>) -> Span>
{
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
        )
}
