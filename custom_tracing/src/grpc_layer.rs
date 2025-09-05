use http::Request;
use tonic::body::Body;
use tower_http::{
    classify::{GrpcErrorsAsFailures, SharedClassifier},
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{Level, Span};

type GrpcLayer =
    TraceLayer<SharedClassifier<GrpcErrorsAsFailures>, for<'a> fn(&'a http::Request<Body>) -> Span>;

pub fn new_with_span(make_span: fn(&Request<Body>) -> Span) -> GrpcLayer {
    TraceLayer::new_for_grpc()
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
