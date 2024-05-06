//! Add a timing span to anything that can be instrumented and returns a Result.
//!
//! Example:
//! ```ignore
//! use poc_metrics::client_requests::ClientMetricTiming;
//!
//! async fn time_function() {
//!     let x: Result<i32, ()> = async { Ok(42) }
//!         .with_timing("iot_fetch_info")
//!         .await;
//!     assert_eq!(42, x.unwrap());
//! }
//! ```
//!
//! This will result in a prometheus metric
//! >> client_request_duration_seconds{name = "iot_fetch_info", quantile="xxx"}
//!
//! Install the `ApiTimingLayer`.
//!
//! Adding `.with_span_events(FmtSpan::CLOSE)` to a regular format layer will
//! print the timing spans to stdout as well.
//!
//! Example:
//! ```ignore
//! use poc_metrics::client_requests;
//! use tracing_subscriber::fmt::format::FmtSpan;
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//!
//! tracing_subscriber::registry()
//!   .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::CLOSE))
//!   .with(client_requests::client_request_timing_layer("histogram_name"))
//!   .init();
//! ```
use futures::{future::Inspect, Future, FutureExt};
use std::time::Instant;
use tracing::{field::Visit, instrument::Instrumented, span, Instrument, Subscriber};
use tracing_subscriber::{filter, layer, registry::LookupSpan, Layer};

const SPAN_NAME: &str = "metrics::timing";
const RESULT_FIELD: &str = "result";
const NAME_FIELD: &str = "name";
const SUCCESS: &str = "ok";
const ERROR: &str = "error";
const UNKNOWN: &str = "unknown";

pub fn client_request_timing_layer<S>(histogram_name: &'static str) -> impl layer::Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    ApiTimingLayer::new(histogram_name).with_filter(filter::filter_fn(|m| m.name() == SPAN_NAME))
}

pub trait ClientMetricTiming<A, B>: Sized + Instrument + FutureExt {
    fn with_timing(
        self,
        name: &'static str,
    ) -> Instrumented<Inspect<Self, impl FnOnce(&Result<A, B>)>>
    where
        Self: Future<Output = Result<A, B>> + Sized;
}

// Impl ClientMetricTiming for all futures that return a Result
impl<F, A, B> ClientMetricTiming<A, B> for F
where
    F: Future<Output = Result<A, B>> + Sized,
{
    fn with_timing(
        self,
        name: &'static str,
    ) -> Instrumented<Inspect<Self, impl FnOnce(&Result<A, B>)>> {
        // NOTE(mj): `tracing::info_span!(SPAN_NAME, {NAME_FIELD} = name, {RESULT_FIELD} = tracing::field::Empty);`
        //
        // Results in the error "format must be a string literal". Maybe one day
        // this will be fixed in the tracing macro so we can use it likes the
        // docs say.
        let span = tracing::info_span!(SPAN_NAME, name, result = tracing::field::Empty);
        let inner_span = span.clone();
        self.inspect(move |res| {
            inner_span.record(RESULT_FIELD, res.as_ref().ok().map_or(ERROR, |_| SUCCESS));
        })
        .instrument(span)
    }
}

struct Timing {
    name: Option<String>,
    start: Instant,
    // ok | error | unknown
    result: String,
}

impl Timing {
    fn new() -> Self {
        Self {
            name: None,
            start: Instant::now(),
            result: UNKNOWN.to_string(),
        }
    }

    fn record(self, histogram_name: &'static str) {
        if let Some(name) = self.name {
            metrics::histogram!(
                histogram_name,
                NAME_FIELD => name,
                RESULT_FIELD => self.result
            )
            .record(self.start.elapsed().as_secs_f64())
        }
    }
}

impl Visit for Timing {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            NAME_FIELD => self.name = Some(value.to_string()),
            RESULT_FIELD => self.result = value.to_string(),
            _ => (),
        }
    }
}

struct ApiTimingLayer {
    histogram_name: &'static str,
}

impl ApiTimingLayer {
    fn new(histogram_name: &'static str) -> Self {
        Self { histogram_name }
    }
}

impl<S> tracing_subscriber::Layer<S> for ApiTimingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");

        let mut timing = Timing::new();
        attrs.values().record(&mut timing);
        span.extensions_mut().insert(timing);
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: layer::Context<'_, S>) {
        let span = ctx.span(id).unwrap();

        if let Some(timing) = span.extensions_mut().get_mut::<Timing>() {
            values.record(timing);
        };
    }

    fn on_close(&self, id: tracing::Id, ctx: layer::Context<S>) {
        let span = ctx.span(&id).unwrap();

        if let Some(timing) = span.extensions_mut().remove::<Timing>() {
            timing.record(self.histogram_name);
        };
    }
}

#[cfg(test)]
mod tests {
    use super::ClientMetricTiming;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[tokio::test]
    async fn test_telemetry() -> Result<(), Box<dyn std::error::Error>> {
        tracing_subscriber::registry()
            // Uncomment to view traces and Spans closing
            // .with(
            //     tracing_subscriber::fmt::layer()
            //         .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE),
            // )
            .with(super::client_request_timing_layer("histogram_name"))
            .init();

        // Let the OS assign a port
        let addr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr()?
        };
        tracing::info!("listening on {addr}");
        super::super::install(addr)?;

        let success = async { Ok("nothing went wrong") };
        let failure = async { Err("something went wrong") };
        let _: Result<&str, &str> = success.with_timing("success").await;
        let _: Result<&str, &str> = failure.with_timing("failing").await;

        // .with_timing() can only be added to futures that return Results.
        // let will_not_compile = async { 1 + 2 }.with_timing("not a result");

        let res = reqwest::get(format!("http://{addr}")).await?;
        let body = res.text().await?;

        tracing::info!("response: \n{body}");
        assert!(body.contains(r#"histogram_name_count{name="success",result="ok"} 1"#));
        assert!(body.contains(r#"histogram_name_count{name="failing",result="error"} 1"#));

        Ok(())
    }
}
