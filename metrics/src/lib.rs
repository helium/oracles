//! Common code shared between the reward and ingest servers.

use metrics_exporter_prometheus::PrometheusBuilder;
use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tower::{Layer, Service};

/// Install the Prometheus export gateway
pub fn install_metrics() {
    let endpoint =
        std::env::var("METRICS_SCRAPE_ENDPOINT").unwrap_or_else(|_| String::from("0.0.0.0:9000"));
    let socket: SocketAddr = endpoint
        .parse()
        .expect("Invalid METRICS_SCRAPE_ENDPOINT value");
    if let Err(e) = PrometheusBuilder::new()
        .with_http_listener(socket)
        .install()
    {
        tracing::error!(target: "poc", "Failed to install Prometheus scrape endpoint: {e}");
    } else {
        tracing::info!(target: "poc", "Metrics scrape endpoint listening on {endpoint}");
    }
}

/// Measure the duration of a block and record it
// TODO(map): Ideally, we would like this to be a function that takes an async function and
// returns an async closure so that we can install this in the router rather than the
// function block. Such code is possible when async closures become stable.
#[macro_export]
macro_rules! record_duration {
    ( $metric_name:expr, $e:expr ) => {{
        let timer = std::time::Instant::now();
        let res = $e;
        ::metrics::histogram!($metric_name, timer.elapsed());
        res
    }};
}

/// Request metrics layer. Measures:
/// 1. Active requests.
///    Incrementing a corresponding Prometheus gauge when the request is
///    received and decrementing it when it has been responded to.
/// 2. Request handling duration.
///    Starting a timer before calling the handler and stoping after the
///    handler returns.
#[derive(Clone)]
pub struct RequestsLayer {
    // XXX Fields marked public just so that they can be constructed from exported macro.
    pub metric_name_count: &'static str,
    pub metric_name_time: &'static str,
}

#[macro_export]
macro_rules! request_layer {
    ( $metric_name:literal ) => {{
        poc_metrics::RequestsLayer {
            metric_name_count: concat!($metric_name, "_count"),
            metric_name_time: concat!($metric_name, "_time"),
        }
    }};
}

impl RequestsLayer {
    pub fn new(metric_name_count: &'static str, metric_name_time: &'static str) -> Self {
        Self {
            metric_name_count,
            metric_name_time,
        }
    }
}

impl<S> Layer<S> for RequestsLayer {
    type Service = Requests<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Requests {
            metric_name_count: self.metric_name_count,
            metric_name_time: self.metric_name_time,
            inner,
        }
    }
}

#[derive(Clone)]
pub struct Requests<S> {
    metric_name_count: &'static str,
    metric_name_time: &'static str,
    inner: S,
}

impl<S, R> Service<R> for Requests<S>
where
    S: Service<R> + Clone + Send + 'static,
    R: Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, ctx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(ctx)
    }

    fn call(&mut self, req: R) -> Self::Future {
        let metric_name_count = self.metric_name_count;
        let metric_name_time = self.metric_name_time;

        let timer = std::time::Instant::now();
        metrics::increment_gauge!(metric_name_count, 1.0);

        let clone = self.inner.clone();
        // take the service that was ready
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let res = inner.call(req).await;
            metrics::decrement_gauge!(metric_name_count, 1.0);
            let elapsed_time = timer.elapsed();
            tracing::debug!("request processed in {elapsed_time:?}");
            // TODO What units to use? Is f64 seconds appropriate?
            ::metrics::histogram!(metric_name_time, elapsed_time.as_secs_f64());
            res
        })
    }
}
