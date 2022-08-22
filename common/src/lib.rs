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
    ( $metric_name:literal, $e:expr ) => {{
        let timer = std::time::Instant::now();
        let res = $e;
        ::metrics::histogram!($metric_name, timer.elapsed());
        res
    }};
}

/// A Layer that automatically tracks active requests, incrementing a corresponding
/// Prometheus gauge when the request is received and decrementing it when it has
/// been responded to.
#[derive(Clone)]
pub struct ActiveRequestsLayer {
    metric_name: &'static str,
}

impl ActiveRequestsLayer {
    pub fn new(metric_name: &'static str) -> Self {
        Self { metric_name }
    }
}

impl<S> Layer<S> for ActiveRequestsLayer {
    type Service = ActiveRequests<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ActiveRequests {
            metric_name: self.metric_name,
            inner,
        }
    }
}

#[derive(Clone)]
pub struct ActiveRequests<S> {
    metric_name: &'static str,
    inner: S,
}

impl<S, R> Service<R> for ActiveRequests<S>
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
        let metric_name = self.metric_name;
        metrics::increment_gauge!(metric_name, 1.0);

        let clone = self.inner.clone();
        // take the service that was ready
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let res = inner.call(req).await;
            metrics::decrement_gauge!(metric_name, 1.0);
            res
        })
    }
}
