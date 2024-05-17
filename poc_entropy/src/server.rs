use crate::entropy_generator::MessageReceiver;
use helium_proto::{
    services::poc_entropy::{EntropyReqV1, PocEntropy, Server as GrpcServer},
    EntropyReportV1,
};
use std::net::SocketAddr;
use tokio::time::Duration;
use tonic::transport;

struct EntropyServer {
    entropy_watch: MessageReceiver,
}

#[tonic::async_trait]
impl PocEntropy for EntropyServer {
    async fn entropy(
        &self,
        _request: tonic::Request<EntropyReqV1>,
    ) -> Result<tonic::Response<EntropyReportV1>, tonic::Status> {
        let entropy = &*self.entropy_watch.borrow();
        metrics::counter!("entropy_server_get_count").increment(1);
        Ok(tonic::Response::new(entropy.into()))
    }
}

pub struct ApiServer {
    pub socket_addr: SocketAddr,
    service: GrpcServer<EntropyServer>,
}

impl ApiServer {
    pub async fn new(
        socket_addr: SocketAddr,
        entropy_watch: MessageReceiver,
    ) -> anyhow::Result<Self> {
        let service = GrpcServer::new(EntropyServer { entropy_watch });

        Ok(Self {
            socket_addr,
            service,
        })
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> anyhow::Result<()> {
        tracing::info!(listen = self.socket_addr.to_string(), "starting");
        transport::Server::builder()
            .layer(custom_tracing::grpc_layer::new_with_span(make_span))
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(self.service)
            .serve_with_shutdown(self.socket_addr, shutdown.clone())
            .await?;
        tracing::info!("stopping api server");
        Ok(())
    }
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(custom_tracing::DEFAULT_SPAN)
}
