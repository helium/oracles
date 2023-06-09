use crate::entropy_generator::MessageReceiver;
use futures::future::LocalBoxFuture;
use helium_proto::{
    services::poc_entropy::{EntropyReqV1, PocEntropy, Server as GrpcServer},
    EntropyReportV1,
};
use std::net::SocketAddr;
use task_manager::ManagedTask;
use tokio::time::Duration;
use tonic::transport;
use tokio_util::sync::CancellationToken;

struct EntropyServer {
    entropy_watch: MessageReceiver,
}

impl ManagedTask for ApiServer {
    fn start_task(
        self: Box<Self>,
        token: CancellationToken,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(token))
    }
}


#[tonic::async_trait]
impl PocEntropy for EntropyServer {
    async fn entropy(
        &self,
        _request: tonic::Request<EntropyReqV1>,
    ) -> Result<tonic::Response<EntropyReportV1>, tonic::Status> {
        let entropy = &*self.entropy_watch.borrow();
        metrics::increment_counter!("entropy_server_get_count");
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

    pub async fn run(self, token: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(listen = self.socket_addr.to_string(), "starting");
        transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(250)))
            .http2_keepalive_timeout(Some(Duration::from_secs(60)))
            .add_service(self.service);
            // TODO: fix this!
            // .serve_with_shutdown(self.socket_addr, token)
            // .await?;
        tracing::info!("stopping api server");
        Ok(())
    }
}
