use crate::{
    admin_service::AdminService, authorization_service::AuthorizationService,
    carrier_service::CarrierService, entity_service::EntityService,
    gateway::service::GatewayService, hex_boosting_service::HexBoostingService,
    sub_dao_service::SubDaoService,
};
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_proto::services::{
    mobile_config::{
        AdminServer, AuthorizationServer, CarrierServiceServer, EntityServer, GatewayServer,
        HexBoostingServer,
    },
    sub_dao::SubDaoServer,
};
use std::{net::SocketAddr, time::Duration};
use task_manager::ManagedTask;
use tonic::transport;

pub struct GrpcServer {
    listen_addr: SocketAddr,
    admin_svc: AdminService,
    gateway_svc: GatewayService,
    auth_svc: AuthorizationService,
    entity_svc: EntityService,
    carrier_svc: CarrierService,
    hex_boosting_svc: HexBoostingService,
    sub_dao_svc: SubDaoService,
}

impl GrpcServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        listen_addr: SocketAddr,
        admin_svc: AdminService,
        gateway_svc: GatewayService,
        auth_svc: AuthorizationService,
        entity_svc: EntityService,
        carrier_svc: CarrierService,
        hex_boosting_svc: HexBoostingService,
        sub_dao_svc: SubDaoService,
    ) -> Self {
        Self {
            listen_addr,
            admin_svc,
            gateway_svc,
            auth_svc,
            entity_svc,
            carrier_svc,
            hex_boosting_svc,
            sub_dao_svc,
        }
    }
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
            transport::Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(250)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .layer(custom_tracing::grpc_layer::new_with_span(make_span))
                .add_service(AdminServer::new(self.admin_svc))
                .add_service(GatewayServer::new(self.gateway_svc))
                .add_service(AuthorizationServer::new(self.auth_svc))
                .add_service(EntityServer::new(self.entity_svc))
                .add_service(CarrierServiceServer::new(self.carrier_svc))
                .add_service(HexBoostingServer::new(self.hex_boosting_svc))
                .add_service(SubDaoServer::new(self.sub_dao_svc))
                .serve_with_shutdown(self.listen_addr, shutdown)
                .map_err(anyhow::Error::from)
                .await
        })
    }
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(
        custom_tracing::DEFAULT_SPAN,
        pub_key = tracing::field::Empty,
        signer = tracing::field::Empty,
    )
}
