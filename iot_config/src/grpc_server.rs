use crate::sub_dao_service::SubDaoService;
use crate::{
    admin_service::AdminService, gateway::service::GatewayService, org_service::OrgService,
    route_service::RouteService,
};
use anyhow::Error;
use futures_util::TryFutureExt;
use helium_proto::services::{
    iot_config::{AdminServer, GatewayServer, OrgServer, RouteServer},
    sub_dao::SubDaoServer,
};
use std::{net::SocketAddr, time::Duration};
use task_manager::ManagedTask;
use tonic::transport;

pub struct GrpcServer {
    listen_addr: SocketAddr,
    gateway_svc: GatewayService,
    route_svc: RouteService,
    org_svc: OrgService,
    admin_svc: AdminService,
    subdao_svc: SubDaoService,
}

impl GrpcServer {
    pub fn new(
        listen_addr: SocketAddr,
        gateway_svc: GatewayService,
        route_svc: RouteService,
        org_svc: OrgService,
        admin_svc: AdminService,
        subdao_svc: SubDaoService,
    ) -> Self {
        Self {
            listen_addr,
            gateway_svc,
            route_svc,
            org_svc,
            admin_svc,
            subdao_svc,
        }
    }
}

impl ManagedTask for GrpcServer {
    fn start_task(self: Box<Self>, shutdown: triggered::Listener) -> task_manager::TaskFuture {
        task_manager::spawn(async move {
            let grpc_server = transport::Server::builder()
                .http2_keepalive_interval(Some(Duration::from_secs(250)))
                .http2_keepalive_timeout(Some(Duration::from_secs(60)))
                .layer(custom_tracing::grpc_layer::new_with_span(make_span))
                .add_service(GatewayServer::new(self.gateway_svc))
                .add_service(OrgServer::new(self.org_svc))
                .add_service(RouteServer::new(self.route_svc))
                .add_service(AdminServer::new(self.admin_svc))
                .add_service(SubDaoServer::new(self.subdao_svc))
                .serve(self.listen_addr)
                .map_err(Error::from);

            tokio::select! {
                _ = shutdown => {
                    tracing::warn!("grpc server shutting down");
                    Ok(())
                }
                res = grpc_server => {
                    match res {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            tracing::error!(?err, "grpc server failed with error");
                            Err(anyhow::anyhow!("grpc server exiting with error"))
                        }
                    }
                }
            }
        })
    }
}

fn make_span(_request: &http::request::Request<tonic::body::Body>) -> tracing::Span {
    tracing::info_span!(
        custom_tracing::DEFAULT_SPAN,
        pub_key = tracing::field::Empty,
        signer = tracing::field::Empty,
        oui = tracing::field::Empty,
        route_id = tracing::field::Empty,
    )
}
