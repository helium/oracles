use std::{net::SocketAddr, path::Path, str::FromStr};

use chrono::Utc;
use file_store::{
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt, MsgVerify},
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKey;
use helium_proto::services::chain_rewardable_entities::{
    self, IotHotspotUpdateReportV1, IotHotspotUpdateReqV1, IotHotspotUpdateRespV1,
    MobileHotspotUpdateReportV1, MobileHotspotUpdateReqV1, MobileHotspotUpdateRespV1,
};
use task_manager::{ManagedTask, TaskManager};
use tonic::{transport::Server, Request, Response, Status};

use crate::Settings;

pub async fn grpc_server(settings: &Settings) -> anyhow::Result<()> {
    let Some(auth_key) = settings.chain_rewardable_entities_auth_key.clone() else {
        anyhow::bail!("Running in mode CHAIN without auth_key is not allowed");
    };
    let auth_key = PublicKey::from_str(&auth_key)?;

    if settings.network != auth_key.network {
        anyhow::bail!(
            "Auth key network {} does not match ingest network {}",
            auth_key.network,
            settings.network
        );
    }

    let (file_upload, file_upload_server) = FileUpload::from_settings_tm(&settings.output).await?;
    let store_base_path = Path::new(&settings.cache);

    let (mobile_sink, mobile_sink_server) = MobileHotspotUpdateReportV1::file_sink(
        store_base_path,
        file_upload.clone(),
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(settings.roll_time),
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let (iot_sink, iot_sink_server) = IotHotspotUpdateReportV1::file_sink(
        store_base_path,
        file_upload,
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(settings.roll_time),
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let grpc_server = GrpcServer {
        iot_sink,
        mobile_sink,
        auth_key,
        address: settings.listen_addr,
    };

    tracing::info!(
        "grpc listening on {} and server mode {:?}",
        settings.listen_addr,
        settings.mode
    );

    TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(mobile_sink_server)
        .add_task(iot_sink_server)
        .add_task(grpc_server)
        .build()
        .start()
        .await
}

#[derive(Debug)]
pub struct GrpcServer {
    iot_sink: FileSinkClient<IotHotspotUpdateReportV1>,
    mobile_sink: FileSinkClient<MobileHotspotUpdateReportV1>,
    address: SocketAddr,
    auth_key: PublicKey,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(self.run(shutdown))
    }
}

impl GrpcServer {
    async fn run(self, shutdown: triggered::Listener) -> anyhow::Result<()> {
        let address = self.address;
        Server::builder()
            .layer(custom_tracing::grpc_layer::new_with_span(make_span))
            .layer(poc_metrics::request_layer!("ingest_server_grpc_connection"))
            .add_service(chain_rewardable_entities::Server::new(self))
            .serve_with_shutdown(address, shutdown)
            .map_err(anyhow::Error::from)
            .await
    }
}

#[tonic::async_trait]
impl chain_rewardable_entities::ChainRewardableEntities for GrpcServer {
    async fn submit_mobile_hotspot_change(
        &self,
        request: Request<MobileHotspotUpdateReqV1>,
    ) -> Result<Response<MobileHotspotUpdateRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.mobile_sink.write(report, []).await;

        Ok(Response::new(MobileHotspotUpdateRespV1 { timestamp_ms }))
    }

    async fn submit_iot_hotspot_change(
        &self,
        request: Request<IotHotspotUpdateReqV1>,
    ) -> Result<Response<IotHotspotUpdateRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.iot_sink.write(report, []).await;

        Ok(Response::new(IotHotspotUpdateRespV1 { timestamp_ms }))
    }
}

fn make_span(_request: &http::request::Request<helium_proto::services::Body>) -> tracing::Span {
    tracing::info_span!(custom_tracing::DEFAULT_SPAN)
}

fn verify_signature<Msg>(msg: &Msg, public_key: &PublicKey) -> Result<(), tonic::Status>
where
    Msg: MsgVerify,
{
    msg.verify(public_key)
        .map_err(|_| Status::invalid_argument("invalid signature"))
}

trait IntoReport {
    type Out;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out;
}

impl IntoReport for MobileHotspotUpdateReqV1 {
    type Out = MobileHotspotUpdateReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        MobileHotspotUpdateReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}

impl IntoReport for IotHotspotUpdateReqV1 {
    type Out = IotHotspotUpdateReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        IotHotspotUpdateReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}
