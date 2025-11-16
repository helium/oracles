use std::{net::SocketAddr, str::FromStr};

use chrono::Utc;
use file_store::{file_sink::FileSinkClient, file_upload};
use file_store_oracles::traits::{
    FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt,
};
use futures::{future::LocalBoxFuture, TryFutureExt};
use helium_crypto::PublicKey;
use helium_proto::{
    services::chain_rewardable_entities::{
        self, EntityOwnershipChangeReportV1, EntityOwnershipChangeReqV1, EntityOwnershipChangeRespV1,
        EntityRewardDestinationChangeReportV1, EntityRewardDestinationChangeReqV1,
        EntityRewardDestinationChangeRespV1, IotHotspotChangeReportV1, IotHotspotChangeReqV1,
        IotHotspotChangeRespV1, MobileHotspotChangeReportV1, MobileHotspotChangeReqV1,
        MobileHotspotChangeRespV1,
    }
};
use helium_proto_crypto::MsgVerify;
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

    let s3_client = settings.file_store.connect().await;
    let (file_upload, file_upload_server) =
        file_upload::FileUpload::new(s3_client, settings.output_bucket.clone()).await;

    let (mobile_sink, mobile_sink_server) = MobileHotspotChangeReportV1::file_sink(
        &settings.cache,
        file_upload.clone(),
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(settings.roll_time),
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let (iot_sink, iot_sink_server) = IotHotspotChangeReportV1::file_sink(
        &settings.cache,
        file_upload.clone(),
        FileSinkCommitStrategy::Automatic,
        FileSinkRollTime::Duration(settings.roll_time),
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let (entity_ownership_sink, entity_ownership_sink_server) =
        EntityOwnershipChangeReportV1::file_sink(
            &settings.cache,
            file_upload.clone(),
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let (entity_reward_destination_sink, entity_reward_destination_sink_server) =
        EntityRewardDestinationChangeReportV1::file_sink(
            &settings.cache,
            file_upload,
            FileSinkCommitStrategy::Automatic,
            FileSinkRollTime::Duration(settings.roll_time),
            env!("CARGO_PKG_NAME"),
        )
        .await?;

    let grpc_server = GrpcServer {
        iot_sink,
        mobile_sink,
        entity_ownership_sink,
        entity_reward_destination_sink,
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
        .add_task(entity_ownership_sink_server)
        .add_task(entity_reward_destination_sink_server)
        .add_task(grpc_server)
        .build()
        .start()
        .await
}

#[derive(Debug)]
pub struct GrpcServer {
    iot_sink: FileSinkClient<IotHotspotChangeReportV1>,
    mobile_sink: FileSinkClient<MobileHotspotChangeReportV1>,
    entity_ownership_sink: FileSinkClient<EntityOwnershipChangeReportV1>,
    entity_reward_destination_sink: FileSinkClient<EntityRewardDestinationChangeReportV1>,
    address: SocketAddr,
    auth_key: PublicKey,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        task_manager::spawn(self.run(shutdown))
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
        request: Request<MobileHotspotChangeReqV1>,
    ) -> Result<Response<MobileHotspotChangeRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.mobile_sink.write(report, []).await;

        Ok(Response::new(MobileHotspotChangeRespV1 { timestamp_ms }))
    }

    async fn submit_iot_hotspot_change(
        &self,
        request: Request<IotHotspotChangeReqV1>,
    ) -> Result<Response<IotHotspotChangeRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.iot_sink.write(report, []).await;

        Ok(Response::new(IotHotspotChangeRespV1 { timestamp_ms }))
    }

    async fn submit_entity_ownership_change(
        &self,
        request: Request<EntityOwnershipChangeReqV1>,
    ) -> Result<Response<EntityOwnershipChangeRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.entity_ownership_sink.write(report, []).await;

        Ok(Response::new(EntityOwnershipChangeRespV1 { timestamp_ms }))
    }

    async fn submit_entity_reward_destination_change(
        &self,
        request: Request<EntityRewardDestinationChangeReqV1>,
    ) -> Result<Response<EntityRewardDestinationChangeRespV1>, tonic::Status> {
        let timestamp_ms = Utc::now().timestamp_millis() as u64;
        let req = request.into_inner();

        verify_signature(&req, &self.auth_key)?;

        let report = req.into_report(timestamp_ms);
        let _ = self.entity_reward_destination_sink.write(report, []).await;

        Ok(Response::new(EntityRewardDestinationChangeRespV1 {
            timestamp_ms,
        }))
    }
}

fn make_span(_request: &http::request::Request<tonic::body::Body>) -> tracing::Span {
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

impl IntoReport for MobileHotspotChangeReqV1 {
    type Out = MobileHotspotChangeReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        MobileHotspotChangeReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}

impl IntoReport for IotHotspotChangeReqV1 {
    type Out = IotHotspotChangeReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        IotHotspotChangeReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}

impl IntoReport for EntityOwnershipChangeReqV1 {
    type Out = EntityOwnershipChangeReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        EntityOwnershipChangeReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}

impl IntoReport for EntityRewardDestinationChangeReqV1 {
    type Out = EntityRewardDestinationChangeReportV1;
    fn into_report(self, received_timestamp_ms: u64) -> Self::Out {
        EntityRewardDestinationChangeReportV1 {
            received_timestamp_ms,
            report: Some(self),
        }
    }
}
