use crate::Settings;
use anyhow::{bail, Error, Result};
use chrono::{Duration, Utc};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload,
    traits::MsgVerify,
    FileType,
};
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatIngestReportV1, CellHeartbeatReqV1, CellHeartbeatRespV1,
    CoverageObjectIngestReportV1, CoverageObjectReqV1, CoverageObjectRespV1,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1, DataTransferSessionRespV1,
    SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestRespV1, SubscriberLocationIngestReportV1,
    SubscriberLocationReqV1, SubscriberLocationRespV1,
};
use std::path::Path;
use tonic::{metadata::MetadataValue, transport, Request, Response, Status};

const INGEST_WAIT_DURATION_MINUTES: i64 = 15;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    heartbeat_report_sink: FileSinkClient,
    speedtest_report_sink: FileSinkClient,
    data_transfer_session_sink: FileSinkClient,
    subscriber_location_report_sink: FileSinkClient,
    coverage_object_report_sink: FileSinkClient,
    required_network: Network,
}

impl GrpcServer {
    fn new(
        heartbeat_report_sink: FileSinkClient,
        speedtest_report_sink: FileSinkClient,
        data_transfer_session_sink: FileSinkClient,
        subscriber_location_report_sink: FileSinkClient,
        coverage_object_report_sink: FileSinkClient,
        required_network: Network,
    ) -> Result<Self> {
        Ok(Self {
            heartbeat_report_sink,
            speedtest_report_sink,
            data_transfer_session_sink,
            subscriber_location_report_sink,
            coverage_object_report_sink,
            required_network,
        })
    }

    fn verify_network(&self, public_key: PublicKey) -> VerifyResult<PublicKey> {
        if self.required_network == public_key.network {
            Ok(public_key)
        } else {
            Err(Status::invalid_argument("invalid network"))
        }
    }

    fn verify_public_key(&self, bytes: &[u8]) -> VerifyResult<PublicKey> {
        PublicKey::try_from(bytes).map_err(|_| Status::invalid_argument("invalid public key"))
    }

    fn verify_signature<E>(&self, public_key: PublicKey, event: E) -> VerifyResult<(PublicKey, E)>
    where
        E: MsgVerify,
    {
        event
            .verify(&public_key)
            .map_err(|_| Status::invalid_argument("invalid signature"))?;
        Ok((public_key, event))
    }
}

#[tonic::async_trait]
impl poc_mobile::PocMobile for GrpcServer {
    async fn submit_speedtest(
        &self,
        request: Request<SpeedtestReqV1>,
    ) -> GrpcResult<SpeedtestRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| SpeedtestIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.speedtest_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(SpeedtestRespV1 { id }))
    }

    async fn submit_cell_heartbeat(
        &self,
        request: Request<CellHeartbeatReqV1>,
    ) -> GrpcResult<CellHeartbeatRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| CellHeartbeatIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.heartbeat_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(CellHeartbeatRespV1 { id }))
    }

    async fn submit_data_transfer_session(
        &self,
        request: Request<DataTransferSessionReqV1>,
    ) -> GrpcResult<DataTransferSessionRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| DataTransferSessionIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.data_transfer_session_sink.write(report, []).await;

        Ok(Response::new(DataTransferSessionRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_subscriber_location(
        &self,
        request: Request<SubscriberLocationReqV1>,
    ) -> GrpcResult<SubscriberLocationRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let subscriber_id = event.subscriber_id.clone();
        let timestamp_millis = event.timestamp;

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| SubscriberLocationIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    subscriber_id = ?subscriber_id,
                    timestamp = %timestamp_millis,
                    status = %status
                );
                status
            })?;

        _ = self.subscriber_location_report_sink.write(report, []).await;

        Ok(Response::new(SubscriberLocationRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_coverage_object(
        &self,
        request: Request<CoverageObjectReqV1>,
    ) -> GrpcResult<CoverageObjectRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| CoverageObjectIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.coverage_object_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(CoverageObjectRespV1 { id }))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener, settings: &Settings) -> Result<()> {
    let grpc_addr = settings.listen_addr()?;

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = Path::new(&settings.cache);

    let (heartbeat_report_sink, heartbeat_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::CellHeartbeatIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_heartbeat_report"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
    .create()
    .await?;

    // speedtests
    let (speedtest_report_sink, speedtest_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::CellSpeedtestIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_speedtest_report"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
    .create()
    .await?;

    let (data_transfer_session_sink, data_transfer_session_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::DataTransferSessionIngestReport,
            store_base_path,
            concat!(
                env!("CARGO_PKG_NAME"),
                "_mobile_data_transfer_session_report"
            ),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (subscriber_location_report_sink, subscriber_location_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::SubscriberLocationIngestReport,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_subscriber_location_report"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (coverage_object_report_sink, coverage_object_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::CoverageObjectIngestReport,
            store_base_path,
            concat!(env!("CARGO_PKG_NAME"), "_coverage_object_report"),
        )
        .deposits(Some(file_upload_tx.clone()))
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let grpc_server = GrpcServer::new(
        heartbeat_report_sink,
        speedtest_report_sink,
        data_transfer_session_sink,
        subscriber_location_report_sink,
        coverage_object_report_sink,
        settings.network,
    )?;

    let Some(api_token) = settings
        .token
        .as_ref()
        .and_then(|token| {
            format!("Bearer {token}")
                .parse::<MetadataValue<_>>()
                .ok()
        }) else {
            bail!("expected valid api token in settings");
        };

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    //TODO start a service with either the poc mobile or poc iot endpoints only - not both
    //     use _server_mode (set above ) to decide
    let server = transport::Server::builder()
        .layer(poc_metrics::request_layer!("ingest_server_grpc_connection"))
        .add_service(poc_mobile::Server::with_interceptor(
            grpc_server,
            move |req: Request<()>| match req.metadata().get("authorization") {
                Some(t) if api_token == t => Ok(req),
                _ => Err(Status::unauthenticated("No valid auth token")),
            },
        ))
        .serve_with_shutdown(grpc_addr, shutdown.clone())
        .map_err(Error::from);

    tokio::try_join!(
        server,
        heartbeat_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        speedtest_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        data_transfer_session_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        subscriber_location_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        coverage_object_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        file_upload.run(shutdown.clone()).map_err(Error::from),
    )
    .map(|_| ())
}
