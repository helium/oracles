use crate::Settings;
use anyhow::{bail, Error, Result};
use chrono::{Duration, Utc};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload,
    traits::MsgVerify,
    FileType,
};
use futures::future::LocalBoxFuture;
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_mobile::{
    self, CellHeartbeatIngestReportV1, CellHeartbeatReqV1, CellHeartbeatRespV1,
    CoverageObjectIngestReportV1, CoverageObjectReqV1, CoverageObjectRespV1,
    DataTransferSessionIngestReportV1, DataTransferSessionReqV1, DataTransferSessionRespV1,
    InvalidatedRadioThresholdIngestReportV1, InvalidatedRadioThresholdReportReqV1,
    InvalidatedRadioThresholdReportRespV1, RadioThresholdIngestReportV1, RadioThresholdReportReqV1,
    RadioThresholdReportRespV1, SpeedtestIngestReportV1, SpeedtestReqV1, SpeedtestRespV1,
    SubscriberLocationIngestReportV1, SubscriberLocationReqV1, SubscriberLocationRespV1,
    WifiHeartbeatIngestReportV1, WifiHeartbeatReqV1, WifiHeartbeatRespV1,
};
use std::{net::SocketAddr, path::Path};
use task_manager::{ManagedTask, TaskManager};
use tonic::{
    metadata::{Ascii, MetadataValue},
    transport, Request, Response, Status,
};

const INGEST_WAIT_DURATION_MINUTES: i64 = 15;

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    heartbeat_report_sink: FileSinkClient,
    wifi_heartbeat_report_sink: FileSinkClient,
    speedtest_report_sink: FileSinkClient,
    data_transfer_session_sink: FileSinkClient,
    subscriber_location_report_sink: FileSinkClient,
    radio_threshold_report_sink: FileSinkClient,
    invalidated_radio_threshold_report_sink: FileSinkClient,
    coverage_object_report_sink: FileSinkClient,
    required_network: Network,
    address: SocketAddr,
    api_token: MetadataValue<Ascii>,
}

impl ManagedTask for GrpcServer {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        let api_token = self.api_token.clone();
        let address = self.address;
        Box::pin(async move {
            transport::Server::builder()
                .layer(poc_metrics::request_layer!("ingest_server_grpc_connection"))
                .add_service(poc_mobile::Server::with_interceptor(
                    *self,
                    move |req: Request<()>| match req.metadata().get("authorization") {
                        Some(t) if api_token == t => Ok(req),
                        _ => Err(Status::unauthenticated("No valid auth token")),
                    },
                ))
                .serve_with_shutdown(address, shutdown)
                .map_err(Error::from)
                .await
        })
    }
}

impl GrpcServer {
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

    async fn submit_wifi_heartbeat(
        &self,
        request: Request<WifiHeartbeatReqV1>,
    ) -> GrpcResult<WifiHeartbeatRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| WifiHeartbeatIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.wifi_heartbeat_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(WifiHeartbeatRespV1 { id }))
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

    async fn submit_threshold_report(
        &self,
        request: Request<RadioThresholdReportReqV1>,
    ) -> GrpcResult<RadioThresholdReportRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let hotspot_pubkey = event.hotspot_pubkey.clone();
        let cbsd_id = event.cbsd_id.clone();
        let threshold_timestamp = event.threshold_timestamp;

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| RadioThresholdIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    hotspot_pubkey = ?hotspot_pubkey,
                    cbsd_id = ?cbsd_id,
                    threshold_timestamp = %threshold_timestamp,
                    status = %status
                );
                status
            })?;

        _ = self.radio_threshold_report_sink.write(report, []).await;

        Ok(Response::new(RadioThresholdReportRespV1 {
            id: timestamp.to_string(),
        }))
    }

    async fn submit_invalidated_threshold_report(
        &self,
        request: Request<InvalidatedRadioThresholdReportReqV1>,
    ) -> GrpcResult<InvalidatedRadioThresholdReportRespV1> {
        let timestamp = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();
        let hotspot_pubkey = event.hotspot_pubkey.clone();
        let cbsd_id = event.cbsd_id.clone();
        let invalidated_timestamp = event.timestamp;

        let report = self
            .verify_public_key(event.carrier_pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| InvalidatedRadioThresholdIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })
            .map_err(|status| {
                tracing::debug!(
                    hotspot_pubkey = ?hotspot_pubkey,
                    cbsd_id = ?cbsd_id,
                    invalidated_timestamp = %invalidated_timestamp,
                    status = %status
                );
                status
            })?;

        _ = self
            .invalidated_radio_threshold_report_sink
            .write(report, [])
            .await;

        Ok(Response::new(InvalidatedRadioThresholdReportRespV1 {
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

pub async fn grpc_server(settings: &Settings) -> Result<()> {
    let grpc_addr = settings.listen_addr()?;

    // Initialize uploader
    let (file_upload, file_upload_server) =
        file_upload::FileUpload::from_settings_tm(&settings.output).await?;

    let store_base_path = Path::new(&settings.cache);

    let (heartbeat_report_sink, heartbeat_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::CbrsHeartbeatIngestReport,
        store_base_path,
        file_upload.clone(),
        concat!(env!("CARGO_PKG_NAME"), "_heartbeat_report"),
    )
    .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
    .create()
    .await?;

    let (wifi_heartbeat_report_sink, wifi_heartbeat_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::WifiHeartbeatIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_wifi_heartbeat_report"),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    // speedtests
    let (speedtest_report_sink, speedtest_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::CellSpeedtestIngestReport,
        store_base_path,
        file_upload.clone(),
        concat!(env!("CARGO_PKG_NAME"), "_speedtest_report"),
    )
    .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
    .create()
    .await?;

    let (data_transfer_session_sink, data_transfer_session_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::DataTransferSessionIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(
                env!("CARGO_PKG_NAME"),
                "_mobile_data_transfer_session_report"
            ),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (subscriber_location_report_sink, subscriber_location_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::SubscriberLocationIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_subscriber_location_report"),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (radio_threshold_report_sink, radio_threshold_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::RadioThresholdIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_radio_threshold_ingest_report"),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (invalidated_radio_threshold_report_sink, invalidated_radio_threshold_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::InvalidatedRadioThresholdIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(
                env!("CARGO_PKG_NAME"),
                "_invalidated_radio_threshold_ingest_report"
            ),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let (coverage_object_report_sink, coverage_object_report_sink_server) =
        file_sink::FileSinkBuilder::new(
            FileType::CoverageObjectIngestReport,
            store_base_path,
            file_upload.clone(),
            concat!(env!("CARGO_PKG_NAME"), "_coverage_object_report"),
        )
        .roll_time(Duration::minutes(INGEST_WAIT_DURATION_MINUTES))
        .create()
        .await?;

    let Some(api_token) = settings
        .token
        .as_ref()
        .and_then(|token| format!("Bearer {token}").parse::<MetadataValue<_>>().ok())
    else {
        bail!("expected valid api token in settings");
    };

    let grpc_server = GrpcServer {
        heartbeat_report_sink,
        wifi_heartbeat_report_sink,
        speedtest_report_sink,
        data_transfer_session_sink,
        subscriber_location_report_sink,
        radio_threshold_report_sink,
        invalidated_radio_threshold_report_sink,
        coverage_object_report_sink,
        required_network: settings.network,
        address: grpc_addr,
        api_token,
    };

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    TaskManager::builder()
        .add_task(file_upload_server)
        .add_task(heartbeat_report_sink_server)
        .add_task(wifi_heartbeat_report_sink_server)
        .add_task(speedtest_report_sink_server)
        .add_task(data_transfer_session_sink_server)
        .add_task(subscriber_location_report_sink_server)
        .add_task(radio_threshold_report_sink_server)
        .add_task(invalidated_radio_threshold_report_sink_server)
        .add_task(coverage_object_report_sink_server)
        .add_task(grpc_server)
        .build()
        .start()
        .await
}
