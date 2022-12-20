use crate::Settings;
use anyhow::{Error, Result};
use chrono::{Duration, Utc};
use file_store::traits::MsgVerify;
use file_store::{file_sink, file_sink_write, file_upload, FileType};
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_iot::{
    self, IotBeaconIngestReportV1, IotBeaconReportReqV1, IotBeaconReportRespV1,
    IotWitnessIngestReportV1, IotWitnessReportReqV1, IotWitnessReportRespV1,
};
use std::{convert::TryFrom, path::Path};
use tonic::{transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    iot_beacon_report_tx: file_sink::MessageSender,
    iot_witness_report_tx: file_sink::MessageSender,
    required_network: Network,
}

impl GrpcServer {
    fn new(
        iot_beacon_report_tx: file_sink::MessageSender,
        iot_witness_report_tx: file_sink::MessageSender,
        required_network: Network,
    ) -> Result<Self> {
        Ok(Self {
            iot_beacon_report_tx,
            iot_witness_report_tx,
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
impl poc_iot::PocIot for GrpcServer {
    async fn submit_iot_beacon(
        &self,
        request: Request<IotBeaconReportReqV1>,
    ) -> GrpcResult<IotBeaconReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| IotBeaconIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = file_sink_write!("beacon_report", &self.iot_beacon_report_tx, report).await;
        metrics::increment_counter!("ingest_server_beacon_report_count");

        let id = timestamp.to_string();
        Ok(Response::new(IotBeaconReportRespV1 { id }))
    }

    async fn submit_iot_witness(
        &self,
        request: Request<IotWitnessReportReqV1>,
    ) -> GrpcResult<IotWitnessReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| IotWitnessIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = file_sink_write!("witness_report", &self.iot_witness_report_tx, report).await;
        metrics::increment_counter!("ingest_server_witness_report_count");

        let id = timestamp.to_string();
        Ok(Response::new(IotWitnessReportRespV1 { id }))
    }
}

pub async fn grpc_server(shutdown: triggered::Listener, settings: &Settings) -> Result<()> {
    let grpc_addr = settings.listen_addr()?;

    // Initialize uploader
    let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
    let file_upload =
        file_upload::FileUpload::from_settings(&settings.output, file_upload_rx).await?;

    let store_base_path = Path::new(&settings.cache);

    // iot beacon reports
    let (iot_beacon_report_tx, iot_beacon_report_rx) = file_sink::message_channel(50);
    let mut iot_beacon_report_sink = file_sink::FileSinkBuilder::new(
        FileType::IotBeaconIngestReport,
        store_base_path,
        iot_beacon_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    // iot witness reports
    let (iot_witness_report_tx, iot_witness_report_rx) = file_sink::message_channel(50);
    let mut iot_witness_report_sink = file_sink::FileSinkBuilder::new(
        FileType::IotWitnessIngestReport,
        store_base_path,
        iot_witness_report_rx,
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    let grpc_server = GrpcServer::new(
        iot_beacon_report_tx,
        iot_witness_report_tx,
        settings.network,
    )?;

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    let server = transport::Server::builder()
        .layer(poc_metrics::request_layer!("ingest_server_iot_connection"))
        .add_service(poc_iot::Server::new(grpc_server))
        .serve_with_shutdown(grpc_addr, shutdown.clone())
        .map_err(Error::from);

    tokio::try_join!(
        server,
        iot_beacon_report_sink.run(&shutdown).map_err(Error::from),
        iot_witness_report_sink.run(&shutdown).map_err(Error::from),
        file_upload.run(&shutdown).map_err(Error::from),
    )
    .map(|_| ())
}
