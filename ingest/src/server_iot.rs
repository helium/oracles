use crate::Settings;
use anyhow::{Error, Result};
use chrono::{Duration, Utc};
use file_store::{
    file_sink::{self, FileSinkClient},
    file_upload,
    traits::MsgVerify,
    FileType,
};
use futures_util::TryFutureExt;
use helium_crypto::{Network, PublicKey};
use helium_proto::services::poc_lora::{
    self, LoraBeaconIngestReportV1, LoraBeaconReportReqV1, LoraBeaconReportRespV1,
    LoraWitnessIngestReportV1, LoraWitnessReportReqV1, LoraWitnessReportRespV1,
};
use std::{convert::TryFrom, path::Path};
use tonic::{transport, Request, Response, Status};

pub type GrpcResult<T> = std::result::Result<Response<T>, Status>;
pub type VerifyResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    beacon_report_sink: FileSinkClient,
    witness_report_sink: FileSinkClient,
    required_network: Network,
}

impl GrpcServer {
    fn new(
        beacon_report_sink: FileSinkClient,
        witness_report_sink: FileSinkClient,
        required_network: Network,
    ) -> Result<Self> {
        Ok(Self {
            beacon_report_sink,
            witness_report_sink,
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
impl poc_lora::PocLora for GrpcServer {
    async fn submit_lora_beacon(
        &self,
        request: Request<LoraBeaconReportReqV1>,
    ) -> GrpcResult<LoraBeaconReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| LoraBeaconIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.beacon_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(LoraBeaconReportRespV1 { id }))
    }

    async fn submit_lora_witness(
        &self,
        request: Request<LoraWitnessReportReqV1>,
    ) -> GrpcResult<LoraWitnessReportRespV1> {
        let timestamp: u64 = Utc::now().timestamp_millis() as u64;
        let event = request.into_inner();

        let report = self
            .verify_public_key(event.pub_key.as_ref())
            .and_then(|public_key| self.verify_network(public_key))
            .and_then(|public_key| self.verify_signature(public_key, event))
            .map(|(_, event)| LoraWitnessIngestReportV1 {
                received_timestamp: timestamp,
                report: Some(event),
            })?;

        _ = self.witness_report_sink.write(report, []).await;

        let id = timestamp.to_string();
        Ok(Response::new(LoraWitnessReportRespV1 { id }))
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
    let (beacon_report_sink, beacon_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotBeaconIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_beacon_report"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    // iot witness reports
    let (witness_report_sink, witness_report_sink_server) = file_sink::FileSinkBuilder::new(
        FileType::IotWitnessIngestReport,
        store_base_path,
        concat!(env!("CARGO_PKG_NAME"), "_witness_report"),
    )
    .deposits(Some(file_upload_tx.clone()))
    .roll_time(Duration::minutes(5))
    .create()
    .await?;

    let grpc_server = GrpcServer::new(beacon_report_sink, witness_report_sink, settings.network)?;

    tracing::info!(
        "grpc listening on {grpc_addr} and server mode {:?}",
        settings.mode
    );

    let server = transport::Server::builder()
        .layer(poc_metrics::request_layer!("ingest_server_iot_connection"))
        .add_service(poc_lora::Server::new(grpc_server))
        .serve_with_shutdown(grpc_addr, shutdown.clone())
        .map_err(Error::from);

    tokio::try_join!(
        server,
        beacon_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        witness_report_sink_server
            .run(shutdown.clone())
            .map_err(Error::from),
        file_upload.run(shutdown.clone()).map_err(Error::from),
    )
    .map(|_| ())
}
