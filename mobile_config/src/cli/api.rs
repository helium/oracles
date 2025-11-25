use crate::settings::Settings;
use futures::StreamExt;
use helium_crypto::Keypair;
use helium_proto::services::mobile_config::{
    DeviceTypeV2, GatewayClient, GatewayInfoStreamReqV3, GatewayInfoStreamResV3,
};
use helium_proto_crypto::MsgSign;
use std::time::Instant;
use tonic::Streaming;

#[derive(Debug, clap::Parser)]
pub struct Api {
    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    InfoStreamV3,
}

impl Api {
    pub async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        match self.cmd {
            Cmd::InfoStreamV3 => {
                custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;

                tracing::info!("started");
                let start = Instant::now();

                let port = settings.listen.port();
                let addr = format!("http://127.0.0.1:{}", port);
                let mut client = GatewayClient::connect(addr).await?;

                let admin_key = settings.signing_keypair.clone();

                let mut stream = gateway_info_stream_v3(
                    &mut client,
                    &admin_key,
                    &[
                        DeviceTypeV2::DataOnly,
                        DeviceTypeV2::Indoor,
                        DeviceTypeV2::Outdoor,
                    ],
                    0,
                    0,
                )
                .await?;

                let mut count = 0;

                while let Some(resp) = stream.next().await {
                    match resp {
                        Ok(res) => {
                            count += res.gateways.len();
                        }
                        Err(err) => {
                            tracing::error!(
                                ?err,
                                "error while reading from gateway_info_stream_v3"
                            );
                            break;
                        }
                    }
                }
                let elapsed = start.elapsed();

                tracing::info!(?elapsed, count, "finished reading stream");

                Ok(())
            }
        }
    }
}

async fn gateway_info_stream_v3(
    client: &mut GatewayClient<tonic::transport::Channel>,
    signer: &Keypair,
    device_types: &[DeviceTypeV2],
    min_updated_at: u64,
    min_location_changed_at: u64,
) -> anyhow::Result<Streaming<GatewayInfoStreamResV3>> {
    let mut req = GatewayInfoStreamReqV3 {
        batch_size: 10000,
        signer: signer.public_key().to_vec(),
        signature: vec![],
        device_types: device_types
            .iter()
            .map(|v| DeviceTypeV2::into(*v))
            .collect(),
        min_updated_at,
        min_location_changed_at,
    };
    req.sign(signer)?;
    let stream = client.info_stream_v3(req).await?.into_inner();

    Ok(stream)
}
