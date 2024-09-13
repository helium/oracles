use file_store::{
    file_info_poller::{
        self, FileInfoPollerConfigBuilder, FileInfoStream, LookbackBehavior,
        ProstFileInfoPollerParser,
    },
    file_sink::FileSinkClient,
    file_upload::FileUpload,
    traits::{FileSinkCommitStrategy, FileSinkRollTime, FileSinkWriteExt},
    FileStore, FileType,
};
use helium_proto::ServiceProviderPromotionFundV1;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::settings::Settings;

pub async fn write_protos(
    protos: Vec<ServiceProviderPromotionFundV1>,
    file_sink: &FileSinkClient<ServiceProviderPromotionFundV1>,
) -> Result<(), anyhow::Error> {
    for proto in protos {
        file_sink.write(proto, []).await?.await??;
    }
    file_sink.commit().await?.await??;
    Ok(())
}

pub async fn start_file_sink(
    settings: &Settings,
    listener: triggered::Listener,
) -> anyhow::Result<(
    FileSinkClient<ServiceProviderPromotionFundV1>,
    JoinHandle<anyhow::Result<()>>,
)> {
    let (upload, upload_server) = FileUpload::from_settings_tm(&settings.file_store_output).await?;
    let (sink, sink_server) = ServiceProviderPromotionFundV1::file_sink(
        &settings.file_sink_cache,
        upload,
        FileSinkCommitStrategy::Manual,
        FileSinkRollTime::Default,
        env!("CARGO_PKG_NAME"),
    )
    .await?;

    let handle = tokio::spawn(async move {
        tokio::try_join!(
            upload_server.run(listener.clone()),
            sink_server.run(listener)
        )?;

        Ok(())
    });

    Ok((sink, handle))
}

pub async fn start_file_source(
    settings: &Settings,
    listener: triggered::Listener,
) -> anyhow::Result<(
    Receiver<FileInfoStream<ServiceProviderPromotionFundV1>>,
    JoinHandle<anyhow::Result<()>>,
)> {
    let file_store = FileStore::from_settings(&settings.file_store_output).await?;
    let (rx, server) =
        FileInfoPollerConfigBuilder::<ServiceProviderPromotionFundV1, _, _, _>::default()
            .parser(ProstFileInfoPollerParser)
            .state(file_info_poller::NoState)
            .store(file_store)
            .prefix(FileType::ServiceProviderPromotionFund.to_string())
            .lookback(LookbackBehavior::Max(settings.s3_lookback_duration))
            .create()
            .await?;

    let handle = tokio::spawn(async move {
        server
            .start(listener)
            .await?
            .await
            .map_err(anyhow::Error::from)?;

        Ok(())
    });

    Ok((rx, handle))
}
