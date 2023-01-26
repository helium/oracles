use file_store::{FileInfo, FileStore};
use futures::{Stream, StreamExt};
use helium_proto::services::router::PacketRouterPacketReportV1;
use prost::Message;

pub const DOWNLOAD_WORKERS: usize = 50;

pub async fn ingest_reports(
    file_store: &FileStore,
    info: FileInfo,
) -> Result<impl Stream<Item = PacketRouterPacketReportV1>, file_store::Error> {
    Ok(file_store
        .stream_file(info)
        .await?
        .filter_map(|msg| async move {
            msg.map_err(|err| {
                tracing::error!("Error fetching packet report: {:?}", err);
                err
            })
            .ok()
        })
        .filter_map(|msg| async move {
            PacketRouterPacketReportV1::decode(msg).map_or_else(
                |err| {
                    tracing::error!("Could not decode packet report: {:?}", err);
                    None
                },
                |report| Some(report),
            )
        }))
}
