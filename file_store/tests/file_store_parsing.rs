use std::str::FromStr;

use aws_local::{aws_local_default_endpoint, gen_bucket_name, AwsLocal};
use chrono::{DateTime, Utc};
use file_store::{
    file_parsers::{MsgDecodeParser, ProstParser},
    speedtest::{CellSpeedtest, CellSpeedtestIngestReport},
    FileType,
};
use futures::StreamExt;
use helium_crypto::PublicKeyBinary;
use helium_proto::services::poc_mobile::SpeedtestIngestReportV1;

#[tokio::test]
async fn single_message_decode() -> anyhow::Result<()> {
    let bucket_name = gen_bucket_name();
    let endpoint = aws_local_default_endpoint();
    let aws = AwsLocal::new(&endpoint, &bucket_name).await;

    // NOTE: Timestamp is not Utc::now() to keep from having to truncate milliseconds and seconds
    // through the conversion to/from protobuf.
    let ts = DateTime::<Utc>::from_str("2023-10-01T12:00:00Z")?;

    let speedtest_report = CellSpeedtestIngestReport {
        received_timestamp: ts,
        report: CellSpeedtest {
            pubkey: PublicKeyBinary::from(vec![1]),
            serial: "test-serial".to_string(),
            timestamp: ts,
            upload_speed: 100,
            download_speed: 100,
            latency: 0,
        },
    };

    // Upload a simple file
    let proto = SpeedtestIngestReportV1::from(speedtest_report.clone());
    aws.put_proto_to_aws(vec![proto], FileType::CellSpeedtestIngestReport, "test")
        .await?;

    // Now we're dealing directly with FileStore
    let file_store = aws.file_store;
    let list = file_store
        .list_all(FileType::CellSpeedtestIngestReport.to_str(), None, None)
        .await?;
    assert_eq!(1, list.len());
    let file = list[0].clone();

    // Decoding to Rust struct with MsgDecode trait
    let msg_store = file_store.with_parser::<CellSpeedtestIngestReport>(MsgDecodeParser);
    let mut msg_stream = msg_store.stream_file(file.clone()).await?;
    let msg = msg_stream.next().await.expect("msg_stream msg");
    // The decoder should give us exactly what we encoded.
    assert_eq!(msg, speedtest_report);

    // Decoding to protobuf with prost::Message trait
    let proto_store = file_store.with_parser::<SpeedtestIngestReportV1>(ProstParser);
    let mut proto_stream = proto_store.stream_file(file).await?;
    let proto = proto_stream.next().await.expect("proto_stream msg");
    // The decode should give us the proto version of the struct we encoded.
    assert_eq!(proto, SpeedtestIngestReportV1::from(speedtest_report));

    Ok(())
}
