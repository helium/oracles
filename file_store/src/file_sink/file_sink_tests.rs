// mod tests {
//     use super::*;
//     use crate::{file_source, file_upload, FileInfo};
//     use futures::stream::StreamExt;
//     use std::str::FromStr;
//     use tempfile::TempDir;
//     use tokio::fs::DirEntry;

//     type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

//     #[tokio::test]
//     async fn auto_commit_file_sent_for_upload_when_size_exceeded() -> TestResult {
//         let tmp_dir = TempDir::new()?;

//         let (file_upload_tx, file_upload_rx) = file_upload::message_channel();
//         let file_upload = FileUpload {
//             sender: file_upload_tx,
//         };

//         let msg = "hello".to_string();
//         let msg_size = FileSink::encode_msg(msg.clone()).len();

//         let (_file_sink_client, mut file_sink_server) =
//             FileSinkBuilder::new("report", tmp_dir.path(), file_upload, "metric")
//                 .auto_commit(true)
//                 .max_size(msg_size + 2) // big enough for one, not for two
//                 .create::<String>()
//                 .await?;

//         file_sink_server.write(msg.clone()).await?;
//         assert!(
//             file_upload_rx.is_empty(),
//             "size not exceeded, nothing to upload"
//         );

//         file_sink_server.write(msg).await?;
//         assert!(
//             !file_upload_rx.is_empty(),
//             "size exceeded, file should be uploaded"
//         );

//         Ok(())
//     }

//     #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
//     async fn writes_a_framed_gzip_encoded_file() {
//         let tmp_dir = TempDir::new().expect("Unable to create temp dir");
//         let (shutdown_trigger, shutdown_listener) = triggered::trigger();
//         let (file_upload_tx, _file_upload_rx) = file_upload::message_channel();
//         let file_upload = FileUpload {
//             sender: file_upload_tx,
//         };

//         let file_prefix = "entropy_report";
//         let (file_sink_client, file_sink_server) =
//             FileSinkBuilder::new(file_prefix, tmp_dir.path(), file_upload, "fake_metric")
//                 .roll_time(Duration::from_millis(100))
//                 .create()
//                 .await
//                 .expect("failed to create file sink");

//         let sink_thread = tokio::spawn(async move {
//             file_sink_server
//                 .run(shutdown_listener.clone())
//                 .await
//                 .expect("failed to complete file sink");
//         });

//         let (on_write_tx, _on_write_rx) = oneshot::channel();

//         file_sink_client
//             .sender
//             .try_send(Message::Data(on_write_tx, "hello".to_string()))
//             .expect("failed to send bytes to file sink");

//         tokio::time::sleep(time::Duration::from_millis(200)).await;

//         shutdown_trigger.trigger();
//         sink_thread.await.expect("file sink did not complete");

//         let entropy_file = get_entropy_file(&tmp_dir, file_prefix)
//             .await
//             .expect("no entropy available");
//         assert_eq!("hello", read_file(&entropy_file).await);
//     }

//     #[tokio::test]
//     async fn only_uploads_after_commit_when_auto_commit_is_false() {
//         let tmp_dir = TempDir::new().expect("Unable to create temp dir");
//         let (shutdown_trigger, shutdown_listener) = triggered::trigger();
//         let (file_upload_tx, mut file_upload_rx) = file_upload::message_channel();
//         let file_upload = FileUpload {
//             sender: file_upload_tx,
//         };

//         let file_prefix = "entropy_report";

//         let (file_sink_client, file_sink_server) =
//             FileSinkBuilder::new(file_prefix, tmp_dir.path(), file_upload, "fake_metric")
//                 .roll_time(Duration::from_millis(100))
//                 .auto_commit(false)
//                 .create()
//                 .await
//                 .expect("failed to create file sink");

//         let sink_thread = tokio::spawn(async move {
//             file_sink_server
//                 .run(shutdown_listener.clone())
//                 .await
//                 .expect("failed to complete file sink");
//         });

//         let (on_write_tx, _on_write_rx) = oneshot::channel();
//         file_sink_client
//             .sender
//             .try_send(Message::Data(
//                 on_write_tx,
//                 String::into_bytes("hello".to_string()),
//             ))
//             .expect("failed to send bytes to file sink");

//         tokio::time::sleep(time::Duration::from_millis(200)).await;

//         assert!(get_entropy_file(&tmp_dir, file_prefix).await.is_err());
//         assert_eq!(
//             Err(tokio::sync::mpsc::error::TryRecvError::Empty),
//             file_upload_rx.try_recv()
//         );

//         let receiver = file_sink_client.commit().await.expect("commit failed");
//         let _ = receiver.await.expect("commit didn't complete completed");

//         assert!(file_upload_rx.try_recv().is_ok());

//         let entropy_file = get_entropy_file(&tmp_dir, file_prefix)
//             .await
//             .expect("no entropy available");
//         assert_eq!("hello", read_file(&entropy_file).await);

//         shutdown_trigger.trigger();
//         sink_thread.await.expect("file sink did not complete");
//     }

//     async fn read_file(entry: &DirEntry) -> String {
//         let bytes = file_source::source([entry.path()])
//             .next()
//             .await
//             .unwrap()
//             .expect("invalid data in file");
//         prost::Message::decode(bytes).expect("encoded string")
//     }

//     async fn get_entropy_file(
//         tmp_dir: &TempDir,
//         prefix: &'static str,
//     ) -> std::result::Result<DirEntry, String> {
//         let mut entries = fs::read_dir(tmp_dir.path())
//             .await
//             .expect("failed to read tmp dir");

//         while let Some(entry) = entries.next_entry().await.unwrap() {
//             if is_entropy_file(&entry, prefix) {
//                 return Ok(entry);
//             }
//         }

//         Err("no entropy available".to_string())
//     }

//     fn is_entropy_file(entry: &DirEntry, prefix: &'static str) -> bool {
//         entry
//             .file_name()
//             .to_str()
//             .and_then(|file_name| FileInfo::from_str(file_name).ok())
//             .is_some_and(|file_info| file_info.prefix == prefix)
//     }
// }
