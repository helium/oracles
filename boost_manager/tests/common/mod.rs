use file_store::file_sink::{FileSinkClient, Message as SinkMessage};
use helium_proto::BoostedHexInfoV1 as BoostedHexInfoProto;
use helium_proto::BoostedHexUpdateV1 as BoostedHexUpdateProto;
use helium_proto::Message;
use tokio::{sync::mpsc::error::TryRecvError, time::timeout};

pub struct MockFileSinkReceiver {
    pub receiver: tokio::sync::mpsc::Receiver<SinkMessage>,
}

#[allow(dead_code)]
impl MockFileSinkReceiver {
    pub async fn receive(&mut self) -> Option<Vec<u8>> {
        match timeout(seconds(2), self.receiver.recv()).await {
            Ok(Some(SinkMessage::Data(on_write_tx, msg))) => {
                let _ = on_write_tx.send(Ok(()));
                Some(msg)
            }
            Ok(None) => None,
            Err(e) => panic!("timeout while waiting for message1 {:?}", e),
            Ok(Some(unexpected_msg)) => {
                println!("ignoring unexpected msg {:?}", unexpected_msg);
                None
            }
        }
    }

    pub async fn get_all(&mut self) -> Vec<Vec<u8>> {
        let mut buf = Vec::new();
        while let Ok(SinkMessage::Data(on_write_tx, msg)) = self.receiver.try_recv() {
            let _ = on_write_tx.send(Ok(()));
            buf.push(msg);
        }
        buf
    }

    pub fn assert_no_messages(&mut self) {
        let Err(TryRecvError::Empty) = self.receiver.try_recv() else {
            panic!("receiver should have been empty")
        };
    }

    pub async fn receive_updated_hex(&mut self) -> BoostedHexInfoProto {
        match self.receive().await {
            Some(bytes) => {
                let boosted_hex_update = BoostedHexUpdateProto::decode(bytes.as_slice())
                    .expect("failed to decode boosted hex update");
                println!("boosted hex update: {:?}", boosted_hex_update);
                match boosted_hex_update.update {
                    Some(r) => r,
                    _ => panic!("failed to get boosted hex update"),
                }
            }
            None => panic!("failed to receive boosted hex update"),
        }
    }
}

#[allow(dead_code)]
pub fn create_file_sink() -> (FileSinkClient, MockFileSinkReceiver) {
    let (tx, rx) = tokio::sync::mpsc::channel(20);
    (
        FileSinkClient {
            sender: tx,
            metric: "metric",
        },
        MockFileSinkReceiver { receiver: rx },
    )
}

pub fn seconds(s: u64) -> std::time::Duration {
    std::time::Duration::from_secs(s)
}
