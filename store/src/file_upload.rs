use crate::{env_var, Error, FileStore, Result};
use futures::StreamExt;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, sync::mpsc, time};
use tokio_stream::wrappers::UnboundedReceiverStream;

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

pub async fn upload_file(tx: &MessageSender, file: &Path) -> Result {
    tx.send(file.to_path_buf()).map_err(|_| Error::channel())
}

pub struct FileUpload {
    enabled: bool,
    messages: UnboundedReceiverStream<PathBuf>,
    store: FileStore,
}

impl FileUpload {
    pub async fn from_env(messages: MessageReceiver) -> Result<Self> {
        let enabled = env_var("FILE_UPLOAD_ENABLED")?.map_or_else(|| true, |str| str == "true");

        Ok(Self {
            enabled,
            messages: UnboundedReceiverStream::new(messages),
            store: FileStore::from_env().await?,
        })
    }

    pub async fn run(self, shutdown: &triggered::Listener) -> Result {
        tracing::info!("starting file uploader");

        let enabled = self.enabled;
        let uploads = self
            .messages
            .map(|msg| (self.store.clone(), msg))
            .for_each_concurrent(5, |(store, path)| async move {
                let path_str = path.display();
                let bucket = &store.bucket;
                if !enabled {
                    tracing::info!("file upload disabled for {path_str} to {bucket}");
                    return;
                }
                if !path.exists() {
                    tracing::warn!("ignoring absent file {path_str}");
                    return;
                }
                if !path.is_file() {
                    tracing::warn!("ignoring non file {path_str}");
                    return;
                }
                let mut retry = 0;
                const MAX_RETRIES: u8 = 5;
                const RETRY_WAIT: Duration = Duration::from_secs(10);
                while retry <= MAX_RETRIES {
                    tracing::debug!("storing {path_str} in {bucket} retry {retry}");
                    match store.put(&path).await {
                        Ok(()) => {
                            match fs::remove_file(&path).await {
                                Ok(()) => {
                                    tracing::info!("stored {path_str} in {bucket}");
                                }
                                Err(err) => {
                                    tracing::error!(
                                        "failed to remove uploaded file {path_str}: {err:?}"
                                    );
                                }
                            }
                            return;
                        }
                        Err(err) => {
                            tracing::error!(
                                "failed to store {path_str} in {bucket} retry: {retry}: {err:?}"
                            );
                            retry += 1;
                            time::sleep(RETRY_WAIT).await;
                        }
                    }
                }
            });

        tokio::select! {
            _ = uploads => (),
            _ = shutdown.clone() => (),
        }

        tracing::info!("stopping file uploader");
        Ok(())
    }
}
