use std::time::Duration;

use metrics::Label;
use tokio::sync::oneshot;

use crate::file_sink::{
    manifest::FileManifest,
    message::{Message, MessageSender},
};

const OK_LABEL: Label = Label::from_static_parts("status", "ok");
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

type FileManifestReceiver = oneshot::Receiver<super::server::Result<FileManifest>>;

#[derive(thiserror::Error, Debug)]
pub enum FileSinkClientError {
    #[error("Failed to send write message to file sink: {0}")]
    Write(String),

    #[error("Failed to send commit message to file sink: {0}")]
    Commit(String),

    #[error("Failed to send rollback message to file sink: {0}")]
    Rollback(String),

    #[error("Failed to receive response from file sink")]
    Recv(#[from] oneshot::error::RecvError),
}

#[derive(Debug, Clone)]
pub struct FileSinkClient<T> {
    pub sender: MessageSender<T>,
    pub metric: String,
}

impl<T> FileSinkClient<T> {
    pub fn new(sender: MessageSender<T>, metric: impl Into<String>) -> Self {
        let metric = metric.into();

        // Seed a value for the metric
        metrics::counter!(metric.clone(), vec![OK_LABEL]).increment(1);

        Self {
            sender,
            metric: metric.into(),
        }
    }

    pub async fn write(
        &self,
        item: impl Into<T>,
        labels: impl IntoIterator<Item = &(&'static str, &'static str)>,
    ) -> Result<oneshot::Receiver<super::server::Result>, FileSinkClientError> {
        let (on_write_tx, on_write_rx) = oneshot::channel();
        let labels = labels.into_iter().map(Label::from);

        match self
            .sender
            .send_timeout(Message::Data(on_write_tx, item.into()), SEND_TIMEOUT)
            .await
        {
            Ok(_) => {
                self.success_metric(labels);
                tracing::debug!("file_sink write succeeded for {:?}", self.metric);
                Ok(on_write_rx)
            }
            Err(_err) => {
                // No need to emit a metric because errors cause the server to
                // shut down, so only 1 error metric would ever be emitted.
                tracing::error!("file_sink write failed for {:?}", self.metric);
                Err(FileSinkClientError::Write(self.metric.to_owned()))
            }
        }
    }

    fn success_metric(&self, labels: impl Iterator<Item = Label>) {
        metrics::counter!(
            self.metric.clone(),
            labels.chain(std::iter::once(OK_LABEL)).collect::<Vec<_>>()
        )
        .increment(1);
    }

    /// Writes all messages to the file sink, return the last oneshot
    pub async fn write_all(
        &self,
        items: impl IntoIterator<Item = T>,
    ) -> Result<Option<oneshot::Receiver<super::server::Result>>, FileSinkClientError> {
        let mut last_oneshot = None;
        for item in items {
            last_oneshot = Some(self.write(item, &[]).await?);
        }

        Ok(last_oneshot)
    }

    pub async fn commit(&self) -> Result<FileManifestReceiver, FileSinkClientError> {
        let (on_commit_tx, on_commit_rx) = oneshot::channel();

        // TODO(mj): should we also be using .sent_timeout() here?
        self.sender
            .send(Message::Commit(on_commit_tx))
            .await
            .map_err(|err| {
                tracing::error!(
                    "file_sink failed to commit for {:?} with {err:?}",
                    self.metric
                );
                FileSinkClientError::Commit(self.metric.clone())
            })?;

        Ok(on_commit_rx)
    }

    pub async fn rollback(&self) -> Result<FileManifestReceiver, FileSinkClientError> {
        let (on_rollback_tx, on_rollback_rx) = oneshot::channel();
        self.sender
            .send(Message::Rollback(on_rollback_tx))
            .await
            .map_err(|e| {
                tracing::error!(
                    "file_sink failed to rollback for {:?} with {e:?}",
                    self.metric
                );
                FileSinkClientError::Rollback(self.metric.clone())
            })?;

        Ok(on_rollback_rx)
    }
}
