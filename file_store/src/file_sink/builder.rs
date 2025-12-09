use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    file_sink::{
        client::FileSinkClient,
        message::message_channel,
        server::{FileSinkServer, FileSinkServerError},
        DEFAULT_SINK_ROLL_SECS,
    },
    file_upload::FileUpload,
    RollingFileSink,
};

pub struct FileSinkBuilder {
    prefix: String,
    target_path: PathBuf,
    tmp_path: PathBuf,
    max_size: usize,
    roll_time: Duration,
    file_upload: FileUpload,
    auto_commit: bool,
    metric: String,
}

impl FileSinkBuilder {
    pub fn new(
        prefix: impl ToString,
        target_path: &Path,
        file_upload: FileUpload,
        metric: impl Into<String>,
    ) -> Self {
        Self {
            prefix: prefix.to_string(),
            target_path: target_path.to_path_buf(),
            tmp_path: target_path.join("tmp"),
            max_size: 50_000_000,
            roll_time: Duration::from_secs(DEFAULT_SINK_ROLL_SECS),
            file_upload,
            auto_commit: true,
            metric: metric.into(),
        }
    }

    pub fn max_size(self, max_size: usize) -> Self {
        Self { max_size, ..self }
    }

    pub fn target_path(self, target_path: &Path) -> Self {
        Self {
            target_path: target_path.to_path_buf(),
            ..self
        }
    }

    pub fn tmp_path(self, path: &Path) -> Self {
        Self {
            tmp_path: path.to_path_buf(),
            ..self
        }
    }

    pub fn auto_commit(self, auto_commit: bool) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn roll_time(self, duration: Duration) -> Self {
        Self {
            roll_time: duration,
            ..self
        }
    }

    pub async fn create<T>(
        self,
    ) -> Result<(FileSinkClient<T>, FileSinkServer<T>), FileSinkServerError>
    where
        T: prost::Message,
    {
        let (tx, rx) = message_channel(50);

        let client = FileSinkClient::new(tx, self.metric);

        let mut sink = FileSinkServer {
            target_path: self.target_path.clone(),
            rolling_sink: RollingFileSink::new(
                self.tmp_path,
                self.prefix,
                self.max_size,
                self.roll_time,
            ),
            file_upload: self.file_upload,
            messages: rx,
            staged_files: Vec::new(),
            auto_commit: self.auto_commit,
        };
        sink.init().await?;

        Ok((client, sink))
    }
}
