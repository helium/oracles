use std::{
    mem,
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use task_manager::ManagedTask;
use tokio::time;

use crate::{
    file_sink::{
        manifest::{FileManifest, FileManifestError},
        message::{Message, MessageReceiver},
        rolling_file_sink::{RollingFileSink, RollingFileSinkError, RollingFileWriteResult},
        SINK_CHECK_MILLIS,
    },
    file_upload::{FileUpload, FileUploadClientError},
};

pub type Result<T = ()> = std::result::Result<T, FileSinkServerError>;

#[derive(thiserror::Error, Debug)]
pub enum FileSinkServerError {
    #[error("Failed file system operation: {0}")]
    Fs(#[from] fs::FsError),

    #[error("Failed to upload file: {0}")]
    Upload(#[from] FileUploadClientError),

    #[error("Failed to cleanup file on shutdown: {0}")]
    Cleanup(RollingFileSinkError),

    #[error("Expected sink filname")]
    NoSinkFilename(PathBuf),

    #[error("sink error: {0}")]
    Sink(#[from] RollingFileSinkError),

    #[error("manifest error: {0}")]
    Manifest(#[from] FileManifestError),
}

#[derive(Debug)]
pub struct FileSinkServer<T> {
    pub(crate) target_path: PathBuf,
    pub(crate) rolling_sink: RollingFileSink,
    pub(crate) messages: MessageReceiver<T>,
    pub(crate) file_upload: FileUpload,
    // TODO(mj): we use stage_files mostly like a FileManifest, maybe we should
    // replace it with one. That would cause errors to bubble up when a file is
    // rolled rather than when committed. Maybe that's better?
    pub(crate) staged_files: Vec<PathBuf>,
    /// 'commit' the file to s3 automatically when either the `roll_time` is
    /// surpassed, or `max_size` would be exceeded by an incoming message.
    pub(crate) auto_commit: bool,
}

impl<T: prost::Message + Send + Sync + 'static> ManagedTask for FileSinkServer<T> {
    fn start_task(
        self: Box<Self>,
        shutdown: triggered::Listener,
    ) -> task_manager::TaskLocalBoxFuture {
        task_manager::spawn(self.run(shutdown).err_into())
    }
}

impl<T: prost::Message> FileSinkServer<T> {
    pub(crate) async fn init(&mut self) -> Result {
        fs::create_dir_all(&self.target_path).await?;
        fs::create_dir_all(self.rolling_sink.dir()).await?;

        // TODO(mj): This would be a good place to enforce that sinks can only
        // upload their own file types. Not if they just match the prefix. Ex:
        // the problem Brian pointed out where we have
        //
        // RadioUsageStatsIngestReport => "radio_usage_stats_ingest_report",
        // RadioUsageStatsIngestReportV2 => "radio_usage_stats_ingest_report_v2",
        //
        // where the old sink can upload files from the new sink.

        // Notify all existing completed sinks via file uploads
        let mut dir = fs::read_dir(&self.target_path).await?;
        while let Ok(Some(entry)) = dir.next_entry().await {
            if !fs::matches_prefix(&entry, self.rolling_sink.prefix()) {
                continue;
            }

            self.file_upload.upload_file(&entry.path()).await?;
        }

        // Move any partial previous sink files to the target
        let mut dir = fs::read_dir(self.rolling_sink.dir()).await?;
        while let Ok(Some(entry)) = dir.next_entry().await {
            if !fs::matches_prefix(&entry, self.rolling_sink.prefix()) {
                continue;
            }

            if self.auto_commit {
                // TODO(mj): we should add some err logging here
                let _ = self.deposit_sink(&entry.path()).await;
            } else {
                let _ = fs::remove_file(&entry.path()).await;
            }
        }

        Ok(())
    }

    pub async fn run(mut self, shutdown: triggered::Listener) -> Result<()> {
        let prefix = self.rolling_sink.prefix().to_string();
        tracing::info!(
            "starting file sink {} in {}",
            prefix,
            self.target_path.display()
        );

        let mut rollover_timer = time::interval(Duration::from_millis(SINK_CHECK_MILLIS));
        rollover_timer.set_missed_tick_behavior(time::MissedTickBehavior::Burst);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.clone() => break,
                _ = rollover_timer.tick() => self.maybe_roll(Utc::now()).await?,
                msg = self.messages.recv() => match msg {
                    Some(Message::Data(on_write_tx, item)) => {
                        let res = self.write(item).await;
                        let _ = on_write_tx.send(res);
                    }
                    Some(Message::Commit(on_commit_tx)) => {
                        let res = self.commit().await;
                        let _ = on_commit_tx.send(res);
                    }
                    Some(Message::Rollback(on_rollback_tx)) => {
                        let res = self.rollback().await;
                        let _ = on_rollback_tx.send(res);
                    }
                    None => {
                        break
                    }
                }
            }
        }

        self.cleanup().await?;
        tracing::info!("stopping file sink {}", &prefix);

        Ok(())
    }

    async fn cleanup(&mut self) -> Result<()> {
        let maybe_closed = self
            .rolling_sink
            .close_current_file_if_exists()
            .await
            .map_err(FileSinkServerError::Cleanup)?;

        if let Some(closed) = maybe_closed {
            // auto_commit files will be uploaded on restart.
            tracing::info!(
                path = closed.display().to_string(),
                auto_commit = self.auto_commit,
                "existing file cleanup"
            );
        }

        Ok(())
    }

    async fn write(&mut self, item: T) -> Result {
        let buf = Self::encode_msg(item);

        match self.rolling_sink.write(buf).await? {
            RollingFileWriteResult::Wrote => (),
            RollingFileWriteResult::Rolled { closed_file } => {
                if self.auto_commit {
                    self.deposit_sink(&closed_file).await?;
                } else {
                    self.staged_files.push(closed_file);
                }
            }
        }
        Ok(())
    }

    async fn commit(&mut self) -> Result<FileManifest> {
        if let Some(closed_file) = self.rolling_sink.close_current_file_if_exists().await? {
            self.staged_files.push(closed_file);
        }

        let mut manifest = FileManifest::default();
        let staged_files = mem::take(&mut self.staged_files);

        for staged_file in staged_files.into_iter() {
            self.deposit_sink(staged_file.as_path()).await?;
            manifest.push(staged_file)?;
        }

        Ok(manifest)
    }

    async fn rollback(&mut self) -> Result<FileManifest> {
        if let Some(closed_file) = self.rolling_sink.close_current_file_if_exists().await? {
            self.staged_files.push(closed_file);
        }

        let mut manifest = FileManifest::default();
        let staged_files = mem::take(&mut self.staged_files);

        for staged_file in staged_files.into_iter() {
            fs::remove_file(&staged_file).await?;
            manifest.push(staged_file)?;
        }

        Ok(manifest)
    }

    async fn maybe_roll(&mut self, now: DateTime<Utc>) -> Result {
        if self.rolling_sink.should_close(now) {
            let rolled_file = self.rolling_sink.close_current_file().await?;
            if self.auto_commit {
                self.deposit_sink(&rolled_file).await?;
            } else {
                self.staged_files.push(rolled_file);
            }
        }

        Ok(())
    }

    async fn deposit_sink(&mut self, sink_path: &Path) -> Result {
        if !sink_path.exists() {
            tracing::warn!(?sink_path, "sink path does not exist");
            return Ok(());
        }

        let target_filename = sink_path
            .file_name()
            .ok_or_else(|| FileSinkServerError::NoSinkFilename(sink_path.to_owned()))?;

        let target_path = self.target_path.join(target_filename);

        fs::rename(sink_path, &target_path).await?;
        self.file_upload.upload_file(&target_path).await?;

        Ok(())
    }

    fn encode_msg(item: T) -> bytes::Bytes {
        bytes::Bytes::from(item.encode_to_vec())
    }
}

mod fs {
    // Re-export the tokio::fs functions we need with their errors mapped to a
    // type we control.

    use std::path::{Path, PathBuf};

    use tokio::fs::{self, DirEntry};

    type Result<T = ()> = std::result::Result<T, FsError>;

    #[derive(thiserror::Error, Debug)]
    pub enum FsError {
        #[error("Failed to create directory: {0}")]
        CreateDirAll(PathBuf),

        #[error("Failed to read directory: {0}")]
        ReadDir(PathBuf),

        #[error("Failed to rename file from: {0} to: {1}")]
        Rename(PathBuf, PathBuf),

        #[error("Failed to remove file: {0}")]
        Remove(PathBuf),
    }

    pub async fn create_dir_all(path: &Path) -> Result {
        fs::create_dir_all(path)
            .await
            .map_err(|_| FsError::CreateDirAll(path.to_owned()))
    }

    pub async fn read_dir(path: &Path) -> Result<tokio::fs::ReadDir> {
        fs::read_dir(path)
            .await
            .map_err(|_| FsError::ReadDir(path.to_owned()))
    }

    pub async fn rename(from: &Path, to: &Path) -> Result {
        fs::rename(from, to)
            .await
            .map_err(|_| FsError::Rename(from.to_owned(), to.to_owned()))
    }

    pub async fn remove_file(path: &Path) -> Result {
        fs::remove_file(path)
            .await
            .map_err(|_| FsError::Remove(path.to_owned()))
    }

    pub fn matches_prefix(dir_entry: &DirEntry, prefix: &str) -> bool {
        let filename = dir_entry.file_name().to_string_lossy().to_string();
        // TODO(mj): be stricter with matching the whole prefix instead of a
        // part of it.
        filename.starts_with(prefix)
    }
}
