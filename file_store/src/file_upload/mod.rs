use crate::{BucketClient, Error, Result};
use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};
use tokio::{fs, sync::mpsc};

pub mod inner_file_upload;
pub mod settings;

pub use inner_file_upload::{FileUploadServer, InnerFileUpload, UPLOAD_METRIC};
pub use settings::Settings;

pub type MessageSender = mpsc::UnboundedSender<PathBuf>;
pub type MessageReceiver = mpsc::UnboundedReceiver<PathBuf>;

pub fn message_channel() -> (MessageSender, MessageReceiver) {
    mpsc::unbounded_channel()
}

/// What a [`crate::file_sink::FileSink`] hands its rolled files to.
///
/// Holds one [`InnerFileUpload`] per bucket — one bucket or several, the sink
/// cannot tell the difference. Each file is hardlinked into every uploader's
/// directory rather than copied, so N buckets cost one copy of the bytes on
/// disk. Each uploader then owns its own link: it retries on its own schedule
/// and unlinks when done, and the bytes survive until the last one is finished.
/// A bucket that is failing therefore cannot delete a file another bucket still
/// needs, nor hold one that every other bucket already has.
///
/// Hardlinks mean every uploader's directory must be on the same filesystem as
/// the file being handed over — rooting them at the sink's own output directory
/// satisfies that.
#[derive(Debug, Clone)]
pub struct FileUpload {
    uploads: Vec<InnerFileUpload>,
}

impl FileUpload {
    /// Connects one uploader per bucket, each staging in `<root>/<bucket>/`.
    ///
    /// Returns a server task per bucket for the caller to manage. At least one
    /// bucket is required: with none, [`Self::upload_file`] would release the
    /// sink's only copy having stored it nowhere. Two buckets may not share a
    /// name, since the directory is named for the bucket and uploaders sharing
    /// one would upload and delete each other's files.
    pub async fn new(
        buckets: Vec<BucketClient>,
        root: impl AsRef<Path>,
    ) -> Result<(Self, Vec<FileUploadServer>)> {
        if buckets.is_empty() {
            return Err(invalid_input("a FileUpload needs at least one bucket"));
        }

        let root = root.as_ref();
        let mut uploads = Vec::with_capacity(buckets.len());
        let mut servers = Vec::with_capacity(buckets.len());

        for bucket in buckets {
            let (upload, server) = InnerFileUpload::new(bucket, root).await?;
            if uploads
                .iter()
                .any(|other: &InnerFileUpload| other.dir() == upload.dir())
            {
                return Err(invalid_input(format!(
                    "two buckets share the directory {}; each needs its own",
                    upload.dir().display()
                )));
            }
            uploads.push(upload);
            servers.push(server);
        }

        Ok((Self { uploads }, servers))
    }

    /// The per-bucket uploaders, in the order they were configured. Order is
    /// only the order links are created in; no uploader is privileged.
    pub fn uploads(&self) -> &[InnerFileUpload] {
        &self.uploads
    }

    /// Test-only: waits until every bucket has finished with at least `n` files
    /// (stored, skipped, or given up on).
    ///
    /// Nothing in production waits on an upload — a sink hands a file over and
    /// moves on. This exists so tests can synchronize on completion rather than
    /// sleeping, and is `pub` only because integration tests in other crates
    /// use it.
    pub async fn wait_for_uploads_at_least(&self, n: u64) {
        for upload in &self.uploads {
            upload.wait_for_uploads_at_least(n).await;
        }
    }

    /// Builds a `FileUpload` around a raw sender with a no-op completion
    /// tracker. Useful in tests that inspect the upload channel directly and
    /// never run a server.
    pub fn from_sender(sender: MessageSender, dir: impl AsRef<Path>) -> Self {
        Self {
            uploads: vec![InnerFileUpload::from_sender(sender, dir)],
        }
    }

    /// Takes ownership of `file`: every bucket gets its own link to it, and the
    /// caller's is released.
    pub async fn upload_file(&self, file: &Path) -> Result {
        let mut links = Vec::with_capacity(self.uploads.len());
        for upload in &self.uploads {
            links.push(upload.stage(file).await?);
        }

        // The caller's link is spent once every uploader holds its own. The
        // bytes stay alive behind those until the last has stored the file.
        if !links.iter().any(|link| link == file) {
            fs::remove_file(file).await?;
        }
        Ok(())
    }
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::from(std::io::Error::new(ErrorKind::InvalidInput, message.into()))
}
