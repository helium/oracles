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
    /// Connects one uploader per bucket, each staging in
    /// `<root>/<label>_<bucket>/`.
    ///
    /// Buckets are given as `(label, client)`. The label is what keeps two
    /// buckets that share a name on different providers separate on disk; the
    /// bucket name is there so the directory says where its files are going. Returns a server task per bucket for the caller to
    /// manage. At least one bucket is required: with none, [`Self::upload_file`]
    /// would release the sink's only copy having stored it nowhere.
    pub async fn new(
        buckets: Vec<(String, BucketClient)>,
        root: impl AsRef<Path>,
    ) -> Result<(Self, Vec<FileUploadServer>)> {
        if buckets.is_empty() {
            return Err(invalid_input("a FileUpload needs at least one bucket"));
        }

        let root = root.as_ref();
        let mut uploads = Vec::with_capacity(buckets.len());
        let mut servers = Vec::with_capacity(buckets.len());

        for (label, bucket) in buckets {
            let (upload, server) = InnerFileUpload::new(&label, bucket, root).await?;
            // Labels are unique when they come from settings, where they are
            // map keys. This guards the direct path.
            if uploads
                .iter()
                .any(|other: &InnerFileUpload| other.dir() == upload.dir())
            {
                return Err(invalid_input(format!(
                    "two buckets share the directory {}; each needs its own label",
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
    ///
    /// A bucket that cannot be staged into is logged and skipped rather than
    /// returned as an error. This is called from a sink's roll, where an error
    /// aborts the sink task and takes the whole service down with it — so
    /// propagating would let one bucket's unwritable directory stop every other
    /// bucket and the service besides, which is the coupling this type exists
    /// to avoid.
    pub async fn upload_file(&self, file: &Path) -> Result {
        let mut links = Vec::with_capacity(self.uploads.len());
        let mut staged_everywhere = true;

        for upload in &self.uploads {
            match upload.stage(file).await {
                Ok(link) => links.push(link),
                Err(err) => {
                    tracing::error!(
                        "failed to stage {} for {}: {err:?}",
                        file.display(),
                        upload.bucket()
                    );
                    staged_everywhere = false;
                }
            }
        }

        // The caller's link is spent once every uploader holds its own. If one
        // missed out, keep it: the sink hands over whatever is still here at
        // the next startup, which is that bucket's second chance.
        if staged_everywhere && !links.iter().any(|link| link == file) {
            if let Err(err) = fs::remove_file(file).await {
                tracing::error!("failed to release {}: {err:?}", file.display());
            }
        }

        Ok(())
    }
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::from(std::io::Error::new(ErrorKind::InvalidInput, message.into()))
}
