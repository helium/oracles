use crate::file_upload::file_uploader::FileUploader;
use crate::file_upload::FileUpload;
use crate::{Error, Result};
use std::path::Path;
use std::{io::ErrorKind, path::PathBuf};
use tokio::fs;

/// Fans one file out to several buckets, one [`FileUpload`] each.
///
/// The file is **hardlinked** into every uploader's directory rather than
/// copied, so N buckets cost one copy of the bytes on disk. Each uploader then
/// owns its own link: it retries on its own schedule and unlinks when done, and
/// the bytes survive until the last one is finished. A bucket that is failing
/// therefore cannot delete a file another bucket still needs, nor hold one that
/// every other bucket already has.
///
/// Hardlinks mean every uploader's directory must be on the same filesystem as
/// the file being handed over — placing them under the sink's own output
/// directory satisfies that.
#[derive(Debug, Clone)]
pub struct MultiFileUpload {
    uploads: Vec<(PathBuf, FileUpload)>,
}

impl MultiFileUpload {
    /// Takes the first uploader separately so the set cannot be empty: with no
    /// uploaders, [`FileUploader::upload_file`] would unlink the sink's only
    /// copy having stored it nowhere.
    ///
    /// Every uploader must own a directory to be linked into, and no two may
    /// share one — uploaders sharing a directory would upload and delete each
    /// other's files. Order is only the order links are created in; no uploader
    /// is privileged.
    pub fn new(first: FileUpload, rest: Vec<FileUpload>) -> Result<Self> {
        let mut uploads: Vec<(PathBuf, FileUpload)> = Vec::with_capacity(1 + rest.len());

        for upload in std::iter::once(first).chain(rest) {
            let Some(dir) = upload.dir().map(Path::to_path_buf) else {
                return Err(invalid_input(
                    "every uploader under a MultiFileUpload needs a directory of \
                     its own; build them with FileUpload::staged_in",
                ));
            };
            if uploads.iter().any(|(existing, _)| *existing == dir) {
                return Err(invalid_input(format!(
                    "two uploaders share the directory {}; each needs its own",
                    dir.display()
                )));
            }
            uploads.push((dir, upload));
        }

        Ok(Self { uploads })
    }
}

#[async_trait::async_trait]
impl FileUploader for MultiFileUpload {
    async fn upload_file(&self, file: &Path) -> Result {
        let Some(name) = file.file_name() else {
            return Err(Error::from(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("expected a file name in {}", file.display()),
            )));
        };

        for (dir, upload) in &self.uploads {
            let link = dir.join(name);
            match fs::hard_link(file, &link).await {
                Ok(()) => {}
                // Staged by a run that did not finish. The name identifies the
                // rolled file, so the link already there is the same file; that
                // uploader's startup scan has queued it, and queueing it again
                // below is harmless.
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {}
                Err(err) => return Err(Error::from(err)),
            }
            upload.upload_file(&link).await?;
        }

        // Our link is spent. The bytes stay alive behind the uploaders' links
        // until the last of them has stored the file.
        fs::remove_file(file).await?;
        Ok(())
    }
}

fn invalid_input(message: impl Into<String>) -> Error {
    Error::from(std::io::Error::new(ErrorKind::InvalidInput, message.into()))
}
