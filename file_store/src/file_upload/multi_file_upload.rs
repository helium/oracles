use crate::file_upload::file_uploader::FileUploader;
use crate::file_upload::FileUpload;
use crate::{file_upload::invalid_input, Result};
use std::path::Path;
use tokio::fs;

/// Fans one file out to several buckets, one [`FileUpload`] each.
///
/// Each uploader hardlinks the file into its own directory rather than copying
/// it, so N buckets cost one copy of the bytes on disk. Each then owns its own
/// link: it retries on its own schedule and unlinks when done, and the bytes
/// survive until the last one is finished. A bucket that is failing therefore
/// cannot delete a file another bucket still needs, nor hold one that every
/// other bucket already has.
///
/// Hardlinks mean every uploader's directory must be on the same filesystem as
/// the file being handed over — rooting them at the sink's own output directory
/// satisfies that.
#[derive(Debug, Clone)]
pub struct MultiFileUpload {
    uploads: Vec<FileUpload>,
}

impl MultiFileUpload {
    /// Takes the first uploader separately so the set cannot be empty: with no
    /// uploaders, [`FileUploader::upload_file`] would release the caller's only
    /// copy having stored it nowhere.
    ///
    /// No two uploaders may share a directory — they would upload and delete
    /// each other's files. Since a directory is named for its bucket, that
    /// amounts to requiring distinct bucket names under one root. Order is only
    /// the order links are created in; no uploader is privileged.
    pub fn new(first: FileUpload, rest: Vec<FileUpload>) -> Result<Self> {
        let uploads: Vec<FileUpload> = std::iter::once(first).chain(rest).collect();

        for (i, upload) in uploads.iter().enumerate() {
            if uploads[..i].iter().any(|other| other.dir() == upload.dir()) {
                return Err(invalid_input(format!(
                    "two uploaders share the directory {}; each needs its own",
                    upload.dir().display()
                )));
            }
        }

        Ok(Self { uploads })
    }
}

#[async_trait::async_trait]
impl FileUploader for MultiFileUpload {
    async fn upload_file(&self, file: &Path) -> Result {
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
