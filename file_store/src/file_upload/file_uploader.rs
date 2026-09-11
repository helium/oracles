use crate::Result;
use std::path::Path;

/// Somewhere a rolled file can be handed off to be stored.
///
/// A sink does not know, and does not need to know, whether its output goes to
/// one bucket or several. It hands the file over; from that point the uploader
/// owns it, including removing it once it is safely stored.
///
/// Implemented by [`FileUpload`] for a single bucket and [`MultiFileUpload`]
/// for several. A [`crate::file_sink::FileSink`] is generic over this, and the
/// concrete type is inferred from the uploader handed to
/// [`crate::FileSinkBuilder::new`], so no caller has to name it.
#[async_trait::async_trait]
pub trait FileUploader: Send + Sync {
    async fn upload_file(&self, file: &Path) -> Result;
}
