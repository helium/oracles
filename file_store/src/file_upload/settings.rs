//! Where a service's rolled files go: the buckets, and the directory they stage
//! under.

use super::{invalid_input, FileUpload, FileUploadServer};
use crate::{BucketSettings, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Settings {
    /// Every bucket a rolled file is written to, keyed by a label of the
    /// operator's choosing. At least one is required.
    ///
    /// The label is not the bucket name — that lives in the entry, alongside
    /// its own region, endpoint and credentials, so one bucket can sit on a
    /// different provider from the others. Labels exist because bucket names
    /// are usually hyphenated and hyphens cannot appear in environment variable
    /// names; keying on the bucket name would put
    /// `..__BUCKETS__<LABEL>__SECRET_ACCESS_KEY` out of reach, and a mirror's
    /// credentials belong in the environment rather than the settings file.
    ///
    /// A `BTreeMap` rather than a `HashMap` so the order buckets are connected,
    /// logged and reported in is the same on every run.
    #[serde(default)]
    pub buckets: BTreeMap<String, BucketSettings>,

    /// Directory the file sinks write into, and under which each bucket stages
    /// its own `<root>/<bucket>/`.
    ///
    /// No default: it used to be each service's `cache`, and the right value
    /// differs per service, so a shared default would silently point somewhere
    /// other than where a deployment's files already are.
    pub root: PathBuf,
}

impl Settings {
    /// Connects every configured bucket, returning the uploader the sinks write
    /// through and a server task per bucket for the caller to manage.
    pub async fn connect(&self) -> Result<(FileUpload, Vec<FileUploadServer>)> {
        self.validate()?;

        let mut buckets = Vec::with_capacity(self.buckets.len());
        for bucket in self.buckets.values() {
            buckets.push(bucket.connect().await);
        }

        FileUpload::new(buckets, &self.root).await
    }

    /// Rejects settings that cannot work as written, before anything connects,
    /// so a misconfiguration is a startup error rather than a failure on the
    /// first upload.
    pub fn validate(&self) -> Result<()> {
        if self.buckets.is_empty() {
            return Err(invalid_input(
                "no upload buckets configured; at least one is required",
            ));
        }

        for (label, bucket) in &self.buckets {
            if bucket.bucket.trim().is_empty() {
                return Err(invalid_input(format!(
                    "upload bucket {label:?} has no bucket name"
                )));
            }

            // The S3 client installs static credentials only when it has both
            // halves of the pair, and otherwise falls back to the ambient AWS
            // credential chain — which cannot authenticate against a non-AWS
            // endpoint. Catch a typo in one of the two key names here rather
            // than at the first upload.
            let creds = (
                &bucket.settings.access_key_id,
                &bucket.settings.secret_access_key,
            );
            if matches!(creds, (Some(_), None) | (None, Some(_))) {
                return Err(invalid_input(format!(
                    "upload bucket {label:?} sets only one of access_key_id / \
                     secret_access_key; set both or neither"
                )));
            }
        }

        Ok(())
    }
}
