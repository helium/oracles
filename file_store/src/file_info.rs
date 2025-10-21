use crate::{traits::TimestampDecode, DecodeError, Error, Result};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Serialize;
use std::{fmt, os::unix::fs::MetadataExt, path::Path, str::FromStr, sync::LazyLock};

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub key: String,
    pub prefix: String,
    pub timestamp: DateTime<Utc>,
    pub size: usize,
}

static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([a-z,\d,_]+)\.(\d+)(\.gz)?").unwrap());

impl FromStr for FileInfo {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| DecodeError::file_info("failed to decode file info"))?;
        let prefix = cap[1].to_owned();
        let timestamp = u64::from_str(&cap[2])
            .map_err(|_| DecodeError::file_info("failed to decode timestamp"))?
            .to_timestamp_millis()?;
        Ok(Self {
            key,
            prefix,
            timestamp,
            size: 0,
        })
    }
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl AsRef<str> for FileInfo {
    fn as_ref(&self) -> &str {
        &self.key
    }
}

impl From<FileInfo> for String {
    fn from(v: FileInfo) -> Self {
        v.key
    }
}

impl<T: Into<String>> From<(T, DateTime<Utc>)> for FileInfo {
    fn from(value: (T, DateTime<Utc>)) -> Self {
        let (prefix, timestamp) = value;
        let prefix = prefix.into();
        Self {
            key: format!("{}.{}.gz", &prefix, timestamp.timestamp_millis()),
            prefix,
            timestamp,
            size: 0,
        }
    }
}

impl TryFrom<&aws_sdk_s3::types::Object> for FileInfo {
    type Error = Error;
    fn try_from(value: &aws_sdk_s3::types::Object) -> Result<Self> {
        let size = value.size().unwrap_or_default() as usize;
        let key = value
            .key
            .as_ref()
            .ok_or_else(|| Error::not_found("no file name found"))?;
        let mut info = Self::from_str(key)?;
        info.size = size;
        Ok(info)
    }
}

impl TryFrom<&Path> for FileInfo {
    type Error = Error;
    fn try_from(value: &Path) -> Result<Self> {
        let mut info = Self::from_str(&value.to_string_lossy())?;
        info.size = value.metadata()?.size() as usize;
        Ok(info)
    }
}

impl FileInfo {
    pub fn matches(str: &str) -> bool {
        RE.is_match(str)
    }
}
