use crate::traits::{TimestampDecode, TimestampDecodeError};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Serialize;
use std::{fmt, io, os::unix::fs::MetadataExt, path::Path, str::FromStr, sync::LazyLock};

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub key: String,
    pub prefix: String,
    pub timestamp: DateTime<Utc>,
    pub size: usize,
}

static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"([a-z,\d,_]+)\.(\d+)(\.gz)?").unwrap());

#[derive(thiserror::Error, Debug)]
pub enum FileInfoError {
    #[error("invalid timestamp: {0}")]
    Timestamp(#[from] TimestampDecodeError),

    #[error("invalid timestamp string: {0}")]
    TimestampStr(#[from] std::num::ParseIntError),

    #[error("filename did not match regex: {0}")]
    Regex(String),

    #[error("no file name found")]
    MissingFilename,

    #[error("IO: {0}")]
    Io(#[from] io::Error),
}

impl FromStr for FileInfo {
    type Err = FileInfoError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let key = s.to_string();
        let cap = RE
            .captures(s)
            .ok_or_else(|| FileInfoError::Regex(key.clone()))?;
        let prefix = cap[1].to_owned();

        let timestamp = u64::from_str(&cap[2])?.to_timestamp_millis()?;
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
    type Error = FileInfoError;

    fn try_from(value: &aws_sdk_s3::types::Object) -> std::result::Result<Self, Self::Error> {
        let size = value.size().unwrap_or_default() as usize;
        let key = value.key.as_ref().ok_or(FileInfoError::MissingFilename)?;
        let mut info = Self::from_str(key)?;
        info.size = size;
        Ok(info)
    }
}

impl TryFrom<&Path> for FileInfo {
    type Error = FileInfoError;

    fn try_from(value: &Path) -> std::result::Result<Self, Self::Error> {
        let mut info = Self::from_str(&value.to_string_lossy())?;
        info.size = value.metadata()?.size() as usize;
        Ok(info)
    }
}

impl FileInfo {
    pub fn from_maybe_dotted_prefix(prefix: &str, dt: DateTime<Utc>) -> Self {
        let prefix = prefix.trim_end_matches('.').to_string();
        Self {
            key: format!("{}.{}.gz", &prefix, dt.timestamp_millis()),
            prefix,
            timestamp: dt,
            size: 0,
        }
    }

    pub fn matches(str: &str) -> bool {
        RE.is_match(str)
    }
}
