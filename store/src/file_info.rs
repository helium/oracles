use crate::{datetime_from_epoch_millis, error::DecodeError, Error, Result};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::Serialize;
use std::{fmt, io, path::Path, str::FromStr};

#[derive(Debug, Clone, Serialize)]
pub struct FileInfo {
    pub file_name: String,
    pub file_type: FileType,
    pub file_timestamp: DateTime<Utc>,
}

lazy_static! {
    static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.gz)?").unwrap();
}

impl TryFrom<&Path> for FileInfo {
    type Error = Error;
    fn try_from(value: &Path) -> Result<Self> {
        let file_name = value
            .file_name()
            .map(|str| str.to_string_lossy().into_owned())
            .ok_or_else(|| Error::not_found("missing filename"))?;
        let cap = RE
            .captures(&file_name)
            .ok_or_else(|| DecodeError::file_info("failed to decode file info"))?;
        let file_type = FileType::from_str(&cap[1])?;
        let file_timestamp = datetime_from_epoch_millis(
            u64::from_str(&cap[2])
                .map_err(|_| DecodeError::file_info("faild to decode timestamp"))?,
        );
        Ok(Self {
            file_name,
            file_type,
            file_timestamp,
        })
    }
}

impl FromStr for FileInfo {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::try_from(Path::new(s))
    }
}

impl From<(FileType, DateTime<Utc>)> for FileInfo {
    fn from(v: (FileType, DateTime<Utc>)) -> Self {
        Self::new(v.0, v.1)
    }
}

impl FileInfo {
    pub fn matches(str: &str) -> bool {
        RE.is_match(str)
    }

    pub fn new(file_type: FileType, file_timestamp: DateTime<Utc>) -> Self {
        let file_name = format!("{}.{}.gz", file_type, file_timestamp.timestamp_millis());
        Self {
            file_name,
            file_type,
            file_timestamp,
        }
    }
}

pub const CELL_HEARTBEAT: &str = "cell_heartbeat";
pub const CELL_SPEEDTEST: &str = "cell_speedtest";

#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    CellHeartbeat,
    CellSpeedtest,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::CellHeartbeat => CELL_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
        };
        f.write_str(s)
    }
}

impl FileType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::CellHeartbeat => CELL_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
        }
    }
}

impl FromStr for FileType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let result = match s {
            CELL_HEARTBEAT => Self::CellHeartbeat,
            CELL_SPEEDTEST => Self::CellSpeedtest,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        };
        Ok(result)
    }
}
