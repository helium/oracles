use crate::{datetime_from_epoch_millis, error::DecodeError, Error, Result};
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use std::{fmt, io, path::Path, str::FromStr};

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub file_type: FileType,
    pub file_timestamp: DateTime<Utc>,
}

impl TryFrom<&Path> for FileInfo {
    type Error = Error;
    fn try_from(value: &Path) -> Result<Self> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"([a-z,_]+).(\d+)(.gz)?").unwrap();
        }
        let path_str = value.to_string_lossy().into_owned();
        let cap = RE
            .captures(&path_str)
            .ok_or_else(|| DecodeError::file_info("failed to decode file info"))?;
        let file_type = FileType::try_from(&cap[1])?;
        let file_timestamp = datetime_from_epoch_millis(
            u64::from_str(&cap[2])
                .map_err(|_| DecodeError::file_info("faild to decode timestamp"))?,
        );
        Ok(Self {
            file_type,
            file_timestamp,
        })
    }
}

pub const CELL_HEARTBEAT: &str = "cell_heartbeat";
pub const CELL_SPEEDTEST: &str = "cell_speedtest";

#[derive(Debug, PartialEq, Eq, Clone)]
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
    pub fn file_prefix(&self) -> &'static str {
        match self {
            Self::CellHeartbeat => CELL_HEARTBEAT,
            Self::CellSpeedtest => CELL_SPEEDTEST,
        }
    }
}

impl TryFrom<&str> for FileType {
    type Error = Error;
    fn try_from(value: &str) -> Result<Self> {
        let result = match value {
            CELL_HEARTBEAT => Self::CellHeartbeat,
            CELL_SPEEDTEST => Self::CellSpeedtest,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        };
        Ok(result)
    }
}
