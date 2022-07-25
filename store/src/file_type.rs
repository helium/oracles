use crate::{Error, Result};
use std::{ffi::OsStr, fmt, io, path::Path};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FileType {
    CellHeartbeat,
    CellSpeedtest,
}

impl fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::CellHeartbeat => "cell_heartbeat",
            Self::CellSpeedtest => "cell_speedtest",
        };
        f.write_str(s)
    }
}

impl FileType {
    pub fn file_prefix(&self) -> &'static str {
        match self {
            Self::CellHeartbeat => "cell_heartbeat",
            Self::CellSpeedtest => "cell_speedtest",
        }
    }
}

impl TryFrom<&OsStr> for FileType {
    type Error = Error;
    fn try_from(value: &OsStr) -> Result<Self> {
        let result = match value.to_str() {
            Some("cell_heartbeat") => Self::CellHeartbeat,
            Some("cell_speedtest") => Self::CellSpeedtest,
            _ => return Err(Error::from(io::Error::from(io::ErrorKind::InvalidInput))),
        };
        Ok(result)
    }
}

impl TryFrom<&Path> for FileType {
    type Error = Error;

    fn try_from(value: &Path) -> Result<Self> {
        match value.file_stem().and_then(|f| Path::new(f).file_stem()) {
            Some(prefix) => Self::try_from(prefix),
            None => Err(Error::not_found("no file prefix found")),
        }
    }
}
