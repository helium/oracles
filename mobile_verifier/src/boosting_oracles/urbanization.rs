use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, HexAssignment};

pub struct Urbanization {
    urbanized: Option<DiskTreeMap>,
    timestamp: Option<DateTime<Utc>>,
}

impl Urbanization {
    pub fn new() -> Self {
        Self {
            urbanized: None,
            timestamp: None,
        }
    }

    pub fn new_mock(urbanized: DiskTreeMap) -> Self {
        Self {
            urbanized: Some(urbanized),
            timestamp: None,
        }
    }
}

impl Default for Urbanization {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl DataSet for Urbanization {
    const TYPE: DataSetType = DataSetType::Urbanization;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.urbanized = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.urbanized.is_some()
    }
}

impl HexAssignment for Urbanization {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref urbanized) = self.urbanized else {
            anyhow::bail!("No urbanization data set has been loaded");
        };
        match urbanized.get(cell)? {
            Some((_, &[1])) => Ok(Assignment::A),
            Some((_, &[0])) => Ok(Assignment::B),
            None => Ok(Assignment::C),
            Some((_, other)) => {
                anyhow::bail!("unexpected urbanization disktree data: {cell:?} {other:?}")
            }
        }
    }
}
