use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, DiskTreeLike, HexAssignment};

pub struct Urbanization<DT> {
    urbanized: Option<DT>,
    timestamp: Option<DateTime<Utc>>,
}

impl<DT> Urbanization<DT> {
    pub fn new() -> Self {
        Self {
            urbanized: None,
            timestamp: None,
        }
    }

    pub fn new_mock(urbanized: DT) -> Self {
        Self {
            urbanized: Some(urbanized),
            timestamp: None,
        }
    }
}

impl<DT> Default for Urbanization<DT> {
    fn default() -> Self {
        Self::new()
    }
}

impl DataSet for Urbanization<DiskTreeMap> {
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

impl<Urban> HexAssignment for Urbanization<Urban>
where
    Urban: DiskTreeLike + Send + Sync + 'static,
{
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
