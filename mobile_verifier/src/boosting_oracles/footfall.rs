use std::path::Path;

use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, DataSet, DataSetType, DiskTreeLike, HexAssignment};

pub struct Footfall<Foot> {
    footfall: Option<Foot>,
    timestamp: Option<DateTime<Utc>>,
}

impl<F> Footfall<F> {
    pub fn new() -> Self {
        Self {
            footfall: None,
            timestamp: None,
        }
    }

    pub fn new_mock(footfall: F) -> Self {
        Self {
            footfall: Some(footfall),
            timestamp: None,
        }
    }
}

impl<F> Default for Footfall<F> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl DataSet for Footfall<DiskTreeMap> {
    const TYPE: DataSetType = DataSetType::Footfall;

    fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp
    }

    fn update(&mut self, path: &Path, time_to_use: DateTime<Utc>) -> anyhow::Result<()> {
        self.footfall = Some(DiskTreeMap::open(path)?);
        self.timestamp = Some(time_to_use);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.footfall.is_some()
    }
}

impl<Foot> HexAssignment for Footfall<Foot>
where
    Foot: DiskTreeLike + Send + Sync + 'static,
{
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref footfall) = self.footfall else {
            anyhow::bail!("No footfall data set has been loaded");
        };

        match footfall.get(cell)? {
            Some((_, &[x])) if x >= 1 => Ok(Assignment::A),
            Some((_, &[0])) => Ok(Assignment::B),
            None => Ok(Assignment::C),
            Some((_, other)) => anyhow::bail!("Unexpected disktree data: {cell:?} {other:?}"),
        }
    }
}
