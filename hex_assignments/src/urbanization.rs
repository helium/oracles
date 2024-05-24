use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, HexAssignment};

pub struct Urbanization {
    pub urbanized: Option<DiskTreeMap>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Urbanization {
    pub fn new(urbanized: Option<DiskTreeMap>) -> Self {
        Self {
            urbanized,
            timestamp: None,
        }
    }
}

impl Default for Urbanization {
    fn default() -> Self {
        Self::new(None)
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
