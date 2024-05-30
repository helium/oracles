use chrono::{DateTime, Utc};
use hextree::disktree::DiskTreeMap;

use super::{Assignment, HexAssignment};

pub struct Footfall {
    pub footfall: Option<DiskTreeMap>,
    pub timestamp: Option<DateTime<Utc>>,
}

impl Footfall {
    pub fn new(footfall: Option<DiskTreeMap>) -> Self {
        Self {
            footfall,
            timestamp: None,
        }
    }
}

impl Default for Footfall {
    fn default() -> Self {
        Self::new(None)
    }
}

impl HexAssignment for Footfall {
    fn assignment(&self, cell: hextree::Cell) -> anyhow::Result<Assignment> {
        let Some(ref footfall) = self.footfall else {
            anyhow::bail!("No footfall data set has been loaded");
        };

        // The footfall disktree maps hexes to a single byte, a value of one indicating
        // assignment A and a value of zero indicating assignment B. If no value is present,
        // assignment C is given.
        match footfall.get(cell)? {
            Some((_, &[x])) if x >= 1 => Ok(Assignment::A),
            Some((_, &[0])) => Ok(Assignment::B),
            None => Ok(Assignment::C),
            Some((_, other)) => anyhow::bail!("Unexpected disktree data: {cell:?} {other:?}"),
        }
    }
}
