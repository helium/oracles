pub mod assignment;
pub mod urbanization;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::geofence::GeofenceValidator;
pub use assignment::Assignment;
use sqlx::PgPool;
pub use urbanization::Urbanization;
use hextree::disktree::DiskTreeMap;

pub trait DataSet {
    const PREFIX: &'static str;

    fn update(&mut self, path: &Path) -> hextree::Result<()>;

    fn assign(&self, hex: u64) -> Assignment;
}

pub struct DataSetDownloaderDaemon<A> {
    pool: PgPool,
    urbanization: A,
    /*
    footfall: B,
    landtype: C,
    */
}

impl<A> DataSetDownloaderDaemon<A> {
    pub fn run(self) -> anyhow::Result<()> {
        /*
        find the next timestamp for each data set
        insert that into the pending table,
        wait until the given time and then download that file
        update the data sets
         */

        Ok(())
    }
}

pub trait DiskTreeLike: Send + Sync + 'static {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>>;
}

impl DiskTreeLike for DiskTreeMap {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        self.get(cell)
    }
}

impl DiskTreeLike for HashMap<hextree::Cell, Vec<u8>> {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(self.get(&cell).map(|x| (cell, x.as_slice())))
    }
}

pub struct MockDiskTree;

impl DiskTreeLike for MockDiskTree {
    fn get(&self, cell: hextree::Cell) -> hextree::Result<Option<(hextree::Cell, &[u8])>> {
        Ok(Some((cell, &[])))
    }
}
