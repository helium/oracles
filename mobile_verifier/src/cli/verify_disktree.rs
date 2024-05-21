use std::{collections::HashMap, path::PathBuf};

use hextree::disktree::DiskTreeMap;

use crate::{
    boosting_oracles::{landtype::LandtypeValue, Assignment},
    Settings,
};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Path to the unzipped .h3tree file
    #[clap(long)]
    path: PathBuf,

    /// Expected type of the .h3tree file
    #[clap(long)]
    r#type: DisktreeType,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum DisktreeType {
    Landtype,
}

impl Cmd {
    pub async fn run(self, _settings: &Settings) -> anyhow::Result<()> {
        let disktree = DiskTreeMap::open(&self.path)?;

        let mut value_counts = HashMap::<u8, usize>::new();
        let mut idx: u128 = 0;
        let start = tokio::time::Instant::now();

        println!("Checking {}, this may take a while...", self.path.display());
        for x in disktree.iter()? {
            idx += 1;
            if idx % 100_000_000 == 0 {
                println!("Processed {} cells after {:?}", idx, start.elapsed());
            }
            let (_cell, vals) = x.unwrap();
            *value_counts.entry(vals[0]).or_insert(0) += 1;
        }

        println!("REPORT {}", "=".repeat(50));
        match self.r#type {
            DisktreeType::Landtype => {
                for (key, count) in value_counts {
                    let landtype = LandtypeValue::try_from(key);
                    let assignment = landtype.as_ref().map(|lt| Assignment::from(*lt));
                    // cover is formatted twice to allow for padding a result
                    println!(
                        "| {key:<4} | {count:<12} | {:<20} | {assignment:?} |",
                        format!("{landtype:?}")
                    );
                }
            }
        }

        Ok(())
    }
}
