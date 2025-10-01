use crate::{
    gateway::db::{Gateway, LocationChangedAtUpdate},
    settings::Settings,
};
use chrono::{DateTime, Utc};
use h3o::{error::InvalidCellIndex, CellIndex};
use helium_crypto::PublicKey;
use serde::Deserialize;
use std::{
    fs::File,
    path::{Path, PathBuf},
    str::FromStr,
    time::Instant,
};

#[derive(Debug, clap::Parser)]
pub struct Import {
    #[clap(short = 'f')]
    file: PathBuf,

    #[clap(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    HotspotsAssertions,
}

impl Import {
    pub async fn run(self, settings: &Settings) -> anyhow::Result<()> {
        match self.cmd {
            Cmd::HotspotsAssertions => {
                custom_tracing::init(settings.log.clone(), settings.custom_tracing.clone()).await?;

                tracing::info!("started");

                let pool = settings.database.connect("mobile-config-store").await?;

                let start = Instant::now();

                let updates = read_csv(self.file)?
                    .into_iter()
                    .filter_map(|row| row.try_into().ok())
                    .collect::<Vec<LocationChangedAtUpdate>>();

                tracing::info!("file read, updating {} records", updates.len());

                let updated = Gateway::update_bulk_location_changed_at(&pool, &updates).await?;

                let elapsed = start.elapsed();
                tracing::info!(?elapsed, updated, "finished");

                Ok(())
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct CsvRow {
    public_key: PublicKey,
    // serialnumber: String,
    time: DateTime<Utc>,
    // latitude: f64,
    // longitude: f64,
    h3: String,
    // assertion_type: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CsvRowError {
    #[error("H3 index parse error: {0}")]
    H3IndexParseError(#[from] InvalidCellIndex),
}

impl TryFrom<CsvRow> for LocationChangedAtUpdate {
    type Error = CsvRowError;

    fn try_from(row: CsvRow) -> Result<Self, Self::Error> {
        let cell = CellIndex::from_str(&row.h3)?;

        Ok(Self {
            address: row.public_key.into(),
            location_changed_at: row.time,
            location: cell.into(),
        })
    }
}

fn read_csv<P: AsRef<Path>>(path: P) -> anyhow::Result<Vec<CsvRow>> {
    let file = File::open(path)?;
    let mut rdr = csv::Reader::from_reader(file);
    let mut rows = Vec::new();

    for result in rdr.deserialize() {
        let record: CsvRow = result?;
        rows.push(record);
    }

    Ok(rows)
}
