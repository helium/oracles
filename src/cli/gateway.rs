use crate::{
    api::gateway::Gateway,
    cli::{mk_db_pool, print_json},
    maker, Error, PublicKey, Result,
};
use chrono::{TimeZone, Utc};
use serde_json::json;
use std::{path::PathBuf, str::FromStr};

/// Import eligible gateways to align with the blockchain-node this follower is
/// connected to  
#[derive(Debug, clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: GatewayCmd,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum GatewayCmd {
    Import(Import),
}

impl GatewayCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::Import(cmd) => cmd.run().await,
        }
    }
}

/// Import a csv of gateway records from stdin. The entries are filtered by the
/// allowed makers as listed in the makers table
#[derive(Debug, clap::Args)]
pub struct Import {
    path: PathBuf,
}

#[derive(Debug, serde::Deserialize)]
pub struct GatewayLine {
    pub pubkey: String,
    pub owner: String,
    pub payer: String,
    pub height: i64,
    pub txn_hash: String,
    pub block_timestamp: String,
}

impl TryFrom<GatewayLine> for Gateway {
    type Error = Error;
    fn try_from(value: GatewayLine) -> Result<Self> {
        Ok(Self {
            pubkey: PublicKey::from_str(&value.pubkey)?,
            owner: PublicKey::from_str(&value.owner)?,
            payer: PublicKey::from_str(&value.payer)?,
            height: value.height,
            txn_hash: value.txn_hash,
            block_timestamp: Utc
                .datetime_from_str(&value.block_timestamp, "%Y-%m-%d %H:%M:%S%.f%#z")?,
            last_attach: None,
            last_heartbeat: None,
            last_speedtest: None,
        })
    }
}

impl Import {
    pub async fn run(&self) -> Result {
        let pool = mk_db_pool(1).await?;

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_path(&self.path)?;
        let mut processed: usize = 0;
        for result in rdr.deserialize() {
            let record: GatewayLine = result?;
            let gateway = Gateway::try_from(record)?;
            if maker::allows(&gateway.payer) {
                gateway.insert_into(&pool).await?;
            } else {
                eprintln!(
                    "Ignoring {gateway} for unknown maker {maker}",
                    gateway = gateway.pubkey,
                    maker = gateway.payer
                );
            }
            processed += 1;
        }
        pool.close().await;
        print_json(&json!({ "processed": processed }))
    }
}
