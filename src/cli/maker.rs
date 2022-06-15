use crate::{
    cli::{mk_db_pool, print_json},
    Maker, PublicKey, Result,
};
use serde_json::json;

/// Add or remove eligible makers
#[derive(Debug, clap::Parser)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: MakerCmd,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum MakerCmd {
    Add(Add),
    Remove(Remove),
    List(List),
}

/// Add a maker to the approved list to make their hotspots eligible for rewards
#[derive(Debug, clap::Args)]
pub struct Add {
    pub pubkey: PublicKey,
    pub description: Option<String>,
}

/// Remove a given maker by their pubkey
#[derive(Debug, clap::Args)]
pub struct Remove {
    pub pubkey: PublicKey,
}

/// List all stored makers with their descriptions
#[derive(Debug, clap::Args)]
pub struct List {}

impl MakerCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::Add(cmd) => cmd.run().await,
            Self::Remove(cmd) => cmd.run().await,
            Self::List(cmd) => cmd.run().await,
        }
    }
}

impl Add {
    pub async fn run(&self) -> Result {
        let pool = mk_db_pool(1).await?;
        let result = Maker::new(self.pubkey.clone(), self.description.clone())
            .insert_into(&pool)
            .await?;
        print_json(&result)
    }
}

impl Remove {
    pub async fn run(&self) -> Result {
        let pool = mk_db_pool(1).await?;
        Maker::remove(&self.pubkey, &pool).await?;
        print_json(&json!({
            "pubkey": self.pubkey,
        }))
    }
}

impl List {
    pub async fn run(&self) -> Result {
        let pool = mk_db_pool(1).await?;
        let list = Maker::list(&pool).await?;
        print_json(&list)
    }
}
