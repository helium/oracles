use crate::{
    api::gateway::Gateway,
    cli::{mk_db_pool, print_json},
    PublicKey, Result,
};
use serde_json::json;

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
    Get(Get),
}

impl GatewayCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::Get(cmd) => cmd.run().await,
        }
    }
}

/// Get gateway information for a given address
#[derive(Debug, clap::Args)]
pub struct Get {
    address: PublicKey,
}

impl Get {
    pub async fn run(&self) -> Result {
        let pool = mk_db_pool(1).await?;

        let gateway = Gateway::get(&pool, &self.address).await?;

        print_json(&json!({ "gateway": gateway }))
    }
}
