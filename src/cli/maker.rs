use crate::{cli::print_json, maker, Result};

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
    List(List),
}

/// Commands on makers and their descriptions
#[derive(Debug, clap::Args)]
pub struct List {}

impl MakerCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::List(cmd) => cmd.run().await,
        }
    }
}

impl List {
    pub async fn run(&self) -> Result {
        let list = maker::allowed();
        print_json(&list)
    }
}
