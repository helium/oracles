pub mod bucket;
pub mod dump;
pub mod dump_mobile_rewards;
pub mod info;

pub fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Bucket Commands")]
pub struct Cli {
    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Info(info::Cmd),
    Dump(dump::Cmd),
    Bucket(Box<bucket::Cmd>),
    DumpMobileRewards(dump_mobile_rewards::Cmd),
}

impl Cmd {
    pub async fn run(&self) -> anyhow::Result<()> {
        match self {
            Cmd::Info(cmd) => cmd.run().await,
            Cmd::Dump(cmd) => cmd.run().await,
            Cmd::Bucket(cmd) => cmd.run().await,
            Cmd::DumpMobileRewards(cmd) => cmd.run().await,
        }
    }
}
