use crate::{
    cli::{api::Api, import::Import, server::Server},
    settings::Settings,
};
use base64::{engine::general_purpose, Engine};
use helium_crypto::{KeyTag, Keypair};
use rand::rngs::OsRng;
use std::io::Write;
use std::{fs::File, path::PathBuf};

pub mod api;
pub mod import;
pub mod server;

#[derive(Debug, clap::Parser)]
#[clap(version = env!("CARGO_PKG_VERSION"))]
#[clap(about = "Helium Mobile Config Service")]
pub struct Cli {
    /// Optional configuration file to use. If present, the toml file at the
    /// given path will be loaded. Environment variables can override the
    /// settings in the given file.
    #[clap(short = 'c')]
    config: Option<PathBuf>,

    #[clap(subcommand)]
    cmd: Cmd,
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.cmd {
            Cmd::Api(api) => {
                let settings = Settings::new(self.config)?;
                api.run(&settings).await
            }
            Cmd::GenerateKey => {
                let kp = Keypair::generate(KeyTag::default(), &mut OsRng);

                // Serialize to raw bytes
                let bytes = kp.to_vec();

                // Encode to base64 string for use in your config.toml
                let b64 = general_purpose::STANDARD.encode(&bytes);

                // Public key in base58
                let pubkey_b58 = kp.public_key().to_string();

                let file_name = format!("{pubkey_b58}.key");
                let mut f = File::create(file_name.clone())?;
                writeln!(f, "{}", b64)?;

                println!("Public key (b58):\n{}", pubkey_b58);
                println!("Base64 signing_keypair:\n{}", b64);
                println!("Keypair saved to file: {}", file_name);

                Ok(())
            }
            Cmd::Import(import) => {
                let settings = Settings::new(self.config)?;
                import.run(&settings).await
            }
            Cmd::Server(server) => {
                let settings = Settings::new(self.config)?;
                server.run(&settings).await
            }
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum Cmd {
    Api(Api),
    GenerateKey,
    Import(Import),
    Server(Server),
}
