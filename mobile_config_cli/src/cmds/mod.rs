use crate::{cmds::env::NetworkArg, NetworkKeyRole, Result};
use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use helium_crypto::PublicKey;
use mobile_config::KeyRole;
use std::path::PathBuf;

pub mod admin;
pub mod authorization;
pub mod carrier;
pub mod entity;
pub mod env;
pub mod gateway;

pub const ENV_CONFIG_HOST: &str = "HELIUM_CONFIG_HOST";
pub const ENV_CONFIG_PUBKEY: &str = "HELIUM_CONFIG_PUBKEY";
pub const ENV_KEYPAIR_BIN: &str = "HELIUM_KEYPAIR_BIN";
pub const ENV_LOG_FILTER: &str = "HELIUM_LOG_FILTER";

#[derive(Debug, Parser)]
#[command(name = "mobile-config")]
#[command(author, version, about, long_about=None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(
        global = true,
        long,
        env = ENV_CONFIG_HOST,
        default_value = "https://config.mobile.mainnet.helium.io:6080"
    )]
    pub config_host: String,

    #[arg(
        global = true,
        long,
        env = ENV_CONFIG_PUBKEY,
        default_value = "138DPEU3WyKQryVUuy3b7bLxRM7KAdQYGvDvS9cGZQHnRSWMkew"
    )]
    pub config_pubkey: String,

    #[arg(
        global = true,
        long,
        env = ENV_KEYPAIR_BIN,
        default_value = "./keypair.bin"
    )]
    pub keypair: PathBuf,

    #[arg(global = true, long)]
    pub print_command: bool,

    #[arg(
        global = true,
        long,
        env = ENV_LOG_FILTER,
        default_value = "Error"
    )]
    pub log_filter: String,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Environment
    Env {
        #[command(subcommand)]
        command: EnvCommands,
    },
    /// Admin
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
    },
    /// Authorization
    Authorization {
        #[command(subcommand)]
        command: AuthCommands,
    },
    Carrier {
        #[command(subcommand)]
        command: CarrierCommands,
    },
    /// Entity
    Entity {
        #[command(subcommand)]
        command: EntityCommands,
    },
    /// Gateway
    Gateway {
        #[command(subcommand)]
        command: GatewayCommands,
    },
}

#[derive(Debug, Subcommand)]
pub enum AuthCommands {
    /// Verify the pubkey/role is registered
    VerifyKey(VerifyNetKey),
    /// List registered keys by role
    ListKeys(ListNetKeys),
}

#[derive(Debug, Args)]
pub struct VerifyNetKey {
    #[arg(long, value_enum)]
    pub key_role: NetworkKeyRole,
    #[arg(long)]
    pub pubkey: PublicKey,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Args)]
pub struct ListNetKeys {
    #[arg(long, value_enum)]
    pub key_role: NetworkKeyRole,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Subcommand)]
pub enum CarrierCommands {
    ListIncentivePromotions(ListIncentivePromotions),
}

#[derive(Debug, Args)]
pub struct ListIncentivePromotions {
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Subcommand)]
pub enum EntityCommands {
    /// Verify the rewardable entity on-chain
    VerifyEntity(VerifyRewardableEntity),
}

#[derive(Debug, Args)]
pub struct VerifyRewardableEntity {
    #[arg(short, long)]
    pub entity_id: String,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Subcommand)]
pub enum GatewayCommands {
    /// Retrieve the on-chain registered info for the hotspot
    Info(GetHotspot),
    /// Retrieve the on-chain registered info for the batch of hotspots
    /// requested by list of Public Key Binaries
    InfoBatch(GetHotspotBatch),
}

#[derive(Debug, Args)]
pub struct GetHotspot {
    #[arg(long)]
    pub hotspot: PublicKey,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Args)]
pub struct GetHotspotBatch {
    #[arg(long)]
    pub hotspot: Vec<PublicKey>,
    #[arg(short, long, default_value = "5")]
    pub batch_size: u32,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
}

#[derive(Debug, Subcommand)]
pub enum EnvCommands {
    /// Make Environment variable to ease use
    Init,
    /// View information about your environment
    Info(EnvInfo),
    /// Make a new keypair
    GenerateKeypair(GenerateKeypair),
}

#[derive(Debug, Args)]
pub struct EnvInfo {
    #[arg(long, env = ENV_CONFIG_HOST, default_value="unset")]
    pub config_host: Option<String>,
    #[arg(long, env = ENV_KEYPAIR_BIN, default_value="unset")]
    pub keypair: Option<PathBuf>,
    #[arg(long, env = ENV_CONFIG_PUBKEY, default_value="unset")]
    pub config_pubkey: Option<String>,
}

#[derive(Debug, Args)]
pub struct GenerateKeypair {
    #[arg(default_value = "./keypair.bin")]
    pub out_file: PathBuf,
    /// The Helium network for which to issue keys
    #[arg(long, short, value_enum, default_value = "mainnet")]
    pub network: NetworkArg,
    /// overwrite <out_file> if it already exists
    #[arg(long)]
    pub commit: bool,
}

#[derive(Debug, Subcommand)]
pub enum AdminCommands {
    /// Add a pubkey/role
    AddKey(AdminKeyArgs),
    /// Remove a pubkey/role
    RemoveKey(AdminKeyArgs),
}

#[derive(Debug, Args)]
pub struct AdminKeyArgs {
    #[arg(long, value_enum)]
    pub key_role: KeyRole,
    #[arg(long)]
    pub pubkey: PublicKey,
    #[arg(from_global)]
    pub keypair: PathBuf,
    #[arg(from_global)]
    pub config_host: String,
    #[arg(from_global)]
    pub config_pubkey: String,
    #[arg(long)]
    pub commit: bool,
}

pub trait PathBufKeypair {
    fn to_keypair(&self) -> Result<helium_crypto::Keypair>;
}

impl PathBufKeypair for PathBuf {
    fn to_keypair(&self) -> Result<helium_crypto::Keypair> {
        let data = std::fs::read(self).context("reading keypair file")?;
        Ok(helium_crypto::Keypair::try_from(&data[..])?)
    }
}
