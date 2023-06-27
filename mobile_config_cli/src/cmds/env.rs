use serde::{Deserialize, Serialize};
use std::{env, fs, path::PathBuf};

use super::{EnvInfo, GenerateKeypair, ENV_CONFIG_HOST, ENV_CONFIG_PUBKEY, ENV_KEYPAIR_BIN};
use crate::{Msg, PrettyJson, Result};
use anyhow::Context;
use dialoguer::Input;
use helium_crypto::Keypair;
use rand::rngs::OsRng;
use serde_json::json;

pub async fn env_init() -> Result<Msg> {
    println!("----- Leave blank to ignore...");
    let config_host: String = Input::new()
        .with_prompt("Config Service Host")
        .allow_empty(true)
        .interact()?;
    let keypair_path: String = Input::<String>::new()
        .with_prompt("Keypair Location")
        .with_initial_text("./keypair.bin")
        .allow_empty(true)
        .interact()?;
    let config_pubkey: String = Input::new()
        .with_prompt("Config Service Signing Pubkey")
        .allow_empty(true)
        .interact()?;

    let mut report = vec![
        "".to_string(),
        "Put these in your environment".to_string(),
        "------------------------------------".to_string(),
    ];
    if !config_host.is_empty() {
        report.push(format!("{ENV_CONFIG_HOST}={config_host}"));
    }
    if !keypair_path.is_empty() {
        report.push(format!("{ENV_KEYPAIR_BIN}={keypair_path}"))
    }
    if !config_pubkey.is_empty() {
        report.push(format!("{ENV_CONFIG_PUBKEY}={config_pubkey}"))
    }

    Msg::ok(report.join("\n"))
}

pub fn env_info(args: EnvInfo) -> Result<Msg> {
    let env_keypair = env::var(ENV_KEYPAIR_BIN).ok().map(|i| i.into());
    let (env_keypair_location, env_public_key) = get_public_key_from_path(env_keypair);
    let (arg_keypair_location, arg_public_key) = get_public_key_from_path(args.keypair);

    let output = json!({
        "environment": {
            ENV_CONFIG_HOST: env::var(ENV_CONFIG_HOST).unwrap_or_else(|_| "unset".into()),
            ENV_CONFIG_PUBKEY: env::var(ENV_CONFIG_PUBKEY).unwrap_or_else(|_| "unset".into()),
            ENV_KEYPAIR_BIN: env_keypair_location,
            "public_key_from_keypair": env_public_key
        },
        "arguments": {
            "config_host": args.config_host,
            "config_pubkey": args.config_pubkey,
            "keypair": arg_keypair_location,
            "public_key_from_keypair": arg_public_key
        }
    });
    Msg::ok(output.pretty_json()?)
}

#[derive(clap::ValueEnum, Clone, Serialize, Debug, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NetworkArg {
    #[default]
    Mainnet,
    Testnet,
}

pub fn generate_keypair(args: GenerateKeypair) -> Result<Msg> {
    let network: helium_crypto::Network = match args.network {
        NetworkArg::Mainnet => helium_crypto::Network::MainNet,
        NetworkArg::Testnet => helium_crypto::Network::TestNet,
    };
    let key = helium_crypto::Keypair::generate(
        helium_crypto::KeyTag {
            network,
            key_type: helium_crypto::KeyType::Ed25519,
        },
        &mut OsRng,
    );
    if let Some(parent) = args.out_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.out_file, key.to_vec())?;
    Msg::ok(format!(
        "New Keypair created and written to {:?}",
        args.out_file.display()
    ))
}

pub fn get_public_key_from_path(path: Option<PathBuf>) -> (String, String) {
    match path {
        None => ("unset".to_string(), "unset".to_string()),
        Some(path) => {
            let display_path = path.as_path().display().to_string();
            match fs::read(path).with_context(|| format!("path does not exist: {display_path}")) {
                Err(e) => (e.to_string(), "".to_string()),
                Ok(data) => match Keypair::try_from(&data[..]) {
                    Err(e) => (display_path, e.to_string()),
                    Ok(keypair) => (display_path, keypair.public_key().to_string()),
                },
            }
        }
    }
}
