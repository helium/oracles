use crate::{client, cmds::PathBufKeypair, Msg, PrettyJson, Result};

use super::{ListNetKeys, VerifyNetKey};
use serde_json::json;

pub async fn verify_key_role(args: VerifyNetKey) -> Result<Msg> {
    let mut client = client::AuthClient::new(&args.config_host, &args.config_pubkey).await?;
    let registered = client
        .verify(&args.pubkey, args.key_role, &args.keypair.to_keypair()?)
        .await?;
    let output = json!({
        "pubkey": args.pubkey,
        "role": args.key_role,
        "registered": registered
    });
    Msg::ok(output.pretty_json()?)
}

pub async fn list_keys_role(args: ListNetKeys) -> Result<Msg> {
    let mut client = client::AuthClient::new(&args.config_host, &args.config_pubkey).await?;
    let keys = client
        .list(args.key_role, &args.keypair.to_keypair()?)
        .await?;
    let output = json!({
        "role": args.key_role,
        "registered_keys": keys
    });
    Msg::ok(output.pretty_json()?)
}
