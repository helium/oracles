use crate::{client, cmds::PathBufKeypair, Msg, Result};
use anyhow::Context;
use std::{
    fs::{self, File},
    io::Read,
};

use super::AdminKeyArgs;

pub async fn add_key(args: AdminKeyArgs) -> Result<Msg> {
    let output = format!("Added {} as {} key", args.pubkey, args.key_role);

    if args.commit {
        let mut client = client::AdminClient::new(&args.config_host, &args.config_pubkey).await?;
        client.add_key(&args.pubkey, args.key_role, &args.keypair.to_keypair()?).await?;
        return Msg::ok(output);
    }
    Msg::dry_run(output)
}

pub async fn remove_key(args: AdminKeyArgs) -> Result<Msg> {
    let output = format!("Removed {} as {} key", args.pubkey, args.key_role);

    if args.commit {
        let mut client = client::AdminClient::new(&args.config_host, &args.config_pubkey).await?;
        client.remove_key(&args.pubkey, &args.keypair.to_keypair()?).await?;
        return Msg::ok(output);
    }
    Msg::dry_run(output)
}
