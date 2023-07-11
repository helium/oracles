use crate::{client, cmds::PathBufKeypair, Msg, PrettyJson, Result};

use super::VerifyRewardableEntity;
use serde_json::json;

pub async fn verify_entity(args: VerifyRewardableEntity) -> Result<Msg> {
    let mut client = client::EntityClient::new(&args.config_host, &args.config_pubkey).await?;
    let verified = client
        .verify(&args.entity_id, &args.keypair.to_keypair()?)
        .await?;
    let output = json!({
        "entity_id": args.entity_id,
        "on_chain": verified
    });
    Msg::ok(output.pretty_json()?)
}
