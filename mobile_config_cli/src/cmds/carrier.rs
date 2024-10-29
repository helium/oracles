use crate::{client, Msg, PrettyJson, Result};

use super::{ListIncentivePromotions, PathBufKeypair};

pub async fn list_incentive_promotions(args: ListIncentivePromotions) -> Result<Msg> {
    let mut client = client::CarrierClient::new(&args.config_host, &args.config_pubkey).await?;
    let list = client
        .list_incentive_promotions(&args.keypair.to_keypair()?)
        .await?;
    Msg::ok(list.pretty_json()?)
}
