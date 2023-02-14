use crate::{DenyList, Settings};
use helium_crypto::PublicKey;
use std::{path::PathBuf, str::FromStr};

/// Check if pubkey b58 is in denylist
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Required pubkey in b58 form
    #[clap(long)]
    key: String,
    /// Required path to local_filter_bin
    #[clap(long)]
    filter_path: PathBuf,
}

impl Cmd {
    pub async fn run(&self, _settings: &Settings) -> anyhow::Result<bool> {
        let pubkey = PublicKey::from_str(&self.key)?;
        let pubkey_bin = pubkey.to_vec();
        let dl = DenyList::from_filter(self.filter_path.as_path())?;
        let res = dl.check_key(&pubkey_bin).await;
        Ok(res)
    }
}
