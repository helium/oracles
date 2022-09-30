// TODO: This should really be in something like poc-common
// which other workspaces can import and use
use crate::Result;
use std::{convert::TryFrom, fs, io, path};

pub use helium_crypto::Keypair;

pub fn load_from_file(path: &str) -> Result<Keypair> {
    let data = fs::read(path)?;
    Ok(helium_crypto::Keypair::try_from(&data[..])?)
}

pub fn save_to_file(keypair: &Keypair, path: &str) -> io::Result<()> {
    if let Some(parent) = path::PathBuf::from(path).parent() {
        fs::create_dir_all(parent)?;
    };
    fs::write(path, &keypair.to_vec())?;
    Ok(())
}
