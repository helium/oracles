use crate::Result;
use helium_crypto::Sign;
use std::{convert::TryFrom, fs, io, path};

#[derive(Debug)]
pub struct Keypair(helium_crypto::Keypair);

impl Keypair {
    pub fn generate(key_tag: helium_crypto::KeyTag) -> Self {
        use rand::rngs::OsRng;
        Keypair(helium_crypto::Keypair::generate(key_tag, &mut OsRng))
    }

    pub fn public_key(&self) -> &helium_crypto::PublicKey {
        self.0.public_key()
    }

    pub fn sign(&self, msg: &[u8]) -> Result<Vec<u8>> {
        Ok(self.0.sign(msg)?)
    }
}

pub fn load_from_file(path: &str) -> Result<Keypair> {
    let data = fs::read(path)?;
    Ok(helium_crypto::Keypair::try_from(&data[..])?.into())
}

pub fn save_to_file(keypair: &Keypair, path: &str) -> io::Result<()> {
    if let Some(parent) = path::PathBuf::from(path).parent() {
        fs::create_dir_all(parent)?;
    };
    fs::write(path, &keypair.0.to_vec())?;
    Ok(())
}

impl From<helium_crypto::Keypair> for Keypair {
    fn from(v: helium_crypto::Keypair) -> Self {
        Self(v)
    }
}

impl std::ops::Deref for Keypair {
    type Target = helium_crypto::Keypair;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
