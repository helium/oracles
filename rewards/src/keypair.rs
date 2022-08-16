use crate::{Error, Result};
use helium_crypto::{KeyTag, KeyType, Keypair as CryptoKeypair, Sign};
use std::{convert::TryFrom, fs, io, path};

#[derive(Debug)]
pub struct Keypair(pub CryptoKeypair);

impl Keypair {
    pub fn generate(key_tag: KeyTag) -> Self {
        use rand::rngs::OsRng;
        Keypair(CryptoKeypair::generate(key_tag, &mut OsRng))
    }

    pub fn public_key(&self) -> &helium_crypto::PublicKey {
        self.0.public_key()
    }

    pub fn sign(&self, msg: &[u8]) -> Result<Vec<u8>> {
        Ok(self.0.sign(msg)?)
    }

    pub fn generate_from_entropy(key_tag: KeyTag, entropy: &[u8]) -> Result<Keypair> {
        Ok(Keypair(CryptoKeypair::generate_from_entropy(
            key_tag, entropy,
        )?))
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

impl From<CryptoKeypair> for Keypair {
    fn from(v: CryptoKeypair) -> Self {
        Self(v)
    }
}

impl std::ops::Deref for Keypair {
    type Target = CryptoKeypair;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&[u8]> for Keypair {
    type Error = Error;

    fn try_from(input: &[u8]) -> Result<Self> {
        match KeyType::try_from(input[0])? {
            KeyType::Ed25519 => Ok(CryptoKeypair::try_from(input)?.into()),
            KeyType::EccCompact => Ok(CryptoKeypair::try_from(input)?.into()),
        }
    }
}
