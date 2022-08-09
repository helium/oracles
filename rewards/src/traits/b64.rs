use crate::Result;

pub trait B64 {
    fn to_b64(&self) -> Result<String> {
        self.to_b64_config(base64::STANDARD)
    }

    fn to_b64_url(&self) -> Result<String> {
        self.to_b64_config(base64::URL_SAFE_NO_PAD)
    }
    fn from_b64(str: &str) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        Self::from_b64_config(str, base64::STANDARD)
    }

    fn from_b64_url(str: &str) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        Self::from_b64_config(str, base64::URL_SAFE_NO_PAD)
    }

    fn to_b64_config(&self, config: base64::Config) -> Result<String>;
    fn from_b64_config(str: &str, config: base64::Config) -> Result<Self>
    where
        Self: std::marker::Sized;
}

impl B64 for Vec<u8> {
    fn to_b64_config(&self, config: base64::Config) -> Result<String> {
        Ok(base64::encode_config(&self, config))
    }

    fn from_b64_config(b64: &str, config: base64::Config) -> Result<Self> {
        let decoded = base64::decode_config(b64, config)?;
        Ok(decoded)
    }
}
