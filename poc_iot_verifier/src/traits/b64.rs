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
        Ok(base64::encode_config(self, config))
    }

    fn from_b64_config(b64: &str, config: base64::Config) -> Result<Self> {
        let decoded = base64::decode_config(b64, config)?;
        Ok(decoded)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn roundtrip() {
        let known_b64_enc: &str = "QslzojktHlbSuMF3FEpAW0nuIhgxhm_PUjE4QXo_x9A";
        let raw = Vec::from_b64_url(known_b64_enc).unwrap();
        let b64 = raw.to_b64_url().unwrap();
        assert_eq!(b64, known_b64_enc);
    }
}
