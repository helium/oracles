use std::fmt::Formatter;
use std::str::FromStr;

pub enum ServiceProviderRewardType {
    Network,
    Subscriber,
}

impl std::fmt::Display for ServiceProviderRewardType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceProviderRewardType::Subscriber => f.write_str("Helium Mobile Service Rewards"),
            ServiceProviderRewardType::Network => f.write_str("Helium Mobile"),
        }
    }
}

impl FromStr for ServiceProviderRewardType {
    type Err = prost::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Helium Mobile Service Rewards" => Ok(ServiceProviderRewardType::Subscriber),
            "Helium Mobile" => Ok(ServiceProviderRewardType::Network),
            unknown => Err(prost::DecodeError::new(format!(
                "unknown service provider reward type: {unknown}"
            ))),
        }
    }
}
