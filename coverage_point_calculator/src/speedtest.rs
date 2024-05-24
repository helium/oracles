use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::MaxOneMultplier;

#[derive(Debug, Default, Clone, Copy, PartialEq, PartialOrd)]
pub struct BytesPs(u64);

impl BytesPs {
    pub fn new(bytes_per_second: u64) -> Self {
        Self(bytes_per_second)
    }

    pub fn mbps(megabytes_per_second: u64) -> Self {
        Self(megabytes_per_second * 12500)
    }

    fn to_mbps(&self) -> u64 {
        self.0 / 12500
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Millis(u32);

impl Millis {
    pub fn new(milliseconds: u32) -> Self {
        Self(milliseconds)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Speedtest {
    pub upload_speed: BytesPs,
    pub download_speed: BytesPs,
    pub latency: Millis,
}

impl Speedtest {
    pub fn multiplier(&self) -> Decimal {
        let upload = SpeedtestTier::from_upload(&self.upload_speed);
        let download = SpeedtestTier::from_download(&self.download_speed);
        let latency = SpeedtestTier::from_latency(&self.latency);

        let tier = upload.min(download).min(latency);
        tier.multiplier()
    }

    pub fn avg(speedtests: &[Self]) -> Self {
        let mut download = 0;
        let mut upload = 0;
        let mut latency = 0;

        for test in speedtests {
            upload += test.upload_speed.0;
            download += test.download_speed.0;
            latency += test.latency.0;
        }

        let count = speedtests.len();
        Self {
            upload_speed: BytesPs::new(upload / count as u64),
            download_speed: BytesPs::new(download / count as u64),
            latency: Millis::new(latency / count as u32),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpeedtestTier {
    Good = 4,
    Acceptable = 3,
    Degraded = 2,
    Poor = 1,
    Fail = 0,
}

impl SpeedtestTier {
    pub fn multiplier(&self) -> MaxOneMultplier {
        match self {
            SpeedtestTier::Good => dec!(1.00),
            SpeedtestTier::Acceptable => dec!(0.75),
            SpeedtestTier::Degraded => dec!(0.50),
            SpeedtestTier::Poor => dec!(0.25),
            SpeedtestTier::Fail => dec!(0),
        }
    }

    // FIXME: The modeled coverage blog post declares all comparisons as non-inclusive.
    // And in the blog post, the UI shows what appear to be inclusive ranges.
    //
    // Speed Test Tier
    // Acceptable	100+ Download, AND 10+ Upload, AND <50 Latency
    // In the UI
    // Acceptable: 0-50ms
    //
    // I find this confusing

    fn from_download(bytes: &BytesPs) -> Self {
        match bytes.to_mbps() {
            100.. => Self::Good,
            75.. => Self::Acceptable,
            50.. => Self::Degraded,
            30.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_upload(bytes: &BytesPs) -> Self {
        match bytes.to_mbps() {
            10.. => Self::Good,
            8.. => Self::Acceptable,
            5.. => Self::Degraded,
            2.. => Self::Poor,
            _ => Self::Fail,
        }
    }

    fn from_latency(Millis(millis): &Millis) -> Self {
        // FIXME: comparison in mobile-verifier is non-inclusive
        match millis {
            ..=50 => Self::Good,
            ..=60 => Self::Acceptable,
            ..=75 => Self::Degraded,
            ..=100 => Self::Poor,
            _ => Self::Fail,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn speedtest_teirs() {
        use SpeedtestTier::*;
        // download
        assert_eq!(Good, SpeedtestTier::from_download(&BytesPs::mbps(100)));
        assert_eq!(Acceptable, SpeedtestTier::from_download(&BytesPs::mbps(80)));
        assert_eq!(Degraded, SpeedtestTier::from_download(&BytesPs::mbps(62)));
        assert_eq!(Poor, SpeedtestTier::from_download(&BytesPs::mbps(42)));
        assert_eq!(Fail, SpeedtestTier::from_download(&BytesPs::mbps(20)));

        // upload
        assert_eq!(Good, SpeedtestTier::from_upload(&BytesPs::mbps(10)));
        assert_eq!(Acceptable, SpeedtestTier::from_upload(&BytesPs::mbps(8)));
        assert_eq!(Degraded, SpeedtestTier::from_upload(&BytesPs::mbps(6)));
        assert_eq!(Poor, SpeedtestTier::from_upload(&BytesPs::mbps(4)));
        assert_eq!(Fail, SpeedtestTier::from_upload(&BytesPs::mbps(1)));

        // latency
        assert_eq!(Good, SpeedtestTier::from_latency(&Millis::new(50)));
        assert_eq!(Acceptable, SpeedtestTier::from_latency(&Millis::new(60)));
        assert_eq!(Degraded, SpeedtestTier::from_latency(&Millis::new(75)));
        assert_eq!(Poor, SpeedtestTier::from_latency(&Millis::new(100)));
        assert_eq!(Fail, SpeedtestTier::from_latency(&Millis::new(101)));
    }
}
