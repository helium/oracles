//use crate::PublicKey;
use helium_proto::services::poc_mobile::{Average as AverageProto, SpeedShare as SpeedShareProto};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};

// 12500000 bytes/sec = 100 Mbps
const MIN_DOWNLOAD: u64 = 12500000;
// 1250000 bytes/sec = 10 Mbps
const MIN_UPLOAD: u64 = 1250000;
// 50 ms
const MAX_LATENCY: u32 = 50;
// Window size of rolling average
pub const MOVING_WINDOW_SIZE: usize = 6;

use helium_crypto::PublicKey;

/// Map from gw_public_key to vec<speed_share>
pub type SpeedShares = HashMap<PublicKey, Vec<SpeedShare>>;

pub type InvalidSpeedShares = Vec<InvalidSpeedShare>;

#[derive(Clone, Debug, Default, Serialize)]
pub struct SpeedShareMovingAvgs(HashMap<PublicKey, MovingAvg>);

impl SpeedShareMovingAvgs {
    pub fn update(&mut self, speed_shares_by_pub_key: &SpeedShares) {
        for (public_key, speed_shares) in speed_shares_by_pub_key {
            for share in speed_shares {
                self.0
                    .entry(public_key.clone())
                    .or_default()
                    .update(share.to_owned())
            }
        }
    }

    pub fn get(&self, gw_public_key: &PublicKey) -> Option<&MovingAvg> {
        self.0.get(gw_public_key)
    }

    pub fn into_inner(self) -> HashMap<PublicKey, MovingAvg> {
        self.0
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MovingAvg {
    pub window_size: usize,
    pub speed_shares: VecDeque<SpeedShare>,
    pub average: Average,
    pub is_valid: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Average {
    pub upload_speed_avg: u64,
    pub download_speed_avg: u64,
    pub latency_avg: u32,
}

impl Into<AverageProto> for Average {
    fn into(self) -> AverageProto {
        AverageProto {
            upload_speed_avg: self.upload_speed_avg,
            download_speed_avg: self.download_speed_avg,
            latency_avg: self.latency_avg,
        }
    }
}

impl Average {
    pub fn new(upload_speed_avg: u64, download_speed_avg: u64, latency_avg: u32) -> Self {
        Self {
            upload_speed_avg,
            download_speed_avg,
            latency_avg,
        }
    }
}

impl Default for MovingAvg {
    fn default() -> Self {
        Self {
            window_size: MOVING_WINDOW_SIZE,
            speed_shares: VecDeque::new(),
            average: Average::default(),
            is_valid: false,
        }
    }
}

impl MovingAvg {
    pub fn update(&mut self, value: SpeedShare) {
        self.speed_shares.push_back(value);

        if self.speed_shares.len() > self.window_size {
            self.speed_shares.pop_front();
        }

        if !self.speed_shares.is_empty() {
            let len = self.speed_shares.len();
            let mut s1 = 0;
            let mut s2 = 0;
            let mut s3 = 0;

            for val in self.speed_shares.iter() {
                s1 += val.upload_speed;
                s2 += val.download_speed;
                s3 += val.latency;
            }

            let a1 = s1 / len as u64;
            let a2 = s2 / len as u64;
            let a3 = s3 / len as u32;
            self.average = Average::new(a1, a2, a3)
        }
        self.is_valid = self.check_validity()
    }

    fn check_validity(&self) -> bool {
        self.speed_shares.len() >= MOVING_WINDOW_SIZE
            && self.average.download_speed_avg >= MIN_DOWNLOAD
            && self.average.upload_speed_avg >= MIN_UPLOAD
            && self.average.latency_avg <= MAX_LATENCY
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SpeedShare {
    pub pub_key: PublicKey,
    pub timestamp: u64,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub latency: u32,
}

impl Into<SpeedShareProto> for SpeedShare {
    fn into(self) -> SpeedShareProto {
        SpeedShareProto {
            pub_key: self.pub_key.to_vec(),
            timestamp: self.timestamp,
            upload_speed: self.upload_speed,
            download_speed: self.download_speed,
            latency: self.latency,
        }
    }
}

impl SpeedShare {
    pub fn new(
        pub_key: PublicKey,
        timestamp: u64,
        upload_speed: u64,
        download_speed: u64,
        latency: u32,
    ) -> Self {
        Self {
            pub_key,
            timestamp,
            upload_speed,
            download_speed,
            latency,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct InvalidSpeedShare {
    pub pub_key: Vec<u8>,
    pub timestamp: u64,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub latency: u32,
    pub invalid_reason: &'static str,
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn check_valid() {
        let gw_public_key =
            PublicKey::from_str("1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd")
                .expect("unable to get pubkey");
        let mut shares = vec![];
        for timestamp in 1..=13 {
            let share = SpeedShare::new(
                gw_public_key.clone(),
                timestamp as u64,
                MIN_UPLOAD + 1,
                MIN_DOWNLOAD + 1,
                MAX_LATENCY - 1,
            );
            shares.push(share)
        }
        let mut moving_avg = MovingAvg::default();
        for (index, share) in shares.iter().enumerate() {
            moving_avg.update(share.to_owned());
            if (index + 1) < MOVING_WINDOW_SIZE {
                assert!(!moving_avg.is_valid)
            } else {
                assert!(moving_avg.is_valid)
            }
        }
    }

    #[test]
    fn check_invalid() {
        let gw_public_key =
            PublicKey::from_str("1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd")
                .expect("unable to get pubkey");
        let mut shares = vec![];
        for timestamp in 1..=13 {
            let share = SpeedShare::new(
                gw_public_key.clone(),
                timestamp as u64,
                MIN_UPLOAD - 1,
                MIN_DOWNLOAD - 1,
                MAX_LATENCY + 1,
            );
            shares.push(share)
        }
        let mut moving_avg = MovingAvg::default();
        for share in shares {
            moving_avg.update(share);
            assert!(!moving_avg.is_valid)
        }
    }

    // NOTE: We should get some ruby code to check a speedtest file and compare what it says about
    // valid speedtests for hotspots too. Enable this or a similar test then...
    // #[test]
    // fn check_sample() {
    //     let gw_public_key =
    //         PublicKey::from_str("1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd")
    //             .expect("unable to get pubkey");
    //     let shares = vec![
    //         SpeedShare::new(gw_public_key.clone(), 1661578086, 2182223, 11739568, 118),
    //         SpeedShare::new(gw_public_key.clone(), 1661581686, 2589229, 12618734, 30),
    //         SpeedShare::new(gw_public_key.clone(), 1661585286, 11420942, 11376519, 8),
    //         SpeedShare::new(gw_public_key.clone(), 1661588886, 5646683, 10551784, 6),
    //         SpeedShare::new(gw_public_key.clone(), 1661592486, 1655764, 19594159, 47),
    //         SpeedShare::new(gw_public_key.clone(), 1661596086, 1461670, 33046967, 24),
    //         SpeedShare::new(gw_public_key.clone(), 1661599686, 623987, 104615, 317),
    //         SpeedShare::new(gw_public_key.clone(), 1661603286, 1451442, 41535836, 31),
    //         SpeedShare::new(gw_public_key.clone(), 1661606886, 1448379, 27020854, 66),
    //         SpeedShare::new(gw_public_key.clone(), 1661610486, 2515468, 11118020, 39),
    //         SpeedShare::new(gw_public_key.clone(), 1661614086, 734261, 26084694, 34),
    //         SpeedShare::new(gw_public_key.clone(), 1661617686, 102592, 2051052, 74),
    //         SpeedShare::new(gw_public_key.clone(), 1661621286, 1214982, 1030764, 165),
    //         SpeedShare::new(gw_public_key.clone(), 1661624886, 1218291, 28013571, 124),
    //         SpeedShare::new(gw_public_key, 1661628486, 580032, 3799167, 27),
    //     ];

    //     let mut moving_avg = MovingAvg::default();
    //     for share in shares {
    //         moving_avg.update(share);
    //         println!("moving_avg is_valid: {:#?}", moving_avg.is_valid);
    //     }

    // }
}
