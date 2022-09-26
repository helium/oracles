use chrono::{DateTime, Utc};
use file_store::speedtest::CellSpeedtest;
use helium_proto::services::poc_mobile::{
    Average as AverageProto, ShareValidity, SpeedShare as SpeedShareProto,
};
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
// Minimum samples required to check validity
pub const MIN_REQUIRED_SAMPLES: usize = 2;

use helium_crypto::PublicKey;

/// Map from gw_public_key to vec<speed_share>
pub type SpeedShares = HashMap<PublicKey, Vec<SpeedShare>>;

pub type InvalidSpeedShares = Vec<SpeedShare>;

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
    pub upload_speed_avg_bps: u64,
    pub download_speed_avg_bps: u64,
    pub latency_avg_ms: u32,
}

impl From<Average> for AverageProto {
    fn from(avg: Average) -> Self {
        Self {
            upload_speed_avg_bps: avg.upload_speed_avg_bps,
            download_speed_avg_bps: avg.download_speed_avg_bps,
            latency_avg_ms: avg.latency_avg_ms,
        }
    }
}

impl Average {
    pub fn new(
        upload_speed_avg_bps: u64,
        download_speed_avg_bps: u64,
        latency_avg_ms: u32,
    ) -> Self {
        Self {
            upload_speed_avg_bps,
            download_speed_avg_bps,
            latency_avg_ms,
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
        self.speed_shares.len() >= MIN_REQUIRED_SAMPLES
            && self.average.download_speed_avg_bps >= MIN_DOWNLOAD
            && self.average.upload_speed_avg_bps >= MIN_UPLOAD
            && self.average.latency_avg_ms <= MAX_LATENCY
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SpeedShare {
    pub pub_key: PublicKey,
    pub timestamp: DateTime<Utc>,
    pub upload_speed: u64,
    pub download_speed: u64,
    pub latency: u32,
    pub validity: ShareValidity,
}

impl TryFrom<CellSpeedtest> for SpeedShare {
    type Error = helium_crypto::Error;

    fn try_from(speed_test: CellSpeedtest) -> Result<Self, Self::Error> {
        Ok(Self {
            pub_key: speed_test.pubkey,
            timestamp: speed_test.timestamp,
            upload_speed: speed_test.upload_speed,
            download_speed: speed_test.download_speed,
            latency: speed_test.latency,
            validity: ShareValidity::Valid,
        })
    }
}

impl From<SpeedShare> for SpeedShareProto {
    fn from(ss: SpeedShare) -> Self {
        Self {
            pub_key: ss.pub_key.to_vec(),
            timestamp: ss.timestamp.timestamp() as u64,
            upload_speed_bps: ss.upload_speed,
            download_speed_bps: ss.download_speed,
            latency_ms: ss.latency,
            validity: ss.validity as i32,
        }
    }
}

impl SpeedShare {
    pub fn new(
        pub_key: PublicKey,
        timestamp: DateTime<Utc>,
        upload_speed: u64,
        download_speed: u64,
        latency: u32,
        validity: ShareValidity,
    ) -> Self {
        Self {
            pub_key,
            timestamp,
            upload_speed,
            download_speed,
            latency,
            validity,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{DateTime, TimeZone};
    use std::str::FromStr;

    fn parse_dt(dt: &str) -> DateTime<Utc> {
        Utc.datetime_from_str(dt, "%Y-%m-%d %H:%M:%S %z")
            .expect("unable_to_parse")
    }

    fn bytes_per_s(mbps: u64) -> u64 {
        mbps * 125000
    }

    fn known_shares() -> Vec<SpeedShare> {
        // This data is taken from the spreadsheet
        // Timestamp	DL	UL	Latency	DL RA	UL RA	Latency RA	Pass?
        // 2022-08-01 0:00:00	0	0	0	0.00	0.00	0.00	FALSE*
        // 2022-08-01 6:00:00	150	20	70	75.00	10.00	35.00	FALSE
        // 2022-08-01 12:00:00	118	10	50	89.33	10.00	40.00	FALSE
        // 2022-08-01 18:00:00	112	30	40	95.00	15.00	40.00	FALSE
        // 2022-08-02 0:00:00	90	15	10	94.00	15.00	34.00	FALSE
        // 2022-08-02 6:00:00	130	20	10	100.00	15.83	30.00	TRUE
        // 2022-08-02 12:00:00	100	10	30	116.67	17.50	35.00	TRUE
        // 2022-08-02 18:00:00	70	30	40	103.33	19.17	30.00	TRUE

        let gw_public_key =
            PublicKey::from_str("1126cBTucnhedhxnWp6puBWBk6Xdbpi7nkqeaX4s4xoDy2ja7bcd")
                .expect("unable to get pubkey");
        let mut shares = vec![];

        let s0 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-01 0:00:00 +0000"),
            0,
            0,
            0,
            ShareValidity::Valid,
        );
        let s1 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-01 6:00:00 +0000"),
            bytes_per_s(20),
            bytes_per_s(150),
            70,
            ShareValidity::Valid,
        );
        let s2 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-01 12:00:00 +0000"),
            bytes_per_s(10),
            bytes_per_s(118),
            50,
            ShareValidity::Valid,
        );
        let s3 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-01 18:00:00 +0000"),
            bytes_per_s(30),
            bytes_per_s(112),
            40,
            ShareValidity::Valid,
        );
        let s4 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-02 0:00:00 +0000"),
            bytes_per_s(15),
            bytes_per_s(90),
            10,
            ShareValidity::Valid,
        );
        let s5 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-02 6:00:00 +0000"),
            bytes_per_s(20),
            bytes_per_s(130),
            10,
            ShareValidity::Valid,
        );
        let s6 = SpeedShare::new(
            gw_public_key.clone(),
            parse_dt("2022-08-02 12:00:00 +0000"),
            bytes_per_s(10),
            bytes_per_s(100),
            30,
            ShareValidity::Valid,
        );
        let s7 = SpeedShare::new(
            gw_public_key,
            parse_dt("2022-08-02 18:00:00 +0000"),
            bytes_per_s(30),
            bytes_per_s(70),
            40,
            ShareValidity::Valid,
        );
        shares.push(s0);
        shares.push(s1);
        shares.push(s2);
        shares.push(s3);
        shares.push(s4);
        shares.push(s5);
        shares.push(s6);
        shares.push(s7);
        shares
    }

    #[test]
    fn check_known_valid() {
        let mut moving_avg = MovingAvg::default();
        let shares = known_shares();
        for (index, share) in shares.iter().enumerate() {
            moving_avg.update(share.to_owned());
            if index > 4 {
                // This should be valid according to the spreadsheet
                assert!(moving_avg.is_valid)
            } else {
                assert!(!moving_avg.is_valid)
            }
        }
    }

    #[test]
    fn check_minimum_known_valid() {
        let mut moving_avg = MovingAvg::default();
        let shares = known_shares();
        for (index, share) in shares[4..6].iter().enumerate() {
            moving_avg.update(share.to_owned());
            if index > 0 {
                // This becomes valid as soon as s5 comes in
                assert!(moving_avg.is_valid)
            } else {
                assert!(!moving_avg.is_valid)
            }
        }
    }

    #[test]
    fn check_minimum_known_invalid() {
        let mut moving_avg = MovingAvg::default();
        let shares = known_shares();
        for (_index, share) in shares[5..6].iter().enumerate() {
            moving_avg.update(share.to_owned());
            // This is invalid because MIN_REQUIRED_SAMPLES is not satifsied
            assert!(!moving_avg.is_valid)
        }
    }
}
