#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Model {
    Nova436H,
    Nova430I,
    Neutrino430,
    SercommIndoor,
    SercommOutdoor,
}

pub struct Hotspot<'a> {
    pub model: Model,
    pub fcc_id: &'a str,
    // reward_wt is x10, so 15 = 1.5 (actual)
    pub reward_wt: u64,
}

impl<'a> Hotspot<'a> {
    pub const NOVA436H: Hotspot<'a> = Hotspot {
        model: Model::Nova436H,
        fcc_id: "2AG32MBS3100196N",
        reward_wt: 20,
    };
    pub const NOVA430I: Hotspot<'a> = Hotspot {
        model: Model::Nova430I,
        fcc_id: "2AG32PBS3101S",
        reward_wt: 15,
    };
    pub const NEUTRINO430: Hotspot<'a> = Hotspot {
        model: Model::Neutrino430,
        fcc_id: "2AG32PBS31010",
        reward_wt: 10,
    };
    pub const SERCOMMINDOOR: Hotspot<'a> = Hotspot {
        model: Model::SercommIndoor,
        fcc_id: "P27-SCE4255W",
        reward_wt: 10,
    };
    pub const SERCOMMOUTDOOR: Hotspot<'a> = Hotspot {
        model: Model::SercommOutdoor,
        fcc_id: "P27-SCO4255PA10",
        reward_wt: 15,
    };

    pub fn reward_shares(&self, units: u64) -> u64 {
        self.reward_wt * units
    }

    pub fn rewards(&self, base_rewards: u64) -> u64 {
        self.reward_wt * base_rewards
    }
}
