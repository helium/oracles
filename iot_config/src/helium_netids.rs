use crate::lora_field::{LoraField, NetIdField};

const TYPE_0_ID: NetIdField = LoraField(0x00003c);
const TYPE_3_ID: NetIdField = LoraField(0x60002d);
const TYPE_6_ID: NetIdField = LoraField(0xc00053);

#[derive(Clone, Copy)]
pub enum HeliumNetId {
    Type0_0x00003c,
    Type3_0x60002d,
    Type6_0xc00053,
}

impl TryFrom<NetIdField> for HeliumNetId {
    type Error = &'static str;

    fn try_from(field: NetIdField) -> Result<Self, Self::Error> {
        let id = match field {
            TYPE_0_ID => HeliumNetId::Type0_0x00003c,
            TYPE_3_ID => HeliumNetId::Type3_0x60002d,
            TYPE_6_ID => HeliumNetId::Type6_0xc00053,
            _ => return Err("not a helium id"),
        };
        Ok(id)
    }
}
