use crate::{Error, Result};
use bytes::Buf;
use helium_proto::Message;

pub trait MsgDecode {
    type Msg: Message + Default;

    fn decode<B: Buf>(buf: B) -> Result<Self>
    where
        Self: Sized,
        Self: TryFrom<Self::Msg, Error = Error>,
    {
        let req = Self::Msg::decode(buf)?;
        Self::try_from(req)
    }
}
