use bytes::Buf;
use prost::Message;

#[derive(thiserror::Error, Debug)]
pub enum MsgDecodeError<E> {
    #[error("prost decode error: {0}")]
    Prost(#[from] prost::DecodeError),

    #[error("conversion error: {0}")]
    Conversion(E),
}
pub trait MsgDecode: Sized + TryFrom<Self::Msg> {
    type Msg: Message + Default;

    fn decode<B: Buf>(buf: B) -> Result<Self, MsgDecodeError<Self::Error>> {
        let req = Self::Msg::decode(buf)?;
        Self::try_from(req).map_err(MsgDecodeError::Conversion)
    }
}
