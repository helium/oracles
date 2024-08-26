pub trait MsgBytes {
    fn as_bytes(&self) -> bytes::Bytes;
}

// As prost::Message is implemented for basically all types, implementing
// MsgBytes for anything that implements prost::Message makes it so you
// cannot use a FileSink for anything that is _not_ a protobuf. So we
// provide utility implementations for Vec<u8> an String, and require all
// protos to be implemented directly, following the pattern of verifying and
// signing messages.
impl MsgBytes for Vec<u8> {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl MsgBytes for String {
    fn as_bytes(&self) -> bytes::Bytes {
        bytes::Bytes::from(self.clone())
    }
}

impl MsgBytes for bytes::Bytes {
    fn as_bytes(&self) -> bytes::Bytes {
        self.clone()
    }
}
