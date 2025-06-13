use crate::{traits::MsgDecode, Error, Result};

/// A trait for parsing and handling items from a file info poller stream.
///
/// This trait defines the interface for decoding items from a byte stream
/// and logging potential errors that may occur during streaming or
/// decoding. Implementors of this trait are expected to provide a decoding
/// strategy for items of type `T`.
///
/// # Example:
/// ```text
/// struct Bs58Parser;
///
/// impl FileInfoParser<String> for Bs58Parser {
///    fn decode_item(&self, item: bytes::BytesMut) -> Result<String> {
///        bs58::decode(item).into_string().map_err(Error::from)
///   }
/// }
/// ```
///
/// # Stateful Example:
/// ```text
/// struct Bs58MetricsParser {
///     streaming_err: metrics::Counter,
///     decoding_err: metrics::Counter,
/// }
///
/// impl Bs58MetricsParser {
///     fn new() -> Self {
///         Self {
///             streaming_err: metrics::counter!("streaming", "status" => "err"),
///             decoding_err: metrics::counter!("decoding", "status" => "err"),
///         }
///     }
/// }
///
/// impl FileInfoParser<String> for Bs58MetricsParser {
///     fn decode_item(&self, item: bytes::BytesMut) -> Result<String> {
///         bs58::decode(item).into_string().map_err(Error::from)
///     }
///     fn streaming_err(&self, err: &Error) {
///         self.streaming_err.increment(1);
///     }
///     fn decoding_err(&self, _err: &Error) {
///         self.decoding_err.increment(1);
///     }
/// }
/// ```
pub trait FileInfoParser<T>: Send + Sync + 'static {
    fn decode_item(&self, item: bytes::BytesMut) -> Result<T>;

    fn handle_item(&self, res: Result<bytes::BytesMut>) -> Result<T> {
        // Unwrap to prevent from double logging streaming error
        let bytes = res.inspect_err(|err| self.streaming_err(err))?;
        self.decode_item(bytes)
            .inspect_err(|err| self.decoding_err(err))
    }

    fn streaming_err(&self, err: &Error) {
        tracing::error!(
            ?err,
            file_type = std::any::type_name::<T>(),
            "streaming entry",
        );
    }

    fn decoding_err(&self, err: &Error) {
        tracing::error!(
            ?err,
            file_type = std::any::type_name::<T>(),
            "decoding message",
        );
    }

    fn boxed(self) -> Box<dyn FileInfoParser<T>>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

/// A parser for decoding file info items from a byte stream into a Rust
/// struct using the [`MsgDecode`] trait.
pub struct MsgDecodeParser;

impl<T> FileInfoParser<T> for MsgDecodeParser
where
    T: MsgDecode + TryFrom<T::Msg, Error = Error> + Send + Sync + 'static,
{
    fn decode_item(&self, item: bytes::BytesMut) -> std::result::Result<T, Error> {
        T::decode(item)
    }
}

/// A parser for decoding file info items from a byte stream into a proto
/// Message using the [`helium_proto::Message`] trait.
pub struct ProstParser;

impl<T> FileInfoParser<T> for ProstParser
where
    T: helium_proto::Message + Default,
{
    fn decode_item(&self, item: bytes::BytesMut) -> Result<T> {
        T::decode(item).map_err(Error::from)
    }
}
