use crate::Result;
use async_trait::async_trait;
use serde::Serialize;

pub type BoxedDataWriter<T> = std::sync::Arc<dyn DataWriter<T>>;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send + 'static,
{
    /// Non-idempotent append. Each call produces a new snapshot.
    async fn write(&self, records: Vec<T>) -> Result;

    /// Idempotent append keyed by `id`. If any snapshot on main already
    /// carries `helium.write_id == id` in its summary, this is a no-op.
    /// Otherwise performs a `fast_append` with that property stamped onto
    /// the new snapshot.
    async fn write_idempotent(&self, id: &str, records: Vec<T>) -> Result;
}

pub trait IntoBoxedDataWriter<T> {
    fn boxed(self) -> BoxedDataWriter<T>;
}

impl<Msg, T> IntoBoxedDataWriter<Msg> for T
where
    T: DataWriter<Msg> + 'static,
    Msg: Serialize + Send + 'static,
{
    fn boxed(self) -> BoxedDataWriter<Msg> {
        std::sync::Arc::new(self)
    }
}
