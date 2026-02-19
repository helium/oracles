use crate::Result;
use async_trait::async_trait;
use serde::Serialize;

pub type BoxedDataWriter<T> = std::sync::Arc<dyn DataWriter<T>>;

#[async_trait]
pub trait DataWriter<T>: Send + Sync
where
    T: Serialize + Send + 'static,
{
    async fn write(&self, records: Vec<T>) -> Result;
    async fn begin(&self, wap_id: &str) -> Result<BranchTransaction<T>>;
}

pub enum BranchTransaction<T: Serialize + Send + 'static> {
    Writer(Box<dyn BranchWriter<T>>),
    Publisher(Box<dyn BranchPublisher>),
    Complete,
}

impl<T> BranchTransaction<T>
where
    T: Serialize + Send + 'static,
{
    pub async fn with_writer<F, Fut>(self, f: F) -> Result<Self>
    where
        F: FnOnce(Box<dyn BranchWriter<T>>) -> Fut,
        Fut: std::future::Future<Output = Result<Box<dyn BranchPublisher>>>,
    {
        match self {
            Self::Writer(writer) => f(writer).await.map(Self::Publisher),
            other => Ok(other),
        }
    }

    pub async fn publish(self) -> Result<()> {
        match self {
            Self::Writer(_) => Err(crate::Error::Writer(
                "publish called before writing".to_string(),
            )),
            Self::Publisher(publisher) => publisher.publish().await,
            Self::Complete => Ok(()),
        }
    }
}

#[async_trait]
pub trait BranchWriter<T: Serialize + Send + 'static>: Send {
    async fn write(self: Box<Self>, records: Vec<T>) -> Result<Box<dyn BranchPublisher>>;
}

#[async_trait]
pub trait BranchPublisher: Send {
    async fn publish(self: Box<Self>) -> Result;
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
