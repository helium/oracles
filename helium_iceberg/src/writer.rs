use crate::Result;
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;

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
    pub async fn write_with_properties(
        &mut self,
        records: Vec<T>,
        custom_properties: HashMap<String, String>,
    ) -> Result<()> {
        let prev = std::mem::replace(self, Self::Complete);
        match prev {
            Self::Writer(branch_writer) => {
                let publisher = branch_writer
                    .write_with_properties(records, custom_properties)
                    .await?;
                *self = Self::Publisher(publisher);
            }
            other => {
                tracing::info!("called write on not writer state");
                *self = other
            }
        }

        Ok(())
    }

    pub async fn write(&mut self, records: Vec<T>) -> Result<()> {
        self.write_with_properties(records, HashMap::new()).await
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
    async fn write_with_properties(
        self: Box<Self>,
        records: Vec<T>,
        custom_properties: HashMap<String, String>,
    ) -> Result<Box<dyn BranchPublisher>>;

    async fn write(self: Box<Self>, records: Vec<T>) -> Result<Box<dyn BranchPublisher>> {
        self.write_with_properties(records, HashMap::new()).await
    }
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
