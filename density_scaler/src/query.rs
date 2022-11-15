use crate::{Error, Result};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub struct QueryMsg {
    pub hex: String,
    pub response: ResultSender,
}

#[derive(Clone, Debug)]
pub struct QuerySender(mpsc::Sender<QueryMsg>);
pub struct QueryReceiver(mpsc::Receiver<QueryMsg>);

pub fn query_channel(size: usize) -> (QuerySender, QueryReceiver) {
    let (tx, rx) = mpsc::channel(size);
    (QuerySender(tx), QueryReceiver(rx))
}

impl QuerySender {
    pub async fn query(&self, hex: String) -> Result<Option<Decimal>> {
        let (tx, rx) = result_channel();
        let _ = self.0.send(QueryMsg { hex, response: tx }).await;
        rx.recv().await
    }
}

impl QueryReceiver {
    pub async fn recv(&mut self) -> Option<QueryMsg> {
        self.0.recv().await
    }
}

#[derive(Debug)]
pub struct ResultSender(oneshot::Sender<Option<Decimal>>);
pub struct ResultReceiver(oneshot::Receiver<Option<Decimal>>);

pub fn result_channel() -> (ResultSender, ResultReceiver) {
    let (tx, rx) = oneshot::channel();
    (ResultSender(tx), ResultReceiver(rx))
}

impl ResultSender {
    pub fn send(self, msg: Option<Decimal>) {
        match self.0.send(msg) {
            Ok(()) => (),
            Err(err) => tracing::warn!("failed to return result for reason : {err:?}"),
        }
    }
}

impl ResultReceiver {
    pub async fn recv(self) -> Result<Option<Decimal>> {
        self.0.await.map_err(|_| Error::channel())
    }
}
