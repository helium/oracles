use crate::{
    session_key::{self, SessionKeyFilter},
    GrpcResult, GrpcStreamRequest, GrpcStreamResult, Settings,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::stream::StreamExt;
use helium_crypto::PublicKey;
use helium_proto::services::iot_config::{
    self, ActionV1, SessionKeyFilterGetReqV1, SessionKeyFilterListReqV1,
    SessionKeyFilterStreamReqV1, SessionKeyFilterStreamResV1, SessionKeyFilterUpdateReqV1,
    SessionKeyFilterUpdateResV1, SessionKeyFilterV1,
};
use sqlx::{Pool, Postgres};
use tokio::{
    pin,
    sync::broadcast::{Receiver, Sender},
};
use tonic::{Request, Response, Status};

pub struct SessionKeyFilterService {
    admin_pubkey: PublicKey,
    pool: Pool<Postgres>,
    update_channel: Sender<SessionKeyFilterStreamResV1>,
}

impl SessionKeyFilterService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let (update_tx, _) = tokio::sync::broadcast::channel(128);
        Ok(Self {
            admin_pubkey: settings.admin_pubkey()?,
            pool: settings.database.connect(10).await?,
            update_channel: update_tx,
        })
    }

    fn subscribe_to_session_keys(&self) -> Receiver<SessionKeyFilterStreamResV1> {
        self.update_channel.subscribe()
    }

    fn clone_update_channel(&self) -> Sender<SessionKeyFilterStreamResV1> {
        self.update_channel.clone()
    }

    fn verify_admin_signature<R>(&self, request: R) -> Result<R, Status>
    where
        R: MsgVerify,
    {
        request
            .verify(&self.admin_pubkey)
            .map_err(|_| Status::permission_denied("invalid admin signature"))?;
        Ok(request)
    }
}

#[tonic::async_trait]
impl iot_config::SessionKeyFilter for SessionKeyFilterService {
    type listStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn list(
        &self,
        request: Request<SessionKeyFilterListReqV1>,
    ) -> GrpcResult<Self::listStream> {
        let request = request.into_inner();

        let filters = session_key::list_for_oui(request.oui, &self.pool)
            .await
            .map_err(|_| Status::internal("list session key filters failed"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        tokio::spawn(async move {
            for filter in filters {
                if tx.send(Ok(filter.into())).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type getStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn get(&self, request: Request<SessionKeyFilterGetReqV1>) -> GrpcResult<Self::getStream> {
        let request = request.into_inner();

        let session_key_filters =
            session_key::list_for_oui_and_devaddr(request.oui, request.devaddr.into(), &self.pool)
                .await
                .map_err(|_| Status::internal("get session key filters failed"))?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        tokio::spawn(async move {
            for session_key_filter in session_key_filters {
                if tx.send(Ok(session_key_filter.into())).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    async fn update(
        &self,
        request: GrpcStreamRequest<SessionKeyFilterUpdateReqV1>,
    ) -> GrpcResult<SessionKeyFilterUpdateResV1> {
        let mut request = request.into_inner();

        let mut to_add: Vec<SessionKeyFilter> = vec![];
        let mut to_remove: Vec<SessionKeyFilter> = vec![];

        while let Ok(Some(update)) = request.message().await {
            match (update.action(), update.filter) {
                (ActionV1::Add, Some(filter)) => to_add.push(filter.into()),
                (ActionV1::Remove, Some(filter)) => to_remove.push(filter.into()),
                _ => return Err(Status::invalid_argument("no filter provided")),
            }
        }
        tracing::debug!(
            adding = to_add.len(),
            removing = to_remove.len(),
            "updating session key filters",
        );

        session_key::update_session_keys(
            &to_add,
            &to_remove,
            &self.pool,
            self.clone_update_channel(),
        )
        .await
        .map_err(|err| {
            tracing::error!("session key update failed: {err:?}");
            Status::internal("session key update failed")
        })?;

        Ok(Response::new(SessionKeyFilterUpdateResV1 {}))
    }

    type streamStream = GrpcStreamResult<SessionKeyFilterStreamResV1>;
    async fn stream(
        &self,
        request: Request<SessionKeyFilterStreamReqV1>,
    ) -> GrpcResult<Self::streamStream> {
        let request = request.into_inner();
        self.verify_admin_signature(request)?;

        tracing::info!("client subscribed to session key stream");
        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        let mut session_key_updates = self.subscribe_to_session_keys();

        tokio::spawn(async move {
            let session_key_filters = session_key::list_stream(&pool).await;
            pin!(session_key_filters);

            while let Some(session_key_filter) = session_key_filters.next().await {
                let update = SessionKeyFilterStreamResV1 {
                    action: ActionV1::Add.into(),
                    filter: Some(session_key_filter.into()),
                };
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
            while let Ok(update) = session_key_updates.recv().await {
                if tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}
