use crate::{
    admin::{AuthCache, KeyType},
    org::get_org_pubkeys,
    session_key::{self, SessionKeyFilter},
    GrpcResult, GrpcStreamRequest, GrpcStreamResult,
};
use anyhow::Result;
use file_store::traits::MsgVerify;
use futures::stream::StreamExt;
use helium_proto::services::iot_config::{
    self, ActionV1, SessionKeyFilterGetReqV1, SessionKeyFilterListReqV1,
    SessionKeyFilterStreamReqV1, SessionKeyFilterStreamResV1, SessionKeyFilterUpdateReqV1,
    SessionKeyFilterUpdateResV1, SessionKeyFilterV1,
};
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast::{Receiver, Sender};
use tonic::{Request, Response, Status};

pub struct SessionKeyFilterService {
    auth_cache: AuthCache,
    pool: Pool<Postgres>,
    update_channel: Sender<SessionKeyFilterStreamResV1>,
    shutdown: triggered::Listener,
}

impl SessionKeyFilterService {
    pub fn new(auth_cache: AuthCache, pool: Pool<Postgres>, shutdown: triggered::Listener) -> Self {
        let (update_tx, _) = tokio::sync::broadcast::channel(128);
        Self {
            auth_cache,
            pool,
            update_channel: update_tx,
            shutdown,
        }
    }

    fn subscribe_to_session_keys(&self) -> Receiver<SessionKeyFilterStreamResV1> {
        self.update_channel.subscribe()
    }

    fn clone_update_channel(&self) -> Sender<SessionKeyFilterStreamResV1> {
        self.update_channel.clone()
    }

    async fn verify_request_signature<'a, R>(&self, request: &R, id: u64) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            return Ok(());
        }

        let org_keys = get_org_pubkeys(id, &self.pool)
            .await
            .map_err(|_| Status::internal("auth verification error"))?;

        for pubkey in org_keys.iter() {
            if request.verify(pubkey).is_ok() {
                tracing::debug!("request authorized by delegate {pubkey}");
                return Ok(());
            }
        }
        Err(Status::permission_denied("unauthorized request signature"))
    }

    async fn verify_stream_request_signature<R>(&self, request: &R) -> Result<(), Status>
    where
        R: MsgVerify,
    {
        if self
            .auth_cache
            .verify_signature(KeyType::PacketRouter, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized for registered packet router");
            Ok(())
        } else if self
            .auth_cache
            .verify_signature(KeyType::Administrator, request)
            .await
            .is_ok()
        {
            tracing::debug!("request authorized by admin");
            Ok(())
        } else {
            Err(Status::permission_denied("unauthorized request signature"))
        }
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

        self.verify_request_signature(&request, request.oui).await?;

        let pool = self.pool.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        tokio::spawn(async move {
            let mut filters = session_key::list_for_oui(request.oui, &pool);

            while let Some(filter) = filters.next().await {
                let message = match filter {
                    Ok(filter) => Ok(filter.into()),
                    Err(bad_filter) => Err(Status::internal(format!(
                        "invalid session key filter {:?}",
                        bad_filter
                    ))),
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }

    type getStream = GrpcStreamResult<SessionKeyFilterV1>;
    async fn get(&self, request: Request<SessionKeyFilterGetReqV1>) -> GrpcResult<Self::getStream> {
        let request = request.into_inner();

        self.verify_request_signature(&request, request.oui).await?;

        let (tx, rx) = tokio::sync::mpsc::channel(20);
        let pool = self.pool.clone();

        tokio::spawn(async move {
            let mut filters =
                session_key::list_for_oui_and_devaddr(request.oui, request.devaddr.into(), &pool);

            while let Some(filter) = filters.next().await {
                let message = match filter {
                    Ok(filter) => Ok(filter.into()),
                    Err(bad_filter) => Err(Status::internal(format!(
                        "invalid session key filter {:?}",
                        bad_filter
                    ))),
                };
                if tx.send(message).await.is_err() {
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

        if let Ok(Some(first_update)) = request.message().await {
            if let Some(filter) = &first_update.filter {
                self.verify_request_signature(&first_update, filter.oui)
                    .await?;
                match first_update.action() {
                    ActionV1::Add => to_add.push(filter.into()),
                    ActionV1::Remove => to_remove.push(filter.into()),
                }
            }
        } else {
            return Err(Status::invalid_argument("no session key filter provided"));
        }

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

        self.verify_stream_request_signature(&request).await?;

        tracing::info!("client subscribed to session key stream");

        let pool = self.pool.clone();
        let shutdown_listener = self.shutdown.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(20);

        let mut session_key_updates = self.subscribe_to_session_keys();

        tokio::spawn(async move {
            let mut session_key_filters = session_key::list_stream(&pool);

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
                if shutdown_listener.is_triggered() || tx.send(Ok(update)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(GrpcStreamResult::new(rx)))
    }
}
