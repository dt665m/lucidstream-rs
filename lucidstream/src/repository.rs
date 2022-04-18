use crate::traits::{Aggregate, EventStore, Repository, Retryable, SnapshotStore};
use crate::types::AggregateRoot;
use crate::utils::retry_future;

use std::fmt::{Debug, Display};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("EventStore error: `{source}`")]
    EventStore {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        retryable: bool,
    },

    #[error("Entity command error: `{source}`")]
    Aggregate {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Duplicate entity error")]
    DuplicateEntity,

    #[error("Unknown entity error")]
    UnknownEntity,

    #[error("Optimistic concurrency error")]
    Concurrency,
}

pub fn concurrency_retryable<T>(result: &Result<T, Error>) -> bool {
    match result.as_ref() {
        Err(Error::EventStore { retryable, .. }) => *retryable,
        _ => false,
    }
}

#[derive(Clone)]
pub struct Repo<E> {
    eventstore: E,
}

impl<E> Repo<E> {
    pub fn new(eventstore: E) -> Self {
        Self { eventstore }
    }

    pub fn inner_ref(&self) -> &E {
        &self.eventstore
    }
}

#[async_trait]
impl<E> Repository<E> for Repo<E>
where
    E: EventStore<Id = String> + Send + Sync,
    E::Error: Retryable,
{
    type Error = Error;

    async fn handle<T, I>(
        &self,
        stream_id: &E::Id,
        id: I,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        I: Display,
        I: Display + Send + Sync,
    {
        let state = AggregateRoot::new(id);
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    async fn handle_with<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    async fn handle_concurrent<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        let mut ar = self.dry_run(stream_id, state, command, false).await?;
        let changes = ar.take_changes();

        self.eventstore
            .commit(stream_id, ar.version(), &changes)
            .await
            .map(|_| {
                ar.apply(changes);
                ar
            })
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
    }

    async fn handle_not_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        // we don't need incur a load from the event store because the commit
        // will guarantee that this aggregate id does not exist / has no events
        let mut ar = state;
        let changes = ar
            .handle(command)
            .map_err(|e| Error::Aggregate { source: e.into() })?
            .take_changes();

        self.eventstore
            .commit_not_exists(stream_id, &changes)
            .await
            .map(|_| {
                ar.apply(changes);
                ar
            })
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
    }

    async fn handle_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        let mut ar = self.dry_run(stream_id, state, command, false).await?;
        let changes = ar.take_changes();

        self.eventstore
            .commit_exists(stream_id, &changes)
            .await
            .map(|_| {
                ar.apply(changes);
                ar
            })
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
    }

    async fn manual_commit<T>(
        &self,
        stream_id: &E::Id,
        mut state: AggregateRoot<T>,
        events: Vec<T::Event>,
        entry: E::ManualEntry,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        self.eventstore
            .manual_commit(stream_id, entry)
            .await
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })?;

        state.apply(events);
        Ok(state)
    }

    async fn dry_run<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        let mut ar = state;
        let start_position = ar.version();
        let mut f = |e, _| {
            ar.apply(std::iter::once(e));
        };

        let _count = self
            .eventstore
            .load_to(stream_id, start_position, &mut f)
            .await
            .map_err(|e| Error::EventStore {
                source: e.into(),
                retryable: false,
            })
            .and_then(|count| match (count, allow_unknown) {
                (0, false) => Err(Error::UnknownEntity),
                _ => Ok(count),
            })?;

        ar.handle(command)
            .map_err(|e| Error::Aggregate { source: e.into() })?;
        Ok(ar)
    }
}

#[derive(Clone)]
pub struct SnapshotRepo<E, S> {
    inner: Repo<E>,
    cache: S,
}

impl<E, S> SnapshotRepo<E, S> {
    pub fn new(eventstore: E, cache: S) -> Self {
        Self {
            inner: Repo::new(eventstore),
            cache,
        }
    }

    pub fn inner_ref(&self) -> &E {
        &self.inner.eventstore
    }

    pub fn inner_cache_ref(&self) -> &S {
        &self.cache
    }
}

#[async_trait]
impl<E, S> Repository<E> for SnapshotRepo<E, S>
where
    E: EventStore<Id = String> + Send + Sync,
    E::Error: Retryable,
    S: SnapshotStore + Send + Sync,
{
    type Error = Error;

    async fn handle<T, I>(
        &self,
        stream_id: &E::Id,
        id: I,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        I: Display + Send + Sync,
    {
        let state = AggregateRoot::new(id);
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    async fn handle_with<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    async fn handle_concurrent<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        if let Some(freq) = T::snapshot_frequency() {
            let state = self.cache.get(stream_id).await.unwrap_or(state);
            let ar = self
                .inner
                .handle_concurrent(stream_id, state, command)
                .await?;
            if ar.version() % freq == 0 {
                self.cache.set(stream_id, &ar).await;
            }
            Ok(ar)
        } else {
            self.inner
                .handle_concurrent(stream_id, state, command)
                .await
        }
    }

    async fn handle_not_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        self.inner
            .handle_not_exists(stream_id, state, command)
            .await
    }

    async fn handle_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        if let Some(freq) = T::snapshot_frequency() {
            let state = self.cache.get(stream_id).await.unwrap_or(state);
            let ar = self.inner.handle_exists(stream_id, state, command).await?;
            if ar.version() % freq == 0 {
                self.cache.set(stream_id, &ar).await;
            }
            Ok(ar)
        } else {
            self.inner.handle_exists(stream_id, state, command).await
        }
    }

    async fn manual_commit<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        events: Vec<T::Event>,
        entry: E::ManualEntry,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        let ar = self
            .inner
            .manual_commit(stream_id, state, events, entry)
            .await?;

        match T::snapshot_frequency() {
            Some(freq) if ar.version() % freq == 0 => self.cache.set(stream_id, &ar).await,
            _ => (),
        }
        Ok(ar)
    }

    async fn dry_run<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
    {
        let state = self.cache.get(stream_id).await.unwrap_or(state);
        self.inner
            .dry_run(stream_id, state, command, allow_unknown)
            .await
    }
}
