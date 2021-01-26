use crate::traits::{Aggregate, EventStore, Retryable};
use crate::types::AggregateRoot;
use crate::utils::retry_future;

use std::fmt::Debug;

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

#[async_trait]
pub trait SnapshotStore {
    async fn get<T, S>(&self, key: S) -> Option<AggregateRoot<T>>
    where
        T: Aggregate + DeserializeOwned + Send,
        T::Id: DeserializeOwned + Send,
        S: AsRef<str> + Send;

    async fn set<T, S>(&self, key: S, ar: &AggregateRoot<T>)
    where
        T: Aggregate + Serialize + Send,
        T::Id: Serialize,
        S: AsRef<str> + Send;
}

#[async_trait]
impl SnapshotStore for () {
    async fn get<T, S>(&self, _key: S) -> Option<AggregateRoot<T>>
    where
        T: Aggregate + DeserializeOwned + Send,
        S: AsRef<str> + Send,
    {
        None
    }
    async fn set<T, S>(&self, _key: S, _ar: &AggregateRoot<T>)
    where
        T: Aggregate + Serialize + Send,
        S: AsRef<str> + Send,
    {
    }
}

#[derive(Clone)]
pub struct Repository<E, S> {
    eventstore: E,
    cache: Option<S>,
}

impl<E, S> Repository<E, S>
where
    S: SnapshotStore,
{
    pub fn new(eventstore: E) -> Self {
        Self {
            eventstore,
            cache: None,
        }
    }

    pub fn with_cache(self, cache: S) -> Self {
        Self {
            cache: Some(cache),
            ..self
        }
    }

    pub fn inner_ref(&self) -> &E {
        &self.eventstore
    }

    pub fn inner_cache_ref(&self) -> &Option<S> {
        &self.cache
    }

    /// Handle command using the ```default()``` as initial state AggregateRoot's version as
    /// optimistic concurrency, retrying ```retry_count``` times
    pub async fn handle<T>(
        &self,
        stream_id: &E::Id,
        id: T::Id,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static,
        E: EventStore<Id = String>,
        E::Error: Retryable,
    {
        let state = AggregateRoot::default(id);
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    /// Handle command using ```state``` as initial state and AggregateRoot's version as optimistic
    /// concurrency, retrying ```retry_count``` times
    pub async fn handle_with_init<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static,
        E: EventStore<Id = String>,
        E::Error: Retryable,
    {
        retry_future(
            || self.handle_concurrent(stream_id, state.clone(), command.clone()),
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    /// Handle commands using the AggregateRoot's version for optimistic concurrency.
    pub async fn handle_concurrent<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<Id = String>,
        E::Error: Retryable,
    {
        let mut ar = self
            .dry_run_with_init(stream_id, state, command, false)
            .await?;
        let changes = ar.take_changes();

        let ar = self
            .eventstore
            .commit(stream_id, ar.version(), &changes)
            .await
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
            .map(|_| {
                ar.apply_iter(changes);
                ar
            })?;

        if let Some(ref cache) = self.cache {
            cache.set(stream_id, &ar).await;
        }
        Ok(ar)
    }

    /// Handle commands using the a non-existing stream as optimistic concurrency.
    pub async fn handle_not_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static,
        E: EventStore,
        E::Error: Retryable,
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
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
            .map(|_| {
                ar.apply_iter(changes);
                ar
            })
    }

    /// Handle commands with the least strict concurrency guarantee.  #NOTE use this very
    /// carefully.  This is only safe when the command generates an event that does not alter the
    /// state of the aggregate and order isn't important.  This function is here for completeness
    /// and maybe some extreme cases of optimization.  Use sparingly.
    pub async fn handle_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<Id = String>,
        E::Error: Retryable,
    {
        let mut ar = self
            .dry_run_with_init(stream_id, state, command, false)
            .await?;
        let changes = ar.take_changes();

        let ar = self
            .eventstore
            .commit_exists(stream_id, &changes)
            .await
            .map_err(|e| {
                let retryable = e.retryable();
                Error::EventStore {
                    source: e.into(),
                    retryable,
                }
            })
            .map(|_| {
                ar.apply_iter(changes);
                ar
            })?;

        if let Some(ref cache) = self.cache {
            cache.set(stream_id, &ar).await;
        }
        Ok(ar)
    }

    pub async fn manual_commit<T>(
        &self,
        stream_id: &E::Id,
        ar: AggregateRoot<T>,
        events: Vec<T::Event>,
        entry: E::ManualEntry,
    ) -> Result<(), Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<Id = String>,
        E::Error: Retryable,
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

        let ar = events.iter().fold(ar, |mut ar, e| {
            ar.apply(e);
            ar
        });

        if let Some(ref cache) = self.cache {
            cache.set(stream_id, &ar).await;
        }
        Ok(())
    }

    /// Load Aggregate and handle command without committing to eventstore
    pub async fn dry_run<T>(
        &self,
        stream_id: &E::Id,
        id: T::Id,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + DeserializeOwned,
        T::Id: DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<Id = String>,
    {
        let state = AggregateRoot::default(id);
        self.dry_run_with_init(stream_id, state, command, allow_unknown)
            .await
    }

    /// Load Aggregate and handle command using the specified state without committing to
    /// eventstore
    pub async fn dry_run_with_init<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate + DeserializeOwned,
        T::Id: DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<Id = String>,
    {
        let mut ar = match self.cache {
            Some(ref cache) => cache.get(stream_id).await.unwrap_or(state),
            _ => state,
        };
        let start_position = ar.version();
        let mut f = |e| {
            ar.apply(&e);
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
