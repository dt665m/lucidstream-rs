use crate::traits::{Aggregate, EventStore};
use crate::types::AggregateRoot;
use crate::utils::retry_future;

use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

pub trait Retryable {
    fn retryable(&self) -> bool;
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("EventStore load error: `{0}`")]
    Load(String),

    #[error("EventStore commit error: `{0}`")]
    Commit(String, bool),

    #[error("Entity command error: `{0}`")]
    EntityCommand(String),

    #[error("Duplicate entity error")]
    DuplicateEntity,

    #[error("Unknown entity error")]
    UnknownEntity,

    #[error("Optimistic concurrency error")]
    Concurrency,
}

pub fn concurrency_retryable<T>(result: &Result<T, Error>) -> bool {
    match result.as_ref() {
        Err(Error::Commit(_, retryable)) => *retryable,
        _ => false,
    }
}

#[derive(Clone)]
pub struct Repository<E>(E);

impl<E> Repository<E> {
    pub fn new(eventstore: E) -> Self {
        Self(eventstore)
    }

    pub fn inner_ref(&self) -> &E {
        &self.0
    }

    pub async fn handle<T>(
        &self,
        stream_id: &str,
        id: T::Id,
        command: T::Command,
        initial_state: Option<T>,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static,
        E: EventStore<T::Event>,
        E::Error: Retryable,
    {
        retry_future(
            || {
                self.handle_exists(
                    stream_id,
                    id.clone(),
                    command.clone(),
                    initial_state.clone(),
                )
            },
            concurrency_retryable,
            retry_count,
        )
        .await
    }

    pub async fn handle_not_exists<T>(
        &self,
        stream_id: &str,
        id: T::Id,
        command: T::Command,
        initial_state: Option<T>,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static,
        E: EventStore<T::Event>,
        E::Error: Retryable,
    {
        // we don't need incur a load from the event store because the commit
        // will guarantee that this aggregate id does not exist / has no events
        let initial_state = initial_state.unwrap_or_else(T::default);
        let mut ar = AggregateRoot::new_with_state(id, initial_state, 0);

        let changes = ar
            .handle(command)
            .map_err(|e| Error::EntityCommand(e.to_string()))?
            .take_changes();

        self.0
            .commit_not_exists(stream_id, &changes)
            .await
            .map_err(|e| Error::Commit(e.to_string(), e.retryable()))
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
        stream_id: &str,
        id: T::Id,
        command: T::Command,
        initial_state: Option<T>,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<T::Event>,
        E::Error: Retryable,
    {
        let initial_state = initial_state.unwrap_or_else(T::default);
        let mut ar = AggregateRoot::new_with_state(id, initial_state, 0);
        let mut f = |e| {
            ar.apply(&e);
        };

        let _count = self
            .0
            .load_to(stream_id, &mut f)
            .await
            .map_err(|e| Error::Load(e.to_string()))
            .and_then(|count| match count {
                0 => Err(Error::UnknownEntity),
                _ => Ok(count),
            })?;

        let changes = ar
            .handle(command)
            .map_err(|e: T::Error| Error::EntityCommand(e.to_string()))?
            .take_changes();

        self.0
            .commit_exists(&stream_id, &changes)
            .await
            .map_err(|e: E::Error| Error::Commit(e.to_string(), e.retryable()))
            .map(|_| {
                ar.apply_iter(changes);
                ar
            })
    }

    pub async fn dry_run<T>(
        &self,
        stream_id: &str,
        id: T::Id,
        command: T::Command,
        initial_state: Option<T>,
    ) -> Result<AggregateRoot<T>, Error>
    where
        T: Aggregate,
        T::Event: Serialize + DeserializeOwned,
        E: EventStore<T::Event>,
        E::Error: Retryable,
    {
        let initial_state = initial_state.unwrap_or_else(T::default);
        let mut ar = AggregateRoot::new_with_state(id, initial_state, 0);
        let mut f = |e| {
            ar.apply(&e);
        };

        let _count = self
            .0
            .load_to(stream_id, &mut f)
            .await
            .map_err(|e| Error::Load(e.to_string()))
            .and_then(|count| match count {
                0 => Err(Error::UnknownEntity),
                _ => Ok(count),
            })?;

        ar.handle(command)
            .map_err(|e: T::Error| Error::EntityCommand(e.to_string()))?;
        Ok(ar)
    }
}
