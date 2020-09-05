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

pub struct Repository<T, E>(std::marker::PhantomData<(T, E)>);

impl<T: Aggregate, E: EventStore> Repository<T, E>
where
    T::Id: Serialize + DeserializeOwned,
    T::Event: Serialize + DeserializeOwned,
    E::Error: Retryable,
{
    pub async fn handle(
        store: &E,
        id: T::Id,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Error> {
        let factory = || {
            let id = id.clone();
            let command = command.clone();

            async move {
                let mut ar = store
                    .load::<T>(id)
                    .await
                    .map_err(|e| Error::Load(e.to_string()))
                    .and_then(|ar| match ar.version() {
                        0 => Err(Error::UnknownEntity),
                        _ => Ok(ar),
                    })?;

                let changes = ar
                    .handle(command.clone())
                    .map_err(|e| Error::EntityCommand(e.to_string()))?
                    .take_changes();

                // eventstore event index starts at 0, but we use u64 for aggregate so we start at
                // version 1, so the -1 is required to align with the store
                let expected_version = ar.version() - 1;
                store
                    .commit::<T>(ar.id(), expected_version, &changes)
                    .await
                    .map_err(|e| Error::Commit(e.to_string(), e.retryable()))
                    .map(|_| {
                        ar.apply_iter(changes);
                        ar
                    })
            }
        };

        retry_future(factory, concurrency_retryable, retry_count).await
    }

    pub async fn handle_not_exists(
        store: &E,
        id: T::Id,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error> {
        // we don't need incur a load from the event store because the commit
        // will guarantee that this aggregate id does not exist / has no events
        let mut ar = AggregateRoot::<T>::new(id);
        let changes = ar
            .handle(command)
            .map_err(|e| Error::EntityCommand(e.to_string()))?
            .take_changes();

        store
            .commit_not_exists::<T>(ar.id(), &changes)
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
    pub async fn handle_exists(
        store: &E,
        id: T::Id,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error> {
        let mut ar = store
            .load::<T>(id)
            .await
            .map_err(|e| Error::Load(e.to_string()))
            .and_then(|ar| match ar.version() {
                0 => Err(Error::UnknownEntity),
                _ => Ok(ar),
            })?;

        let changes = ar
            .handle(command)
            .map_err(|e| Error::EntityCommand(e.to_string()))?
            .take_changes();

        store
            .commit_exists::<T>(ar.id(), &changes)
            .await
            .map_err(|e| Error::Commit(e.to_string(), e.retryable()))
            .map(|_| {
                ar.apply_iter(changes);
                ar
            })
    }

    pub async fn dry_run(
        store: &E,
        id: T::Id,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Error> {
        let ar = store
            .load::<T>(id)
            .await
            .map_err(|e| Error::Load(e.to_string()))
            .and_then(|ar| match ar.version() {
                0 => Err(Error::UnknownEntity),
                _ => Ok(ar),
            })?;

        let mut ar = ar;
        ar.handle(command)
            .map_err(|e| Error::EntityCommand(e.to_string()))?;
        Ok(ar)
    }
}
