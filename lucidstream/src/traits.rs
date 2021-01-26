use std::fmt::{Debug, Display};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

pub trait Aggregate: Default + Debug + Clone + Send + Sync {
    /// unique identifier type
    type Id: Display + Clone + Send + Sync;

    /// command type
    type Command: Display + Clone + Send + Sync;

    /// event type
    type Event: Display + Clone + Send + Sync;

    /// error type
    type Error: std::error::Error + Send + Sync + 'static;

    /// aggregate type description
    fn kind() -> &'static str;

    /// handle commands, generating events
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;

    /// apply events on aggretate
    fn apply(self, event: &Self::Event) -> Self;
}

#[async_trait]
pub trait EventStore {
    type Id: Send + Sync + 'static;
    type ManualEntry: Send + Sync + 'static;
    type Error: std::error::Error + Send + Sync + 'static;

    /// load events based on `kind` and `id` and applys it into `f` callback.  Returns the total
    /// count loaded  
    async fn load_to<E, F>(
        &self,
        id: &Self::Id,
        start_position: u64,
        f: &mut F,
    ) -> Result<u64, Self::Error>
    where
        E: DeserializeOwned + Send + Sync,
        F: FnMut(E) + Send + Sync;

    /// load events based on `kind` and `id`.  Returns Vector of Events loaded
    async fn load_history<E>(&self, id: &Self::Id) -> Result<Vec<E>, Self::Error>
    where
        E: DeserializeOwned + Send + Sync,
        E: 'async_trait;

    /// commit `events` for `kind` and `id` using `version` as optimistic concurrency
    async fn commit<E>(&self, id: &Self::Id, version: u64, events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync;

    /// commit `events` for `kind` and `id` using "must exist" as optimistic concurrency
    async fn commit_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync;

    /// commit `events` for `kind` and `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync;

    /// commit `params` for `id`.  This is a wildcard implementation left to the specific
    /// eventstore implementer
    async fn manual_commit(
        &self,
        id: &Self::Id,
        params: Self::ManualEntry,
    ) -> Result<(), Self::Error>;

    /// simple helper to check if any events exist for `kind` and `id`
    async fn exists(&self, id: &Self::Id) -> Result<bool, Self::Error>;
}

pub trait Retryable {
    fn retryable(&self) -> bool;
}
