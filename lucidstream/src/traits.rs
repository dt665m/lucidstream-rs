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
pub trait EventStore<E>
where
    E: Serialize + DeserializeOwned + Display + Send + Sync,
{
    type Error: std::error::Error + Send + Sync + 'static;

    /// load events based on `kind` and `id` and applys it into `f` callback.  Returns the total
    /// count loaded  
    async fn load_to<S, F>(&self, id: S, f: &mut F) -> Result<u64, Self::Error>
    where
        E: 'async_trait,
        F: FnMut(E) + Send + Sync,
        S: AsRef<str> + Send + Sync;

    /// load events based on `kind` and `id`.  Returns Vector of Events loaded
    async fn load_history<S: AsRef<str>>(&self, id: S) -> Result<Vec<E>, Self::Error>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync;

    /// commit `events` for `kind` and `id` using `version` as optimistic concurrency
    async fn commit<S>(&self, id: S, version: u64, events: &[E]) -> Result<(), Self::Error>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync;

    /// commit `events` for `kind` and `id` using "must exist" as optimistic concurrency
    async fn commit_exists<S: AsRef<str>>(&self, id: S, events: &[E]) -> Result<(), Self::Error>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync;

    /// commit `events` for `kind` and `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<S>(&self, id: S, events: &[E]) -> Result<(), Self::Error>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync;

    /// simple helper to check if any events exist for `kind` and `id`
    async fn exists<S>(&self, id: S) -> Result<bool, Self::Error>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync;
}

pub trait Retryable {
    fn retryable(&self) -> bool;
}
