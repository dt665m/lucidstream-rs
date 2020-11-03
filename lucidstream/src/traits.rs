use crate::types::AggregateRoot;

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
    type Error: std::error::Error + Send + 'static;

    /// aggregate type description
    fn kind() -> &'static str;

    /// handle commands, generating events
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;

    /// apply events on aggretate
    fn apply(self, event: &Self::Event) -> Self;
}

#[async_trait]
pub trait EventStore {
    type Error: std::error::Error + 'static;

    /// load events for Aggregate based on `id`.  Returns an event-hydrated Aggregate
    async fn load<T: Aggregate>(&self, id: T::Id) -> Result<AggregateRoot<T>, Self::Error>
    where
        T::Id: DeserializeOwned,
        T::Event: DeserializeOwned;

    /// commit `events` for `id` using `version` as optimistic concurrency
    async fn commit<T: Aggregate>(
        &self,
        id: &T::Id,
        version: u64,
        events: &[T::Event],
    ) -> Result<(), Self::Error>
    where
        T::Id: Serialize,
        T::Event: Serialize;

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<T: Aggregate>(
        &self,
        id: &T::Id,
        events: &[T::Event],
    ) -> Result<(), Self::Error>
    where
        T::Id: Serialize,
        T::Event: Serialize;

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<T: Aggregate>(
        &self,
        id: &T::Id,
        events: &[T::Event],
    ) -> Result<(), Self::Error>
    where
        T::Id: Serialize,
        T::Event: Serialize;

    /// simple helper to check if any events exist for `id` aggregate
    async fn exists<T: Aggregate>(&self, id: T::Id) -> Result<bool, Self::Error>;
}
