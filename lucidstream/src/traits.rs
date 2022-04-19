use crate::types::AggregateRoot;

use std::fmt::{Debug, Display};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

pub trait Aggregate: Default + Debug + Clone + Send + Sync + Sized {
    /// Command
    type Command: Serialize + DeserializeOwned + Display + Clone + Send + Sync;

    /// Event type
    type Event: Serialize + DeserializeOwned + Display + Clone + Send + Sync;

    /// Error type
    type Error: std::error::Error + Send + Sync + 'static;

    /// Gets snapshot frequency by event version
    /// #NOTE A Snapshot frequency of 0 will panic the Snapshot store on cache set.
    fn snapshot_frequency() -> Option<u64> {
        None
    }

    /// Kind of aggregate
    fn kind() -> &'static str;

    /// Handle a command, which generates events
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;

    /// Apply events   
    fn apply(self, event: &Self::Event) -> Self;
}

pub trait AR: Default + Debug + Clone + Send + Sync + Sized {
    /// Command
    type Command: Serialize + DeserializeOwned + Display + Clone + Send + Sync;

    /// Event type
    type Event: Serialize + DeserializeOwned + Display + Clone + Send + Sync;

    /// Error type
    type Error: std::error::Error + Send + Sync + 'static;

    /// Gets snapshot frequency by event version
    fn snapshot_frequency() -> Option<u64> {
        None
    }

    /// Kind of aggregate
    fn kind() -> &'static str;
}

#[async_trait]
pub trait Rep<E: EventStore> {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Handle command using the ```default()``` as initial state AggregateRoot's version as
    /// optimistic concurrency, retrying ```retry_count``` times
    async fn handle<T, I>(
        &self,
        stream_id: &E::Id,
        id: I,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        I: Display + Send + Sync;
}

#[async_trait]
pub trait Repository<E: EventStore> {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Handle command using the ```default()``` as initial state AggregateRoot's version as
    /// optimistic concurrency, retrying ```retry_count``` times
    async fn handle<T, I>(
        &self,
        stream_id: &E::Id,
        id: I,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        I: Display + Send + Sync;

    /// Handle command using passed in state's version as optimistic concurrency, retrying
    /// ```retry_count``` times
    async fn handle_with<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;

    /// Handle commands using the AggregateRoot's version for optimistic concurrency.
    async fn handle_concurrent<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;

    /// Handle commands using the a non-existing stream as optimistic concurrency.
    async fn handle_not_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;

    /// Handle commands with the least strict concurrency guarantee.  #NOTE use this very
    /// carefully.  This is only safe when the command generates an event that does not alter the
    /// state of the aggregate and order isn't important.  This function is here for completeness
    /// and maybe some extreme cases of optimization.  Use sparingly.
    async fn handle_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;

    /// Manually commit to the underlying eventstore, using the eventstore's ManualEntry type
    /// wrapper
    async fn manual_commit<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        events: Vec<T::Event>,
        entry: E::ManualEntry,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;

    /// Load Aggregate and handle command using the specified state without committing to
    /// eventstore
    async fn dry_run<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned;
}

#[async_trait]
pub trait EventStore: Send + Sync {
    type Id: Send + Sync + 'static;
    type Metadata: Send + Sync + 'static;
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
        F: FnMut(E, Self::Metadata) + Send + Sync;

    /// load events based on `kind` and `id`.  Returns Vector of Events loaded
    async fn load_history<E>(&self, id: &Self::Id) -> Result<Vec<(E, Self::Metadata)>, Self::Error>
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

#[async_trait]
pub trait SnapshotStore {
    async fn get<T, S>(&self, key: S) -> Option<T>
    where
        T: DeserializeOwned + Send,
        S: AsRef<str> + Send;

    async fn set<T, S>(&self, key: S, ar: &T)
    where
        T: Serialize + Send + Sync,
        S: AsRef<str> + Send;
}

pub trait Retryable {
    fn retryable(&self) -> bool;
}
