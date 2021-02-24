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
    type Error: std::error::Error + Send + Sync + 'static;

    /// whether the aggregate should be snapshotted and how frequent
    fn snapshot_frequency() -> Option<u64> {
        None
    }

    /// aggregate type description
    fn kind() -> &'static str;

    /// handle commands, generating events
    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error>;

    /// apply events on aggretate
    fn apply(self, event: &Self::Event) -> Self;
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
pub trait Repository<E: EventStore> {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Handle command using the ```default()``` as initial state AggregateRoot's version as
    /// optimistic concurrency, retrying ```retry_count``` times
    async fn handle<T>(
        &self,
        stream_id: &E::Id,
        id: T::Id,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static;

    /// Handle command using ```state``` as initial state and AggregateRoot's version as optimistic
    /// concurrency, retrying ```retry_count``` times
    async fn handle_with_init<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        retry_count: usize,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static;

    /// Handle commands using the AggregateRoot's version for optimistic concurrency.
    async fn handle_concurrent<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned;

    /// Handle commands using the a non-existing stream as optimistic concurrency.
    async fn handle_not_exists<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned,
        T::Error: std::error::Error + Send + 'static;

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
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned;

    /// Manually commit to the underlying eventstore, using the eventstore's ManualEntry type
    /// wrapper
    async fn manual_commit<T>(
        &self,
        stream_id: &E::Id,
        ar: AggregateRoot<T>,
        events: Vec<T::Event>,
        entry: E::ManualEntry,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + Serialize + DeserializeOwned,
        T::Id: Serialize + DeserializeOwned,
        T::Event: Serialize + DeserializeOwned;

    /// Load Aggregate and handle command without committing to eventstore
    async fn dry_run<T>(
        &self,
        stream_id: &E::Id,
        id: T::Id,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + DeserializeOwned,
        T::Id: DeserializeOwned,
        T::Event: Serialize + DeserializeOwned;

    /// Load Aggregate and handle command using the specified state without committing to
    /// eventstore
    async fn dry_run_with_init<T>(
        &self,
        stream_id: &E::Id,
        state: AggregateRoot<T>,
        command: T::Command,
        allow_unknown: bool,
    ) -> Result<AggregateRoot<T>, Self::Error>
    where
        T: Aggregate + DeserializeOwned,
        T::Id: DeserializeOwned,
        T::Event: Serialize + DeserializeOwned;
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

pub trait Retryable {
    fn retryable(&self) -> bool;
}
