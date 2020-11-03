use lucidstream::repository::Retryable;
use lucidstream::traits::{Aggregate, EventStore as EventStoreT};
use lucidstream::types::AggregateRoot;

use async_trait::async_trait;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

use eventstore::{EventData, EventStoreDBConnection, ExpectedVersion, ReadResult};

pub mod includes {
    pub use eventstore;
    pub use uuid;
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("EventStore error: `{0}`")]
    EventStore(String),
    #[error("Wrong expected version")]
    WrongExpectedVersion,
    #[error("Serialization error: `{0}`")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct EventStore {
    inner: EventStoreDBConnection,
    batch_count: u64,
}

impl Retryable for Error {
    fn retryable(&self) -> bool {
        match self {
            Error::WrongExpectedVersion => true,
            _ => false,
        }
    }
}

impl EventStore {
    pub fn new(inner: EventStoreDBConnection, batch_count: u64) -> Self {
        Self { inner, batch_count }
    }

    pub fn inner_ref(&self) -> &EventStoreDBConnection {
        &self.inner
    }

    pub async fn commit_with_uuids<T: Aggregate>(
        &self,
        id: &T::Id,
        version: u64,
        events: &[(T::Event, Uuid)],
    ) -> Result<()>
    where
        T::Event: Serialize,
    {
        let event_datum = events
            .iter()
            .map(|(e, id)| EventData::json(e.to_string(), e).map(|e| e.id(*id)))
            .collect::<std::result::Result<Vec<EventData>, _>>()?;

        let stream_id = [T::kind(), "_", &id.to_string()].concat();
        let write_result = self
            .inner
            .write_events(stream_id)
            .expected_version(ExpectedVersion::Exact(version))
            .send(stream::iter(event_datum))
            .await
            .map_err(|e| Error::EventStore(e.to_string()))?;

        println!("write result: {:?}", write_result);
        write_result
            .map(|_| ())
            .map_err(|_| Error::WrongExpectedVersion)
    }
}

#[async_trait]
impl EventStoreT for EventStore {
    type Error = Error;

    async fn load<T: Aggregate>(&self, id: T::Id) -> Result<AggregateRoot<T>>
    where
        T::Event: DeserializeOwned,
    {
        let stream_id = [T::kind(), "_", &id.to_string()].concat();
        let mut history = vec![];
        let mut ar = AggregateRoot::new(id);
        let mut position = 0;
        loop {
            println!("loading batch from {:?}", position);
            let res = self
                .inner
                .read_stream(stream_id.to_owned())
                .start_from(position)
                .execute(self.batch_count)
                .await
                .map_err(|e| Error::EventStore(e.to_string()))?;

            let mut count = 0;
            if let ReadResult::Ok(mut stream) = res {
                while let Some(event) = stream
                    .try_next()
                    .await
                    .map_err(|e| Error::EventStore(e.to_string()))?
                {
                    let event = event.get_original_event();
                    let payload: T::Event = event.as_json()?;

                    ar.apply(&payload);
                    count += 1;

                    history.push(payload);
                    println!("event: {:?}", event.event_type);
                    println!("  stream id: {:?}", event.stream_id);
                    println!("  revision: {:?}", event.revision);
                    println!("  position: {:?}", event.position);
                    println!("  count: {:?}", count);
                }

                if count == self.batch_count {
                    // we read a whole batch
                    position = position + self.batch_count;
                    println!(
                        "read {:?}, moving on to next batch at {:?}",
                        count, position
                    );
                } else {
                    println!("batch complete, {:?}/{:?}", count, self.batch_count);
                    // there isn't anymore to read
                    break;
                }
            }
        }

        println!("history length: {:?}", history.len());

        Ok(ar)
    }

    async fn commit<T: Aggregate>(
        &self,
        id: &T::Id,
        version: u64,
        events: &[T::Event],
    ) -> Result<()>
    where
        T::Id: Serialize,
        T::Event: Serialize,
    {
        // eventstore event index starts at 0.  We use u64 for aggregate starting at 1 for the
        // first event, so the -1 is required to align with the store
        commit::<T>(self, id, events, ExpectedVersion::Exact(version - 1)).await
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<T: Aggregate>(&self, id: &T::Id, events: &[T::Event]) -> Result<()>
    where
        T::Id: Serialize,
        T::Event: Serialize,
    {
        commit::<T>(self, id, events, ExpectedVersion::StreamExists).await
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<T: Aggregate>(&self, id: &T::Id, events: &[T::Event]) -> Result<()>
    where
        T::Id: Serialize,
        T::Event: Serialize,
    {
        commit::<T>(self, id, events, ExpectedVersion::NoStream).await
    }

    async fn exists<T: Aggregate>(&self, id: T::Id) -> Result<bool> {
        let stream_id = [T::kind(), "_", &id.to_string()].concat();
        let res = self
            .inner
            .read_stream(stream_id)
            .start_from_beginning()
            .execute(1)
            .await
            .map_err(|e| Error::EventStore(e.to_string()))?;

        match res {
            ReadResult::Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }
}

async fn commit<T: Aggregate>(
    eventstore: &EventStore,
    id: &T::Id,
    events: &[T::Event],
    expected_version: ExpectedVersion,
) -> Result<()>
where
    T::Id: Serialize,
    T::Event: Serialize,
{
    let event_datum = events
        .iter()
        .map(|e| EventData::json(e.to_string(), e))
        .collect::<std::result::Result<Vec<EventData>, _>>()?;

    let stream_id = [T::kind(), "_", &id.to_string()].concat();
    let write_result = eventstore
        .inner
        .write_events(stream_id)
        .expected_version(expected_version)
        .send(stream::iter(event_datum))
        .await
        .map_err(|e| Error::EventStore(e.to_string()))?;

    println!("write result: {:?}", write_result);
    write_result
        .map(|_| ())
        .map_err(|_| Error::WrongExpectedVersion)
}
