use std::fmt::Display;

use async_trait::async_trait;
use eventstore::{Client, EventData, ExpectedVersion, ReadResult};
use futures::prelude::*;
use lucidstream::traits::{EventStore as EventStoreT, Retryable};
use serde::{de::DeserializeOwned, Serialize};

pub mod includes {
    pub use eventstore;
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("EventStore error: `{source}`")]
    EventStore {
        #[from]
        source: eventstore::Error,
    },

    #[error("Wrong expected version")]
    WrongExpectedVersion,

    #[error("Serialization error: `{0}`")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct EventStore {
    inner: Client,
    batch_count: u64,
}

impl Retryable for Error {
    fn retryable(&self) -> bool {
        matches!(self, Error::WrongExpectedVersion)
    }
}

pub struct ManualEntry {
    pub version: Option<u64>,
    pub events: Vec<EventData>,
}

impl EventStore {
    pub fn new(inner: Client, batch_count: u64) -> Self {
        Self { inner, batch_count }
    }

    pub fn inner_ref(&self) -> &Client {
        &self.inner
    }

    pub fn stream_id(kind: &str, id: &str) -> String {
        [kind, "_", id].concat()
    }

    pub fn expected_version(aggregate_version: u64) -> Option<u64> {
        match aggregate_version {
            0 => None,
            n => Some(n - 1),
        }
    }
}

#[async_trait]
impl EventStoreT for EventStore {
    type Id = String;
    type ManualEntry = ManualEntry;
    type Error = Error;

    async fn load_to<E, F>(&self, id: &Self::Id, start_position: u64, f: &mut F) -> Result<u64>
    where
        E: DeserializeOwned + Send + Sync,
        F: FnMut(E) + Send + Sync,
    {
        load_all_events::<E, _>(&self.inner, id, start_position, self.batch_count, f).await
    }

    async fn load_history<E>(&self, id: &Self::Id) -> Result<Vec<E>>
    where
        E: DeserializeOwned + Send + Sync,
    {
        let mut history = vec![];
        let mut f = |e| {
            history.push(e);
        };
        let _ = load_all_events::<E, _>(&self.inner, id, 0, self.batch_count, &mut f).await?;
        Ok(history)
    }

    async fn commit<E>(&self, id: &Self::Id, version: u64, events: &[E]) -> Result<()>
    where
        E: Serialize + Display + Send + Sync,
    {
        // eventstore event index starts at 0.  We use u64 for aggregate starting at 1 for the
        // first event, so the -1 is required to align with the store
        commit(&self.inner, id, events, ExpectedVersion::Exact(version - 1)).await
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<()>
    where
        E: Serialize + Display + Send + Sync,
    {
        commit(&self.inner, id, events, ExpectedVersion::StreamExists).await
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<()>
    where
        E: Serialize + Display + Send + Sync,
    {
        commit(&self.inner, id, events, ExpectedVersion::NoStream).await
    }

    /// commit `events` for `kind` and `id` using "must not exist" as optimistic concurrency
    async fn manual_commit(
        &self,
        id: &Self::Id,
        entry: Self::ManualEntry,
    ) -> Result<(), Self::Error> {
        let ManualEntry { version, events } = entry;
        let write_result = self
            .inner
            .write_events(id)
            .expected_version(version.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact))
            .send(stream::iter(events))
            .await?;

        log::debug!("write result: {:?}", write_result);
        write_result
            .map(|_| ())
            .map_err(|_| Error::WrongExpectedVersion)
    }

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        let res = self
            .inner
            .read_stream(id)
            .start_from_beginning()
            .execute(1)
            .await?;

        match res {
            ReadResult::Ok(_) => Ok(true),
            _ => Ok(false),
        }
    }
}

async fn commit<E>(
    conn: &Client,
    stream_id: &str,
    events: &[E],
    expected_version: ExpectedVersion,
) -> Result<()>
where
    E: Serialize + Display,
{
    let event_datum = events
        .iter()
        .map(|e| EventData::json(e.to_string(), e))
        .collect::<std::result::Result<Vec<EventData>, _>>()?;

    let write_result = conn
        .write_events(stream_id)
        .expected_version(expected_version)
        .send(stream::iter(event_datum))
        .await?;

    log::debug!("write result: {:?}", write_result);
    write_result
        .map(|_| ())
        .map_err(|_| Error::WrongExpectedVersion)
}

async fn load_all_events<E, F>(
    conn: &Client,
    stream_id: &str,
    start_position: u64,
    batch_count: u64,
    f: &mut F,
) -> Result<u64>
where
    E: DeserializeOwned,
    F: FnMut(E),
{
    let mut position = start_position;
    loop {
        let count = load_events::<E, _>(conn, stream_id, position, batch_count, f).await?;

        position += count;
        if count == batch_count {
            // completed a whole batch, try more
            log::debug!(
                "read {:?}, moving on to next batch from {:?} to {:?}",
                count,
                position,
                batch_count,
            );
        } else {
            // there isn't anymore to read
            log::debug!("batch complete, {:?}/{:?}", count, batch_count);
            break;
        }
    }
    Ok(position)
}

async fn load_events<E, F>(
    conn: &Client,
    stream_id: &str,
    start_position: u64,
    load_count: u64,
    f: &mut F,
) -> Result<u64>
where
    E: DeserializeOwned,
    F: FnMut(E),
{
    log::debug!("loading events from {:?}", start_position);
    let res = conn
        .read_stream(stream_id.to_owned())
        .start_from(start_position)
        .execute(load_count)
        .await?;

    let mut count = 0;
    if let ReadResult::Ok(mut stream) = res {
        while let Some(event) = stream.try_next().await? {
            let event = event.get_original_event();
            let payload: E = event.as_json()?;
            count += 1;
            f(payload);

            log::debug!("event: {:?}", event.event_type);
            log::debug!("  stream id: {:?}", event.stream_id);
            log::debug!("  revision: {:?}", event.revision);
            log::debug!("  position: {:?}", event.position);
            log::debug!("  count: {:?}", count);
        }
    }

    Ok(count)
}
