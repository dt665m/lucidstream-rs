use std::fmt::Display;

use async_trait::async_trait;
use bytes::Bytes;
use eventstore::{
    AppendToStreamOptions, Client, EventData, ExpectedRevision, ReadStreamOptions, ResolvedEvent,
    StreamPosition,
};
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

    #[error("Event missing from Record")]
    MissingEvent,
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
    pub expected_revision: ExpectedRevision,
    pub events: Vec<EventData>,
}

impl EventStore {
    pub fn new(inner: Client, batch_count: u64) -> Self {
        Self { inner, batch_count }
    }

    pub fn inner_ref(&self) -> &Client {
        &self.inner
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
    type Metadata = Bytes;
    type ManualEntry = ManualEntry;
    type Error = Error;

    async fn load_to<E, F>(&self, id: &Self::Id, start_position: u64, f: &mut F) -> Result<u64>
    where
        E: DeserializeOwned + Send + Sync,
        F: FnMut(E, Self::Metadata) + Send + Sync,
    {
        load_all_events::<E, _>(&self.inner, id, start_position, self.batch_count, f).await
    }

    async fn load_history<E>(&self, id: &Self::Id) -> Result<Vec<(E, Self::Metadata)>>
    where
        E: DeserializeOwned + Send + Sync,
    {
        let mut history = vec![];
        let mut f = |e, metadata| {
            history.push((e, metadata));
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
        commit(
            &self.inner,
            id,
            events,
            ExpectedRevision::Exact(version - 1),
        )
        .await
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<()>
    where
        E: Serialize + Display + Send + Sync,
    {
        commit(&self.inner, id, events, ExpectedRevision::StreamExists).await
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<E>(&self, id: &Self::Id, events: &[E]) -> Result<()>
    where
        E: Serialize + Display + Send + Sync,
    {
        commit(&self.inner, id, events, ExpectedRevision::NoStream).await
    }

    /// commit `events` for `kind` and `id` using "must not exist" as optimistic concurrency
    async fn manual_commit(
        &self,
        id: &Self::Id,
        entry: Self::ManualEntry,
    ) -> Result<(), Self::Error> {
        let ManualEntry {
            expected_revision,
            events,
        } = entry;
        let opts = AppendToStreamOptions::default().expected_revision(expected_revision);
        let write_result = self.inner.append_to_stream(id, &opts, events).await?;

        log::debug!("write result: {:?}", write_result);
        Ok(())
    }

    async fn exists(&self, id: &Self::Id) -> Result<bool> {
        Ok(self
            .inner
            .read_stream(id, &ReadStreamOptions::default().max_count(1))
            .await?
            .next()
            .await?
            .is_some())
    }
}

async fn commit<E>(
    conn: &Client,
    stream_id: &str,
    events: &[E],
    expected_revision: ExpectedRevision,
) -> Result<()>
where
    E: Serialize + Display,
{
    let event_datum = events
        .iter()
        .map(|e| EventData::json(e.to_string(), e))
        .collect::<std::result::Result<Vec<EventData>, _>>()?;

    let opts = AppendToStreamOptions::default().expected_revision(expected_revision);
    let write_result = conn.append_to_stream(stream_id, &opts, event_datum).await?;

    log::debug!("write result: {:?}", write_result);
    Ok(())
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
    F: FnMut(E, Bytes),
{
    let mut position = start_position;
    loop {
        let count = load_events::<E, _>(conn, stream_id, position, batch_count as usize, f).await?;

        position += count;
        if count == batch_count {
            log::debug!(
                "read {:?}, moving on to next batch from {:?} to {:?}",
                count,
                position,
                batch_count,
            );
        } else {
            log::debug!("batch complete: {:?}/{:?}", count, batch_count);
            break;
        }
    }
    Ok(position)
}

async fn load_events<E, F>(
    conn: &Client,
    stream_id: &str,
    start_position: u64,
    load_count: usize,
    f: &mut F,
) -> Result<u64>
where
    E: DeserializeOwned,
    F: FnMut(E, Bytes),
{
    let opts = ReadStreamOptions::default()
        .position(StreamPosition::Position(start_position))
        .max_count(load_count);

    let mut stream = conn.read_stream(&stream_id, &opts).await?;
    let mut count = 0;
    while let Some(resolved) = stream.next().await? {
        let ResolvedEvent { event, .. } = resolved;
        let event = event.ok_or(Error::MissingEvent)?;
        count += 1;

        log::debug!(
            "event: {:?}\n  streamid: {:?}\n  revison: {:?}\n  position: {:?}\n  count: {:?}",
            event.event_type,
            event.stream_id,
            event.revision,
            event.position,
            count
        );
        f(event.as_json()?, event.custom_metadata);
    }

    Ok(count)
}
