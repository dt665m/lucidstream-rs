use lucidstream::repository::Retryable;
use lucidstream::traits::EventStore as EventStoreT;

use std::fmt::Display;

use async_trait::async_trait;
use futures::prelude::*;
use serde::{de::DeserializeOwned, Serialize};

use eventstore::{Client, EventData, ExpectedVersion, ReadResult};

pub mod includes {
    pub use eventstore;
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("EventStore error: `{source}`")]
    EventStore { source: eventstore::Error },

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

    pub fn to_expected_version(aggregate_version: u64) -> Option<u64> {
        match aggregate_version {
            0 => None,
            n => Some(n - 1),
        }
    }

    pub async fn manual_commit(
        &self,
        stream_id: &str,
        version: Option<u64>,
        events: Vec<EventData>,
    ) -> Result<()> {
        let write_result = self
            .inner
            .write_events(stream_id)
            .expected_version(version.map_or(ExpectedVersion::NoStream, ExpectedVersion::Exact))
            .send(stream::iter(events))
            .await
            .map_err(|e| Error::EventStore { source: e.into() })?;

        log::debug!("write result: {:?}", write_result);
        write_result
            .map(|_| ())
            .map_err(|_| Error::WrongExpectedVersion)
    }
}

#[async_trait]
impl<E> EventStoreT<E> for EventStore
where
    E: Serialize + DeserializeOwned + Display + Send + Sync,
{
    type Error = Error;

    async fn load_to<S, F>(&self, id: S, f: &mut F) -> Result<u64>
    where
        E: 'async_trait,
        F: FnMut(E) + Send + Sync,
        S: AsRef<str> + Send + Sync,
    {
        load_all_events::<E, _>(&self.inner, id.as_ref(), self.batch_count, f).await
    }

    async fn load_history<S>(&self, id: S) -> Result<Vec<E>>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync,
    {
        let mut history = vec![];
        let mut f = |e| {
            history.push(e);
        };
        let _ = load_all_events::<E, _>(&self.inner, id.as_ref(), self.batch_count, &mut f).await?;
        Ok(history)
    }

    async fn commit<S>(&self, id: S, version: u64, events: &[E]) -> Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        // eventstore event index starts at 0.  We use u64 for aggregate starting at 1 for the
        // first event, so the -1 is required to align with the store
        commit(
            &self.inner,
            id.as_ref(),
            events,
            ExpectedVersion::Exact(version - 1),
        )
        .await
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<S>(&self, id: S, events: &[E]) -> Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        commit(
            &self.inner,
            id.as_ref(),
            events,
            ExpectedVersion::StreamExists,
        )
        .await
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<S>(&self, id: S, events: &[E]) -> Result<()>
    where
        S: AsRef<str> + Send + Sync,
    {
        commit(&self.inner, id.as_ref(), events, ExpectedVersion::NoStream).await
    }

    async fn exists<S>(&self, id: S) -> Result<bool>
    where
        E: 'async_trait,
        S: AsRef<str> + Send + Sync,
    {
        let res = self
            .inner
            .read_stream(id.as_ref())
            .start_from_beginning()
            .execute(1)
            .await
            .map_err(|e| Error::EventStore { source: e.into() })?;

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
        .await
        .map_err(|e| Error::EventStore { source: e.into() })?;

    log::debug!("write result: {:?}", write_result);
    write_result
        .map(|_| ())
        .map_err(|_| Error::WrongExpectedVersion)
}

async fn load_all_events<E, F>(
    conn: &Client,
    stream_id: &str,
    batch_count: u64,
    f: &mut F,
) -> Result<u64>
where
    E: DeserializeOwned,
    F: FnMut(E),
{
    let mut position = 0;
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
        .await
        .map_err(|e| Error::EventStore { source: e.into() })?;

    let mut count = 0;
    if let ReadResult::Ok(mut stream) = res {
        while let Some(event) = stream
            .try_next()
            .await
            .map_err(|e| Error::EventStore { source: e.into() })?
        {
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
