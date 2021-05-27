use lucidstream::traits::EventStore as EventStoreT;
use lucidstream::types::Envelope;

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error: `{0}`")]
    EventStore(&'static str),
}

pub struct MemEventStore(Arc<Mutex<HashMap<String, Vec<String>>>>);

impl MemEventStore {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl Default for MemEventStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventStoreT for MemEventStore {
    type Id = String;
    type Metadata = ();
    type ManualEntry = bool;
    type Error = Error;

    async fn load_to<E, F>(
        &self,
        id: &Self::Id,
        start_position: u64,
        f: &mut F,
    ) -> Result<u64, Self::Error>
    where
        E: DeserializeOwned + Send + Sync,
        F: FnMut(E, Self::Metadata) + Send + Sync,
    {
        if let Some(entries) = self.0.lock().unwrap().get(id) {
            let history = entries
                .iter()
                .skip(start_position.try_into().unwrap())
                .map(|e| serde_json::from_str(e))
                .collect::<Result<Vec<Envelope<E>>, _>>()
                .unwrap();
            let len = history.len();
            history.into_iter().for_each(|e| {
                let inner = e.into_inner();
                f(inner, ());
            });
            Ok(len as u64)
        } else {
            Ok(0)
        }
    }

    async fn load_history<E>(&self, id: &Self::Id) -> Result<Vec<(E, Self::Metadata)>, Self::Error>
    where
        E: DeserializeOwned + Send + Sync,
    {
        if let Some(entries) = self.0.lock().unwrap().get(id) {
            let history = entries
                .iter()
                .map(|e| serde_json::from_str(e))
                .collect::<Result<Vec<Envelope<E>>, _>>()
                .unwrap()
                .into_iter()
                .map(|e| (e.into_inner(), ()))
                .collect::<Vec<(E, Self::Metadata)>>();
            Ok(history)
        } else {
            Ok(vec![])
        }
    }

    async fn commit<E>(&self, id: &Self::Id, version: u64, events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync,
    {
        for e in events {
            self.0
                .lock()
                .unwrap()
                .entry(id.to_owned())
                .or_insert_with(Vec::new)
                .push(
                    serde_json::json!(Envelope {
                        id: id.to_owned(),
                        version,
                        data: e
                    })
                    .to_string(),
                );
        }
        Ok(())
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<E>(&self, _id: &Self::Id, _events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync,
    {
        unimplemented!();
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<E>(&self, _id: &Self::Id, _events: &[E]) -> Result<(), Self::Error>
    where
        E: Serialize + Display + Send + Sync,
    {
        unimplemented!();
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn manual_commit(
        &self,
        _id: &Self::Id,
        _entry: Self::ManualEntry,
    ) -> Result<(), Self::Error> {
        unimplemented!();
    }

    async fn exists(&self, id: &Self::Id) -> Result<bool, Self::Error> {
        Ok(self.0.lock().unwrap().contains_key(id))
    }
}
