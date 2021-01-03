use lucidstream::traits::EventStore as EventStoreT;
use lucidstream::types::Envelope;

use std::collections::HashMap;
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
impl<E> EventStoreT<E> for MemEventStore
where
    E: Serialize + DeserializeOwned + Display + Send + Sync + 'static,
{
    type Error = Error;

    async fn load_to<S, F>(&self, id: S, f: &mut F) -> Result<u64, Self::Error>
    where
        F: FnMut(E) + Send + Sync,
        S: AsRef<str> + Send + Sync,
    {
        if let Some(entries) = self.0.lock().unwrap().get(id.as_ref()) {
            let history = entries
                .iter()
                .map(|e| serde_json::from_str(e))
                .collect::<Result<Vec<Envelope<String, E>>, _>>()
                .unwrap();
            let len = history.len();
            history.into_iter().for_each(|e| {
                let inner = e.into_inner();
                f(inner);
            });
            Ok(len as u64)
        } else {
            Ok(0)
        }
    }

    async fn load_history<S>(&self, id: S) -> Result<Vec<E>, Self::Error>
    where
        S: AsRef<str> + Send + Sync,
    {
        if let Some(entries) = self.0.lock().unwrap().get(id.as_ref()) {
            let history = entries
                .iter()
                .map(|e| serde_json::from_str(e))
                .collect::<Result<Vec<Envelope<String, E>>, _>>()
                .unwrap()
                .into_iter()
                .map(|e| e.into_inner())
                .collect::<Vec<E>>();
            Ok(history)
        } else {
            Ok(vec![])
        }
    }

    async fn commit<S>(&self, id: S, version: u64, events: &[E]) -> Result<(), Self::Error>
    where
        S: AsRef<str> + Send + Sync,
    {
        for e in events {
            self.0
                .lock()
                .unwrap()
                .entry(id.as_ref().to_owned())
                .or_insert_with(Vec::new)
                .push(
                    serde_json::json!(Envelope {
                        id: id.as_ref(),
                        version,
                        data: e
                    })
                    .to_string(),
                );
        }
        Ok(())
    }

    /// commit `events` for `id` using "must exist" as optimistic concurrency
    async fn commit_exists<S>(&self, _id: S, _events: &[E]) -> Result<(), Self::Error>
    where
        S: AsRef<str> + Send + Sync,
    {
        unimplemented!();
    }

    /// commit `events` for `id` using "must not exist" as optimistic concurrency
    async fn commit_not_exists<S>(&self, _id: S, _events: &[E]) -> Result<(), Self::Error>
    where
        S: AsRef<str> + Send + Sync,
    {
        unimplemented!();
    }

    async fn exists<S>(&self, id: S) -> Result<bool, Self::Error>
    where
        S: AsRef<str> + Send + Sync,
    {
        Ok(self.0.lock().unwrap().contains_key(id.as_ref()))
    }
}
