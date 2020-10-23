use lucidstream::traits::{Aggregate, EventStore};
use lucidstream::types::{AggregateRoot, Envelope};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum MemEventStoreError {
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
impl EventStore for MemEventStore {
    type Error = MemEventStoreError;

    async fn load<T: Aggregate>(&self, id: T::Id) -> Result<AggregateRoot<T>, Self::Error>
    where
        T::Id: DeserializeOwned,
        T::Event: DeserializeOwned,
    {
        let mut ar = AggregateRoot::<T>::new(id);
        if let Some(entries) = self.0.lock().unwrap().get(&ar.id().to_string()) {
            let history = entries
                .iter()
                .map(|e| serde_json::from_str(e))
                .collect::<Result<Vec<Envelope<T::Id, T::Event>>, _>>()
                .expect("success")
                .into_iter()
                .map(|e| e.into_inner())
                .collect::<Vec<T::Event>>();

            ar.apply_iter(history);
        }
        Ok(ar)
    }

    async fn commit<T: Aggregate>(
        &self,
        id: &T::Id,
        version: u64,
        events: &[T::Event],
    ) -> Result<(), Self::Error>
    where
        T::Id: Serialize,
        T::Event: Serialize,
    {
        for e in events {
            self.0
                .lock()
                .unwrap()
                .entry(id.to_string())
                .or_insert_with(Vec::new)
                .push(
                    serde_json::json!(Envelope {
                        id: id.clone(),
                        version,
                        data: e
                    })
                    .to_string(),
                );
        }
        Ok(())
    }

    async fn commit_exists<T: Aggregate>(
        &self,
        _id: &T::Id,
        _events: &[T::Event],
    ) -> Result<(), Self::Error> {
        unimplemented!();
    }

    async fn commit_not_exists<T: Aggregate>(
        &self,
        _id: &T::Id,
        _events: &[T::Event],
    ) -> Result<(), Self::Error> {
        unimplemented!();
    }

    async fn exists<T: Aggregate>(&self, id: T::Id) -> Result<bool, Self::Error> {
        Ok(self.0.lock().unwrap().contains_key(&id.to_string()))
    }
}
