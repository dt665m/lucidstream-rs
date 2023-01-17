use lucidstream::traits::Aggregate;
use lucidstream::types::AggregateRoot;

use std::{fmt::Debug, marker::Unpin, num::TryFromIntError};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgRow},
    types::Json,
    Row,
};
use uuid::Uuid;

pub static EMBEDDED_MIGRATE: Migrator = sqlx::migrate!();

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An envelope for borrowed members for optimization
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct CommitEnvelope<'a, T> {
    pub aggregate_id: &'a Uuid,
    pub version: i64,
    #[serde(flatten)]
    pub data: &'a T,
}

/// Envelope structure for DTO's that may need the Id and Version of an aggregate.  Can be used to
/// encapsulate events or aggregates before serialization
#[derive(Debug, Eq, PartialEq, Deserialize)]
pub struct Envelope<T> {
    pub sequence: i64,
    pub aggregate_id: String,
    pub id: String,
    pub version: u64,
    #[serde(flatten)]
    pub data: T,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("sqlx error: `{0}`")]
    Sqlx(#[from] sqlx::Error),

    #[error("Int Conversion error: `{0}`")]
    IntConversion(#[from] TryFromIntError),

    #[error("Invalid UUID")]
    UuidError(#[from] uuid::Error),
}

//#TODO this might work if we just turned this into an EventStore trait
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Repo {
    pool: PgPool,

    domain: String,
    commit_proc: String,
    aggregate_query: String,
}

impl Repo {
    pub async fn new<S: Into<String>>(pool: PgPool, domain: S) -> Result<Self> {
        let domain = domain.into();
        let commit_proc = format!("CALL {}_commit($1, $2, $3, $4, $5, $6)", &domain);
        let aggregate_query = format!(
            "SELECT current_state FROM {}_aggregates WHERE aggregate_id = $1",
            &domain
        );

        let mut repo = Self {
            pool,

            domain,
            commit_proc,
            aggregate_query,
        };

        //#NOTE integrity check is vital.  If we crash from accidentally screwing up the insert of
        //events by repeating used UUID's or if there was a postgres server crash, sequences will
        //be off.  Its better to be safe and just fix it everytime.
        repo.check_sequence_integrity().await?;
        Ok(repo)
    }

    pub fn pool(&mut self) -> &PgPool {
        &self.pool
    }

    pub async fn load<T: Aggregate + DeserializeOwned + Unpin>(
        &mut self,
        id: &Uuid,
    ) -> Result<AggregateRoot<T>>
    where
        T::Event: Unpin,
    {
        sqlx::query(&self.aggregate_query)
            .bind(id)
            .try_map(|row: PgRow| Ok(row.try_get::<Json<AggregateRoot<T>>, _>(0)?.0))
            .fetch_optional(&self.pool)
            .await
            .map(|a| a.unwrap_or_else(|| AggregateRoot::<T>::new(id)))
            .map_err(Into::into)
    }

    pub async fn commit_with_state<T: Aggregate + Serialize>(
        &mut self,
        aggregate: &mut AggregateRoot<T>,
        maybe_event_ids: Option<Vec<Uuid>>,
    ) -> Result<Vec<T::Event>> {
        let event_len = aggregate.changes().len();
        let event_ids = if let Some(ids) = maybe_event_ids {
            ids
        } else {
            let mut v = vec![];
            for _ in 0..event_len {
                v.push(Uuid::new_v4())
            }
            v
        };

        assert!(event_len > 0, "cannot have zero changes in aggregate");
        assert!(
            event_len == event_ids.len(),
            "event and event_id length mismatch"
        );

        let events = aggregate.take_changes();
        let expected_version: i64 = aggregate.version().try_into()?;
        let aggregate = aggregate.apply(&events);
        let updated_version: i64 = aggregate.version().try_into()?;
        let aggregate_id = Uuid::parse_str(aggregate.id())?;

        // prepare for serialization
        let events_jsonb = (1i64..)
            .zip(events.iter())
            .map(|(i, data)| {
                Json(CommitEnvelope {
                    aggregate_id: &aggregate_id,
                    version: expected_version + i,
                    data,
                })
            })
            .collect::<Vec<Json<CommitEnvelope<T::Event>>>>();

        sqlx::query(&self.commit_proc)
            .bind(aggregate_id)
            .bind(expected_version)
            .bind(updated_version)
            .bind(Json(&aggregate))
            .bind(events_jsonb)
            .bind(event_ids)
            .execute(&self.pool)
            .await?;

        Ok(events)
    }

    pub async fn check_sequence_integrity(&mut self) -> Result<()> {
        sqlx::query("SELECT ls_check_sequence_integrity($1)")
            .bind(&self.domain)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }
}

pub async fn init_domain(pool: &PgPool, domain: &str) -> Result<()> {
    sqlx::query("SELECT ls_new_commit_proc($1)")
        .bind(domain)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

pub async fn migrate(pool: &PgPool) {
    EMBEDDED_MIGRATE
        .run(pool)
        .await
        .expect("migration should succeed");
}
