use lucidstream::traits::Aggregate;
use lucidstream::types::AggregateRoot;

use std::{fmt::Debug, marker::Unpin, num::TryFromIntError};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgRow, Postgres},
    types::Json,
    Executor, FromRow, Row,
};
use uuid::Uuid;

pub static EMBEDDED_MIGRATE: Migrator = sqlx::migrate!();

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An envelope for borrowed members for optimization
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct CommitEnvelope<'a, T, U = ()> {
    pub aggregate_id: &'a str,
    pub version: i64,
    #[serde(flatten)]
    pub data: &'a T,
    pub metadata: Option<&'a U>,
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

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub async fn load<T: Aggregate + DeserializeOwned + Unpin>(
        &mut self,
        id: &str,
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
    ) -> Result<Vec<T::Event>> {
        let event_len = aggregate.changes().len();
        assert!(event_len > 0, "cannot have zero changes in aggregate");
        let mut event_ids = vec![];
        for _ in 0..event_len {
            event_ids.push(Uuid::new_v4())
        }

        let events = aggregate.take_changes();
        let expected_version: i64 = aggregate.version().try_into()?;
        let aggregate = aggregate.apply(&events);
        let updated_version: i64 = aggregate.version().try_into()?;
        let aggregate_id = aggregate.id();

        // prepare for serialization
        let events_jsonb = (1i64..)
            .zip(events.iter())
            .map(|(i, data)| {
                Json(CommitEnvelope {
                    aggregate_id: &aggregate_id,
                    version: expected_version + i,
                    data,
                    metadata: None,
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

    /// The Aggregate is expected to be pre-handled by the caller, which is why the parameter is
    /// 'applied_aggregate'.
    /// example:
    /// ```
    /// let expected_version = aggregate.version();
    /// let events = aggregate.handle(command).unwrap();
    /// aggregate.apply(events);
    /// let meta = 42;
    /// let m_evt = events.iter()
    ///     .map(|e| ManualEvent{ event, id: Uuid::new_v4(), metadata: &meta })
    ///     .collect();
    /// repo.manual_commit(expected_version, aggregate, m_evt).await.unwrap();
    /// ```
    pub async fn manual_commit<'a, T, U>(
        &self,
        expected_version: i64,
        applied_aggregate: &AggregateRoot<T>,
        events: &[ManualEvent<'a, T::Event, U>],
    ) -> Result<()>
    where
        T: Aggregate + Serialize,
        U: Serialize + Send + Sync + Unpin,
    {
        assert!(events.len() > 0, "cannot have zero events to commit");
        let aggregate_id = applied_aggregate.id();
        let updated_version: i64 = applied_aggregate.version().try_into()?;

        let mut event_ids = vec![];
        let events_jsonb = (1i64..)
            .zip(events.iter())
            .map(|(i, manual)| {
                event_ids.push(manual.id);
                Json(CommitEnvelope {
                    aggregate_id: &aggregate_id,
                    version: expected_version + i,
                    data: manual.event,
                    metadata: manual.metadata,
                })
            })
            .collect::<Vec<Json<CommitEnvelope<T::Event, U>>>>();

        sqlx::query(&self.commit_proc)
            .bind(&aggregate_id)
            .bind(expected_version)
            .bind(updated_version)
            .bind(Json(&applied_aggregate))
            .bind(events_jsonb)
            .bind(event_ids)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn check_sequence_integrity(&mut self) -> Result<()> {
        sqlx::query("SELECT ls_check_sequence_integrity($1)")
            .bind(&self.domain)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn select_latest_sequence(&self) -> Result<i64> {
        select_latest_sequence(&self.pool, &self.domain)
            .await
            .map_err(Into::into)
    }

    pub async fn select_events_from<T: Aggregate + Serialize, U>(
        &self,
        start_seq: i64,
        end_seq: i64,
        limit: i64,
    ) -> Result<Vec<QueryEvent<T::Event, U>>>
    where
        T::Event: Unpin,
        U: DeserializeOwned + Send + Unpin,
    {
        select_events_from(&self.pool, &self.domain, start_seq, end_seq, limit)
            .await
            .map_err(Into::into)
    }
}

pub struct ManualEvent<'a, T, U> {
    pub event: &'a T,
    pub metadata: Option<&'a U>,
    pub id: Uuid,
}

#[derive(Clone, Debug, Deserialize)]
pub struct QueryEvent<T, U> {
    pub sequence: i64,
    pub aggregate_id: String,
    pub id: Uuid,
    pub version: i64,
    pub data: T,
    pub metadata: Option<U>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InnerData<T, U> {
    #[serde(flatten)]
    pub data: T,
    pub metadata: Option<U>,
}

impl<T, U> FromRow<'_, PgRow> for QueryEvent<T, U>
where
    T: DeserializeOwned + Send + Unpin,
    U: DeserializeOwned + Send + Unpin,
{
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let InnerData { data, metadata } =
            row.try_get::<sqlx::types::Json<InnerData<T, U>>, _>(4)?.0;
        Ok(Self {
            sequence: row.try_get(0)?,
            aggregate_id: row.try_get(1)?,
            id: row.try_get(2)?,
            version: row.try_get(3)?,
            data,
            metadata,
        })
    }
}

pub async fn select_latest_sequence<'e, T>(conn: T, domain: &str) -> sqlx::Result<i64>
where
    T: Executor<'e, Database = Postgres>,
{
    sqlx::query(&format!(
        r#"
        SELECT sequence
        FROM {}_events
        ORDER BY sequence DESC LIMIT 1
    "#,
        domain
    ))
    .try_map(|row: PgRow| row.try_get(0))
    .fetch_optional(conn)
    .await
    .map(|v| v.unwrap_or_default())
}

pub async fn select_events_from<'e, C, T, U>(
    conn: C,
    domain: &str,
    start_seq: i64,
    end_seq: i64,
    limit: i64,
) -> sqlx::Result<Vec<QueryEvent<T, U>>>
where
    C: Executor<'e, Database = Postgres>,
    T: DeserializeOwned + Send + Unpin,
    U: DeserializeOwned + Send + Unpin,
{
    sqlx::query_as(&format!(
        r#"
        SELECT sequence, aggregate_id, id, version, data
        FROM {}_events
        WHERE sequence > $1 AND sequence <= $2
        ORDER BY sequence
        LIMIT $3
    "#,
        domain
    ))
    .bind(start_seq)
    .bind(end_seq)
    .bind(limit)
    .fetch_all(conn)
    .await
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
