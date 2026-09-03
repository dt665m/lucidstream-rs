pub mod chaser;
pub use chaser::{
    DEFAULT_CHASE_BATCH_SIZE, DEFAULT_RECONCILIATION_INTERVAL, PgEventChaser, PgEventChaserConfig,
    capture_safe_head, verify_chaser_sequence,
};

use lucidstream::traits::Aggregate;
use lucidstream::types::AggregateRoot;

use std::{fmt::Debug, marker::Unpin, num::TryFromIntError};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::{
    Executor, FromRow, Row,
    migrate::Migrator,
    postgres::{PgPool, PgRow, Postgres},
    types::Json,
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
    Sqlx(sqlx::Error),

    #[error("Int Conversion error: `{0}`")]
    IntConversion(#[from] TryFromIntError),

    #[error("Invalid UUID")]
    UuidError(#[from] uuid::Error),

    #[error("Invalid domain name: `{0}`")]
    InvalidDomain(String),

    #[error("Optimistic concurrency conflict")]
    Concurrency,

    #[error("Duplicate event id")]
    DuplicateEventId,

    #[error("Invalid commit: event length or ids mismatch")]
    InvalidCommit,

    #[error("Migration failed: `{0}`")]
    MigrationFailed(String),

    #[error("Invalid event chaser batch size: `{0}`")]
    InvalidChaserBatchSize(i64),

    #[error("The event chaser reconciliation interval must be greater than zero")]
    InvalidReconciliationInterval,

    #[error("No owned sequence exists for event table `{0}`")]
    MissingEventSequence(String),

    #[error("Sequence `{sequence}` uses CACHE {cache_size}; safe chasing requires CACHE 1")]
    UnsafeSequenceCache { sequence: String, cache_size: i64 },

    #[error(transparent)]
    EventSorter(#[from] lucidstream::chaser::EventSorterError),
}

impl From<sqlx::Error> for Error {
    fn from(value: sqlx::Error) -> Self {
        map_commit_error(value)
    }
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
        let domain: String = domain.into();
        validate_domain(&domain)?;
        let commit_proc = format!("CALL {}_commit($1, $2, $3, $4, $5, $6)", domain);
        let aggregate_query = format!(
            "SELECT current_state FROM {}_aggregates WHERE aggregate_id = $1",
            domain
        );

        let repo = Self {
            pool,
            domain,
            commit_proc,
            aggregate_query,
        };

        // Gaps are valid. Rewinding this sequence to MAX(sequence) could reuse a position
        // already certified by a chaser, so startup only verifies the safe-chasing invariant.
        chaser::verify_chaser_sequence(&repo.pool, &repo.domain).await?;
        Ok(repo)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub async fn load<T: Aggregate + DeserializeOwned + Unpin>(
        &self,
        id: &str,
    ) -> Result<AggregateRoot<T>>
    where
        T::Event: Unpin,
    {
        sqlx::query(sqlx::AssertSqlSafe(self.aggregate_query.as_str()))
            .bind(id)
            .try_map(|row: PgRow| Ok(row.try_get::<Json<AggregateRoot<T>>, _>(0)?.0))
            .fetch_optional(&self.pool)
            .await
            .map(|a| a.unwrap_or_else(|| AggregateRoot::<T>::new(id)))
            .map_err(Into::into)
    }

    pub async fn commit_with_state<T: Aggregate + Serialize>(
        &self,
        aggregate: &mut AggregateRoot<T>,
    ) -> Result<Vec<T::Event>> {
        let event_len = aggregate.changes().len();
        assert!(event_len > 0, "cannot have zero changes in aggregate");
        let mut event_ids = vec![];
        for _ in 0..event_len {
            event_ids.push(Uuid::new_v4())
        }

        let mut committed_aggregate = aggregate.clone();
        let events = committed_aggregate.take_changes();
        let expected_version: i64 = committed_aggregate.version().try_into()?;
        committed_aggregate.apply(&events);
        let updated_version: i64 = committed_aggregate.version().try_into()?;
        let aggregate_id = committed_aggregate.id();

        // prepare for serialization
        let events_jsonb = (1i64..)
            .zip(events.iter())
            .map(|(i, data)| {
                Json(CommitEnvelope {
                    aggregate_id,
                    version: expected_version + i,
                    data,
                    metadata: None,
                })
            })
            .collect::<Vec<Json<CommitEnvelope<T::Event>>>>();

        sqlx::query(sqlx::AssertSqlSafe(self.commit_proc.as_str()))
            .bind(aggregate_id)
            .bind(expected_version)
            .bind(updated_version)
            .bind(Json(&committed_aggregate))
            .bind(events_jsonb)
            .bind(event_ids)
            .execute(&self.pool)
            .await?;
        *aggregate = committed_aggregate;
        Ok(events)
    }

    /// Commit using caller-provided event IDs to enable idempotent retries
    pub async fn commit_with_state_with_ids<T: Aggregate + Serialize>(
        &self,
        aggregate: &mut AggregateRoot<T>,
        event_ids: &[Uuid],
    ) -> Result<Vec<T::Event>> {
        let event_len = aggregate.changes().len();
        assert!(event_len > 0, "cannot have zero changes in aggregate");
        if event_ids.len() != event_len {
            return Err(Error::InvalidCommit);
        }

        let mut committed_aggregate = aggregate.clone();
        let events = committed_aggregate.take_changes();
        let expected_version: i64 = committed_aggregate.version().try_into()?;
        committed_aggregate.apply(&events);
        let updated_version: i64 = committed_aggregate.version().try_into()?;
        let aggregate_id = committed_aggregate.id();

        let events_jsonb = (1i64..)
            .zip(events.iter().zip(event_ids.iter()))
            .map(|(i, (data, _id))| {
                Json(CommitEnvelope {
                    aggregate_id,
                    version: expected_version + i,
                    data,
                    metadata: None::<&()>,
                })
            })
            .collect::<Vec<Json<CommitEnvelope<T::Event>>>>();

        sqlx::query(sqlx::AssertSqlSafe(self.commit_proc.as_str()))
            .bind(aggregate_id)
            .bind(expected_version)
            .bind(updated_version)
            .bind(Json(&committed_aggregate))
            .bind(events_jsonb)
            .bind(event_ids)
            .execute(&self.pool)
            .await?;
        *aggregate = committed_aggregate;
        Ok(events)
    }

    /// The Aggregate is expected to be pre-handled by the caller, which is why the parameter is
    /// 'applied_aggregate'.
    /// example:
    /// ```ignore
    /// let expected_version = aggregate.version();
    /// aggregate.handle(command).unwrap();
    /// let events = aggregate.take_changes();
    /// aggregate.apply(&events);
    /// let meta = 42;
    /// let m_evt = events.iter()
    ///     .map(|event| ManualEvent {
    ///         event,
    ///         id: Uuid::new_v4(),
    ///         metadata: Some(&meta),
    ///     })
    ///     .collect::<Vec<_>>();
    /// repo.manual_commit(expected_version, &aggregate, &m_evt).await.unwrap();
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
        assert!(!events.is_empty(), "cannot have zero events to commit");
        let aggregate_id = applied_aggregate.id();
        let updated_version: i64 = applied_aggregate.version().try_into()?;

        let mut event_ids = vec![];
        let events_jsonb = (1i64..)
            .zip(events.iter())
            .map(|(i, manual)| {
                event_ids.push(manual.id);
                Json(CommitEnvelope {
                    aggregate_id,
                    version: expected_version + i,
                    data: manual.event,
                    metadata: manual.metadata,
                })
            })
            .collect::<Vec<Json<CommitEnvelope<T::Event, U>>>>();

        sqlx::query(sqlx::AssertSqlSafe(self.commit_proc.as_str()))
            .bind(aggregate_id)
            .bind(expected_version)
            .bind(updated_version)
            .bind(Json(&applied_aggregate))
            .bind(events_jsonb)
            .bind(event_ids)
            .execute(&self.pool)
            .await?;
        Ok(())
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
struct InnerData<T, U> {
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
    validate_domain(domain).map_err(|error| sqlx::Error::InvalidArgument(error.to_string()))?;
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        r#"
        SELECT sequence, aggregate_id, id, version, data
        FROM {}_events
        WHERE sequence > $1 AND sequence <= $2
        ORDER BY sequence
        LIMIT $3
    "#,
        domain
    )))
    .bind(start_seq)
    .bind(end_seq)
    .bind(limit)
    .fetch_all(conn)
    .await
}

pub async fn init_domain(pool: &PgPool, domain: &str) -> Result<()> {
    validate_domain(domain)?;
    sqlx::query("SELECT ls_new_commit_proc($1)")
        .bind(domain)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    EMBEDDED_MIGRATE
        .run(pool)
        .await
        .map_err(|e| Error::MigrationFailed(e.to_string()))?;
    Ok(())
}

fn validate_domain(domain: &str) -> Result<()> {
    // allow: start [a-z], then [a-z0-9_], length <= 30
    let bytes = domain.as_bytes();
    if bytes.is_empty() || bytes.len() > 30 {
        return Err(Error::InvalidDomain(domain.to_string()));
    }
    let first = bytes[0];
    if !first.is_ascii_lowercase() {
        return Err(Error::InvalidDomain(domain.to_string()));
    }
    if !bytes[1..]
        .iter()
        .all(|c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
    {
        return Err(Error::InvalidDomain(domain.to_string()));
    }
    Ok(())
}

fn map_commit_error(e: sqlx::Error) -> Error {
    if let sqlx::Error::Database(db_err) = &e {
        let code_opt = db_err.code();
        let code = code_opt.as_deref().unwrap_or("");
        let msg = db_err.message();
        let constraint_opt = db_err.constraint();
        let constraint = constraint_opt.unwrap_or("");

        // Unique violation
        if code == "23505" {
            if constraint.contains("_id") {
                return Error::DuplicateEventId;
            }
            if constraint.contains("optimistic_concurrency") {
                return Error::Concurrency;
            }
        }

        // RAISE EXCEPTION from plpgsql
        if code == "P0001" {
            if msg.contains("optimistic concurrency exception") {
                return Error::Concurrency;
            }
            if msg.contains("event id reused") {
                return Error::DuplicateEventId;
            }
            if msg.contains("event length or event_id length invalid") {
                return Error::InvalidCommit;
            }
        }
    }
    // Fallback to raw sqlx error
    Error::Sqlx(e)
}

#[cfg(test)]
mod tests {
    use std::fmt::{self, Display};

    use serde::{Deserialize, Serialize};
    use sqlx::postgres::PgPoolOptions;

    use super::*;

    #[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
    struct TestAggregate {
        handled: usize,
        committed: usize,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    enum TestCommand {
        Increment,
    }

    impl Display for TestCommand {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("increment")
        }
    }

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    enum TestEvent {
        Incremented,
    }

    impl Display for TestEvent {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("incremented")
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("test aggregate error")]
    struct TestError;

    impl Aggregate for TestAggregate {
        type Command = TestCommand;
        type Error = TestError;
        type Event = TestEvent;

        fn kind() -> &'static str {
            "test"
        }

        fn handle(
            &mut self,
            _: Self::Command,
        ) -> std::result::Result<Vec<Self::Event>, Self::Error> {
            self.handled += 1;
            Ok(vec![TestEvent::Incremented])
        }

        fn apply(mut self, _: &Self::Event) -> Self {
            self.committed += 1;
            self
        }
    }

    async fn closed_repo() -> Repo {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres@localhost/lucidstream")
            .expect("test database URL is valid");
        pool.close().await;

        Repo {
            pool,
            domain: "test".to_owned(),
            commit_proc: "CALL test_commit($1, $2, $3, $4, $5, $6)".to_owned(),
            aggregate_query: "SELECT 1".to_owned(),
        }
    }

    fn aggregate_with_pending_change() -> AggregateRoot<TestAggregate> {
        let mut aggregate = AggregateRoot::new("aggregate-id");
        aggregate
            .handle(TestCommand::Increment)
            .expect("test command succeeds");
        aggregate
    }

    fn assert_pending_change_is_preserved(aggregate: &AggregateRoot<TestAggregate>) {
        assert_eq!(aggregate.version(), 0);
        assert_eq!(
            aggregate.state(),
            &TestAggregate {
                handled: 1,
                committed: 0,
            }
        );
        assert_eq!(aggregate.changes(), &[TestEvent::Incremented]);
    }

    #[tokio::test]
    async fn commit_with_state_preserves_aggregate_when_database_commit_fails() {
        let repo = closed_repo().await;
        let mut aggregate = aggregate_with_pending_change();

        let error = repo
            .commit_with_state(&mut aggregate)
            .await
            .expect_err("a closed pool must reject the commit");

        assert!(matches!(error, Error::Sqlx(sqlx::Error::PoolClosed)));
        assert_pending_change_is_preserved(&aggregate);
    }

    #[tokio::test]
    async fn commit_with_state_with_ids_preserves_aggregate_when_database_commit_fails() {
        let repo = closed_repo().await;
        let mut aggregate = aggregate_with_pending_change();

        let error = repo
            .commit_with_state_with_ids(&mut aggregate, &[Uuid::new_v4()])
            .await
            .expect_err("a closed pool must reject the commit");

        assert!(matches!(error, Error::Sqlx(sqlx::Error::PoolClosed)));
        assert_pending_change_is_preserved(&aggregate);
    }

    #[tokio::test]
    async fn dynamic_query_helpers_reject_unsafe_domain_names() {
        let repo = closed_repo().await;

        let error =
            select_events_from::<_, TestEvent, ()>(&repo.pool, "test; DROP TABLE events", 0, 1, 1)
                .await
                .expect_err("unsafe domain names must be rejected before querying");

        assert!(matches!(error, sqlx::Error::InvalidArgument(_)));
    }
}
