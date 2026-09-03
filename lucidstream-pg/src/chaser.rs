use std::time::Duration;

use lucidstream::chaser::{EventSorter, SequencedEvent};
use serde::de::DeserializeOwned;
use sqlx::{Acquire, PgPool, Postgres, Transaction, postgres::PgListener};

use crate::{Error, QueryEvent, Result, select_events_from, validate_domain};

pub const DEFAULT_CHASE_BATCH_SIZE: i64 = 100;
pub const DEFAULT_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);

/// Runtime policy for a PostgreSQL event chaser.
#[derive(Clone, Copy, Debug)]
pub struct PgEventChaserConfig {
    batch_size: i64,
    reconciliation_interval: Duration,
}

impl Default for PgEventChaserConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_CHASE_BATCH_SIZE,
            reconciliation_interval: DEFAULT_RECONCILIATION_INTERVAL,
        }
    }
}

impl PgEventChaserConfig {
    pub fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_reconciliation_interval(mut self, interval: Duration) -> Self {
        self.reconciliation_interval = interval;
        self
    }

    pub fn batch_size(&self) -> i64 {
        self.batch_size
    }

    pub fn reconciliation_interval(&self) -> Duration {
        self.reconciliation_interval
    }

    fn validate(&self) -> Result<()> {
        if self.batch_size <= 0 {
            return Err(Error::InvalidChaserBatchSize(self.batch_size));
        }
        if self.reconciliation_interval.is_zero() {
            return Err(Error::InvalidReconciliationInterval);
        }
        Ok(())
    }
}

impl<T, U> SequencedEvent for QueryEvent<T, U> {
    fn sequence(&self) -> i64 {
        self.sequence
    }
}

/// A live, strictly ordered chaser for a Lucid Stream PostgreSQL event table.
///
/// PostgreSQL notifications are wake-up hints only. Events are loaded from a safe,
/// writer-fenced window and are never dispatched directly from notification payloads.
/// Every returned item must be acknowledged after the projection transaction commits.
///
/// The guarantee assumes Lucid Stream's append-only writer contract: event positions are
/// allocated by the event table's owned `CACHE 1` sequence as part of the `INSERT`. Callers
/// must not preallocate positions, insert explicit positions, update positions, or delete
/// events behind a running chaser.
pub struct PgEventChaser<T, U> {
    listener: PgListener,
    domain: String,
    config: PgEventChaserConfig,
    sorter: EventSorter<QueryEvent<T, U>>,
    initial_reconciliation: bool,
}

impl<T, U> PgEventChaser<T, U>
where
    T: DeserializeOwned + Send + Unpin,
    U: DeserializeOwned + Send + Unpin,
{
    pub async fn connect(
        pool: &PgPool,
        domain: impl Into<String>,
        checkpoint: i64,
    ) -> Result<Self> {
        Self::connect_with_config(pool, domain, checkpoint, PgEventChaserConfig::default()).await
    }

    pub async fn connect_with_config(
        pool: &PgPool,
        domain: impl Into<String>,
        checkpoint: i64,
        config: PgEventChaserConfig,
    ) -> Result<Self> {
        let domain = domain.into();
        validate_domain(&domain)?;
        config.validate()?;
        verify_chaser_sequence(pool, &domain).await?;

        // LISTEN is established before the initial head capture. A commit racing with
        // startup is therefore either visible to that capture or leaves a notification.
        let mut listener = PgListener::connect_with(pool).await?;
        listener.listen(&events_table(&domain)).await?;

        Ok(Self {
            listener,
            domain,
            config,
            sorter: EventSorter::new(checkpoint)?,
            initial_reconciliation: true,
        })
    }

    pub fn checkpoint(&self) -> i64 {
        self.sorter.acknowledged()
    }

    pub fn safe_head(&self) -> i64 {
        self.sorter.safe_head()
    }

    pub fn pending_sequence(&self) -> Option<i64> {
        self.sorter.pending_sequence()
    }

    /// Waits for and returns exactly one ordered item.
    ///
    /// Call [`Self::acknowledge`] only after the projection and its durable checkpoint
    /// have committed. Calling `next` again first returns an error rather than allowing
    /// a second event to overtake the pending event.
    pub async fn next(&mut self) -> Result<QueryEvent<T, U>> {
        loop {
            if let Some(item) = self.sorter.next_item()? {
                return Ok(item);
            }

            if self.sorter.needs_load() {
                self.load_next_page().await?;
                continue;
            }

            self.wait_for_reconciliation().await?;
        }
    }

    pub fn acknowledge(&mut self, sequence: i64) -> Result<()> {
        self.sorter.acknowledge(sequence)?;
        Ok(())
    }

    async fn load_next_page(&mut self) -> Result<()> {
        let start = self.sorter.loaded_through();
        let safe_head = self.sorter.safe_head();
        let events = select_events_from::<_, T, U>(
            &mut self.listener,
            &self.domain,
            start,
            safe_head,
            self.config.batch_size,
        )
        .await?;

        let page_is_exhausted = events.len() < self.config.batch_size as usize
            || events
                .last()
                .is_some_and(|event| event.sequence == safe_head);
        self.sorter.push_page(events)?;
        if page_is_exhausted {
            self.sorter.finish_loading()?;
        }
        Ok(())
    }

    async fn wait_for_reconciliation(&mut self) -> Result<()> {
        if self.initial_reconciliation {
            self.initial_reconciliation = false;
        } else if self.listener.next_buffered().is_none() {
            let wake =
                tokio::time::timeout(self.config.reconciliation_interval, self.listener.recv())
                    .await;
            if let Ok(notification) = wake {
                notification?;
            }
        }

        // Coalesce the notifications already received. Their payload and ordering are
        // intentionally irrelevant; the database range is the authoritative source.
        while self.listener.next_buffered().is_some() {}

        let safe_head = capture_safe_head_with_listener(&mut self.listener, &self.domain).await?;
        self.sorter.set_safe_head(safe_head)?;
        Ok(())
    }
}

/// Captures the highest committed event position after fencing concurrent event writers.
///
/// This has the same append-only writer requirements as [`PgEventChaser`].
pub async fn capture_safe_head(pool: &PgPool, domain: &str) -> Result<i64> {
    validate_domain(domain)?;
    verify_chaser_sequence(pool, domain).await?;
    let mut transaction = pool.begin().await?;
    let head = capture_safe_head_in_transaction(&mut transaction, domain).await?;
    transaction.commit().await?;
    Ok(head)
}

/// Verifies the sequence invariant required by safe-head capture.
pub async fn verify_chaser_sequence(pool: &PgPool, domain: &str) -> Result<()> {
    validate_domain(domain)?;
    let table = events_table(domain);
    let settings: Option<(String, i64)> = sqlx::query_as(
        r#"
        SELECT sequence_class.oid::regclass::text, sequence_settings.seqcache
        FROM pg_class AS sequence_class
        JOIN pg_sequence AS sequence_settings
          ON sequence_settings.seqrelid = sequence_class.oid
        WHERE sequence_class.oid = pg_get_serial_sequence($1, 'sequence')::regclass
        "#,
    )
    .bind(&table)
    .fetch_optional(pool)
    .await?;

    let Some((sequence, cache_size)) = settings else {
        return Err(Error::MissingEventSequence(table));
    };
    if cache_size != 1 {
        return Err(Error::UnsafeSequenceCache {
            sequence,
            cache_size,
        });
    }
    Ok(())
}

async fn capture_safe_head_with_listener(listener: &mut PgListener, domain: &str) -> Result<i64> {
    let mut transaction = listener.begin().await?;
    let head = capture_safe_head_in_transaction(&mut transaction, domain).await?;
    transaction.commit().await?;
    Ok(head)
}

async fn capture_safe_head_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    domain: &str,
) -> Result<i64> {
    // READ COMMITTED is part of the proof: the SELECT must take its snapshot only
    // after the table lock has waited for all earlier event writers to resolve.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut **transaction)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {} IN SHARE MODE",
        events_table(domain)
    )))
    .execute(&mut **transaction)
    .await?;
    let head = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COALESCE((SELECT sequence FROM {} ORDER BY sequence DESC LIMIT 1), 0)",
        events_table(domain)
    )))
    .fetch_one(&mut **transaction)
    .await?;
    Ok(head)
}

fn events_table(domain: &str) -> String {
    format!("{domain}_events")
}
