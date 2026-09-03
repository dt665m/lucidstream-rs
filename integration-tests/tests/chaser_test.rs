use std::time::Duration;

use lucidstream_pg::{PgEventChaser, QueryEvent};
use serde::Deserialize;
use sqlx::{Executor, PgPool, postgres::PgPoolOptions, types::Json};
use uuid::Uuid;

const DOMAIN: &str = "it_chaser";
const INTEGRITY_DOMAIN: &str = "it_chaser_integrity";

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind")]
enum Event {
    Recorded { label: String },
}

async fn connect() -> PgPool {
    let database_url = std::env::var("LUCIDSTREAM_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:123456@localhost:5432/postgres?sslmode=disable".to_owned()
    });
    PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("test database should connect")
}

async fn insert_event<'e, E>(executor: E, aggregate_id: &str, label: &str) -> i64
where
    E: Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar(
        r#"
        INSERT INTO it_chaser_events (aggregate_id, id, version, md5short, data)
        VALUES ($1, $2, 1, 0, $3)
        RETURNING sequence
        "#,
    )
    .bind(aggregate_id)
    .bind(Uuid::new_v4())
    .bind(Json(serde_json::json!({
        "kind": "Recorded",
        "label": label,
        "metadata": null
    })))
    .fetch_one(executor)
    .await
    .expect("event insert should succeed")
}

fn assert_event(event: QueryEvent<Event, ()>, sequence: i64, label: &str) {
    assert_eq!(event.sequence, sequence);
    assert_eq!(
        event.data,
        Event::Recorded {
            label: label.to_owned()
        }
    );
}

#[tokio::test]
async fn chaser_holds_a_higher_commit_until_the_lower_position_resolves() {
    let pool = connect().await;
    lucidstream_pg::migrate(&pool).await.unwrap();
    lucidstream_pg::init_domain(&pool, DOMAIN).await.unwrap();
    sqlx::query("DROP TRIGGER IF EXISTS ls_notify_sparse ON it_chaser_events")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("SELECT ls_notify_sparse('it_chaser_events')")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("TRUNCATE it_chaser_events RESTART IDENTITY")
        .execute(&pool)
        .await
        .unwrap();

    let mut chaser = PgEventChaser::<Event, ()>::connect(&pool, DOMAIN, 0)
        .await
        .unwrap();

    let mut lower_transaction = pool.begin().await.unwrap();
    assert_eq!(
        insert_event(&mut *lower_transaction, "lower-commit", "lower").await,
        1
    );
    assert_eq!(insert_event(&pool, "higher-commit", "higher").await, 2);

    let first = {
        let pending = chaser.next();
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("higher event escaped the writer fence: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        lower_transaction.commit().await.unwrap();
        pending.await.unwrap()
    };
    let first_sequence = first.sequence;
    assert_event(first, 1, "lower");
    chaser.acknowledge(first_sequence).unwrap();

    let second = chaser.next().await.unwrap();
    let second_sequence = second.sequence;
    assert_event(second, 2, "higher");
    chaser.acknowledge(second_sequence).unwrap();

    let mut rolled_back_transaction = pool.begin().await.unwrap();
    assert_eq!(
        insert_event(
            &mut *rolled_back_transaction,
            "lower-rollback",
            "rolled-back"
        )
        .await,
        3
    );
    assert_eq!(insert_event(&pool, "after-gap", "after-gap").await, 4);

    let after_gap = {
        let pending = chaser.next();
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => panic!("event escaped before the lower gap was permanent: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        rolled_back_transaction.rollback().await.unwrap();
        pending.await.unwrap()
    };
    let after_gap_sequence = after_gap.sequence;
    assert_event(after_gap, 4, "after-gap");
    chaser.acknowledge(after_gap_sequence).unwrap();
    assert_eq!(chaser.checkpoint(), 4);
}

#[tokio::test]
async fn migrated_integrity_check_never_rewinds_and_rejects_unsafe_cache() {
    let pool = connect().await;
    lucidstream_pg::migrate(&pool).await.unwrap();
    lucidstream_pg::init_domain(&pool, INTEGRITY_DOMAIN)
        .await
        .unwrap();
    sqlx::query("TRUNCATE it_chaser_integrity_events RESTART IDENTITY")
        .execute(&pool)
        .await
        .unwrap();

    let mut transaction = pool.begin().await.unwrap();
    let consumed: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO it_chaser_integrity_events (aggregate_id, id, version, md5short, data)
        VALUES ('rolled-back', $1, 1, 0, '{}')
        RETURNING sequence
        "#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(consumed, 1);
    transaction.rollback().await.unwrap();

    sqlx::query("SELECT ls_check_sequence_integrity($1)")
        .bind(INTEGRITY_DOMAIN)
        .execute(&pool)
        .await
        .unwrap();

    let after_gap: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO it_chaser_integrity_events (aggregate_id, id, version, md5short, data)
        VALUES ('after-gap', $1, 1, 0, '{}')
        RETURNING sequence
        "#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after_gap, 2, "the integrity check must not reuse the gap");

    sqlx::query("ALTER SEQUENCE it_chaser_integrity_events_sequence_seq CACHE 2")
        .execute(&pool)
        .await
        .unwrap();

    let sql_error = sqlx::query("SELECT ls_check_sequence_integrity($1)")
        .bind(INTEGRITY_DOMAIN)
        .execute(&pool)
        .await
        .expect_err("the migration check must reject CACHE 2");
    assert!(sql_error.to_string().contains("must use CACHE 1"));

    let rust_error = lucidstream_pg::verify_chaser_sequence(&pool, INTEGRITY_DOMAIN)
        .await
        .expect_err("the Rust startup check must reject CACHE 2");
    assert!(matches!(
        rust_error,
        lucidstream_pg::Error::UnsafeSequenceCache { cache_size: 2, .. }
    ));

    sqlx::query("ALTER SEQUENCE it_chaser_integrity_events_sequence_seq CACHE 1")
        .execute(&pool)
        .await
        .unwrap();
}
