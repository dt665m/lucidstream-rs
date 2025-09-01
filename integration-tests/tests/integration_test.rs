use std::fmt::{self, Display};

use lucidstream::prelude::*;
use lucidstream_pg::Repo as PgRepo;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use uuid::Uuid;

fn init() {
    let _ = pretty_env_logger::try_init();
}

async fn connect_pg_repo() -> PgRepo {
    let pool =
        PgPool::connect("postgres://postgres:123456@localhost:5432/postgres?sslmode=disable")
            .await
            .expect("pool should connect. qed");
    let domain = "it_account";
    lucidstream_pg::EMBEDDED_MIGRATE
        .run(&pool)
        .await
        .expect("migration should succeed");
    lucidstream_pg::init_domain(&pool, domain)
        .await
        .expect("commit procedure creation should succeed. qed");
    PgRepo::new(pool, domain)
        .await
        .expect("pg repo should succeed")
}

#[tokio::test]
async fn test_all_pg() {
    use lucidstream_pg::ManualEvent;

    init();
    log::info!("TEST_ALL_PG");
    let repo = connect_pg_repo().await;
    let id = Uuid::new_v4();

    // should create a new one
    let mut ar = repo
        .load::<Account>(&id.simple().to_string())
        .await
        .unwrap();
    assert_eq!(ar.id(), &id.simple().to_string());

    log::info!("====== testing ... commands");
    let mut all_events = vec![];
    ar.handle(Command::Credit { value: 5 }).unwrap();
    all_events.extend(repo.commit_with_state(&mut ar).await.unwrap());
    ar.handle(Command::Credit { value: 5 }).unwrap();
    all_events.extend(repo.commit_with_state(&mut ar).await.unwrap());
    ar.handle(Command::Debit { value: 5 }).unwrap();
    all_events.extend(repo.commit_with_state(&mut ar).await.unwrap());

    log::info!("====== testing ... manual commit");
    ar.handle(Command::Credit { value: 5 }).unwrap();
    let events = ar.take_changes();
    let expected_version: i64 = ar.version().try_into().unwrap();
    let ar = ar.apply(&events);
    let manual_event_id = Uuid::parse_str("00000000-0000-0000-0000-00000000beef").unwrap();
    let manual_metadata = 42;
    let manual_events = events
        .iter()
        .map(|event| ManualEvent {
            event,
            metadata: Some(&manual_metadata),
            id: manual_event_id,
        })
        .collect::<Vec<ManualEvent<Event, i32>>>();
    repo.manual_commit(expected_version, ar, &manual_events)
        .await
        .unwrap();
    all_events.extend(events);

    log::info!("====== testing ... manual commit accidental same UUID");
    ar.handle(Command::Credit { value: 5 }).unwrap();
    let events = ar.take_changes();
    let expected_version: i64 = ar.version().try_into().unwrap();
    let mut bad_ar = ar.clone();
    let bad_ar = bad_ar.apply(&events);
    let manual_event_id = Uuid::parse_str("00000000-0000-0000-0000-00000000beef").unwrap();
    let manual_metadata = 43;
    let manual_events = events
        .iter()
        .map(|event| ManualEvent {
            event,
            metadata: Some(&manual_metadata),
            id: manual_event_id,
        })
        .collect::<Vec<ManualEvent<Event, i32>>>();
    let res = repo
        .manual_commit(expected_version, bad_ar, &manual_events)
        .await;
    assert!(res.is_err());

    log::info!("====== testing ... manual commit again to check if sequence has holes");
    ar.handle(Command::Credit { value: 5 }).unwrap();
    let events = ar.take_changes();
    let expected_version: i64 = ar.version().try_into().unwrap();
    let ar = ar.apply(&events);
    let manual_event_id_2 = Uuid::parse_str("00000000-0000-0000-0000-00000000beff").unwrap();
    let manual_metadata = 42;
    let manual_events = events
        .iter()
        .map(|event| ManualEvent {
            event,
            metadata: Some(&manual_metadata),
            id: manual_event_id_2,
        })
        .collect::<Vec<ManualEvent<Event, i32>>>();
    repo.manual_commit(expected_version, ar, &manual_events)
        .await
        .unwrap();
    all_events.extend(events);
    log::info!("====== complete");

    log::info!("====== testing ... event loading and aggregate rehydration");
    let ar = repo
        .load::<Account>(&id.simple().to_string())
        .await
        .unwrap();
    assert_eq!(ar.id(), &id.simple().to_string());
    assert_eq!(ar.state().balance, 15);
    assert_eq!(ar.version(), 5);

    let history = repo
        .select_events_from::<Account, i32>(0, 5, 5)
        .await
        .unwrap();
    assert_eq!(history.len(), 5);

    history.iter().enumerate().for_each(|(i, x)| {
        assert_eq!(i as i64 + 1, x.sequence);
        assert_eq!(all_events[i], x.data);
        if let Some(metadata) = x.metadata {
            assert_eq!(metadata, manual_metadata);
            if x.sequence == 4 {
                assert_eq!(x.id, manual_event_id);
            } else if x.sequence == 5 {
                assert_eq!(x.id, manual_event_id_2);
            }
        }
    });
    log::info!("====== complete");
}

#[tokio::test]
async fn benchmark() {
    init();
    log::debug!("BENCHMARK");

    // Use the Postgres repo directly for benchmarking
    let repo = std::sync::Arc::new(connect_pg_repo().await);

    // Ensure the aggregate exists with an initial event
    let id = "654321".to_string();
    let mut ar = repo.load::<Account>(&id).await.unwrap();
    ar.handle(Command::Credit { value: 5 }).unwrap();
    repo.commit_with_state(&mut ar).await.unwrap();

    // Run the benchmark with concurrent credits against the same aggregate
    malory::judge_me(
        10000,
        5,
        repo.clone(),
        move |r: std::sync::Arc<PgRepo>, _i| async move {
            let id = "654321".to_string();
            let mut ar = r.load::<Account>(&id).await.unwrap();
            ar.handle(Command::Credit { value: 5 }).unwrap();
            // Ignore occasional concurrency conflicts for throughput-focused benchmark
            let _ = r.commit_with_state(&mut ar).await;
            true
        },
    )
    .await;
}

#[derive(Debug)]
pub enum Error {
    Msg(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test error")
    }
}

impl std::error::Error for Error {}

#[derive(Serialize, Deserialize, Clone)]
pub enum Command {
    Create { owner: String, balance: i64 },
    Debit { value: i64 },
    Credit { value: i64 },
}

impl Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Command::Create { .. } => "Create",
                Command::Debit { .. } => "Debit",
                Command::Credit { .. } => "Credit",
            }
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
#[serde(tag = "kind")]
pub enum Event {
    Created { owner: String },
    Credited { value: i64 },
    Debited { value: i64 },
}

impl Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Event::Created { .. } => "Created",
                Event::Debited { .. } => "Debited",
                Event::Credited { .. } => "Credited",
            }
        )
    }
}

pub type AccountAR = AggregateRoot<Account>;

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub owner: String,
    pub suspended: bool,
    pub balance: i64,
}

impl Aggregate for Account {
    type Command = Command;
    type Event = Event;
    type Error = Error;

    fn kind() -> &'static str {
        "accountAggregate"
    }

    fn handle(&self, command: Self::Command) -> Result<Vec<Self::Event>, Self::Error> {
        // validations
        match command {
            Command::Create { owner, balance } => Ok(vec![
                Event::Created { owner },
                Event::Debited { value: balance },
            ]),

            Command::Debit { value } => {
                if let Some(val) = self.balance.checked_sub(value) {
                    if val < 0 {
                        Err(Error::Msg("insufficient funds"))
                    } else {
                        Ok(vec![Event::Debited { value }])
                    }
                } else {
                    Err(Error::Msg("invalid debit"))
                }
            }

            Command::Credit { value } => Ok(vec![Event::Credited { value }]),
        }
    }

    fn apply(self, event: &Self::Event) -> Self {
        match event {
            Event::Created { owner } => Self {
                owner: owner.clone(),
                ..self
            },
            Event::Credited { value } => Self {
                balance: self.balance + value,
                ..self
            },
            Event::Debited { value } => Self {
                balance: self.balance - value,
                ..self
            },
        }
    }
}
