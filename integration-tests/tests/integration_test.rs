use std::fmt::{self, Display};

use lucidstream::prelude::*;
use lucidstream_ges::includes::eventstore::Client;
use lucidstream_ges::EventStore;
use lucidstream_pg::Repo as PgRepo;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use uuid::Uuid;

fn init() {
    let _ = pretty_env_logger::try_init();
}

async fn connect_ges() -> Client {
    let settings = "esdb://admin:changeit@localhost:2113?tls=false"
        .parse()
        .expect("eventstore url should be valid.");
    Client::create(settings)
        .await
        .expect("eventstore connection is required.")
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
async fn test_all_es() {
    init();
    log::info!("TEST_ALL_ES");
    let conn = connect_ges().await;
    let es = EventStore::new(conn.clone(), 5);
    let repo = Repo::new(es);
    let id = "123456".to_string();
    let stream_id = [Account::kind(), ":", &id].concat();

    log::info!("====== testing ... commands");
    let state = AccountAR::new(id.clone());
    repo.handle_not_exists(&stream_id, state.clone(), Command::Credit { value: 5 })
        .await
        .unwrap();
    repo.handle_with(&stream_id, state.clone(), Command::Credit { value: 5 }, 2)
        .await
        .unwrap();
    repo.handle_with(&stream_id, state.clone(), Command::Debit { value: 5 }, 2)
        .await
        .unwrap();
    log::info!("====== complete");

    let mut ar = AccountAR::new(id.clone());
    let start_position = ar.version();
    let mut f = |e, _| {
        ar.apply_single(&e);
    };

    log::info!("====== testing ... event loading and aggregate rehydration");
    let _count = repo
        .inner_ref()
        .load_to(&stream_id, start_position, &mut f)
        .await
        .unwrap();
    assert_eq!(ar.id(), &id);
    assert_eq!(ar.state().balance, 5);
    log::info!("{:?} {:?}", _count, ar);
    log::info!("====== complete");
}

#[tokio::test]
async fn test_all_pg() {
    init();
    log::info!("TEST_ALL_PG");
    let mut repo = connect_pg_repo().await;
    let id = Uuid::new_v4();

    // should create a new one
    let mut ar = repo
        .load::<Account>(&id.simple().to_string())
        .await
        .unwrap();
    assert_eq!(ar.id(), &id.simple().to_string());

    log::info!("====== testing ... commands");
    ar.handle(Command::Credit { value: 5 }).unwrap();
    repo.commit_with_state(&mut ar, None).await.unwrap();
    ar.handle(Command::Credit { value: 5 }).unwrap();
    repo.commit_with_state(&mut ar, None).await.unwrap();
    ar.handle(Command::Debit { value: 5 }).unwrap();
    repo.commit_with_state(&mut ar, None).await.unwrap();
    log::info!("====== complete");

    log::info!("====== testing ... event loading and aggregate rehydration");
    let ar = repo
        .load::<Account>(&id.simple().to_string())
        .await
        .unwrap();
    assert_eq!(ar.id(), &id.simple().to_string());
    assert_eq!(ar.state().balance, 5);
    assert_eq!(ar.version(), 3);
    log::info!("====== complete");
}

#[tokio::test]
async fn benchmark() {
    init();
    log::debug!("BENCHMARK");

    let conn = connect_ges().await;
    let es = EventStore::new(conn.clone(), 5);
    let repo = std::sync::Arc::new(Repo::new(es));

    let id = "654321".to_string();
    let stream_id = [Account::kind(), ":", &id].concat();
    let state = AccountAR::new(id);
    repo.handle_not_exists(&stream_id, state, Command::Credit { value: 5 })
        .await
        .unwrap();

    malory::judge_me(10000, 5, repo.clone(), move |r, _i| async move {
        let id = "654321".to_string();
        let stream_id = [Account::kind(), ":", &id].concat();
        let state = AccountAR::new(id);
        r.handle_exists(&stream_id, state, Command::Credit { value: 5 })
            .await
            .unwrap();
        true
    })
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

type AccountAR = AggregateRoot<Account>;

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
