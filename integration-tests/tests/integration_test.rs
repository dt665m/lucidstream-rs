use std::fmt::{self, Display};

use lucidstream::repository::Repository;
use lucidstream::traits::{Aggregate, EventStore as EventStoreT};
use lucidstream::types::*;
use lucidstream_ges::includes::eventstore::Client;
use lucidstream_ges::EventStore;
use serde::{Deserialize, Serialize};

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

#[tokio::test]
async fn test_all() {
    init();
    log::info!("TEST_ALL");
    let conn = connect_ges().await;
    let es = EventStore::new(conn.clone(), 5);
    let repo = Repository::<_, ()>::new(es);
    let id = "123456".to_string();
    let stream_id = EventStore::stream_id(Account::kind(), &id);

    log::info!("====== testing ... commands");
    let state = AggregateRoot::<Account>::default(id.clone());
    repo.handle_not_exists::<Account>(&stream_id, state.clone(), Command::Credit { value: 5 })
        .await
        .unwrap();
    repo.handle::<Account>(&stream_id, id.clone(), Command::Credit { value: 5 }, 2)
        .await
        .unwrap();
    repo.handle::<Account>(&stream_id, id.clone(), Command::Debit { value: 5 }, 2)
        .await
        .unwrap();
    log::info!("====== complete");

    let mut ar = AggregateRoot::<Account>::default(id.clone());
    let mut f = |e| {
        ar.apply(&e);
    };

    log::info!("====== testing ... event loading and aggregate rehydration");
    let _count = repo.inner_ref().load_to(&stream_id, &mut f).await.unwrap();
    assert_eq!(ar.id(), &id);
    assert_eq!(ar.state().balance, 5);
    log::info!("{:?} {:?}", _count, ar);
    log::info!("====== complete");
}

#[tokio::test]
async fn benchmark() {
    init();
    log::debug!("BENCHMARK");

    let conn = connect_ges().await;
    let es = EventStore::new(conn.clone(), 5);
    let repo = std::sync::Arc::new(Repository::<_, ()>::new(es));

    let id = "654321".to_string();
    let stream_id = EventStore::stream_id(Account::kind(), &id);
    let state = AggregateRoot::default(id);
    repo.handle_not_exists::<Account>(&stream_id, state, Command::Credit { value: 5 })
        .await
        .unwrap();

    malory::judge_me(10000, 5, repo.clone(), move |r, _i| async move {
        let id = "654321".to_string();
        let stream_id = EventStore::stream_id(Account::kind(), &id);
        let state = AggregateRoot::default(id);
        r.handle_exists::<Account>(&stream_id, state, Command::Credit { value: 5 })
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

#[derive(Clone)]
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

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
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

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    pub owner: String,
    pub suspended: bool,
    pub balance: i64,
}

impl Aggregate for Account {
    type Id = String;
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
