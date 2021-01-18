use crate::traits::Aggregate;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateRoot<T: Aggregate> {
    id: T::Id,

    version: u64,

    #[serde(flatten)]
    state: T,

    #[serde(skip_serializing)]
    #[serde(skip_deserializing)]
    changes: Vec<T::Event>,
}

impl<T: Aggregate> AggregateRoot<T>
where
    T::Error: std::error::Error,
{
    pub fn new(id: T::Id, state: T, version: u64) -> Self {
        Self {
            id,
            state,
            version,
            changes: vec![],
        }
    }

    pub fn default(id: T::Id) -> Self {
        AggregateRoot::<T>::new(id, T::default(), 0)
    }

    pub fn id(&self) -> &T::Id {
        &self.id
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn state(&self) -> &T {
        &self.state
    }

    // returning &mut Self is a little questionable.  It allows callers to not repeatedly reset the
    // returned variable (when using -> Self) but at the cost of some weird intermediate
    // differences to the original Self.
    // Currently, this design allows a caller to do the following:
    // ```
    // let ar = Aggregate....
    // ar.handle(${cmd}).take_changes() <--- this leaves the original ar variable intact
    // ```
    pub fn handle(&mut self, command: T::Command) -> Result<&mut Self, T::Error> {
        let mut events = self.state.handle(command)?;
        self.changes.append(&mut events);
        Ok(self)
    }

    pub fn take_changes(&mut self) -> Vec<T::Event> {
        std::mem::take(&mut self.changes)
    }

    pub fn apply(&mut self, event: &T::Event) -> &mut Self {
        let state = std::mem::take(&mut self.state);
        self.state = T::apply(state, event);
        self.version += 1;
        self
    }

    pub fn apply_iter<I>(&mut self, events: I) -> &mut Self
    where
        I: IntoIterator<Item = T::Event>,
    {
        let state = std::mem::take(&mut self.state);
        self.state = events.into_iter().fold(state, |acc, event| {
            self.version += 1;
            T::apply(acc, &event)
        });
        self
    }
}

/// Envelope structure for DTO's that may need the Id and Version of an aggregate.  Can be used to
/// encapsulate events or aggregates before serialization
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Envelope<T, U> {
    pub id: T,
    pub version: u64,
    #[serde(flatten)]
    pub data: U,
}

impl<T, U> Envelope<T, U> {
    pub fn into_inner(self) -> U {
        self.data
    }
}

/// From implementation for AggregateRoot for convenience
impl<T: Aggregate> From<AggregateRoot<T>> for Envelope<T::Id, T> {
    fn from(item: AggregateRoot<T>) -> Envelope<T::Id, T> {
        let AggregateRoot {
            id, state, version, ..
        } = item;

        Envelope {
            id,
            version,
            data: state,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fmt::{self, Display};

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

    #[derive(Debug, Eq, PartialEq, Clone, Serialize)]
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

    #[derive(Default, Clone, Debug)]
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

    #[test]
    fn it_conforms_to_aggregate_root() {
        let history = vec![
            Event::Created {
                owner: "armor".to_owned(),
            },
            Event::Credited { value: 10 },
            Event::Credited { value: 10 },
            Event::Credited { value: 10 },
            Event::Debited { value: 5 },
        ];
        let ar_version = history.len() as u64;

        let mut ar = AggregateRoot::<Account>::new("abcd1".to_owned());
        ar.apply_iter(history);

        assert_eq!(*ar.id(), "abcd1".to_owned());
        assert_eq!(ar.version(), ar_version);
        assert_eq!(ar.state().owner, "armor".to_owned());
        assert_eq!(ar.state().balance, 25);
        assert!(!ar.state().suspended);

        ar.handle(Command::Debit { value: 5 })
            .expect("command handler succeeds");
        let changes = ar.take_changes();

        let current_version = ar.version();
        let to_save = changes
            .iter()
            .enumerate()
            .map(|(count, x)| Envelope {
                id: ar.id().clone(),
                version: current_version + (count + 1) as u64,
                data: x.clone(),
            })
            .collect::<Vec<Envelope<_, _>>>();

        assert_eq!(ar.version(), ar_version);
        ar.apply_iter(changes);
        assert_eq!(ar.version(), ar_version + 1);

        assert_eq!(*ar.id(), "abcd1".to_owned());
        assert_eq!(ar.state().owner, "armor".to_owned(),);
        assert_eq!(ar.state().balance, 20);
        assert!(!ar.state().suspended);
        assert_eq!(to_save.len(), 1);

        assert_eq!(
            vec![Envelope {
                id: ar.id().clone(),
                version: current_version + 1,
                data: Event::Debited { value: 5 }
            }],
            to_save
        );
    }
}
