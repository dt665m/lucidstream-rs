use std::collections::VecDeque;

/// An event whose global position can be used by an ordered event chaser.
pub trait SequencedEvent {
    fn sequence(&self) -> i64;
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EventSorterError {
    #[error("a checkpoint cannot be negative: {0}")]
    NegativeCheckpoint(i64),

    #[error("safe head {safe_head} is behind acknowledged checkpoint {checkpoint}")]
    SafeHeadRegression { checkpoint: i64, safe_head: i64 },

    #[error("cannot replace a safe window before the current window is acknowledged")]
    WindowNotDrained,

    #[error("cannot load another page while delivered or buffered events are outstanding")]
    WindowBusy,

    #[error("event sequence {sequence} is not greater than loaded sequence {loaded_through}")]
    EventOutOfOrder { loaded_through: i64, sequence: i64 },

    #[error("event sequence {sequence} is beyond safe head {safe_head}")]
    EventBeyondSafeHead { safe_head: i64, sequence: i64 },

    #[error(
        "safe head {safe_head} was not present in the exhausted event window; last event was {loaded_through}"
    )]
    SafeHeadMissing { safe_head: i64, loaded_through: i64 },

    #[error("event {0} must be acknowledged before another item can be delivered")]
    PendingAcknowledgement(i64),

    #[error("received acknowledgement {actual}, but the pending item is {expected}")]
    UnexpectedAcknowledgement { expected: i64, actual: i64 },

    #[error("received acknowledgement {0} when no item is pending")]
    NothingToAcknowledge(i64),
}

/// State machine that serializes events loaded from certified safe windows.
///
/// It deliberately permits permanent numerical gaps. Ordering is enforced by requiring
/// every loaded event to have a position greater than the preceding loaded position.
#[derive(Debug)]
pub struct EventSorter<E> {
    acknowledged: i64,
    safe_head: i64,
    loaded_through: i64,
    pending: Option<i64>,
    queue: VecDeque<E>,
}

impl<E: SequencedEvent> EventSorter<E> {
    pub fn new(checkpoint: i64) -> Result<Self, EventSorterError> {
        if checkpoint < 0 {
            return Err(EventSorterError::NegativeCheckpoint(checkpoint));
        }

        Ok(Self {
            acknowledged: checkpoint,
            safe_head: checkpoint,
            loaded_through: checkpoint,
            pending: None,
            queue: VecDeque::new(),
        })
    }

    pub fn acknowledged(&self) -> i64 {
        self.acknowledged
    }

    pub fn safe_head(&self) -> i64 {
        self.safe_head
    }

    pub fn loaded_through(&self) -> i64 {
        self.loaded_through
    }

    pub fn pending_sequence(&self) -> Option<i64> {
        self.pending
    }

    pub fn needs_load(&self) -> bool {
        self.pending.is_none() && self.queue.is_empty() && self.loaded_through < self.safe_head
    }

    pub fn is_caught_up(&self) -> bool {
        self.pending.is_none()
            && self.queue.is_empty()
            && self.acknowledged == self.safe_head
            && self.loaded_through == self.safe_head
    }

    /// Installs a head captured while the source was locked against concurrent writers.
    pub fn set_safe_head(&mut self, safe_head: i64) -> Result<(), EventSorterError> {
        if !self.is_caught_up() {
            return Err(EventSorterError::WindowNotDrained);
        }
        if safe_head < self.acknowledged {
            return Err(EventSorterError::SafeHeadRegression {
                checkpoint: self.acknowledged,
                safe_head,
            });
        }

        self.safe_head = safe_head;
        Ok(())
    }

    /// Adds one database page. Events must already be sorted by sequence.
    pub fn push_page(&mut self, events: Vec<E>) -> Result<(), EventSorterError> {
        if self.pending.is_some() || !self.queue.is_empty() {
            return Err(EventSorterError::WindowBusy);
        }

        let mut previous = self.loaded_through;
        for event in &events {
            let sequence = event.sequence();
            if sequence <= previous {
                return Err(EventSorterError::EventOutOfOrder {
                    loaded_through: previous,
                    sequence,
                });
            }
            if sequence > self.safe_head {
                return Err(EventSorterError::EventBeyondSafeHead {
                    safe_head: self.safe_head,
                    sequence,
                });
            }
            previous = sequence;
        }

        self.loaded_through = previous;
        self.queue.extend(events);
        Ok(())
    }

    /// Confirms that an exhausted source page ended at the certified safe head.
    pub fn finish_loading(&self) -> Result<(), EventSorterError> {
        if self.loaded_through != self.safe_head {
            return Err(EventSorterError::SafeHeadMissing {
                safe_head: self.safe_head,
                loaded_through: self.loaded_through,
            });
        }
        Ok(())
    }

    /// Returns one event and requires it to be acknowledged before returning another.
    pub fn next_item(&mut self) -> Result<Option<E>, EventSorterError> {
        if let Some(sequence) = self.pending {
            return Err(EventSorterError::PendingAcknowledgement(sequence));
        }

        if let Some(event) = self.queue.pop_front() {
            self.pending = Some(event.sequence());
            return Ok(Some(event));
        }

        Ok(None)
    }

    /// Records that the projection transaction for the pending item committed successfully.
    pub fn acknowledge(&mut self, sequence: i64) -> Result<(), EventSorterError> {
        let Some(expected) = self.pending else {
            return Err(EventSorterError::NothingToAcknowledge(sequence));
        };
        if expected != sequence {
            return Err(EventSorterError::UnexpectedAcknowledgement {
                expected,
                actual: sequence,
            });
        }

        self.acknowledged = sequence;
        self.pending = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Event(i64);

    impl SequencedEvent for Event {
        fn sequence(&self) -> i64 {
            self.0
        }
    }

    #[test]
    fn permanent_gaps_are_allowed_but_delivery_stays_ordered() {
        let mut sorter = EventSorter::new(40).unwrap();
        sorter.set_safe_head(44).unwrap();
        sorter.push_page(vec![Event(42), Event(44)]).unwrap();

        let first = sorter.next_item().unwrap().unwrap();
        assert_eq!(first, Event(42));
        sorter.acknowledge(first.sequence()).unwrap();

        let second = sorter.next_item().unwrap().unwrap();
        assert_eq!(second, Event(44));
        sorter.acknowledge(second.sequence()).unwrap();

        assert!(sorter.is_caught_up());
        assert!(sorter.next_item().unwrap().is_none());
    }

    #[test]
    fn an_exhausted_window_must_contain_its_safe_head() {
        let mut sorter = EventSorter::<Event>::new(40).unwrap();
        sorter.set_safe_head(42).unwrap();
        assert_eq!(
            sorter.finish_loading(),
            Err(EventSorterError::SafeHeadMissing {
                safe_head: 42,
                loaded_through: 40,
            })
        );
    }

    #[test]
    fn an_item_must_be_acknowledged_before_the_next_delivery() {
        let mut sorter = EventSorter::new(0).unwrap();
        sorter.set_safe_head(2).unwrap();
        sorter.push_page(vec![Event(1), Event(2)]).unwrap();

        let _ = sorter.next_item().unwrap().unwrap();
        assert_eq!(
            sorter.next_item(),
            Err(EventSorterError::PendingAcknowledgement(1))
        );
        assert_eq!(
            sorter.acknowledge(2),
            Err(EventSorterError::UnexpectedAcknowledgement {
                expected: 1,
                actual: 2,
            })
        );
    }

    #[test]
    fn an_out_of_order_page_is_rejected_without_partial_mutation() {
        let mut sorter = EventSorter::new(0).unwrap();
        sorter.set_safe_head(5).unwrap();

        assert_eq!(
            sorter.push_page(vec![Event(2), Event(1)]),
            Err(EventSorterError::EventOutOfOrder {
                loaded_through: 2,
                sequence: 1,
            })
        );
        assert_eq!(sorter.loaded_through(), 0);
        assert!(sorter.next_item().unwrap().is_none());
    }

    #[test]
    fn a_safe_head_cannot_move_behind_the_checkpoint() {
        let mut sorter = EventSorter::<Event>::new(5).unwrap();
        assert_eq!(
            sorter.set_safe_head(4),
            Err(EventSorterError::SafeHeadRegression {
                checkpoint: 5,
                safe_head: 4,
            })
        );
    }
}
