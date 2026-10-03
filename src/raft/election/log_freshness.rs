use crate::{LogIndex, Term};

/// Information about the last entry in a Raft log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    last_index: Option<LogIndex>,
    last_term: Option<Term>,
}

impl LogPosition {
    /// Creates information for a log position.
    pub const fn new(last_index: Option<LogIndex>, last_term: Option<Term>) -> Self {
        Self {
            last_index,
            last_term,
        }
    }

    /// Creates a position representing an empty log.
    pub const fn empty() -> Self {
        Self {
            last_index: None,
            last_term: None,
        }
    }

    /// Returns the last log index.
    pub const fn last_index(&self) -> Option<LogIndex> {
        self.last_index
    }

    /// Returns the term of the last log entry.
    pub const fn last_term(&self) -> Option<Term> {
        self.last_term
    }
}

/// Determines whether a candidate's log is at least as up-to-date
/// as the receiver's log.
///
/// Raft compares the last log term first. If the terms are equal,
/// it compares the last log index.
pub fn is_at_least_as_up_to_date(candidate: LogPosition, receiver: LogPosition) -> bool {
    match (candidate.last_term, receiver.last_term) {
        (None, None) => true,

        (Some(candidate_term), None) => candidate_term > Term::ZERO,

        (None, Some(_)) => false,

        (Some(candidate_term), Some(receiver_term)) => {
            if candidate_term != receiver_term {
                candidate_term > receiver_term
            } else {
                candidate.last_index >= receiver.last_index
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LogPosition, is_at_least_as_up_to_date};
    use crate::{LogIndex, Term};

    #[test]
    fn two_empty_logs_are_equally_up_to_date() {
        assert!(is_at_least_as_up_to_date(
            LogPosition::empty(),
            LogPosition::empty()
        ));
    }

    #[test]
    fn non_empty_log_is_newer_than_empty_log() {
        let candidate = LogPosition::new(Some(LogIndex::new(1)), Some(Term::new(1)));

        assert!(is_at_least_as_up_to_date(candidate, LogPosition::empty()));
    }

    #[test]
    fn empty_log_is_not_newer_than_non_empty_log() {
        let receiver = LogPosition::new(Some(LogIndex::new(1)), Some(Term::new(1)));

        assert!(!is_at_least_as_up_to_date(LogPosition::empty(), receiver));
    }

    #[test]
    fn higher_term_is_more_up_to_date() {
        let candidate = LogPosition::new(Some(LogIndex::new(1)), Some(Term::new(5)));

        let receiver = LogPosition::new(Some(LogIndex::new(100)), Some(Term::new(4)));

        assert!(is_at_least_as_up_to_date(candidate, receiver));
    }

    #[test]
    fn lower_term_is_not_more_up_to_date() {
        let candidate = LogPosition::new(Some(LogIndex::new(100)), Some(Term::new(4)));

        let receiver = LogPosition::new(Some(LogIndex::new(1)), Some(Term::new(5)));

        assert!(!is_at_least_as_up_to_date(candidate, receiver));
    }

    #[test]
    fn equal_terms_use_log_index() {
        let candidate = LogPosition::new(Some(LogIndex::new(10)), Some(Term::new(5)));

        let receiver = LogPosition::new(Some(LogIndex::new(8)), Some(Term::new(5)));

        assert!(is_at_least_as_up_to_date(candidate, receiver));
    }

    #[test]
    fn lower_index_with_equal_term_is_not_up_to_date() {
        let candidate = LogPosition::new(Some(LogIndex::new(8)), Some(Term::new(5)));

        let receiver = LogPosition::new(Some(LogIndex::new(10)), Some(Term::new(5)));

        assert!(!is_at_least_as_up_to_date(candidate, receiver));
    }

    #[test]
    fn equal_term_and_index_are_up_to_date() {
        let candidate = LogPosition::new(Some(LogIndex::new(10)), Some(Term::new(5)));

        let receiver = LogPosition::new(Some(LogIndex::new(10)), Some(Term::new(5)));

        assert!(is_at_least_as_up_to_date(candidate, receiver));
    }

    #[test]
    fn position_accessors_work() {
        let position = LogPosition::new(Some(LogIndex::new(42)), Some(Term::new(7)));

        assert_eq!(position.last_index(), Some(LogIndex::new(42)));
        assert_eq!(position.last_term(), Some(Term::new(7)));
    }
}
