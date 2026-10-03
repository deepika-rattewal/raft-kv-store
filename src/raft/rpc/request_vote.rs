use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

/// A candidate's request for votes during a Raft election.
///
/// A candidate sends this message to other nodes when its
/// election timeout expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVote {
    /// Candidate's current term.
    pub term: Term,

    /// ID of the candidate requesting the vote.
    pub candidate_id: NodeId,

    /// Index of the candidate's last log entry.
    ///
    /// `None` means the candidate's log is empty.
    pub last_log_index: Option<LogIndex>,

    /// Term of the candidate's last log entry.
    ///
    /// `None` means the candidate's log is empty.
    pub last_log_term: Option<Term>,
}

impl RequestVote {
    /// Creates a new RequestVote message.
    pub const fn new(
        term: Term,
        candidate_id: NodeId,
        last_log_index: Option<LogIndex>,
        last_log_term: Option<Term>,
    ) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }

    /// Creates a RequestVote message for a candidate whose log is empty.
    pub const fn for_empty_log(term: Term, candidate_id: NodeId) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index: None,
            last_log_term: None,
        }
    }

    /// Returns the candidate's last log index.
    pub const fn last_log_index(&self) -> Option<LogIndex> {
        self.last_log_index
    }

    /// Returns the candidate's last log term.
    pub const fn last_log_term(&self) -> Option<Term> {
        self.last_log_term
    }
}

#[cfg(test)]
mod tests {
    use super::{LogIndex, NodeId, RequestVote, Term};

    #[test]
    fn request_vote_stores_term() {
        let request = RequestVote::new(Term::new(5), NodeId::new(1), None, None);

        assert_eq!(request.term, Term::new(5));
    }

    #[test]
    fn request_vote_stores_candidate_id() {
        let request = RequestVote::new(Term::new(5), NodeId::new(7), None, None);

        assert_eq!(request.candidate_id, NodeId::new(7));
    }

    #[test]
    fn request_vote_stores_last_log_index() {
        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(1),
            Some(LogIndex::new(42)),
            Some(Term::new(4)),
        );

        assert_eq!(request.last_log_index(), Some(LogIndex::new(42)));
    }

    #[test]
    fn request_vote_stores_last_log_term() {
        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(1),
            Some(LogIndex::new(42)),
            Some(Term::new(4)),
        );

        assert_eq!(request.last_log_term(), Some(Term::new(4)));
    }

    #[test]
    fn empty_log_request_has_no_log_information() {
        let request = RequestVote::for_empty_log(Term::new(2), NodeId::new(3));

        assert_eq!(request.last_log_index(), None);
        assert_eq!(request.last_log_term(), None);
    }

    #[test]
    fn request_vote_preserves_all_fields() {
        let request = RequestVote::new(
            Term::new(10),
            NodeId::new(5),
            Some(LogIndex::new(100)),
            Some(Term::new(9)),
        );

        assert_eq!(request.term, Term::new(10));
        assert_eq!(request.candidate_id, NodeId::new(5));
        assert_eq!(request.last_log_index(), Some(LogIndex::new(100)));
        assert_eq!(request.last_log_term(), Some(Term::new(9)));
    }
}
