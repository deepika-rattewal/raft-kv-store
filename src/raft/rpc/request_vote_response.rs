use crate::Term;

use serde::{Deserialize, Serialize};

/// A Raft node's response to a `RequestVote` message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The responder's current term.
    ///
    /// A candidate uses this to discover if another node
    /// has a newer term.
    pub term: Term,

    /// Whether the responder granted its vote.
    pub vote_granted: bool,
}

impl RequestVoteResponse {
    /// Creates a new vote response.
    pub const fn new(term: Term, vote_granted: bool) -> Self {
        Self { term, vote_granted }
    }

    /// Creates a response indicating that the vote was granted.
    pub const fn granted(term: Term) -> Self {
        Self {
            term,
            vote_granted: true,
        }
    }

    /// Creates a response indicating that the vote was rejected.
    pub const fn rejected(term: Term) -> Self {
        Self {
            term,
            vote_granted: false,
        }
    }

    /// Returns the responder's current term.
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Returns whether the vote was granted.
    pub const fn vote_granted(&self) -> bool {
        self.vote_granted
    }
}

#[cfg(test)]
mod tests {
    use super::{RequestVoteResponse, Term};

    #[test]
    fn response_stores_term() {
        let response = RequestVoteResponse::new(Term::new(5), true);

        assert_eq!(response.term(), Term::new(5));
    }

    #[test]
    fn response_stores_vote_granted() {
        let response = RequestVoteResponse::new(Term::new(5), true);

        assert!(response.vote_granted());
    }

    #[test]
    fn response_stores_vote_rejected() {
        let response = RequestVoteResponse::new(Term::new(5), false);

        assert!(!response.vote_granted());
    }

    #[test]
    fn granted_helper_creates_granted_response() {
        let response = RequestVoteResponse::granted(Term::new(3));

        assert_eq!(response.term(), Term::new(3));
        assert!(response.vote_granted());
    }

    #[test]
    fn rejected_helper_creates_rejected_response() {
        let response = RequestVoteResponse::rejected(Term::new(7));

        assert_eq!(response.term(), Term::new(7));
        assert!(!response.vote_granted());
    }

    #[test]
    fn response_preserves_all_fields() {
        let response = RequestVoteResponse::new(Term::new(10), true);

        assert_eq!(response.term(), Term::new(10));
        assert!(response.vote_granted());
    }
}
