use serde::{Deserialize, Serialize};

use crate::Term;

/// Response sent by a follower after receiving an AppendEntries RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    term: Term,
    success: bool,
}

impl AppendEntriesResponse {
    /// Creates a successful AppendEntries response.
    pub const fn success(term: Term) -> Self {
        Self {
            term,
            success: true,
        }
    }

    /// Creates a failed AppendEntries response.
    pub const fn failure(term: Term) -> Self {
        Self {
            term,
            success: false,
        }
    }

    /// Returns the responder's current Raft term.
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Returns whether the AppendEntries request succeeded.
    pub const fn is_success(&self) -> bool {
        self.success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

    #[test]
    fn successful_response_stores_term() {
        let term = Term::new(5);
        let response = AppendEntriesResponse::success(term);

        assert_eq!(response.term(), term);
    }

    #[test]
    fn successful_response_reports_success() {
        let response = AppendEntriesResponse::success(Term::new(3));

        assert!(response.is_success());
    }

    #[test]
    fn failure_response_stores_term() {
        let term = Term::new(7);
        let response = AppendEntriesResponse::failure(term);

        assert_eq!(response.term(), term);
    }

    #[test]
    fn failure_response_reports_failure() {
        let response = AppendEntriesResponse::failure(Term::new(4));

        assert!(!response.is_success());
    }
}
