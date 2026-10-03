use super::log_freshness::{LogPosition, is_at_least_as_up_to_date};
use crate::{NodeId, Term};

/// The result of evaluating a RequestVote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteDecision {
    /// The vote can be granted.
    Granted,

    /// The candidate's term is older than the receiver's term.
    RejectedOldTerm,

    /// The receiver has already voted for another candidate.
    RejectedAlreadyVoted,

    /// The candidate's log is not sufficiently up-to-date.
    RejectedOutdatedLog,
}

/// Determines whether a node should grant its vote.
///
/// This function contains the core voting rules from the Raft
/// election protocol.
pub fn decide_vote(
    receiver_term: Term,
    voted_for: Option<NodeId>,
    candidate_id: NodeId,
    candidate_term: Term,
    candidate_log: LogPosition,
    receiver_log: LogPosition,
) -> VoteDecision {
    // A candidate from an older term cannot receive a vote.
    if candidate_term < receiver_term {
        return VoteDecision::RejectedOldTerm;
    }

    // If we already voted for a different candidate,
    // this candidate cannot receive our vote.
    if let Some(previous_candidate) = voted_for
        && previous_candidate != candidate_id
    {
        return VoteDecision::RejectedAlreadyVoted;
    }

    // The candidate's log must be at least as up-to-date
    // as our own log.
    if !is_at_least_as_up_to_date(candidate_log, receiver_log) {
        return VoteDecision::RejectedOutdatedLog;
    }

    VoteDecision::Granted
}

#[cfg(test)]
mod tests {
    use super::{VoteDecision, decide_vote};
    use crate::{LogIndex, LogPosition, NodeId, Term};

    fn empty_log() -> LogPosition {
        LogPosition::empty()
    }

    fn log(index: u64, term: u64) -> LogPosition {
        LogPosition::new(Some(LogIndex::new(index)), Some(Term::new(term)))
    }

    #[test]
    fn grants_vote_for_valid_candidate() {
        let decision = decide_vote(
            Term::new(1),
            None,
            NodeId::new(2),
            Term::new(1),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }

    #[test]
    fn rejects_candidate_from_old_term() {
        let decision = decide_vote(
            Term::new(5),
            None,
            NodeId::new(2),
            Term::new(4),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::RejectedOldTerm);
    }

    #[test]
    fn rejects_different_candidate_after_voting() {
        let decision = decide_vote(
            Term::new(5),
            Some(NodeId::new(1)),
            NodeId::new(2),
            Term::new(5),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::RejectedAlreadyVoted);
    }

    #[test]
    fn allows_same_candidate_to_receive_vote_again() {
        let decision = decide_vote(
            Term::new(5),
            Some(NodeId::new(2)),
            NodeId::new(2),
            Term::new(5),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }

    #[test]
    fn rejects_candidate_with_outdated_log() {
        let decision = decide_vote(
            Term::new(5),
            None,
            NodeId::new(2),
            Term::new(5),
            log(5, 4),
            log(10, 5),
        );

        assert_eq!(decision, VoteDecision::RejectedOutdatedLog);
    }

    #[test]
    fn accepts_candidate_with_newer_log_term() {
        let decision = decide_vote(
            Term::new(5),
            None,
            NodeId::new(2),
            Term::new(5),
            log(2, 6),
            log(100, 5),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }

    #[test]
    fn accepts_candidate_with_same_term_and_newer_index() {
        let decision = decide_vote(
            Term::new(5),
            None,
            NodeId::new(2),
            Term::new(5),
            log(10, 5),
            log(8, 5),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }

    #[test]
    fn rejects_candidate_with_same_term_and_older_index() {
        let decision = decide_vote(
            Term::new(5),
            None,
            NodeId::new(2),
            Term::new(5),
            log(8, 5),
            log(10, 5),
        );

        assert_eq!(decision, VoteDecision::RejectedOutdatedLog);
    }

    #[test]
    fn candidate_with_newer_term_can_receive_vote() {
        let decision = decide_vote(
            Term::new(4),
            None,
            NodeId::new(2),
            Term::new(5),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }

    #[test]
    fn previous_vote_for_same_candidate_does_not_block_vote() {
        let candidate = NodeId::new(7);

        let decision = decide_vote(
            Term::new(3),
            Some(candidate),
            candidate,
            Term::new(3),
            empty_log(),
            empty_log(),
        );

        assert_eq!(decision, VoteDecision::Granted);
    }
}
