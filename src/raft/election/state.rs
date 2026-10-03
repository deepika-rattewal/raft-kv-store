use std::time::Duration;

use super::{ElectionTimeout, VoteTracker};
use crate::{NodeId, RaftNode, Role, Term};

/// The state of an election currently being conducted by a Raft node.
#[derive(Debug)]
pub struct ElectionState {
    timeout: ElectionTimeout,
    votes: VoteTracker,
}

impl ElectionState {
    /// Creates a new election state.
    pub fn new(cluster_size: usize, election_timeout: Duration) -> Self {
        Self {
            timeout: ElectionTimeout::new(election_timeout),
            votes: VoteTracker::new(cluster_size),
        }
    }

    /// Returns the configured election timeout.
    pub const fn timeout(&self) -> &ElectionTimeout {
        &self.timeout
    }

    /// Returns the number of votes currently recorded.
    pub fn vote_count(&self) -> usize {
        self.votes.vote_count()
    }

    /// Returns the number of votes required for a majority.
    pub const fn majority(&self) -> usize {
        self.votes.majority()
    }

    /// Returns whether this election has received a majority.
    pub fn has_majority(&self) -> bool {
        self.votes.has_majority()
    }

    /// Returns whether the election timeout has expired.
    pub fn is_timeout_expired(&self) -> bool {
        self.timeout.expired()
    }

    /// Advances the election timer.
    pub fn advance_time(&mut self, duration: Duration) {
        self.timeout.advance(duration);
    }

    /// Resets the election timer and vote tracker.
    pub fn reset(&mut self) {
        self.timeout.reset();
        self.votes.reset();
    }

    /// Starts an election for the supplied Raft node.
    ///
    /// The node increments its term, becomes a candidate,
    /// and votes for itself. The self-vote is also recorded
    /// in the vote tracker.
    pub fn start(&mut self, node: &mut RaftNode) {
        node.start_election();

        self.timeout.reset();
        self.votes.reset();

        self.votes.record_vote(node.id());
    }

    /// Records a vote received from another node.
    ///
    /// Returns `true` if this is a new vote and `false` if
    /// that node had already voted in this election.
    pub fn record_vote(&mut self, voter: NodeId) -> bool {
        self.votes.record_vote(voter)
    }

    /// If the election has reached a majority, makes the node leader.
    ///
    /// Returns `true` if the node became leader.
    pub fn try_become_leader(&mut self, node: &mut RaftNode) -> bool {
        if !self.has_majority() {
            return false;
        }

        node.become_leader();
        true
    }

    /// Returns the current term of the node.
    pub const fn current_term(&self, node: &RaftNode) -> Term {
        node.current_term()
    }

    /// Returns whether the node is currently a candidate.
    pub fn is_candidate(&self, node: &RaftNode) -> bool {
        node.role() == Role::Candidate
    }
}

#[cfg(test)]
mod tests {
    use super::ElectionState;
    use crate::{NodeId, RaftNode, Role, Term};
    use std::time::Duration;

    fn election() -> ElectionState {
        ElectionState::new(3, Duration::from_millis(150))
    }

    #[test]
    fn new_election_has_no_votes() {
        let election = election();

        assert_eq!(election.vote_count(), 0);
    }

    #[test]
    fn three_node_cluster_needs_two_votes() {
        let election = election();

        assert_eq!(election.majority(), 2);
    }

    #[test]
    fn election_starts_with_unexpired_timeout() {
        let election = election();

        assert!(!election.is_timeout_expired());
    }

    #[test]
    fn starting_election_makes_node_candidate() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert_eq!(node.role(), Role::Candidate);
    }

    #[test]
    fn starting_election_increments_node_term() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert_eq!(node.current_term(), Term::new(1));
    }

    #[test]
    fn starting_election_records_self_vote() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert_eq!(election.vote_count(), 1);
    }

    #[test]
    fn single_node_election_reaches_majority() {
        let mut election = ElectionState::new(1, Duration::from_millis(150));

        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert!(election.has_majority());
    }

    #[test]
    fn three_node_election_needs_one_additional_vote() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert!(!election.has_majority());

        assert!(election.record_vote(NodeId::new(2)));

        assert!(election.has_majority());
    }

    #[test]
    fn duplicate_vote_is_not_counted_twice() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert!(election.record_vote(NodeId::new(2)));
        assert!(!election.record_vote(NodeId::new(2)));

        assert_eq!(election.vote_count(), 2);
    }

    #[test]
    fn majority_can_make_candidate_leader() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);
        election.record_vote(NodeId::new(2));

        assert!(election.try_become_leader(&mut node));
        assert_eq!(node.role(), Role::Leader);
    }

    #[test]
    fn candidate_without_majority_does_not_become_leader() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert!(!election.try_become_leader(&mut node));
        assert_eq!(node.role(), Role::Candidate);
    }

    #[test]
    fn election_timeout_can_be_advanced() {
        let mut election = election();

        election.advance_time(Duration::from_millis(100));

        assert!(!election.is_timeout_expired());

        election.advance_time(Duration::from_millis(50));

        assert!(election.is_timeout_expired());
    }

    #[test]
    fn reset_clears_timer_and_votes() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);
        election.record_vote(NodeId::new(2));
        election.advance_time(Duration::from_millis(150));

        election.reset();

        assert_eq!(election.vote_count(), 0);
        assert!(!election.is_timeout_expired());
    }

    #[test]
    fn current_term_matches_node_term() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert_eq!(election.current_term(&node), Term::new(1));
    }

    #[test]
    fn is_candidate_reports_node_role() {
        let mut election = election();
        let mut node = RaftNode::new(NodeId::new(1));

        election.start(&mut node);

        assert!(election.is_candidate(&node));

        node.become_leader();

        assert!(!election.is_candidate(&node));
    }
}
