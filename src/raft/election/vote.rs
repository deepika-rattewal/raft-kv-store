use std::collections::HashSet;

use crate::NodeId;

/// Tracks votes received by a Raft candidate during an election.
///
/// The tracker prevents the same node from being counted more than
/// once and knows when the candidate has reached a majority.
#[derive(Debug, Clone)]
pub struct VoteTracker {
    /// Total number of nodes in the cluster.
    cluster_size: usize,

    /// IDs of nodes that have voted for this candidate.
    votes: HashSet<NodeId>,
}

impl VoteTracker {
    /// Creates a new vote tracker for a cluster.
    ///
    /// # Panics
    ///
    /// Panics if `cluster_size` is zero because a Raft cluster
    /// must contain at least one node.
    pub fn new(cluster_size: usize) -> Self {
        assert!(cluster_size > 0, "cluster size must be greater than zero");

        Self {
            cluster_size,
            votes: HashSet::new(),
        }
    }

    /// Returns the total number of nodes in the cluster.
    pub const fn cluster_size(&self) -> usize {
        self.cluster_size
    }

    /// Returns the number of votes currently recorded.
    pub fn vote_count(&self) -> usize {
        self.votes.len()
    }

    /// Returns the number of votes required for a majority.
    ///
    /// For example:
    ///
    /// - 1 node → 1 vote
    /// - 2 nodes → 2 votes
    /// - 3 nodes → 2 votes
    /// - 4 nodes → 3 votes
    /// - 5 nodes → 3 votes
    pub const fn majority(&self) -> usize {
        self.cluster_size / 2 + 1
    }

    /// Records a vote from a node.
    ///
    /// Returns `true` if the vote was newly recorded.
    ///
    /// Returns `false` if the same node had already voted.
    pub fn record_vote(&mut self, voter: NodeId) -> bool {
        self.votes.insert(voter)
    }

    /// Returns `true` if this candidate has received a majority.
    pub fn has_majority(&self) -> bool {
        self.vote_count() >= self.majority()
    }

    /// Returns `true` if a specific node has already voted.
    pub fn has_voted(&self, voter: NodeId) -> bool {
        self.votes.contains(&voter)
    }

    /// Returns an iterator over all nodes that have voted.
    pub fn voters(&self) -> impl Iterator<Item = &NodeId> {
        self.votes.iter()
    }

    /// Clears all recorded votes.
    ///
    /// This is useful when starting a completely new election.
    pub fn reset(&mut self) {
        self.votes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeId, VoteTracker};

    #[test]
    fn tracker_stores_cluster_size() {
        let tracker = VoteTracker::new(3);

        assert_eq!(tracker.cluster_size(), 3);
    }

    #[test]
    fn majority_for_three_nodes_is_two() {
        let tracker = VoteTracker::new(3);

        assert_eq!(tracker.majority(), 2);
    }

    #[test]
    fn majority_for_five_nodes_is_three() {
        let tracker = VoteTracker::new(5);

        assert_eq!(tracker.majority(), 3);
    }

    #[test]
    fn single_node_cluster_requires_one_vote() {
        let tracker = VoteTracker::new(1);

        assert_eq!(tracker.majority(), 1);
    }

    #[test]
    fn new_tracker_has_zero_votes() {
        let tracker = VoteTracker::new(3);

        assert_eq!(tracker.vote_count(), 0);
        assert!(!tracker.has_majority());
    }

    #[test]
    fn recording_vote_increases_count() {
        let mut tracker = VoteTracker::new(3);

        let recorded = tracker.record_vote(NodeId::new(1));

        assert!(recorded);
        assert_eq!(tracker.vote_count(), 1);
    }

    #[test]
    fn duplicate_vote_is_not_counted_twice() {
        let mut tracker = VoteTracker::new(3);

        assert!(tracker.record_vote(NodeId::new(1)));
        assert!(!tracker.record_vote(NodeId::new(1)));

        assert_eq!(tracker.vote_count(), 1);
    }

    #[test]
    fn majority_is_detected() {
        let mut tracker = VoteTracker::new(3);

        tracker.record_vote(NodeId::new(1));
        assert!(!tracker.has_majority());

        tracker.record_vote(NodeId::new(2));
        assert!(tracker.has_majority());
    }

    #[test]
    fn five_node_cluster_requires_three_votes() {
        let mut tracker = VoteTracker::new(5);

        tracker.record_vote(NodeId::new(1));
        tracker.record_vote(NodeId::new(2));

        assert!(!tracker.has_majority());

        tracker.record_vote(NodeId::new(3));

        assert!(tracker.has_majority());
    }

    #[test]
    fn has_voted_detects_existing_voter() {
        let mut tracker = VoteTracker::new(3);

        tracker.record_vote(NodeId::new(2));

        assert!(tracker.has_voted(NodeId::new(2)));
        assert!(!tracker.has_voted(NodeId::new(3)));
    }

    #[test]
    fn reset_removes_all_votes() {
        let mut tracker = VoteTracker::new(3);

        tracker.record_vote(NodeId::new(1));
        tracker.record_vote(NodeId::new(2));

        assert!(tracker.has_majority());

        tracker.reset();

        assert_eq!(tracker.vote_count(), 0);
        assert!(!tracker.has_majority());
    }

    #[test]
    #[should_panic(expected = "cluster size must be greater than zero")]
    fn zero_node_cluster_is_rejected() {
        VoteTracker::new(0);
    }
}
