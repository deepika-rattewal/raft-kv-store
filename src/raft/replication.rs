use std::collections::HashMap;

use crate::{LogIndex, NodeId};

/// Tracks log-replication progress for each follower of a Raft leader.
///
/// `next_index` is the next log index that should be sent to a follower.
///
/// `match_index` is the highest log index that the leader knows has been
/// successfully replicated on that follower.
#[derive(Debug, Clone)]
pub struct ReplicationState {
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, Option<LogIndex>>,
}

impl ReplicationState {
    /// Creates an empty replication state.
    pub fn new() -> Self {
        Self {
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }

    /// Registers a follower.
    pub fn add_peer(&mut self, peer: NodeId, next_index: LogIndex) {
        self.next_index.insert(peer, next_index);
        self.match_index.insert(peer, None);
    }

    /// Returns the next log index that should be sent to a follower.
    pub fn next_index(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index.get(&peer).copied()
    }

    /// Returns the highest log index known to be replicated on a follower.
    pub fn match_index(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied().flatten()
    }

    /// Records a successful replication.
    ///
    /// If index N was successfully replicated, the next attempt should
    /// begin at N + 1.
    pub fn advance(&mut self, peer: NodeId, replicated_index: LogIndex) {
        self.match_index.insert(peer, Some(replicated_index));

        self.next_index
            .insert(peer, LogIndex::new(replicated_index.value() + 1));
    }

    /// Moves a follower's next index backwards after a failed replication.
    pub fn backtrack(&mut self, peer: NodeId) -> Option<LogIndex> {
        let current = self.next_index.get(&peer).copied()?;

        if current.value() <= 1 {
            return Some(current);
        }

        let previous = LogIndex::new(current.value() - 1);

        self.next_index.insert(peer, previous);

        Some(previous)
    }

    /// Returns the number of tracked followers.
    pub fn peer_count(&self) -> usize {
        self.next_index.len()
    }

    /// Determines the highest log index known to be replicated on a
    /// majority of the cluster.
    ///
    /// The leader is included automatically because its own log contains
    /// `leader_last_index`.
    pub fn majority_match_index(
        &self,
        cluster_size: usize,
        leader_last_index: Option<LogIndex>,
    ) -> Option<LogIndex> {
        let leader_last_index = leader_last_index?;

        if cluster_size == 0 {
            return None;
        }

        let majority = cluster_size / 2 + 1;

        let mut indexes = Vec::with_capacity(self.match_index.len() + 1);

        // The leader counts as one replicated node.
        indexes.push(leader_last_index);

        // Add followers for which replication has succeeded.
        indexes.extend(self.match_index.values().flatten().copied());

        // We need at least a majority of nodes represented.
        if indexes.len() < majority {
            return None;
        }

        indexes.sort_by_key(|index| index.value());

        // Select the largest index that is held by at least `majority`
        // nodes.
        let majority_position = indexes.len() - majority;

        indexes.get(majority_position).copied()
    }
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_has_no_peers() {
        let state = ReplicationState::new();

        assert_eq!(state.peer_count(), 0);
    }

    #[test]
    fn add_peer_sets_next_index() {
        let mut state = ReplicationState::new();
        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(4));

        assert_eq!(state.next_index(peer), Some(LogIndex::new(4)));
        assert_eq!(state.match_index(peer), None);
        assert_eq!(state.peer_count(), 1);
    }

    #[test]
    fn advance_updates_match_and_next_index() {
        let mut state = ReplicationState::new();
        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(1));

        state.advance(peer, LogIndex::new(4));

        assert_eq!(state.match_index(peer), Some(LogIndex::new(4)));
        assert_eq!(state.next_index(peer), Some(LogIndex::new(5)));
    }

    #[test]
    fn backtrack_decreases_next_index() {
        let mut state = ReplicationState::new();
        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(5));

        assert_eq!(state.backtrack(peer), Some(LogIndex::new(4)));

        assert_eq!(state.next_index(peer), Some(LogIndex::new(4)));
    }

    #[test]
    fn backtrack_does_not_go_below_one() {
        let mut state = ReplicationState::new();
        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(1));

        assert_eq!(state.backtrack(peer), Some(LogIndex::new(1)));

        assert_eq!(state.next_index(peer), Some(LogIndex::new(1)));
    }
    #[test]
    fn backtrack_can_retry_multiple_times() {
        let mut state = ReplicationState::new();
        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(5));

        assert_eq!(state.backtrack(peer), Some(LogIndex::new(4)));
        assert_eq!(state.backtrack(peer), Some(LogIndex::new(3)));
        assert_eq!(state.backtrack(peer), Some(LogIndex::new(2)));

        assert_eq!(state.next_index(peer), Some(LogIndex::new(2)));
    }

    #[test]
    fn unknown_peer_returns_none() {
        let state = ReplicationState::new();

        let peer = NodeId::new(99);

        assert_eq!(state.next_index(peer), None);
        assert_eq!(state.match_index(peer), None);
    }

    #[test]
    fn majority_match_index_requires_majority() {
        let mut state = ReplicationState::new();

        let peer1 = NodeId::new(2);
        let peer2 = NodeId::new(3);

        state.add_peer(peer1, LogIndex::new(1));
        state.add_peer(peer2, LogIndex::new(1));

        // Leader alone is not a majority in a 3-node cluster.
        assert_eq!(state.majority_match_index(3, Some(LogIndex::new(5))), None);

        // Leader + peer1 = 2/3, which is a majority.
        state.advance(peer1, LogIndex::new(5));

        assert_eq!(
            state.majority_match_index(3, Some(LogIndex::new(5))),
            Some(LogIndex::new(5))
        );
    }

    #[test]
    fn majority_match_index_uses_index_reached_by_majority() {
        let mut state = ReplicationState::new();

        let peer1 = NodeId::new(2);
        let peer2 = NodeId::new(3);

        state.add_peer(peer1, LogIndex::new(1));
        state.add_peer(peer2, LogIndex::new(1));

        state.advance(peer1, LogIndex::new(5));
        state.advance(peer2, LogIndex::new(3));

        // All three nodes have at least index 3.
        assert_eq!(
            state.majority_match_index(3, Some(LogIndex::new(5))),
            Some(LogIndex::new(5))
        );
    }

    #[test]
    fn majority_match_index_returns_none_for_empty_leader_log() {
        let state = ReplicationState::new();

        assert_eq!(state.majority_match_index(3, None), None);
    }

    #[test]
    fn majority_match_index_works_for_two_node_cluster() {
        let mut state = ReplicationState::new();

        let peer = NodeId::new(2);

        state.add_peer(peer, LogIndex::new(1));
        state.advance(peer, LogIndex::new(4));

        assert_eq!(
            state.majority_match_index(2, Some(LogIndex::new(4))),
            Some(LogIndex::new(4))
        );
    }
}
