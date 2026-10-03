use super::{
    AppendEntries, AppendEntriesResponse, LogEntry, LogIndex, LogPosition, NodeId, RaftLog,
    ReplicationState, RequestVote, RequestVoteResponse, Role, Term, VoteDecision, decide_vote,
};
use crate::KvStateMachine;
use crate::network::transport::NodeTransport;
/// The current state of a Raft server.
///
/// This structure contains the core logical state needed by a
/// Raft node. Networking, timers, persistence, and log replication
/// will be added incrementally.
#[derive(Debug)]
pub struct RaftNode {
    id: NodeId,
    current_term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    log: RaftLog,
    commit_index: Option<LogIndex>,
    last_applied: Option<LogIndex>,
    state_machine: KvStateMachine,
}

impl RaftNode {
    pub fn build_append_entries(
        &self,
        peer: NodeId,
        replication: &ReplicationState,
    ) -> Option<AppendEntries> {
        let next_index = replication.next_index(peer)?;

        let prev_log_index = if next_index.value() <= 1 {
            None
        } else {
            Some(LogIndex::new(next_index.value() - 1))
        };

        let prev_log_term =
            prev_log_index.and_then(|index| self.log.get(index).map(LogEntry::term));

        let entries_start = LogIndex::new(next_index.value().saturating_sub(1));
        let entries = self.log.entries_from(entries_start);

        Some(AppendEntries::new(
            self.current_term,
            self.id,
            prev_log_index,
            prev_log_term,
            entries,
            self.commit_index,
        ))
    }

    pub fn advance_commit_index(
        &mut self,
        replication: &ReplicationState,
        cluster_size: usize,
    ) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return self.commit_index;
        }

        let Some(candidate) = replication.majority_match_index(cluster_size, self.log.last_index())
        else {
            return self.commit_index;
        };

        if let Some(current_commit) = self.commit_index
            && candidate <= current_commit
        {
            return self.commit_index;
        }

        // `RaftLog::get()` uses the project's one-based Raft indexing.
        // Do not use `term_at()` here because `term_at()` currently follows
        // the legacy `entry_at()` indexing behavior used by existing tests.
        let Some(candidate_term) = self.log.get(candidate).map(LogEntry::term) else {
            return self.commit_index;
        };

        if candidate_term != self.current_term {
            return self.commit_index;
        }

        self.commit_index = Some(candidate);

        self.commit_index
    }
    pub fn apply_committed_entries(&mut self) -> Option<LogIndex> {
        let commit_index = self.commit_index?;

        let mut next_index = match self.last_applied {
            Some(index) => index.value().checked_add(1)?,
            None => 1,
        };

        while next_index <= commit_index.value() {
            let index = LogIndex::new(next_index);

            let Some(entry) = self.log.get(index) else {
                break;
            };

            let command = entry.command().clone();

            self.state_machine.apply(command);

            self.last_applied = Some(index);

            next_index = next_index.checked_add(1)?;
        }

        self.last_applied
    }

    pub fn advance_and_apply_committed_entries(
        &mut self,
        replication: &ReplicationState,
        cluster_size: usize,
    ) -> Option<LogIndex> {
        self.advance_commit_index(replication, cluster_size);
        self.apply_committed_entries()
    }
    pub fn handle_append_entries_response(
        &mut self,
        peer: NodeId,
        replicated_index: LogIndex,
        response: AppendEntriesResponse,
        replication: &mut ReplicationState,
    ) -> bool {
        // A response from a newer term means this node is stale.
        // It must update its term and step down to follower.
        if response.term() > self.current_term {
            self.current_term = response.term();
            self.voted_for = None;
            self.role = Role::Follower;
            return false;
        }

        // Only the leader should process successful replication responses.
        if self.role != Role::Leader {
            return false;
        }

        if response.is_success() {
            // The follower successfully replicated the entry.
            replication.advance(peer, replicated_index);
        } else {
            // The follower rejected the append.
            // Move next_index backwards so the leader can retry.
            replication.backtrack(peer);
        }

        true
    }
    /// Creates a new Raft node with an empty log.
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            current_term: Term::ZERO,
            voted_for: None,
            role: Role::Follower,
            log: RaftLog::new(),
            commit_index: None,
            last_applied: None,
            state_machine: KvStateMachine::new(),
        }
    }
    fn update_commit_index(&mut self, leader_commit: Option<LogIndex>) {
        let Some(leader_commit) = leader_commit else {
            return;
        };

        let Some(last_index) = self.log.last_index() else {
            return;
        };

        let new_commit_index = if leader_commit < last_index {
            leader_commit
        } else {
            last_index
        };

        if self
            .commit_index
            .is_none_or(|current| new_commit_index > current)
        {
            self.commit_index = Some(new_commit_index);
        }
    }

    /// Applies all committed but not-yet-applied log entries to the state machine.
    /// Raft requires every node to apply committed entries in log order.
    /// `last_applied` records the highest entry that has already been applied.
    /// Starts a new election.
    /// The node increments its term, becomes a candidate,
    /// and votes for itself.
    pub fn start_election(&mut self) {
        self.current_term = self.current_term.next();
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
    }
    /// Builds a RequestVote message using this node's current
    /// election term and latest log position.
    ///
    /// This message is sent to other nodes when this node
    /// becomes a candidate.
    pub fn build_request_vote(&self) -> RequestVote {
        RequestVote::new(
            self.current_term,
            self.id,
            self.log.last_index(),
            self.log.last_term(),
        )
    }

    pub async fn send_request_vote(
        &self,
        transport: &NodeTransport,
        peer: NodeId,
    ) -> std::io::Result<()> {
        let request = self.build_request_vote();

        transport.send_request_vote(peer, self.id, request).await
    }

    pub async fn send_append_entries(
        &self,
        transport: &NodeTransport,
        peer: NodeId,
        replication: &ReplicationState,
    ) -> std::io::Result<()> {
        if let Some(request) = self.build_append_entries(peer, replication) {
            transport
                .send_append_entries(peer, self.id, request)
                .await?;
        }

        Ok(())
    }

    pub async fn send_request_vote_response(
        &self,
        transport: &NodeTransport,
        peer: NodeId,
        response: crate::raft::rpc::RequestVoteResponse,
    ) -> std::io::Result<()> {
        transport
            .send_request_vote_response(peer, self.id, response)
            .await
    }

    pub async fn send_append_entries_response(
        &self,
        transport: &NodeTransport,
        peer: NodeId,
        response: crate::raft::rpc::AppendEntriesResponse,
    ) -> std::io::Result<()> {
        transport
            .send_append_entries_response(peer, self.id, response)
            .await
    }
    pub fn build_heartbeat(&self) -> AppendEntries {
        AppendEntries::heartbeat(
            self.current_term,
            self.id,
            self.log.last_index(),
            self.log.last_term(),
            self.commit_index,
        )
    }

    pub fn handle_append_entries(&mut self, request: AppendEntries) -> AppendEntriesResponse {
        if request.term < self.current_term {
            return AppendEntriesResponse::failure(self.current_term);
        }

        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None;
        }

        self.role = Role::Follower;

        if let Some(prev_log_index) = request.prev_log_index {
            let Some(local_term) = self.log.get(prev_log_index).map(LogEntry::term) else {
                return AppendEntriesResponse::failure(self.current_term);
            };

            if request.prev_log_term != Some(local_term) {
                return AppendEntriesResponse::failure(self.current_term);
            }
        }

        let replace_index = request
            .prev_log_index
            .map(|index| LogIndex::new(index.value().saturating_sub(1)));

        self.log.replace_suffix(replace_index, &request.entries);

        self.update_commit_index(request.leader_commit);
        self.apply_committed_entries();

        AppendEntriesResponse::success(self.current_term)
    }

    pub fn handle_append_entries_message(
        &mut self,
        request: crate::raft::rpc::AppendEntries,
    ) -> crate::raft::rpc::AppendEntriesResponse {
        self.handle_append_entries(request)
    }
    /// Makes this node the leader.
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
    }

    pub fn handle_append_entries_response_message(
        &mut self,
        peer: NodeId,
        replicated_index: LogIndex,
        response: crate::raft::rpc::AppendEntriesResponse,
        replication: &mut ReplicationState,
    ) {
        self.handle_append_entries_response(peer, replicated_index, response, replication);
    }
    pub fn handle_request_vote_response_message(
        &mut self,
        peer: NodeId,
        response: crate::raft::rpc::RequestVoteResponse,
        election: &mut crate::raft::election::state::ElectionState,
    ) -> bool {
        if response.term() > self.current_term() {
            self.become_follower(response.term());
            election.reset();
            return false;
        }

        if !response.vote_granted() {
            return false;
        }

        if response.term() < self.current_term() {
            return false;
        }

        election.record_vote(peer);

        election.try_become_leader(self)
    }

    /// Makes this node a follower.
    ///
    /// If the supplied term is newer than the node's current term,
    /// the node updates its term and clears its previous vote.
    pub fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }

        self.role = Role::Follower;
    }

    /// Handles an incoming RequestVote message.
    ///
    /// The node checks:
    /// - the candidate's term,
    /// - whether it has already voted for another candidate,
    /// - and whether the candidate's log is sufficiently up-to-date.
    pub fn handle_request_vote(
        &mut self,
        request: RequestVote,
        receiver_log: LogPosition,
    ) -> RequestVoteResponse {
        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None;
            self.role = Role::Follower;
        }

        let candidate_log = LogPosition::new(request.last_log_index, request.last_log_term);

        let decision = decide_vote(
            self.current_term,
            self.voted_for,
            request.candidate_id,
            request.term,
            candidate_log,
            receiver_log,
        );

        match decision {
            VoteDecision::Granted => {
                self.voted_for = Some(request.candidate_id);

                RequestVoteResponse::granted(self.current_term)
            }

            VoteDecision::RejectedOldTerm
            | VoteDecision::RejectedAlreadyVoted
            | VoteDecision::RejectedOutdatedLog => RequestVoteResponse::rejected(self.current_term),
        }
    }

    pub fn handle_request_vote_message(
        &mut self,
        request: crate::raft::rpc::RequestVote,
    ) -> crate::raft::rpc::RequestVoteResponse {
        let receiver_log = crate::raft::election::log_freshness::LogPosition::new(
            self.log.last_index(),
            self.log.last_term(),
        );

        self.handle_request_vote(request, receiver_log)
    }

    /// Appends an entry to this node's local Raft log.
    pub fn append_log_entry(&mut self, entry: LogEntry) {
        self.log.append(entry);
    }

    /// Returns a reference to this node's Raft log.
    pub const fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Returns a mutable reference to this node's Raft log.
    pub fn log_mut(&mut self) -> &mut RaftLog {
        &mut self.log
    }

    /// Returns the position of the last entry in this node's log.
    pub fn log_position(&self) -> LogPosition {
        LogPosition::new(self.log.last_index(), self.log.last_term())
    }

    /// Returns this node's ID.
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Returns the node's current term.
    pub const fn current_term(&self) -> Term {
        self.current_term
    }

    /// Returns the candidate this node voted for in the current term.
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the node's current role.
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Returns the highest log index known to be committed.
    pub const fn commit_index(&self) -> Option<LogIndex> {
        self.commit_index
    }

    /// Returns the highest log index that has been applied
    /// to the state machine.
    pub const fn last_applied(&self) -> Option<LogIndex> {
        self.last_applied
    }

    pub const fn state_machine(&self) -> &KvStateMachine {
        &self.state_machine
    }

    pub fn state_machine_mut(&mut self) -> &mut KvStateMachine {
        &mut self.state_machine
    }
    pub fn handle_network_message(
        &mut self,
        message: crate::network::message::NetworkMessage,
        peer: NodeId,
        replicated_index: LogIndex,
        election: &mut crate::raft::election::state::ElectionState,
        replication: &mut ReplicationState,
    ) -> Option<crate::network::message::NetworkMessage> {
        match message {
            crate::network::message::NetworkMessage::RequestVote(request) => {
                let response = self.handle_request_vote_message(request);

                Some(crate::network::message::NetworkMessage::RequestVoteResponse(response))
            }

            crate::network::message::NetworkMessage::RequestVoteResponse(response) => {
                self.handle_request_vote_response_message(peer, response, election);

                None
            }

            crate::network::message::NetworkMessage::AppendEntries(request) => {
                let response = self.handle_append_entries_message(request);

                Some(crate::network::message::NetworkMessage::AppendEntriesResponse(response))
            }

            crate::network::message::NetworkMessage::AppendEntriesResponse(response) => {
                self.handle_append_entries_response_message(
                    peer,
                    replicated_index,
                    response,
                    replication,
                );

                None
            }
        }
    }
    pub fn handle_network_envelope(
        &mut self,
        envelope: crate::network::message::NetworkEnvelope,
        replicated_index: LogIndex,
        election: &mut crate::raft::election::state::ElectionState,
        replication: &mut ReplicationState,
    ) -> Option<crate::network::message::NetworkEnvelope> {
        let peer = envelope.sender();

        self.handle_network_message(
            envelope.into_message(),
            peer,
            replicated_index,
            election,
            replication,
        )
        .map(|message| crate::network::message::NetworkEnvelope::new(self.id, message))
    }

    pub async fn receive_network_message(
        &mut self,
        connection: &mut crate::network::server::NetworkConnection,
        replicated_index: LogIndex,
        election: &mut crate::raft::election::state::ElectionState,
        replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        let envelope = connection.receive_envelope().await?;

        if let Some(response) =
            self.handle_network_envelope(envelope, replicated_index, election, replication)
        {
            connection.send_envelope(&response).await?;
        }

        Ok(())
    }

    pub async fn run_server_messages(
        &mut self,
        server: &crate::network::server::NetworkServer,
        message_count: usize,
        replicated_index: LogIndex,
        election: &mut crate::raft::election::state::ElectionState,
        replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        server
            .process_messages(self, message_count, replicated_index, election, replication)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::RaftNode;
    use crate::network::transport::NodeTransport;
    use crate::{
        AppendEntries, AppendEntriesResponse, KvCommand, LogEntry, LogIndex, LogPosition, NodeId,
        ReplicationState, RequestVote, Role, Term,
    };

    #[test]
    fn new_node_has_correct_id() {
        let node = RaftNode::new(NodeId::new(7));

        assert_eq!(node.id(), NodeId::new(7));
    }

    #[test]
    fn new_node_starts_at_term_zero() {
        let node = RaftNode::new(NodeId::new(1));

        assert_eq!(node.current_term(), Term::ZERO);
    }

    #[test]
    fn new_node_starts_as_follower() {
        let node = RaftNode::new(NodeId::new(1));

        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn new_node_has_no_vote() {
        let node = RaftNode::new(NodeId::new(1));

        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn new_node_has_empty_log() {
        let node = RaftNode::new(NodeId::new(1));

        assert!(node.log().is_empty());
    }

    #[test]
    fn new_node_has_no_commit_index() {
        let node = RaftNode::new(NodeId::new(1));

        assert_eq!(node.commit_index(), None);
    }

    #[test]
    fn new_node_has_no_last_applied_index() {
        let node = RaftNode::new(NodeId::new(1));

        assert_eq!(node.last_applied(), None);
    }

    #[test]
    fn new_node_has_consistent_initial_state() {
        let node = RaftNode::new(NodeId::new(42));

        assert_eq!(node.id(), NodeId::new(42));
        assert_eq!(node.current_term(), Term::ZERO);
        assert_eq!(node.voted_for(), None);
        assert_eq!(node.role(), Role::Follower);
        assert!(node.log().is_empty());
        assert_eq!(node.commit_index(), None);
        assert_eq!(node.last_applied(), None);
    }

    #[test]
    fn start_election_increments_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        assert_eq!(node.current_term(), Term::new(1));
    }

    #[test]
    fn start_election_makes_node_candidate() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        assert_eq!(node.role(), Role::Candidate);
    }

    #[test]
    fn start_election_votes_for_self() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        assert_eq!(node.voted_for(), Some(NodeId::new(1)));
    }

    #[test]
    fn multiple_elections_increment_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.start_election();
        node.start_election();

        assert_eq!(node.current_term(), Term::new(3));
    }

    #[test]
    fn candidate_can_become_leader() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        assert_eq!(node.role(), Role::Leader);
    }

    #[test]
    fn newer_term_makes_node_follower() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        node.become_follower(Term::new(5));

        assert_eq!(node.current_term(), Term::new(5));
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn newer_term_clears_previous_vote() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        assert_eq!(node.voted_for(), Some(NodeId::new(1)));

        node.become_follower(Term::new(5));

        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn same_term_preserves_vote() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_follower(Term::new(1));

        assert_eq!(node.current_term(), Term::new(1));
        assert_eq!(node.voted_for(), Some(NodeId::new(1)));
    }

    #[test]
    fn older_term_does_not_change_current_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_follower(Term::new(0));

        assert_eq!(node.current_term(), Term::new(1));
    }

    #[test]
    fn appending_entry_adds_to_node_log() {
        let mut node = RaftNode::new(NodeId::new(1));

        let entry = LogEntry::new(Term::new(1), KvCommand::put("name", "Div"));

        node.append_log_entry(entry);

        assert_eq!(node.log().len(), 1);
    }

    #[test]
    fn node_log_position_starts_empty() {
        let node = RaftNode::new(NodeId::new(1));

        let position = node.log_position();

        assert_eq!(position.last_index(), None);
        assert_eq!(position.last_term(), None);
    }

    #[test]
    fn node_log_position_tracks_last_entry() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(3), KvCommand::put("name", "Div")));

        node.append_log_entry(LogEntry::new(
            Term::new(5),
            KvCommand::put("language", "Rust"),
        ));

        let position = node.log_position();

        assert_eq!(position.last_index(), Some(LogIndex::new(2)));
        assert_eq!(position.last_term(), Some(Term::new(5)));
    }

    #[test]
    fn grants_vote_to_valid_candidate() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(2));

        let response = node.handle_request_vote(request, node.log_position());

        assert!(response.vote_granted());
        assert_eq!(response.term(), Term::new(1));
        assert_eq!(node.voted_for(), Some(NodeId::new(2)));
    }

    #[test]
    fn rejects_candidate_from_old_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.become_follower(Term::new(5));

        let request = RequestVote::for_empty_log(Term::new(4), NodeId::new(2));

        let response = node.handle_request_vote(request, node.log_position());

        assert!(!response.vote_granted());
        assert_eq!(response.term(), Term::new(5));
        assert_eq!(node.current_term(), Term::new(5));
    }

    #[test]
    fn newer_candidate_term_updates_node_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::for_empty_log(Term::new(5), NodeId::new(2));

        let response = node.handle_request_vote(request, node.log_position());

        assert_eq!(node.current_term(), Term::new(5));
        assert_eq!(node.role(), Role::Follower);
        assert!(response.vote_granted());
    }

    #[test]
    fn does_not_vote_for_two_different_candidates_in_same_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        let first_request = RequestVote::for_empty_log(Term::new(3), NodeId::new(2));

        let first_response = node.handle_request_vote(first_request, node.log_position());

        assert!(first_response.vote_granted());
        assert_eq!(node.voted_for(), Some(NodeId::new(2)));

        let second_request = RequestVote::for_empty_log(Term::new(3), NodeId::new(3));

        let second_response = node.handle_request_vote(second_request, node.log_position());

        assert!(!second_response.vote_granted());
        assert_eq!(node.voted_for(), Some(NodeId::new(2)));
    }

    #[test]
    fn same_candidate_can_receive_vote_again() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::for_empty_log(Term::new(3), NodeId::new(2));

        let first_response = node.handle_request_vote(request, node.log_position());

        assert!(first_response.vote_granted());

        let second_response = node.handle_request_vote(request, node.log_position());

        assert!(second_response.vote_granted());
    }

    #[test]
    fn rejects_candidate_with_outdated_log() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(5), KvCommand::put("name", "Div")));

        for _ in 0..9 {
            node.append_log_entry(LogEntry::new(Term::new(5), KvCommand::put("key", "value")));
        }

        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(2),
            Some(LogIndex::new(5)),
            Some(Term::new(4)),
        );

        let receiver_log = node.log_position();

        let response = node.handle_request_vote(request, receiver_log);

        assert!(!response.vote_granted());
        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn accepts_candidate_with_newer_log_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(2),
            Some(LogIndex::new(2)),
            Some(Term::new(6)),
        );

        let receiver_log = LogPosition::new(Some(LogIndex::new(100)), Some(Term::new(5)));

        let response = node.handle_request_vote(request, receiver_log);

        assert!(response.vote_granted());
        assert_eq!(node.voted_for(), Some(NodeId::new(2)));
    }

    #[test]
    fn accepts_candidate_with_same_term_and_newer_index() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(2),
            Some(LogIndex::new(10)),
            Some(Term::new(5)),
        );

        let receiver_log = LogPosition::new(Some(LogIndex::new(8)), Some(Term::new(5)));

        let response = node.handle_request_vote(request, receiver_log);

        assert!(response.vote_granted());
    }

    #[test]
    fn rejects_candidate_with_same_term_and_older_index() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = RequestVote::new(
            Term::new(5),
            NodeId::new(2),
            Some(LogIndex::new(8)),
            Some(Term::new(5)),
        );

        let receiver_log = LogPosition::new(Some(LogIndex::new(10)), Some(Term::new(5)));

        let response = node.handle_request_vote(request, receiver_log);

        assert!(!response.vote_granted());
        assert_eq!(node.voted_for(), None);
    }

    #[test]
    fn request_vote_contains_current_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        let request = node.build_request_vote();

        assert_eq!(request.term, Term::new(1));
    }

    #[test]
    fn request_vote_contains_candidate_id() {
        let mut node = RaftNode::new(NodeId::new(7));

        node.start_election();

        let request = node.build_request_vote();

        assert_eq!(request.candidate_id, NodeId::new(7));
    }

    #[test]
    fn request_vote_contains_empty_log_information() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        let request = node.build_request_vote();

        assert_eq!(request.last_log_index, None);
        assert_eq!(request.last_log_term, None);
    }

    #[test]
    fn request_vote_contains_last_log_position() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(2), KvCommand::put("name", "Div")));

        node.append_log_entry(LogEntry::new(
            Term::new(5),
            KvCommand::put("language", "Rust"),
        ));

        node.start_election();

        let request = node.build_request_vote();

        assert_eq!(request.last_log_index, Some(LogIndex::new(2)));

        assert_eq!(request.last_log_term, Some(Term::new(5)));
    }

    #[test]
    fn request_vote_updates_after_new_election_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();

        let first_request = node.build_request_vote();

        node.start_election();

        let second_request = node.build_request_vote();

        assert_eq!(first_request.term, Term::new(1));
        assert_eq!(second_request.term, Term::new(2));
    }
    #[test]
    fn build_heartbeat_contains_current_node_information() {
        let node = RaftNode::new(NodeId::new(1));

        let heartbeat = node.build_heartbeat();

        assert_eq!(heartbeat.term, Term::ZERO);
        assert_eq!(heartbeat.leader_id, NodeId::new(1));
        assert!(heartbeat.is_heartbeat());
        assert_eq!(heartbeat.prev_log_index, None);
        assert_eq!(heartbeat.prev_log_term, None);
        assert_eq!(heartbeat.leader_commit, None);
    }

    #[test]
    fn append_entries_with_older_term_is_rejected() {
        let mut node = RaftNode::new(NodeId::new(1));
        node.start_election();

        let request = AppendEntries::heartbeat(Term::ZERO, NodeId::new(2), None, None, None);

        let response = node.handle_append_entries(request);

        assert!(!response.is_success());
        assert_eq!(response.term(), Term::new(1));
        assert_eq!(node.current_term(), Term::new(1));
    }

    #[test]
    fn append_entries_with_newer_term_updates_node() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = AppendEntries::heartbeat(Term::new(5), NodeId::new(2), None, None, None);

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(response.term(), Term::new(5));
        assert_eq!(node.current_term(), Term::new(5));
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn heartbeat_from_current_term_is_accepted() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = AppendEntries::heartbeat(Term::ZERO, NodeId::new(2), None, None, None);

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(response.term(), Term::ZERO);
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn append_entries_causes_candidate_to_become_follower() {
        let mut node = RaftNode::new(NodeId::new(1));
        node.start_election();

        assert_eq!(node.role(), Role::Candidate);

        let request =
            AppendEntries::heartbeat(node.current_term(), NodeId::new(2), None, None, None);

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.role(), Role::Follower);
    }
    #[test]
    fn append_entries_rejects_missing_previous_log_entry() {
        let mut node = RaftNode::new(NodeId::new(1));

        let request = AppendEntries::heartbeat(
            Term::ZERO,
            NodeId::new(2),
            Some(LogIndex::new(0)),
            Some(Term::ZERO),
            None,
        );

        let response = node.handle_append_entries(request);

        assert!(!response.is_success());
    }

    #[test]
    fn append_entries_accepts_matching_previous_log_entry() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "Div")));

        let request = AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(2),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            None,
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
    }

    #[test]
    fn append_entries_rejects_mismatched_previous_log_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "Div")));

        let request = AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(2),
            Some(LogIndex::new(0)),
            Some(Term::new(2)),
            None,
        );

        let response = node.handle_append_entries(request);

        assert!(!response.is_success());
    }
    #[test]
    fn append_entries_replicates_new_entries() {
        let mut node = RaftNode::new(NodeId::new(1));

        let entries = vec![
            LogEntry::new(Term::new(1), KvCommand::put("name", "Div")),
            LogEntry::new(Term::new(1), KvCommand::put("language", "Rust")),
        ];

        let request = AppendEntries::new(Term::new(1), NodeId::new(2), None, None, entries, None);

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.log().len(), 2);
        assert_eq!(node.log().term_at(LogIndex::new(0)), Some(Term::new(1)));
        assert_eq!(node.log().term_at(LogIndex::new(1)), Some(Term::new(1)));
    }
    #[test]
    fn append_entries_replaces_conflicting_suffix() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "old")));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("b", "old")));

        let entries = vec![LogEntry::new(Term::new(2), KvCommand::put("b", "new"))];

        let request = AppendEntries::new(
            Term::new(2),
            NodeId::new(2),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            entries,
            None,
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.log().len(), 2);
        assert_eq!(node.log().term_at(LogIndex::new(0)), Some(Term::new(1)));
        assert_eq!(node.log().term_at(LogIndex::new(1)), Some(Term::new(2)));
    }
    #[test]
    fn append_entries_updates_commit_index() {
        let mut node = RaftNode::new(NodeId::new(1));

        let entries = vec![
            LogEntry::new(Term::new(1), KvCommand::put("a", "1")),
            LogEntry::new(Term::new(1), KvCommand::put("b", "2")),
            LogEntry::new(Term::new(1), KvCommand::put("c", "3")),
        ];

        let request = AppendEntries::new(
            Term::new(1),
            NodeId::new(2),
            None,
            None,
            entries,
            Some(LogIndex::new(2)),
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.commit_index(), Some(LogIndex::new(2)));
    }
    #[test]
    fn append_entries_does_not_commit_beyond_local_log() {
        let mut node = RaftNode::new(NodeId::new(1));

        let entries = vec![
            LogEntry::new(Term::new(1), KvCommand::put("a", "1")),
            LogEntry::new(Term::new(1), KvCommand::put("b", "2")),
        ];

        let request = AppendEntries::new(
            Term::new(1),
            NodeId::new(2),
            None,
            None,
            entries,
            Some(LogIndex::new(10)),
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.commit_index(), Some(LogIndex::new(2)));
    }
    #[test]
    fn commit_index_does_not_move_backwards() {
        let mut node = RaftNode::new(NodeId::new(1));

        let entries = vec![
            LogEntry::new(Term::new(1), KvCommand::put("a", "1")),
            LogEntry::new(Term::new(1), KvCommand::put("b", "2")),
            LogEntry::new(Term::new(1), KvCommand::put("c", "3")),
        ];

        let request = AppendEntries::new(
            Term::new(1),
            NodeId::new(2),
            None,
            None,
            entries,
            Some(LogIndex::new(2)),
        );

        assert!(node.handle_append_entries(request).is_success());
        assert_eq!(node.commit_index(), Some(LogIndex::new(2)));

        let heartbeat = AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(2),
            Some(LogIndex::new(2)),
            Some(Term::new(1)),
            Some(LogIndex::new(1)),
        );

        assert!(node.handle_append_entries(heartbeat).is_success());
        assert_eq!(node.commit_index(), Some(LogIndex::new(2)));
    }
    #[test]
    fn build_append_entries_uses_replication_state() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("b", "2")));

        node.append_log_entry(LogEntry::new(Term::new(2), KvCommand::put("c", "3")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(2));

        let request = node
            .build_append_entries(peer, &replication)
            .expect("peer should be registered");

        assert_eq!(request.term, Term::ZERO);
        assert_eq!(request.leader_id, NodeId::new(1));
        assert_eq!(request.prev_log_index, Some(LogIndex::new(1)));
        assert_eq!(request.prev_log_term, Some(Term::new(1)));
        assert_eq!(request.entries.len(), 2);
        assert_eq!(request.entries[0].term(), Term::new(1));
        assert_eq!(request.entries[1].term(), Term::new(2));
    }

    #[test]
    fn build_append_entries_returns_none_for_unknown_peer() {
        let node = RaftNode::new(NodeId::new(1));
        let replication = ReplicationState::new();

        let result = node.build_append_entries(NodeId::new(99), &replication);

        assert!(result.is_none());
    }

    #[test]
    fn build_append_entries_from_first_log_entry_has_no_previous_entry() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));

        let request = node
            .build_append_entries(peer, &replication)
            .expect("peer should be registered");

        assert_eq!(request.prev_log_index, None);
        assert_eq!(request.prev_log_term, None);
        assert_eq!(request.entries.len(), 1);
    }
    #[test]
    fn build_append_entries_uses_correct_previous_log_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(Term::new(2), KvCommand::put("b", "2")));

        node.append_log_entry(LogEntry::new(Term::new(3), KvCommand::put("c", "3")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        // The follower needs entry 3, so the previous entry is
        // Raft log index 2, whose term is 2.
        replication.add_peer(peer, LogIndex::new(3));

        let request = node
            .build_append_entries(peer, &replication)
            .expect("peer should be registered");

        assert_eq!(request.prev_log_index, Some(LogIndex::new(2)));
        assert_eq!(request.prev_log_term, Some(Term::new(2)));
        assert_eq!(request.entries.len(), 1);
        assert_eq!(request.entries[0].term(), Term::new(3));
    }
    #[test]
    fn leader_builds_heartbeat_when_follower_is_up_to_date() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        // The follower already has the leader's latest entry.
        replication.add_peer(peer, LogIndex::new(2));

        let request = node
            .build_append_entries(peer, &replication)
            .expect("peer should be registered");

        assert_eq!(request.term, current_term);
        assert_eq!(request.leader_id, NodeId::new(1));
        assert_eq!(request.prev_log_index, Some(LogIndex::new(1)));
        assert_eq!(request.prev_log_term, Some(current_term));
        assert!(request.is_heartbeat());
        assert_eq!(request.entry_count(), 0);
    }
    #[test]
    fn successful_append_entries_response_advances_replication() {
        let mut node = RaftNode::new(NodeId::new(1));
        node.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));

        let response = AppendEntriesResponse::success(Term::ZERO);

        node.handle_append_entries_response(peer, LogIndex::new(3), response, &mut replication);

        assert_eq!(replication.match_index(peer), Some(LogIndex::new(3)));

        assert_eq!(replication.next_index(peer), Some(LogIndex::new(4)));

        assert_eq!(node.role(), Role::Leader);
    }
    #[test]
    fn failed_append_entries_response_backtracks_replication() {
        let mut node = RaftNode::new(NodeId::new(1));
        node.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(4));

        let response = AppendEntriesResponse::failure(Term::ZERO);

        node.handle_append_entries_response(peer, LogIndex::new(3), response, &mut replication);

        assert_eq!(replication.next_index(peer), Some(LogIndex::new(3)));

        assert_eq!(replication.match_index(peer), None);
        assert_eq!(node.role(), Role::Leader);
    }
    #[test]
    fn handle_append_entries_response_backtracks_failed_replication() {
        let mut leader = RaftNode::new(NodeId::new(1));

        // Make the node a leader.
        leader.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        // The leader will initially try to send from index 5.
        replication.add_peer(peer, LogIndex::new(5));

        let response = AppendEntriesResponse::failure(leader.current_term());

        let handled = leader.handle_append_entries_response(
            peer,
            LogIndex::new(4),
            response,
            &mut replication,
        );

        assert!(handled);

        // The failed replication should move next_index backward:
        // 5 -> 4
        assert_eq!(replication.next_index(peer), Some(LogIndex::new(4)));
    }

    #[test]
    fn leader_retries_append_entries_after_replication_failure() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("b", "2")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        // Start by assuming the follower needs the entry at index 2.
        replication.add_peer(peer, LogIndex::new(2));

        let response = AppendEntriesResponse::failure(current_term);

        let handled =
            node.handle_append_entries_response(peer, LogIndex::new(1), response, &mut replication);

        assert!(handled);
        assert_eq!(replication.next_index(peer), Some(LogIndex::new(1)));
    }
    #[test]
    fn newer_term_in_append_entries_response_demotes_leader() {
        let mut node = RaftNode::new(NodeId::new(1));
        node.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(2));

        let response = AppendEntriesResponse::success(Term::new(5));

        node.handle_append_entries_response(peer, LogIndex::new(1), response, &mut replication);

        assert_eq!(node.current_term(), Term::new(5));
        assert_eq!(node.role(), Role::Follower);
    }
    #[test]
    fn append_entries_response_is_ignored_when_node_is_not_leader() {
        let mut node = RaftNode::new(NodeId::new(1));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(2));

        let response = AppendEntriesResponse::success(Term::ZERO);

        node.handle_append_entries_response(peer, LogIndex::new(1), response, &mut replication);

        assert_eq!(replication.next_index(peer), Some(LogIndex::new(2)));

        assert_eq!(replication.match_index(peer), None);
        assert_eq!(node.role(), Role::Follower);
    }
    #[test]
    fn leader_advances_commit_index_for_current_term_entry() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));
        replication.advance(peer, LogIndex::new(1));

        let result = node.advance_commit_index(&replication, 3);

        assert_eq!(result, Some(LogIndex::new(1)));
        assert_eq!(node.commit_index(), Some(LogIndex::new(1)));
    }
    #[test]
    fn leader_does_not_advance_commit_index_without_majority() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        let peer1 = NodeId::new(2);
        let peer2 = NodeId::new(3);

        let mut replication = ReplicationState::new();

        replication.add_peer(peer1, LogIndex::new(1));
        replication.add_peer(peer2, LogIndex::new(1));

        // Only the leader has the entry.
        let result = node.advance_commit_index(&replication, 3);

        assert_eq!(result, None);
        assert_eq!(node.commit_index(), None);
    }
    #[test]
    fn follower_does_not_advance_commit_index() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));
        replication.advance(peer, LogIndex::new(1));

        let result = node.advance_commit_index(&replication, 3);

        assert_eq!(result, None);
        assert_eq!(node.commit_index(), None);
    }
    #[test]
    fn apply_committed_entries_applies_entries_to_state_machine() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("b", "2")));

        node.update_commit_index(Some(LogIndex::new(2)));

        let last_applied = node.apply_committed_entries();

        assert_eq!(last_applied, Some(LogIndex::new(2)));

        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));
        assert_eq!(node.state_machine().get("b"), Some("2".to_string()));
        assert_eq!(node.last_applied(), Some(LogIndex::new(2)));
    }

    #[test]
    fn apply_committed_entries_does_not_apply_same_entry_twice() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        node.update_commit_index(Some(LogIndex::new(1)));

        assert_eq!(node.apply_committed_entries(), Some(LogIndex::new(1)));

        assert_eq!(node.apply_committed_entries(), Some(LogIndex::new(1)));

        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));
        assert_eq!(node.last_applied(), Some(LogIndex::new(1)));
    }
    #[test]
    fn apply_committed_entries_applies_entries_in_order() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let term = node.current_term();

        node.append_log_entry(LogEntry::new(term, KvCommand::put("first", "one")));

        node.append_log_entry(LogEntry::new(term, KvCommand::put("second", "two")));

        node.commit_index = Some(LogIndex::new(2));

        let result = node.apply_committed_entries();

        assert_eq!(result, Some(LogIndex::new(2)));
        assert_eq!(node.last_applied(), Some(LogIndex::new(2)));

        assert_eq!(node.state_machine().get("first"), Some("one".to_string()));
        assert_eq!(node.state_machine().get("second"), Some("two".to_string()));
    }
    #[test]
    fn leader_does_not_directly_commit_entry_from_old_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("old", "term")));

        // The leader is now in term 2.
        node.start_election();
        node.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));
        replication.advance(peer, LogIndex::new(1));

        let result = node.advance_commit_index(&replication, 3);

        assert_eq!(result, None);
        assert_eq!(node.commit_index(), None);
    }

    #[test]
    fn committed_entry_is_applied_to_state_machine() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        node.commit_index = Some(LogIndex::new(1));

        let result = node.apply_committed_entries();

        assert_eq!(result, Some(LogIndex::new(1)));
        assert_eq!(node.last_applied(), Some(LogIndex::new(1)));
        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));
    }
    #[test]
    fn multiple_committed_entries_are_applied_in_order() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("b", "2")));

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("c", "3")));

        node.commit_index = Some(LogIndex::new(3));

        let result = node.apply_committed_entries();

        assert_eq!(result, Some(LogIndex::new(3)));
        assert_eq!(node.last_applied(), Some(LogIndex::new(3)));

        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));

        assert_eq!(node.state_machine().get("b"), Some("2".to_string()));

        assert_eq!(node.state_machine().get("c"), Some("3".to_string()));
    }
    #[test]
    fn already_applied_entries_are_not_applied_again() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        node.commit_index = Some(LogIndex::new(1));

        assert_eq!(node.apply_committed_entries(), Some(LogIndex::new(1)));

        assert_eq!(node.last_applied(), Some(LogIndex::new(1)));

        // Calling it again must not reapply index 1.
        assert_eq!(node.apply_committed_entries(), Some(LogIndex::new(1)));

        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));
    }
    #[test]
    fn advance_and_apply_committed_entries_updates_state_machine() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "100")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));
        replication.advance(peer, LogIndex::new(1));

        let result = node.advance_and_apply_committed_entries(&replication, 3);

        assert_eq!(result, Some(LogIndex::new(1)));
        assert_eq!(node.commit_index(), Some(LogIndex::new(1)));
        assert_eq!(node.last_applied(), Some(LogIndex::new(1)));
        assert_eq!(node.state_machine().get("a"), Some("100".to_string()));
    }
    #[test]
    fn follower_applies_entries_committed_by_leader() {
        let mut node = RaftNode::new(NodeId::new(2));

        let request = AppendEntries::new(
            Term::new(1),
            NodeId::new(1),
            None,
            None,
            vec![LogEntry::new(Term::new(1), KvCommand::put("a", "1"))],
            Some(LogIndex::new(1)),
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());
        assert_eq!(node.commit_index(), Some(LogIndex::new(1)));
        assert_eq!(node.last_applied(), Some(LogIndex::new(1)));
        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));
    }
    #[test]
    fn follower_applies_multiple_committed_entries_in_order() {
        let mut node = RaftNode::new(NodeId::new(2));

        let request = AppendEntries::new(
            Term::new(1),
            NodeId::new(1),
            None,
            None,
            vec![
                LogEntry::new(Term::new(1), KvCommand::put("a", "1")),
                LogEntry::new(Term::new(1), KvCommand::put("b", "2")),
                LogEntry::new(Term::new(1), KvCommand::put("c", "3")),
            ],
            Some(LogIndex::new(3)),
        );

        let response = node.handle_append_entries(request);

        assert!(response.is_success());

        assert_eq!(node.commit_index(), Some(LogIndex::new(3)));

        assert_eq!(node.last_applied(), Some(LogIndex::new(3)));

        assert_eq!(node.state_machine().get("a"), Some("1".to_string()));

        assert_eq!(node.state_machine().get("b"), Some("2".to_string()));

        assert_eq!(node.state_machine().get("c"), Some("3".to_string()));
    }
    #[test]
    fn leader_records_successful_append_entries_response() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let peer = NodeId::new(2);
        let replicated_index = LogIndex::new(1);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));

        let response = AppendEntriesResponse::success(node.current_term());

        let handled =
            node.handle_append_entries_response(peer, replicated_index, response, &mut replication);

        assert!(handled);

        assert_eq!(replication.match_index(peer), Some(replicated_index));

        assert_eq!(replication.next_index(peer), Some(LogIndex::new(2)));
    }
    #[test]
    fn leader_steps_down_when_append_entries_response_has_higher_term() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();
        replication.add_peer(peer, LogIndex::new(1));

        let higher_term = Term::new(node.current_term().value() + 1);

        let response = AppendEntriesResponse::success(higher_term);

        let handled =
            node.handle_append_entries_response(peer, LogIndex::new(1), response, &mut replication);

        assert!(!handled);
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), higher_term);
    }
    #[test]
    fn handle_append_entries_response_advances_successful_replication() {
        let mut leader = RaftNode::new(NodeId::new(1));

        leader.become_leader();

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        replication.add_peer(peer, LogIndex::new(4));

        let response = AppendEntriesResponse::success(leader.current_term());

        let handled = leader.handle_append_entries_response(
            peer,
            LogIndex::new(4),
            response,
            &mut replication,
        );

        assert!(handled);

        // Successful replication means:
        // match_index = 4
        // next_index = 5
        assert_eq!(replication.match_index(peer), Some(LogIndex::new(4)));

        assert_eq!(replication.next_index(peer), Some(LogIndex::new(5)));
    }
    #[test]
    fn leader_builds_retry_after_replication_failure() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.start_election();
        node.become_leader();

        let current_term = node.current_term();

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("a", "1")));

        node.append_log_entry(LogEntry::new(current_term, KvCommand::put("b", "2")));

        let peer = NodeId::new(2);

        let mut replication = ReplicationState::new();

        // Initially the follower is expected to receive entry 2.
        replication.add_peer(peer, LogIndex::new(2));

        // The follower rejects that attempt.
        let response = AppendEntriesResponse::failure(current_term);

        let handled =
            node.handle_append_entries_response(peer, LogIndex::new(2), response, &mut replication);

        assert!(handled);

        // The leader should retry from entry 1.
        assert_eq!(replication.next_index(peer), Some(LogIndex::new(1)));

        let retry = node
            .build_append_entries(peer, &replication)
            .expect("peer should be registered");

        assert_eq!(retry.prev_log_index, None);
        assert_eq!(retry.prev_log_term, None);
        assert_eq!(retry.entries.len(), 2);
        assert_eq!(retry.entries[0].term(), current_term);
        assert_eq!(retry.entries[1].term(), current_term);
    }
    #[tokio::test]
    async fn node_can_send_request_vote() {
        let server = crate::network::server::NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut node = RaftNode::new(NodeId::new(1));
        node.start_election();

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());

        transport.add_peer(NodeId::new(2), address);

        node.send_request_vote(&transport, NodeId::new(2))
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));

        match received.message() {
            crate::network::message::NetworkMessage::RequestVote(request) => {
                assert_eq!(request.candidate_id, NodeId::new(1));
                assert_eq!(request.term, Term::new(1));
            }
            _ => panic!("expected RequestVote message"),
        }
    }

    #[tokio::test]
    async fn node_can_send_append_entries() {
        let server = crate::network::server::NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let node = RaftNode::new(NodeId::new(1));

        let mut replication = ReplicationState::new();
        replication.add_peer(NodeId::new(2), LogIndex::new(1));

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());

        transport.add_peer(NodeId::new(2), address);

        node.send_append_entries(&transport, NodeId::new(2), &replication)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));

        match received.message() {
            crate::network::message::NetworkMessage::AppendEntries(request) => {
                assert_eq!(request.term, Term::ZERO);
                assert_eq!(request.leader_id, NodeId::new(1));
                assert!(request.is_heartbeat());
            }
            _ => panic!("expected AppendEntries message"),
        }
    }
    #[tokio::test]
    async fn node_can_send_request_vote_response() {
        let server = crate::network::server::NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let node = RaftNode::new(NodeId::new(1));

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());

        transport.add_peer(NodeId::new(2), address);

        let response = crate::raft::rpc::RequestVoteResponse::granted(Term::ZERO);

        node.send_request_vote_response(&transport, NodeId::new(2), response)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));

        match received.message() {
            crate::network::message::NetworkMessage::RequestVoteResponse(received_response) => {
                assert_eq!(received_response, &response);
                assert!(received_response.vote_granted);
            }
            _ => panic!("expected RequestVoteResponse message"),
        }
    }

    #[tokio::test]
    async fn node_can_send_append_entries_response() {
        let server = crate::network::server::NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let node = RaftNode::new(NodeId::new(1));

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());

        transport.add_peer(NodeId::new(2), address);

        let response = crate::raft::rpc::AppendEntriesResponse::success(Term::ZERO);

        node.send_append_entries_response(&transport, NodeId::new(2), response)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));

        match received.message() {
            crate::network::message::NetworkMessage::AppendEntriesResponse(received_response) => {
                assert_eq!(received_response, &response);
                assert!(received_response.is_success());
            }
            _ => panic!("expected AppendEntriesResponse message"),
        }
    }
    #[test]
    fn node_can_handle_request_vote_message() {
        let mut node = RaftNode::new(NodeId::new(2));

        let request = crate::raft::rpc::RequestVote::for_empty_log(Term::new(1), NodeId::new(1));

        let response = node.handle_request_vote_message(request);

        assert_eq!(response.term, Term::new(1));
        assert!(response.vote_granted);
    }

    #[test]
    fn node_can_handle_append_entries_message() {
        let mut node = RaftNode::new(NodeId::new(2));

        let request = crate::raft::rpc::AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            None,
            None,
            None,
        );

        let response = node.handle_append_entries_message(request);

        assert_eq!(response.term(), Term::new(1));
        assert!(response.is_success());
    }

    #[test]
    fn node_can_handle_request_vote_response_message() {
        let mut node = RaftNode::new(NodeId::new(1));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        election.start(&mut node);

        let response = crate::raft::rpc::RequestVoteResponse::granted(Term::new(1));

        let became_leader =
            node.handle_request_vote_response_message(NodeId::new(2), response, &mut election);

        assert!(became_leader);
        assert_eq!(node.role(), Role::Leader);
    }
    #[test]
    fn node_can_handle_append_entries_response_message() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.become_leader();

        let mut replication = ReplicationState::new();
        replication.add_peer(NodeId::new(2), LogIndex::new(1));

        let response = crate::raft::rpc::AppendEntriesResponse::success(Term::ZERO);

        node.handle_append_entries_response_message(
            NodeId::new(2),
            LogIndex::new(1),
            response,
            &mut replication,
        );

        assert_eq!(
            replication.match_index(NodeId::new(2)),
            Some(LogIndex::new(1))
        );
    }
    #[test]
    fn node_can_dispatch_request_vote_message() {
        let mut node = RaftNode::new(NodeId::new(2));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        let mut replication = ReplicationState::new();

        let request = crate::raft::rpc::RequestVote::for_empty_log(Term::ZERO, NodeId::new(1));

        let response = node.handle_network_message(
            crate::network::message::NetworkMessage::RequestVote(request),
            NodeId::new(1),
            LogIndex::new(1),
            &mut election,
            &mut replication,
        );

        assert!(matches!(
            response,
            Some(crate::network::message::NetworkMessage::RequestVoteResponse(_))
        ));
    }
    #[test]
    fn node_can_dispatch_append_entries_message() {
        let mut node = RaftNode::new(NodeId::new(2));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        let mut replication = ReplicationState::new();

        let request = crate::raft::rpc::AppendEntries::heartbeat(
            Term::ZERO,
            NodeId::new(1),
            None,
            None,
            None,
        );

        let response = node.handle_network_message(
            crate::network::message::NetworkMessage::AppendEntries(request),
            NodeId::new(1),
            LogIndex::new(1),
            &mut election,
            &mut replication,
        );

        assert!(matches!(
            response,
            Some(crate::network::message::NetworkMessage::AppendEntriesResponse(_))
        ));
    }
    #[test]
    fn node_can_dispatch_request_vote_response_message() {
        let mut node = RaftNode::new(NodeId::new(1));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        election.start(&mut node);

        let mut replication = ReplicationState::new();

        let response = crate::raft::rpc::RequestVoteResponse::granted(Term::new(1));

        let result = node.handle_network_message(
            crate::network::message::NetworkMessage::RequestVoteResponse(response),
            NodeId::new(2),
            LogIndex::new(1),
            &mut election,
            &mut replication,
        );

        assert!(result.is_none());
        assert_eq!(node.role(), Role::Leader);
    }
    #[test]
    fn node_can_dispatch_append_entries_response_message() {
        let mut node = RaftNode::new(NodeId::new(1));

        node.become_leader();

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        let mut replication = ReplicationState::new();
        replication.add_peer(NodeId::new(2), LogIndex::new(1));

        let response = crate::raft::rpc::AppendEntriesResponse::success(Term::ZERO);

        let result = node.handle_network_message(
            crate::network::message::NetworkMessage::AppendEntriesResponse(response),
            NodeId::new(2),
            LogIndex::new(1),
            &mut election,
            &mut replication,
        );

        assert!(result.is_none());
        assert_eq!(
            replication.match_index(NodeId::new(2)),
            Some(LogIndex::new(1))
        );
    }
    #[test]
    fn node_can_handle_network_envelope() {
        let mut node = RaftNode::new(NodeId::new(2));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        let mut replication = ReplicationState::new();

        let request = crate::raft::rpc::RequestVote::for_empty_log(Term::ZERO, NodeId::new(1));

        let envelope = crate::network::message::NetworkEnvelope::new(
            NodeId::new(1),
            crate::network::message::NetworkMessage::RequestVote(request),
        );

        let response = node.handle_network_envelope(
            envelope,
            LogIndex::new(0),
            &mut election,
            &mut replication,
        );

        assert!(response.is_some());

        let response = response.unwrap();

        assert_eq!(response.sender(), NodeId::new(2));

        match response.message() {
            crate::network::message::NetworkMessage::RequestVoteResponse(response) => {
                assert_eq!(response.term(), Term::ZERO);
                assert!(response.vote_granted());
            }
            _ => panic!("expected RequestVoteResponse"),
        }
    }
    #[test]
    fn node_can_handle_append_entries_network_envelope() {
        let mut node = RaftNode::new(NodeId::new(2));

        let mut election = crate::raft::election::state::ElectionState::new(
            3,
            std::time::Duration::from_millis(150),
        );

        let mut replication = ReplicationState::new();

        let request = crate::raft::rpc::AppendEntries::heartbeat(
            Term::ZERO,
            NodeId::new(1),
            None,
            None,
            None,
        );

        let envelope = crate::network::message::NetworkEnvelope::new(
            NodeId::new(1),
            crate::network::message::NetworkMessage::AppendEntries(request),
        );

        let response = node.handle_network_envelope(
            envelope,
            LogIndex::new(0),
            &mut election,
            &mut replication,
        );

        assert!(response.is_some());

        let response = response.unwrap();

        assert_eq!(response.sender(), NodeId::new(2));

        match response.message() {
            crate::network::message::NetworkMessage::AppendEntriesResponse(response) => {
                assert_eq!(response.term(), Term::ZERO);
                assert!(response.is_success());
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    #[tokio::test]
    async fn node_can_receive_and_respond_to_network_message() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::network::server::NetworkServer;
        use crate::raft::election::state::ElectionState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut client = NetworkClient::connect(address).await.unwrap();

            let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(2));

            let envelope =
                NetworkEnvelope::new(NodeId::new(2), NetworkMessage::RequestVote(request));

            client.send_envelope(&envelope).await.unwrap();

            let response = client.receive_envelope().await.unwrap();

            assert_eq!(response.sender(), NodeId::new(1));

            assert!(matches!(
                response.message(),
                NetworkMessage::RequestVoteResponse(_)
            ));
        });

        let mut connection = server.accept_and_receive().await.unwrap();

        let mut node = RaftNode::new(NodeId::new(1));
        let mut election = ElectionState::new(3, Duration::from_millis(150));
        let mut replication = ReplicationState::new();

        node.receive_network_message(
            &mut connection,
            LogIndex::new(0),
            &mut election,
            &mut replication,
        )
        .await
        .unwrap();

        client.await.unwrap();
    }

    #[tokio::test]
    async fn node_can_run_server_messages() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::network::server::NetworkServer;
        use crate::raft::election::state::ElectionState;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut client = NetworkClient::connect(address).await.unwrap();

            let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(2));

            let envelope =
                NetworkEnvelope::new(NodeId::new(2), NetworkMessage::RequestVote(request));

            client.send_envelope(&envelope).await.unwrap();

            let response = client.receive_envelope().await.unwrap();

            assert_eq!(response.sender(), NodeId::new(1));

            assert!(matches!(
                response.message(),
                NetworkMessage::RequestVoteResponse(_)
            ));
        });

        let mut node = RaftNode::new(NodeId::new(1));
        let mut election = ElectionState::new(3, Duration::from_millis(150));
        let mut replication = ReplicationState::new();

        node.run_server_messages(
            &server,
            1,
            LogIndex::new(0),
            &mut election,
            &mut replication,
        )
        .await
        .unwrap();

        client.await.unwrap();
    }

    #[tokio::test]
    async fn node_can_run_multiple_server_messages() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::network::server::NetworkServer;
        use crate::raft::election::state::ElectionState;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            for _ in 0..2 {
                let mut client = NetworkClient::connect(address).await.unwrap();

                let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(2));

                let envelope =
                    NetworkEnvelope::new(NodeId::new(2), NetworkMessage::RequestVote(request));

                client.send_envelope(&envelope).await.unwrap();

                let response = client.receive_envelope().await.unwrap();

                assert_eq!(response.sender(), NodeId::new(1));

                assert!(matches!(
                    response.message(),
                    NetworkMessage::RequestVoteResponse(_)
                ));
            }
        });

        let mut node = RaftNode::new(NodeId::new(1));
        let mut election = ElectionState::new(3, Duration::from_millis(150));
        let mut replication = ReplicationState::new();

        node.run_server_messages(
            &server,
            2,
            LogIndex::new(0),
            &mut election,
            &mut replication,
        )
        .await
        .unwrap();

        client.await.unwrap();
    }
}
