pub mod timer;

use crate::config::RuntimeConfig;
use crate::network::server::NetworkServer;
use crate::network::transport::NodeTransport;
use crate::node::timer::NodeTimer;
use crate::raft::election::state::ElectionState;
use crate::raft::node::RaftNode;
use crate::raft::replication::ReplicationState;
use crate::raft::types::LogIndex;

#[derive(Debug)]
pub struct NodeRuntime {
    config: RuntimeConfig,
    raft: RaftNode,
    transport: NodeTransport,
    server: Option<NetworkServer>,
    timer: NodeTimer,
    election: ElectionState,
    replication: ReplicationState,
}

impl NodeRuntime {
    pub fn new(config: RuntimeConfig) -> Self {
        let node_id = config.node_id();
        let cluster_size = config.cluster().node_count();
        let raft = RaftNode::new(node_id);

        let transport = NodeTransport::from_cluster_config(config.cluster(), node_id)
            .expect("local node must exist in cluster configuration");

        Self {
            raft,
            transport,
            server: None,
            timer: NodeTimer::new(std::time::Duration::from_millis(100)),
            election: ElectionState::new(cluster_size, std::time::Duration::from_millis(500)),
            replication: {
                let mut replication = ReplicationState::new();

                for peer in config.peer_nodes() {
                    replication.add_peer(peer.node_id(), LogIndex::new(1));
                }

                replication
            },
            config,
        }
    }

    pub async fn bind(config: RuntimeConfig) -> std::io::Result<Self> {
        let node_id = config.node_id();
        let cluster_size = config.cluster().node_count();
        let raft = RaftNode::new(node_id);

        let transport = NodeTransport::from_cluster_config(config.cluster(), node_id)
            .expect("local node must exist in cluster configuration");

        let server = NetworkServer::bind(config.node().address()).await?;

        Ok(Self {
            raft,
            transport,
            server: Some(server),
            timer: NodeTimer::new(std::time::Duration::from_millis(100)),
            election: ElectionState::new(cluster_size, std::time::Duration::from_millis(500)),
            replication: {
                let mut replication = ReplicationState::new();

                for peer in config.peer_nodes() {
                    replication.add_peer(peer.node_id(), LogIndex::new(1));
                }

                replication
            },
            config,
        })
    }

    pub const fn timer(&self) -> &NodeTimer {
        &self.timer
    }

    pub const fn election(&self) -> &ElectionState {
        &self.election
    }
    pub fn advance_timer(&mut self, duration: std::time::Duration) {
        self.timer.advance(duration);
    }

    pub fn election_mut(&mut self) -> &mut ElectionState {
        &mut self.election
    }

    pub fn reset_timer(&mut self) {
        self.timer.reset();
    }

    pub async fn tick(&mut self) -> std::io::Result<()> {
        if !self.timer.take_if_expired() {
            return Ok(());
        }

        if self.raft.role().is_leader() {
            let peers: Vec<_> = self
                .config
                .peer_nodes()
                .iter()
                .map(|peer| peer.node_id())
                .collect();

            for peer in peers {
                if let Some(request) = self.raft.build_append_entries(peer, &self.replication) {
                    self.transport
                        .send_append_entries(peer, self.raft.id(), request)
                        .await?;
                }
            }

            return Ok(());
        }

        if !self.raft.role().is_follower() {
            return Ok(());
        }

        self.election.start(&mut self.raft);

        let peers: Vec<_> = self
            .config
            .peer_nodes()
            .iter()
            .map(|peer| peer.node_id())
            .collect();

        for peer in peers {
            let request = self.raft.build_request_vote();

            let response = self
                .transport
                .request_vote(peer, self.raft.id(), request)
                .await?;

            self.raft
                .handle_request_vote_response_message(peer, response, &mut self.election);

            if self.election.has_majority() {
                self.raft.become_leader();
                break;
            }
        }

        Ok(())
    }

    pub const fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub const fn raft(&self) -> &RaftNode {
        &self.raft
    }

    pub fn raft_mut(&mut self) -> &mut RaftNode {
        &mut self.raft
    }

    pub const fn transport(&self) -> &NodeTransport {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut NodeTransport {
        &mut self.transport
    }

    pub const fn replication(&self) -> &ReplicationState {
        &self.replication
    }

    pub fn replication_mut(&mut self) -> &mut ReplicationState {
        &mut self.replication
    }

    pub const fn server(&self) -> Option<&NetworkServer> {
        self.server.as_ref()
    }

    pub fn server_address(&self) -> std::io::Result<Option<std::net::SocketAddr>> {
        self.server
            .as_ref()
            .map(NetworkServer::local_addr)
            .transpose()
    }

    pub async fn process_one_message(
        &mut self,
        replicated_index: LogIndex,
        election: &mut ElectionState,
        _replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        let server = self
            .server
            .as_ref()
            .expect("network server must be bound before processing messages");

        let mut connection = server.receive_one_message().await?;
        let envelope = connection.receive_envelope().await?;

        let is_heartbeat = matches!(
            envelope.message(),
            crate::network::message::NetworkMessage::AppendEntries(request)
                if request.is_heartbeat()
        );

        let is_append_entries_response = matches!(
            envelope.message(),
            crate::network::message::NetworkMessage::AppendEntriesResponse(_)
        );

        if is_heartbeat {
            self.reset_timer();
        }

        if let Some(response) = self.raft.handle_network_envelope(
            envelope,
            replicated_index,
            election,
            &mut self.replication,
        ) {
            connection.send_envelope(&response).await?;
        }

        if is_append_entries_response {
            let cluster_size = self.config.cluster().node_count();

            self.raft
                .advance_and_apply_committed_entries(&self.replication, cluster_size);
        }

        Ok(())
    }

    pub async fn process_messages(
        &mut self,
        message_count: usize,
        replicated_index: LogIndex,
        election: &mut ElectionState,
        replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        for _ in 0..message_count {
            self.process_one_message(replicated_index, election, replication)
                .await?;
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, NodeConfig};
    use crate::network::message::NetworkEnvelope;
    use crate::raft::types::NodeId;
    use std::net::SocketAddr;

    fn test_config() -> RuntimeConfig {
        let node = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
        );

        let peer = NodeConfig::new(
            NodeId::new(2),
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        RuntimeConfig::new(node, cluster)
    }

    #[test]
    fn creates_runtime_with_config() {
        let config = test_config();
        let runtime = NodeRuntime::new(config.clone());

        assert_eq!(runtime.config(), &config);
        assert!(runtime.server().is_none());
    }

    #[test]
    fn creates_raft_node_with_configured_node_id() {
        let config = test_config();
        let runtime = NodeRuntime::new(config);

        assert_eq!(runtime.raft().id(), NodeId::new(1));
    }

    #[test]
    fn exposes_mutable_raft_node() {
        let config = test_config();
        let mut runtime = NodeRuntime::new(config);

        runtime.raft_mut().start_election();

        assert!(runtime.raft().role().is_candidate());
    }

    #[test]
    fn creates_transport_with_cluster_peers() {
        let config = test_config();
        let runtime = NodeRuntime::new(config);

        assert_eq!(runtime.transport().peer_count(), 1);
        assert_eq!(
            runtime.transport().peer_address(NodeId::new(2)),
            Some("127.0.0.1:7002".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn bind_creates_network_server() {
        let config = test_config();
        let runtime = NodeRuntime::bind(config).await.unwrap();

        assert!(runtime.server().is_some());
        assert!(runtime.server_address().unwrap().is_some());
    }

    #[tokio::test]
    async fn processes_one_incoming_raft_message() {
        use crate::network::client::NetworkClient;
        use crate::network::message::NetworkMessage;
        use crate::raft::election::state::ElectionState;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let mut config = test_config();

        config = RuntimeConfig::new(
            NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap()),
            config.cluster().clone(),
        );

        let mut runtime = NodeRuntime::bind(config).await.unwrap();

        let address = runtime.server_address().unwrap().unwrap();

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

        let mut election = ElectionState::new(2, Duration::from_millis(150));
        let mut replication = ReplicationState::new();

        runtime
            .process_one_message(LogIndex::new(0), &mut election, &mut replication)
            .await
            .unwrap();

        client.await.unwrap();
    }

    #[tokio::test]
    async fn processes_multiple_incoming_raft_messages() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::election::state::ElectionState;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let peer = NodeConfig::new(
            NodeId::new(2),
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);
        let config = RuntimeConfig::new(node, cluster);

        let mut runtime = NodeRuntime::bind(config).await.unwrap();

        let address = runtime.server_address().unwrap().unwrap();

        let client_one = tokio::spawn(async move {
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

        let address = runtime.server_address().unwrap().unwrap();

        let client_two = tokio::spawn(async move {
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

        let mut election = ElectionState::new(2, Duration::from_millis(150));
        let mut replication = ReplicationState::new();

        runtime
            .process_messages(2, LogIndex::new(0), &mut election, &mut replication)
            .await
            .unwrap();

        client_one.await.unwrap();
        client_two.await.unwrap();
    }

    #[test]
    fn runtime_starts_with_unexpired_timer() {
        let runtime = NodeRuntime::new(test_config());

        assert_eq!(runtime.timer().elapsed(), std::time::Duration::ZERO);
        assert!(!runtime.timer().expired());
    }

    #[test]
    fn runtime_exposes_configured_timer_interval() {
        let runtime = NodeRuntime::new(test_config());

        assert_eq!(
            runtime.timer().interval(),
            std::time::Duration::from_millis(100)
        );
    }

    #[test]
    fn runtime_can_advance_timer() {
        let mut runtime = NodeRuntime::new(test_config());

        runtime.advance_timer(std::time::Duration::from_millis(50));

        assert_eq!(
            runtime.timer().elapsed(),
            std::time::Duration::from_millis(50)
        );
        assert!(!runtime.timer().expired());

        runtime.advance_timer(std::time::Duration::from_millis(50));

        assert!(runtime.timer().expired());
    }

    #[test]
    fn runtime_can_reset_timer() {
        let mut runtime = NodeRuntime::new(test_config());

        runtime.advance_timer(std::time::Duration::from_millis(100));

        assert!(runtime.timer().expired());

        runtime.reset_timer();

        assert_eq!(runtime.timer().elapsed(), std::time::Duration::ZERO);
        assert!(!runtime.timer().expired());
    }

    #[tokio::test]
    async fn runtime_starts_election_when_timer_expires() {
        let mut runtime = NodeRuntime::new(test_config());

        assert!(runtime.raft().role().is_follower());
        assert_eq!(
            runtime.raft().current_term(),
            crate::raft::types::Term::ZERO
        );

        runtime.advance_timer(std::time::Duration::from_millis(100));

        // The peer is not running in this unit test, so the network
        // connection is expected to fail after the election starts.
        let result = runtime.tick().await;

        assert!(result.is_err());

        assert!(runtime.raft().role().is_candidate());
        assert_eq!(
            runtime.raft().current_term(),
            crate::raft::types::Term::new(1)
        );
        assert_eq!(runtime.timer().elapsed(), std::time::Duration::ZERO);
    }

    #[tokio::test]
    async fn runtime_sends_request_vote_when_election_starts() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::rpc::RequestVoteResponse;
        use crate::raft::types::{NodeId, Term};
        use std::net::SocketAddr;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let peer_address = listener.local_addr().unwrap();

        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let peer = NodeConfig::new(NodeId::new(2), peer_address);

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        let config = RuntimeConfig::new(node, cluster);

        let mut runtime = NodeRuntime::new(config);

        let receiver = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            let mut client = NetworkClient::from_stream(stream);

            let envelope = client.receive_envelope().await.unwrap();

            assert_eq!(envelope.sender(), NodeId::new(1));

            match envelope.message() {
                NetworkMessage::RequestVote(request) => {
                    assert_eq!(request.term, Term::new(1));
                    assert_eq!(request.candidate_id, NodeId::new(1));
                }
                other => panic!("expected RequestVote, got {other:?}"),
            }

            let response = RequestVoteResponse::granted(Term::new(1));

            let response_envelope = NetworkEnvelope::new(
                NodeId::new(2),
                NetworkMessage::RequestVoteResponse(response),
            );

            client.send_envelope(&response_envelope).await.unwrap();
        });

        runtime.advance_timer(std::time::Duration::from_millis(100));

        runtime.tick().await.unwrap();

        receiver.await.unwrap();

        assert!(runtime.raft().role().is_leader());
        assert_eq!(runtime.raft().current_term(), Term::new(1));
        assert_eq!(runtime.election().vote_count(), 2);
    }

    #[tokio::test]
    async fn runtime_becomes_leader_after_three_node_quorum() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::rpc::RequestVoteResponse;
        use crate::raft::types::{NodeId, Term};
        use std::net::SocketAddr;

        async fn start_vote_server(node_id: NodeId) -> SocketAddr {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

            let address = listener.local_addr().unwrap();

            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();

                let mut client = NetworkClient::from_stream(stream);

                let envelope = client.receive_envelope().await.unwrap();

                assert_eq!(envelope.sender(), NodeId::new(1));

                match envelope.message() {
                    NetworkMessage::RequestVote(request) => {
                        assert_eq!(request.term, Term::new(1));
                        assert_eq!(request.candidate_id, NodeId::new(1));
                    }
                    other => panic!("expected RequestVote, got {other:?}"),
                }

                let response = RequestVoteResponse::granted(Term::new(1));

                let response_envelope =
                    NetworkEnvelope::new(node_id, NetworkMessage::RequestVoteResponse(response));

                client.send_envelope(&response_envelope).await.unwrap();

                client.shutdown().await.unwrap();
            });

            address
        }

        let node_2_address = start_vote_server(NodeId::new(2)).await;

        let node_3_address = start_vote_server(NodeId::new(3)).await;

        let node_1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let node_2 = NodeConfig::new(NodeId::new(2), node_2_address);

        let node_3 = NodeConfig::new(NodeId::new(3), node_3_address);

        let cluster = ClusterConfig::new(vec![node_1.clone(), node_2, node_3]);

        let config = RuntimeConfig::new(node_1, cluster);

        let mut runtime = NodeRuntime::new(config);

        runtime.advance_timer(std::time::Duration::from_millis(100));

        runtime.tick().await.unwrap();

        assert!(runtime.raft().role().is_leader());
        assert_eq!(runtime.raft().current_term(), Term::new(1));
        assert_eq!(runtime.election().vote_count(), 2);
        assert!(runtime.election().has_majority());
    }

    #[tokio::test]
    async fn leader_sends_heartbeat_to_peer() {
        use crate::network::client::NetworkClient;
        use crate::network::message::NetworkMessage;
        use crate::raft::types::{NodeId, Term};
        use std::net::SocketAddr;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let peer_address = listener.local_addr().unwrap();

        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let peer = NodeConfig::new(NodeId::new(2), peer_address);

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        let config = RuntimeConfig::new(node, cluster);

        let mut runtime = NodeRuntime::new(config);

        runtime.advance_timer(std::time::Duration::from_millis(100));

        runtime.raft_mut().start_election();

        runtime.raft_mut().become_leader();

        let receiver = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            let mut client = NetworkClient::from_stream(stream);

            let envelope = client.receive_envelope().await.unwrap();

            assert_eq!(envelope.sender(), NodeId::new(1));

            match envelope.message() {
                NetworkMessage::AppendEntries(request) => {
                    assert_eq!(request.term, Term::new(1));

                    assert_eq!(request.leader_id, NodeId::new(1));

                    assert!(request.is_heartbeat());

                    assert_eq!(request.entry_count(), 0);
                }

                other => {
                    panic!("expected AppendEntries heartbeat, got {other:?}");
                }
            }

            client.shutdown().await.unwrap();
        });

        runtime.tick().await.unwrap();

        receiver.await.unwrap();

        assert_eq!(runtime.timer().elapsed(), std::time::Duration::ZERO);
    }

    #[tokio::test]
    async fn leader_sends_log_entry_to_peer() {
        use crate::kv::command::KvCommand;
        use crate::network::client::NetworkClient;
        use crate::network::message::NetworkMessage;
        use crate::raft::log::LogEntry;
        use crate::raft::types::{NodeId, Term};
        use std::net::SocketAddr;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        let peer_address = listener.local_addr().unwrap();

        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let peer = NodeConfig::new(NodeId::new(2), peer_address);

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        let config = RuntimeConfig::new(node, cluster);

        let mut runtime = NodeRuntime::new(config);

        runtime.advance_timer(std::time::Duration::from_millis(100));

        runtime.raft_mut().start_election();
        runtime.raft_mut().become_leader();

        runtime
            .raft_mut()
            .append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "raft")));

        let receiver = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();

            let mut client = NetworkClient::from_stream(stream);

            let envelope = client.receive_envelope().await.unwrap();

            assert_eq!(envelope.sender(), NodeId::new(1));

            match envelope.message() {
                NetworkMessage::AppendEntries(request) => {
                    assert_eq!(request.term, Term::new(1));
                    assert_eq!(request.leader_id, NodeId::new(1));
                    assert!(!request.is_heartbeat());
                    assert_eq!(request.entry_count(), 1);
                    assert_eq!(request.entries[0].term(), Term::new(1));

                    match request.entries[0].command() {
                        KvCommand::Put { key, value } => {
                            assert_eq!(key, "name");
                            assert_eq!(value, "raft");
                        }
                        other => {
                            panic!("expected Put command, got {other:?}");
                        }
                    }
                }
                other => {
                    panic!("expected AppendEntries, got {other:?}");
                }
            }

            client.shutdown().await.unwrap();
        });

        runtime.tick().await.unwrap();

        receiver.await.unwrap();

        assert_eq!(runtime.timer().elapsed(), std::time::Duration::ZERO);
    }

    #[tokio::test]
    async fn follower_receives_log_entry_and_applies_it_to_state_machine() {
        use crate::config::{ClusterConfig, NodeConfig, RuntimeConfig};
        use crate::kv::command::KvCommand;
        use crate::network::client::NetworkClient;
        use crate::network::message::NetworkMessage;
        use crate::raft::election::state::ElectionState;
        use crate::raft::log::LogEntry;
        use crate::raft::replication::ReplicationState;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::net::SocketAddr;
        use std::time::Duration;

        let leader = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let follower =
            NodeConfig::new(NodeId::new(2), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let cluster = ClusterConfig::new(vec![leader.clone(), follower.clone()]);

        let follower_config = RuntimeConfig::new(follower, cluster);

        let mut follower_runtime = NodeRuntime::bind(follower_config).await.unwrap();

        let follower_address = follower_runtime.server_address().unwrap().unwrap();

        let mut leader_node = crate::raft::node::RaftNode::new(NodeId::new(1));

        leader_node.start_election();
        leader_node.become_leader();

        leader_node.append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "raft")));

        let mut replication = ReplicationState::new();

        replication.add_peer(NodeId::new(2), LogIndex::new(1));

        let mut request = leader_node
            .build_append_entries(NodeId::new(2), &replication)
            .unwrap();

        request.leader_commit = Some(LogIndex::new(1));

        assert_eq!(request.entry_count(), 1);

        let sender = tokio::spawn(async move {
            let mut client = NetworkClient::connect(follower_address).await.unwrap();

            let envelope = crate::network::message::NetworkEnvelope::new(
                NodeId::new(1),
                NetworkMessage::AppendEntries(request),
            );

            client.send_envelope(&envelope).await.unwrap();

            let response = client.receive_envelope().await.unwrap();

            match response.message() {
                NetworkMessage::AppendEntriesResponse(response) => {
                    assert!(response.is_success());
                }
                other => {
                    panic!("expected AppendEntriesResponse, got {other:?}");
                }
            }

            client.shutdown().await.unwrap();
        });

        let mut election = ElectionState::new(2, Duration::from_millis(500));
        let mut follower_replication = ReplicationState::new();

        follower_runtime
            .process_one_message(LogIndex::new(1), &mut election, &mut follower_replication)
            .await
            .unwrap();

        sender.await.unwrap();

        assert_eq!(follower_runtime.raft().log().len(), 1);
        assert_eq!(
            follower_runtime.raft().commit_index(),
            Some(LogIndex::new(1))
        );
        assert_eq!(
            follower_runtime.raft().last_applied(),
            Some(LogIndex::new(1))
        );
        assert_eq!(
            follower_runtime.raft().state_machine().get("name"),
            Some("raft".to_string())
        );
    }

    #[tokio::test]
    async fn follower_resets_election_timer_when_heartbeat_is_received() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::election::state::ElectionState;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::AppendEntries;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::net::SocketAddr;
        use std::time::Duration;

        let node = NodeConfig::new(NodeId::new(2), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let peer = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        let config = RuntimeConfig::new(node, cluster);

        let mut runtime = NodeRuntime::bind(config).await.unwrap();

        runtime.advance_timer(Duration::from_millis(90));

        assert_eq!(runtime.timer().elapsed(), Duration::from_millis(90));

        let server_address = runtime.server().unwrap().local_addr().unwrap();

        let sender = tokio::spawn(async move {
            let mut client = NetworkClient::connect(server_address).await.unwrap();

            let heartbeat = AppendEntries::heartbeat(
                Term::new(1),
                NodeId::new(1),
                None,
                None,
                Some(LogIndex::new(0)),
            );

            let envelope =
                NetworkEnvelope::new(NodeId::new(1), NetworkMessage::AppendEntries(heartbeat));

            client.send_envelope(&envelope).await.unwrap();

            let response = client.receive_envelope().await.unwrap();

            assert_eq!(response.sender(), NodeId::new(2));

            match response.message() {
                NetworkMessage::AppendEntriesResponse(response) => {
                    assert_eq!(response.term(), Term::new(1));
                }
                other => panic!("expected AppendEntriesResponse, got {other:?}"),
            }

            client.shutdown().await.unwrap();
        });

        let mut election = ElectionState::new(2, Duration::from_millis(500));

        let mut replication = ReplicationState::new();

        runtime
            .process_one_message(LogIndex::new(0), &mut election, &mut replication)
            .await
            .unwrap();

        sender.await.unwrap();

        assert_eq!(runtime.timer().elapsed(), Duration::ZERO);
    }

    #[tokio::test]
    async fn peer_sends_request_vote_response_over_tcp() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::network::server::NetworkServer;
        use crate::raft::election::state::ElectionState;
        use crate::raft::node::RaftNode;
        use crate::raft::replication::ReplicationState;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::time::Duration;

        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let server_address = server.local_addr().unwrap();

        let mut peer = RaftNode::new(NodeId::new(2));

        let mut election = ElectionState::new(2, Duration::from_millis(500));

        let mut replication = ReplicationState::new();

        let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(1));

        let envelope = NetworkEnvelope::new(NodeId::new(1), NetworkMessage::RequestVote(request));

        let peer_task = tokio::spawn(async move {
            server
                .process_one_message(&mut peer, LogIndex::new(0), &mut election, &mut replication)
                .await
                .unwrap();
        });

        let stream = tokio::net::TcpStream::connect(server_address)
            .await
            .unwrap();

        let mut client = NetworkClient::from_stream(stream);

        client.send_envelope(&envelope).await.unwrap();

        let response = client.receive_envelope().await.unwrap();

        assert_eq!(response.sender(), NodeId::new(2));

        match response.message() {
            NetworkMessage::RequestVoteResponse(response) => {
                assert_eq!(response.term(), Term::new(1));
                assert!(response.vote_granted());
            }
            other => panic!("expected RequestVoteResponse, got {other:?}"),
        }

        peer_task.await.unwrap();
    }

    #[test]
    fn candidate_becomes_leader_after_receiving_majority_vote() {
        use crate::raft::election::state::ElectionState;
        use crate::raft::node::RaftNode;
        use crate::raft::rpc::RequestVoteResponse;
        use crate::raft::types::{NodeId, Term};
        use std::time::Duration;

        let mut node = RaftNode::new(NodeId::new(1));

        let mut election = ElectionState::new(2, Duration::from_millis(500));

        node.start_election();

        election.record_vote(node.id());

        assert!(node.role().is_candidate());
        assert_eq!(node.current_term(), Term::new(1));
        assert_eq!(election.vote_count(), 1);
        assert!(!election.has_majority());

        let response = RequestVoteResponse::granted(Term::new(1));

        let became_leader =
            node.handle_request_vote_response_message(NodeId::new(2), response, &mut election);

        assert!(became_leader);
        assert!(node.role().is_leader());
        assert_eq!(node.current_term(), Term::new(1));
        assert_eq!(election.vote_count(), 2);
    }

    #[tokio::test]
    async fn leader_updates_replication_state_after_follower_acknowledges_entry() {
        use crate::kv::command::KvCommand;
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::log::LogEntry;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::net::SocketAddr;

        let leader = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let follower =
            NodeConfig::new(NodeId::new(2), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let cluster = ClusterConfig::new(vec![leader.clone(), follower.clone()]);

        let config = RuntimeConfig::new(leader, cluster);

        let mut runtime = NodeRuntime::bind(config).await.unwrap();

        runtime.raft_mut().start_election();
        runtime.raft_mut().become_leader();

        runtime
            .raft_mut()
            .append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "raft")));

        let follower_address = follower.address();

        runtime
            .transport_mut()
            .add_peer(NodeId::new(2), follower_address);

        let leader_address = runtime.server_address().unwrap().unwrap();

        let peer = NodeId::new(2);
        let replicated_index = LogIndex::new(1);

        let response = crate::raft::rpc::AppendEntriesResponse::success(Term::new(1));

        let network_response =
            NetworkEnvelope::new(peer, NetworkMessage::AppendEntriesResponse(response));

        let receiver = tokio::spawn(async move {
            let mut client = NetworkClient::connect(leader_address).await.unwrap();

            client.send_envelope(&network_response).await.unwrap();

            client.shutdown().await.unwrap();
        });

        runtime.replication_mut().add_peer(peer, LogIndex::new(1));

        let (raft, replication) = (&mut runtime.raft, &mut runtime.replication);

        raft.handle_append_entries_response_message(peer, replicated_index, response, replication);

        receiver.await.unwrap();

        assert_eq!(
            runtime.replication().match_index(peer),
            Some(LogIndex::new(1))
        );

        assert_eq!(
            runtime.replication().next_index(peer),
            Some(LogIndex::new(2))
        );
    }

    #[test]
    fn runtime_commits_replicated_entry_after_majority_acknowledgement() {
        use crate::kv::command::KvCommand;
        use crate::raft::log::LogEntry;
        use crate::raft::rpc::AppendEntriesResponse;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::net::SocketAddr;

        let leader = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
        );

        let follower = NodeConfig::new(
            NodeId::new(2),
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![leader.clone(), follower]);

        let config = RuntimeConfig::new(leader, cluster);

        let mut runtime = NodeRuntime::new(config);

        runtime.raft_mut().start_election();
        runtime.raft_mut().become_leader();

        runtime
            .raft_mut()
            .append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "raft")));

        let peer = NodeId::new(2);
        let replicated_index = LogIndex::new(1);

        let response = AppendEntriesResponse::success(Term::new(1));

        let (raft, replication) = (&mut runtime.raft, &mut runtime.replication);

        raft.handle_append_entries_response_message(peer, replicated_index, response, replication);
        assert_eq!(
            runtime.replication().match_index(peer),
            Some(LogIndex::new(1))
        );

        let committed = {
            let (raft, replication) = (&mut runtime.raft, &runtime.replication);

            raft.advance_commit_index(replication, 2)
        };

        assert_eq!(committed, Some(LogIndex::new(1)));
        assert_eq!(runtime.raft().commit_index(), Some(LogIndex::new(1)));

        runtime.raft_mut().apply_committed_entries();

        assert_eq!(runtime.raft().last_applied(), Some(LogIndex::new(1)));

        assert_eq!(
            runtime.raft().state_machine().get("name"),
            Some("raft".to_string())
        );
    }

    #[tokio::test]
    async fn runtime_commits_entry_after_tcp_replication_response() {
        use crate::config::cluster_config::ClusterConfig;
        use crate::config::node_config::NodeConfig;
        use crate::config::runtime_config::RuntimeConfig;
        use crate::kv::command::KvCommand;
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::log::LogEntry;
        use crate::raft::rpc::AppendEntriesResponse;
        use crate::raft::types::{LogIndex, NodeId, Term};
        use std::net::SocketAddr;

        let leader = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let follower =
            NodeConfig::new(NodeId::new(2), "127.0.0.1:0".parse::<SocketAddr>().unwrap());

        let cluster = ClusterConfig::new(vec![leader.clone(), follower.clone()]);
        let config = RuntimeConfig::new(leader, cluster);

        let mut runtime = NodeRuntime::bind(config).await.unwrap();

        runtime.raft_mut().start_election();
        runtime.raft_mut().become_leader();

        runtime
            .raft_mut()
            .append_log_entry(LogEntry::new(Term::new(1), KvCommand::put("name", "raft")));

        let leader_address = runtime.server_address().unwrap().unwrap();

        let peer = NodeId::new(2);

        let response = AppendEntriesResponse::success(Term::new(1));

        let envelope = NetworkEnvelope::new(peer, NetworkMessage::AppendEntriesResponse(response));

        let sender = tokio::spawn(async move {
            let mut client = NetworkClient::connect(leader_address).await.unwrap();

            client.send_envelope(&envelope).await.unwrap();

            client.shutdown().await.unwrap();
        });

        let mut election = crate::raft::election::state::ElectionState::new(
            2,
            std::time::Duration::from_millis(500),
        );

        let mut replication = crate::raft::replication::ReplicationState::new();

        replication.add_peer(peer, LogIndex::new(1));

        runtime
            .process_one_message(LogIndex::new(1), &mut election, &mut replication)
            .await
            .unwrap();

        sender.await.unwrap();

        assert_eq!(
            runtime.replication().match_index(peer),
            Some(LogIndex::new(1))
        );

        assert_eq!(
            runtime.replication().next_index(peer),
            Some(LogIndex::new(2))
        );

        assert_eq!(runtime.raft().commit_index(), Some(LogIndex::new(1)));

        assert_eq!(runtime.raft().last_applied(), Some(LogIndex::new(1)));

        assert_eq!(
            runtime.raft().state_machine().get("name"),
            Some("raft".to_string())
        );
    }
}
