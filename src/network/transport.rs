//! Transport abstraction for node-to-node communication.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

use crate::network::client::NetworkClient;
use crate::network::message::NetworkEnvelope;
use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    UnknownPeer(NodeId),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownPeer(node_id) => {
                write!(formatter, "unknown peer: {}", node_id.value())
            }
        }
    }
}

impl std::error::Error for TransportError {}

impl From<TransportError> for io::Error {
    fn from(error: TransportError) -> Self {
        io::Error::new(io::ErrorKind::NotFound, error)
    }
}
/// Sends network envelopes to another Raft node.
#[derive(Debug)]
pub struct NodeTransport {
    address: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,
}
impl NodeTransport {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            peers: HashMap::new(),
        }
    }

    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn add_peer(&mut self, node_id: NodeId, address: SocketAddr) {
        self.peers.insert(node_id, address);
    }

    pub fn remove_peer(&mut self, node_id: NodeId) -> Option<SocketAddr> {
        self.peers.remove(&node_id)
    }

    pub fn peer_address(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.peers.get(&node_id).copied()
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peers(&self) -> &HashMap<NodeId, SocketAddr> {
        &self.peers
    }

    pub async fn send_to_peer(
        &self,
        node_id: NodeId,
        envelope: &NetworkEnvelope,
    ) -> io::Result<()> {
        let address = self
            .peer_address(node_id)
            .ok_or_else(|| io::Error::from(TransportError::UnknownPeer(node_id)))?;
        let mut client = NetworkClient::connect(address).await?;
        client.send_envelope(envelope).await?;
        client.shutdown().await
    }

    pub async fn send_request_vote(
        &self,
        peer: NodeId,
        sender: NodeId,
        request: crate::raft::rpc::RequestVote,
    ) -> io::Result<()> {
        let envelope = NetworkEnvelope::new(sender, request.into());
        self.send_to_peer(peer, &envelope).await
    }

    pub async fn send_append_entries(
        &self,
        peer: NodeId,
        sender: NodeId,
        request: crate::raft::rpc::AppendEntries,
    ) -> io::Result<()> {
        let envelope = NetworkEnvelope::new(sender, request.into());
        self.send_to_peer(peer, &envelope).await
    }
    pub async fn send_request_vote_response(
        &self,
        peer: NodeId,
        sender: NodeId,
        response: crate::raft::rpc::RequestVoteResponse,
    ) -> io::Result<()> {
        let envelope = NetworkEnvelope::new(sender, response.into());
        self.send_to_peer(peer, &envelope).await
    }
    pub async fn send_append_entries_response(
        &self,
        peer: NodeId,
        sender: NodeId,
        response: crate::raft::rpc::AppendEntriesResponse,
    ) -> io::Result<()> {
        let envelope = NetworkEnvelope::new(sender, response.into());
        self.send_to_peer(peer, &envelope).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message::{NetworkEnvelope, NetworkMessage};
    use crate::network::server::NetworkServer;
    use crate::raft::rpc::RequestVote;
    use crate::raft::types::{NodeId, Term};
    use std::io;

    #[test]
    fn transport_stores_destination_address() {
        let address = "127.0.0.1:9000".parse().unwrap();
        let transport = NodeTransport::new(address);

        assert_eq!(transport.address(), address);
    }

    #[tokio::test]
    async fn transport_can_send_envelope() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());
        transport.add_peer(NodeId::new(2), address);

        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message);

        transport
            .send_to_peer(NodeId::new(2), &envelope)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received, envelope);
    }

    #[tokio::test]
    async fn transport_can_send_request_vote() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());
        transport.add_peer(NodeId::new(2), address);

        let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(1));

        transport
            .send_request_vote(NodeId::new(2), NodeId::new(1), request)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));
        assert_eq!(received.message(), &NetworkMessage::RequestVote(request));
    }

    #[tokio::test]
    async fn transport_can_send_append_entries() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());
        transport.add_peer(NodeId::new(2), address);

        let request = crate::raft::rpc::AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            None,
            Some(Term::ZERO),
            None,
        );

        transport
            .send_append_entries(NodeId::new(2), NodeId::new(1), request.clone())
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));
        assert_eq!(received.message(), &NetworkMessage::AppendEntries(request));
    }

    #[tokio::test]
    async fn transport_can_send_request_vote_response() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());
        transport.add_peer(NodeId::new(2), address);

        let response = crate::raft::rpc::RequestVoteResponse::granted(Term::new(1));

        transport
            .send_request_vote_response(NodeId::new(2), NodeId::new(1), response)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));
        assert_eq!(
            received.message(),
            &NetworkMessage::RequestVoteResponse(response)
        );
    }

    #[tokio::test]
    async fn transport_can_send_append_entries_response() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            connection.receive_envelope().await.unwrap()
        });

        let mut transport = NodeTransport::new("127.0.0.1:9000".parse().unwrap());
        transport.add_peer(NodeId::new(2), address);

        let response = crate::raft::rpc::AppendEntriesResponse::success(Term::new(1));

        transport
            .send_append_entries_response(NodeId::new(2), NodeId::new(1), response)
            .await
            .unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received.sender(), NodeId::new(1));
        assert_eq!(
            received.message(),
            &NetworkMessage::AppendEntriesResponse(response)
        );
    }
    #[test]
    fn transport_can_manage_peer_addresses() {
        let address = "127.0.0.1:9000".parse().unwrap();
        let peer_address = "127.0.0.1:9001".parse().unwrap();

        let mut transport = NodeTransport::new(address);

        assert_eq!(transport.peer_count(), 0);
        assert_eq!(transport.peer_address(NodeId::new(2)), None);

        transport.add_peer(NodeId::new(2), peer_address);

        assert_eq!(transport.peer_count(), 1);
        assert_eq!(transport.peer_address(NodeId::new(2)), Some(peer_address));
        assert_eq!(transport.peers().get(&NodeId::new(2)), Some(&peer_address));

        assert_eq!(transport.remove_peer(NodeId::new(2)), Some(peer_address));

        assert_eq!(transport.peer_count(), 0);
        assert_eq!(transport.peer_address(NodeId::new(2)), None);

        assert_eq!(transport.remove_peer(NodeId::new(2)), None);
    }

    #[tokio::test]
    async fn transport_rejects_unknown_peer() {
        let address = "127.0.0.1:9000".parse().unwrap();
        let transport = NodeTransport::new(address);

        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message);

        let result = transport.send_to_peer(NodeId::new(2), &envelope).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn transport_error_describes_unknown_peer() {
        let error = TransportError::UnknownPeer(NodeId::new(7));

        assert_eq!(error.to_string(), "unknown peer: 7");
    }
    #[test]
    fn transport_error_converts_to_io_error() {
        let error = TransportError::UnknownPeer(NodeId::new(7));
        let io_error: io::Error = error.into();

        assert_eq!(io_error.kind(), io::ErrorKind::NotFound);
        assert_eq!(io_error.to_string(), "unknown peer: 7");
    }
    #[test]
    fn transport_error_is_a_standard_error() {
        let error = TransportError::UnknownPeer(NodeId::new(42));
        let standard_error: &dyn std::error::Error = &error;

        assert_eq!(standard_error.to_string(), "unknown peer: 42");
    }
    #[tokio::test]
    async fn transport_returns_unknown_peer_error() {
        let address = "127.0.0.1:9000".parse().unwrap();
        let transport = NodeTransport::new(address);

        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message);

        let result = transport.send_to_peer(NodeId::new(99), &envelope).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "unknown peer: 99");
    }
}
