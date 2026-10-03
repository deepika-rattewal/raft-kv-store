//! High-level server for receiving Raft network messages.

use std::io;
use std::net::SocketAddr;

use crate::network::message::NetworkMessage;
use crate::network::tcp::{TcpConnection, TcpServer};

use crate::raft::election::state::ElectionState;
use crate::raft::node::RaftNode;
use crate::raft::replication::ReplicationState;
use crate::raft::types::LogIndex;

/// High-level network server for Raft nodes.
#[derive(Debug)]
pub struct NetworkServer {
    server: TcpServer,
}

impl NetworkServer {
    /// Binds the server to the specified address.
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            server: TcpServer::bind(address).await?,
        })
    }

    /// Returns the address the server is listening on.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.server.local_addr()
    }

    /// Accepts the next incoming connection.
    pub async fn accept(&self) -> io::Result<NetworkConnection> {
        let (stream, peer_addr) = self.server.accept().await?;

        Ok(NetworkConnection {
            connection: TcpConnection::new(stream),
            peer_addr,
        })
    }
    pub async fn accept_and_receive(&self) -> std::io::Result<NetworkConnection> {
        self.accept().await
    }

    pub async fn process_one_message(
        &self,
        node: &mut RaftNode,
        replicated_index: LogIndex,
        election: &mut ElectionState,
        replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        let mut connection = self.accept_and_receive().await?;

        node.receive_network_message(&mut connection, replicated_index, election, replication)
            .await
    }

    pub async fn process_messages(
        &self,
        node: &mut RaftNode,
        message_count: usize,
        replicated_index: LogIndex,
        election: &mut ElectionState,
        replication: &mut ReplicationState,
    ) -> std::io::Result<()> {
        for _ in 0..message_count {
            self.process_one_message(node, replicated_index, election, replication)
                .await?;
        }

        Ok(())
    }
}

/// A high-level connection to another Raft node.
#[derive(Debug)]
pub struct NetworkConnection {
    connection: TcpConnection,
    peer_addr: SocketAddr,
}

impl NetworkConnection {
    /// Returns the remote peer address.
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Receives the next network message.
    pub async fn receive(&mut self) -> io::Result<NetworkMessage> {
        self.connection.receive_message().await
    }

    /// Receives a network envelope from the remote node.
    pub async fn receive_envelope(
        &mut self,
    ) -> io::Result<crate::network::message::NetworkEnvelope> {
        self.connection.receive_envelope().await
    }

    /// Sends a network message.
    pub async fn send(&mut self, message: &NetworkMessage) -> io::Result<()> {
        self.connection.send_message(message).await
    }

    /// Sends a network envelope to the remote node.
    pub async fn send_envelope(
        &mut self,
        envelope: &crate::network::message::NetworkEnvelope,
    ) -> io::Result<()> {
        self.connection.send_envelope(envelope).await
    }

    /// Shuts down the connection.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.connection.shutdown().await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message::NetworkMessage;
    use crate::raft::rpc::RequestVote;
    use crate::raft::types::{NodeId, Term};

    #[tokio::test]
    async fn server_can_bind_and_report_address() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        assert_eq!(address.ip().to_string(), "127.0.0.1");
        assert_ne!(address.port(), 0);
    }

    #[tokio::test]
    async fn server_can_accept_and_receive_message() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let client_task = tokio::spawn(async move {
            let mut client = crate::network::client::NetworkClient::connect(address)
                .await
                .unwrap();

            let message = NetworkMessage::RequestVote(RequestVote::for_empty_log(
                Term::new(1),
                NodeId::new(2),
            ));

            client.send(&message).await.unwrap();
        });

        let mut connection = server.accept().await.unwrap();

        let received = connection.receive().await.unwrap();

        client_task.await.unwrap();

        assert_eq!(
            received,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2),))
        );
    }

    #[tokio::test]
    async fn server_connection_can_send_response() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let client_task = tokio::spawn(async move {
            let mut client = crate::network::client::NetworkClient::connect(address)
                .await
                .unwrap();

            let request = NetworkMessage::RequestVote(RequestVote::for_empty_log(
                Term::new(1),
                NodeId::new(2),
            ));

            client.send(&request).await.unwrap();

            client.receive().await.unwrap()
        });

        let mut connection = server.accept().await.unwrap();

        let received = connection.receive().await.unwrap();

        assert_eq!(
            received,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2),))
        );

        let response =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(2), NodeId::new(1)));

        connection.send(&response).await.unwrap();

        let received_response = client_task.await.unwrap();

        assert_eq!(
            received_response,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(2), NodeId::new(1),))
        );
    }

    #[tokio::test]
    async fn server_connection_can_send_and_receive_network_envelope() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let client_task = tokio::spawn(async move {
            let stream = TcpServer::connect(address).await.unwrap();
            let mut connection = TcpConnection::new(stream);

            let envelope = connection.receive_envelope().await.unwrap();

            connection.send_envelope(&envelope).await.unwrap();

            envelope
        });

        let mut connection = server.accept().await.unwrap();

        let message = NetworkMessage::RequestVote(crate::raft::rpc::RequestVote::for_empty_log(
            crate::raft::types::Term::new(1),
            crate::raft::types::NodeId::new(2),
        ));

        let envelope = crate::network::message::NetworkEnvelope::new(
            crate::raft::types::NodeId::new(1),
            message,
        );

        connection.send_envelope(&envelope).await.unwrap();

        let received = connection.receive_envelope().await.unwrap();
        let client_received = client_task.await.unwrap();

        assert_eq!(received, envelope);
        assert_eq!(client_received, envelope);
    }

    #[tokio::test]
    async fn server_can_accept_and_receive_connection() {
        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(address).await.unwrap();
            crate::network::tcp::TcpConnection::new(stream)
        });

        let connection = server.accept_and_receive().await.unwrap();

        assert!(connection.peer_addr().ip().is_loopback());

        let _client_connection = client.await.unwrap();
    }

    #[tokio::test]
    async fn connection_can_receive_network_envelope() {
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{LogIndex, NodeId, Term};

        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut connection = crate::network::client::NetworkClient::connect(address)
                .await
                .unwrap();

            let request = RequestVote::new(
                Term::new(1),
                NodeId::new(1),
                Some(LogIndex::new(0)),
                Some(Term::ZERO),
            );

            let envelope =
                NetworkEnvelope::new(NodeId::new(1), NetworkMessage::RequestVote(request));

            connection.send_envelope(&envelope).await.unwrap();
        });

        let mut connection = server.accept_and_receive().await.unwrap();

        let envelope = connection.receive_envelope().await.unwrap();

        assert_eq!(envelope.sender(), NodeId::new(1));
        assert!(matches!(envelope.message(), NetworkMessage::RequestVote(_)));

        client.await.unwrap();
    }

    #[tokio::test]
    async fn connection_can_send_network_envelope() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::rpc::RequestVoteResponse;
        use crate::raft::types::{NodeId, Term};

        let address = "127.0.0.1:0".parse().unwrap();
        let server = NetworkServer::bind(address).await.unwrap();
        let address = server.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut client = NetworkClient::connect(address).await.unwrap();

            let envelope = client.receive_envelope().await.unwrap();

            assert_eq!(envelope.sender(), NodeId::new(1));
            assert!(matches!(
                envelope.message(),
                NetworkMessage::RequestVoteResponse(_)
            ));
        });

        let mut connection = server.accept_and_receive().await.unwrap();

        let response = RequestVoteResponse::granted(Term::new(1));

        let envelope = NetworkEnvelope::new(
            NodeId::new(1),
            NetworkMessage::RequestVoteResponse(response),
        );

        connection.send_envelope(&envelope).await.unwrap();

        client.await.unwrap();
    }

    #[tokio::test]
    async fn server_can_process_one_raft_message() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::election::state::ElectionState;
        use crate::raft::node::RaftNode;
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

        server
            .process_one_message(&mut node, LogIndex::new(0), &mut election, &mut replication)
            .await
            .unwrap();

        client.await.unwrap();
    }

    #[tokio::test]
    async fn server_can_process_multiple_raft_messages() {
        use crate::network::client::NetworkClient;
        use crate::network::message::{NetworkEnvelope, NetworkMessage};
        use crate::raft::election::state::ElectionState;
        use crate::raft::node::RaftNode;
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

        server
            .process_messages(
                &mut node,
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
