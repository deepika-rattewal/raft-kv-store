//! Client for communicating with a remote Raft node.

use std::io;
use std::net::SocketAddr;

use crate::network::message::NetworkMessage;
use crate::network::tcp::{TcpConnection, TcpServer};

/// A client connection to another Raft node.
#[derive(Debug)]
pub struct NetworkClient {
    connection: TcpConnection,
}

impl NetworkClient {
    /// Connects to a remote Raft node.
    pub async fn connect(address: SocketAddr) -> io::Result<Self> {
        let stream = TcpServer::connect(address).await?;
        Ok(Self {
            connection: TcpConnection::new(stream),
        })
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

    /// Receives a network message.
    pub async fn receive(&mut self) -> io::Result<NetworkMessage> {
        self.connection.receive_message().await
    }

    /// Receives a network envelope from the remote node.
    pub async fn receive_envelope(
        &mut self,
    ) -> io::Result<crate::network::message::NetworkEnvelope> {
        self.connection.receive_envelope().await
    }

    /// Returns the remote peer address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.connection.peer_addr()
    }

    /// Shuts down the connection.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.connection.shutdown().await
    }

    pub fn from_stream(stream: tokio::net::TcpStream) -> Self {
        Self {
            connection: TcpConnection::new(stream),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message::NetworkMessage;
    use crate::network::server::NetworkServer;
    use crate::network::tcp::TcpServer;
    use crate::raft::rpc::RequestVote;
    use crate::raft::types::{NodeId, Term};

    #[tokio::test]
    async fn client_can_connect_and_send_message() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let client_task = tokio::spawn(async move {
            let mut client = NetworkClient::connect(address).await.unwrap();

            let message = NetworkMessage::RequestVote(RequestVote::for_empty_log(
                Term::new(1),
                NodeId::new(2),
            ));

            client.send(&message).await.unwrap();
        });

        let (stream, _) = server.accept().await.unwrap();
        let mut connection = crate::network::tcp::TcpConnection::new(stream);

        let received = connection.receive_message().await.unwrap();

        client_task.await.unwrap();

        assert_eq!(
            received,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2),))
        );
    }

    #[tokio::test]
    async fn client_can_send_and_receive_message() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (stream, _) = server.accept().await.unwrap();
            let mut connection = TcpConnection::new(stream);

            let received = connection.receive_message().await.unwrap();

            assert_eq!(
                received,
                NetworkMessage::RequestVote(RequestVote::for_empty_log(
                    Term::new(1),
                    NodeId::new(2),
                ))
            );

            let response = NetworkMessage::RequestVote(RequestVote::for_empty_log(
                Term::new(2),
                NodeId::new(1),
            ));

            connection.send_message(&response).await.unwrap();
        });

        let mut client = NetworkClient::connect(address).await.unwrap();

        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        client.send(&message).await.unwrap();

        let response = client.receive().await.unwrap();

        server_task.await.unwrap();

        assert_eq!(
            response,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(2), NodeId::new(1),))
        );
    }

    #[tokio::test]
    async fn client_can_send_and_receive_network_envelope() {
        let server = NetworkServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut connection = server.accept().await.unwrap();
            let envelope = connection.receive_envelope().await.unwrap();

            connection.send_envelope(&envelope).await.unwrap();

            envelope
        });

        let mut client = NetworkClient::connect(address).await.unwrap();

        let message = NetworkMessage::RequestVote(crate::raft::rpc::RequestVote::for_empty_log(
            crate::raft::types::Term::new(1),
            crate::raft::types::NodeId::new(2),
        ));

        let envelope = crate::network::message::NetworkEnvelope::new(
            crate::raft::types::NodeId::new(1),
            message,
        );

        client.send_envelope(&envelope).await.unwrap();

        let received = client.receive_envelope().await.unwrap();
        let server_received = server_task.await.unwrap();

        assert_eq!(received, envelope);
        assert_eq!(server_received, envelope);
    }
}
