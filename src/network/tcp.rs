use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::network::codec;
use crate::network::message::NetworkMessage;

const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

/// TCP server responsible for accepting incoming connections.
#[derive(Debug)]
pub struct TcpServer {
    listener: TcpListener,
}

impl TcpServer {
    /// Bind a TCP server to the provided address.
    pub async fn bind(address: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await?;

        Ok(Self { listener })
    }

    /// Return the local address the server is listening on.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept the next incoming TCP connection.
    pub async fn accept(&self) -> io::Result<(tokio::net::TcpStream, SocketAddr)> {
        self.listener.accept().await
    }

    /// Connect to another Raft node.
    pub async fn connect(address: SocketAddr) -> io::Result<tokio::net::TcpStream> {
        tokio::net::TcpStream::connect(address).await
    }
}

/// A TCP connection used for framed message communication.
#[derive(Debug)]
pub struct TcpConnection {
    stream: TcpStream,
}

/// A length-prefixed network frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    payload: Vec<u8>,
}

impl Frame {
    /// Create a frame from raw payload bytes.
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    pub fn from_slice(payload: &[u8]) -> Self {
        Self::new(payload.to_vec())
    }

    /// Return the frame payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub const fn len(&self) -> usize {
        self.payload.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    /// Consume the frame and return its payload.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl TcpConnection {
    /// Create a connection from an established TCP stream.
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    /// Send one length-prefixed frame.
    pub async fn send_frame(&mut self, frame: &Frame) -> io::Result<()> {
        let length = u32::try_from(frame.payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame is too large"))?;

        self.stream.write_all(&length.to_be_bytes()).await?;
        self.stream.write_all(&frame.payload).await?;
        self.stream.flush().await?;

        Ok(())
    }

    /// Receive one length-prefixed frame.
    pub async fn receive_frame(&mut self) -> io::Result<Frame> {
        let mut length_bytes = [0_u8; 4];

        self.stream.read_exact(&mut length_bytes).await?;

        let length = u32::from_be_bytes(length_bytes) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame is too large",
            ));
        }

        let mut payload = vec![0_u8; length];

        self.stream.read_exact(&mut payload).await?;

        Ok(Frame::new(payload))
    }

    pub async fn send_message(&mut self, message: &NetworkMessage) -> io::Result<()> {
        let payload = codec::encode(message)?;
        self.send_frame(&Frame::new(payload)).await
    }

    /// Sends a network envelope over this connection.
    pub async fn send_envelope(
        &mut self,
        envelope: &crate::network::message::NetworkEnvelope,
    ) -> io::Result<()> {
        let payload = codec::encode_envelope(envelope)?;
        self.send_frame(&Frame::new(payload)).await
    }

    pub async fn receive_message(&mut self) -> io::Result<NetworkMessage> {
        let frame = self.receive_frame().await?;
        codec::decode(frame.payload())
    }

    /// Receives a network envelope from this connection.
    pub async fn receive_envelope(
        &mut self,
    ) -> io::Result<crate::network::message::NetworkEnvelope> {
        let frame = self.receive_frame().await?;
        codec::decode_envelope(frame.payload())
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_can_bind_to_localhost() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        assert_eq!(
            address.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        assert_ne!(address.port(), 0);
    }
    #[tokio::test]
    async fn server_can_accept_connection() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (_connection, peer_address) = server
            .accept()
            .await
            .expect("server should accept connection");

        assert_eq!(
            peer_address.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );

        drop(client);
    }

    #[tokio::test]
    async fn connection_can_send_and_receive_frame() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut client = TcpConnection::new(client_stream);
        let mut server_connection = TcpConnection::new(server_stream);

        let message = b"hello raft";
        let frame = Frame::new(message.to_vec());

        client
            .send_frame(&frame)
            .await
            .expect("client should send frame");

        let received = server_connection
            .receive_frame()
            .await
            .expect("server should receive frame");

        assert_eq!(received.payload(), message);
    }
    #[tokio::test]
    async fn connection_rejects_oversized_frame() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let mut client = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut server_connection = TcpConnection::new(server_stream);

        let oversized_length = (MAX_FRAME_SIZE as u32) + 1;

        client
            .write_all(&oversized_length.to_be_bytes())
            .await
            .expect("client should send frame length");

        let result = server_connection.receive_frame().await;

        assert!(result.is_err());

        let error = result.expect_err("oversized frame should be rejected");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn frame_can_be_consumed_into_payload() {
        let payload = vec![1_u8, 2, 3, 4];
        let frame = Frame::new(payload.clone());

        assert_eq!(frame.into_payload(), payload);
    }

    #[tokio::test]
    async fn connection_can_send_multiple_frames() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut client = TcpConnection::new(client_stream);
        let mut server_connection = TcpConnection::new(server_stream);

        let first_frame = Frame::new(b"first message".to_vec());
        let second_frame = Frame::new(b"second message".to_vec());

        client
            .send_frame(&first_frame)
            .await
            .expect("first frame should be sent");

        client
            .send_frame(&second_frame)
            .await
            .expect("second frame should be sent");

        let first_received = server_connection
            .receive_frame()
            .await
            .expect("first frame should be received");

        let second_received = server_connection
            .receive_frame()
            .await
            .expect("second frame should be received");

        assert_eq!(first_received.payload(), b"first message");
        assert_eq!(second_received.payload(), b"second message");
    }
    #[test]
    fn frame_can_contain_empty_payload() {
        let frame = Frame::new(Vec::new());

        assert!(frame.payload().is_empty());
        assert_eq!(frame.into_payload(), Vec::<u8>::new());
    }
    #[tokio::test]
    async fn connection_preserves_frame_order() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut client = TcpConnection::new(client_stream);
        let mut server_connection = TcpConnection::new(server_stream);

        for number in 1_u8..=5 {
            let frame = Frame::new(vec![number]);

            client
                .send_frame(&frame)
                .await
                .expect("frame should be sent");
        }

        for expected in 1_u8..=5 {
            let received = server_connection
                .receive_frame()
                .await
                .expect("frame should be received");

            assert_eq!(received.payload(), &[expected]);
        }
    }
    #[tokio::test]
    async fn connection_can_shutdown() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut client = TcpConnection::new(client_stream);
        let _server_connection = TcpConnection::new(server_stream);

        client
            .shutdown()
            .await
            .expect("connection should shut down");
    }
    #[tokio::test]
    async fn connection_can_send_and_receive_empty_frame() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let mut client = TcpConnection::new(client_stream);
        let mut server_connection = TcpConnection::new(server_stream);

        let frame = Frame::new(Vec::new());

        client
            .send_frame(&frame)
            .await
            .expect("empty frame should be sent");

        let received = server_connection
            .receive_frame()
            .await
            .expect("empty frame should be received");

        assert!(received.payload().is_empty());
    }

    #[tokio::test]
    async fn connection_reports_peer_address() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let client = TcpConnection::new(client_stream);
        let server_connection = TcpConnection::new(server_stream);

        let client_peer = client
            .peer_addr()
            .expect("client should have a peer address");

        let server_peer = server_connection
            .peer_addr()
            .expect("server should have a peer address");

        assert_eq!(
            client_peer.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );

        assert_eq!(
            server_peer.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
    }
    #[tokio::test]
    async fn connection_reports_local_address() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .expect("server should bind");

        let address = server
            .local_addr()
            .expect("server should have a local address");

        let client_stream = TcpServer::connect(address)
            .await
            .expect("client should connect");

        let (server_stream, _) = server
            .accept()
            .await
            .expect("server should accept connection");

        let client = TcpConnection::new(client_stream);
        let server_connection = TcpConnection::new(server_stream);

        let client_local = client
            .local_addr()
            .expect("client should have a local address");

        let server_local = server_connection
            .local_addr()
            .expect("server should have a local address");

        assert_eq!(
            client_local.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );

        assert_eq!(
            server_local.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );

        assert_ne!(client_local.port(), 0);
        assert_ne!(server_local.port(), 0);
    }
    #[test]
    fn frame_reports_payload_length() {
        let payload = vec![1_u8, 2, 3, 4, 5];
        let frame = Frame::new(payload);

        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn frame_reports_whether_payload_is_empty() {
        let empty_frame = Frame::new(Vec::new());
        let non_empty_frame = Frame::new(vec![1_u8, 2, 3]);

        assert!(empty_frame.is_empty());
        assert!(!non_empty_frame.is_empty());
    }

    #[test]
    fn frame_can_be_created_from_slice() {
        let payload = [10_u8, 20, 30, 40];
        let frame = Frame::from_slice(&payload);

        assert_eq!(frame.payload(), &payload);
        assert_eq!(frame.len(), 4);
    }

    #[tokio::test]
    async fn connection_can_send_and_receive_network_message() {
        use crate::network::message::NetworkMessage;
        use crate::raft::rpc::RequestVote;
        use crate::raft::types::{NodeId, Term};

        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let client_task = tokio::spawn(async move {
            let stream = TcpServer::connect(address).await.unwrap();
            let mut connection = TcpConnection::new(stream);

            let message = NetworkMessage::RequestVote(RequestVote::for_empty_log(
                Term::new(1),
                NodeId::new(2),
            ));

            connection.send_message(&message).await.unwrap();
        });

        let (stream, _) = server.accept().await.unwrap();
        let mut connection = TcpConnection::new(stream);

        let received = connection.receive_message().await.unwrap();

        client_task.await.unwrap();

        assert_eq!(
            received,
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2),))
        );
    }
    #[tokio::test]
    async fn connection_can_send_and_receive_network_envelope() {
        let server = TcpServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let address = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (stream, _) = server.accept().await.unwrap();
            let mut connection = TcpConnection::new(stream);

            connection.receive_envelope().await.unwrap()
        });

        let stream = TcpServer::connect(address).await.unwrap();
        let mut connection = TcpConnection::new(stream);

        let message = NetworkMessage::RequestVote(crate::raft::rpc::RequestVote::for_empty_log(
            crate::raft::types::Term::new(1),
            crate::raft::types::NodeId::new(2),
        ));

        let envelope = crate::network::message::NetworkEnvelope::new(
            crate::raft::types::NodeId::new(1),
            message,
        );

        connection.send_envelope(&envelope).await.unwrap();

        let received = server_task.await.unwrap();

        assert_eq!(received, envelope);
    }
}
