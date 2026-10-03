//! Serialization and deserialization for network messages.

use std::io;

use crate::network::message::NetworkMessage;

const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Encodes a network message into bytes.
pub fn encode(message: &NetworkMessage) -> io::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(message, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Decodes a network message from bytes.
pub fn decode(bytes: &[u8]) -> io::Result<NetworkMessage> {
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message is too large",
        ));
    }

    let (message, consumed) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    if consumed != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes after network message",
        ));
    }

    Ok(message)
}

/// Encodes a network envelope into bytes.
pub fn encode_envelope(envelope: &crate::network::message::NetworkEnvelope) -> io::Result<Vec<u8>> {
    bincode::serde::encode_to_vec(envelope, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Decodes a network envelope from bytes.
pub fn decode_envelope(bytes: &[u8]) -> io::Result<crate::network::message::NetworkEnvelope> {
    if bytes.len() > MAX_MESSAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message is too large",
        ));
    }

    let (envelope, consumed) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    if consumed != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes after network envelope",
        ));
    }

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::message::NetworkMessage;
    use crate::raft::rpc::RequestVote;
    use crate::raft::types::{NodeId, Term};

    #[test]
    fn network_message_can_be_encoded_and_decoded() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let encoded = encode(&message).expect("message should encode");
        let decoded = decode(&encoded).expect("message should decode");

        assert_eq!(decoded, message);
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let mut encoded = encode(&message).expect("message should encode");
        encoded.push(0);

        let result = decode(&encoded);

        assert!(result.is_err());
    }

    #[test]
    fn oversized_message_is_rejected() {
        let bytes = vec![0_u8; MAX_MESSAGE_SIZE + 1];

        let result = decode(&bytes);

        assert!(result.is_err());
    }

    #[test]
    fn network_envelope_can_be_encoded_and_decoded() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = crate::network::message::NetworkEnvelope::new(NodeId::new(1), message);

        let encoded = encode_envelope(&envelope).expect("envelope should encode");
        let decoded = decode_envelope(&encoded).expect("envelope should decode");

        assert_eq!(decoded, envelope);
    }

    #[test]
    fn envelope_trailing_bytes_are_rejected() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = crate::network::message::NetworkEnvelope::new(NodeId::new(1), message);

        let mut encoded = encode_envelope(&envelope).expect("envelope should encode");
        encoded.push(0);

        let result = decode_envelope(&encoded);

        assert!(result.is_err());
    }

    #[test]
    fn oversized_envelope_is_rejected() {
        let bytes = vec![0_u8; MAX_MESSAGE_SIZE + 1];

        let result = decode_envelope(&bytes);

        assert!(result.is_err());
    }
}
