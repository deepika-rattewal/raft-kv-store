//! Network messages exchanged between Raft nodes.

use serde::{Deserialize, Serialize};

use std::fmt;

use crate::raft::rpc::{AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse};
use crate::raft::types::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NetworkMessageKind {
    #[default]
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
}

impl NetworkMessageKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RequestVote => "request_vote",
            Self::RequestVoteResponse => "request_vote_response",
            Self::AppendEntries => "append_entries",
            Self::AppendEntriesResponse => "append_entries_response",
        }
    }
}

impl fmt::Display for NetworkMessageKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkMessage {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkEnvelope {
    sender: NodeId,
    message: NetworkMessage,
}

impl NetworkEnvelope {
    pub fn new(sender: NodeId, message: NetworkMessage) -> Self {
        Self { sender, message }
    }

    pub const fn sender(&self) -> NodeId {
        self.sender
    }

    pub const fn message(&self) -> &NetworkMessage {
        &self.message
    }

    pub fn into_message(self) -> NetworkMessage {
        self.message
    }
}

impl NetworkMessage {
    pub const fn is_request_vote(&self) -> bool {
        matches!(self, Self::RequestVote(_))
    }

    pub const fn is_request_vote_response(&self) -> bool {
        matches!(self, Self::RequestVoteResponse(_))
    }

    pub const fn is_append_entries(&self) -> bool {
        matches!(self, Self::AppendEntries(_))
    }

    pub const fn is_append_entries_response(&self) -> bool {
        matches!(self, Self::AppendEntriesResponse(_))
    }

    pub const fn is_rpc(&self) -> bool {
        true
    }

    pub const fn kind(&self) -> NetworkMessageKind {
        match self {
            Self::RequestVote(_) => NetworkMessageKind::RequestVote,
            Self::RequestVoteResponse(_) => NetworkMessageKind::RequestVoteResponse,
            Self::AppendEntries(_) => NetworkMessageKind::AppendEntries,
            Self::AppendEntriesResponse(_) => NetworkMessageKind::AppendEntriesResponse,
        }
    }
    pub const fn kind_name(&self) -> &'static str {
        self.kind().as_str()
    }
    pub const fn request_vote(&self) -> Option<&RequestVote> {
        match self {
            Self::RequestVote(request) => Some(request),
            _ => None,
        }
    }

    pub const fn request_vote_response(&self) -> Option<&RequestVoteResponse> {
        match self {
            Self::RequestVoteResponse(response) => Some(response),
            _ => None,
        }
    }

    pub const fn append_entries(&self) -> Option<&AppendEntries> {
        match self {
            Self::AppendEntries(request) => Some(request),
            _ => None,
        }
    }

    pub const fn append_entries_response(&self) -> Option<&AppendEntriesResponse> {
        match self {
            Self::AppendEntriesResponse(response) => Some(response),
            _ => None,
        }
    }
}

impl From<RequestVote> for NetworkMessage {
    fn from(request: RequestVote) -> Self {
        Self::RequestVote(request)
    }
}

impl From<RequestVoteResponse> for NetworkMessage {
    fn from(response: RequestVoteResponse) -> Self {
        Self::RequestVoteResponse(response)
    }
}

impl From<AppendEntries> for NetworkMessage {
    fn from(request: AppendEntries) -> Self {
        Self::AppendEntries(request)
    }
}

impl From<AppendEntriesResponse> for NetworkMessage {
    fn from(response: AppendEntriesResponse) -> Self {
        Self::AppendEntriesResponse(response)
    }
}
impl From<&NetworkMessage> for NetworkMessageKind {
    fn from(message: &NetworkMessage) -> Self {
        message.kind()
    }
}

impl fmt::Display for NetworkMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.kind_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::rpc::RequestVote;
    use crate::raft::types::{LogIndex, NodeId, Term};

    #[test]
    fn request_vote_can_be_wrapped_in_network_message() {
        let request = RequestVote::for_empty_log(Term::new(1), NodeId::new(1));

        let message = NetworkMessage::RequestVote(request);

        assert_eq!(message, NetworkMessage::RequestVote(request));
    }

    #[test]
    fn request_vote_response_can_be_wrapped_in_network_message() {
        let response = RequestVoteResponse::granted(Term::new(1));

        let message = NetworkMessage::RequestVoteResponse(response);

        assert_eq!(message, NetworkMessage::RequestVoteResponse(response));
    }

    #[test]
    fn append_entries_can_be_wrapped_in_network_message() {
        let request = AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        );

        let message = NetworkMessage::AppendEntries(request.clone());

        assert_eq!(message, NetworkMessage::AppendEntries(request));
    }

    #[test]
    fn append_entries_response_can_be_wrapped_in_network_message() {
        let response = AppendEntriesResponse::success(Term::new(1));

        let message = NetworkMessage::AppendEntriesResponse(response);

        assert_eq!(message, NetworkMessage::AppendEntriesResponse(response));
    }

    #[test]
    fn network_message_identifies_its_type() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let request_vote_response =
            NetworkMessage::RequestVoteResponse(RequestVoteResponse::granted(Term::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let append_entries_response =
            NetworkMessage::AppendEntriesResponse(AppendEntriesResponse::success(Term::new(1)));

        assert!(request_vote.is_request_vote());
        assert!(!request_vote.is_request_vote_response());
        assert!(!request_vote.is_append_entries());
        assert!(!request_vote.is_append_entries_response());

        assert!(request_vote_response.is_request_vote_response());
        assert!(append_entries.is_append_entries());
        assert!(append_entries_response.is_append_entries_response());
    }

    #[test]
    fn network_message_reports_its_kind() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let request_vote_response =
            NetworkMessage::RequestVoteResponse(RequestVoteResponse::granted(Term::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let append_entries_response =
            NetworkMessage::AppendEntriesResponse(AppendEntriesResponse::success(Term::new(1)));

        assert_eq!(request_vote.kind(), NetworkMessageKind::RequestVote);
        assert_eq!(
            request_vote_response.kind(),
            NetworkMessageKind::RequestVoteResponse
        );
        assert_eq!(append_entries.kind(), NetworkMessageKind::AppendEntries);
        assert_eq!(
            append_entries_response.kind(),
            NetworkMessageKind::AppendEntriesResponse
        );
    }

    #[test]
    fn network_message_kind_has_expected_string() {
        assert_eq!(NetworkMessageKind::RequestVote.as_str(), "request_vote");
        assert_eq!(
            NetworkMessageKind::RequestVoteResponse.as_str(),
            "request_vote_response"
        );
        assert_eq!(NetworkMessageKind::AppendEntries.as_str(), "append_entries");
        assert_eq!(
            NetworkMessageKind::AppendEntriesResponse.as_str(),
            "append_entries_response"
        );
    }

    #[test]
    fn network_message_kind_implements_display() {
        assert_eq!(NetworkMessageKind::RequestVote.to_string(), "request_vote");
        assert_eq!(
            NetworkMessageKind::RequestVoteResponse.to_string(),
            "request_vote_response"
        );
        assert_eq!(
            NetworkMessageKind::AppendEntries.to_string(),
            "append_entries"
        );
        assert_eq!(
            NetworkMessageKind::AppendEntriesResponse.to_string(),
            "append_entries_response"
        );
    }
    #[test]
    fn network_message_reports_kind_name() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        assert_eq!(request_vote.kind_name(), "request_vote");
        assert_eq!(append_entries.kind_name(), "append_entries");
    }
    #[test]
    fn network_message_exposes_its_payload() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let request_vote_response =
            NetworkMessage::RequestVoteResponse(RequestVoteResponse::granted(Term::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let append_entries_response =
            NetworkMessage::AppendEntriesResponse(AppendEntriesResponse::success(Term::new(1)));

        assert!(request_vote.request_vote().is_some());
        assert!(request_vote.request_vote_response().is_none());

        assert!(request_vote_response.request_vote_response().is_some());
        assert!(request_vote_response.request_vote().is_none());

        assert!(append_entries.append_entries().is_some());
        assert!(append_entries.append_entries_response().is_none());

        assert!(append_entries_response.append_entries_response().is_some());
        assert!(append_entries_response.append_entries().is_none());
    }
    #[test]
    fn rpc_messages_can_be_converted_into_network_messages() {
        let request_vote: NetworkMessage =
            RequestVote::for_empty_log(Term::new(1), NodeId::new(1)).into();

        let request_vote_response: NetworkMessage =
            RequestVoteResponse::granted(Term::new(1)).into();

        let append_entries: NetworkMessage = AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        )
        .into();

        let append_entries_response: NetworkMessage =
            AppendEntriesResponse::success(Term::new(1)).into();

        assert!(request_vote.is_request_vote());
        assert!(request_vote_response.is_request_vote_response());
        assert!(append_entries.is_append_entries());
        assert!(append_entries_response.is_append_entries_response());
    }
    #[test]
    fn network_message_can_be_converted_to_its_kind() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let request_vote_kind = NetworkMessageKind::from(&request_vote);
        let append_entries_kind = NetworkMessageKind::from(&append_entries);

        assert_eq!(request_vote_kind, NetworkMessageKind::RequestVote);
        assert_eq!(append_entries_kind, NetworkMessageKind::AppendEntries);
    }
    #[test]
    fn network_message_kind_has_request_vote_as_default() {
        let kind = NetworkMessageKind::default();

        assert_eq!(kind, NetworkMessageKind::RequestVote);
    }
    #[test]
    fn network_message_kind_conversion_matches_kind_method() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let request_vote_response =
            NetworkMessage::RequestVoteResponse(RequestVoteResponse::granted(Term::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let append_entries_response =
            NetworkMessage::AppendEntriesResponse(AppendEntriesResponse::success(Term::new(1)));

        assert_eq!(request_vote.kind(), NetworkMessageKind::from(&request_vote));
        assert_eq!(
            request_vote_response.kind(),
            NetworkMessageKind::from(&request_vote_response)
        );
        assert_eq!(
            append_entries.kind(),
            NetworkMessageKind::from(&append_entries)
        );
        assert_eq!(
            append_entries_response.kind(),
            NetworkMessageKind::from(&append_entries_response)
        );
    }
    #[test]
    fn network_message_implements_display() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        assert_eq!(request_vote.to_string(), "request_vote");
        assert_eq!(append_entries.to_string(), "append_entries");
    }
    #[test]
    fn network_message_debug_contains_variant_name() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let debug_output = format!("{request_vote:?}");

        assert!(debug_output.contains("RequestVote"));
    }
    #[test]
    fn network_messages_are_rpc_messages() {
        let request_vote =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(1)));

        let request_vote_response =
            NetworkMessage::RequestVoteResponse(RequestVoteResponse::granted(Term::new(1)));

        let append_entries = NetworkMessage::AppendEntries(AppendEntries::heartbeat(
            Term::new(1),
            NodeId::new(1),
            Some(LogIndex::new(1)),
            Some(Term::new(1)),
            Some(LogIndex::new(0)),
        ));

        let append_entries_response =
            NetworkMessage::AppendEntriesResponse(AppendEntriesResponse::success(Term::new(1)));

        assert!(request_vote.is_rpc());
        assert!(request_vote_response.is_rpc());
        assert!(append_entries.is_rpc());
        assert!(append_entries_response.is_rpc());
    }

    #[test]
    fn network_message_can_be_serialized_and_deserialized() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let encoded = bincode::serde::encode_to_vec(&message, bincode::config::standard())
            .expect("message should serialize");

        let (decoded, _) = bincode::serde::decode_from_slice::<NetworkMessage, _>(
            &encoded,
            bincode::config::standard(),
        )
        .expect("message should deserialize");

        assert_eq!(decoded, message);
    }

    #[test]
    fn network_envelope_preserves_sender_and_message() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message.clone());

        assert_eq!(envelope.sender(), NodeId::new(1));
        assert_eq!(envelope.message(), &message);
    }

    #[test]
    fn network_envelope_can_be_consumed_into_message() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message.clone());

        assert_eq!(envelope.into_message(), message);
    }

    #[test]
    fn network_envelope_can_be_serialized_and_deserialized() {
        let message =
            NetworkMessage::RequestVote(RequestVote::for_empty_log(Term::new(1), NodeId::new(2)));

        let envelope = NetworkEnvelope::new(NodeId::new(1), message);

        let encoded = bincode::serde::encode_to_vec(&envelope, bincode::config::standard())
            .expect("envelope should serialize");

        let (decoded, _) = bincode::serde::decode_from_slice::<NetworkEnvelope, _>(
            &encoded,
            bincode::config::standard(),
        )
        .expect("envelope should deserialize");

        assert_eq!(decoded, envelope);
    }
}
