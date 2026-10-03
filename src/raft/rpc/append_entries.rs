use serde::{Deserialize, Serialize};

use crate::{LogEntry, LogIndex, NodeId, Term};

/// A leader's AppendEntries RPC.
///
/// The message is used both for heartbeats and for replicating
/// log entries to follower nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntries {
    /// The leader's current term.
    pub term: Term,

    /// ID of the leader sending this message.
    pub leader_id: NodeId,

    /// Index of the log entry immediately preceding the new entries.
    ///
    /// `None` means there is no previous log entry.
    pub prev_log_index: Option<LogIndex>,

    /// Term of the log entry immediately preceding the new entries.
    ///
    /// `None` means there is no previous log entry.
    pub prev_log_term: Option<Term>,

    /// New log entries to replicate.
    ///
    /// An empty vector represents a heartbeat.
    pub entries: Vec<LogEntry>,

    /// The leader's commit index.
    pub leader_commit: Option<LogIndex>,
}

impl AppendEntries {
    /// Creates a new AppendEntries message.
    pub fn new(
        term: Term,
        leader_id: NodeId,
        prev_log_index: Option<LogIndex>,
        prev_log_term: Option<Term>,
        entries: Vec<LogEntry>,
        leader_commit: Option<LogIndex>,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// Creates an empty AppendEntries message.
    ///
    /// An empty message acts as a heartbeat.
    pub fn heartbeat(
        term: Term,
        leader_id: NodeId,
        prev_log_index: Option<LogIndex>,
        prev_log_term: Option<Term>,
        leader_commit: Option<LogIndex>,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }

    /// Returns whether this message is a heartbeat.
    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of entries carried by this message.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{AppendEntries, LogEntry, LogIndex, NodeId, Term};
    use crate::KvCommand;

    fn sample_entry() -> LogEntry {
        LogEntry::new(Term::new(3), KvCommand::put("name", "Div"))
    }

    #[test]
    fn append_entries_stores_term() {
        let message =
            AppendEntries::new(Term::new(5), NodeId::new(1), None, None, Vec::new(), None);

        assert_eq!(message.term, Term::new(5));
    }

    #[test]
    fn append_entries_stores_leader_id() {
        let message =
            AppendEntries::new(Term::new(5), NodeId::new(7), None, None, Vec::new(), None);

        assert_eq!(message.leader_id, NodeId::new(7));
    }

    #[test]
    fn append_entries_stores_previous_log_position() {
        let message = AppendEntries::new(
            Term::new(5),
            NodeId::new(1),
            Some(LogIndex::new(10)),
            Some(Term::new(4)),
            Vec::new(),
            None,
        );

        assert_eq!(message.prev_log_index, Some(LogIndex::new(10)));

        assert_eq!(message.prev_log_term, Some(Term::new(4)));
    }

    #[test]
    fn append_entries_stores_entries() {
        let entries = vec![sample_entry()];

        let message = AppendEntries::new(
            Term::new(5),
            NodeId::new(1),
            None,
            None,
            entries.clone(),
            None,
        );

        assert_eq!(message.entries, entries);
        assert_eq!(message.entry_count(), 1);
    }

    #[test]
    fn append_entries_stores_leader_commit() {
        let message = AppendEntries::new(
            Term::new(5),
            NodeId::new(1),
            None,
            None,
            Vec::new(),
            Some(LogIndex::new(8)),
        );

        assert_eq!(message.leader_commit, Some(LogIndex::new(8)));
    }

    #[test]
    fn empty_entries_are_a_heartbeat() {
        let message = AppendEntries::heartbeat(Term::new(5), NodeId::new(1), None, None, None);

        assert!(message.is_heartbeat());
        assert_eq!(message.entry_count(), 0);
    }

    #[test]
    fn non_empty_entries_are_not_a_heartbeat() {
        let message = AppendEntries::new(
            Term::new(5),
            NodeId::new(1),
            None,
            None,
            vec![sample_entry()],
            None,
        );

        assert!(!message.is_heartbeat());
        assert_eq!(message.entry_count(), 1);
    }

    #[test]
    fn heartbeat_preserves_previous_log_information() {
        let message = AppendEntries::heartbeat(
            Term::new(5),
            NodeId::new(1),
            Some(LogIndex::new(12)),
            Some(Term::new(4)),
            Some(LogIndex::new(10)),
        );

        assert_eq!(message.prev_log_index, Some(LogIndex::new(12)));

        assert_eq!(message.prev_log_term, Some(Term::new(4)));

        assert_eq!(message.leader_commit, Some(LogIndex::new(10)));
    }

    #[test]
    fn append_entries_can_contain_multiple_entries() {
        let entries = vec![
            LogEntry::new(Term::new(2), KvCommand::put("a", "1")),
            LogEntry::new(Term::new(2), KvCommand::put("b", "2")),
            LogEntry::new(Term::new(3), KvCommand::put("c", "3")),
        ];

        let message = AppendEntries::new(
            Term::new(3),
            NodeId::new(1),
            Some(LogIndex::new(5)),
            Some(Term::new(2)),
            entries,
            Some(LogIndex::new(4)),
        );

        assert_eq!(message.entry_count(), 3);
        assert!(!message.is_heartbeat());
    }
}
