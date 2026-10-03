use serde::{Deserialize, Serialize};

use crate::{KvCommand, LogIndex, Term};

/// A single entry in the Raft replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// The Raft term in which this entry was created.
    term: Term,

    /// The command stored in this entry.
    command: KvCommand,
}

impl LogEntry {
    /// Creates a new log entry.
    pub const fn new(term: Term, command: KvCommand) -> Self {
        Self { term, command }
    }

    /// Returns the term associated with this entry.
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Returns the command stored in this entry.
    pub const fn command(&self) -> &KvCommand {
        &self.command
    }
}

/// The replicated log maintained by a Raft node.
#[derive(Debug, Clone, Default)]
pub struct RaftLog {
    entries: Vec<LogEntry>,
}

impl RaftLog {
    pub fn replace_suffix(&mut self, prev_log_index: Option<LogIndex>, entries: &[LogEntry]) {
        match prev_log_index {
            Some(index) => {
                let keep_len = index.value() as usize + 1;
                self.entries.truncate(keep_len);
            }
            None => {
                self.entries.clear();
            }
        }

        self.entries.extend_from_slice(entries);
    }

    pub fn entries_from(&self, index: LogIndex) -> Vec<LogEntry> {
        self.entries
            .iter()
            .skip(index.value() as usize)
            .cloned()
            .collect()
    }
    /// Creates an empty Raft log.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.entries.get(index.value() as usize)
    }

    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.entry_at(index).map(LogEntry::term)
    }

    /// Returns the number of entries in the log.
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the log contains no entries.
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Appends an entry to the end of the log.
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Returns an entry at the given one-based Raft log index.
    ///
    /// Raft log indices start at 1, while Rust vector indices
    /// start at 0, so the index is converted internally.
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        let position = index.value().checked_sub(1)?;

        self.entries.get(position as usize)
    }

    /// Returns the index of the last entry in the log.
    pub fn last_index(&self) -> Option<LogIndex> {
        if self.entries.is_empty() {
            None
        } else {
            Some(LogIndex::new(self.entries.len() as u64))
        }
    }

    /// Returns the term of the last entry in the log.
    pub fn last_term(&self) -> Option<Term> {
        self.entries.last().map(LogEntry::term)
    }

    /// Removes all entries from the log.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{KvCommand, LogEntry, LogIndex, RaftLog, Term};

    fn put_command(key: &str, value: &str) -> KvCommand {
        KvCommand::put(key, value)
    }

    #[test]
    fn new_log_is_empty() {
        let log = RaftLog::new();

        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn append_adds_entry() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        assert_eq!(log.len(), 1);
        assert!(!log.is_empty());
    }

    #[test]
    fn append_multiple_entries() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        log.append(LogEntry::new(Term::new(1), put_command("language", "Rust")));

        assert_eq!(log.len(), 2);
    }

    #[test]
    fn get_returns_entry_at_index() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        let entry = log.get(LogIndex::new(1));

        assert!(entry.is_some());
        assert_eq!(entry.unwrap().term(), Term::new(1));
    }

    #[test]
    fn get_returns_none_for_missing_index() {
        let log = RaftLog::new();

        assert!(log.get(LogIndex::new(1)).is_none());
    }

    #[test]
    fn first_entry_has_index_one() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        assert_eq!(log.last_index(), Some(LogIndex::new(1)));
    }

    #[test]
    fn last_index_tracks_entries() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        log.append(LogEntry::new(Term::new(2), put_command("language", "Rust")));

        assert_eq!(log.last_index(), Some(LogIndex::new(2)));
    }

    #[test]
    fn empty_log_has_no_last_index() {
        let log = RaftLog::new();

        assert_eq!(log.last_index(), None);
    }

    #[test]
    fn last_term_returns_latest_term() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        log.append(LogEntry::new(Term::new(3), put_command("language", "Rust")));

        assert_eq!(log.last_term(), Some(Term::new(3)));
    }

    #[test]
    fn empty_log_has_no_last_term() {
        let log = RaftLog::new();

        assert_eq!(log.last_term(), None);
    }

    #[test]
    fn clear_removes_all_entries() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), put_command("name", "Div")));

        log.clear();

        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.last_index(), None);
        assert_eq!(log.last_term(), None);
    }
    #[test]
    fn entry_at_returns_existing_entry() {
        let mut log = RaftLog::new();

        let entry = LogEntry::new(Term::new(1), KvCommand::put("name", "Div"));
        log.append(entry.clone());

        let found = log.entry_at(LogIndex::new(0));

        assert_eq!(found, Some(&entry));
    }

    #[test]
    fn entry_at_returns_none_for_missing_entry() {
        let log = RaftLog::new();

        assert_eq!(log.entry_at(LogIndex::new(0)), None);
    }

    #[test]
    fn term_at_returns_entry_term() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(3), KvCommand::put("name", "Div")));

        assert_eq!(log.term_at(LogIndex::new(0)), Some(Term::new(3)));
    }
    #[test]
    fn replace_suffix_appends_after_previous_index() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        log.append(LogEntry::new(Term::new(1), KvCommand::put("b", "2")));

        let new_entries = vec![
            LogEntry::new(Term::new(2), KvCommand::put("c", "3")),
            LogEntry::new(Term::new(2), KvCommand::put("d", "4")),
        ];

        log.replace_suffix(Some(LogIndex::new(0)), &new_entries);

        assert_eq!(log.len(), 3);
        assert_eq!(log.term_at(LogIndex::new(0)), Some(Term::new(1)));
        assert_eq!(log.term_at(LogIndex::new(1)), Some(Term::new(2)));
        assert_eq!(log.term_at(LogIndex::new(2)), Some(Term::new(2)));
    }

    #[test]
    fn replace_suffix_with_no_previous_index_replaces_entire_log() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), KvCommand::put("old", "value")));

        let new_entries = vec![LogEntry::new(Term::new(2), KvCommand::put("new", "value"))];

        log.replace_suffix(None, &new_entries);

        assert_eq!(log.len(), 1);
        assert_eq!(log.term_at(LogIndex::new(0)), Some(Term::new(2)));
    }
    #[test]
    fn entries_from_returns_entries_starting_at_index() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        log.append(LogEntry::new(Term::new(1), KvCommand::put("b", "2")));

        log.append(LogEntry::new(Term::new(2), KvCommand::put("c", "3")));

        let entries = log.entries_from(LogIndex::new(1));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].term(), Term::new(1));
        assert_eq!(entries[1].term(), Term::new(2));
    }

    #[test]
    fn entries_from_at_end_returns_empty() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        let entries = log.entries_from(LogIndex::new(1));

        assert!(entries.is_empty());
    }

    #[test]
    fn entries_from_beyond_end_returns_empty() {
        let mut log = RaftLog::new();

        log.append(LogEntry::new(Term::new(1), KvCommand::put("a", "1")));

        let entries = log.entries_from(LogIndex::new(10));

        assert!(entries.is_empty());
    }
}
