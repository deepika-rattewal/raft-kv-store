use serde::{Deserialize, Serialize};

use std::fmt;

/// Unique identifier of a node in the Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Creates a new node identifier.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the numeric identifier.
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "node-{}", self.0)
    }
}

/// Raft term.
///
/// A term is a monotonically increasing logical period in the
/// lifetime of a Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Term(u64);

impl Term {
    /// The initial term before any election has occurred.
    pub const ZERO: Self = Self(0);

    /// Creates a term from its numeric value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric term.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the next term.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Position of an entry in the Raft log.
///
/// Log indexes are zero-based in our implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
    /// Creates a new log index.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric index.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns the next log index.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{LogIndex, NodeId, Term};

    #[test]
    fn node_id_stores_identifier() {
        let node = NodeId::new(42);

        assert_eq!(node.value(), 42);
    }

    #[test]
    fn node_id_displays_correctly() {
        let node = NodeId::new(3);

        assert_eq!(node.to_string(), "node-3");
    }

    #[test]
    fn node_ids_can_be_compared() {
        let first = NodeId::new(1);
        let second = NodeId::new(2);

        assert!(first < second);
        assert_ne!(first, second);
    }

    #[test]
    fn zero_is_initial_term() {
        assert_eq!(Term::ZERO.value(), 0);
    }

    #[test]
    fn term_can_increment() {
        let term = Term::new(4);

        assert_eq!(term.next().value(), 5);
    }

    #[test]
    fn terms_are_ordered() {
        let first = Term::new(1);
        let second = Term::new(2);

        assert!(first < second);
    }

    #[test]
    fn log_index_can_increment() {
        let index = LogIndex::new(7);

        assert_eq!(index.next().value(), 8);
    }

    #[test]
    fn log_indexes_are_ordered() {
        let first = LogIndex::new(1);
        let second = LogIndex::new(2);

        assert!(first < second);
    }
}
