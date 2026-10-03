/// A command that changes the state of the key-value store.
///
/// These commands are the operations that Raft will eventually
/// replicate between nodes.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvCommand {
    /// Insert a new key or replace an existing value.
    Put { key: String, value: String },

    /// Remove a key from the store.
    Delete { key: String },
}

impl KvCommand {
    /// Creates a PUT command.
    pub fn put(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Creates a DELETE command.
    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete { key: key.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::KvCommand;

    #[test]
    fn creates_put_command() {
        let command = KvCommand::put("name", "Div");

        assert_eq!(
            command,
            KvCommand::Put {
                key: "name".to_string(),
                value: "Div".to_string(),
            }
        );
    }

    #[test]
    fn creates_delete_command() {
        let command = KvCommand::delete("name");

        assert_eq!(
            command,
            KvCommand::Delete {
                key: "name".to_string(),
            }
        );
    }

    #[test]
    fn commands_can_be_cloned() {
        let original = KvCommand::put("language", "Rust");
        let cloned = original.clone();

        assert_eq!(original, cloned);
    }
}
