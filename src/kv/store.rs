use std::collections::HashMap;

use super::command::KvCommand;

/// An in-memory key-value store.
///
/// This is the current storage engine for our database.
/// Later, the Raft state machine will apply committed commands
/// to this store, and persistence will be added underneath it.
#[derive(Debug, Default)]
pub struct KvStore {
    data: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new, empty key-value store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies a state-changing command to the store.
    ///
    /// Later, the Raft state machine will call this method
    /// after a command has been committed.
    pub fn apply(&mut self, command: KvCommand) -> Option<String> {
        match command {
            KvCommand::Put { key, value } => self.put(key, value),
            KvCommand::Delete { key } => self.delete(&key),
        }
    }

    /// Inserts or replaces a value for a key.
    ///
    /// Returns the previous value if the key already existed.
    pub fn put(&mut self, key: String, value: String) -> Option<String> {
        self.data.insert(key, value)
    }

    /// Returns a copy of the value associated with a key.
    ///
    /// Returns `None` when the key does not exist.
    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).cloned()
    }

    /// Removes a key from the store.
    ///
    /// Returns the removed value if the key existed.
    pub fn delete(&mut self, key: &str) -> Option<String> {
        self.data.remove(key)
    }

    /// Returns `true` if the key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Returns the number of key-value pairs currently stored.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the store contains no entries.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Removes every key-value pair from the store.
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::{KvCommand, KvStore};

    #[test]
    fn new_store_is_empty() {
        let store = KvStore::new();

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_and_get_work() {
        let mut store = KvStore::new();

        store.put("name".to_string(), "Div".to_string());

        assert_eq!(store.get("name"), Some("Div".to_string()));
    }

    #[test]
    fn put_returns_previous_value() {
        let mut store = KvStore::new();

        assert_eq!(store.put("name".to_string(), "Div".to_string()), None);

        assert_eq!(
            store.put("name".to_string(), "Rust".to_string()),
            Some("Div".to_string())
        );

        assert_eq!(store.get("name"), Some("Rust".to_string()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let store = KvStore::new();

        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn delete_removes_value() {
        let mut store = KvStore::new();

        store.put("name".to_string(), "Div".to_string());

        assert_eq!(store.delete("name"), Some("Div".to_string()));

        assert_eq!(store.get("name"), None);
        assert!(!store.contains_key("name"));
    }

    #[test]
    fn delete_missing_key_returns_none() {
        let mut store = KvStore::new();

        assert_eq!(store.delete("missing"), None);
    }

    #[test]
    fn clear_removes_everything() {
        let mut store = KvStore::new();

        store.put("name".to_string(), "Div".to_string());
        store.put("language".to_string(), "Rust".to_string());

        assert_eq!(store.len(), 2);

        store.clear();

        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn apply_put_command_stores_value() {
        let mut store = KvStore::new();

        let result = store.apply(KvCommand::put("name", "Div"));

        assert_eq!(result, None);
        assert_eq!(store.get("name"), Some("Div".to_string()));
    }

    #[test]
    fn apply_delete_command_removes_value() {
        let mut store = KvStore::new();

        store.apply(KvCommand::put("name", "Div"));

        let result = store.apply(KvCommand::delete("name"));

        assert_eq!(result, Some("Div".to_string()));
        assert_eq!(store.get("name"), None);
    }
}
