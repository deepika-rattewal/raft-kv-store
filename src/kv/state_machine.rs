use super::{KvCommand, KvStore};

/// Applies committed commands to the key-value state.
///
/// In the final distributed system, Raft will be responsible for
/// deciding which commands are committed. This state machine is
/// responsible only for applying those committed commands in order.
#[derive(Debug, Default)]
pub struct KvStateMachine {
    store: KvStore,
}

impl KvStateMachine {
    /// Creates a new state machine with an empty key-value store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one committed command to the state machine.
    ///
    /// Commands must be applied in the same order on every node.
    pub fn apply(&mut self, command: KvCommand) -> Option<String> {
        self.store.apply(command)
    }

    /// Reads a value from the current state.
    pub fn get(&self, key: &str) -> Option<String> {
        self.store.get(key)
    }

    /// Returns the number of entries currently in the state.
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Returns whether the state contains no entries.
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    /// Returns whether a key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::{KvCommand, KvStateMachine};

    #[test]
    fn new_state_machine_is_empty() {
        let state_machine = KvStateMachine::new();

        assert!(state_machine.is_empty());
        assert_eq!(state_machine.len(), 0);
    }

    #[test]
    fn applies_put_command() {
        let mut state_machine = KvStateMachine::new();

        let result = state_machine.apply(KvCommand::put("name", "Div"));

        assert_eq!(result, None);
        assert_eq!(state_machine.get("name"), Some("Div".to_string()));
    }

    #[test]
    fn applies_delete_command() {
        let mut state_machine = KvStateMachine::new();

        state_machine.apply(KvCommand::put("name", "Div"));

        let result = state_machine.apply(KvCommand::delete("name"));

        assert_eq!(result, Some("Div".to_string()));
        assert_eq!(state_machine.get("name"), None);
    }

    #[test]
    fn commands_are_applied_in_order() {
        let mut state_machine = KvStateMachine::new();

        state_machine.apply(KvCommand::put("name", "first"));
        state_machine.apply(KvCommand::put("name", "second"));

        assert_eq!(state_machine.get("name"), Some("second".to_string()));
    }

    #[test]
    fn state_machine_can_handle_multiple_keys() {
        let mut state_machine = KvStateMachine::new();

        state_machine.apply(KvCommand::put("name", "Div"));
        state_machine.apply(KvCommand::put("language", "Rust"));

        assert_eq!(state_machine.len(), 2);
        assert!(state_machine.contains_key("name"));
        assert!(state_machine.contains_key("language"));
    }
}
