/// The three possible roles of a Raft node.
///
/// Every Raft node is always in exactly one of these states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Normal state for a node that is not currently seeking leadership.
    Follower,

    /// A node that is requesting votes in order to become leader.
    Candidate,

    /// The elected leader of the current term.
    Leader,
}

impl Role {
    /// Returns `true` if this node is a follower.
    pub const fn is_follower(self) -> bool {
        matches!(self, Self::Follower)
    }

    /// Returns `true` if this node is a candidate.
    pub const fn is_candidate(self) -> bool {
        matches!(self, Self::Candidate)
    }

    /// Returns `true` if this node is the leader.
    pub const fn is_leader(self) -> bool {
        matches!(self, Self::Leader)
    }

    /// Returns a human-readable name for the role.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        }
    }
}

impl Default for Role {
    /// A newly created Raft node always starts as a follower.
    fn default() -> Self {
        Self::Follower
    }
}

#[cfg(test)]
mod tests {
    use super::Role;

    #[test]
    fn default_role_is_follower() {
        assert_eq!(Role::default(), Role::Follower);
    }

    #[test]
    fn follower_identification_works() {
        let role = Role::Follower;

        assert!(role.is_follower());
        assert!(!role.is_candidate());
        assert!(!role.is_leader());
    }

    #[test]
    fn candidate_identification_works() {
        let role = Role::Candidate;

        assert!(!role.is_follower());
        assert!(role.is_candidate());
        assert!(!role.is_leader());
    }

    #[test]
    fn leader_identification_works() {
        let role = Role::Leader;

        assert!(!role.is_follower());
        assert!(!role.is_candidate());
        assert!(role.is_leader());
    }

    #[test]
    fn role_names_are_correct() {
        assert_eq!(Role::Follower.as_str(), "follower");
        assert_eq!(Role::Candidate.as_str(), "candidate");
        assert_eq!(Role::Leader.as_str(), "leader");
    }
}
