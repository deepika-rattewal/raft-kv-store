use std::fmt;

use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    EmptyCluster,
    DuplicateNodeId(NodeId),
    DuplicateAddress,
    InvalidPort,
    UnspecifiedAddress,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => write!(f, "cluster must contain at least one node"),
            Self::DuplicateNodeId(node_id) => {
                write!(f, "duplicate node id: {node_id:?}")
            }
            Self::DuplicateAddress => write!(f, "duplicate node address"),
            Self::InvalidPort => write!(f, "node address must use a non-zero port"),
            Self::UnspecifiedAddress => {
                write!(f, "node address must not be an unspecified address")
            }
        }
    }
}

impl std::error::Error for ConfigError {}
use crate::config::cluster_config::ClusterConfig;

pub fn validate_cluster(config: &ClusterConfig) -> Result<(), ConfigError> {
    if config.node_count() == 0 {
        return Err(ConfigError::EmptyCluster);
    }

    for (index, node) in config.nodes().iter().enumerate() {
        if !node.has_valid_port() {
            return Err(ConfigError::InvalidPort);
        }

        if node.address().ip().is_unspecified() {
            return Err(ConfigError::UnspecifiedAddress);
        }

        for other in config.nodes().iter().skip(index + 1) {
            if node.node_id() == other.node_id() {
                return Err(ConfigError::DuplicateNodeId(node.node_id()));
            }

            if node.address() == other.address() {
                return Err(ConfigError::DuplicateAddress);
            }
        }
    }

    Ok(())
}

pub fn has_odd_cluster_size(config: &ClusterConfig) -> bool {
    config.node_count() % 2 == 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::cluster_config::ClusterConfig;
    use crate::config::node_config::NodeConfig;
    use crate::raft::types::NodeId;

    #[test]
    fn empty_cluster_is_rejected() {
        let cluster = ClusterConfig::new(Vec::new());

        assert_eq!(validate_cluster(&cluster), Err(ConfigError::EmptyCluster));
    }

    #[test]
    fn valid_cluster_is_accepted() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(validate_cluster(&cluster), Ok(()));
    }

    #[test]
    fn duplicate_node_id_is_rejected() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(
            validate_cluster(&cluster),
            Err(ConfigError::DuplicateNodeId(NodeId::new(1)))
        );
    }

    #[test]
    fn duplicate_address_is_rejected() {
        let address = "127.0.0.1:7001".parse().unwrap();

        let node1 = NodeConfig::new(NodeId::new(1), address);
        let node2 = NodeConfig::new(NodeId::new(2), address);

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(
            validate_cluster(&cluster),
            Err(ConfigError::DuplicateAddress)
        );
    }

    #[test]
    fn invalid_cluster_toml_is_rejected() {
        let config = r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 1
address = "127.0.0.1:7002"
"#;

        let cluster = ClusterConfig::from_toml(config).unwrap();

        assert_eq!(
            validate_cluster(&cluster),
            Err(ConfigError::DuplicateNodeId(NodeId::new(1)))
        );
    }

    #[test]
    fn cluster_size_can_be_checked_for_oddness() {
        let one_node = ClusterConfig::new(vec![NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        )]);

        assert!(has_odd_cluster_size(&one_node));

        let two_nodes = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
        ]);

        assert!(!has_odd_cluster_size(&two_nodes));

        let three_nodes = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
            NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap()),
        ]);

        assert!(has_odd_cluster_size(&three_nodes));
    }

    #[test]
    fn invalid_port_is_rejected() {
        let config = ClusterConfig::new(vec![NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:0".parse().unwrap(),
        )]);

        assert_eq!(validate_cluster(&config), Err(ConfigError::InvalidPort));
    }

    #[test]
    fn unspecified_address_is_rejected() {
        let config = ClusterConfig::new(vec![NodeConfig::new(
            NodeId::new(1),
            "0.0.0.0:7001".parse().unwrap(),
        )]);

        assert_eq!(
            validate_cluster(&config),
            Err(ConfigError::UnspecifiedAddress)
        );
    }
}
