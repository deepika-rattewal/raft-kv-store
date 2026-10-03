pub mod cluster_config;
pub mod node_config;
pub mod validation;

pub use cluster_config::ClusterConfig;
pub use node_config::NodeConfig;
pub use validation::{ConfigError, has_odd_cluster_size, validate_cluster};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_config_api_is_available() {
        let node = NodeConfig::new(
            crate::raft::types::NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node]);

        assert_eq!(cluster.node_count(), 1);
        assert_eq!(validate_cluster(&cluster), Ok(()));
    }

    #[test]
    fn public_config_api_exposes_cluster_size_validation() {
        let node = NodeConfig::new(
            crate::raft::types::NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node]);

        assert!(has_odd_cluster_size(&cluster));
    }
}
