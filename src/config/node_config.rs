use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub address: SocketAddr,
}

impl NodeConfig {
    pub const fn new(node_id: NodeId, address: SocketAddr) -> Self {
        Self { node_id, address }
    }

    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn from_toml(toml: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(toml)
    }

    pub fn load_from_file<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        Ok(Self::from_toml(&contents)?)
    }

    pub fn has_valid_port(&self) -> bool {
        self.address.port() != 0
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::new(1),
            address: "127.0.0.1:7001"
                .parse()
                .expect("default node address must be valid"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::NodeId;

    #[test]
    fn node_config_stores_node_id_and_address() {
        let address = "127.0.0.1:7001".parse().unwrap();
        let config = NodeConfig::new(NodeId::new(1), address);

        assert_eq!(config.node_id(), NodeId::new(1));
        assert_eq!(config.address(), address);
    }

    #[test]
    fn node_config_can_be_serialized_and_deserialized() {
        let address = "127.0.0.1:7001".parse().unwrap();
        let config = NodeConfig::new(NodeId::new(1), address);

        let bytes = bincode::serde::encode_to_vec(&config, bincode::config::standard()).unwrap();

        let (decoded, consumed): (NodeConfig, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, config);
    }

    #[test]
    fn node_config_can_be_loaded_from_toml() {
        let config = r#"
node_id = 1
address = "127.0.0.1:7001"
"#;

        let node = NodeConfig::from_toml(config).unwrap();

        assert_eq!(node.node_id(), NodeId::new(1));
        assert_eq!(node.address(), "127.0.0.1:7001".parse().unwrap());
    }

    #[test]
    fn node_config_can_be_loaded_from_file() {
        let path = std::env::temp_dir().join("raft_kv_store_test_node.toml");

        std::fs::write(
            &path,
            r#"
node_id = 1
address = "127.0.0.1:7001"
"#,
        )
        .unwrap();

        let node = NodeConfig::load_from_file(&path).unwrap();

        assert_eq!(node.node_id(), NodeId::new(1));
        assert_eq!(node.address(), "127.0.0.1:7001".parse().unwrap());

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn node_config_has_a_useful_default() {
        let config = NodeConfig::default();

        assert_eq!(config.node_id(), NodeId::new(1));
        assert_eq!(config.address(), "127.0.0.1:7001".parse().unwrap());
    }

    #[test]
    fn node_config_can_validate_port() {
        let valid_node = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        assert!(valid_node.has_valid_port());

        let invalid_node = NodeConfig::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap());

        assert!(!invalid_node.has_valid_port());
    }
}
