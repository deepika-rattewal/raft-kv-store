use serde::{Deserialize, Serialize};

use crate::config::{ClusterConfig, NodeConfig};
use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    node: NodeConfig,
    cluster: ClusterConfig,
}

impl RuntimeConfig {
    pub fn new(node: NodeConfig, cluster: ClusterConfig) -> Self {
        Self { node, cluster }
    }

    pub const fn node(&self) -> &NodeConfig {
        &self.node
    }

    pub const fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    pub const fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    pub fn peer_nodes(&self) -> Vec<&NodeConfig> {
        self.cluster.peer_nodes(self.node_id())
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
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_node_and_cluster_configuration() {
        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let cluster = ClusterConfig::new(vec![
            node.clone(),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
        ]);

        let config = RuntimeConfig::new(node.clone(), cluster.clone());

        assert_eq!(config.node(), &node);
        assert_eq!(config.cluster(), &cluster);
    }

    #[test]
    fn returns_local_node_id() {
        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node.clone()]);

        let config = RuntimeConfig::new(node, cluster);

        assert_eq!(config.node_id(), NodeId::new(1));
    }

    #[test]
    fn returns_peer_nodes() {
        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let peer = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node.clone(), peer.clone()]);

        let config = RuntimeConfig::new(node, cluster);

        let peers = config.peer_nodes();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], &peer);
    }

    #[test]
fn loads_from_toml() {
    let toml = r#"
[node]
node_id = 1
address = "127.0.0.1:7001"

[cluster]

[[cluster.nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[cluster.nodes]]
node_id = 2
address = "127.0.0.1:7002"
"#;

    let config = RuntimeConfig::from_toml(toml)
        .expect("runtime configuration should load from TOML");

    assert_eq!(config.node_id(), NodeId::new(1));
    assert_eq!(config.node().address(), "127.0.0.1:7001".parse().unwrap());
    assert_eq!(config.cluster().node_count(), 2);
    assert_eq!(config.peer_nodes().len(), 1);
}

#[test]
fn loads_from_file() {
    let toml = r#"
[node]
node_id = 1
address = "127.0.0.1:7001"

[cluster]

[[cluster.nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[cluster.nodes]]
node_id = 2
address = "127.0.0.1:7002"
"#;

    let path = std::env::temp_dir().join("raft_kv_runtime_config_test.toml");

    std::fs::write(&path, toml).expect("test configuration should be written");

    let config = RuntimeConfig::load_from_file(&path)
        .expect("runtime configuration should load from file");

    assert_eq!(config.node_id(), NodeId::new(1));
    assert_eq!(config.cluster().node_count(), 2);

    std::fs::remove_file(&path).expect("test configuration should be removed");
}
}
