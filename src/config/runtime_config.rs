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
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_node_and_cluster_configuration() {
        let node = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![
            node.clone(),
            NodeConfig::new(
                NodeId::new(2),
                "127.0.0.1:7002".parse().unwrap(),
            ),
        ]);

        let config = RuntimeConfig::new(node.clone(), cluster.clone());

        assert_eq!(config.node(), &node);
        assert_eq!(config.cluster(), &cluster);
    }

    #[test]
    fn returns_local_node_id() {
        let node = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node.clone()]);

        let config = RuntimeConfig::new(node, cluster);

        assert_eq!(config.node_id(), NodeId::new(1));
    }

    #[test]
    fn returns_peer_nodes() {
        let node = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        );

        let peer = NodeConfig::new(
            NodeId::new(2),
            "127.0.0.1:7002".parse().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node.clone(), peer.clone()]);

        let config = RuntimeConfig::new(node, cluster);

        let peers = config.peer_nodes();

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0], &peer);
    }
}
