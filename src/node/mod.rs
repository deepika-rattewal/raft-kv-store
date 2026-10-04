use crate::config::RuntimeConfig;
use crate::network::transport::NodeTransport;
use crate::raft::node::RaftNode;

#[derive(Debug)]
pub struct NodeRuntime {
    config: RuntimeConfig,
    raft: RaftNode,
    transport: NodeTransport,
}

impl NodeRuntime {
    pub fn new(config: RuntimeConfig) -> Self {
        let node_id = config.node_id();
        let raft = RaftNode::new(node_id);

        let transport = NodeTransport::from_cluster_config(config.cluster(), node_id)
            .expect("local node must exist in cluster configuration");

        Self {
            config,
            raft,
            transport,
        }
    }

    pub const fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub const fn raft(&self) -> &RaftNode {
        &self.raft
    }

    pub fn raft_mut(&mut self) -> &mut RaftNode {
        &mut self.raft
    }

    pub const fn transport(&self) -> &NodeTransport {
        &self.transport
    }

    pub const fn transport_mut(&mut self) -> &mut NodeTransport {
        &mut self.transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, NodeConfig};
    use crate::raft::types::NodeId;
    use std::net::SocketAddr;

    fn test_config() -> RuntimeConfig {
        let node = NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse::<SocketAddr>().unwrap(),
        );

        let peer = NodeConfig::new(
            NodeId::new(2),
            "127.0.0.1:7002".parse::<SocketAddr>().unwrap(),
        );

        let cluster = ClusterConfig::new(vec![node.clone(), peer]);

        RuntimeConfig::new(node, cluster)
    }

    #[test]
    fn creates_runtime_with_config() {
        let config = test_config();
        let runtime = NodeRuntime::new(config.clone());

        assert_eq!(runtime.config(), &config);
    }

    #[test]
    fn creates_raft_node_with_configured_node_id() {
        let config = test_config();
        let runtime = NodeRuntime::new(config);

        assert_eq!(runtime.raft().id(), NodeId::new(1));
    }

    #[test]
    fn exposes_mutable_raft_node() {
        let config = test_config();
        let mut runtime = NodeRuntime::new(config);

        runtime.raft_mut().start_election();

        assert!(runtime.raft().role().is_candidate());
    }

    #[test]
    fn creates_transport_with_cluster_peers() {
        let config = test_config();
        let runtime = NodeRuntime::new(config);

        assert_eq!(runtime.transport().peer_count(), 1);
        assert_eq!(
            runtime.transport().peer_address(NodeId::new(2)),
            Some("127.0.0.1:7002".parse().unwrap())
        );
    }
}
