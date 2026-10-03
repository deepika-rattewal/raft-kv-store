use serde::{Deserialize, Serialize};

use crate::config::node_config::NodeConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    nodes: Vec<NodeConfig>,
}

impl ClusterConfig {
    pub fn new(nodes: Vec<NodeConfig>) -> Self {
        Self { nodes }
    }

    pub fn try_new(nodes: Vec<NodeConfig>) -> Result<Self, crate::config::validation::ConfigError> {
        let config = Self::new(nodes);
        config.validate()?;
        Ok(config)
    }

    pub fn nodes(&self) -> &[NodeConfig] {
        &self.nodes
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn find_node(&self, node_id: crate::raft::types::NodeId) -> Option<&NodeConfig> {
        self.nodes.iter().find(|node| node.node_id() == node_id)
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

    pub fn load_and_validate_from_file<P: AsRef<std::path::Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster = Self::load_from_file(path)?;
        crate::config::validation::validate_cluster(&cluster)
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error>)?;
        Ok(cluster)
    }

    pub fn validate(&self) -> Result<(), crate::config::validation::ConfigError> {
        crate::config::validation::validate_cluster(self)
    }

    pub fn load_and_validate_from_toml(toml: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster = Self::from_toml(toml)?;
        cluster.validate()?;
        Ok(cluster)
    }

    pub fn contains_node(&self, node_id: crate::raft::types::NodeId) -> bool {
        self.find_node(node_id).is_some()
    }

    pub fn node_ids(&self) -> Vec<crate::raft::types::NodeId> {
        self.nodes.iter().map(NodeConfig::node_id).collect()
    }

    pub fn peer_nodes(&self, node_id: crate::raft::types::NodeId) -> Vec<&NodeConfig> {
        self.nodes
            .iter()
            .filter(|node| node.node_id() != node_id)
            .collect()
    }

    pub fn quorum_size(&self) -> usize {
        self.node_count() / 2 + 1
    }

    pub fn is_single_node(&self) -> bool {
        self.node_count() == 1
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::new(vec![NodeConfig::default()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::NodeId;

    #[test]
    fn cluster_config_stores_nodes() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1.clone(), node2.clone()]);

        assert_eq!(cluster.node_count(), 2);
        assert_eq!(cluster.nodes(), &[node1, node2]);
    }

    #[test]
    fn cluster_config_can_find_node_by_id() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(
            cluster.find_node(NodeId::new(2)).unwrap().address(),
            "127.0.0.1:7002".parse().unwrap()
        );

        assert!(cluster.find_node(NodeId::new(99)).is_none());
    }

    #[test]
    fn cluster_config_can_be_loaded_from_toml() {
        let config = r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 2
address = "127.0.0.1:7002"
"#;

        let cluster = ClusterConfig::from_toml(config).unwrap();

        assert_eq!(cluster.node_count(), 2);
        assert_eq!(
            cluster.find_node(NodeId::new(1)).unwrap().address(),
            "127.0.0.1:7001".parse().unwrap()
        );
        assert_eq!(
            cluster.find_node(NodeId::new(2)).unwrap().address(),
            "127.0.0.1:7002".parse().unwrap()
        );
    }

    #[test]
    fn cluster_config_can_be_loaded_from_file() {
        let path = std::env::temp_dir().join("raft_kv_store_test_cluster.toml");

        std::fs::write(
            &path,
            r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 2
address = "127.0.0.1:7002"
"#,
        )
        .unwrap();

        let cluster = ClusterConfig::load_from_file(&path).unwrap();

        assert_eq!(cluster.node_count(), 2);
        assert_eq!(
            cluster.find_node(NodeId::new(1)).unwrap().address(),
            "127.0.0.1:7001".parse().unwrap()
        );
        assert_eq!(
            cluster.find_node(NodeId::new(2)).unwrap().address(),
            "127.0.0.1:7002".parse().unwrap()
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cluster_config_can_load_and_validate_from_file() {
        let path = std::env::temp_dir().join("raft_kv_store_valid_cluster.toml");

        std::fs::write(
            &path,
            r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 2
address = "127.0.0.1:7002"

[[nodes]]
node_id = 3
address = "127.0.0.1:7003"
"#,
        )
        .unwrap();

        let cluster = ClusterConfig::load_and_validate_from_file(&path).unwrap();

        assert_eq!(cluster.node_count(), 3);
        assert_eq!(
            cluster.find_node(NodeId::new(1)).unwrap().address(),
            "127.0.0.1:7001".parse().unwrap()
        );
        assert_eq!(
            cluster.find_node(NodeId::new(2)).unwrap().address(),
            "127.0.0.1:7002".parse().unwrap()
        );
        assert_eq!(
            cluster.find_node(NodeId::new(3)).unwrap().address(),
            "127.0.0.1:7003".parse().unwrap()
        );

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cluster_config_rejects_duplicate_node_ids_from_file() {
        let path = std::env::temp_dir().join("raft_kv_store_invalid_cluster.toml");

        std::fs::write(
            &path,
            r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 1
address = "127.0.0.1:7002"
"#,
        )
        .unwrap();

        let result = ClusterConfig::load_and_validate_from_file(&path);

        assert!(matches!(
            result,
            Err(error) if error.to_string().contains("duplicate node id")
        ));

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn cluster_config_validate_method_works() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(cluster.validate(), Ok(()));
    }

    #[test]
    fn cluster_config_validate_rejects_duplicate_addresses() {
        let address = "127.0.0.1:7001".parse().unwrap();

        let node1 = NodeConfig::new(NodeId::new(1), address);
        let node2 = NodeConfig::new(NodeId::new(2), address);

        let cluster = ClusterConfig::new(vec![node1, node2]);

        assert_eq!(
            cluster.validate(),
            Err(crate::config::validation::ConfigError::DuplicateAddress)
        );
    }

    #[test]
    fn cluster_config_can_load_and_validate_from_toml() {
        let config = r#"
[[nodes]]
node_id = 1
address = "127.0.0.1:7001"

[[nodes]]
node_id = 2
address = "127.0.0.1:7002"
"#;

        let cluster = ClusterConfig::load_and_validate_from_toml(config).unwrap();

        assert_eq!(cluster.node_count(), 2);
        assert_eq!(cluster.validate(), Ok(()));
    }

    #[test]
    fn cluster_config_can_check_if_node_exists() {
        let node = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node]);

        assert!(cluster.contains_node(NodeId::new(1)));
        assert!(!cluster.contains_node(NodeId::new(2)));
    }

    #[test]
    fn cluster_config_can_return_node_ids() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let node3 = NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2, node3]);

        assert_eq!(
            cluster.node_ids(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3),]
        );
    }

    #[test]
    fn cluster_config_can_return_peer_nodes() {
        let node1 = NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap());

        let node2 = NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap());

        let node3 = NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap());

        let cluster = ClusterConfig::new(vec![node1, node2, node3]);

        let peers = cluster.peer_nodes(NodeId::new(2));

        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].node_id(), NodeId::new(1));
        assert_eq!(peers[1].node_id(), NodeId::new(3));
    }

    #[test]
    fn cluster_config_has_a_useful_default() {
        let cluster = ClusterConfig::default();

        assert_eq!(cluster.node_count(), 1);
        assert!(cluster.contains_node(NodeId::new(1)));

        assert_eq!(
            cluster.find_node(NodeId::new(1)).unwrap().address(),
            "127.0.0.1:7001".parse().unwrap()
        );

        assert_eq!(cluster.validate(), Ok(()));
    }

    #[test]
    fn cluster_config_calculates_quorum_size() {
        let one_node = ClusterConfig::new(vec![NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        )]);

        assert_eq!(one_node.quorum_size(), 1);

        let two_nodes = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
        ]);

        assert_eq!(two_nodes.quorum_size(), 2);

        let three_nodes = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
            NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap()),
        ]);

        assert_eq!(three_nodes.quorum_size(), 2);

        let five_nodes = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
            NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap()),
            NodeConfig::new(NodeId::new(4), "127.0.0.1:7004".parse().unwrap()),
            NodeConfig::new(NodeId::new(5), "127.0.0.1:7005".parse().unwrap()),
        ]);

        assert_eq!(five_nodes.quorum_size(), 3);
    }

    #[test]
    fn cluster_config_can_detect_single_node_cluster() {
        let single_node = ClusterConfig::new(vec![NodeConfig::new(
            NodeId::new(1),
            "127.0.0.1:7001".parse().unwrap(),
        )]);

        assert!(single_node.is_single_node());

        let multi_node = ClusterConfig::new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
        ]);

        assert!(!multi_node.is_single_node());
    }

    #[test]
    fn try_new_rejects_invalid_cluster() {
        let result = ClusterConfig::try_new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7002".parse().unwrap()),
        ]);

        assert_eq!(
            result,
            Err(crate::config::validation::ConfigError::DuplicateNodeId(
                NodeId::new(1)
            ))
        );
    }

    #[test]
    fn try_new_accepts_valid_cluster() {
        let result = ClusterConfig::try_new(vec![
            NodeConfig::new(NodeId::new(1), "127.0.0.1:7001".parse().unwrap()),
            NodeConfig::new(NodeId::new(2), "127.0.0.1:7002".parse().unwrap()),
            NodeConfig::new(NodeId::new(3), "127.0.0.1:7003".parse().unwrap()),
        ]);

        let cluster = result.expect("valid cluster should be accepted");

        assert_eq!(cluster.node_count(), 3);
        assert_eq!(cluster.quorum_size(), 2);
    }
}
