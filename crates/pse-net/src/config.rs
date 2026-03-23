//! Configuration for a PSE swarm node.

use serde::{Deserialize, Serialize};

/// Configuration for a PSE swarm node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SwarmConfig {
    /// Maximum number of connected peers.
    pub max_peers: usize,
    /// Maximum hop count for crystal propagation (TTL).
    pub max_hops: u8,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,
    /// TCP connect timeout in milliseconds.
    pub connect_timeout_ms: u64,
    /// TCP read timeout in milliseconds.
    pub read_timeout_ms: u64,
    /// Kuramoto acceptance threshold (0.0 to 1.0).
    /// Crystals are accepted when the order parameter stays above this value.
    pub acceptance_threshold: f64,
    /// Maximum messages accepted per second (rate limiting).
    pub rate_limit_per_sec: usize,
    /// Seed peer addresses as "host:port".
    pub seed_peers: Vec<String>,
    /// Local listen address (e.g. "127.0.0.1:0" for random port).
    pub listen_addr: String,
    /// Seed for deterministic node ID generation.
    pub node_seed: u64,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            max_peers: 16,
            max_hops: 3,
            heartbeat_interval_ms: 5000,
            connect_timeout_ms: 3000,
            read_timeout_ms: 2000,
            acceptance_threshold: 0.51,
            rate_limit_per_sec: 100,
            seed_peers: Vec::new(),
            listen_addr: "127.0.0.1:0".to_string(),
            node_seed: 0,
        }
    }
}
