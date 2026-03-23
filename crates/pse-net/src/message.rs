//! Wire protocol messages for PSE peer-to-peer swarm communication.

use pse_types::Hash256;
use serde::{Deserialize, Serialize};

use crate::envelope::CrystalEnvelope;

/// Information about a known peer in the swarm.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique node identifier.
    pub node_id: Hash256,
    /// Network address as "host:port".
    pub addr: String,
    /// Last time this peer was seen (unix timestamp seconds).
    pub last_seen: u64,
}

/// Wire messages exchanged between PSE swarm peers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SwarmMessage {
    /// Initial handshake when connecting to a peer.
    Hello {
        node_id: Hash256,
        version: String,
        listen_port: u16,
    },
    /// Propagate a crystal envelope to peers (gossip).
    CrystalPropagate {
        envelope: CrystalEnvelope,
        ttl: u8,
        hops: Vec<Hash256>,
    },
    /// Request crystals created since a given tick.
    CrystalSync {
        since_tick: u64,
    },
    /// Response to a CrystalSync request.
    CrystalSyncResponse {
        crystals: Vec<CrystalEnvelope>,
    },
    /// Heartbeat ping.
    Ping {
        nonce: u64,
    },
    /// Heartbeat pong.
    Pong {
        nonce: u64,
    },
    /// Request known peers.
    PeerRequest,
    /// Response with known peers.
    PeerResponse {
        peers: Vec<PeerInfo>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization_roundtrip() {
        let messages = vec![
            SwarmMessage::Hello {
                node_id: [1u8; 32],
                version: "0.1.0".to_string(),
                listen_port: 9001,
            },
            SwarmMessage::Ping { nonce: 42 },
            SwarmMessage::Pong { nonce: 42 },
            SwarmMessage::PeerRequest,
            SwarmMessage::PeerResponse {
                peers: vec![PeerInfo {
                    node_id: [2u8; 32],
                    addr: "127.0.0.1:9002".to_string(),
                    last_seen: 1000,
                }],
            },
            SwarmMessage::CrystalSync { since_tick: 5 },
        ];

        for msg in messages {
            let json = serde_json::to_string(&msg).expect("serialize");
            let _: SwarmMessage = serde_json::from_str(&json).expect("deserialize");
        }
    }
}
