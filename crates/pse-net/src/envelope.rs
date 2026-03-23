//! Crystal envelope for network propagation with content-addressed integrity.

use pse_types::{Hash256, SemanticCrystal};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A crystal wrapped for network propagation.
///
/// Contains the crystal itself plus metadata for verification:
/// - `content_hash`: SHA-256 of the canonical JSON of the crystal, used to
///   verify integrity after transit.
/// - `origin_node`: the node that originally created this crystal.
/// - `origin_tick`: the commit index at which the crystal was created.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrystalEnvelope {
    /// The crystal being propagated.
    pub crystal: SemanticCrystal,
    /// Node ID of the originating node.
    pub origin_node: Hash256,
    /// Commit index at the origin node when this crystal was created.
    pub origin_tick: u64,
    /// SHA-256 hash of the canonical JSON serialization of the crystal.
    pub content_hash: Hash256,
    /// Unix timestamp (seconds) when this envelope was created.
    pub propagation_timestamp: u64,
}

impl CrystalEnvelope {
    /// Wrap a crystal in an envelope, computing the content hash.
    pub fn wrap(crystal: SemanticCrystal, node_id: Hash256) -> Self {
        let content_hash = Self::compute_hash(&crystal);
        let origin_tick = crystal.created_at;
        let propagation_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Self {
            crystal,
            origin_node: node_id,
            origin_tick,
            content_hash,
            propagation_timestamp,
        }
    }

    /// Verify the content hash matches the crystal.
    pub fn verify(&self) -> bool {
        let expected = Self::compute_hash(&self.crystal);
        expected == self.content_hash
    }

    /// Compute SHA-256 of the canonical JSON of a crystal.
    fn compute_hash(crystal: &SemanticCrystal) -> Hash256 {
        let json = serde_json::to_string(crystal).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_types::*;
    #[allow(unused_imports)]
    use std::collections::BTreeMap;

    fn mock_crystal() -> SemanticCrystal {
        SemanticCrystal {
            crystal_id: [0u8; 32],
            region: vec![1, 2, 3],
            constraint_program: Vec::new(),
            stability_score: 0.95,
            topology_signature: TopologySignature {
                betti_0: 1,
                betti_1: 0,
                betti_2: 0,
                spectral_gap: 0.42,
                euler_char: 1,
                cheeger_estimate: 0.3,
                kuramoto_coherence: 0.8,
                mean_propagation_time: 1.5,
                dtl_connected: true,
            },
            betti_numbers: vec![1, 0, 0],
            evidence_chain: Vec::new(),
            commit_proof: CommitProof::default(),
            operator_versions: BTreeMap::new(),
            created_at: 1,
            free_energy: 0.1,
            carrier_instance_idx: 0,
            scale_tag: String::new(),
            universe_id: String::new(),
            sub_crystal_ids: Vec::new(),
            parent_crystal_ids: Vec::new(),
            genesis_metadata: None,
        }
    }

    #[test]
    fn test_envelope_verify_valid() {
        let crystal = mock_crystal();
        let envelope = CrystalEnvelope::wrap(crystal, [1u8; 32]);
        assert!(envelope.verify());
    }

    #[test]
    fn test_envelope_verify_tampered() {
        let crystal = mock_crystal();
        let mut envelope = CrystalEnvelope::wrap(crystal, [1u8; 32]);
        // Tamper with the content hash
        envelope.content_hash[0] ^= 0xff;
        assert!(!envelope.verify());
    }

    #[test]
    fn test_envelope_verify_tampered_crystal() {
        let crystal = mock_crystal();
        let mut envelope = CrystalEnvelope::wrap(crystal, [1u8; 32]);
        // Tamper with the crystal
        envelope.crystal.stability_score = 0.0;
        assert!(!envelope.verify());
    }
}
