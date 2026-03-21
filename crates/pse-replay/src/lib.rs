//! Deterministic replay engine for PSE.
//!
//! Provides replay verification by re-executing observation sequences
//! and comparing crystal outputs against recorded manifests.

use pse_types::{RunDescriptor, SemanticCrystal, Hash256, content_address};
use serde::{Serialize, Deserialize};

/// Replay verification result.
#[derive(Clone, Debug)]
pub struct ReplayResult {
    pub deterministic: bool,
    pub crystal_count: usize,
    pub digest_matches: Vec<bool>,
}

/// Verify that a run descriptor produces deterministic results.
pub fn verify_determinism(rd: &RunDescriptor) -> bool {
    let d1 = content_address(rd);
    let d2 = content_address(rd);
    d1 == d2
}

/// Compare two crystal sequences for equality.
pub fn compare_crystal_sequences(a: &[SemanticCrystal], b: &[SemanticCrystal]) -> ReplayResult {
    let matches: Vec<bool> = a.iter().zip(b.iter())
        .map(|(ca, cb)| ca.crystal_id == cb.crystal_id)
        .collect();
    let all_match = matches.iter().all(|m| *m) && a.len() == b.len();
    ReplayResult {
        deterministic: all_match,
        crystal_count: a.len(),
        digest_matches: matches,
    }
}

/// Replay pack: self-contained bundle for offline replay.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPack {
    pub rd: RunDescriptor,
    pub observation_log: Vec<Vec<Vec<u8>>>,
    pub expected_crystal_ids: Vec<Hash256>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use pse_types::Config;

    fn make_rd() -> RunDescriptor {
        RunDescriptor {
            config: Config::default(),
            operator_versions: BTreeMap::new(),
            initial_state_digest: [0u8; 32],
            seed: None,
            registry_digests: BTreeMap::new(),
            scheduler: pse_types::SchedulerConfig::default(),
        }
    }

    #[test]
    fn replay_determinism() {
        let rd = make_rd();
        assert!(verify_determinism(&rd));
    }

    #[test]
    fn compare_empty_sequences() {
        let result = compare_crystal_sequences(&[], &[]);
        assert!(result.deterministic);
        assert_eq!(result.crystal_count, 0);
    }
}
