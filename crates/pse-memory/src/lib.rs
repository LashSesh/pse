//! # pse-memory — Persistent Pattern Memory
//!
//! Loads crystals from prior sessions, builds a topological similarity index,
//! and provides fast lookup for incoming patterns against known crystals.
//! This is what makes PSE learn across sessions.
//!
//! The memory index uses spectral/topological features extracted from each
//! crystal's `TopologySignature` and computes cosine similarity weighted
//! with resonance and confidence proximity.

use pse_types::{Hash256, SemanticCrystal};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A crystal's topological fingerprint for similarity comparison.
/// Extracted from the crystal's topology signature, stability score,
/// and consensus result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CrystalSignature {
    /// Crystal ID this signature belongs to.
    pub crystal_id: Hash256,
    /// Spectral signature: topological features of the crystal.
    /// Components: [spectral_gap, cheeger_estimate, kuramoto_coherence,
    ///              mean_propagation_time, betti_0, betti_1, betti_2, euler_char]
    pub spectral: Vec<f64>,
    /// Resonance at time of crystallization (stability_score).
    pub resonance: f64,
    /// Confidence at time of crystallization (consensus MCI).
    pub confidence: f64,
    /// Content hash for exact-match dedup.
    pub content_hash: [u8; 32],
    /// Tick when this crystal was created.
    pub tick_range: (u64, u64),
    /// Number of vertices in the crystal's region.
    pub observation_count: usize,
}

/// Configuration for pattern memory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Similarity threshold for "known pattern" (0.0–1.0).
    /// Below this: pattern is considered novel → run full cascade.
    /// Above this: pattern is known → skip cascade, increment hit counter.
    pub similarity_threshold: f64,
    /// Maximum signatures to keep in memory (LRU eviction if exceeded).
    pub max_signatures: usize,
    /// Number of spectral components to use for comparison.
    pub spectral_k: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.85,
            max_signatures: 100_000,
            spectral_k: 8,
        }
    }
}

/// Statistics for pattern memory performance.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStats {
    /// Total lookups performed.
    pub total_lookups: u64,
    /// Lookups that found a similar known crystal (hit).
    pub hits: u64,
    /// Lookups that found no similar crystal (miss → full cascade).
    pub misses: u64,
    /// Hit rate = hits / total_lookups.
    pub hit_rate: f64,
    /// Estimated time saved (hits × average_cascade_time_ms).
    pub estimated_time_saved_ms: f64,
    /// Number of signatures currently in index.
    pub index_size: usize,
}

/// The pattern memory index.
///
/// Built from persisted crystals on startup.
/// Updated incrementally as new crystals are created during a session.
pub struct PatternMemory {
    /// Topological signatures of known crystals, indexed for fast lookup.
    index: Vec<CrystalSignature>,
    /// Configuration.
    config: MemoryConfig,
    /// Statistics.
    stats: MemoryStats,
    /// Estimated average cascade time in ms (for time-saved estimation).
    avg_cascade_ms: f64,
}

impl PatternMemory {
    /// Create empty memory with configuration.
    pub fn new(config: MemoryConfig) -> Self {
        Self {
            index: Vec::new(),
            config,
            stats: MemoryStats::default(),
            avg_cascade_ms: 1.0,
        }
    }

    /// Load crystals and build index.
    /// Called once on engine startup to restore cross-session memory.
    /// Returns number of signatures loaded.
    pub fn load_from_crystals(&mut self, crystals: &[SemanticCrystal]) -> usize {
        let mut loaded = 0;
        for crystal in crystals {
            let sig = Self::extract_signature(crystal);
            self.index.push(sig);
            loaded += 1;
            // Respect max_signatures during load
            if self.index.len() > self.config.max_signatures {
                self.index.remove(0); // LRU: remove oldest
            }
        }
        self.stats.index_size = self.index.len();
        loaded
    }

    /// Check if a candidate pattern is similar to any known crystal.
    /// Returns `Some(crystal_id)` if similar crystal found (hit), `None` if novel (miss).
    /// This is the hot path — optimized for fast scanning.
    pub fn lookup(&mut self, candidate: &CrystalSignature) -> Option<Hash256> {
        self.stats.total_lookups += 1;

        let spectral_k = self.config.spectral_k;
        let threshold = self.config.similarity_threshold;

        let mut best_id: Option<Hash256> = None;
        let mut best_sim: f64 = 0.0;

        for sig in &self.index {
            let sim = similarity(candidate, sig, spectral_k);
            if sim >= 1.0 {
                // Exact match — return immediately
                let id = sig.crystal_id;
                self.stats.hits += 1;
                self.update_hit_rate();
                return Some(id);
            }
            if sim > best_sim {
                best_sim = sim;
                best_id = Some(sig.crystal_id);
            }
        }

        if best_sim >= threshold {
            self.stats.hits += 1;
            self.stats.estimated_time_saved_ms += self.avg_cascade_ms;
            self.update_hit_rate();
            best_id
        } else {
            self.stats.misses += 1;
            self.update_hit_rate();
            None
        }
    }

    /// Add a newly created crystal to the index.
    /// Called after successful cascade validation + crystallization.
    pub fn insert(&mut self, signature: CrystalSignature) {
        self.index.push(signature);
        // LRU eviction if over capacity
        if self.index.len() > self.config.max_signatures {
            self.index.remove(0);
        }
        self.stats.index_size = self.index.len();
    }

    /// Get current statistics.
    pub fn stats(&self) -> &MemoryStats {
        &self.stats
    }

    /// Number of signatures in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Extract a `CrystalSignature` from a `SemanticCrystal`.
    /// Uses the crystal's topological features to create the fingerprint.
    pub fn extract_signature(crystal: &SemanticCrystal) -> CrystalSignature {
        let ts = &crystal.topology_signature;

        // Build spectral vector from topology signature fields
        let spectral = vec![
            ts.spectral_gap,
            ts.cheeger_estimate,
            ts.kuramoto_coherence,
            ts.mean_propagation_time,
            ts.betti_0 as f64,
            ts.betti_1 as f64,
            ts.betti_2 as f64,
            ts.euler_char as f64,
        ];

        // Confidence from consensus MCI
        let confidence = crystal.commit_proof.consensus_result.mci;

        // Content hash: SHA-256 of canonical crystal bytes
        let content_hash = compute_content_hash(crystal);

        CrystalSignature {
            crystal_id: crystal.crystal_id,
            spectral,
            resonance: crystal.stability_score,
            confidence,
            content_hash,
            tick_range: (crystal.created_at, crystal.created_at),
            observation_count: crystal.region.len(),
        }
    }

    /// Extract a signature from pre-crystal data (metrics + topology).
    /// Used for lookup before the cascade runs.
    pub fn extract_candidate_signature(
        spectral_gap: f64,
        cheeger_estimate: f64,
        kuramoto_coherence: f64,
        mean_propagation_time: f64,
        betti_0: u64,
        betti_1: u64,
        betti_2: u64,
        euler_char: i64,
        stability_score: f64,
        region_size: usize,
    ) -> CrystalSignature {
        let spectral = vec![
            spectral_gap,
            cheeger_estimate,
            kuramoto_coherence,
            mean_propagation_time,
            betti_0 as f64,
            betti_1 as f64,
            betti_2 as f64,
            euler_char as f64,
        ];

        CrystalSignature {
            crystal_id: [0u8; 32], // Unknown until crystallized
            spectral,
            resonance: stability_score,
            confidence: 0.0, // Unknown before consensus
            content_hash: [0u8; 32], // Unknown before crystallization
            tick_range: (0, 0),
            observation_count: region_size,
        }
    }

    /// Update the hit rate statistic.
    fn update_hit_rate(&mut self) {
        if self.stats.total_lookups > 0 {
            self.stats.hit_rate = self.stats.hits as f64 / self.stats.total_lookups as f64;
        }
    }
}

/// Compute similarity between two crystal signatures.
/// Uses cosine similarity on spectral components + resonance proximity.
///
/// Returns value in [0.0, 1.0] where 1.0 = identical.
pub fn similarity(a: &CrystalSignature, b: &CrystalSignature, spectral_k: usize) -> f64 {
    // 1. Exact content match (hash comparison) → return 1.0 immediately
    if a.content_hash != [0u8; 32] && b.content_hash != [0u8; 32] && a.content_hash == b.content_hash {
        return 1.0;
    }

    // 2. Spectral cosine similarity (70% weight)
    let spectral_sim = cosine_similarity(&a.spectral, &b.spectral, spectral_k);

    // 3. Resonance proximity (15% weight)
    let resonance_sim = 1.0 - (a.resonance - b.resonance).abs().min(1.0);

    // 4. Confidence proximity (15% weight)
    let confidence_sim = 1.0 - (a.confidence - b.confidence).abs().min(1.0);

    0.70 * spectral_sim + 0.15 * resonance_sim + 0.15 * confidence_sim
}

/// Cosine similarity between two vectors, using at most `k` components.
/// Returns 0.0 if either vector is zero.
fn cosine_similarity(a: &[f64], b: &[f64], k: usize) -> f64 {
    let len = a.len().min(b.len()).min(k);
    if len == 0 {
        return 0.0;
    }

    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for i in 0..len {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom < 1e-12 {
        return 0.0;
    }

    // Clamp to [0, 1] (cosine can be negative for opposing vectors)
    (dot / denom).max(0.0).min(1.0)
}

/// Compute SHA-256 content hash of a crystal.
fn compute_content_hash(crystal: &SemanticCrystal) -> [u8; 32] {
    let mut hasher = Sha256::new();
    // Hash the crystal_id + stability_score + region size for a stable fingerprint
    hasher.update(crystal.crystal_id);
    hasher.update(crystal.stability_score.to_le_bytes());
    hasher.update((crystal.region.len() as u64).to_le_bytes());
    hasher.update(crystal.free_energy.to_le_bytes());
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sig(spectral: Vec<f64>, resonance: f64, confidence: f64) -> CrystalSignature {
        CrystalSignature {
            crystal_id: [1u8; 32],
            spectral,
            resonance,
            confidence,
            content_hash: [0u8; 32],
            tick_range: (0, 0),
            observation_count: 10,
        }
    }

    fn make_sig_with_hash(spectral: Vec<f64>, hash: [u8; 32]) -> CrystalSignature {
        CrystalSignature {
            crystal_id: [1u8; 32],
            spectral,
            resonance: 0.5,
            confidence: 0.5,
            content_hash: hash,
            tick_range: (0, 0),
            observation_count: 10,
        }
    }

    #[test]
    fn empty_memory_returns_none() {
        let mut mem = PatternMemory::new(MemoryConfig::default());
        let sig = make_sig(vec![1.0, 2.0, 3.0], 0.5, 0.5);
        assert!(mem.lookup(&sig).is_none());
        assert_eq!(mem.stats().misses, 1);
    }

    #[test]
    fn insert_and_lookup_returns_crystal_id() {
        let mut mem = PatternMemory::new(MemoryConfig::default());
        let mut sig = make_sig(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 0.5, 0.5);
        sig.crystal_id = [42u8; 32];
        mem.insert(sig.clone());

        let result = mem.lookup(&sig);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), [42u8; 32]);
        assert_eq!(mem.stats().hits, 1);
    }

    #[test]
    fn similar_patterns_match_above_threshold() {
        let mut mem = PatternMemory::new(MemoryConfig {
            similarity_threshold: 0.85,
            ..Default::default()
        });
        let mut stored = make_sig(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 0.5, 0.5);
        stored.crystal_id = [42u8; 32];
        mem.insert(stored);

        // Very similar but not identical
        let candidate = make_sig(vec![1.01, 2.01, 3.01, 4.01, 5.01, 6.01, 7.01, 8.01], 0.51, 0.51);
        assert!(mem.lookup(&candidate).is_some());
    }

    #[test]
    fn dissimilar_patterns_dont_match() {
        let mut mem = PatternMemory::new(MemoryConfig {
            similarity_threshold: 0.85,
            ..Default::default()
        });
        let stored = make_sig(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 0.9, 0.9);
        mem.insert(stored);

        // Very different spectral signature
        let candidate = make_sig(vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0], 0.1, 0.1);
        assert!(mem.lookup(&candidate).is_none());
    }

    #[test]
    fn exact_hash_match_always_returns_hit() {
        let mut mem = PatternMemory::new(MemoryConfig {
            similarity_threshold: 0.99, // Very high threshold
            ..Default::default()
        });
        let hash = [99u8; 32];
        let mut stored = make_sig_with_hash(vec![1.0, 2.0, 3.0], hash);
        stored.crystal_id = [42u8; 32];
        mem.insert(stored);

        // Different spectral but same hash → still matches
        let candidate = make_sig_with_hash(vec![100.0, 200.0, 300.0], hash);
        assert!(mem.lookup(&candidate).is_some());
    }

    #[test]
    fn stats_tracking() {
        let mut mem = PatternMemory::new(MemoryConfig::default());
        let sig = make_sig(vec![1.0, 2.0, 3.0], 0.5, 0.5);

        // Three misses
        mem.lookup(&sig);
        mem.lookup(&sig);
        mem.lookup(&sig);

        assert_eq!(mem.stats().total_lookups, 3);
        assert_eq!(mem.stats().misses, 3);
        assert_eq!(mem.stats().hits, 0);
        assert!((mem.stats().hit_rate - 0.0).abs() < 1e-9);

        // Insert and hit
        let mut stored = sig.clone();
        stored.crystal_id = [1u8; 32];
        mem.insert(stored);
        mem.lookup(&sig);

        assert_eq!(mem.stats().total_lookups, 4);
        assert_eq!(mem.stats().hits, 1);
        assert!((mem.stats().hit_rate - 0.25).abs() < 1e-9);
    }

    #[test]
    fn lru_eviction() {
        let mut mem = PatternMemory::new(MemoryConfig {
            max_signatures: 3,
            ..Default::default()
        });

        for i in 0..5u8 {
            let mut sig = make_sig(vec![i as f64], 0.5, 0.5);
            sig.crystal_id = [i; 32];
            mem.insert(sig);
        }

        // Should have evicted oldest entries, keeping only last 3
        assert_eq!(mem.len(), 3);
        assert_eq!(mem.stats().index_size, 3);
    }

    #[test]
    fn cosine_similarity_identical_vectors() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b, 8);
        assert!((sim - 1.0).abs() < 1e-9);
    }

    #[test]
    fn cosine_similarity_orthogonal_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b, 8);
        assert!(sim.abs() < 1e-9);
    }

    #[test]
    fn cosine_similarity_zero_vector() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&a, &b, 8);
        assert!(sim.abs() < 1e-9);
    }
}
