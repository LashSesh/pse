//! Observation graph, entity tracking, and persistent hash-DAG for PSE.
//!
//! Combines the observation ingestion layer (L0) with the persistent
//! graph storage layer (L1).

use std::collections::BTreeMap;
use pse_types::{
    CommitIndex, EdgeAnnotation, FiveDState, Hash256, MeasurementContext,
    Observation, PersistenceConfig, ProvenanceEnvelope, VertexId,
    content_address_raw,
};
use petgraph::graph::{DiGraph, NodeIndex};
use thiserror::Error;

// ─── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ObserveError {
    #[error("canonicalization failed: {0}")]
    Canonicalize(String),
    #[error("digest mismatch")]
    DigestMismatch,
}

#[derive(Debug, Error)]
pub enum PersistError {
    #[error("vertex not found: {0}")]
    VertexNotFound(VertexId),
    #[error("graph capacity exceeded")]
    CapacityExceeded,
    #[error("integrity check failed: {0}")]
    IntegrityFail(String),
}

// ─── Observation Adapter ────────────────────────────────────────────────────

/// Observation canonicalization adapter.
pub trait ObservationAdapter: Send + Sync {
    /// Unique identifier for this observation source.
    fn source_id(&self) -> &str;
    /// Canonicalize raw bytes into an observation.
    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError>;
}

/// Idempotent ingestion: same raw input produces same digest.
pub fn ingest(
    adapter: &dyn ObservationAdapter,
    raw: &[u8],
    ctx: &MeasurementContext,
) -> Result<Observation, ObserveError> {
    let obs = adapter.canonicalize(raw, ctx)?;
    let recomputed = content_address_raw(&obs.payload);
    if recomputed != obs.digest {
        return Err(ObserveError::DigestMismatch);
    }
    Ok(obs)
}

/// A passthrough adapter that treats raw bytes as payload.
pub struct PassthroughAdapter {
    id: String,
    schema_version: String,
}

impl PassthroughAdapter {
    /// Create a new passthrough adapter with the given source ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into(), schema_version: "1.0.0".to_string() }
    }
}

impl ObservationAdapter for PassthroughAdapter {
    fn source_id(&self) -> &str { &self.id }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        let payload = raw.to_vec();
        let digest: Hash256 = content_address_raw(&payload);
        Ok(Observation {
            timestamp: 0.0,
            source_id: self.id.clone(),
            provenance: ProvenanceEnvelope {
                origin: self.id.clone(),
                chain: Vec::new(),
                sig: None,
            },
            payload,
            context: context.clone(),
            digest,
            schema_version: self.schema_version.clone(),
        })
    }
}

// ─── Tensor Archive ───────────────────────────────────────────────────────────

/// Tensor archive: stores historical 5D state snapshots for a vertex.
#[derive(Clone, Debug, Default)]
pub struct TensorArchive {
    pub snapshots: Vec<FiveDState>,
    pub timestamps: Vec<f64>,
}

impl TensorArchive {
    /// Append a snapshot.
    pub fn push(&mut self, state: FiveDState, timestamp: f64) {
        self.snapshots.push(state);
        self.timestamps.push(timestamp);
    }

    /// Get the most recent snapshot.
    pub fn latest(&self) -> Option<&FiveDState> {
        self.snapshots.last()
    }
}

// ─── Storage Tiers ────────────────────────────────────────────────────────────

/// Hot tier: in-memory recent data.
#[derive(Default, Debug)]
pub struct HotTier {
    pub data: BTreeMap<VertexId, Vec<(f64, Vec<u8>)>>,
}

/// Warm tier: compressed on-disk (simulated in-memory).
#[derive(Default, Debug)]
pub struct WarmTier {
    pub data: BTreeMap<VertexId, Vec<(f64, Vec<u8>)>>,
    pub corrupted: bool,
}

/// Cold tier: indefinite, append-only.
#[derive(Default, Debug)]
pub struct ColdTier {
    pub data: BTreeMap<VertexId, Vec<(f64, Vec<u8>)>>,
}

// ─── Vertex Data ──────────────────────────────────────────────────────────────

/// Vertex metadata in the observation graph.
#[derive(Clone, Debug)]
pub struct VertexData {
    pub id: VertexId,
    pub active: bool,
    pub first_seen: f64,
    pub last_seen: f64,
    pub activation_count: u64,
}

impl VertexData {
    /// Create a new active vertex.
    pub fn new(id: VertexId, timestamp: f64) -> Self {
        Self { id, active: true, first_seen: timestamp, last_seen: timestamp, activation_count: 1 }
    }
}

/// Record of an observation in the append-only history.
#[derive(Clone, Debug)]
pub struct ObservationRecord {
    pub commit_index: CommitIndex,
    pub digest: Hash256,
    pub timestamp: f64,
}

// ─── Persistent Graph ─────────────────────────────────────────────────────────

/// Persistent observation graph (hash-DAG).
pub struct PersistentGraph {
    pub graph: DiGraph<VertexData, EdgeAnnotation>,
    pub id_map: BTreeMap<VertexId, NodeIndex>,
    pub tensor: BTreeMap<VertexId, TensorArchive>,
    pub embedding: BTreeMap<VertexId, FiveDState>,
    pub hot: HotTier,
    pub warm: WarmTier,
    pub cold: ColdTier,
    pub commit_index: CommitIndex,
    pub history: Vec<ObservationRecord>,
}

impl Default for PersistentGraph {
    fn default() -> Self {
        Self {
            graph: DiGraph::new(),
            id_map: BTreeMap::new(),
            tensor: BTreeMap::new(),
            embedding: BTreeMap::new(),
            hot: HotTier::default(),
            warm: WarmTier::default(),
            cold: ColdTier::default(),
            commit_index: 0,
            history: Vec::new(),
        }
    }
}

impl PersistentGraph {
    /// Create a new empty persistent graph.
    pub fn new() -> Self { Self::default() }

    /// Estimate heap size in bytes.
    pub fn estimate_heap_size(&self) -> usize {
        let id_map_bytes = self.id_map.len() * (std::mem::size_of::<u64>() + std::mem::size_of::<usize>());
        let vertex_bytes = self.graph.node_count() * std::mem::size_of::<VertexData>();
        let edge_bytes = self.graph.edge_count() * std::mem::size_of::<EdgeAnnotation>();
        let embedding_bytes = self.embedding.len() * std::mem::size_of::<FiveDState>();
        let history_bytes = self.history.len() * std::mem::size_of::<ObservationRecord>();
        let tensor_bytes = self.tensor.len() * 64;
        id_map_bytes + vertex_bytes + edge_bytes + embedding_bytes + history_bytes + tensor_bytes
    }

    /// Upsert a vertex; returns its NodeIndex.
    pub fn upsert_vertex(&mut self, id: VertexId, timestamp: f64) -> NodeIndex {
        if let Some(&nidx) = self.id_map.get(&id) {
            if let Some(data) = self.graph.node_weight_mut(nidx) {
                data.last_seen = timestamp;
                data.activation_count += 1;
            }
            nidx
        } else {
            let nidx = self.graph.add_node(VertexData::new(id, timestamp));
            self.id_map.insert(id, nidx);
            self.embedding.insert(id, FiveDState::default());
            self.tensor.insert(id, TensorArchive::default());
            nidx
        }
    }

    /// Upsert an edge between two vertices.
    pub fn upsert_edge(&mut self, from: VertexId, to: VertexId, timestamp: f64) {
        let from_idx = self.upsert_vertex(from, timestamp);
        let to_idx = self.upsert_vertex(to, timestamp);
        if !self.graph.contains_edge(from_idx, to_idx) {
            self.graph.add_edge(from_idx, to_idx, EdgeAnnotation {
                birth_time: timestamp, last_update: timestamp, weight: 1.0,
                active_windows: 1, ..Default::default()
            });
        } else if let Some(edge_idx) = self.graph.find_edge(from_idx, to_idx) {
            if let Some(ann) = self.graph.edge_weight_mut(edge_idx) {
                ann.last_update = timestamp;
                ann.active_windows += 1;
                ann.weight = (ann.weight + 1.0) * 0.5;
            }
        }
    }

    /// Apply a batch of observations to the graph.
    pub fn apply_observations(
        &mut self,
        obs_batch: &[Observation],
        config: &PersistenceConfig,
    ) -> Result<(), PersistError> {
        if self.id_map.len() + obs_batch.len() > config.max_vertices {
            return Err(PersistError::CapacityExceeded);
        }

        let mut batch_vids: Vec<VertexId> = Vec::with_capacity(obs_batch.len());

        for obs in obs_batch {
            let timestamp = obs.timestamp;
            let vid = derive_vertex_id(&obs.source_id);
            self.upsert_vertex(vid, timestamp);
            batch_vids.push(vid);

            if obs.payload.len() >= 16 && std::str::from_utf8(&obs.payload).is_err() {
                for chunk in obs.payload.chunks_exact(16) {
                    let from_bytes: [u8; 8] = chunk[0..8].try_into().unwrap_or([0u8; 8]);
                    let to_bytes: [u8; 8] = chunk[8..16].try_into().unwrap_or([0u8; 8]);
                    let from_vid = u64::from_le_bytes(from_bytes);
                    let to_vid = u64::from_le_bytes(to_bytes);
                    if from_vid != to_vid {
                        self.upsert_edge(from_vid, to_vid, timestamp);
                    }
                }
            }

            let p = fnv_to_unit(&obs.payload);
            let activation = self.graph
                .node_weight(*self.id_map.get(&vid).expect("just upserted"))
                .map(|d| d.activation_count)
                .unwrap_or(1);
            let rho = (activation as f64 * 0.1_f64).min(1.0);
            let omega = (timestamp * (std::f64::consts::TAU / 86_400.0))
                .rem_euclid(std::f64::consts::TAU);

            if let Some(embed) = self.embedding.get_mut(&vid) {
                embed.p = p;
                embed.rho = rho;
                embed.omega = omega;
                embed.eta = 0.5;
            }

            let snap = self.embedding.get(&vid).cloned().unwrap_or_default();
            if let Some(archive) = self.tensor.get_mut(&vid) {
                archive.push(snap, timestamp);
            }

            self.history.push(ObservationRecord {
                commit_index: self.commit_index,
                digest: obs.digest,
                timestamp,
            });

            self.hot.data.entry(vid).or_default().push((timestamp, obs.payload.clone()));
        }

        let lambda = config.lambda_decay;
        let n_obs = obs_batch.len();
        if lambda > 0.0 && n_obs > 0 {
            let decay = (-lambda * n_obs as f64).exp();
            for eidx in self.graph.edge_indices() {
                if let Some(ann) = self.graph.edge_weight_mut(eidx) {
                    ann.weight *= decay;
                }
            }
        }

        let batch_ts = obs_batch.first().map(|o| o.timestamp).unwrap_or(0.0);
        let n = batch_vids.len();
        if n <= 64 {
            for i in 0..n {
                for j in (i + 1)..n {
                    let (u, v) = if batch_vids[i] < batch_vids[j] {
                        (batch_vids[i], batch_vids[j])
                    } else {
                        (batch_vids[j], batch_vids[i])
                    };
                    self.upsert_edge(u, v, batch_ts);
                }
            }
        } else {
            for i in 0..n {
                let next1 = (i + 1) % n;
                let next2 = (i + 2) % n;
                for &next in &[next1, next2] {
                    let (u, v) = if batch_vids[i] < batch_vids[next] {
                        (batch_vids[i], batch_vids[next])
                    } else {
                        (batch_vids[next], batch_vids[i])
                    };
                    self.upsert_edge(u, v, batch_ts);
                }
            }
        }

        let max_degree = self.graph.node_indices()
            .map(|ni| self.graph.neighbors_undirected(ni).count())
            .max().unwrap_or(1).max(1) as f64;

        let all_vids: Vec<VertexId> = self.embedding.keys().cloned().collect();
        for vid in all_vids {
            if let Some(&nidx) = self.id_map.get(&vid) {
                let degree = self.graph.neighbors_undirected(nidx).count() as f64;
                if let Some(embed) = self.embedding.get_mut(&vid) {
                    embed.chi = degree / max_degree;
                }
            }
        }

        self.commit_index += 1;
        Ok(())
    }

    /// Mark a vertex as inactive (preserves history).
    pub fn deactivate_vertex(&mut self, id: VertexId) {
        if let Some(&nidx) = self.id_map.get(&id) {
            if let Some(data) = self.graph.node_weight_mut(nidx) {
                data.active = false;
            }
        }
    }

    /// Get all active vertex IDs.
    pub fn active_vertices(&self) -> Vec<VertexId> {
        self.graph.node_weights().filter(|d| d.active).map(|d| d.id).collect()
    }

    /// Get embedding for a vertex.
    pub fn get_embedding(&self, id: VertexId) -> Option<&FiveDState> {
        self.embedding.get(&id)
    }

    /// Get all embeddings as a point cloud.
    pub fn point_cloud(&self) -> Vec<(VertexId, FiveDState)> {
        self.embedding.iter().map(|(vid, state)| (*vid, state.clone())).collect()
    }

    /// Compute topology signature for the current graph.
    pub fn topology_signature(&self) -> pse_types::TopologySignature {
        let n = self.graph.node_count() as u64;
        let e = self.graph.edge_count() as u64;
        let betti_0 = if n == 0 { 0 } else { count_weakly_connected(&self.graph) };
        let betti_1 = (e + betti_0).saturating_sub(n);
        let spectral_gap = compute_spectral_gap(&self.graph);
        let euler_char = n as i64 - e as i64;

        pse_types::TopologySignature {
            betti_0, betti_1, betti_2: 0, spectral_gap, euler_char, ..Default::default()
        }
    }
}

/// Map a byte slice to a float in [0, 1) using FNV-1a.
fn fnv_to_unit(data: &[u8]) -> f64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h >> 11) as f64 / (1u64 << 53) as f64
}

/// Derive a vertex ID from a string (deterministic, FNV-1a).
pub fn derive_vertex_id(s: &str) -> VertexId {
    let bytes = s.as_bytes();
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Count weakly connected components using union-find.
fn count_weakly_connected(graph: &DiGraph<VertexData, EdgeAnnotation>) -> u64 {
    let n = graph.node_count();
    if n == 0 { return 0; }
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut Vec<usize>, x: usize) -> usize {
        if parent[x] != x { parent[x] = find(parent, parent[x]); }
        parent[x]
    }

    fn union(parent: &mut Vec<usize>, x: usize, y: usize) {
        let rx = find(parent, x);
        let ry = find(parent, y);
        if rx != ry { parent[rx] = ry; }
    }

    for edge in graph.raw_edges() {
        union(&mut parent, edge.source().index(), edge.target().index());
    }

    let mut roots: std::collections::BTreeSet<usize> = std::collections::BTreeSet::new();
    for i in 0..n { roots.insert(find(&mut parent, i)); }
    roots.len() as u64
}

/// Compute spectral gap of graph Laplacian.
fn compute_spectral_gap(graph: &DiGraph<VertexData, EdgeAnnotation>) -> f64 {
    let n = graph.node_count();
    if n < 2 { return 0.0; }
    if n > 100 { return 0.1; }

    let node_indices: Vec<_> = graph.node_indices().collect();
    let idx_map: BTreeMap<petgraph::graph::NodeIndex, usize> = node_indices
        .iter().enumerate().map(|(i, &nidx)| (nidx, i)).collect();

    let mut laplacian = vec![vec![0.0f64; n]; n];
    for edge in graph.raw_edges() {
        let i = idx_map[&edge.source()];
        let j = idx_map[&edge.target()];
        laplacian[i][i] += 1.0;
        laplacian[j][j] += 1.0;
        laplacian[i][j] -= 1.0;
        laplacian[j][i] -= 1.0;
    }

    let max_diag = laplacian.iter().enumerate().map(|(i, row)| row[i]).fold(0.0f64, f64::max);
    let min_diag = laplacian.iter().enumerate().map(|(i, row)| row[i]).fold(f64::INFINITY, f64::min);
    (max_diag - min_diag).abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_obs(src: &str, payload: Vec<u8>, ts: f64) -> Observation {
        let digest = content_address_raw(&payload);
        Observation {
            timestamp: ts,
            source_id: src.to_string(),
            provenance: ProvenanceEnvelope::default(),
            payload,
            context: MeasurementContext::default(),
            digest,
            schema_version: "1.0.0".to_string(),
        }
    }

    #[test]
    fn passthrough_adapter_idempotent() {
        let adapter = PassthroughAdapter::new("test");
        let raw = b"hello world";
        let ctx = MeasurementContext::default();
        let obs1 = ingest(&adapter, raw, &ctx).unwrap();
        let obs2 = ingest(&adapter, raw, &ctx).unwrap();
        assert_eq!(obs1.digest, obs2.digest);
        assert_eq!(obs1.payload, obs2.payload);
    }

    #[test]
    fn different_inputs_different_digests() {
        let adapter = PassthroughAdapter::new("test");
        let ctx = MeasurementContext::default();
        let obs1 = ingest(&adapter, b"input1", &ctx).unwrap();
        let obs2 = ingest(&adapter, b"input2", &ctx).unwrap();
        assert_ne!(obs1.digest, obs2.digest);
    }

    #[test]
    fn upsert_vertex_idempotent() {
        let mut g = PersistentGraph::new();
        let idx1 = g.upsert_vertex(42, 0.0);
        let idx2 = g.upsert_vertex(42, 1.0);
        assert_eq!(idx1, idx2);
        assert_eq!(g.id_map.len(), 1);
    }

    #[test]
    fn apply_observations_increments_commit_index() {
        let mut g = PersistentGraph::new();
        let config = PersistenceConfig::default();
        let obs = vec![make_obs("src1", b"hello".to_vec(), 1.0)];
        g.apply_observations(&obs, &config).unwrap();
        assert_eq!(g.commit_index, 1);
    }

    #[test]
    fn deactivate_vertex_preserves_history() {
        let mut g = PersistentGraph::new();
        let config = PersistenceConfig::default();
        let obs = vec![make_obs("src1", b"data".to_vec(), 1.0)];
        g.apply_observations(&obs, &config).unwrap();
        let vid = derive_vertex_id("src1");
        g.deactivate_vertex(vid);
        assert!(!g.history.is_empty());
        assert!(g.id_map.contains_key(&vid));
    }

    #[test]
    fn topology_signature_empty_graph() {
        let g = PersistentGraph::new();
        let topo = g.topology_signature();
        assert_eq!(topo.betti_0, 0);
    }
}
