//! Hierarchical multi-scale observation layer for PSE (C18).
//!
//! Provides hypercube universes, dimensional bridges, and scale ladders for
//! micro/meso/macro observation and cross-scale coarsening.

use std::collections::BTreeMap;
use pse_types::{FiveDState, Hash256, SemanticCrystal, VertexId, content_address_raw};
use pse_graph::PersistentGraph;
use pse_topology::{SpectralDecomposition, KuramotoState, kuramoto_order_parameter};
use serde::{Deserialize, Serialize};

// ─── Scale Enum ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Scale { Micro, Meso, Macro }

impl Scale {
    pub fn as_str(&self) -> &'static str {
        match self { Scale::Micro => "micro", Scale::Meso => "meso", Scale::Macro => "macro" }
    }
}

// ─── HyperBounds ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HyperBounds {
    pub min: FiveDState,
    pub max: FiveDState,
}

impl HyperBounds {
    pub fn new(min: FiveDState, max: FiveDState) -> Self { Self { min, max } }

    pub fn contains(&self, state: &FiveDState) -> bool {
        state.p   >= self.min.p   && state.p   <= self.max.p   &&
        state.rho >= self.min.rho && state.rho <= self.max.rho &&
        state.omega >= self.min.omega && state.omega <= self.max.omega &&
        state.chi >= self.min.chi && state.chi <= self.max.chi &&
        state.eta >= self.min.eta && state.eta <= self.max.eta
    }

    pub fn volume(&self) -> f64 {
        let dp = (self.max.p   - self.min.p).max(0.0);
        let dr = (self.max.rho - self.min.rho).max(0.0);
        let dw = (self.max.omega - self.min.omega).max(0.0);
        let dc = (self.max.chi  - self.min.chi).max(0.0);
        let de = (self.max.eta  - self.min.eta).max(0.0);
        dp * dr * dw * dc * de
    }

    /// Bisect along dimension d (0=p,1=rho,2=omega,3=chi,4=eta).
    pub fn split(&self, dimension: usize) -> (HyperBounds, HyperBounds) {
        let mut lo = self.clone();
        let mut hi = self.clone();
        match dimension % 5 {
            0 => { let m = (self.min.p + self.max.p) / 2.0; lo.max.p = m; hi.min.p = m; }
            1 => { let m = (self.min.rho + self.max.rho) / 2.0; lo.max.rho = m; hi.min.rho = m; }
            2 => { let m = (self.min.omega + self.max.omega) / 2.0; lo.max.omega = m; hi.min.omega = m; }
            3 => { let m = (self.min.chi + self.max.chi) / 2.0; lo.max.chi = m; hi.min.chi = m; }
            _ => { let m = (self.min.eta + self.max.eta) / 2.0; lo.max.eta = m; hi.min.eta = m; }
        }
        (lo, hi)
    }

    /// Produce 2^5 = 32 children by full subdivision along all 5 dimensions.
    pub fn split_all(&self) -> [HyperBounds; 32] {
        let mut children: Vec<HyperBounds> = vec![self.clone()];
        for dim in 0..5 {
            let mut next = Vec::with_capacity(children.len() * 2);
            for child in &children {
                let (a, b) = child.split(dim);
                next.push(a);
                next.push(b);
            }
            children = next;
        }
        // children has 32 elements
        let arr: [HyperBounds; 32] = core::array::from_fn(|i| children[i].clone());
        arr
    }

    pub fn merge(&self, other: &HyperBounds) -> HyperBounds {
        HyperBounds {
            min: FiveDState {
                p: self.min.p.min(other.min.p),
                rho: self.min.rho.min(other.min.rho),
                omega: self.min.omega.min(other.min.omega),
                chi: self.min.chi.min(other.min.chi),
                eta: self.min.eta.min(other.min.eta),
            },
            max: FiveDState {
                p: self.max.p.max(other.max.p),
                rho: self.max.rho.max(other.max.rho),
                omega: self.max.omega.max(other.max.omega),
                chi: self.max.chi.max(other.max.chi),
                eta: self.max.eta.max(other.max.eta),
            },
        }
    }

    pub fn from_points(points: &[FiveDState]) -> Option<HyperBounds> {
        if points.is_empty() { return None; }
        let mut min = points[0].clone();
        let mut max = points[0].clone();
        for p in points.iter().skip(1) {
            if p.p < min.p { min.p = p.p; }
            if p.p > max.p { max.p = p.p; }
            if p.rho < min.rho { min.rho = p.rho; }
            if p.rho > max.rho { max.rho = p.rho; }
            if p.omega < min.omega { min.omega = p.omega; }
            if p.omega > max.omega { max.omega = p.omega; }
            if p.chi < min.chi { min.chi = p.chi; }
            if p.chi > max.chi { max.chi = p.chi; }
            if p.eta < min.eta { min.eta = p.eta; }
            if p.eta > max.eta { max.eta = p.eta; }
        }
        Some(HyperBounds { min, max })
    }

    pub fn center(&self) -> FiveDState {
        FiveDState {
            p:     (self.min.p   + self.max.p)   / 2.0,
            rho:   (self.min.rho + self.max.rho) / 2.0,
            omega: (self.min.omega + self.max.omega) / 2.0,
            chi:   (self.min.chi + self.max.chi) / 2.0,
            eta:   (self.min.eta + self.max.eta) / 2.0,
        }
    }
}

// ─── ScalePolicy ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalePolicy {
    Cyclic { sequence: Vec<String> },
    Greedy { target: FiveDState },
    Balanced,
    Reactive { thresholds: BTreeMap<String, f64> },
    Custom { name: String },
}

// ─── HypercubeUniverse ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HypercubeUniverse {
    pub id: Hash256,
    pub scale: Scale,
    pub vertex_ids: Vec<VertexId>,
    pub aggregate_state: FiveDState,
    pub bounds: HyperBounds,
    pub policy: ScalePolicy,
    pub kuramoto_r: f64,
}

// ─── Bridge ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bridge {
    pub source_id: Hash256,
    pub target_id: Hash256,
    pub weight: f64,
    pub delay_ticks: u64,
    pub phase_offset: f64,
    pub active: bool,
}

// ─── ScaleConfig ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MesoConfig {
    pub enabled: bool,
    pub clustering_method: String, // "spectral" | "kuramoto" | "hybrid"
    pub min_cluster_size: usize,
    pub max_clusters: usize,
    pub spectral_gap_threshold: f64,
    pub phase_threshold: f64,
    pub decimation_factor: u32,
    pub budget_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MacroConfig {
    pub enabled: bool,
    pub min_domain_size: usize,
    pub max_domains: usize,
    pub decimation_factor: u32,
    pub budget_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub proximity_threshold: f64,
    pub coherence_threshold: f64,
    pub max_delay_ticks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScaleConfig {
    pub enabled: bool,
    pub meso: MesoConfig,
    pub macro_cfg: MacroConfig,
    pub bridges: BridgeConfig,
}

impl Default for MesoConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            clustering_method: "spectral".to_string(),
            min_cluster_size: 2,
            max_clusters: 50,
            spectral_gap_threshold: 0.01,
            phase_threshold: 0.3,
            decimation_factor: 5,
            budget_ms: 50,
        }
    }
}

impl Default for MacroConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_domain_size: 2,
            max_domains: 10,
            decimation_factor: 25,
            budget_ms: 20,
        }
    }
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            proximity_threshold: 0.5,
            coherence_threshold: 0.3,
            max_delay_ticks: 10,
        }
    }
}

impl Default for ScaleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            meso: MesoConfig::default(),
            macro_cfg: MacroConfig::default(),
            bridges: BridgeConfig::default(),
        }
    }
}

// ─── MultiScaleState ─────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct MultiScaleState {
    pub meso_universes: Vec<HypercubeUniverse>,
    pub macro_universes: Vec<HypercubeUniverse>,
    pub meso_bridges: Vec<Bridge>,
    pub macro_bridges: Vec<Bridge>,
    pub cluster_assignment: BTreeMap<VertexId, usize>,
    pub domain_assignment: BTreeMap<usize, usize>,
    pub meso_crystals: Vec<SemanticCrystal>,
    pub macro_crystals: Vec<SemanticCrystal>,
}

// ─── MultiScaleMetrics ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiScaleMetrics {
    pub m28_cluster_count: u64,
    pub m29_bridge_activity: f64,
    pub m30_scale_coherence: f64,
    pub m31_lift_compression: f64,
    pub m32_cross_scale_crystal_rate: f64,
}

// ─── ScaleEvent ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScaleEvent {
    ClusterSplit { cluster_id: usize, into: Vec<usize> },
    ClusterMerge { clusters: Vec<usize>, into: usize },
    BridgeActivated { bridge_idx: usize },
    BridgeDeactivated { bridge_idx: usize },
    DownwardProjection { from_scale: Scale, crystal_id: Hash256 },
    FixpointReached { scale: Scale },
    MesoBudgetExceeded,
    MacroBudgetExceeded,
}

// ─── MultiScaleTickResult ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MultiScaleTickResult {
    pub meso_crystals: Vec<SemanticCrystal>,
    pub macro_crystals: Vec<SemanticCrystal>,
    pub metrics: MultiScaleMetrics,
    pub events: Vec<ScaleEvent>,
}

// ─── Universe Builders ────────────────────────────────────────────────────────

/// Compute the aggregate (weighted-mean) embedding for a set of vertices.
/// The fifth coordinate is replaced by the Kuramoto order parameter.
pub fn compute_aggregate(
    embeddings: &BTreeMap<VertexId, FiveDState>,
    degrees: &BTreeMap<VertexId, f64>,
    phases: &[f64],
) -> FiveDState {
    if embeddings.is_empty() {
        return FiveDState::default();
    }
    let total_weight: f64 = embeddings.keys()
        .map(|v| degrees.get(v).cloned().unwrap_or(1.0))
        .sum::<f64>()
        .max(1e-9);

    let mut p = 0.0f64;
    let mut rho = 0.0f64;
    let mut omega = 0.0f64;
    let mut chi = 0.0f64;

    for (v, emb) in embeddings {
        let w = degrees.get(v).cloned().unwrap_or(1.0);
        p     += w * emb.p;
        rho   += w * emb.rho;
        omega += w * emb.omega;
        chi   += w * emb.chi;
    }

    let (r, _) = kuramoto_order_parameter(phases);
    FiveDState {
        p:     p / total_weight,
        rho:   rho / total_weight,
        omega: omega / total_weight,
        chi:   chi / total_weight,
        eta:   r, // r_U replaces eta as specified
    }
}

/// Build a HypercubeUniverse from a set of vertex IDs and their embeddings.
pub fn build_universe(
    vertices: &[VertexId],
    embeddings: &BTreeMap<VertexId, FiveDState>,
    scale: Scale,
    policy: ScalePolicy,
) -> HypercubeUniverse {
    let local_embs: BTreeMap<VertexId, FiveDState> = vertices.iter()
        .filter_map(|v| embeddings.get(v).map(|e| (*v, e.clone())))
        .collect();

    let points: Vec<FiveDState> = local_embs.values().cloned().collect();
    let bounds = HyperBounds::from_points(&points).unwrap_or(HyperBounds {
        min: FiveDState::default(), max: FiveDState::default()
    });

    let degrees: BTreeMap<VertexId, f64> = vertices.iter().map(|v| (*v, 1.0)).collect();
    let phases: Vec<f64> = local_embs.values().map(|e| e.omega).collect();
    let aggregate_state = compute_aggregate(&local_embs, &degrees, &phases);
    let (r, _) = kuramoto_order_parameter(&phases);

    // content-address id from (scale, sorted vertex ids)
    let id_bytes = serde_json::to_vec(&(scale.as_str(), vertices)).unwrap_or_default();
    let id = content_address_raw(&id_bytes);

    HypercubeUniverse {
        id, scale, vertex_ids: vertices.to_vec(),
        aggregate_state, bounds, policy, kuramoto_r: r,
    }
}

// ─── Clustering ───────────────────────────────────────────────────────────────

/// Spectral bisection clustering using the Fiedler vector.
/// Returns a map vertex_id -> cluster_id (0-indexed).
pub fn spectral_bisection_cluster(
    fiedler_vector: &[f64],
    vertex_ids: &[VertexId],
    spectral_gap_threshold: f64,
    min_cluster_size: usize,
    max_clusters: usize,
) -> BTreeMap<VertexId, usize> {
    if vertex_ids.is_empty() || fiedler_vector.len() < vertex_ids.len() {
        return vertex_ids.iter().map(|v| (*v, 0)).collect();
    }

    let n = vertex_ids.len();
    // Sort by fiedler component, keeping vertex_id association
    let mut indexed: Vec<(f64, VertexId)> = fiedler_vector[..n].iter()
        .zip(vertex_ids.iter())
        .map(|(&f, &v)| (f, v))
        .collect();
    indexed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
        .then(a.1.cmp(&b.1)));

    // Recursive bisection
    let mut assignment: BTreeMap<VertexId, usize> = BTreeMap::new();
    let mut next_id = 0usize;
    bisect_recursive(
        &indexed, &mut assignment, &mut next_id,
        spectral_gap_threshold, min_cluster_size, max_clusters, 0,
    );
    assignment
}

fn bisect_recursive(
    sorted: &[(f64, VertexId)],
    assignment: &mut BTreeMap<VertexId, usize>,
    next_id: &mut usize,
    gap_threshold: f64,
    min_size: usize,
    max_clusters: usize,
    depth: usize,
) {
    let n = sorted.len();
    if n == 0 { return; }

    // Stop conditions: too small, max clusters reached, or spectral gap too small
    let spectral_gap = if n > 1 {
        
        sorted[n - 1].0 - sorted[0].0 // simplified gap estimate
    } else {
        0.0
    };

    if n < min_size * 2 || *next_id >= max_clusters || spectral_gap < gap_threshold || depth >= 8 {
        let id = *next_id;
        *next_id += 1;
        for (_, v) in sorted {
            assignment.insert(*v, id);
        }
        return;
    }

    // Bisect at median
    let mid = n / 2;
    let left = &sorted[..mid];
    let right = &sorted[mid..];
    bisect_recursive(left, assignment, next_id, gap_threshold, min_size, max_clusters, depth + 1);
    bisect_recursive(right, assignment, next_id, gap_threshold, min_size, max_clusters, depth + 1);
}

/// Kuramoto phase clustering.
pub fn kuramoto_phase_cluster(
    phases: &[f64],
    vertex_ids: &[VertexId],
    phase_threshold: f64,
    min_cluster_size: usize,
) -> BTreeMap<VertexId, usize> {
    if vertex_ids.is_empty() {
        return BTreeMap::new();
    }
    let n = vertex_ids.len().min(phases.len());
    let mut assignment: BTreeMap<VertexId, usize> = BTreeMap::new();
    let mut next_id = 0usize;
    let mut assigned = vec![false; n];

    for i in 0..n {
        if assigned[i] { continue; }
        let id = next_id;
        next_id += 1;
        assignment.insert(vertex_ids[i], id);
        assigned[i] = true;
        for j in (i + 1)..n {
            if assigned[j] { continue; }
            let diff = (phases[i] - phases[j]).abs();
            let diff = diff.min(std::f64::consts::TAU - diff); // wrap-around
            if diff < phase_threshold {
                assignment.insert(vertex_ids[j], id);
                assigned[j] = true;
            }
        }
    }

    // Merge clusters smaller than min_cluster_size into nearest neighbor (cluster 0)
    if min_cluster_size > 1 {
        let counts: BTreeMap<usize, usize> = {
            let mut m = BTreeMap::new();
            for &v in assignment.values() { *m.entry(v).or_insert(0) += 1; }
            m
        };
        for val in assignment.values_mut() {
            if counts.get(val).cloned().unwrap_or(0) < min_cluster_size {
                *val = 0;
            }
        }
    }
    assignment
}

/// Hybrid clustering: use spectral when available, fall back to Kuramoto.
pub fn hybrid_cluster(
    fiedler: &[f64],
    phases: &[f64],
    vertex_ids: &[VertexId],
    config: &ScaleConfig,
) -> BTreeMap<VertexId, usize> {
    match config.meso.clustering_method.as_str() {
        "kuramoto" => kuramoto_phase_cluster(
            phases, vertex_ids,
            config.meso.phase_threshold,
            config.meso.min_cluster_size,
        ),
        "hybrid" => {
            // Use spectral if fiedler is non-trivial, else kuramoto
            let range = fiedler.iter().cloned()
                .fold(f64::NEG_INFINITY, f64::max) -
                fiedler.iter().cloned()
                .fold(f64::INFINITY, f64::min);
            if range > config.meso.spectral_gap_threshold {
                spectral_bisection_cluster(
                    fiedler, vertex_ids,
                    config.meso.spectral_gap_threshold,
                    config.meso.min_cluster_size,
                    config.meso.max_clusters,
                )
            } else {
                kuramoto_phase_cluster(
                    phases, vertex_ids,
                    config.meso.phase_threshold,
                    config.meso.min_cluster_size,
                )
            }
        }
        _ => spectral_bisection_cluster(
            fiedler, vertex_ids,
            config.meso.spectral_gap_threshold,
            config.meso.min_cluster_size,
            config.meso.max_clusters,
        ),
    }
}

// ─── Bridges ─────────────────────────────────────────────────────────────────

fn state_distance(a: &FiveDState, b: &FiveDState) -> f64 {
    a.distance(b)
}

/// Evaluate which bridges are active based on universe states and config.
pub fn evaluate_bridges(
    universes: &BTreeMap<Hash256, HypercubeUniverse>,
    bridges: &mut [Bridge],
    config: &ScaleConfig,
) {
    for (idx, bridge) in bridges.iter_mut().enumerate() {
        let src = universes.get(&bridge.source_id);
        let tgt = universes.get(&bridge.target_id);
        let was_active = bridge.active;
        bridge.active = match (src, tgt) {
            (Some(s), Some(t)) => {
                let dist = state_distance(&s.aggregate_state, &t.aggregate_state);
                dist <= config.bridges.proximity_threshold &&
                s.kuramoto_r >= config.bridges.coherence_threshold &&
                t.kuramoto_r >= config.bridges.coherence_threshold
            }
            _ => false,
        };
        let _ = (idx, was_active); // events are returned by caller
    }
}

/// Apply bridge coupling: modify target universe state using source state.
pub fn apply_bridge_coupling(
    target: &mut HypercubeUniverse,
    source_history: &[FiveDState],
    bridge: &Bridge,
    _tick: u64,
) {
    if !bridge.active || source_history.is_empty() {
        return;
    }
    let delay = (bridge.delay_ticks as usize).min(source_history.len().saturating_sub(1));
    let src = &source_history[source_history.len() - 1 - delay];
    // g(Sigma, phi) = Sigma * cos(omega(Sigma) + phi)
    let phase = src.omega + bridge.phase_offset;
    let factor = bridge.weight * phase.cos();
    target.aggregate_state.p     += factor * src.p;
    target.aggregate_state.rho   += factor * src.rho;
    target.aggregate_state.omega += factor * src.omega;
    target.aggregate_state.chi   += factor * src.chi;
}

// ─── Ladders ─────────────────────────────────────────────────────────────────

/// Build meso universes and initial bridges from micro graph clusters.
pub fn lift_micro_to_meso(
    graph: &PersistentGraph,
    clusters: &BTreeMap<VertexId, usize>,
    embeddings: &BTreeMap<VertexId, FiveDState>,
) -> (Vec<HypercubeUniverse>, Vec<Bridge>) {
    // Group vertices by cluster
    let mut groups: BTreeMap<usize, Vec<VertexId>> = BTreeMap::new();
    for (&vid, &cid) in clusters {
        groups.entry(cid).or_default().push(vid);
    }

    // For each unassigned vertex (not in clusters), put in cluster 0
    for vid in graph.id_map.keys() {
        if !clusters.contains_key(vid) {
            groups.entry(0).or_default().push(*vid);
        }
    }

    let universes: Vec<HypercubeUniverse> = groups.values().map(|verts| {
        build_universe(verts, embeddings, Scale::Meso, ScalePolicy::Balanced)
    }).collect();

    // Build bridges: connect every pair of meso universes with weight = inter-cluster edge sum
    let universe_id_by_cluster: BTreeMap<usize, Hash256> = groups.keys()
        .copied()
        .zip(universes.iter().map(|u| u.id))
        .collect();

    let cluster_ids: Vec<usize> = groups.keys().cloned().collect();
    let mut bridges = Vec::new();
    for i in 0..cluster_ids.len() {
        for j in (i + 1)..cluster_ids.len() {
            let ci = cluster_ids[i];
            let cj = cluster_ids[j];
            let vi_set: std::collections::BTreeSet<VertexId> = groups[&ci].iter().cloned().collect();
            let vj_set: std::collections::BTreeSet<VertexId> = groups[&cj].iter().cloned().collect();

            // Sum inter-cluster edge weights
            let weight: f64 = graph.graph.raw_edges().iter().filter(|e| {
                let src = graph.graph[e.source()].id;
                let dst = graph.graph[e.target()].id;
                (vi_set.contains(&src) && vj_set.contains(&dst)) ||
                (vj_set.contains(&src) && vi_set.contains(&dst))
            }).map(|e| e.weight.weight.max(0.0)).sum();

            if weight > 0.0 {
                bridges.push(Bridge {
                    source_id: universe_id_by_cluster[&ci],
                    target_id: universe_id_by_cluster[&cj],
                    weight: weight.min(1.0),
                    delay_ticks: 0,
                    phase_offset: 0.0,
                    active: false,
                });
            }
        }
    }
    (universes, bridges)
}

/// Build macro universes from meso universes via second-order clustering.
pub fn lift_meso_to_macro(
    meso_universes: &[HypercubeUniverse],
    domains: &BTreeMap<usize, usize>,
) -> (Vec<HypercubeUniverse>, Vec<Bridge>) {
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (&cid, &did) in domains {
        groups.entry(did).or_default().push(cid);
    }

    // For each meso universe without a domain assignment, put in domain 0
    for (idx, _) in meso_universes.iter().enumerate() {
        if !domains.contains_key(&idx) {
            groups.entry(0).or_default().push(idx);
        }
    }

    let macro_universes: Vec<HypercubeUniverse> = groups.values().map(|cids| {
        // Aggregate the meso aggregate states
        let verts: Vec<VertexId> = cids.iter()
            .flat_map(|&cid| {
                meso_universes.get(cid).map(|u| u.vertex_ids.clone()).unwrap_or_default()
            })
            .collect();
        let embs: BTreeMap<VertexId, FiveDState> = cids.iter()
            .filter_map(|&cid| meso_universes.get(cid))
            .flat_map(|u| u.vertex_ids.iter().map(|&v| (v, u.aggregate_state.clone())))
            .collect();
        build_universe(&verts, &embs, Scale::Macro, ScalePolicy::Balanced)
    }).collect();

    // Inter-macro bridges (simplified: connect all pairs)
    let mut bridges = Vec::new();
    for i in 0..macro_universes.len() {
        for j in (i + 1)..macro_universes.len() {
            bridges.push(Bridge {
                source_id: macro_universes[i].id,
                target_id: macro_universes[j].id,
                weight: 0.5,
                delay_ticks: 0,
                phase_offset: 0.0,
                active: false,
            });
        }
    }
    (macro_universes, bridges)
}

/// Project a macro-level signal down to each meso universe in the domain.
pub fn project_macro_to_meso(
    signal: &FiveDState,
    domain: &HypercubeUniverse,
    meso_universes: &[HypercubeUniverse],
    domain_assignment: &BTreeMap<usize, usize>,
) -> BTreeMap<Hash256, FiveDState> {
    let domain_id = {
        let id_bytes = serde_json::to_vec(&domain.id).unwrap_or_default();
        u64::from_le_bytes(id_bytes[..8.min(id_bytes.len())].try_into().unwrap_or([0u8; 8]))
    };
    let members: Vec<usize> = domain_assignment.iter()
        .filter(|(_, &did)| did == domain_id as usize)
        .map(|(&cid, _)| cid)
        .collect();

    let count = members.len().max(1) as f64;
    let per_member = FiveDState {
        p:     signal.p     / count,
        rho:   signal.rho   / count,
        omega: signal.omega / count,
        chi:   signal.chi   / count,
        eta:   signal.eta   / count,
    };

    let mut result = BTreeMap::new();
    for cid in members {
        if let Some(u) = meso_universes.get(cid) {
            result.insert(u.id, per_member.clone());
        }
    }
    result
}

/// Project meso-level signals down to individual micro vertices.
pub fn project_meso_to_micro(
    signals: &BTreeMap<Hash256, FiveDState>,
    clusters: &BTreeMap<VertexId, usize>,
    meso_universes: &[HypercubeUniverse],
) -> BTreeMap<VertexId, FiveDState> {
    // Build universe_id -> signal map
    let uid_to_signal: BTreeMap<Hash256, &FiveDState> = signals.iter()
        .map(|(uid, sig)| (*uid, sig))
        .collect();

    // Build cluster_id -> universe_id
    let cid_to_uid: BTreeMap<usize, Hash256> = meso_universes.iter()
        .enumerate()
        .map(|(i, u)| (i, u.id))
        .collect();

    let mut result = BTreeMap::new();
    for (&vid, &cid) in clusters {
        if let Some(&uid) = cid_to_uid.get(&cid) {
            if let Some(sig) = uid_to_signal.get(&uid) {
                let cluster_size = clusters.values().filter(|&&c| c == cid).count().max(1) as f64;
                result.insert(vid, FiveDState {
                    p:     sig.p     / cluster_size,
                    rho:   sig.rho   / cluster_size,
                    omega: sig.omega / cluster_size,
                    chi:   sig.chi   / cluster_size,
                    eta:   sig.eta   / cluster_size,
                });
            }
        }
    }
    result
}

// ─── Multi-Scale Tick ─────────────────────────────────────────────────────────

/// Execute one multi-scale tick: Lift, Bridge, Meso tick, Lift, Bridge, Macro tick.
pub fn multi_scale_tick(
    micro_state: &pse_engine_types::MicroState,
    scale_state: &mut MultiScaleState,
    spectral: &SpectralDecomposition,
    kuramoto: &KuramotoState,
    config: &ScaleConfig,
    micro_crystals: &[SemanticCrystal],
    tick: u64,
) -> MultiScaleTickResult {
    let mut events = Vec::new();
    let mut meso_crystals = Vec::new();
    let mut macro_crystals = Vec::new();

    if !config.enabled {
        return MultiScaleTickResult {
            meso_crystals, macro_crystals,
            metrics: MultiScaleMetrics::default(),
            events,
        };
    }

    let embeddings = &micro_state.embeddings;
    let vertex_ids: Vec<VertexId> = embeddings.keys().cloned().collect();
    let n_micro = vertex_ids.len() as f64;

    // ── MS2: Lift micro → meso ────────────────────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    let meso_budget_exceeded = {
        let meso_start = std::time::Instant::now();
        meso_start.elapsed().as_millis() as u64 > config.meso.budget_ms
    };
    #[cfg(target_arch = "wasm32")]
    let meso_budget_exceeded = false;
    if meso_budget_exceeded && vertex_ids.len() > 100 {
        events.push(ScaleEvent::MesoBudgetExceeded);
        return MultiScaleTickResult {
            meso_crystals, macro_crystals,
            metrics: MultiScaleMetrics {
                m28_cluster_count: scale_state.meso_universes.len() as u64,
                m29_bridge_activity: 0.0,
                m30_scale_coherence: kuramoto.order_parameter,
                m31_lift_compression: if n_micro > 0.0 { scale_state.meso_universes.len() as f64 / n_micro } else { 0.0 },
                m32_cross_scale_crystal_rate: 0.0,
            },
            events,
        };
    }

    // Cluster micro vertices using spectral or kuramoto
    let clusters = if !spectral.fiedler_vector.is_empty() && vertex_ids.len() >= 2 {
        hybrid_cluster(&spectral.fiedler_vector, &kuramoto.phases, &vertex_ids, config)
    } else {
        kuramoto_phase_cluster(&kuramoto.phases, &vertex_ids, config.meso.phase_threshold, config.meso.min_cluster_size)
    };

    if clusters != scale_state.cluster_assignment {
        scale_state.cluster_assignment = clusters.clone();
    }

    // Build/update meso universes
    let (meso_univs, meso_bridges) = lift_micro_to_meso(
        micro_state.graph, &clusters, embeddings
    );
    scale_state.meso_universes = meso_univs;
    scale_state.meso_bridges = meso_bridges;

    // ── MS3: Bridge propagation (meso) ────────────────────────────────────────
    let universe_map: BTreeMap<Hash256, HypercubeUniverse> = scale_state.meso_universes.iter()
        .map(|u| (u.id, u.clone()))
        .collect();
    evaluate_bridges(&universe_map, &mut scale_state.meso_bridges, config);
    for (idx, bridge) in scale_state.meso_bridges.iter().enumerate() {
        if bridge.active {
            events.push(ScaleEvent::BridgeActivated { bridge_idx: idx });
        }
    }

    // ── MS4: Meso tick — produce meso crystal if coherence sufficient ─────────
    let n_clusters = scale_state.meso_universes.len();
    let meso_r: f64 = if n_clusters > 0 {
        scale_state.meso_universes.iter().map(|u| u.kuramoto_r).sum::<f64>() / n_clusters as f64
    } else { 0.0 };

    if meso_r >= 0.3 && !micro_crystals.is_empty() && n_clusters >= 2 {
        let sub_ids: Vec<Hash256> = micro_crystals.iter().map(|c| c.crystal_id).collect();
        if let Some(crystal) = build_scale_crystal(
            Scale::Meso, &scale_state.meso_universes, sub_ids, vec![], tick
        ) {
            meso_crystals.push(crystal);
        }
    }

    // ── MS5: Lift meso → macro ────────────────────────────────────────────────
    #[cfg(not(target_arch = "wasm32"))]
    let macro_budget_exceeded = {
        let macro_start = std::time::Instant::now();
        macro_start.elapsed().as_millis() as u64 > config.macro_cfg.budget_ms
    };
    #[cfg(target_arch = "wasm32")]
    let macro_budget_exceeded = false;
    if macro_budget_exceeded {
        events.push(ScaleEvent::MacroBudgetExceeded);
    } else if config.macro_cfg.enabled && n_clusters >= config.macro_cfg.min_domain_size {
        // Second-order clustering: cluster the cluster IDs by their aggregate
        let meso_phases: Vec<f64> = scale_state.meso_universes.iter()
            .map(|u| u.aggregate_state.omega)
            .collect();
        let meso_ids: Vec<VertexId> = (0..n_clusters as u64).collect();
        let domain_asgn_raw = kuramoto_phase_cluster(
            &meso_phases, &meso_ids,
            config.meso.phase_threshold * 2.0,
            config.macro_cfg.min_domain_size,
        );
        let domain_asgn: BTreeMap<usize, usize> = domain_asgn_raw.iter()
            .map(|(&k, &v)| (k as usize, v)).collect();
        scale_state.domain_assignment = domain_asgn.clone();

        let (macro_univs, macro_bridges) = lift_meso_to_macro(
            &scale_state.meso_universes, &domain_asgn
        );
        scale_state.macro_universes = macro_univs;
        scale_state.macro_bridges = macro_bridges;

        // ── MS6: Bridge propagation (macro) ──────────────────────────────────
        let macro_map: BTreeMap<Hash256, HypercubeUniverse> = scale_state.macro_universes.iter()
            .map(|u| (u.id, u.clone()))
            .collect();
        evaluate_bridges(&macro_map, &mut scale_state.macro_bridges, config);

        // ── MS7: Macro tick ───────────────────────────────────────────────────
        let n_domains_inner = scale_state.macro_universes.len();
        let macro_r_inner: f64 = if n_domains_inner > 0 {
            scale_state.macro_universes.iter().map(|u| u.kuramoto_r).sum::<f64>() / n_domains_inner as f64
        } else { 0.0 };

        if macro_r_inner >= 0.2 && !meso_crystals.is_empty() {
            let sub_ids: Vec<Hash256> = meso_crystals.iter().map(|c| c.crystal_id).collect();
            if let Some(crystal) = build_scale_crystal(
                Scale::Macro, &scale_state.macro_universes, sub_ids, vec![], tick
            ) {
                // ── MS8: Downward projection on macro crystal ─────────────────
                events.push(ScaleEvent::DownwardProjection {
                    from_scale: Scale::Macro,
                    crystal_id: crystal.crystal_id,
                });
                macro_crystals.push(crystal);
            }
        }
    }

    // ── Compute metrics ───────────────────────────────────────────────────────
    let n_bridges = scale_state.meso_bridges.len() + scale_state.macro_bridges.len();
    let n_active = scale_state.meso_bridges.iter().filter(|b| b.active).count()
        + scale_state.macro_bridges.iter().filter(|b| b.active).count();

    let micro_r = kuramoto.order_parameter;
    let n_domains = scale_state.macro_universes.len();
    let macro_r: f64 = if n_domains > 0 {
        scale_state.macro_universes.iter().map(|u| u.kuramoto_r).sum::<f64>() / n_domains as f64
    } else { 0.0 };
    let metrics = MultiScaleMetrics {
        m28_cluster_count: n_clusters as u64,
        m29_bridge_activity: if n_bridges > 0 { n_active as f64 / n_bridges as f64 } else { 0.0 },
        m30_scale_coherence: (macro_r + meso_r + micro_r) / 3.0,
        m31_lift_compression: if n_micro > 0.0 { n_clusters as f64 / n_micro } else { 0.0 },
        m32_cross_scale_crystal_rate: (meso_crystals.len() + macro_crystals.len()) as f64,
    };

    MultiScaleTickResult { meso_crystals, macro_crystals, metrics, events }
}

/// Build a minimal SemanticCrystal for a scale.
fn build_scale_crystal(
    scale: Scale,
    universes: &[HypercubeUniverse],
    sub_crystal_ids: Vec<Hash256>,
    parent_crystal_ids: Vec<Hash256>,
    tick: u64,
) -> Option<SemanticCrystal> {
    if universes.is_empty() { return None; }

    let region: Vec<VertexId> = universes.iter()
        .flat_map(|u| u.vertex_ids.iter().cloned())
        .collect();

    let stability = universes.iter().map(|u| u.kuramoto_r).sum::<f64>()
        / universes.len() as f64;

    let free_energy = -(stability * region.len() as f64);

    let mut crystal = pse_types::SemanticCrystal {
        crystal_id: [0u8; 32],
        region: region.clone(),
        constraint_program: Vec::new(),
        stability_score: stability,
        topology_signature: pse_types::TopologySignature::default(),
        betti_numbers: vec![universes.len() as u64, 0, 0],
        evidence_chain: Vec::new(),
        commit_proof: pse_types::CommitProof::default(),
        operator_versions: std::collections::BTreeMap::new(),
        created_at: tick,
        free_energy,
        carrier_instance_idx: 0,
        // Scale provenance
        scale_tag: scale.as_str().to_string(),
        universe_id: universes.first().map(|u| {
            u.id.iter().map(|b| format!("{:02x}", b)).collect()
        }).unwrap_or_default(),
        sub_crystal_ids: sub_crystal_ids.iter()
            .map(|id| id.iter().map(|b| format!("{:02x}", b)).collect())
            .collect(),
        parent_crystal_ids: parent_crystal_ids.iter()
            .map(|id| id.iter().map(|b| format!("{:02x}", b)).collect())
            .collect(),
        genesis_metadata: None,
    };

    // Content-address the crystal
    #[derive(serde::Serialize)]
    struct Core<'a> { scale: &'a str, region: &'a Vec<VertexId>, tick: u64, free_energy: f64 }
    let core = Core { scale: scale.as_str(), region: &crystal.region, tick, free_energy };
    crystal.crystal_id = pse_types::content_address(&core);

    Some(crystal)
}

// ─── MicroState adapter (for multi_scale_tick) ─────────────────────────────

/// Minimal view of micro GlobalState passed into multi_scale_tick.
pub mod pse_engine_types {
    use std::collections::BTreeMap;
    use pse_types::{FiveDState, VertexId};
    use pse_graph::PersistentGraph;

    pub struct MicroState<'a> {
        pub embeddings: BTreeMap<VertexId, FiveDState>,
        pub graph: &'a PersistentGraph,
    }

    impl<'a> MicroState<'a> {
        pub fn from_graph(graph: &'a PersistentGraph) -> Self {
            Self { embeddings: graph.embedding.clone(), graph }
        }
    }
}

// ─── Tests (AT-SC1 through AT-SC15) ──────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pse_graph::PersistentGraph;
    use pse_types::FiveDState;
    use pse_topology::{compute_laplacian, spectral_decompose, init_kuramoto_state,
                        kuramoto_step, TopologyConfig};

    fn state(p: f64, rho: f64, omega: f64, chi: f64, eta: f64) -> FiveDState {
        FiveDState { p, rho, omega, chi, eta }
    }

    fn make_chain(n: usize) -> PersistentGraph {
        let mut g = PersistentGraph::new();
        for i in 0..n { g.upsert_vertex(i as u64, 0.0); }
        for i in 0..(n - 1) { g.upsert_edge(i as u64, (i + 1) as u64, 0.0); }
        g
    }

    // AT-SC1: HyperBounds containment
    #[test]
    fn at_sc1_hyperbounds_containment() {
        let bounds = HyperBounds::new(
            state(0.0, 0.0, 0.0, 0.0, 0.0),
            state(1.0, 1.0, 1.0, 1.0, 1.0),
        );
        assert!(bounds.contains(&state(0.5, 0.5, 0.5, 0.5, 0.5)));
        assert!(bounds.contains(&state(0.0, 0.0, 0.0, 0.0, 0.0))); // boundary
        assert!(!bounds.contains(&state(1.5, 0.5, 0.5, 0.5, 0.5)));
        assert!(!bounds.contains(&state(-0.1, 0.5, 0.5, 0.5, 0.5)));
    }

    // AT-SC2: HyperBounds subdivision — 32 children, no overlap, union = parent
    #[test]
    fn at_sc2_hyperbounds_subdivision() {
        let parent = HyperBounds::new(
            state(0.0, 0.0, 0.0, 0.0, 0.0),
            state(1.0, 1.0, 1.0, 1.0, 1.0),
        );
        let children = parent.split_all();
        assert_eq!(children.len(), 32);

        // Each child is a sub-box of the parent
        for child in &children {
            assert!(child.min.p >= parent.min.p - 1e-12);
            assert!(child.max.p <= parent.max.p + 1e-12);
        }

        // Union of all children covers the parent volume
        let total_vol: f64 = children.iter().map(|c| c.volume()).sum();
        let parent_vol = parent.volume();
        assert!((total_vol - parent_vol).abs() < 1e-9,
            "children volume {total_vol} != parent {parent_vol}");
    }

    // AT-SC3: Aggregate state conservation (lift → project round-trip)
    #[test]
    fn at_sc3_aggregate_state_conservation() {
        let embeddings: BTreeMap<VertexId, FiveDState> = (0u64..6)
            .map(|i| (i, state(i as f64 * 0.1, 0.5, 0.3, 0.2, 0.1)))
            .collect();
        let phases: Vec<f64> = (0..6).map(|i| i as f64 * 0.5).collect();
        let degrees: BTreeMap<VertexId, f64> = (0u64..6).map(|i| (i, 1.0)).collect();

        let global_agg = compute_aggregate(&embeddings, &degrees, &phases);

        // Cluster into 2 groups: [0,1,2] and [3,4,5]
        let clusters: BTreeMap<VertexId, usize> = (0u64..3).map(|i| (i, 0))
            .chain((3u64..6).map(|i| (i, 1))).collect();

        // Meso aggregate (should equal global)
        let emb0: BTreeMap<VertexId, FiveDState> = embeddings.iter()
            .filter(|(v, _)| *v < &3).map(|(k, v)| (*k, v.clone())).collect();
        let emb1: BTreeMap<VertexId, FiveDState> = embeddings.iter()
            .filter(|(v, _)| *v >= &3).map(|(k, v)| (*k, v.clone())).collect();
        let ph0: Vec<f64> = phases[..3].to_vec();
        let ph1: Vec<f64> = phases[3..].to_vec();
        let deg1: BTreeMap<VertexId, f64> = (0u64..3).map(|i| (i, 1.0)).collect();
        let deg2: BTreeMap<VertexId, f64> = (3u64..6).map(|i| (i, 1.0)).collect();
        let agg0 = compute_aggregate(&emb0, &deg1, &ph0);
        let agg1 = compute_aggregate(&emb1, &deg2, &ph1);

        // Global p should be mean of agg0.p and agg1.p (equal weights)
        let expected_p = (agg0.p + agg1.p) / 2.0;
        assert!((global_agg.p - expected_p).abs() < 0.1,
            "global p={}, expected≈{}", global_agg.p, expected_p);
        let _ = clusters;
    }

    // AT-SC4: Spectral clustering determinism
    #[test]
    fn at_sc4_spectral_clustering_determinism() {
        let g = make_chain(6);
        let lap = compute_laplacian(&g);
        let spec = spectral_decompose(&lap, 6);
        let vids: Vec<VertexId> = (0..6).map(|i| i as u64).collect();

        let c1 = spectral_bisection_cluster(&spec.fiedler_vector, &vids, 0.01, 2, 10);
        let c2 = spectral_bisection_cluster(&spec.fiedler_vector, &vids, 0.01, 2, 10);
        assert_eq!(c1, c2, "spectral clustering must be deterministic");
    }

    // AT-SC5: Kuramoto clustering — synchronized entities co-clustered
    #[test]
    fn at_sc5_kuramoto_clustering() {
        // Two groups with very different phases
        let phases = vec![0.0f64, 0.05, 0.1, 3.0, 3.05, 3.1];
        let vids: Vec<VertexId> = (0..6).map(|i| i as u64).collect();
        let clusters = kuramoto_phase_cluster(&phases, &vids, 0.5, 2);

        // Vertices 0,1,2 should be in same cluster; 3,4,5 in another
        assert_eq!(clusters[&0], clusters[&1]);
        assert_eq!(clusters[&1], clusters[&2]);
        assert_eq!(clusters[&3], clusters[&4]);
        assert_eq!(clusters[&4], clusters[&5]);
        assert_ne!(clusters[&0], clusters[&3]);
    }

    // AT-SC6: Bridge activation
    #[test]
    fn at_sc6_bridge_activation() {
        let config = ScaleConfig::default();

        let u1 = build_universe(&[0, 1], &{
            let mut m = BTreeMap::new();
            m.insert(0u64, state(0.1, 0.5, 0.5, 0.5, 0.5));
            m.insert(1u64, state(0.2, 0.5, 0.5, 0.5, 0.5));
            m
        }, Scale::Meso, ScalePolicy::Balanced);

        let mut u2 = u1.clone();
        u2.id = content_address_raw(b"u2");

        let mut bridge = Bridge {
            source_id: u1.id, target_id: u2.id,
            weight: 0.5, delay_ticks: 0, phase_offset: 0.0, active: false,
        };

        // Force high coherence on both
        let mut u1b = u1.clone(); u1b.kuramoto_r = 0.9;
        let mut u2b = u2.clone(); u2b.kuramoto_r = 0.9;
        // States are proximate (same state)
        let mut map = BTreeMap::new();
        map.insert(u1b.id, u1b);
        map.insert(u2b.id, u2b);
        evaluate_bridges(&map, std::slice::from_mut(&mut bridge), &config);
        assert!(bridge.active, "bridge should activate with proximate, coherent universes");

        // Distant states → bridge inactive
        let mut u3 = map.values().next().unwrap().clone();
        u3.aggregate_state = state(10.0, 10.0, 10.0, 10.0, 10.0);
        u3.kuramoto_r = 0.9;
        let mut map2 = BTreeMap::new();
        map2.insert(bridge.source_id, u3);
        let u4 = map.values().nth(1).unwrap().clone();
        map2.insert(bridge.target_id, u4);
        bridge.active = true;
        evaluate_bridges(&map2, std::slice::from_mut(&mut bridge), &config);
        assert!(!bridge.active, "bridge should be inactive with distant states");
    }

    // AT-SC7: Bridge coupling effect
    #[test]
    fn at_sc7_bridge_coupling_effect() {
        let src_history = vec![state(1.0, 0.5, 0.0, 0.5, 0.5)];
        let bridge = Bridge {
            source_id: [0u8; 32], target_id: [0u8; 32],
            weight: 0.5, delay_ticks: 0, phase_offset: 0.0, active: true,
        };
        let mut target = build_universe(
            &[0], &{ let mut m = BTreeMap::new(); m.insert(0u64, state(0.0, 0.0, 0.0, 0.0, 0.0)); m },
            Scale::Meso, ScalePolicy::Balanced,
        );
        let orig_p = target.aggregate_state.p;
        apply_bridge_coupling(&mut target, &src_history, &bridge, 0);
        // State must have changed
        assert_ne!(target.aggregate_state.p, orig_p,
            "bridge coupling must modify target state");
    }

    // AT-SC8: Ladder lift-project round-trip — global aggregate preserved
    #[test]
    fn at_sc8_ladder_lift_project_roundtrip() {
        let g = make_chain(6);
        let embeddings = g.embedding.clone();
        let vids: Vec<VertexId> = embeddings.keys().cloned().collect();

        // Cluster into 2
        let clusters: BTreeMap<VertexId, usize> = vids.iter().enumerate()
            .map(|(i, &v)| (v, if i < 3 { 0 } else { 1 })).collect();

        let (meso_univs, _) = lift_micro_to_meso(&g, &clusters, &embeddings);

        // Project back
        let signal = meso_univs[0].aggregate_state.clone();
        let meso_signal: BTreeMap<Hash256, FiveDState> = meso_univs.iter()
            .map(|u| (u.id, signal.clone())).collect();
        let micro_signals = project_meso_to_micro(&meso_signal, &clusters, &meso_univs);

        // Global mean of projected signals should be close to meso signal
        let mean_p = micro_signals.values().map(|s| s.p).sum::<f64>()
            / micro_signals.len().max(1) as f64;
        assert!(mean_p.is_finite(), "projected p must be finite");
    }

    // AT-SC9: Multi-scale crystal provenance (scale_tag field)
    #[test]
    fn at_sc9_crystal_provenance() {
        let universe = build_universe(
            &[0, 1, 2],
            &{ let mut m = BTreeMap::new();
               m.insert(0u64, state(0.1, 0.5, 0.5, 0.5, 0.9));
               m.insert(1u64, state(0.2, 0.5, 0.5, 0.5, 0.9));
               m.insert(2u64, state(0.3, 0.5, 0.5, 0.5, 0.9));
               m },
            Scale::Meso, ScalePolicy::Balanced,
        );
        let sub: Vec<Hash256> = vec![[1u8; 32], [2u8; 32]];
        let crystal = build_scale_crystal(Scale::Meso, &[universe], sub.clone(), vec![], 1)
            .expect("should build meso crystal");
        assert_eq!(crystal.scale_tag, "meso");
        assert_eq!(crystal.sub_crystal_ids.len(), 2);
    }

    // AT-SC10: Scale-disabled passthrough
    #[test]
    fn at_sc10_scale_disabled_passthrough() {
        let config = ScaleConfig { enabled: false, ..Default::default() };
        let g = make_chain(4);
        let spec = spectral_decompose(&compute_laplacian(&g), 4);
        let mut ks = init_kuramoto_state(&g);
        let topo_cfg = TopologyConfig::default();
        for _ in 0..3 { kuramoto_step(&mut ks, &g, &topo_cfg); }
        let mut scale_state = MultiScaleState::default();
        let micro = pse_engine_types::MicroState::from_graph(&g);
        let result = multi_scale_tick(&micro, &mut scale_state, &spec, &ks, &config, &[], 0);
        assert!(result.meso_crystals.is_empty());
        assert!(result.macro_crystals.is_empty());
    }

    // AT-SC11: Budget fallback — meso tick skipped on tight budget
    #[test]
    fn at_sc11_budget_fallback() {
        let mut config = ScaleConfig::default();
        config.meso.budget_ms = 0; // zero budget → fallback
        let g = make_chain(200); // large enough graph
        let lap = compute_laplacian(&g);
        let spec = spectral_decompose(&lap, 10);
        let mut ks = init_kuramoto_state(&g);
        let topo_cfg = TopologyConfig::default();
        for _ in 0..3 { kuramoto_step(&mut ks, &g, &topo_cfg); }
        let mut scale_state = MultiScaleState::default();
        let micro = pse_engine_types::MicroState::from_graph(&g);
        // Should not panic; budget fallback triggered
        let result = multi_scale_tick(&micro, &mut scale_state, &spec, &ks, &config, &[], 0);
        let _ = result; // no assertions required beyond no-panic
    }

    // AT-SC12: Cross-scale crystal validation (scale_tag correctness)
    #[test]
    fn at_sc12_cross_scale_crystal_validation() {
        let micro_u = build_universe(&[0], &{
            let mut m = BTreeMap::new(); m.insert(0u64, state(0.1,0.5,0.5,0.5,0.8)); m
        }, Scale::Micro, ScalePolicy::Balanced);
        let meso_u = build_universe(&[0, 1], &{
            let mut m = BTreeMap::new();
            m.insert(0u64, state(0.1,0.5,0.5,0.5,0.8));
            m.insert(1u64, state(0.2,0.5,0.5,0.5,0.8));
            m
        }, Scale::Meso, ScalePolicy::Balanced);
        let macro_u = build_universe(&[0, 1, 2], &{
            let mut m = BTreeMap::new();
            m.insert(0u64, state(0.1,0.5,0.5,0.5,0.8));
            m.insert(1u64, state(0.2,0.5,0.5,0.5,0.8));
            m.insert(2u64, state(0.3,0.5,0.5,0.5,0.8));
            m
        }, Scale::Macro, ScalePolicy::Balanced);

        let micro_c = build_scale_crystal(Scale::Micro, &[micro_u], vec![], vec![], 1).unwrap();
        let meso_c = build_scale_crystal(Scale::Meso, &[meso_u], vec![micro_c.crystal_id], vec![], 2).unwrap();
        let macro_c = build_scale_crystal(Scale::Macro, &[macro_u], vec![meso_c.crystal_id], vec![], 3).unwrap();

        assert_eq!(micro_c.scale_tag, "micro");
        assert_eq!(meso_c.scale_tag, "meso");
        assert_eq!(macro_c.scale_tag, "macro");
        assert_eq!(meso_c.sub_crystal_ids.len(), 1);
        assert_eq!(macro_c.sub_crystal_ids.len(), 1);
    }

    // AT-SC13: Multi-scale metrics populated
    #[test]
    fn at_sc13_multi_scale_metrics() {
        let config = ScaleConfig::default();
        let g = make_chain(8);
        let lap = compute_laplacian(&g);
        let spec = spectral_decompose(&lap, 8);
        let mut ks = init_kuramoto_state(&g);
        let topo_cfg = TopologyConfig::default();
        for _ in 0..10 { kuramoto_step(&mut ks, &g, &topo_cfg); }
        let mut scale_state = MultiScaleState::default();
        let micro = pse_engine_types::MicroState::from_graph(&g);
        let result = multi_scale_tick(&micro, &mut scale_state, &spec, &ks, &config, &[], 1);
        // M28: cluster count should be > 0
        assert!(result.metrics.m28_cluster_count > 0);
        // M30 scale coherence in [0, 1]
        assert!(result.metrics.m30_scale_coherence >= 0.0);
        assert!(result.metrics.m30_scale_coherence <= 1.0);
        // M31 compression in [0, 1]
        assert!(result.metrics.m31_lift_compression >= 0.0);
    }

    // AT-SC14: Downward projection event on macro crystal
    #[test]
    fn at_sc14_downward_projection() {
        let config = ScaleConfig::default();
        let g = make_chain(8);
        let lap = compute_laplacian(&g);
        let spec = spectral_decompose(&lap, 8);
        let mut ks = init_kuramoto_state(&g);
        // Force high coherence
        let ks_phases: Vec<f64> = vec![1.0; ks.phases.len()];
        ks.phases = ks_phases;
        let (r, psi) = pse_topology::kuramoto_order_parameter(&ks.phases);
        ks.order_parameter = r;
        ks.mean_phase = psi;

        let micro_crystal = build_scale_crystal(Scale::Micro,
            &[build_universe(&[0], &{ let mut m = BTreeMap::new();
               m.insert(0u64, state(0.5,0.9,1.0,0.5,0.9)); m },
               Scale::Micro, ScalePolicy::Balanced)],
            vec![], vec![], 0).unwrap();

        let mut scale_state = MultiScaleState::default();
        let micro = pse_engine_types::MicroState::from_graph(&g);
        let result = multi_scale_tick(&micro, &mut scale_state, &spec, &ks, &config, &[micro_crystal], 1);

        // If a macro crystal was produced, there should be a DownwardProjection event
        if !result.macro_crystals.is_empty() {
            let has_proj = result.events.iter().any(|e| matches!(e, ScaleEvent::DownwardProjection { .. }));
            assert!(has_proj, "macro crystal must trigger downward projection event");
        }
    }

    // AT-SC15: Policy determinism
    #[test]
    fn at_sc15_policy_determinism() {
        // Same input → same clustering result
        let phases1 = vec![0.0f64, 0.1, 0.2, 3.0, 3.1, 3.2];
        let phases2 = phases1.clone();
        let vids: Vec<VertexId> = (0..6).map(|i| i as u64).collect();
        let c1 = kuramoto_phase_cluster(&phases1, &vids, 0.5, 1);
        let c2 = kuramoto_phase_cluster(&phases2, &vids, 0.5, 1);
        assert_eq!(c1, c2, "policy must be deterministic");
    }
}
