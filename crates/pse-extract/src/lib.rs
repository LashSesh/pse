//! Constraint extraction from observations (Layer L2).
//!
//! Reads the persistent graph to discover constraint candidates and weave
//! them into constraint programs using configurable operator libraries.

// isls-extract: Constraint extraction (Layer L2 / ECLS assimilated)
// C4 — depends on pse-types, pse-graph
// Inv I5: read-only on PersistentGraph (takes &PersistentGraph, immutable ref)

use std::collections::BTreeMap;
use pse_types::{
    content_address, ConstraintCandidate, ConstraintProgram, ConstraintTemplate,
    ExtractionConfig, FiveDState, VertexId,
};
use pse_graph::PersistentGraph;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExtractError {
    #[error("extraction failed: {0}")]
    Extraction(String),
}

pub type Result<T> = std::result::Result<T, ExtractError>;

// ─── Time Window ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct TimeWindow {
    pub start: f64,
    pub end: f64,
}

impl TimeWindow {
    pub fn last_n_hours(hours: f64) -> Self {
        // In a real system this would use wall clock; for determinism use a sentinel
        let end = f64::MAX;
        let start = end - hours * 3600.0;
        Self { start, end }
    }

    pub fn all() -> Self {
        Self { start: f64::NEG_INFINITY, end: f64::INFINITY }
    }
}

// ─── Operator Trait ───────────────────────────────────────────────────────────

/// Core operator trait (OI-02)
pub trait Operator: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> &str;
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64; // -> [0,1]
    fn is_deterministic(&self) -> bool {
        true
    }
    fn template(&self) -> ConstraintTemplate;
}

// ─── Reference Operators ─────────────────────────────────────────────────────

/// Band operator: evaluates if a dimension is within a band [lo, hi]
pub struct BandOp;

impl Operator for BandOp {
    fn id(&self) -> &str { "band" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Band }
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64 {
        let lo = params.get("lo").copied().unwrap_or(0.0);
        let hi = params.get("hi").copied().unwrap_or(1.0);
        let dim = params.get("dim").copied().unwrap_or(0.0) as usize;
        let v = input.as_array()[dim.min(4)];
        if v >= lo && v <= hi { 1.0 } else { 0.0 }
    }
}

/// Ratio operator: evaluates ratio of two dimensions
pub struct RatioOp;

impl Operator for RatioOp {
    fn id(&self) -> &str { "ratio" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Ratio }
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64 {
        let a_idx = params.get("a").copied().unwrap_or(0.0) as usize;
        let b_idx = params.get("b").copied().unwrap_or(1.0) as usize;
        let arr = input.as_array();
        let a = arr[a_idx.min(4)];
        let b = arr[b_idx.min(4)];
        if b.abs() < 1e-10 { 0.0 } else { (a / b).abs().min(1.0) }
    }
}

/// Correlation operator: measures correlation-like metric between dimensions
pub struct CorrelationOp;

impl Operator for CorrelationOp {
    fn id(&self) -> &str { "correlation" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Correlation }
    fn evaluate(&self, input: &FiveDState, _params: &BTreeMap<String, f64>) -> f64 {
        // Normalized dot product as correlation proxy
        let norm_sq = input.norm_sq();
        if norm_sq < 1e-10 { 0.0 } else { (1.0 - 1.0 / (1.0 + norm_sq)).min(1.0) }
    }
}

/// Granger operator: causality proxy
pub struct GrangerOp;

impl Operator for GrangerOp {
    fn id(&self) -> &str { "granger" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Granger }
    fn evaluate(&self, input: &FiveDState, _params: &BTreeMap<String, f64>) -> f64 {
        // Eta dimension as causality metric, normalized to [0,1]
        (input.eta.abs() / (1.0 + input.eta.abs())).min(1.0)
    }
}

/// Spectral operator: frequency-based metric
pub struct SpectralOp;

impl Operator for SpectralOp {
    fn id(&self) -> &str { "spectral" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Spectral }
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64 {
        let target = params.get("target_freq").copied().unwrap_or(1.0);
        let bw = params.get("bandwidth").copied().unwrap_or(1.0);
        let diff = (input.omega - target).abs();
        (-diff / bw).exp()
    }
}

/// Topological operator: topology-based score
pub struct TopologicalOp;

impl Operator for TopologicalOp {
    fn id(&self) -> &str { "topological" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Topological }
    fn evaluate(&self, input: &FiveDState, _params: &BTreeMap<String, f64>) -> f64 {
        // Chi dimension as connectivity/topology proxy
        (input.chi.abs() / (1.0 + input.chi.abs())).min(1.0)
    }
}

/// Phase operator: phase coherence metric
pub struct PhaseOp;

impl Operator for PhaseOp {
    fn id(&self) -> &str { "phase" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Phase }
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64 {
        let target = params.get("target_phase").copied().unwrap_or(0.0);
        let diff = (input.omega - target).abs() % std::f64::consts::TAU;
        let diff = diff.min(std::f64::consts::TAU - diff);
        1.0 - diff / std::f64::consts::PI
    }
}

/// Contraction operator: measures contraction of the state space
pub struct ContractionOp;

impl Operator for ContractionOp {
    fn id(&self) -> &str { "contraction" }
    fn version(&self) -> &str { "1.0.0" }
    fn template(&self) -> ConstraintTemplate { ConstraintTemplate::Contraction }
    fn evaluate(&self, input: &FiveDState, params: &BTreeMap<String, f64>) -> f64 {
        let radius = params.get("radius").copied().unwrap_or(1.0);
        let norm = input.norm_sq().sqrt();
        (-norm / radius).exp()
    }
}

/// Default library of all 8 reference operators
pub fn default_operator_library() -> Vec<Box<dyn Operator>> {
    vec![
        Box::new(BandOp),
        Box::new(RatioOp),
        Box::new(CorrelationOp),
        Box::new(GrangerOp),
        Box::new(SpectralOp),
        Box::new(TopologicalOp),
        Box::new(PhaseOp),
        Box::new(ContractionOp),
    ]
}

// ─── Helper Functions ─────────────────────────────────────────────────────────

/// Compute variance of a region
pub fn variance(region: &[(VertexId, FiveDState)]) -> f64 {
    if region.len() < 2 {
        return 0.0;
    }
    let n = region.len() as f64;
    // Variance = mean(sum of squared norms) - mean(norm)^2
    let mean_p: f64 = region.iter().map(|(_, s)| s.p).sum::<f64>() / n;
    let var_p: f64 = region.iter().map(|(_, s)| (s.p - mean_p).powi(2)).sum::<f64>() / n;
    var_p
}

/// Entropy estimate for a region
pub fn region_entropy(region: &[(VertexId, FiveDState)]) -> f64 {
    if region.is_empty() {
        return 0.0;
    }
    // Differential entropy approximation via variance
    let v = variance(region);
    if v < 1e-15 { 0.0 } else { 0.5 * (2.0 * std::f64::consts::PI * std::f64::consts::E * v).ln() }
}

/// Evaluate a constraint candidate against a state
fn evaluate_candidate(
    op: &dyn Operator,
    state: &FiveDState,
    config: &ExtractionConfig,
) -> Option<ConstraintCandidate> {
    let params = BTreeMap::new();
    let score = op.evaluate(state, &params);
    if score < 0.1 {
        return None;
    }

    // Build candidate from operator output
    let mut candidate_params = BTreeMap::new();
    candidate_params.insert("score".to_string(), score);

    // Compute a stable ID for this constraint type on this state
    #[derive(serde::Serialize)]
    struct CandidateKey<'a> { op_id: &'a str, template: &'a ConstraintTemplate, score_bucket: u64 }
    let key = CandidateKey {
        op_id: op.id(),
        template: &op.template(),
        score_bucket: (score * 100.0) as u64,
    };
    let id = content_address(&key);

    Some(ConstraintCandidate {
        id,
        template: op.template(),
        parameters: candidate_params,
        coverage: score,
        threshold: config.alpha_min,
        formation_energy: -score, // negative = spontaneous
        bond_strength: 1,
        activation_energy: 1.0 - score,
    })
}

/// Select best constraint by info-gain per risk
fn select_best(candidates: &[ConstraintCandidate]) -> Option<&ConstraintCandidate> {
    candidates.iter().max_by(|a, b| {
        let qa = (-a.formation_energy) / (a.activation_energy + 1e-10);
        let qb = (-b.formation_energy) / (b.activation_energy + 1e-10);
        qa.partial_cmp(&qb).unwrap_or(std::cmp::Ordering::Equal)
    })
}

// ─── Inverse Weaving ─────────────────────────────────────────────────────────

/// Inverse Weaving: recover emergent constraint program (PSE Algo 1)
/// Inv I5: takes &PersistentGraph (immutable ref)
pub fn inverse_weave(
    graph: &PersistentGraph,
    _window: &TimeWindow,
    library: &[Box<dyn Operator>],
    config: &ExtractionConfig,
) -> (ConstraintProgram, Vec<VertexId>) {
    let point_cloud: Vec<(VertexId, FiveDState)> = graph.point_cloud();

    // Scan constraints over all states
    let mut active: Vec<ConstraintCandidate> = Vec::new();
    for (_, state) in &point_cloud {
        for op in library {
            if let Some(candidate) = evaluate_candidate(op.as_ref(), state, config) {
                let pass = candidate.coverage >= config.alpha_min;
                eprintln!("  candidate {}: coverage={:.4}, alpha_min={:.4}, pass={}",
                          op.id(), candidate.coverage, config.alpha_min, pass);
                if pass {
                    active.push(candidate);
                }
            }
        }
    }

    // Deduplicate by ID using BTreeMap
    let mut seen_ids: BTreeMap<[u8; 32], ()> = BTreeMap::new();
    active.retain(|c| seen_ids.insert(c.id, ()).is_none());

    let mut program = ConstraintProgram::new();
    let mut region = point_cloud.clone();

    while !active.is_empty() && variance(&region) > config.convergence_tau {
        let best = match select_best(&active) {
            Some(b) => b.clone(),
            None => break,
        };

        // Retain only points that pass the best constraint
        // (simplified: retain all since we don't have per-state evaluate)
        let before_len = region.len();
        region.retain(|(_, state)| {
            let params = BTreeMap::new();
            // Find the operator matching this template
            let score = library
                .iter()
                .find(|op| op.template() == best.template)
                .map(|op| op.evaluate(state, &params))
                .unwrap_or(0.0);
            score >= best.threshold
        });
        let contraction = if before_len > 0 {
            region.len() as f64 / before_len as f64
        } else {
            0.0
        };

        program.push(best.clone());
        active.retain(|c| c.id != best.id);

        if contraction > config.kappa_max {
            break;
        }
    }

    eprintln!("  inverse_weave result: {} active constraints, region size={}",
              program.len(), region.len());
    let vertex_ids: Vec<VertexId> = region.iter().map(|(vid, _)| *vid).collect();
    (program, vertex_ids)
}

/// Lattice free energy: F(L) = -sum(q(g_i)) + T * H(K*) (PSE Def 14.4, OI-06)
pub fn lattice_free_energy(
    program: &ConstraintProgram,
    region: &[(VertexId, FiveDState)],
    temperature: f64,
) -> f64 {
    let info_gain: f64 = program.iter().map(|c| -c.formation_energy).sum();
    let entropy = region_entropy(region);
    -info_gain + temperature * entropy
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_graph::PersistentGraph;

    #[test]
    fn inverse_weave_read_only() {
        // Inv I5: graph passed as immutable ref, verify it's unchanged
        let mut g = PersistentGraph::new();
        let initial_commit = g.commit_index;

        let library = default_operator_library();
        let window = TimeWindow::all();
        let config = ExtractionConfig::default();

        let (program, region) = inverse_weave(&g, &window, &library, &config);
        // Graph unchanged
        assert_eq!(g.commit_index, initial_commit);
        // Empty graph produces empty program
        assert!(program.is_empty());
        assert!(region.is_empty());
    }

    #[test]
    fn band_op_evaluates() {
        let op = BandOp;
        let state = FiveDState { p: 0.5, rho: 0.0, omega: 0.0, chi: 0.0, eta: 0.0 };
        let mut params = BTreeMap::new();
        params.insert("lo".to_string(), 0.0);
        params.insert("hi".to_string(), 1.0);
        params.insert("dim".to_string(), 0.0);
        assert_eq!(op.evaluate(&state, &params), 1.0);
    }

    #[test]
    fn band_op_outside_band() {
        let op = BandOp;
        let state = FiveDState { p: 2.0, rho: 0.0, omega: 0.0, chi: 0.0, eta: 0.0 };
        let mut params = BTreeMap::new();
        params.insert("lo".to_string(), 0.0);
        params.insert("hi".to_string(), 1.0);
        params.insert("dim".to_string(), 0.0);
        assert_eq!(op.evaluate(&state, &params), 0.0);
    }

    #[test]
    fn lattice_free_energy_zero_program() {
        let program = ConstraintProgram::new();
        let region: Vec<(VertexId, FiveDState)> = Vec::new();
        let fe = lattice_free_energy(&program, &region, 1.0);
        assert_eq!(fe, 0.0);
    }
}
