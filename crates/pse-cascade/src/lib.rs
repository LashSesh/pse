//! Adversarial validation cascade, consensus, and carrier geometry for PSE.
//!
//! Combines consensus validation (PoR gate, dual consensus, cascade operators)
//! with carrier geometry (helix pairs, mandorla, phase ladder).

//! Consensus validation and proof-of-rigor gate (Layer L3).
//!
//! Implements dual-consensus operators, metric normalization, and the PoR
//! finite-state machine that gates crystal formation.

// isls-consensus: Consensus, PoR gate, proof engine (Layer L3)
// C5 — depends on pse-types, pse-graph

use pse_types::{
    ConsensusConfig, ConsensusResult, GateSnapshot, NormalizationConfig,
    PoRTrace, ThresholdConfig,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("consensus failed: {0}")]
    Failed(String),
}

pub type Result<T> = std::result::Result<T, ConsensusError>;

// ─── Normalization Functions (PSE Sec 10) ───────────────────────────────────

/// Saturation normalization: N(u; mu) = u / (u + mu) (Def 10.1)
pub fn norm_saturate(u: f64, mu: f64) -> f64 {
    u / (u + mu)
}

/// Exponential normalization: N_exp(d; lambda) = exp(-lambda * d) (Def 10.2)
pub fn norm_exp(d: f64, lambda: f64) -> f64 {
    (-lambda * d).exp()
}

// ─── Metric Set ───────────────────────────────────────────────────────────────

/// All 11 metrics from PSE Sec 10
#[derive(Clone, Debug, Default)]
pub struct MetricSet {
    pub d_deformation: f64,  // Def 10.3: N(D_raw; mu_D)
    pub q_coherence: f64,    // Def 10.4: N(Q_raw; mu_Q)
    pub r_resonance: f64,    // Def 10.5: exp(-d_R(H, s_ref))
    pub g_readiness: f64,    // Def 10.6: gamma_D*D + gamma_Q*Q + gamma_R*R
    pub j_doublekick: f64,   // Def 10.7: N(J_raw; mu_J)
    pub p_projection: f64,   // Def 10.8: exp(-diam(P) * lambda_P)
    pub n_seam: f64,         // Def 10.9: exp(-d_seam(L,R))
    pub k_crystal: f64,      // Def 10.10: lambda_C*C + lambda_E*E
    pub f_friction: f64,     // Def 10.11: N(F_raw; mu_F)
    pub s_shock: f64,        // Def 10.12: N(S_raw; mu_S)
    pub l_migration: f64,    // from carrier readiness
}

impl MetricSet {
    pub fn gate_snapshot(&self, thresholds: &ThresholdConfig) -> GateSnapshot {
        GateSnapshot {
            d: self.d_deformation,
            q: self.q_coherence,
            r: self.r_resonance,
            g: self.g_readiness,
            j: self.j_doublekick,
            p: self.p_projection,
            n: self.n_seam,
            k: self.k_crystal,
            kairos: self.d_deformation >= thresholds.d
                && self.q_coherence >= thresholds.q
                && self.r_resonance >= thresholds.r
                && self.g_readiness >= thresholds.g
                && self.j_doublekick >= thresholds.j
                && self.p_projection >= thresholds.p
                && self.n_seam >= thresholds.n
                && self.k_crystal >= thresholds.k,
        }
    }

    /// Compute g_readiness from components
    pub fn compute_readiness(&mut self, norm: &NormalizationConfig) {
        self.g_readiness = norm.gamma_d * self.d_deformation
            + norm.gamma_q * self.q_coherence
            + norm.gamma_r * self.r_resonance;
    }

    /// Compute k_crystal from components
    pub fn compute_k_crystal(&mut self, coherence: f64, entropy: f64, norm: &NormalizationConfig) {
        self.k_crystal = norm.lambda_c * coherence + norm.lambda_e * (1.0 - entropy.min(1.0));
    }
}

// ─── PoR State Machine ───────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum PoRState {
    Search,
    Lock,
    Verify,
    Commit,
}

pub struct PoRFsm {
    pub state: PoRState,
    pub stability_history: Vec<f64>,
    pub lock_tick: Option<f64>,
    pub verify_tick: Option<f64>,
    pub trace: PoRTrace,
}

impl Default for PoRFsm {
    fn default() -> Self {
        Self {
            state: PoRState::Search,
            stability_history: Vec::new(),
            lock_tick: None,
            verify_tick: None,
            trace: PoRTrace { search_enter: 0.0, ..Default::default() },
        }
    }
}

impl PoRFsm {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn step(&mut self, kappa: f64, t2: f64, config: &ConsensusConfig) -> PoRState {
        match self.state {
            PoRState::Search => {
                if kappa >= config.por_kappa_bar {
                    self.stability_history.push(kappa);
                    if self.stability_history.len() >= config.por_t_min {
                        self.state = PoRState::Lock;
                        self.lock_tick = Some(t2);
                        self.trace.lock_enter = Some(t2);
                    }
                } else {
                    self.stability_history.clear();
                }
            }
            PoRState::Lock => {
                let last = self.stability_history.last().copied().unwrap_or(0.0);
                let delta = (kappa - last).abs();
                if delta <= config.por_epsilon {
                    self.stability_history.push(kappa);
                    if self.stability_history.len()
                        >= config.por_t_min + config.por_t_stable
                    {
                        self.state = PoRState::Verify;
                        self.verify_tick = Some(t2);
                        self.trace.verify_enter = Some(t2);
                    }
                } else {
                    self.reset(t2);
                }
            }
            PoRState::Verify => {
                // Check policy constraints and latch time
                // For now, immediately commit if kappa still >= threshold
                if kappa >= config.por_kappa_bar {
                    self.state = PoRState::Commit;
                    self.trace.commit_enter = Some(t2);
                } else {
                    self.reset(t2);
                }
            }
            PoRState::Commit => {
                // Terminal state until reset
            }
        }
        self.state.clone()
    }

    pub fn reset(&mut self, t2: f64) {
        self.state = PoRState::Search;
        self.stability_history.clear();
        self.lock_tick = None;
        self.verify_tick = None;
        self.trace = PoRTrace {
            search_enter: t2,
            lock_enter: None,
            verify_enter: None,
            commit_enter: None,
        };
    }

    pub fn get_trace(&self) -> &PoRTrace {
        &self.trace
    }
}

// ─── Crystal Precursor ───────────────────────────────────────────────────────

/// Precursor for crystal formation (before consensus)
#[derive(Clone, Debug)]
pub struct CrystalPrecursor {
    pub program: pse_types::ConstraintProgram,
    pub region: Vec<pse_types::VertexId>,
    pub seam_score: f64,
    pub metrics: MetricSet,
    pub stability_score: f64,
}

impl CrystalPrecursor {
    pub fn stability_score(&self) -> f64 {
        self.stability_score
    }

    pub fn distance(&self, other: &Self) -> f64 {
        let s1 = self.stability_score;
        let s2 = other.stability_score;
        (s1 - s2).abs()
    }
}

// ─── Cascade Operator Trait ──────────────────────────────────────────────────

/// Cascade operator for dual consensus (DK -> SW -> PI -> WT and reverse)
pub trait CascadeOperator: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, precursor: &CrystalPrecursor) -> CrystalPrecursor;
}

// Reference cascade operators
pub struct DKOperator;
pub struct SWOperator;
pub struct PIOperator;
pub struct WTOperator;

impl CascadeOperator for DKOperator {
    fn name(&self) -> &str { "DK" }
    fn apply(&self, p: &CrystalPrecursor) -> CrystalPrecursor {
        let mut out = p.clone();
        // Double-kick: amplify stability by coherence factor
        out.stability_score = (p.stability_score * 1.1).min(1.0);
        out
    }
}

impl CascadeOperator for SWOperator {
    fn name(&self) -> &str { "SW" }
    fn apply(&self, p: &CrystalPrecursor) -> CrystalPrecursor {
        let mut out = p.clone();
        // Symmetry-weave: maintain stability
        out.stability_score = p.stability_score;
        out
    }
}

impl CascadeOperator for PIOperator {
    fn name(&self) -> &str { "PI" }
    fn apply(&self, p: &CrystalPrecursor) -> CrystalPrecursor {
        let mut out = p.clone();
        // Phase integration: smooth
        out.stability_score = (p.stability_score * 0.95 + 0.05).min(1.0);
        out
    }
}

impl CascadeOperator for WTOperator {
    fn name(&self) -> &str { "WT" }
    fn apply(&self, p: &CrystalPrecursor) -> CrystalPrecursor {
        let mut out = p.clone();
        // Wave transfer: finalize
        out.stability_score = (p.stability_score * 1.05).min(1.0);
        out
    }
}

pub fn run_cascade(
    precursor: &CrystalPrecursor,
    ops: &[&dyn CascadeOperator],
) -> CrystalPrecursor {
    let mut state = precursor.clone();
    for op in ops {
        state = op.apply(&state);
    }
    state
}

/// Run primal and dual operator paths; check MCI (OI-04/OI-05)
pub fn dual_consensus(
    precursor: &CrystalPrecursor,
    primal_ops: &[&dyn CascadeOperator], // DK -> SW -> PI -> WT
    dual_ops: &[&dyn CascadeOperator],   // PI -> WT -> DK -> SW
    config: &ConsensusConfig,
) -> ConsensusResult {
    let primal_state = run_cascade(precursor, primal_ops);
    let dual_state = run_cascade(precursor, dual_ops);
    let mci = 1.0 - primal_state.distance(&dual_state);
    ConsensusResult {
        primal_score: primal_state.stability_score(),
        dual_score: dual_state.stability_score(),
        mci,
        threshold: config.consensus_threshold,
    }
}

/// Default primal cascade operators: DK -> SW -> PI -> WT
pub fn default_primal_ops() -> (DKOperator, SWOperator, PIOperator, WTOperator) {
    (DKOperator, SWOperator, PIOperator, WTOperator)
}

/// Default dual cascade operators: PI -> WT -> DK -> SW
pub fn default_dual_ops() -> (PIOperator, WTOperator, DKOperator, SWOperator) {
    (PIOperator, WTOperator, DKOperator, SWOperator)
}

// ─── Carrier Geometry ─────────────────────────────────────────────────────

use pse_types::{
    CarrierConfig, CarrierInstance, MandorlaState, PhaseLadder, TubusCoord,
};


// ─── Helix Pair ───────────────────────────────────────────────────────────────

/// Helix pair with pi-phase coupling (PSE Def 7.2, Inv I15)
/// Inv I15: enforces pi offset between helix_a and helix_b
pub fn helix_pair(tau: f64, phi: f64, r: f64) -> (TubusCoord, TubusCoord) {
    let alpha = TubusCoord { tau, phi, r };
    let beta = TubusCoord {
        tau,
        phi: (phi + std::f64::consts::PI) % (2.0 * std::f64::consts::PI),
        r,
    };
    (alpha, beta)
}

// ─── Mandorla Formation ───────────────────────────────────────────────────────

/// Mandorla formation (PSE Def 7.3, OI-07 resolved)
/// `kappa(t) = exp(-lambda * delta_phi(t)) * exp(-mu_r * r(t)^2)` in `[0,1]`
pub fn mandorla(
    alpha: &TubusCoord,
    beta: &TubusCoord,
    lambda: f64,
    mu_r: f64,
) -> MandorlaState {
    let raw_diff = (alpha.phi - beta.phi).abs();
    let delta_phi = raw_diff.min(2.0 * std::f64::consts::PI - raw_diff);
    let kappa = (-lambda * delta_phi).exp() * (-mu_r * alpha.r * alpha.r).exp();
    MandorlaState {
        tau: alpha.tau,
        r: alpha.r,
        delta_phi,
        kappa,
    }
}

// ─── Carrier Migration ────────────────────────────────────────────────────────

/// Carrier migration admissibility (PSE Def 8.3)
pub fn migration_admissible(
    metrics: &MetricSet,
    target: &CarrierInstance,
    thresholds: &ThresholdConfig,
    config: &CarrierConfig,
) -> bool {
    let friction_or_shock = metrics.f_friction >= thresholds.f_friction
        || metrics.s_shock >= thresholds.s_shock;
    let readiness = config.lambda_q * target.resonance
        + config.lambda_r * 0.5 // target resonance proxy
        + config.lambda_m * target.mandorla.kappa;
    friction_or_shock && readiness >= thresholds.l_migration
}

// ─── Phase Ladder ─────────────────────────────────────────────────────────────

/// Build a phase ladder with `n` evenly spaced carrier instances
pub fn build_phase_ladder(n: usize, tau: f64, r: f64) -> PhaseLadder {
    assert!(n > 0, "phase ladder must have at least 1 carrier");
    let step = 2.0 * std::f64::consts::PI / n as f64;
    (0..n)
        .map(|i| {
            let offset = i as f64 * step;
            let phi = offset;
            let (ha, hb) = helix_pair(tau, phi, r);
            let m = mandorla(&ha, &hb, 1.0, 1.0);
            CarrierInstance {
                helix_a: ha,
                helix_b: hb,
                mandorla: m,
                resonance: 0.0,
                offset,
            }
        })
        .collect()
}

/// Advance the phase ladder by one tick (tau += delta_tau)
pub fn advance_phase_ladder(ladder: &mut PhaseLadder, delta_tau: f64) {
    for carrier in ladder.iter_mut() {
        carrier.helix_a.tau += delta_tau;
        carrier.helix_b.tau += delta_tau;
        carrier.mandorla.tau += delta_tau;
        // Inv I7: phase monotonicity enforced by only advancing forward
        assert!(delta_tau >= 0.0, "phase monotonicity violated: delta_tau < 0");
    }
}

/// Update mandorla state for a carrier instance
pub fn update_carrier_mandorla(carrier: &mut CarrierInstance, lambda: f64, mu_r: f64) {
    carrier.mandorla = mandorla(&carrier.helix_a, &carrier.helix_b, lambda, mu_r);
}

/// Restore carrier to neutral phase (reset for symmetry restoration, AT-20)
pub fn restore_neutrality(carrier: &mut CarrierInstance) {
    let tau = carrier.helix_a.tau;
    let r = carrier.helix_a.r;
    let phi = carrier.offset;
    let (ha, hb) = helix_pair(tau, phi, r);
    let m = mandorla(&ha, &hb, 1.0, 1.0);
    carrier.helix_a = ha;
    carrier.helix_b = hb;
    carrier.mandorla = m;
    carrier.resonance = 0.0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helix_pair_pi_offset() {
        let (a, b) = helix_pair(0.0, 0.0, 1.0);
        // b.phi should be pi offset from a.phi
        let diff = (b.phi - a.phi).abs();
        let diff_mod = diff.min(2.0 * std::f64::consts::PI - diff);
        assert!((diff_mod - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn helix_pair_pi_offset_nonzero_phi() {
        let (a, b) = helix_pair(0.0, std::f64::consts::PI, 1.0);
        // b.phi = (pi + pi) % 2pi = 0
        assert!((b.phi - 0.0).abs() < 1e-10, "b.phi = {}", b.phi);
        // diff = pi
        let diff = (a.phi - b.phi).abs();
        let diff_mod = diff.min(2.0 * std::f64::consts::PI - diff);
        assert!((diff_mod - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn mandorla_kappa_at_zero_phase_diff() {
        // For a helix pair with pi offset, delta_phi = pi
        let (a, b) = helix_pair(0.0, 0.0, 0.0); // r=0 so radial part = 1
        let m = mandorla(&a, &b, 1.0, 1.0);
        // kappa = exp(-1 * pi) * exp(-1 * 0) = exp(-pi)
        let expected = (-std::f64::consts::PI).exp();
        assert!((m.kappa - expected).abs() < 1e-10);
    }

    #[test]
    fn mandorla_kappa_in_unit_interval() {
        let (a, b) = helix_pair(0.0, 0.0, 0.5);
        let m = mandorla(&a, &b, 0.5, 0.5);
        assert!(m.kappa >= 0.0 && m.kappa <= 1.0);
    }

    #[test]
    fn build_phase_ladder_size() {
        let ladder = build_phase_ladder(4, 0.0, 1.0);
        assert_eq!(ladder.len(), 4);
    }

    #[test]
    fn advance_phase_ladder_monotonic() {
        let mut ladder = build_phase_ladder(4, 0.0, 1.0);
        advance_phase_ladder(&mut ladder, 0.1);
        for carrier in &ladder {
            assert!((carrier.helix_a.tau - 0.1).abs() < 1e-10);
        }
    }

    #[test]
    fn restore_neutrality_resets_resonance() {
        let mut ladder = build_phase_ladder(1, 0.0, 1.0);
        ladder[0].resonance = 0.9;
        restore_neutrality(&mut ladder[0]);
        assert_eq!(ladder[0].resonance, 0.0);
    }
}

#[cfg(test)]
mod consensus_tests {
    use super::*;

    #[test]
    fn norm_saturate_basic() {
        assert!((norm_saturate(1.0, 1.0) - 0.5).abs() < 1e-10);
        assert!((norm_saturate(0.0, 1.0) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn norm_exp_basic() {
        assert!((norm_exp(0.0, 1.0) - 1.0).abs() < 1e-10);
        assert!(norm_exp(100.0, 1.0) < 1e-10);
    }

    #[test]
    fn gate_snapshot_kairos_all_high() {
        let metrics = MetricSet {
            d_deformation: 1.0,
            q_coherence: 1.0,
            r_resonance: 1.0,
            g_readiness: 1.0,
            j_doublekick: 1.0,
            p_projection: 1.0,
            n_seam: 1.0,
            k_crystal: 1.0,
            ..Default::default()
        };
        let thresholds = ThresholdConfig::default();
        let gate = metrics.gate_snapshot(&thresholds);
        assert!(gate.kairos);
    }

    #[test]
    fn gate_snapshot_kairos_one_below() {
        let mut metrics = MetricSet {
            d_deformation: 1.0,
            q_coherence: 1.0,
            r_resonance: 1.0,
            g_readiness: 1.0,
            j_doublekick: 1.0,
            p_projection: 1.0,
            n_seam: 1.0,
            k_crystal: 1.0,
            ..Default::default()
        };
        let thresholds = ThresholdConfig::default();
        // Set d below threshold
        metrics.d_deformation = 0.0;
        let gate = metrics.gate_snapshot(&thresholds);
        assert!(!gate.kairos);
    }

    #[test]
    fn por_fsm_transitions_search_to_commit() {
        let mut fsm = PoRFsm::new();
        let config = ConsensusConfig {
            por_kappa_bar: 0.5,
            por_t_min: 2,
            por_t_stable: 1,
            por_epsilon: 0.1,
            ..Default::default()
        };

        // Search -> Lock (need por_t_min=2 steps above threshold)
        assert_eq!(fsm.step(0.8, 1.0, &config), PoRState::Search);
        assert_eq!(fsm.step(0.8, 2.0, &config), PoRState::Lock);
        // Lock -> Verify (need por_t_min + por_t_stable = 3 total steps)
        assert_eq!(fsm.step(0.8, 3.0, &config), PoRState::Verify);
        // Verify -> Commit
        assert_eq!(fsm.step(0.8, 4.0, &config), PoRState::Commit);
    }

    #[test]
    fn por_fsm_resets_on_instability() {
        let mut fsm = PoRFsm::new();
        let config = ConsensusConfig {
            por_kappa_bar: 0.5,
            por_t_min: 3,
            por_t_stable: 2,
            por_epsilon: 0.05,
            ..Default::default()
        };

        // Build up to Lock
        fsm.step(0.8, 1.0, &config);
        fsm.step(0.8, 2.0, &config);
        fsm.step(0.8, 3.0, &config);
        assert_eq!(fsm.state, PoRState::Lock);

        // Large delta should reset
        fsm.step(0.2, 4.0, &config); // big drop - but wait, 0.2 < por_kappa_bar resets in Search
        // Actually in Lock, we check delta. But 0.2 < kappa_bar doesn't matter; delta = |0.2 - 0.8| = 0.6 > epsilon
        assert_eq!(fsm.state, PoRState::Search);
    }

    #[test]
    fn dual_consensus_basic() {
        let precursor = CrystalPrecursor {
            program: Vec::new(),
            region: Vec::new(),
            seam_score: 0.8,
            metrics: MetricSet::default(),
            stability_score: 0.8,
        };
        let (dk, sw, pi, wt) = default_primal_ops();
        let (pi2, wt2, dk2, sw2) = default_dual_ops();
        let primal: Vec<&dyn CascadeOperator> = vec![&dk, &sw, &pi, &wt];
        let dual: Vec<&dyn CascadeOperator> = vec![&pi2, &wt2, &dk2, &sw2];
        let config = ConsensusConfig::default();
        let result = dual_consensus(&precursor, &primal, &dual, &config);
        assert!(result.primal_score >= 0.0);
        assert!(result.dual_score >= 0.0);
        assert!(result.mci >= 0.0 && result.mci <= 1.0);
    }
}
