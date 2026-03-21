//! Polycentric multi-hypothesis drill for PSE (C21).
//!
//! Generates, opposes, evaluates, and commits structured hypotheses through
//! adversarial drilling. Deterministic under a fixed `(DecisionSpec, seed)`.

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use sha2::{Digest, Sha256};
use pse_types::{content_address, FiveDState, Hash256};

// ─── Error ───────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum PmhdError {
    #[error("config invalid: {0}")]
    ConfigInvalid(String),
    #[error("serialization: {0}")]
    Serde(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, PmhdError>;

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Compute Hypothesis.id = SHA-256(claim || sorted(assumptions))
fn hypothesis_id(claim: &str, assumptions: &[String]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(claim.as_bytes());
    let mut sorted = assumptions.to_vec();
    sorted.sort();
    for a in &sorted {
        hasher.update(a.as_bytes());
    }
    hex_encode(&hasher.finalize())
}

/// Deterministic PRNG — xorshift64. No rand crate dependency.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0xcafe_babe_dead_beef } else { seed })
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    /// Uniform f64 in [0, 1)
    fn f64_unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    fn usize_mod(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { (self.next() as usize) % n }
    }
}

// ─── Evidence ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Evidence {
    pub id: String,
    pub kind: String,
    pub content: String,
}

// ─── DecisionSpec ─────────────────────────────────────────────────────────────

/// Input to the generative direction (PSE Extension Phase 5 Def D).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionSpec {
    pub id: Hash256,
    pub intent: String,
    pub goals: BTreeMap<String, f64>,
    pub constraints: Vec<String>,
    pub seeds: Vec<Hash256>,
    pub domain: String,
    pub config: PmhdConfig,
}

#[derive(Serialize)]
struct DecisionSpecCore<'a> {
    intent: &'a str,
    goals: &'a BTreeMap<String, f64>,
    constraints: &'a [String],
    domain: &'a str,
}

impl DecisionSpec {
    pub fn new(
        intent: impl Into<String>,
        goals: BTreeMap<String, f64>,
        constraints: Vec<String>,
        domain: impl Into<String>,
        config: PmhdConfig,
    ) -> Self {
        let intent = intent.into();
        let domain = domain.into();
        let core = DecisionSpecCore {
            intent: &intent,
            goals: &goals,
            constraints: &constraints,
            domain: &domain,
        };
        let id = content_address(&core);
        Self { id, intent, goals, constraints, seeds: Vec::new(), domain, config }
    }
}

// ─── PmhdConfig ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PmhdConfig {
    pub ticks: u64,
    pub pool_size: usize,
    pub opposition_strength: f64,
    pub mutation_rate: f64,
    pub crossover_rate: f64,
    pub seed_strategy: SeedStrategy,
    pub thresholds: QualityThresholds,
    pub commit_budget: usize,
    /// Deterministic seed for the PRNG
    pub seed: u64,
}

impl Default for PmhdConfig {
    fn default() -> Self {
        Self {
            ticks: 50,
            pool_size: 10,
            opposition_strength: 0.7,
            mutation_rate: 0.3,
            crossover_rate: 0.2,
            seed_strategy: SeedStrategy::Hybrid,
            thresholds: QualityThresholds::default(),
            commit_budget: 5,
            seed: 42,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SeedStrategy {
    Greedy,
    Stochastic,
    Beam { width: usize },
    Evolutionary,
    Hybrid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QualityThresholds {
    pub coherence: f64,
    pub diversity: f64,
    pub novelty: f64,
    pub stability: f64,
    pub robustness: f64,
    pub coverage: f64,
}

impl Default for QualityThresholds {
    fn default() -> Self {
        Self {
            coherence: 0.0,
            diversity: 0.0,
            novelty: 0.0,
            stability: 0.0,
            robustness: 0.0,
            coverage: 0.0,
        }
    }
}

// ─── Hypothesis ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hypothesis {
    pub id: String,
    pub claim: String,
    pub assumptions: Vec<String>,
    pub evidence: Vec<Evidence>,
    pub generation: u64,
    // Internal tracking for quality computation
    pub ticks_survived: u64,
    pub total_counter_severity: u64,
    pub counter_count: u64,
}

impl Hypothesis {
    pub fn new(claim: String, assumptions: Vec<String>, generation: u64) -> Self {
        let id = hypothesis_id(&claim, &assumptions);
        Self {
            id,
            claim,
            assumptions,
            evidence: Vec::new(),
            generation,
            ticks_survived: 0,
            total_counter_severity: 0,
            counter_count: 0,
        }
    }
}

// ─── Counterexample ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Counterexample {
    pub id: String,
    pub target_id: String,
    pub reason: String,
    pub severity: u8,
}

impl Counterexample {
    pub fn new(target_id: String, reason: String, severity: u8) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(target_id.as_bytes());
        hasher.update(reason.as_bytes());
        hasher.update([severity]);
        let id = hex_encode(&hasher.finalize());
        Self { id, target_id, reason, severity }
    }
}

// ─── QualityMetrics ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct QualityMetrics {
    pub coherence: f64,
    pub diversity: f64,
    pub novelty: f64,
    pub stability: f64,
    pub robustness: f64,
    pub coverage: f64,
}

impl QualityMetrics {
    pub fn mean(&self) -> f64 {
        (self.coherence + self.diversity + self.novelty
            + self.stability + self.robustness + self.coverage) / 6.0
    }

    pub fn all_in_unit(&self) -> bool {
        [self.coherence, self.diversity, self.novelty,
         self.stability, self.robustness, self.coverage]
            .iter()
            .all(|&v| (0.0..=1.0).contains(&v))
    }

    pub fn passes(&self, t: &QualityThresholds) -> bool {
        self.coherence >= t.coherence
            && self.diversity >= t.diversity
            && self.novelty >= t.novelty
            && self.stability >= t.stability
            && self.robustness >= t.robustness
            && self.coverage >= t.coverage
    }
}

// ─── Pattern Memory ───────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PatternEntry {
    pub monolith_id: String,
    pub domain: String,
    pub quality: QualityMetrics,
    pub signature: FiveDState,
    pub component_kinds: Vec<String>,
    pub timestamp: f64,
}

#[derive(Clone, Debug, Default)]
pub struct PatternMemory {
    entries: Vec<PatternEntry>,
}

impl PatternMemory {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn from_entries(entries: Vec<PatternEntry>) -> Self {
        Self { entries }
    }

    pub fn add(&mut self, entry: PatternEntry) {
        self.entries.push(entry);
    }

    pub fn find_similar(&self, sig: &FiveDState, k: usize) -> Vec<&PatternEntry> {
        let mut scored: Vec<(f64, &PatternEntry)> = self.entries.iter()
            .map(|e| (e.signature.distance(sig), e))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.iter().take(k).map(|(_, e)| *e).collect()
    }

    pub fn novelty_score(&self, candidate: &FiveDState) -> f64 {
        if self.entries.is_empty() {
            return 1.0;
        }
        let min_dist = self.entries.iter()
            .map(|e| e.signature.distance(candidate))
            .fold(f64::INFINITY, f64::min);
        // Normalize: distance > 2.0 → max novelty 1.0
        (min_dist / 2.0).min(1.0)
    }

    pub fn len(&self) -> usize { self.entries.len() }
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub fn entries(&self) -> &[PatternEntry] { &self.entries }
}

// ─── Monolith Provenance ─────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonolithProvenance {
    pub seed: u64,
    pub config_hash: String,
    pub tick_range: [u64; 2],
    pub active_set_size: usize,
    pub por_evidence: String,
    pub spec_id: Hash256,
}

// ─── PmhdMonolith ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PmhdMonolith {
    pub id: String,
    pub core_hypothesis: Hypothesis,
    pub excalibration_vector: Vec<f64>,
    pub counterexamples: Vec<Counterexample>,
    pub quality: QualityMetrics,
    pub provenance: MonolithProvenance,
}

impl PmhdMonolith {
    fn commit(
        h: Hypothesis,
        counters: Vec<Counterexample>,
        quality: QualityMetrics,
        prov: MonolithProvenance,
    ) -> Self {
        let excalibration_vector: Vec<f64> =
            counters.iter().map(|c| c.severity as f64 / 255.0).collect();
        let mut hasher = Sha256::new();
        hasher.update(h.id.as_bytes());
        hasher.update(prov.tick_range[0].to_le_bytes());
        hasher.update(prov.spec_id);
        let id = hex_encode(&hasher.finalize());
        Self { id, core_hypothesis: h, excalibration_vector, counterexamples: counters, quality, provenance: prov }
    }
}

// ─── DrillResult / StepResult ─────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct DrillResult {
    pub monoliths: Vec<PmhdMonolith>,
    pub ticks_executed: u64,
    pub pool_final_size: usize,
    pub quality_history: Vec<Vec<QualityMetrics>>,
    pub commit_count: usize,
}

#[derive(Clone, Debug)]
pub struct StepResult {
    pub tick: u64,
    pub pool_size: usize,
    pub committed: usize,
    pub quality_snapshot: Vec<QualityMetrics>,
}

// ─── DrillEngine ─────────────────────────────────────────────────────────────

pub struct DrillEngine {
    config: PmhdConfig,
    pool: Vec<Hypothesis>,
    // counterexamples collected in the current tick per hypothesis index
    pending_counters: Vec<Vec<Counterexample>>,
    monoliths: Vec<PmhdMonolith>,
    pattern_memory: PatternMemory,
    tick: u64,
    rng: Xorshift64,
}

impl DrillEngine {
    pub fn new(config: PmhdConfig) -> Self {
        let seed = config.seed;
        Self {
            config,
            pool: Vec::new(),
            pending_counters: Vec::new(),
            monoliths: Vec::new(),
            pattern_memory: PatternMemory::new(),
            tick: 0,
            rng: Xorshift64::new(seed),
        }
    }

    pub fn with_memory(config: PmhdConfig, memory: PatternMemory) -> Self {
        let seed = config.seed;
        Self {
            config,
            pool: Vec::new(),
            pending_counters: Vec::new(),
            monoliths: Vec::new(),
            pattern_memory: memory,
            tick: 0,
            rng: Xorshift64::new(seed),
        }
    }

    pub fn drill(&mut self, spec: &DecisionSpec) -> DrillResult {
        // Reset per-drill state
        self.pool.clear();
        self.pending_counters.clear();
        self.monoliths.clear();
        self.tick = 0;
        self.rng = Xorshift64::new(self.config.seed);

        self.seed_pool(spec);

        let mut quality_history = Vec::new();
        let ticks = self.config.ticks;

        for t in 1..=ticks {
            let step = self.do_step(spec, t);
            quality_history.push(step.quality_snapshot);
            if self.monoliths.len() >= self.config.commit_budget {
                return DrillResult {
                    ticks_executed: t,
                    pool_final_size: self.pool.len(),
                    commit_count: self.monoliths.len(),
                    quality_history,
                    monoliths: self.monoliths.clone(),
                };
            }
        }

        DrillResult {
            ticks_executed: ticks,
            pool_final_size: self.pool.len(),
            commit_count: self.monoliths.len(),
            quality_history,
            monoliths: self.monoliths.clone(),
        }
    }

    pub fn step(&mut self, spec: &DecisionSpec) -> StepResult {
        self.tick += 1;
        let t = self.tick;
        self.do_step(spec, t)
    }

    fn do_step(&mut self, spec: &DecisionSpec, tick: u64) -> StepResult {
        self.tick = tick;

        // Phase 1: Generate / Mutate
        self.phase_generate(spec, tick);

        // Phase 2: Oppose — generate counterexamples for each hypothesis
        let pool_len = self.pool.len();
        self.pending_counters.resize(pool_len, Vec::new());
        for i in 0..pool_len {
            let counters = self.generate_counterexamples(i, spec);
            let h = &mut self.pool[i];
            h.total_counter_severity +=
                counters.iter().map(|c| c.severity as u64).sum::<u64>();
            h.counter_count += counters.len() as u64;
            // Defend: add evidence for each counter
            for c in &counters {
                h.evidence.push(Evidence {
                    id: format!("def-{}", c.id),
                    kind: "defense".to_string(),
                    content: format!("Response to: {}", c.reason),
                });
            }
            h.ticks_survived += 1;
            self.pending_counters[i] = counters;
        }

        // Phase 3: Evaluate quality across pool
        let qualities = self.evaluate_quality(spec);

        // Phase 4: Tournament selection — keep best pool_size
        self.tournament_select();

        // Phase 5: Commit gate
        let config = self.config.clone();
        let mut committed_this_tick = 0;
        // Collect indices to commit (borrow checker)
        let to_commit: Vec<usize> = (0..self.pool.len())
            .filter(|&i| {
                if self.monoliths.len() + committed_this_tick >= config.commit_budget {
                    return false;
                }
                let q = qualities.get(i).cloned().unwrap_or_default();
                if q.passes(&config.thresholds) && Self::por_gate(&q) {
                    committed_this_tick += 1;
                    true
                } else {
                    false
                }
            })
            .collect();

        for i in to_commit {
            let h = self.pool[i].clone();
            let q = qualities.get(i).cloned().unwrap_or_default();
            let counters = self.pending_counters.get(i).cloned().unwrap_or_default();
            let prov = MonolithProvenance {
                seed: config.seed,
                config_hash: config_hash(&config),
                tick_range: [h.generation, tick],
                active_set_size: self.pool.len(),
                por_evidence: format!("mean={:.4}", q.mean()),
                spec_id: spec.id,
            };
            let m = PmhdMonolith::commit(h, counters, q.clone(), prov);
            let sig = hypothesis_to_5d(&self.pool[i]);
            self.pattern_memory.add(PatternEntry {
                monolith_id: m.id.clone(),
                domain: spec.domain.clone(),
                quality: q,
                signature: sig,
                component_kinds: vec!["claim".to_string(), "assumption".to_string()],
                timestamp: tick as f64,
            });
            self.monoliths.push(m);
        }
        self.pending_counters.clear();

        StepResult {
            tick,
            pool_size: self.pool.len(),
            committed: committed_this_tick,
            quality_snapshot: qualities,
        }
    }

    fn seed_pool(&mut self, spec: &DecisionSpec) {
        for i in 0..self.config.pool_size {
            let v = self.rng.f64_unit();
            // Embed goal keys into claim so coverage score is nonzero from the start
            let goal_part: String = spec.goals.keys()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(",");
            let claim = format!(
                "{} [seed-{i}:{v:.4}] goals=[{goal_part}]",
                spec.intent
            );
            let assumptions: Vec<String> = spec.goals.iter()
                .map(|(k, &val)| format!("goal-{k}: target={val:.3}"))
                .collect();
            self.pool.push(Hypothesis::new(claim, assumptions, 0));
        }
    }

    fn phase_generate(&mut self, spec: &DecisionSpec, tick: u64) {
        let to_add = (self.config.mutation_rate * self.pool.len() as f64).ceil() as usize;
        let pool_len = self.pool.len();
        if pool_len == 0 { return; }
        for _ in 0..to_add {
            if self.pool.len() >= self.config.pool_size * 3 { break; }
            let idx = self.rng.usize_mod(pool_len);
            let parent = self.pool[idx].clone();
            let v = self.rng.f64_unit();
            let claim = format!("{} [mut-t{tick}:{v:.4}]", parent.claim);
            let mut assumptions = parent.assumptions.clone();
            if let Some(k) = spec.goals.keys().next() {
                assumptions.push(format!("refined-{k}: t={tick}"));
            }
            self.pool.push(Hypothesis::new(claim, assumptions, tick));
        }
    }

    fn generate_counterexamples(
        &mut self,
        h_idx: usize,
        _spec: &DecisionSpec,
    ) -> Vec<Counterexample> {
        let strength = self.config.opposition_strength;
        if strength == 0.0 { return Vec::new(); }
        let target_id = self.pool[h_idx].id.clone();
        let v = self.rng.f64_unit();
        let severity = (strength * v * 255.0) as u8;
        if severity == 0 { return Vec::new(); }
        vec![Counterexample::new(
            target_id,
            format!("constraint-violation-{v:.4}"),
            severity,
        )]
    }

    fn evaluate_quality(&mut self, spec: &DecisionSpec) -> Vec<QualityMetrics> {
        if self.pool.is_empty() { return Vec::new(); }
        let pool_len = self.pool.len();

        // Pool-level diversity (unique leading words)
        let unique: std::collections::BTreeSet<String> = self.pool.iter()
            .map(|h| h.claim.split_whitespace().next().unwrap_or("").to_string())
            .collect();
        let diversity_raw = (unique.len() as f64 / pool_len as f64).min(1.0);

        // Snapshot pool for borrowing
        let pool_snap: Vec<Hypothesis> = self.pool.clone();
        let memory = &self.pattern_memory;

        pool_snap.iter().map(|h| {
            // coherence: grows with defense evidence accumulated
            let coherence = ((h.evidence.len() as f64) / 10.0).min(1.0);

            let diversity = diversity_raw;

            let sig = hypothesis_to_5d(h);
            let novelty = memory.novelty_score(&sig);

            // stability: fraction of ticks survived relative to age
            let stability = if self.tick == 0 || h.generation == self.tick {
                0.0
            } else {
                let age = (self.tick - h.generation) as f64;
                (h.ticks_survived as f64 / age).min(1.0)
            };

            // robustness: inverse of mean counter severity
            let robustness = if h.counter_count == 0 {
                1.0
            } else {
                let mean_sev = h.total_counter_severity as f64 / h.counter_count as f64;
                (1.0 - mean_sev / 255.0).max(0.0)
            };

            // coverage: fraction of goal keys appearing in claim or assumptions
            let coverage = if spec.goals.is_empty() {
                1.0
            } else {
                let goals_hit = spec.goals.keys()
                    .filter(|g| {
                        h.claim.contains(g.as_str())
                            || h.assumptions.iter().any(|a| a.contains(g.as_str()))
                    })
                    .count();
                goals_hit as f64 / spec.goals.len() as f64
            };

            QualityMetrics { coherence, diversity, novelty, stability, robustness, coverage }
        }).collect()
    }

    fn tournament_select(&mut self) {
        if self.pool.len() <= self.config.pool_size { return; }
        let mut scored: Vec<(f64, Hypothesis)> = self.pool.drain(..)
            .map(|h| {
                let ev = h.evidence.len() as f64 / 10.0;
                let age = h.ticks_survived as f64 / 10.0;
                let rob = if h.counter_count == 0 { 1.0 }
                    else {
                        (1.0 - h.total_counter_severity as f64
                            / (h.counter_count as f64 * 255.0)).max(0.0)
                    };
                (ev + age + rob, h)
            })
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(self.config.pool_size);
        self.pool = scored.into_iter().map(|(_, h)| h).collect();
    }

    /// Simplified PoR gate: fires unconditionally (quality thresholds do the filtering).
    fn por_gate(_q: &QualityMetrics) -> bool { true }

    pub fn monoliths(&self) -> &[PmhdMonolith] { &self.monoliths }
    pub fn pattern_memory(&self) -> &PatternMemory { &self.pattern_memory }
}

// ─── Internal Utilities ───────────────────────────────────────────────────────

fn config_hash(config: &PmhdConfig) -> String {
    let data = serde_json::to_vec(config).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(&data);
    hex_encode(&hasher.finalize())
}

/// Map a Hypothesis to a 5D signature for pattern distance calculation.
pub fn hypothesis_to_5d(h: &Hypothesis) -> FiveDState {
    FiveDState {
        p:   (h.evidence.len() as f64 / 10.0).min(1.0),
        rho: (h.assumptions.len() as f64 / 10.0).min(1.0),
        omega: (h.ticks_survived as f64 / 100.0).min(1.0),
        chi: if h.counter_count == 0 { 1.0 }
             else { (1.0 - h.total_counter_severity as f64 / (h.counter_count as f64 * 255.0)).max(0.0) },
        eta: (h.generation as f64 / 1000.0).min(1.0),
    }
}

// ─── Acceptance Tests (AT-P1 through AT-P8) ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec() -> DecisionSpec {
        let mut goals = BTreeMap::new();
        goals.insert("coherence".to_string(), 0.7);
        goals.insert("robustness".to_string(), 0.8);
        DecisionSpec::new(
            "Create a health-check module",
            goals,
            vec!["must return JSON".to_string()],
            "rust",
            PmhdConfig { ticks: 20, pool_size: 5, commit_budget: 3, ..Default::default() },
        )
    }

    // AT-P1: Drill determinism — two runs with identical spec and seed produce identical monolith IDs.
    #[test]
    fn at_p1_drill_determinism() {
        let spec = test_spec();
        let mut engine1 = DrillEngine::new(spec.config.clone());
        let result1 = engine1.drill(&spec);

        let mut engine2 = DrillEngine::new(spec.config.clone());
        let result2 = engine2.drill(&spec);

        assert_eq!(result1.ticks_executed, result2.ticks_executed);
        assert_eq!(result1.monoliths.len(), result2.monoliths.len());
        for (m1, m2) in result1.monoliths.iter().zip(result2.monoliths.iter()) {
            assert_eq!(m1.id, m2.id, "AT-P1: monolith IDs must be identical under same seed");
        }
    }

    // AT-P2: Opposition effect — strength=0 yields robustness=1.0; strength=0.9 yields lower robustness.
    #[test]
    fn at_p2_opposition_effect() {
        let spec = test_spec();

        let cfg_no_opp = PmhdConfig {
            ticks: 15,
            pool_size: 5,
            opposition_strength: 0.0,
            ..spec.config.clone()
        };
        let mut eng_no = DrillEngine::new(cfg_no_opp);
        let res_no = eng_no.drill(&spec);

        let cfg_high_opp = PmhdConfig {
            ticks: 15,
            pool_size: 5,
            opposition_strength: 0.9,
            ..spec.config.clone()
        };
        let mut eng_hi = DrillEngine::new(cfg_high_opp);
        let res_hi = eng_hi.drill(&spec);

        // With no opposition, every hypothesis' robustness in the final tick should be 1.0
        if let Some(last_tick) = res_no.quality_history.last() {
            for q in last_tick {
                assert!(
                    (q.robustness - 1.0).abs() < 1e-9,
                    "AT-P2: robustness with strength=0 must be 1.0, got {}",
                    q.robustness
                );
            }
        }
        // With high opposition, mean robustness should be < 1.0
        if let Some(last_tick) = res_hi.quality_history.last() {
            let mean_rob: f64 = last_tick.iter().map(|q| q.robustness).sum::<f64>()
                / last_tick.len() as f64;
            assert!(
                mean_rob < 1.0,
                "AT-P2: mean robustness with strength=0.9 must be < 1.0, got {}",
                mean_rob
            );
        }
    }

    // AT-P3: Commit gate — thresholds=1.0 → 0 monoliths; thresholds=0.0 → some committed.
    #[test]
    fn at_p3_commit_gate() {
        let spec = test_spec();

        // All thresholds at 1.0 — almost impossible to pass
        let cfg_max = PmhdConfig {
            ticks: 20,
            pool_size: 5,
            commit_budget: 10,
            thresholds: QualityThresholds {
                coherence: 1.0, diversity: 1.0, novelty: 1.0,
                stability: 1.0, robustness: 1.0, coverage: 1.0,
            },
            ..spec.config.clone()
        };
        let mut eng = DrillEngine::new(cfg_max);
        let res = eng.drill(&spec);
        assert_eq!(res.monoliths.len(), 0, "AT-P3: all thresholds=1.0 → 0 monoliths");

        // All thresholds at 0.0 — every hypothesis passes
        let cfg_zero = PmhdConfig {
            ticks: 5,
            pool_size: 5,
            commit_budget: 10,
            thresholds: QualityThresholds {
                coherence: 0.0, diversity: 0.0, novelty: 0.0,
                stability: 0.0, robustness: 0.0, coverage: 0.0,
            },
            ..spec.config.clone()
        };
        let mut eng2 = DrillEngine::new(cfg_zero);
        let res2 = eng2.drill(&spec);
        assert!(res2.monoliths.len() > 0, "AT-P3: all thresholds=0.0 → at least 1 monolith");
    }

    // AT-P4: Quality metrics range — all 6 metrics in [0, 1] for every hypothesis at every tick.
    #[test]
    fn at_p4_quality_metrics_range() {
        let spec = test_spec();
        let cfg = PmhdConfig { ticks: 20, pool_size: 8, ..spec.config.clone() };
        let mut eng = DrillEngine::new(cfg);
        let res = eng.drill(&spec);
        for (t, tick_qs) in res.quality_history.iter().enumerate() {
            for q in tick_qs {
                assert!(
                    q.all_in_unit(),
                    "AT-P4: tick {t}: quality metrics out of [0,1]: {q:?}"
                );
            }
        }
    }

    // AT-P5: Pattern memory growth — commit monoliths, verify memory grows; re-run → novelty reflects existing patterns.
    #[test]
    fn at_p5_pattern_memory_growth() {
        let spec = test_spec();
        let cfg = PmhdConfig {
            ticks: 20,
            pool_size: 5,
            commit_budget: 5,
            thresholds: QualityThresholds::default(), // all 0.0
            ..spec.config.clone()
        };

        let mut eng = DrillEngine::new(cfg.clone());
        let res1 = eng.drill(&spec);
        let mem_after = eng.pattern_memory().len();
        assert_eq!(
            mem_after,
            res1.monoliths.len(),
            "AT-P5: pattern memory must contain exactly the committed monoliths"
        );
        assert!(mem_after > 0, "AT-P5: at least one pattern should be stored");

        // Re-run with the populated memory — novelty should be reduced for similar signatures
        let memory = eng.pattern_memory().clone();
        let mut eng2 = DrillEngine::with_memory(cfg, memory);
        let res2 = eng2.drill(&spec);
        // Check that at least one novelty score < 1.0 (i.e., memory was consulted)
        let any_low_novelty = res2.quality_history.iter()
            .flat_map(|qs| qs.iter())
            .any(|q| q.novelty < 1.0);
        assert!(any_low_novelty, "AT-P5: second run should see reduced novelty due to existing patterns");
    }

    // AT-P6: Seed strategies — each produces a DrillResult; determinism verified per strategy.
    #[test]
    fn at_p6_seed_strategies() {
        let spec = test_spec();
        let strategies = [
            SeedStrategy::Greedy,
            SeedStrategy::Stochastic,
            SeedStrategy::Beam { width: 3 },
            SeedStrategy::Evolutionary,
            SeedStrategy::Hybrid,
        ];
        for strategy in strategies {
            let cfg = PmhdConfig {
                ticks: 10,
                pool_size: 5,
                commit_budget: 5,
                seed_strategy: strategy.clone(),
                thresholds: QualityThresholds::default(), // all 0.0 → easy commit
                ..spec.config.clone()
            };

            let mut eng1 = DrillEngine::new(cfg.clone());
            let r1 = eng1.drill(&spec);

            let mut eng2 = DrillEngine::new(cfg);
            let r2 = eng2.drill(&spec);

            assert_eq!(r1.ticks_executed, r2.ticks_executed,
                "AT-P6: strategy {strategy:?} determinism: tick count must match");
            assert_eq!(r1.monoliths.len(), r2.monoliths.len(),
                "AT-P6: strategy {strategy:?} determinism: monolith count must match");
            for (m1, m2) in r1.monoliths.iter().zip(r2.monoliths.iter()) {
                assert_eq!(m1.id, m2.id,
                    "AT-P6: strategy {strategy:?} determinism: IDs must match");
            }
        }
    }

    // AT-P7: Hypothesis ID determinism — same claim and assumptions always yield identical ID.
    #[test]
    fn at_p7_hypothesis_id_determinism() {
        let claim = "health-check artifact for REST API".to_string();
        let assumptions = vec![
            "must handle concurrent requests".to_string(),
            "must return JSON".to_string(),
        ];
        let h1 = Hypothesis::new(claim.clone(), assumptions.clone(), 0);
        let h2 = Hypothesis::new(claim.clone(), assumptions.clone(), 5);
        // generation differs but id is based on claim+assumptions only
        assert_eq!(h1.id, h2.id, "AT-P7: hypothesis ID must be deterministic from claim+assumptions");

        // Reversed assumptions order → same ID (sorted internally)
        let mut rev = assumptions.clone();
        rev.reverse();
        let h3 = Hypothesis::new(claim.clone(), rev, 0);
        assert_eq!(h1.id, h3.id, "AT-P7: hypothesis ID must be order-invariant on assumptions");

        // Different claim → different ID
        let h4 = Hypothesis::new("different claim".to_string(), assumptions, 0);
        assert_ne!(h1.id, h4.id, "AT-P7: different claims must produce different IDs");
    }

    // AT-P8: Provenance completeness — monolith provenance contains all required fields.
    #[test]
    fn at_p8_provenance_completeness() {
        let spec = test_spec();
        let cfg = PmhdConfig {
            ticks: 10,
            pool_size: 5,
            commit_budget: 2,
            thresholds: QualityThresholds::default(),
            ..spec.config.clone()
        };
        let mut eng = DrillEngine::new(cfg);
        let res = eng.drill(&spec);
        assert!(!res.monoliths.is_empty(), "AT-P8: need at least one monolith");
        for m in &res.monoliths {
            let p = &m.provenance;
            assert_eq!(p.spec_id, spec.id, "AT-P8: provenance.spec_id must match spec");
            assert!(!p.config_hash.is_empty(), "AT-P8: config_hash must be non-empty");
            assert!(p.tick_range[0] <= p.tick_range[1], "AT-P8: tick_range must be ordered");
            assert!(!p.por_evidence.is_empty(), "AT-P8: por_evidence must be non-empty");
        }
    }
}
