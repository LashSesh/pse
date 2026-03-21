//! Multi-agent swarm coordinator for PSE.
//!
//! Spawns N autonomous agents with distinct deterministic seeds, runs them
//! through configurable rounds, and applies a consensus policy (optionally
//! backed by PMHD drills) to converge on a collective result.

// pse-swarm: Multi-Agent Swarm Coordinator
// Many voices. One resonance.
//
// The Swarm spawns N autonomous agents, assigns each a distinct
// exploration seed, and runs them through configurable rounds.  After
// every round the Swarm collects each agent's latest step result and
// applies a consensus policy to decide whether the collective goal has
// been reached.  When `SwarmPolicy.drill_config` is set, the Swarm also
// runs a PMHD DrillEngine each round to adversarially test whether
// the agents' collective hypothesis survives formal scrutiny.
//
// Design invariants
// -----------------
//   SI-1  Every agent gets a unique, deterministic seed (base_seed + agent_id).
//   SI-2  A round is complete when *all* active agents have produced a step.
//   SI-3  ConsensusVote confidence = mean score of steps produced this round,
//         clamped to [0, 1].  For DrillBacked mode: mean PMHD quality.
//   SI-4  The Swarm terminates when consensus is reached OR max_rounds is hit
//         OR all members have exhausted their plans.
//   SI-5  SwarmReport is serialisable and deterministic for the same inputs.
//   SI-6  PMHD drills are seeded by (base_seed XOR round_id * constant) so
//         each round receives a distinct, deterministic drill seed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

// ─── Re-exports ───────────────────────────────────────────────────────────────

pub use policy::{ConsensusMode, SwarmPolicy};
pub use report::{ConsensusVote, DrillSummary, SwarmReport, SwarmRound};

// ─── Modules ──────────────────────────────────────────────────────────────────

pub mod policy;
pub mod report;

// ─── Embedded Agent Types ─────────────────────────────────────────────────────
// These types mirror the agent primitives needed by the swarm coordinator.

/// Free-form goal description for an agent.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentGoal {
    /// Free-form intent description
    pub intent: String,
    /// Target domain (e.g. "rust", "typescript", "rest-api")
    pub domain: Option<String>,
    /// Hard constraints the agent must satisfy
    pub constraints: Vec<String>,
    /// Minimum resonance score considered a success (0–1)
    pub confidence_target: f64,
}

impl AgentGoal {
    pub fn new(intent: impl Into<String>) -> Self {
        Self {
            intent: intent.into(),
            domain: None,
            constraints: Vec::new(),
            confidence_target: 0.75,
        }
    }

    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    pub fn with_constraint(mut self, c: impl Into<String>) -> Self {
        self.constraints.push(c.into());
        self
    }

    pub fn with_confidence(mut self, target: f64) -> Self {
        self.confidence_target = target.clamp(0.0, 1.0);
        self
    }

    /// Stable hash of the intent string (deterministic plan seed)
    pub fn intent_hash(&self) -> u64 {
        let mut h: u64 = 14695981039346656037;
        for b in self.intent.as_bytes() {
            h = h.wrapping_mul(1099511628211) ^ (*b as u64);
        }
        h
    }
}

/// Action type enumeration for agent plans.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActionType {
    Explore,
    ForgeAtom,
    ValidateConstraint,
    Synthesize,
    Adapt,
    Complete,
}

impl ActionType {
    pub fn label(&self) -> &'static str {
        match self {
            ActionType::Explore            => "explore",
            ActionType::ForgeAtom          => "forge_atom",
            ActionType::ValidateConstraint => "validate_constraint",
            ActionType::Synthesize         => "synthesize",
            ActionType::Adapt              => "adapt",
            ActionType::Complete           => "complete",
        }
    }
}

/// A single action in an agent's plan.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AgentAction {
    pub action_type: ActionType,
    pub description: String,
    pub parameters: BTreeMap<String, String>,
}

impl AgentAction {
    pub fn new(action_type: ActionType, description: impl Into<String>) -> Self {
        Self {
            action_type,
            description: description.into(),
            parameters: BTreeMap::new(),
        }
    }

    pub fn with_param(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.parameters.insert(k.into(), v.into());
        self
    }
}

/// Result of executing one action.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentStep {
    pub step_id:   usize,
    pub action:    AgentAction,
    pub outcome:   String,
    pub score:     f64,
    pub timestamp: u64,
}

/// Ordered sequence of actions.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AgentPlan {
    pub actions:     Vec<AgentAction>,
    pub current_idx: usize,
}

impl AgentPlan {
    pub fn remaining(&self) -> usize {
        self.actions.len().saturating_sub(self.current_idx)
    }

    pub fn is_exhausted(&self) -> bool {
        self.current_idx >= self.actions.len()
    }

    pub fn current_action(&self) -> Option<&AgentAction> {
        self.actions.get(self.current_idx)
    }

    pub fn advance(&mut self) {
        if self.current_idx < self.actions.len() {
            self.current_idx += 1;
        }
    }
}

/// Agent configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentConfig {
    pub max_steps: usize,
    pub confidence_target: f64,
    pub seed: u64,
    pub dim: usize,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_steps:         100,
            confidence_target: 0.75,
            seed:              0,
            dim:               5,
        }
    }
}

/// Deterministic plan generator: same goal + same seed → same plan.
fn plan_from_goal(goal: &AgentGoal, config: &AgentConfig) -> AgentPlan {
    let seed = if config.seed == 0 { goal.intent_hash() } else { config.seed };

    let mut rng = seed;
    let next = |r: &mut u64| -> u64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *r
    };

    let domain = goal.domain.clone().unwrap_or_else(|| "general".to_string());
    let explore_steps = 10 + (next(&mut rng) % 20) as usize;

    let mut actions: Vec<AgentAction> = Vec::new();

    actions.push(
        AgentAction::new(ActionType::Explore, "Map the configuration space with spectral spiral")
            .with_param("steps", explore_steps.to_string())
            .with_param("mode", "config"),
    );

    let atom_name = format!("{}-core", domain);
    actions.push(
        AgentAction::new(ActionType::ForgeAtom, format!("Synthesise atom '{}'", atom_name))
            .with_param("atom_name", &atom_name)
            .with_param("source", &goal.intent),
    );

    for (i, constraint) in goal.constraints.iter().enumerate().take(3) {
        actions.push(
            AgentAction::new(
                ActionType::ValidateConstraint,
                format!("Validate constraint: {}", constraint),
            )
            .with_param("constraint_id", format!("C-{:02}", i + 1))
            .with_param("constraint", constraint),
        );
    }
    if goal.constraints.is_empty() {
        actions.push(
            AgentAction::new(ActionType::ValidateConstraint, "Validate core quality constraint")
                .with_param("constraint_id", "C-01")
                .with_param("constraint", "resonance >= confidence_target"),
        );
    }

    actions.push(
        AgentAction::new(ActionType::Adapt, "Adapt synthesis parameters based on resonance feedback")
            .with_param("threshold", goal.confidence_target.to_string()),
    );

    actions.push(
        AgentAction::new(ActionType::Synthesize, format!("Compose final output for '{}'", domain))
            .with_param("target", &domain)
            .with_param("atom", &atom_name),
    );

    actions.push(AgentAction::new(ActionType::Complete, "Goal complete — record crystal signature"));

    AgentPlan { actions, current_idx: 0 }
}

/// Deterministic mock resonance per action type.
fn score_for_action(action: &AgentAction, seed: u64, step_id: usize) -> f64 {
    let mut h = seed ^ (step_id as u64).wrapping_mul(2654435761);
    for b in action.description.as_bytes() {
        h = h.wrapping_mul(1099511628211) ^ (*b as u64);
    }
    0.5 + (h as f64 / u64::MAX as f64) * 0.5
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Autonomous goal-directed agent.
pub struct Agent {
    pub config: AgentConfig,
    pub goal: AgentGoal,
    plan: AgentPlan,
    history: Vec<AgentStep>,
    best_score_val: f64,
    complete: bool,
    steps_run: usize,
}

impl Agent {
    pub fn new(config: AgentConfig, goal: AgentGoal) -> Self {
        let effective_seed = if config.seed == 0 { goal.intent_hash() } else { config.seed };
        let plan = plan_from_goal(&goal, &config);
        Self {
            config: AgentConfig { seed: effective_seed, ..config },
            goal,
            plan,
            history: Vec::new(),
            best_score_val: 0.0,
            complete: false,
            steps_run: 0,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn best_score(&self) -> f64 {
        self.best_score_val
    }

    /// Execute one action from the plan and record the step.
    pub fn step(&mut self) -> Option<AgentStep> {
        if self.complete || self.plan.is_exhausted() {
            self.complete = true;
            return None;
        }

        let action = self.plan.current_action()?.clone();
        let step_id = self.steps_run;
        let score = score_for_action(&action, self.config.seed, step_id);

        let outcome = match &action.action_type {
            ActionType::Explore => {
                let steps = action.parameters.get("steps")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(10);
                format!("Explored {} points; resonance={:.4}", steps, score)
            }
            ActionType::ForgeAtom => {
                let atom = action.parameters.get("atom_name").map(|s| s.as_str()).unwrap_or("atom");
                format!("Atom '{}' synthesised; score={:.4}", atom, score)
            }
            ActionType::ValidateConstraint => {
                let cid = action.parameters.get("constraint_id").map(|s| s.as_str()).unwrap_or("?");
                let pass = score >= self.goal.confidence_target;
                format!("{} {} (score={:.4})", cid, if pass { "PASS" } else { "WARN" }, score)
            }
            ActionType::Adapt => {
                format!("Parameters adapted; new_score_est={:.4}", score)
            }
            ActionType::Synthesize => {
                let target = action.parameters.get("target").map(|s| s.as_str()).unwrap_or("output");
                format!("Synthesis complete for '{}'; quality={:.4}", target, score)
            }
            ActionType::Complete => {
                self.complete = true;
                format!("Goal complete; best_resonance={:.4}", self.best_score_val)
            }
        };

        if score > self.best_score_val {
            self.best_score_val = score;
        }

        let step = AgentStep {
            step_id,
            action: action.clone(),
            outcome,
            score,
            timestamp: unix_secs(),
        };

        self.history.push(step.clone());
        self.steps_run += 1;
        self.plan.advance();

        if self.plan.is_exhausted() {
            self.complete = true;
        }

        Some(step)
    }
}

// ─── SwarmMember ──────────────────────────────────────────────────────────────

/// One agent inside the Swarm, with its assigned id.
pub struct SwarmMember {
    pub member_id: usize,
    pub agent: Agent,
}

impl SwarmMember {
    fn new(member_id: usize, config: AgentConfig, goal: AgentGoal) -> Self {
        Self { member_id, agent: Agent::new(config, goal) }
    }

    fn step(&mut self) -> Option<AgentStep> {
        self.agent.step()
    }

    fn is_complete(&self) -> bool {
        self.agent.is_complete()
    }

    /// Current best score (resonance proxy).
    fn best_score(&self) -> f64 {
        self.agent.best_score()
    }
}

// ─── Swarm ────────────────────────────────────────────────────────────────────

/// Multi-agent coordinator with optional PMHD adversarial drilling.
///
/// Basic usage (no PMHD):
/// ```
/// use pse_swarm::{Swarm, SwarmPolicy, ConsensusMode, AgentGoal};
///
/// let goal  = AgentGoal::new("discover structural invariants");
/// let policy = SwarmPolicy {
///     size: 3,
///     base_seed: 42,
///     max_rounds: 10,
///     consensus_mode: ConsensusMode::WeightedResonance,
///     consensus_threshold: 0.5,
///     drill_config: None,
/// };
/// let mut swarm = Swarm::new(policy, goal);
/// let report = swarm.run();
/// assert!(!report.rounds.is_empty());
/// ```
pub struct Swarm {
    pub policy: SwarmPolicy,
    pub goal: AgentGoal,
    members: Vec<SwarmMember>,
    pub rounds_run: usize,
    pub complete: bool,
}

impl Swarm {
    /// Create a new Swarm.  Members are initialised but not yet stepped.
    pub fn new(policy: SwarmPolicy, goal: AgentGoal) -> Self {
        let members = (0..policy.size)
            .map(|id| {
                let cfg = AgentConfig {
                    seed: policy.base_seed.wrapping_add(id as u64),
                    ..Default::default()
                };
                SwarmMember::new(id, cfg, goal.clone())
            })
            .collect();

        Self { policy, goal, members, rounds_run: 0, complete: false }
    }

    /// Execute one round:
    ///   1. Step every active member.
    ///   2. Optionally run a PMHD drill.
    ///   3. Build a ConsensusVote.
    pub fn round(&mut self) -> SwarmRound {
        let round_id = self.rounds_run;
        self.rounds_run += 1;

        // Step all non-exhausted members
        let mut member_steps: BTreeMap<usize, Option<AgentStep>> = BTreeMap::new();
        for m in &mut self.members {
            if !m.is_complete() {
                member_steps.insert(m.member_id, m.step());
            } else {
                member_steps.insert(m.member_id, None);
            }
        }

        // Optional PMHD drill
        let drill_summary = self.policy.drill_config.as_ref().map(|cfg| {
            policy::drill_round(
                &self.goal.intent,
                self.goal.domain.as_deref(),
                &self.goal.constraints,
                self.goal.confidence_target,
                round_id,
                cfg,
            )
        });

        // Consensus evaluation
        let vote = self.policy.consensus_mode.vote(
            &self.members,
            &member_steps,
            self.policy.consensus_threshold,
            drill_summary.as_ref(),
        );

        if vote.reached {
            self.complete = true;
        }

        // Also terminate when all members have exhausted their plans
        if self.members.iter().all(|m| m.is_complete()) {
            self.complete = true;
        }

        SwarmRound { round_id, member_steps, consensus: vote, drill_summary }
    }

    /// Run until consensus is reached or `max_rounds` is exhausted.
    pub fn run(&mut self) -> SwarmReport {
        let mut rounds = Vec::new();
        while !self.complete && self.rounds_run < self.policy.max_rounds {
            rounds.push(self.round());
        }
        self.build_report(rounds)
    }

    fn build_report(&self, rounds: Vec<SwarmRound>) -> SwarmReport {
        let final_resonance = if self.members.is_empty() {
            0.0
        } else {
            let sum: f64 = self.members.iter().map(|m| m.best_score()).sum();
            (sum / self.members.len() as f64).clamp(0.0, 1.0)
        };

        let consensus_reached = rounds.iter().any(|r| r.consensus.reached);

        SwarmReport {
            goal_intent: self.goal.intent.clone(),
            swarm_size: self.members.len(),
            rounds_run: self.rounds_run,
            consensus_reached,
            final_resonance,
            rounds,
        }
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pse_pmhd::PmhdConfig;

    fn default_policy() -> SwarmPolicy {
        SwarmPolicy {
            size: 3,
            base_seed: 7,
            max_rounds: 20,
            consensus_mode: ConsensusMode::WeightedResonance,
            consensus_threshold: 0.4,
            drill_config: None,
        }
    }

    fn default_goal() -> AgentGoal {
        AgentGoal::new("test swarm coordination")
    }

    fn drill_config() -> PmhdConfig {
        PmhdConfig {
            ticks: 5,
            pool_size: 4,
            commit_budget: 3,
            seed: 99,
            ..PmhdConfig::default()
        }
    }

    // AT-SW1: Swarm creates the correct number of members
    #[test]
    fn at_sw1_member_count() {
        let swarm = Swarm::new(default_policy(), default_goal());
        assert_eq!(swarm.member_count(), 3);
    }

    // AT-SW2: Each member gets a unique member_id
    #[test]
    fn at_sw2_unique_member_ids() {
        let swarm = Swarm::new(default_policy(), default_goal());
        let ids: Vec<usize> = swarm.members.iter().map(|m| m.member_id).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    // AT-SW3: Swarm is not complete before running
    #[test]
    fn at_sw3_initial_not_complete() {
        let swarm = Swarm::new(default_policy(), default_goal());
        assert!(!swarm.is_complete());
        assert_eq!(swarm.rounds_run, 0);
    }

    // AT-SW4: A single round increments rounds_run by 1
    #[test]
    fn at_sw4_round_increments_counter() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        swarm.round();
        assert_eq!(swarm.rounds_run, 1);
    }

    // AT-SW5: Round result has the correct round_id
    #[test]
    fn at_sw5_round_id_correct() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let r0 = swarm.round();
        let r1 = swarm.round();
        assert_eq!(r0.round_id, 0);
        assert_eq!(r1.round_id, 1);
    }

    // AT-SW6: member_steps contains an entry for each member
    #[test]
    fn at_sw6_member_steps_populated() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let round = swarm.round();
        assert_eq!(round.member_steps.len(), 3);
    }

    // AT-SW7: run() respects max_rounds ceiling
    #[test]
    fn at_sw7_max_rounds_respected() {
        let policy = SwarmPolicy {
            size: 2,
            base_seed: 0,
            max_rounds: 5,
            consensus_mode: ConsensusMode::Majority,
            consensus_threshold: 0.99,
            drill_config: None,
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        assert!(report.rounds_run <= 5);
    }

    // AT-SW8: run() returns a SwarmReport with rounds count matching rounds_run
    #[test]
    fn at_sw8_report_rounds_consistent() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let report = swarm.run();
        assert_eq!(report.rounds.len(), report.rounds_run);
    }

    // AT-SW9: SwarmReport goal_intent matches the input goal
    #[test]
    fn at_sw9_report_goal_intent() {
        let goal = AgentGoal::new("invariant discovery in time-series");
        let mut swarm = Swarm::new(default_policy(), goal);
        let report = swarm.run();
        assert_eq!(report.goal_intent, "invariant discovery in time-series");
    }

    // AT-SW10: SwarmReport is serialisable and round-trips cleanly
    #[test]
    fn at_sw10_report_serialisation() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let report = swarm.run();
        let json = serde_json::to_string(&report).expect("serialise");
        let back: SwarmReport = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(back.swarm_size, report.swarm_size);
        assert_eq!(back.rounds_run, report.rounds_run);
        assert_eq!(back.goal_intent, report.goal_intent);
    }

    // AT-SW11: Determinism — identical policy + goal → identical report structure
    #[test]
    fn at_sw11_determinism() {
        let p = default_policy();
        let g = default_goal();
        let mut s1 = Swarm::new(p.clone(), g.clone());
        let mut s2 = Swarm::new(p, g);
        let r1 = s1.run();
        let r2 = s2.run();
        assert_eq!(r1.rounds_run, r2.rounds_run);
        assert_eq!(r1.consensus_reached, r2.consensus_reached);
    }

    // AT-SW12: final_resonance is in [0, 1]
    #[test]
    fn at_sw12_resonance_range() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let report = swarm.run();
        assert!(report.final_resonance >= 0.0, "resonance below 0");
        assert!(report.final_resonance <= 1.0, "resonance above 1");
    }

    // AT-SW13: Unanimous consensus requires all members to succeed
    #[test]
    fn at_sw13_unanimous_mode() {
        let policy = SwarmPolicy {
            size: 3,
            base_seed: 42,
            max_rounds: 20,
            consensus_mode: ConsensusMode::Unanimous,
            consensus_threshold: 0.01,
            drill_config: None,
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        assert!(report.rounds_run > 0);
    }

    // AT-SW14: SwarmPolicy serialisation round-trips
    #[test]
    fn at_sw14_policy_serialisation() {
        let p = default_policy();
        let json = serde_json::to_string(&p).expect("serialise policy");
        let back: SwarmPolicy = serde_json::from_str(&json).expect("deserialise policy");
        assert_eq!(back.size, p.size);
        assert_eq!(back.base_seed, p.base_seed);
        assert_eq!(back.max_rounds, p.max_rounds);
    }

    // AT-SW15: Zero-size swarm produces empty report without panic
    #[test]
    fn at_sw15_zero_size_swarm() {
        let policy = SwarmPolicy { size: 0, ..default_policy() };
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        assert_eq!(report.swarm_size, 0);
        assert_eq!(report.final_resonance, 0.0);
    }

    // AT-SW16: Single-member swarm behaves like a solo agent
    #[test]
    fn at_sw16_single_member_swarm() {
        let policy = SwarmPolicy { size: 1, ..default_policy() };
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        assert_eq!(report.swarm_size, 1);
        assert!(report.rounds_run > 0);
    }

    // AT-SW17: ConsensusVote participating_agents == swarm size
    #[test]
    fn at_sw17_participating_agents_count() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let round = swarm.round();
        assert_eq!(round.consensus.participating_agents, 3);
    }

    // AT-SW18: swarm_size in report equals policy.size
    #[test]
    fn at_sw18_report_swarm_size() {
        let policy = default_policy();
        let size = policy.size;
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        assert_eq!(report.swarm_size, size);
    }

    // ─── PMHD Integration Tests ───────────────────────────────────────────────

    // AT-SW19: Without drill_config, drill_summary is None in every round
    #[test]
    fn at_sw19_no_drill_config_no_summary() {
        let mut swarm = Swarm::new(default_policy(), default_goal());
        let round = swarm.round();
        assert!(round.drill_summary.is_none(),
            "drill_summary should be None when drill_config is not set");
    }

    // AT-SW20: With drill_config set, drill_summary is Some in every round
    #[test]
    fn at_sw20_drill_config_produces_summary() {
        let policy = SwarmPolicy {
            drill_config: Some(drill_config()),
            ..default_policy()
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let round = swarm.round();
        assert!(round.drill_summary.is_some(),
            "drill_summary should be Some when drill_config is set");
    }

    // AT-SW21: DrillSummary ticks_executed > 0
    #[test]
    fn at_sw21_drill_summary_ticks_positive() {
        let policy = SwarmPolicy {
            drill_config: Some(drill_config()),
            ..default_policy()
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let round = swarm.round();
        let ds = round.drill_summary.unwrap();
        assert!(ds.ticks_executed > 0, "drill must execute at least one tick");
    }

    // AT-SW22: DrillSummary mean_quality is in [0, 1]
    #[test]
    fn at_sw22_drill_summary_quality_range() {
        let policy = SwarmPolicy {
            drill_config: Some(drill_config()),
            ..default_policy()
        };
        let mut swarm = Swarm::new(policy, default_goal());
        swarm.run(); // run all rounds
        // Check every round's drill summary
        // (re-run for inspection since run() consumes rounds internally)
        let mut swarm2 = Swarm::new(
            SwarmPolicy { drill_config: Some(drill_config()), ..default_policy() },
            default_goal(),
        );
        let report = swarm2.run();
        for r in &report.rounds {
            if let Some(ds) = &r.drill_summary {
                assert!(ds.mean_quality >= 0.0 && ds.mean_quality <= 1.0,
                    "round {}: mean_quality {} out of [0,1]", r.round_id, ds.mean_quality);
            }
        }
    }

    // AT-SW23: DrillBacked mode — consensus based on PMHD, not agent scores
    #[test]
    fn at_sw23_drill_backed_mode() {
        let policy = SwarmPolicy {
            size: 3,
            base_seed: 13,
            max_rounds: 15,
            consensus_mode: ConsensusMode::DrillBacked,
            consensus_threshold: 0.0, // any monolith committed → consensus
            drill_config: Some(PmhdConfig {
                ticks: 10,
                pool_size: 5,
                commit_budget: 5,
                seed: 77,
                ..PmhdConfig::default()
            }),
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let report = swarm.run();
        // With threshold=0.0 and non-trivial drill, consensus should be reached
        assert!(report.consensus_reached,
            "DrillBacked with threshold=0.0 should reach consensus");
    }

    // AT-SW24: Drill determinism — same policy + goal → same DrillSummary each round
    #[test]
    fn at_sw24_drill_determinism() {
        let policy = SwarmPolicy {
            drill_config: Some(drill_config()),
            ..default_policy()
        };
        let g = default_goal();
        let mut s1 = Swarm::new(policy.clone(), g.clone());
        let mut s2 = Swarm::new(policy, g);
        let r1 = s1.round();
        let r2 = s2.round();
        let ds1 = r1.drill_summary.unwrap();
        let ds2 = r2.drill_summary.unwrap();
        assert_eq!(ds1, ds2, "drill summary must be deterministic for same inputs");
    }

    // AT-SW25: SwarmRound with drill serialises cleanly
    #[test]
    fn at_sw25_drill_round_serialisation() {
        let policy = SwarmPolicy {
            drill_config: Some(drill_config()),
            ..default_policy()
        };
        let mut swarm = Swarm::new(policy, default_goal());
        let round = swarm.round();
        let json = serde_json::to_string(&round).expect("serialise round");
        let back: SwarmRound = serde_json::from_str(&json).expect("deserialise round");
        assert_eq!(back.round_id, round.round_id);
        assert!(back.drill_summary.is_some());
    }
}
