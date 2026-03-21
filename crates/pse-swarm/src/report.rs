// pse-swarm: report — SwarmRound, ConsensusVote, SwarmReport, DrillSummary

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::AgentStep;

// ─── DrillSummary ─────────────────────────────────────────────────────────────

/// Lightweight summary of a PMHD drill run performed during a Swarm round.
///
/// When `SwarmPolicy.drill_config` is set, the Swarm runs one PMHD drill per
/// round to adversarially probe whether the agents' collective hypothesis
/// survives formal scrutiny.  This summary records the outcome.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DrillSummary {
    /// Ticks the drill engine actually executed.
    pub ticks_executed: u64,
    /// Number of hypotheses in the pool at drill end.
    pub pool_final_size: usize,
    /// Number of monoliths committed (hypotheses that passed the PoR gate).
    pub monolith_count: usize,
    /// Mean quality score across all committed monoliths (0–1).
    /// 0.0 when no monoliths were committed.
    pub mean_quality: f64,
}

// ─── ConsensusVote ────────────────────────────────────────────────────────────

/// The outcome of one round's consensus evaluation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ConsensusVote {
    /// Whether consensus was reached this round.
    pub reached: bool,
    /// Mean score of successful steps, clamped to [0, 1].
    /// For DrillBacked mode this is the PMHD mean quality instead.
    pub confidence: f64,
    /// Number of members that participated (had a pending step).
    pub participating_agents: usize,
    /// Number of members whose step had score > 0.
    pub successful_agents: usize,
}

// ─── SwarmRound ───────────────────────────────────────────────────────────────

/// Record of a single Swarm round.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SwarmRound {
    pub round_id: usize,
    /// Map from member_id → step result (None if member was already complete).
    pub member_steps: BTreeMap<usize, Option<AgentStep>>,
    pub consensus: ConsensusVote,
    /// PMHD drill summary for this round, present when `drill_config` is set.
    pub drill_summary: Option<DrillSummary>,
}

// ─── SwarmReport ──────────────────────────────────────────────────────────────

/// Aggregated report emitted after a full Swarm run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SwarmReport {
    pub goal_intent: String,
    pub swarm_size: usize,
    pub rounds_run: usize,
    pub consensus_reached: bool,
    /// Mean best_score across all members at run end, clamped to [0, 1].
    pub final_resonance: f64,
    pub rounds: Vec<SwarmRound>,
}
