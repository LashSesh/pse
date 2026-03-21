// pse-swarm: policy — SwarmPolicy and ConsensusMode

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use pse_pmhd::{DecisionSpec, DrillEngine, PmhdConfig};

use crate::{
    report::{ConsensusVote, DrillSummary},
    AgentStep,
    SwarmMember,
};

// ─── ConsensusMode ────────────────────────────────────────────────────────────

/// Strategy used to evaluate whether a round's results constitute consensus.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusMode {
    /// Consensus when >50 % of active members produced a step (any score > 0).
    Majority,
    /// Consensus when the mean score of steps produced this round ≥ threshold.
    WeightedResonance,
    /// Consensus only when *every* active member produced a step with score > 0.
    Unanimous,
    /// Consensus backed by the PMHD Drill Engine.
    ///
    /// After each round the Swarm runs a `DrillEngine` whose `DecisionSpec` is
    /// derived from the goal and the round's step results.  Consensus is
    /// reached when the drill commits ≥ 1 monolith AND the mean monolith
    /// quality is ≥ `consensus_threshold`.
    DrillBacked,
}

impl ConsensusMode {
    /// Evaluate the round results and return a ConsensusVote.
    ///
    /// A step is "successful" when `score > 0.0`.
    /// For `DrillBacked` mode the drill result is used instead; `drill_summary`
    /// must already be populated in that case.
    pub fn vote(
        &self,
        _members: &[SwarmMember],
        step_results: &BTreeMap<usize, Option<AgentStep>>,
        threshold: f64,
        drill_summary: Option<&DrillSummary>,
    ) -> ConsensusVote {
        let active_count = step_results.len();
        if active_count == 0 {
            return ConsensusVote {
                reached: false,
                confidence: 0.0,
                participating_agents: 0,
                successful_agents: 0,
            };
        }

        let scores: Vec<f64> = step_results
            .values()
            .filter_map(|opt| opt.as_ref())
            .map(|s: &AgentStep| s.score)
            .filter(|&sc| sc > 0.0)
            .collect();

        let successful_count = scores.len();
        let mean_score = if successful_count == 0 {
            0.0
        } else {
            (scores.iter().sum::<f64>() / successful_count as f64).clamp(0.0, 1.0)
        };

        match self {
            ConsensusMode::DrillBacked => {
                // Consensus is determined by the PMHD drill result
                let (reached, confidence) = match drill_summary {
                    Some(ds) => {
                        let q = ds.mean_quality;
                        (ds.monolith_count >= 1 && q >= threshold, q)
                    }
                    None => (false, 0.0),
                };
                ConsensusVote {
                    reached,
                    confidence,
                    participating_agents: active_count,
                    successful_agents: successful_count,
                }
            }
            ConsensusMode::Majority => ConsensusVote {
                reached: successful_count * 2 > active_count,
                confidence: mean_score,
                participating_agents: active_count,
                successful_agents: successful_count,
            },
            ConsensusMode::WeightedResonance => ConsensusVote {
                reached: mean_score >= threshold,
                confidence: mean_score,
                participating_agents: active_count,
                successful_agents: successful_count,
            },
            ConsensusMode::Unanimous => ConsensusVote {
                reached: successful_count == active_count,
                confidence: mean_score,
                participating_agents: active_count,
                successful_agents: successful_count,
            },
        }
    }
}

// ─── drill_round ──────────────────────────────────────────────────────────────

/// Run a PMHD drill for one Swarm round.
///
/// The `DecisionSpec` encodes the swarm goal as the intent and maps the
/// `confidence_target` into a goal weight so that the drill's internal
/// quality evaluation aligns with the swarm's convergence target.
pub fn drill_round(
    goal_intent: &str,
    goal_domain: Option<&str>,
    goal_constraints: &[String],
    confidence_target: f64,
    round_id: usize,
    cfg: &PmhdConfig,
) -> DrillSummary {
    let mut goals = BTreeMap::new();
    goals.insert("resonance".to_string(), confidence_target);
    goals.insert("consensus".to_string(), 1.0);

    let spec = DecisionSpec::new(
        goal_intent,
        goals,
        goal_constraints.to_vec(),
        goal_domain.unwrap_or("swarm"),
        // Seed per round for determinism: base seed XOR round_id
        PmhdConfig {
            seed: cfg.seed ^ (round_id as u64 * 0x9e37_79b9_7f4a_7c15),
            ..cfg.clone()
        },
    );

    let mut engine = DrillEngine::new(PmhdConfig {
        seed: cfg.seed ^ (round_id as u64 * 0x9e37_79b9_7f4a_7c15),
        ..cfg.clone()
    });

    let result = engine.drill(&spec);

    let mean_quality = if result.monoliths.is_empty() {
        0.0
    } else {
        let sum: f64 = result.monoliths.iter().map(|m| m.quality.mean()).sum();
        (sum / result.monoliths.len() as f64).clamp(0.0, 1.0)
    };

    DrillSummary {
        ticks_executed: result.ticks_executed,
        pool_final_size: result.pool_final_size,
        monolith_count: result.monoliths.len(),
        mean_quality,
    }
}

// ─── SwarmPolicy ──────────────────────────────────────────────────────────────

/// Configuration for a Swarm run.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SwarmPolicy {
    /// Number of agent members.
    pub size: usize,
    /// Base seed; member i gets seed `base_seed + i`.
    pub base_seed: u64,
    /// Maximum number of rounds before the Swarm stops.
    pub max_rounds: usize,
    /// How consensus is determined.
    pub consensus_mode: ConsensusMode,
    /// Threshold used by WeightedResonance and DrillBacked consensus (0–1).
    pub consensus_threshold: f64,
    /// When set, a PMHD drill is run each round.
    /// Required when `consensus_mode == DrillBacked`.
    pub drill_config: Option<PmhdConfig>,
}

impl Default for SwarmPolicy {
    fn default() -> Self {
        Self {
            size: 4,
            base_seed: 0,
            max_rounds: 16,
            consensus_mode: ConsensusMode::WeightedResonance,
            consensus_threshold: 0.6,
            drill_config: None,
        }
    }
}
