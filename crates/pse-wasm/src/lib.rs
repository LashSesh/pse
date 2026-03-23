//! # pse-wasm — WebAssembly build of PSE
//!
//! Wraps the PSE engine with a JSON-in/JSON-out interface for browser use.
//! All processing happens locally in the browser — no data leaves the machine.

use wasm_bindgen::prelude::*;
use pse_core::{macro_step, GlobalState};
use pse_graph::PassthroughAdapter;
use pse_types::Config;

/// The WASM-exposed PSE engine.
/// Wraps the real engine with a JSON-in/JSON-out interface.
#[wasm_bindgen]
pub struct PseWasm {
    state: GlobalState,
    config: Config,
    adapter: PassthroughAdapter,
    /// Running accumulation curve data.
    curve_data: Vec<CurvePoint>,
    /// Total ticks processed.
    total_ticks: u32,
}

struct CurvePoint {
    tick: u32,
    total_crystals: usize,
    memory_hits: u64,
}

#[wasm_bindgen]
impl PseWasm {
    /// Create a new PSE engine instance.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        console_error_panic_hook::set_once();
        let config = Config::default();
        let state = GlobalState::new(&config);
        Self {
            state,
            config,
            adapter: PassthroughAdapter::new("wasm"),
            curve_data: Vec::new(),
            total_ticks: 0,
        }
    }

    /// Ingest CSV data as a string.
    /// Returns JSON: { "rows_ingested": N, "columns": N, "entities": N, "column_stats": [...] }
    #[wasm_bindgen]
    pub fn ingest_csv(&mut self, csv_data: &str) -> String {
        let tab_config = pse_adapter_tabular::TabularConfig::default();
        match pse_adapter_tabular::parse_csv(csv_data, &tab_config) {
            Ok((rows, stats)) => {
                // Feed rows as observations to the engine
                for row in &rows {
                    let payload = match serde_json::to_vec(&serde_json::json!({
                        "entity": &row.entity_id,
                        "row_index": row.row_index,
                        "values": row.values,
                    })) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let _ = macro_step(&mut self.state, &[payload], &self.config, &self.adapter);
                    self.total_ticks += 1;
                }

                let mut entities: Vec<&str> = rows.iter().map(|r| r.entity_id.as_str()).collect();
                entities.sort();
                entities.dedup();
                let entity_count = entities.len();

                serde_json::json!({
                    "rows_ingested": rows.len(),
                    "columns": stats.len(),
                    "entities": entity_count,
                    "column_stats": stats,
                }).to_string()
            }
            Err(e) => {
                serde_json::json!({
                    "error": e.to_string()
                }).to_string()
            }
        }
    }

    /// Run N ticks of the engine with synthetic data.
    /// Returns JSON: { "ticks_run": N, "new_crystals": N, "memory_hits": N, "time_ms": F }
    #[wasm_bindgen]
    pub fn run(&mut self, ticks: u32) -> String {
        let crystals_before = self.state.archive.len();
        let hits_before = self.state.pattern_hits;

        for tick in 0..ticks {
            let t = self.total_ticks + tick;
            let n_entities = 10;
            let mut batch = Vec::with_capacity(n_entities);
            for entity in 0..n_entities {
                let value = ((t as f64 * 0.1) + (entity as f64 * 0.2)).sin();
                if let Ok(payload) = serde_json::to_vec(&serde_json::json!({
                    "entity": format!("e_{:03}", entity),
                    "value": value,
                    "tick": t,
                })) {
                    batch.push(payload);
                }
            }
            let _ = macro_step(&mut self.state, &batch, &self.config, &self.adapter);

            // Record accumulation curve data every 10 ticks
            if (t + 1) % 10 == 0 {
                self.curve_data.push(CurvePoint {
                    tick: t + 1,
                    total_crystals: self.state.archive.len(),
                    memory_hits: self.state.pattern_hits,
                });
            }
        }
        self.total_ticks += ticks;

        let new_crystals = self.state.archive.len() - crystals_before;
        let new_hits = self.state.pattern_hits - hits_before;

        serde_json::json!({
            "ticks_run": ticks,
            "new_crystals": new_crystals,
            "memory_hits": new_hits,
            "total_crystals": self.state.archive.len(),
            "total_ticks": self.total_ticks,
        }).to_string()
    }

    /// Get all crystals as JSON array.
    #[wasm_bindgen]
    pub fn crystals(&self) -> String {
        let crystals: Vec<serde_json::Value> = self.state.archive.crystals().iter().map(|c| {
            serde_json::json!({
                "crystal_id": hex_encode(&c.crystal_id),
                "stability_score": c.stability_score,
                "free_energy": c.free_energy,
                "region_size": c.region.len(),
                "created_at": c.created_at,
                "constraint_count": c.constraint_program.len(),
                "topology": {
                    "spectral_gap": c.topology_signature.spectral_gap,
                    "betti_0": c.topology_signature.betti_0,
                    "betti_1": c.topology_signature.betti_1,
                    "kuramoto_coherence": c.topology_signature.kuramoto_coherence,
                },
                "consensus": {
                    "primal_score": c.commit_proof.consensus_result.primal_score,
                    "dual_score": c.commit_proof.consensus_result.dual_score,
                    "mci": c.commit_proof.consensus_result.mci,
                },
            })
        }).collect();
        serde_json::to_string(&crystals).unwrap_or_else(|_| "[]".to_string())
    }

    /// Get quality report for ingested CSV data.
    /// Returns JSON with anomalies, drift events, column stats.
    #[wasm_bindgen]
    pub fn quality_report(&self, csv_data: &str) -> String {
        let tab_config = pse_adapter_tabular::TabularConfig::default();
        match pse_adapter_tabular::parse_csv(csv_data, &tab_config) {
            Ok((rows, stats)) => {
                let anomalies = pse_adapter_tabular::detect_outliers(&rows, &stats);
                let drifts = pse_adapter_tabular::detect_drift(&rows, &stats);
                serde_json::json!({
                    "rows": rows.len(),
                    "columns": stats.len(),
                    "column_stats": stats,
                    "anomalies": anomalies,
                    "drift_events": drifts,
                    "anomaly_count": anomalies.len(),
                    "drift_count": drifts.len(),
                }).to_string()
            }
            Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
        }
    }

    /// Get engine status.
    /// Returns JSON: { "observations": N, "crystals": N, "memory_size": N, "hit_rate": F }
    #[wasm_bindgen]
    pub fn status(&self) -> String {
        let mem_stats = self.state.memory.stats();
        serde_json::json!({
            "total_ticks": self.total_ticks,
            "crystals": self.state.archive.len(),
            "graph_vertices": self.state.graph.graph.node_count(),
            "graph_edges": self.state.graph.graph.edge_count(),
            "memory_size": mem_stats.index_size,
            "memory_hits": mem_stats.hits,
            "memory_misses": mem_stats.misses,
            "hit_rate": mem_stats.hit_rate,
            "pattern_hits": self.state.pattern_hits,
        }).to_string()
    }

    /// Get accumulation curve data.
    /// Returns JSON array of { "tick": N, "total_crystals": N, "memory_hits": N }
    #[wasm_bindgen]
    pub fn accumulation_curve(&self) -> String {
        let points: Vec<serde_json::Value> = self.curve_data.iter().map(|p| {
            serde_json::json!({
                "tick": p.tick,
                "total_crystals": p.total_crystals,
                "memory_hits": p.memory_hits,
            })
        }).collect();
        serde_json::to_string(&points).unwrap_or_else(|_| "[]".to_string())
    }

    /// Reset observations (clear graph but keep crystals in memory).
    #[wasm_bindgen]
    pub fn reset_observations(&mut self) {
        self.state.graph = pse_graph::PersistentGraph::new();
        self.state.prev_embeddings.clear();
        self.state.candidates.clear();
        self.state.morph = pse_constraint::MorphState::new();
        self.state.commit_index = 0;
        self.state.engine_state = pse_core::EngineState::Idle;
        self.state.consensus = pse_core::ConsensusState::default();
        self.state.h5_state = pse_types::FiveDState::default();
        self.state.t2 = 0.0;
        self.state.last_constraint_count = 0;
        self.state.last_gate_passed = false;
        self.state.por_fsm = pse_cascade::PoRFsm::new();
        self.state.phase_ladder = pse_cascade::build_phase_ladder(
            self.config.carrier.num_carriers, 0.0, 1.0,
        );
        self.state.active_carrier = 0;
        self.state.scale_state = pse_scale::MultiScaleState::default();
        self.total_ticks = 0;
        self.curve_data.clear();
    }

    /// Full reset (clear everything including pattern memory).
    #[wasm_bindgen]
    pub fn reset_all(&mut self) {
        self.state = GlobalState::new(&self.config);
        self.total_ticks = 0;
        self.curve_data.clear();
    }

    /// Get the embedded sample CSV data for "Try with sample data".
    #[wasm_bindgen]
    pub fn sample_csv() -> String {
        pse_adapter_tabular::embedded_test_csv()
    }
}

/// Convert bytes to hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
