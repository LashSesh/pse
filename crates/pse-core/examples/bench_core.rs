//! PSE Benchmark Suite — Phase 1: Core benchmarks
//!
//! Run with: cargo run --release --example bench_core

use std::collections::BTreeMap;
use std::time::Instant;

use pse_core::{macro_step, GlobalState};
use pse_evidence::verify_crystal;
use pse_graph::{ingest, PassthroughAdapter};
use pse_replay::compare_crystal_sequences;
use pse_types::{Config, MeasurementContext, RunDescriptor, SchedulerConfig, SemanticCrystal};

fn main() {
    println!("PSE Benchmark Suite v0.1.0");
    println!("==========================\n");

    bench_b01a_observe_only();
    let crystals_b01 = bench_b01b_full_pipeline();
    bench_b02_crystal_serialization(&crystals_b01);
    bench_b03_evidence_verification(&crystals_b01);
    bench_b04_replay_speed();
    bench_b05_determinism();

    println!("\nDone.");
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Run the standard scenario: 50 entities, n_ticks ticks.
/// Returns collected crystals.
fn run_scenario(config: &Config, n_entities: usize, n_ticks: usize) -> Vec<SemanticCrystal> {
    let mut state = GlobalState::new(config);
    let adapter = PassthroughAdapter::new("bench");
    let mut crystals = Vec::new();

    for tick in 0..n_ticks {
        let mut batch = Vec::with_capacity(n_entities);
        for entity in 0..n_entities {
            let value = ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin();
            let payload = serde_json::json!({
                "entity": format!("sensor_{:03}", entity),
                "value": value,
                "tick": tick,
                "phase": (tick as f64 * 0.05 + entity as f64 * 0.1)
                    % std::f64::consts::TAU,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }

        match macro_step(&mut state, &batch, config, &adapter) {
            Ok(Some(crystal)) => crystals.push(crystal),
            Ok(None) => {}
            Err(_) => {}
        }
    }

    crystals
}

/// Build observation batches for the standard scenario (for replay).
fn build_obs_batches(n_entities: usize, n_ticks: usize) -> Vec<Vec<Vec<u8>>> {
    (0..n_ticks)
        .map(|tick| {
            (0..n_entities)
                .map(|entity| {
                    let value = ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin();
                    let payload = serde_json::json!({
                        "entity": format!("sensor_{:03}", entity),
                        "value": value,
                        "tick": tick,
                        "phase": (tick as f64 * 0.05 + entity as f64 * 0.1)
                            % std::f64::consts::TAU,
                    });
                    serde_json::to_vec(&payload).unwrap()
                })
                .collect()
        })
        .collect()
}

// ─── B01a: Observe Only (canonicalize + graph persist, NO tick/crystallize) ──

fn bench_b01a_observe_only() {
    let n_entities = 50;
    let n_ticks = 200;
    let config = Config::default();
    let adapter = PassthroughAdapter::new("bench");
    let ctx = MeasurementContext::default();

    // Pre-build all payloads so we don't measure JSON serialization
    let all_batches: Vec<Vec<Vec<u8>>> = (0..n_ticks)
        .map(|tick| {
            (0..n_entities)
                .map(|entity| {
                    let value = ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin();
                    let payload = serde_json::json!({
                        "entity": format!("sensor_{:03}", entity),
                        "value": value,
                        "tick": tick,
                        "phase": (tick as f64 * 0.05 + entity as f64 * 0.1)
                            % std::f64::consts::TAU,
                    });
                    serde_json::to_vec(&payload).unwrap()
                })
                .collect()
        })
        .collect();

    let mut graph = pse_graph::PersistentGraph::new();

    let start = Instant::now();
    for batch in &all_batches {
        // L0: canonicalize each raw payload -> Observation (SHA-256 digest)
        let mut canonical: Vec<pse_types::Observation> = Vec::with_capacity(batch.len());
        for raw in batch {
            let obs = ingest(&adapter, raw, &ctx).unwrap();
            canonical.push(obs);
        }
        // L1: persist into graph (upsert vertex, edges, embeddings)
        graph
            .apply_observations(&canonical, &config.persistence)
            .unwrap();
    }
    let elapsed = start.elapsed();

    let total_obs = (n_entities * n_ticks) as f64;
    let obs_per_sec = total_obs / elapsed.as_secs_f64();

    println!(
        "B01a observe_only: {:.0} obs/sec ({} obs in {:.4}s, graph: {} vertices {} edges)",
        obs_per_sec,
        total_obs as u64,
        elapsed.as_secs_f64(),
        graph.graph.node_count(),
        graph.graph.edge_count(),
    );
}

// ─── B01b: Full Pipeline (observe + tick + crystallize) ─────────────────────

fn bench_b01b_full_pipeline() -> Vec<SemanticCrystal> {
    let n_entities = 50;
    let n_ticks = 200;
    let config = Config::default();

    let start = Instant::now();
    let crystals = run_scenario(&config, n_entities, n_ticks);
    let elapsed = start.elapsed();

    let total_obs = (n_entities * n_ticks) as f64;
    let obs_per_sec = total_obs / elapsed.as_secs_f64();

    println!(
        "B01b full_pipeline: {:.0} obs/sec ({} crystals from {} obs in {:.4}s)",
        obs_per_sec,
        crystals.len(),
        total_obs as u64,
        elapsed.as_secs_f64()
    );

    crystals
}

// ─── B02: Crystal Serialization ──────────────────────────────────────────────

fn bench_b02_crystal_serialization(crystals: &[SemanticCrystal]) {
    if crystals.is_empty() {
        println!("B02 crystal_serialization: SKIPPED (no crystals produced)");
        return;
    }

    let crystal = &crystals[0];
    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let json = serde_json::to_vec(crystal).unwrap();
        let _: SemanticCrystal = serde_json::from_slice(&json).unwrap();
    }
    let elapsed = start.elapsed();

    let us_per_crystal = elapsed.as_micros() as f64 / iterations as f64;
    println!("B02 crystal_serialization: {:.1} µs/crystal", us_per_crystal);
}

// ─── B03: Evidence Verification ──────────────────────────────────────────────

fn bench_b03_evidence_verification(crystals: &[SemanticCrystal]) {
    if crystals.is_empty() {
        println!("B03 evidence_verification: SKIPPED (no crystals produced)");
        return;
    }

    let crystal = &crystals[0];
    let pinned: BTreeMap<String, String> = BTreeMap::new();
    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = verify_crystal(crystal, &pinned);
    }
    let elapsed = start.elapsed();

    let us_per_verify = elapsed.as_micros() as f64 / iterations as f64;
    println!("B03 evidence_verification: {:.1} µs/verify", us_per_verify);
}

// ─── B04: Replay Speed ──────────────────────────────────────────────────────

fn bench_b04_replay_speed() {
    let n_entities = 50;
    let n_ticks = 200;
    let config = Config::default();

    let rd = RunDescriptor {
        config: config.clone(),
        operator_versions: BTreeMap::new(),
        initial_state_digest: [0u8; 32],
        seed: Some(42),
        registry_digests: BTreeMap::new(),
        scheduler: SchedulerConfig::default(),
    };

    let obs_batches = build_obs_batches(n_entities, n_ticks);

    let start = Instant::now();
    let results = pse_core::run_with_descriptor(&rd, &obs_batches).unwrap();
    let elapsed = start.elapsed();

    let steps_per_sec = n_ticks as f64 / elapsed.as_secs_f64();
    let crystal_count = results.iter().filter(|r| r.is_some()).count();

    println!(
        "B04 replay_speed: {:.0} steps/sec ({} crystals in {:.2}s)",
        steps_per_sec, crystal_count, elapsed.as_secs_f64()
    );
}

// ─── B05: Determinism Check ──────────────────────────────────────────────────

fn bench_b05_determinism() {
    let n_entities = 50;
    let n_ticks = 200;
    let config = Config::default();

    // Run 1
    let crystals_a = run_scenario(&config, n_entities, n_ticks);

    // Run 2 (identical)
    let crystals_b = run_scenario(&config, n_entities, n_ticks);

    let result = compare_crystal_sequences(&crystals_a, &crystals_b);

    if result.deterministic {
        println!(
            "B05 determinism_check: PASS ({} crystals)",
            result.crystal_count
        );
    } else {
        let mismatches = result
            .digest_matches
            .iter()
            .filter(|m| !**m)
            .count();
        println!(
            "B05 determinism_check: FAIL ({} mismatches out of {} crystals)",
            mismatches,
            result.crystal_count
        );
    }
}
