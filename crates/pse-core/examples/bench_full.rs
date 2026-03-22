//! PSE Benchmark Suite — Phase 3: Full benchmark (B01–B15)
//!
//! Run with: cargo run --release --example bench_full

use std::collections::BTreeMap;
use std::time::Instant;

use pse_capsule::{seal, open, CapsulePolicy};
use pse_core::{macro_step, GlobalState};
use pse_evidence::{verify_crystal, Archive};
use pse_graph::{ingest, PassthroughAdapter, PersistentGraph};
use pse_manifest::build_manifest;
use pse_navigator::{Navigator, NavigatorConfig, SpectralSignature};
use pse_registry::{RegistryEntry, RegistryKind, RegistrySet};
use pse_replay::compare_crystal_sequences;
use pse_swarm::{AgentGoal, ConsensusMode, Swarm, SwarmPolicy};
use pse_topology::{
    compute_laplacian, init_kuramoto_state, kuramoto_step, spectral_decompose, TopologyConfig,
};
use pse_types::{
    Config, FiveDState, MeasurementContext, RunDescriptor, SchedulerConfig, SemanticCrystal,
};

/// Collected benchmark result for JSON output.
struct BenchResult {
    key: &'static str,
    value: f64,
    unit: &'static str,
    detail: String,
}

fn main() {
    print_platform_header();

    println!("PSE Benchmark Suite v0.1.0 — Full");
    println!("==================================\n");

    let mut results: Vec<BenchResult> = Vec::new();

    // Phase 1: Core
    results.push(bench_b01a_observe_only());
    let (crystals, r) = bench_b01b_full_pipeline();
    results.push(r);
    results.push(bench_b02_crystal_serialization(&crystals));
    results.push(bench_b03_evidence_verification(&crystals));
    results.push(bench_b04_replay_speed());
    results.push(bench_b05_determinism());

    println!();

    // Phase 2: Topology
    let graph = build_topo_graph(200, 800);
    println!(
        "Topology graph: {} vertices, {} edges\n",
        graph.graph.node_count(),
        graph.graph.edge_count()
    );
    results.push(bench_b06_laplacian(&graph));
    results.push(bench_b07_fiedler(&graph));
    results.push(bench_b08_kuramoto(&graph));
    results.push(bench_b09_navigator_step());
    results.push(bench_b10_constraint_propagation());

    println!();

    // Phase 3: Extended
    results.push(bench_b11_memory_scaling());
    results.push(bench_b12_capsule_roundtrip());
    results.push(bench_b13_registry_lookup());
    results.push(bench_b14_swarm_consensus());
    results.push(bench_b15_full_macro_step());

    println!();

    // Write JSON
    write_results_json(&results);

    println!("\nDone.");
}

// ─── Platform Header ─────────────────────────────────────────────────────────

fn print_platform_header() {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;

    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(|l| l.split(':').nth(1).unwrap_or("unknown").trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let ram = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal"))
                .map(|l| l.split(':').nth(1).unwrap_or("unknown").trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    println!("Platform: {} {}", os, arch);
    println!("CPU: {}", cpu);
    println!("RAM: {}", ram);
    println!("Timestamp: {}", timestamp);
    println!();
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

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
            _ => {}
        }
    }
    crystals
}

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

fn build_topo_graph(n_nodes: usize, n_edges: usize) -> PersistentGraph {
    let mut graph = PersistentGraph::new();

    for i in 0..n_nodes {
        let vid = i as u64 + 1;
        graph.upsert_vertex(vid, i as f64 * 0.01);
        let phase = (i as f64 * 0.1) % std::f64::consts::TAU;
        graph.embedding.insert(
            vid,
            FiveDState {
                p: i as f64 / n_nodes as f64,
                rho: 0.5 + 0.3 * phase.sin(),
                omega: phase,
                chi: (i as f64 * 0.07).cos(),
                eta: 0.1,
            },
        );
    }

    let mut rng: u64 = 42;
    let next = |r: &mut u64| -> u64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *r
    };

    let mut edge_count = 0;
    for i in 0..(n_nodes - 1) {
        graph.upsert_edge(i as u64 + 1, i as u64 + 2, 0.0);
        edge_count += 1;
    }
    while edge_count < n_edges {
        let a = (next(&mut rng) % n_nodes as u64) + 1;
        let b = (next(&mut rng) % n_nodes as u64) + 1;
        if a != b {
            graph.upsert_edge(a, b, edge_count as f64 * 0.001);
            edge_count += 1;
        }
    }
    graph
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phase 1: Core Benchmarks (B01a, B01b, B02–B05)
// ═══════════════════════════════════════════════════════════════════════════════

fn bench_b01a_observe_only() -> BenchResult {
    let n_entities = 50;
    let n_ticks = 200;
    let config = Config::default();
    let adapter = PassthroughAdapter::new("bench");
    let ctx = MeasurementContext::default();

    let all_batches = build_obs_batches(n_entities, n_ticks);
    let mut graph = PersistentGraph::new();

    let start = Instant::now();
    for batch in &all_batches {
        let mut canonical: Vec<pse_types::Observation> = Vec::with_capacity(batch.len());
        for raw in batch {
            canonical.push(ingest(&adapter, raw, &ctx).unwrap());
        }
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

    BenchResult {
        key: "B01a_observe_only",
        value: obs_per_sec,
        unit: "obs/sec",
        detail: format!("{} obs, {} vertices", total_obs as u64, graph.graph.node_count()),
    }
}

fn bench_b01b_full_pipeline() -> (Vec<SemanticCrystal>, BenchResult) {
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

    let r = BenchResult {
        key: "B01b_full_pipeline",
        value: obs_per_sec,
        unit: "obs/sec",
        detail: format!("{} crystals from {} obs", crystals.len(), total_obs as u64),
    };
    (crystals, r)
}

fn bench_b02_crystal_serialization(crystals: &[SemanticCrystal]) -> BenchResult {
    if crystals.is_empty() {
        println!("B02 crystal_serialization: SKIPPED (no crystals produced)");
        return BenchResult {
            key: "B02_crystal_serialization", value: 0.0,
            unit: "µs/crystal", detail: "SKIPPED".into(),
        };
    }

    let crystal = &crystals[0];
    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let json = serde_json::to_vec(crystal).unwrap();
        let _: SemanticCrystal = serde_json::from_slice(&json).unwrap();
    }
    let elapsed = start.elapsed();
    let us = elapsed.as_micros() as f64 / iterations as f64;

    println!("B02 crystal_serialization: {:.1} µs/crystal", us);

    BenchResult {
        key: "B02_crystal_serialization", value: us,
        unit: "µs/crystal", detail: format!("{} iterations", iterations),
    }
}

fn bench_b03_evidence_verification(crystals: &[SemanticCrystal]) -> BenchResult {
    if crystals.is_empty() {
        println!("B03 evidence_verification: SKIPPED (no crystals produced)");
        return BenchResult {
            key: "B03_evidence_verification", value: 0.0,
            unit: "µs/verify", detail: "SKIPPED".into(),
        };
    }

    let crystal = &crystals[0];
    let pinned: BTreeMap<String, String> = BTreeMap::new();
    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = verify_crystal(crystal, &pinned);
    }
    let elapsed = start.elapsed();
    let us = elapsed.as_micros() as f64 / iterations as f64;

    println!("B03 evidence_verification: {:.1} µs/verify", us);

    BenchResult {
        key: "B03_evidence_verification", value: us,
        unit: "µs/verify", detail: format!("{} iterations", iterations),
    }
}

fn bench_b04_replay_speed() -> BenchResult {
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

    let crystal_count = results.iter().filter(|r| r.is_some()).count();
    let steps_per_sec = n_ticks as f64 / elapsed.as_secs_f64();

    println!(
        "B04 replay_speed: {:.0} steps/sec ({} crystals in {:.2}s)",
        steps_per_sec, crystal_count, elapsed.as_secs_f64()
    );

    BenchResult {
        key: "B04_replay_speed", value: steps_per_sec,
        unit: "steps/sec", detail: format!("{} crystals", crystal_count),
    }
}

fn bench_b05_determinism() -> BenchResult {
    let config = Config::default();
    let crystals_a = run_scenario(&config, 50, 200);
    let crystals_b = run_scenario(&config, 50, 200);

    let result = compare_crystal_sequences(&crystals_a, &crystals_b);

    if result.deterministic {
        println!(
            "B05 determinism_check: PASS ({} crystals)",
            result.crystal_count
        );
    } else {
        let mismatches = result.digest_matches.iter().filter(|m| !**m).count();
        println!(
            "B05 determinism_check: FAIL ({} mismatches out of {} crystals)",
            mismatches, result.crystal_count
        );
    }

    BenchResult {
        key: "B05_determinism_check",
        value: if result.deterministic { 1.0 } else { 0.0 },
        unit: "pass",
        detail: format!("{} crystals, {}", result.crystal_count,
            if result.deterministic { "PASS" } else { "FAIL" }),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phase 2: Topology Benchmarks (B06–B10)
// ═══════════════════════════════════════════════════════════════════════════════

fn bench_b06_laplacian(graph: &PersistentGraph) -> BenchResult {
    let start = Instant::now();
    let laplacian = compute_laplacian(graph);
    let topo_config = TopologyConfig::default();
    let decomp = spectral_decompose(&laplacian, topo_config.spectral_k_max);
    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;

    println!(
        "B06 laplacian_computation: {:.1} ms (n={}, rank={}, gap={:.4})",
        ms, laplacian.n, decomp.truncation_rank, decomp.spectral_gap,
    );

    BenchResult {
        key: "B06_laplacian_computation", value: ms,
        unit: "ms", detail: format!("n={}, rank={}", laplacian.n, decomp.truncation_rank),
    }
}

fn bench_b07_fiedler(graph: &PersistentGraph) -> BenchResult {
    let laplacian = compute_laplacian(graph);

    let start = Instant::now();
    let decomp = spectral_decompose(&laplacian, 2);
    let elapsed = start.elapsed();
    let us = elapsed.as_micros() as f64;

    let fiedler_len = decomp.fiedler_vector.len();
    let fiedler_range = if !decomp.fiedler_vector.is_empty() {
        let min = decomp.fiedler_vector.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = decomp.fiedler_vector.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        max - min
    } else {
        0.0
    };

    println!(
        "B07 fiedler_vector: {:.1} µs (dim={}, range={:.4})",
        us, fiedler_len, fiedler_range,
    );

    BenchResult {
        key: "B07_fiedler_vector", value: us,
        unit: "µs", detail: format!("dim={}, range={:.4}", fiedler_len, fiedler_range),
    }
}

fn bench_b08_kuramoto(graph: &PersistentGraph) -> BenchResult {
    if graph.graph.node_count() < 2 {
        println!("B08 kuramoto_convergence: SKIPPED (graph too small)");
        return BenchResult {
            key: "B08_kuramoto_convergence", value: 0.0,
            unit: "ms", detail: "SKIPPED".into(),
        };
    }

    let topo_config = TopologyConfig {
        kuramoto_steps: 500,
        kuramoto_dt: 0.05,
        kuramoto_coupling: 2.0,
        ..TopologyConfig::default()
    };

    let mut kstate = init_kuramoto_state(graph);
    let threshold = 0.95;

    let start = Instant::now();
    let mut converged_tick = None;
    for tick in 0..topo_config.kuramoto_steps {
        kuramoto_step(&mut kstate, graph, &topo_config);
        if kstate.order_parameter >= threshold && converged_tick.is_none() {
            converged_tick = Some(tick + 1);
        }
    }
    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;

    match converged_tick {
        Some(t) => println!(
            "B08 kuramoto_convergence: {} ticks, {:.1} ms (r={:.4})",
            t, ms, kstate.order_parameter,
        ),
        None => println!(
            "B08 kuramoto_convergence: NOT CONVERGED in {} ticks, {:.1} ms (r={:.4})",
            topo_config.kuramoto_steps, ms, kstate.order_parameter,
        ),
    }

    BenchResult {
        key: "B08_kuramoto_convergence", value: ms,
        unit: "ms",
        detail: format!("{} ticks, r={:.4}",
            converged_tick.unwrap_or(topo_config.kuramoto_steps), kstate.order_parameter),
    }
}

fn bench_b09_navigator_step() -> BenchResult {
    let nav_config = NavigatorConfig {
        dim: 5,
        seed: 42,
        ..NavigatorConfig::default()
    };

    let evaluator = |point: &[f64]| -> SpectralSignature {
        let psi = point.iter().sum::<f64>() / point.len() as f64;
        let rho = 1.0 - (psi - 0.5).abs();
        let omega = point.iter().map(|x| x * x).sum::<f64>().sqrt();
        SpectralSignature::new(psi, rho, omega)
    };

    let mut navigator = Navigator::new(nav_config, evaluator);
    let n_steps = 50;

    let start = Instant::now();
    for _ in 0..n_steps {
        navigator.step();
    }
    let elapsed = start.elapsed();
    let us_per_step = elapsed.as_micros() as f64 / n_steps as f64;

    println!(
        "B09 navigator_step: {:.1} µs/step ({} steps, best_res={:.4})",
        us_per_step, n_steps, navigator.spiral.best_resonance(),
    );

    BenchResult {
        key: "B09_navigator_step", value: us_per_step,
        unit: "µs/step", detail: format!("{} steps", n_steps),
    }
}

fn bench_b10_constraint_propagation() -> BenchResult {
    let config = Config::default();
    let n_components = 20;

    let start = Instant::now();
    let mut dof_zero = 0;
    for i in 0..n_components {
        let mut state = GlobalState::new(&config);
        let adapter = PassthroughAdapter::new(format!("comp_{}", i));
        for tick in 0..10 {
            let value = ((tick as f64 * 0.1) + (i as f64 * 0.3)).sin();
            let payload = serde_json::json!({
                "entity": format!("comp_{}", i),
                "value": value,
                "tick": tick,
            });
            let batch = vec![serde_json::to_vec(&payload).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        if state.morph.pressure.values().all(|p| *p < 0.01) {
            dof_zero += 1;
        }
    }
    let elapsed = start.elapsed();
    let us_per_comp = elapsed.as_micros() as f64 / n_components as f64;
    let pct_zero = (dof_zero as f64 / n_components as f64) * 100.0;

    println!(
        "B10 constraint_propagation: {:.1} µs/component, {:.0}% DoF=0",
        us_per_comp, pct_zero,
    );

    BenchResult {
        key: "B10_constraint_propagation", value: us_per_comp,
        unit: "µs/component", detail: format!("{:.0}% DoF=0", pct_zero),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Phase 3: Extended Benchmarks (B11–B15)
// ═══════════════════════════════════════════════════════════════════════════════

// ─── B11: Memory Scaling ─────────────────────────────────────────────────────

fn bench_b11_memory_scaling() -> BenchResult {
    let n_entities = 5000;
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("memory_scale");

    let start = Instant::now();
    for tick in 0..10 {
        let mut batch = Vec::with_capacity(n_entities);
        for entity in 0..n_entities {
            let value = ((tick as f64 * 0.1) + (entity as f64 * 0.01)).sin();
            let payload = serde_json::json!({
                "entity": format!("ent_{:05}", entity),
                "value": value,
                "tick": tick,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }
        let _ = macro_step(&mut state, &batch, &config, &adapter);
    }
    let elapsed = start.elapsed();
    let ms = elapsed.as_secs_f64() * 1000.0;

    let total_obs = n_entities * 10;
    let graph_mem = state.graph.estimate_heap_size();
    let crystals = state.archive.len();

    println!(
        "B11 memory_scaling: {:.1} ms for {} entities ({} obs, {} crystals, graph ~{} bytes)",
        ms, n_entities, total_obs, crystals, graph_mem,
    );

    BenchResult {
        key: "B11_memory_scaling", value: ms,
        unit: "ms", detail: format!("{} entities, {} obs, ~{} bytes", n_entities, total_obs, graph_mem),
    }
}

// ─── B12: Capsule Roundtrip ──────────────────────────────────────────────────

fn bench_b12_capsule_roundtrip() -> BenchResult {
    let config = Config::default();
    let rd = RunDescriptor {
        config,
        operator_versions: BTreeMap::new(),
        initial_state_digest: [0u8; 32],
        seed: Some(42),
        registry_digests: BTreeMap::new(),
        scheduler: SchedulerConfig::default(),
    };
    let archive = Archive::new();
    let registries = RegistrySet::new();
    let traces: Vec<pse_manifest::TraceEntry> = vec![];
    let obs_log: Vec<Vec<Vec<u8>>> = vec![];
    let manifest = build_manifest(&rd, &traces, &archive, &registries, "bench", &obs_log);

    let master_key: &[u8; 32] = b"bench-master-key-32bytes-pad!!@@";
    let secret = b"benchmark-secret-payload-data-42";

    let policy = CapsulePolicy {
        require_lock_program_id: [0u8; 32],
        require_rd_digest: manifest.rd_digest,
        require_gate_proofs: vec![],
        require_manifest_id: Some(manifest.run_id),
        expires_at: None,
        max_uses: None,
    };

    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let capsule = seal(
            secret,
            policy.clone(),
            BTreeMap::new(),
            master_key,
            &manifest,
        )
        .unwrap();
        let recovered = open(&capsule, master_key, &manifest, None).unwrap();
        assert_eq!(recovered.len(), secret.len());
    }
    let elapsed = start.elapsed();

    let us = elapsed.as_micros() as f64 / iterations as f64;
    println!("B12 capsule_roundtrip: {:.1} µs/roundtrip", us);

    BenchResult {
        key: "B12_capsule_roundtrip", value: us,
        unit: "µs/roundtrip", detail: format!("{} iterations", iterations),
    }
}

// ─── B13: Registry Lookup ────────────────────────────────────────────────────

fn bench_b13_registry_lookup() -> BenchResult {
    let mut registry = pse_registry::Registry::new(RegistryKind::Operator);

    // Register 100 operators
    for i in 0..100 {
        let entry = RegistryEntry::new(
            format!("Operator_{:03}", i),
            "1.0.0".to_string(),
            [i as u8; 32],
            RegistryKind::Operator,
            BTreeMap::new(),
        );
        registry.register(entry).unwrap();
    }

    let iterations = 10_000;

    let start = Instant::now();
    for i in 0..iterations {
        let name = format!("Operator_{:03}", i % 100);
        let _ = registry.resolve(&name);
    }
    let elapsed = start.elapsed();

    let us = elapsed.as_micros() as f64 / iterations as f64;
    println!("B13 registry_lookup: {:.1} µs/lookup ({} entries)", us, 100);

    BenchResult {
        key: "B13_registry_lookup", value: us,
        unit: "µs/lookup", detail: format!("100 entries, {} lookups", iterations),
    }
}

// ─── B14: Swarm Consensus ────────────────────────────────────────────────────

fn bench_b14_swarm_consensus() -> BenchResult {
    let goal = AgentGoal::new("discover structural invariants in time-series");
    let policy = SwarmPolicy {
        size: 4,
        base_seed: 42,
        max_rounds: 5,
        consensus_mode: ConsensusMode::WeightedResonance,
        consensus_threshold: 0.5,
        drill_config: None,
    };

    let start = Instant::now();
    let mut swarm = Swarm::new(policy, goal);
    let report = swarm.run();
    let elapsed = start.elapsed();

    let ms = elapsed.as_secs_f64() * 1000.0;
    println!(
        "B14 swarm_consensus: {} members, {} rounds, {:.1} ms (consensus={}, resonance={:.4})",
        report.swarm_size, report.rounds_run, ms,
        report.consensus_reached, report.final_resonance,
    );

    BenchResult {
        key: "B14_swarm_consensus", value: ms,
        unit: "ms",
        detail: format!("{} members, {} rounds, consensus={}",
            report.swarm_size, report.rounds_run, report.consensus_reached),
    }
}

// ─── B15: Full Macro-Step ────────────────────────────────────────────────────

fn bench_b15_full_macro_step() -> BenchResult {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("full_step");
    let n_entities = 50;

    // Warm up with 50 ticks
    for tick in 0..50 {
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
        let _ = macro_step(&mut state, &batch, &config, &adapter);
    }

    // Measure one end-to-end tick
    let tick = 50;
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

    let start = Instant::now();
    let result = macro_step(&mut state, &batch, &config, &adapter);
    let elapsed = start.elapsed();

    let crystal_str = match &result {
        Ok(Some(_)) => "CRYSTAL",
        Ok(None) => "REJECTED",
        Err(e) => {
            eprintln!("B15 error: {}", e);
            "ERROR"
        }
    };

    let us = elapsed.as_micros() as f64;
    println!(
        "B15 full_macro_step: {:.1} µs ({}, {} obs)",
        us, crystal_str, n_entities,
    );

    BenchResult {
        key: "B15_full_macro_step", value: us,
        unit: "µs", detail: format!("{}, {} obs", crystal_str, n_entities),
    }
}

// ─── JSON Output ─────────────────────────────────────────────────────────────

fn write_results_json(results: &[BenchResult]) {
    let mut result_map = serde_json::Map::new();
    for r in results {
        result_map.insert(
            r.key.to_string(),
            serde_json::json!({
                "value": r.value,
                "unit": r.unit,
                "detail": r.detail,
            }),
        );
    }

    let output = serde_json::json!({
        "suite": "PSE Benchmark Suite",
        "version": "0.1.0",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "results": result_map,
    });

    std::fs::write(
        "bench_results.json",
        serde_json::to_string_pretty(&output).unwrap(),
    )
    .unwrap();
    println!("Results written to bench_results.json");
}
