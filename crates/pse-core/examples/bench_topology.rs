//! PSE Benchmark Suite — Phase 2: Topology benchmarks
//!
//! Run with: cargo run --release --example bench_topology

use std::time::Instant;

use pse_core::GlobalState;
use pse_graph::PersistentGraph;
use pse_navigator::{Navigator, NavigatorConfig, SpectralSignature};
use pse_topology::{
    compute_laplacian, init_kuramoto_state, kuramoto_step, spectral_decompose, TopologyConfig,
};
use pse_types::{Config, FiveDState};

fn main() {
    println!("PSE Benchmark Suite v0.1.0 — Topology");
    println!("======================================\n");

    // Build a graph with 200 nodes, ~800 edges directly
    let graph = build_graph(200, 800);
    let n = graph.graph.node_count();
    let e = graph.graph.edge_count();
    println!("Graph built: {} vertices, {} edges\n", n, e);

    bench_b06_laplacian(&graph);
    bench_b07_fiedler(&graph);
    bench_b08_kuramoto(&graph);
    bench_b09_navigator_step();
    bench_b10_constraint_propagation();

    println!("\nDone.");
}

/// Build a PersistentGraph with n_nodes and ~n_edges edges.
/// Uses deterministic construction with seed=42.
fn build_graph(n_nodes: usize, n_edges: usize) -> PersistentGraph {
    let mut graph = PersistentGraph::new();

    // Create vertices with embeddings
    for i in 0..n_nodes {
        let vid = i as u64 + 1;
        graph.upsert_vertex(vid, i as f64 * 0.01);
        // Set embedding so Kuramoto has natural frequencies
        let phase = (i as f64 * 0.1) % std::f64::consts::TAU;
        graph.embedding.insert(
            vid,
            FiveDState {
                p: (i as f64 / n_nodes as f64),
                rho: 0.5 + 0.3 * phase.sin(),
                omega: phase,
                chi: (i as f64 * 0.07).cos(),
                eta: 0.1,
            },
        );
    }

    // Create edges: connect i to (i+1), (i+prime_offset), etc.
    // Use a simple LCG to deterministically pick edges
    let mut rng: u64 = 42;
    let next = |r: &mut u64| -> u64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *r
    };

    let mut edge_count = 0;
    // First: chain all nodes
    for i in 0..(n_nodes - 1) {
        let from = (i as u64) + 1;
        let to = (i as u64) + 2;
        graph.upsert_edge(from, to, 0.0);
        edge_count += 1;
    }
    // Then: random additional edges until we reach target
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

// ─── B06: Laplacian Computation ──────────────────────────────────────────────

fn bench_b06_laplacian(graph: &PersistentGraph) {
    let start = Instant::now();
    let laplacian = compute_laplacian(graph);
    let topo_config = TopologyConfig::default();
    let decomp = spectral_decompose(&laplacian, topo_config.spectral_k_max);
    let elapsed = start.elapsed();

    println!(
        "B06 laplacian_computation: {:.1} ms (n={}, rank={}, gap={:.4})",
        elapsed.as_secs_f64() * 1000.0,
        laplacian.n,
        decomp.truncation_rank,
        decomp.spectral_gap,
    );
}

// ─── B07: Fiedler Vector ────────────────────────────────────────────────────

fn bench_b07_fiedler(graph: &PersistentGraph) {
    let laplacian = compute_laplacian(graph);

    let start = Instant::now();
    let decomp = spectral_decompose(&laplacian, 2);
    let elapsed = start.elapsed();

    let fiedler_len = decomp.fiedler_vector.len();
    let fiedler_range = if !decomp.fiedler_vector.is_empty() {
        let min = decomp
            .fiedler_vector
            .iter()
            .cloned()
            .fold(f64::INFINITY, f64::min);
        let max = decomp
            .fiedler_vector
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max);
        max - min
    } else {
        0.0
    };

    println!(
        "B07 fiedler_vector: {:.1} µs (dim={}, range={:.4})",
        elapsed.as_micros() as f64,
        fiedler_len,
        fiedler_range,
    );
}

// ─── B08: Kuramoto Convergence ──────────────────────────────────────────────

fn bench_b08_kuramoto(graph: &PersistentGraph) {
    let n = graph.graph.node_count();
    if n < 2 {
        println!("B08 kuramoto_convergence: SKIPPED (graph too small)");
        return;
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

    match converged_tick {
        Some(t) => println!(
            "B08 kuramoto_convergence: {} ticks, {:.1} ms (r={:.4})",
            t,
            elapsed.as_secs_f64() * 1000.0,
            kstate.order_parameter,
        ),
        None => println!(
            "B08 kuramoto_convergence: NOT CONVERGED in {} ticks, {:.1} ms (r={:.4})",
            topo_config.kuramoto_steps,
            elapsed.as_secs_f64() * 1000.0,
            kstate.order_parameter,
        ),
    }
}

// ─── B09: Navigator Spiral Step ──────────────────────────────────────────────

fn bench_b09_navigator_step() {
    let nav_config = NavigatorConfig {
        dim: 5,
        seed: 42,
        ..NavigatorConfig::default()
    };

    // Simple evaluator: mock spectral signature from point
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
        us_per_step,
        n_steps,
        navigator.spiral.best_resonance(),
    );
}

// ─── B10: Constraint Propagation ─────────────────────────────────────────────

fn bench_b10_constraint_propagation() {
    // pse-constraint doesn't expose a DoF analysis API directly.
    // Benchmark morphogenic pressure computation across components as a proxy.
    let config = Config::default();
    let n_components = 20;

    let start = Instant::now();
    let mut dof_zero = 0;
    for i in 0..n_components {
        let mut state = GlobalState::new(&config);
        // Use a unique adapter per component so each gets its own vertex
        let adapter = pse_graph::PassthroughAdapter::new(format!("comp_{}", i));
        for tick in 0..10 {
            let value = ((tick as f64 * 0.1) + (i as f64 * 0.3)).sin();
            let payload = serde_json::json!({
                "entity": format!("comp_{}", i),
                "value": value,
                "tick": tick,
            });
            let batch = vec![serde_json::to_vec(&payload).unwrap()];
            let _ = pse_core::macro_step(&mut state, &batch, &config, &adapter);
        }
        // Check if morph pressure is zero (proxy for DoF=0)
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
}
