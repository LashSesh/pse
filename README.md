# PSE — Post-Symbolic Engine

A universal computation engine that processes information through topology,
physics, and geometry rather than through symbols or statistics.

## What It Is

PSE observes data streams, detects invariant patterns through Kuramoto phase
synchronization, validates them through an 8-gate adversarial falsification
cascade, and crystallizes survivors as cryptographically anchored,
deterministically reproducible artifacts. It accumulates knowledge over time
through progressive convergence — no subscriptions, no cloud dependency.

It is domain-agnostic. Financial markets, medical diagnostics, cybersecurity,
industrial sensors, climate data — any domain that produces observable data
streams can use PSE by implementing a thin adapter trait.

## Core Principles

- **Resonance, not statistics**: Pattern detection via Kuramoto phase coupling
- **Topology, not rules**: Navigation via Laplacian spectral decomposition
- **Crystallization, not caching**: SHA-256 content-addressed, evidence-chained artifacts
- **Falsification, not verification**: 8-gate adversarial cascade (Popperian epistemology)
- **Convergence, not subscription**: Progressive offline capability via pattern accumulation
- **Constitution, not configuration**: ADAMANT Protocol (21 machine-verifiable axioms)

## Benchmarks

Measured on Intel Core i3 (2 cores / 4 threads, 2.1 GHz), 8 GB RAM, Windows.
No GPU. No cluster. No cloud.

| ID | Benchmark | Value | Unit |
|----|-----------|-------|------|
| B01a | Observe throughput | 655,115 | obs/sec |
| B01b | Full 15-stage pipeline | 9,695 | cycles/sec |
| B02 | Crystal serialization | 52.4 | µs/crystal |
| B03 | Evidence verification | 5.5 | µs/verify |
| B05 | Determinism check | PASS | 58 crystals, bit-identical |
| B06 | Laplacian (200 nodes) | 6.9 | ms |
| B07 | Fiedler vector | 130 | µs |
| B08 | Kuramoto convergence | 3.6 | ms (18 ticks, r=0.95) |
| B09 | Navigator step (TRITON) | 148 | µs/step |
| B10 | Constraint propagation | 12.7 | µs/component |
| B11 | Memory (5K entities) | 2.4 | MB |
| B12 | Capsule (AES-256-GCM) | 9.6 | µs/roundtrip |
| B13 | Registry lookup | 0.16 | µs |
| B14 | Swarm consensus (4 agents) | 0.05 | ms |
| B15 | Full macro-step | 4.3 | ms |

Run benchmarks yourself:

```bash
cargo run --release --example bench_full
```

## Quick Start

```bash
# Build
cargo build --release

# Run benchmarks
cargo run --release --example bench_full

# Run synthetic scenario (58 crystals from 200 ticks)
cargo run --release --example synthetic
```

```rust
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;
use pse_types::Config;

let config = Config::default();
let mut state = GlobalState::new(&config);
let adapter = PassthroughAdapter::new("my_source");

// Feed observations, get crystals
let batch = vec![serde_json::to_vec(&my_data).unwrap()];
if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
    println!("Crystal: {:?}", crystal.crystal_id);
}
```

## Domain Adapter

PSE is domain-agnostic. Implement the `DomainAdapter` trait to connect any
data source:

```rust
use pse_core::DomainAdapter;

struct MyFinanceAdapter;

impl DomainAdapter for MyFinanceAdapter {
    fn domain_name(&self) -> &str { "financial" }
}
```

## Architecture

```
PSE (20 crates, 152 tests)
├── Observation:     pse-graph, pse-scale (Kuramoto, multi-scale)
├── Analysis:        pse-extract, pse-topology (Laplacian, Fiedler, Betti)
├── Validation:      pse-cascade, pse-pmhd (8-gate adversarial falsification)
├── Crystallization: pse-types, pse-evidence, pse-registry, pse-manifest
├── Exploration:     pse-navigator (TRITON golden-angle spiral, SimplexMesh)
├── Coordination:    pse-swarm (multi-agent consensus)
├── Constraint:      pse-constraint (degrees-of-freedom analysis)
├── Infrastructure:  pse-store, pse-capsule, pse-scheduler, pse-replay
├── Interface:       pse-gateway, pse-cli
└── Core:            pse-core (DomainAdapter trait, Engine)
```

## Derived From

PSE is extracted from ISLS (Intelligent Semantic Ledger Substrate).
Constitutional governance: ADAMANT Protocol (Zenodo, CC BY 4.0).

## Author

Sebastian Klemm

## License

MIT
