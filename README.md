# PSE — Post-Symbolic Engine

A universal computation engine that processes information through topology,
physics, and geometry rather than through symbols or statistics.

## What It Is

PSE observes data streams, detects invariant patterns through Kuramoto phase
synchronization, validates them through an adversarial falsification cascade,
and crystallizes survivors as cryptographically anchored, deterministically
reproducible artifacts.

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

## Quick Start

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

## Performance

Measured on Intel Core i3 (2C/4T, 2.1 GHz), 8 GB RAM, no GPU.
Run: `cargo run --release --example bench_full`

### Throughput

| Benchmark | Result | What it measures |
|-----------|--------|------------------|
| B01a observe | 644,521 obs/sec | Canonicalize + SHA-256 + graph persist |
| B01b pipeline | 9,116 cycles/sec | Full 15-stage macro\_step |

### Crystallization

| Benchmark | Result | What it measures |
|-----------|--------|------------------|
| B02 serialize | 54.7 µs/crystal | JSON round-trip (serialize + deserialize) |
| B03 evidence | 5.5 µs/verify | Content address + hash chain + gate check |
| B05 determinism | PASS (58 crystals) | Two identical runs produce identical crystal IDs |

### Topology

| Benchmark | Result | What it measures |
|-----------|--------|------------------|
| B06 laplacian | 14.5 ms | Eigendecomposition of 200-node graph |
| B07 fiedler | 12.7 ms | Fiedler vector extraction (k=2) |
| B08 kuramoto | 3.6 ms, 18 ticks, r=0.95 | Phase synchronization convergence |
| B09 navigator | 218 µs/step | TRITON golden-angle spiral exploration |

### Constraint + Memory

| Benchmark | Result | What it measures |
|-----------|--------|------------------|
| B10 constraint | 13 µs/component | Morphogenic pressure + mutation |
| B11 memory | 2.4 MB | Graph footprint for 5,000 entities |

### Crypto + Infrastructure

| Benchmark | Result | What it measures |
|-----------|--------|------------------|
| B12 capsule | 9.2 µs | AES-256-GCM seal + open round-trip |
| B13 registry | 0.16 µs | Content-addressed operator lookup |
| B14 swarm | 0.05 ms | 4-agent consensus (1 round) |
| B15 macro\_step | 2.7 ms | Single end-to-end tick (gate rejection) |

## Architecture

```
PSE (20 crates)
├── Observation Layer:  pse-graph, pse-scale (Kuramoto, multi-scale)
├── Analysis Layer:     pse-extract, pse-topology (Laplacian, Fiedler, Betti)
├── Validation Layer:   pse-cascade, pse-pmhd (8-gate falsification)
├── Crystallization:    pse-types, pse-evidence, pse-registry, pse-manifest
├── Exploration:        pse-navigator (TRITON golden-angle, SimplexMesh)
├── Coordination:       pse-swarm (multi-agent consensus)
├── Constraint:         pse-constraint (DoF analysis, pre-computation routing)
├── Infrastructure:     pse-store, pse-capsule, pse-scheduler, pse-replay
└── Interface:          pse-gateway, pse-cli
```

## Derived From

PSE is extracted from ISLS (Intelligent Semantic Ledger Substrate).
Constitutional governance: ADAMANT Protocol (Zenodo, CC BY 4.0).

## License

MIT
