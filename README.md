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
use pse_core::{Engine, DomainAdapter};
use pse_types::{Observation, SemanticCrystal};

// Implement your domain adapter
struct MyAdapter;

impl DomainAdapter for MyAdapter {
    fn domain_name(&self) -> &str { "financial" }
}

// Create engine and run
let config = pse_types::Config::default();
let mut state = pse_core::GlobalState::new(&config);
```

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
