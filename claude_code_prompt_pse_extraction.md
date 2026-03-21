## Task: Extract Universal Post-Symbolic Computation Engine from ISLS

### Context

The ISLS repository (uploaded as zip) contains a 31-crate Rust workspace for software intelligence. Approximately 80% of its architecture is domain-agnostic — it processes information through topology, physics, and geometry rather than through symbols or statistics. The software-specific parts (code generation, compilation, Rust-specific tooling) are a domain plugin sitting on top of this universal core.

Your task: extract the universal core into a new, clean, standalone Rust workspace. The result is a **post-symbolic computation engine** — a library that any domain (finance, medicine, cybersecurity, climate science, manufacturing) can use by implementing a thin domain adapter.

### Name: `pse` (Post-Symbolic Engine)

Repository name: `pse`
Crate prefix: `pse-`

---

### PHASE 1: Identify and Copy Core Crates

Read the ISLS source. Map each crate to one of three categories:

#### Category A: UNIVERSAL CORE → Extract and rename `pse-*`

| ISLS Crate | PSE Crate | Purpose |
|---|---|---|
| `isls-types` | `pse-types` | Core types: Crystal, Observation, TemporalPrimitive, CrystalId, EvidenceChain. **Generalize**: remove any software-specific types. Make `Observation` generic over a payload type `T: Serialize + DeserializeOwned`. |
| `isls-graph` | `pse-graph` | Observation graph, entity tracking, correlation edges. Already generic. |
| `isls-extract` | `pse-extract` | Pattern extraction, invariance detection, Kairos conjunction. Already generic. |
| `isls-cascade` | `pse-cascade` | 8-gate adversarial validation cascade. Already generic. |
| `isls-crystal` | N/A | If crystallization logic is in `isls-types`, skip. Otherwise extract into `pse-types`. |
| `isls-replay` | `pse-replay` | Deterministic replay engine. Already generic. |
| `isls-evidence` | `pse-evidence` | Evidence chain construction, SHA-256 linking. Already generic. |
| `isls-registry` | `pse-registry` | Crystal registry, lookup, statistics. Already generic. |
| `isls-manifest` | `pse-manifest` | Manifest construction and verification. Already generic. |
| `isls-capsule` | `pse-capsule` | AES-256-GCM encryption, HKDF key derivation. Already generic. |
| `isls-scheduler` | `pse-scheduler` | Tick-based scheduling, macro-step orchestration. Already generic. |
| `isls-topology` | `pse-topology` | **CRITICAL**: Laplacian computation, spectral decomposition, Fiedler vector, Betti numbers, spectral gap. This is the topological computation core. |
| `isls-store` | `pse-store` | SQLite persistence, crystal storage. Already generic. |
| `isls-scale` | `pse-scale` | Multi-scale observation (micro/meso/macro), hypercube universes, bridges, ladders, Kuramoto clustering, Fiedler bisection. **CRITICAL**: this contains the Kuramoto synchronization logic. |
| `isls-pmhd` | `pse-pmhd` | Adversarial drill engine. Hypothesis testing, monolith generation. Already generic. |
| `isls-navigator` | `pse-navigator` | **CRITICAL**: TRITON golden-angle spiral, SimplexMesh triangulation, Laplacian spectral guidance, Betti stability guards, entropy control, singularity detection. |
| `isls-swarm` | `pse-swarm` | Multi-agent coordination, 4 consensus modes, PMHD-backed verification. Already generic. |
| `isls-constraint` | `pse-constraint` | **PARTIAL**: Extract the DoF analysis and constraint propagation logic. Remove any Rust/software-specific component classification (e.g., "CrudOperation", "RouteHandler"). Replace with a generic `ComponentClassifier` trait that domains implement. |

#### Category B: SOFTWARE-SPECIFIC → Do NOT extract

| ISLS Crate | Reason |
|---|---|
| `isls-forge` | Rust code skeleton generation |
| `isls-foundry` | cargo build / compile-test-fix loop |
| `isls-oracle` | LLM prompt construction (software-specific prompts) |
| `isls-templates` | Rust architecture templates |
| `isls-compose` | Code composition, recursive weaving |
| `isls-artifact-ir` | Code intermediate representation |
| `isls-babylon` | Multi-language code generation |
| `isls-agent` | No-code software operator |
| `isls-studio` | Web UI for software pipeline |

#### Category C: NEEDS NEW VERSION → Rebuild for PSE

| ISLS Crate | PSE Equivalent | What changes |
|---|---|---|
| `isls-gateway` | `pse-gateway` | New HTTP API exposing universal endpoints only: /health, /crystals, /observe, /navigate, /constitution, /benchmarks, /accumulation. No /forge, /agent, /studio. |
| `isls-cli` | `pse-cli` | New CLI: `pse init`, `pse observe`, `pse status`, `pse crystals`, `pse navigate`, `pse bench`. No `pse forge`, `pse agent`. |

---

### PHASE 2: Generalize Types

The key generalization: `Observation` must be generic.

In ISLS, an observation is a fixed struct. In PSE, it must work with ANY data:

```rust
// pse-types/src/observation.rs

/// A timestamped observation of any domain-specific payload.
/// 
/// The PSE engine is agnostic to what T contains.
/// Financial ticks, medical vitals, sensor readings, network packets —
/// all are valid observation payloads.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation<T: Clone + Serialize> {
    /// Monotonic tick number
    pub tick: u64,
    /// Entity identifier (what is being observed)
    pub entity_id: EntityId,
    /// Domain-specific payload
    pub payload: T,
    /// Timestamp (UTC)
    pub timestamp: chrono::DateTime<chrono::Utc>,
}
```

Similarly, `Crystal` should be generic over its content:

```rust
/// A Semantic Crystal — the fundamental unit of validated knowledge.
///
/// Content-addressed (SHA-256), evidence-chained, deterministically reproducible.
/// The identity IS the content: id = SHA-256(canonical(crystal)).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Crystal<T: Clone + Serialize> {
    pub id: CrystalId,
    pub content: T,
    pub evidence: EvidenceChain,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub resonance: f64,
    pub confidence: f64,
}
```

### PHASE 3: Define Domain Adapter Trait

Create `pse-core/src/adapter.rs`:

```rust
/// The trait that domain plugins implement to use the PSE engine.
///
/// A financial adapter defines what a "data point" is (tick),
/// what "validation" means (profitable backtest), and what a
/// "crystal" contains (trading rule).
///
/// A medical adapter defines what a "data point" is (vital sign),
/// what "validation" means (clinical significance test), and what a
/// "crystal" contains (diagnostic pattern).
pub trait DomainAdapter: Send + Sync + 'static {
    /// The raw observation payload type
    type Observation: Clone + Serialize + DeserializeOwned + Send + Sync;
    
    /// The crystal content type (what gets crystallized)
    type CrystalContent: Clone + Serialize + DeserializeOwned + Send + Sync;
    
    /// Convert a raw observation into the engine's internal format
    fn ingest(&self, raw: Self::Observation) -> EngineObservation;
    
    /// Define domain-specific validation gates (in addition to the universal 8 gates)
    fn domain_gates(&self) -> Vec<Box<dyn ValidationGate>>;
    
    /// Classify a component's degrees of freedom (for constraint propagation)
    fn classify_component(&self, component: &ComponentDescriptor) -> DegreesOfFreedom;
    
    /// Domain-specific crystal content extraction from validated patterns
    fn crystallize(&self, pattern: &ValidatedPattern) -> Self::CrystalContent;
    
    /// Human-readable description of a crystal (for UI/reporting)
    fn describe_crystal(&self, crystal: &Crystal<Self::CrystalContent>) -> String;
}
```

### PHASE 4: Create Workspace Structure

```
pse/
├── Cargo.toml              (workspace)
├── README.md
├── LICENSE                  (MIT)
├── .gitignore
├── crates/
│   ├── pse-types/           (generic types, Crystal<T>, Observation<T>)
│   ├── pse-core/            (DomainAdapter trait, Engine orchestrator)
│   ├── pse-graph/           (observation graph, entity tracking)
│   ├── pse-extract/         (pattern extraction, invariance detection)
│   ├── pse-cascade/         (8-gate adversarial validation)
│   ├── pse-replay/          (deterministic replay)
│   ├── pse-evidence/        (SHA-256 evidence chains)
│   ├── pse-registry/        (crystal registry)
│   ├── pse-manifest/        (manifest construction/verification)
│   ├── pse-capsule/         (AES-256-GCM encryption)
│   ├── pse-scheduler/       (tick scheduling, orchestration)
│   ├── pse-topology/        (Laplacian, spectral, Fiedler, Betti)
│   ├── pse-store/           (SQLite persistence)
│   ├── pse-scale/           (multi-scale, Kuramoto, clustering)
│   ├── pse-pmhd/            (adversarial drill)
│   ├── pse-navigator/       (TRITON, SimplexMesh, spectral guidance)
│   ├── pse-constraint/      (DoF analysis, generic constraint propagation)
│   ├── pse-swarm/           (multi-agent coordination)
│   ├── pse-gateway/         (HTTP API)
│   └── pse-cli/             (command line interface)
├── examples/
│   ├── financial_ticks.rs   (minimal example: observe price data, find invariances)
│   ├── sensor_stream.rs     (minimal example: IoT sensor anomaly detection)
│   └── synthetic.rs         (S-Basic scenario ported from ISLS)
└── tests/
    └── integration/
        └── smoke_test.rs    (init → ingest → tick → crystal → verify)
```

### PHASE 5: Port Tests

For every `AT-*` test in ISLS that tests a universal component, port it to PSE:

- AT-01–AT-20 (core) → `pse-core` tests
- AT-R1–R5 (registry) → `pse-registry` tests
- AT-M1–M5 (manifest) → `pse-manifest` tests
- AT-C1–C6 (capsule) → `pse-capsule` tests
- AT-S1–S5 (scheduler) → `pse-scheduler` tests
- AT-T1–T12 (topology) → `pse-topology` tests
- AT-D1–D8 (store) → `pse-store` tests
- AT-SC1–SC15 (scale) → `pse-scale` tests
- AT-P1–P8 (pmhd) → `pse-pmhd` tests
- AT-CP1–CP12 (constraint) → `pse-constraint` tests (generalized)
- AT-NV1–NV12 (navigator) → `pse-navigator` tests
- AT-SW1–SW25 (swarm) → `pse-swarm` tests

Do NOT port: AT-F (forge), AT-CO (compose), AT-O (oracle), AT-TM (templates), AT-FD (foundry), AT-ST (studio), AT-BB (babylon), AT-AG (agent), AT-IR (artifact-ir).

Expected test count after port: ~160-180 tests (all universal components).

### PHASE 6: Create README.md

```markdown
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
use pse_types::{Observation, Crystal};

// Implement your domain adapter
struct MyAdapter;

impl DomainAdapter for MyAdapter {
    type Observation = f64;  // e.g., price ticks
    type CrystalContent = String;  // e.g., discovered rule
    // ... implement required methods
}

// Create engine and observe
let engine = Engine::new(MyAdapter);
engine.observe(Observation::new(entity, 42.0));
engine.tick();  // process one macro-step
let crystals = engine.crystals();  // retrieve validated artifacts
```

## Architecture

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

## Derived From

PSE is extracted from ISLS (Intelligent Semantic Ledger Substrate).
Constitutional governance: ADAMANT Protocol (Zenodo, CC BY 4.0).

## License

MIT
```

### PHASE 7: Cargo.toml Workspace

```toml
[workspace]
resolver = "2"
members = [
    "crates/pse-types",
    "crates/pse-core",
    "crates/pse-graph",
    "crates/pse-extract",
    "crates/pse-cascade",
    "crates/pse-replay",
    "crates/pse-evidence",
    "crates/pse-registry",
    "crates/pse-manifest",
    "crates/pse-capsule",
    "crates/pse-scheduler",
    "crates/pse-topology",
    "crates/pse-store",
    "crates/pse-scale",
    "crates/pse-pmhd",
    "crates/pse-navigator",
    "crates/pse-constraint",
    "crates/pse-swarm",
    "crates/pse-gateway",
    "crates/pse-cli",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
authors = ["Sebastian Klemm"]
license = "MIT"
description = "Post-Symbolic Engine — universal computation through topology, physics, and geometry"

[workspace.dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
chrono = { version = "0.4", features = ["serde"] }
ordered-float = { version = "4", features = ["serde"] }
thiserror = "1"
rusqlite = { version = "0.31", features = ["bundled"] }
```

### Constraints

1. **No software-specific code.** If a type, function, or module references Rust compilation, cargo, file paths, code generation, LLM prompts for code, or anything software-engineering-specific — do NOT include it.

2. **All generics must compile.** Every `<T>` must have proper trait bounds. Test with `cargo build --workspace`.

3. **All ported tests must pass.** `cargo test --workspace` must show 0 failures.

4. **Zero clippy warnings.** `cargo clippy --workspace` must be clean.

5. **Every public item documented.** `///` doc comments on every pub struct, fn, enum, trait.

6. **The synthetic scenario must work.** Port S-Basic (50 entities, 100 ticks) as `examples/synthetic.rs`. It must produce at least 1 crystal when run with `cargo run --example synthetic`.

7. **No external LLM dependency.** The core engine must function completely offline. No API keys, no HTTP calls to LLM providers. The engine observes, analyzes, validates, and crystallizes using only local computation.

### Execution Order

1. Read the ISLS zip, understand the crate dependency graph
2. Create the workspace structure
3. Copy and rename Category A crates, generalizing types
4. Create `pse-core` with the `DomainAdapter` trait and `Engine` orchestrator
5. Rebuild Category C crates (gateway, cli)
6. Port tests
7. Create examples
8. `cargo build --workspace` → 0 errors
9. `cargo test --workspace` → 0 failures
10. `cargo clippy --workspace` → 0 warnings
11. Write README.md
12. Create LICENSE (MIT)
13. Final verification: `cargo run --example synthetic` produces crystals
