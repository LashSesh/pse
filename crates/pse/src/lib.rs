//! # PSE — Post-Symbolic Engine
//!
//! A universal computation engine that processes information through
//! topology, physics, and geometry rather than through symbols or statistics.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use pse::prelude::*;
//!
//! // Implement ObservationAdapter for your domain, then:
//! // let config = Config::default();
//! // let mut state = GlobalState::new(&config);
//! // let adapter = PassthroughAdapter::new("my-domain");
//! // let result = macro_step(&mut state, &observations, &config, &adapter);
//! ```

/// Core types: SemanticCrystal, Observation, VertexId, Hash256, EvidenceChain, Config.
pub use pse_types as types;
/// Engine orchestrator: GlobalState, macro_step, DomainAdapter trait.
pub use pse_core as core;
/// Observation graph: ObservationAdapter, PassthroughAdapter, PersistentGraph.
pub use pse_graph as graph;
/// Pattern extraction: inverse_weave, operator library.
pub use pse_extract as extract;
/// Adversarial validation cascade: MetricSet, dual_consensus, PoRFsm.
pub use pse_cascade as cascade;
/// Evidence chain construction and crystal verification.
pub use pse_evidence as evidence;
/// Digest-bound operator registry.
pub use pse_registry as registry;
/// Topological analysis: Laplacian, Fiedler, Betti, Kuramoto, CTQW, DTL.
pub use pse_topology as topology;
/// Multi-scale observation: micro/meso/macro universes, Kuramoto synchronization.
pub use pse_scale as scale;
/// TRITON navigator: golden-angle spiral, spectral-guided exploration.
pub use pse_navigator as navigator;
/// Constraint propagation: morphogenic mutations, DoF analysis.
pub use pse_constraint as constraint;
/// Multi-agent swarm coordination with deterministic consensus.
pub use pse_swarm as swarm;
/// SQLite persistence layer.
pub use pse_store as store;
/// AES-256-GCM capsule encryption with policy-gated seal/open.
pub use pse_capsule as capsule;
/// Tick-based adaptive scheduling.
pub use pse_scheduler as scheduler;
/// Deterministic replay and verification.
pub use pse_replay as replay;
/// Execution manifest construction and verification.
pub use pse_manifest as manifest;
/// Polycentric multi-hypothesis drill engine.
pub use pse_pmhd as pmhd;

/// Prelude — import everything you need for a typical PSE application.
pub mod prelude {
    pub use pse_core::*;
    pub use pse_types::*;
}
