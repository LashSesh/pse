//! Execution manifest for PSE (C13).
//!
//! Produces a content-addressed meta-artifact that binds an entire run,
//! linking crystals, traces, registries, and evidence into a single verifiable envelope.

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use pse_types::{content_address, EvidenceEntry, GateSnapshot, Hash256, RunDescriptor};
use pse_evidence::Archive;
use pse_registry::RegistrySet;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("run_id mismatch: expected {expected}, got {actual}")]
    RunIdMismatch { expected: String, actual: String },
    #[error("rd_digest mismatch")]
    RdDigestMismatch,
    #[error("crystal digest {0} not found in archive")]
    CrystalNotFound(String),
    #[error("trace digest mismatch at index {0}")]
    TraceDigestMismatch(usize),
    #[error("registry digest mismatch for {0}")]
    RegistryDigestMismatch(String),
    #[error("evidence chain verification failed")]
    EvidenceChainFailed,
}

pub type Result<T> = std::result::Result<T, ManifestError>;

// ─── Trace Entry ─────────────────────────────────────────────────────────────

/// Per-macro-step trace record (PSE Extension Def T_k)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceEntry {
    pub tick: u64,
    pub input_digest: Hash256,
    pub state_digest: Hash256,
    pub crystal_id: Option<Hash256>,
    pub gate_snapshot: GateSnapshot,
    pub metrics_digest: Hash256,
}

// ─── Execution Manifest ───────────────────────────────────────────────────────

/// Core fields used for content-addressing run_id
#[derive(Serialize)]
struct ManifestCore<'a> {
    rd_digest: &'a Hash256,
    program_id: &'a str,
    input_digest: &'a Hash256,
    crystal_digests: &'a Vec<Hash256>,
    trace_digests: &'a Vec<Hash256>,
    registry_digests: &'a BTreeMap<String, Hash256>,
    mef_head: &'a Option<Hash256>,
}

/// Execution manifest binding an entire run (PSE Extension Def M)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionManifest {
    /// Content address of the manifest core fields
    pub run_id: Hash256,
    pub rd_digest: Hash256,
    /// "discovery" for analytical mode, crystal_id hex for execute mode
    pub program_id: String,
    pub input_digest: Hash256,
    pub crystal_digests: Vec<Hash256>,
    pub trace_digests: Vec<Hash256>,
    pub registry_digests: BTreeMap<String, Hash256>,
    pub mef_head: Option<Hash256>,
    pub timestamp: f64,
}

// ─── Replay Pack ─────────────────────────────────────────────────────────────

/// Self-contained bundle sufficient for offline replay (PSE Extension Def RP)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayPack {
    pub manifest: ExecutionManifest,
    pub rd: RunDescriptor,
    pub observation_log: Vec<Vec<Vec<u8>>>,
    pub registries: RegistrySet,
    pub boundary_evidence: Vec<EvidenceEntry>,
}

// ─── Builder ──────────────────────────────────────────────────────────────────

/// Construct an execution manifest post-run (Algorithm BuildManifest)
pub fn build_manifest(
    rd: &RunDescriptor,
    traces: &[TraceEntry],
    archive: &Archive,
    registries: &RegistrySet,
    program_id: &str,
    observation_log: &[Vec<Vec<u8>>],
) -> ExecutionManifest {
    let rd_digest = content_address(rd);
    let input_digest = content_address(&observation_log);
    let crystal_digests: Vec<Hash256> = archive.crystals().iter().map(|c| c.crystal_id).collect();
    let trace_digests: Vec<Hash256> = traces.iter().map(content_address).collect();
    let registry_digests = registries.digests();

    // mef_head: head of the evidence chain — last crystal's last evidence entry
    let mef_head = archive.crystals().last().and_then(|c| c.evidence_chain.last()).map(|e| e.digest);

    let core = ManifestCore {
        rd_digest: &rd_digest,
        program_id,
        input_digest: &input_digest,
        crystal_digests: &crystal_digests,
        trace_digests: &trace_digests,
        registry_digests: &registry_digests,
        mef_head: &mef_head,
    };
    let run_id = content_address(&core);

    let timestamp = chrono::Utc::now().timestamp_millis() as f64;

    ExecutionManifest {
        run_id,
        rd_digest,
        program_id: program_id.to_string(),
        input_digest,
        crystal_digests,
        trace_digests,
        registry_digests,
        mef_head,
        timestamp,
    }
}

/// Verify a manifest against provided run artifacts (MV1–MV6)
pub fn verify_manifest(
    manifest: &ExecutionManifest,
    rd: &RunDescriptor,
    archive: &Archive,
    traces: &[TraceEntry],
    registries: &RegistrySet,
) -> Result<()> {
    // MV1: run_id = SHA-256(JCS(core))
    let core = ManifestCore {
        rd_digest: &manifest.rd_digest,
        program_id: &manifest.program_id,
        input_digest: &manifest.input_digest,
        crystal_digests: &manifest.crystal_digests,
        trace_digests: &manifest.trace_digests,
        registry_digests: &manifest.registry_digests,
        mef_head: &manifest.mef_head,
    };
    let expected_run_id = content_address(&core);
    if expected_run_id != manifest.run_id {
        return Err(ManifestError::RunIdMismatch {
            expected: hex(&expected_run_id),
            actual: hex(&manifest.run_id),
        });
    }

    // MV2: rd_digest matches provided RD
    let actual_rd_digest = content_address(rd);
    if actual_rd_digest != manifest.rd_digest {
        return Err(ManifestError::RdDigestMismatch);
    }

    // MV3: every crystal digest resolves in the archive
    let archive_ids: std::collections::BTreeSet<Hash256> =
        archive.crystals().iter().map(|c| c.crystal_id).collect();
    for (i, cd) in manifest.crystal_digests.iter().enumerate() {
        if !archive_ids.contains(cd) {
            return Err(ManifestError::CrystalNotFound(format!("index {}: {}", i, hex(cd))));
        }
    }

    // MV4: every trace digest is recomputable
    for (i, td) in manifest.trace_digests.iter().enumerate() {
        if i >= traces.len() {
            return Err(ManifestError::TraceDigestMismatch(i));
        }
        let recomputed = content_address(&traces[i]);
        if recomputed != *td {
            return Err(ManifestError::TraceDigestMismatch(i));
        }
    }

    // MV5: registry digests match
    let actual_reg_digests = registries.digests();
    for (kind, expected_digest) in &manifest.registry_digests {
        match actual_reg_digests.get(kind) {
            Some(actual) if actual == expected_digest => {}
            _ => return Err(ManifestError::RegistryDigestMismatch(kind.clone())),
        }
    }

    // MV6: if mef_head present, verify evidence chain head
    if let Some(expected_head) = &manifest.mef_head {
        let actual_head = archive.crystals().last()
            .and_then(|c| c.evidence_chain.last())
            .map(|e| e.digest);
        match actual_head {
            Some(h) if &h == expected_head => {}
            _ => return Err(ManifestError::EvidenceChainFailed),
        }
    }

    Ok(())
}

/// Assemble a replay pack from manifest components
pub fn build_replay_pack(
    manifest: ExecutionManifest,
    rd: RunDescriptor,
    observation_log: Vec<Vec<Vec<u8>>>,
    registries: RegistrySet,
    boundary_evidence: Vec<EvidenceEntry>,
) -> ReplayPack {
    ReplayPack { manifest, rd, observation_log, registries, boundary_evidence }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn hex(h: &Hash256) -> String {
    h.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_types::{Config, GateSnapshot};
    use pse_evidence::Archive;
    use pse_registry::RegistrySet;

    fn make_rd() -> RunDescriptor {
        RunDescriptor {
            config: Config::default(),
            operator_versions: BTreeMap::new(),
            initial_state_digest: [0u8; 32],
            seed: None,
            registry_digests: BTreeMap::new(),
            scheduler: pse_types::SchedulerConfig::default(),
        }
    }

    fn make_trace(tick: u64) -> TraceEntry {
        TraceEntry {
            tick,
            input_digest: [0u8; 32],
            state_digest: [1u8; 32],
            crystal_id: None,
            gate_snapshot: GateSnapshot::default(),
            metrics_digest: [2u8; 32],
        }
    }

    // AT-M1: Manifest content address
    #[test]
    fn at_m1_manifest_content_address() {
        let rd = make_rd();
        let archive = Archive::new();
        let registries = RegistrySet::new();
        let traces = vec![make_trace(0), make_trace(1)];
        let obs_log: Vec<Vec<Vec<u8>>> = vec![];

        let manifest = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);

        // Recompute run_id from core
        let core = ManifestCore {
            rd_digest: &manifest.rd_digest,
            program_id: &manifest.program_id,
            input_digest: &manifest.input_digest,
            crystal_digests: &manifest.crystal_digests,
            trace_digests: &manifest.trace_digests,
            registry_digests: &manifest.registry_digests,
            mef_head: &manifest.mef_head,
        };
        let expected = content_address(&core);
        assert_eq!(manifest.run_id, expected);
    }

    // AT-M2: Manifest verification — all MV1–MV6 pass
    #[test]
    fn at_m2_manifest_verification() {
        let rd = make_rd();
        let archive = Archive::new();
        let registries = RegistrySet::new();
        let traces = vec![make_trace(0)];
        let obs_log: Vec<Vec<Vec<u8>>> = vec![];

        let manifest = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);
        let result = verify_manifest(&manifest, &rd, &archive, &traces, &registries);
        assert!(result.is_ok(), "{:?}", result);
    }

    // AT-M3: Tamper detection — alter a crystal digest -> MV3 fails
    #[test]
    fn at_m3_tamper_detection() {
        let rd = make_rd();
        let archive = Archive::new();
        let registries = RegistrySet::new();
        let traces: Vec<TraceEntry> = vec![];
        let obs_log: Vec<Vec<Vec<u8>>> = vec![];

        let mut manifest = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);
        // Inject a fake crystal digest
        manifest.crystal_digests.push([0xdeu8; 32]);
        // Recompute run_id to avoid MV1 failure
        let core = ManifestCore {
            rd_digest: &manifest.rd_digest,
            program_id: &manifest.program_id,
            input_digest: &manifest.input_digest,
            crystal_digests: &manifest.crystal_digests,
            trace_digests: &manifest.trace_digests,
            registry_digests: &manifest.registry_digests,
            mef_head: &manifest.mef_head,
        };
        manifest.run_id = content_address(&core);

        let result = verify_manifest(&manifest, &rd, &archive, &traces, &registries);
        assert!(matches!(result, Err(ManifestError::CrystalNotFound(_))));
    }

    // AT-M4: Replay pack sufficiency — build pack; verify all data enables identical replay
    #[test]
    fn at_m4_replay_pack_sufficiency() {
        use pse_types::EvidenceEntry;
        let rd = make_rd();
        let archive = Archive::new();
        let registries = RegistrySet::new();
        let traces = vec![make_trace(0), make_trace(1)];
        let obs_log: Vec<Vec<Vec<u8>>> = vec![vec![vec![1, 2, 3]]];
        let boundary_evidence: Vec<EvidenceEntry> = vec![];

        let manifest = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);
        let pack = build_replay_pack(
            manifest.clone(),
            rd.clone(),
            obs_log.clone(),
            registries.clone(),
            boundary_evidence,
        );

        // Pack must preserve the manifest exactly
        assert_eq!(pack.manifest.run_id, manifest.run_id, "pack must preserve run_id");

        // Rebuild manifest from pack contents → must produce identical outputs (replay determinism)
        let manifest2 = build_manifest(
            &pack.rd,
            &traces,
            &archive,
            &pack.registries,
            "discovery",
            &pack.observation_log,
        );
        assert_eq!(manifest.trace_digests, manifest2.trace_digests,
            "replay produced different trace digests");
        assert_eq!(manifest.crystal_digests, manifest2.crystal_digests,
            "replay produced different crystal digests");
        assert_eq!(manifest.registry_digests, manifest2.registry_digests,
            "replay produced different registry digests");
    }

    // AT-M5: Trace determinism — same RD produces same trace digests
    #[test]
    fn at_m5_trace_determinism() {
        let rd = make_rd();
        let archive = Archive::new();
        let registries = RegistrySet::new();
        let traces = vec![make_trace(0), make_trace(1)];
        let obs_log: Vec<Vec<Vec<u8>>> = vec![];

        let m1 = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);
        let m2 = build_manifest(&rd, &traces, &archive, &registries, "discovery", &obs_log);
        assert_eq!(m1.trace_digests, m2.trace_digests);
    }
}
