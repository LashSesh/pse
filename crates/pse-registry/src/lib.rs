//! Digest-bound registry infrastructure for PSE (C12).
//!
//! Provides content-addressed catalogs for operators, profiles, obligations,
//! and macros, with integrity verification via SHA-256 digests.

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use pse_types::{content_address, Hash256};

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("entry already exists: {0}")]
    AlreadyExists(String),
    #[error("entry not found: {0}")]
    NotFound(String),
    #[error("operator drift: digest mismatch for {0}")]
    OperatorDrift(String),
}

pub type Result<T> = std::result::Result<T, RegistryError>;

// ─── Registry Kind ────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegistryKind {
    Operator,
    Profile,
    Obligation,
    Macro,
}

impl std::fmt::Display for RegistryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryKind::Operator => write!(f, "operators"),
            RegistryKind::Profile => write!(f, "profiles"),
            RegistryKind::Obligation => write!(f, "obligations"),
            RegistryKind::Macro => write!(f, "macros"),
        }
    }
}

// ─── Registry Entry ───────────────────────────────────────────────────────────

/// Core fields used for content-addressing (excludes id itself)
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RegistryEntryCore {
    pub name: String,
    pub version: String,
    pub digest: Hash256,
    pub kind: RegistryKind,
    pub metadata: BTreeMap<String, String>,
}

/// A single registry entry (PSE Extension Def 1)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryEntry {
    /// Content address: SHA-256(JCS(core fields))
    pub id: Hash256,
    pub name: String,
    pub version: String,
    /// SHA-256 of the canonical serialization of the entry's operational content
    pub digest: Hash256,
    pub kind: RegistryKind,
    pub metadata: BTreeMap<String, String>,
}

impl RegistryEntry {
    pub fn new(
        name: String,
        version: String,
        digest: Hash256,
        kind: RegistryKind,
        metadata: BTreeMap<String, String>,
    ) -> Self {
        let core = RegistryEntryCore {
            name: name.clone(),
            version: version.clone(),
            digest,
            kind: kind.clone(),
            metadata: metadata.clone(),
        };
        let id = content_address(&core);
        Self { id, name, version, digest, kind, metadata }
    }
}

// ─── Registry ────────────────────────────────────────────────────────────────

/// Content-addressed, append-only collection of registry entries (PSE Extension Def 2)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registry {
    pub registry_id: Hash256,
    pub kind: RegistryKind,
    /// name -> entry (BTreeMap for deterministic ordering)
    pub entries: BTreeMap<String, RegistryEntry>,
    pub digest: Hash256,
}

impl Registry {
    pub fn new(kind: RegistryKind) -> Self {
        let entries: BTreeMap<String, RegistryEntry> = BTreeMap::new();
        let digest = content_address(&entries);
        let registry_id = digest;
        Self { registry_id, kind, entries, digest }
    }

    /// Register a new entry (append-only: returns error if name already exists)
    pub fn register(&mut self, entry: RegistryEntry) -> Result<()> {
        if self.entries.contains_key(&entry.name) {
            return Err(RegistryError::AlreadyExists(entry.name.clone()));
        }
        self.entries.insert(entry.name.clone(), entry);
        self.recompute_digest();
        Ok(())
    }

    /// Resolve an entry by name
    pub fn resolve(&self, name: &str) -> Option<&RegistryEntry> {
        self.entries.get(name)
    }

    /// Verify that a named entry matches the expected digest
    pub fn verify_digest(&self, name: &str, expected: &Hash256) -> bool {
        self.entries.get(name).is_some_and(|e| &e.digest == expected)
    }

    /// Recompute and return the registry digest
    pub fn compute_digest(&self) -> Hash256 {
        content_address(&self.entries)
    }

    fn recompute_digest(&mut self) {
        self.digest = self.compute_digest();
        self.registry_id = self.digest;
    }
}

// ─── Registry Set ────────────────────────────────────────────────────────────

/// Tuple of all four registries (PSE Extension Def 3)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistrySet {
    pub operators: Registry,
    pub profiles: Registry,
    pub obligations: Registry,
    pub macros: Registry,
}

impl RegistrySet {
    pub fn new() -> Self {
        Self {
            operators: Registry::new(RegistryKind::Operator),
            profiles: Registry::new(RegistryKind::Profile),
            obligations: Registry::new(RegistryKind::Obligation),
            macros: Registry::new(RegistryKind::Macro),
        }
    }

    /// Returns a BTreeMap of kind-name -> digest for RD binding
    pub fn digests(&self) -> BTreeMap<String, Hash256> {
        let mut m = BTreeMap::new();
        m.insert("operators".to_string(), self.operators.digest);
        m.insert("profiles".to_string(), self.profiles.digest);
        m.insert("obligations".to_string(), self.obligations.digest);
        m.insert("macros".to_string(), self.macros.digest);
        m
    }

    /// Verify an operator by name and expected implementation digest
    pub fn verify_operator(&self, name: &str, digest: &Hash256) -> bool {
        self.operators.verify_digest(name, digest)
    }
}

impl Default for RegistrySet {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Operator Registration Helper ────────────────────────────────────────────

/// Extended operator registration fields
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorRegistration {
    pub name: String,
    pub version: String,
    pub type_signature: String,
    pub role: String,
    pub determinism_certificate: bool,
}

impl OperatorRegistration {
    pub fn to_entry(&self) -> RegistryEntry {
        let mut metadata = BTreeMap::new();
        metadata.insert("type_signature".to_string(), self.type_signature.clone());
        metadata.insert("role".to_string(), self.role.clone());
        metadata.insert(
            "determinism_certificate".to_string(),
            self.determinism_certificate.to_string(),
        );
        let digest = content_address(self);
        RegistryEntry::new(
            self.name.clone(),
            self.version.clone(),
            digest,
            RegistryKind::Operator,
            metadata,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(name: &str) -> RegistryEntry {
        RegistryEntry::new(
            name.to_string(),
            "1.0.0".to_string(),
            [0u8; 32],
            RegistryKind::Operator,
            BTreeMap::new(),
        )
    }

    // AT-R1: Registry content address
    #[test]
    fn at_r1_registry_content_address() {
        let entry = make_entry("BandOperator");
        // id should be SHA-256(JCS(core fields))
        let core = RegistryEntryCore {
            name: entry.name.clone(),
            version: entry.version.clone(),
            digest: entry.digest,
            kind: entry.kind.clone(),
            metadata: entry.metadata.clone(),
        };
        let expected_id = content_address(&core);
        assert_eq!(entry.id, expected_id);
    }

    // AT-R2: Drift detection
    #[test]
    fn at_r2_drift_detection() {
        let mut reg = Registry::new(RegistryKind::Operator);
        let entry = make_entry("BandOperator");
        reg.register(entry.clone()).unwrap();
        // Different digest should not match
        let bad_digest = [1u8; 32];
        assert!(!reg.verify_digest("BandOperator", &bad_digest));
        // Correct digest should match
        assert!(reg.verify_digest("BandOperator", &entry.digest));
    }

    // AT-R3: RD binding — unregistered operator cannot be resolved
    #[test]
    fn at_r3_rd_binding() {
        let reg = Registry::new(RegistryKind::Operator);
        assert!(reg.resolve("UnknownOperator").is_none());
    }

    // AT-R4: Append-only — cannot overwrite existing entry
    #[test]
    fn at_r4_append_only() {
        let mut reg = Registry::new(RegistryKind::Operator);
        reg.register(make_entry("BandOperator")).unwrap();
        let result = reg.register(make_entry("BandOperator"));
        assert!(matches!(result, Err(RegistryError::AlreadyExists(_))));
    }

    // AT-R5: Deterministic digest — order of registration doesn't affect digest
    #[test]
    fn at_r5_deterministic_digest() {
        let mut reg1 = Registry::new(RegistryKind::Operator);
        reg1.register(make_entry("Alpha")).unwrap();
        reg1.register(make_entry("Beta")).unwrap();

        let mut reg2 = Registry::new(RegistryKind::Operator);
        reg2.register(make_entry("Beta")).unwrap();
        reg2.register(make_entry("Alpha")).unwrap();

        assert_eq!(reg1.digest, reg2.digest);
    }
}
