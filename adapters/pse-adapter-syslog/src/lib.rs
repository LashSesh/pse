//! PSE domain adapter for syslog anomaly detection.
//!
//! Processes server log entries to detect security and performance anomalies
//! including DDoS onset, brute force attempts, and service degradation.

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

/// A single server log entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    /// Hostname of the server.
    pub host: String,
    /// Service name.
    pub service: String,
    /// Log severity level.
    pub level: LogLevel,
    /// HTTP status code if applicable.
    pub status_code: Option<u16>,
    /// Response time in milliseconds if applicable.
    pub response_time_ms: Option<f64>,
    /// Log message text.
    pub message: String,
    /// Response body size in bytes if applicable.
    pub bytes: Option<u64>,
}

/// Log severity level.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    /// Debug-level message.
    Debug,
    /// Informational message.
    Info,
    /// Warning condition.
    Warn,
    /// Error condition.
    Error,
    /// Fatal/critical error.
    Fatal,
}

impl LogEntry {
    /// Validate: response time non-negative, status code in range, host non-empty.
    pub fn is_valid(&self) -> bool {
        if let Some(rt) = self.response_time_ms {
            if rt < 0.0 || rt.is_nan() || rt.is_infinite() { return false; }
        }
        if let Some(code) = self.status_code {
            if !(100..=599).contains(&code) { return false; }
        }
        !self.host.is_empty() && !self.service.is_empty()
    }
}

/// Crystallized security/performance pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityPattern {
    /// Type of detected anomaly.
    pub pattern_type: SecurityPatternType,
    /// Affected hosts.
    pub hosts: Vec<String>,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Severity classification.
    pub severity: SecuritySeverity,
    /// Human-readable description.
    pub description: String,
}

/// Classification of security/performance pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SecurityPatternType {
    /// Request rate spike + latency spike synchronized across hosts.
    DDoSOnset,
    /// Cluster of 401/403 responses.
    BruteForceAttempt,
    /// Response time gradually increasing.
    MemoryLeak,
    /// Error rate jumps after deployment.
    DeploymentRegression,
    /// P99 latency creeping up.
    ServiceDegradation,
    /// Anomalous traffic pattern.
    AnomalousTrafficPattern,
}

/// Severity classification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SecuritySeverity {
    /// Low — informational only.
    Low,
    /// Medium — monitor closely.
    Medium,
    /// High — investigate soon.
    High,
    /// Critical — immediate action required.
    Critical,
}

/// PSE observation adapter for server log data.
pub struct SyslogAdapter {
    source: String,
}

impl SyslogAdapter {
    /// Create a new adapter for the given host.
    pub fn new(host: &str) -> Self {
        Self { source: format!("syslog:{}", host) }
    }
}

impl ObservationAdapter for SyslogAdapter {
    fn source_id(&self) -> &str { &self.source }
    fn canonicalize(&self, raw: &[u8], context: &MeasurementContext) -> Result<Observation, ObserveError> {
        if let Ok(entry) = serde_json::from_slice::<LogEntry>(raw) {
            if !entry.is_valid() {
                return Err(ObserveError::Canonicalize("invalid log entry".into()));
            }
        }
        let payload = raw.to_vec();
        let digest: Hash256 = content_address_raw(&payload);
        Ok(Observation {
            timestamp: 0.0, source_id: self.source.clone(),
            provenance: ProvenanceEnvelope { origin: self.source.clone(), chain: Vec::new(), sig: None },
            payload, context: context.clone(), digest, schema_version: "1.0.0".into(),
        })
    }
}

impl pse_core::DomainAdapter for SyslogAdapter {
    fn domain_name(&self) -> &str { "syslog" }
}

/// Generate 10,000 synthetic log entries across 3 hosts.
///
/// DDoS onset at entry 7,000: request rate 10x, response time 5x, error rate 20x.
pub fn generate_embedded_data(seed: u64) -> Vec<LogEntry> {
    let mut entries = Vec::with_capacity(10000);
    let mut rng = seed;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *r as f64 / u64::MAX as f64
    };
    let hosts = ["web-01", "web-02", "web-03"];
    let services = ["nginx", "api-gateway", "auth-svc"];

    for i in 0..10000_u32 {
        let host_idx = (i as usize) % 3;
        let is_ddos = i >= 7000;
        let response_time = if is_ddos {
            50.0 + next_rng(&mut rng) * 500.0
        } else {
            10.0 + next_rng(&mut rng) * 90.0
        };
        let error_roll = next_rng(&mut rng);
        let (status, level) = if is_ddos && error_roll < 0.20 {
            (503_u16, LogLevel::Error)
        } else if !is_ddos && error_roll < 0.01 {
            (500, LogLevel::Error)
        } else {
            (200, LogLevel::Info)
        };
        let msg = if status >= 500 {
            format!("Error {}", status)
        } else {
            format!("GET /api/resource/{}", (next_rng(&mut rng) * 1000.0) as u32)
        };
        entries.push(LogEntry {
            host: hosts[host_idx].into(), service: services[host_idx].into(),
            level, status_code: Some(status), response_time_ms: Some(response_time),
            message: msg, bytes: Some((next_rng(&mut rng) * 10000.0) as u64 + 100),
        });
    }
    entries
}

/// Describe a crystal in syslog context.
pub fn describe_crystal(crystal: &pse_types::SemanticCrystal, entry: u64) -> String {
    format!("Syslog: pattern at entry {}, stability={:.4}, region={} vertices",
        entry, crystal.stability_score, crystal.region.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test] fn test_log_entry_roundtrip() {
        let e = LogEntry { host: "w1".into(), service: "nginx".into(), level: LogLevel::Info,
            status_code: Some(200), response_time_ms: Some(12.5), message: "OK".into(), bytes: Some(1024) };
        let json = serde_json::to_vec(&e).unwrap();
        let r: LogEntry = serde_json::from_slice(&json).unwrap();
        assert_eq!(r.host, "w1");
    }
    #[test] fn test_pattern_roundtrip() {
        let p = SecurityPattern { pattern_type: SecurityPatternType::DDoSOnset,
            hosts: vec!["w1".into()], confidence: 0.95, severity: SecuritySeverity::Critical,
            description: "test".into() };
        let json = serde_json::to_string(&p).unwrap();
        let _: SecurityPattern = serde_json::from_str(&json).unwrap();
    }
    #[test] fn test_validation() {
        let v = LogEntry { host: "h".into(), service: "s".into(), level: LogLevel::Info,
            status_code: Some(200), response_time_ms: Some(10.0), message: "ok".into(), bytes: None };
        assert!(v.is_valid());
        assert!(!LogEntry { host: "".into(), ..v.clone() }.is_valid());
        assert!(!LogEntry { response_time_ms: Some(-1.0), ..v }.is_valid());
    }
    #[test] fn test_embedded_data() {
        let d = generate_embedded_data(42);
        assert_eq!(d.len(), 10000);
        assert!(d.iter().all(|e| e.is_valid()));
    }
    #[test] fn test_offline_pipeline() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = SyslogAdapter::new("web-01");
        for entry in generate_embedded_data(42).iter().take(500) {
            let batch = vec![serde_json::to_vec(entry).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        assert!(state.commit_index > 0);
    }
}
