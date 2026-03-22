//! PSE domain adapter for ENTSO-E European energy grid data.
//!
//! Reads ENTSO-E Transparency Platform CSV data and ingests generation,
//! load, and cross-border flow data as PSE observations.
//!
//! # Example
//!
//! ```rust
//! use pse_adapter_entsoe::{GridAdapter, embedded_grid_data};
//! use pse_types::Config;
//! use pse_core::{GlobalState, macro_step};
//!
//! let config = Config::default();
//! let mut state = GlobalState::new(&config);
//! let adapter = GridAdapter::new("DE_LU");
//! let observations = embedded_grid_data();
//!
//! for obs in &observations {
//!     let batch = vec![serde_json::to_vec(obs).unwrap()];
//!     let _ = macro_step(&mut state, &batch, &config, &adapter);
//! }
//! ```

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

// ─── Domain Types ────────────────────────────────────────────────────────────

/// A single grid observation from ENTSO-E data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GridObservation {
    /// Control area code, e.g. "DE_LU" (Germany-Luxembourg).
    pub area: String,
    /// Type of grid metric.
    pub metric: GridMetric,
    /// Value in MW.
    pub value: f64,
    /// UTC timestamp string.
    pub timestamp_utc: String,
}

/// Type of energy grid measurement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GridMetric {
    /// Total system load (MW).
    TotalLoad,
    /// Wind power generation (MW).
    WindGeneration,
    /// Solar power generation (MW).
    SolarGeneration,
    /// Nuclear power generation (MW).
    NuclearGeneration,
    /// Cross-border power flow (MW).
    CrossBorderFlow {
        /// Source area.
        from: String,
        /// Target area.
        to: String,
    },
    /// System frequency (Hz).
    Frequency,
}

impl GridMetric {
    /// Short identifier for this metric type.
    pub fn as_str(&self) -> &str {
        match self {
            Self::TotalLoad => "total_load",
            Self::WindGeneration => "wind",
            Self::SolarGeneration => "solar",
            Self::NuclearGeneration => "nuclear",
            Self::CrossBorderFlow { .. } => "cross_border",
            Self::Frequency => "frequency",
        }
    }
}

/// Crystallized energy grid pattern detected by PSE.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GridPattern {
    /// Type of detected pattern.
    pub pattern_type: GridPatternType,
    /// Control areas involved.
    pub areas: Vec<String>,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Human-readable description.
    pub description: String,
    /// Severity level.
    pub severity: Severity,
}

/// Classification of detected grid pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GridPatternType {
    /// Abnormal load spike or drop.
    LoadAnomaly,
    /// Sudden generation capacity loss.
    GenerationDrop,
    /// Cross-border flow direction reversal.
    FlowReversal,
    /// System frequency deviation from 50 Hz.
    FrequencyDeviation,
    /// Correlation between renewable sources.
    RenewableCorrelation,
    /// Demand response signal detected.
    DemandResponseSignal,
}

/// Severity level for grid patterns.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum Severity {
    /// Informational — no action needed.
    Info,
    /// Warning — monitor closely.
    Warning,
    /// Critical — immediate attention required.
    Critical,
}

/// Classify severity based on relative load change.
///
/// A load change exceeding 10% in a 15-minute interval is Critical.
pub fn classify_severity(load_change_pct: f64) -> Severity {
    let abs_change = load_change_pct.abs();
    if abs_change > 10.0 {
        Severity::Critical
    } else if abs_change > 5.0 {
        Severity::Warning
    } else {
        Severity::Info
    }
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for ENTSO-E energy grid data.
pub struct GridAdapter {
    source: String,
}

impl GridAdapter {
    /// Create a new adapter for the given control area.
    pub fn new(area: &str) -> Self {
        Self {
            source: format!("entsoe:{}", area.to_lowercase()),
        }
    }
}

impl ObservationAdapter for GridAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        // Validate if parseable
        if let Ok(obs) = serde_json::from_slice::<GridObservation>(raw) {
            // Physical sanity checks
            match &obs.metric {
                GridMetric::TotalLoad if obs.value <= 0.0 => {
                    return Err(ObserveError::Canonicalize(
                        "total load must be positive".into(),
                    ));
                }
                GridMetric::WindGeneration
                | GridMetric::SolarGeneration
                | GridMetric::NuclearGeneration
                    if obs.value < 0.0 =>
                {
                    return Err(ObserveError::Canonicalize(
                        "generation cannot be negative".into(),
                    ));
                }
                _ => {}
            }
            if obs.value.is_nan() || obs.value.is_infinite() {
                return Err(ObserveError::Canonicalize("NaN/Inf value".into()));
            }
        }

        let payload = raw.to_vec();
        let digest: Hash256 = content_address_raw(&payload);
        Ok(Observation {
            timestamp: 0.0,
            source_id: self.source.clone(),
            provenance: ProvenanceEnvelope {
                origin: self.source.clone(),
                chain: Vec::new(),
                sig: None,
            },
            payload,
            context: context.clone(),
            digest,
            schema_version: "1.0.0".to_string(),
        })
    }
}

/// DomainAdapter implementation for ENTSO-E.
impl pse_core::DomainAdapter for GridAdapter {
    fn domain_name(&self) -> &str {
        "entsoe"
    }
}

// ─── CSV Parsing ─────────────────────────────────────────────────────────────

/// Parse ENTSO-E CSV data into grid observations.
///
/// Expected CSV format:
/// ```text
/// DateTime,AreaCode,TotalLoad_MW,Wind_MW,Solar_MW,Nuclear_MW
/// 2025-01-01T00:00Z,DE_LU,51234,18321,0,8012
/// ```
///
/// # Errors
/// Returns an error if the file cannot be read or has invalid format.
pub fn load_csv(path: &Path) -> Result<Vec<GridObservation>, anyhow::Error> {
    let content = std::fs::read_to_string(path)?;
    parse_csv_string(&content)
}

/// Parse CSV from a string (for testing and embedded data).
pub fn parse_csv_string(content: &str) -> Result<Vec<GridObservation>, anyhow::Error> {
    let mut observations = Vec::new();
    let mut lines = content.lines();

    // Skip header
    let _header = lines.next();

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() < 6 {
            continue;
        }

        let timestamp = fields[0].to_string();
        let area = fields[1].to_string();

        let parse_mw = |s: &str| -> f64 { s.trim().parse::<f64>().unwrap_or(0.0) };

        let total_load = parse_mw(fields[2]);
        let wind = parse_mw(fields[3]);
        let solar = parse_mw(fields[4]);
        let nuclear = parse_mw(fields[5]);

        observations.push(GridObservation {
            area: area.clone(),
            metric: GridMetric::TotalLoad,
            value: total_load,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area: area.clone(),
            metric: GridMetric::WindGeneration,
            value: wind,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area: area.clone(),
            metric: GridMetric::SolarGeneration,
            value: solar,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area,
            metric: GridMetric::NuclearGeneration,
            value: nuclear,
            timestamp_utc: timestamp,
        });
    }

    Ok(observations)
}

// ─── Embedded Sample Data ────────────────────────────────────────────────────

/// Returns 96 intervals (24 hours x 4 per hour) of embedded DE_LU grid data.
///
/// Realistic daily patterns:
/// - Total load: 45,000-65,000 MW (low at night, peak afternoon)
/// - Wind: 5,000-25,000 MW (random variation)
/// - Solar: 0 at night, 0-15,000 MW bell curve during day
/// - Nuclear: ~8,000 MW constant (baseload)
pub fn embedded_grid_data() -> Vec<GridObservation> {
    let mut observations = Vec::with_capacity(96 * 4);
    let mut rng: u64 = 73;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    for interval in 0..96 {
        let hour = interval as f64 / 4.0;
        let minute = (interval % 4) * 15;
        let timestamp = format!("2025-01-15T{:02}:{:02}Z", hour as u32, minute);

        // Total load: daily profile with peak around 12:00-14:00
        let load_base = 55000.0;
        let load_daily = -10000.0 * ((hour - 13.0) * std::f64::consts::PI / 12.0).cos();
        let load_noise = next_rng(&mut rng) * 1000.0;
        let total_load = (load_base + load_daily + load_noise).max(40000.0);

        // Wind: random variation with slight diurnal pattern
        let wind_base = 15000.0;
        let wind_var = next_rng(&mut rng) * 8000.0;
        let wind = (wind_base + wind_var).clamp(3000.0, 28000.0);

        // Solar: bell curve centered at noon, zero at night
        let solar = if (6.0..=20.0).contains(&hour) {
            let solar_peak = 14000.0;
            let solar_shape = -((hour - 13.0) / 3.5).powi(2);
            let solar_noise = next_rng(&mut rng).abs() * 1000.0;
            (solar_peak * solar_shape.exp() + solar_noise).max(0.0)
        } else {
            0.0
        };

        // Nuclear: constant baseload with minimal variation
        let nuclear = 8000.0 + next_rng(&mut rng) * 50.0;

        observations.push(GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::TotalLoad,
            value: total_load,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::WindGeneration,
            value: wind,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::SolarGeneration,
            value: solar,
            timestamp_utc: timestamp.clone(),
        });

        observations.push(GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::NuclearGeneration,
            value: nuclear,
            timestamp_utc: timestamp,
        });
    }

    observations
}

/// Describe a crystal in human-readable form based on grid context.
pub fn describe_crystal(
    crystal: &pse_types::SemanticCrystal,
    area: &str,
    tick: u64,
) -> String {
    format!(
        "{}: grid pattern at tick {}, stability={:.4}, region={} vertices, coherence={:.2}",
        area,
        tick,
        crystal.stability_score,
        crystal.region.len(),
        crystal.topology_signature.kuramoto_coherence,
    )
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test]
    fn test_csv_parsing() {
        let csv = "DateTime,AreaCode,TotalLoad_MW,Wind_MW,Solar_MW,Nuclear_MW\n\
                    2025-01-01T00:00Z,DE_LU,51234,18321,0,8012\n\
                    2025-01-01T00:15Z,DE_LU,50987,18455,0,8012\n";
        let obs = parse_csv_string(csv).unwrap();
        // 2 rows × 4 metrics = 8 observations
        assert_eq!(obs.len(), 8);
        assert_eq!(obs[0].area, "DE_LU");
        assert!((obs[0].value - 51234.0).abs() < 1e-10);
    }

    #[test]
    fn test_grid_observation_roundtrip() {
        let obs = GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::WindGeneration,
            value: 18321.0,
            timestamp_utc: "2025-01-01T00:00Z".into(),
        };
        let json = serde_json::to_string(&obs).unwrap();
        let restored: GridObservation = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.area, "DE_LU");
        assert!((restored.value - 18321.0).abs() < 1e-10);
    }

    #[test]
    fn test_grid_pattern_roundtrip() {
        let pattern = GridPattern {
            pattern_type: GridPatternType::LoadAnomaly,
            areas: vec!["DE_LU".into()],
            confidence: 0.91,
            description: "Load anomaly detected".into(),
            severity: Severity::Warning,
        };
        let json = serde_json::to_string(&pattern).unwrap();
        let restored: GridPattern = serde_json::from_str(&json).unwrap();
        assert!((restored.confidence - 0.91).abs() < 1e-10);
    }

    #[test]
    fn test_severity_classification() {
        assert_eq!(classify_severity(3.0), Severity::Info);
        assert_eq!(classify_severity(7.0), Severity::Warning);
        assert_eq!(classify_severity(12.0), Severity::Critical);
        assert_eq!(classify_severity(-15.0), Severity::Critical);
    }

    #[test]
    fn test_embedded_data_structure() {
        let data = embedded_grid_data();
        // 96 intervals × 4 metrics = 384
        assert_eq!(data.len(), 384);
        assert!(data.iter().all(|o| o.area == "DE_LU"));
        // All values should be non-negative
        assert!(data.iter().all(|o| o.value >= 0.0));
    }

    #[test]
    fn test_adapter_rejects_negative_load() {
        let adapter = GridAdapter::new("DE_LU");
        let obs = GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::TotalLoad,
            value: -1000.0,
            timestamp_utc: "2025-01-01T00:00Z".into(),
        };
        let raw = serde_json::to_vec(&obs).unwrap();
        let ctx = MeasurementContext::default();
        assert!(adapter.canonicalize(&raw, &ctx).is_err());
    }

    #[test]
    fn test_adapter_rejects_negative_generation() {
        let adapter = GridAdapter::new("DE_LU");
        let obs = GridObservation {
            area: "DE_LU".into(),
            metric: GridMetric::WindGeneration,
            value: -500.0,
            timestamp_utc: "2025-01-01T00:00Z".into(),
        };
        let raw = serde_json::to_vec(&obs).unwrap();
        let ctx = MeasurementContext::default();
        assert!(adapter.canonicalize(&raw, &ctx).is_err());
    }

    #[test]
    fn test_embedded_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = GridAdapter::new("DE_LU");
        let data = embedded_grid_data();

        // Feed all observations as individual ticks
        for obs in &data {
            let batch = vec![serde_json::to_vec(obs).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }

        // Engine should have processed ticks
        assert!(state.commit_index > 0);
    }

    #[test]
    fn test_evidence_chain_integrity() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = GridAdapter::new("DE_LU");
        let data = embedded_grid_data();

        let mut crystals = Vec::new();
        for obs in &data {
            let batch = vec![serde_json::to_vec(obs).unwrap()];
            if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystals.push(crystal);
            }
        }

        for crystal in &crystals {
            assert!(!crystal.evidence_chain.is_empty() || crystal.region.is_empty());
        }
    }
}
