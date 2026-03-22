//! PSE adapter for ML model monitoring and drift detection.
//!
//! Monitors inference events to detect input drift, confidence degradation,
//! latency anomalies, and accuracy drops.

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single model inference event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferenceEvent {
    /// Model identifier.
    pub model_id: String,
    /// Input feature values (numeric features flattened).
    pub input_features: HashMap<String, f64>,
    /// Model prediction (numeric).
    pub prediction: f64,
    /// Model confidence score in [0, 1].
    pub confidence: f64,
    /// Ground truth label if available.
    pub ground_truth: Option<f64>,
    /// Inference latency in milliseconds.
    pub latency_ms: f64,
}

impl InferenceEvent {
    /// Validate: confidence in [0,1], latency >= 0, no NaN in features.
    pub fn is_valid(&self) -> bool {
        if self.confidence.is_nan() || self.latency_ms.is_nan() { return false; }
        if !(0.0..=1.0).contains(&self.confidence) { return false; }
        if self.latency_ms < 0.0 { return false; }
        self.input_features.values().all(|v| v.is_finite())
    }
}

/// Crystallized model monitoring pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitorPattern {
    /// Type of detected issue.
    pub pattern_type: MonitorPatternType,
    /// Model identifier.
    pub model_id: String,
    /// Affected features.
    pub features_affected: Vec<String>,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Severity classification.
    pub severity: MonitorSeverity,
    /// Human-readable description.
    pub description: String,
    /// Recommended action.
    pub recommendation: String,
}

/// Classification of model monitoring issue.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MonitorPatternType {
    /// Feature distribution shift.
    InputDrift,
    /// Prediction distribution changed.
    OutputDistributionShift,
    /// Average confidence decreasing.
    ConfidenceDegradation,
    /// Inference getting slower.
    LatencyAnomaly,
    /// Accuracy declining (when ground truth available).
    AccuracyDrop,
    /// Feature relationships changed vs training.
    FeatureCorrelationBreak,
    /// NaN, inf, or out-of-range inputs appearing.
    DataQualityIssue,
}

/// Severity classification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MonitorSeverity {
    /// Operating normally.
    Nominal,
    /// Minor deviation, keep watching.
    Watch,
    /// Significant deviation.
    Warning,
    /// Action required.
    Alert,
}

/// PSE observation adapter for model inference events.
pub struct ModelMonAdapter {
    source: String,
}

impl ModelMonAdapter {
    /// Create a new adapter for the given model.
    pub fn new(model_id: &str) -> Self {
        Self { source: format!("modelmon:{}", model_id) }
    }
}

impl ObservationAdapter for ModelMonAdapter {
    fn source_id(&self) -> &str { &self.source }
    fn canonicalize(&self, raw: &[u8], context: &MeasurementContext) -> Result<Observation, ObserveError> {
        if let Ok(event) = serde_json::from_slice::<InferenceEvent>(raw) {
            if !event.is_valid() {
                return Err(ObserveError::Canonicalize("invalid inference event".into()));
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

impl pse_core::DomainAdapter for ModelMonAdapter {
    fn domain_name(&self) -> &str { "modelmon" }
}

/// Generate 1000 embedded inference events with drift patterns.
///
/// Events 1-700: stable (no drift).
/// Events 701-850: gradual input drift on "amount" (mean 100 -> 180).
/// Events 851-1000: confidence degradation (mean 0.92 -> 0.71).
pub fn generate_embedded_data(seed: u64) -> Vec<InferenceEvent> {
    let mut events = Vec::with_capacity(1000);
    let mut rng = seed;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    for i in 0..1000_u32 {
        let amount_mean = if i > 700 {
            100.0 + (i - 700) as f64 * 0.533 // drifts to ~180 by 850
        } else {
            100.0
        };
        let amount = amount_mean + next_rng(&mut rng) * 30.0;

        let hour = ((i as f64 * 0.3) % 24.0).floor();
        let merchant = (next_rng(&mut rng).abs() * 5.0).floor();

        let confidence_mean = if i > 850 {
            0.92 - (i - 850) as f64 * 0.0014 // degrades to ~0.71 by 1000
        } else {
            0.92
        };
        let confidence = (confidence_mean + next_rng(&mut rng) * 0.05).clamp(0.0, 1.0);

        let prediction = if next_rng(&mut rng) > 0.0 { 0.0 } else { 1.0 };
        let ground_truth = if i < 500 { Some(prediction) } else { None };
        let latency = 10.0 + next_rng(&mut rng).abs() * 5.0;

        let mut features = HashMap::new();
        features.insert("amount".into(), amount);
        features.insert("hour".into(), hour);
        features.insert("merchant_category".into(), merchant);

        events.push(InferenceEvent {
            model_id: "fraud_v3".into(),
            input_features: features,
            prediction, confidence, ground_truth,
            latency_ms: latency,
        });
    }
    events
}

/// Parse JSONL (JSON Lines) content into inference events.
pub fn parse_jsonl(content: &str) -> Result<Vec<InferenceEvent>, anyhow::Error> {
    let mut events = Vec::new();
    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() { continue; }
        match serde_json::from_str::<InferenceEvent>(line) {
            Ok(event) => {
                if event.is_valid() {
                    events.push(event);
                }
            }
            Err(e) => {
                eprintln!("Warning: skipping malformed line {}: {}", line_num + 1, e);
            }
        }
    }
    Ok(events)
}

/// Describe a crystal in model monitoring context.
pub fn describe_crystal(crystal: &pse_types::SemanticCrystal, event_idx: u64) -> String {
    format!("ModelMon: pattern at event {}, stability={:.4}, region={} vertices",
        event_idx, crystal.stability_score, crystal.region.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test] fn test_inference_event_roundtrip() {
        let mut features = HashMap::new();
        features.insert("amount".into(), 100.0);
        let e = InferenceEvent { model_id: "m1".into(), input_features: features,
            prediction: 0.0, confidence: 0.95, ground_truth: Some(0.0), latency_ms: 10.0 };
        let json = serde_json::to_vec(&e).unwrap();
        let r: InferenceEvent = serde_json::from_slice(&json).unwrap();
        assert!((r.confidence - 0.95).abs() < 1e-10);
    }
    #[test] fn test_monitor_pattern_roundtrip() {
        let p = MonitorPattern { pattern_type: MonitorPatternType::InputDrift,
            model_id: "m1".into(), features_affected: vec!["amount".into()],
            confidence: 0.88, severity: MonitorSeverity::Warning,
            description: "drift".into(), recommendation: "retrain".into() };
        let json = serde_json::to_string(&p).unwrap();
        let _: MonitorPattern = serde_json::from_str(&json).unwrap();
    }
    #[test] fn test_validation() {
        let mut f = HashMap::new();
        f.insert("a".into(), 1.0);
        let v = InferenceEvent { model_id: "m".into(), input_features: f,
            prediction: 0.0, confidence: 0.9, ground_truth: None, latency_ms: 5.0 };
        assert!(v.is_valid());
        assert!(!InferenceEvent { confidence: 1.5, ..v.clone() }.is_valid());
        assert!(!InferenceEvent { latency_ms: -1.0, ..v }.is_valid());
    }
    #[test] fn test_embedded_data() {
        let d = generate_embedded_data(42);
        assert_eq!(d.len(), 1000);
        assert!(d.iter().all(|e| e.is_valid()));
    }
    #[test] fn test_jsonl_parsing() {
        let jsonl = r#"{"model_id":"m","input_features":{"a":1.0},"prediction":0.0,"confidence":0.9,"ground_truth":null,"latency_ms":5.0}
{"model_id":"m","input_features":{"a":2.0},"prediction":1.0,"confidence":0.8,"ground_truth":null,"latency_ms":6.0}"#;
        let events = parse_jsonl(jsonl).unwrap();
        assert_eq!(events.len(), 2);
    }
    #[test] fn test_offline_pipeline() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = ModelMonAdapter::new("fraud_v3");
        for event in generate_embedded_data(42).iter().take(200) {
            let batch = vec![serde_json::to_vec(event).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        assert!(state.commit_index > 0);
    }
}
