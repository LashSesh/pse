//! PSE domain adapter for heartbeat and vital signs monitoring.
//!
//! Generates synthetic ECG-like signals to detect cardiac rhythm anomalies.
//!
//! **DISCLAIMER: FOR DEMONSTRATION PURPOSES ONLY. NOT CLINICALLY VALIDATED.
//! NOT A MEDICAL DEVICE. DO NOT USE FOR DIAGNOSTIC OR TREATMENT DECISIONS.**

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

/// Medical disclaimer printed with every output.
pub const MEDICAL_DISCLAIMER: &str =
    "DISCLAIMER: FOR DEMONSTRATION PURPOSES ONLY. NOT CLINICALLY VALIDATED. NOT A MEDICAL DEVICE.";

/// A single vital sign reading.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VitalReading {
    /// Patient identifier.
    pub patient_id: String,
    /// Type of vital signal.
    pub signal_type: VitalSignal,
    /// Measured value.
    pub value: f64,
    /// Sample rate in Hz.
    pub sample_rate_hz: u32,
}

/// Type of vital signal.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum VitalSignal {
    /// Electrocardiogram in millivolts.
    ECG,
    /// Blood oxygen saturation percentage.
    SpO2,
    /// Blood pressure in mmHg (systolic).
    BloodPressure,
    /// Heart rate in beats per minute.
    HeartRate,
    /// Respiration rate in breaths per minute.
    Respiration,
}

impl VitalReading {
    /// Validate: value must be finite, SpO2 in [0,100], HR > 0.
    pub fn is_valid(&self) -> bool {
        if self.value.is_nan() || self.value.is_infinite() { return false; }
        match self.signal_type {
            VitalSignal::SpO2 => (0.0..=100.0).contains(&self.value),
            VitalSignal::HeartRate => self.value >= 0.0 && self.value <= 300.0,
            VitalSignal::Respiration => self.value >= 0.0 && self.value <= 60.0,
            VitalSignal::BloodPressure => self.value >= 0.0 && self.value <= 300.0,
            VitalSignal::ECG => true, // ECG can be negative (mV)
        }
    }
}

/// Crystallized vital sign pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VitalPattern {
    /// Type of detected pattern.
    pub pattern_type: VitalPatternType,
    /// Patient identifier.
    pub patient_id: String,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Clinical note (demonstration only).
    pub clinical_note: String,
}

/// Classification of vital sign pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VitalPatternType {
    /// Normal sinus rhythm.
    NormalSinusRhythm,
    /// Irregular R-R intervals (Kuramoto desync).
    AtrialFibrillation,
    /// Heart rate above normal range.
    Tachycardia,
    /// Heart rate below normal range.
    Bradycardia,
    /// ST segment elevation (potential MI).
    STElevation,
    /// SpO2 + respiration correlation break.
    RespiratoryDistress,
}

/// PSE observation adapter for vital signs data.
pub struct VitalsAdapter {
    source: String,
}

impl VitalsAdapter {
    /// Create a new adapter for the given patient.
    pub fn new(patient_id: &str) -> Self {
        Self { source: format!("vitals:{}", patient_id) }
    }
}

impl ObservationAdapter for VitalsAdapter {
    fn source_id(&self) -> &str { &self.source }
    fn canonicalize(&self, raw: &[u8], context: &MeasurementContext) -> Result<Observation, ObserveError> {
        if let Ok(reading) = serde_json::from_slice::<VitalReading>(raw) {
            if !reading.is_valid() {
                return Err(ObserveError::Canonicalize("invalid vital reading".into()));
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

impl pse_core::DomainAdapter for VitalsAdapter {
    fn domain_name(&self) -> &str { "vitals" }
}

/// Generate synthetic ECG-like data for two patients.
///
/// Patient A: normal sinus rhythm (60 seconds at 250 Hz).
/// Patient B: normal for 40 seconds, then atrial fibrillation onset
/// (R-R interval becomes irregular).
///
/// Returns downsampled readings (1 per 0.1s = 10 Hz effective) for PSE ingestion.
pub fn generate_embedded_data(seed: u64, duration_sec: u32) -> Vec<VitalReading> {
    let mut readings = Vec::new();
    let mut rng = seed;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    let samples_per_sec = 10_u32; // downsampled for PSE
    let total_samples = duration_sec * samples_per_sec;

    for sample in 0..total_samples {
        let t = sample as f64 / samples_per_sec as f64;

        // Patient A: normal sinus rhythm, HR ~72 bpm
        let hr_a = 72.0 + next_rng(&mut rng) * 2.0;
        let ecg_a = (t * hr_a / 60.0 * std::f64::consts::TAU).sin() * 1.2
            + next_rng(&mut rng) * 0.05;
        readings.push(VitalReading {
            patient_id: "patient_A".into(),
            signal_type: VitalSignal::ECG,
            value: ecg_a,
            sample_rate_hz: samples_per_sec,
        });

        // Patient B: normal for first 40s, then afib
        let afib_onset = duration_sec as f64 * 0.67; // ~40s for 60s duration
        let is_afib = t >= afib_onset;
        let hr_b = if is_afib {
            72.0 + next_rng(&mut rng) * 40.0 // highly irregular
        } else {
            72.0 + next_rng(&mut rng) * 2.0
        };
        let ecg_b = (t * hr_b / 60.0 * std::f64::consts::TAU).sin() * 1.2
            + next_rng(&mut rng) * if is_afib { 0.3 } else { 0.05 };
        readings.push(VitalReading {
            patient_id: "patient_B".into(),
            signal_type: VitalSignal::ECG,
            value: ecg_b,
            sample_rate_hz: samples_per_sec,
        });
    }
    readings
}

/// Describe a crystal with medical disclaimer.
pub fn describe_crystal(crystal: &pse_types::SemanticCrystal, tick: u64) -> String {
    format!(
        "Vitals: pattern at sample {}, stability={:.4}, region={} vertices [{}]",
        tick, crystal.stability_score, crystal.region.len(), MEDICAL_DISCLAIMER,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test] fn test_vital_reading_roundtrip() {
        let r = VitalReading { patient_id: "A".into(), signal_type: VitalSignal::ECG,
            value: 1.2, sample_rate_hz: 250 };
        let json = serde_json::to_vec(&r).unwrap();
        let res: VitalReading = serde_json::from_slice(&json).unwrap();
        assert!((res.value - 1.2).abs() < 1e-10);
    }
    #[test] fn test_pattern_roundtrip() {
        let p = VitalPattern { pattern_type: VitalPatternType::AtrialFibrillation,
            patient_id: "B".into(), confidence: 0.88, clinical_note: "test".into() };
        let json = serde_json::to_string(&p).unwrap();
        let _: VitalPattern = serde_json::from_str(&json).unwrap();
    }
    #[test] fn test_validation() {
        let v = VitalReading { patient_id: "A".into(), signal_type: VitalSignal::SpO2,
            value: 98.0, sample_rate_hz: 1 };
        assert!(v.is_valid());
        assert!(!VitalReading { value: 105.0, ..v.clone() }.is_valid());
        assert!(!VitalReading { value: f64::NAN, ..v }.is_valid());
    }
    #[test] fn test_embedded_data() {
        let d = generate_embedded_data(42, 60);
        assert!(d.len() >= 600); // 60s * 10Hz * 2 patients
        assert!(d.iter().all(|r| r.is_valid()));
    }
    #[test] fn test_offline_pipeline() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = VitalsAdapter::new("patient_B");
        for r in generate_embedded_data(42, 30).iter().take(300) {
            let batch = vec![serde_json::to_vec(r).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        assert!(state.commit_index > 0);
    }
    #[test] fn test_disclaimer_present() {
        assert!(MEDICAL_DISCLAIMER.contains("NOT A MEDICAL DEVICE"));
    }
}
