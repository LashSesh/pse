//! PSE domain adapter for IoT predictive maintenance sensor data.
//!
//! Generates and processes industrial sensor readings (vibration, temperature,
//! pressure, current, RPM, oil viscosity) to detect equipment degradation patterns.

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

// ─── Domain Types ────────────────────────────────────────────────────────────

/// A single sensor reading from an industrial machine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SensorReading {
    /// Machine identifier, e.g. "machine_003".
    pub machine_id: String,
    /// Type of sensor measurement.
    pub sensor_type: SensorType,
    /// Measured value.
    pub value: f64,
    /// Unit of measurement.
    pub unit: String,
}

/// Type of industrial sensor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SensorType {
    /// Vibration in mm/s RMS.
    Vibration,
    /// Temperature in degrees Celsius.
    Temperature,
    /// Pressure in bar.
    Pressure,
    /// Electrical current in Amperes.
    Current,
    /// Rotational speed in revolutions per minute.
    RPM,
    /// Oil viscosity in centistokes.
    OilViscosity,
}

impl SensorReading {
    /// Validate sensor reading: value must be finite and non-negative for most types.
    pub fn is_valid(&self) -> bool {
        if self.value.is_nan() || self.value.is_infinite() {
            return false;
        }
        match self.sensor_type {
            SensorType::Vibration => self.value >= 0.0,
            SensorType::Temperature => self.value > -273.15,
            SensorType::Pressure => self.value >= 0.0,
            SensorType::Current => self.value >= 0.0,
            SensorType::RPM => self.value >= 0.0,
            SensorType::OilViscosity => self.value > 0.0,
        }
    }
}

/// Crystallized maintenance pattern detected by PSE.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MaintenancePattern {
    /// Type of detected degradation or fault.
    pub pattern_type: MaintenancePatternType,
    /// Affected machine identifier.
    pub machine_id: String,
    /// Sensors involved in the pattern.
    pub sensors_involved: Vec<SensorType>,
    /// Urgency classification.
    pub urgency: Urgency,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Human-readable description.
    pub description: String,
    /// Estimated remaining useful life in hours, if calculable.
    pub estimated_rul_hours: Option<f64>,
}

/// Classification of maintenance pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MaintenancePatternType {
    /// Vibration + temperature rise indicating bearing wear.
    BearingDegradation,
    /// Pressure drop + oil viscosity change.
    SealLeak,
    /// Current anomaly + RPM fluctuation.
    ElectricalFault,
    /// Gradual degradation within specification limits.
    NormalWear,
    /// Multiple sensor anomalies converging — immediate attention needed.
    ImmediateFailureRisk,
}

/// Urgency classification for maintenance actions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Urgency {
    /// Normal operating condition.
    Routine,
    /// Schedule maintenance within next planned window.
    Planned,
    /// Schedule maintenance as soon as possible.
    Urgent,
    /// Stop equipment immediately.
    Emergency,
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for industrial IoT sensor data.
pub struct IoTAdapter {
    source: String,
}

impl IoTAdapter {
    /// Create a new adapter for the given machine.
    pub fn new(machine_id: &str) -> Self {
        Self {
            source: format!("iot:{}", machine_id),
        }
    }
}

impl ObservationAdapter for IoTAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        if let Ok(reading) = serde_json::from_slice::<SensorReading>(raw) {
            if !reading.is_valid() {
                return Err(ObserveError::Canonicalize(
                    "invalid sensor reading".into(),
                ));
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

impl pse_core::DomainAdapter for IoTAdapter {
    fn domain_name(&self) -> &str {
        "iot"
    }
}

// ─── Embedded Data Generator ─────────────────────────────────────────────────

/// Generate embedded sensor data for 5 machines over 1000 ticks.
///
/// Machine 3 has an embedded bearing degradation pattern:
///
/// - Vibration gradually increases from tick 400 (0.8 to 4.2 mm/s)
/// - Temperature increases from tick 500 (45C to 78C)
/// - At tick 800: both cross threshold simultaneously
///
/// Other machines show normal operating noise.
pub fn generate_embedded_data(seed: u64) -> Vec<SensorReading> {
    let mut readings = Vec::with_capacity(10000);
    let mut rng = seed;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    for tick in 0..1000_u32 {
        for machine in 0..5_u32 {
            let machine_id = format!("machine_{:03}", machine);

            let vib_base = if machine == 3 && tick >= 400 {
                0.8 + (tick - 400) as f64 * 0.005667
            } else {
                0.8
            };
            let vib = (vib_base + next_rng(&mut rng) * 0.15).max(0.0);
            readings.push(SensorReading {
                machine_id: machine_id.clone(),
                sensor_type: SensorType::Vibration,
                value: vib,
                unit: "mm/s".into(),
            });

            let temp_base = if machine == 3 && tick >= 500 {
                45.0 + (tick - 500) as f64 * 0.066
            } else {
                45.0
            };
            let temp = temp_base + next_rng(&mut rng) * 2.0;
            readings.push(SensorReading {
                machine_id,
                sensor_type: SensorType::Temperature,
                value: temp,
                unit: "C".into(),
            });
        }
    }
    readings
}

/// Describe a crystal in human-readable form for IoT context.
pub fn describe_crystal(crystal: &pse_types::SemanticCrystal, tick: u64) -> String {
    format!(
        "IoT: pattern at tick {}, stability={:.4}, region={} vertices",
        tick, crystal.stability_score, crystal.region.len(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test]
    fn test_sensor_reading_roundtrip() {
        let r = SensorReading {
            machine_id: "m3".into(), sensor_type: SensorType::Vibration,
            value: 2.5, unit: "mm/s".into(),
        };
        let json = serde_json::to_vec(&r).unwrap();
        let restored: SensorReading = serde_json::from_slice(&json).unwrap();
        assert!((restored.value - 2.5).abs() < 1e-10);
    }

    #[test]
    fn test_maintenance_pattern_roundtrip() {
        let p = MaintenancePattern {
            pattern_type: MaintenancePatternType::BearingDegradation,
            machine_id: "m3".into(),
            sensors_involved: vec![SensorType::Vibration],
            urgency: Urgency::Urgent, confidence: 0.87,
            description: "test".into(), estimated_rul_hours: Some(120.0),
        };
        let json = serde_json::to_string(&p).unwrap();
        let _: MaintenancePattern = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn test_validation() {
        let valid = SensorReading {
            machine_id: "m1".into(), sensor_type: SensorType::Vibration,
            value: 1.0, unit: "mm/s".into(),
        };
        assert!(valid.is_valid());
        assert!(!SensorReading { value: -1.0, ..valid.clone() }.is_valid());
        assert!(!SensorReading { value: f64::NAN, ..valid }.is_valid());
    }

    #[test]
    fn test_embedded_data_exists() {
        let data = generate_embedded_data(42);
        assert!(data.len() >= 1000);
        assert!(data.iter().all(|r| r.is_valid()));
    }

    #[test]
    fn test_offline_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = IoTAdapter::new("machine_003");
        let data = generate_embedded_data(42);
        for reading in data.iter().take(500) {
            let batch = vec![serde_json::to_vec(reading).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        assert!(state.commit_index > 0);
    }

    #[test]
    fn test_adapter_rejects_invalid() {
        let adapter = IoTAdapter::new("m1");
        let reading = SensorReading {
            machine_id: "m1".into(), sensor_type: SensorType::Vibration,
            value: -5.0, unit: "mm/s".into(),
        };
        let raw = serde_json::to_vec(&reading).unwrap();
        let ctx = MeasurementContext::default();
        assert!(adapter.canonicalize(&raw, &ctx).is_err());
    }
}
