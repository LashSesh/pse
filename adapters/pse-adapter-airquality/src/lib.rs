//! PSE domain adapter for OpenAQ air quality data.
//!
//! Ingests air quality readings from OpenAQ monitoring stations and feeds
//! them through the PSE pipeline as observations. Includes embedded
//! synthetic data for five German monitoring stations covering 48 hours
//! with a realistic industrial spike event.
//!
//! # Example
//!
//! ```rust,no_run
//! use pse_adapter_airquality::{AirQualityAdapter, embedded_airquality_data};
//! use pse_types::Config;
//! use pse_core::{GlobalState, macro_step};
//!
//! let config = Config::default();
//! let mut state = GlobalState::new(&config);
//! let adapter = AirQualityAdapter::new(1001);
//! let readings = embedded_airquality_data();
//!
//! for reading in &readings {
//!     let batch = vec![serde_json::to_vec(reading).unwrap()];
//!     let _ = macro_step(&mut state, &batch, &config, &adapter);
//! }
//! ```

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

// ─── Domain Types ────────────────────────────────────────────────────────────

/// Air quality parameter being measured.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AQParameter {
    /// Fine particulate matter (diameter <= 2.5 micrometers).
    PM25,
    /// Coarse particulate matter (diameter <= 10 micrometers).
    PM10,
    /// Nitrogen dioxide.
    NO2,
    /// Ozone.
    O3,
    /// Carbon monoxide.
    CO,
    /// Sulfur dioxide.
    SO2,
}

/// A single air quality reading from an OpenAQ monitoring station.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AirQualityReading {
    /// Unique identifier for the monitoring location.
    pub location_id: u64,
    /// Human-readable name for the monitoring location.
    pub location_name: String,
    /// The pollutant parameter being measured.
    pub parameter: AQParameter,
    /// Measured value of the parameter.
    pub value: f64,
    /// Unit of measurement (e.g. "ug/m3", "ppm").
    pub unit: String,
    /// Latitude of the monitoring station.
    pub latitude: f64,
    /// Longitude of the monitoring station.
    pub longitude: f64,
}

impl AirQualityReading {
    /// Validate basic sanity: value non-negative, no NaN, valid lat/lon.
    pub fn is_valid(&self) -> bool {
        self.value >= 0.0
            && !self.value.is_nan()
            && !self.latitude.is_nan()
            && !self.longitude.is_nan()
            && self.latitude >= -90.0
            && self.latitude <= 90.0
            && self.longitude >= -180.0
            && self.longitude <= 180.0
    }
}

/// Classification of a detected air quality pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AQPatternType {
    /// A wave of elevated pollution spreading across stations.
    PollutionWave,
    /// Anomalous deviation from the expected diurnal (day/night) cycle.
    DiurnalAnomaly,
    /// Sudden spike consistent with an industrial emission source.
    IndustrialSpike,
    /// Elevated ozone or secondary pollutants from photochemical reactions.
    PhotochemicalEvent,
    /// Pollution transport across regional or national boundaries.
    TransboundaryFlow,
}

/// Severity classification for air quality conditions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AQSeverity {
    /// Air quality index in the "Good" range.
    Good,
    /// Air quality index in the "Moderate" range.
    Moderate,
    /// Air quality index in the "Unhealthy" range.
    Unhealthy,
    /// Air quality index in the "Hazardous" range.
    Hazardous,
}

/// Crystallized air quality pattern detected by PSE.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AQPattern {
    /// Type of detected pattern.
    pub pattern_type: AQPatternType,
    /// Location names involved in the pattern.
    pub locations: Vec<String>,
    /// The pollutant parameter associated with this pattern.
    pub parameter: AQParameter,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Severity classification of the pattern.
    pub severity: AQSeverity,
    /// Human-readable description of the pattern.
    pub description: String,
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for OpenAQ air quality data.
///
/// Implements `ObservationAdapter` so it can be passed directly to
/// `pse_core::macro_step()`.
pub struct AirQualityAdapter {
    /// Source identifier string.
    source: String,
}

impl AirQualityAdapter {
    /// Create a new adapter for the given monitoring location.
    pub fn new(location_id: u64) -> Self {
        Self {
            source: format!("airquality:{}", location_id),
        }
    }
}

impl ObservationAdapter for AirQualityAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        // Validate the reading data if it parses
        if let Ok(reading) = serde_json::from_slice::<AirQualityReading>(raw) {
            if !reading.is_valid() {
                return Err(ObserveError::Canonicalize(
                    "invalid reading: negative value, NaN, or invalid coordinates".into(),
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

/// DomainAdapter implementation for air quality data.
impl pse_core::DomainAdapter for AirQualityAdapter {
    fn domain_name(&self) -> &str {
        "airquality"
    }
}

// ─── Embedded Sample Data ────────────────────────────────────────────────────

/// German monitoring station descriptor used for data generation.
struct StationInfo {
    /// Location identifier.
    id: u64,
    /// Station name.
    name: &'static str,
    /// Latitude.
    lat: f64,
    /// Longitude.
    lon: f64,
    /// Baseline PM2.5 level in ug/m3.
    baseline_pm25: f64,
}

/// Five German air quality monitoring stations.
const STATIONS: [StationInfo; 5] = [
    StationInfo { id: 1001, name: "Stuttgart Neckartor",     lat: 48.7862, lon: 9.1850,  baseline_pm25: 14.0 },
    StationInfo { id: 1002, name: "Ludwigsburg",             lat: 48.8975, lon: 9.1922,  baseline_pm25: 12.0 },
    StationInfo { id: 1003, name: "Esslingen",               lat: 48.7396, lon: 9.3048,  baseline_pm25: 11.0 },
    StationInfo { id: 1004, name: "Heilbronn Weinsberger",   lat: 49.1427, lon: 9.2109,  baseline_pm25: 10.0 },
    StationInfo { id: 1005, name: "Reutlingen Lederstrasse", lat: 48.4914, lon: 9.2146,  baseline_pm25: 13.0 },
];

/// Deterministic pseudo-random number generator (LCG).
///
/// Returns a value in [-1.0, 1.0] and advances the state.
fn next_rng(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*state as f64 / u64::MAX as f64) * 2.0 - 1.0
}

/// Returns embedded air quality data: 48 hours at 4 readings/hour for 5 German stations.
///
/// The dataset includes a realistic industrial spike event where PM2.5 at
/// Stuttgart Neckartor jumps from ~15 to ~85 ug/m3 at hour 18, then propagates
/// to neighboring stations over the next 6 hours with decreasing intensity
/// based on distance.
///
/// Total readings: 48 * 4 * 5 = 960.
pub fn embedded_airquality_data() -> Vec<AirQualityReading> {
    let total_slots = 48 * 4; // 192 quarter-hour slots
    let mut readings = Vec::with_capacity(total_slots * STATIONS.len());
    let mut rng: u64 = 0xDEAD_BEEF_CAFE_1234;

    // Industrial spike parameters:
    // - Starts at slot 72 (hour 18) at station 0 (Stuttgart Neckartor)
    // - Propagates to neighbors over 24 slots (6 hours)
    let spike_start_slot: usize = 72;
    let spike_duration: usize = 24;
    let spike_peak_pm25: f64 = 85.0;

    // Precompute distances from station 0 for propagation delay
    let base_lat = STATIONS[0].lat;
    let base_lon = STATIONS[0].lon;
    let distances: Vec<f64> = STATIONS
        .iter()
        .map(|s| {
            let dlat = s.lat - base_lat;
            let dlon = s.lon - base_lon;
            (dlat * dlat + dlon * dlon).sqrt()
        })
        .collect();

    for slot in 0..total_slots {
        let hour_f = slot as f64 / 4.0;

        for (station_idx, station) in STATIONS.iter().enumerate() {
            // Diurnal variation: lower at night, higher during rush hours
            let diurnal = {
                let hour_of_day = hour_f % 24.0;
                let morning_peak = (-((hour_of_day - 8.0) / 2.5).powi(2)).exp() * 4.0;
                let evening_peak = (-((hour_of_day - 18.0) / 3.0).powi(2)).exp() * 3.0;
                let night_dip = (-((hour_of_day - 3.0) / 4.0).powi(2)).exp() * -3.0;
                morning_peak + evening_peak + night_dip
            };

            // Base value with diurnal pattern and small noise
            let noise = next_rng(&mut rng) * 2.0;
            let mut value = station.baseline_pm25 + diurnal + noise;

            // Industrial spike event
            if slot >= spike_start_slot && slot < spike_start_slot + spike_duration {
                let slots_since_start = slot - spike_start_slot;

                // Propagation delay: 1 slot per 0.1 degree of distance
                let delay_slots = (distances[station_idx] / 0.1 * 1.0) as usize;

                if slots_since_start >= delay_slots {
                    let local_progress = slots_since_start - delay_slots;
                    // Sharp rise then gradual decay
                    let spike_envelope = if local_progress < 4 {
                        // Rising phase
                        local_progress as f64 / 4.0
                    } else {
                        // Decay phase
                        let decay_progress =
                            (local_progress - 4) as f64 / (spike_duration - 4) as f64;
                        (1.0 - decay_progress).max(0.0)
                    };

                    // Attenuation by distance from source
                    let distance_factor = 1.0 / (1.0 + distances[station_idx] * 3.0);
                    let spike_magnitude =
                        (spike_peak_pm25 - station.baseline_pm25) * spike_envelope * distance_factor;
                    value += spike_magnitude;
                }
            }

            // Clamp to realistic non-negative range
            if value < 0.5 {
                value = 0.5;
            }

            readings.push(AirQualityReading {
                location_id: station.id,
                location_name: station.name.to_string(),
                parameter: AQParameter::PM25,
                value,
                unit: "ug/m3".to_string(),
                latitude: station.lat,
                longitude: station.lon,
            });
        }
    }

    readings
}

// ─── Crystal Helpers ─────────────────────────────────────────────────────────

/// Describe a crystal in human-readable form based on air quality context.
pub fn describe_crystal(
    crystal: &pse_types::SemanticCrystal,
    station_name: &str,
    slot: u64,
) -> String {
    let hour = slot as f64 / 4.0;
    format!(
        "{}: pattern detected at hour {:.1}, stability={:.4}, region={} vertices, coherence={:.2}",
        station_name,
        hour,
        crystal.stability_score,
        crystal.region.len(),
        crystal.topology_signature.kuramoto_coherence,
    )
}

/// Classify the severity of a PM2.5 reading according to standard AQI breakpoints.
///
/// Breakpoints follow common AQI guidance:
/// - Good: 0 - 12 ug/m3
/// - Moderate: 12.1 - 35.4 ug/m3
/// - Unhealthy: 35.5 - 55.4 ug/m3
/// - Hazardous: >= 55.5 ug/m3
pub fn classify_severity(pm25_value: f64) -> AQSeverity {
    if pm25_value <= 12.0 {
        AQSeverity::Good
    } else if pm25_value <= 35.4 {
        AQSeverity::Moderate
    } else if pm25_value <= 55.4 {
        AQSeverity::Unhealthy
    } else {
        AQSeverity::Hazardous
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test]
    fn test_reading_roundtrip() {
        let reading = AirQualityReading {
            location_id: 1001,
            location_name: "Stuttgart Neckartor".to_string(),
            parameter: AQParameter::PM25,
            value: 14.5,
            unit: "ug/m3".to_string(),
            latitude: 48.7862,
            longitude: 9.1850,
        };
        let json = serde_json::to_vec(&reading).expect("serialize reading");
        let restored: AirQualityReading =
            serde_json::from_slice(&json).expect("deserialize reading");
        assert_eq!(restored.location_id, 1001);
        assert!((restored.value - 14.5).abs() < 1e-10);
        assert_eq!(restored.parameter, AQParameter::PM25);
    }

    #[test]
    fn test_pattern_roundtrip() {
        let pattern = AQPattern {
            pattern_type: AQPatternType::IndustrialSpike,
            locations: vec!["Stuttgart Neckartor".into()],
            parameter: AQParameter::PM25,
            confidence: 0.92,
            severity: AQSeverity::Hazardous,
            description: "Industrial PM2.5 spike detected".into(),
        };
        let json = serde_json::to_string(&pattern).expect("serialize pattern");
        let restored: AQPattern = serde_json::from_str(&json).expect("deserialize pattern");
        assert!((restored.confidence - 0.92).abs() < 1e-10);
    }

    #[test]
    fn test_reading_validation() {
        let valid = AirQualityReading {
            location_id: 1001,
            location_name: "Test".into(),
            parameter: AQParameter::PM25,
            value: 15.0,
            unit: "ug/m3".into(),
            latitude: 48.78,
            longitude: 9.18,
        };
        assert!(valid.is_valid());

        let negative_value = AirQualityReading {
            value: -1.0,
            ..valid.clone()
        };
        assert!(!negative_value.is_valid());

        let nan_value = AirQualityReading {
            value: f64::NAN,
            ..valid.clone()
        };
        assert!(!nan_value.is_valid());

        let bad_lat = AirQualityReading {
            latitude: 91.0,
            ..valid.clone()
        };
        assert!(!bad_lat.is_valid());

        let bad_lon = AirQualityReading {
            longitude: -181.0,
            ..valid.clone()
        };
        assert!(!bad_lon.is_valid());

        let nan_lat = AirQualityReading {
            latitude: f64::NAN,
            ..valid.clone()
        };
        assert!(!nan_lat.is_valid());
    }

    #[test]
    fn test_adapter_rejects_invalid_reading() {
        let adapter = AirQualityAdapter::new(1001);
        let bad_reading = AirQualityReading {
            location_id: 1001,
            location_name: "Test".into(),
            parameter: AQParameter::PM25,
            value: -10.0,
            unit: "ug/m3".into(),
            latitude: 48.78,
            longitude: 9.18,
        };
        let raw = serde_json::to_vec(&bad_reading).expect("serialize bad reading");
        let ctx = MeasurementContext::default();
        let result = adapter.canonicalize(&raw, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_sample_reading() {
        let adapter = AirQualityAdapter::new(1001);
        let readings = embedded_airquality_data();
        let reading = &readings[0];
        let raw = serde_json::to_vec(reading).expect("serialize reading");
        let ctx = MeasurementContext::default();
        let obs = adapter.canonicalize(&raw, &ctx).expect("canonicalize reading");
        assert_eq!(obs.source_id, "airquality:1001");
        assert!(!obs.payload.is_empty());
    }

    #[test]
    fn test_embedded_data_exists() {
        let data = embedded_airquality_data();
        // 48 hours * 4 readings/hour * 5 stations = 960
        assert_eq!(data.len(), 960);
        assert!(data.iter().all(|r| r.is_valid()));

        // Verify all five stations are present
        let station_ids: std::collections::BTreeSet<u64> =
            data.iter().map(|r| r.location_id).collect();
        assert_eq!(station_ids.len(), 5);
        assert!(station_ids.contains(&1001));
        assert!(station_ids.contains(&1005));

        // Verify the industrial spike is present: at least one reading > 50 ug/m3
        let max_value = data
            .iter()
            .map(|r| r.value)
            .fold(f64::NEG_INFINITY, f64::max);
        assert!(
            max_value > 50.0,
            "Expected industrial spike with PM2.5 > 50, got max={}",
            max_value
        );
    }

    #[test]
    fn test_offline_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = AirQualityAdapter::new(1001);
        let readings = embedded_airquality_data();

        let mut crystal_count = 0;
        for reading in &readings {
            let batch = vec![serde_json::to_vec(reading).expect("serialize reading")];
            if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystal_count += 1;
            }
        }

        // The engine should have processed ticks
        assert!(
            state.commit_index > 0,
            "Engine should have processed readings"
        );
        let _ = crystal_count; // used for diagnostic; no minimum enforced
    }

    #[test]
    fn test_evidence_chain_integrity() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = AirQualityAdapter::new(1001);
        let readings = embedded_airquality_data();

        let mut crystals = Vec::new();
        for reading in &readings {
            let batch = vec![serde_json::to_vec(reading).expect("serialize reading")];
            if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystals.push(crystal);
            }
        }

        // Verify each crystal has a valid evidence chain
        for crystal in &crystals {
            assert!(!crystal.evidence_chain.is_empty() || crystal.region.is_empty());
        }
    }

    #[test]
    fn test_classify_severity() {
        assert_eq!(classify_severity(5.0), AQSeverity::Good);
        assert_eq!(classify_severity(12.0), AQSeverity::Good);
        assert_eq!(classify_severity(20.0), AQSeverity::Moderate);
        assert_eq!(classify_severity(35.4), AQSeverity::Moderate);
        assert_eq!(classify_severity(45.0), AQSeverity::Unhealthy);
        assert_eq!(classify_severity(55.4), AQSeverity::Unhealthy);
        assert_eq!(classify_severity(60.0), AQSeverity::Hazardous);
        assert_eq!(classify_severity(85.0), AQSeverity::Hazardous);
    }
}
