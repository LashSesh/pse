//! PSE domain adapter for USGS earthquake seismology data.
//!
//! Connects to the USGS Earthquake Hazards Program GeoJSON API to fetch
//! real-time and historical seismic event data and ingest it as PSE
//! observations.
//!
//! # Example
//!
//! ```rust,no_run
//! use pse_adapter_seismo::{SeismoAdapter, embedded_seismo_data};
//! use pse_types::Config;
//! use pse_core::{GlobalState, macro_step};
//!
//! let config = Config::default();
//! let mut state = GlobalState::new(&config);
//! let adapter = SeismoAdapter::new("pacific_rim");
//! let events = embedded_seismo_data();
//!
//! for event in &events {
//!     let batch = vec![serde_json::to_vec(event).unwrap()];
//!     let _ = macro_step(&mut state, &batch, &config, &adapter);
//! }
//! ```

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

// ─── Domain Types ────────────────────────────────────────────────────────────

/// A single seismic event from the USGS earthquake catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeismoEvent {
    /// Latitude of the earthquake epicenter in decimal degrees.
    pub latitude: f64,
    /// Longitude of the earthquake epicenter in decimal degrees.
    pub longitude: f64,
    /// Depth of the earthquake hypocenter in kilometers.
    pub depth_km: f64,
    /// Earthquake magnitude (e.g. Richter, moment magnitude).
    pub magnitude: f64,
    /// Magnitude type code, e.g. "ml", "mw", "mb".
    pub magnitude_type: String,
    /// Human-readable place description, e.g. "15km SW of Hualien, Taiwan".
    pub place: String,
    /// Event classification, e.g. "earthquake", "quarry blast".
    pub event_type: String,
    /// Event origin time as Unix epoch milliseconds.
    pub timestamp_ms: u64,
}

impl SeismoEvent {
    /// Validate that this event has physically plausible values.
    ///
    /// Returns `true` when magnitude > 0, depth in (0, 700] km,
    /// latitude in [-90, 90], and longitude in [-180, 180].
    pub fn is_valid(&self) -> bool {
        self.magnitude > 0.0
            && !self.magnitude.is_nan()
            && self.depth_km > 0.0
            && self.depth_km <= 700.0
            && !self.depth_km.is_nan()
            && self.latitude >= -90.0
            && self.latitude <= 90.0
            && !self.latitude.is_nan()
            && self.longitude >= -180.0
            && self.longitude <= 180.0
            && !self.longitude.is_nan()
    }
}

/// A seismological pattern detected by the PSE engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeismoPattern {
    /// Classification of the detected pattern.
    pub pattern_type: SeismoPatternType,
    /// Geographic region where the pattern was detected.
    pub region: String,
    /// Number of seismic events contributing to this pattern.
    pub events_involved: usize,
    /// Confidence score in the range [0, 1].
    pub confidence: f64,
    /// Human-readable description of the detected pattern.
    pub description: String,
}

/// Classification of detected seismological patterns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SeismoPatternType {
    /// A mainshock followed by a decaying sequence of aftershocks.
    AftershockSequence,
    /// Spatially clustered swarm of events without a dominant mainshock.
    SwarmActivity,
    /// Anomalous hypocenter depth relative to regional norms.
    DepthAnomaly,
    /// Progressive increase in magnitude over a short time window.
    MagnitudeEscalation,
    /// Resumption of activity after a period of seismic quiescence.
    QuiescenceBreak,
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for USGS seismology data.
///
/// Implements `ObservationAdapter` so it can be passed directly to
/// `pse_core::macro_step()`.
pub struct SeismoAdapter {
    /// Source identifier in the form "seismo:{grid_cell}".
    source: String,
}

impl SeismoAdapter {
    /// Create a new adapter for the given geographic grid cell identifier.
    ///
    /// The grid cell is a human-readable region tag, e.g. "pacific_rim".
    pub fn new(grid_cell: &str) -> Self {
        Self {
            source: format!("seismo:{}", grid_cell),
        }
    }
}

impl ObservationAdapter for SeismoAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        // Validate the event data if it parses
        if let Ok(event) = serde_json::from_slice::<SeismoEvent>(raw) {
            if !event.is_valid() {
                return Err(ObserveError::Canonicalize(
                    "invalid seismic event: magnitude must be > 0, depth in (0, 700], lat in [-90, 90], lon in [-180, 180]".into(),
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

/// DomainAdapter implementation for USGS seismology.
impl pse_core::DomainAdapter for SeismoAdapter {
    fn domain_name(&self) -> &str {
        "seismo"
    }
}

// ─── Data Fetching ───────────────────────────────────────────────────────────

/// Fetch recent earthquake events from the USGS GeoJSON API.
///
/// Queries the USGS Earthquake Hazards Program for events matching the
/// given minimum magnitude and time window.
///
/// # Arguments
/// * `min_magnitude` - Minimum magnitude threshold (e.g. 2.5)
/// * `limit` - Maximum number of events to return
///
/// # Errors
/// Returns an error if the HTTP request fails or the response is malformed.
pub async fn fetch_events(
    min_magnitude: f64,
    limit: u16,
) -> Result<Vec<SeismoEvent>, anyhow::Error> {
    let url = format!(
        "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minmagnitude={}&limit={}&orderby=time",
        min_magnitude, limit
    );

    let resp = reqwest::get(&url).await?;
    let body: serde_json::Value = resp.json().await?;

    let features = body
        .get("features")
        .and_then(|f| f.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing 'features' array in GeoJSON response"))?;

    let mut events = Vec::with_capacity(features.len());
    for feature in features {
        let props = match feature.get("properties") {
            Some(p) => p,
            None => continue,
        };
        let geom = match feature.get("geometry") {
            Some(g) => g,
            None => continue,
        };
        let coords = match geom.get("coordinates").and_then(|c| c.as_array()) {
            Some(c) if c.len() >= 3 => c,
            _ => continue,
        };

        let longitude = coords[0].as_f64().unwrap_or(0.0);
        let latitude = coords[1].as_f64().unwrap_or(0.0);
        let depth_km = coords[2].as_f64().unwrap_or(0.0);
        let magnitude = props.get("mag").and_then(|m| m.as_f64()).unwrap_or(0.0);
        let magnitude_type = props
            .get("magType")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown")
            .to_string();
        let place = props
            .get("place")
            .and_then(|p| p.as_str())
            .unwrap_or("unknown")
            .to_string();
        let event_type = props
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("earthquake")
            .to_string();
        let timestamp_ms = props.get("time").and_then(|t| t.as_u64()).unwrap_or(0);

        let event = SeismoEvent {
            latitude,
            longitude,
            depth_km,
            magnitude,
            magnitude_type,
            place,
            event_type,
            timestamp_ms,
        };

        if event.is_valid() {
            events.push(event);
        }
    }

    Ok(events)
}

// ─── Embedded Sample Data ────────────────────────────────────────────────────

/// Returns 200 embedded seismic events for offline mode.
///
/// The synthetic dataset covers the Pacific Ring of Fire and includes:
/// - 184 background events distributed along major subduction zones
/// - 1 M6.0 mainshock near Hualien, Taiwan (lat=23.4, lon=121.5)
/// - 15 M2-4 aftershocks within 50 km and 72 hours of the mainshock
///
/// All events are deterministically generated for reproducible testing.
pub fn embedded_seismo_data() -> Vec<SeismoEvent> {
    let mut events = Vec::with_capacity(200);
    let mut rng: u64 = 42;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Map to [0.0, 1.0)
        (*r as f64 / u64::MAX as f64).abs()
    };

    // Pacific Ring of Fire anchor points (lat, lon, name)
    let anchors: &[(f64, f64, &str)] = &[
        (35.7, 139.7, "near Tokyo, Japan"),
        (38.3, 142.4, "off the coast of Miyagi, Japan"),
        (-5.0, 152.0, "New Britain, Papua New Guinea"),
        (-15.5, -75.0, "southern Peru"),
        (-33.4, -70.7, "near Santiago, Chile"),
        (19.4, -155.3, "Hawaii, USA"),
        (61.2, -150.0, "southern Alaska, USA"),
        (51.9, 177.0, "Aleutian Islands, Alaska"),
        (14.6, 121.0, "near Manila, Philippines"),
        (0.8, 127.4, "northern Molucca Sea"),
        (-8.2, 115.5, "Bali region, Indonesia"),
        (-37.8, 178.3, "off east coast of New Zealand"),
        (40.4, 143.9, "off the coast of Hokkaido, Japan"),
        (10.0, -84.2, "Costa Rica"),
        (16.9, -99.9, "near Acapulco, Mexico"),
        (-4.5, 102.3, "southern Sumatra, Indonesia"),
    ];

    // Base timestamp: 2024-01-15 00:00:00 UTC in ms
    let base_ts: u64 = 1_705_276_800_000;

    // Generate 184 background events across the Ring of Fire
    for i in 0..184 {
        let anchor = &anchors[i % anchors.len()];
        let lat_jitter = (next_rng(&mut rng) - 0.5) * 4.0;
        let lon_jitter = (next_rng(&mut rng) - 0.5) * 4.0;
        let latitude = (anchor.0 + lat_jitter).clamp(-90.0, 90.0);
        let longitude = (anchor.1 + lon_jitter).clamp(-180.0, 180.0);
        let depth_km = 5.0 + next_rng(&mut rng) * 295.0; // 5-300 km
        let magnitude = 1.5 + next_rng(&mut rng) * 4.5; // 1.5-6.0
        let time_offset_ms = (next_rng(&mut rng) * 30.0 * 24.0 * 3600.0 * 1000.0) as u64;

        let mag_types = ["ml", "mw", "mb", "ms"];
        let mag_idx = (next_rng(&mut rng) * mag_types.len() as f64) as usize;
        let magnitude_type = mag_types[mag_idx.min(mag_types.len() - 1)].to_string();

        events.push(SeismoEvent {
            latitude,
            longitude,
            depth_km,
            magnitude,
            magnitude_type,
            place: format!("{:.0}km from {}", 10.0 + next_rng(&mut rng) * 90.0, anchor.2),
            event_type: "earthquake".to_string(),
            timestamp_ms: base_ts + time_offset_ms,
        });
    }

    // Mainshock: M6.0 near Hualien, Taiwan
    let mainshock_ts = base_ts + 15 * 24 * 3600 * 1000; // Day 15
    events.push(SeismoEvent {
        latitude: 23.4,
        longitude: 121.5,
        depth_km: 15.0,
        magnitude: 6.0,
        magnitude_type: "mw".to_string(),
        place: "15km SW of Hualien, Taiwan".to_string(),
        event_type: "earthquake".to_string(),
        timestamp_ms: mainshock_ts,
    });

    // 15 aftershocks within 50 km and 72 hours
    for i in 0..15 {
        // Scatter within ~50 km (~0.45 degrees)
        let lat_off = (next_rng(&mut rng) - 0.5) * 0.9;
        let lon_off = (next_rng(&mut rng) - 0.5) * 0.9;
        let latitude = 23.4 + lat_off;
        let longitude = 121.5 + lon_off;
        // Aftershock depths cluster near mainshock depth
        let depth_km = 8.0 + next_rng(&mut rng) * 25.0;
        // Magnitudes M2-4, decaying with time (Bath's law)
        let decay = 1.0 - (i as f64 / 15.0) * 0.3;
        let magnitude = 2.0 + next_rng(&mut rng) * 2.0 * decay;
        // Time offsets: within 72 hours, front-loaded (Omori's law)
        let hour_offset = next_rng(&mut rng).sqrt() * 72.0;
        let time_offset_ms = (hour_offset * 3600.0 * 1000.0) as u64;

        events.push(SeismoEvent {
            latitude,
            longitude,
            depth_km,
            magnitude,
            magnitude_type: "ml".to_string(),
            place: format!("aftershock {:.0}km from Hualien, Taiwan", 5.0 + next_rng(&mut rng) * 45.0),
            event_type: "earthquake".to_string(),
            timestamp_ms: mainshock_ts + time_offset_ms,
        });
    }

    events
}

/// Describe a crystal in human-readable form based on seismological context.
///
/// Formats the crystal's stability, region size, and coherence into a
/// single-line summary suitable for console output.
pub fn describe_crystal(
    crystal: &pse_types::SemanticCrystal,
    region_name: &str,
    event_idx: u64,
) -> String {
    format!(
        "{}: pattern detected at event {}, stability={:.4}, region={} vertices, confidence={:.2}",
        region_name,
        event_idx,
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
    fn test_seismo_event_roundtrip() {
        let event = SeismoEvent {
            latitude: 35.7,
            longitude: 139.7,
            depth_km: 50.0,
            magnitude: 5.2,
            magnitude_type: "mw".to_string(),
            place: "near Tokyo, Japan".to_string(),
            event_type: "earthquake".to_string(),
            timestamp_ms: 1_705_276_800_000,
        };
        let json = serde_json::to_vec(&event).unwrap();
        let restored: SeismoEvent = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.place, "near Tokyo, Japan");
        assert!((restored.magnitude - 5.2).abs() < 1e-10);
        assert!((restored.latitude - 35.7).abs() < 1e-10);
    }

    #[test]
    fn test_seismo_pattern_roundtrip() {
        let pattern = SeismoPattern {
            pattern_type: SeismoPatternType::AftershockSequence,
            region: "Hualien, Taiwan".to_string(),
            events_involved: 16,
            confidence: 0.92,
            description: "M6.0 mainshock with 15 aftershocks".to_string(),
        };
        let json = serde_json::to_string(&pattern).unwrap();
        let restored: SeismoPattern = serde_json::from_str(&json).unwrap();
        assert!((restored.confidence - 0.92).abs() < 1e-10);
        assert_eq!(restored.events_involved, 16);
    }

    #[test]
    fn test_validation_rejects_invalid() {
        // Zero magnitude
        let bad_mag = SeismoEvent {
            latitude: 35.0,
            longitude: 139.0,
            depth_km: 10.0,
            magnitude: 0.0,
            magnitude_type: "ml".into(),
            place: "test".into(),
            event_type: "earthquake".into(),
            timestamp_ms: 1000,
        };
        assert!(!bad_mag.is_valid());

        // Negative depth
        let bad_depth = SeismoEvent {
            depth_km: -5.0,
            magnitude: 3.0,
            ..bad_mag.clone()
        };
        assert!(!bad_depth.is_valid());

        // Depth too deep
        let too_deep = SeismoEvent {
            depth_km: 701.0,
            magnitude: 3.0,
            ..bad_mag.clone()
        };
        assert!(!too_deep.is_valid());

        // Invalid latitude
        let bad_lat = SeismoEvent {
            latitude: 91.0,
            magnitude: 3.0,
            depth_km: 10.0,
            ..bad_mag.clone()
        };
        assert!(!bad_lat.is_valid());

        // Invalid longitude
        let bad_lon = SeismoEvent {
            longitude: 181.0,
            magnitude: 3.0,
            depth_km: 10.0,
            ..bad_mag.clone()
        };
        assert!(!bad_lon.is_valid());

        // NaN magnitude
        let nan_mag = SeismoEvent {
            magnitude: f64::NAN,
            depth_km: 10.0,
            ..bad_mag.clone()
        };
        assert!(!nan_mag.is_valid());

        // Adapter rejects invalid event
        let adapter = SeismoAdapter::new("test_region");
        let raw = serde_json::to_vec(&bad_mag).unwrap();
        let ctx = MeasurementContext::default();
        let result = adapter.canonicalize(&raw, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_embedded_data_exists() {
        let events = embedded_seismo_data();
        assert_eq!(events.len(), 200);
        assert!(events.iter().all(|e| e.is_valid()));
    }

    #[test]
    fn test_offline_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = SeismoAdapter::new("pacific_rim");
        let events = embedded_seismo_data();

        for event in &events {
            let batch = vec![serde_json::to_vec(event).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }

        // The engine should have processed events
        assert!(
            state.commit_index > 0,
            "Engine should have processed seismic events"
        );
    }

    #[test]
    fn test_evidence_chain_integrity() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = SeismoAdapter::new("pacific_rim");
        let events = embedded_seismo_data();

        let mut crystals = Vec::new();
        for event in &events {
            let batch = vec![serde_json::to_vec(event).unwrap()];
            if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystals.push(crystal);
            }
        }

        // Verify each crystal has a valid evidence chain
        for crystal in &crystals {
            assert!(!crystal.evidence_chain.is_empty() || crystal.region.is_empty());
        }
    }
}
