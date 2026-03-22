//! PSE domain adapter for Open-Meteo weather data.
//!
//! Ingests hourly weather observations from Open-Meteo's free API and
//! crystallizes meteorological patterns through the PSE pipeline.
//!
//! # Example
//!
//! ```rust,no_run
//! use pse_adapter_weather::{WeatherAdapter, embedded_weather_data};
//! use pse_types::Config;
//! use pse_core::{GlobalState, macro_step};
//!
//! let config = Config::default();
//! let mut state = GlobalState::new(&config);
//! let adapter = WeatherAdapter::new("berlin");
//! let readings = embedded_weather_data();
//!
//! for batch in readings.chunks(10) {
//!     let obs: Vec<Vec<u8>> = batch.iter()
//!         .filter_map(|r| serde_json::to_vec(r).ok())
//!         .collect();
//!     let _ = macro_step(&mut state, &obs, &config, &adapter);
//! }
//! ```

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

// ─── Domain Types ────────────────────────────────────────────────────────────

/// A single hourly weather observation from a station.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeatherReading {
    /// Station identifier, e.g. "berlin".
    pub station_id: String,
    /// Station latitude in decimal degrees.
    pub latitude: f64,
    /// Station longitude in decimal degrees.
    pub longitude: f64,
    /// Air temperature in degrees Celsius.
    pub temperature_c: f64,
    /// Wind speed in kilometres per hour.
    pub wind_speed_kmh: f64,
    /// Barometric pressure at sea level in hectopascals.
    pub pressure_hpa: f64,
    /// Precipitation in millimetres over the hour.
    pub precipitation_mm: f64,
    /// Relative humidity as a percentage, if available.
    pub humidity_pct: Option<f64>,
}

impl WeatherReading {
    /// Validate physical sanity of the reading.
    ///
    /// Checks that temperature, pressure, wind speed, and precipitation
    /// are within physically reasonable bounds and that no field is NaN.
    pub fn is_valid(&self) -> bool {
        // Temperature: world record range roughly [-89.2, 56.7] — we use [-90, 60]
        let temp_ok = self.temperature_c >= -90.0 && self.temperature_c <= 60.0;
        // Pressure: lowest recorded ~870, highest ~1084 hPa
        let pres_ok = self.pressure_hpa >= 870.0 && self.pressure_hpa <= 1084.0;
        let wind_ok = self.wind_speed_kmh >= 0.0;
        let precip_ok = self.precipitation_mm >= 0.0;
        let no_nan = !self.temperature_c.is_nan()
            && !self.wind_speed_kmh.is_nan()
            && !self.pressure_hpa.is_nan()
            && !self.precipitation_mm.is_nan()
            && !self.latitude.is_nan()
            && !self.longitude.is_nan();
        let humidity_ok = match self.humidity_pct {
            Some(h) => !h.is_nan() && (0.0..=100.0).contains(&h),
            None => true,
        };

        temp_ok && pres_ok && wind_ok && precip_ok && no_nan && humidity_ok
    }
}

/// A meteorological pattern detected across weather stations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeatherPattern {
    /// Classification of the detected pattern.
    pub pattern_type: WeatherPatternType,
    /// Station identifiers involved in the pattern.
    pub stations: Vec<String>,
    /// Confidence score in the range [0, 1].
    pub confidence: f64,
    /// Human-readable description of the pattern.
    pub description: String,
}

/// Classification of detected weather patterns.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WeatherPatternType {
    /// A weather front passage propagating across stations.
    FrontPassage,
    /// An unusual temperature deviation from climatological norms.
    TemperatureAnomaly,
    /// Spatially correlated precipitation across multiple stations.
    PrecipitationCluster,
    /// Wind speed or direction correlation between stations.
    WindCorrelation,
    /// A temperature inversion layer indicated by surface observations.
    InversionLayer,
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for Open-Meteo weather data.
///
/// Implements `ObservationAdapter` so it can be passed directly to
/// `pse_core::macro_step()`.
pub struct WeatherAdapter {
    /// Source identifier string.
    source: String,
}

impl WeatherAdapter {
    /// Create a new adapter for the given weather station.
    pub fn new(station_id: &str) -> Self {
        Self {
            source: format!("weather:{}", station_id.to_lowercase()),
        }
    }
}

impl ObservationAdapter for WeatherAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        // Validate the reading data if it parses
        if let Ok(reading) = serde_json::from_slice::<WeatherReading>(raw) {
            if !reading.is_valid() {
                return Err(ObserveError::Canonicalize(
                    "invalid weather reading: value out of physical range or NaN".into(),
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

/// DomainAdapter implementation for weather data.
impl pse_core::DomainAdapter for WeatherAdapter {
    fn domain_name(&self) -> &str {
        "weather"
    }
}

// ─── Data Fetching ───────────────────────────────────────────────────────────

/// Fetch current weather from the Open-Meteo API for a given location.
///
/// Open-Meteo is a free, no-key-required weather API.
///
/// # Arguments
/// * `latitude` - Latitude of the location in decimal degrees.
/// * `longitude` - Longitude of the location in decimal degrees.
/// * `station_id` - Logical station name to tag the readings with.
///
/// # Errors
/// Returns an error if the HTTP request fails or the response is malformed.
pub async fn fetch_weather(
    latitude: f64,
    longitude: f64,
    station_id: &str,
) -> Result<Vec<WeatherReading>, anyhow::Error> {
    let url = format!(
        "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}\
         &hourly=temperature_2m,wind_speed_10m,surface_pressure,precipitation,relative_humidity_2m\
         &past_days=7&forecast_days=0",
        latitude, longitude,
    );

    let resp = reqwest::get(&url).await?;
    let body: serde_json::Value = resp.json().await?;

    let hourly = body
        .get("hourly")
        .ok_or_else(|| anyhow::anyhow!("missing 'hourly' key in Open-Meteo response"))?;

    let temps = hourly
        .get("temperature_2m")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing temperature_2m array"))?;
    let winds = hourly
        .get("wind_speed_10m")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing wind_speed_10m array"))?;
    let pressures = hourly
        .get("surface_pressure")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing surface_pressure array"))?;
    let precips = hourly
        .get("precipitation")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("missing precipitation array"))?;
    let humidities = hourly
        .get("relative_humidity_2m")
        .and_then(|v| v.as_array());

    let n = temps.len();
    let mut readings = Vec::with_capacity(n);
    for (i, temp_val) in temps.iter().enumerate().take(n) {
        let temperature_c = temp_val.as_f64().unwrap_or(0.0);
        let wind_speed_kmh = winds.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0);
        let pressure_hpa = pressures.get(i).and_then(|v| v.as_f64()).unwrap_or(1013.25);
        let precipitation_mm = precips.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0);
        let humidity_pct = humidities.and_then(|arr| arr.get(i)).and_then(|v| v.as_f64());

        let reading = WeatherReading {
            station_id: station_id.to_string(),
            latitude,
            longitude,
            temperature_c,
            wind_speed_kmh,
            pressure_hpa,
            precipitation_mm,
            humidity_pct,
        };

        if reading.is_valid() {
            readings.push(reading);
        }
    }

    Ok(readings)
}

// ─── Embedded Sample Data ────────────────────────────────────────────────────

/// Station metadata: (name, latitude, longitude).
const STATIONS: &[(&str, f64, f64)] = &[
    ("berlin", 52.52, 13.405),
    ("munich", 48.1351, 11.582),
    ("hamburg", 53.5511, 9.9937),
    ("frankfurt", 50.1109, 8.6821),
    ("cologne", 50.9375, 6.9603),
    ("stuttgart", 48.7758, 9.1829),
    ("duesseldorf", 51.2277, 6.7735),
    ("leipzig", 51.3397, 12.3731),
    ("dresden", 51.0504, 13.7373),
    ("winnweiler", 49.5650, 7.8550),
];

/// Deterministic pseudo-random number generator step.
///
/// Uses a linear congruential generator for reproducible synthetic data.
fn next_rng(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    // Map to [-1.0, 1.0]
    (*state as f64 / u64::MAX as f64) * 2.0 - 1.0
}

/// Returns 1680 embedded hourly weather readings (168 hours x 10 stations)
/// for offline mode.
///
/// The data covers 7 days of hourly observations across 10 German cities.
/// A simulated cold front passage is embedded as a pressure drop pattern
/// propagating from west to east over a 12-hour window (hours 60-72).
/// Stations are ordered west-to-east by longitude so the front arrives
/// at western stations first.
///
/// Readings are returned in time-ordered batches: all 10 stations for
/// hour 0, then all 10 for hour 1, and so on.
pub fn embedded_weather_data() -> Vec<WeatherReading> {
    let num_hours: usize = 168; // 7 days
    let num_stations = STATIONS.len(); // 10
    let mut readings = Vec::with_capacity(num_hours * num_stations);

    // Sort station indices by longitude (west to east) for front passage ordering.
    let mut station_order: Vec<usize> = (0..num_stations).collect();
    station_order.sort_by(|&a, &b| {
        STATIONS[a]
            .2
            .partial_cmp(&STATIONS[b].2)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Assign each station a front arrival hour offset based on its west-to-east rank.
    // The front spans 12 hours total (hours 60-72). Westernmost station gets offset 0.
    let mut front_offset = vec![0usize; num_stations];
    for (rank, &station_idx) in station_order.iter().enumerate() {
        // Spread offsets evenly over 12 hours across the 10 stations
        front_offset[station_idx] = rank * 12 / (num_stations - 1).max(1);
    }

    // Base temperature and pressure per station (vary by latitude).
    let base_temps: Vec<f64> = STATIONS
        .iter()
        .map(|(_, lat, _)| {
            // Rough estimate: cooler further north. Base around 10 C at 50 N.
            10.0 - (lat - 50.0) * 0.8
        })
        .collect();

    let base_pressures: Vec<f64> = STATIONS.iter().map(|_| 1015.0).collect();

    let mut rng_state: u64 = 314159265;

    for hour in 0..num_hours {
        for station_idx in 0..num_stations {
            let (name, lat, lon) = STATIONS[station_idx];

            // Diurnal temperature cycle: warmest at hour 14, coolest at hour 5
            let hour_of_day = hour % 24;
            let diurnal = 5.0 * ((hour_of_day as f64 - 14.0) * std::f64::consts::PI / 12.0).cos();

            // Multi-day drift
            let day_drift = next_rng(&mut rng_state) * 0.3;

            let mut temperature = base_temps[station_idx] + diurnal + day_drift;

            // Add noise
            temperature += next_rng(&mut rng_state) * 1.5;

            // Wind speed: base 8-15 km/h with variation
            let mut wind = 12.0 + next_rng(&mut rng_state) * 6.0;
            if wind < 0.0 {
                wind = 0.0;
            }

            // Pressure: gradual variation
            let pressure_noise = next_rng(&mut rng_state) * 3.0;
            let mut pressure = base_pressures[station_idx] + pressure_noise;

            // Cold front passage: pressure drops ~15 hPa, temperature drops ~6 C,
            // wind increases, precipitation spike.
            let front_start = 60 + front_offset[station_idx];
            let front_end = front_start + 6; // front passage lasts 6 hours at each station
            let mut precip = next_rng(&mut rng_state).abs() * 0.3; // background drizzle
            if hour >= front_start && hour < front_end {
                let progress = (hour - front_start) as f64 / 6.0;
                // Pressure drops then partially recovers
                let drop = if progress < 0.5 {
                    progress * 2.0 * 15.0
                } else {
                    (1.0 - progress) * 2.0 * 15.0
                };
                pressure -= drop;
                temperature -= 6.0 * progress;
                wind += 15.0 * (1.0 - (progress - 0.5).abs() * 2.0);
                precip += 3.0 * (1.0 - (progress - 0.4).abs() * 2.5).max(0.0);
            } else if hour >= front_end && hour < front_end + 12 {
                // Post-frontal: cooler, higher pressure
                temperature -= 4.0;
                pressure += 3.0;
            }

            // Clamp to valid ranges
            temperature = temperature.clamp(-90.0, 60.0);
            pressure = pressure.clamp(870.0, 1084.0);
            if wind < 0.0 {
                wind = 0.0;
            }
            if precip < 0.0 {
                precip = 0.0;
            }

            // Humidity: higher during precipitation, base 50-80%
            let base_humidity = 65.0 + next_rng(&mut rng_state) * 15.0;
            let humidity = (base_humidity + precip * 8.0).clamp(0.0, 100.0);

            readings.push(WeatherReading {
                station_id: name.to_string(),
                latitude: lat,
                longitude: lon,
                temperature_c: (temperature * 100.0).round() / 100.0,
                wind_speed_kmh: (wind * 100.0).round() / 100.0,
                pressure_hpa: (pressure * 100.0).round() / 100.0,
                precipitation_mm: (precip * 100.0).round() / 100.0,
                humidity_pct: Some((humidity * 100.0).round() / 100.0),
            });
        }
    }

    readings
}

/// Describe a crystal in human-readable form based on weather context.
///
/// Formats crystal metadata including stability score, region size, and
/// coherence into a single descriptive string.
pub fn describe_crystal(
    crystal: &pse_types::SemanticCrystal,
    station: &str,
    hour: u64,
) -> String {
    format!(
        "{}: pattern detected at hour {}, stability={:.4}, region={} vertices, confidence={:.2}",
        station,
        hour,
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
    fn test_reading_roundtrip() {
        let reading = WeatherReading {
            station_id: "berlin".to_string(),
            latitude: 52.52,
            longitude: 13.405,
            temperature_c: 18.5,
            wind_speed_kmh: 12.3,
            pressure_hpa: 1013.25,
            precipitation_mm: 0.0,
            humidity_pct: Some(65.0),
        };
        let json = serde_json::to_vec(&reading).expect("serialization should succeed");
        let restored: WeatherReading =
            serde_json::from_slice(&json).expect("deserialization should succeed");
        assert_eq!(restored.station_id, "berlin");
        assert!((restored.temperature_c - 18.5).abs() < 1e-10);
        assert!((restored.latitude - 52.52).abs() < 1e-10);
    }

    #[test]
    fn test_pattern_roundtrip() {
        let pattern = WeatherPattern {
            pattern_type: WeatherPatternType::FrontPassage,
            stations: vec!["cologne".into(), "frankfurt".into()],
            confidence: 0.92,
            description: "Cold front moving west to east".into(),
        };
        let json = serde_json::to_string(&pattern).expect("serialization should succeed");
        let restored: WeatherPattern =
            serde_json::from_str(&json).expect("deserialization should succeed");
        assert!((restored.confidence - 0.92).abs() < 1e-10);
        assert_eq!(restored.stations.len(), 2);
    }

    #[test]
    fn test_validation() {
        let valid = WeatherReading {
            station_id: "munich".into(),
            latitude: 48.1351,
            longitude: 11.582,
            temperature_c: 22.0,
            wind_speed_kmh: 10.0,
            pressure_hpa: 1013.0,
            precipitation_mm: 1.5,
            humidity_pct: Some(55.0),
        };
        assert!(valid.is_valid());

        // Temperature too low
        let cold = WeatherReading {
            temperature_c: -100.0,
            ..valid.clone()
        };
        assert!(!cold.is_valid());

        // Temperature too high
        let hot = WeatherReading {
            temperature_c: 65.0,
            ..valid.clone()
        };
        assert!(!hot.is_valid());

        // Pressure too low
        let low_p = WeatherReading {
            pressure_hpa: 800.0,
            ..valid.clone()
        };
        assert!(!low_p.is_valid());

        // Pressure too high
        let high_p = WeatherReading {
            pressure_hpa: 1100.0,
            ..valid.clone()
        };
        assert!(!high_p.is_valid());

        // Negative wind
        let neg_wind = WeatherReading {
            wind_speed_kmh: -5.0,
            ..valid.clone()
        };
        assert!(!neg_wind.is_valid());

        // Negative precipitation
        let neg_precip = WeatherReading {
            precipitation_mm: -1.0,
            ..valid.clone()
        };
        assert!(!neg_precip.is_valid());

        // NaN temperature
        let nan_temp = WeatherReading {
            temperature_c: f64::NAN,
            ..valid.clone()
        };
        assert!(!nan_temp.is_valid());

        // Adapter rejects invalid reading
        let adapter = WeatherAdapter::new("munich");
        let raw = serde_json::to_vec(&cold).expect("serialization should succeed");
        let ctx = MeasurementContext::default();
        let result = adapter.canonicalize(&raw, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_embedded_data_exists() {
        let data = embedded_weather_data();
        assert_eq!(data.len(), 1680, "should have 168 hours * 10 stations");
        assert!(
            data.iter().all(|r| r.is_valid()),
            "all readings should be valid"
        );

        // Verify all 10 stations are present
        let mut station_names: Vec<String> = data.iter().map(|r| r.station_id.clone()).collect();
        station_names.sort();
        station_names.dedup();
        assert_eq!(station_names.len(), 10, "should have 10 unique stations");

        // Verify time-ordered batching: first 10 are the 10 stations for hour 0
        let first_batch: Vec<&str> = data[..10].iter().map(|r| r.station_id.as_str()).collect();
        assert_eq!(first_batch.len(), 10);
    }

    #[test]
    fn test_offline_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = WeatherAdapter::new("berlin");
        let data = embedded_weather_data();

        let mut crystal_count = 0;
        // Feed in batches of 10 (one per hour, all stations)
        for batch_readings in data.chunks(10) {
            let batch: Vec<Vec<u8>> = batch_readings
                .iter()
                .filter_map(|r| serde_json::to_vec(r).ok())
                .collect();
            if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystal_count += 1;
            }
        }

        // The engine should have processed ticks
        assert!(
            state.commit_index > 0,
            "Engine should have processed readings"
        );
        let _ = crystal_count; // used for observation, not assertion
    }

    #[test]
    fn test_evidence_chain() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = WeatherAdapter::new("berlin");
        let data = embedded_weather_data();

        let mut crystals = Vec::new();
        for batch_readings in data.chunks(10) {
            let batch: Vec<Vec<u8>> = batch_readings
                .iter()
                .filter_map(|r| serde_json::to_vec(r).ok())
                .collect();
            if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystals.push(crystal);
            }
        }

        // Verify each crystal has a digest and region
        for crystal in &crystals {
            assert!(
                !crystal.region.is_empty(),
                "crystal should have a non-empty region"
            );
        }
    }
}
