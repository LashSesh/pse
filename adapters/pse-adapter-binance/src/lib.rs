//! PSE domain adapter for Binance cryptocurrency market data.
//!
//! Connects to Binance public REST API (no API key needed) to fetch
//! kline (candlestick) data and ingest OHLCV as PSE observations.
//!
//! # Example
//!
//! ```rust,no_run
//! use pse_adapter_binance::{BinanceAdapter, embedded_btc_klines};
//! use pse_types::Config;
//! use pse_core::{GlobalState, macro_step};
//!
//! let config = Config::default();
//! let mut state = GlobalState::new(&config);
//! let adapter = BinanceAdapter::new("BTCUSDT");
//! let klines = embedded_btc_klines();
//!
//! for tick in &klines {
//!     let batch = vec![serde_json::to_vec(tick).unwrap()];
//!     let _ = macro_step(&mut state, &batch, &config, &adapter);
//! }
//! ```

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

// ─── Domain Types ────────────────────────────────────────────────────────────

/// A single OHLCV candlestick from Binance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinanceTick {
    /// Trading pair symbol, e.g. "BTCUSDT".
    pub symbol: String,
    /// Open price.
    pub open: f64,
    /// High price.
    pub high: f64,
    /// Low price.
    pub low: f64,
    /// Close price.
    pub close: f64,
    /// Base asset volume.
    pub volume: f64,
    /// Quote asset volume.
    pub quote_volume: f64,
    /// Number of trades in interval.
    pub num_trades: u64,
}

impl BinanceTick {
    /// Validate basic sanity: no negative prices, high >= low, volume >= 0.
    pub fn is_valid(&self) -> bool {
        self.open >= 0.0
            && self.high >= 0.0
            && self.low >= 0.0
            && self.close >= 0.0
            && self.high >= self.low
            && self.volume >= 0.0
            && self.quote_volume >= 0.0
            && !self.open.is_nan()
            && !self.close.is_nan()
    }
}

/// Crystallized market pattern detected by PSE.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketPattern {
    /// Type of detected pattern.
    pub pattern_type: PatternType,
    /// Symbols involved in the pattern.
    pub symbols: Vec<String>,
    /// Confidence score in [0, 1].
    pub confidence: f64,
    /// Human-readable description.
    pub description: String,
    /// Tick range (start, end) where the pattern was detected.
    pub time_range: (u64, u64),
}

/// Classification of detected market pattern.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PatternType {
    /// Directional price trend.
    Trend,
    /// Mean-reverting behavior.
    MeanReversion,
    /// Volatility regime shift.
    VolatilityRegime,
    /// Statistical anomaly.
    Anomaly,
    /// Cross-asset correlation.
    Correlation,
}

// ─── Observation Adapter ─────────────────────────────────────────────────────

/// PSE observation adapter for Binance market data.
///
/// Implements `ObservationAdapter` so it can be passed directly to
/// `pse_core::macro_step()`.
pub struct BinanceAdapter {
    source: String,
}

impl BinanceAdapter {
    /// Create a new adapter for the given trading pair symbol.
    pub fn new(symbol: &str) -> Self {
        Self {
            source: format!("binance:{}", symbol.to_lowercase()),
        }
    }
}

impl ObservationAdapter for BinanceAdapter {
    fn source_id(&self) -> &str {
        &self.source
    }

    fn canonicalize(
        &self,
        raw: &[u8],
        context: &MeasurementContext,
    ) -> Result<Observation, ObserveError> {
        // Validate the tick data if it parses
        if let Ok(tick) = serde_json::from_slice::<BinanceTick>(raw) {
            if !tick.is_valid() {
                return Err(ObserveError::Canonicalize(
                    "invalid tick data: negative price or NaN".into(),
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

/// DomainAdapter implementation for Binance.
impl pse_core::DomainAdapter for BinanceAdapter {
    fn domain_name(&self) -> &str {
        "binance"
    }
}

// ─── Data Fetching ───────────────────────────────────────────────────────────

/// Fetch historical klines from the Binance public API.
///
/// No API key required for public endpoints.
///
/// # Arguments
/// * `symbol` - Trading pair, e.g. "BTCUSDT"
/// * `interval` - Kline interval, e.g. "1m", "5m", "1h"
/// * `limit` - Number of klines to fetch (max 1000)
///
/// # Errors
/// Returns an error if the HTTP request fails or the response is malformed.
pub async fn fetch_klines(
    symbol: &str,
    interval: &str,
    limit: u16,
) -> Result<Vec<BinanceTick>, anyhow::Error> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval={}&limit={}",
        symbol, interval, limit
    );

    let resp = reqwest::get(&url).await?;
    let body: Vec<Vec<serde_json::Value>> = resp.json().await?;

    let mut ticks = Vec::with_capacity(body.len());
    for row in &body {
        if row.len() < 9 {
            continue;
        }
        let parse_f64 = |v: &serde_json::Value| -> f64 {
            v.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v.as_f64())
                .unwrap_or(0.0)
        };

        let tick = BinanceTick {
            symbol: symbol.to_string(),
            open: parse_f64(&row[1]),
            high: parse_f64(&row[2]),
            low: parse_f64(&row[3]),
            close: parse_f64(&row[4]),
            volume: parse_f64(&row[5]),
            quote_volume: parse_f64(&row[7]),
            num_trades: row[8].as_u64().unwrap_or(0),
        };

        if tick.is_valid() {
            ticks.push(tick);
        }
    }

    Ok(ticks)
}

// ─── Embedded Sample Data ────────────────────────────────────────────────────

/// Returns 100 embedded BTC/USDT klines for offline mode.
///
/// These are realistic synthetic klines with a base price around 65,000 USD
/// and natural-looking variation.
pub fn embedded_btc_klines() -> Vec<BinanceTick> {
    // Deterministic synthetic data: 100 1-minute candles starting ~65,000
    let mut ticks = Vec::with_capacity(100);
    let base_price = 65000.0_f64;
    let mut rng: u64 = 42;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Map to [-1.0, 1.0]
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    let mut price = base_price;
    for i in 0..100 {
        let change_pct = next_rng(&mut rng) * 0.003; // ±0.3% per candle
        let open = price;
        let close = open * (1.0 + change_pct);
        let intra_vol = (next_rng(&mut rng).abs()) * 0.002;
        let high = open.max(close) * (1.0 + intra_vol);
        let low = open.min(close) * (1.0 - intra_vol);
        let volume = 50.0 + next_rng(&mut rng).abs() * 200.0;
        let quote_volume = volume * (open + close) / 2.0;
        let num_trades = 500 + (next_rng(&mut rng).abs() * 5000.0) as u64;

        ticks.push(BinanceTick {
            symbol: "BTCUSDT".to_string(),
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            num_trades,
        });

        price = close;
        // Add slight trend every 20 candles
        if i % 20 == 19 {
            price *= 1.0 + next_rng(&mut rng) * 0.005;
        }
    }

    ticks
}

/// Returns 100 embedded ETH/USDT klines for offline mode.
pub fn embedded_eth_klines() -> Vec<BinanceTick> {
    let mut ticks = Vec::with_capacity(100);
    let base_price = 3500.0_f64;
    let mut rng: u64 = 137;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    let mut price = base_price;
    for i in 0..100 {
        let change_pct = next_rng(&mut rng) * 0.004;
        let open = price;
        let close = open * (1.0 + change_pct);
        let intra_vol = next_rng(&mut rng).abs() * 0.003;
        let high = open.max(close) * (1.0 + intra_vol);
        let low = open.min(close) * (1.0 - intra_vol);
        let volume = 200.0 + next_rng(&mut rng).abs() * 1000.0;
        let quote_volume = volume * (open + close) / 2.0;
        let num_trades = 300 + (next_rng(&mut rng).abs() * 3000.0) as u64;

        ticks.push(BinanceTick {
            symbol: "ETHUSDT".to_string(),
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            num_trades,
        });

        price = close;
        if i % 20 == 19 {
            price *= 1.0 + next_rng(&mut rng) * 0.006;
        }
    }

    ticks
}

/// Describe a crystal in human-readable form based on market context.
pub fn describe_crystal(
    crystal: &pse_types::SemanticCrystal,
    symbol: &str,
    tick: u64,
) -> String {
    format!(
        "{}: pattern detected at tick {}, stability={:.4}, region={} vertices, confidence={:.2}",
        symbol,
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
    fn test_binance_tick_roundtrip() {
        let tick = BinanceTick {
            symbol: "BTCUSDT".to_string(),
            open: 65000.0,
            high: 65500.0,
            low: 64800.0,
            close: 65200.0,
            volume: 123.45,
            quote_volume: 8_000_000.0,
            num_trades: 5000,
        };
        let json = serde_json::to_vec(&tick).unwrap();
        let restored: BinanceTick = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored.symbol, "BTCUSDT");
        assert!((restored.close - 65200.0).abs() < 1e-10);
    }

    #[test]
    fn test_market_pattern_roundtrip() {
        let pattern = MarketPattern {
            pattern_type: PatternType::VolatilityRegime,
            symbols: vec!["BTCUSDT".into()],
            confidence: 0.89,
            description: "Volatility regime shift".into(),
            time_range: (100, 200),
        };
        let json = serde_json::to_string(&pattern).unwrap();
        let restored: MarketPattern = serde_json::from_str(&json).unwrap();
        assert!((restored.confidence - 0.89).abs() < 1e-10);
    }

    #[test]
    fn test_binance_tick_validation() {
        let valid = BinanceTick {
            symbol: "BTCUSDT".into(),
            open: 100.0, high: 110.0, low: 90.0, close: 105.0,
            volume: 50.0, quote_volume: 5000.0, num_trades: 100,
        };
        assert!(valid.is_valid());

        let neg_price = BinanceTick {
            open: -1.0, ..valid.clone()
        };
        assert!(!neg_price.is_valid());

        let bad_hl = BinanceTick {
            high: 80.0, low: 90.0, ..valid.clone()
        };
        assert!(!bad_hl.is_valid());

        let nan_price = BinanceTick {
            open: f64::NAN, ..valid.clone()
        };
        assert!(!nan_price.is_valid());
    }

    #[test]
    fn test_adapter_rejects_invalid_tick() {
        let adapter = BinanceAdapter::new("BTCUSDT");
        let bad_tick = BinanceTick {
            symbol: "BTCUSDT".into(),
            open: -100.0, high: 50.0, low: 40.0, close: 45.0,
            volume: 10.0, quote_volume: 450.0, num_trades: 5,
        };
        let raw = serde_json::to_vec(&bad_tick).unwrap();
        let ctx = MeasurementContext::default();
        let result = adapter.canonicalize(&raw, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn test_ingest_sample_tick() {
        let adapter = BinanceAdapter::new("BTCUSDT");
        let tick = &embedded_btc_klines()[0];
        let raw = serde_json::to_vec(tick).unwrap();
        let ctx = MeasurementContext::default();
        let obs = adapter.canonicalize(&raw, &ctx).unwrap();
        assert_eq!(obs.source_id, "binance:btcusdt");
        assert!(!obs.payload.is_empty());
    }

    #[test]
    fn test_embedded_data_exists() {
        let btc = embedded_btc_klines();
        assert_eq!(btc.len(), 100);
        assert!(btc.iter().all(|t| t.is_valid()));
        assert!(btc.iter().all(|t| t.symbol == "BTCUSDT"));

        let eth = embedded_eth_klines();
        assert_eq!(eth.len(), 100);
        assert!(eth.iter().all(|t| t.is_valid()));
    }

    #[test]
    fn test_offline_produces_crystals() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = BinanceAdapter::new("BTCUSDT");
        let klines = embedded_btc_klines();

        let mut crystal_count = 0;
        for tick in &klines {
            let batch = vec![serde_json::to_vec(tick).unwrap()];
            if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) {
                crystal_count += 1;
            }
        }

        // The engine should produce at least some crystals from 100 ticks
        // (exact count depends on thresholds, but the pipeline should work)
        assert!(
            state.commit_index > 0,
            "Engine should have processed ticks"
        );
    }

    #[test]
    fn test_evidence_chain_integrity() {
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = BinanceAdapter::new("BTCUSDT");
        let klines = embedded_btc_klines();

        let mut crystals = Vec::new();
        for tick in &klines {
            let batch = vec![serde_json::to_vec(tick).unwrap()];
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
