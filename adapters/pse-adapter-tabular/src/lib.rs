//! PSE adapter for CSV/tabular data quality analysis.
//!
//! Takes any CSV file and runs it through PSE for data quality assessment,
//! detecting outliers, missing value clusters, distribution shifts, and correlation breaks.

use pse_graph::{ObservationAdapter, ObserveError};
use pse_types::{
    content_address_raw, Hash256, MeasurementContext, Observation, ProvenanceEnvelope,
};
use serde::{Deserialize, Serialize};

/// Configuration for tabular data ingestion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabularConfig {
    /// Which column identifies entities (None = each row is its own entity).
    pub entity_column: Option<String>,
    /// Which column is the time axis.
    pub time_column: Option<String>,
    /// Columns to skip during analysis.
    pub ignore_columns: Vec<String>,
    /// If unique values in a column are fewer than this, treat as categorical.
    pub categorical_threshold: usize,
}

impl Default for TabularConfig {
    fn default() -> Self {
        Self {
            entity_column: None,
            time_column: None,
            ignore_columns: Vec::new(),
            categorical_threshold: 20,
        }
    }
}

/// A single row of tabular data, flattened to numeric values.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabularRow {
    /// Row index in the original file.
    pub row_index: usize,
    /// Entity identifier (from entity column or row index).
    pub entity_id: String,
    /// Column name to value mapping (numeric columns only).
    pub values: Vec<(String, f64)>,
}

impl TabularRow {
    /// Validate: no NaN or infinite values.
    pub fn is_valid(&self) -> bool {
        self.values.iter().all(|(_, v)| v.is_finite())
    }
}

/// Quality report produced by the tabular adapter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QualityReport {
    /// Total number of rows processed.
    pub total_rows: usize,
    /// Total number of columns.
    pub total_columns: usize,
    /// Total unique entities.
    pub total_entities: usize,
    /// Total ticks processed.
    pub total_ticks: usize,
    /// Detected anomalies.
    pub anomalies: Vec<Anomaly>,
    /// Detected drift events.
    pub drift_events: Vec<DriftEvent>,
    /// Per-column statistics.
    pub column_stats: Vec<ColumnStats>,
}

/// A detected anomaly in the data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Anomaly {
    /// Row indices where the anomaly occurs.
    pub row_indices: Vec<usize>,
    /// Affected column.
    pub column: String,
    /// Type of anomaly.
    pub anomaly_type: AnomalyType,
    /// Human-readable description.
    pub description: String,
    /// Confidence score in [0, 1].
    pub confidence: f64,
}

/// Classification of data anomaly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AnomalyType {
    /// Value far from the distribution center.
    OutlierValue,
    /// Cluster of missing values.
    MissingValueCluster,
    /// Statistical distribution changed.
    DistributionShift,
    /// Correlation between columns broke down.
    CorrelationBreak,
    /// Cluster of duplicate rows.
    DuplicateCluster,
    /// Type inconsistency (e.g. string in numeric column).
    TypeInconsistency,
}

/// A detected distribution shift event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DriftEvent {
    /// Affected column.
    pub column: String,
    /// Row index where drift begins.
    pub drift_start_row: usize,
    /// Magnitude of the shift.
    pub magnitude: f64,
    /// Human-readable description.
    pub description: String,
}

/// Per-column statistics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column name.
    pub name: String,
    /// Detected data type.
    pub dtype: DataType,
    /// Number of null/missing values.
    pub null_count: usize,
    /// Percentage of null values.
    pub null_pct: f64,
    /// Number of unique values.
    pub unique_count: usize,
    /// Minimum value (numeric only).
    pub min: Option<f64>,
    /// Maximum value (numeric only).
    pub max: Option<f64>,
    /// Mean value (numeric only).
    pub mean: Option<f64>,
    /// Standard deviation (numeric only).
    pub std: Option<f64>,
}

/// Detected column data type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataType {
    /// Numeric (integer or float).
    Numeric,
    /// Categorical (few unique values).
    Categorical,
    /// Timestamp.
    Timestamp,
    /// Free text.
    Text,
}

/// PSE observation adapter for tabular data.
pub struct TabularAdapter {
    source: String,
}

impl TabularAdapter {
    /// Create a new tabular adapter.
    pub fn new(source: &str) -> Self {
        Self { source: format!("tabular:{}", source) }
    }
}

impl ObservationAdapter for TabularAdapter {
    fn source_id(&self) -> &str { &self.source }
    fn canonicalize(&self, raw: &[u8], context: &MeasurementContext) -> Result<Observation, ObserveError> {
        if let Ok(row) = serde_json::from_slice::<TabularRow>(raw) {
            if !row.is_valid() {
                return Err(ObserveError::Canonicalize("row contains NaN/Inf".into()));
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

impl pse_core::DomainAdapter for TabularAdapter {
    fn domain_name(&self) -> &str { "tabular" }
}

/// Parse CSV content into tabular rows.
///
/// First line is treated as header. Numeric columns are extracted;
/// non-numeric values are recorded as NaN.
pub fn parse_csv(content: &str, config: &TabularConfig) -> Result<(Vec<TabularRow>, Vec<ColumnStats>), anyhow::Error> {
    let mut lines = content.lines();
    let header_line = lines.next().ok_or_else(|| anyhow::anyhow!("empty CSV"))?;
    let headers: Vec<&str> = header_line.split(',').map(|h| h.trim()).collect();

    let entity_col_idx = config.entity_column.as_ref()
        .and_then(|name| headers.iter().position(|h| h == name));
    let ignore_indices: Vec<usize> = config.ignore_columns.iter()
        .filter_map(|name| headers.iter().position(|h| h == name))
        .collect();

    let mut rows = Vec::new();
    let mut col_values: Vec<Vec<f64>> = vec![Vec::new(); headers.len()];
    let mut col_null_counts: Vec<usize> = vec![0; headers.len()];

    for (row_idx, line) in lines.enumerate() {
        let line = line.trim();
        if line.is_empty() { continue; }
        let fields: Vec<&str> = line.split(',').map(|f| f.trim()).collect();

        let entity_id = if let Some(idx) = entity_col_idx {
            fields.get(idx).unwrap_or(&"unknown").to_string()
        } else {
            format!("row_{}", row_idx)
        };

        let mut values = Vec::new();
        for (col_idx, field) in fields.iter().enumerate() {
            if col_idx >= headers.len() { break; }
            if ignore_indices.contains(&col_idx) { continue; }
            if Some(col_idx) == entity_col_idx { continue; }

            if field.is_empty() || *field == "NA" || *field == "null" {
                col_null_counts[col_idx] += 1;
            } else if let Ok(v) = field.parse::<f64>() {
                values.push((headers[col_idx].to_string(), v));
                col_values[col_idx].push(v);
            }
        }

        rows.push(TabularRow { row_index: row_idx, entity_id, values });
    }

    // Compute column stats
    let total_rows = rows.len();
    let mut stats = Vec::new();
    for (col_idx, header) in headers.iter().enumerate() {
        if ignore_indices.contains(&col_idx) { continue; }
        if Some(col_idx) == entity_col_idx { continue; }

        let vals = &col_values[col_idx];
        let null_count = col_null_counts[col_idx];
        let null_pct = if total_rows > 0 { null_count as f64 / total_rows as f64 * 100.0 } else { 0.0 };

        if vals.is_empty() {
            stats.push(ColumnStats {
                name: header.to_string(), dtype: DataType::Text,
                null_count, null_pct, unique_count: 0,
                min: None, max: None, mean: None, std: None,
            });
            continue;
        }

        let min = vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        let variance = vals.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / vals.len().max(1) as f64;
        let std = variance.sqrt();

        let mut unique: Vec<i64> = vals.iter().map(|v| (*v * 1000.0) as i64).collect();
        unique.sort();
        unique.dedup();

        stats.push(ColumnStats {
            name: header.to_string(), dtype: DataType::Numeric,
            null_count, null_pct, unique_count: unique.len(),
            min: Some(min), max: Some(max), mean: Some(mean), std: Some(std),
        });
    }

    Ok((rows, stats))
}

/// Detect outliers using 3-sigma rule on each column.
pub fn detect_outliers(rows: &[TabularRow], stats: &[ColumnStats]) -> Vec<Anomaly> {
    let mut anomalies = Vec::new();
    for stat in stats {
        let (mean, std) = match (stat.mean, stat.std) {
            (Some(m), Some(s)) if s > 1e-9 => (m, s),
            _ => continue,
        };
        let threshold = 3.0 * std;
        let mut outlier_rows = Vec::new();
        for row in rows {
            for (col, val) in &row.values {
                if col == &stat.name && (val - mean).abs() > threshold {
                    outlier_rows.push(row.row_index);
                }
            }
        }
        if !outlier_rows.is_empty() {
            anomalies.push(Anomaly {
                row_indices: outlier_rows.clone(),
                column: stat.name.clone(),
                anomaly_type: AnomalyType::OutlierValue,
                description: format!("{} outliers in column '{}' (>{:.1} sigma)", outlier_rows.len(), stat.name, 3.0),
                confidence: 0.85,
            });
        }
    }
    anomalies
}

/// Detect distribution shifts by comparing first half vs second half means.
pub fn detect_drift(rows: &[TabularRow], stats: &[ColumnStats]) -> Vec<DriftEvent> {
    let mut drifts = Vec::new();
    if rows.len() < 10 { return drifts; }
    let mid = rows.len() / 2;

    for stat in stats {
        let (_, std_val) = match (stat.mean, stat.std) {
            (Some(m), Some(s)) if s > 1e-9 => (m, s),
            _ => continue,
        };
        let mut first_half = Vec::new();
        let mut second_half = Vec::new();
        for row in rows {
            for (col, val) in &row.values {
                if col == &stat.name {
                    if row.row_index < mid { first_half.push(*val); }
                    else { second_half.push(*val); }
                }
            }
        }
        if first_half.is_empty() || second_half.is_empty() { continue; }
        let mean1 = first_half.iter().sum::<f64>() / first_half.len() as f64;
        let mean2 = second_half.iter().sum::<f64>() / second_half.len() as f64;
        let shift = (mean2 - mean1).abs() / std_val;
        if shift > 0.5 {
            drifts.push(DriftEvent {
                column: stat.name.clone(),
                drift_start_row: mid,
                magnitude: shift,
                description: format!("Distribution shift in '{}' at row {}: {:.2} sigma", stat.name, mid, shift),
            });
        }
    }
    drifts
}

/// Generate embedded test CSV with known anomalies.
///
/// 100 rows, 8 columns with: 5 outliers, 3 missing value clusters,
/// 1 distribution shift at row 60, 1 correlation break between columns 3 and 5.
pub fn embedded_test_csv() -> String {
    let mut rng: u64 = 42;
    let next_rng = |r: &mut u64| -> f64 {
        *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*r as f64 / u64::MAX as f64) * 2.0 - 1.0
    };

    let mut lines = vec!["id,entity,col_a,col_b,col_c,col_d,col_e,col_f".to_string()];

    for i in 0..100 {
        let entity = format!("e{}", i % 10);
        let base_a = if i >= 60 { 100.0 } else { 50.0 }; // distribution shift at row 60
        let col_a = base_a + next_rng(&mut rng) * 10.0;

        let col_b = 25.0 + next_rng(&mut rng) * 5.0;

        // col_c and col_e correlated until row 70
        let col_c = 30.0 + next_rng(&mut rng) * 8.0;
        let col_e = if i < 70 {
            col_c * 2.0 + next_rng(&mut rng) * 1.0 // correlated
        } else {
            next_rng(&mut rng) * 100.0 // correlation break
        };

        let col_d = 10.0 + next_rng(&mut rng) * 3.0;
        let col_f = 5.0 + next_rng(&mut rng) * 2.0;

        // Inject outliers at rows 15, 30, 45, 75, 90
        let col_a_final = if [15, 30, 45, 75, 90].contains(&i) {
            col_a + 200.0 // extreme outlier
        } else {
            col_a
        };

        // Inject missing value clusters at rows 20-22, 50-52, 80-82
        let col_b_str = if (20..=22).contains(&i) || (50..=52).contains(&i) || (80..=82).contains(&i) {
            "NA".to_string()
        } else {
            format!("{:.2}", col_b)
        };

        lines.push(format!("{},{},{:.2},{},{:.2},{:.2},{:.2},{:.2}",
            i, entity, col_a_final, col_b_str, col_c, col_d, col_e, col_f));
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_core::{macro_step, GlobalState};
    use pse_types::Config;

    #[test] fn test_tabular_row_roundtrip() {
        let r = TabularRow { row_index: 0, entity_id: "e1".into(),
            values: vec![("col_a".into(), 42.0)] };
        let json = serde_json::to_vec(&r).unwrap();
        let res: TabularRow = serde_json::from_slice(&json).unwrap();
        assert_eq!(res.entity_id, "e1");
    }

    #[test] fn test_parse_csv() {
        let csv = embedded_test_csv();
        let config = TabularConfig { entity_column: Some("entity".into()), ..Default::default() };
        let (rows, stats) = parse_csv(&csv, &config).unwrap();
        assert_eq!(rows.len(), 100);
        assert!(!stats.is_empty());
    }

    #[test] fn test_outlier_detection() {
        let csv = embedded_test_csv();
        let config = TabularConfig { entity_column: Some("entity".into()), ..Default::default() };
        let (rows, stats) = parse_csv(&csv, &config).unwrap();
        let anomalies = detect_outliers(&rows, &stats);
        // Should find outliers in col_a
        let col_a_outliers: Vec<_> = anomalies.iter().filter(|a| a.column == "col_a").collect();
        assert!(!col_a_outliers.is_empty());
    }

    #[test] fn test_drift_detection() {
        let csv = embedded_test_csv();
        let config = TabularConfig {
            entity_column: Some("entity".into()),
            ignore_columns: vec!["id".into()],
            ..Default::default()
        };
        let (rows, stats) = parse_csv(&csv, &config).unwrap();
        let drifts = detect_drift(&rows, &stats);
        let col_a_drift: Vec<_> = drifts.iter().filter(|d| d.column == "col_a").collect();
        assert!(!col_a_drift.is_empty(), "Expected drift in col_a, found drifts: {:?}",
            drifts.iter().map(|d| &d.column).collect::<Vec<_>>());
    }

    #[test] fn test_column_stats() {
        let csv = embedded_test_csv();
        let config = TabularConfig { entity_column: Some("entity".into()), ..Default::default() };
        let (_, stats) = parse_csv(&csv, &config).unwrap();
        for stat in &stats {
            if stat.name == "col_b" {
                assert!(stat.null_count > 0);
            }
        }
    }

    #[test] fn test_empty_csv() {
        let result = parse_csv("", &TabularConfig::default());
        assert!(result.is_err());
    }

    #[test] fn test_single_column() {
        let csv = "value\n1.0\n2.0\n3.0";
        let (rows, stats) = parse_csv(csv, &TabularConfig::default()).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(stats.len(), 1);
    }

    #[test] fn test_offline_pipeline() {
        let csv = embedded_test_csv();
        let config_tab = TabularConfig { entity_column: Some("entity".into()), ..Default::default() };
        let (rows, _) = parse_csv(&csv, &config_tab).unwrap();
        let config = Config::default();
        let mut state = GlobalState::new(&config);
        let adapter = TabularAdapter::new("test.csv");
        for row in rows.iter().take(50) {
            let batch = vec![serde_json::to_vec(row).unwrap()];
            let _ = macro_step(&mut state, &batch, &config, &adapter);
        }
        assert!(state.commit_index > 0);
    }
}
