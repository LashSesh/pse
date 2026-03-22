//! CSV data quality check example.
//!
//! Run: cargo run --release --example quality_check -p pse-adapter-tabular

use pse_adapter_tabular::*;
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file_path = args.iter().position(|a| a == "--file")
        .and_then(|i| args.get(i + 1).cloned());

    let csv_content = if let Some(path) = file_path {
        println!("Loading CSV from: {}", path);
        match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => { eprintln!("Error reading file: {}. Using embedded test data.", e); embedded_test_csv() }
        }
    } else {
        println!("Using embedded test CSV (100 rows, 8 columns).");
        embedded_test_csv()
    };

    let tab_config = TabularConfig {
        entity_column: Some("entity".into()),
        ..Default::default()
    };

    let (rows, stats) = match parse_csv(&csv_content, &tab_config) {
        Ok(r) => r,
        Err(e) => { eprintln!("CSV parse error: {}", e); return; }
    };

    println!("\n=== Data Quality Report ===");
    println!("Rows: {}, Columns: {}", rows.len(), stats.len());

    println!("\n--- Column Statistics ---");
    for stat in &stats {
        println!("  {}: min={:.2?} max={:.2?} mean={:.2?} std={:.2?} nulls={}({:.1}%)",
            stat.name, stat.min, stat.max, stat.mean, stat.std, stat.null_count, stat.null_pct);
    }

    let anomalies = detect_outliers(&rows, &stats);
    if !anomalies.is_empty() {
        println!("\n--- Outliers ---");
        for a in &anomalies {
            println!("  {}: {} (rows: {:?})", a.column, a.description,
                &a.row_indices[..a.row_indices.len().min(5)]);
        }
    }

    let drifts = detect_drift(&rows, &stats);
    if !drifts.is_empty() {
        println!("\n--- Distribution Shifts ---");
        for d in &drifts {
            println!("  {}", d.description);
        }
    }

    // Run through PSE engine
    println!("\n--- PSE Analysis ---");
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = TabularAdapter::new("quality");
    let mut crystal_count = 0;

    for row in &rows {
        let batch = vec![serde_json::to_vec(row).unwrap()];
        if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
        }
    }
    println!("PSE crystals: {}, ticks: {}", crystal_count, state.commit_index);
}
