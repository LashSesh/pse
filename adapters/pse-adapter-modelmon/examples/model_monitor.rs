//! ML model monitoring example.
//!
//! Run: cargo run --release --example model_monitor -p pse-adapter-modelmon

use pse_adapter_modelmon::*;
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let file_path = args.iter().position(|a| a == "--file")
        .and_then(|i| args.get(i + 1).cloned());

    let events = if let Some(path) = file_path {
        println!("Loading JSONL from: {}", path);
        match std::fs::read_to_string(&path) {
            Ok(content) => parse_jsonl(&content).unwrap_or_default(),
            Err(e) => { eprintln!("Error: {}. Using embedded.", e); generate_embedded_data(42) }
        }
    } else {
        println!("Using embedded inference data (1000 events).");
        generate_embedded_data(42)
    };

    println!("\n=== Model Monitor ===");
    println!("Events: {}, batched 20 per tick", events.len());
    println!("{}", "─".repeat(60));

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = ModelMonAdapter::new("fraud_v3");
    let batch_size = 20;

    let serialized: Vec<Vec<u8>> = events.iter()
        .map(|e| serde_json::to_vec(e).unwrap()).collect();

    let mut crystal_count = 0;
    for (tick, chunk) in serialized.chunks(batch_size).enumerate() {
        let batch: Vec<Vec<u8>> = chunk.to_vec();
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  Crystal {}: {}", crystal_count, describe_crystal(&crystal, tick as u64));
        }
    }
    println!("{}", "─".repeat(60));
    println!("Final: {} crystals from {} events", crystal_count, events.len());
}
