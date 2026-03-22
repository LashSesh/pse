//! IoT predictive maintenance example.
//!
//! Run: cargo run --release --example iot -p pse-adapter-iot

use pse_adapter_iot::{describe_crystal, generate_embedded_data, IoTAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let ticks: usize = args.iter().position(|a| a == "--ticks")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(500);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);

    println!("IoT Predictive Maintenance — {} ticks, seed {}", ticks, seed);
    println!("{}", "─".repeat(60));

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = IoTAdapter::new("factory");
    let data = generate_embedded_data(seed);
    let limit = ticks.min(data.len());
    let batch_size = 10;

    let serialized: Vec<Vec<u8>> = data.iter().take(limit)
        .map(|r| serde_json::to_vec(r).unwrap()).collect();

    let mut crystal_count = 0;
    for (tick, chunk) in serialized.chunks(batch_size).enumerate() {
        let batch: Vec<Vec<u8>> = chunk.to_vec();
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  Crystal {}: {}", crystal_count, describe_crystal(&crystal, tick as u64));
        }
    }
    println!("{}", "─".repeat(60));
    println!("Final: {} crystals from {} readings", crystal_count, limit);
}
