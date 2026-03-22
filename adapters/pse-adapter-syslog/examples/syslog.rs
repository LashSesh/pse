//! Syslog anomaly detection example.
//!
//! Run: cargo run --release --example syslog -p pse-adapter-syslog

use pse_adapter_syslog::{describe_crystal, generate_embedded_data, SyslogAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let entries: usize = args.iter().position(|a| a == "--entries")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);

    println!("Syslog Anomaly Detection — {} entries, seed {}", entries, seed);
    println!("{}", "─".repeat(60));

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = SyslogAdapter::new("cluster");
    let data = generate_embedded_data(seed);
    let limit = entries.min(data.len());
    let batch_size = 20;

    let serialized: Vec<Vec<u8>> = data.iter().take(limit)
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
    println!("Final: {} crystals from {} entries", crystal_count, limit);
}
