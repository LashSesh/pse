//! Vital signs monitoring example.
//!
//! Run: cargo run --release --example vitals -p pse-adapter-vitals

use pse_adapter_vitals::{describe_crystal, generate_embedded_data, VitalsAdapter, MEDICAL_DISCLAIMER};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    println!("{}\n", MEDICAL_DISCLAIMER);

    let args: Vec<String> = std::env::args().collect();
    let duration: u32 = args.iter().position(|a| a == "--duration-sec")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(60);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);

    println!("Vital Signs Monitoring — {}s duration, seed {}", duration, seed);
    println!("{}", "─".repeat(60));

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = VitalsAdapter::new("ward");
    let data = generate_embedded_data(seed, duration);
    let batch_size = 20;

    let serialized: Vec<Vec<u8>> = data.iter()
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
    println!("Final: {} crystals from {} samples", crystal_count, data.len());
    println!("\n{}", MEDICAL_DISCLAIMER);
}
