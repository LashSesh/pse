//! OpenAQ air quality example — detects pollution waves and anomalies.
//!
//! Run: cargo run --release --example airquality -p pse-adapter-airquality

use pse_adapter_airquality::{describe_crystal, embedded_airquality_data, AirQualityAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    println!("Using embedded air quality data (5 stations, 48 hours).");

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = AirQualityAdapter::new(1001);
    let readings = embedded_airquality_data();
    let total = readings.len();
    let batch_size = 20;

    println!("\nProcessing {} readings ({} per tick)...", total, batch_size);
    println!("{}", "─".repeat(60));

    let serialized: Vec<Vec<u8>> = readings.iter()
        .map(|r| serde_json::to_vec(r).expect("serialize"))
        .collect();

    let mut crystal_count = 0;
    for (tick, chunk) in serialized.chunks(batch_size).enumerate() {
        let batch: Vec<Vec<u8>> = chunk.to_vec();
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  Crystal {}: {}", crystal_count,
                describe_crystal(&crystal, "DE", tick as u64));
        }
    }

    println!("{}", "─".repeat(60));
    println!("Final: {} crystals from {} readings", crystal_count, total);
}
