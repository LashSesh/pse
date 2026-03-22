//! Fetch weather data and feed to PSE, printing discovered crystals.
//!
//! Run offline:  cargo run --release --example weather -p pse-adapter-weather -- --offline
//! Run online:   cargo run --release --example weather -p pse-adapter-weather

use pse_adapter_weather::{describe_crystal, embedded_weather_data, WeatherAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let offline = args.iter().any(|a| a == "--offline");

    let config = Config::default();
    let mut state = GlobalState::new(&config);

    let readings = if offline {
        println!("Using embedded offline data (1680 readings, 10 stations, 168 hours).");
        embedded_weather_data()
    } else {
        println!("Fetching live data from Open-Meteo... (use --offline for embedded data)");
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

        // Fetch Berlin as the primary station
        match rt.block_on(pse_adapter_weather::fetch_weather(52.52, 13.405, "berlin")) {
            Ok(readings) => {
                println!("Fetched {} readings for Berlin", readings.len());
                readings
            }
            Err(e) => {
                eprintln!("Failed to fetch live data: {}. Falling back to embedded.", e);
                embedded_weather_data()
            }
        }
    };

    let adapter = WeatherAdapter::new("berlin");

    let total_batches = (readings.len() + 9) / 10;
    let mut crystal_count = 0;
    let mut all_crystals = Vec::new();

    println!(
        "\nProcessing {} readings in {} batches...",
        readings.len(),
        total_batches
    );
    println!("{}", "-".repeat(60));

    for (batch_idx, batch_readings) in readings.chunks(10).enumerate() {
        let batch: Vec<Vec<u8>> = batch_readings
            .iter()
            .filter_map(|r| serde_json::to_vec(r).ok())
            .collect();

        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let desc = describe_crystal(&crystal, "weather", batch_idx as u64);
                println!("  Crystal {}: {}", crystal_count, desc);
                all_crystals.push(crystal);
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("  batch {}: error: {}", batch_idx, e);
            }
        }

        if (batch_idx + 1) % 50 == 0 {
            println!(
                "Batch {}: {} entities, {} crystals",
                batch_idx + 1,
                state.graph.graph.node_count(),
                crystal_count
            );
        }
    }

    println!("{}", "-".repeat(60));
    println!(
        "Final: {} crystals from {} batches ({} readings)",
        crystal_count,
        total_batches,
        readings.len()
    );

    // Save crystals to JSON
    if !all_crystals.is_empty() {
        let json = serde_json::to_string_pretty(&all_crystals).unwrap_or_default();
        if let Err(e) = std::fs::write("crystals_weather.json", &json) {
            eprintln!("Failed to write crystals_weather.json: {}", e);
        } else {
            println!("Crystals saved to crystals_weather.json");
        }
    }
}
