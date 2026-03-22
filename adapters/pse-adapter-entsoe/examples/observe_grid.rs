//! Observe European energy grid data and feed to PSE.
//!
//! Run with embedded data: cargo run --release --example observe_grid -p pse-adapter-entsoe -- --embedded
//! Run with CSV file:      cargo run --release --example observe_grid -p pse-adapter-entsoe -- --csv path/to/data.csv

use pse_adapter_entsoe::{describe_crystal, embedded_grid_data, load_csv, GridAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let observations = if args.iter().any(|a| a == "--embedded") {
        println!("Using embedded DE_LU sample data (96 intervals, 24 hours).");
        embedded_grid_data()
    } else if let Some(pos) = args.iter().position(|a| a == "--csv") {
        let path = args
            .get(pos + 1)
            .expect("--csv requires a file path argument");
        println!("Loading CSV from: {}", path);
        match load_csv(std::path::Path::new(path)) {
            Ok(obs) => {
                println!("Loaded {} observations from CSV.", obs.len());
                obs
            }
            Err(e) => {
                eprintln!("Failed to load CSV: {}. Falling back to embedded data.", e);
                embedded_grid_data()
            }
        }
    } else {
        println!("No data source specified. Use --embedded or --csv <path>.");
        println!("Defaulting to embedded data.");
        embedded_grid_data()
    };

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = GridAdapter::new("DE_LU");

    let total = observations.len();
    let mut crystal_count = 0;
    let mut all_crystals = Vec::new();

    println!(
        "\nProcessing {} grid observations...",
        total
    );
    println!("{}", "─".repeat(60));

    for (tick, obs) in observations.iter().enumerate() {
        let batch = vec![serde_json::to_vec(obs).unwrap()];

        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let desc = describe_crystal(&crystal, &obs.area, tick as u64);
                println!("  Crystal {}: {}", crystal_count, desc);
                all_crystals.push(crystal);
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("  tick {}: error: {}", tick, e);
            }
        }

        if (tick + 1) % 96 == 0 {
            println!(
                "Tick {}: {} entities, {} crystals",
                tick + 1,
                state.graph.graph.node_count(),
                crystal_count
            );
        }
    }

    println!("{}", "─".repeat(60));
    println!(
        "Final: {} crystals from {} observations",
        crystal_count, total
    );

    if !all_crystals.is_empty() {
        let json = serde_json::to_string_pretty(&all_crystals).unwrap_or_default();
        if let Err(e) = std::fs::write("crystals_entsoe.json", &json) {
            eprintln!("Failed to write crystals_entsoe.json: {}", e);
        } else {
            println!("Crystals saved to crystals_entsoe.json");
        }
    }
}
