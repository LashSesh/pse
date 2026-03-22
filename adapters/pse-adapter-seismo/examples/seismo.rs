//! USGS seismology example — detects earthquake clusters and patterns.
//!
//! Run: cargo run --release --example seismo -p pse-adapter-seismo -- --embedded

use pse_adapter_seismo::{describe_crystal, embedded_seismo_data, SeismoAdapter};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let embedded = args.iter().any(|a| a == "--embedded") || !args.iter().any(|a| a == "--online");

    let config = Config::default();
    let mut state = GlobalState::new(&config);

    let events = if embedded {
        println!("Using embedded seismology data (200 events).");
        embedded_seismo_data()
    } else {
        println!("Fetching live data from USGS...");
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        match rt.block_on(pse_adapter_seismo::fetch_events(2.5, 200)) {
            Ok(e) => { println!("Fetched {} events.", e.len()); e }
            Err(e) => { eprintln!("Failed: {}. Using embedded.", e); embedded_seismo_data() }
        }
    };

    let adapter = SeismoAdapter::new("pacific_rim");
    let total = events.len();
    let mut crystal_count = 0;
    let batch_size = 10;

    println!("\nProcessing {} events ({} per tick)...", total, batch_size);
    println!("{}", "─".repeat(60));

    let serialized: Vec<Vec<u8>> = events.iter()
        .map(|e| serde_json::to_vec(e).expect("serialize"))
        .collect();

    for (tick, chunk) in serialized.chunks(batch_size).enumerate() {
        let batch: Vec<Vec<u8>> = chunk.to_vec();
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  Crystal {}: {}", crystal_count,
                describe_crystal(&crystal, "Pacific Ring of Fire", tick as u64));
        }
    }

    println!("{}", "─".repeat(60));
    println!("Final: {} crystals from {} events", crystal_count, total);
}
