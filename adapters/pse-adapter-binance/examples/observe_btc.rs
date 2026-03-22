//! Fetch BTC/USDT data and feed to PSE, printing discovered crystals.
//!
//! Run offline:  cargo run --release --example observe_btc -p pse-adapter-binance -- --offline
//! Run online:   cargo run --release --example observe_btc -p pse-adapter-binance

use pse_adapter_binance::{
    describe_crystal, embedded_btc_klines, embedded_eth_klines, BinanceAdapter,
};
use pse_core::{macro_step, GlobalState};
use pse_types::Config;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let offline = args.iter().any(|a| a == "--offline");

    let config = Config::default();
    let mut state = GlobalState::new(&config);

    // Collect klines for multiple pairs
    let btc_klines = if offline {
        println!("Using embedded offline data.");
        embedded_btc_klines()
    } else {
        println!("Fetching live data from Binance... (use --offline for embedded data)");
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        match rt.block_on(pse_adapter_binance::fetch_klines("BTCUSDT", "1m", 1000)) {
            Ok(klines) => {
                println!("Fetched {} klines for BTCUSDT", klines.len());
                klines
            }
            Err(e) => {
                eprintln!("Failed to fetch live data: {}. Falling back to embedded.", e);
                embedded_btc_klines()
            }
        }
    };

    let eth_klines = if offline {
        embedded_eth_klines()
    } else {
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        match rt.block_on(pse_adapter_binance::fetch_klines("ETHUSDT", "1m", 1000)) {
            Ok(klines) => {
                println!("Fetched {} klines for ETHUSDT", klines.len());
                klines
            }
            Err(e) => {
                eprintln!("Failed to fetch ETH data: {}. Falling back to embedded.", e);
                embedded_eth_klines()
            }
        }
    };

    let btc_adapter = BinanceAdapter::new("BTCUSDT");
    let eth_adapter = BinanceAdapter::new("ETHUSDT");

    let total_ticks = btc_klines.len().max(eth_klines.len());
    let mut crystal_count = 0;
    let mut all_crystals = Vec::new();

    println!(
        "\nProcessing {} ticks ({} BTC, {} ETH klines)...",
        total_ticks,
        btc_klines.len(),
        eth_klines.len()
    );
    println!("{}", "─".repeat(60));

    for tick in 0..total_ticks {
        // Build batch from available klines
        let mut batch = Vec::new();
        if let Some(btc_tick) = btc_klines.get(tick) {
            batch.push(serde_json::to_vec(btc_tick).unwrap());
        }
        if let Some(eth_tick) = eth_klines.get(tick) {
            batch.push(serde_json::to_vec(eth_tick).unwrap());
        }

        // Alternate adapters — use BTC adapter for primary ingestion
        let adapter: &dyn pse_graph::ObservationAdapter = &btc_adapter;
        match macro_step(&mut state, &batch, &config, adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let desc = describe_crystal(&crystal, "BTC/USDT", tick as u64);
                println!("  Crystal {}: {}", crystal_count, desc);
                all_crystals.push(crystal);
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("  tick {}: error: {}", tick, e);
            }
        }

        if (tick + 1) % 100 == 0 {
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
        "Final: {} crystals from {} ticks",
        crystal_count, total_ticks
    );

    // Save crystals to JSON
    if !all_crystals.is_empty() {
        let json = serde_json::to_string_pretty(&all_crystals).unwrap_or_default();
        if let Err(e) = std::fs::write("crystals_binance.json", &json) {
            eprintln!("Failed to write crystals_binance.json: {}", e);
        } else {
            println!("Crystals saved to crystals_binance.json");
        }
    }

    // Also use eth_adapter to show it's wired up
    let _ = &eth_adapter;
}
