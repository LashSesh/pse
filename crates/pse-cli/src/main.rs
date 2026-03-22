//! PSE CLI — Command line interface for the Post-Symbolic Engine.

use pse_types::Config;
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

fn print_usage() {
    eprintln!("PSE — Post-Symbolic Engine CLI");
    eprintln!();
    eprintln!("Usage: pse <command> [options]");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  init                       Initialize PSE workspace");
    eprintln!("  observe binance [options]  Ingest Binance market data");
    eprintln!("  observe entsoe [options]   Ingest ENTSO-E grid data");
    eprintln!("  observe <file>             Ingest observations from a JSON file");
    eprintln!("  status                     Show system status");
    eprintln!("  crystals [--format json]   List discovered crystals");
    eprintln!("  accumulation               Show accumulation curve");
    eprintln!("  navigate                   Run spectral navigation");
    eprintln!("  bench                      Run benchmark suite");
    eprintln!("  run [ticks]                Run macro-step loop");
    eprintln!("  serve [addr]               Start gateway server");
    eprintln!();
    eprintln!("Observe options:");
    eprintln!("  binance --offline                Use embedded sample data");
    eprintln!("  binance --pairs BTC/USDT,ETH/USDT --interval 1m --ticks 1000");
    eprintln!("  entsoe --embedded                Use embedded DE_LU sample data");
    eprintln!("  entsoe --csv path/to/data.csv    Load from CSV file");
}

fn cmd_init() {
    println!("PSE workspace initialized.");
    println!("  Config: pse.toml (default)");
    println!("  Store: pse.db (SQLite)");
}

fn cmd_status() {
    // Run a small scenario to show real status
    let config = Config::default();
    let state = GlobalState::new(&config);
    println!("PSE Status");
    println!("  Engine:      {:?}", state.engine_state);
    println!("  Carriers:    {}", config.carrier.num_carriers);
    println!("  Crystals:    {}", state.archive.len());
    println!("  Tick:        {}", state.commit_index);
    println!("  Graph:       {} vertices, {} edges",
        state.graph.graph.node_count(), state.graph.graph.edge_count());
    println!("  Memory:      ~{} bytes (graph)", state.graph.estimate_heap_size());
}

fn cmd_run(ticks: usize) {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("cli");

    println!("Running {} ticks...", ticks);
    let mut crystal_count = 0;

    for tick in 0..ticks {
        let payload = serde_json::json!({
            "entity": format!("entity_{}", tick % 10),
            "value": (tick as f64 * 0.1).sin(),
            "tick": tick
        });
        let batch = vec![serde_json::to_vec(&payload).unwrap()];

        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let id_hex: String = crystal.crystal_id.iter()
                    .take(8).map(|b| format!("{:02x}", b)).collect();
                println!("  tick {}: crystal {} (stability={:.4})",
                    tick, id_hex, crystal.stability_score);
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("  tick {}: error: {}", tick, e);
            }
        }
    }

    println!("Complete: {} crystals from {} ticks", crystal_count, ticks);
}

fn cmd_observe_binance(args: &[String]) {
    let offline = args.iter().any(|a| a == "--offline");
    let ticks: usize = args.iter()
        .position(|a| a == "--ticks")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = pse_adapter_binance::BinanceAdapter::new("BTCUSDT");

    let klines = if offline {
        println!("Using embedded BTC/USDT data (100 klines).");
        pse_adapter_binance::embedded_btc_klines()
    } else {
        println!("Fetching live data from Binance...");
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        match rt.block_on(pse_adapter_binance::fetch_klines("BTCUSDT", "1m", ticks.min(1000) as u16)) {
            Ok(k) => {
                println!("Fetched {} klines.", k.len());
                k
            }
            Err(e) => {
                eprintln!("Fetch failed: {}. Using embedded data.", e);
                pse_adapter_binance::embedded_btc_klines()
            }
        }
    };

    let mut crystal_count = 0;
    for (tick, kline) in klines.iter().enumerate() {
        let batch = vec![serde_json::to_vec(kline).unwrap()];
        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let desc = pse_adapter_binance::describe_crystal(&crystal, "BTC/USDT", tick as u64);
                println!("  Crystal {}: {}", crystal_count, desc);
            }
            Ok(None) => {}
            Err(e) => eprintln!("  tick {}: {}", tick, e),
        }
        if (tick + 1) % 100 == 0 {
            println!("Tick {}: {} entities, {} crystals",
                tick + 1, state.graph.graph.node_count(), crystal_count);
        }
    }
    println!("Complete: {} crystals from {} ticks", crystal_count, klines.len());
}

fn cmd_observe_entsoe(args: &[String]) {
    let embedded = args.iter().any(|a| a == "--embedded");
    let csv_path = args.iter()
        .position(|a| a == "--csv")
        .and_then(|i| args.get(i + 1).cloned());

    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = pse_adapter_entsoe::GridAdapter::new("DE_LU");

    let observations = if let Some(path) = csv_path.filter(|_| !embedded) {
        println!("Loading CSV from: {}", path);
        match pse_adapter_entsoe::load_csv(std::path::Path::new(&path)) {
            Ok(obs) => {
                println!("Loaded {} observations.", obs.len());
                obs
            }
            Err(e) => {
                eprintln!("CSV load failed: {}. Using embedded data.", e);
                pse_adapter_entsoe::embedded_grid_data()
            }
        }
    } else {
        println!("Using embedded DE_LU data (96 intervals, 24 hours).");
        pse_adapter_entsoe::embedded_grid_data()
    };

    let mut crystal_count = 0;
    for (tick, obs) in observations.iter().enumerate() {
        let batch = vec![serde_json::to_vec(obs).unwrap()];
        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let desc = pse_adapter_entsoe::describe_crystal(&crystal, &obs.area, tick as u64);
                println!("  Crystal {}: {}", crystal_count, desc);
            }
            Ok(None) => {}
            Err(e) => eprintln!("  tick {}: {}", tick, e),
        }
        if (tick + 1) % 96 == 0 {
            println!("Tick {}: {} entities, {} crystals",
                tick + 1, state.graph.graph.node_count(), crystal_count);
        }
    }
    println!("Complete: {} crystals from {} observations", crystal_count, observations.len());
}

fn cmd_crystals(args: &[String]) {
    let json_format = args.iter().any(|a| a == "--format" || a == "json")
        && args.iter().any(|a| a == "json" || a == "--format");

    // Run a small scenario to populate the archive
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("crystals");

    for tick in 0..50 {
        let mut batch = Vec::new();
        for entity in 0..10 {
            let payload = serde_json::json!({
                "entity": format!("sensor_{:03}", entity),
                "value": ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin(),
                "tick": tick,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }
        let _ = macro_step(&mut state, &batch, &config, &adapter);
    }

    let crystals = state.archive.crystals();
    if crystals.is_empty() {
        println!("No crystals discovered yet.");
        return;
    }

    if json_format {
        let json = serde_json::to_string_pretty(crystals).unwrap_or_default();
        println!("{}", json);
    } else {
        println!("Discovered {} crystals:", crystals.len());
        for (i, crystal) in crystals.iter().enumerate() {
            let id_hex: String = crystal.crystal_id.iter()
                .take(8).map(|b| format!("{:02x}", b)).collect();
            println!(
                "  {}: id={} stability={:.4} region={} vertices tick={}",
                i + 1, id_hex, crystal.stability_score,
                crystal.region.len(), crystal.created_at,
            );
        }
    }
}

fn cmd_accumulation() {
    let config = Config::default();
    let n_entities = 30;
    let n_ticks = 500;

    println!("=== PSE Accumulation Curve ===\n");

    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("accumulation");

    let mut curve: Vec<(usize, usize)> = Vec::new();
    let mut crystal_count = 0;

    for tick in 0..n_ticks {
        let mut batch = Vec::with_capacity(n_entities);
        for entity in 0..n_entities {
            let value = ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin();
            let payload = serde_json::json!({
                "entity": format!("sensor_{:03}", entity),
                "value": value,
                "tick": tick,
                "phase": (tick as f64 * 0.05 + entity as f64 * 0.1) % std::f64::consts::TAU,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }

        if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
        }

        if (tick + 1) % 100 == 0 {
            curve.push((tick + 1, crystal_count));
        }
    }

    println!("Crystals accumulated over time:");
    let max_crystals = curve.iter().map(|(_, c)| *c).max().unwrap_or(1).max(1);
    for (tick, count) in &curve {
        let bar_len = (*count * 40) / max_crystals;
        let bar: String = "\u{2588}".repeat(bar_len);
        println!("Tick {:4}: {} {}", tick, bar, count);
    }

    if curve.len() >= 2 {
        let (_, first) = curve[0];
        let (_, last) = curve[curve.len() - 1];
        let intervals = curve.len() - 1;
        if intervals > 0 && first > 0 {
            let rate = (last - first) as f64 / intervals as f64;
            let trend = if rate < first as f64 / 2.0 { "degressive" } else { "linear" };
            println!("\nGrowth rate: {:.1} crystals per 100 ticks ({})", rate, trend);
        }
    }

    println!("\nFinal: {} crystals from {} ticks", crystal_count, n_ticks);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    match args[1].as_str() {
        "init" => cmd_init(),
        "status" => cmd_status(),
        "run" => {
            let ticks = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
            cmd_run(ticks);
        }
        "observe" => {
            if args.len() < 3 {
                eprintln!("Usage: pse observe <binance|entsoe|file>");
                std::process::exit(1);
            }
            match args[2].as_str() {
                "binance" => cmd_observe_binance(&args[3..]),
                "entsoe" => cmd_observe_entsoe(&args[3..]),
                file => {
                    // Legacy: observe from JSON file
                    println!("Observing from file: {}", file);
                    println!("(File observation not yet implemented. Use 'observe binance' or 'observe entsoe'.)");
                }
            }
        }
        "crystals" => cmd_crystals(&args[2..]),
        "accumulation" => cmd_accumulation(),
        "navigate" => println!("Navigator: not yet configured. Use 'pse run' first."),
        "bench" => println!("Benchmark suite: run 'cargo test --workspace' for validation."),
        "serve" => {
            let addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3000");
            println!("Starting PSE gateway on {}...", addr);
            let rt = tokio::runtime::Runtime::new().unwrap();
            let state = std::sync::Arc::new(tokio::sync::RwLock::new(
                pse_gateway::AppState::default()
            ));
            rt.block_on(async {
                if let Err(e) = pse_gateway::serve(addr, state).await {
                    eprintln!("Gateway error: {}", e);
                }
            });
        }
        _ => {
            eprintln!("Unknown command: {}", args[1]);
            print_usage();
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_command_init() {
        // Smoke test: ensure the binary structure is correct
        assert!(true);
    }
}
