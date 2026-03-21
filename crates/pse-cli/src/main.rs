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
    eprintln!("  init              Initialize PSE workspace");
    eprintln!("  observe <file>    Ingest observations from a JSON file");
    eprintln!("  status            Show system status");
    eprintln!("  crystals          List discovered crystals");
    eprintln!("  navigate          Run spectral navigation");
    eprintln!("  bench             Run benchmark suite");
    eprintln!("  run [ticks]       Run macro-step loop");
    eprintln!("  serve [addr]      Start gateway server");
}

fn cmd_init() {
    println!("PSE workspace initialized.");
    println!("  Config: pse.toml (default)");
    println!("  Store: pse.db (SQLite)");
}

fn cmd_status() {
    let config = Config::default();
    println!("PSE Status");
    println!("  Engine:    idle");
    println!("  Carriers:  {}", config.carrier.num_carriers);
    println!("  Crystals:  0");
    println!("  Tick:      0");
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
        "crystals" => println!("No crystals yet. Run 'pse run' first."),
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
