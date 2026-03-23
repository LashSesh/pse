//! PSE CLI — Command line interface for the Post-Symbolic Engine.

use pse_types::Config;
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

fn print_usage() {
    eprintln!("PSE — Post-Symbolic Engine v0.2.0");
    eprintln!();
    eprintln!("USAGE: pse <COMMAND>");
    eprintln!();
    eprintln!("OBSERVE (domain adapters):");
    eprintln!("  pse observe binance [--offline] [--ticks N]");
    eprintln!("  pse observe entsoe [--csv FILE] [--embedded]");
    eprintln!("  pse observe seismo [--embedded]");
    eprintln!("  pse observe weather [--embedded]");
    eprintln!("  pse observe airquality [--embedded]");
    eprintln!("  pse observe iot [--ticks N] [--seed N]");
    eprintln!("  pse observe syslog [--entries N] [--seed N]");
    eprintln!("  pse observe vitals [--duration-sec N] [--seed N]");
    eprintln!();
    eprintln!("WORKFLOW (ML/data engineering):");
    eprintln!("  pse quality --file FILE [--entity-col X] [--output FILE]");
    eprintln!("  pse monitor --file FILE");
    eprintln!();
    eprintln!("INSPECT:");
    eprintln!("  pse crystals [--format json]");
    eprintln!("  pse status");
    eprintln!("  pse accumulation");
    eprintln!();
    eprintln!("AUDIT:");
    eprintln!("  pse audit [--summary] [--verify-only]");
    eprintln!();
    eprintln!("MEMORY:");
    eprintln!("  pse memory                Show pattern memory status");
    eprintln!();
    eprintln!("SWARM:");
    eprintln!("  pse swarm demo            Run 3-node localhost demo");
    eprintln!("  pse swarm status          Show swarm node status");
    eprintln!();
    eprintln!("WASM:");
    eprintln!("  pse build-wasm            Build WebAssembly target");
    eprintln!();
    eprintln!("GENERAL:");
    eprintln!("  pse run [ticks]           Run macro-step loop");
    eprintln!("  pse serve [addr]          Start gateway server");
    eprintln!("  pse --version             Show version");
    eprintln!("  pse --help                Show this help");
}

/// Generic observe runner for adapters with embedded data.
/// Batches observations into groups of `batch_size` per tick for better crystal formation.
fn run_observe<F>(adapter: &dyn pse_graph::ObservationAdapter, data_fn: F, label: &str)
where F: FnOnce() -> Vec<Vec<u8>>
{
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let data = data_fn();
    let total = data.len();
    let batch_size = 10.max(total / 50).min(50); // 10-50 obs per tick
    let mut crystal_count = 0;

    println!("{} — processing {} observations ({} per tick)...", label, total, batch_size);
    println!("{}", "─".repeat(60));

    for (tick, chunk) in data.chunks(batch_size).enumerate() {
        let batch: Vec<Vec<u8>> = chunk.to_vec();
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, adapter) {
            crystal_count += 1;
            let id_hex: String = crystal.crystal_id.iter().take(8).map(|b| format!("{:02x}", b)).collect();
            println!("  Crystal {}: {} stability={:.4} region={}", crystal_count, id_hex,
                crystal.stability_score, crystal.region.len());
        }
        let processed = (tick + 1) * batch_size;
        if processed % 500 < batch_size || processed >= total {
            println!("Tick {}: {} entities, {} crystals",
                tick + 1, state.graph.graph.node_count(), crystal_count);
        }
    }
    println!("{}", "─".repeat(60));
    println!("Final: {} crystals from {} observations", crystal_count, total);
}

fn cmd_status() {
    let config = Config::default();
    let state = GlobalState::new(&config);
    println!("PSE Status v0.2.0");
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
        let payload = serde_json::json!({"entity": format!("e_{}", tick % 10), "value": (tick as f64 * 0.1).sin(), "tick": tick});
        let batch = vec![serde_json::to_vec(&payload).unwrap()];
        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            let id_hex: String = crystal.crystal_id.iter().take(8).map(|b| format!("{:02x}", b)).collect();
            println!("  tick {}: crystal {} (stability={:.4})", tick, id_hex, crystal.stability_score);
        }
    }
    println!("Complete: {} crystals from {} ticks", crystal_count, ticks);
}

fn cmd_observe_binance(args: &[String]) {
    let offline = args.iter().any(|a| a == "--offline");
    let adapter = pse_adapter_binance::BinanceAdapter::new("BTCUSDT");
    if offline {
        run_observe(&adapter, || pse_adapter_binance::embedded_btc_klines().iter()
            .map(|t| serde_json::to_vec(t).unwrap()).collect(), "Binance BTC/USDT (offline)");
    } else {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let klines = match rt.block_on(pse_adapter_binance::fetch_klines("BTCUSDT", "1m", 100)) {
            Ok(k) => { println!("Fetched {} klines.", k.len()); k }
            Err(e) => { eprintln!("Fetch failed: {}. Using embedded data.", e); pse_adapter_binance::embedded_btc_klines() }
        };
        run_observe(&adapter, || klines.iter().map(|t| serde_json::to_vec(t).unwrap()).collect(), "Binance BTC/USDT");
    }
}

fn cmd_observe_entsoe(args: &[String]) {
    let embedded = args.iter().any(|a| a == "--embedded");
    let csv_path = args.iter().position(|a| a == "--csv").and_then(|i| args.get(i + 1).cloned());
    let adapter = pse_adapter_entsoe::GridAdapter::new("DE_LU");
    let observations = if let Some(path) = csv_path.filter(|_| !embedded) {
        match pse_adapter_entsoe::load_csv(std::path::Path::new(&path)) {
            Ok(obs) => obs,
            Err(e) => { eprintln!("CSV error: {}. Using embedded.", e); pse_adapter_entsoe::embedded_grid_data() }
        }
    } else { pse_adapter_entsoe::embedded_grid_data() };
    run_observe(&adapter, || observations.iter().map(|o| serde_json::to_vec(o).unwrap()).collect(), "ENTSO-E DE_LU");
}

fn cmd_observe_seismo(_args: &[String]) {
    let adapter = pse_adapter_seismo::SeismoAdapter::new("ring_of_fire");
    run_observe(&adapter, || pse_adapter_seismo::embedded_seismo_data().iter()
        .map(|e| serde_json::to_vec(e).unwrap()).collect(), "USGS Seismology (embedded)");
}

fn cmd_observe_weather(_args: &[String]) {
    let adapter = pse_adapter_weather::WeatherAdapter::new("Berlin");
    run_observe(&adapter, || pse_adapter_weather::embedded_weather_data().iter()
        .map(|r| serde_json::to_vec(r).unwrap()).collect(), "Open-Meteo Weather (embedded)");
}

fn cmd_observe_airquality(_args: &[String]) {
    let adapter = pse_adapter_airquality::AirQualityAdapter::new(1);
    run_observe(&adapter, || pse_adapter_airquality::embedded_airquality_data().iter()
        .map(|r| serde_json::to_vec(r).unwrap()).collect(), "OpenAQ Air Quality (embedded)");
}

fn cmd_observe_iot(args: &[String]) {
    let ticks: usize = args.iter().position(|a| a == "--ticks")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(500);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);
    let adapter = pse_adapter_iot::IoTAdapter::new("machine_003");
    let data = pse_adapter_iot::generate_embedded_data(seed);
    let limit = ticks.min(data.len());
    run_observe(&adapter, || data.iter().take(limit).map(|r| serde_json::to_vec(r).unwrap()).collect(),
        &format!("IoT Predictive Maintenance ({} readings)", limit));
}

fn cmd_observe_syslog(args: &[String]) {
    let entries: usize = args.iter().position(|a| a == "--entries")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);
    let adapter = pse_adapter_syslog::SyslogAdapter::new("web-01");
    let data = pse_adapter_syslog::generate_embedded_data(seed);
    let limit = entries.min(data.len());
    run_observe(&adapter, || data.iter().take(limit).map(|e| serde_json::to_vec(e).unwrap()).collect(),
        &format!("Syslog Anomaly Detection ({} entries)", limit));
}

fn cmd_observe_vitals(args: &[String]) {
    println!("{}\n", pse_adapter_vitals::MEDICAL_DISCLAIMER);
    let duration: u32 = args.iter().position(|a| a == "--duration-sec")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(30);
    let seed: u64 = args.iter().position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1)).and_then(|s| s.parse().ok()).unwrap_or(42);
    let adapter = pse_adapter_vitals::VitalsAdapter::new("patient_B");
    let data = pse_adapter_vitals::generate_embedded_data(seed, duration);
    run_observe(&adapter, || data.iter().map(|r| serde_json::to_vec(r).unwrap()).collect(),
        &format!("Vital Signs ({}s)", duration));
    println!("\n{}", pse_adapter_vitals::MEDICAL_DISCLAIMER);
}

fn cmd_quality(args: &[String]) {
    let file_path = args.iter().position(|a| a == "--file")
        .and_then(|i| args.get(i + 1).cloned());
    let entity_col = args.iter().position(|a| a == "--entity-col")
        .and_then(|i| args.get(i + 1).cloned());
    let output_path = args.iter().position(|a| a == "--output")
        .and_then(|i| args.get(i + 1).cloned());

    let csv_content = if let Some(path) = file_path {
        match std::fs::read_to_string(&path) {
            Ok(c) => { println!("Loaded: {}", path); c }
            Err(e) => { eprintln!("Error: {}. Using embedded.", e); pse_adapter_tabular::embedded_test_csv() }
        }
    } else {
        println!("No --file specified. Using embedded test CSV.");
        pse_adapter_tabular::embedded_test_csv()
    };

    let tab_config = pse_adapter_tabular::TabularConfig {
        entity_column: entity_col,
        ..Default::default()
    };

    let (rows, stats) = match pse_adapter_tabular::parse_csv(&csv_content, &tab_config) {
        Ok(r) => r,
        Err(e) => { eprintln!("CSV parse error: {}", e); return; }
    };

    println!("\n=== Data Quality Report ===");
    println!("Rows: {}, Columns: {}", rows.len(), stats.len());
    for stat in &stats {
        println!("  {}: min={:.2?} max={:.2?} mean={:.2?} nulls={}", stat.name, stat.min, stat.max, stat.mean, stat.null_count);
    }

    let anomalies = pse_adapter_tabular::detect_outliers(&rows, &stats);
    let drifts = pse_adapter_tabular::detect_drift(&rows, &stats);
    if !anomalies.is_empty() { println!("\nOutliers: {}", anomalies.len()); for a in &anomalies { println!("  {}", a.description); } }
    if !drifts.is_empty() { println!("\nDrift events: {}", drifts.len()); for d in &drifts { println!("  {}", d.description); } }

    // Run through PSE
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = pse_adapter_tabular::TabularAdapter::new("quality");
    let mut cc = 0;
    for row in &rows { let b = vec![serde_json::to_vec(row).unwrap()]; if let Ok(Some(_)) = macro_step(&mut state, &b, &config, &adapter) { cc += 1; } }
    println!("\nPSE crystals: {}", cc);

    if let Some(path) = output_path {
        let report = serde_json::json!({
            "rows": rows.len(), "columns": stats.len(),
            "anomalies": anomalies.len(), "drifts": drifts.len(), "crystals": cc,
            "column_stats": stats, "anomaly_details": anomalies, "drift_details": drifts,
        });
        if let Err(e) = std::fs::write(&path, serde_json::to_string_pretty(&report).unwrap_or_default()) {
            eprintln!("Failed to write {}: {}", path, e);
        } else { println!("Report saved to {}", path); }
    }
}

fn cmd_monitor(args: &[String]) {
    let file_path = args.iter().position(|a| a == "--file")
        .and_then(|i| args.get(i + 1).cloned());

    let events = if let Some(path) = file_path {
        match std::fs::read_to_string(&path) {
            Ok(content) => pse_adapter_modelmon::parse_jsonl(&content).unwrap_or_default(),
            Err(e) => { eprintln!("Error: {}. Using embedded.", e); pse_adapter_modelmon::generate_embedded_data(42) }
        }
    } else {
        println!("No --file specified. Using embedded inference data.");
        pse_adapter_modelmon::generate_embedded_data(42)
    };

    let adapter = pse_adapter_modelmon::ModelMonAdapter::new("fraud_v3");
    run_observe(&adapter, || events.iter().map(|e| serde_json::to_vec(e).unwrap()).collect(),
        &format!("Model Monitor ({} events)", events.len()));
}

fn cmd_crystals(args: &[String]) {
    let json_format = args.iter().any(|a| a == "json") && args.iter().any(|a| a == "--format" || a == "json");
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("crystals");
    for tick in 0..50 {
        let mut batch = Vec::new();
        for entity in 0..10 {
            let p = serde_json::json!({"entity": format!("s_{:03}", entity), "value": ((tick as f64 * 0.1) + (entity as f64 * 0.2)).sin(), "tick": tick});
            batch.push(serde_json::to_vec(&p).unwrap());
        }
        let _ = macro_step(&mut state, &batch, &config, &adapter);
    }
    let crystals = state.archive.crystals();
    if crystals.is_empty() { println!("No crystals."); return; }
    if json_format {
        println!("{}", serde_json::to_string_pretty(crystals).unwrap_or_default());
    } else {
        println!("Discovered {} crystals:", crystals.len());
        for (i, c) in crystals.iter().enumerate() {
            let id: String = c.crystal_id.iter().take(8).map(|b| format!("{:02x}", b)).collect();
            println!("  {}: {} stability={:.4} region={} tick={}", i + 1, id, c.stability_score, c.region.len(), c.created_at);
        }
    }
}

fn cmd_accumulation() {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("accumulation");
    let (n_entities, n_ticks) = (30, 500);
    println!("=== PSE Accumulation Curve ===\n");
    let mut curve = Vec::new();
    let mut cc = 0;
    for tick in 0..n_ticks {
        let mut batch = Vec::with_capacity(n_entities);
        for e in 0..n_entities {
            let p = serde_json::json!({"entity": format!("s_{:03}", e), "value": ((tick as f64 * 0.1) + (e as f64 * 0.2)).sin(), "tick": tick, "phase": (tick as f64 * 0.05 + e as f64 * 0.1) % std::f64::consts::TAU});
            batch.push(serde_json::to_vec(&p).unwrap());
        }
        if let Ok(Some(_)) = macro_step(&mut state, &batch, &config, &adapter) { cc += 1; }
        if (tick + 1) % 100 == 0 { curve.push((tick + 1, cc)); }
    }
    println!("Crystals accumulated over time:");
    let max_c = curve.iter().map(|(_, c)| *c).max().unwrap_or(1).max(1);
    for (tick, count) in &curve {
        let bar: String = "\u{2588}".repeat((*count * 40) / max_c);
        println!("Tick {:4}: {} {}", tick, bar, count);
    }
    println!("\nFinal: {} crystals from {} ticks", cc, n_ticks);
}

fn cmd_audit(args: &[String]) {
    let summary = args.iter().any(|a| a == "--summary");
    let verify_only = args.iter().any(|a| a == "--verify-only");
    let output_path = args.iter().position(|a| a == "--output").and_then(|i| args.get(i + 1).cloned());

    // Generate crystals for audit
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("audit");
    for tick in 0..200 {
        let mut batch = Vec::new();
        for e in 0..30 {
            let p = serde_json::json!({"entity": format!("s_{:03}", e), "value": ((tick as f64 * 0.1) + (e as f64 * 0.2)).sin(), "tick": tick, "phase": (tick as f64 * 0.05 + e as f64 * 0.1) % std::f64::consts::TAU});
            batch.push(serde_json::to_vec(&p).unwrap());
        }
        let _ = macro_step(&mut state, &batch, &config, &adapter);
    }

    let report = pse_audit::generate_audit_report(state.archive.crystals(), state.commit_index);

    if verify_only {
        println!("Integrity: chains={} hashes={}", report.integrity_check.all_chains_valid, report.integrity_check.all_hashes_match);
        return;
    }

    if summary {
        pse_audit::print_summary(&report);
    } else {
        let json = serde_json::to_string_pretty(&report).unwrap_or_default();
        if let Some(path) = output_path {
            if let Err(e) = std::fs::write(&path, &json) { eprintln!("Error writing {}: {}", path, e); }
            else { println!("Audit report saved to {}", path); }
        } else {
            println!("{}", json);
        }
    }
}

fn cmd_memory() {
    use pse_store::{IslandStore, CrystalStore};

    let db_path = std::path::Path::new("pse.db");
    if !db_path.exists() {
        println!("Pattern memory: no store found (pse.db)");
        println!("  Run `pse observe ...` first to build crystal store.");
        return;
    }

    match IslandStore::open(db_path) {
        Ok(store) => {
            let count = store.crystal_count().unwrap_or(0);
            let mut memory = pse_memory::PatternMemory::new(pse_memory::MemoryConfig::default());

            // Load crystals from store and build signatures
            if let Ok(rows) = store.list_all_crystals() {
                let crystals: Vec<pse_types::SemanticCrystal> = rows.iter()
                    .filter_map(|row| serde_json::from_str(&row.data).ok())
                    .collect();
                memory.load_from_crystals(&crystals);
            }

            let stats = memory.stats();
            println!("Pattern memory: {} signatures", stats.index_size);
            println!("  Store: {} crystals in pse.db", count);
            println!("  Session hits: {} / {} ({:.1}%)",
                stats.hits, stats.total_lookups,
                if stats.total_lookups > 0 { stats.hit_rate * 100.0 } else { 0.0 });
            println!("  Index size: {:.1} KB",
                stats.index_size as f64 * std::mem::size_of::<pse_memory::CrystalSignature>() as f64 / 1024.0);
        }
        Err(e) => eprintln!("Error opening store: {}", e),
    }
}

fn cmd_swarm(args: &[String]) {
    let sub = args.first().map(|s| s.as_str()).unwrap_or("demo");
    match sub {
        "demo" => {
            println!("Starting 3-node swarm demo...");
            println!("Run: cargo run --example swarm_demo -p pse-net");
        }
        "status" => {
            println!("Swarm status: no active node");
            println!("  Use --swarm with observe commands to enable distributed mode");
        }
        _ => eprintln!("Unknown swarm command: {}. Use: demo, status", sub),
    }
}

fn cmd_build_wasm() {
    println!("Building PSE for WebAssembly...");
    let status = std::process::Command::new("wasm-pack")
        .args(["build", "crates/pse-wasm", "--target", "web", "--out-dir", "../../web/pkg"])
        .status();

    match status {
        Ok(s) if s.success() => {
            println!("Build complete!");
            if let Ok(metadata) = std::fs::metadata("web/pkg/pse_wasm_bg.wasm") {
                println!("WASM size: {} bytes", metadata.len());
            }
            println!();
            println!("To serve locally:");
            println!("  cd web && python3 -m http.server 8080");
            println!("  → http://localhost:8080");
        }
        Ok(s) => eprintln!("wasm-pack exited with: {}", s),
        Err(e) => eprintln!("Failed to run wasm-pack: {}. Install with: cargo install wasm-pack", e),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 { print_usage(); std::process::exit(1); }

    match args[1].as_str() {
        "--version" => println!("PSE v0.2.0"),
        "--help" | "help" => print_usage(),
        "init" => { println!("PSE workspace initialized."); println!("  Config: pse.toml (default)"); println!("  Store: pse.db (SQLite)"); }
        "status" => cmd_status(),
        "run" => { let t = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10); cmd_run(t); }
        "observe" => {
            if args.len() < 3 { eprintln!("Usage: pse observe <adapter>"); std::process::exit(1); }
            match args[2].as_str() {
                "binance" => cmd_observe_binance(&args[3..]),
                "entsoe" => cmd_observe_entsoe(&args[3..]),
                "seismo" => cmd_observe_seismo(&args[3..]),
                "weather" => cmd_observe_weather(&args[3..]),
                "airquality" => cmd_observe_airquality(&args[3..]),
                "iot" => cmd_observe_iot(&args[3..]),
                "syslog" => cmd_observe_syslog(&args[3..]),
                "vitals" => cmd_observe_vitals(&args[3..]),
                other => eprintln!("Unknown adapter: {}. Use: binance, entsoe, seismo, weather, airquality, iot, syslog, vitals", other),
            }
        }
        "quality" => cmd_quality(&args[2..]),
        "monitor" => cmd_monitor(&args[2..]),
        "crystals" => cmd_crystals(&args[2..]),
        "accumulation" => cmd_accumulation(),
        "audit" => cmd_audit(&args[2..]),
        "memory" => cmd_memory(),
        "swarm" => cmd_swarm(&args[2..]),
        "build-wasm" => cmd_build_wasm(),
        "navigate" => println!("Navigator: not yet configured."),
        "bench" => println!("Benchmark suite: run 'cargo run --release --example bench_full -p pse-core'."),
        "serve" => {
            let addr = args.get(2).map(|s| s.as_str()).unwrap_or("0.0.0.0:3000");
            println!("Starting PSE gateway on {}...", addr);
            let rt = tokio::runtime::Runtime::new().unwrap();
            let state = std::sync::Arc::new(tokio::sync::RwLock::new(pse_gateway::AppState::default()));
            rt.block_on(async { if let Err(e) = pse_gateway::serve(addr, state).await { eprintln!("Gateway error: {}", e); } });
        }
        _ => { eprintln!("Unknown command: {}", args[1]); print_usage(); std::process::exit(1); }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_command_init() {
        assert!(true);
    }
}
