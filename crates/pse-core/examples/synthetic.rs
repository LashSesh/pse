//! S-Basic scenario: 50 entities, 100 ticks.
//!
//! Demonstrates the PSE engine producing crystals from synthetic observation data.

use pse_types::Config;
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

fn main() {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("synthetic");

    let n_entities = 50;
    let n_ticks = 100;
    let mut crystal_count = 0;

    println!("PSE Synthetic Scenario: {} entities, {} ticks", n_entities, n_ticks);
    println!("─────────────────────────────────────────────");

    for tick in 0..n_ticks {
        // Generate observation batch: one observation per entity
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

        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystal_count += 1;
                let id_hex: String = crystal.crystal_id.iter()
                    .take(8).map(|b| format!("{:02x}", b)).collect();
                println!("  tick {:3}: CRYSTAL {} | stability={:.4} | region={} vertices",
                    tick, id_hex, crystal.stability_score, crystal.region.len());
            }
            Ok(None) => {
                // No crystal this tick (gate rejection)
            }
            Err(e) => {
                eprintln!("  tick {:3}: ERROR: {}", tick, e);
            }
        }
    }

    println!("─────────────────────────────────────────────");
    println!("Result: {} crystals from {} ticks ({} entities)",
        crystal_count, n_ticks, n_entities);

    if crystal_count > 0 {
        println!("SUCCESS: Engine produced crystals.");
    } else {
        println!("NOTE: No crystals produced (thresholds may need tuning).");
    }
}
