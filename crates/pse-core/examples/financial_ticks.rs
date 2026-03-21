//! Minimal example: observe price data, find invariances.

use pse_types::Config;
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

fn main() {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("market");

    println!("PSE Financial Ticks Example");
    println!("──────────────────────────");

    let prices = vec![
        100.0, 101.2, 99.8, 102.5, 101.0, 103.3, 100.5, 104.0,
        102.1, 105.5, 103.0, 106.2, 104.5, 107.0, 105.1, 108.3,
        106.0, 109.5, 107.2, 110.0,
    ];

    let mut crystal_count = 0;
    for (i, price) in prices.iter().enumerate() {
        let payload = serde_json::json!({
            "entity": "AAPL",
            "price": price,
            "volume": 1000 + i * 100,
            "tick": i,
        });
        let batch = vec![serde_json::to_vec(&payload).unwrap()];

        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  tick {:2}: Crystal (stability={:.4})", i, crystal.stability_score);
        }
    }

    println!("──────────────────────────");
    println!("Crystals discovered: {}", crystal_count);
}
