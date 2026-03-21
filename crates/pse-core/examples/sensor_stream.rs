//! Minimal example: IoT sensor anomaly detection.

use pse_types::Config;
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

fn main() {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("iot");

    println!("PSE Sensor Stream Example");
    println!("─────────────────────────");

    let n_sensors = 5;
    let n_readings = 50;
    let mut crystal_count = 0;

    for tick in 0..n_readings {
        let mut batch = Vec::new();
        for sensor in 0..n_sensors {
            let temp = 20.0 + (tick as f64 * 0.1).sin() * 5.0 + sensor as f64 * 0.5;
            let humidity = 50.0 + (tick as f64 * 0.05).cos() * 10.0;
            let payload = serde_json::json!({
                "entity": format!("sensor_{}", sensor),
                "temperature": temp,
                "humidity": humidity,
                "tick": tick,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }

        if let Ok(Some(crystal)) = macro_step(&mut state, &batch, &config, &adapter) {
            crystal_count += 1;
            println!("  tick {:2}: Crystal (stability={:.4})", tick, crystal.stability_score);
        }
    }

    println!("─────────────────────────");
    println!("Crystals discovered: {}", crystal_count);
}
