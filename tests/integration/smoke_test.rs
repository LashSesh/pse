//! Integration smoke test: init → ingest → tick → crystal → verify.

use pse_types::{Config, content_address_raw};
use pse_core::{GlobalState, macro_step};
use pse_graph::PassthroughAdapter;

#[test]
fn smoke_test_full_pipeline() {
    let config = Config::default();
    let mut state = GlobalState::new(&config);
    let adapter = PassthroughAdapter::new("smoke");

    let mut crystals_produced = 0;

    for tick in 0..10 {
        let mut batch = Vec::new();
        for entity in 0..5 {
            let payload = serde_json::json!({
                "entity": format!("e_{}", entity),
                "value": (tick as f64 * 0.3 + entity as f64).sin(),
                "tick": tick,
            });
            batch.push(serde_json::to_vec(&payload).unwrap());
        }

        match macro_step(&mut state, &batch, &config, &adapter) {
            Ok(Some(crystal)) => {
                crystals_produced += 1;
                // Verify crystal has a valid SHA-256 ID
                assert_ne!(crystal.crystal_id, [0u8; 32]);
                // Verify stability score is in [0, 1]
                assert!(crystal.stability_score >= 0.0 && crystal.stability_score <= 1.0);
            }
            Ok(None) => {} // Gate rejection is normal
            Err(e) => panic!("Engine error at tick {}: {}", tick, e),
        }
    }

    // Engine should have processed all ticks without error
    assert!(state.commit_index > 0);
}
