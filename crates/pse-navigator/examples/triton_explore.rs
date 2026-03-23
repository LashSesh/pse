//! TRITON exploration demo: TritonNavigator exploring a multi-modal landscape.
//!
//! Run with: `cargo run --example triton_explore -p pse-navigator`

use pse_navigator::{NavigatorConfig, SpectralSignature, TritonNavigator};

fn main() {
    println!("TRITON Exploration Demo");
    println!("{}", "═".repeat(60));

    // Multi-modal landscape with two peaks
    let evaluator = |params: &[f64]| -> SpectralSignature {
        // Peak 1 at (0.3, 0.3) with radius 0.2
        let d1 = ((params[0] - 0.3).powi(2) + (params[1] - 0.3).powi(2)).sqrt();
        let r1 = (1.0 - d1 / 0.2).max(0.0);

        // Peak 2 at (0.8, 0.7) with radius 0.15
        let d2 = ((params[0] - 0.8).powi(2) + (params[1] - 0.7).powi(2)).sqrt();
        let r2 = (0.8 - d2 / 0.15).max(0.0);

        let best = r1.max(r2);
        SpectralSignature::new(best, best * 0.9, best * 0.8)
    };

    let config = NavigatorConfig {
        dim: 2,
        k: 4,
        seed: 42,
        allow_betti_change: true,
        singularity_spectral_gap_threshold: 0.05,
        ..Default::default()
    };

    let mut triton = TritonNavigator::new(config, evaluator);

    println!("\nRunning 100 exploration steps...\n");
    println!(
        "{:>5} {:>10} {:>10} {:>10} {:>10} {:>8} {:>6}",
        "Step", "X", "Y", "Gap", "Entropy", "Betti", "Sing?"
    );
    println!("{}", "─".repeat(68));

    let steps = triton.run(100);

    // Print every 10th step
    for step in steps.iter().filter(|s| s.index % 10 == 0 || s.is_singularity) {
        println!(
            "{:>5} {:>10.4} {:>10.4} {:>10.6} {:>10.4} {:>8} {:>6}",
            step.index,
            step.point[0],
            step.point[1],
            step.spectral_gap,
            step.local_entropy,
            format!("{:?}", step.betti),
            if step.is_singularity { "*" } else { "" }
        );
    }

    println!("\n{}", "─".repeat(60));
    println!("Summary:");
    println!("  Steps:              {}", steps.len());
    println!("  Mesh vertices:      {}", triton.mesh().vertex_count());
    println!("  Mesh edges:         {}", triton.mesh().edges.len());
    println!("  Mesh simplices:     {}", triton.mesh().simplices.len());
    println!("  Singularities:      {}", triton.singularity_count());
    println!("  Topology events:    {}", triton.topology_events());
    println!(
        "  Best resonance:     {:.6}",
        triton
            .best_signature()
            .map(|s| s.resonance())
            .unwrap_or(0.0)
    );
    println!(
        "  Final Betti:        {:?}",
        triton.betti_history.last().unwrap_or(&vec![])
    );

    // Spectral gap convergence
    if triton.spectral_gap_history.len() >= 10 {
        let last10: f64 = triton.spectral_gap_history[triton.spectral_gap_history.len() - 10..]
            .iter()
            .sum::<f64>()
            / 10.0;
        let first10: f64 = triton.spectral_gap_history[..10].iter().sum::<f64>() / 10.0;
        println!("  Gap trend:          {:.6} -> {:.6}", first10, last10);
    }

    // Spectral singularities in final mesh
    let spectral_sings = triton.spectral_singularities();
    println!(
        "  Spectral bottlenecks: {} vertices",
        spectral_sings.len()
    );

    println!("\n{}", "═".repeat(60));
    println!("Demo complete.");
}
