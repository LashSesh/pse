//! Kuramoto-inspired crystal acceptance criterion for the PSE swarm.
//!
//! When a crystal arrives from a peer, the node must decide whether to accept it.
//! This module implements acceptance based on the Kuramoto order parameter:
//! each crystal's `spectral_gap` is mapped to a phase angle, and the collective
//! synchronization (order parameter r) determines acceptance.

use pse_types::SemanticCrystal;

/// Compute the Kuramoto order parameter from a set of phase angles.
///
/// Returns `(r, psi)` where `r` is the coherence (0 = incoherent, 1 = fully synchronized)
/// and `psi` is the mean phase. This is the same math as `pse_topology::kuramoto_order_parameter`
/// but operates on bare `&[f64]` without requiring a `PersistentGraph`.
pub fn kuramoto_order_parameter(phases: &[f64]) -> (f64, f64) {
    if phases.is_empty() {
        return (0.0, 0.0);
    }
    let n = phases.len() as f64;
    let re: f64 = phases.iter().map(|phi| phi.cos()).sum::<f64>() / n;
    let im: f64 = phases.iter().map(|phi| phi.sin()).sum::<f64>() / n;
    let r = (re * re + im * im).sqrt();
    let psi = im.atan2(re);
    (r, psi)
}

/// Acceptance criterion for incoming crystals.
///
/// Models each crystal's `topology_signature.spectral_gap` as a phase oscillator.
/// The incoming crystal is accepted if adding its phase would not decrease
/// the Kuramoto order parameter below the `acceptance_threshold`, or if the
/// order parameter does not decrease.
///
/// # Arguments
/// - `local_crystals`: references to crystals already accepted by this node
/// - `incoming`: the crystal being evaluated for acceptance
/// - `threshold`: minimum required Kuramoto order parameter (e.g., 0.51)
///
/// # Returns
/// `true` if the crystal should be accepted into the local store.
pub fn accept_crystal(
    local_crystals: &[&SemanticCrystal],
    incoming: &SemanticCrystal,
    threshold: f64,
) -> bool {
    // Map spectral_gap to phase angle (TAU maps [0,1] gap to full circle)
    let local_phases: Vec<f64> = local_crystals
        .iter()
        .map(|c| c.topology_signature.spectral_gap * std::f64::consts::TAU)
        .collect();

    let incoming_phase = incoming.topology_signature.spectral_gap * std::f64::consts::TAU;

    // If no local crystals, always accept the first one
    if local_phases.is_empty() {
        return true;
    }

    // Compute current order parameter
    let (r_before, _) = kuramoto_order_parameter(&local_phases);

    // Compute order parameter with incoming crystal added
    let mut extended = local_phases;
    extended.push(incoming_phase);
    let (r_after, _) = kuramoto_order_parameter(&extended);

    // Accept if the new order parameter stays above threshold
    // OR if it doesn't decrease (new crystal aligns with existing ones)
    r_after >= threshold || r_after >= r_before
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_types::*;
    use std::collections::BTreeMap;

    fn crystal_with_gap(gap: f64) -> SemanticCrystal {
        SemanticCrystal {
            crystal_id: [0u8; 32],
            region: vec![1],
            constraint_program: Vec::new(),
            stability_score: 0.9,
            topology_signature: TopologySignature {
                betti_0: 1,
                betti_1: 0,
                betti_2: 0,
                spectral_gap: gap,
                euler_char: 1,
                cheeger_estimate: 0.3,
                kuramoto_coherence: 0.8,
                mean_propagation_time: 1.0,
                dtl_connected: true,
            },
            betti_numbers: vec![1, 0, 0],
            evidence_chain: Vec::new(),
            commit_proof: CommitProof::default(),
            operator_versions: BTreeMap::new(),
            created_at: 1,
            free_energy: 0.1,
            carrier_instance_idx: 0,
            scale_tag: String::new(),
            universe_id: String::new(),
            sub_crystal_ids: Vec::new(),
            parent_crystal_ids: Vec::new(),
            genesis_metadata: None,
        }
    }

    #[test]
    fn test_kuramoto_order_parameter_empty() {
        let (r, _) = kuramoto_order_parameter(&[]);
        assert_eq!(r, 0.0);
    }

    #[test]
    fn test_kuramoto_order_parameter_synchronized() {
        // All same phase -> r ≈ 1.0
        let phases = vec![1.0, 1.0, 1.0, 1.0];
        let (r, _) = kuramoto_order_parameter(&phases);
        assert!((r - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_kuramoto_order_parameter_desynchronized() {
        // Evenly spread phases -> r ≈ 0.0
        use std::f64::consts::TAU;
        let n = 100;
        let phases: Vec<f64> = (0..n).map(|i| i as f64 * TAU / n as f64).collect();
        let (r, _) = kuramoto_order_parameter(&phases);
        assert!(r < 0.05);
    }

    #[test]
    fn test_accept_first_crystal() {
        let incoming = crystal_with_gap(0.5);
        assert!(accept_crystal(&[], &incoming, 0.51));
    }

    #[test]
    fn test_accept_aligned_crystal() {
        // All crystals have similar spectral gaps -> high order parameter
        let c1 = crystal_with_gap(0.40);
        let c2 = crystal_with_gap(0.42);
        let c3 = crystal_with_gap(0.41);
        let locals: Vec<&SemanticCrystal> = vec![&c1, &c2, &c3];

        let incoming = crystal_with_gap(0.43);
        assert!(accept_crystal(&locals, &incoming, 0.51));
    }

    #[test]
    fn test_reject_misaligned_crystal() {
        // Existing crystals are tightly aligned; incoming is very different
        let c1 = crystal_with_gap(0.10);
        let c2 = crystal_with_gap(0.10);
        let c3 = crystal_with_gap(0.10);
        let c4 = crystal_with_gap(0.10);
        let c5 = crystal_with_gap(0.10);
        let locals: Vec<&SemanticCrystal> = vec![&c1, &c2, &c3, &c4, &c5];

        // Incoming crystal has very different spectral gap
        let incoming = crystal_with_gap(0.60);
        let result = accept_crystal(&locals, &incoming, 0.99);
        // With threshold=0.99, the order parameter after adding a misaligned
        // crystal will drop below threshold AND below r_before
        assert!(!result);
    }
}
