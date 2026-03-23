//! Swarm demo: 3 PSE nodes on localhost sharing crystals via gossip.
//!
//! Run with: `cargo run --example swarm_demo -p pse-net`

use pse_net::{SwarmConfig, SwarmNode};
use pse_types::*;
use std::collections::BTreeMap;
use std::time::Duration;

/// Create a mock crystal with a given spectral gap and region.
fn make_crystal(gap: f64, region: Vec<VertexId>, tick: u64) -> SemanticCrystal {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(gap.to_le_bytes());
    h.update(tick.to_le_bytes());
    let r = h.finalize();
    let mut id = [0u8; 32];
    id.copy_from_slice(&r);

    SemanticCrystal {
        crystal_id: id,
        region,
        constraint_program: Vec::new(),
        stability_score: 0.85 + gap * 0.1,
        topology_signature: TopologySignature {
            betti_0: 1,
            betti_1: 0,
            betti_2: 0,
            spectral_gap: gap,
            euler_char: 1,
            cheeger_estimate: 0.3,
            kuramoto_coherence: 0.75 + gap * 0.2,
            mean_propagation_time: 1.0,
            dtl_connected: true,
        },
        betti_numbers: vec![1, 0, 0],
        evidence_chain: Vec::new(),
        commit_proof: CommitProof::default(),
        operator_versions: BTreeMap::new(),
        created_at: tick,
        free_energy: 0.1,
        carrier_instance_idx: 0,
        scale_tag: String::new(),
        universe_id: String::new(),
        sub_crystal_ids: Vec::new(),
        parent_crystal_ids: Vec::new(),
        genesis_metadata: None,
    }
}

fn main() {
    println!("PSE Distributed Swarm Demo");
    println!("{}", "═".repeat(60));

    // Create 3 nodes with different seeds
    let mut configs: Vec<SwarmConfig> = (0..3)
        .map(|i| {
            let mut c = SwarmConfig::default();
            c.node_seed = (i + 1) as u64;
            c.listen_addr = "127.0.0.1:0".to_string();
            c
        })
        .collect();

    let mut nodes: Vec<SwarmNode> = configs
        .drain(..)
        .map(|c| SwarmNode::new(c))
        .collect();

    // Start all nodes
    for (i, node) in nodes.iter_mut().enumerate() {
        node.start().expect("start node");
        let addr = node.local_addr().expect("addr");
        let id_hex: String = node.node_id.iter().take(4).map(|b| format!("{:02x}", b)).collect();
        println!("Node {} started: {} at {}", i + 1, id_hex, addr);
    }

    // Connect in ring topology: 1→2, 2→3, 3→1
    let addrs: Vec<String> = nodes.iter().map(|n| n.local_addr().unwrap().to_string()).collect();
    nodes[0].connect_peer(&addrs[1]).expect("1→2");
    nodes[1].connect_peer(&addrs[2]).expect("2→3");
    nodes[2].connect_peer(&addrs[0]).expect("3→1");
    println!("\nRing topology established: 1→2→3→1");

    std::thread::sleep(Duration::from_millis(200));

    // Each node creates and propagates crystals
    println!("\n{}", "─".repeat(60));
    println!("Phase 1: Crystal creation and propagation\n");

    let crystal_gaps = [
        vec![0.40, 0.42, 0.41],  // Node 1: tightly aligned
        vec![0.43, 0.44, 0.42],  // Node 2: tightly aligned, close to node 1
        vec![0.39, 0.41, 0.40],  // Node 3: tightly aligned, close to others
    ];

    for (node_idx, gaps) in crystal_gaps.iter().enumerate() {
        for (j, &gap) in gaps.iter().enumerate() {
            let crystal = make_crystal(gap, vec![(node_idx * 10 + j) as u64], j as u64 + 1);
            let sent = nodes[node_idx].propagate_crystal(crystal).expect("propagate");
            println!("  Node {} created crystal (gap={:.2}, sent to {} peers)", node_idx + 1, gap, sent);
        }
    }

    // Wait for gossip propagation
    println!("\nWaiting for gossip propagation...");
    std::thread::sleep(Duration::from_secs(2));

    // Report results
    println!("\n{}", "─".repeat(60));
    println!("Phase 2: Results\n");

    for (i, node) in nodes.iter().enumerate() {
        let accepted = node.accepted_crystals();
        let local = node.local_crystal_count();
        println!(
            "  Node {}: {} local crystals, {} accepted from network, {} peers",
            i + 1,
            local,
            accepted.len(),
            node.peer_count()
        );

        if !accepted.is_empty() {
            let gaps: Vec<f64> = accepted
                .iter()
                .map(|e| e.crystal.topology_signature.spectral_gap)
                .collect();
            println!("    Accepted spectral gaps: {:?}", gaps);
        }
    }

    // Compute swarm statistics
    println!("\n{}", "─".repeat(60));
    println!("Swarm Summary:\n");

    let total_local: usize = nodes.iter().map(|n| n.local_crystal_count()).sum();
    let total_accepted: usize = nodes.iter().map(|n| n.accepted_crystals().len()).sum();

    // Compute Kuramoto order parameter across all nodes' crystals
    let all_gaps: Vec<f64> = nodes
        .iter()
        .flat_map(|n| {
            n.accepted_crystals()
                .iter()
                .map(|e| e.crystal.topology_signature.spectral_gap * std::f64::consts::TAU)
                .collect::<Vec<_>>()
        })
        .collect();

    let (r, _psi) = if all_gaps.is_empty() {
        (0.0, 0.0)
    } else {
        pse_net::kuramoto_order_parameter(&all_gaps)
    };

    println!("  Total local crystals:    {}", total_local);
    println!("  Total network accepted:  {}", total_accepted);
    println!("  Kuramoto order param:    {:.4}", r);
    println!("  Consensus status:        {}", if r > 0.51 { "ALIGNED" } else { "DIVERGENT" });

    // Stop all nodes
    for node in &nodes {
        node.stop();
    }

    println!("\n{}", "═".repeat(60));
    println!("Demo complete.");
}
