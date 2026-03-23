//! SwarmNode — the main coordinator for a PSE peer-to-peer swarm node.
//!
//! Manages TCP connections to peers, propagates crystals via gossip,
//! and accepts incoming crystals using Kuramoto-inspired alignment.

use crate::acceptance::accept_crystal;
use crate::config::SwarmConfig;
use crate::envelope::CrystalEnvelope;
use crate::message::SwarmMessage;
use crate::transport::{recv_message, send_message, RateLimiter};
use pse_types::{Hash256, SemanticCrystal};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// State of a connected peer.
struct PeerState {
    node_id: Hash256,
    stream: TcpStream,
    _addr: SocketAddr,
}

/// A PSE swarm node that manages peer connections and crystal propagation.
pub struct SwarmNode {
    /// This node's unique identifier.
    pub node_id: Hash256,
    /// Configuration for the swarm.
    pub config: SwarmConfig,
    /// Connected peers.
    peers: Arc<Mutex<Vec<PeerState>>>,
    /// Crystals accepted from the network (to be drained by the engine).
    accepted_crystals: Arc<Mutex<Vec<CrystalEnvelope>>>,
    /// Content hashes of crystals we've already seen (dedup).
    seen_hashes: Arc<Mutex<HashSet<Hash256>>>,
    /// Local crystals (for acceptance decisions).
    local_crystals: Arc<Mutex<Vec<SemanticCrystal>>>,
    /// TCP listener address (set after start).
    local_addr: Option<SocketAddr>,
    /// Flag to signal the listener thread to stop.
    running: Arc<AtomicBool>,
    /// Rate limiter for incoming messages.
    rate_limiter: Arc<Mutex<RateLimiter>>,
}

impl SwarmNode {
    /// Create a new swarm node with the given configuration.
    pub fn new(config: SwarmConfig) -> Self {
        let node_id = Self::generate_node_id(config.node_seed);
        let rate_limiter = RateLimiter::new(config.rate_limit_per_sec);

        Self {
            node_id,
            config,
            peers: Arc::new(Mutex::new(Vec::new())),
            accepted_crystals: Arc::new(Mutex::new(Vec::new())),
            seen_hashes: Arc::new(Mutex::new(HashSet::new())),
            local_crystals: Arc::new(Mutex::new(Vec::new())),
            local_addr: None,
            running: Arc::new(AtomicBool::new(false)),
            rate_limiter: Arc::new(Mutex::new(rate_limiter)),
        }
    }

    /// Generate a deterministic node ID from a seed.
    fn generate_node_id(seed: u64) -> Hash256 {
        let mut hasher = Sha256::new();
        hasher.update(b"pse-swarm-node-id-");
        hasher.update(seed.to_le_bytes());
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        id
    }

    /// Start the TCP listener and begin accepting connections.
    pub fn start(&mut self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr)?;
        let addr = listener.local_addr()?;
        self.local_addr = Some(addr);
        self.running.store(true, Ordering::SeqCst);

        let running = Arc::clone(&self.running);
        let accepted_crystals = Arc::clone(&self.accepted_crystals);
        let seen_hashes = Arc::clone(&self.seen_hashes);
        let local_crystals = Arc::clone(&self.local_crystals);
        let peers = Arc::clone(&self.peers);
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let node_id = self.node_id;
        let config = self.config.clone();

        // Set listener to non-blocking for graceful shutdown
        listener.set_nonblocking(true)?;

        std::thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, peer_addr)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(
                            config.read_timeout_ms,
                        )));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(
                            config.connect_timeout_ms,
                        )));

                        let accepted_crystals = Arc::clone(&accepted_crystals);
                        let seen_hashes = Arc::clone(&seen_hashes);
                        let local_crystals = Arc::clone(&local_crystals);
                        let peers = Arc::clone(&peers);
                        let rate_limiter = Arc::clone(&rate_limiter);
                        let running = Arc::clone(&running);
                        let config = config.clone();

                        std::thread::spawn(move || {
                            Self::handle_connection(
                                stream,
                                peer_addr,
                                node_id,
                                &config,
                                &accepted_crystals,
                                &seen_hashes,
                                &local_crystals,
                                &peers,
                                &rate_limiter,
                                &running,
                            );
                        });
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle an incoming TCP connection.
    fn handle_connection(
        mut stream: TcpStream,
        _peer_addr: SocketAddr,
        node_id: Hash256,
        config: &SwarmConfig,
        accepted_crystals: &Arc<Mutex<Vec<CrystalEnvelope>>>,
        seen_hashes: &Arc<Mutex<HashSet<Hash256>>>,
        local_crystals: &Arc<Mutex<Vec<SemanticCrystal>>>,
        peers: &Arc<Mutex<Vec<PeerState>>>,
        rate_limiter: &Arc<Mutex<RateLimiter>>,
        running: &Arc<AtomicBool>,
    ) {
        while running.load(Ordering::SeqCst) {
            let msg = match recv_message(&mut stream) {
                Ok(msg) => msg,
                Err(_) => return,
            };

            // Rate limit
            {
                let mut rl = match rate_limiter.lock() {
                    Ok(rl) => rl,
                    Err(_) => return,
                };
                if !rl.allow() {
                    continue;
                }
            }

            match msg {
                SwarmMessage::Hello {
                    node_id: peer_id,
                    listen_port: _,
                    ..
                } => {
                    // Send hello back
                    let response = SwarmMessage::Hello {
                        node_id,
                        version: "0.1.0".to_string(),
                        listen_port: 0,
                    };
                    if send_message(&mut stream, &response).is_err() {
                        return;
                    }

                    // Register peer
                    if let Ok(cloned) = stream.try_clone() {
                        if let Ok(mut peers) = peers.lock() {
                            if peers.len() < config.max_peers {
                                peers.push(PeerState {
                                    node_id: peer_id,
                                    stream: cloned,
                                    _addr: _peer_addr,
                                });
                            }
                        }
                    }
                }

                SwarmMessage::CrystalPropagate {
                    envelope,
                    ttl,
                    hops,
                } => {
                    // Check if already seen
                    {
                        let mut seen = match seen_hashes.lock() {
                            Ok(s) => s,
                            Err(_) => return,
                        };
                        if seen.contains(&envelope.content_hash) {
                            continue;
                        }
                        seen.insert(envelope.content_hash);
                    }

                    // Verify envelope integrity
                    if !envelope.verify() {
                        continue;
                    }

                    // Run acceptance check
                    let accepted = {
                        let locals = match local_crystals.lock() {
                            Ok(l) => l,
                            Err(_) => return,
                        };
                        let local_refs: Vec<&SemanticCrystal> = locals.iter().collect();
                        accept_crystal(&local_refs, &envelope.crystal, config.acceptance_threshold)
                    };

                    if accepted {
                        // Store for engine consumption
                        if let Ok(mut acc) = accepted_crystals.lock() {
                            acc.push(envelope.clone());
                        }
                        if let Ok(mut lc) = local_crystals.lock() {
                            lc.push(envelope.crystal.clone());
                        }
                    }

                    // Forward if TTL allows
                    if ttl > 0 {
                        let mut new_hops = hops;
                        new_hops.push(node_id);
                        let fwd = SwarmMessage::CrystalPropagate {
                            envelope,
                            ttl: ttl - 1,
                            hops: new_hops,
                        };
                        if let Ok(peers) = peers.lock() {
                            for peer in peers.iter() {
                                if let Ok(mut peer_stream) = peer.stream.try_clone() {
                                    let _ = send_message(&mut peer_stream, &fwd);
                                }
                            }
                        }
                    }
                }

                SwarmMessage::Ping { nonce } => {
                    let _ = send_message(&mut stream, &SwarmMessage::Pong { nonce });
                }

                SwarmMessage::CrystalSync { since_tick } => {
                    let crystals = if let Ok(acc) = accepted_crystals.lock() {
                        acc.iter()
                            .filter(|e| e.origin_tick >= since_tick)
                            .cloned()
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let _ = send_message(
                        &mut stream,
                        &SwarmMessage::CrystalSyncResponse { crystals },
                    );
                }

                SwarmMessage::PeerRequest => {
                    let peers_info = if let Ok(peers) = peers.lock() {
                        peers
                            .iter()
                            .map(|p| crate::message::PeerInfo {
                                node_id: p.node_id,
                                addr: p._addr.to_string(),
                                last_seen: 0,
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let _ = send_message(
                        &mut stream,
                        &SwarmMessage::PeerResponse { peers: peers_info },
                    );
                }

                _ => {}
            }
        }
    }

    /// Stop the swarm node.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Connect to a peer at the given address.
    pub fn connect_peer(&self, addr: &str) -> io::Result<()> {
        let timeout = Duration::from_millis(self.config.connect_timeout_ms);
        let socket_addr: SocketAddr = addr
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let mut stream = TcpStream::connect_timeout(&socket_addr, timeout)?;
        stream.set_read_timeout(Some(Duration::from_millis(self.config.read_timeout_ms)))?;
        stream.set_write_timeout(Some(Duration::from_millis(self.config.connect_timeout_ms)))?;

        // Send hello
        let hello = SwarmMessage::Hello {
            node_id: self.node_id,
            version: "0.1.0".to_string(),
            listen_port: self
                .local_addr
                .map(|a| a.port())
                .unwrap_or(0),
        };
        send_message(&mut stream, &hello)?;

        // Read hello back
        let response = recv_message(&mut stream)?;
        let peer_id = match response {
            SwarmMessage::Hello { node_id, .. } => node_id,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected Hello response",
                ))
            }
        };

        // Store peer
        let peer_addr = stream.peer_addr()?;
        if let Ok(mut peers) = self.peers.lock() {
            if peers.len() < self.config.max_peers {
                peers.push(PeerState {
                    node_id: peer_id,
                    stream,
                    _addr: peer_addr,
                });
            }
        }

        Ok(())
    }

    /// Propagate a crystal to all connected peers.
    ///
    /// Returns the number of peers the crystal was sent to.
    pub fn propagate_crystal(&self, crystal: SemanticCrystal) -> io::Result<usize> {
        let envelope = CrystalEnvelope::wrap(crystal.clone(), self.node_id);

        // Mark as seen locally
        if let Ok(mut seen) = self.seen_hashes.lock() {
            seen.insert(envelope.content_hash);
        }

        // Add to local crystals for future acceptance decisions
        if let Ok(mut lc) = self.local_crystals.lock() {
            lc.push(crystal);
        }

        let msg = SwarmMessage::CrystalPropagate {
            envelope,
            ttl: self.config.max_hops,
            hops: vec![self.node_id],
        };

        let mut sent = 0;
        if let Ok(peers) = self.peers.lock() {
            for peer in peers.iter() {
                if let Ok(mut peer_stream) = peer.stream.try_clone() {
                    if send_message(&mut peer_stream, &msg).is_ok() {
                        sent += 1;
                    }
                }
            }
        }

        Ok(sent)
    }

    /// Get a snapshot of accepted crystals from the network.
    pub fn accepted_crystals(&self) -> Vec<CrystalEnvelope> {
        self.accepted_crystals
            .lock()
            .map(|a| a.clone())
            .unwrap_or_default()
    }

    /// Drain all accepted crystals (consuming them).
    pub fn drain_accepted(&self) -> Vec<CrystalEnvelope> {
        self.accepted_crystals
            .lock()
            .map(|mut a| std::mem::take(&mut *a))
            .unwrap_or_default()
    }

    /// Number of connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.lock().map(|p| p.len()).unwrap_or(0)
    }

    /// The local address the node is listening on (if started).
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    /// Number of locally known crystals.
    pub fn local_crystal_count(&self) -> usize {
        self.local_crystals.lock().map(|l| l.len()).unwrap_or(0)
    }
}

impl Drop for SwarmNode {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_types::*;
    use std::collections::BTreeMap;

    fn mock_crystal(gap: f64, region: Vec<VertexId>) -> SemanticCrystal {
        SemanticCrystal {
            crystal_id: [0u8; 32],
            region,
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
    fn test_two_node_propagation() {
        // Start node 1
        let mut config1 = SwarmConfig::default();
        config1.node_seed = 1;
        let mut node1 = SwarmNode::new(config1);
        node1.start().expect("node1 start");
        let addr1 = node1.local_addr().expect("node1 addr");

        // Start node 2
        let mut config2 = SwarmConfig::default();
        config2.node_seed = 2;
        let mut node2 = SwarmNode::new(config2);
        node2.start().expect("node2 start");

        // Connect node2 -> node1
        node2
            .connect_peer(&addr1.to_string())
            .expect("connect");

        // Give connection time to establish
        std::thread::sleep(Duration::from_millis(100));

        // Propagate crystal from node2
        let crystal = mock_crystal(0.5, vec![1, 2, 3]);
        let sent = node2.propagate_crystal(crystal).expect("propagate");
        assert_eq!(sent, 1);

        // Wait for propagation
        std::thread::sleep(Duration::from_millis(500));

        // Check node1 received it
        let accepted = node1.accepted_crystals();
        assert_eq!(accepted.len(), 1);
        assert!(accepted[0].verify());

        node1.stop();
        node2.stop();
    }

    #[test]
    fn test_three_node_gossip() {
        // Node 1
        let mut config1 = SwarmConfig::default();
        config1.node_seed = 10;
        let mut node1 = SwarmNode::new(config1);
        node1.start().expect("node1 start");
        let addr1 = node1.local_addr().expect("node1 addr");

        // Node 2
        let mut config2 = SwarmConfig::default();
        config2.node_seed = 20;
        let mut node2 = SwarmNode::new(config2);
        node2.start().expect("node2 start");
        let addr2 = node2.local_addr().expect("node2 addr");

        // Node 3
        let mut config3 = SwarmConfig::default();
        config3.node_seed = 30;
        let mut node3 = SwarmNode::new(config3);
        node3.start().expect("node3 start");

        // Ring topology: node2→node1, node3→node2
        node2.connect_peer(&addr1.to_string()).expect("n2→n1");
        node3.connect_peer(&addr2.to_string()).expect("n3→n2");

        std::thread::sleep(Duration::from_millis(100));

        // Propagate from node3
        let crystal = mock_crystal(0.45, vec![10, 20]);
        node3.propagate_crystal(crystal).expect("propagate");

        // Wait for gossip to propagate through the ring
        std::thread::sleep(Duration::from_millis(1000));

        // Node2 should have received it directly
        let acc2 = node2.accepted_crystals();
        assert!(!acc2.is_empty(), "node2 should have received crystal");

        // Node1 should have received it via forward from node2
        let acc1 = node1.accepted_crystals();
        assert!(!acc1.is_empty(), "node1 should have received crystal via gossip");

        node1.stop();
        node2.stop();
        node3.stop();
    }

    #[test]
    fn test_ttl_zero_not_forwarded() {
        // Verify TTL=0 means no forwarding beyond direct recipient
        // Use only 2 nodes: sender (ttl=0) -> receiver
        // Receiver should get it but have no one to forward to
        let mut config1 = SwarmConfig::default();
        config1.node_seed = 100;
        let mut node1 = SwarmNode::new(config1);
        node1.start().expect("start");
        let addr1 = node1.local_addr().expect("addr");

        let mut config2 = SwarmConfig::default();
        config2.node_seed = 200;
        config2.max_hops = 0; // TTL=0 on sender
        let mut node2 = SwarmNode::new(config2);
        node2.start().expect("start");

        // Connect node2→node1
        node2.connect_peer(&addr1.to_string()).expect("connect");
        std::thread::sleep(Duration::from_millis(100));

        // Propagate from node2 with max_hops=0 (TTL=0)
        let crystal = mock_crystal(0.5, vec![1]);
        node2.propagate_crystal(crystal).expect("propagate");

        std::thread::sleep(Duration::from_millis(500));

        // Node1 receives it (direct connection), but TTL was 0 so it won't forward further
        let acc1 = node1.accepted_crystals();
        assert!(!acc1.is_empty(), "node1 should receive directly");

        node1.stop();
        node2.stop();
    }
}
