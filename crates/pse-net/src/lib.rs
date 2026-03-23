//! # pse-net — Distributed Swarm Networking for PSE
//!
//! Provides TCP peer-to-peer crystal propagation for the Post-Symbolic Engine.
//! Multiple PSE instances can share crystals over the network using gossip-based
//! propagation with Kuramoto-inspired acceptance criteria.
//!
//! ## Architecture
//!
//! - **SwarmNode**: Main coordinator managing TCP connections and crystal flow
//! - **CrystalEnvelope**: Content-addressed wrapper for network-propagated crystals
//! - **Kuramoto acceptance**: Phase-based acceptance criterion using spectral gap alignment
//! - **Transport**: Length-prefixed JSON framing over TCP with rate limiting
//!
//! ## Example
//!
//! ```no_run
//! use pse_net::{SwarmNode, SwarmConfig};
//!
//! let mut config = SwarmConfig::default();
//! config.listen_addr = "127.0.0.1:0".to_string();
//! let mut node = SwarmNode::new(config);
//! node.start().expect("start");
//! println!("Listening on {:?}", node.local_addr());
//! ```

pub mod acceptance;
pub mod config;
pub mod envelope;
pub mod message;
pub mod node;
pub mod transport;

pub use acceptance::{accept_crystal, kuramoto_order_parameter};
pub use config::SwarmConfig;
pub use envelope::CrystalEnvelope;
pub use message::{PeerInfo, SwarmMessage};
pub use node::SwarmNode;
pub use transport::{recv_message, send_message, RateLimiter};
