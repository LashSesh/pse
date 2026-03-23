//! TCP transport layer for PSE swarm communication.
//!
//! Provides length-prefixed JSON framing over TCP and rate limiting.

use crate::message::SwarmMessage;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

/// Send a message over a TCP stream using length-prefixed JSON framing.
///
/// Frame format: `[4 bytes big-endian length][JSON payload]`
pub fn send_message(stream: &mut TcpStream, msg: &SwarmMessage) -> io::Result<()> {
    let json = serde_json::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = json.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&json)?;
    stream.flush()
}

/// Receive a message from a TCP stream using length-prefixed JSON framing.
///
/// Frame format: `[4 bytes big-endian length][JSON payload]`
pub fn recv_message(stream: &mut TcpStream) -> io::Result<SwarmMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check: reject frames larger than 16 MB
    if len > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", len),
        ));
    }

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;

    serde_json::from_slice(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Simple token-bucket rate limiter.
///
/// Allows up to `max_tokens` operations per second. Tokens refill each second.
pub struct RateLimiter {
    tokens: usize,
    max_tokens: usize,
    last_refill: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter with the given maximum tokens per second.
    pub fn new(max_per_sec: usize) -> Self {
        Self {
            tokens: max_per_sec,
            max_tokens: max_per_sec,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns `true` if allowed, `false` if rate-limited.
    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        if elapsed.as_secs() >= 1 {
            self.tokens = self.max_tokens;
            self.last_refill = now;
        }

        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let mut rl = RateLimiter::new(5);
        for _ in 0..5 {
            assert!(rl.allow());
        }
        // 6th should be denied
        assert!(!rl.allow());
    }

    #[test]
    fn test_send_recv_roundtrip() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let msg = SwarmMessage::Ping { nonce: 12345 };
        let msg_clone = msg.clone();

        let handle = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect");
            send_message(&mut stream, &msg_clone).expect("send");
        });

        let (mut conn, _) = listener.accept().expect("accept");
        conn.set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .expect("set timeout");
        let received = recv_message(&mut conn).expect("recv");

        handle.join().expect("join");

        match received {
            SwarmMessage::Ping { nonce } => assert_eq!(nonce, 12345),
            _ => panic!("unexpected message type"),
        }
    }
}
