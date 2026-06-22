//! `enxame-tracker` — a BitTorrent tracker server core.
//!
//! The **registry** ([`Registry`]) is the testable, I/O-free heart: it
//! maps each swarm (info-hash) to its currently-announcing peers,
//! processes an [`AnnounceRequest`] into an [`AnnounceResponse`]
//! (registering/refreshing/removing the peer and selecting peers to
//! return), and reaps peers that stop announcing. The binary
//! (`src/main.rs`) is the HTTP shell. Same sans-io split as the rest of
//! ENXAME (`theory/ENXAME.md`).
//!
//! The response is built with the typed [`bencode`] AST — never a
//! `format!()` of the wire (★★ TYPED EMISSION).

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bencode::Bencode;

/// How often a peer is asked to re-announce (seconds).
pub const ANNOUNCE_INTERVAL: i64 = 1800;
/// Max compact peers returned per announce.
pub const MAX_PEERS: usize = 50;
/// A peer is reaped after this long without an announce.
pub const PEER_TTL: Duration = Duration::from_secs(2 * ANNOUNCE_INTERVAL as u64);

/// The `event` a peer reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    /// First announce for this swarm.
    Started,
    /// Graceful leave (the peer is removed).
    Stopped,
    /// Download finished (a leecher became a seeder).
    Completed,
    /// A periodic refresh.
    None,
}

impl AnnounceEvent {
    /// Parse the `event` query value.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "started" => Self::Started,
            "stopped" => Self::Stopped,
            "completed" => Self::Completed,
            _ => Self::None,
        }
    }
}

/// One announce from a peer.
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    /// 20-byte swarm identity.
    pub info_hash: [u8; 20],
    /// 20-byte peer id.
    pub peer_id: [u8; 20],
    /// The peer's reachable address (ip from the query or the socket; port from the query).
    pub addr: SocketAddr,
    /// Bytes left to download (`0` = seeder).
    pub left: u64,
    /// The reported event.
    pub event: AnnounceEvent,
}

/// The tracker's reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    /// Re-announce interval (seconds).
    pub interval: i64,
    /// Seeders in the swarm.
    pub complete: i64,
    /// Leechers in the swarm.
    pub incomplete: i64,
    /// Peers to try (excluding the announcer).
    pub peers: Vec<SocketAddr>,
}

impl AnnounceResponse {
    /// Bencode the response (compact IPv4 peers).
    #[must_use]
    pub fn to_bencode(&self) -> Vec<u8> {
        let mut compact = Vec::with_capacity(self.peers.len() * 6);
        for p in &self.peers {
            if let SocketAddr::V4(v4) = p {
                compact.extend_from_slice(&v4.ip().octets());
                compact.extend_from_slice(&v4.port().to_be_bytes());
            }
        }
        let mut dict = BTreeMap::new();
        dict.insert(b"interval".to_vec(), Bencode::Int(self.interval));
        dict.insert(b"complete".to_vec(), Bencode::Int(self.complete));
        dict.insert(b"incomplete".to_vec(), Bencode::Int(self.incomplete));
        dict.insert(b"peers".to_vec(), Bencode::Bytes(compact));
        Bencode::Dict(dict).to_bytes()
    }

    /// Bencode a `failure reason` reply.
    #[must_use]
    pub fn failure(reason: &str) -> Vec<u8> {
        let mut dict = BTreeMap::new();
        dict.insert(
            b"failure reason".to_vec(),
            Bencode::Bytes(reason.as_bytes().to_vec()),
        );
        Bencode::Dict(dict).to_bytes()
    }
}

struct PeerEntry {
    addr: SocketAddr,
    is_seeder: bool,
    last_seen: Instant,
}

/// The in-memory swarm registry.
#[derive(Default)]
pub struct Registry {
    swarms: HashMap<[u8; 20], HashMap<[u8; 20], PeerEntry>>,
}

impl Registry {
    /// A fresh, empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Process an announce: update the swarm and select peers to return.
    /// `now` is injected (testability — the same time-injection contract
    /// as the rest of the fleet).
    pub fn announce(&mut self, req: &AnnounceRequest, now: Instant) -> AnnounceResponse {
        let swarm = self.swarms.entry(req.info_hash).or_default();

        if req.event == AnnounceEvent::Stopped {
            swarm.remove(&req.peer_id);
        } else {
            swarm.insert(
                req.peer_id,
                PeerEntry {
                    addr: req.addr,
                    is_seeder: req.left == 0,
                    last_seen: now,
                },
            );
        }

        // Reap stale peers in this swarm.
        swarm.retain(|_, e| now.duration_since(e.last_seen) < PEER_TTL);

        let (mut complete, mut incomplete) = (0i64, 0i64);
        let mut peers = Vec::new();
        for (pid, entry) in swarm.iter() {
            if entry.is_seeder {
                complete += 1;
            } else {
                incomplete += 1;
            }
            if *pid != req.peer_id && peers.len() < MAX_PEERS {
                peers.push(entry.addr);
            }
        }

        AnnounceResponse {
            interval: ANNOUNCE_INTERVAL,
            complete,
            incomplete,
            peers,
        }
    }

    /// Number of swarms currently tracked.
    #[must_use]
    pub fn swarm_count(&self) -> usize {
        self.swarms.len()
    }

    /// Reap every swarm of stale peers (the periodic maintenance tick),
    /// dropping swarms that go empty.
    pub fn reap(&mut self, now: Instant) {
        self.swarms.retain(|_, swarm| {
            swarm.retain(|_, e| now.duration_since(e.last_seen) < PEER_TTL);
            !swarm.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(peer: u8, port: u16, left: u64, event: AnnounceEvent) -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [1u8; 20],
            peer_id: [peer; 20],
            addr: SocketAddr::from(([10, 0, 0, peer], port)),
            left,
            event,
        }
    }

    #[test]
    fn registers_peers_and_returns_others() {
        let mut r = Registry::new();
        let now = Instant::now();
        // Seeder A announces (empty swarm → no peers back).
        let resp = r.announce(&req(1, 6881, 0, AnnounceEvent::Started), now);
        assert_eq!(resp.peers, vec![]);
        assert_eq!(resp.complete, 1);
        assert_eq!(resp.incomplete, 0);
        // Leecher B announces → gets A back.
        let resp = r.announce(&req(2, 6882, 100, AnnounceEvent::Started), now);
        assert_eq!(resp.peers, vec![SocketAddr::from(([10, 0, 0, 1], 6881))]);
        assert_eq!(resp.complete, 1);
        assert_eq!(resp.incomplete, 1);
    }

    #[test]
    fn stopped_removes_the_peer() {
        let mut r = Registry::new();
        let now = Instant::now();
        r.announce(&req(1, 6881, 0, AnnounceEvent::Started), now);
        r.announce(&req(2, 6882, 0, AnnounceEvent::Started), now);
        let resp = r.announce(&req(1, 6881, 0, AnnounceEvent::Stopped), now);
        // After A stops, only B remains; A's own response excludes itself.
        assert_eq!(resp.complete, 1); // just B
    }

    #[test]
    fn reaps_stale_peers() {
        let mut r = Registry::new();
        let t0 = Instant::now();
        r.announce(&req(1, 6881, 0, AnnounceEvent::Started), t0);
        let later = t0 + PEER_TTL + Duration::from_secs(1);
        r.reap(later);
        assert_eq!(r.swarm_count(), 0);
    }

    #[test]
    fn response_bencodes_compact_peers() {
        let resp = AnnounceResponse {
            interval: 1800,
            complete: 1,
            incomplete: 2,
            peers: vec![SocketAddr::from(([1, 2, 3, 4], 6881))],
        };
        let bytes = resp.to_bencode();
        let parsed = bencode::parse(&bytes).unwrap();
        assert_eq!(
            parsed.get(b"interval").and_then(Bencode::as_int),
            Some(1800)
        );
        let peers = parsed.get(b"peers").and_then(Bencode::as_bytes).unwrap();
        assert_eq!(peers, &[1, 2, 3, 4, 0x1a, 0xe1]); // 6881 = 0x1ae1
    }
}
