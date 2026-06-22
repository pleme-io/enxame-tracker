//! `enxame-tracker` server — the HTTP shell around [`enxame_tracker::Registry`].
//!
//! A tokio accept loop that parses `GET /announce?…` (and a minimal
//! `/scrape`), drives the testable registry, and writes a bencoded
//! reply. Dependency-light by design (no framework for two routes); the
//! protocol correctness lives in the unit-tested `lib.rs` core. Deploy
//! as a caixa Servico (`theory/ENXAME.md` L3).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use enxame_tracker::{AnnounceEvent, AnnounceRequest, Registry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<()> {
    let bind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:6969".into());
    let registry = Arc::new(Mutex::new(Registry::new()));

    // Periodic reaper.
    {
        let reg = Arc::clone(&registry);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                reg.lock().await.reap(Instant::now());
            }
        });
    }

    let listener = TcpListener::bind(&bind).await?;
    eprintln!("enxame-tracker listening on http://{bind}/announce");
    loop {
        let (stream, peer) = listener.accept().await?;
        let reg = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) = serve(stream, peer, reg).await {
                eprintln!("enxame-tracker: connection {peer} error: {e}");
            }
        });
    }
}

async fn serve(
    mut stream: TcpStream,
    peer: SocketAddr,
    registry: Arc<Mutex<Registry>>,
) -> Result<()> {
    // Read just the request line + headers (the announce is a GET, no body).
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 8192 {
            break;
        }
    }
    let request_line = buf.split(|&b| b == b'\r').next().unwrap_or(&[]);
    let target = parse_request_target(request_line);

    let body = match target {
        Some((path, query)) if path == b"/announce" => match build_announce(&query, peer) {
            Ok(req) => registry
                .lock()
                .await
                .announce(&req, Instant::now())
                .to_bencode(),
            Err(reason) => enxame_tracker::AnnounceResponse::failure(reason),
        },
        Some((path, _)) if path == b"/scrape" => {
            // Minimal scrape: an empty files dict (per-hash stats are a
            // follow-up). Keeps scrape clients from erroring.
            b"d5:filesdee".to_vec()
        }
        _ => enxame_tracker::AnnounceResponse::failure("unknown endpoint"),
    };

    let mut resp = Vec::with_capacity(body.len() + 96);
    resp.extend_from_slice(b"HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ");
    resp.extend_from_slice(body.len().to_string().as_bytes());
    resp.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    resp.extend_from_slice(&body);
    stream.write_all(&resp).await?;
    Ok(())
}

/// Extract `(path, query)` from a `GET <target> HTTP/1.x` request line.
fn parse_request_target(line: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut parts = line.split(|&b| b == b' ');
    let _method = parts.next()?;
    let target = parts.next()?;
    let (path, query) = match target.iter().position(|&b| b == b'?') {
        Some(i) => (target[..i].to_vec(), target[i + 1..].to_vec()),
        None => (target.to_vec(), Vec::new()),
    };
    Some((path, query))
}

/// Turn an announce query into a typed [`AnnounceRequest`], using the
/// connecting socket's IP when the query doesn't override it.
fn build_announce(query: &[u8], peer: SocketAddr) -> Result<AnnounceRequest, &'static str> {
    let mut info_hash: Option<[u8; 20]> = None;
    let mut peer_id: Option<[u8; 20]> = None;
    let mut port: Option<u16> = None;
    let mut left: u64 = 0;
    let mut event = AnnounceEvent::None;
    let mut ip_override: Option<std::net::IpAddr> = None;

    for pair in query.split(|&b| b == b'&') {
        let (key, value) = match pair.iter().position(|&b| b == b'=') {
            Some(i) => (&pair[..i], &pair[i + 1..]),
            None => (pair, &b""[..]),
        };
        let decoded = percent_decode(value);
        match key {
            b"info_hash" => info_hash = decoded.try_into().ok(),
            b"peer_id" => peer_id = decoded.try_into().ok(),
            b"port" => port = ascii_u64(&decoded).and_then(|n| u16::try_from(n).ok()),
            b"left" => left = ascii_u64(&decoded).unwrap_or(0),
            b"event" => event = AnnounceEvent::parse(&String::from_utf8_lossy(&decoded)),
            b"ip" => ip_override = String::from_utf8_lossy(&decoded).parse().ok(),
            _ => {}
        }
    }

    let info_hash = info_hash.ok_or("missing or malformed info_hash")?;
    let peer_id = peer_id.ok_or("missing or malformed peer_id")?;
    let port = port.ok_or("missing port")?;
    let ip = ip_override.unwrap_or_else(|| peer.ip());
    Ok(AnnounceRequest {
        info_hash,
        peer_id,
        addr: SocketAddr::new(ip, port),
        left,
        event,
    })
}

fn percent_decode(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'%' && i + 2 < s.len() {
            if let (Some(hi), Some(lo)) = (hex(s[i + 1]), hex(s[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(if s[i] == b'+' { b' ' } else { s[i] });
        i += 1;
    }
    out
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn ascii_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_target() {
        let (path, query) = parse_request_target(b"GET /announce?info_hash=x HTTP/1.1").unwrap();
        assert_eq!(path, b"/announce");
        assert_eq!(query, b"info_hash=x");
    }

    #[test]
    fn builds_announce_from_query() {
        // info_hash + peer_id as 20 %-encoded bytes each (0x41 = 'A').
        let ih: String = "%41".repeat(20);
        let pid: String = "%42".repeat(20);
        let q = format!("info_hash={ih}&peer_id={pid}&port=6881&left=100&event=started");
        let req = build_announce(q.as_bytes(), "1.2.3.4:9999".parse().unwrap()).unwrap();
        assert_eq!(req.info_hash, [0x41u8; 20]);
        assert_eq!(req.peer_id, [0x42u8; 20]);
        assert_eq!(req.addr, "1.2.3.4:6881".parse().unwrap()); // ip from socket, port from query
        assert_eq!(req.left, 100);
        assert_eq!(req.event, AnnounceEvent::Started);
    }

    #[test]
    fn rejects_missing_fields() {
        assert!(build_announce(b"port=6881", "1.2.3.4:9999".parse().unwrap()).is_err());
    }
}
