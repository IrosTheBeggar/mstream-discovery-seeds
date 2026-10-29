// mstream-discovery-seed — a community bootstrap node for the mStream
// music-discovery network.
//
// WHAT A SEED IS: a well-known, always-on member of the network's gossip
// mesh (iroh-gossip over the catalog topic). Fresh mStream servers ship
// with seed tickets (mStream repo: seeds/discovery-seeds.json) and
// bootstrap off them; HyParView then weaves the newcomer into the real
// mesh and it learns actual peers. Seeds are training wheels, not hubs —
// after bootstrap, traffic doesn't depend on them.
//
// WHAT A SEED IS NOT: it holds no music, no snapshots, and never
// announces. It relays gossip like any mesh member. It cannot forge
// catalog entries (announcements are signed by their origins — see
// mStream's p2p-sidecar) and it cannot observe similarity queries (those
// never leave each user's machine).
//
// ── Cross-repo contract with mStream (p2p-sidecar/src/main.rs) ───────────
// CATALOG_TOPIC below and the iroh/iroh-gossip version pins in Cargo.toml
// must stay aligned with the mStream repo, or seeds go quietly deaf
// (symptom: neighbors=0 forever in the heartbeat log).
//
// OPERATIONS:
//   mstream-discovery-seed --data-dir /var/lib/mstream-seed
//     [--bootstrap <ticket>]...   mesh with other seeds (optional)
//     [--print-id]                print endpoint id and exit (CI self-test)
//     [--port <udp-port>]         bind this UDP port (default: a random one)
//
//   - identity.key in the data dir IS the seed's identity: back it up.
//     Lose it and every shipped ticket pointing here goes dead.
//   - On boot the seed prints its own ticket on stdout — that's the string
//     that goes into the seed list.
//   - stdin is deliberately ignored (systemd gives us /dev/null; mStream's
//     sidecar exits on stdin EOF, a seed must NOT). Stop with SIGTERM/^C.
//   - FIXED PORT (v1.5.0+): --port binds ONE UDP port (IPv4 required,
//     IPv6 best-effort) so the public address inside the printed ticket
//     survives restarts. The fleet publishes it (deploy/run-seed.sh
//     --port): shipped tickets then stay directly dialable. Without it
//     every restart picks a fresh random port, the direct addresses in
//     seeds/discovery-seeds.json go stale, and bootstrap is relay-only
//     (how the fleet ran from July to October 2026).
//   - LIVENESS (v1.4.0+): the gossip actor is one task; when it stops
//     turning the seed LOOKS alive — QUIC links up, "neighbors=N" steady —
//     while it answers no joins at all. The 2026-10-22 outage froze both
//     community seeds exactly like that for ~20 h (the 0.101.0 blocking
//     send, see Cargo.toml). Now a dead subscriber is re-created, and an
//     actor probe (a fresh subscribe must yield the NeighborUp replay
//     within 20 s; two misses in a row) exits with code 3 so the container
//     restart policy (deploy/run-seed.sh: --restart unless-stopped) brings
//     a fresh process. A seed with ZERO neighbors is the re-join loop's
//     business, not the probe's.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use iroh::{
    address_lookup::memory::MemoryLookup,
    endpoint::{presets, BindOpts},
    protocol::Router,
    Endpoint, EndpointId, SecretKey,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::Gossip,
    proto::TopicId,
};
use iroh_tickets::endpoint::EndpointTicket;
use tokio_stream::StreamExt;

// blake3("mstream/discovery/catalog/v1") — byte-identical to the sidecar's
// iroh_blobs::Hash::new(CATALOG_TOPIC) derivation.
const CATALOG_TOPIC: &[u8] = b"mstream/discovery/catalog/v1";
const HEARTBEAT: Duration = Duration::from_secs(60);
// Gossip liveness (see LIVENESS in the header). Same numbers as the sidecar.
const ACTOR_PROBE_INTERVAL: Duration = Duration::from_secs(60);
const ACTOR_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const ACTOR_PROBE_MAX_MISSES: u32 = 2;
const RESUBSCRIBE_MAX_ATTEMPTS: u32 = 10;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const EXIT_GOSSIP_DEAD: i32 = 3;

type Neighbors = Arc<std::sync::Mutex<HashSet<EndpointId>>>;

fn neighbor_count(neighbors: &Neighbors) -> usize {
    neighbors.lock().unwrap_or_else(|e| e.into_inner()).len()
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut data_dir: Option<PathBuf> = None;
    let mut bootstrap: Vec<String> = Vec::new();
    let mut print_id_only = false;
    let mut port: u16 = 0;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(args.next().context("--data-dir needs a value")?)),
            "--bootstrap" => bootstrap.push(args.next().context("--bootstrap needs a value")?),
            "--print-id" => print_id_only = true,
            "--port" => port = parse_port(&args.next().context("--port needs a value")?)?,
            other => bail!("unknown argument: {other}"),
        }
    }
    let data_dir = data_dir.context("--data-dir is required")?;
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data dir {}", data_dir.display()))?;

    if print_id_only {
        let key = load_or_create_identity(&data_dir)?;
        println!("{}", key.public());
        return Ok(());
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(data_dir, bootstrap, port))
}

// 0 = let the OS pick (the pre-v1.5.0 behaviour). Anything else is bound on
// both address families, see FIXED PORT in the header.
fn parse_port(s: &str) -> Result<u16> {
    s.parse::<u16>()
        .map_err(|_| anyhow!("--port must be a UDP port number 0-65535 (got '{s}')"))
}

async fn run(data_dir: PathBuf, bootstrap: Vec<String>, port: u16) -> Result<()> {
    let key = load_or_create_identity(&data_dir)?;
    let memory_lookup = MemoryLookup::new();
    let mut builder = Endpoint::builder(presets::N0)
        .secret_key(key)
        .address_lookup(memory_lookup.clone());
    if port != 0 {
        // IPv4 must bind — a seed nobody can reach over v4 is no seed, and a
        // port clash is better found at boot than as a silently random port.
        // IPv6 is best-effort: the fleet's containers have no IPv6, and
        // iroh's own default `[::]` bind is allowed to fail the same way.
        builder = builder
            .bind_addr(SocketAddr::from(([0, 0, 0, 0], port)))
            .map_err(|e| anyhow!("invalid IPv4 bind address for --port {port}: {e}"))?
            .bind_addr_with_opts(
                SocketAddr::from(([0u16; 8], port)),
                BindOpts::default().set_is_required(false),
            )
            .map_err(|e| anyhow!("invalid IPv6 bind address for --port {port}: {e}"))?;
    }
    let endpoint = builder
        .bind()
        .await
        .map_err(|e| anyhow!("endpoint bind failed (--port {port}): {e}"))?;
    if port != 0 {
        eprintln!("[seed] bound udp port {port} — publish it so the ticket's direct address stays dialable");
    }

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(iroh_gossip::ALPN, gossip.clone())
        .spawn();

    // Bounded wait for a home relay so the printed ticket carries relay
    // info (a seed on a public IP also gets direct addresses in it).
    let _ = tokio::time::timeout(Duration::from_secs(8), endpoint.online()).await;

    let ticket = EndpointTicket::new(endpoint.addr()).to_string();
    // stdout = the operator-facing facts (what goes into the seed list).
    println!("endpoint-id: {}", endpoint.id());
    println!("ticket: {ticket}");
    eprintln!("[seed] up — copy the ticket above into seeds/discovery-seeds.json (mStream repo)");

    // Bootstrap entries: endpoint tickets (dialable with zero external
    // lookup — seeds meshing with each other should use these) or bare
    // endpoint ids (resolved via n0 DNS).
    let mut ids: Vec<EndpointId> = Vec::new();
    for entry in &bootstrap {
        if let Ok(t) = EndpointTicket::from_str(entry) {
            let addr = t.endpoint_addr().clone();
            let id = addr.id;
            memory_lookup.add_endpoint_info(addr);
            ids.push(id);
        } else if let Ok(id) = EndpointId::from_str(entry) {
            ids.push(id);
        } else {
            bail!("bootstrap entry is neither an endpoint ticket nor an endpoint id: {entry}");
        }
    }
    ids.retain(|id| *id != endpoint.id());

    let topic = TopicId::from_bytes(*blake3::hash(CATALOG_TOPIC).as_bytes());
    // Non-blocking subscribe: the first seed in an empty network must come
    // up alone and simply wait to be everyone else's first contact.
    let handle = gossip.subscribe(topic, ids.clone()).await
        .map_err(|e| anyhow!("gossip subscribe failed: {e}"))?;
    let (sender, receiver) = handle.split();

    // Live mesh membership, tracked from the subscriber's NeighborUp/Down
    // events and rebuilt from the actor's replay whenever the subscriber is
    // re-created. Broadcast content is drained and ignored — the relay work
    // itself happens inside the gossip actor regardless, but the stream
    // must be consumed so it never backs up.
    let neighbors: Neighbors = Arc::new(std::sync::Mutex::new(HashSet::new()));
    tokio::spawn(subscriber_loop(gossip.clone(), topic, receiver, neighbors.clone()));
    tokio::spawn(actor_probe_loop(gossip.clone(), topic, neighbors.clone()));

    // Bootstrap re-join: the subscribe above fires its join dials exactly
    // once, and a dial that loses the boot race (the endpoint's network
    // path not ready yet) was never retried — the seed then waited
    // passively forever (issue #2; bit the v1.0.0 fleet rollout, where a
    // restarted seed sat at neighbors=0 until a manual restart gave it a
    // second attempt). While this seed KNOWS bootstrap peers and has NO
    // mesh at all, re-issue the join on a slow clock. join_peers is
    // idempotent (mStream's sidecar leans on that for its own re-joins),
    // it goes quiet the moment any neighbor exists, and checking
    // `neighbors == 0` rather than "until first success" also heals any
    // FUTURE total isolation — a sibling restarting on a calm mesh, not
    // just our own boot. First check at +30s so a healthy boot dial never
    // produces a redundant re-join or a noise line.
    if !ids.is_empty() {
        let n = neighbors.clone();
        let sender = sender.clone();
        let retry_ids = ids.clone();
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut tick = tokio::time::interval_at(start, Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if neighbor_count(&n) > 0 {
                    continue;
                }
                eprintln!("[seed] no neighbors — re-dialing {} bootstrap peer(s)", retry_ids.len());
                // Bounded: the sender's command channel is finite, so a
                // wedged actor would otherwise park this loop forever and
                // hide the wedge (the actor probe is what acts on it).
                match tokio::time::timeout(COMMAND_TIMEOUT, sender.join_peers(retry_ids.clone())).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => eprintln!("[seed] bootstrap re-join failed: {e}"),
                    Err(_) => eprintln!(
                        "[seed] bootstrap re-join timed out after {}s — the gossip actor is not accepting commands",
                        COMMAND_TIMEOUT.as_secs()
                    ),
                }
            }
        });
    }

    // Heartbeat for ops (grep the journal for "neighbors="); run until
    // SIGTERM / SIGINT. Deliberately NO stdin interaction of any kind.
    let mut tick = tokio::time::interval(HEARTBEAT);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                eprintln!("[seed] neighbors={}", neighbor_count(&neighbors));
            }
            _ = shutdown_signal() => break,
        }
    }

    eprintln!("[seed] shutting down");
    SHUTTING_DOWN.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = router.shutdown().await;
    endpoint.close().await;
    Ok(())
}

// Set on SIGTERM/^C: the gossip stream closing is then the shutdown itself,
// not a dead subscriber to re-create (and not a wedged actor to exit over).
static SHUTTING_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn shutting_down() -> bool {
    SHUTTING_DOWN.load(std::sync::atomic::Ordering::Relaxed)
}

// Drain the subscriber, and outlive it: the gossip API CLOSES a lagged
// subscriber and never revives a closed stream, while the topic itself
// stays joined through the GossipSender the re-join loop holds. So when the
// stream dies, subscribe again — the actor replays NeighborUp for every
// current neighbor into the fresh receiver, which also rebuilds the live
// set from scratch.
async fn subscriber_loop(gossip: Gossip, topic: TopicId, mut receiver: GossipReceiver, neighbors: Neighbors) {
    loop {
        let why = loop {
            match receiver.next().await {
                Some(Ok(Event::NeighborUp(id))) => {
                    neighbors.lock().unwrap_or_else(|e| e.into_inner()).insert(id);
                    eprintln!("[seed] neighbor up: {id}");
                }
                Some(Ok(Event::NeighborDown(id))) => {
                    neighbors.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                    eprintln!("[seed] neighbor down: {id}");
                }
                // Don't wait for the close that follows — the stream is dead
                // either way.
                Some(Ok(Event::Lagged)) => break "lagged — the actor closed the subscriber".to_string(),
                Some(Ok(_)) => {} // broadcast content (or a newer event kind) — not ours to read
                Some(Err(e)) => break format!("stream error: {e}"),
                None => break "stream closed".to_string(),
            }
        };
        if shutting_down() { return; }
        eprintln!("[seed] gossip subscriber ended ({why}) — re-subscribing");
        neighbors.lock().unwrap_or_else(|e| e.into_inner()).clear();
        let mut attempt = 0u32;
        receiver = loop {
            attempt += 1;
            match tokio::time::timeout(ACTOR_PROBE_TIMEOUT, gossip.subscribe(topic, Vec::new())).await {
                Ok(Ok(handle)) => break handle.split().1,
                Ok(Err(e)) => eprintln!("[seed] re-subscribe failed (attempt {attempt}): {e}"),
                Err(_) => eprintln!("[seed] re-subscribe timed out (attempt {attempt}) — gossip actor unresponsive"),
            }
            if attempt >= RESUBSCRIBE_MAX_ATTEMPTS {
                eprintln!(
                    "[seed] FATAL: could not re-subscribe to the catalog topic after {attempt} attempts — \
                     exiting with code {EXIT_GOSSIP_DEAD} for the restart policy"
                );
                std::process::exit(EXIT_GOSSIP_DEAD);
            }
            tokio::time::sleep(Duration::from_secs(1 << attempt.min(5))).await;
        };
    }
}

// The actor probe (see LIVENESS in the header): a fresh subscriber on a
// topic with neighbors gets the actor's NeighborUp replay as its first
// events, so a probe that sees no event in time proves the actor isn't
// turning. The probe subscriber is dropped right after; the topic stays
// joined through the other handles.
async fn actor_probe_loop(gossip: Gossip, topic: TopicId, neighbors: Neighbors) {
    let mut misses = 0u32;
    loop {
        tokio::time::sleep(ACTOR_PROBE_INTERVAL).await;
        if shutting_down() { return; }
        if neighbor_count(&neighbors) == 0 {
            misses = 0;
            continue;
        }
        let probe = async {
            let handle = gossip.subscribe(topic, Vec::new()).await?;
            let (_sender, mut rx) = handle.split();
            rx.next().await.transpose()
        };
        let alive = matches!(tokio::time::timeout(ACTOR_PROBE_TIMEOUT, probe).await, Ok(Ok(Some(_))));
        if alive {
            misses = 0;
            continue;
        }
        misses += 1;
        eprintln!(
            "[seed] gossip actor probe failed ({misses}/{ACTOR_PROBE_MAX_MISSES}): no event within {}s on a fresh subscriber",
            ACTOR_PROBE_TIMEOUT.as_secs()
        );
        if misses >= ACTOR_PROBE_MAX_MISSES {
            eprintln!(
                "[seed] FATAL: the gossip actor is wedged — exiting with code {EXIT_GOSSIP_DEAD} for the restart policy"
            );
            std::process::exit(EXIT_GOSSIP_DEAD);
        }
    }
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

// Same identity persistence as mStream's p2p-sidecar: 32 raw bytes,
// created on first run, 0600 on unix.
fn load_or_create_identity(data_dir: &Path) -> Result<SecretKey> {
    let key_path = data_dir.join("identity.key");
    if key_path.exists() {
        let bytes = std::fs::read(&key_path)
            .with_context(|| format!("reading {}", key_path.display()))?;
        let arr: [u8; 32] = bytes.as_slice().try_into()
            .map_err(|_| anyhow!("{} is corrupt (expected 32 bytes, got {})", key_path.display(), bytes.len()))?;
        return Ok(SecretKey::from_bytes(&arr));
    }
    let key = SecretKey::generate();
    std::fs::write(&key_path, key.to_bytes())
        .with_context(|| format!("writing {}", key_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
    }
    eprintln!("[seed] generated new identity at {} — BACK THIS FILE UP", key_path.display());
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::parse_port;

    #[test]
    fn port_parses_the_fleet_port_and_rejects_junk() {
        assert_eq!(parse_port("11204").unwrap(), 11204);
        assert_eq!(parse_port("0").unwrap(), 0, "0 keeps the random-port default");
        assert!(parse_port("65536").is_err());
        assert!(parse_port("-1").is_err());
        assert!(parse_port("udp").is_err());
        assert!(parse_port("").is_err());
    }
}
