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
//
//   - identity.key in the data dir IS the seed's identity: back it up.
//     Lose it and every shipped ticket pointing here goes dead.
//   - On boot the seed prints its own ticket on stdout — that's the string
//     that goes into the seed list.
//   - stdin is deliberately ignored (systemd gives us /dev/null; mStream's
//     sidecar exits on stdin EOF, a seed must NOT). Stop with SIGTERM/^C.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use iroh::{
    address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router, Endpoint,
    EndpointId, SecretKey,
};
use iroh_gossip::{api::Event, net::Gossip, proto::TopicId};
use iroh_tickets::endpoint::EndpointTicket;
use tokio_stream::StreamExt;

// blake3("mstream/discovery/catalog/v1") — byte-identical to the sidecar's
// iroh_blobs::Hash::new(CATALOG_TOPIC) derivation.
const CATALOG_TOPIC: &[u8] = b"mstream/discovery/catalog/v1";
const HEARTBEAT: Duration = Duration::from_secs(60);

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut data_dir: Option<PathBuf> = None;
    let mut bootstrap: Vec<String> = Vec::new();
    let mut print_id_only = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(args.next().context("--data-dir needs a value")?)),
            "--bootstrap" => bootstrap.push(args.next().context("--bootstrap needs a value")?),
            "--print-id" => print_id_only = true,
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
        .block_on(run(data_dir, bootstrap))
}

async fn run(data_dir: PathBuf, bootstrap: Vec<String>) -> Result<()> {
    let key = load_or_create_identity(&data_dir)?;
    let memory_lookup = MemoryLookup::new();
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(key)
        .address_lookup(memory_lookup.clone())
        .bind()
        .await
        .map_err(|e| anyhow!("endpoint bind failed: {e}"))?;

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
    let (sender, mut receiver) = handle.split();

    // Count mesh membership; drain (and ignore) broadcast content — the
    // relay work itself happens inside the gossip actor regardless, but the
    // stream must be consumed so it never backs up.
    let neighbors = Arc::new(AtomicI64::new(0));
    let n = neighbors.clone();
    tokio::spawn(async move {
        while let Some(event) = receiver.next().await {
            match event {
                Ok(Event::NeighborUp(id)) => {
                    n.fetch_add(1, Ordering::Relaxed);
                    eprintln!("[seed] neighbor up: {id}");
                }
                Ok(Event::NeighborDown(id)) => {
                    n.fetch_sub(1, Ordering::Relaxed);
                    eprintln!("[seed] neighbor down: {id}");
                }
                Ok(_) => {} // broadcast content — not ours to read
                Err(e) => {
                    eprintln!("[seed] gossip stream error: {e}");
                    break;
                }
            }
        }
    });

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
                if n.load(Ordering::Relaxed) > 0 {
                    continue;
                }
                eprintln!("[seed] no neighbors — re-dialing {} bootstrap peer(s)", retry_ids.len());
                if let Err(e) = sender.join_peers(retry_ids.clone()).await {
                    eprintln!("[seed] bootstrap re-join failed: {e}");
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
                eprintln!("[seed] neighbors={}", neighbors.load(Ordering::Relaxed).max(0));
            }
            _ = shutdown_signal() => break,
        }
    }

    eprintln!("[seed] shutting down");
    let _ = router.shutdown().await;
    endpoint.close().await;
    Ok(())
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
