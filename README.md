# mstream-discovery-seeds

Community **seed nodes** for the [mStream](https://github.com/IrosTheBeggar/mStream) music-discovery network.

A seed is a small, always-on member of the network's gossip mesh. Fresh mStream servers ship with the seed tickets from [`seeds/discovery-seeds.json` in the mStream repo](https://github.com/IrosTheBeggar/mStream/blob/master/seeds/discovery-seeds.json) and bootstrap off them to find the network — after that, they know real peers and the seed is no longer needed. Bitcoin's DNS-seeds model, for music discovery.

**A seed holds no music and no discovery data.** It relays gossip like any mesh member; catalog announcements are signed by their origins (a seed can't forge them) and similarity queries never leave each user's machine (a seed can't observe them).

Footprint: one ~8 MB static-ish binary, ~30 MB RSS, gossip-only bandwidth. Runs happily on the cheapest VPS or a free-tier box.

## Run one

### Docker (recommended — how the reference fleet runs)

Use [`deploy/run-seed.sh`](deploy/run-seed.sh): it pins the image tag, backs
up `identity.key` on upgrades, applies the fleet-standard memory cap, wires
`--bootstrap` tickets so restarts re-mesh in seconds, and verifies the seed
actually came up meshed:

```sh
./deploy/run-seed.sh v1.0.0 --bootstrap <an-existing-seed-ticket>
docker logs mstream-seed        # shows endpoint-id + ticket on boot
```

Upgrades, health checks, and the add-a-seed runbook live in
[`deploy/README.md`](deploy/README.md).

### Binary + systemd

Grab a binary from [Releases](../../releases) (or `cargo build --release`), then follow the header comments in [`deploy/mstream-discovery-seed.service`](deploy/mstream-discovery-seed.service).

### Flags

| Flag | Meaning |
|---|---|
| `--data-dir <dir>` | required — holds `identity.key` |
| `--bootstrap <ticket>` | optional, repeatable — mesh with other seeds |
| `--print-id` | print the endpoint id and exit (used by CI) |

## Operating notes

- **Back up `identity.key`.** It *is* the seed's identity: lose it and every shipped ticket pointing at this seed goes dead. Restoring the file restores the seed, on any host.
- On boot the seed prints `ticket: endpoint…` on stdout — **that string is what goes into the seed list.**
- Health: the log prints `neighbors=N` every minute. `neighbors=0` forever on an established network means the seed is unreachable or version-drifted (see contract below) — or, right after a restart with no `--bootstrap` tickets, merely waiting passively to be rediscovered, which can take a long time on a calm mesh. Run seeds with their siblings' tickets (see [`deploy/README.md`](deploy/README.md)) so restarts never wait.
- Networking: outbound UDP + HTTPS is enough (iroh relays handle inbound behind NAT). A directly reachable UDP port improves things but is not required.
- Stop with SIGTERM/Ctrl-C. The seed deliberately ignores stdin.

## Cross-repo contract with mStream

Two things must stay aligned with `p2p-sidecar` in the mStream repo, or seeds go quietly deaf:

1. the catalog topic string (`mstream/discovery/catalog/v1` — see `src/main.rs`),
2. the exact `iroh` / `iroh-gossip` version pins in `Cargo.toml`.

Bump both repos together, deliberately. A topic protocol break moves to `…/v2` in both places at once.

## Volunteering a seed

Run one as above (the [add-a-seed runbook](deploy/README.md#adding-a-new-seed) is the full recipe), then open a PR against the mStream repo adding your `endpointId` + `ticket` to `seeds/discovery-seeds.json`. (Policy for community-run seeds is still being worked out — expect a conversation on the PR.)
