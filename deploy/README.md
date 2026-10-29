# Operating the seed fleet

The runbook behind the reference seeds (`seed-au-1`, `seed-eu-1`). Everything
here was extracted from operating them for real — including the incidents.
The short version: **every deploy is `run-seed.sh`**, whether it's a first
install, an upgrade, or a new seed joining the fleet.

The reference fleet runs the **Docker image** with a named volume. The
[systemd unit](mstream-discovery-seed.service) remains the bare-metal
alternative; if you use it instead, mirror the same ideas (memory cap,
`Restart=always`, persistent data dir).

## The fleet-standard container

`run-seed.sh` produces exactly this shape:

```sh
docker run -d --name mstream-seed \
  -v mstream-seed-data:/data \
  --restart unless-stopped \
  --memory 200m --memory-swap 200m \
  -p 11204:11204/udp \
  ghcr.io/irosthebeggar/mstream-discovery-seed:vX.Y.Z \
  --port 11204 --bootstrap <sibling-ticket> [--bootstrap <sibling-ticket>…]
```

Every flag earned its place:

- **`-v mstream-seed-data:/data`** — `identity.key` lives in the volume, so
  container recreates (upgrades) keep the seed's identity, which keeps every
  shipped ticket in mStream's `seeds/discovery-seeds.json` valid. Losing the
  key means shipping a new seed list.
- **`--memory 200m --memory-swap 200m`** — a healthy seed sits around 30 MB.
  The pre-v1.0.0 gossip leak grew seeds to ~150 MB on a ~3.7-day fuse, and on
  512 MB droplets the *kernel* OOM killer handled it — taking out `fwupd`,
  `unattended-upgrades`, and once `apt-get` as collateral. The cap turns any
  future leak into a bounded, container-scoped kill that `unless-stopped`
  restarts in seconds.
- **`-p 11204:11204/udp` + `--port 11204`** (v1.5.0+) — one fixed UDP port,
  published 1:1. The ticket a seed prints carries its public `ip:port`. With
  a random port (every version before 1.5.0) that address went stale on the
  first restart, so the tickets shipped in `seeds/discovery-seeds.json` were
  only ever dialable through the iroh relays — how the fleet ran from July
  to October 2026. With the port pinned and published, the direct address
  survives restarts and a shipped ticket stays directly dialable. IPv4 must
  bind (the seed refuses to start on a port clash); IPv6 is best-effort.
- **`--bootstrap <sibling tickets>`** — a seed never dials anyone on its own
  and restarts with an empty peer memory. Without bootstrap tickets, a
  restarted seed waits **passively** to be rediscovered; on a calm mesh that
  took 15+ minutes in practice (nobody has a repair reason to dial a quiet
  seed — the same restart re-meshed in 13 s during a churn storm, which is
  what made the gap easy to miss). Give every seed its siblings' tickets and
  a restart re-meshes in seconds instead.

  ⚠ In v1.0.0 the bootstrap dial was **one-shot** — a lost boot race meant
  it silently never tried again ([#2]). Since **v1.0.1** the seed re-dials
  its bootstrap peers every 30 s for as long as it has zero neighbors, so a
  lost race (or any future total isolation) self-heals without help.
  `run-seed.sh`'s restart-once check stays as a belt and for v1.0.0
  binaries.

[#2]: https://github.com/IrosTheBeggar/mstream-discovery-seeds/issues/2

## Upgrading a seed

One seed at a time — the others keep the network's front door open.

```sh
# on the seed host
./run-seed.sh v1.5.0 --port 11204 --bootstrap <sibling-ticket>
```

The script pulls the pinned tag, backs up `identity.key` (copy that backup
**off the box** too), recreates the container, and verifies. Afterwards
check, in this order:

1. `endpoint-id:` in the logs is **unchanged** — identity survived. If it
   changed, stop: you've lost/replaced the key; restore it into the volume
   and recreate, or you're shipping a new seed list.
2. `neighbor up:` lines appear (the script waits for this when bootstrap
   tickets were given).
3. `neighbors=N` heartbeats settle at N ≥ 1 over the next minutes.
4. If you added or changed `--port`, the `ticket:` line changed too (same
   identity, new direct address): re-sign the seed list — step 4 of
   "Adding a new seed" below. Until that lands, the old ticket still works
   through the relays.

The previous image stays on disk. Rollback = re-run `run-seed.sh` with the
previous tag.

## Adding a new seed

1. **Provision** any small Docker host — 512 MB is plenty (~30 MB RSS,
   gossip-only bandwidth, outbound UDP + HTTPS). Open one inbound UDP port
   if you can (the fleet uses 11204) so the seed is directly dialable;
   without it inbound still arrives through the iroh relays.
2. **Deploy**, bootstrapping off the existing fleet (tickets are in
   [mStream's `seeds/discovery-seeds.json`](https://github.com/IrosTheBeggar/mStream/blob/master/seeds/discovery-seeds.json)):

   ```sh
   curl -fsSLO https://raw.githubusercontent.com/IrosTheBeggar/mstream-discovery-seeds/master/deploy/run-seed.sh
   chmod +x run-seed.sh
   ./run-seed.sh v1.5.0 --port 11204 --bootstrap <seed-au-1 ticket> --bootstrap <seed-eu-1 ticket>
   ```

3. **Back up** the new `identity.key` off the box (the script printed where
   the on-box copy landed).
4. **Ship it to users** — in the mStream repo:
   - add `{ name, endpointId, ticket }` (from the script's output) to
     `seeds/discovery-seeds.json`;
   - re-sign: `node scripts/sign-discovery-seeds.mjs --key <offline-pem>` —
     it auto-bumps `seq` (rollback protection) and self-verifies against the
     baked public key;
   - commit. Existing servers pick the list up within ~a day (it's fetched,
     cached, and falls back to baked defaults); no mStream release needed.
   - Optionally also add it to the baked `DEFAULT_SEEDS` in
     `src/state/discovery-seeds.js` so brand-new installs know it before
     their first list fetch — that one does ride the next release.
5. **Give the existing seeds the new sibling** on their next upgrade: add the
   new seed's ticket to their `--bootstrap` list. Not urgent — bootstrap
   only matters at restart.

Community-run seeds: same steps 1–3, then open a PR for step 4 (policy per
the top-level README).

## Health checks

```sh
docker logs --tail 5 mstream-seed          # neighbors=N heartbeat (1/min)
docker logs mstream-seed 2>&1 | grep -E 'neighbor (up|down)' | tail    # churn
ps -o rss= -C mstream-discovery-seed       # 15–75 MB seen on v1.4.0 (fresh → day 5); the 200 MB cap is the line
journalctl -k | grep -ci 'Out of memory'   # should stop growing at the cap
```

`neighbors=0` forever on an established network means unreachable or
version-drifted (see the cross-repo contract in the top-level README) — or,
after a restart with no `--bootstrap`, just lonely: it is still reachable
and will be woven back in when any peer dials it, but that can take a long
time on a calm mesh. Bootstrap tickets exist so you never wait on it.

A steady `neighbors=N` with **no** `neighbor up/down` lines for hours is the
other failure: a wedged gossip actor (2026-10-22 — both fleet seeds, ~20 h,
zero joins answered). Since v1.4.0 the seed catches that itself: it probes
the actor once a minute and exits with code 3 after two misses, and the
`--restart unless-stopped` policy restarts it. Check with:

```sh
docker logs mstream-seed 2>&1 | grep -E 'actor probe|FATAL|re-subscrib' | tail
docker inspect --format '{{.RestartCount}} {{.State.StartedAt}}' mstream-seed
```
