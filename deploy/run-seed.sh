#!/usr/bin/env bash
# Deploy or upgrade an mStream discovery seed on any Docker host — the one
# script behind the reference fleet. Idempotent: safe to re-run, safe for
# first install and for upgrades alike.
#
#   ./run-seed.sh v1.0.0
#   ./run-seed.sh v1.0.0 --bootstrap <other-seed-ticket> [--bootstrap <…>]
#
# What it does, in order:
#   1. pulls the PINNED image tag (never :latest — an upgrade should be a
#      deliberate act, and the old image stays on disk as the rollback),
#   2. backs up identity.key out of the volume to /root/seed-identity-backups
#      (the key IS the seed's identity: lose it and every shipped ticket
#      pointing here goes dead — ALSO copy the backup off the box),
#   3. recreates the container with the fleet-standard flags:
#        -v mstream-seed-data:/data      identity survives recreates
#        --restart unless-stopped        OOM/crash = auto-restart
#        --memory 200m (=swap)           a leak is a bounded container-scoped
#                                        kill + restart, never a droplet-wide
#                                        OOM shooting bystanders (healthy RSS
#                                        is ~30 MB; the pre-v1.0.0 gossip leak
#                                        grew seeds to ~150 MB on a ~3.7-day
#                                        fuse and the kernel OOM killer took
#                                        out fwupd/apt as collateral)
#        --bootstrap <ticket>…           dial these peers at startup — give
#                                        every seed its siblings' tickets so
#                                        a restarted seed re-meshes in
#                                        seconds instead of waiting passively
#                                        to be rediscovered (see README),
#   4. verifies: prints endpoint-id + ticket, and (when bootstrap tickets
#      were given) waits for the first neighbor. v1.0.0's bootstrap dial
#      was ONE-SHOT (a lost boot race meant no retry); v1.0.1+ re-dials
#      every 30s on its own while at zero neighbors — the restart-once
#      below stays as a belt and for v1.0.0 binaries.
#
# After a FIRST install: copy the printed ticket into mStream's
# seeds/discovery-seeds.json (see deploy/README.md, "Adding a new seed").
# After an UPGRADE: the endpoint-id printed at the end must be UNCHANGED.
set -euo pipefail

NAME=mstream-seed
VOLUME=mstream-seed-data
MEMORY=200m
IMAGE_REPO=ghcr.io/irosthebeggar/mstream-discovery-seed

TAG="${1:-}"
case "$TAG" in
  v*) shift ;;
  *) echo "usage: $0 vX.Y.Z [--bootstrap <ticket>]... [--name NAME] [--volume VOL]" >&2; exit 2 ;;
esac

RUN_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --bootstrap) RUN_ARGS+=(--bootstrap "$2"); shift 2 ;;
    --name)      NAME="$2"; shift 2 ;;
    --volume)    VOLUME="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
IMAGE="$IMAGE_REPO:$TAG"

echo "==> pulling $IMAGE"
docker pull "$IMAGE"

# Rollback aid: remember what was running before we touch anything.
OLD_IMAGE=$(docker inspect -f '{{.Image}}' "$NAME" 2>/dev/null || true)
[ -n "$OLD_IMAGE" ] && echo "==> current container runs $OLD_IMAGE (kept on disk for rollback)"

# Identity backup — only meaningful once a volume with a key exists.
if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
  MOUNT=$(docker volume inspect -f '{{.Mountpoint}}' "$VOLUME")
  if [ -f "$MOUNT/identity.key" ]; then
    BK=/root/seed-identity-backups
    mkdir -p "$BK"
    STAMP=$(date +%Y%m%d-%H%M%S)
    cp -p "$MOUNT/identity.key" "$BK/identity.key.$STAMP"
    chmod 600 "$BK/identity.key.$STAMP"
    echo "==> identity.key backed up to $BK/identity.key.$STAMP"
    echo "    (an on-box backup is not a backup — scp it somewhere else too)"
  fi
fi

echo "==> recreating container $NAME"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" \
  -v "$VOLUME":/data \
  --restart unless-stopped \
  --memory "$MEMORY" --memory-swap "$MEMORY" \
  "$IMAGE" ${RUN_ARGS[0]+"${RUN_ARGS[@]}"} >/dev/null

# The seed prints its identity within ~10s (8s bounded relay wait).
echo "==> waiting for the seed to come up"
for _ in $(seq 1 20); do
  docker logs "$NAME" 2>/dev/null | grep -q '^ticket: ' && break
  sleep 1
done
docker logs "$NAME" 2>/dev/null | grep -E '^(endpoint-id|ticket): ' || {
  echo "ERROR: seed never printed its identity — logs follow" >&2
  docker logs "$NAME" >&2 || true
  exit 1
}

wait_for_neighbor() {
  for _ in $(seq 1 20); do
    if docker logs "$NAME" 2>&1 | grep -q 'neighbor up'; then return 0; fi
    sleep 3
  done
  return 1
}

if [ ${#RUN_ARGS[@]} -gt 0 ]; then
  echo "==> bootstrap tickets given — waiting for the first neighbor"
  if ! wait_for_neighbor; then
    # v1.0.0's bootstrap dial was one-shot; a lost boot race left the seed
    # waiting passively forever. v1.0.1+ retries on its own — this restart
    # is the belt (and the fix for v1.0.0 binaries).
    echo "==> no neighbor after 60s — restarting once"
    docker restart "$NAME" >/dev/null
    if ! wait_for_neighbor; then
      echo "ERROR: still no neighbor after a retry. The bootstrap peers may be" >&2
      echo "down or the tickets stale — check them, or leave the seed running:" >&2
      echo "it is reachable and will be woven in as peers dial it." >&2
      exit 1
    fi
  fi
  docker logs "$NAME" 2>&1 | grep 'neighbor up' | tail -3
fi

echo "==> done. health check: docker logs --tail 5 $NAME   (expect neighbors=N heartbeats)"
