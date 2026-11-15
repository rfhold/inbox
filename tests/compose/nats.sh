#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
case "${1:-}" in
    ''|--write|--verify) ;;
    *) printf '%s\n' 'Usage: sh tests/compose/nats.sh [--write|--verify]' >&2; exit 2 ;;
esac
docker compose config --quiet
container=${INBOX_NATS_PROBE_CONTAINER:-}
if [ -z "$container" ]; then
    container=$(docker compose ps -q nats)
fi
if [ -z "$container" ]; then
    printf '%s\n' 'Start NATS explicitly with docker compose up -d --wait nats first.' >&2
    exit 1
fi
test "$(docker inspect --format '{{.State.Health.Status}}' "$container")" = healthy
docker run --rm -i --network "container:$container" --read-only --cap-drop ALL \
    --security-opt no-new-privileges:true --user 65534:65534 \
    python:3.13.5-slim-bookworm@sha256:4c2cf9917bd1cbacc5e9b07320025bdb7cdf2df7b0ceaccb55e9dd7e30987419 \
    python - "$@" < tests/compose/nats_probe.py
