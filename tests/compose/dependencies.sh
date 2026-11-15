#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
python3 scripts/setup.py
docker compose config --quiet
docker compose up --wait postgres garage
probe() {
    docker run --rm --network inbox_default --env-file .env \
        --mount "type=bind,src=$(pwd)/tests/compose/s3_probe.py,dst=/probe.py,readonly" \
        python:3.13.5-slim-bookworm@sha256:4c2cf9917bd1cbacc5e9b07320025bdb7cdf2df7b0ceaccb55e9dd7e30987419 \
        python /probe.py "$@"
}
probe
docker compose exec -T postgres psql -U inbox -d inbox -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE IF NOT EXISTS compose_durability_probe (id integer PRIMARY KEY); INSERT INTO compose_durability_probe VALUES (1) ON CONFLICT DO NOTHING;" >/dev/null
docker compose restart postgres garage
docker compose up --wait postgres garage
probe --read-only
docker compose exec -T postgres psql -U inbox -d inbox -v ON_ERROR_STOP=1 -c \
    'DO $$ BEGIN IF NOT EXISTS (SELECT FROM compose_durability_probe WHERE id = 1) THEN RAISE EXCEPTION '\''Durability probe missing'\''; END IF; END $$;' >/dev/null
printf '%s\n' 'PostgreSQL and Garage restart durability passed. Volumes and probe records retained.'
