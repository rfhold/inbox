#!/usr/bin/env python3
"""Create local Compose secrets once, without displaying or rotating them."""
import os
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parents[1]
os.umask(0o077)
env = ROOT / ".env"
local = ROOT / ".local"
local.mkdir(mode=0o700, exist_ok=True)
rpc = local / "garage-rpc-secret"

if env.exists():
    values = dict(
        line.split("=", 1)
        for line in env.read_text().splitlines()
        if line and not line.startswith("#") and "=" in line
    )
    required = ("POSTGRES_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")
    if any(not values.get(key) or values[key] == "generated-by-setup" for key in required):
        raise SystemExit("Existing .env lacks usable secrets; refusing to overwrite it.")
    if not rpc.is_file():
        raise SystemExit("Existing .env but missing Garage RPC secret; restore .local from backup.")
else:
    if rpc.exists():
        raise SystemExit("Garage RPC secret exists but .env is missing; restore .env from backup.")
    with rpc.open("x") as output:
        output.write(secrets.token_hex(32) + "\n")
    with env.open("x") as output:
        output.write(
            f"POSTGRES_PASSWORD={secrets.token_hex(24)}\n"
            f"AWS_ACCESS_KEY_ID=GK{secrets.token_hex(16)}\n"
            f"AWS_SECRET_ACCESS_KEY={secrets.token_hex(32)}\n"
        )

env.chmod(0o600)
rpc.chmod(0o600)
print("Local secrets are ready; existing credentials were preserved.")
print("Start: docker compose up --build --wait")
