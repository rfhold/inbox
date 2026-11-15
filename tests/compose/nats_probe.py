#!/usr/bin/env python3
"""Check private JetStream health; optional synthetic file-store round-trip."""
import argparse
import base64
import json
import socket
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
mode = parser.add_mutually_exclusive_group()
mode.add_argument("--write", action="store_true", help="Retain a synthetic probe stream/message")
mode.add_argument("--verify", action="store_true", help="Read the retained synthetic message")
args = parser.parse_args()


def monitor(path):
    with urllib.request.urlopen("http://127.0.0.1:8222/" + path, timeout=5) as response:
        return json.load(response)


assert monitor("healthz?js-enabled-only=true")["status"] == "ok"
server = monitor("varz")
assert server["server_name"] == "inbox-nats"
assert server["max_payload"] == 1024 * 1024
limits = monitor("jsz")["config"]
assert limits["max_storage"] == 4 * 1024**3
assert limits["max_memory"] == 64 * 1024**2
assert limits["store_dir"] == "/var/tmp/jetstream"
print("NATS JetStream health, name, payload, and storage limits passed.")

if not (args.write or args.verify):
    raise SystemExit(0)

stream = "INBOX_COMPOSE_PROBE"
subject = "_compose.probe.synthetic"
payload = b"Inbox synthetic JetStream persistence probe\r\n\x00\xff"

with socket.create_connection(("127.0.0.1", 4222), timeout=10) as connection:
    reader = connection.makefile("rb")
    assert reader.readline().startswith(b"INFO ")
    connection.sendall(b'CONNECT {"verbose":false,"pedantic":true}\r\nSUB _INBOX.compose.probe 1\r\n')

    def request(target, body):
        connection.sendall(
            f"PUB {target} _INBOX.compose.probe {len(body)}\r\n".encode()
            + body + b"\r\n"
        )
        while True:
            line = reader.readline()
            if line == b"PING\r\n":
                connection.sendall(b"PONG\r\n")
            elif line.startswith(b"MSG "):
                size = int(line.split()[-1])
                result = json.loads(reader.read(size))
                assert reader.read(2) == b"\r\n"
                assert "error" not in result, "JetStream rejected the synthetic probe request"
                return result
            elif not line or line.startswith((b"-ERR", b"HMSG ")):
                raise RuntimeError("NATS protocol error during synthetic probe")

    if args.write:
        config = {
            "name": stream, "subjects": [subject], "storage": "file",
            "num_replicas": 1, "max_bytes": 1024 * 1024, "discard": "new",
        }
        request("$JS.API.STREAM.CREATE." + stream, json.dumps(config).encode())
        ack = request(subject, payload)
        assert ack["stream"] == stream and ack["seq"] > 0
    result = request(
        "$JS.API.STREAM.MSG.GET." + stream,
        json.dumps({"last_by_subj": subject}).encode(),
    )
    assert base64.b64decode(result["message"]["data"]) == payload
print("Synthetic JetStream file-store exact-byte read passed; probe data retained.")
