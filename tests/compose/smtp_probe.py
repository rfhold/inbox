#!/usr/bin/env python3
"""Send synthetic binary-safe MIME, optionally compare a raw-download URL."""
import argparse
from pathlib import Path
import smtplib
import urllib.request
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("recipient", help="Existing inbox address")
parser.add_argument("--port", type=int, default=2525)
parser.add_argument("--raw-url", help="Raw message URL to compare after delivery")
args = parser.parse_args()
token = str(uuid.uuid4())
raw = (
    f"From: sender@example.test\r\nTo: {args.recipient}\r\n"
    f"Subject: Compose fidelity {token}\r\nMessage-ID: <{token}@example.test>\r\n"
    'MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="compose-test"\r\n\r\n'
    '--compose-test\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n'
    'Synthetic message.\r\n.leading dot\r\n..two dots\r\n'
    '--compose-test\r\nContent-Type: application/octet-stream\r\n'
    'Content-Disposition: attachment; filename="probe.bin"\r\n'
    'Content-Transfer-Encoding: base64\r\n\r\nAP8NCg==\r\n--compose-test--\r\n'
).encode()
root = Path(__file__).resolve().parents[2] / ".local"
root.mkdir(mode=0o700, exist_ok=True)
expected = root / f"smtp-{token}.eml"
expected.write_bytes(raw)
with smtplib.SMTP("127.0.0.1", args.port, timeout=20) as smtp:
    smtp.sendmail("sender@example.test", [args.recipient], raw)
print(f"SMTP accepted synthetic MIME. Expected raw bytes: {expected}")
if args.raw_url:
    with urllib.request.urlopen(args.raw_url, timeout=20) as response:
        assert response.read() == raw, "Raw MIME differs from submitted DATA"
    print("Raw MIME byte-for-byte comparison passed.")
