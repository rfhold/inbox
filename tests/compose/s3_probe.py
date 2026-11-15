#!/usr/bin/env python3
"""Path-style SigV4 round-trip; keeps a fixed test object for restart checks."""
import datetime
import hashlib
import hmac
import os
import sys
import urllib.parse
import urllib.request

PAYLOAD = b"Inbox Garage durability probe\r\n\x00\xff\r\n.dot-prefixed\r\n"


def request(method, payload=b""):
    now = datetime.datetime.now(datetime.timezone.utc)
    date, timestamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
    host = "garage:3900"
    bucket = os.environ.get("INBOX_S3_BUCKET", "inbox")
    path = "/" + urllib.parse.quote(bucket, safe="") + "/_compose-test/roundtrip.bin"
    digest = hashlib.sha256(payload).hexdigest()
    headers = {"host": host, "x-amz-content-sha256": digest, "x-amz-date": timestamp}
    signed = ";".join(sorted(headers))
    canonical_headers = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers))
    canonical = "\n".join((method, path, "", canonical_headers, signed, digest))
    scope = f"{date}/garage/s3/aws4_request"
    to_sign = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + os.environ["AWS_SECRET_ACCESS_KEY"]).encode()
    for value in (date, "garage", "s3", "aws4_request"):
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={os.environ['AWS_ACCESS_KEY_ID']}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    req = urllib.request.Request(
        "http://" + host + path, data=payload if method == "PUT" else None,
        headers=headers, method=method,
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.read()


if "--read-only" not in sys.argv:
    request("PUT", PAYLOAD)
assert request("GET") == PAYLOAD, "S3 changed the stored bytes"
print("Garage path-style S3 byte-for-byte read passed.")
