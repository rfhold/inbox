"""Independent gRPC-Web client using temporary standard protobuf bindings."""

import base64
import http.client
import importlib.util
import os
import queue
from pathlib import Path
import struct
import socket
import subprocess
import sys
import tempfile
import threading
from urllib.parse import unquote, urlsplit


def load_proto():
    root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="inbox-acceptance-proto-") as output:
        subprocess.run(
            [sys.executable, "-m", "grpc_tools.protoc", f"-I{root / 'proto'}",
             f"--python_out={output}", str(root / "proto/inbox/v1/inbox.proto")],
            check=True,
        )
        spec = importlib.util.spec_from_file_location(
            "acceptance_inbox_pb2", Path(output) / "inbox/v1/inbox_pb2.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


pb = load_proto()


class RpcError(Exception):
    def __init__(self, code, detail=""):
        self.code = int(code)
        self.detail = detail
        super().__init__(f"gRPC {self.code}: {detail}")


def frame(payload, flags=0):
    return struct.pack(">BI", flags, len(payload)) + payload


def decode_response(body, headers, response_type):
    headers = {key.lower(): value for key, value in headers.items()}
    if "grpc-web-text" in headers.get("content-type", ""):
        body = base64.b64decode(body, validate=True)
    trailers = {}
    messages = []
    while body:
        if len(body) < 5:
            raise ValueError("Truncated gRPC-Web frame header")
        flags, size = struct.unpack(">BI", body[:5])
        if len(body) < size + 5:
            raise ValueError("Truncated gRPC-Web frame payload")
        payload, body = body[5:5 + size], body[5 + size:]
        if flags == 0x80:
            for line in payload.decode("ascii").split("\r\n"):
                if line:
                    key, value = line.split(":", 1)
                    trailers[key.lower()] = value.strip()
        elif flags == 0:
            messages.append(payload)
        else:
            raise ValueError(f"Unsupported gRPC-Web frame flags {flags}")
    metadata = {**headers, **trailers}
    if "grpc-status" not in metadata:
        raise ValueError("Missing grpc-status; HTTP 200 alone is not success")
    status = int(metadata["grpc-status"])
    if status:
        raise RpcError(status, unquote(metadata.get("grpc-message", "")))
    if len(messages) != 1:
        raise ValueError(f"Expected one unary response, received {len(messages)}")
    return response_type.FromString(messages[0])


class Client:
    def __init__(self, url=None):
        self.url = urlsplit(url or os.getenv("INBOX_TEST_HTTP_URL", "http://localhost:8080"))
        if self.url.scheme not in ("http", "https"):
            raise ValueError("INBOX_TEST_HTTP_URL must be HTTP(S)")

    def call(self, method, request=None, drop_body=False, **fields):
        if request is None:
            request = getattr(pb, method + "Request")(**fields)
        connection_type = (http.client.HTTPSConnection if self.url.scheme == "https"
                           else http.client.HTTPConnection)
        connection = connection_type(self.url.hostname, self.url.port, timeout=30)
        path = self.url.path.rstrip("/") + "/inbox.v1.MailService/" + method
        try:
            connection.request("POST", path, frame(request.SerializeToString()), {
                "Content-Type": "application/grpc-web+proto",
                "Accept": "application/grpc-web+proto",
                "X-Grpc-Web": "1",
            })
            response = connection.getresponse()
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {response.read(1024)!r}")
            if drop_body:
                # Discard the response before decoding any submission/message IDs.
                return None
            return decode_response(response.read(), dict(response.getheaders()),
                                   getattr(pb, method + "Response"))
        finally:
            connection.close()

    def watch(self, resume_token="", timeout=60):
        return ChangeStream(self.url, resume_token, timeout)


class IncrementalDecoder:
    """Bounded binary gRPC-Web framing; EOF is valid only after status trailers."""

    def __init__(self, max_frame=65536):
        self.max_frame = max_frame
        self.buffer = bytearray()
        self.ended = False

    def feed(self, chunk):
        messages = []
        # Consume input in bounded slices even if a caller provides a huge chunk.
        for offset in range(0, len(chunk), 4096):
            if self.ended:
                raise ValueError("Data after gRPC-Web trailers")
            self.buffer.extend(chunk[offset:offset + 4096])
            while len(self.buffer) >= 5:
                flags, size = struct.unpack(">BI", self.buffer[:5])
                if flags not in (0, 0x80):
                    raise ValueError("Unsupported streaming frame flags")
                if size > self.max_frame:
                    raise ValueError("Streaming frame exceeds bound")
                if len(self.buffer) < size + 5:
                    break
                payload = bytes(self.buffer[5:5 + size])
                del self.buffer[:5 + size]
                if flags == 0:
                    messages.append(payload)
                    continue
                self.ended = True
                metadata = {}
                for line in payload.decode("ascii").split("\r\n"):
                    if line:
                        key, value = line.split(":", 1)
                        metadata[key.lower()] = value.strip()
                if "grpc-status" not in metadata:
                    raise ValueError("Missing streaming grpc-status")
                code = int(metadata["grpc-status"])
                if code:
                    raise RpcError(code, unquote(metadata.get("grpc-message", "")))
                if self.buffer:
                    raise ValueError("Data after gRPC-Web trailers")
                break
        return messages

    def finish(self):
        if self.buffer:
            raise ValueError("Truncated streaming frame")
        if not self.ended:
            raise ValueError("Stream ended without grpc-status trailers")


class ChangeStream:
    """Incremental HTTP stream with bounded waits and explicit cancellation."""

    def __init__(self, url, resume_token, timeout):
        connection_type = (http.client.HTTPSConnection if url.scheme == "https"
                           else http.client.HTTPConnection)
        self.connection = connection_type(url.hostname, url.port, timeout=timeout)
        self.response = None
        self.closed = threading.Event()
        self.events = queue.Queue(maxsize=1024)
        try:
            self.connection.request(
                "POST", url.path.rstrip("/") + "/inbox.v1.MailService/WatchChanges",
                frame(pb.WatchChangesRequest(resume_token=resume_token).SerializeToString()),
                {"Content-Type": "application/grpc-web+proto",
                 "Accept": "application/grpc-web+proto", "X-Grpc-Web": "1"},
            )
            self.response = self.connection.getresponse()
            if self.response.status != 200:
                raise RuntimeError(f"WatchChanges HTTP {self.response.status}")
            headers = dict((k.lower(), v) for k, v in self.response.getheaders())
            if int(headers.get("grpc-status", "0")):
                raise RpcError(headers["grpc-status"], unquote(headers.get("grpc-message", "")))
            if "application/grpc-web+proto" not in headers.get("content-type", ""):
                raise ValueError("WatchChanges requires binary gRPC-Web protobuf")
            self.thread = threading.Thread(target=self._read, daemon=True)
            self.thread.start()
        except BaseException:
            self.close()
            raise

    def _put(self, value):
        while not self.closed.is_set():
            try:
                self.events.put(value, timeout=0.1)
                return
            except queue.Full:
                continue

    def _read(self):
        decoder = IncrementalDecoder()
        try:
            while not self.closed.is_set():
                chunk = self.response.read1(4096)
                if not chunk:
                    decoder.finish()
                    self._put(StopIteration("WatchChanges ended"))
                    return
                for payload in decoder.feed(chunk):
                    self._put(pb.ChangeEvent.FromString(payload))
                if decoder.ended:
                    self._put(StopIteration("WatchChanges ended"))
                    return
        except Exception as error:
            if not self.closed.is_set():
                self._put(error)

    def next_event(self, timeout=30):
        if self.closed.is_set():
            raise ValueError("WatchChanges is closed")
        try:
            result = self.events.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("Timed out waiting for WatchChanges event") from None
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed.set()
        # Interrupt a blocked read before closing its buffered HTTP file.
        sock = self.connection.sock
        if sock is None and self.response is not None and self.response.fp is not None:
            sock = getattr(getattr(self.response.fp, "raw", None), "_sock", None)
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self.response is not None:
            self.response.close()
        self.connection.close()
        if hasattr(self, "thread"):
            self.thread.join(timeout=2)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
