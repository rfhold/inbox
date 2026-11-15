"""Isolated wire checks; these never contact the app or Compose."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import struct
import threading
import time
import unittest

from client import Client, IncrementalDecoder, RpcError, frame, pb
from test_nats_faults import rpc_fate


class StreamProtocolTests(unittest.TestCase):
    def test_confirmation_errors_do_not_prove_rejection_or_admission(self):
        for code in (1, 4, 14):
            self.assertEqual(rpc_fate(("grpc", code)), "unknown")
        for outcome in (("http", 503), ("transport-uncertain", "TimeoutError"), ("grpc", 13)):
            self.assertEqual(rpc_fate(outcome), "unknown")
        for code in (3, 5, 6):
            self.assertEqual(rpc_fate(("grpc", code)), "permanent-application-error")

    def test_every_split_partial_headers_bodies_and_multiple_frames(self):
        wire = frame(b"first") + frame(b"second") + frame(b"grpc-status: 0\r\n", 0x80)
        for split in range(len(wire) + 1):
            decoder = IncrementalDecoder()
            result = decoder.feed(wire[:split]) + decoder.feed(wire[split:])
            self.assertEqual(result, [b"first", b"second"])
            decoder.finish()
        decoder = IncrementalDecoder()
        result = []
        for byte in wire:
            result.extend(decoder.feed(bytes([byte])))
        self.assertEqual(result, [b"first", b"second"])
        decoder.finish()

    def test_non_ok_status_and_percent_decoding(self):
        decoder = IncrementalDecoder()
        with self.assertRaises(RpcError) as caught:
            decoder.feed(frame(b"grpc-status: 14\r\ngrpc-message: retry%20later\r\n", 0x80))
        self.assertEqual((caught.exception.code, caught.exception.detail), (14, "retry later"))

    def test_eof_truncation_missing_status_bad_flags_and_oversize(self):
        for wire in (b"", b"\0", frame(b"abc")[:-1], frame(b"abc")):
            decoder = IncrementalDecoder()
            decoder.feed(wire)
            with self.assertRaises(ValueError):
                decoder.finish()
        for wire in (frame(b"", 1), frame(b"", 0x81), frame(b"missing: status\r\n", 0x80),
                     struct.pack(">BI", 0, 65537), struct.pack(">BI", 0x80, 65537)):
            with self.assertRaises(ValueError):
                IncrementalDecoder().feed(wire)

    def test_trailers_are_terminal(self):
        trailer = frame(b"grpc-status: 0\r\n", 0x80)
        with self.assertRaises(ValueError):
            IncrementalDecoder().feed(trailer + frame(b"late"))
        decoder = IncrementalDecoder()
        decoder.feed(trailer)
        decoder.finish()
        with self.assertRaises(ValueError):
            decoder.feed(b"x")

    def test_buffer_bound_with_many_frames(self):
        decoder = IncrementalDecoder(max_frame=32)
        self.assertEqual(len(decoder.feed(frame(b"x") * 10000)), 10000)
        self.assertLessEqual(len(decoder.buffer), 37)
        decoder.feed(frame(b"grpc-status: 0\r\n", 0x80))
        decoder.finish()

    def test_generated_contract_field_numbers(self):
        expected = {"WatchChangesRequest": {"resume_token": 1},
                    "ChangeEvent": {"resume_token": 1, "kind": 2, "mailbox_id": 3,
                                    "entry_id": 4, "submission_id": 5, "domain_id": 6}}
        for name, fields in expected.items():
            descriptor = getattr(pb, name).DESCRIPTOR
            self.assertEqual({f.name: f.number for f in descriptor.fields}, fields)
        self.assertEqual(pb.CreateMailboxRequest.DESCRIPTOR.fields_by_name["request_id"].number, 3)
        self.assertEqual(pb.CreateMailboxRequest.DESCRIPTOR.fields_by_name["domain_id"].number, 4)
        self.assertEqual(pb.SetReadStateRequest.DESCRIPTOR.fields_by_name["request_id"].number, 4)
        self.assertEqual([pb.CHANGE_KIND_UNSPECIFIED, pb.CHANGE_KIND_RESET, pb.CHANGE_KIND_MAILBOX,
                          pb.CHANGE_KIND_MESSAGE, pb.CHANGE_KIND_SUBMISSION, pb.CHANGE_KIND_HEARTBEAT,
                          pb.CHANGE_KIND_DOMAIN],
                         list(range(7)))

    def test_stream_yields_before_eof_bounded_wait_and_cancel(self):
        release = threading.Event()
        observed = {}

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                observed["path"] = self.path
                observed["body"] = self.rfile.read(int(self.headers["Content-Length"]))
                observed["type"] = self.headers["Content-Type"]
                self.send_response(200)
                self.send_header("Content-Type", "application/grpc-web+proto")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                payload = frame(pb.ChangeEvent(resume_token="watermark", kind=1).SerializeToString())
                try:
                    for chunk in (payload[:3], payload[3:]):
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                        self.wfile.flush()
                    release.wait(5)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                self.close_connection = True

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stream = None
        try:
            stream = Client(f"http://127.0.0.1:{server.server_port}/prefix").watch("saved", timeout=3)
            self.assertEqual(stream.next_event(2).resume_token, "watermark")
            self.assertFalse(release.is_set(), "Must yield before the HTTP body ends")
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                stream.next_event(0.05)
            self.assertLess(time.monotonic() - started, 1)
            stream.close()
            self.assertFalse(stream.thread.is_alive(), "Cancellation must interrupt the HTTP read")
            self.assertEqual(observed["path"], "/prefix/inbox.v1.MailService/WatchChanges")
            self.assertEqual(observed["type"], "application/grpc-web+proto")
            self.assertEqual(observed["body"], frame(pb.WatchChangesRequest(resume_token="saved").SerializeToString()))
        finally:
            release.set()
            if stream is not None:
                stream.close()
            server.shutdown()
            server.server_close()
            thread.join(2)


if __name__ == "__main__":
    unittest.main()
