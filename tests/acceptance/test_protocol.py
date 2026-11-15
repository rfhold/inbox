import base64
import email.policy
from email.parser import BytesParser
import http.server
import threading
import unittest

from client import Client, RpcError, decode_response, frame, pb


class ProtocolTests(unittest.TestCase):
    def test_standard_generated_request_roundtrip(self):
        request = pb.SubmitMessageRequest(
            from_mailbox_id="sender", to=["one@inbox.test"], cc=["two@inbox.test"],
            subject="héllo", text_body="body", idempotency_key="key",
            reply_to_entry_id="entry", source_mailbox_id="source",
        )
        self.assertEqual(pb.SubmitMessageRequest.FromString(request.SerializeToString()), request)
        self.assertTrue(request.HasField("reply_to_entry_id"))

    def test_binary_and_text_success(self):
        expected = pb.CreateMailboxResponse(mailbox=pb.Mailbox(id="abc"))
        wire = frame(expected.SerializeToString()) + frame(b"grpc-status: 0\r\n", 0x80)
        for content_type, body in [
            ("application/grpc-web+proto", wire),
            ("application/grpc-web-text+proto", base64.b64encode(wire)),
        ]:
            self.assertEqual(decode_response(body, {"Content-Type": content_type},
                                             pb.CreateMailboxResponse), expected)

    def test_error_trailers_and_headers_only(self):
        for body, headers in [
            (frame(b"grpc-status: 6\r\ngrpc-message: conflicting%20payload\r\n", 0x80), {}),
            (b"", {"Grpc-Status": "6", "Grpc-Message": "conflicting%20payload"}),
        ]:
            with self.assertRaises(RpcError) as caught:
                decode_response(body, headers, pb.SubmitMessageResponse)
            self.assertEqual(caught.exception.code, 6)
            self.assertEqual(caught.exception.detail, "conflicting payload")

    def test_truncated_missing_status_and_compression_rejected(self):
        for body in [b"\0", frame(b"x")[:-1], frame(b""), frame(b"", 1)]:
            with self.subTest(body=body), self.assertRaises(ValueError):
                decode_response(body, {}, pb.GetMessageResponse)

    def test_client_http_transport_against_local_protocol_server(self):
        observed = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                observed["path"] = self.path
                observed["content_type"] = self.headers["Content-Type"]
                observed["body"] = self.rfile.read(int(self.headers["Content-Length"]))
                payload = pb.CreateMailboxResponse(mailbox=pb.Mailbox(id="fixture-id"))
                wire = frame(payload.SerializeToString()) + frame(b"grpc-status: 0\r\n", 0x80)
                self.send_response(200)
                self.send_header("Content-Type", "application/grpc-web+proto")
                self.send_header("Content-Length", str(len(wire)))
                self.end_headers()
                self.wfile.write(wire)

            def log_message(self, *args):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = Client(f"http://127.0.0.1:{server.server_port}")
            response = client.call("CreateMailbox", local_part="fixture", name="Fixture")
            self.assertEqual(response.mailbox.id, "fixture-id")
            self.assertEqual(observed["path"], "/inbox.v1.MailService/CreateMailbox")
            self.assertEqual(observed["content_type"], "application/grpc-web+proto")
            expected = pb.CreateMailboxRequest(local_part="fixture", name="Fixture")
            self.assertEqual(observed["body"], frame(expected.SerializeToString()))
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=5)

    def test_mime_fixture_plaintext_and_attachment(self):
        from test_acceptance import mime_fixture

        raw = mime_fixture("one@inbox.test", "two@inbox.test", "fixture")
        message = BytesParser(policy=email.policy.default).parsebytes(raw)
        plain = message.get_body(preferencelist=("plain",))
        self.assertEqual(plain.get_payload(decode=True).decode("utf-8"), "héllo 世界\r\n.leading dot")
        attachments = list(message.iter_attachments())
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].get_payload(decode=True), b"\0\1\2\3\xff")
        self.assertFalse(message.defects)

    def test_identifier_semantics_preserve_non_wrapper_text(self):
        from test_acceptance import identifier

        self.assertEqual(identifier("<message@example.invalid>"), "message@example.invalid")
        self.assertEqual(identifier("message@example.invalid"), "message@example.invalid")
        self.assertEqual(identifier("<broken"), "<broken")

    def test_fault_sql_ids_require_uuid(self):
        from test_faults import sql_uuid

        value = "12345678-1234-5678-1234-567812345678"
        self.assertEqual(sql_uuid(value), f"'{value}'::uuid")
        with self.assertRaises(ValueError):
            sql_uuid("'; UPDATE deliveries SET status=1; --")

    def test_deferred_commit_fixture_sql_scope_and_cleanup_ownership(self):
        from test_faults import DeferredCommitFixture

        token = "12345678123456781234567812345678"
        subject = "acceptance-" + token
        fixture = DeferredCommitFixture(subject, token=token)
        self.assertIn("CREATE CONSTRAINT TRIGGER " + fixture.trigger, fixture.create_sql)
        self.assertIn("AFTER INSERT ON public.messages DEFERRABLE INITIALLY DEFERRED", fixture.create_sql)
        self.assertIn(f"WHEN (NEW.content->>'subject' = '{subject}')", fixture.create_sql)
        self.assertIn("RAISE LOG '" + fixture.event, fixture.create_sql)
        self.assertIn("ERRCODE='40001'", fixture.create_sql)
        self.assertNotIn("OR REPLACE", fixture.create_sql)
        self.assertEqual(fixture.cleanup_sql.count("DROP TRIGGER "), 1)
        self.assertEqual(fixture.cleanup_sql.count("DROP FUNCTION "), 1)
        self.assertIn(f"DROP TRIGGER {fixture.trigger} ON public.messages;", fixture.cleanup_sql)
        self.assertIn(f"DROP FUNCTION public.{fixture.function}();", fixture.cleanup_sql)
        self.assertEqual(fixture.cleanup_sql.count(f"IS DISTINCT FROM '{fixture.owner}'"), 2)
        for sql in (fixture.create_sql, fixture.cleanup_sql):
            for forbidden in ("DELETE ", "TRUNCATE ", "DROP TABLE ", "CASCADE", "UPDATE "):
                self.assertNotIn(forbidden, sql)

    def test_deferred_commit_fixture_rejects_unsafe_names_and_subjects(self):
        from test_faults import DeferredCommitFixture

        valid = "12345678123456781234567812345678"
        for subject, token in [("normal-user-subject", valid), ("acceptance-';--", valid),
                               ("acceptance-" + valid, "x" * 32),
                               ("acceptance-" + valid, "A" * 32)]:
            with self.subTest(subject=subject, token=token), self.assertRaises(ValueError):
                DeferredCommitFixture(subject, token=token)

    def test_deferred_commit_fixture_log_evidence_requires_matching_distinct_ids(self):
        from test_faults import DeferredCommitFixture

        fixture = DeferredCommitFixture("acceptance-" + "1" * 32, token="2" * 32)
        first = "12345678-1234-5678-1234-567812345678"
        second = "87654321-4321-8765-4321-876543218765"
        logs = (f"LOG: {fixture.event} {first}\nLOG: {fixture.event} {first}\n"
                f"LOG: {fixture.event} {second}\nLOG: OTHER_FIXTURE {first}\n"
                f"STATEMENT: RAISE LOG '{fixture.event} %', NEW.id;\n")
        self.assertEqual(fixture.observed_message_ids(logs), {first, second})


if __name__ == "__main__":
    unittest.main()
