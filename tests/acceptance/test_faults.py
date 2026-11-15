"""Explicit opt-in, coordinator-run Compose faults; never safe alongside other tests."""

import http.client
import json
import os
from pathlib import Path
import re
import smtplib
import subprocess
import time
import unittest
import uuid

from client import Client, RpcError, decode_response, frame, pb
import test_acceptance as standard


ROOT = Path(__file__).resolve().parents[2]


def sql_uuid(value):
    return "'" + str(uuid.UUID(value)) + "'::uuid"


class DeferredCommitFixture:
    def __init__(self, subject, token=None):
        self.token = uuid.uuid4().hex if token is None else token
        if not re.fullmatch(r"[0-9a-f]{32}", self.token):
            raise ValueError("Fixture token must be lowercase UUID hex")
        if not re.fullmatch(r"acceptance-[0-9a-f]{32}", subject):
            raise ValueError("Fixture subject must be a unique synthetic acceptance label")
        self.subject = subject
        self.function = "inbox_acceptance_commit_fn_" + self.token
        self.trigger = "inbox_acceptance_commit_tr_" + self.token
        for name in (self.function, self.trigger):
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name):
                raise ValueError("Unsafe fixture SQL identifier")
        self.owner = "inbox-acceptance-commit:" + self.token
        self.event = "INBOX_ACCEPTANCE_COMMIT_HIT_" + self.token

    @property
    def create_sql(self):
        return f"""BEGIN;
CREATE FUNCTION public.{self.function}() RETURNS trigger LANGUAGE plpgsql AS $fixture$
BEGIN
    IF NEW.content->>'subject' = '{self.subject}' THEN
        RAISE LOG '{self.event} %', NEW.id;
        RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='synthetic acceptance deferred commit failure';
    END IF;
    RETURN NEW;
END;
$fixture$;
COMMENT ON FUNCTION public.{self.function}() IS '{self.owner}';
CREATE CONSTRAINT TRIGGER {self.trigger}
AFTER INSERT ON public.messages DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.content->>'subject' = '{self.subject}')
EXECUTE FUNCTION public.{self.function}();
COMMENT ON TRIGGER {self.trigger} ON public.messages IS '{self.owner}';
COMMIT;
"""

    @property
    def cleanup_sql(self):
        # Ownership checks also handle uncertain fixture-creation outcomes safely.
        return f"""DO $cleanup$
DECLARE
    trigger_oid oid;
    function_oid oid;
BEGIN
    SELECT oid INTO trigger_oid FROM pg_trigger
        WHERE tgrelid='public.messages'::regclass AND tgname='{self.trigger}';
    SELECT to_regprocedure('public.{self.function}()')::oid INTO function_oid;
    IF trigger_oid IS NOT NULL THEN
        IF obj_description(trigger_oid, 'pg_trigger') IS DISTINCT FROM '{self.owner}' THEN
            RAISE EXCEPTION 'Refuse removal of a trigger without fixture ownership';
        END IF;
        DROP TRIGGER {self.trigger} ON public.messages;
    END IF;
    IF function_oid IS NOT NULL THEN
        IF obj_description(function_oid, 'pg_proc') IS DISTINCT FROM '{self.owner}' THEN
            RAISE EXCEPTION 'Refuse removal of a function without fixture ownership';
        END IF;
        DROP FUNCTION public.{self.function}();
    END IF;
END;
$cleanup$;
"""

    def observed_message_ids(self, logs):
        return {str(uuid.UUID(value)) for value in re.findall(
            re.escape(self.event) + r" ([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\b", logs)}


@unittest.skipUnless(os.getenv("INBOX_TEST_FAULTS") == "1", "Requires exclusive INBOX_TEST_FAULTS=1")
class FaultTests(unittest.TestCase):
    mailbox = standard.AcceptanceTests.mailbox
    entries = standard.AcceptanceTests.entries
    delivered = standard.AcceptanceTests.delivered
    terminal_submission = standard.AcceptanceTests.terminal_submission

    def setUp(self):
        self.client = Client()
        self.timeout = float(os.getenv("INBOX_TEST_FAULT_TIMEOUT", "90"))
        self.smtp_host = os.getenv("INBOX_TEST_SMTP_HOST", "localhost")
        self.smtp_port = int(os.getenv("INBOX_TEST_SMTP_PORT", "2525"))
        local = {"localhost", "127.0.0.1", "::1"}
        self.assertIn(self.client.url.hostname, local, "Faults require a local Compose endpoint")
        self.assertIn(self.smtp_host, local, "Faults require a local SMTP endpoint")
        self.sender = self.mailbox()
        self.recipient = self.mailbox()
        self.delivery_id = None
        # Cleanup also covers assertions and subprocess failures after a service stop.
        self.addCleanup(self.restore_services)

    def compose(self, *arguments, input_text=None):
        process = subprocess.run(
            ["docker", "compose", *arguments], cwd=ROOT, input=input_text,
            text=True, capture_output=True, timeout=self.timeout + 30,
        )
        if process.returncode:
            # Do not include Compose configuration, environment, or diagnostic output.
            self.fail(f"docker compose {arguments[0]} failed with exit {process.returncode}")
        return process.stdout.strip()

    def sql(self, statement):
        return self.compose("exec", "-T", "postgres", "psql", "--no-psqlrc", "-U", "inbox",
                            "-d", "inbox", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1",
                            input_text=statement)

    def poll(self, operation, description):
        deadline = time.monotonic() + self.timeout
        while True:
            value = operation()
            if value:
                return value
            if time.monotonic() >= deadline:
                self.fail("Timed out waiting for " + description)
            time.sleep(0.1)

    def restore_services(self):
        # A command failure must not prevent attempts to start the other services.
        failures = []
        for service in ("postgres", "garage", "app"):
            try:
                self.compose("start", "--wait", "--wait-timeout", str(int(self.timeout)), service)
            except Exception as error:
                failures.append(f"{service}: {type(error).__name__}")
        if failures:
            self.fail("Service restoration failed: " + ", ".join(failures))
        if self.delivery_id:
            # Restore synthetic staged status without removing rows or mailbox entries.
            self.sql(
                "UPDATE deliveries AS d SET status=2,entry_id=e.id,lease_until=NULL,"
                "updated_at=now() FROM mailbox_entries AS e "
                f"WHERE d.id={sql_uuid(self.delivery_id)} "
                f"AND d.submission_id={sql_uuid(self.seed.id)} "
                f"AND d.mailbox_id={sql_uuid(self.recipient.id)} "
                "AND e.mailbox_id=d.mailbox_id AND e.folder=1 "
                f"AND e.message_id={sql_uuid(self.seed.message_id)};"
            )

    def fields(self):
        return dict(from_mailbox_id=self.sender.id, to=[self.recipient.address],
                    subject=standard.unique(), text_body="synthetic fault acceptance",
                    idempotency_key=standard.unique())

    def fault_submit(self, fields):
        # Longer request timeout permits the server's dependency timeout to return an error.
        url = self.client.url
        connection_type = (http.client.HTTPSConnection if url.scheme == "https"
                           else http.client.HTTPConnection)
        connection = connection_type(url.hostname, url.port, timeout=self.timeout)
        try:
            connection.request(
                "POST", url.path.rstrip("/") + "/inbox.v1.MailService/SubmitMessage",
                frame(pb.SubmitMessageRequest(**fields).SerializeToString()),
                {"Content-Type": "application/grpc-web+proto", "X-Grpc-Web": "1"},
            )
            response = connection.getresponse()
            body = response.read()
            if response.status != 200:
                return ("http", response.status)
            result = decode_response(body, dict(response.getheaders()), pb.SubmitMessageResponse)
            return result.submission
        except RpcError as error:
            return ("grpc", error.code)
        finally:
            connection.close()

    def smtp_rejected(self, subject, allow_rcpt_rejection):
        smtp = smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.timeout)
        try:
            smtp.ehlo_or_helo_if_needed()
            code, _ = smtp.mail("synthetic@example.invalid")
            self.assertEqual(code // 100, 2, "Fault check must reach RCPT or DATA")
            code, _ = smtp.rcpt(self.recipient.address)
            if code // 100 != 2:
                self.assertTrue(allow_rcpt_rejection, "Garage fault must reach SMTP DATA")
                self.assertIn(code // 100, (4, 5))
                return ("rcpt", code)
            raw = (f"From: synthetic@example.invalid\r\nTo: {self.recipient.address}\r\n"
                   f"Subject: {subject}\r\n\r\nno outage acceptance\r\n").encode()
            try:
                code, _ = smtp.data(raw)
            except smtplib.SMTPDataError as error:
                code = error.smtp_code
            self.assertIn(code // 100, (4, 5), "SMTP must not acknowledge DATA during the outage")
            return ("data", code)
        finally:
            smtp.close()

    def recover_submission(self, fields):
        def retry():
            try:
                result = self.fault_submit(fields)
            except (OSError, http.client.HTTPException):
                return None
            return result if isinstance(result, pb.Submission) else None

        return self.poll(retry, "same-key submission after service recovery")

    def assert_single_delivery(self, submission, original_entry_id=None):
        terminal = self.delivered(submission)
        self.assertEqual(len(terminal.recipients), 1)
        recipient = terminal.recipients[0]
        for mailbox, folder in ((self.recipient, pb.FOLDER_INBOX), (self.sender, pb.FOLDER_SENT)):
            entries = [entry for entry in self.entries(mailbox, folder)
                       if entry.message_id == submission.message_id]
            self.assertEqual(len(entries), 1)
            if folder == pb.FOLDER_INBOX:
                self.assertEqual(entries[0].id, recipient.entry_id)
                if original_entry_id:
                    self.assertEqual(entries[0].id, original_entry_id)
        return terminal

    def outage(self, service):
        fields = self.fields()
        smtp_subject = standard.unique()
        try:
            self.compose("stop", service)
            rejected = self.fault_submit(fields)
            self.assertIsInstance(rejected, tuple, "API must return non-success during the outage")
            smtp_rejection = self.smtp_rejected(smtp_subject, allow_rcpt_rejection=service == "postgres")
            print(f"\n{service} outage: API {rejected}, SMTP {smtp_rejection}", flush=True)
        finally:
            self.restore_services()
        recovered = self.recover_submission(fields)
        self.assert_single_delivery(recovered)
        repeated = self.recover_submission(fields)
        self.assertEqual((recovered.id, recovered.message_id), (repeated.id, repeated.message_id))
        self.assert_single_delivery(repeated)
        self.assertFalse([entry for entry in self.entries(self.recipient, pb.FOLDER_INBOX)
                          if entry.subject == smtp_subject], "Rejected SMTP data must not deliver later")

    def test_garage_outage_no_false_acceptance_and_same_key_recovery(self):
        self.outage("garage")

    def test_postgres_outage_no_false_acceptance_and_same_key_recovery(self):
        self.outage("postgres")

    def test_deferred_postgres_commit_failure_after_s3_put(self):
        fields = self.fields()
        fixture = DeferredCommitFixture(fields["subject"])
        try:
            self.sql(fixture.create_sql)
            self.assertEqual(self.fault_submit(fields), ("grpc", 14),
                             "Deferred commit failure must return UNAVAILABLE, not acceptance")
            self.assertEqual(self.smtp_rejected(fields["subject"], allow_rcpt_rejection=False),
                             ("data", 451), "SMTP must accept RCPT but reject final DATA")

            def trigger_hits():
                logs = self.compose("logs", "--no-color", "--tail", "500", "postgres")
                hits = fixture.observed_message_ids(logs)
                return hits if len(hits) >= 2 else None

            hits = self.poll(trigger_hits, "two distinct deferred commit trigger executions")
            print(f"\nDeferred commit fixture reached for {len(hits)} distinct synthetic messages", flush=True)
            counts = json.loads(self.sql(
                "SELECT json_build_object("
                "'messages',(SELECT count(*) FROM messages AS m "
                f"WHERE m.content->>'subject'='{fixture.subject}'),"
                "'entries',(SELECT count(*) FROM mailbox_entries AS e JOIN messages AS m ON m.id=e.message_id "
                f"WHERE m.content->>'subject'='{fixture.subject}'),"
                "'submissions',(SELECT count(*) FROM submissions AS s JOIN messages AS m ON m.id=s.message_id "
                f"WHERE m.content->>'subject'='{fixture.subject}'),"
                "'deliveries',(SELECT count(*) FROM deliveries AS d JOIN submissions AS s ON s.id=d.submission_id "
                "JOIN messages AS m ON m.id=s.message_id "
                f"WHERE m.content->>'subject'='{fixture.subject}'))::text;"
            ))
            self.assertEqual(counts, dict(messages=0, entries=0, submissions=0, deliveries=0))
            self.assertFalse([entry for entry in self.entries(self.recipient, pb.FOLDER_INBOX)
                              if entry.subject == fixture.subject])
            self.assertFalse([entry for entry in self.entries(self.sender, pb.FOLDER_SENT)
                              if entry.subject == fixture.subject])
        finally:
            self.sql(fixture.cleanup_sql)

        recovered = self.recover_submission(fields)
        self.assert_single_delivery(recovered)
        repeated = self.recover_submission(fields)
        self.assertEqual((repeated.id, repeated.message_id), (recovered.id, recovered.message_id))
        self.assert_single_delivery(repeated)
        # API and SMTP deliberately share the guarded subject; only the API retry may appear.
        matches = [entry for entry in self.entries(self.recipient, pb.FOLDER_INBOX)
                   if entry.subject == fixture.subject]
        self.assertEqual(len(matches), 1, "Rejected SMTP input must not deliver after fixture removal")
        self.assertEqual(matches[0].message_id, recovered.message_id)
        detail = self.client.call("GetMessage", mailbox_id=self.recipient.id,
                                  entry_id=matches[0].id).message
        self.assertEqual(detail.entry.from_address, self.sender.address)
        self.assertEqual(detail.text_body.rstrip("\r\n"), fields["text_body"])
        self.assertNotIn("no outage acceptance", detail.text_body)

    def stage_delivery(self, attempts, lease_seconds):
        self.seed = self.recover_submission(self.fields())
        self.seed_terminal = self.assert_single_delivery(self.seed)
        self.original_entry_id = self.seed_terminal.recipients[0].entry_id
        self.compose("stop", "app")
        # Only this test's verified submission, sender, recipient, and message can match.
        result = self.sql(
            "UPDATE deliveries AS d SET status=1,entry_id=NULL,"
            f"attempts={int(attempts)},lease_until=now()+make_interval(secs=>{int(lease_seconds)}),"
            "updated_at=now() FROM submissions AS s "
            f"WHERE s.id={sql_uuid(self.seed.id)} AND s.id=d.submission_id "
            f"AND s.message_id={sql_uuid(self.seed.message_id)} "
            f"AND s.from_mailbox_id={sql_uuid(self.sender.id)} "
            f"AND d.mailbox_id={sql_uuid(self.recipient.id)} "
            "RETURNING d.id::text || '|' || extract(epoch from d.lease_until)::text;"
        )
        rows = result.splitlines()
        self.assertEqual(len(rows), 1, "Staging must update exactly one synthetic delivery")
        delivery_id, lease_epoch = rows[0].split("|")
        self.delivery_id = str(uuid.UUID(delivery_id))
        self.lease_epoch = float(lease_epoch)
        self.compose("start", "app")

    def delivery_state(self):
        result = self.sql(
            "SELECT json_build_object('status',d.status,'attempts',d.attempts,"
            "'entry_id',d.entry_id,'now_epoch',extract(epoch from now()))::text "
            "FROM deliveries AS d "
            f"WHERE d.id={sql_uuid(self.delivery_id)} "
            f"AND d.submission_id={sql_uuid(self.seed.id)} "
            f"AND d.mailbox_id={sql_uuid(self.recipient.id)};"
        )
        self.assertTrue(result, "Synthetic delivery row disappeared")
        return json.loads(result)

    def ready_submission(self):
        def fetch():
            try:
                return self.client.call("GetSubmission", submission_id=self.seed.id).submission
            except (OSError, http.client.HTTPException, RpcError):
                return None

        return self.poll(fetch, "app API after restart")

    def test_expired_lease_replay_preserves_unique_original_entry(self):
        try:
            self.stage_delivery(attempts=0, lease_seconds=-1)
            self.ready_submission()
            self.assert_single_delivery(self.seed, self.original_entry_id)
            self.assertEqual(self.delivery_state()["attempts"], 1)
        finally:
            self.restore_services()

    def test_future_lease_defers_claim_then_recovers(self):
        seconds = int(os.getenv("INBOX_TEST_LEASE_SECONDS", "3"))
        self.assertGreaterEqual(seconds, 3)
        self.assertLessEqual(seconds, 30)
        try:
            self.stage_delivery(attempts=0, lease_seconds=seconds)
            self.ready_submission()
            observations = 0
            deadline = time.monotonic() + self.timeout
            while True:
                state = self.delivery_state()
                if state["now_epoch"] >= self.lease_epoch:
                    break
                observations += 1
                self.assertEqual(state["status"], 1, "Worker claimed an unexpired lease")
                self.assertEqual(state["attempts"], 0, "Worker attempted an unexpired lease")
                self.assertIsNone(state["entry_id"])
                self.assertLess(time.monotonic(), deadline, "Lease observation exceeded its deadline")
                time.sleep(0.1)
            self.assertGreater(observations, 0, "Lease expired before app readiness; increase INBOX_TEST_LEASE_SECONDS")
            self.assert_single_delivery(self.seed, self.original_entry_id)
            self.assertEqual(self.delivery_state()["attempts"], 1)
        finally:
            self.restore_services()

    def test_exhausted_attempts_become_terminal_failure_without_new_claim(self):
        try:
            self.stage_delivery(attempts=8, lease_seconds=-1)
            self.ready_submission()
            terminal = self.poll(lambda: self.terminal_submission(self.seed.id), "retry exhaustion")
            self.assertEqual(terminal.status, pb.SUBMISSION_STATUS_FAILED)
            self.assertEqual(len(terminal.recipients), 1)
            self.assertEqual(terminal.recipients[0].status, pb.RECIPIENT_STATUS_FAILED)
            self.assertFalse(terminal.recipients[0].entry_id)
            state = self.delivery_state()
            self.assertEqual(state["status"], 3)
            self.assertEqual(state["attempts"], 8, "Exhausted work must not increment attempts forever")
            for mailbox, folder in ((self.recipient, pb.FOLDER_INBOX), (self.sender, pb.FOLDER_SENT)):
                entries = [entry for entry in self.entries(mailbox, folder)
                           if entry.message_id == self.seed.message_id]
                self.assertEqual(len(entries), 1)
        finally:
            self.restore_services()


if __name__ == "__main__":
    unittest.main()
