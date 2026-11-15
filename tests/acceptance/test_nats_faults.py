"""Exclusive opt-in NATS faults. No broker port, purge, table, or volume changes."""

import http.client
import os
import smtplib
import unittest

from client import Client, RpcError, decode_response, frame, pb
import test_acceptance as standard
import test_changes as changes
import test_faults as faults


def rpc_fate(outcome):
    # These contract errors differ from unavailable/cancelled/deadline confirmation.
    if outcome[0] == "grpc" and outcome[1] in (3, 5, 6):
        return "permanent-application-error"
    return "unknown"


@unittest.skipUnless(os.getenv("INBOX_TEST_NATS_FAULTS") == "1",
                     "Requires exclusive INBOX_TEST_NATS_FAULTS=1")
class NatsFaultTests(unittest.TestCase):
    mailbox = standard.AcceptanceTests.mailbox
    entries = standard.AcceptanceTests.entries
    delivered = standard.AcceptanceTests.delivered
    terminal_submission = standard.AcceptanceTests.terminal_submission
    poll = faults.FaultTests.poll
    fields = faults.FaultTests.fields
    fault_submit = faults.FaultTests.fault_submit
    recover_submission = faults.FaultTests.recover_submission
    assert_single_delivery = faults.FaultTests.assert_single_delivery
    smtp_rejected = faults.FaultTests.smtp_rejected

    def setUp(self):
        self.client = Client()
        self.timeout = float(os.getenv("INBOX_TEST_FAULT_TIMEOUT", "90"))
        self.smtp_host = os.getenv("INBOX_TEST_SMTP_HOST", "localhost")
        self.smtp_port = int(os.getenv("INBOX_TEST_SMTP_PORT", "2525"))
        for host in (self.client.url.hostname, self.smtp_host):
            self.assertIn(host, {"localhost", "127.0.0.1", "::1"}, "Faults require local Compose")
        self.sender, self.recipient = self.mailbox(), self.mailbox()
        self.addCleanup(self.restore_services)

    def restore_services(self):
        from crash_helpers import (DependencyReadinessError, RestorationError,
                                   operation_failure, wait_service_healthy)

        self.unpause_nats()
        failures = []
        healthy = set()
        for service in ("postgres", "garage", "nats", "app"):
            if service == "app" and healthy != {"postgres", "garage", "nats"}:
                failures.append(operation_failure("restore-app", DependencyReadinessError()))
                continue
            try:
                self.compose("start", service)
                wait_service_healthy(self.compose, service, self.timeout)
                healthy.add(service)
            except Exception as error:
                failures.append(operation_failure("restore-" + service, error))
        if failures:
            raise RestorationError(failures)

    def compose(self, *arguments, input_text=None):
        from crash_helpers import run_compose

        return run_compose(arguments, self.timeout, input_text)

    def unpause_nats(self):
        if getattr(self, "nats_pause_attempted", False):
            self.compose("unpause", "nats")
            self.nats_pause_attempted = False

    def unconfirmed_rpc(self, method, fields):
        url = self.client.url
        connection_type = (http.client.HTTPSConnection if url.scheme == "https"
                           else http.client.HTTPConnection)
        connection = connection_type(url.hostname, url.port, timeout=self.timeout)
        try:
            connection.request("POST", url.path.rstrip("/") + "/inbox.v1.MailService/" + method,
                               frame(getattr(pb, method + "Request")(**fields).SerializeToString()),
                               {"Content-Type": "application/grpc-web+proto", "X-Grpc-Web": "1"})
            response = connection.getresponse()
            if response.status != 200:
                return ("http", response.status)
            decode_response(response.read(), dict(response.getheaders()), getattr(pb, method + "Response"))
            self.fail(method + " returned application success while NATS was stopped")
        except RpcError as error:
            return ("grpc", error.code)
        except (OSError, http.client.HTTPException) as error:
            # This is ambiguous acceptance, not evidence of a rejected command.
            return ("transport-uncertain", type(error).__name__)
        finally:
            connection.close()

    def retry_rpc(self, method, fields):
        def retry():
            try:
                return self.client.call(method, **fields)
            except (RpcError, OSError, http.client.HTTPException):
                return None
        return self.poll(retry, method + " confirmation after broker recovery")

    def test_nats_outage_all_mutations_unconfirmed_reads_available_and_same_key_recovery(self):
        seed_fields = self.fields()
        seed = self.recover_submission(seed_fields)
        terminal = self.assert_single_delivery(seed)
        entry_id = terminal.recipients[0].entry_id
        create_fields = dict(local_part=standard.unique(), name=standard.unique(),
                             request_id=standard.unique())
        read_fields = dict(mailbox_id=self.recipient.id, entry_id=entry_id, is_read=True,
                           request_id=standard.unique())
        submit_fields = self.fields()
        smtp_subject = standard.unique()
        smtp_outcome = None
        try:
            self.compose("stop", "nats")
            self.assertTrue(self.entries(self.recipient, pb.FOLDER_INBOX))
            self.client.call("ListMailboxes", page_size=100)
            self.assertEqual(self.client.call("GetSubmission", submission_id=seed.id).submission.id, seed.id)
            self.client.call("GetMessage", mailbox_id=self.recipient.id, entry_id=entry_id)
            self.assertTrue(self.client.call("GetRawMessage", mailbox_id=self.recipient.id,
                                             entry_id=entry_id).raw_message)
            for method, fields in (("CreateMailbox", create_fields), ("SetReadState", read_fields),
                                   ("SubmitMessage", submit_fields)):
                outcome = self.unconfirmed_rpc(method, fields)
                print(f"\nNATS outage {method}: {outcome}, fate={rpc_fate(outcome)}", flush=True)
            try:
                smtp_outcome = self.smtp_rejected(smtp_subject, allow_rcpt_rejection=True)
            except (OSError, smtplib.SMTPServerDisconnected) as error:
                smtp_outcome = ("transport-uncertain", type(error).__name__)
            print(f"\nNATS outage SMTP: {smtp_outcome}", flush=True)
        finally:
            self.restore_services()
        # Status alone does not prove worker rejection; retain every original key.
        created = self.retry_rpc("CreateMailbox", create_fields).mailbox
        self.assertEqual(self.retry_rpc("CreateMailbox", create_fields).mailbox, created)
        self.assertEqual(created.address, create_fields["local_part"] + "@inbox.test")
        updated = self.retry_rpc("SetReadState", read_fields).entry
        self.assertTrue(updated.is_read)
        self.assertEqual(self.retry_rpc("SetReadState", read_fields).entry, updated)
        recovered = self.recover_submission(submit_fields)
        self.assert_single_delivery(recovered)
        self.assertEqual(self.recover_submission(submit_fields).id, recovered.id)
        self.assert_single_delivery(recovered)
        # DATA 451 can follow commit plus lost confirmation. SMTP has no retry key.
        # Never retry this send or assert its absence after broker recovery.

    def test_app_and_broker_restart_preserve_confirmed_submission_and_resume(self):
        fields = self.fields()
        watch = self.client.watch(timeout=self.timeout + 10)
        self.addCleanup(watch.close)
        baseline = watch.next_event(self.timeout)
        self.assertEqual(baseline.kind, pb.CHANGE_KIND_RESET)
        self.assertTrue(baseline.resume_token)
        seed = self.recover_submission(fields)
        try:
            watch.close()
            self.compose("stop", "app")
            self.compose("restart", "nats")
        finally:
            self.restore_services()
        # Poll the original ID before resubmitting; old durable work must survive.
        terminal = self.assert_single_delivery(seed)
        recovered = self.recover_submission(fields)
        self.assertEqual((recovered.id, recovered.message_id), (seed.id, seed.message_id))
        self.assert_single_delivery(recovered)
        resumed = self.client.watch(baseline.resume_token, timeout=self.timeout + 10)
        self.addCleanup(resumed.close)
        seen = changes.ChangeTests.next_matching(
            self, resumed, lambda e: e.kind == 1 or (e.kind == 3 and e.entry_id == terminal.recipients[0].entry_id))
        self.assertTrue(seen.resume_token)
        fresh = self.client.watch(timeout=self.timeout + 10)
        self.addCleanup(fresh.close)
        reset = fresh.next_event(self.timeout)
        self.assertEqual(reset.kind, 1)
        self.assertTrue(reset.resume_token)

    @unittest.skipUnless(os.getenv("INBOX_TEST_NATS_CRASH") == "1",
                         "Requires additional exclusive INBOX_TEST_NATS_CRASH=1")
    def test_process_crash_after_pg_commit_before_nats_confirmation(self):
        from crash_helpers import CrashScope, OwnedLockSession, OwnedSubmit, cleanup_operations, commit_rows

        self.timeout = float(os.getenv("INBOX_TEST_NATS_CRASH_TIMEOUT", "300"))
        self.assertGreaterEqual(self.timeout, 240, "Crash recovery must allow the 120-second AckWait")
        self.assertLessEqual(self.timeout, 600, "Crash fixture must remain bounded")
        sql = faults.FaultTests.sql.__get__(self)
        fields = self.fields()
        scope = CrashScope(fields["from_mailbox_id"], fields["idempotency_key"], fields["subject"])
        lock = OwnedLockSession(scope, sql, min(self.timeout, 30))
        request = OwnedSubmit(self.client, fields, self.timeout)
        try:
            pid = lock.acquire()
            request.thread.start()
            # A consumer waiting on this exact lock proves command admission, not just HTTP send.
            self.poll(lambda: int(sql(scope.admission_sql(pid))) > 0,
                      "admitted consumer waiting at the synthetic advisory lock")
            self.nats_pause_attempted = True
            self.compose("pause", "nats")
            lock.close()
            rows = self.poll(lambda: commit_rows(sql, scope),
                             "submission, journal result, and confirmation outbox committed")
            proof = rows[0]
            self.assertFalse(proof["published"], "Confirmation published before the crash barrier")
            self.assertTrue(proof["result_bytes"] and proof["sent_entry_id"])
            raw = self.client.call("GetRawMessage", mailbox_id=self.sender.id,
                                   entry_id=proof["sent_entry_id"]).raw_message
            self.assertTrue(fields["subject"].encode() in raw, "Committed S3 object must be readable")
            request.assert_no_success()
            # NATS is still paused: neither consumer nor outbox can confirm this new result.
            self.compose("kill", "-s", "SIGKILL", "app")
            request.thread.join(timeout=10)
            request.assert_no_success()
            self.assertFalse(request.thread.is_alive(), "Killed app must close the owned HTTP request")
            rows = commit_rows(sql, scope)
            self.assertEqual(len(rows), 1)
            self.assertFalse(rows[0]["published"])
            self.assertEqual(rows[0]["command_id"], proof["command_id"])
            print("\nCrash barriers: admitted consumer, committed S3/PG, unpublished confirmation, app SIGKILL", flush=True)
        finally:
            # Never wait for readiness while the broker might still be paused.
            cleanup_operations((
                ("unpause-nats", self.unpause_nats),
                ("release-owned-postgres-lock", lock.close),
                ("restore-services", self.restore_services),
                ("close-owned-http-submit", request.close),
            ))

        # First recover solely by original IDs, never by a new Submit command.
        seed = pb.Submission(id=proof["submission_id"], message_id=proof["message_id"],
                             from_mailbox_id=self.sender.id)
        self.assert_single_delivery(seed)

        def published():
            rows = commit_rows(sql, scope)
            return rows and rows[0]["published"] and rows[0]["command_id"] == proof["command_id"]

        self.poll(published, "original confirmation outbox publication after process restart")
        retry = self.recover_submission(fields)
        self.assertEqual((retry.id, retry.message_id), (seed.id, seed.message_id))
        self.assert_single_delivery(retry)

    validate = changes.ChangeTests.validate

    @property
    def private(self):
        return []


if __name__ == "__main__":
    unittest.main()
