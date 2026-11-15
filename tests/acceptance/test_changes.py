"""Public WatchChanges acceptance. Run only after the NATS stack is ready."""

import os
import time
import unittest

from client import Client, pb
import test_acceptance as standard


class ChangeTests(unittest.TestCase):
    mailbox = standard.AcceptanceTests.mailbox
    entries = standard.AcceptanceTests.entries
    submit = standard.AcceptanceTests.submit
    delivered = standard.AcceptanceTests.delivered
    terminal_submission = standard.AcceptanceTests.terminal_submission
    poll = standard.AcceptanceTests.poll
    message = standard.AcceptanceTests.message
    raw = standard.AcceptanceTests.raw
    rpc_error = standard.AcceptanceTests.rpc_error

    def setUp(self):
        self.client = Client()
        self.timeout = float(os.getenv("INBOX_TEST_TIMEOUT", "30"))
        self.heartbeat_timeout = float(os.getenv("INBOX_TEST_HEARTBEAT_TIMEOUT", "35"))
        self.private = []

    def watch(self, token=""):
        stream = self.client.watch(token, timeout=max(self.timeout, self.heartbeat_timeout) + 10)
        self.addCleanup(stream.close)
        return stream

    def validate(self, event):
        self.assertIn(event.kind, (1, 2, 3, 4, 5))
        self.assertTrue(event.resume_token, "Every event needs a resumable watermark")
        wire = event.SerializeToString()
        for secret in self.private:
            self.assertNotIn(secret.encode(), wire, "Change events must not contain message content")
        if event.kind in (1, 5):
            self.assertFalse(event.mailbox_id or event.entry_id or event.submission_id)
        elif event.kind == 2:
            self.assertTrue(event.mailbox_id)
        elif event.kind == 3:
            self.assertTrue(event.mailbox_id and event.entry_id)
        elif event.kind == 4:
            self.assertTrue(event.submission_id)
        return event

    def next_matching(self, stream, predicate, timeout=None):
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            remaining = deadline - time.monotonic()
            self.assertGreater(remaining, 0, "Timed out waiting for a matching change")
            event = self.validate(stream.next_event(remaining))
            if predicate(event):
                return event

    def reset(self, stream):
        event = self.validate(stream.next_event(self.timeout))
        self.assertEqual(event.kind, pb.CHANGE_KIND_RESET)
        return event

    def test_committed_changes_multiple_clients_and_read_state(self):
        first, second = self.watch(), self.watch()
        self.reset(first)
        self.reset(second)
        self.sender, self.recipient = self.mailbox(), self.mailbox()
        self.private.extend([self.sender.address, self.recipient.address])
        for stream in (first, second):
            self.next_matching(stream, lambda e: e.kind == 2 and e.mailbox_id == self.recipient.id)
        self.assertTrue(self.entries(self.recipient, pb.FOLDER_INBOX) == [])
        fields, submission = self.submit(text_body="private-" + standard.unique())
        self.private.extend([fields["subject"], fields["text_body"]])
        # At event observation, the ID must already resolve through the direct read API.
        terminal = self.delivered(submission)
        entry_id = terminal.recipients[0].entry_id
        seen = set()
        deadline = time.monotonic() + self.timeout
        while seen != {"message", "submission"}:
            event = self.validate(first.next_event(max(0.01, deadline - time.monotonic())))
            self.assertLess(time.monotonic(), deadline)
            if event.kind == 4 and event.submission_id == submission.id:
                committed = self.client.call("GetSubmission", submission_id=event.submission_id).submission
                self.assertEqual(committed.id, submission.id)
                seen.add("submission")
            if event.kind == 3 and event.entry_id == entry_id:
                detail = self.client.call("GetMessage", mailbox_id=event.mailbox_id,
                                          entry_id=event.entry_id).message
                self.assertEqual(detail.entry.message_id, submission.message_id)
                self.assertFalse(detail.entry.is_read)
                seen.add("message")
        self.next_matching(second, lambda e: e.kind == 3 and e.entry_id == entry_id)
        first.close()
        request_id = standard.unique()
        fields = dict(mailbox_id=self.recipient.id, entry_id=entry_id, is_read=True,
                      request_id=request_id)
        updated = self.client.call("SetReadState", **fields).entry
        self.assertTrue(updated.is_read)
        self.next_matching(second, lambda e: e.kind == 3 and e.entry_id == entry_id)
        self.assertTrue(self.client.call("GetMessage", mailbox_id=self.recipient.id,
                                        entry_id=entry_id).message.entry.is_read)
        self.assertEqual(self.client.call("SetReadState", **fields).entry, updated)
        self.rpc_error(6, "SetReadState", **{**fields, "is_read": False})
        self.assertTrue(self.client.call("GetMessage", mailbox_id=self.recipient.id,
                                        entry_id=entry_id).message.entry.is_read)

    def test_create_request_id_replay_and_conflict(self):
        local, request_id = standard.unique(), standard.unique()
        fields = dict(local_part=local, name=local, request_id=request_id)
        original = self.client.call("CreateMailbox", **fields).mailbox
        self.assertEqual(self.client.call("CreateMailbox", **fields).mailbox, original)
        self.rpc_error(6, "CreateMailbox", **{**fields, "name": standard.unique()})
        self.rpc_error(6, "CreateMailbox", **{**fields, "local_part": standard.unique()})

    def test_resume_offline_mutations_and_invalid_cursor(self):
        initial = self.watch()
        saved = self.reset(initial).resume_token
        initial.close()
        self.sender, self.recipient = self.mailbox(), self.mailbox()
        fields, submission = self.submit()
        terminal = self.delivered(submission)
        entry_id = terminal.recipients[0].entry_id
        self.private.extend([fields["subject"], fields["text_body"], self.recipient.address])
        resumed = self.watch(saved)
        seen = set()
        deadline = time.monotonic() + self.timeout
        while seen != {"mailbox", "message", "submission"}:
            event = self.validate(resumed.next_event(max(0.01, deadline - time.monotonic())))
            self.assertLess(time.monotonic(), deadline)
            if event.kind == 1:
                # A reset is an explicit full-refresh instruction, never a silent gap.
                self.assertTrue([e for e in self.entries(self.recipient, pb.FOLDER_INBOX)
                                 if e.id == entry_id])
                self.assertEqual(self.client.call("GetSubmission", submission_id=submission.id).submission.id,
                                 submission.id)
                break
            if event.kind == 2 and event.mailbox_id == self.recipient.id:
                seen.add("mailbox")
            if event.kind == 3 and event.entry_id == entry_id:
                seen.add("message")
            if event.kind == 4 and event.submission_id == submission.id:
                seen.add("submission")
        invalid = self.watch("not-a-valid-cursor-" + standard.unique())
        self.reset(invalid)

    def test_reads_do_not_emit_mutations_and_heartbeat_has_no_invalidation_ids(self):
        self.sender, self.recipient = self.mailbox(), self.mailbox()
        _, submission = self.submit()
        terminal = self.delivered(submission)
        entry = self.client.call("GetMessage", mailbox_id=self.recipient.id,
                                 entry_id=terminal.recipients[0].entry_id).message.entry
        # Terminal PG state does not imply its change outbox has published yet.
        stream = self.watch()
        self.reset(stream)
        barrier = self.mailbox()
        # The serial oldest-first publisher must publish older seed changes first.
        self.next_matching(stream, lambda e: e.kind == 2 and e.mailbox_id == barrier.id)
        self.client.call("ListMailboxes", page_size=100)
        self.entries(self.recipient, pb.FOLDER_INBOX)
        self.message(self.recipient, entry)
        self.raw(self.recipient, entry)
        self.client.call("GetSubmission", submission_id=submission.id)
        deadline = time.monotonic() + self.heartbeat_timeout
        saw_heartbeat = False
        while time.monotonic() < deadline:
            try:
                event = self.validate(stream.next_event(max(0.01, deadline - time.monotonic())))
            except TimeoutError:
                break
            self.assertFalse(event.kind in (2, 3, 4) and
                             (event.mailbox_id in (self.sender.id, self.recipient.id) or
                              event.submission_id == submission.id), "Read RPC emitted a mutation")
            self.assertNotEqual(event.kind, 1, "A healthy idle stream must not force full refresh")
            if event.kind == 5:
                saw_heartbeat = True
                break
        self.assertTrue(saw_heartbeat, "No heartbeat within INBOX_TEST_HEARTBEAT_TIMEOUT")
        self.assertFalse(self.message(self.recipient, entry).entry.is_read)


if __name__ == "__main__":
    unittest.main()
