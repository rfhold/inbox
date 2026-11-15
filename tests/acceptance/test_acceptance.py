import email.policy
from concurrent.futures import ThreadPoolExecutor
from email.parser import BytesParser
import os
import smtplib
import time
import unittest
import uuid

from client import Client, RpcError, pb


def unique():
    return "acceptance-" + uuid.uuid4().hex


def identifier(value):
    # API metadata can omit RFC header angle wrappers.
    value = value.strip()
    return value[1:-1] if value.startswith("<") and value.endswith(">") else value


def mime_fixture(address, reply_address, subject):
    # CRLF is intentional. The leading dot tests SMTP transparency, not MIME rewriting.
    return (f"From: UTF-8 Sender <outside@example.invalid>\r\n"
            f"To: {address}\r\n"
            f"Reply-To: Preferred <{reply_address}>, ignored@example.invalid\r\n"
            f"Subject: {subject}\r\n"
            "Date: Tue, 10 Nov 2026 12:34:56 +0000\r\n"
            "Message-ID: <fixture-" + subject + "@example.invalid>\r\n"
            "In-Reply-To: <ancestor@example.invalid>\r\n"
            "References: <root@example.invalid> <ancestor@example.invalid>\r\n"
            "MIME-Version: 1.0\r\n"
            "Content-Type: multipart/mixed; boundary=outer-boundary\r\n\r\n"
            "--outer-boundary\r\n"
            "Content-Type: multipart/alternative; boundary=inner-boundary\r\n\r\n"
            "--inner-boundary\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "Content-Transfer-Encoding: 8bit\r\n\r\n"
            "héllo 世界\r\n.leading dot\r\n"
            "--inner-boundary\r\n"
            "Content-Type: text/html; charset=utf-8\r\n\r\n"
            "<p>HTML must not replace plain text</p>\r\n"
            "--inner-boundary--\r\n"
            "--outer-boundary\r\n"
            "Content-Type: application/octet-stream\r\n"
            "Content-Disposition: attachment; filename=fixture.bin\r\n"
            "Content-Transfer-Encoding: base64\r\n\r\n"
            "AAECA/8=\r\n"
            "--outer-boundary--\r\n").encode("utf-8")


class AcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = Client()
        cls.timeout = float(os.getenv("INBOX_TEST_TIMEOUT", "30"))

    def setUp(self):
        self.sender = self.mailbox()
        self.recipient = self.mailbox()

    def mailbox(self):
        local = unique()
        result = self.client.call("CreateMailbox", local_part=local, name=local).mailbox
        self.assertEqual(result.address, local + "@inbox.test")
        self.assertTrue(result.id)
        return result

    def rpc_error(self, code, method, **fields):
        with self.assertRaises(RpcError) as caught:
            self.client.call(method, **fields)
        self.assertEqual(caught.exception.code, code, str(caught.exception))

    def poll(self, operation, description):
        deadline = time.monotonic() + self.timeout
        while True:
            value = operation()
            if value:
                return value
            if time.monotonic() >= deadline:
                self.fail("Timed out waiting for " + description)
            time.sleep(0.1)

    def entries(self, mailbox, folder):
        result, token, seen_tokens = [], "", set()
        while True:
            self.assertLess(len(seen_tokens), 100, "Pagination did not terminate")
            page = self.client.call("ListMessages", mailbox_id=mailbox.id, folder=folder,
                                    page_size=100, page_token=token)
            result.extend(page.entries)
            if not page.next_page_token:
                return result
            self.assertNotIn(page.next_page_token, seen_tokens)
            seen_tokens.add(page.next_page_token)
            token = page.next_page_token

    def submit(self, **overrides):
        fields = dict(from_mailbox_id=self.sender.id, to=[self.recipient.address],
                      subject=unique(), text_body="héllo\nbody", idempotency_key=unique())
        fields.update(overrides)
        response = self.client.call("SubmitMessage", **fields).submission
        self.assertTrue(response.id)
        self.assertTrue(response.message_id)
        self.assertEqual(response.from_mailbox_id, fields["from_mailbox_id"])
        return fields, response

    def delivered(self, submission):
        terminal = self.poll(
            lambda: self.terminal_submission(submission.id), "submission " + submission.id)
        self.assertEqual(terminal.status, pb.SUBMISSION_STATUS_SUCCEEDED, str(terminal))
        self.assertEqual(terminal.message_id, submission.message_id)
        self.assertTrue(terminal.recipients)
        for recipient in terminal.recipients:
            self.assertEqual(recipient.status, pb.RECIPIENT_STATUS_DELIVERED, str(recipient))
            self.assertTrue(recipient.entry_id)
            self.assertTrue(recipient.mailbox_id)
        return terminal

    def terminal_submission(self, submission_id):
        result = self.client.call("GetSubmission", submission_id=submission_id).submission
        return result if result.status in (2, 3, 4) else None

    def message(self, mailbox, entry):
        return self.client.call("GetMessage", mailbox_id=mailbox.id, entry_id=entry.id).message

    def raw(self, mailbox, entry):
        return self.client.call("GetRawMessage", mailbox_id=mailbox.id,
                                entry_id=entry.id).raw_message

    def test_delivery_idempotency_scoping_read_state_and_composed_headers(self):
        fields, initial = self.submit(cc=[self.recipient.address])
        terminal = self.delivered(initial)
        self.assertEqual(len(terminal.recipients), 1, "To/Cc recipient must be deduplicated")
        retry = self.client.call("SubmitMessage", **fields).submission
        self.assertEqual((retry.id, retry.message_id), (initial.id, initial.message_id))
        self.rpc_error(6, "SubmitMessage", **{**fields, "text_body": "different payload"})
        inbox = [e for e in self.entries(self.recipient, pb.FOLDER_INBOX)
                 if e.message_id == initial.message_id]
        sent = [e for e in self.entries(self.sender, pb.FOLDER_SENT)
                if e.message_id == initial.message_id]
        self.assertEqual(len(inbox), 1)
        self.assertEqual(len(sent), 1)
        entry = inbox[0]
        self.assertEqual(terminal.recipients[0].entry_id, entry.id)
        self.assertEqual(terminal.recipients[0].mailbox_id, self.recipient.id)
        self.assertEqual(entry.folder, pb.FOLDER_INBOX)
        self.assertNotEqual(entry.id, sent[0].id)
        self.assertFalse(self.message(self.recipient, entry).entry.is_read)
        self.assertFalse(self.message(self.recipient, entry).entry.is_read,
                         "GetMessage must not mark read")
        for desired in (True, True, False, False):
            updated = self.client.call("SetReadState", mailbox_id=self.recipient.id,
                                       entry_id=entry.id, is_read=desired).entry
            self.assertEqual(updated.is_read, desired)
            self.assertEqual(self.message(self.recipient, entry).entry.is_read, desired)
        for method in ("GetMessage", "GetRawMessage", "SetReadState"):
            self.rpc_error(5, method, mailbox_id=self.sender.id, entry_id=entry.id)
        raw = self.raw(self.recipient, entry)
        parsed = BytesParser(policy=email.policy.default).parsebytes(raw)
        detail = self.message(self.recipient, entry)
        self.assertEqual(detail.text_body.replace("\r\n", "\n").rstrip("\n"), fields["text_body"])
        self.assertEqual([address.addr_spec for address in parsed["From"].addresses], [self.sender.address])
        self.assertEqual([address.addr_spec for address in parsed["To"].addresses], [self.recipient.address])
        self.assertEqual([address.addr_spec for address in parsed["Cc"].addresses], [self.recipient.address])
        self.assertEqual(str(parsed["Subject"]), fields["subject"])
        self.assertTrue(parsed["Date"])
        self.assertTrue(parsed["Message-ID"])
        self.assertEqual(identifier(detail.internet_message_id), identifier(str(parsed["Message-ID"])))
        self.assertEqual(detail.reply_to_address, self.sender.address)
        self.assertIsNone(parsed["In-Reply-To"])
        self.assertIsNone(parsed["References"])
        self.assertEqual(entry.raw_size_bytes, len(raw))
        self.assertEqual(self.raw(self.sender, sent[0]), raw)
        _, other = self.submit(from_mailbox_id=self.recipient.id, to=[self.sender.address],
                               idempotency_key=fields["idempotency_key"])
        self.assertNotEqual(other.id, initial.id, "Keys are scoped to the sending mailbox")
        self.delivered(other)

    def test_mixed_invalid_recipients_reject_whole_submission(self):
        for bad in (unique() + "@inbox.test", unique() + "@example.invalid"):
            subject = unique()
            self.rpc_error(3, "SubmitMessage", from_mailbox_id=self.sender.id,
                           to=[self.recipient.address, bad], subject=subject,
                           text_body="must not arrive", idempotency_key=unique())
            self.rpc_error(3, "SubmitMessage", from_mailbox_id=self.sender.id,
                           to=[self.recipient.address], cc=[bad], subject=subject,
                           text_body="must not arrive", idempotency_key=unique())
            # A successful later delivery acts as a worker progress barrier.
            _, barrier = self.submit()
            self.delivered(barrier)
            for mailbox, folder in ((self.recipient, pb.FOLDER_INBOX),
                                    (self.sender, pb.FOLDER_SENT)):
                self.assertFalse([e for e in self.entries(mailbox, folder) if e.subject == subject])

    def test_cursor_stable_while_new_mail_arrives(self):
        original = set()
        for _ in range(4):
            _, submission = self.submit()
            terminal = self.delivered(submission)
            original.add(terminal.recipients[0].entry_id)
        first = self.client.call("ListMessages", mailbox_id=self.recipient.id,
                                 folder=pb.FOLDER_INBOX, page_size=2)
        self.assertEqual(len(first.entries), 2)
        self.assertTrue(first.next_page_token)
        _, newer = self.submit()
        new_entry = self.delivered(newer).recipients[0].entry_id
        result = list(first.entries)
        token, tokens = first.next_page_token, set()
        while token:
            self.assertLess(len(tokens), 100, "Pagination did not terminate")
            self.assertNotIn(token, tokens)
            tokens.add(token)
            page = self.client.call("ListMessages", mailbox_id=self.recipient.id,
                                    folder=pb.FOLDER_INBOX, page_size=2, page_token=token)
            result.extend(page.entries)
            token = page.next_page_token
        ids = [entry.id for entry in result]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(set(ids), original)
        self.assertNotIn(new_entry, ids)
        order = [(entry.created_at, entry.id) for entry in result]
        self.assertEqual(order, sorted(order, reverse=True))
        self.rpc_error(3, "ListMessages", mailbox_id=self.sender.id, folder=pb.FOLDER_INBOX,
                       page_size=2, page_token=first.next_page_token)
        self.rpc_error(3, "ListMessages", mailbox_id=self.recipient.id, folder=pb.FOLDER_SENT,
                       page_size=2, page_token=first.next_page_token)

    def smtp(self):
        return smtplib.SMTP(os.getenv("INBOX_TEST_SMTP_HOST", "localhost"),
                            int(os.getenv("INBOX_TEST_SMTP_PORT", "2525")), timeout=30)

    def test_smtp_mime_raw_fidelity_and_server_derived_reply_headers(self):
        subject = unique()
        raw = mime_fixture(self.recipient.address, self.sender.address, subject)
        with self.smtp() as smtp:
            self.assertEqual(smtp.sendmail("outside@example.invalid", [self.recipient.address], raw), {})
        entry = self.poll(lambda: next((e for e in self.entries(self.recipient, pb.FOLDER_INBOX)
                                       if e.subject == subject), None), "SMTP MIME delivery")
        self.assertEqual(self.raw(self.recipient, entry), raw)
        detail = self.message(self.recipient, entry)
        self.assertEqual(detail.text_body, "héllo 世界\r\n.leading dot")
        self.assertEqual(detail.reply_to_address, self.sender.address)
        self.assertEqual(identifier(detail.in_reply_to), "ancestor@example.invalid")
        self.assertEqual([identifier(value) for value in detail.references],
                         ["root@example.invalid", "ancestor@example.invalid"])
        self.assertEqual(entry.raw_size_bytes, len(raw))
        self.assertEqual(identifier(detail.internet_message_id), f"fixture-{subject}@example.invalid")
        fields, submission = self.submit(
            from_mailbox_id=self.recipient.id, to=[detail.reply_to_address],
            source_mailbox_id=self.recipient.id, reply_to_entry_id=entry.id)
        terminal = self.delivered(submission)
        reply_entry = next(e for e in self.entries(self.sender, pb.FOLDER_INBOX)
                           if e.id == terminal.recipients[0].entry_id)
        parsed = BytesParser(policy=email.policy.default).parsebytes(self.raw(self.sender, reply_entry))
        self.assertEqual([address.addr_spec for address in parsed["To"].addresses], [self.sender.address])
        self.assertEqual([address.addr_spec for address in parsed["From"].addresses], [self.recipient.address])
        self.assertEqual(str(parsed["In-Reply-To"]), f"<{identifier(detail.internet_message_id)}>")
        self.assertEqual(str(parsed["References"]).split(),
                         [f"<{identifier(value)}>" for value in detail.references]
                         + [f"<{identifier(detail.internet_message_id)}>"])
        self.assertTrue(parsed["Message-ID"])
        self.assertTrue(str(parsed["Message-ID"]).startswith("<"))
        self.assertTrue(str(parsed["Message-ID"]).endswith(">"))
        self.assertNotEqual(identifier(str(parsed["Message-ID"])), identifier(detail.internet_message_id))
        self.rpc_error(5, "SubmitMessage", **{**fields, "idempotency_key": unique(),
                                             "source_mailbox_id": self.sender.id})

    def test_smtp_unknown_recipient_and_external_relay_refused(self):
        with self.smtp() as smtp:
            smtp.ehlo_or_helo_if_needed()
            for address in (unique() + "@inbox.test", unique() + "@example.invalid"):
                self.assertEqual(smtp.mail("outside@example.invalid")[0] // 100, 2)
                code, _ = smtp.rcpt(address)
                self.assertEqual(code // 100, 5, f"Must reject RCPT TO {address}: {code}")
                smtp.rset()

    def test_header_injection_and_reply_field_validation(self):
        base = dict(from_mailbox_id=self.sender.id, to=[self.recipient.address],
                    subject=unique(), text_body="body", idempotency_key=unique())
        for changes in (
            {"subject": "ok\r\nBcc: attacker@example.invalid"},
            {"subject": "ok\nBcc: attacker@example.invalid"},
            {"to": [self.recipient.address + "\r\nBcc: attacker@example.invalid"]},
            {"cc": [self.recipient.address + "\nBcc: attacker@example.invalid"]},
            {"reply_to_entry_id": "missing"},
            {"source_mailbox_id": self.recipient.id},
            {"to": []},
            {"idempotency_key": ""},
        ):
            with self.subTest(changes=changes):
                self.rpc_error(3, "SubmitMessage", **{**base, **changes})

    def test_concurrent_identical_retries_create_one_delivery(self):
        fields = dict(from_mailbox_id=self.sender.id, to=[self.recipient.address],
                      subject=unique(), text_body="concurrent retry", idempotency_key=unique())
        with ThreadPoolExecutor(max_workers=4) as executor:
            submissions = list(executor.map(
                lambda _: self.client.call("SubmitMessage", **fields).submission, range(4)))
        identities = {(submission.id, submission.message_id) for submission in submissions}
        self.assertEqual(len(identities), 1)
        self.delivered(submissions[0])
        for mailbox, folder in ((self.recipient, pb.FOLDER_INBOX), (self.sender, pb.FOLDER_SENT)):
            matches = [e for e in self.entries(mailbox, folder) if e.subject == fields["subject"]]
            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0].message_id, submissions[0].message_id)

    def test_retry_after_discarding_response_body(self):
        fields = dict(from_mailbox_id=self.sender.id, to=[self.recipient.address],
                      subject=unique(), text_body="lost response", idempotency_key=unique())
        self.client.call("SubmitMessage", drop_body=True, **fields)
        recovered = self.client.call("SubmitMessage", **fields).submission
        self.delivered(recovered)
        again = self.client.call("SubmitMessage", **fields).submission
        self.assertEqual((again.id, again.message_id), (recovered.id, recovered.message_id))
        for mailbox, folder in ((self.recipient, pb.FOLDER_INBOX), (self.sender, pb.FOLDER_SENT)):
            matching = [e for e in self.entries(mailbox, folder) if e.subject == fields["subject"]]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0].message_id, recovered.message_id)

    @unittest.skipUnless(os.getenv("INBOX_TEST_LARGE") == "1", "Opt-in 25 MiB boundary checks")
    def test_exact_raw_limit_smtp_acceptance_and_download(self):
        subject = unique()
        headers = (f"From: outside@example.invalid\r\nTo: {self.recipient.address}\r\n"
                   f"Subject: {subject}\r\nContent-Type: text/plain; charset=us-ascii\r\n\r\n").encode()
        count, remainder = divmod(25 * 1024 * 1024 - len(headers), 78)
        if remainder == 1:
            count -= 1
            remainder += 78
        body = (b"x" * 76 + b"\r\n") * count
        if remainder:
            body += b"x" * (remainder - 2) + b"\r\n"
        raw = headers + body
        self.assertEqual(len(raw), 25 * 1024 * 1024)
        with self.smtp() as smtp:
            self.assertEqual(smtp.sendmail("outside@example.invalid", [self.recipient.address], raw), {})
        entry = self.poll(lambda: next((e for e in self.entries(self.recipient, pb.FOLDER_INBOX)
                                       if e.subject == subject), None), "exact-limit SMTP delivery")
        self.assertEqual(entry.raw_size_bytes, len(raw))
        self.assertEqual(self.raw(self.recipient, entry), raw,
                         "gRPC-Web must allow protobuf/frame overhead above the raw limit")

    @unittest.skipUnless(os.getenv("INBOX_TEST_LARGE") == "1", "Opt-in 25 MiB boundary checks")
    def test_oversize_api_and_smtp_rejected(self):
        self.rpc_error(8, "SubmitMessage", from_mailbox_id=self.sender.id,
                       to=[self.recipient.address], subject=unique(), text_body="x" * (25 * 1024 * 1024),
                       idempotency_key=unique())
        subject = unique()
        raw = (f"From: outside@example.invalid\r\nTo: {self.recipient.address}\r\n"
               f"Subject: {subject}\r\n\r\n").encode() + b"x" * 998 + b"\r\n"
        raw += (b"x" * 998 + b"\r\n") * ((25 * 1024 * 1024) // 1000 + 1)
        with self.smtp() as smtp:
            smtp.ehlo_or_helo_if_needed()
            options = [f"SIZE={len(raw)}"] if smtp.has_extn("size") else []
            code, _ = smtp.mail("outside@example.invalid", options=options)
            if code // 100 == 2:
                self.assertEqual(smtp.rcpt(self.recipient.address)[0] // 100, 2)
                try:
                    code, _ = smtp.data(raw)
                except smtplib.SMTPDataError as error:
                    code = error.smtp_code
            self.assertEqual(code, 552, "Oversize rejection must be a size-limit response")
        _, barrier = self.submit()
        self.delivered(barrier)
        self.assertFalse([e for e in self.entries(self.recipient, pb.FOLDER_INBOX)
                          if e.subject == subject])


if __name__ == "__main__":
    unittest.main()
