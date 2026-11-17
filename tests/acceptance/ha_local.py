"""Opt-in disposable two-process HA check; never targets configured/live services."""

from concurrent.futures import ThreadPoolExecutor
import contextlib
import json
import os
from pathlib import Path
import signal
import smtplib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from client import Client, pb


ROOT = Path(__file__).resolve().parents[2]


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=120).strip()


def eventually(check, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.URLError, subprocess.CalledProcessError):
            pass
        time.sleep(0.1)
    raise AssertionError("bounded local check timed out")


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def status(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main():
    if os.getenv("INBOX_TEST_DISPOSABLE_HA") != "1":
        raise SystemExit("Set INBOX_TEST_DISPOSABLE_HA=1 to create task-owned Docker services.")
    prefix = "inbox-ha-" + uuid.uuid4().hex[:10]
    containers, processes, logs = [], [], []
    (ROOT / ".local").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=prefix, dir=ROOT / ".local") as directory:
        directory = Path(directory)
        try:
            def container(name, image, container_port, *args, options=()):
                name = prefix + "-" + name
                containers.append(name)
                command("docker", "run", "-d", "--name", name, "--label", f"inbox.ha-test={prefix}",
                        "-p", f"127.0.0.1::{container_port}", *options, image, *args)
                bindings = json.loads(command("docker", "inspect", "--format",
                                              "{{json .NetworkSettings.Ports}}", name))
                if f"{container_port}/tcp" not in bindings:
                    raise RuntimeError(command("docker", "logs", name))
                return int(bindings[f"{container_port}/tcp"][0]["HostPort"])

            pg = container("pg", "postgres:17-alpine", 5432, options=(
                "-e", "POSTGRES_PASSWORD=synthetic-ha-only", "-e", "POSTGRES_DB=inbox"))
            nats = container("nats", "nats:2.15.3-alpine3.22@sha256:558394a23dd3c36f2d3a64b51865fd09908010e80a542cea5e75c2af543ed62a",
                             4222, "-js", "-sd", "/data")
            (directory / "rpc").write_text("0" * 64)
            (directory / "rpc").chmod(0o600)
            garage_config = (ROOT / "docker/garage.toml").read_text().replace(
                "/run/secrets/garage_rpc_secret", "/ha-rpc")
            (directory / "garage.toml").write_text(garage_config)
            s3 = container("s3", "dxflrs/garage:v2.5.0", 3900, "/garage", "server",
                           "--single-node", "--default-bucket", options=(
                               "-v", f"{directory / 'garage.toml'}:/etc/garage.toml:ro",
                               "-v", f"{directory / 'rpc'}:/ha-rpc:ro",
                               "-e", "GARAGE_DEFAULT_ACCESS_KEY=GK" + "1" * 24,
                               "-e", "GARAGE_DEFAULT_SECRET_KEY=" + "2" * 64,
                               "-e", "GARAGE_DEFAULT_BUCKET=inbox"))
            eventually(lambda: command("docker", "exec", containers[0],
                                       "pg_isready", "-U", "postgres", "-d", "inbox") and True)
            eventually(lambda: command("docker", "exec", containers[2],
                                       "/garage", "bucket", "info", "inbox") and True)
            subprocess.run(["cargo", "test", "--locked", "-p", "inbox-server", "--lib",
                            "pipeline::tests::shutdown_keeps_publisher_until_workers_finish_and_bounds_all_tasks",
                            "--", "--ignored", "--exact"], cwd=ROOT, check=True, timeout=180,
                           env={**os.environ, "INBOX_TEST_NATS_URL": f"nats://127.0.0.1:{nats}"})
            subprocess.run(["cargo", "test", "--locked", "-p", "inbox-server", "--lib",
                            "auth::tests::", "--", "--ignored"], cwd=ROOT, check=True, timeout=180,
                           env={**os.environ, "INBOX_TEST_DATABASE_URL":
                                f"postgres://postgres:synthetic-ha-only@127.0.0.1:{pg}/inbox"})
            urls, smtp_ports = [], []
            for index in range(2):
                http_port, smtp_port = port(), port()
                urls.append(f"http://127.0.0.1:{http_port}")
                smtp_ports.append(smtp_port)
                log = (directory / f"app-{index}.log").open("w+")
                logs.append(log)
                env = {"PATH": os.environ["PATH"], "HOME": str(directory), "INBOX_AUTH_MODE": "disabled",
                       "DATABASE_URL": f"postgres://postgres:synthetic-ha-only@127.0.0.1:{pg}/inbox",
                       "INBOX_NATS_URL": f"nats://127.0.0.1:{nats}",
                       "INBOX_NATS_NAMESPACE": "INBOX", "INBOX_NATS_REPLICAS": "1",
                       "INBOX_S3_ENDPOINT": f"http://127.0.0.1:{s3}",
                       "INBOX_S3_REGION": "garage", "INBOX_S3_BUCKET": "inbox",
                       "AWS_ACCESS_KEY_ID": "GK" + "1" * 24, "AWS_SECRET_ACCESS_KEY": "2" * 64,
                       "INBOX_DOMAIN": "inbox.test", "INBOX_HTTP_ADDR": f"127.0.0.1:{http_port}",
                       "INBOX_SMTP_ADDR": f"127.0.0.1:{smtp_port}",
                       "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1",
                       "INBOX_WEB_DIR": str(ROOT / "web/dist")}
                processes.append(subprocess.Popen([str(ROOT / "target/debug/inbox-server")],
                                                  cwd=ROOT, env=env, stdout=log, stderr=log))
            for url in urls:
                eventually(lambda: status(url + "/ready") == 200)
            clients = [Client(url) for url in urls]
            key = str(uuid.uuid4())
            fields = dict(local_part="ha-" + uuid.uuid4().hex, name="Synthetic HA", request_id=key)
            with ThreadPoolExecutor(max_workers=2) as pool:
                created = list(pool.map(lambda client: client.call("CreateMailbox", **fields), clients))
            assert created[0].mailbox.id == created[1].mailbox.id
            mailbox = created[0].mailbox
            assert sum(box.id == mailbox.id for box in clients[1].call("ListMailboxes").mailboxes) == 1
            print("PASS: two real processes, cross-replica same-key creation has one mailbox", flush=True)

            sender = clients[0].call("CreateMailbox", local_part="sender-" + uuid.uuid4().hex,
                                     name="Synthetic sender", request_id=str(uuid.uuid4())).mailbox
            page = clients[0].call("ListMailboxes", page_size=1)
            assert len(page.mailboxes) == 1 and page.next_page_token
            next_page = clients[1].call("ListMailboxes", page_size=1,
                                       page_token=page.next_page_token)
            assert len(next_page.mailboxes) == 1
            assert {page.mailboxes[0].id, next_page.mailboxes[0].id} == {sender.id, mailbox.id}
            print("PASS: page cursor from one replica resolves on the other", flush=True)
            send = dict(from_mailbox_id=sender.id, to=[mailbox.address], cc=[],
                        subject="Synthetic HA", text_body="Synthetic", idempotency_key=str(uuid.uuid4()))
            with ThreadPoolExecutor(max_workers=2) as pool:
                submissions = list(pool.map(lambda client: client.call("SubmitMessage", **send), clients))
            assert submissions[0].submission.id == submissions[1].submission.id
            def received():
                entries = clients[1].call("ListMessages", mailbox_id=mailbox.id, folder=1).entries
                return entries if len(entries) == 1 else None
            initial_entries = eventually(received)
            initial_entry_id = initial_entries[0].id
            print("PASS: cross-replica same-key submission has one Inbox effect", flush=True)

            with clients[0].watch() as watch:
                first = watch.next_event(10)
                cursor = first.resume_token
                smtp = smtplib.SMTP("127.0.0.1", smtp_ports[0], timeout=20)
                idle = smtplib.SMTP("127.0.0.1", smtp_ports[0], timeout=20)
                smtp.ehlo()
                assert smtp.mail("external@example.org")[0] == 250
                assert smtp.rcpt(mailbox.address)[0] == 250
                smtp.putcmd("DATA")
                assert smtp.getreply()[0] == 354
                started = time.monotonic()
                processes[0].send_signal(signal.SIGTERM)
                eventually(lambda: status(urls[0] + "/ready") == 503, 5)
                assert status(urls[0] + "/health") == 200
                assert clients[0].call("ListMailboxes").mailboxes
                time.sleep(max(0, started + 10.5 - time.monotonic()))
                assert idle.getreply()[0] == 421
                idle.close()
                smtp.send(b"From: external@example.org\r\nTo: " + mailbox.address.encode() +
                          b"\r\nSubject: Synthetic drain\r\n\r\nAccepted before drain\r\n.\r\n")
                assert smtp.getreply()[0] == 250
                smtp.close()
                processes[0].wait(timeout=130)
                assert processes[0].returncode == 0
                assert time.monotonic() - started < 30, "streams must not consume admission budget"
                assert status(urls[1] + "/ready") == 200
            def smtp_received():
                entries = clients[1].call("ListMessages", mailbox_id=mailbox.id, folder=1).entries
                return entries if len(entries) == 2 else None
            entries = eventually(smtp_received)
            smtp_entry_id = next(entry.id for entry in entries if entry.id != initial_entry_id)
            with clients[1].watch(cursor) as resumed:
                deadline = time.monotonic() + 15
                while True:
                    event = resumed.next_event(max(0.1, deadline - time.monotonic()))
                    assert event.kind != pb.CHANGE_KIND_RESET, "private retained history must replay"
                    if (event.kind == pb.CHANGE_KIND_MESSAGE and event.mailbox_id == mailbox.id
                            and event.entry_id == smtp_entry_id):
                        assert event.resume_token and event.resume_token != cursor
                        break
                    assert time.monotonic() < deadline, "SMTP entry change did not replay"
            print("PASS: SIGTERM readiness withdrawal, idle SMTP 421, admitted DATA durable 250, "
                  "surviving replica and exact SMTP entry change replay", flush=True)
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=250)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs:
                log.seek(0)
                content = log.read()
                if "ERROR" in content:
                    print(content[-4000:])
                log.close()
            for name in reversed(containers):
                with contextlib.suppress(subprocess.CalledProcessError):
                    owner = command("docker", "inspect", "--format",
                                    '{{index .Config.Labels "inbox.ha-test"}}', name)
                    if owner == prefix:
                        command("docker", "rm", "-f", name)


if __name__ == "__main__":
    main()
