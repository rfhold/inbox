"""Scoped SQL and owned processes for the explicitly enabled crash fixture."""

import http.client
import json
import queue
import re
import socket
import subprocess
import threading
import time
import uuid

from client import decode_response, frame, pb
from test_faults import ROOT, sql_uuid


class ComposeOperationError(RuntimeError):
    def __init__(self, operation, service, exit_status):
        if operation not in {"start", "stop", "restart", "pause", "unpause", "kill", "exec", "logs", "ps"}:
            raise ValueError("Invalid diagnostic operation")
        if service not in {"postgres", "garage", "nats", "app", "unspecified"}:
            raise ValueError("Invalid diagnostic service")
        if type(exit_status) is not int and exit_status not in {"timeout", "not-started"}:
            raise ValueError("Invalid diagnostic exit status")
        self.operation, self.service, self.exit_status = operation, service, exit_status
        super().__init__(f"docker compose operation={operation} service={service} exit_status={exit_status}")


class RestorationError(AssertionError):
    def __init__(self, failures):
        self.failures = tuple(failures)
        super().__init__("Service restoration failed: " + "; ".join(str(item) for item in self.failures))


class ServiceReadinessError(RuntimeError):
    def __init__(self, service, state, health):
        if service not in {"postgres", "garage", "nats", "app"}:
            raise ValueError("Invalid readiness service")
        if state not in {"created", "running", "paused", "restarting", "removing", "exited", "dead", "missing", "invalid-response"}:
            raise ValueError("Invalid readiness state")
        if health not in {"", "starting", "healthy", "unhealthy", "unknown"}:
            raise ValueError("Invalid readiness health")
        super().__init__(f"readiness service={service} state={state} health={health or 'unset'}")


class DependencyReadinessError(RuntimeError):
    def __init__(self):
        super().__init__("app start withheld: not all required services observed running and healthy")


def wait_service_healthy(compose, service, timeout, clock=time.monotonic, pause=time.sleep):
    ServiceReadinessError(service, "missing", "unknown")
    deadline = clock() + timeout
    state, health = "missing", "unknown"
    while clock() < deadline:
        response = compose("ps", "--all", "--format", "{{.State}}|{{.Health}}", service)
        if not isinstance(response, str):
            raise ServiceReadinessError(service, "invalid-response", "unknown")
        rows = response.strip().splitlines()
        if not rows:
            state, health = "missing", "unknown"
        elif len(rows) == 1 and rows[0].count("|") == 1:
            state, health = rows[0].split("|")
            try:
                ServiceReadinessError(service, state, health)
            except ValueError:
                raise ServiceReadinessError(service, "invalid-response", "unknown") from None
        else:
            raise ServiceReadinessError(service, "invalid-response", "unknown")
        if (state, health) == ("running", "healthy") and clock() < deadline:
            return
        remaining = deadline - clock()
        if remaining > 0:
            pause(min(0.1, remaining))
    raise ServiceReadinessError(service, state, health)


_SAFE_HELPER_MESSAGES = {
    "Owned PostgreSQL lock readiness marker missing",
    "Owned PostgreSQL lock session exited before readiness",
    "Owned PostgreSQL lock process exited unsuccessfully",
    "Owned Submit HTTP thread did not stop",
}


def safe_error_detail(error):
    if isinstance(error, (ComposeOperationError, RestorationError, ServiceReadinessError, DependencyReadinessError)):
        return str(error)[:1000]
    if type(error) in (RuntimeError, TimeoutError) and str(error) in _SAFE_HELPER_MESSAGES:
        return type(error).__name__ + ": " + str(error)
    # Never copy arbitrary exception text, command arguments, stdout, or stderr.
    return re.sub(r"[^a-zA-Z0-9_]", "_", type(error).__name__)[:64]


def operation_failure(operation, error):
    if not re.fullmatch(r"[a-z-]{1,64}", operation):
        raise ValueError("Invalid diagnostic operation label")
    return AssertionError(operation + ": " + safe_error_detail(error))


def cleanup_operations(operations):
    failures = []
    for name, operation in operations:
        try:
            operation()
        except Exception as error:
            failures.append(operation_failure(name, error))
    if failures:
        # Raised outside the handlers: sanitized children do not retain unsafe causes.
        # An already-active test failure remains the group's normal exception context.
        raise ExceptionGroup("Crash fixture restoration failed", failures)


def run_compose(arguments, timeout, input_text=None):
    operation = arguments[0]
    candidate = arguments[2] if arguments[:2] == ("exec", "-T") else arguments[-1]
    service = candidate if candidate in {"postgres", "garage", "nats", "app"} else "unspecified"
    ComposeOperationError(operation, service, 0)
    try:
        command_timeout = min(timeout + 30, 5) if operation == "ps" else timeout + 30
        process = subprocess.run(["docker", "compose", *arguments], cwd=ROOT, input=input_text,
                                 text=True, capture_output=True, timeout=command_timeout)
    except subprocess.TimeoutExpired:
        raise ComposeOperationError(operation, service, "timeout") from None
    except OSError:
        raise ComposeOperationError(operation, service, "not-started") from None
    if process.returncode:
        raise ComposeOperationError(operation, service, process.returncode)
    return process.stdout.strip()


def synthetic(value):
    if not isinstance(value, str) or not re.fullmatch(r"acceptance-[0-9a-f]{32}", value):
        raise ValueError("Expected a unique synthetic acceptance label")
    return "'" + value + "'"


class CrashScope:
    def __init__(self, sender, key, subject):
        self.sender = sql_uuid(sender)
        self.key = synthetic(key)
        self.subject = synthetic(subject)
        self.lock_key = "'" + str(uuid.UUID(sender)) + ":" + key + "'"
        self.owner = "acceptance-lock-" + uuid.uuid4().hex

    @property
    def acquire_sql(self):
        return (f"SET application_name='{self.owner}';\n"
                f"SELECT pg_advisory_lock(hashtextextended({self.lock_key},0));\n"
                "SELECT 'ACCEPTANCE_LOCK_READY:' || pg_backend_pid();\n")

    @property
    def release_sql(self):
        return f"SELECT pg_advisory_unlock(hashtextextended({self.lock_key},0));\n\\q\n"

    def terminate_sql(self, pid):
        if type(pid) is not int or not 1 <= pid <= 2147483647:
            raise ValueError("Invalid owned backend PID")
        return ("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE pid={pid} AND application_name='{self.owner}';")

    @property
    def terminate_owner_sql(self):
        return ("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE application_name='{self.owner}' AND backend_type='client backend';")

    def admission_sql(self, pid):
        self.terminate_sql(pid)
        return f"""WITH k AS (SELECT hashtextextended({self.lock_key},0) AS v)
SELECT count(*) FROM pg_locks AS held JOIN pg_locks AS waiting
 ON waiting.locktype=held.locktype AND waiting.database=held.database
 AND waiting.classid=held.classid AND waiting.objid=held.objid AND waiting.objsubid=held.objsubid
 JOIN pg_stat_activity AS owner ON owner.pid=held.pid CROSS JOIN k
WHERE held.locktype='advisory' AND held.granted AND held.pid={pid}
 AND owner.application_name='{self.owner}'
 AND held.classid=((k.v >> 32) & 4294967295)::oid
 AND held.objid=(k.v & 4294967295)::oid AND held.objsubid=1
 AND NOT waiting.granted AND waiting.pid<>held.pid;"""

    @property
    def commit_sql(self):
        # Outcome protobuf bytes contain both UUID strings; no unrelated journal rows qualify.
        return f"""SELECT coalesce(json_agg(json_build_object(
 'submission_id',s.id,'message_id',s.message_id,'command_id',j.id,
 'published',o.published,'result_bytes',octet_length(j.result),
 'sent_entry_id',(SELECT id FROM mailbox_entries WHERE mailbox_id=s.from_mailbox_id
                  AND message_id=s.message_id AND folder=2))), '[]'::json)::text
FROM submissions AS s JOIN messages AS m ON m.id=s.message_id
JOIN command_journal AS j
 ON position(convert_to(s.id::text,'UTF8') IN j.result)>0
 AND position(convert_to(s.message_id::text,'UTF8') IN j.result)>0
JOIN command_outbox AS o ON o.subject='INBOX.results.' || j.id::text
WHERE s.from_mailbox_id={self.sender} AND s.idempotency_key={self.key}
 AND m.content->>'subject'={self.subject};"""


def lock_command():
    return ["docker", "compose", "exec", "-T", "postgres", "psql", "--no-psqlrc",
            "-U", "inbox", "-d", "inbox", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"]


class OwnedLockSession:
    def __init__(self, scope, sql, timeout):
        self.scope, self.sql, self.timeout = scope, sql, timeout
        self.process = None
        self.pid = None

    def acquire(self):
        self.process = subprocess.Popen(lock_command(), cwd=ROOT, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                        text=True, bufsize=1)
        ready = queue.Queue(maxsize=1)

        def marker():
            for line in self.process.stdout:
                match = re.fullmatch(r"ACCEPTANCE_LOCK_READY:([0-9]+)\s*", line)
                if match:
                    ready.put(int(match[1]))
                    return
            ready.put(None)

        reader = threading.Thread(target=marker, daemon=True)
        reader.start()
        self.process.stdin.write(self.scope.acquire_sql)
        self.process.stdin.flush()
        try:
            self.pid = ready.get(timeout=self.timeout)
        except queue.Empty:
            raise TimeoutError("Owned PostgreSQL lock readiness marker missing") from None
        if self.pid is None:
            raise RuntimeError("Owned PostgreSQL lock session exited before readiness")
        reader.join(timeout=2)
        return self.pid

    def close(self):
        process = self.process
        if process is None:
            return
        try:
            if process.poll() is None:
                try:
                    process.stdin.write(self.scope.release_sql)
                    process.stdin.flush()
                    process.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    try:
                        self.sql(self.scope.terminate_sql(self.pid) if self.pid is not None
                                 else self.scope.terminate_owner_sql)
                    finally:
                        if process.poll() is None:
                            process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
            if process.returncode not in (0, -15, -9):
                raise RuntimeError("Owned PostgreSQL lock process exited unsuccessfully")
        finally:
            for pipe in (process.stdin, process.stdout):
                if pipe is not None:
                    pipe.close()


class OwnedSubmit:
    def __init__(self, client, fields, timeout):
        url = client.url
        connection_type = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
        self.connection = connection_type(url.hostname, url.port, timeout=timeout)
        self.url, self.fields = url, fields
        self.results = queue.Queue(maxsize=1)
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            self.connection.request("POST", self.url.path.rstrip("/") + "/inbox.v1.MailService/SubmitMessage",
                                    frame(pb.SubmitMessageRequest(**self.fields).SerializeToString()),
                                    {"Content-Type": "application/grpc-web+proto", "X-Grpc-Web": "1"})
            response = self.connection.getresponse()
            if response.status != 200:
                result = ("http", response.status)
            else:
                result = decode_response(response.read(), dict(response.getheaders()),
                                         pb.SubmitMessageResponse).submission
        except Exception as error:
            result = error
        finally:
            self.connection.close()
        self.results.put(result)

    def assert_no_success(self):
        try:
            result = self.results.get_nowait()
        except queue.Empty:
            return
        if isinstance(result, pb.Submission):
            raise AssertionError("Application succeeded before durable NATS confirmation")

    def close(self):
        sock = self.connection.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.connection.close()
        if self.thread.ident is not None:
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                raise RuntimeError("Owned Submit HTTP thread did not stop")


def commit_rows(sql, scope):
    rows = json.loads(sql(scope.commit_sql))
    if len(rows) > 1:
        raise AssertionError("Synthetic command matched multiple journal confirmations")
    return rows
