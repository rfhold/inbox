"""Pure fixture guards; no app, Docker, SQL, or service mutations."""

import io
import subprocess
import traceback
import unittest
from unittest.mock import Mock, patch
import uuid

from crash_helpers import (ComposeOperationError, CrashScope, OwnedLockSession, RestorationError,
                           ServiceReadinessError, cleanup_operations, lock_command, run_compose,
                           synthetic, wait_service_healthy)
import test_nats_faults as nats


class Clock:
    def __init__(self):
        self.now, self.delays = 0.0, []

    def __call__(self):
        return self.now

    def pause(self, seconds):
        self.delays.append(seconds)
        self.now += seconds


class CrashProtocolTests(unittest.TestCase):
    def scope(self):
        return CrashScope(str(uuid.uuid4()), "acceptance-" + uuid.uuid4().hex,
                          "acceptance-" + uuid.uuid4().hex)

    def test_scope_rejects_sql_and_non_synthetic_values(self):
        for value in ("x'; SELECT 1; --", "acceptance-" + "a" * 31, "acceptance-" + "A" * 32,
                      "acceptance-" + "a" * 32 + "\n", None):
            with self.assertRaises(ValueError):
                synthetic(value)
        with self.assertRaises(ValueError):
            CrashScope("bad uuid", "acceptance-" + "a" * 32, "acceptance-" + "b" * 32)

    def test_admission_uses_exact_hash_owner_and_waiter_not_queries_or_global_counts(self):
        scope = self.scope()
        sql = scope.admission_sql(123)
        for fragment in (scope.lock_key, "hashtextextended", "held.pid=123", scope.owner,
                         "NOT waiting.granted", "held.objsubid=1", "4294967295", "waiting.database=held.database"):
            self.assertIn(fragment, sql)
        self.assertIn(scope.lock_key, scope.acquire_sql)
        self.assertIn(scope.lock_key, scope.release_sql)
        self.assertIn("pg_backend_pid()", scope.acquire_sql)
        for pid in (0, -1, 2**31, "123", True, "1; SELECT 1"):
            with self.assertRaises(ValueError):
                scope.terminate_sql(pid)

    def test_commit_proof_scoped_and_read_only_and_lock_command_local(self):
        scope = self.scope()
        sql = scope.commit_sql
        for fragment in (scope.sender, scope.key, scope.subject, "command_journal", "command_outbox",
                         "'INBOX.results.' || j.id::text", "o.published", "convert_to(s.id::text",
                         "convert_to(s.message_id::text"):
            self.assertIn(fragment, sql)
        for forbidden in ("DELETE", "UPDATE", "DROP", "TRUNCATE", "INSERT"):
            self.assertNotIn(forbidden, sql)
        command = lock_command()
        self.assertEqual(command[:6], ["docker", "compose", "exec", "-T", "postgres", "psql"])
        self.assertNotIn("shell", command)
        self.assertIn("ON_ERROR_STOP=1", command)

    def test_cleanup_targets_only_owned_process_and_backend(self):
        scope = self.scope()
        sql = Mock()
        session = OwnedLockSession(scope, sql, 1)
        process = Mock()
        process.stdin, process.stdout = io.StringIO(), io.StringIO()
        process.poll.side_effect = [None, None]
        process.wait.side_effect = [subprocess.TimeoutExpired("owned", 5), 0]
        process.returncode = -15
        session.process, session.pid = process, 123
        session.close()
        sql.assert_called_once_with(scope.terminate_sql(123))
        self.assertIn("pid=123", sql.call_args.args[0])
        self.assertIn(scope.owner, sql.call_args.args[0])
        process.terminate.assert_called_once_with()
        process.kill.assert_not_called()
        self.assertTrue(process.stdin.closed and process.stdout.closed)

    def test_cleanup_attempts_owned_process_even_when_sql_fails(self):
        scope = self.scope()
        sql = Mock(side_effect=RuntimeError("synthetic SQL failure"))
        session = OwnedLockSession(scope, sql, 1)
        process = Mock()
        process.stdin, process.stdout = io.StringIO(), io.StringIO()
        process.poll.side_effect = [None, None]
        process.wait.side_effect = [subprocess.TimeoutExpired("owned", 5), 0]
        session.process = process
        with self.assertRaises(RuntimeError):
            session.close()
        sql.assert_called_once_with(scope.terminate_owner_sql)
        process.terminate.assert_called_once_with()
        self.assertTrue(process.stdin.closed and process.stdout.closed)

    def test_readiness_marker_and_process_creation_are_owned_and_bounded(self):
        session = OwnedLockSession(self.scope(), Mock(), 1)
        process = Mock()
        process.stdin = io.StringIO()
        process.stdout = io.StringIO("\nACCEPTANCE_LOCK_READY:123\n")
        process.poll.return_value = 0
        process.returncode = 0
        with patch("crash_helpers.subprocess.Popen", return_value=process) as popen:
            self.assertEqual(session.acquire(), 123)
            self.assertEqual(process.stdin.getvalue(), session.scope.acquire_sql)
            self.assertEqual(popen.call_args.args[0], lock_command())
            self.assertNotIn("shell", popen.call_args.kwargs)
            self.assertEqual(popen.call_args.kwargs["stderr"], subprocess.DEVNULL)
        session.close()

    def test_unpause_precedes_every_readiness_wait(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout, fixture.nats_pause_attempted = 300, True
        fixture.compose = Mock(return_value="running|healthy")
        fixture.restore_services()
        calls = fixture.compose.call_args_list
        self.assertEqual(calls[0].args, ("unpause", "nats"))
        self.assertEqual([call.args[0] for call in calls[1:]], ["start", "ps"] * 4)
        self.assertTrue(all(call.args == ("start", call.args[-1]) for call in calls[1::2]))
        self.assertFalse(fixture.nats_pause_attempted)

    def test_failed_unpause_prevents_readiness_wait(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout, fixture.nats_pause_attempted = 300, True
        fixture.compose = Mock(side_effect=RuntimeError("synthetic unpause failure"))
        with self.assertRaises(RuntimeError):
            fixture.restore_services()
        fixture.compose.assert_called_once_with("unpause", "nats")
        self.assertTrue(fixture.nats_pause_attempted)

    def test_compose_failure_reports_only_operation_service_and_exit_status(self):
        process = Mock(returncode=1, stdout="PRIVATE_STDOUT", stderr="PRIVATE_STDERR")
        with patch("crash_helpers.subprocess.run", return_value=process):
            with self.assertRaises(ComposeOperationError) as raised:
                run_compose(("start", "--wait", "--wait-timeout", "300", "app"), 300,
                            input_text="PRIVATE_INPUT")
        self.assertEqual(str(raised.exception), "docker compose operation=start service=app exit_status=1")
        self.assertIsNone(raised.exception.__cause__)

    def test_compose_timeout_and_launch_failure_do_not_expose_exception_text(self):
        for error, status in (
            (subprocess.TimeoutExpired(["PRIVATE_ARG"], 1, output="PRIVATE_OUTPUT", stderr="PRIVATE_ERROR"), "timeout"),
            (OSError("PRIVATE_ERROR"), "not-started"),
        ):
            with patch("crash_helpers.subprocess.run", side_effect=error):
                with self.assertRaises(ComposeOperationError) as raised:
                    run_compose(("exec", "-T", "postgres", "psql"), 1, "PRIVATE_SQL")
            self.assertEqual(str(raised.exception),
                             f"docker compose operation=exec service=postgres exit_status={status}")
            rendered = "".join(traceback.format_exception(raised.exception))
            self.assertNotIn("PRIVATE_", rendered)

    def test_cleanup_names_every_failure_and_attempts_every_operation(self):
        operations = [
            ("unpause-nats", Mock(side_effect=ComposeOperationError("unpause", "nats", 1))),
            ("release-owned-postgres-lock", Mock(side_effect=RuntimeError("PRIVATE_SQL_OR_CONTENT"))),
            ("restore-services", Mock()),
            ("close-owned-http-submit", Mock(side_effect=RuntimeError("Owned Submit HTTP thread did not stop"))),
        ]
        with self.assertRaises(ExceptionGroup) as raised:
            cleanup_operations(operations)
        for _, operation in operations:
            operation.assert_called_once_with()
        messages = [str(error) for error in raised.exception.exceptions]
        self.assertEqual(messages, [
            "unpause-nats: docker compose operation=unpause service=nats exit_status=1",
            "release-owned-postgres-lock: RuntimeError",
            "close-owned-http-submit: RuntimeError: Owned Submit HTTP thread did not stop",
        ])
        rendered = "".join(traceback.format_exception(raised.exception))
        self.assertNotIn("PRIVATE_SQL_OR_CONTENT", rendered)

    def test_cleanup_group_preserves_original_failure_context(self):
        original = AssertionError("original proof assertion failed")
        try:
            try:
                raise original
            finally:
                cleanup_operations((("restore-services", Mock(side_effect=ComposeOperationError("start", "app", 1))),))
        except ExceptionGroup as error:
            self.assertIs(error.__context__, original)
            self.assertIn("original proof assertion failed", "".join(traceback.format_exception(error)))
        else:
            self.fail("Expected restoration failure group")

    def test_restore_keeps_service_exit_status_and_attempts_other_services(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout = 300

        def compose(*arguments):
            if arguments[-1] == "app":
                raise ComposeOperationError("start", "app", 1)
            return "running|healthy"

        fixture.compose = Mock(side_effect=compose)
        with self.assertRaises(RestorationError) as raised:
            fixture.restore_services()
        self.assertEqual([call.args[-1] for call in fixture.compose.call_args_list],
                          ["postgres", "postgres", "garage", "garage", "nats", "nats", "app"])
        self.assertEqual(str(raised.exception),
                          "Service restoration failed: restore-app: docker compose operation=start service=app exit_status=1")
        with self.assertRaises(ExceptionGroup) as grouped:
            cleanup_operations((("restore-services", Mock(side_effect=raised.exception)),))
        self.assertIn("service=app exit_status=1", str(grouped.exception.exceptions[0]))

    def test_invalid_diagnostic_operation_never_launches_compose(self):
        with patch("crash_helpers.subprocess.run") as run:
            with self.assertRaises(ValueError):
                run_compose(("PRIVATE_INVALID_OPERATION", "app"), 1)
        run.assert_not_called()

    def test_existing_unhealthy_service_waits_for_observed_healthy_transition(self):
        clock = Clock()
        compose = Mock(side_effect=["running|unhealthy", "running|starting", "running|healthy"])
        wait_service_healthy(compose, "nats", 1, clock=clock, pause=clock.pause)
        self.assertEqual(compose.call_count, 3)
        self.assertEqual(clock.delays, [0.1, 0.1])
        self.assertTrue(all(call.args == ("ps", "--all", "--format", "{{.State}}|{{.Health}}", "nats")
                            for call in compose.call_args_list))

    def test_permanent_unhealthy_service_fails_at_bounded_deadline(self):
        clock = Clock()
        compose = Mock(return_value="running|unhealthy")
        with self.assertRaises(ServiceReadinessError) as raised:
            wait_service_healthy(compose, "nats", 0.25, clock=clock, pause=clock.pause)
        self.assertEqual(clock.now, 0.25)
        self.assertEqual(compose.call_count, 3)
        self.assertEqual(str(raised.exception), "readiness service=nats state=running health=unhealthy")

    def test_app_start_follows_nats_health_not_just_nats_start(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout = 1
        responses = iter(["running|unhealthy", "running|healthy"])
        clock = Clock()

        def compose(*arguments):
            if arguments[0] == "ps" and arguments[-1] == "nats":
                return next(responses)
            return "running|healthy"

        fixture.compose = Mock(side_effect=compose)
        with patch("crash_helpers.wait_service_healthy",
                   side_effect=lambda command, service, timeout: wait_service_healthy(
                       command, service, timeout, clock=clock, pause=clock.pause)):
            fixture.restore_services()
        commands = [call.args for call in fixture.compose.call_args_list]
        app_start = commands.index(("start", "app"))
        nats_polls = [index for index, command in enumerate(commands)
                      if command[0] == "ps" and command[-1] == "nats"]
        self.assertEqual(len(nats_polls), 2)
        self.assertGreater(app_start, nats_polls[-1])
        self.assertFalse(any("--wait" in command for command in commands))

    def test_unhealthy_dependency_blocks_app_start_without_restarting_anything(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout = 1
        fixture.compose = Mock()

        def readiness(command, service, timeout):
            if service == "nats":
                raise ServiceReadinessError("nats", "running", "unhealthy")

        with patch("crash_helpers.wait_service_healthy", side_effect=readiness):
            with self.assertRaises(RestorationError) as raised:
                fixture.restore_services()
        self.assertEqual([call.args for call in fixture.compose.call_args_list],
                         [("start", "postgres"), ("start", "garage"), ("start", "nats")])
        self.assertIn("service=nats state=running health=unhealthy", str(raised.exception))
        self.assertIn("app start withheld", str(raised.exception))

    def test_invalid_or_multiple_health_rows_fail_without_exposing_response(self):
        for response in ("PRIVATE_RESPONSE", "running|healthy\nrunning|healthy", "PRIVATE|VALUE"):
            clock = Clock()
            with self.assertRaises(ServiceReadinessError) as raised:
                wait_service_healthy(Mock(return_value=response), "nats", 1, clock=clock, pause=clock.pause)
            self.assertEqual(str(raised.exception), "readiness service=nats state=invalid-response health=unknown")
            self.assertFalse(clock.delays)

    def test_failed_dependency_start_still_attempts_other_independent_services(self):
        fixture = nats.NatsFaultTests("test_process_crash_after_pg_commit_before_nats_confirmation")
        fixture.timeout = 1

        def compose(*arguments):
            if arguments == ("start", "postgres"):
                raise ComposeOperationError("start", "postgres", 1)
            return "running|healthy"

        fixture.compose = Mock(side_effect=compose)
        with self.assertRaises(RestorationError) as raised:
            fixture.restore_services()
        self.assertEqual([call.args for call in fixture.compose.call_args_list if call.args[0] == "start"],
                         [("start", "postgres"), ("start", "garage"), ("start", "nats")])
        self.assertEqual([call.args[-1] for call in fixture.compose.call_args_list if call.args[0] == "ps"],
                         ["garage", "nats"])
        self.assertIn("service=postgres exit_status=1", str(raised.exception))
        self.assertIn("app start withheld", str(raised.exception))

    def test_ps_process_timeout_is_capped(self):
        process = Mock(returncode=0, stdout="running|healthy")
        with patch("crash_helpers.subprocess.run", return_value=process) as run:
            run_compose(("ps", "--all", "--format", "{{.State}}|{{.Health}}", "nats"), 300)
        self.assertEqual(run.call_args.kwargs["timeout"], 5)


if __name__ == "__main__":
    unittest.main()
