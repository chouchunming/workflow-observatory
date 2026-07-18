from pathlib import Path
from collections import deque
import io
import json
import os
import queue
import runpy
import subprocess
import sys
import tempfile
import traceback
import typing
import unittest
from unittest import mock

from scripts import run_observing_workflows_task9_eval as task9_eval
from scripts.run_observing_workflows_task9_eval import (
    AppServer,
    ATTEMPT_AUDIT_WRAPPER,
    InjectedResultCrash,
    RESULT_COMMIT_FILENAME,
    RESULT_GENERATION_DIRECTORY,
    assert_observation_attempt_ledger,
    build_embedded_audit_wrapper,
    build_disabled_skills_override,
    build_shell_environment_override,
    decision_from_checkpoint,
    inventory_external_skill_paths,
    persist_result_pair,
    recording_failure_disclosed,
    resolve_committed_result_pair,
    run_configured_integrity,
    run_with_production_guard,
    validate_frozen_manifests,
)


class FakeExecProcess:
    pid = 4321

    def __init__(
        self,
        *,
        stdout="",
        stderr="",
        returncode=0,
        timeout=False,
        timeout_command="codex",
    ):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.timeout = timeout
        self.timeout_command = timeout_command
        self.calls = []

    def communicate(self, *, input, timeout):
        self.calls.append(("communicate", input, timeout))
        if self.timeout:
            raise subprocess.TimeoutExpired(
                self.timeout_command, timeout, self.stdout, self.stderr
            )
        return self.stdout, self.stderr

    def poll(self):
        return self.returncode

    def terminate(self):
        self.calls.append(("terminate",))

    def wait(self, timeout):
        self.calls.append(("wait", timeout))
        if self.timeout and not any(call[0] == "kill" for call in self.calls):
            raise subprocess.TimeoutExpired("codex", timeout)
        return self.returncode

    def kill(self):
        self.calls.append(("kill",))


class PostKillWaitTimeoutExecProcess(FakeExecProcess):
    def wait(self, timeout):
        self.calls.append(("wait", timeout))
        raise subprocess.TimeoutExpired(
            "WAIT_ARGV_SECRET",
            timeout,
            "WAIT_STDOUT_SECRET",
            "WAIT_STDERR_SECRET",
        )


class TerminateStopsExecProcess(FakeExecProcess):
    def wait(self, timeout):
        self.calls.append(("wait", timeout))
        return self.returncode


class Task9EvalRunnerTests(unittest.TestCase):
    def test_transport_timeouts_cover_delegated_review_and_gate_latency(self):
        self.assertEqual(20 * 60, task9_eval.EXEC_TURN_TIMEOUT_SECONDS)
        self.assertEqual(10 * 60, task9_eval.APP_SERVER_TURN_TIMEOUT_SECONDS)
        self.assertEqual(5 * 60, task9_eval.GATE_TIMEOUT_SECONDS)

    def test_execute_case_transport_uses_exec_for_one_turn(self):
        expected = task9_eval.CaseExecution("completed", "done", (), ())
        calls = []

        class FakeExec:
            def __init__(
                self, cwd, runtime, popen_factory=subprocess.Popen
            ):
                calls.append(("construct-exec", cwd, runtime))

            def run(self, prompt, timeout=task9_eval.EXEC_TURN_TIMEOUT_SECONDS):
                calls.append(("run-exec", prompt))
                return expected

        with mock.patch.object(task9_eval, "ExecTransport", FakeExec), \
             mock.patch.object(
                 task9_eval,
                 "AppServer",
                 side_effect=AssertionError("app-server used"),
             ):
            result = task9_eval.execute_case_transport(
                {"turns": [{"prompt": "one"}]},
                Path("/fixture"),
                mock.sentinel.runtime,
                Path("/store"),
                None,
            )

        self.assertIs(expected, result)
        self.assertEqual("run-exec", calls[-1][0])

    def test_execute_case_transport_rejects_checkpoint_for_exec(self):
        with mock.patch.object(
            task9_eval,
            "ExecTransport",
            side_effect=AssertionError("exec constructed"),
        ):
            with self.assertRaisesRegex(
                ValueError, "exec transport cannot accept a first-turn checkpoint"
            ):
                task9_eval.execute_case_transport(
                    {"turns": [{"prompt": "one"}]},
                    Path("/fixture"),
                    mock.sentinel.runtime,
                    Path("/store"),
                    lambda: None,
                )

    def test_execute_case_transport_requires_checkpoint_for_app_server(self):
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(
            task9_eval,
            "AppServer",
            side_effect=AssertionError("app-server constructed"),
        ):
            with self.assertRaisesRegex(
                ValueError,
                "app-server transport requires a first-turn checkpoint",
            ):
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    mock.Mock(
                        environment={},
                        disabled_skill_paths=(),
                        writable_roots=[],
                    ),
                    Path("/store"),
                    None,
                )

    def test_execute_case_transport_steers_two_turn_case(self):
        calls = []

        class FakeServer:
            def __init__(self, cwd, environment, disabled_skill_paths=()):
                self.agent_messages = ["done"]
                self.command_executions = ["python3 fixture.py"]
                self.observation_command_diagnostics = []

            def initialize(self):
                calls.append("initialize")

            def start_thread(self, cwd):
                calls.append("thread")
                return "thread-1"

            def start_turn(self, *args):
                calls.append("turn")
                return "turn-1"

            def steer(self, *args):
                calls.append("steer")

            def wait_turn(self, *args):
                calls.append("wait")
                return {"status": "completed"}

            def close(self):
                calls.append("close")

        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", FakeServer), \
             mock.patch.object(
                 task9_eval,
                 "ExecTransport",
                 side_effect=AssertionError("exec used"),
             ), \
             mock.patch.object(
                 task9_eval,
                 "_wait_for_gate",
                 side_effect=lambda *args: calls.append("gate"),
             ), \
             mock.patch.object(
                 task9_eval,
                 "release_gate",
                 side_effect=lambda *args: calls.append("release"),
             ):
            result = task9_eval.execute_case_transport(
                case,
                Path("/fixture"),
                mock.Mock(
                    environment={}, disabled_skill_paths=(), writable_roots=[]
                ),
                Path("/store"),
                lambda: calls.append("checkpoint"),
            )

        self.assertEqual(
            [
                "initialize",
                "thread",
                "turn",
                "gate",
                "checkpoint",
                "steer",
                "release",
                "wait",
                "close",
            ],
            calls,
        )
        self.assertEqual("done", result.final_text)

    def test_app_server_transport_releases_gate_and_closes_when_steer_fails(self):
        server = mock.Mock()
        server.start_thread.return_value = "thread-1"
        server.start_turn.return_value = "turn-1"
        server.steer.side_effect = RuntimeError("steer failed")
        runtime = mock.Mock(
            environment={}, disabled_skill_paths=(), writable_roots=[]
        )
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", return_value=server), \
             mock.patch.object(task9_eval, "_wait_for_gate"), \
             mock.patch.object(task9_eval, "release_gate") as release:
            with self.assertRaisesRegex(RuntimeError, "steer failed"):
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    runtime,
                    Path("/store"),
                    lambda: None,
                )

        release.assert_called_once_with("late-trigger")
        server.close.assert_called_once_with()

    def test_app_server_transport_closes_when_gate_release_fails(self):
        server = mock.Mock()
        server.start_thread.return_value = "thread-1"
        server.start_turn.return_value = "turn-1"
        runtime = mock.Mock(
            environment={}, disabled_skill_paths=(), writable_roots=[]
        )
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", return_value=server), \
             mock.patch.object(task9_eval, "_wait_for_gate"), \
             mock.patch.object(
                 task9_eval,
                 "release_gate",
                 side_effect=RuntimeError("release failed"),
             ):
            with self.assertRaises(BaseException) as caught:
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    runtime,
                    Path("/store"),
                    lambda: None,
                )

        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertIn("release failed", rendered)
        server.close.assert_called_once_with()

    def test_app_server_transport_preserves_execution_release_and_close_failures(self):
        server = mock.Mock()
        server.start_thread.return_value = "thread-1"
        server.start_turn.return_value = "turn-1"
        server.steer.side_effect = RuntimeError("steer failed")
        server.close.side_effect = RuntimeError("close failed")
        runtime = mock.Mock(
            environment={}, disabled_skill_paths=(), writable_roots=[]
        )
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", return_value=server), \
             mock.patch.object(task9_eval, "_wait_for_gate"), \
             mock.patch.object(
                 task9_eval,
                 "release_gate",
                 side_effect=RuntimeError("release failed"),
             ):
            with self.assertRaises(ExceptionGroup) as caught:
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    runtime,
                    Path("/store"),
                    lambda: None,
                )

        self.assertEqual(
            ["steer failed", "release failed", "close failed"],
            [str(error) for error in caught.exception.exceptions],
        )
        server.close.assert_called_once_with()

    def test_app_server_transport_requires_final_agent_message(self):
        server = mock.Mock()
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.start_thread.return_value = "thread-1"
        server.start_turn.return_value = "turn-1"
        runtime = mock.Mock(
            environment={}, disabled_skill_paths=(), writable_roots=[]
        )
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", return_value=server), \
             mock.patch.object(task9_eval, "_wait_for_gate"), \
             mock.patch.object(task9_eval, "release_gate") as release:
            with self.assertRaisesRegex(
                RuntimeError, "app-server final agent message is missing"
            ):
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    runtime,
                    Path("/store"),
                    lambda: None,
                )

        release.assert_called_once_with("late-trigger")
        server.close.assert_called_once_with()

    def test_transport_selection_is_derived_only_from_turn_count(self):
        self.assertEqual("exec", task9_eval.select_case_transport({"turns": [{}]}))
        self.assertEqual(
            "app-server", task9_eval.select_case_transport({"turns": [{}, {}]})
        )
        for turns in ([], [{}, {}, {}]):
            with self.subTest(turns=len(turns)):
                with self.assertRaisesRegex(ValueError, "unsupported turn count"):
                    task9_eval.select_case_transport({"turns": turns})

    def test_frozen_route_ids_are_stable(self):
        repository = Path(__file__).resolve().parents[1]
        forward, lifecycle = (
            json.loads((repository / path).read_text(encoding="utf-8"))
            for path in (
                "tests/skill_evals/observing_workflows_cases.json",
                "tests/skill_evals/observing_workflows_lifecycle_cases.json",
            )
        )
        self.assertEqual(
            [
                ("forward", "late-trigger"),
                ("forward", "scope-supersession"),
                ("lifecycle", "late-success"),
                ("lifecycle", "scope-supersession"),
            ],
            [
                (mode, case["id"])
                for mode, cases in (
                    ("forward", forward),
                    ("lifecycle", lifecycle),
                )
                for case in cases
                if task9_eval.select_case_transport(case) == "app-server"
            ],
        )

    def test_run_suite_routes_frozen_cases_in_order_before_one_persist(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        calls = []

        def fake_case(case, destination, lifecycle, runtime_factory=None):
            calls.append(
                (
                    "lifecycle" if lifecycle else "forward",
                    case["id"],
                    task9_eval.select_case_transport(case),
                )
            )
            return {"id": case["id"]}

        expected_case_events = [
            ("forward", "multi-file-feature", "exec"),
            ("forward", "tested-bugfix", "exec"),
            ("forward", "reviewed-refactor", "exec"),
            ("forward", "multi-file-docs", "exec"),
            ("forward", "wiki-compile", "exec"),
            ("forward", "durable-query", "exec"),
            ("forward", "inbox-processing", "exec"),
            ("forward", "late-trigger", "app-server"),
            ("forward", "scope-supersession", "app-server"),
            ("forward", "parent-managed-subagent", "exec"),
            ("forward", "chat", "exec"),
            ("forward", "read-only-search", "exec"),
            ("forward", "answer-only", "exec"),
            ("forward", "plan-only", "exec"),
            ("forward", "single-file-typo", "exec"),
            ("forward", "single-file-copy", "exec"),
            ("forward", "status-question", "exec"),
            ("forward", "review-only", "exec"),
            ("forward", "worker-with-parent-marker", "exec"),
            ("forward", "ambiguous-default-no-trigger", "exec"),
            ("lifecycle", "planned-success", "exec"),
            ("lifecycle", "late-success", "app-server"),
            ("lifecycle", "scope-supersession", "app-server"),
            ("lifecycle", "parent-managed-subagent", "exec"),
            ("lifecycle", "task-failure", "exec"),
            ("lifecycle", "central-cli-unavailable", "exec"),
            ("lifecycle", "complete-eval-override", "exec"),
            ("lifecycle", "incomplete-eval-override", "exec"),
        ]
        expected_ids = {
            mode: [
                case_id
                for event_mode, case_id, _route in expected_case_events
                if event_mode == mode
            ]
            for mode in ("forward", "lifecycle")
        }
        expected_results = {
            mode: [{"id": case_id} for case_id in expected_ids[mode]]
            for mode in ("forward", "lifecycle")
        }

        def fake_persist(result_destinations, results, manifests):
            self.assertEqual({"forward", "lifecycle"}, set(result_destinations))
            self.assertEqual(expected_results, results)
            self.assertEqual(
                expected_ids,
                {
                    mode: [case["id"] for case in manifests[mode]]
                    for mode in ("forward", "lifecycle")
                },
            )
            calls.append(("persist",))

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(
                 task9_eval, "persist_result_pair", side_effect=fake_persist
             ) as persist:
            task9_eval.run_suite(
                repository,
                manifest_paths=paths,
                result_destinations={
                    "forward": Path(temporary) / "forward.json",
                    "lifecycle": Path(temporary) / "lifecycle.json",
                },
            )

        self.assertEqual(expected_case_events, calls[:-1])
        self.assertEqual(("persist",), calls[-1])
        self.assertEqual(29, len(calls))
        self.assertEqual(
            24, sum(route == "exec" for _, _, route in calls[:-1])
        )
        self.assertEqual(
            4, sum(route == "app-server" for _, _, route in calls[:-1])
        )
        persist.assert_called_once()

    def test_run_suite_transport_failure_never_persists(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        calls = []

        def fail_third(case, destination, lifecycle, runtime_factory=None):
            calls.append(case["id"])
            if len(calls) == 3:
                raise RuntimeError("transport failed")
            return {"id": case["id"]}

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fail_third), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(task9_eval, "persist_result_pair") as persist:
            with self.assertRaisesRegex(RuntimeError, "transport failed"):
                task9_eval.run_suite(
                    repository,
                    manifest_paths=paths,
                    result_destinations={
                        "forward": Path(temporary) / "forward.json",
                        "lifecycle": Path(temporary) / "lifecycle.json",
                    },
                )

        self.assertEqual(3, len(calls))
        persist.assert_not_called()

    def test_discovery_sweep_continues_case_failures_and_never_persists(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        calls = []
        safety_checks = []

        def fake_case(case, destination, lifecycle, runtime_factory=None):
            mode = "lifecycle" if lifecycle else "forward"
            calls.append((mode, case["id"]))
            if case["id"] in {"reviewed-refactor", "late-success"}:
                raise ValueError("CASE_SECRET ordinary failure")
            return {"id": case["id"]}

        def check_safety(case, mode):
            safety_checks.append((mode, case["id"]))

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(task9_eval, "persist_result_pair") as persist:
            report = task9_eval.run_discovery_sweep(
                repository,
                manifest_paths=paths,
                runtime_factory=mock.sentinel.runtime_factory,
                case_safety_check=check_safety,
                destination=Path(temporary),
            )

        self.assertEqual(28, len(calls))
        self.assertEqual(
            [
                (mode, case_id)
                for mode in ("forward", "lifecycle")
                for case_id in task9_eval.FROZEN_MANIFEST_IDS[mode]
            ],
            calls,
        )
        self.assertEqual(calls, safety_checks)
        self.assertEqual(1, report["schema_version"])
        self.assertIs(False, report["authoritative"])
        self.assertIs(True, report["complete"])
        self.assertEqual(26, report["passed"])
        self.assertEqual(2, report["failed"])
        failures = [row for row in report["cases"] if row["status"] == "failed"]
        self.assertEqual(
            [("forward", "reviewed-refactor"), ("lifecycle", "late-success")],
            [(row["mode"], row["id"]) for row in failures],
        )
        self.assertTrue(all("failure" in row for row in failures))
        self.assertNotIn("CASE_SECRET", json.dumps(report, sort_keys=True))
        persist.assert_not_called()

    def test_discovery_sweep_aborts_on_case_safety_failure(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        calls = []

        def fake_case(case, destination, lifecycle, runtime_factory=None):
            calls.append(case["id"])
            raise ValueError("ordinary case failure")

        def fail_safety(case, mode):
            raise AssertionError("configured store integrity failed")

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(task9_eval, "persist_result_pair") as persist:
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "case safety"
            ) as caught:
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    case_safety_check=fail_safety,
                    destination=Path(temporary),
                )

        self.assertEqual(["multi-file-feature"], calls)
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)
        self.assertEqual(
            ["ordinary case failure", "configured store integrity failed"],
            [str(error) for error in caught.exception.__cause__.exceptions],
        )
        persist.assert_not_called()

    def test_discovery_sweep_aborts_on_production_change(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        calls = []

        def fake_case(case, destination, lifecycle, runtime_factory=None):
            calls.append(case["id"])
            return {"id": case["id"]}

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(
                 task9_eval,
                 "assert_production_unchanged",
                 side_effect=AssertionError("production changed"),
             ), \
             mock.patch.object(task9_eval, "persist_result_pair") as persist:
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "production fingerprint"
            ):
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    destination=Path(temporary),
                )

        self.assertEqual(["multi-file-feature"], calls)
        persist.assert_not_called()

    def test_discovery_sweep_preserves_case_and_production_failures(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(
                 task9_eval,
                 "_run_case",
                 side_effect=ValueError("ordinary case failure"),
             ), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(
                 task9_eval,
                 "assert_production_unchanged",
                 side_effect=AssertionError("production changed"),
             ):
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "production fingerprint"
            ) as caught:
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    destination=Path(temporary),
                )

        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)
        self.assertEqual(
            ["ordinary case failure", "production changed"],
            [str(error) for error in caught.exception.__cause__.exceptions],
        )

    def test_discovery_sweep_runs_guards_before_aborting_base_exception(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        safety = mock.Mock()
        production = mock.Mock()

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(
                 task9_eval, "_run_case", side_effect=KeyboardInterrupt()
             ), mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), mock.patch.object(
                 task9_eval, "assert_production_unchanged", production
             ):
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "case infrastructure"
            ) as caught:
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    case_safety_check=safety,
                    destination=Path(temporary),
                )

        self.assertIsInstance(caught.exception.__cause__, KeyboardInterrupt)
        safety.assert_called_once()
        production.assert_called_once_with("baseline")

    def test_discovery_sweep_aborts_on_typed_infrastructure_failure(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(
                 task9_eval,
                 "_run_case",
                 side_effect=task9_eval.CaseInfrastructureFailure(
                     "isolated case setup failed"
                 ),
             ), mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), mock.patch.object(task9_eval, "assert_production_unchanged"):
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "case infrastructure"
            ):
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    destination=Path(temporary),
                )

    def test_discovery_sweep_aborts_on_cleanup_failure(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(
                 task9_eval,
                 "_run_case",
                 side_effect=task9_eval.CaseCleanupFailure("close failed"),
             ), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(task9_eval, "persist_result_pair") as persist:
            with self.assertRaisesRegex(
                task9_eval.DiscoverySweepAbort, "cleanup"
            ):
                task9_eval.run_discovery_sweep(
                    repository,
                    manifest_paths=paths,
                    destination=Path(temporary),
                )

        persist.assert_not_called()

    def test_discovery_sweep_aborts_when_frozen_manifest_changes(self):
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            paths = {
                "forward": temporary_root / "observing_workflows_cases.json",
                "lifecycle": temporary_root
                / "observing_workflows_lifecycle_cases.json",
            }
            for mode, source_name in (
                ("forward", "observing_workflows_cases.json"),
                ("lifecycle", "observing_workflows_lifecycle_cases.json"),
            ):
                paths[mode].write_bytes(
                    (repository / "tests/skill_evals" / source_name).read_bytes()
                )

            calls = []

            def mutate_manifest(case, destination, lifecycle, runtime_factory=None):
                calls.append(case["id"])
                paths["forward"].write_text("[]\n", encoding="utf-8")
                return {"id": case["id"]}

            with mock.patch.object(
                task9_eval, "_run_case", side_effect=mutate_manifest
            ), mock.patch.object(
                task9_eval, "snapshot_production", return_value="baseline"
            ), mock.patch.object(
                task9_eval, "assert_production_unchanged"
            ), mock.patch.object(
                task9_eval, "persist_result_pair"
            ) as persist:
                with self.assertRaisesRegex(
                    task9_eval.DiscoverySweepAbort, "manifest integrity"
                ):
                    task9_eval.run_discovery_sweep(
                        repository,
                        manifest_paths=paths,
                        destination=temporary_root / "cases",
                    )

            self.assertEqual(["multi-file-feature"], calls)
            persist.assert_not_called()

    def test_formal_and_sweep_reject_manifest_race_before_first_case(self):
        repository = Path(__file__).resolve().parents[1]
        for runner_name in ("formal", "sweep"):
            with self.subTest(runner=runner_name), \
                 tempfile.TemporaryDirectory() as temporary:
                temporary_root = Path(temporary)
                paths = {
                    "forward": temporary_root / "forward.json",
                    "lifecycle": temporary_root / "lifecycle.json",
                }
                for mode, source_name in (
                    ("forward", "observing_workflows_cases.json"),
                    ("lifecycle", "observing_workflows_lifecycle_cases.json"),
                ):
                    paths[mode].write_bytes(
                        (repository / "tests/skill_evals" / source_name).read_bytes()
                    )
                calls = []

                def mutate_after_validation(*args, **kwargs):
                    paths["forward"].write_text("[]\n", encoding="utf-8")

                common = (
                    mock.patch.object(
                        task9_eval,
                        "validate_frozen_manifests",
                        side_effect=mutate_after_validation,
                    ),
                    mock.patch.object(
                        task9_eval,
                        "_run_case",
                        side_effect=lambda *args, **kwargs: calls.append(args[0]),
                    ),
                    mock.patch.object(
                        task9_eval, "snapshot_production", return_value="baseline"
                    ),
                    mock.patch.object(task9_eval, "assert_production_unchanged"),
                    mock.patch.object(task9_eval, "persist_result_pair"),
                )
                with common[0], common[1], common[2], common[3], common[4]:
                    if runner_name == "formal":
                        with self.assertRaisesRegex(
                            AssertionError, "manifest changed"
                        ):
                            task9_eval.run_suite(
                                repository,
                                manifest_paths=paths,
                                result_destinations={
                                    "forward": temporary_root / "forward-result.json",
                                    "lifecycle": temporary_root
                                    / "lifecycle-result.json",
                                },
                            )
                    else:
                        with self.assertRaisesRegex(
                            task9_eval.DiscoverySweepAbort,
                            "manifest integrity",
                        ):
                            task9_eval.run_discovery_sweep(
                                repository,
                                manifest_paths=paths,
                                destination=temporary_root / "cases",
                            )
                self.assertEqual([], calls)

    def test_formal_and_sweep_parse_only_captured_manifest_bytes(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository
            / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        original_read_text = Path.read_text

        def reject_second_manifest_read(path, *args, **kwargs):
            if path in paths.values():
                raise AssertionError("manifest was read a second time")
            return original_read_text(path, *args, **kwargs)

        def fake_case(case, *args, **kwargs):
            return {"id": case["id"]}

        with tempfile.TemporaryDirectory() as temporary, \
             mock.patch.object(Path, "read_text", reject_second_manifest_read), \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), mock.patch.object(
                 task9_eval, "assert_production_unchanged"
             ), mock.patch.object(task9_eval, "persist_result_pair"):
            task9_eval.run_suite(
                repository,
                manifest_paths=paths,
                result_destinations={
                    "forward": Path(temporary) / "forward.json",
                    "lifecycle": Path(temporary) / "lifecycle.json",
                },
            )
            task9_eval.run_discovery_sweep(
                repository,
                manifest_paths=paths,
                destination=Path(temporary) / "sweep",
            )

    def test_run_case_rejects_incomplete_transport_before_store_read(self):
        case = {
            "id": "incomplete-transport-test",
            "fixture": "empty",
            "turns": [{"prompt": "one"}],
        }
        failed = task9_eval.CaseExecution("failed", "failed", (), ())

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=mock.sentinel.audit,
                environment={},
                writable_roots=[],
            )
            with mock.patch.object(
                task9_eval, "build_case_fixture", return_value=root / "fixture"
            ), mock.patch.object(
                task9_eval, "execute_case_transport", return_value=failed
            ), mock.patch.object(
                task9_eval,
                "inspect_store",
                side_effect=RuntimeError("store read before terminal check"),
            ) as inspect:
                with self.assertRaises(AssertionError):
                    task9_eval._run_case(
                        case,
                        root,
                        lifecycle=False,
                        runtime_factory=lambda *args: runtime,
                    )

        inspect.assert_not_called()

    def test_frozen_manifests_route_exactly_24_exec_and_4_app_server(self):
        repository = Path(__file__).resolve().parents[1]
        paths = (
            repository / "tests/skill_evals/observing_workflows_cases.json",
            repository / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        )
        cases = [row for path in paths for row in json.loads(path.read_text())]
        routes = [task9_eval.select_case_transport(case) for case in cases]
        self.assertEqual(24, routes.count("exec"))
        self.assertEqual(4, routes.count("app-server"))
        self.assertEqual(
            {
                ("late-trigger", "app-server"),
                ("late-success", "app-server"),
                ("scope-supersession", "app-server"),
            },
            {(case["id"], route) for case, route in zip(cases, routes) if route == "app-server"},
        )

    def test_exec_common_config_overrides_are_fail_closed_and_complete(self):
        disabled = (Path("/skills/one"), Path("/skills/two"))
        overrides = task9_eval.build_codex_config_overrides(
            {"B": "two", "A": "one"}, disabled
        )
        self.assertEqual(
            (
                'shell_environment_policy.set={ A = "one", B = "two" }',
                'approval_policy="never"',
                'web_search="disabled"',
                "features.multi_agent=true",
                build_disabled_skills_override(disabled),
            ),
            overrides,
        )

    def test_app_server_uses_common_codex_config_overrides(self):
        class ExitedProcess:
            stdout = ()
            stderr = ()

            @staticmethod
            def poll():
                return 0

        environment = {"A": "one"}
        disabled = (Path("/skills/one"),)
        expected = ["codex", "app-server", "--stdio"]
        for override in task9_eval.build_codex_config_overrides(
            environment, disabled
        ):
            expected.extend(("-c", override))

        with mock.patch.object(
            task9_eval.subprocess, "Popen", return_value=ExitedProcess()
        ) as popen:
            AppServer(Path("/fixture"), environment, disabled)

        self.assertEqual(expected, popen.call_args.args[0])

    def test_exec_command_is_ephemeral_json_fail_closed_and_prompt_free(self):
        root = Path("/fixture")
        output = Path("/audit/final.txt")
        overrides = ('approval_policy="never"', 'web_search="disabled"')
        command = task9_eval.build_exec_command(
            root, [Path("/store"), Path("/audit")], output, overrides
        )
        self.assertEqual(
            [
                "codex", "exec", "--json", "--ephemeral", "--ignore-rules",
                "--sandbox", "workspace-write", "-C", "/fixture",
                "-o", "/audit/final.txt",
                "-c", 'approval_policy="never"',
                "-c", 'web_search="disabled"',
                "--add-dir", "/store",
                "--add-dir", "/audit",
                "-",
            ],
            command,
        )
        self.assertNotIn("synthetic secret prompt", command)

    def test_exec_jsonl_normalizes_completed_turn(self):
        stdout = "\n".join((
            '{"type":"thread.started","thread_id":"thread-1"}',
            '{"type":"turn.started"}',
            '{"type":"item.started","item":{"id":"cmd-1","type":"command_execution","command":"python3 -m unittest","status":"in_progress"}}',
            '{"type":"item.completed","item":{"id":"cmd-1","type":"command_execution","command":"python3 -m unittest","aggregated_output":"OK","exit_code":0,"status":"completed"}}',
            '{"type":"item.completed","item":{"id":"msg-1","type":"agent_message","text":"done"}}',
            '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}',
        ))
        result = task9_eval.parse_exec_jsonl(stdout, "done")
        self.assertEqual("completed", result.terminal_status)
        self.assertEqual("done", result.final_text)
        self.assertEqual(("python3 -m unittest",), result.command_executions)

    def test_exec_jsonl_normalizes_failed_command_inside_completed_turn(self):
        command = "python3 -m unittest expected_red_test"
        stdout = "\n".join((
            json.dumps({"type": "thread.started", "thread_id": "thread-1"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "item.started", "item": {
                "id": "cmd-1", "type": "command_execution",
                "command": command, "status": "in_progress",
            }}),
            json.dumps({"type": "item.completed", "item": {
                "id": "cmd-1", "type": "command_execution",
                "command": command, "aggregated_output": "FAILED",
                "exit_code": 1, "status": "failed",
            }}),
            json.dumps({"type": "item.completed", "item": {
                "id": "msg-1", "type": "agent_message", "text": "done",
            }}),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ))

        result = task9_eval.parse_exec_jsonl(stdout, "done")

        self.assertEqual("completed", result.terminal_status)
        self.assertEqual((command,), result.command_executions)

    def test_exec_jsonl_rejects_incomplete_or_failed_protocol_without_leaking(self):
        secret = "PROMPT_SECRET command-secret stderr-secret tool-output-secret"
        cases = {
            "malformed": "not-json " + secret,
            "error": json.dumps({"type": "error", "message": secret}),
            "turn-failed": json.dumps(
                {"type": "turn.failed", "error": {"message": secret}}
            ),
            "missing-terminal": json.dumps({
                "type": "item.completed",
                "item": {"id": "msg", "type": "agent_message", "text": "done"},
            }),
            "active-command": "\n".join((
                json.dumps({"type": "item.started", "item": {
                    "id": "cmd", "type": "command_execution", "command": secret,
                }}),
                json.dumps({"type": "item.completed", "item": {
                    "id": "msg", "type": "agent_message", "text": "done",
                }}),
                json.dumps({"type": "turn.completed", "usage": {}}),
            )),
            "missing-agent": json.dumps({"type": "turn.completed", "usage": {}}),
        }
        for label, stdout in cases.items():
            with self.subTest(label=label):
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    task9_eval.parse_exec_jsonl(stdout, "done")
                self.assertNotIn(secret, str(caught.exception))

    def test_exec_jsonl_bounds_many_active_command_ids_without_leaking(self):
        secret = "PROMPT_SECRET command-secret"
        stdout = "\n".join((
            *(
                json.dumps({"type": "item.started", "item": {
                    "id": f"{index:02d}" + ("x" * 78),
                    "type": "command_execution",
                    "command": secret,
                }})
                for index in range(20)
            ),
            json.dumps({"type": "item.completed", "item": {
                "id": "msg", "type": "agent_message", "text": "done",
            }}),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ))

        with self.assertRaises(RuntimeError) as caught:
            task9_eval.parse_exec_jsonl(stdout, "done")

        message = str(caught.exception)
        self.assertIn("count=20", message)
        self.assertNotIn(secret, message)
        self.assertLess(len(message), 1024)

    def test_exec_jsonl_rejects_final_message_disagreement(self):
        stdout = "\n".join((
            json.dumps({"type": "item.completed", "item": {
                "id": "msg", "type": "agent_message", "text": "event-final",
            }}),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ))
        with self.assertRaisesRegex(ValueError, "final message mismatch"):
            task9_eval.parse_exec_jsonl(stdout, "file-final")

    def test_exec_jsonl_rejects_out_of_order_terminal_lifecycle(self):
        command = "COMMAND_SECRET"
        cases = {
            "terminal-before-agent-message": (
                json.dumps({"type": "turn.completed", "usage": {}}),
                json.dumps({"type": "item.completed", "item": {
                    "id": "msg", "type": "agent_message", "text": "done",
                }}),
            ),
            "terminal-before-command-completion": (
                json.dumps({"type": "item.started", "item": {
                    "id": "cmd", "type": "command_execution", "command": command,
                }}),
                json.dumps({"type": "item.completed", "item": {
                    "id": "msg", "type": "agent_message", "text": "done",
                }}),
                json.dumps({"type": "turn.completed", "usage": {}}),
                json.dumps({"type": "item.completed", "item": {
                    "id": "cmd", "type": "command_execution", "command": command,
                    "status": "completed", "exit_code": 0,
                }}),
            ),
            "lifecycle-item-after-terminal": (
                json.dumps({"type": "item.completed", "item": {
                    "id": "msg", "type": "agent_message", "text": "done",
                }}),
                json.dumps({"type": "turn.completed", "usage": {}}),
                json.dumps({"type": "item.started", "item": {
                    "id": "late", "type": "agent_message",
                }}),
            ),
        }
        for label, events in cases.items():
            with self.subTest(label=label):
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    task9_eval.parse_exec_jsonl("\n".join(events), "done")
                self.assertNotIn(command, str(caught.exception))

    def test_exec_transport_sends_prompt_only_on_stdin_and_deletes_output(self):
        stdout = "\n".join((
            json.dumps({"type": "item.completed", "item": {
                "id": "msg", "type": "agent_message", "text": "done",
            }}),
            json.dumps({"type": "turn.completed", "usage": {}}),
        ))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            audit = task9_eval.RuntimePayloadAudit(
                root=audit_root,
                payload_dir=payload_dir,
                log_path=audit_root / "audit.jsonl",
                wrapper_path=audit_root / "workflow_observer_cli.py",
            )
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=audit,
                environment={},
                writable_roots=[store, audit_root],
            )
            process = FakeExecProcess(stdout=stdout)

            def popen_factory(command, **kwargs):
                self.assertNotIn("PROMPT_SECRET", command)
                Path(command[command.index("-o") + 1]).write_text(
                    "done", encoding="utf-8"
                )
                return process

            transport = task9_eval.ExecTransport(root, runtime, popen_factory)
            result = transport.run("PROMPT_SECRET")
            self.assertEqual("done", result.final_text)
            self.assertEqual(
                (
                    "communicate",
                    "PROMPT_SECRET",
                    task9_eval.EXEC_TURN_TIMEOUT_SECONDS,
                ),
                process.calls[0],
            )
            self.assertFalse((audit_root / "exec-final-message.txt").exists())

    def test_exec_transport_timeout_terminates_then_kills_without_leaking(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            process = FakeExecProcess(
                stdout="PROMPT_SECRET",
                stderr="STDERR_SECRET",
                timeout=True,
                timeout_command="ARGV_SECRET",
            )
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )
            with self.assertRaises(TimeoutError) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)
            self.assertEqual(
                ["communicate", "terminate", "wait", "kill", "wait"],
                [call[0] for call in process.calls],
            )
            error = caught.exception
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            visible = str(error) + "\n" + "".join(traceback.format_exception(error))
            for secret in ("PROMPT_SECRET", "STDERR_SECRET", "ARGV_SECRET"):
                self.assertNotIn(secret, visible)
            self.assertLess(len(str(error)), 1024)
            self.assertFalse((audit_root / "exec-final-message.txt").exists())

    def test_exec_transport_completed_agent_message_without_terminal_still_times_out(self):
        stdout = "\n".join((
            json.dumps({"type": "thread.started"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "item.completed", "item": {
                "id": "message-1",
                "type": "agent_message",
                "text": "VISIBLE_FINAL_SECRET",
            }}),
        ))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            process = TerminateStopsExecProcess(stdout=stdout, timeout=True)
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(TimeoutError) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)

        message = str(caught.exception)
        self.assertEqual(
            ["communicate", "terminate", "wait"],
            [call[0] for call in process.calls],
        )
        self.assertIn("'terminal_event_count': 0", message)
        self.assertIn("'agent_message_count': 1", message)
        self.assertIn("'last_item_type': 'agent_message'", message)
        self.assertIn("'last_item_status': 'none'", message)
        self.assertIn("('item.completed', 'agent_message', 'none')", message)
        self.assertNotIn("VISIBLE_FINAL_SECRET", message)
        self.assertNotIn("PROMPT_SECRET", message)
        self.assertLess(len(message), 1024)

    def test_exec_transport_sanitizes_post_kill_wait_timeout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            process = PostKillWaitTimeoutExecProcess(
                stdout="PROMPT_SECRET",
                stderr="STDERR_SECRET",
                timeout=True,
                timeout_command="ARGV_SECRET",
            )
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(Exception) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)

            error = caught.exception
            self.assertIs(type(error), task9_eval.CaseCleanupFailure)
            self.assertIn("cleanup failed after kill", str(error))
            self.assertIsNone(error.__cause__)
            self.assertIsNone(error.__context__)
            visible = str(error) + "\n" + "".join(traceback.format_exception(error))
            for secret in (
                "PROMPT_SECRET",
                "STDERR_SECRET",
                "ARGV_SECRET",
                "WAIT_ARGV_SECRET",
                "WAIT_STDOUT_SECRET",
                "WAIT_STDERR_SECRET",
            ):
                self.assertNotIn(secret, visible)
            self.assertLess(len(str(error)), 1024)
            self.assertEqual(
                ["communicate", "terminate", "wait", "kill", "wait"],
                [call[0] for call in process.calls],
            )
            self.assertFalse((audit_root / "exec-final-message.txt").exists())

    def test_exec_transport_classifies_output_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            output_path = audit_root / "exec-final-message.txt"
            output_path.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            transport = task9_eval.ExecTransport(
                root,
                runtime,
                mock.Mock(side_effect=AssertionError("process must not start")),
            )

            with self.assertRaisesRegex(
                task9_eval.CaseCleanupFailure, "output cleanup failed"
            ) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)

            rendered = "".join(traceback.format_exception(caught.exception))
            self.assertNotIn(str(output_path), rendered)

    def test_exec_transport_classifies_process_startup_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            transport = task9_eval.ExecTransport(
                root,
                runtime,
                mock.Mock(side_effect=OSError("STARTUP_PATH_SECRET")),
            )

            with self.assertRaisesRegex(
                task9_eval.CaseInfrastructureFailure,
                "codex exec startup failed",
            ) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)

            rendered = "".join(traceback.format_exception(caught.exception))
            self.assertNotIn("STARTUP_PATH_SECRET", rendered)
            self.assertNotIn("PROMPT_SECRET", rendered)

    def test_app_server_transport_classifies_process_startup_failure(self):
        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        runtime = mock.Mock(
            environment={}, disabled_skill_paths=(), writable_roots=[]
        )
        with mock.patch.object(
            task9_eval,
            "AppServer",
            side_effect=OSError("STARTUP_PATH_SECRET"),
        ):
            with self.assertRaisesRegex(
                task9_eval.CaseInfrastructureFailure,
                "app-server startup failed",
            ) as caught:
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    runtime,
                    Path("/store"),
                    lambda: None,
                )

        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertNotIn("STARTUP_PATH_SECRET", rendered)

    def test_run_case_classifies_post_factory_wrapper_setup_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            workspace = root / "workspace"
            workspace.mkdir()
            wrapper_directory = root / "wrapper-directory"
            wrapper_directory.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=mock.Mock(),
                environment={},
                writable_roots=[],
                audited_wrapper_path=wrapper_directory,
                audited_wrapper_content="wrapper",
            )
            with mock.patch.object(
                task9_eval, "build_case_fixture", return_value=workspace
            ):
                with self.assertRaisesRegex(
                    task9_eval.CaseInfrastructureFailure,
                    "isolated case setup failed",
                ) as caught:
                    task9_eval._run_case(
                        {"id": "multi-file-feature"},
                        root,
                        lifecycle=False,
                        runtime_factory=lambda *args: runtime,
                    )

        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertNotIn(str(wrapper_directory), rendered)

    def test_exec_transport_does_not_kill_when_terminate_wait_succeeds(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            process = TerminateStopsExecProcess(timeout=True)
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(TimeoutError):
                transport.run("PROMPT_SECRET", timeout=0.01)

            self.assertEqual(
                ["communicate", "terminate", "wait"],
                [call[0] for call in process.calls],
            )

    def test_exec_transport_removes_output_when_process_construction_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            output_path = audit_root / "exec-final-message.txt"
            output_path.write_text("stale", encoding="utf-8")
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            stale_state_at_launch = []

            def raising_factory(command, **kwargs):
                stale_state_at_launch.append(output_path.exists())
                output_path.write_text("factory-created", encoding="utf-8")
                raise RuntimeError("spawn failed")

            transport = task9_eval.ExecTransport(root, runtime, raising_factory)
            with self.assertRaisesRegex(
                task9_eval.CaseInfrastructureFailure,
                "codex exec startup failed",
            ) as caught:
                transport.run("PROMPT_SECRET")

            self.assertEqual([False], stale_state_at_launch)
            self.assertFalse(output_path.exists())
            rendered = "".join(traceback.format_exception(caught.exception))
            self.assertNotIn("spawn failed", rendered)
            self.assertNotIn("PROMPT_SECRET", rendered)

    def test_exec_transport_cleans_output_on_nonzero_exit_and_parse_failure(self):
        for label, process in (
            ("nonzero", FakeExecProcess(stderr="STDERR_SECRET", returncode=7)),
            ("parse", FakeExecProcess(stdout="not-json PROMPT_SECRET")),
        ):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                audit_root = root / "audit"
                payload_dir = audit_root / "tmp"
                store = root / "store"
                payload_dir.mkdir(parents=True)
                store.mkdir()
                runtime = task9_eval.CaseRuntime(
                    store_root=store,
                    audit=task9_eval.RuntimePayloadAudit(
                        root=audit_root,
                        payload_dir=payload_dir,
                        log_path=audit_root / "audit.jsonl",
                        wrapper_path=audit_root / "workflow_observer_cli.py",
                    ),
                    environment={},
                    writable_roots=[store, audit_root],
                )

                def popen_factory(command, **kwargs):
                    Path(command[command.index("-o") + 1]).write_text(
                        "file-final", encoding="utf-8"
                    )
                    return process

                transport = task9_eval.ExecTransport(root, runtime, popen_factory)
                with self.assertRaises((ValueError, RuntimeError)) as caught:
                    transport.run("PROMPT_SECRET")
                self.assertNotIn("PROMPT_SECRET", str(caught.exception))
                self.assertNotIn("STDERR_SECRET", str(caught.exception))
                self.assertFalse((audit_root / "exec-final-message.txt").exists())

    def test_exec_transport_worst_case_failure_diagnostic_is_bounded_and_redacted(self):
        secret = "PROMPT_SECRET STDERR_SECRET"
        stdout = "\n".join(
            json.dumps({
                "type": chr(97 + index) * 80,
                "message": secret,
                "item": {
                    "id": secret,
                    "type": chr(107 + index) * 80,
                    "status": chr(115 + index) * 80,
                    "text": secret,
                    "command": secret,
                    "aggregated_output": secret,
                },
            })
            for index in range(8)
        )
        process = FakeExecProcess(
            stdout=stdout,
            stderr=secret + ("x" * 10_000),
            returncode=7,
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(RuntimeError) as caught:
                transport.run(secret)

        message = str(caught.exception)
        self.assertNotIn(secret, message)
        self.assertIn("event_count", message)
        self.assertIn("terminal_event_count", message)
        self.assertIn("item_type_counts", message)
        self.assertIn("agent_message_count", message)
        self.assertIn("last_item_type", message)
        self.assertIn("last_item_status", message)
        self.assertIn("tail", message)
        self.assertNotIn("a" * 80, message)
        self.assertNotIn("k" * 80, message)
        self.assertNotIn("s" * 80, message)
        self.assertLess(len(message), 1024)

    def test_case_fixture_exposes_choreography_gate_only_when_prompt_names_it(self):
        builder = getattr(task9_eval, "build_case_fixture", None)
        self.assertIsNotNone(builder, "case-aware fixture builder is required")
        if builder is None:
            return
        reviewed = {
            "id": "reviewed-refactor-fixture-test",
            "fixture": "python-cli",
            "turns": [{"prompt": "Refactor two modules and require a reviewer gate."}],
        }
        gated = {
            "id": "late-trigger-fixture-test",
            "fixture": "python-cli",
            "turns": [{
                "prompt": (
                    "After the mutation, run `python3 scripts/gate.py "
                    "late-trigger-fixture-test` and wait."
                )
            }],
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reviewed_root = builder(reviewed, root)
            gated_root = builder(gated, root)
            self.assertFalse((reviewed_root / "scripts/gate.py").exists())
            self.assertTrue((gated_root / "scripts/gate.py").is_file())

    def test_case_fixture_exposes_failure_script_only_to_failure_case(self):
        success_case = {
            "id": "complete-eval-override-fixture-test",
            "fixture": "python-cli",
            "turns": [{"prompt": "Fix the parser and run the relevant tests."}],
        }
        failure_case = {
            "id": "task-failure",
            "fixture": "python-cli",
            "turns": [{
                "prompt": "Run `python3 scripts/fail_task.py` as required."
            }],
        }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            success_root = task9_eval.build_case_fixture(success_case, root)
            failure_root = task9_eval.build_case_fixture(failure_case, root)
            self.assertFalse((success_root / "scripts/fail_task.py").exists())
            self.assertTrue((failure_root / "scripts/fail_task.py").is_file())

    def test_case_fixture_commits_transport_neutral_evaluator_guidance(self):
        case = {
            "id": "evaluator-guidance-fixture-test",
            "fixture": "python-cli",
            "turns": [{"prompt": "Implement the approved change."}],
        }

        with tempfile.TemporaryDirectory() as temporary:
            workspace = task9_eval.build_case_fixture(case, Path(temporary))
            self.assertEqual(
                (task9_eval.EVALUATOR_DEVELOPER_INSTRUCTIONS + "\n").encode(),
                (workspace / "AGENTS.md").read_bytes(),
            )
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=workspace,
                check=True,
                text=True,
                capture_output=True,
            )

        self.assertEqual("", status.stdout)

    def test_app_server_thread_relies_on_fixture_guidance(self):
        server = AppServer.__new__(AppServer)
        server.request = mock.Mock(
            return_value={"thread": {"id": "thread-1"}}
        )

        self.assertEqual("thread-1", server.start_thread(Path("/fixture")))

        params = server.request.call_args.args[1]
        self.assertNotIn("developerInstructions", params)

    def test_app_server_silence_timeout_reports_live_turn_diagnostics(self):
        class LiveProcess:
            pid = 4321

            @staticmethod
            def poll():
                return None

        server = AppServer.__new__(AppServer)
        server.process = LiveProcess()
        server.messages = queue.Queue()
        server.stderr_tail = deque(["transport warning"], maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server._record(
            {
                "method": "item/started",
                "params": {
                    "item": {
                        "id": "cmd-1",
                        "type": "commandExecution",
                        "command": "python3 -m unittest",
                    }
                },
            }
        )

        with self.assertRaises(TimeoutError) as caught:
            server._receive(0.001)

        message = str(caught.exception)
        self.assertIn("app-server silence timeout", message)
        self.assertIn("pid=4321", message)
        self.assertIn("last_event=item/started", message)
        self.assertNotIn("python3 -m unittest", message)
        self.assertNotIn("transport warning", message)
        self.assertIn("active_commands=[{'chars': 19, 'sha256':", message)
        self.assertIn("stderr_tail=[{'chars': 17, 'sha256':", message)
        self.assertLess(len(message), 1024)

    def test_app_server_silence_timeout_bounds_and_redacts_diagnostics(self):
        class LiveProcess:
            pid = 4321

            @staticmethod
            def poll():
                return None

        secret = "API_TOKEN=top-secret-value"
        server = AppServer.__new__(AppServer)
        server.process = LiveProcess()
        server.messages = queue.Queue()
        server.stderr_tail = deque([secret + ("x" * 10_000)], maxlen=80)
        server.events = [{"method": "item/started"}]
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {
            "cmd-1": f"python3 script.py --token {secret}" + ("y" * 10_000)
        }

        with self.assertRaises(TimeoutError) as caught:
            server._receive(0.001)

        message = str(caught.exception)
        self.assertNotIn(secret, message)
        self.assertIn("sha256", message)
        self.assertLess(len(message), 1024)

    def test_app_server_exit_redacts_stderr_diagnostics(self):
        class ExitedProcess:
            pid = 4321

            @staticmethod
            def poll():
                return 7

        secret = "API_TOKEN=top-secret-value"
        server = AppServer.__new__(AppServer)
        server.process = ExitedProcess()
        server.messages = queue.Queue()
        server.stderr_tail = deque([secret + ("x" * 10_000)], maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}

        with self.assertRaises(RuntimeError) as caught:
            server._receive(0.001)

        message = str(caught.exception)
        self.assertNotIn(secret, message)
        self.assertIn("stderr_tail=[{'chars':", message)
        self.assertLess(len(message), 1024)

    def test_app_server_fails_fast_on_unhandled_server_request(self):
        class LiveProcess:
            pid = 4321

            @staticmethod
            def poll():
                return None

        server = AppServer.__new__(AppServer)
        server.process = LiveProcess()
        server.messages = queue.Queue()
        server.messages.put(
            {
                "id": 77,
                "method": "item/commandExecution/requestApproval",
                "params": {"itemId": "cmd-1"},
            }
        )
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}

        with self.assertRaisesRegex(
            RuntimeError,
            "unsupported app-server request.*"
            "item/commandExecution/requestApproval.*id=77",
        ):
            server._receive(1)

        self.assertEqual(
            "item/commandExecution/requestApproval", server.events[-1]["method"]
        )

    def test_app_server_drain_fails_fast_on_unhandled_server_request(self):
        server = AppServer.__new__(AppServer)
        server.messages = queue.Queue()
        server.messages.put(
            {
                "id": 88,
                "method": "item/fileChange/requestApproval",
                "params": {"itemId": "change-1"},
            }
        )
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}

        with self.assertRaisesRegex(
            RuntimeError,
            "unsupported app-server request.*"
            "item/fileChange/requestApproval.*id=88",
        ):
            server.drain()

    def test_app_server_accepts_responses_and_notifications(self):
        server = AppServer.__new__(AppServer)
        server.messages = queue.Queue()
        server.messages.put({"id": 1, "result": {"ok": True}})
        server.messages.put(
            {"method": "turn/started", "params": {"turn": {"id": "turn-1"}}}
        )
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}

        self.assertEqual({"id": 1, "result": {"ok": True}}, server._receive(1))
        self.assertEqual("turn/started", server._receive(1)["method"])

    def _attempt(self, argv, payloads=(), *, exit_code=0, errors=()):
        return {
            "argv": list(argv),
            "payloads": list(payloads),
            "errors": list(errors),
            "target_exit_code": exit_code,
            "target_error": None,
        }

    def _payload(self, flag, path, text, argv_index=1):
        return {
            "flag": flag,
            "argv_index": argv_index,
            "path": path,
            "device": 1,
            "inode": hash(path),
            "mode": 0o600,
            "regular": True,
            "text": text,
            "error": None,
        }

    def test_attempt_ledger_accepts_command_execution_sequences(self):
        hints = typing.get_type_hints(
            task9_eval.assert_observation_attempt_ledger
        )
        self.assertEqual(
            typing.Sequence[str], hints["command_executions"]
        )

    def test_attempt_ledger_rejects_help_or_draft_inspection_after_start(self):
        scope = self._payload(
            "--scope-from-file",
            "/tmp/scope.md",
            "## Scope\n\n- Goal: Implement feature\n- Included: Code\n- Excluded: None.\n",
        )
        attempts = [
            self._attempt(["start", "--scope-from-file", "/tmp/scope.md"], [scope]),
            self._attempt(["finish", "--help"]),
        ]
        with self.assertRaisesRegex(AssertionError, "help after start"):
            assert_observation_attempt_ledger(attempts, [], 1, 0)

        attempts[1] = self._attempt(["report"])
        with self.assertRaisesRegex(AssertionError, "draft inspection after start"):
            assert_observation_attempt_ledger(attempts, [], 1, 0)

    def test_attempt_ledger_rejects_second_completion_bearing_call(self):
        completion = self._payload(
            "--from-file", "/tmp/completion-1.md", self._valid_completion(), 2
        )
        second = dict(completion, path="/tmp/completion-2.md", inode=99)
        attempts = [
            self._attempt(["start", "--scope-from-file", "/tmp/scope.md"], [
                self._payload("--scope-from-file", "/tmp/scope.md", "scope")
            ]),
            self._attempt(["finish", "run", "--from-file", completion["path"]], [completion]),
            self._attempt(["finish", "run", "--from-file", second["path"]], [second]),
        ]
        with self.assertRaisesRegex(AssertionError, "finish invocations"):
            assert_observation_attempt_ledger(attempts, [], 1, 1)

    def test_attempt_ledger_counts_payloadless_finish_and_repeated_flags(self):
        scope = self._payload("--scope-from-file", "/tmp/scope.md", "scope")
        completion = self._payload(
            "--from-file", "/tmp/completion.md", self._valid_completion(), 2
        )
        attempts = [
            self._attempt(["start", "--scope-from-file", scope["path"]], [scope]),
            self._attempt(["finish", "run", "--from-file", completion["path"]], [completion]),
            self._attempt(["finish", "run"]),
        ]
        with self.assertRaisesRegex(AssertionError, "finish invocations"):
            assert_observation_attempt_ledger(attempts, [], 1, 1)

        repeated = dict(completion, path="/tmp/second.md", inode=88, argv_index=4)
        attempts = attempts[:2]
        attempts[1] = self._attempt(
            ["finish", "run", "--from-file", completion["path"], "--from-file", repeated["path"]],
            [completion, repeated],
        )
        with self.assertRaisesRegex(AssertionError, "repeats --from-file"):
            assert_observation_attempt_ledger(attempts, [], 1, 1)

    def test_attempt_ledger_rejects_payload_not_bound_to_argv_occurrence(self):
        payload = self._payload("--scope-from-file", "/tmp/wrong.md", "scope")
        attempts = [self._attempt(
            ["start", "--scope-from-file", "/tmp/right.md"], [payload]
        )]
        with self.assertRaisesRegex(AssertionError, "does not match argv"):
            assert_observation_attempt_ledger(attempts, [], 1, 0)

    def test_embedded_wrapper_is_only_cli_and_ledgers_report_and_finish(self):
        repository = Path(__file__).resolve().parents[1]
        scripts = repository / "marketplace/workflow-observatory/plugins/workflow-observer/scripts"
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            for dependency in ("store_config.py", "wiki_observations.py"):
                (root / dependency).write_bytes((scripts / dependency).read_bytes())
            wrapper = root / "workflow_observer_cli.py"
            wrapper.write_text(
                build_embedded_audit_wrapper(
                    (scripts / "workflow_observer_cli.py").read_bytes()
                ),
                encoding="utf-8",
            )
            log = root / "audit.jsonl"
            home = root / "home"
            environment = os.environ.copy()
            environment.update({
                "OBSERVATION_AUDIT_LOG": str(log),
                "WORKFLOW_OBSERVATORY_HOME": str(home),
            })
            environment.pop("OBSERVATION_AUDIT_TARGET_CLI", None)

            report = subprocess.run(
                [sys.executable, str(wrapper), "report"], env=environment,
                text=True, capture_output=True,
            )
            self.assertEqual(0, report.returncode, report.stderr)
            payload = root / "completion.md"
            payload.write_text(self._valid_completion(), encoding="utf-8")
            payload.chmod(0o600)
            finish = subprocess.run(
                [sys.executable, str(wrapper), "finish", "obs-20260101-000000-abcdef",
                 "--status", "success", "--from-file", str(payload)],
                env=environment, text=True, capture_output=True,
            )
            self.assertEqual(2, finish.returncode)
            rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(["report", "finish"], [row["argv"][0] for row in rows])
            self.assertEqual([0, 2], [row["target_exit_code"] for row in rows])
            self.assertNotIn("OBSERVATION_AUDIT_TARGET_CLI", environment)
            self.assertEqual([], list(root.glob("*target*.py")))
            self.assertEqual([wrapper], list(root.glob("workflow_observer_cli*.py")))

            for command in (
                ["find", str(root), "-name", ".target-*.py"],
                ["find", str(root), "-name", "*sidecar*"],
                ["find", str(root), "-name", "*target*"],
            ):
                found = subprocess.run(command, text=True, capture_output=True, check=True)
                self.assertEqual("", found.stdout)

    def test_audit_wrapper_logs_pre_read_failure_and_preserves_target_exit(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            wrapper = root / "wrapper.py"
            wrapper.write_text(ATTEMPT_AUDIT_WRAPPER, encoding="utf-8")
            target = root / "target.py"
            target.write_text("raise SystemExit(7)\n", encoding="utf-8")
            log = root / "audit.jsonl"
            environment = os.environ.copy()
            environment.update({
                "OBSERVATION_AUDIT_TARGET_CLI": str(target),
                "OBSERVATION_AUDIT_LOG": str(log),
            })
            completed = subprocess.run(
                [sys.executable, str(wrapper), "finish", "run", "--from-file", str(root / "missing.md")],
                env=environment,
                text=True,
                capture_output=True,
            )
            self.assertEqual(7, completed.returncode)
            rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(1, len(rows))
            self.assertEqual(7, rows[0]["target_exit_code"])
            self.assertEqual(2, rows[0]["payloads"][0]["argv_index"])
            self.assertIn("could not read", rows[0]["payloads"][0]["error"])

            environment["OBSERVATION_AUDIT_TARGET_CLI"] = str(root / "missing-target.py")
            completed = subprocess.run(
                [sys.executable, str(wrapper), "start"],
                env=environment,
                text=True,
                capture_output=True,
            )
            self.assertEqual(2, completed.returncode)
            rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(2, len(rows))
            self.assertEqual(2, rows[1]["target_exit_code"])
            self.assertIsNone(rows[1]["target_error"])
            self.assertIn("can't open file", completed.stderr)

    def test_attempt_ledger_rejects_completion_scalar_over_200_code_points(self):
        completion = self._payload(
            "--from-file",
            "/tmp/completion.md",
            self._valid_completion().replace("tests passed", "U0001f642" * 201),
        )
        attempts = [
            self._attempt(["start", "--scope-from-file", "/tmp/scope.md"], [
                self._payload("--scope-from-file", "/tmp/scope.md", "scope")
            ]),
            self._attempt(["finish", "run", "--from-file", completion["path"]], [completion]),
        ]
        with self.assertRaisesRegex(AssertionError, "completion scalar exceeds 200"):
            assert_observation_attempt_ledger(attempts, [], 1, 1)

    @staticmethod
    def _valid_completion():
        return """## Execution evidence

- Verification: tests passed
- Artifacts: implementation

## Outcome and observation

- Outcome: completed
- Observation: bounded workflow

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 0
defects_found: 0
rework_count: 0
rework_reason: none
```
"""

    def test_skill_documents_exact_task_type_variant_taxonomy(self):
        repository = Path(__file__).resolve().parents[1]
        skill = " ".join(
            (repository / "skills/observing-workflows/SKILL.md")
            .read_text(encoding="utf-8")
            .split()
        )

        expected_rules = (
            "`feature`, `bugfix`, `refactor`, or `documentation`: "
            "`implementation-basic` or `implementation-with-review`",
            "`maintenance`: `maintenance-basic` or `implementation-with-review`",
            "`compile` or `inbox-processing`: `compile-basic` or "
            "`compile-with-review`",
            "`query`: `research-basic`",
            "Never use `maintenance-basic` with another task type.",
        )
        for rule in expected_rules:
            with self.subTest(rule=rule):
                self.assertIn(rule, skill)

        self.assertIn(
            "A research or query task remains `query` with `research-basic` when "
            "its durable output is a comparison, answer, or Markdown summary.",
            skill,
        )
        self.assertIn(
            "Use `documentation` only when the authorized task itself is to create "
            "or maintain documentation.",
            skill,
        )

    def test_skill_documents_deterministic_review_variant_selection(self):
        repository = Path(__file__).resolve().parents[1]
        skill = " ".join(
            (repository / "skills/observing-workflows/SKILL.md")
            .read_text(encoding="utf-8")
            .split()
        )

        self.assertIn(
            "Select a review variant only when the authorized task instructions or "
            "an already-applicable workflow explicitly require a distinct reviewer, "
            "review gate, or delegated independent review.",
            skill,
        )
        self.assertIn(
            "Multiple files, tests, lint, link checks, or ordinary self-verification "
            "do not by themselves imply a review variant.",
            skill,
        )
        self.assertIn(
            "Otherwise choose the legal basic variant for the task type.",
            skill,
        )

    def test_skill_documents_compile_inbox_and_scope_replacement_rules(self):
        repository = Path(__file__).resolve().parents[1]
        skill = " ".join(
            (repository / "skills/observing-workflows/SKILL.md")
            .read_text(encoding="utf-8")
            .split()
        )

        self.assertIn(
            "Treat a compile or inbox workflow that updates Wiki pages or generated "
            "catalogs as eligible even though it is knowledge-base maintenance rather "
            "than software implementation.",
            skill,
        )
        self.assertIn(
            "In controlled evaluation, complete the observation start before entering "
            "a required fixture gate, while still entering that gate before the first "
            "task mutation.",
            skill,
        )
        self.assertIn(
            "A material Scope replacement is the only exception: start one replacement "
            "run first, then finish the prior run as `superseded` with "
            "`--superseded-by`.",
            skill,
        )
        self.assertIn(
            "Add `--task` or `--source` only when the selected adapter is `llmwiki` "
            "and the exact canonical referent exists under that adapter's configured "
            "Wiki root.",
            skill,
        )
        self.assertIn(
            "Omit both options for the portable adapter even when the subject workspace "
            "contains similarly named task or raw files.",
            skill,
        )
        self.assertIn(
            "Treat an open-ended request to improve something if useful that names no "
            "specific change or validation requirement as uncertain.",
            skill,
        )
        self.assertIn(
            "Default to no observation, and do not manufacture eligibility by "
            "voluntarily expanding it into multiple files or tests.",
            skill,
        )

    def test_skill_documents_exact_finish_status_taxonomy(self):
        repository = Path(__file__).resolve().parents[1]
        skill = (repository / "skills/observing-workflows/SKILL.md").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "Finish exactly once using `success`, `partial`, `failed`, "
            "`rolled-back`, or `superseded`; never `completed`.",
            skill,
        )

    def test_skill_documents_payload_scalar_limit(self):
        repository = Path(__file__).resolve().parents[1]
        skill = (repository / "skills/observing-workflows/SKILL.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("200 Unicode code points", skill)

    def test_skill_documents_zsh_safe_exit_code_and_cleanup(self):
        repository = Path(__file__).resolve().parents[1]
        skill = " ".join(
            (repository / "skills/observing-workflows/SKILL.md")
            .read_text(encoding="utf-8")
            .split()
        )
        self.assertIn(
            "When a shell wrapper records an exit code, use `exit_code`; never assign "
            "to zsh's read-only special parameter `status`.",
            skill,
        )
        self.assertIn(
            "Keep the cleanup trap active across every command after payload creation",
            skill,
        )

    def test_frozen_manifest_validation_rejects_truncated_forward_set(self):
        repository = Path(__file__).resolve().parents[1]
        paths = {
            "forward": repository / "tests/skill_evals/observing_workflows_cases.json",
            "lifecycle": repository
            / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        }
        forward = __import__("json").loads(paths["forward"].read_text(encoding="utf-8"))
        lifecycle = __import__("json").loads(
            paths["lifecycle"].read_text(encoding="utf-8")
        )

        with self.assertRaisesRegex(AssertionError, "forward manifest hash"):
            validate_frozen_manifests(
                paths,
                {"forward": forward[:-1], "lifecycle": lifecycle},
                raw_bytes={
                    "forward": b"truncated",
                    "lifecycle": paths["lifecycle"].read_bytes(),
                },
            )

    def test_result_commit_pointer_hides_all_precommit_crashes(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            forward = root / "forward.json"
            lifecycle = root / "lifecycle.json"
            destinations = {"forward": forward, "lifecycle": lifecycle}
            manifests, old, new = self._result_fixture()
            pointer = persist_result_pair(destinations, old, manifests)
            self.assertEqual(old, resolve_committed_result_pair(pointer, manifests))
            for crash_at in (
                "after_forward_write", "after_forward_rename",
                "after_lifecycle_write", "after_lifecycle_rename",
                "after_pointer_write",
            ):
                with self.subTest(crash_at=crash_at):
                    with self.assertRaises(InjectedResultCrash):
                        persist_result_pair(destinations, new, manifests, crash_at=crash_at)
                    self.assertEqual(old, resolve_committed_result_pair(pointer, manifests))
            with self.assertRaises(InjectedResultCrash):
                persist_result_pair(destinations, new, manifests, crash_at="after_pointer_rename")
            self.assertEqual(new, resolve_committed_result_pair(pointer, manifests))
            committed = json.loads(pointer.read_text(encoding="utf-8"))
            forward_generation = root / committed["files"]["forward"]["path"]
            forward_generation.write_bytes(forward_generation.read_bytes() + b" ")
            with self.assertRaisesRegex(AssertionError, "generation hash mismatch"):
                resolve_committed_result_pair(pointer, manifests)

    def test_result_store_rejects_symlink_roots_and_pointer(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            external = root / "external"
            external.mkdir()
            manifests, old, _ = self._result_fixture()

            linked_parent = root / "linked-parent"
            linked_parent.symlink_to(external, target_is_directory=True)
            destinations = {
                "forward": linked_parent / "forward.json",
                "lifecycle": linked_parent / "lifecycle.json",
            }
            with self.assertRaisesRegex((AssertionError, OSError), "symlink|directory"):
                persist_result_pair(destinations, old, manifests)
            self.assertEqual([], list(external.iterdir()))

            safe = root / "safe"
            safe.mkdir()
            generation_link = safe / ".observing_workflows_result_generations"
            generation_link.symlink_to(external, target_is_directory=True)
            destinations = {
                "forward": safe / "forward.json",
                "lifecycle": safe / "lifecycle.json",
            }
            with self.assertRaisesRegex((AssertionError, OSError), "symlink|directory"):
                persist_result_pair(destinations, old, manifests)
            self.assertEqual([], list(external.iterdir()))

            generation_link.unlink()
            pointer = persist_result_pair(destinations, old, manifests)
            real_pointer = safe / "real-pointer.json"
            pointer.rename(real_pointer)
            pointer.symlink_to(real_pointer)
            with self.assertRaisesRegex((AssertionError, OSError), "symlink|regular"):
                resolve_committed_result_pair(pointer, manifests)

    @staticmethod
    def _result_fixture():
        forward_case = {
            "id": "one", "expected_decisions": [{"after_turn": 1, "triggered": False}],
            "task_type": None, "workflow_variant": None,
            "expected_record_checkpoints": [{"after_turn": 1, "records": []}],
            "expected_run_count": 0, "expected_final_statuses": [],
        }
        lifecycle_case = {
            "id": "life", "mode": "command-selection-only",
            "expected_record_checkpoints": None, "expected_run_count": None,
            "expected_draft_count": None, "expected_final_statuses": None,
            "expect_failure_disclosure": None, "expected_selected_command": "command",
        }
        manifests = {"forward": [forward_case], "lifecycle": [lifecycle_case]}
        old = {
            "forward": [{
                "id": "one", "decisions": [{"after_turn": 1, "triggered": False,
                "task_type": None, "workflow_variant": None}],
                "record_checkpoints": [{"after_turn": 1, "records": []}],
                "run_count": 0, "draft_count": 0, "final_statuses": [],
            }],
            "lifecycle": [{
                "id": "life", "record_checkpoints": None, "run_count": None,
                "draft_count": None, "final_statuses": None,
                "failure_disclosed": None, "selected_command": "command",
            }],
        }
        new = json.loads(json.dumps(old))
        # Result-shape validation permits this distinct generation; semantic scoring
        # remains a separate all-cases gate before the real runner calls persistence.
        new["forward"][0]["draft_count"] = 1
        return manifests, old, new

    def test_configured_integrity_requires_exact_stdout_and_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cli = root / "cli.py"
            cli.write_text(
                "print('healthy records=2 invalidated=0')\n", encoding="utf-8"
            )
            command = (sys.executable, str(cli), "integrity")
            self.assertEqual(
                {"records": 2, "invalidated": 0},
                run_configured_integrity(command, {}, expected_records=2),
            )
            cli.write_text("print('healthy records=2 invalidated=0 extra')\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "malformed integrity stdout"):
                run_configured_integrity(command, {}, expected_records=2)
            cli.write_text("raise SystemExit(4)\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "integrity exit code 4"):
                run_configured_integrity(command, {}, expected_records=2)

    def test_external_skill_inventory_includes_user_and_plugin_roots_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            home = root / "home"
            codex_home = home / ".codex"
            expected = [
                home / ".agents/skills/user/SKILL.md",
                codex_home / "skills/global/SKILL.md",
                codex_home / "plugins/cache/vendor/plugin/skills/cached/SKILL.md",
                codex_home / "plugins/installed/plugin/skills/installed/SKILL.md",
            ]
            fixture = root / "fixture/.agents/skills/workflow-observer/SKILL.md"
            for path in [*expected, fixture]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("---\nname: test\n---\n", encoding="utf-8")
            inventory = inventory_external_skill_paths(
                home=home,
                codex_home=codex_home,
                fixture_skill_paths=(fixture,),
            )
            self.assertEqual(tuple(sorted(expected)), inventory)
            self.assertNotIn(fixture, inventory)
            override = build_disabled_skills_override(inventory)
            for path in expected:
                self.assertIn(json.dumps(str(path)), override)
            self.assertNotIn(str(fixture), override)

    def test_actual_marketplace_cli_integrity_is_exact(self):
        repository = Path(__file__).resolve().parents[1]
        cli = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "scripts/workflow_observer_cli.py"
        )
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            result = run_configured_integrity(
                (sys.executable, str(cli), "integrity"),
                {"WORKFLOW_OBSERVATORY_HOME": temporary},
                expected_records=0,
            )
        self.assertEqual({"records": 0, "invalidated": 0}, result)

    def test_production_guard_preserves_case_and_fingerprint_failures(self):
        def case_failure():
            raise ValueError("case failed")

        def fingerprint_failure():
            raise AssertionError("production changed")

        with self.assertRaises(ExceptionGroup) as caught:
            run_with_production_guard(case_failure, fingerprint_failure)
        messages = [str(error) for error in caught.exception.exceptions]
        self.assertEqual(["case failed", "production changed"], messages)

        with self.assertRaisesRegex(ValueError, "case failed"):
            run_with_production_guard(case_failure, lambda: None)
        with self.assertRaisesRegex(AssertionError, "production changed"):
            run_with_production_guard(lambda: "ok", fingerprint_failure)

    def test_production_guard_preserves_base_exception_pairs(self):
        for operation_error, production_error in (
            (KeyboardInterrupt("case interrupted"), AssertionError("production changed")),
            (ValueError("case failed"), SystemExit("production interrupted")),
        ):
            with self.subTest(
                operation=type(operation_error).__name__,
                production=type(production_error).__name__,
            ):
                def fail_operation(error=operation_error):
                    raise error

                def fail_production(error=production_error):
                    raise error

                with self.assertRaises(BaseExceptionGroup) as caught:
                    run_with_production_guard(fail_operation, fail_production)

                self.assertIs(type(caught.exception), BaseExceptionGroup)
                self.assertEqual(
                    [operation_error, production_error],
                    list(caught.exception.exceptions),
                )

    def test_script_is_directly_executable_from_repository_root(self):
        repository = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [
                sys.executable,
                str(repository / "scripts/run_observing_workflows_task9_eval.py"),
                "--help",
            ],
            cwd=repository,
            text=True,
            capture_output=True,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--preflight", result.stdout)

    def test_marketplace_runner_and_frozen_manifest_copies(self):
        repository = Path(__file__).resolve().parents[1]
        marketplace_tests = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/tests"
        )
        for name in (
            "observing_workflows_cases.json",
            "observing_workflows_lifecycle_cases.json",
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    (repository / "tests/skill_evals" / name).read_bytes(),
                    (marketplace_tests / "skill_evals" / name).read_bytes(),
                )
        result = subprocess.run(
            [sys.executable, str(marketplace_tests / "run_marketplace_eval.py"), "--help"],
            cwd=repository,
            text=True,
            capture_output=True,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--preflight", result.stdout)
        self.assertIn("--diagnostic-case", result.stdout)
        self.assertIn("--sweep", result.stdout)

    def test_marketplace_sweep_is_non_authoritative_and_skips_formal_suite(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["main"].__globals__
        calls = []

        def fake_sweep(*args, **kwargs):
            calls.append(("sweep", kwargs))
            return {
                "schema_version": 1,
                "authoritative": False,
                "complete": True,
                "passed": 28,
                "failed": 0,
                "cases": [],
            }

        def forbidden(label):
            def fail_if_called(*args, **kwargs):
                self.fail(f"{label} must not run during discovery sweep")

            return fail_if_called

        with mock.patch.dict(
            runtime_globals,
            {
                "validate_marketplace_manifest_hashes": lambda: calls.append(
                    "manifest"
                ),
                "run_discovery_sweep": fake_sweep,
                "run_suite": forbidden("formal suite"),
            },
        ), mock.patch.object(
            sys, "argv", [str(runner), "--sweep"]
        ), mock.patch.object(sys, "stdout", new_callable=io.StringIO) as stdout:
            self.assertEqual(0, namespace["main"]())

        self.assertEqual("manifest", calls[0])
        self.assertEqual("sweep", calls[1][0])
        self.assertIn("case_safety_check", calls[1][1])
        self.assertNotIn("result_destinations", calls[1][1])
        rendered = stdout.getvalue()
        self.assertIn('"authoritative": false', rendered)

    def test_marketplace_sweep_case_safety_checks_integrity_and_cleanup(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["MarketplaceRuntimeFactory"].__init__.__globals__

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload_dir = root / "tmp"
            payload_dir.mkdir()
            runtime = mock.Mock(
                integrity_command=("python", "cli.py", "integrity"),
                environment={"WORKFLOW_OBSERVATORY_HOME": str(root)},
                store_root=root / "store",
                audit=mock.Mock(root=root, payload_dir=payload_dir),
            )
            factory = namespace["MarketplaceRuntimeFactory"]()
            factory.runtimes.append(runtime)
            integrity = mock.Mock()

            with mock.patch.dict(
                runtime_globals,
                {
                    "inspect_store": lambda path: {"run_count": 1},
                    "run_configured_integrity": integrity,
                },
            ):
                factory.verify_case_safety(
                    {"id": "multi-file-feature"}, "forward"
                )

            self.assertEqual(1, factory.verified_runtimes)
            integrity.assert_called_once_with(
                runtime.integrity_command,
                runtime.environment,
                expected_records=1,
            )

            (payload_dir / "leftover").write_text("sensitive", encoding="utf-8")
            second = namespace["MarketplaceRuntimeFactory"]()
            second.runtimes.append(runtime)
            with mock.patch.dict(
                runtime_globals,
                {
                    "inspect_store": lambda path: {"run_count": 1},
                    "run_configured_integrity": mock.Mock(),
                },
            ):
                with self.assertRaisesRegex(
                    AssertionError, "payload cleanup left 1 path"
                ):
                    second.verify_case_safety(
                        {"id": "multi-file-feature"}, "forward"
                    )

    def test_marketplace_diagnostic_allows_only_reviewed_refactor_exec_case(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        case, lifecycle = namespace["_find_diagnostic_case"]("reviewed-refactor")
        self.assertFalse(lifecycle)
        self.assertEqual("exec", task9_eval.select_case_transport(case))
        for rejected in (
            "multi-file-feature",
            "late-trigger",
            "scope-supersession",
        ):
            with self.subTest(rejected=rejected):
                with self.assertRaisesRegex(LookupError, "diagnostic case is fixed"):
                    namespace["_find_diagnostic_case"](rejected)

    def test_marketplace_diagnostic_case_rejects_disallowed_id_before_transport(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        with self.assertRaisesRegex(LookupError, "diagnostic case is fixed"):
            namespace["_find_diagnostic_case"]("not-a-case")

    def test_marketplace_main_rejects_disallowed_diagnostic_before_manifest_reads(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["main"].__globals__
        for diagnostic_case in ("late-trigger", ""):
            with self.subTest(diagnostic_case=diagnostic_case):
                side_effects = []

                def forbidden(label):
                    def fail_if_called(*args, **kwargs):
                        side_effects.append(label)
                        self.fail(
                            f"{label} reached before fixed diagnostic rejection"
                        )

                    return fail_if_called

                with mock.patch.dict(
                    runtime_globals,
                    {
                        "validate_marketplace_manifest_hashes": forbidden(
                            "manifest hash validation/read"
                        ),
                        "run_preflight": forbidden("preflight"),
                        "run_diagnostic_case": forbidden(
                            "diagnostic fixture/transport"
                        ),
                        "run_suite": forbidden("formal suite"),
                    },
                ), mock.patch.object(
                    sys,
                    "argv",
                    [str(runner), "--diagnostic-case", diagnostic_case],
                ), mock.patch.object(
                    sys, "stderr", new_callable=io.StringIO
                ) as stderr:
                    with self.assertRaises(SystemExit) as caught:
                        namespace["main"]()

                self.assertEqual(2, caught.exception.code)
                self.assertIn(
                    "diagnostic case is fixed to reviewed-refactor",
                    stderr.getvalue(),
                )
                self.assertEqual([], side_effects)

    def test_marketplace_diagnostic_case_is_guarded_and_does_not_persist_results(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["run_diagnostic_case"].__globals__
        calls = []

        class Factory:
            def verify_all_integrity(self):
                calls.append("integrity")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "diagnostic"
            destination.mkdir()
            result_parent = root / "results"
            result_parent.mkdir()
            forward = result_parent / "forward.json"
            lifecycle = result_parent / "lifecycle.json"
            forward.write_text("forward-sentinel", encoding="utf-8")
            lifecycle.write_text("lifecycle-sentinel", encoding="utf-8")
            generation_root = result_parent / RESULT_GENERATION_DIRECTORY
            generation_root.mkdir()
            generation_sentinel = generation_root / "existing-generation.json"
            generation_sentinel.write_text("generation-sentinel", encoding="utf-8")
            commit_pointer = result_parent / RESULT_COMMIT_FILENAME
            commit_pointer.write_text("commit-sentinel", encoding="utf-8")
            runtime_globals["RESULT_PATHS"] = {
                "forward": forward,
                "lifecycle": lifecycle,
            }
            runtime_globals["persist_result_pair"] = (
                lambda *args, **kwargs: self.fail("diagnostic persisted a result pair")
            )
            runtime_globals["run_suite"] = (
                lambda *args, **kwargs: self.fail("diagnostic invoked the formal suite")
            )
            runtime_globals["MarketplaceRuntimeFactory"] = Factory
            runtime_globals["snapshot_production"] = lambda repo: "baseline"
            runtime_globals["assert_production_unchanged"] = (
                lambda baseline: calls.append(("verify-production", baseline))
            )

            def fake_diagnostic(case, *args, **kwargs):
                self.assertEqual("reviewed-refactor", case["id"])
                self.assertEqual("exec", task9_eval.select_case_transport(case))
                return {"id": case["id"]}

            runtime_globals["_run_case"] = fake_diagnostic

            with mock.patch.object(
                runtime_globals["tempfile"], "mkdtemp", return_value=str(destination)
            ):
                result = namespace["run_diagnostic_case"]("reviewed-refactor")

            self.assertEqual({"id": "reviewed-refactor"}, result)
            self.assertEqual(
                ["integrity", ("verify-production", "baseline")], calls
            )
            self.assertEqual("forward-sentinel", forward.read_text(encoding="utf-8"))
            self.assertEqual(
                "lifecycle-sentinel", lifecycle.read_text(encoding="utf-8")
            )
            self.assertEqual(
                [generation_sentinel], sorted(generation_root.iterdir())
            )
            self.assertEqual(
                "generation-sentinel",
                generation_sentinel.read_text(encoding="utf-8"),
            )
            self.assertEqual(
                "commit-sentinel", commit_pointer.read_text(encoding="utf-8")
            )

    def test_marketplace_diagnostic_case_verifies_production_after_failure(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["run_diagnostic_case"].__globals__
        calls = []

        class Factory:
            def verify_all_integrity(self):
                calls.append("integrity")

        def fail_case(*args, **kwargs):
            raise ValueError("diagnostic failed")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            destination = root / "diagnostic"
            destination.mkdir()
            result_parent = root / "results"
            result_parent.mkdir()
            forward = result_parent / "forward.json"
            lifecycle = result_parent / "lifecycle.json"
            generation_root = result_parent / RESULT_GENERATION_DIRECTORY
            commit_pointer = result_parent / RESULT_COMMIT_FILENAME
            runtime_globals["RESULT_PATHS"] = {
                "forward": forward,
                "lifecycle": lifecycle,
            }
            runtime_globals["persist_result_pair"] = (
                lambda *args, **kwargs: self.fail("diagnostic persisted a result pair")
            )
            runtime_globals["run_suite"] = (
                lambda *args, **kwargs: self.fail("diagnostic invoked the formal suite")
            )
            runtime_globals["MarketplaceRuntimeFactory"] = Factory
            runtime_globals["snapshot_production"] = lambda repo: "baseline"
            runtime_globals["assert_production_unchanged"] = (
                lambda baseline: calls.append(("verify-production", baseline))
            )
            runtime_globals["_run_case"] = fail_case

            with mock.patch.object(
                runtime_globals["tempfile"], "mkdtemp", return_value=str(destination)
            ):
                with self.assertRaisesRegex(ValueError, "diagnostic failed"):
                    namespace["run_diagnostic_case"]("reviewed-refactor")

            self.assertFalse(forward.exists())
            self.assertFalse(lifecycle.exists())
            self.assertFalse(generation_root.exists())
            self.assertFalse(commit_pointer.exists())
            self.assertEqual([], list(result_parent.iterdir()))

        self.assertEqual([("verify-production", "baseline")], calls)

    def test_shell_environment_override_is_deterministic_toml(self):
        self.assertEqual(
            'shell_environment_policy.set={ A = "one", PATH_VALUE = "a\\\\b\\\"c" }',
            build_shell_environment_override({"PATH_VALUE": 'a\\b"c', "A": "one"}),
        )

    def test_decision_uses_newest_run_and_delta(self):
        records = [
            {
                "run_id": "old",
                "timestamp": "2026-01-01T00:00:00Z",
                "task_type": "feature",
                "workflow_variant": "implementation-basic",
            },
            {
                "run_id": "new",
                "timestamp": "2026-01-01T00:00:01Z",
                "task_type": "bugfix",
                "workflow_variant": "implementation-with-review",
            },
        ]
        self.assertEqual(
            {
                "after_turn": 2,
                "triggered": True,
                "task_type": "bugfix",
                "workflow_variant": "implementation-with-review",
            },
            decision_from_checkpoint(2, records, previous_run_count=1),
        )
        self.assertEqual(
            {
                "after_turn": 2,
                "triggered": False,
                "task_type": None,
                "workflow_variant": None,
            },
            decision_from_checkpoint(2, records[:1], previous_run_count=1),
        )

    def test_forward_taxonomy_scores_the_final_triggered_scope(self):
        case = {
            "id": "scope-replacement",
            "expected_decisions": [
                {"after_turn": 1, "triggered": True},
                {"after_turn": 2, "triggered": True},
            ],
            "task_type": "feature",
            "workflow_variant": "implementation-with-review",
        }
        decisions = [
            {
                "after_turn": 1,
                "triggered": True,
                "task_type": "feature",
                "workflow_variant": "implementation-basic",
            },
            {
                "after_turn": 2,
                "triggered": True,
                "task_type": "feature",
                "workflow_variant": "implementation-with-review",
            },
        ]

        task9_eval.validate_forward_decisions(case, decisions)

        decisions[-1]["workflow_variant"] = "implementation-basic"
        with self.assertRaisesRegex(AssertionError, "taxonomy mismatch"):
            task9_eval.validate_forward_decisions(case, decisions)

    def test_forward_taxonomy_still_rejects_decision_shape_mismatch(self):
        case = {
            "id": "late-trigger",
            "expected_decisions": [
                {"after_turn": 1, "triggered": False},
                {"after_turn": 2, "triggered": True},
            ],
            "task_type": "feature",
            "workflow_variant": "implementation-basic",
        }
        decisions = [{
            "after_turn": 1,
            "triggered": True,
            "task_type": "feature",
            "workflow_variant": "implementation-basic",
        }]

        with self.assertRaisesRegex(AssertionError, "decision mismatch"):
            task9_eval.validate_forward_decisions(case, decisions)

    def test_failure_disclosure_requires_recording_context_and_failure(self):
        explicit_failures = (
            "The task passed, but workflow observation recording failed because "
            "the CLI was unavailable.",
            "The workflow observation was not recorded because the CLI could not "
            "be launched.",
            "The observation could not be recorded because the CLI was unavailable.",
            "The recording command failed before it could create the observation.",
        )
        for disclosure in explicit_failures:
            with self.subTest(disclosure=disclosure):
                self.assertTrue(recording_failure_disclosed(disclosure))
        self.assertFalse(recording_failure_disclosed("The implementation test failed."))
        self.assertFalse(recording_failure_disclosed("The observation was recorded successfully."))
        self.assertFalse(
            recording_failure_disclosed(
                "The task failed as required. Observation obs-example was finished "
                "with status failed."
            )
        )
        status_qualified_outcomes = (
            "The task failed, so the observation was not recorded as successful; "
            "it was recorded with status failed.",
            "The observation could not be recorded as success; it was recorded "
            "with status failed instead.",
        )
        for outcome in status_qualified_outcomes:
            with self.subTest(outcome=outcome):
                self.assertFalse(recording_failure_disclosed(outcome))


if __name__ == "__main__":
    unittest.main()
