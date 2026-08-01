from pathlib import Path
from collections import deque
from contextlib import contextmanager
import hashlib
import io
import json
import os
import queue
import runpy
import shutil
import stat
import subprocess
import sys
import tempfile
import traceback
import typing
import unittest
from unittest import mock

from scripts import run_observing_workflows_task9_eval as task9_eval
from scripts import workflow_eval_sharding as sharding
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
        self.returncode = None if timeout and returncode == 0 else returncode
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
        if any(call[0] == "kill" for call in self.calls):
            self.returncode = -9
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
        self.returncode = -15
        return self.returncode


def _test_transport_config() -> task9_eval.ResolvedTransportConfig:
    executable = Path(sys.executable).resolve(strict=True)
    metadata = executable.stat()
    return task9_eval.ResolvedTransportConfig(
        schema_version=1,
        codex_version="test-codex 1.0",
        codex_executable_path=str(executable),
        codex_executable_sha256=task9_eval.hashlib.sha256(
            executable.read_bytes()
        ).hexdigest(),
        codex_executable_device=metadata.st_dev,
        codex_executable_inode=metadata.st_ino,
        codex_executable_size=metadata.st_size,
        model="test-model",
        model_reasoning_effort="medium",
        approval_policy="never",
        sandbox_mode="workspace-write",
        network_access=False,
        web_search="disabled",
        multi_agent=True,
        exec_timeout_seconds=1200,
        app_server_timeout_seconds=600,
        gate_timeout_seconds=300,
    )


def _test_transport_environment(root: Path) -> dict[str, str]:
    case_home = Path(tempfile.mkdtemp(prefix="case-codex-home-", dir=root))
    case_home.chmod(0o700)
    auth = case_home / "auth.json"
    auth.write_text("{}\n", encoding="utf-8")
    auth.chmod(0o600)
    return {"CODEX_HOME": str(case_home)}


def _default_auth_case_fixture(root: Path):
    destination = root / "destination"
    destination.mkdir(mode=0o700)
    workspace = root / "workspace"
    workspace.mkdir(mode=0o700)
    audit_root = root / "audit"
    payload_dir = audit_root / "tmp"
    payload_dir.mkdir(parents=True)
    audit = task9_eval.RuntimePayloadAudit(
        root=audit_root,
        payload_dir=payload_dir,
        log_path=audit_root / "audit.jsonl",
        wrapper_path=audit_root / "workflow_observer_cli.py",
    )
    source_home = root / "source-home"
    source_home.mkdir(mode=0o700)
    (source_home / "config.toml").write_text(
        'model = "test-model"\nmodel_reasoning_effort = "medium"\n',
        encoding="utf-8",
    )
    source_auth = source_home / "auth.json"
    source_auth.write_text('{"token":"TEST_ONLY"}\n', encoding="utf-8")
    source_auth.chmod(0o600)
    executable = root / "fake-codex"
    executable.write_text(
        "#!/bin/sh\nprintf 'codex-cli 9.9.9\\n'\n", encoding="utf-8"
    )
    executable.chmod(0o700)
    case = {
        "id": "default-auth-owner",
        "fixture": "empty",
        "turns": [{"prompt": "one"}],
        "expected_run_count": 0,
        "expected_final_statuses": [],
    }
    return case, destination, workspace, audit, source_home, executable


@contextmanager
def _patched_default_auth_case(
    fixture,
    captured: dict[str, object],
    *,
    transport_side_effect=None,
    install_side_effect=None,
    use_real_transport=False,
):
    case, _, workspace, audit, source_home, executable = fixture
    real_prepare = task9_eval.prepare_auth_bootstrap
    real_install = task9_eval.install_case_auth

    def capture_prepare(**kwargs):
        captured["coordinator_root"] = kwargs["coordinator_root"]
        bootstrap = real_prepare(**kwargs)
        captured["bootstrap"] = bootstrap
        return bootstrap

    def capture_install(**kwargs):
        captured["case_codex_home"] = kwargs["case_codex_home"]
        if install_side_effect is not None:
            raise install_side_effect
        return real_install(**kwargs)

    completed = task9_eval.CaseExecution(
        "completed", "done", (), (), task9_eval.ZERO_TOKEN_USAGE
    )
    transport = mock.Mock(
        return_value=completed,
        side_effect=transport_side_effect,
    )
    with mock.patch.dict(
        os.environ, {"CODEX_HOME": str(source_home)}, clear=False
    ), mock.patch.object(
        task9_eval.shutil, "which", return_value=str(executable)
    ), mock.patch.object(
        task9_eval, "build_case_fixture", return_value=workspace
    ), mock.patch.object(
        task9_eval, "build_payload_audit", return_value=audit
    ), mock.patch.object(
        task9_eval, "prepare_auth_bootstrap", side_effect=capture_prepare
    ), mock.patch.object(
        task9_eval, "install_case_auth", side_effect=capture_install
    ), mock.patch.object(
        task9_eval,
        "inspect_store",
        return_value={"run_count": 0, "draft_count": 0, "final_statuses": []},
    ), mock.patch.object(
        task9_eval, "load_observation_attempt_ledger", return_value=[]
    ), mock.patch.object(
        task9_eval, "assert_observation_attempt_ledger"
    ), mock.patch.object(
        task9_eval, "validate_forward_decisions"
    ):
        if use_real_transport:
            yield case
        else:
            with mock.patch.object(
                task9_eval, "execute_case_transport", transport
            ):
                yield case


class Task9EvalRunnerTests(unittest.TestCase):
    def test_transport_timeouts_cover_delegated_review_and_gate_latency(self):
        self.assertEqual(20 * 60, task9_eval.EXEC_TURN_TIMEOUT_SECONDS)
        self.assertEqual(10 * 60, task9_eval.APP_SERVER_TURN_TIMEOUT_SECONDS)
        self.assertEqual(5 * 60, task9_eval.GATE_TIMEOUT_SECONDS)

    def test_resolved_config_binds_both_transports(self):
        self.assertTrue(
            hasattr(task9_eval, "resolve_transport_config"),
            "resolved transport config API is missing",
        )
        class ExitedProcess:
            pid = 4321
            stdout = ()
            stderr = ()

            @staticmethod
            def poll():
                return 0

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            source_config = source_home / "config.toml"
            source_config.write_text(
                'model = "sealed-model"\nmodel_reasoning_effort = "high"\n',
                encoding="utf-8",
            )
            executable = root / "fake-codex"
            executable.write_text(
                "#!/bin/sh\nprintf 'codex-cli 9.9.9\\n'\n", encoding="utf-8"
            )
            executable.chmod(0o700)
            resolved = task9_eval.resolve_transport_config(
                codex_executable=executable,
                source_codex_home=source_home,
                requested_model=None,
                requested_reasoning_effort=None,
            )
            case_home = root / "case-home"
            case_home.mkdir(mode=0o700)
            (case_home / "auth.json").write_text("{}\n", encoding="utf-8")
            (case_home / "auth.json").chmod(0o600)
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=task9_eval.RuntimePayloadAudit(
                    root=root / "audit",
                    payload_dir=root / "audit" / "tmp",
                    log_path=root / "audit" / "audit.jsonl",
                    wrapper_path=root / "audit" / "wrapper.py",
                ),
                environment={"CODEX_HOME": str(case_home)},
                writable_roots=(root / "store", root / "audit"),
                transport_config=resolved,
            )

            source_config.write_text(
                'model = "ambient-model"\nmodel_reasoning_effort = "low"\n',
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {"PATH": str(root / "hostile-path")}):
                overrides = task9_eval.build_codex_config_overrides(
                    resolved, runtime.environment, ()
                )
                exec_command = task9_eval.build_exec_command(
                    resolved,
                    root,
                    runtime.writable_roots,
                    root / "final.txt",
                    overrides,
                )
                app_command = task9_eval.build_app_server_command(
                    resolved, overrides
                )

            executable_path = str(executable.resolve())
            self.assertEqual(executable_path, exec_command[0])
            self.assertEqual(executable_path, app_command[0])
            self.assertIn("--ignore-user-config", exec_command)
            self.assertIn("--strict-config", exec_command)
            self.assertIn("--strict-config", app_command)
            for command in (exec_command, app_command):
                self.assertIn('model="sealed-model"', command)
                self.assertIn('model_reasoning_effort="high"', command)
                self.assertIn('approval_policy="never"', command)
                self.assertIn('sandbox_mode="workspace-write"', command)
                self.assertIn("sandbox_workspace_write.network_access=false", command)
                self.assertIn('web_search="disabled"', command)
                self.assertIn("features.multi_agent=true", command)
                self.assertNotIn("ambient-model", command)

            executable.unlink()
            executable.write_text(
                "#!/bin/sh\nprintf 'codex-cli replacement\\n'\n", encoding="utf-8"
            )
            executable.chmod(0o700)
            popen = mock.Mock(return_value=ExitedProcess())
            with self.assertRaisesRegex(RuntimeError, "executable identity changed"):
                task9_eval.ExecTransport(root, runtime, popen).run("prompt")
            popen.assert_not_called()
            with self.assertRaisesRegex(RuntimeError, "executable identity changed"):
                task9_eval.AppServer(root, runtime, popen_factory=popen)
            popen.assert_not_called()

    def test_default_run_case_cleans_owned_auth_after_success_outside_destination(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            with _patched_default_auth_case(fixture, captured) as case:
                task9_eval._run_case(case, destination, lifecycle=False)

            owner = Path(captured["coordinator_root"])
            self.assertFalse(owner.is_relative_to(destination))
            self.assertFalse(Path(captured["case_codex_home"]).exists())
            self.assertFalse(Path(captured["bootstrap"]).exists())
            self.assertFalse(owner.exists())

    def test_default_run_case_cleans_owned_auth_after_transport_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            with _patched_default_auth_case(
                fixture,
                captured,
                transport_side_effect=RuntimeError("PRIMARY_TRANSPORT_SENTINEL"),
            ) as case:
                with self.assertRaisesRegex(
                    RuntimeError, "PRIMARY_TRANSPORT_SENTINEL"
                ):
                    task9_eval._run_case(case, destination, lifecycle=False)

            self.assertFalse(Path(captured["case_codex_home"]).exists())
            self.assertFalse(Path(captured["bootstrap"]).exists())
            self.assertFalse(Path(captured["coordinator_root"]).exists())

    def test_default_run_case_cleans_bootstrap_after_setup_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            with _patched_default_auth_case(
                fixture,
                captured,
                install_side_effect=RuntimeError("SETUP_SENTINEL"),
            ) as case:
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(case, destination, lifecycle=False)

            self.assertFalse(Path(captured["bootstrap"]).exists())
            self.assertFalse(Path(captured["coordinator_root"]).exists())

    def test_default_run_case_reports_auth_cleanup_failure(self):
        self.assertTrue(
            hasattr(task9_eval, "_remove_owned_auth_directory"),
            "owned auth cleanup API is missing",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            real_remove = task9_eval._remove_owned_auth_directory
            cleanup_calls = []

            def fail_case_home(path):
                cleanup_calls.append(path.name)
                if path.name == "case-codex-home":
                    raise task9_eval.CaseCleanupFailure("AUTH_CLEANUP_SENTINEL")
                return real_remove(path)

            with _patched_default_auth_case(fixture, captured) as case, mock.patch.object(
                task9_eval,
                "_remove_owned_auth_directory",
                side_effect=fail_case_home,
            ):
                with self.assertRaises(task9_eval.CaseCleanupFailure) as caught:
                    task9_eval._run_case(case, destination, lifecycle=False)
            self.assertIn("AUTH_CLEANUP_SENTINEL", str(caught.exception))
            self.assertEqual(["case-codex-home"], cleanup_calls)
            for key in ("case_codex_home", "bootstrap", "coordinator_root"):
                self.assertTrue(Path(captured[key]).exists(), key)
            real_remove(Path(captured["coordinator_root"]))

    def test_default_run_case_preserves_primary_and_auth_cleanup_failures(self):
        self.assertTrue(
            hasattr(task9_eval, "_remove_owned_auth_directory"),
            "owned auth cleanup API is missing",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            real_remove = task9_eval._remove_owned_auth_directory
            cleanup_calls = []

            def fail_case_home(path):
                cleanup_calls.append(path.name)
                if path.name == "case-codex-home":
                    raise task9_eval.CaseCleanupFailure("AUTH_CLEANUP_SENTINEL")
                return real_remove(path)

            with _patched_default_auth_case(
                fixture,
                captured,
                transport_side_effect=RuntimeError("PRIMARY_TRANSPORT_SENTINEL"),
            ) as case, mock.patch.object(
                task9_eval,
                "_remove_owned_auth_directory",
                side_effect=fail_case_home,
            ):
                with self.assertRaises(BaseExceptionGroup) as caught:
                    task9_eval._run_case(case, destination, lifecycle=False)

            rendered = "\n".join(
                str(error) for error in caught.exception.exceptions
            )
            self.assertIn("PRIMARY_TRANSPORT_SENTINEL", rendered)
            self.assertIn("AUTH_CLEANUP_SENTINEL", rendered)
            self.assertEqual(["case-codex-home"], cleanup_calls)
            for key in ("case_codex_home", "bootstrap", "coordinator_root"):
                self.assertTrue(Path(captured[key]).exists(), key)
            real_remove(Path(captured["coordinator_root"]))

    def test_default_run_case_retains_owner_after_bootstrap_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            cleanup_calls = []
            real_remove = task9_eval._remove_owned_auth_directory

            def fail_bootstrap(path):
                cleanup_calls.append(path.name)
                if path.name.startswith("auth-bootstrap-"):
                    raise task9_eval.CaseCleanupFailure("BOOTSTRAP_CLEANUP_SENTINEL")
                return real_remove(path)

            with _patched_default_auth_case(fixture, captured) as case, mock.patch.object(
                task9_eval,
                "_remove_owned_auth_directory",
                side_effect=fail_bootstrap,
            ):
                with self.assertRaisesRegex(
                    task9_eval.CaseCleanupFailure,
                    "BOOTSTRAP_CLEANUP_SENTINEL",
                ):
                    task9_eval._run_case(case, destination, lifecycle=False)

            self.assertEqual("case-codex-home", cleanup_calls[0])
            self.assertTrue(cleanup_calls[1].startswith("auth-bootstrap-"))
            self.assertEqual(2, len(cleanup_calls))
            self.assertFalse(Path(captured["case_codex_home"]).exists())
            self.assertTrue(Path(captured["bootstrap"]).exists())
            self.assertTrue(Path(captured["coordinator_root"]).exists())
            real_remove(Path(captured["coordinator_root"]))

    def test_owned_auth_cleanup_is_symlink_safe_and_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            external = root / "external-secret"
            external.write_text("EXTERNAL_SECRET_SENTINEL", encoding="utf-8")
            owned = root / "owned-auth"
            nested = owned / "nested"
            nested.mkdir(parents=True)
            (nested / "auth.json").write_text("{}\n", encoding="utf-8")
            (nested / "external-link").symlink_to(external)

            task9_eval._remove_owned_auth_directory(owned)

            self.assertFalse(owned.exists())
            self.assertEqual(
                "EXTERNAL_SECRET_SENTINEL", external.read_text(encoding="utf-8")
            )

            bounded = root / "bounded-auth"
            bounded.mkdir()
            for index in range(3):
                (bounded / f"entry-{index}").write_text("x", encoding="utf-8")
            with mock.patch.object(task9_eval, "AUTH_CLEANUP_MAX_ENTRIES", 2):
                with self.assertRaisesRegex(
                    task9_eval.CaseCleanupFailure, "owned auth cleanup failed"
                ) as caught:
                    task9_eval._remove_owned_auth_directory(bounded)
            self.assertNotIn(str(bounded), str(caught.exception))

    def test_default_run_case_retains_auth_when_exec_process_survives_cleanup(self):
        self.assertTrue(
            hasattr(task9_eval, "ProcessSurvivalCleanupFailure"),
            "typed process-survival cleanup failure is missing",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            process = PostKillWaitTimeoutExecProcess(
                stdout="PROMPT_SECRET",
                stderr="STDERR_SECRET",
                returncode=None,
                timeout=True,
            )
            real_transport = task9_eval.ExecTransport

            def construct_exec(cwd, runtime, popen_factory=subprocess.Popen):
                return real_transport(
                    cwd, runtime, lambda *args, **kwargs: process
                )

            with _patched_default_auth_case(
                fixture, captured, use_real_transport=True
            ) as case, mock.patch.object(
                task9_eval, "ExecTransport", side_effect=construct_exec
            ):
                with self.assertRaises(BaseException) as caught:
                    task9_eval._run_case(case, destination, lifecycle=False)

            self.assertIsNone(process.poll())
            self.assertTrue(
                task9_eval._contains_process_survival_failure(caught.exception)
            )
            self.assertEqual(
                ["communicate", "terminate", "wait", "kill", "wait"],
                [call[0] for call in process.calls],
            )
            for key in ("case_codex_home", "bootstrap", "coordinator_root"):
                self.assertTrue(Path(captured[key]).exists(), key)
                self.assertNotIn(str(captured[key]), str(caught.exception))
            task9_eval._remove_owned_auth_directory(
                Path(captured["coordinator_root"])
            )

    def test_app_server_close_classifies_surviving_process_and_group_preserves_it(self):
        self.assertTrue(
            hasattr(task9_eval, "ProcessSurvivalCleanupFailure"),
            "typed process-survival cleanup failure is missing",
        )
        process = PostKillWaitTimeoutExecProcess(returncode=None, timeout=True)
        server = task9_eval.AppServer.__new__(task9_eval.AppServer)
        server.process = process
        with self.assertRaises(task9_eval.ProcessSurvivalCleanupFailure):
            server.close()
        self.assertIsNone(process.poll())

        survival = task9_eval.ProcessSurvivalCleanupFailure(
            "app-server process cleanup remained incomplete"
        )

        class FakeServer:
            agent_messages = []
            command_executions = []
            observation_command_diagnostics = []

            def initialize(self):
                raise RuntimeError("APP_PRIMARY_SENTINEL")

            def close(self):
                raise survival

        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }
        with mock.patch.object(task9_eval, "AppServer", return_value=FakeServer()), \
             mock.patch.object(task9_eval, "release_gate"):
            with self.assertRaises(ExceptionGroup) as caught:
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    mock.Mock(writable_roots=()),
                    Path("/wiki"),
                    lambda: None,
                )
        self.assertTrue(
            task9_eval._contains_process_survival_failure(caught.exception)
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = _default_auth_case_fixture(root)
            _, destination, _, _, _, _ = fixture
            captured = {}
            with _patched_default_auth_case(
                fixture,
                captured,
                transport_side_effect=caught.exception,
            ) as default_case:
                with self.assertRaises(ExceptionGroup):
                    task9_eval._run_case(
                        default_case, destination, lifecycle=False
                    )
            for key in ("case_codex_home", "bootstrap", "coordinator_root"):
                self.assertTrue(Path(captured[key]).exists(), key)
            task9_eval._remove_owned_auth_directory(
                Path(captured["coordinator_root"])
            )

    def test_auth_cleanup_entry_cap_stops_lazy_enumeration_before_all_names(self):
        consumed = {"listdir": 0, "scandir": 0}

        def lazy_names(kind):
            for index in range(10_000):
                consumed[kind] += 1
                if kind == "scandir":
                    yield mock.Mock(name=f"entry-{index}")
                else:
                    yield f"entry-{index}"

        class LazyScandir:
            def __enter__(self):
                return iter(lazy_names("scandir"))

            def __exit__(self, *args):
                return False

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            owned = root / "owned-auth"
            owned.mkdir()
            with mock.patch.object(
                task9_eval, "AUTH_CLEANUP_MAX_ENTRIES", 2
            ), mock.patch.object(
                task9_eval.os, "listdir", return_value=lazy_names("listdir")
            ), mock.patch.object(
                task9_eval.os, "scandir", return_value=LazyScandir()
            ):
                with self.assertRaises(task9_eval.CaseCleanupFailure):
                    task9_eval._remove_owned_auth_directory(owned)

        total_consumed = consumed["listdir"] + consumed["scandir"]
        self.assertLessEqual(total_consumed, 3)

    def test_execute_case_transport_uses_exec_for_one_turn(self):
        expected = task9_eval.CaseExecution(
            "completed", "done", (), (), task9_eval.ZERO_TOKEN_USAGE
        )
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

    def test_execute_transport_accepts_event_sink(self):
        expected = task9_eval.CaseExecution(
            "completed", "done", (), (), task9_eval.ZERO_TOKEN_USAGE
        )
        events = []

        class FakeExec:
            def __init__(self, cwd, runtime, popen_factory=subprocess.Popen):
                self.event_sink = None

            def run(self, prompt, timeout=task9_eval.EXEC_TURN_TIMEOUT_SECONDS):
                self.event_sink("process-started", 41, 41)
                self.event_sink("model-started", 41, 41)
                self.event_sink("process-stopped", 41, 41)
                return expected

        with mock.patch.object(task9_eval, "ExecTransport", FakeExec):
            result = task9_eval.execute_case_transport(
                {"turns": [{"prompt": "one"}]},
                Path("/fixture"),
                mock.sentinel.runtime,
                Path("/store"),
                event_sink=lambda *event: events.append(event),
            )

        self.assertIs(expected, result)
        self.assertEqual(
            [
                ("process-started", 41, 41),
                ("model-started", 41, 41),
                ("process-stopped", 41, 41),
            ],
            events,
        )

    def test_app_server_dropped_start_response_is_model_started(self):
        events = []
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=52)
        server.process_group_id = 52
        server.event_sink = lambda *event: events.append(event)

        def dropped_send(message):
            events.append(("send", 52, 52))
            raise BrokenPipeError("response dropped")

        server._send = dropped_send
        with self.assertRaises(BaseException) as caught:
            server.request("turn/start", {"threadId": "thread-1"})

        failure_type = getattr(task9_eval, "CaseTransportFailure", None)
        self.assertIsNotNone(failure_type)
        self.assertIs(type(caught.exception), failure_type)
        self.assertTrue(caught.exception.model_started)
        self.assertEqual("post-start-transport", caught.exception.classification)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(
            [("model-started", 52, 52), ("send", 52, 52)],
            events,
        )

    def test_app_server_notification_send_failure_is_typed_pre_model(self):
        server = AppServer.__new__(AppServer)
        server.model_started = False
        server._send = mock.Mock(side_effect=BrokenPipeError("NOTIFY_SECRET"))

        with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
            server.notify("initialized", {})

        self.assertFalse(caught.exception.model_started)
        self.assertEqual(
            "pre-model-infrastructure", caught.exception.classification
        )
        self.assertTrue(caught.exception.retryable)
        self.assertNotIn("NOTIFY_SECRET", str(caught.exception))

    def test_app_server_model_start_sink_failure_is_terminal_before_send(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=53)
        server.process_group_id = 53
        server._send = mock.Mock()
        server.event_sink = mock.Mock(side_effect=RuntimeError("SINK_SECRET"))

        with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
            server.request("turn/start", {"threadId": "thread-1"})

        self.assertTrue(caught.exception.model_started)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual("event-sink", caught.exception.classification)
        self.assertTrue(server.model_started)
        server._send.assert_not_called()
        self.assertNotIn("SINK_SECRET", str(caught.exception))

    def test_app_server_process_start_sink_failure_still_closes(self):
        calls = []

        class FakeServer:
            process = mock.Mock(pid=54)
            process_group_id = 54
            event_sink = None

            def close(self):
                calls.append("close")

        case = {
            "id": "late-trigger",
            "turns": [
                {"prompt": "first"},
                {"prompt": "second", "dispatch_when": "after_draft_run"},
            ],
        }

        def fail_process_start(event, pid, pgid):
            if event == "process-started":
                raise RuntimeError("PROCESS_SINK_SECRET")

        with mock.patch.object(task9_eval, "AppServer", return_value=FakeServer()), \
             mock.patch.object(task9_eval, "release_gate"):
            with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
                task9_eval.execute_case_transport(
                    case,
                    Path("/fixture"),
                    mock.Mock(writable_roots=()),
                    Path("/wiki"),
                    lambda: None,
                    event_sink=fail_process_start,
                )

        self.assertEqual("event-sink", caught.exception.classification)
        self.assertFalse(caught.exception.model_started)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual(["close"], calls)
        self.assertNotIn("PROCESS_SINK_SECRET", str(caught.exception))

    def test_app_server_partial_startup_cleans_new_process_group(self):
        process = mock.Mock(pid=56)
        process.process_group_id = 56
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(
                task9_eval,
                "_process_group_id",
                side_effect=task9_eval.CaseInfrastructureFailure(
                    "group setup failed"
                ),
            ), mock.patch.object(task9_eval, "stop_process_group") as stop:
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    AppServer(
                        Path("/fixture"),
                        mock.Mock(
                            transport_config=_test_transport_config(),
                            environment=_test_transport_environment(
                                Path(temporary)
                            ),
                            disabled_skill_paths=(),
                        ),
                        popen_factory=mock.Mock(return_value=process),
                    )

        stop.assert_called_once_with(process, readers=())

    def test_stop_process_group_kills_surviving_descendant(self):
        script = (
            "import signal,subprocess,sys,time;"
            "child=subprocess.Popen([sys.executable,'-c',"
            "'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)']);"
            "print(child.pid,flush=True);"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
            "time.sleep(60)"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        assert process.stdout is not None
        child_pid = int(process.stdout.readline().strip())
        process.process_group_id = os.getpgid(process.pid)
        try:
            task9_eval.stop_process_group(
                process,
                readers=(),
                terminate_timeout=0.05,
                kill_timeout=1.0,
            )
            self.assertIsNotNone(process.poll())
            with self.assertRaises(ProcessLookupError):
                os.killpg(process.process_group_id, 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
        finally:
            if process.poll() is None:
                os.killpg(process.process_group_id, 9)
                process.wait(timeout=2)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()

    def test_process_group_probe_error_is_terminal_quiescence_failure(self):
        with mock.patch.object(
            task9_eval.os,
            "killpg",
            side_effect=OSError("PROBE_SECRET"),
        ):
            with self.assertRaises(
                task9_eval.ProcessSurvivalCleanupFailure
            ) as caught:
                task9_eval._process_group_exists(57)

        self.assertNotIn("PROBE_SECRET", str(caught.exception))

    def test_process_group_lookup_error_is_terminal_quiescence_failure(self):
        process = mock.Mock(pid=59)
        with mock.patch.object(
            task9_eval.os,
            "getpgid",
            side_effect=OSError("LOOKUP_SECRET"),
        ):
            with self.assertRaises(
                task9_eval.ProcessSurvivalCleanupFailure
            ) as caught:
                task9_eval._process_group_id(process)

        self.assertNotIn("LOOKUP_SECRET", str(caught.exception))

    def test_process_poll_error_is_terminal_quiescence_failure(self):
        process = mock.Mock(process_group_id=58)
        process.poll.side_effect = OSError("POLL_SECRET")
        with mock.patch.object(
            task9_eval,
            "_process_group_exists",
            return_value=False,
        ):
            with self.assertRaises(
                task9_eval.ProcessSurvivalCleanupFailure
            ) as caught:
                task9_eval.stop_process_group(
                    process,
                    readers=(),
                    terminate_timeout=0.01,
                    kill_timeout=0.01,
                )

        self.assertNotIn("POLL_SECRET", str(caught.exception))

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

    def test_explicit_workspace_parent_builds_fixture_before_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "one"}],
            }
            calls = []

            def builder(value, case_root):
                calls.append(("builder", case_root))
                workspace = case_root / value["id"]
                workspace.mkdir()
                return workspace

            def runtime_factory(value, case_root, workspace, lifecycle):
                calls.append(
                    ("runtime", case_root, workspace, workspace.is_dir())
                )
                raise RuntimeError("STOP_AFTER_BINDING")

            with mock.patch.object(task9_eval, "build_case_fixture", builder):
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=runtime_factory,
                        workspace_parent=parent,
                    )

            self.assertEqual(
                [
                    ("builder", parent),
                    (
                        "runtime",
                        destination,
                        parent / "bounded-case",
                        True,
                    ),
                ],
                calls,
            )

    def test_explicit_workspace_rejections_leave_builder_and_gate_untouched(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            valid_parent = destination / "workspace"
            valid_parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "one"}],
            }
            invalid_parent = destination / "not-workspace"
            invalid_parent.mkdir(mode=0o700)
            existing_child = valid_parent / case["id"]
            before = dict(harness._GATE_ROOTS)
            for label, parent, create_child in (
                ("wrong-name", invalid_parent, False),
                ("preexisting-child", valid_parent, True),
            ):
                with self.subTest(label=label):
                    if create_child:
                        existing_child.mkdir()
                    with mock.patch.object(
                        task9_eval,
                        "build_case_fixture",
                        side_effect=AssertionError("builder must not run"),
                    ):
                        with self.assertRaises(
                            task9_eval.CaseInfrastructureFailure
                        ):
                            task9_eval._run_case(
                                case,
                                destination,
                                lifecycle=False,
                                runtime_factory=mock.Mock(),
                                workspace_parent=parent,
                            )
                    self.assertEqual(before, harness._GATE_ROOTS)
                    if create_child:
                        existing_child.rmdir()

    def test_explicit_workspace_rejects_symlink_parent_without_gate_residue(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            destination = root / "case-root"
            destination.mkdir(mode=0o700)
            external = root / "external-workspace"
            external.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.symlink_to(external, target_is_directory=True)
            before = dict(harness._GATE_ROOTS)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            with mock.patch.object(
                task9_eval,
                "build_case_fixture",
                side_effect=AssertionError("builder must not run"),
            ):
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=mock.Mock(),
                        workspace_parent=parent,
                    )

            self.assertEqual(before, harness._GATE_ROOTS)
            self.assertEqual([], list(external.iterdir()))

    def test_noncanonical_fixture_result_releases_public_gate(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            def wrong_builder(value, fixture_parent):
                harness.build_fixture(
                    value["id"],
                    value["fixture"],
                    fixture_parent,
                    include_gate=True,
                )
                wrong = fixture_parent / "wrong-result"
                wrong.mkdir()
                return wrong

            with mock.patch.object(
                task9_eval, "build_case_fixture", wrong_builder
            ):
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=mock.Mock(),
                        workspace_parent=parent,
                    )

            self.assertNotIn(case["id"], harness._GATE_ROOTS)
            self.assertTrue(
                parent.joinpath(
                    case["id"], ".eval-gates", f"{case['id']}.release"
                ).is_file()
            )

    def test_runtime_setup_failure_releases_registered_public_gate(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            try:
                with self.assertRaises(
                    task9_eval.CaseInfrastructureFailure
                ) as caught:
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=mock.Mock(
                            side_effect=RuntimeError("SETUP_SECRET")
                        ),
                        workspace_parent=parent,
                    )

                self.assertNotIn(case["id"], harness._GATE_ROOTS)
                self.assertTrue(
                    parent.joinpath(
                        case["id"], ".eval-gates", f"{case['id']}.release"
                    ).is_file()
                )
                rendered = "".join(
                    traceback.format_exception(caught.exception)
                )
                self.assertNotIn("SETUP_SECRET", rendered)
            finally:
                if case["id"] in harness._GATE_ROOTS:
                    harness.release_gate(case["id"])

    def test_runtime_setup_and_gate_release_failures_preserve_primary_first(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            try:
                with mock.patch.object(
                    task9_eval,
                    "release_gate",
                    side_effect=RuntimeError("RELEASE_SECRET"),
                ):
                    with self.assertRaises(ExceptionGroup) as caught:
                        task9_eval._run_case(
                            case,
                            destination,
                            lifecycle=False,
                            runtime_factory=mock.Mock(
                                side_effect=RuntimeError("SETUP_SECRET")
                            ),
                            workspace_parent=parent,
                        )

                self.assertEqual(2, len(caught.exception.exceptions))
                self.assertIsInstance(
                    caught.exception.exceptions[0],
                    task9_eval.CaseInfrastructureFailure,
                )
                self.assertIsInstance(
                    caught.exception.exceptions[1],
                    task9_eval.CaseCleanupFailure,
                )
                rendered = "".join(
                    traceback.format_exception(caught.exception)
                )
                self.assertNotIn("SETUP_SECRET", rendered)
                self.assertNotIn("RELEASE_SECRET", rendered)
            finally:
                if case["id"] in harness._GATE_ROOTS:
                    harness.release_gate(case["id"])

    def test_fixture_guidance_failure_releases_registered_public_gate(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            try:
                with mock.patch.object(
                    task9_eval,
                    "install_evaluator_guidance",
                    side_effect=RuntimeError("GUIDANCE_SECRET"),
                ):
                    with self.assertRaises(
                        task9_eval.CaseInfrastructureFailure
                    ) as caught:
                        task9_eval._run_case(
                            case,
                            destination,
                            lifecycle=False,
                            runtime_factory=mock.Mock(),
                            workspace_parent=parent,
                        )

                self.assertNotIn(case["id"], harness._GATE_ROOTS)
                rendered = "".join(
                    traceback.format_exception(caught.exception)
                )
                self.assertNotIn("GUIDANCE_SECRET", rendered)
            finally:
                if case["id"] in harness._GATE_ROOTS:
                    harness.release_gate(case["id"])

    def test_guidance_and_gate_release_failures_preserve_primary_first(self):
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "run scripts/gate.py"}],
            }

            try:
                with mock.patch.object(
                    task9_eval,
                    "install_evaluator_guidance",
                    side_effect=RuntimeError("GUIDANCE_SECRET"),
                ), mock.patch.object(
                    task9_eval,
                    "release_gate",
                    side_effect=RuntimeError("RELEASE_SECRET"),
                ):
                    with self.assertRaises(ExceptionGroup) as caught:
                        task9_eval._run_case(
                            case,
                            destination,
                            lifecycle=False,
                            runtime_factory=mock.Mock(),
                            workspace_parent=parent,
                        )

                self.assertEqual(2, len(caught.exception.exceptions))
                self.assertIsInstance(
                    caught.exception.exceptions[0],
                    task9_eval.CaseInfrastructureFailure,
                )
                self.assertIsInstance(
                    caught.exception.exceptions[1],
                    task9_eval.CaseCleanupFailure,
                )
                rendered = "".join(
                    traceback.format_exception(caught.exception)
                )
                self.assertNotIn("GUIDANCE_SECRET", rendered)
                self.assertNotIn("RELEASE_SECRET", rendered)
            finally:
                if case["id"] in harness._GATE_ROOTS:
                    harness.release_gate(case["id"])

    def test_runtime_setup_without_gate_never_calls_public_release(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True) / "case-root"
            destination.mkdir(mode=0o700)
            parent = destination / "workspace"
            parent.mkdir(mode=0o700)
            case = {
                "id": "bounded-case",
                "fixture": "empty",
                "turns": [{"prompt": "no checkpoint"}],
            }

            with mock.patch.object(task9_eval, "release_gate") as release:
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=mock.Mock(
                            side_effect=RuntimeError("setup failed")
                        ),
                        workspace_parent=parent,
                    )

            release.assert_not_called()

    def test_legacy_workspace_branch_remains_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True)
            case = {
                "id": "legacy-case",
                "fixture": "empty",
                "turns": [{"prompt": "one"}],
            }
            calls = []

            def builder(value, case_root):
                calls.append(("builder", case_root))
                workspace = case_root / value["id"]
                workspace.mkdir()
                return workspace

            def runtime_factory(value, case_root, workspace, lifecycle):
                calls.append(("runtime", case_root, workspace))
                raise RuntimeError("STOP_AFTER_LEGACY_BINDING")

            with mock.patch.object(task9_eval, "build_case_fixture", builder):
                with self.assertRaises(task9_eval.CaseInfrastructureFailure):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=runtime_factory,
                    )

            legacy_root = destination / "forward"
            self.assertEqual(
                [
                    ("builder", legacy_root),
                    ("runtime", legacy_root, legacy_root / "legacy-case"),
                ],
                calls,
            )

    def test_verified_evaluator_rejects_legacy_runtime_before_setup(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary).resolve(strict=True)
            case = {
                "id": "retained-case",
                "fixture": "empty",
                "turns": [{"prompt": "one"}],
            }

            with (
                mock.patch.object(
                    task9_eval,
                    "_VERIFIED_SOURCE_CAPABILITY",
                    object(),
                ),
                mock.patch.object(
                    task9_eval, "build_case_fixture"
                ) as fixture,
                mock.patch.object(
                    task9_eval, "build_payload_audit"
                ) as audit,
            ):
                with self.assertRaisesRegex(
                    task9_eval.CaseInfrastructureFailure,
                    "verified evaluator requires a runtime factory",
                ):
                    task9_eval._run_case(
                        case,
                        destination,
                        lifecycle=False,
                        runtime_factory=None,
                    )

            fixture.assert_not_called()
            audit.assert_not_called()

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
                        writable_roots=(),
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
                self.token_usage = task9_eval.ZERO_TOKEN_USAGE

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
                    environment={}, disabled_skill_paths=(), writable_roots=()
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
            environment={}, disabled_skill_paths=(), writable_roots=()
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
            environment={}, disabled_skill_paths=(), writable_roots=()
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
            environment={}, disabled_skill_paths=(), writable_roots=()
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
            environment={}, disabled_skill_paths=(), writable_roots=()
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

        def fake_persist(result_destinations, results, manifests, *, authority):
            self.assertEqual({"forward", "lifecycle"}, set(result_destinations))
            self.assertEqual("serial-coordinator", authority.role)
            self.assertEqual(expected_results, results)
            self.assertEqual(
                expected_ids,
                {
                    mode: [case["id"] for case in manifests[mode]]
                    for mode in ("forward", "lifecycle")
                },
            )
            calls.append(("persist",))
            return mock.Mock(results=json.loads(json.dumps(results)))

        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary, \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), \
             mock.patch.object(task9_eval, "assert_production_unchanged"), \
             mock.patch.object(
                 task9_eval,
                 "_persist_result_pair_retained",
                 side_effect=fake_persist,
             ) as persist, mock.patch.object(
                 task9_eval, "_validate_committed_result_semantics"
             ), mock.patch.object(
                 task9_eval, "_assert_exact_result_repository_delta"
             ), mock.patch.object(
                 task9_eval,
                 "resolve_committed_result_pair",
                 side_effect=AssertionError("pathname readback is forbidden"),
             ):
            writer_repository = Path(temporary, "writer-repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(writer_repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            task9_eval.run_suite(
                repository,
                repository_root=writer_repository,
                manifest_paths=paths,
                result_destinations={
                    "forward": writer_repository / "results" / "forward.json",
                    "lifecycle": writer_repository / "results" / "lifecycle.json",
                },
                coordinator_role="serial-coordinator",
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

    def test_run_suite_holds_one_serial_lease_through_readback_and_delta_check(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifests = {
                "forward": repository / "inputs" / "forward.json",
                "lifecycle": repository / "inputs" / "lifecycle.json",
            }
            manifests["forward"].parent.mkdir()
            for path in manifests.values():
                path.write_text("[]\n", encoding="utf-8")
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            events = []
            leases = []
            original_acquire = sharding.ResultWriterLease.acquire
            original_persist = task9_eval._persist_result_pair_retained
            original_rescore = task9_eval._validate_committed_result_semantics
            original_delta = task9_eval._assert_exact_result_repository_delta
            original_subprocess_run = subprocess.run

            def acquire(*args, **kwargs):
                events.append("acquire")
                lease = original_acquire(*args, **kwargs)
                leases.append(lease)
                return lease

            def require_live(event):
                self.assertEqual(1, len(leases))
                leases[0]._validate_live()
                events.append(event)

            def final_guard(_snapshot):
                if not leases:
                    events.append("prelease-production-check")
                else:
                    require_live("final-production-check")

            def persist(*args, **kwargs):
                require_live("persist-enter")
                retained = original_persist(*args, **kwargs)
                retained.result_parent._validate_live()
                require_live("persist-return")
                return retained

            def rescore(results, manifest_rows):
                require_live("semantic-rescore")
                return original_rescore(results, manifest_rows)

            def exact_delta(before, retained, lease):
                retained.result_parent._validate_live()
                require_live("exact-delta")
                return original_delta(before, retained, lease)

            def forbid_post_acquire_git(command, *args, **kwargs):
                if leases and command and command[0] == "git":
                    raise AssertionError("post-acquire Git reopen is forbidden")
                return original_subprocess_run(command, *args, **kwargs)

            with mock.patch.object(
                task9_eval, "validate_frozen_manifests"
            ), mock.patch.object(
                task9_eval, "snapshot_production", return_value="baseline"
            ), mock.patch.object(
                task9_eval, "assert_production_unchanged", side_effect=final_guard
            ), mock.patch.object(
                task9_eval.ResultWriterLease, "acquire", side_effect=acquire
            ) as acquire_call, mock.patch.object(
                task9_eval, "_persist_result_pair_retained", side_effect=persist
            ), mock.patch.object(
                task9_eval,
                "_validate_committed_result_semantics",
                side_effect=rescore,
            ), mock.patch.object(
                task9_eval,
                "_assert_exact_result_repository_delta",
                side_effect=exact_delta,
            ), mock.patch.object(
                task9_eval.subprocess,
                "run",
                side_effect=forbid_post_acquire_git,
            ), mock.patch.object(
                task9_eval,
                "resolve_committed_result_pair",
                side_effect=AssertionError(
                    "authoritative run_suite must not reopen committed results by path"
                ),
            ):
                results = task9_eval.run_suite(
                    repository,
                    repository_root=repository,
                    manifest_paths=manifests,
                    result_destinations=destinations,
                    coordinator_role="serial-coordinator",
                )

            self.assertEqual(([], []), results)
            self.assertEqual(1, acquire_call.call_count)
            self.assertEqual(
                [
                    "prelease-production-check",
                    "acquire",
                    "final-production-check",
                    "persist-enter",
                    "persist-return",
                    "semantic-rescore",
                    "exact-delta",
                ],
                events,
            )
            with self.assertRaisesRegex(RuntimeError, "closed"):
                leases[0]._validate_live()

    def test_run_suite_rescores_descriptor_readback_after_pointer_replace(self):
        manifests, _valid, semantic_mismatch = self._result_fixture()
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifest_paths = {
                mode: repository / "inputs" / f"{mode}.json"
                for mode in ("forward", "lifecycle")
            }
            manifest_paths["forward"].parent.mkdir()
            for mode, path in manifest_paths.items():
                path.write_text(
                    json.dumps(manifests[mode]) + "\n", encoding="utf-8"
                )
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            by_id = {
                row["id"]: row
                for rows in semantic_mismatch.values()
                for row in rows
            }

            with mock.patch.object(
                task9_eval, "validate_frozen_manifests"
            ), mock.patch.object(
                task9_eval,
                "_run_case",
                side_effect=lambda case, *_args, **_kwargs: by_id[case["id"]],
            ), mock.patch.object(
                task9_eval,
                "resolve_committed_result_pair",
                side_effect=AssertionError(
                    "authoritative rescore must not reopen results by path"
                ),
            ):
                with self.assertRaisesRegex(
                    AssertionError, "draft records remain"
                ):
                    task9_eval.run_suite(
                        repository,
                        repository_root=repository,
                        manifest_paths=manifest_paths,
                        result_destinations=destinations,
                        coordinator_role="serial-coordinator",
                    )

            self.assertTrue(
                (repository / "results" / RESULT_COMMIT_FILENAME).is_file()
            )
            reacquired = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            reacquired.close()

    def test_run_suite_rejects_unexpected_delta_after_pointer_replace(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifest_paths = {
                mode: repository / "inputs" / f"{mode}.json"
                for mode in ("forward", "lifecycle")
            }
            manifest_paths["forward"].parent.mkdir()
            for path in manifest_paths.values():
                path.write_text("[]\n", encoding="utf-8")
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            original_readback = task9_eval._readback_result_pair_at

            def mutate_after_pointer_replace(*args, **kwargs):
                decoded = original_readback(*args, **kwargs)
                (repository / "unexpected.txt").write_text(
                    "unexpected\n", encoding="utf-8"
                )
                return decoded

            with mock.patch.object(
                task9_eval, "validate_frozen_manifests"
            ), mock.patch.object(
                task9_eval,
                "_readback_result_pair_at",
                side_effect=mutate_after_pointer_replace,
            ), mock.patch.object(
                task9_eval,
                "resolve_committed_result_pair",
                side_effect=AssertionError(
                    "authoritative delta check must not reopen results by path"
                ),
            ):
                with self.assertRaisesRegex(
                    AssertionError, "unexpected repository delta"
                ):
                    task9_eval.run_suite(
                        repository,
                        repository_root=repository,
                        manifest_paths=manifest_paths,
                        result_destinations=destinations,
                        coordinator_role="serial-coordinator",
                    )

            self.assertTrue(
                (repository / "results" / RESULT_COMMIT_FILENAME).is_file()
            )

    def test_run_suite_rejects_semantic_rewrite_of_committed_generation(self):
        manifests, valid, semantic_mismatch = self._result_fixture()
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifest_paths = {
                mode: repository / "inputs" / f"{mode}.json"
                for mode in ("forward", "lifecycle")
            }
            manifest_paths["forward"].parent.mkdir()
            for mode, path in manifest_paths.items():
                path.write_text(
                    json.dumps(manifests[mode]) + "\n", encoding="utf-8"
                )
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            by_id = {
                row["id"]: row for rows in valid.values() for row in rows
            }
            original_rescore = task9_eval._validate_committed_result_semantics

            def rewrite_after_rescore(results, manifest_rows):
                original_rescore(results, manifest_rows)
                pointer_path = (
                    repository / "results" / RESULT_COMMIT_FILENAME
                )
                pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
                replacement = task9_eval._json_bytes(
                    semantic_mismatch["forward"]
                )
                generation_path = (
                    repository / "results" / pointer["files"]["forward"]["path"]
                )
                generation_path.write_bytes(replacement)
                pointer["files"]["forward"]["sha256"] = hashlib.sha256(
                    replacement
                ).hexdigest()
                pointer_path.write_bytes(task9_eval._json_bytes(pointer))

            with mock.patch.object(
                task9_eval, "validate_frozen_manifests"
            ), mock.patch.object(
                task9_eval,
                "_run_case",
                side_effect=lambda case, *_args, **_kwargs: by_id[case["id"]],
            ), mock.patch.object(
                task9_eval,
                "_validate_committed_result_semantics",
                side_effect=rewrite_after_rescore,
            ):
                with self.assertRaisesRegex(
                    AssertionError, "unexpected repository delta"
                ):
                    task9_eval.run_suite(
                        repository,
                        repository_root=repository,
                        manifest_paths=manifest_paths,
                        result_destinations=destinations,
                        coordinator_role="serial-coordinator",
                    )

    def test_run_suite_rejects_same_byte_authoritative_inode_replacement(self):
        manifests, valid, _semantic_mismatch = self._result_fixture()
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifest_paths = {
                mode: repository / "inputs" / f"{mode}.json"
                for mode in ("forward", "lifecycle")
            }
            manifest_paths["forward"].parent.mkdir()
            for mode, path in manifest_paths.items():
                path.write_text(
                    json.dumps(manifests[mode]) + "\n", encoding="utf-8"
                )
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            by_id = {
                row["id"]: row for rows in valid.values() for row in rows
            }
            original_rescore = task9_eval._validate_committed_result_semantics

            def replace_after_rescore(results, manifest_rows):
                original_rescore(results, manifest_rows)
                result_root = repository / "results"
                pointer_path = result_root / RESULT_COMMIT_FILENAME
                pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
                authoritative = [
                    pointer_path,
                    *(
                        result_root / pointer["files"][mode]["path"]
                        for mode in ("forward", "lifecycle")
                    ),
                ]
                for index, path in enumerate(authoritative):
                    replacement = path.with_name(f".replacement-{index}")
                    replacement.write_bytes(path.read_bytes())
                    replacement.chmod(stat.S_IMODE(path.stat().st_mode))
                    os.replace(replacement, path)

            with mock.patch.object(
                task9_eval, "validate_frozen_manifests"
            ), mock.patch.object(
                task9_eval,
                "_run_case",
                side_effect=lambda case, *_args, **_kwargs: by_id[case["id"]],
            ), mock.patch.object(
                task9_eval,
                "_validate_committed_result_semantics",
                side_effect=replace_after_rescore,
            ):
                with self.assertRaisesRegex(
                    (AssertionError, RuntimeError), "changed"
                ):
                    task9_eval.run_suite(
                        repository,
                        repository_root=repository,
                        manifest_paths=manifest_paths,
                        result_destinations=destinations,
                        coordinator_role="serial-coordinator",
                    )

    def test_unset_worker_unknown_and_nonformal_roles_fail_before_result_paths(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifests = {
                "forward": repository / "inputs" / "forward.json",
                "lifecycle": repository / "inputs" / "lifecycle.json",
            }
            manifests["forward"].parent.mkdir()
            for path in manifests.values():
                path.write_text("[]\n", encoding="utf-8")

            class EqualitySpoof:
                def __eq__(self, _other):
                    return True

            for role in (
                None,
                "worker",
                "parallel-coordinator",
                "unknown",
                EqualitySpoof(),
            ):
                with self.subTest(role=role):
                    result_root = repository / f"results-{role}"
                    with mock.patch.object(
                        task9_eval, "validate_frozen_manifests"
                    ), mock.patch.object(
                        task9_eval, "snapshot_production", return_value="baseline"
                    ), mock.patch.object(
                        task9_eval, "assert_production_unchanged"
                    ), self.assertRaises(ValueError):
                        task9_eval.run_suite(
                            repository,
                            repository_root=repository,
                            manifest_paths=manifests,
                            result_destinations={
                                "forward": result_root / "forward.json",
                                "lifecycle": result_root / "lifecycle.json",
                            },
                            coordinator_role=role,
                        )
                    self.assertFalse(result_root.exists())

            for run_kind in ("diagnostic", "discovery"):
                with self.subTest(run_kind=run_kind), self.assertRaises(ValueError):
                    sharding.ResultWriterLease.acquire(
                        repository,
                        role="serial-coordinator",
                        run_kind=run_kind,
                        run_lease=None,
                    )

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
                    repository_root=repository.parent,
                    manifest_paths=paths,
                    result_destinations={
                        "forward": Path(temporary) / "forward.json",
                        "lifecycle": Path(temporary) / "lifecycle.json",
                    },
                    coordinator_role="serial-coordinator",
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
                                repository_root=repository.parent,
                                manifest_paths=paths,
                                result_destinations={
                                    "forward": temporary_root / "forward-result.json",
                                    "lifecycle": temporary_root
                                    / "lifecycle-result.json",
                                },
                                coordinator_role="serial-coordinator",
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

        committed = {}

        def capture_persist(_destinations, results, _manifests, *, authority):
            self.assertEqual("serial-coordinator", authority.role)
            committed.clear()
            committed.update(results)
            return mock.Mock(results=json.loads(json.dumps(results)))

        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary, \
             mock.patch.object(Path, "read_text", reject_second_manifest_read), \
             mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
             mock.patch.object(
                 task9_eval, "snapshot_production", return_value="baseline"
             ), mock.patch.object(
                 task9_eval, "assert_production_unchanged"
             ), mock.patch.object(
                 task9_eval,
                 "_persist_result_pair_retained",
                 side_effect=capture_persist,
             ), mock.patch.object(
                 task9_eval, "_validate_committed_result_semantics"
             ), mock.patch.object(
                 task9_eval, "_assert_exact_result_repository_delta"
             ), mock.patch.object(
                 task9_eval,
                 "resolve_committed_result_pair",
                 side_effect=AssertionError("pathname readback is forbidden"),
             ):
            writer_repository = Path(temporary, "writer-repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(writer_repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            task9_eval.run_suite(
                repository,
                repository_root=writer_repository,
                manifest_paths=paths,
                result_destinations={
                    "forward": writer_repository / "results" / "forward.json",
                    "lifecycle": writer_repository / "results" / "lifecycle.json",
                },
                coordinator_role="serial-coordinator",
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
        failed = task9_eval.CaseExecution(
            "failed", "failed", (), (), task9_eval.ZERO_TOKEN_USAGE
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=mock.sentinel.audit,
                environment={},
                writable_roots=(),
                transport_config=_test_transport_config(),
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
        config = _test_transport_config()
        overrides = task9_eval.build_codex_config_overrides(
            config, {"B": "two", "A": "one"}, disabled
        )
        self.assertEqual(
            (
                'shell_environment_policy.set={ A = "one", B = "two" }',
                'model="test-model"',
                'model_reasoning_effort="medium"',
                'approval_policy="never"',
                'sandbox_mode="workspace-write"',
                "sandbox_workspace_write.network_access=false",
                'web_search="disabled"',
                "features.multi_agent=true",
                build_disabled_skills_override(disabled),
            ),
            overrides,
        )

    def test_app_server_uses_common_codex_config_overrides(self):
        class ExitedProcess:
            pid = 4321
            stdout = ()
            stderr = ()

            @staticmethod
            def poll():
                return 0

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            environment = _test_transport_environment(root)
            environment["A"] = "one"
            disabled = (Path("/skills/one"),)
            config = _test_transport_config()
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=mock.sentinel.audit,
                environment=environment,
                writable_roots=(),
                transport_config=config,
                disabled_skill_paths=disabled,
            )
            expected = task9_eval.build_app_server_command(
                config,
                task9_eval.build_codex_config_overrides(
                    config, environment, disabled
                ),
            )
            popen = mock.Mock(return_value=ExitedProcess())
            AppServer(Path("/fixture"), runtime, popen_factory=popen)

        self.assertEqual(expected, popen.call_args.args[0])
        self.assertEqual(
            environment["CODEX_HOME"], popen.call_args.kwargs["env"]["CODEX_HOME"]
        )

    def test_exec_command_is_ephemeral_json_fail_closed_and_prompt_free(self):
        root = Path("/fixture")
        output = Path("/audit/final.txt")
        overrides = ('approval_policy="never"', 'web_search="disabled"')
        config = _test_transport_config()
        command = task9_eval.build_exec_command(
            config, root, [Path("/store"), Path("/audit")], output, overrides
        )
        self.assertEqual(
            [
                config.codex_executable_path,
                "exec", "--json", "--ephemeral", "--ignore-rules",
                "--ignore-user-config", "--strict-config",
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
        self.assertEqual(
            task9_eval.TokenUsage(1, 0, 1, 0, 2), result.usage
        )

    def test_exec_jsonl_rejects_missing_invalid_or_overflowing_usage(self):
        valid_prefix = json.dumps({
            "type": "item.completed",
            "item": {"id": "msg", "type": "agent_message", "text": "done"},
        })
        invalid_usage = (
            {},
            {"input_tokens": True, "output_tokens": 1},
            {"input_tokens": -1, "output_tokens": 1},
            {"input_tokens": 1, "output_tokens": 1, "cached_input_tokens": 2},
            {"input_tokens": 1, "output_tokens": 1, "reasoning_output_tokens": 2},
            {"input_tokens": 1, "output_tokens": 1, "total_tokens": 3},
            {"input_tokens": 2**63, "output_tokens": 0},
        )
        for usage in invalid_usage:
            with self.subTest(usage=usage):
                stdout = "\n".join((
                    valid_prefix,
                    json.dumps({"type": "turn.completed", "usage": usage}),
                ))
                with self.assertRaises(task9_eval.CaseProtocolFailure):
                    task9_eval.parse_exec_jsonl(stdout, "done")

    def test_app_server_uses_latest_exact_token_usage_update(self):
        server = AppServer.__new__(AppServer)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.token_usage = None
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-active"
        server._record({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-active",
                "tokenUsage": {"total": {
                "inputTokens": 3,
                "cachedInputTokens": 1,
                "outputTokens": 2,
                "reasoningOutputTokens": 1,
                "totalTokens": 5,
            }}},
        })
        server._record({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-other",
                "turnId": "turn-active",
                "tokenUsage": {"total": {
                    "inputTokens": 30,
                    "outputTokens": 20,
                    "totalTokens": 50,
                }},
            },
        })
        server._record({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-active",
                "tokenUsage": {"total": {
                "inputTokens": 7,
                "cachedInputTokens": 2,
                "outputTokens": 5,
                "reasoningOutputTokens": 3,
                "totalTokens": 12,
            }}},
        })
        server._record({
            "method": "thread/tokenUsage/updated",
            "params": {"tokenUsage": {"total": {
                "inputTokens": 90,
                "outputTokens": 10,
                "totalTokens": 100,
            }}},
        })
        server._record({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-other",
                "tokenUsage": {"total": {
                    "inputTokens": 60,
                    "outputTokens": 40,
                    "totalTokens": 100,
                }},
            },
        })

        self.assertEqual(
            task9_eval.TokenUsage(7, 2, 5, 3, 12), server.token_usage
        )

    def test_app_server_active_usage_is_strict_and_content_free(self):
        server = AppServer.__new__(AppServer)
        server.events = []
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-active"
        server.token_usage = None

        with self.assertRaises(task9_eval.CaseProtocolFailure) as caught:
            server._record({
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-active",
                    "turnId": "turn-active",
                    "tokenUsage": {"total": {
                        "inputTokens": 1,
                        "outputTokens": 1,
                        "UNSAFE_SECRET": "PROMPT_SECRET",
                    }},
                },
            })

        self.assertNotIn("UNSAFE_SECRET", str(caught.exception))
        self.assertNotIn("PROMPT_SECRET", str(caught.exception))

    def test_app_server_new_turn_resets_selected_token_usage(self):
        server = AppServer.__new__(AppServer)
        server.events = []
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-old"
        server.token_usage = task9_eval.TokenUsage(3, 0, 2, 0, 5)
        server.request = mock.Mock(
            return_value={"turn": {"id": "turn-new"}}
        )

        turn_id = server.start_turn(
            "thread-active",
            "prompt",
            Path("/workspace"),
            (),
        )

        self.assertEqual("turn-new", turn_id)
        self.assertEqual("thread-active", server.active_thread_id)
        self.assertEqual("turn-new", server.active_turn_id)
        self.assertIsNone(server.token_usage)

    def test_app_server_keeps_usage_received_before_turn_start_response(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=61)
        server.process.poll.return_value = None
        server.process_group_id = 61
        server.event_sink = None
        server.model_started = False
        server._send = mock.Mock()
        server.messages = queue.Queue()
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-old"
        server.token_usage = task9_eval.TokenUsage(3, 0, 2, 0, 5)
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-new",
                "tokenUsage": {"total": {
                    "inputTokens": 8,
                    "cachedInputTokens": 2,
                    "outputTokens": 5,
                    "reasoningOutputTokens": 1,
                    "totalTokens": 13,
                }},
            },
        })
        server.messages.put({
            "id": 1,
            "result": {"turn": {"id": "turn-new"}},
        })

        turn_id = server.start_turn(
            "thread-active",
            "prompt",
            Path("/workspace"),
            (),
        )

        self.assertEqual("turn-new", turn_id)
        self.assertEqual(
            task9_eval.TokenUsage(8, 2, 5, 1, 13),
            server.token_usage,
        )

    def test_app_server_ignores_malformed_pending_usage_for_other_turn(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=62)
        server.process.poll.return_value = None
        server.process_group_id = 62
        server.event_sink = None
        server.model_started = False
        server._send = mock.Mock()
        server.messages = queue.Queue()
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-old"
        server.token_usage = None
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-other",
                "tokenUsage": {"total": {
                    "inputTokens": "PENDING_SECRET",
                    "outputTokens": 1,
                }},
            },
        })
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-new",
                "tokenUsage": {"total": {
                    "inputTokens": 8,
                    "outputTokens": 5,
                    "totalTokens": 13,
                }},
            },
        })
        server.messages.put({
            "id": 1,
            "result": {"turn": {"id": "turn-new"}},
        })

        turn_id = server.start_turn(
            "thread-active", "prompt", Path("/workspace"), ()
        )

        self.assertEqual("turn-new", turn_id)
        self.assertEqual(
            task9_eval.TokenUsage(8, 0, 5, 0, 13),
            server.token_usage,
        )

    def test_app_server_suspends_old_usage_while_new_turn_is_pending(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=64)
        server.process.poll.return_value = None
        server.process_group_id = 64
        server.event_sink = None
        server.model_started = False
        server._send = mock.Mock()
        server.messages = queue.Queue()
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-old"
        server.token_usage = task9_eval.TokenUsage(3, 0, 2, 0, 5)
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-old",
                "tokenUsage": {"total": {
                    "inputTokens": "OLD_TURN_SECRET",
                    "outputTokens": 1,
                }},
            },
        })
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-new",
                "tokenUsage": {"total": {
                    "inputTokens": 8,
                    "outputTokens": 5,
                    "totalTokens": 13,
                }},
            },
        })
        server.messages.put({
            "id": 1,
            "result": {"turn": {"id": "turn-new"}},
        })

        turn_id = server.start_turn(
            "thread-active", "prompt", Path("/workspace"), ()
        )

        self.assertEqual("turn-new", turn_id)
        self.assertEqual(
            task9_eval.TokenUsage(8, 0, 5, 0, 13),
            server.token_usage,
        )

    def test_app_server_defers_matching_pending_usage_failure(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=63)
        server.process.poll.return_value = None
        server.process_group_id = 63
        server.event_sink = None
        server.model_started = False
        server._send = mock.Mock()
        server.messages = queue.Queue()
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.active_thread_id = "thread-active"
        server.active_turn_id = "turn-old"
        server.token_usage = None
        server.messages.put({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "thread-active",
                "turnId": "turn-new",
                "tokenUsage": {"total": {
                    "inputTokens": "MATCHING_SECRET",
                    "outputTokens": 1,
                }},
            },
        })
        server.messages.put({
            "id": 1,
            "result": {"turn": {"id": "turn-new"}},
        })

        with self.assertRaises(task9_eval.CaseProtocolFailure) as caught:
            server.start_turn(
                "thread-active", "prompt", Path("/workspace"), ()
            )

        self.assertTrue(server.messages.empty())
        rendered = "".join(traceback.format_exception(caught.exception))
        self.assertNotIn("MATCHING_SECRET", rendered)

    def test_app_server_error_response_is_typed_model_failure(self):
        server = AppServer.__new__(AppServer)
        server._request_id = 0
        server.process = mock.Mock(pid=55)
        server.process_group_id = 55
        server.event_sink = None
        server.model_started = False
        server._send = mock.Mock()
        server._receive = mock.Mock(
            return_value={"id": 1, "error": {"code": -1, "message": "SECRET"}}
        )

        with self.assertRaises(task9_eval.CaseModelFailure) as caught:
            server.request("turn/start", {"threadId": "thread-1"})

        self.assertTrue(caught.exception.model_started)
        self.assertEqual("model", caught.exception.classification)
        self.assertFalse(caught.exception.retryable)
        self.assertNotIn("SECRET", str(caught.exception))

    def test_app_server_failed_turn_is_typed_model_failure(self):
        server = AppServer.__new__(AppServer)
        server.completed_turns = {
            "turn-1": {"id": "turn-1", "status": "failed"}
        }
        with self.assertRaises(task9_eval.CaseModelFailure):
            server.wait_turn("turn-1", timeout=0.01)

    def test_app_server_malformed_stdout_is_typed_protocol_failure(self):
        server = AppServer.__new__(AppServer)
        server.process = mock.Mock(stdout=["not-json SECRET\n"])
        server.messages = queue.Queue()
        server.stderr_tail = deque(maxlen=80)
        server.events = []
        server.agent_messages = []
        server.command_executions = []
        server.observation_command_diagnostics = []
        server.completed_turns = {}
        server.active_command_executions = {}
        server.model_started = True

        server._read_stdout()
        with self.assertRaises(task9_eval.CaseProtocolFailure) as caught:
            server._receive(0.01)

        self.assertTrue(caught.exception.model_started)
        self.assertNotIn("SECRET", str(caught.exception))

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
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 0, "output_tokens": 0,
            }}),
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
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 0, "output_tokens": 0,
                }}),
            )),
            "missing-agent": json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 0, "output_tokens": 0,
            }}),
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
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 0, "output_tokens": 0,
            }}),
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
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 0, "output_tokens": 0,
            }}),
        ))
        with self.assertRaisesRegex(ValueError, "final message mismatch"):
            task9_eval.parse_exec_jsonl(stdout, "file-final")

    def test_exec_jsonl_rejects_out_of_order_terminal_lifecycle(self):
        command = "COMMAND_SECRET"
        cases = {
            "terminal-before-agent-message": (
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 0, "output_tokens": 0,
                }}),
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
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 0, "output_tokens": 0,
                }}),
                json.dumps({"type": "item.completed", "item": {
                    "id": "cmd", "type": "command_execution", "command": command,
                    "status": "completed", "exit_code": 0,
                }}),
            ),
            "lifecycle-item-after-terminal": (
                json.dumps({"type": "item.completed", "item": {
                    "id": "msg", "type": "agent_message", "text": "done",
                }}),
                json.dumps({"type": "turn.completed", "usage": {
                    "input_tokens": 0, "output_tokens": 0,
                }}),
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
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 0, "output_tokens": 0,
            }}),
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
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

    def test_exec_events_bracket_prompt_and_group_cleanup(self):
        stdout = "\n".join((
            json.dumps({"type": "item.completed", "item": {
                "id": "msg", "type": "agent_message", "text": "done",
            }}),
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 1, "output_tokens": 1,
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = FakeExecProcess(stdout=stdout)
            popen_kwargs = {}

            def popen_factory(command, **kwargs):
                popen_kwargs.update(kwargs)
                Path(command[command.index("-o") + 1]).write_text(
                    "done", encoding="utf-8"
                )
                return process

            events = []
            transport = task9_eval.ExecTransport(root, runtime, popen_factory)
            transport.event_sink = lambda *event: events.append(event)
            with mock.patch.object(
                task9_eval,
                "stop_process_group",
                side_effect=lambda *a, **k: events.append(("cleanup", 4321, 4321)),
            ) as stop:
                result = transport.run("PROMPT_SECRET")

        self.assertEqual("done", result.final_text)
        self.assertTrue(popen_kwargs["start_new_session"])
        self.assertEqual(
            [
                ("process-started", 4321, 4321),
                ("model-started", 4321, 4321),
                ("cleanup", 4321, 4321),
                ("process-stopped", 4321, 4321),
            ],
            events,
        )
        stop.assert_called_once_with(process, readers=())

    def test_exec_model_event_sink_failure_never_sends_prompt_but_cleans(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            (audit_root / "tmp").mkdir(parents=True)
            store = root / "store"
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=audit_root / "tmp",
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = FakeExecProcess()
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *a, **k: process
            )

            def sink(event, pid, pgid):
                if event == "model-started":
                    raise RuntimeError("MODEL_EVENT_SECRET")

            transport.event_sink = sink
            with mock.patch.object(task9_eval, "stop_process_group") as stop:
                with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
                    transport.run("PROMPT_SECRET")

        self.assertTrue(caught.exception.model_started)
        self.assertEqual("event-sink", caught.exception.classification)
        self.assertFalse(caught.exception.retryable)
        self.assertEqual([], process.calls)
        stop.assert_called_once_with(process, readers=())
        self.assertNotIn("MODEL_EVENT_SECRET", str(caught.exception))

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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = FakeExecProcess(
                stdout="PROMPT_SECRET",
                stderr="STDERR_SECRET",
                returncode=None,
                timeout=True,
                timeout_command="ARGV_SECRET",
            )
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )
            with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = TerminateStopsExecProcess(stdout=stdout, timeout=True)
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(task9_eval.CaseTransportFailure) as caught:
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = PostKillWaitTimeoutExecProcess(
                stdout="PROMPT_SECRET",
                stderr="STDERR_SECRET",
                returncode=None,
                timeout=True,
                timeout_command="ARGV_SECRET",
            )
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(Exception) as caught:
                transport.run("PROMPT_SECRET", timeout=0.01)

            error = caught.exception
            self.assertTrue(
                task9_eval._contains_process_survival_failure(error)
            )
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
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
            environment={}, disabled_skill_paths=(), writable_roots=()
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
                writable_roots=(),
                transport_config=_test_transport_config(),
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
            )
            process = TerminateStopsExecProcess(timeout=True)
            transport = task9_eval.ExecTransport(
                root, runtime, lambda *args, **kwargs: process
            )

            with self.assertRaises(task9_eval.CaseTransportFailure):
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
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
                    environment=_test_transport_environment(root),
                    writable_roots=(store, audit_root),
                    transport_config=_test_transport_config(),
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
                environment=_test_transport_environment(root),
                writable_roots=(store, audit_root),
                transport_config=_test_transport_config(),
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

    def test_persist_requires_single_use_authority_before_destination_open(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            manifests = {"forward": [], "lifecycle": []}
            results = {"forward": [], "lifecycle": []}
            unleased_root = repository / "unleased"
            unleased = {
                "forward": unleased_root / "forward.json",
                "lifecycle": unleased_root / "lifecycle.json",
            }
            with self.assertRaises(TypeError):
                persist_result_pair(unleased, results, manifests)
            self.assertFalse(unleased_root.exists())

            fabricated_root = repository / "fabricated"
            fabricated = {
                "forward": fabricated_root / "forward.json",
                "lifecycle": fabricated_root / "lifecycle.json",
            }
            with self.assertRaises(TypeError):
                persist_result_pair(
                    fabricated,
                    results,
                    manifests,
                    authority=object(),
                )
            self.assertFalse(fabricated_root.exists())

            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            try:
                authority = lease.authority()
                committed_root = repository / "committed"
                committed = {
                    "forward": committed_root / "forward.json",
                    "lifecycle": committed_root / "lifecycle.json",
                }
                pointer = persist_result_pair(
                    committed,
                    results,
                    manifests,
                    authority=authority,
                )
                self.assertTrue(authority.consumed)
                self.assertEqual(
                    results,
                    resolve_committed_result_pair(pointer, manifests),
                )

                reused_root = repository / "reused"
                reused = {
                    "forward": reused_root / "forward.json",
                    "lifecycle": reused_root / "lifecycle.json",
                }
                with self.assertRaises(RuntimeError):
                    persist_result_pair(
                        reused,
                        results,
                        manifests,
                        authority=authority,
                    )
                self.assertFalse(reused_root.exists())
            finally:
                lease.close()

    def test_persist_freezes_exact_destination_mapping_before_authority_consumption(self):
        class SwitchingDestinations(dict):
            def __init__(self, initial, switched_root):
                super().__init__(initial)
                self._reads = 0
                self._switched_root = switched_root

            def __getitem__(self, key):
                self._reads += 1
                if self._reads > 2:
                    return self._switched_root / f"{key}.json"
                return super().__getitem__(key)

        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = root / "repository"
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            external = root / "external"
            destinations = SwitchingDestinations(
                {
                    "forward": repository / "results" / "forward.json",
                    "lifecycle": repository / "results" / "lifecycle.json",
                },
                external,
            )
            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            try:
                authority = lease.authority()
                with self.assertRaises(TypeError):
                    persist_result_pair(
                        destinations,
                        {"forward": [], "lifecycle": []},
                        {"forward": [], "lifecycle": []},
                        authority=authority,
                    )
                self.assertFalse(authority.consumed)
                self.assertFalse((repository / "results").exists())
                self.assertFalse(external.exists())
            finally:
                lease.close()

    def test_consumed_authority_rejects_repository_swap_before_result_open(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = root / "repository"
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            destinations = {
                "forward": repository / "results" / "forward.json",
                "lifecycle": repository / "results" / "lifecycle.json",
            }
            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            moved = root / "moved-repository"
            try:
                authority = lease.authority()
                authority._consume(destinations)
                repository.rename(moved)
                subprocess.run(
                    ["git", "init", "-q", str(repository)],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                with self.assertRaises((RuntimeError, ValueError)):
                    authority._open_result_parent()
                self.assertFalse((repository / "results").exists())
                self.assertFalse((moved / "results").exists())
            finally:
                if repository.exists():
                    shutil.rmtree(repository)
                if moved.exists():
                    moved.rename(repository)
                lease.close()

    def test_destination_parent_replacement_before_and_after_retention_is_not_authoritative(self):
        manifests = {"forward": [], "lifecycle": []}
        results = {"forward": [], "lifecycle": []}
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = root / "repository"
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            result_parent = repository / "results"
            result_parent.mkdir(mode=0o700)
            destinations = {
                "forward": result_parent / "forward.json",
                "lifecycle": result_parent / "lifecycle.json",
            }

            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            moved_before = repository / "results-before"
            real_open = sharding.os.open
            swapped = False

            def swap_before_open(path, flags, *args, **kwargs):
                nonlocal swapped
                if path == "results" and kwargs.get("dir_fd") == lease._repository_slot.descriptor:
                    result_parent.rename(moved_before)
                    result_parent.mkdir(mode=0o700)
                    swapped = True
                return real_open(path, flags, *args, **kwargs)

            try:
                authority = lease.authority()
                authority._consume(destinations)
                with mock.patch.object(sharding.os, "open", side_effect=swap_before_open):
                    with self.assertRaises(RuntimeError):
                        authority._open_result_parent()
                self.assertTrue(swapped)
                self.assertTrue(authority.consumed)
                self.assertEqual([], list(result_parent.iterdir()))
                self.assertEqual([], list(moved_before.iterdir()))
            finally:
                shutil.rmtree(result_parent, ignore_errors=True)
                moved_before.rename(result_parent)
                lease.close()

            moved_after = repository / "results-after"
            original_readback = task9_eval._readback_result_pair_at

            def swap_after_retention(*args, **kwargs):
                result_parent.rename(moved_after)
                result_parent.mkdir(mode=0o700)
                return original_readback(*args, **kwargs)

            with self._result_writer_authority(repository) as authority:
                with mock.patch.object(
                    task9_eval,
                    "_readback_result_pair_at",
                    side_effect=swap_after_retention,
                ), self.assertRaises(RuntimeError):
                    persist_result_pair(
                        destinations,
                        results,
                        manifests,
                        authority=authority,
                    )
                self.assertTrue(authority.consumed)
                self.assertEqual([], list(result_parent.iterdir()))
            shutil.rmtree(result_parent)
            moved_after.rename(result_parent)

    def test_authoritative_persistence_descriptors_are_one_shot_and_lock_closes_last(self):
        evidence_root = Path(__file__).parents[1]
        program = r'''
from pathlib import Path
import os
import subprocess
import sys

from scripts import run_observing_workflows_task9_eval as evaluator
from scripts import workflow_eval_sharding as sharding

repository = Path(sys.argv[1])
role = sys.argv[2]
subprocess.run(["git", "init", "-q", str(repository)], check=True)
lease = sharding.ResultWriterLease.acquire(
    repository, role="serial-coordinator", run_kind="formal", run_lease=None
)
destinations = {
    "forward": repository / "results" / "forward.json",
    "lifecycle": repository / "results" / "lifecycle.json",
}
real_open = os.open
real_close = os.close
target = None
target_identity = None
target_reopen = None
target_close_calls = 0
calls = []

def matches(path, flags):
    name = os.fspath(path)
    access = flags & os.O_ACCMODE
    is_directory = bool(flags & getattr(os, "O_DIRECTORY", 0))
    if role == "result-parent":
        return name == "results" and is_directory
    if role == "generation-directory":
        return name == evaluator.RESULT_GENERATION_DIRECTORY and is_directory
    if role == "generation-staging":
        return name.startswith(".") and "-forward.json." in name and name.endswith(".tmp")
    if role == "pointer-staging":
        return name.startswith(f".{evaluator.RESULT_COMMIT_FILENAME}.") and name.endswith(".tmp")
    if role == "pointer-read":
        return name == evaluator.RESULT_COMMIT_FILENAME and access == os.O_RDONLY
    if role == "generation-forward-read":
        return name.endswith("-forward.json") and not name.startswith(".") and access == os.O_RDONLY
    if role == "generation-lifecycle-read":
        return name.endswith("-lifecycle.json") and not name.startswith(".") and access == os.O_RDONLY
    return False

def recording_open(path, flags, *args, **kwargs):
    global target, target_identity, target_reopen
    descriptor = real_open(path, flags, *args, **kwargs)
    if target is None and matches(path, flags):
        target = descriptor
        metadata = os.fstat(descriptor)
        target_identity = (metadata.st_dev, metadata.st_ino)
        reopen_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        if flags & getattr(os, "O_DIRECTORY", 0):
            reopen_flags |= getattr(os, "O_DIRECTORY", 0)
        target_reopen = (path, reopen_flags, kwargs.get("dir_fd"))
    return descriptor

def close_then_reuse(descriptor):
    global target, target_close_calls
    calls.append(descriptor)
    real_close(descriptor)
    if descriptor == target:
        target_close_calls += 1
        path, flags, directory_fd = target_reopen
        options = {} if directory_fd is None else {"dir_fd": directory_fd}
        replacement = real_open(path, flags, **options)
        if replacement != descriptor:
            os.dup2(replacement, descriptor)
            real_close(replacement)
        metadata = os.fstat(descriptor)
        if (metadata.st_dev, metadata.st_ino) != target_identity:
            raise SystemExit(2)
        raise OSError(f"indeterminate {role} close")

evaluator.os.open = recording_open
evaluator.os.close = close_then_reuse
try:
    try:
        evaluator.persist_result_pair(
            destinations,
            {"forward": [], "lifecycle": []},
            {"forward": [], "lifecycle": []},
            authority=lease.authority(),
        )
    except BaseException as error:
        if not sharding.is_indeterminate_descriptor_close(error):
            raise SystemExit(3)
    else:
        raise SystemExit(4)
    if target is None or target_close_calls != 1:
        print(
            f"role={role} target={target} target_close_calls={target_close_calls} calls={calls}",
            file=sys.stderr,
        )
        raise SystemExit(5)
    os.fstat(target)
    first_calls = tuple(calls)
    try:
        sharding.ResultWriterLease.acquire(
            repository,
            role="serial-coordinator",
            run_kind="formal",
            run_lease=None,
        )
    except RuntimeError:
        pass
    else:
        raise SystemExit(6)
    if tuple(calls) != first_calls:
        raise SystemExit(7)
finally:
    try:
        lease.close()
    except RuntimeError:
        pass
    evaluator.os.open = real_open
    evaluator.os.close = real_close
    if target is not None:
        real_close(target)
'''
        roles = (
            "result-parent",
            "generation-directory",
            "generation-staging",
            "pointer-staging",
            "pointer-read",
            "generation-forward-read",
            "generation-lifecycle-read",
        )
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve(strict=True)
            for role in roles:
                with self.subTest(role=role):
                    repository = root / role
                    completed = subprocess.run(
                        [sys.executable, "-c", program, str(repository), role],
                        cwd=evidence_root,
                        env={**os.environ, "PYTHONPATH": str(evidence_root)},
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=15,
                    )
                    self.assertEqual(0, completed.returncode, completed.stderr)

    def test_result_commit_pointer_hides_all_precommit_crashes(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            root = repository / "results"
            forward = root / "forward.json"
            lifecycle = root / "lifecycle.json"
            destinations = {"forward": forward, "lifecycle": lifecycle}
            manifests, old, new = self._result_fixture()
            with self._result_writer_authority(repository) as authority:
                pointer = persist_result_pair(
                    destinations, old, manifests, authority=authority
                )
                self.assertEqual(
                    old, resolve_committed_result_pair(pointer, manifests)
                )
            for crash_at in (
                "after_forward_write", "after_forward_rename",
                "after_lifecycle_write", "after_lifecycle_rename",
                "after_pointer_write",
            ):
                with self.subTest(crash_at=crash_at):
                    with self._result_writer_authority(repository) as authority:
                        with self.assertRaises(InjectedResultCrash):
                            persist_result_pair(
                                destinations,
                                new,
                                manifests,
                                authority=authority,
                                crash_at=crash_at,
                            )
                        self.assertEqual(
                            old,
                            resolve_committed_result_pair(pointer, manifests),
                        )
            with self._result_writer_authority(repository) as authority:
                with self.assertRaises(InjectedResultCrash):
                    persist_result_pair(
                        destinations,
                        new,
                        manifests,
                        authority=authority,
                        crash_at="after_pointer_rename",
                    )
                self.assertEqual(
                    new, resolve_committed_result_pair(pointer, manifests)
                )
            committed = json.loads(pointer.read_text(encoding="utf-8"))
            forward_generation = root / committed["files"]["forward"]["path"]
            forward_generation.write_bytes(forward_generation.read_bytes() + b" ")
            with self.assertRaisesRegex(AssertionError, "generation hash mismatch"):
                resolve_committed_result_pair(pointer, manifests)

    def test_result_store_rejects_symlink_roots_and_pointer(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(root)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            external = root / "external"
            external.mkdir()
            manifests, old, _ = self._result_fixture()

            linked_parent = root / "linked-parent"
            linked_parent.symlink_to(external, target_is_directory=True)
            destinations = {
                "forward": linked_parent / "forward.json",
                "lifecycle": linked_parent / "lifecycle.json",
            }
            with self._result_writer_authority(root) as authority:
                with self.assertRaisesRegex(
                    (AssertionError, OSError, ValueError), "symlink|directory"
                ):
                    persist_result_pair(
                        destinations, old, manifests, authority=authority
                    )
            self.assertEqual([], list(external.iterdir()))

            safe = root / "safe"
            safe.mkdir()
            generation_link = safe / ".observing_workflows_result_generations"
            generation_link.symlink_to(external, target_is_directory=True)
            destinations = {
                "forward": safe / "forward.json",
                "lifecycle": safe / "lifecycle.json",
            }
            with self._result_writer_authority(root) as authority:
                with self.assertRaisesRegex(
                    (AssertionError, OSError, ValueError), "symlink|directory"
                ):
                    persist_result_pair(
                        destinations, old, manifests, authority=authority
                    )
            self.assertEqual([], list(external.iterdir()))

            generation_link.unlink()
            with self._result_writer_authority(root) as authority:
                pointer = persist_result_pair(
                    destinations, old, manifests, authority=authority
                )
                self.assertEqual(
                    old, resolve_committed_result_pair(pointer, manifests)
                )
            real_pointer = safe / "real-pointer.json"
            pointer.rename(real_pointer)
            pointer.symlink_to(real_pointer)
            with self.assertRaisesRegex((AssertionError, OSError), "symlink|regular"):
                resolve_committed_result_pair(pointer, manifests)

    @staticmethod
    @contextmanager
    def _result_writer_authority(repository):
        lease = sharding.ResultWriterLease.acquire(
            repository,
            role="serial-coordinator",
            run_kind="formal",
            run_lease=None,
        )
        try:
            yield lease.authority()
        finally:
            lease.close()

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

    def test_direct_main_rejects_repository_alias_instead_of_canonicalizing_it(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = root / "repository"
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            alias = root / "repository-alias"
            alias.symlink_to(repository, target_is_directory=True)
            with mock.patch.object(
                sys,
                "argv",
                ["run_observing_workflows_task9_eval.py", "--repository-root", str(alias)],
            ), mock.patch.object(
                task9_eval,
                "run_suite",
                side_effect=AssertionError("invalid alias reached the formal suite"),
            ) as run_suite_call, mock.patch.object(
                sys, "stderr", new_callable=io.StringIO
            ), self.assertRaises(SystemExit) as raised:
                task9_eval.main()
            self.assertEqual(2, raised.exception.code)
            run_suite_call.assert_not_called()

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

    def test_marketplace_formal_main_passes_exact_git_root_and_serial_role(self):
        repository = Path(__file__).resolve().parents[1]
        runner = repository / (
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py"
        )
        namespace = runpy.run_path(str(runner))
        runtime_globals = namespace["main"].__globals__
        calls = []
        exact_root = repository.parent

        def fake_suite(*args, **kwargs):
            calls.append((args, kwargs))
            return [], []

        with mock.patch.dict(
            runtime_globals,
            {
                "validate_marketplace_manifest_hashes": lambda: None,
                "exact_git_repository_root": lambda start: exact_root,
                "MarketplaceRuntimeFactory": lambda: mock.sentinel.runtime_factory,
                "run_suite": fake_suite,
            },
        ), mock.patch.object(
            sys, "argv", [str(runner)]
        ), mock.patch.object(sys, "stdout", new_callable=io.StringIO):
            self.assertEqual(0, namespace["main"]())

        self.assertEqual(1, len(calls))
        args, kwargs = calls[0]
        self.assertEqual((runtime_globals["REPOSITORY_ROOT"],), args)
        self.assertEqual(exact_root, kwargs["repository_root"])
        self.assertEqual("serial-coordinator", kwargs["coordinator_role"])
        self.assertEqual(runtime_globals["MANIFEST_PATHS"], kwargs["manifest_paths"])
        self.assertEqual(runtime_globals["RESULT_PATHS"], kwargs["result_destinations"])

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
