#!/usr/bin/env python3
"""Run the frozen Observation Records v2 Task 9 evaluations in isolation."""

from __future__ import annotations

import argparse
import base64
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import secrets
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Literal, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_VERIFIED_SOURCE_CAPABILITY = sys.modules.get(
    "_workflow_observatory_verified_sources"
)
if _VERIFIED_SOURCE_CAPABILITY is None and str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from tests.observing_workflows_eval_harness import (
    PayloadAudit,
    _parse_frontmatter,
    after_draft_run,
    after_single_file_mutation_without_run,
    assert_production_unchanged,
    build_fixture,
    build_payload_audit,
    inspect_store,
    normalize_records,
    release_gate,
    snapshot_production,
)
from tests.run_observing_workflows_eval import (
    score_results,
    validate_decision_recording,
    validate_id_set,
    validate_lifecycle_results,
    validate_manifest_schema,
    validate_result_schema,
)
from scripts import workflow_eval_sharding as sharding_core
from scripts.workflow_eval_sharding import (
    ResolvedTransportConfig,
    ResultWriterAuthority,
    ResultWriterLease,
    TokenUsage,
    install_legacy_case_auth as install_case_auth,
    prepare_legacy_auth_bootstrap as prepare_auth_bootstrap,
    resolve_transport_config,
    transport_config_bytes,
    verify_codex_executable,
)


CENTRAL_CLI = REPOSITORY_ROOT / "wiki_cli.py"
CENTRAL_COMMAND = (
    'python3 "${LLMWIKI_ROOT}/wiki_cli.py" '
    'observe --wiki-root "${LLMWIKI_ROOT}"'
)
EVALUATOR_DEVELOPER_INSTRUCTIONS = (
    "You are a freshly dispatched evaluator subagent operating only in the supplied "
    "isolated fixture. Treat the user turn as the complete task authorization, execute it "
    "directly, and follow matching installed skills. Do not inspect or modify the production "
    "LLM Wiki. Do not ask for expected evaluation outcomes."
)
EXEC_TURN_TIMEOUT_SECONDS = 20 * 60
APP_SERVER_TURN_TIMEOUT_SECONDS = 10 * 60
GATE_TIMEOUT_SECONDS = 5 * 60
EXEC_EVENT_SUMMARY_LABEL_LIMIT = 16
EXEC_EVENT_SUMMARY_COUNT_LIMIT = 6
EXEC_EVENT_SUMMARY_TAIL_LIMIT = 4
FROZEN_MANIFEST_HASHES = {
    "forward": "f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2",
    "lifecycle": "d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b",
}
FROZEN_MANIFEST_IDS = {
    "forward": (
        "multi-file-feature", "tested-bugfix", "reviewed-refactor",
        "multi-file-docs", "wiki-compile", "durable-query", "inbox-processing",
        "late-trigger", "scope-supersession", "parent-managed-subagent", "chat",
        "read-only-search", "answer-only", "plan-only", "single-file-typo",
        "single-file-copy", "status-question", "review-only",
        "worker-with-parent-marker", "ambiguous-default-no-trigger",
    ),
    "lifecycle": (
        "planned-success", "late-success", "scope-supersession",
        "parent-managed-subagent", "task-failure", "central-cli-unavailable",
        "complete-eval-override", "incomplete-eval-override",
    ),
}


def _sensitive_text_summary(value: str) -> dict[str, object]:
    return {
        "chars": len(value),
        "sha256": hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest(),
    }


def _bounded_sensitive_summaries(values, limit: int = 3) -> list[dict[str, object]]:
    return [_sensitive_text_summary(value) for value in list(values)[-limit:]]


def _safe_protocol_label(value) -> str:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_./-]{1,80}", value):
        return value
    return "redacted"


@dataclass(frozen=True)
class RuntimePayloadAudit:
    root: Path
    payload_dir: Path
    log_path: Path
    wrapper_path: Path


@dataclass(frozen=True)
class CaseRuntime:
    store_root: Path
    audit: PayloadAudit | RuntimePayloadAudit
    environment: dict[str, str]
    writable_roots: tuple[Path, ...]
    transport_config: ResolvedTransportConfig
    selected_command: str = CENTRAL_COMMAND
    disabled_skill_paths: tuple[Path, ...] = ()
    integrity_command: tuple[str, ...] | None = None
    audited_wrapper_path: Path | None = None
    audited_wrapper_content: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.writable_roots, tuple) or not all(
            isinstance(path, Path) for path in self.writable_roots
        ):
            raise TypeError("writable_roots must be an immutable tuple of Paths")


TransportName = Literal["exec", "app-server"]
CaseEvent = Literal["process-started", "model-started", "process-stopped"]
CaseEventSink = Callable[[CaseEvent, int | None, int | None], None]


class CaseCleanupFailure(RuntimeError):
    """A case transport could not prove that its transient resources were cleaned."""


class ProcessSurvivalCleanupFailure(CaseCleanupFailure):
    """A transport could not prove that its child process was reaped."""


class CaseInfrastructureFailure(RuntimeError):
    """An isolated case could not establish its required runtime infrastructure."""


class _CaseFixtureSetupFailureGroup(ExceptionGroup):
    """A sanitized fixture setup failure plus its public gate cleanup failure."""


class CaseTransportFailure(RuntimeError):
    """A model transport became ambiguous after its start boundary."""

    def __init__(
        self,
        message: str,
        *,
        model_started: bool,
        classification: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.model_started = model_started
        self.classification = classification or (
            "post-start-transport" if model_started else "pre-model-infrastructure"
        )
        self.retryable = (not model_started) if retryable is None else retryable


class CaseProtocolFailure(CaseTransportFailure):
    """A transport returned malformed or internally inconsistent protocol data."""

    def __init__(self, message: str, *, model_started: bool = True) -> None:
        super().__init__(
            message,
            model_started=model_started,
            classification="protocol",
            retryable=False,
        )


class CaseModelFailure(CaseTransportFailure):
    """The model turn reached an explicit failed terminal state."""

    def __init__(self, message: str) -> None:
        super().__init__(
            message,
            model_started=True,
            classification="model",
            retryable=False,
        )


class DiscoverySweepAbort(RuntimeError):
    """A discovery sweep encountered a suite-integrity failure and must stop."""


ZERO_TOKEN_USAGE = TokenUsage(0, 0, 0, 0, 0)
MAX_TOKEN_COUNT = 2**63 - 1


@dataclass(frozen=True)
class CaseExecution:
    terminal_status: str
    final_text: str
    command_executions: tuple[str, ...]
    observation_command_diagnostics: tuple[dict[str, object], ...]
    usage: TokenUsage


def _token_count(payload: dict, key: str, *, required: bool) -> int:
    if key not in payload:
        if required:
            raise CaseProtocolFailure("token usage is missing a required count")
        return 0
    value = payload[key]
    if type(value) is not int or not 0 <= value <= MAX_TOKEN_COUNT:
        raise CaseProtocolFailure("token usage contains an invalid count")
    return value


def _normalize_token_usage(payload: object, *, app_server: bool) -> TokenUsage:
    if not isinstance(payload, dict):
        raise CaseProtocolFailure("token usage is not an object")
    if app_server:
        names = {
            "input": "inputTokens",
            "cached": "cachedInputTokens",
            "output": "outputTokens",
            "reasoning": "reasoningOutputTokens",
            "total": "totalTokens",
        }
    else:
        names = {
            "input": "input_tokens",
            "cached": "cached_input_tokens",
            "output": "output_tokens",
            "reasoning": "reasoning_output_tokens",
            "total": "total_tokens",
        }
    if not set(payload).issubset(set(names.values())):
        raise CaseProtocolFailure("token usage contains unknown fields")
    input_tokens = _token_count(payload, names["input"], required=True)
    output_tokens = _token_count(payload, names["output"], required=True)
    cached_tokens = _token_count(payload, names["cached"], required=False)
    reasoning_tokens = _token_count(payload, names["reasoning"], required=False)
    if cached_tokens > input_tokens or reasoning_tokens > output_tokens:
        raise CaseProtocolFailure("token usage contains inconsistent sub-counts")
    computed_total = input_tokens + output_tokens
    if computed_total > MAX_TOKEN_COUNT:
        raise CaseProtocolFailure("token usage total overflows")
    if names["total"] in payload:
        total_tokens = _token_count(payload, names["total"], required=True)
        if total_tokens != computed_total:
            raise CaseProtocolFailure("token usage total is inconsistent")
    else:
        total_tokens = computed_total
    return TokenUsage(
        input_tokens,
        cached_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
    )


def _emit_case_event(
    event_sink: CaseEventSink | None,
    event: CaseEvent,
    pid: int | None,
    process_group_id: int | None,
) -> None:
    if event_sink is not None:
        if (
            type(pid) is not int
            or pid <= 0
            or type(process_group_id) is not int
            or process_group_id <= 0
        ):
            raise CaseTransportFailure(
                "production event process identity is invalid",
                model_started=event == "model-started",
                classification="event-sink",
                retryable=False,
            )
        event_sink(event, pid, process_group_id)


def _process_group_id(process: subprocess.Popen) -> int:
    pid = getattr(process, "pid", None)
    if type(pid) is not int or pid <= 0:
        raise CaseInfrastructureFailure("transport process PID is invalid")
    group_verified = True
    try:
        process_group_id = os.getpgid(pid)
    except ProcessLookupError:
        # Injectable test processes may never have existed in the OS table. The
        # real Popen path still owns the start_new_session PID/PGID contract and
        # must probe that group even if the leader exited before this lookup.
        process_group_id = pid
        group_verified = isinstance(process, subprocess.Popen)
    except OSError:
        raise ProcessSurvivalCleanupFailure(
            "transport process group state is indeterminate"
        ) from None
    if type(process_group_id) is not int or process_group_id <= 0:
        raise CaseInfrastructureFailure("transport process group is invalid")
    process._workflow_eval_group_verified = group_verified
    return process_group_id


def _process_group_exists(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        raise ProcessSurvivalCleanupFailure(
            "transport process group ownership is indeterminate"
        ) from None
    except OSError:
        raise ProcessSurvivalCleanupFailure(
            "transport process group state is indeterminate"
        ) from None
    return True


def _process_has_exited(process: subprocess.Popen) -> bool:
    try:
        return process.poll() is not None
    except BaseException:
        raise ProcessSurvivalCleanupFailure(
            "transport process state is indeterminate"
        ) from None


def stop_process_group(
    process: subprocess.Popen,
    *,
    readers: Sequence[threading.Thread],
    terminate_timeout: float = 5.0,
    kill_timeout: float = 5.0,
) -> None:
    process_group_id = getattr(process, "process_group_id", None)
    if type(process_group_id) is not int or process_group_id <= 0:
        process_group_id = _process_group_id(process)
    failures: list[str] = []
    group_verified = bool(
        getattr(process, "_workflow_eval_group_verified", True)
    )

    def group_is_alive() -> bool:
        return group_verified and _process_group_exists(process_group_id)

    group_exists = group_is_alive()
    if group_exists:
        try:
            os.killpg(process_group_id, signal.SIGTERM)
        except ProcessLookupError:
            group_exists = False
        except OSError:
            failures.append("terminate")
    elif not _process_has_exited(process):
        # Unit-test process doubles have no OS process group. Production never
        # enters this fallback because start_new_session creates the group.
        try:
            process.terminate()
        except BaseException:
            failures.append("terminate")

    try:
        process.wait(timeout=terminate_timeout)
    except subprocess.TimeoutExpired:
        pass
    except BaseException:
        failures.append("wait")

    deadline = time.monotonic() + terminate_timeout
    while group_exists and time.monotonic() < deadline:
        if not group_is_alive():
            group_exists = False
            break
        time.sleep(0.01)

    if group_exists or not _process_has_exited(process):
        if group_exists:
            try:
                os.killpg(process_group_id, signal.SIGKILL)
            except ProcessLookupError:
                group_exists = False
            except OSError:
                failures.append("kill")
        else:
            try:
                process.kill()
            except BaseException:
                failures.append("kill")
        try:
            process.wait(timeout=kill_timeout)
        except BaseException:
            failures.append("kill-wait")

    deadline = time.monotonic() + kill_timeout
    while group_is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    if group_is_alive():
        failures.append("group-survived")

    for reader in readers:
        try:
            reader.join(timeout=kill_timeout)
        except BaseException:
            failures.append("reader-join")
            continue
        if reader.is_alive():
            failures.append("reader-survived")

    if group_is_alive():
        failures.append("group-survived-after-readers")

    if failures or not _process_has_exited(process):
        raise ProcessSurvivalCleanupFailure(
            "transport process-group cleanup failed: "
            + ",".join(sorted(set(failures or ["process-survived"])))
        )


def select_case_transport(case: dict) -> TransportName:
    turn_count = len(case.get("turns", ()))
    if turn_count == 1:
        return "exec"
    if turn_count == 2:
        return "app-server"
    raise ValueError(f"unsupported turn count: {turn_count}")


ATTEMPT_AUDIT_WRAPPER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

target = os.environ["OBSERVATION_AUDIT_TARGET_CLI"]
log_path = Path(os.environ["OBSERVATION_AUDIT_LOG"])
argv = sys.argv[1:]
payloads = []
errors = []
target_exit_code = None
target_error = None
for argv_index, flag in enumerate(argv):
    if flag not in ("--scope-from-file", "--from-file"):
        continue
    value = argv[argv_index + 1] if argv_index + 1 < len(argv) else None
    row = {
        "flag": flag,
        "argv_index": argv_index,
        "path": value,
        "device": None,
        "inode": None,
        "mode": None,
        "regular": None,
        "text": None,
        "error": None,
    }
    if value is None:
        row["error"] = "could not read payload: flag has no following path"
    else:
        try:
            payload = Path(value)
            details = os.stat(payload, follow_symlinks=False)
            row.update({
                "device": details.st_dev,
                "inode": details.st_ino,
                "mode": stat.S_IMODE(details.st_mode),
                "regular": stat.S_ISREG(details.st_mode),
                "text": payload.read_text(encoding="utf-8"),
            })
        except Exception as error:
            row["error"] = f"could not read payload: {type(error).__name__}: {error}"
    payloads.append(row)
try:
    try:
        completed = subprocess.run([sys.executable, target, *argv], check=False)
        target_exit_code = completed.returncode
    except OSError as error:
        target_error = f"{type(error).__name__}: {error}"
        target_exit_code = 127
finally:
    ledger_row = {
        "argv": argv,
        "payloads": payloads,
        "errors": errors,
        "target_exit_code": target_exit_code,
        "target_error": target_error,
    }
    try:
        descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(ledger_row, sort_keys=True) + "\n")
    except OSError:
        pass
raise SystemExit(target_exit_code if isinstance(target_exit_code, int) else 127)
'''


EMBEDDED_AUDIT_WRAPPER_TEMPLATE = r'''#!/usr/bin/env python3
import base64
import json
import os
from pathlib import Path
import stat
import sys

argv = sys.argv[1:]
log_path = Path(os.environ["OBSERVATION_AUDIT_LOG"])
payloads = []
errors = []
target_exit_code = None
target_error = None
for argv_index, flag in enumerate(argv):
    if flag not in ("--scope-from-file", "--from-file"):
        continue
    value = argv[argv_index + 1] if argv_index + 1 < len(argv) else None
    row = {
        "flag": flag, "argv_index": argv_index, "path": value,
        "device": None, "inode": None, "mode": None, "regular": None,
        "text": None, "error": None,
    }
    if value is None:
        row["error"] = "could not read payload: flag has no following path"
    else:
        try:
            payload = Path(value)
            details = os.stat(payload, follow_symlinks=False)
            row.update({
                "device": details.st_dev, "inode": details.st_ino,
                "mode": stat.S_IMODE(details.st_mode),
                "regular": stat.S_ISREG(details.st_mode),
                "text": payload.read_text(encoding="utf-8"),
            })
        except Exception as error:
            row["error"] = f"could not read payload: {type(error).__name__}: {error}"
    payloads.append(row)
try:
    if __FORCE_START_UNAVAILABLE__ and argv[:1] == ["start"]:
        print("workflow observer io error: configured CLI unavailable", file=sys.stderr)
        target_exit_code = 1
    else:
        source = base64.b64decode("__EMBEDDED_CLI_BASE64__")
        namespace = {
            "__name__": "_embedded_workflow_observer_cli",
            "__file__": __file__,
            "__package__": None,
        }
        exec(compile(source, __file__, "exec"), namespace)
        try:
            result = namespace["main"](argv)
            target_exit_code = result if isinstance(result, int) else 0
        except SystemExit as error:
            target_exit_code = error.code if isinstance(error.code, int) else 1
except BaseException as error:
    target_error = f"{type(error).__name__}: {error}"
    target_exit_code = 1
    raise
finally:
    ledger_row = {
        "argv": argv, "payloads": payloads, "errors": errors,
        "target_exit_code": target_exit_code, "target_error": target_error,
    }
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(ledger_row, sort_keys=True) + "\n")
raise SystemExit(target_exit_code if isinstance(target_exit_code, int) else 1)
'''


def build_embedded_audit_wrapper(
    cli_source: bytes, *, force_start_unavailable: bool = False
) -> str:
    if not isinstance(cli_source, bytes) or not cli_source:
        raise ValueError("embedded CLI source must be nonempty bytes")
    encoded = base64.b64encode(cli_source).decode("ascii")
    return (
        EMBEDDED_AUDIT_WRAPPER_TEMPLATE
        .replace("__EMBEDDED_CLI_BASE64__", encoded)
        .replace("__FORCE_START_UNAVAILABLE__", repr(force_start_unavailable))
    )


def _attempt_kind(argv: list[str]) -> str | None:
    for value in argv:
        if value in {"start", "finish", "report", "validate", "integrity"}:
            return value
    return None


def _completion_scalars(text: str) -> list[str]:
    scalars = []
    in_metrics = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "```yaml":
            in_metrics = True
            continue
        if stripped == "```":
            in_metrics = False
            continue
        if stripped.startswith("- "):
            value = stripped[2:].split(":", 1)[-1].strip()
            if value:
                scalars.append(value)
        elif in_metrics and ":" in stripped:
            value = stripped.split(":", 1)[1].strip()
            if value:
                scalars.append(value)
    return scalars


def load_observation_attempt_ledger(
    audit: PayloadAudit | RuntimePayloadAudit,
) -> list[dict]:
    if not audit.log_path.exists():
        return []
    attempts = []
    for line_number, line in enumerate(
        audit.log_path.read_text(encoding="utf-8").splitlines(), 1
    ):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise AssertionError(
                f"attempt ledger line {line_number} is invalid JSON"
            ) from error
        if not isinstance(row, dict):
            raise AssertionError(f"attempt ledger line {line_number} is not an object")
        attempts.append(row)
    return attempts


def assert_observation_attempt_ledger(
    attempts: list[dict],
    command_executions: Sequence[str],
    expected_start_calls: int,
    expected_finish_calls: int,
    audit: PayloadAudit | RuntimePayloadAudit | None = None,
) -> None:
    errors = []
    seen_paths = set()
    seen_inodes = set()
    start_seen = False
    start_calls = 0
    finish_calls = 0
    for index, attempt in enumerate(attempts, 1):
        expected_attempt_fields = {
            "argv", "payloads", "errors", "target_exit_code", "target_error"
        }
        if not isinstance(attempt, dict) or set(attempt) != expected_attempt_fields:
            errors.append(f"attempt ledger row {index} has invalid fields")
            continue
        argv = attempt["argv"]
        payloads = attempt["payloads"]
        if not isinstance(argv, list) or not all(isinstance(v, str) for v in argv):
            errors.append(f"attempt ledger row {index} has invalid argv")
            continue
        if not isinstance(payloads, list):
            errors.append(f"attempt ledger row {index} has invalid payloads")
            continue
        if not isinstance(attempt["errors"], list):
            errors.append(f"attempt ledger row {index} has invalid errors")
        if not (
            isinstance(attempt["target_exit_code"], int)
            and not isinstance(attempt["target_exit_code"], bool)
        ):
            errors.append(f"attempt ledger row {index} has invalid target exit code")
        kind = _attempt_kind(argv)
        if start_seen and any(value in {"--help", "-h"} for value in argv):
            errors.append("help after start is forbidden")
        if start_seen and kind in {"report", "validate", "integrity"}:
            errors.append("draft inspection after start is forbidden")
        if kind == "start":
            start_calls += 1
            start_seen = True
        elif kind == "finish":
            finish_calls += 1
        flag_occurrences = [
            (position, value)
            for position, value in enumerate(argv)
            if value in {"--scope-from-file", "--from-file"}
        ]
        for flag in ("--scope-from-file", "--from-file"):
            count = sum(value == flag for _, value in flag_occurrences)
            if count > 1:
                errors.append(f"attempt ledger row {index} repeats {flag}")
        if len(payloads) != len(flag_occurrences):
            errors.append(f"attempt ledger row {index} payload occurrence count mismatch")
        for occurrence, payload in zip(flag_occurrences, payloads):
            required = {
                "flag", "argv_index", "path", "device", "inode", "mode",
                "regular", "text", "error"
            }
            if not isinstance(payload, dict) or set(payload) != required:
                errors.append(f"attempt ledger row {index} has invalid payload fields")
                continue
            argv_index, flag = occurrence
            selected = argv[argv_index + 1] if argv_index + 1 < len(argv) else None
            if (
                payload["flag"] != flag
                or payload["argv_index"] != argv_index
                or payload["path"] != selected
            ):
                errors.append(
                    f"attempt ledger row {index} payload does not match argv occurrence"
                )
            if payload["error"] is not None:
                errors.append(f"attempt ledger row {index} payload capture error")
                continue
            if flag == "--from-file":
                for scalar in _completion_scalars(payload["text"] or ""):
                    if len(scalar) > 200:
                        errors.append("completion scalar exceeds 200 Unicode code points")
                        break
            path = payload["path"]
            identity = (payload["device"], payload["inode"])
            if path in seen_paths:
                errors.append(f"attempt ledger row {index} reuses a payload path")
            if identity in seen_inodes:
                errors.append(f"attempt ledger row {index} reuses a payload inode")
            seen_paths.add(path)
            seen_inodes.add(identity)
            if payload["regular"] is not True or payload["mode"] != 0o600:
                errors.append(f"attempt ledger row {index} payload is not mode-0600 regular")
            if os.path.lexists(path):
                errors.append(f"attempt ledger row {index} payload still exists")
            if audit is not None and not Path(path).resolve(strict=False).is_relative_to(
                audit.payload_dir.resolve(strict=True)
            ):
                errors.append(f"attempt ledger row {index} payload is outside case directory")
        scope_count = sum(flag == "--scope-from-file" for _, flag in flag_occurrences)
        completion_count = sum(flag == "--from-file" for _, flag in flag_occurrences)
        if kind == "start" and scope_count != 1:
            errors.append(f"attempt ledger row {index} start requires one Scope payload")
        if kind == "finish" and completion_count != 1:
            errors.append(f"attempt ledger row {index} finish requires one completion payload")
        if kind not in {"start", "finish"} and (scope_count or completion_count):
            errors.append(f"attempt ledger row {index} non-lifecycle call bears a payload")
    if start_calls != expected_start_calls:
        errors.append(
            f"start invocations: expected {expected_start_calls}, got {start_calls}"
        )
    if finish_calls != expected_finish_calls:
        errors.append(
            f"finish invocations: expected {expected_finish_calls}, got {finish_calls}"
        )
    if audit is not None:
        leftovers = sorted(path.name for path in audit.payload_dir.iterdir())
        if leftovers:
            errors.append("payload directory is not empty: " + ", ".join(leftovers))
    read_pattern = re.compile(
        r"(?:^|[;&|]\s*)(?:cat|find|grep|head|ls|rg|sed|tail)\b[^\n]*"
        r"(?:wiki/observations|workflow-observatory[^\n]*/store)",
    )
    if start_seen and any(read_pattern.search(command) for command in command_executions):
        errors.append("draft inspection after start is forbidden")
    if errors:
        raise AssertionError("; ".join(errors))


def build_shell_environment_override(environment: dict[str, str]) -> str:
    assignments = ", ".join(
        f"{key} = {json.dumps(value)}" for key, value in sorted(environment.items())
    )
    return f"shell_environment_policy.set={{ {assignments} }}"


def build_disabled_skills_override(paths: tuple[Path, ...]) -> str:
    entries = ", ".join(
        "{ path = " + json.dumps(str(path)) + ", enabled = false }"
        for path in paths
    )
    return f"skills.config=[{entries}]"


def build_codex_config_overrides(
    config: ResolvedTransportConfig,
    environment: dict[str, str],
    disabled_skill_paths: tuple[Path, ...],
) -> tuple[str, ...]:
    overrides = [
        build_shell_environment_override(environment),
        "model=" + json.dumps(config.model),
        "model_reasoning_effort=" + json.dumps(config.model_reasoning_effort),
        "approval_policy=" + json.dumps(config.approval_policy),
        "sandbox_mode=" + json.dumps(config.sandbox_mode),
        "sandbox_workspace_write.network_access="
        + ("true" if config.network_access else "false"),
        "web_search=" + json.dumps(config.web_search),
        "features.multi_agent=" + ("true" if config.multi_agent else "false"),
    ]
    if disabled_skill_paths:
        overrides.append(build_disabled_skills_override(disabled_skill_paths))
    return tuple(overrides)


def build_exec_command(
    config: ResolvedTransportConfig,
    cwd: Path,
    writable_roots: tuple[Path, ...],
    output_path: Path,
    overrides: tuple[str, ...],
) -> list[str]:
    command = [
        config.codex_executable_path,
        "exec", "--json", "--ephemeral", "--ignore-rules",
        "--ignore-user-config", "--strict-config",
        "--sandbox", "workspace-write", "-C", str(cwd),
        "-o", str(output_path),
    ]
    for override in overrides:
        command.extend(("-c", override))
    for root in writable_roots:
        command.extend(("--add-dir", str(root)))
    command.append("-")
    return command


def build_app_server_command(
    config: ResolvedTransportConfig, overrides: tuple[str, ...]
) -> list[str]:
    command = [
        config.codex_executable_path,
        "app-server",
        "--stdio",
        "--strict-config",
    ]
    for override in overrides:
        command.extend(("-c", override))
    return command


def _validated_transport_environment(runtime: CaseRuntime) -> dict[str, str]:
    raw_codex_home = runtime.environment.get("CODEX_HOME")
    if not isinstance(raw_codex_home, str) or not raw_codex_home:
        raise CaseInfrastructureFailure("isolated Codex home is missing")
    codex_home = Path(raw_codex_home)
    if not codex_home.is_absolute() or codex_home.is_symlink():
        raise CaseInfrastructureFailure("isolated Codex home is unsafe")
    try:
        metadata = codex_home.stat()
    except OSError:
        raise CaseInfrastructureFailure("isolated Codex home is unsafe") from None
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise CaseInfrastructureFailure("isolated Codex home is unsafe")
    if (
        (codex_home / "config.toml").exists()
        or (codex_home / "config.toml").is_symlink()
    ):
        raise CaseInfrastructureFailure("isolated Codex home contains config.toml")
    try:
        auth_metadata = (codex_home / "auth.json").lstat()
    except OSError:
        raise CaseInfrastructureFailure("isolated Codex auth is missing") from None
    if (
        not stat.S_ISREG(auth_metadata.st_mode)
        or stat.S_ISLNK(auth_metadata.st_mode)
        or stat.S_IMODE(auth_metadata.st_mode) != 0o600
    ):
        raise CaseInfrastructureFailure("isolated Codex auth is unsafe")
    return {**os.environ, **runtime.environment}


def parse_exec_jsonl(stdout: str, final_text: str) -> CaseExecution:
    active_commands: dict[str, str] = {}
    command_executions: list[str] = []
    observation_diagnostics: list[dict[str, object]] = []
    agent_messages: list[str] = []
    terminal_count = 0
    terminal_seen = False
    usage: TokenUsage | None = None
    lifecycle_event_types = {
        "thread.started", "turn.started", "item.started", "item.completed",
        "turn.completed", "turn.failed", "error",
    }
    for line_number, line in enumerate(stdout.splitlines(), 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(
                "malformed codex exec JSONL: "
                f"line={line_number}; summary={_sensitive_text_summary(line)!r}"
            ) from error
        if not isinstance(event, dict):
            raise ValueError(f"codex exec event is not an object: line={line_number}")
        event_type = event.get("type")
        if terminal_seen and event_type in lifecycle_event_types:
            raise RuntimeError("codex exec lifecycle event after terminal")
        if event_type in ("error", "turn.failed"):
            raise RuntimeError(
                "codex exec protocol failure: "
                f"type={_safe_protocol_label(event_type)}; "
                f"summary={_sensitive_text_summary(line)!r}"
            )
        if event_type == "turn.completed":
            terminal_count += 1
            if active_commands:
                bounded_ids = [
                    _safe_protocol_label(value)
                    for value in list(active_commands)[-6:]
                ]
                raise RuntimeError(
                    "codex exec terminal event has active commands: "
                    f"count={len(active_commands)}; ids={bounded_ids!r}"
                )
            if not agent_messages:
                raise RuntimeError("codex exec final agent message is missing")
            usage = _normalize_token_usage(
                event.get("usage"), app_server=False
            )
            terminal_seen = True
            continue
        if event_type not in ("item.started", "item.completed"):
            continue
        item = event.get("item")
        if not isinstance(item, dict):
            raise ValueError(f"codex exec item is not an object: line={line_number}")
        item_id = item.get("id")
        item_type = item.get("type")
        if event_type == "item.started" and item_type == "command_execution":
            if not isinstance(item_id, str) or not isinstance(item.get("command"), str):
                raise ValueError("codex exec command start is malformed")
            if item_id in active_commands:
                raise ValueError("codex exec command started twice")
            active_commands[item_id] = item["command"]
        elif event_type == "item.completed" and item_type == "command_execution":
            if not isinstance(item_id, str) or item_id not in active_commands:
                raise ValueError("codex exec command completed without a start")
            command = active_commands.pop(item_id)
            command_status = item.get("status")
            exit_code = item.get("exit_code")
            failed_command = (
                command_status == "failed"
                and type(exit_code) is int
                and exit_code != 0
            )
            if (
                item.get("command") != command
                or (command_status != "completed" and not failed_command)
            ):
                raise ValueError("codex exec command completion is inconsistent")
            command_executions.append(command)
            if "workflow_observer_cli.py" in command or " observe " in command:
                observation_diagnostics.append({
                    "command": command,
                    "exit_code": item.get("exit_code"),
                    "output": item.get("aggregated_output"),
                })
        elif event_type == "item.completed" and item_type == "agent_message":
            text = item.get("text")
            if not isinstance(text, str):
                raise ValueError("codex exec agent message is malformed")
            agent_messages.append(text)
    if active_commands:
        bounded_ids = [
            _safe_protocol_label(value)
            for value in list(active_commands)[-6:]
        ]
        raise RuntimeError(
            "codex exec ended with active commands: "
            f"count={len(active_commands)}; ids="
            f"{bounded_ids!r}"
        )
    if terminal_count != 1:
        raise RuntimeError(f"codex exec terminal events: expected 1, got {terminal_count}")
    if not agent_messages:
        raise RuntimeError("codex exec final agent message is missing")
    if usage is None:
        raise CaseProtocolFailure("codex exec token usage is missing")
    if agent_messages[-1].rstrip("\n") != final_text.rstrip("\n"):
        raise ValueError("codex exec final message mismatch")
    return CaseExecution(
        terminal_status="completed",
        final_text=agent_messages[-1],
        command_executions=tuple(command_executions),
        observation_command_diagnostics=tuple(observation_diagnostics),
        usage=usage,
    )


def _coerce_diagnostic_text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else ""


def _exec_summary_label(value) -> str:
    """Return a content-free protocol label bounded for failure diagnostics."""

    if value is None:
        return "none"
    label = _safe_protocol_label(value)
    if len(label) <= EXEC_EVENT_SUMMARY_LABEL_LIMIT:
        return label
    digest = hashlib.sha256(label.encode("ascii")).hexdigest()[:8]
    return f"sha256:{digest}"


def _exec_event_summary(value: str | bytes | None) -> dict[str, object]:
    counts: dict[str, int] = {}
    item_type_counts: dict[str, int] = {}
    last_event = "none"
    last_item_type = "none"
    last_item_status = "none"
    terminal_event_count = 0
    agent_message_count = 0
    active_commands: set[str] = set()
    tail: deque[tuple[str, str, str]] = deque(
        maxlen=EXEC_EVENT_SUMMARY_TAIL_LIMIT
    )
    for line in _coerce_diagnostic_text(value).splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        raw_event_type = event.get("type")
        event_type = _exec_summary_label(raw_event_type)
        last_event = event_type
        counts[event_type] = counts.get(event_type, 0) + 1
        item = event.get("item") if isinstance(event.get("item"), dict) else {}
        if item:
            last_item_type = _exec_summary_label(item.get("type"))
            last_item_status = _exec_summary_label(item.get("status"))
            item_type_counts[last_item_type] = (
                item_type_counts.get(last_item_type, 0) + 1
            )
        else:
            last_item_type = "none"
            last_item_status = "none"
        tail.append((event_type, last_item_type, last_item_status))
        if raw_event_type in {"turn.completed", "turn.failed", "error"}:
            terminal_event_count += 1
        if raw_event_type == "item.completed" and item.get("type") == "agent_message":
            agent_message_count += 1
        item_id = item.get("id")
        if isinstance(item_id, str) and item.get("type") == "command_execution":
            if raw_event_type == "item.started":
                active_commands.add(item_id)
            elif raw_event_type == "item.completed":
                active_commands.discard(item_id)
    return {
        "event_count": sum(counts.values()),
        "last_event": last_event,
        "active_command_count": len(active_commands),
        "event_types": dict(
            sorted(counts.items())[-EXEC_EVENT_SUMMARY_COUNT_LIMIT:]
        ),
        "terminal_event_count": terminal_event_count,
        "item_type_counts": dict(
            sorted(item_type_counts.items())[-EXEC_EVENT_SUMMARY_COUNT_LIMIT:]
        ),
        "agent_message_count": agent_message_count,
        "last_item_type": last_item_type,
        "last_item_status": last_item_status,
        "tail": list(tail),
    }


class ExecTransport:
    def __init__(
        self,
        cwd: Path,
        runtime: CaseRuntime,
        popen_factory=subprocess.Popen,
    ):
        self.cwd = cwd
        self.runtime = runtime
        self.popen_factory = popen_factory
        self.event_sink: CaseEventSink | None = None
        self.model_started = False

    def run(
        self, prompt: str, timeout: float = EXEC_TURN_TIMEOUT_SECONDS
    ) -> CaseExecution:
        output_path = self.runtime.audit.root / "exec-final-message.txt"
        overrides = build_codex_config_overrides(
            self.runtime.transport_config,
            self.runtime.environment,
            self.runtime.disabled_skill_paths,
        )
        command = build_exec_command(
            self.runtime.transport_config,
            self.cwd,
            self.runtime.writable_roots,
            output_path,
            overrides,
        )
        self._remove_output(output_path)
        process = None
        result: CaseExecution | None = None
        primary: BaseException | None = None
        cleanup_errors: list[BaseException] = []
        try:
            environment = _validated_transport_environment(self.runtime)
            verify_codex_executable(self.runtime.transport_config)
            try:
                process = self.popen_factory(
                    command,
                    cwd=self.cwd,
                    env=environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )
            except BaseException:
                raise CaseInfrastructureFailure(
                    "codex exec startup failed"
                ) from None
            process.process_group_id = _process_group_id(process)
            try:
                _emit_case_event(
                    self.event_sink,
                    "process-started",
                    process.pid,
                    process.process_group_id,
                )
            except BaseException:
                primary = CaseTransportFailure(
                    "codex exec process-start event sink failed",
                    model_started=False,
                    classification="event-sink",
                    retryable=False,
                )

            if primary is None:
                self.model_started = True
                try:
                    _emit_case_event(
                        self.event_sink,
                        "model-started",
                        process.pid,
                        process.process_group_id,
                    )
                except BaseException:
                    primary = CaseTransportFailure(
                        "codex exec model-start event sink failed",
                        model_started=True,
                        classification="event-sink",
                        retryable=False,
                    )

            stdout = ""
            stderr = ""
            if primary is None:
                try:
                    stdout, stderr = process.communicate(
                        input=prompt, timeout=timeout
                    )
                except subprocess.TimeoutExpired as error:
                    event_summary = _exec_event_summary(error.stdout)
                    stdout_summary = _bounded_sensitive_summaries(
                        (_coerce_diagnostic_text(error.stdout),), limit=1
                    )
                    stderr_summary = _bounded_sensitive_summaries(
                        (_coerce_diagnostic_text(error.stderr),), limit=1
                    )
                    primary = CaseTransportFailure(
                        "codex exec timeout: "
                        f"events={event_summary!r}; "
                        f"stdout={stdout_summary!r}; stderr={stderr_summary!r}",
                        model_started=True,
                    )
                except OSError:
                    primary = CaseTransportFailure(
                        "codex exec communication failed",
                        model_started=True,
                    )
                except BaseException as error:
                    primary = error

            if primary is None:
                stdout = _coerce_diagnostic_text(stdout)
                stderr = _coerce_diagnostic_text(stderr)
                if process.returncode != 0:
                    primary = CaseModelFailure(
                        "codex exec failed: "
                        f"returncode={process.returncode}; "
                        f"events={_exec_event_summary(stdout)!r}; "
                        f"stdout={_bounded_sensitive_summaries((stdout,), limit=1)!r}; "
                        f"stderr={_bounded_sensitive_summaries((stderr,), limit=1)!r}"
                    )
                elif not output_path.is_file():
                    primary = CaseProtocolFailure(
                        "codex exec final message is missing"
                    )
                else:
                    try:
                        result = parse_exec_jsonl(
                            stdout,
                            output_path.read_text(encoding="utf-8").rstrip("\n"),
                        )
                    except CaseTransportFailure as error:
                        primary = error
                    except (OSError, ValueError, RuntimeError):
                        primary = CaseProtocolFailure(
                            "codex exec protocol normalization failed"
                        )
        except BaseException as error:
            primary = error
        finally:
            if process is not None:
                try:
                    stop_process_group(process, readers=())
                except BaseException as error:
                    cleanup_errors.append(error)
                else:
                    try:
                        _emit_case_event(
                            self.event_sink,
                            "process-stopped",
                            process.pid,
                            process.process_group_id,
                        )
                    except BaseException:
                        cleanup_errors.append(
                            CaseTransportFailure(
                                "codex exec process-stop event sink failed",
                                model_started=self.model_started,
                                classification="event-sink",
                                retryable=False,
                            )
                        )
            try:
                self._remove_output(output_path)
            except BaseException as error:
                cleanup_errors.append(error)

        _raise_case_and_auth_cleanup_failures(primary, cleanup_errors)
        assert result is not None
        return result

    @staticmethod
    def _remove_output(output_path: Path) -> None:
        try:
            output_path.unlink(missing_ok=True)
        except OSError:
            raise CaseCleanupFailure(
                "codex exec output cleanup failed"
            ) from None


def inventory_external_skill_paths(
    *,
    home: Path | None = None,
    codex_home: Path | None = None,
    fixture_skill_paths: tuple[Path, ...] = (),
    extra_roots: tuple[Path, ...] = (),
) -> tuple[Path, ...]:
    """Inventory every discoverable external skill path that must be disabled."""

    home = Path.home() if home is None else Path(home)
    codex_home = (
        Path(os.environ.get("CODEX_HOME", home / ".codex"))
        if codex_home is None
        else Path(codex_home)
    )
    roots = (
        codex_home,
        home / ".agents/skills",
        Path("/etc/codex/skills"),
        *extra_roots,
    )
    fixture = {path.absolute() for path in fixture_skill_paths}
    found = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.glob("**/SKILL.md"):
            absolute = path.absolute()
            if absolute not in fixture:
                found.add(absolute)
    return tuple(sorted(found))


def run_configured_integrity(
    command: tuple[str, ...],
    environment: dict[str, str],
    *,
    expected_records: int,
) -> dict[str, int]:
    completed = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, **environment},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"integrity exit code {completed.returncode}: {completed.stderr.strip()}"
        )
    match = re.fullmatch(
        r"healthy records=([0-9]+) invalidated=([0-9]+)\n?", completed.stdout
    )
    if match is None or completed.stderr:
        raise AssertionError(
            "malformed integrity stdout/stderr: "
            f"stdout={completed.stdout!r}, stderr={completed.stderr!r}"
        )
    result = {"records": int(match.group(1)), "invalidated": int(match.group(2))}
    if result["records"] != expected_records:
        raise AssertionError(
            f"integrity records expected {expected_records}, got {result['records']}"
        )
    return result


def run_with_production_guard(operation, verify_production):
    """Always verify production, retaining both failures when both occur."""

    try:
        result = operation()
    except BaseException as operation_error:
        try:
            verify_production()
        except BaseException as production_error:
            group_type = (
                ExceptionGroup
                if isinstance(operation_error, Exception)
                and isinstance(production_error, Exception)
                else BaseExceptionGroup
            )
            raise group_type(
                "case failed and production fingerprint changed",
                [operation_error, production_error],
            )
        raise
    verify_production()
    return result


def decision_from_checkpoint(
    after_turn: int,
    records: list[dict],
    previous_run_count: int,
) -> dict:
    triggered = len(records) > previous_run_count
    newest = max(
        records,
        key=lambda row: (row.get("timestamp", ""), row.get("run_id", "")),
        default={},
    )
    return {
        "after_turn": after_turn,
        "triggered": triggered,
        "task_type": newest.get("task_type") if triggered else None,
        "workflow_variant": newest.get("workflow_variant") if triggered else None,
    }


def validate_forward_decisions(case: dict, decisions: list[dict]) -> None:
    """Score trigger timing and the final active scope's taxonomy."""

    expected_decisions = case["expected_decisions"]
    observed_triggered = [
        {"after_turn": row["after_turn"], "triggered": row["triggered"]}
        for row in decisions
    ]
    if observed_triggered != expected_decisions:
        raise AssertionError(
            f"{case['id']}: decision mismatch: expected "
            f"{expected_decisions}, got {observed_triggered}"
        )

    triggered_decisions = [row for row in decisions if row["triggered"]]
    if not triggered_decisions:
        return
    final_scope = triggered_decisions[-1]
    if (
        final_scope["task_type"] != case["task_type"]
        or final_scope["workflow_variant"] != case["workflow_variant"]
    ):
        raise AssertionError(f"{case['id']}: taxonomy mismatch: {final_scope}")


def recording_failure_disclosed(text: str) -> bool:
    lowered = text.lower()
    patterns = (
        r"\b(?:workflow observation |observation )?recording "
        r"(?:failed|failure|unavailable)\b",
        r"\b(?:workflow )?observation (?:is |was )?not recorded\b"
        r"(?!\s+(?:as|with)\b)",
        r"\b(?:workflow )?observation "
        r"(?:could not|couldn't|was unable to) be recorded\b"
        r"(?!\s+(?:as|with)\b)",
        r"\brecording command "
        r"(?:failed|was unavailable|could not be launched)\b",
        r"\b(?:observation|workflow record|recording) (?:cli )?"
        r"(?:is |was )?unavailable\b",
        r"\b(?:observation|workflow record) (?:start|finish) "
        r"(?:failed|failure)\b",
        r"\b(?:failed|unable) to (?:start|finish|create|write|record) "
        r"(?:the )?(?:workflow )?(?:observation|record)\b",
        r"\bcould not (?:start|finish|create|write|record) "
        r"(?:the )?(?:workflow )?(?:observation|record)\b",
    )
    return any(re.search(pattern, lowered) for pattern in patterns)


def validate_frozen_manifests(
    paths: dict[str, Path],
    manifests: dict[str, list[dict]],
    raw_bytes: dict[str, bytes] | None = None,
) -> None:
    raw_bytes = raw_bytes or {name: path.read_bytes() for name, path in paths.items()}
    for mode in ("forward", "lifecycle"):
        digest = hashlib.sha256(raw_bytes[mode]).hexdigest()
        if digest != FROZEN_MANIFEST_HASHES[mode]:
            raise AssertionError(f"{mode} manifest hash does not match frozen baseline")
        ids = tuple(
            row.get("id") if isinstance(row, dict) else None
            for row in manifests[mode]
        )
        if ids != FROZEN_MANIFEST_IDS[mode]:
            raise AssertionError(f"{mode} manifest IDs do not match frozen baseline")
        schema_mode = "forward" if mode == "forward" else "lifecycle"
        errors = validate_manifest_schema(schema_mode, manifests[mode])
        if errors:
            raise AssertionError(f"{mode} manifest schema: {'; '.join(errors)}")


class InjectedResultCrash(RuntimeError):
    """Test-only crash injection at a result commit protocol boundary."""


RESULT_COMMIT_FILENAME = "observing_workflows_results_commit.json"
RESULT_GENERATION_DIRECTORY = ".observing_workflows_result_generations"


@dataclass(frozen=True)
class _RepositoryDeltaSnapshot:
    repository_key: str
    fingerprints: tuple[tuple[str, str, int, int, str], ...]


@dataclass(frozen=True)
class _RetainedAuthoritativeFile:
    name: str
    identity: tuple[int, int]
    slot: sharding_core._DescriptorSlot


@dataclass(frozen=True)
class _RetainedResultReadback:
    results: dict[str, list[dict]]
    pointer_entry: _RetainedAuthoritativeFile
    generation_entries: dict[str, _RetainedAuthoritativeFile]

    def retirement_slots(self) -> tuple[sharding_core._DescriptorSlot, ...]:
        return (
            self.generation_entries["lifecycle"].slot,
            self.generation_entries["forward"].slot,
            self.pointer_entry.slot,
        )


class _RetainedCommittedResultPair:
    """Descriptor-backed committed rows retained until final acceptance gates."""

    def __init__(
        self,
        *,
        pointer: Path,
        results: dict[str, list[dict]],
        result_parent,
        generation_entry: sharding_core._RetainedDirectory,
        generation_names: dict[str, str],
        relative_parent: Path,
        pointer_entry: _RetainedAuthoritativeFile,
        generation_file_entries: dict[str, _RetainedAuthoritativeFile],
    ) -> None:
        self.pointer = pointer
        self.results = results
        self.result_parent = result_parent
        self.generation_entry = generation_entry
        self.generation_names = generation_names.copy()
        self.relative_parent = relative_parent
        self.pointer_entry = pointer_entry
        self.generation_file_entries = generation_file_entries.copy()
        self._closed = False

    def _validate_live(self) -> None:
        if self._closed:
            raise RuntimeError("committed result capability is closed")
        self.result_parent._validate_live()
        sharding_core._reconcile_named_descriptor_at(
            self.result_parent.descriptor,
            self.generation_entry.name,
            self.generation_entry.slot.descriptor,
            self.generation_entry.identity,
            label="result generation root",
            kind="directory",
            mode=0o700,
        )
        sharding_core._reconcile_named_descriptor_at(
            self.result_parent.descriptor,
            self.pointer_entry.name,
            self.pointer_entry.slot.descriptor,
            self.pointer_entry.identity,
            label="result commit pointer",
            kind="file",
            mode=0o600,
        )
        for mode in ("forward", "lifecycle"):
            entry = self.generation_file_entries[mode]
            sharding_core._reconcile_named_descriptor_at(
                self.generation_entry.slot.descriptor,
                entry.name,
                entry.slot.descriptor,
                entry.identity,
                label=f"{mode} result generation",
                kind="file",
                mode=0o600,
            )

    def close(self, primary: BaseException | None = None) -> None:
        if self._closed:
            if primary is not None:
                raise primary
            raise RuntimeError("committed result capability is closed")
        if primary is None:
            try:
                self._validate_live()
            except BaseException as error:
                primary = error
        self._closed = True
        self.result_parent._closed = True
        sharding_core._retire_task_descriptors(
            [
                self.generation_file_entries["lifecycle"].slot,
                self.generation_file_entries["forward"].slot,
                self.pointer_entry.slot,
                self.generation_entry.slot,
            ]
            + [
                entry.slot
                for entry in reversed(self.result_parent._retained)
            ],
            primary=primary,
            label="committed result verification or descriptor close failed",
        )


def _inject_result_crash(crash_at: str | None, point: str) -> None:
    if crash_at == point:
        raise InjectedResultCrash(point)


def _directory_flags() -> int:
    return os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


def _open_directory_path(path: Path, *, create: bool) -> int:
    candidate = Path(path).absolute()
    if not candidate.is_absolute() or any(part in {".", ".."} for part in candidate.parts):
        raise AssertionError("result directory path must be absolute and normalized")
    descriptor = os.open(candidate.anchor, _directory_flags())
    try:
        for component in candidate.parts[1:]:
            try:
                expected = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, mode=0o700, dir_fd=descriptor)
                expected = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(expected.st_mode):
                raise AssertionError("result directory path contains a symlink")
            if not stat.S_ISDIR(expected.st_mode):
                raise AssertionError("result path component is not a directory")
            child = os.open(component, _directory_flags(), dir_fd=descriptor)
            try:
                opened = os.fstat(child)
                current = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                identities = {
                    (expected.st_dev, expected.st_ino),
                    (opened.st_dev, opened.st_ino),
                    (current.st_dev, current.st_ino),
                }
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or not stat.S_ISDIR(current.st_mode)
                    or len(identities) != 1
                ):
                    raise AssertionError("result directory changed while opening")
            except Exception:
                os.close(child)
                raise
            os.close(descriptor)
            descriptor = child
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_child_directory(parent_fd: int, name: str, *, create: bool) -> int:
    try:
        expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        if not create:
            raise
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if stat.S_ISLNK(expected.st_mode):
        raise AssertionError("result generation root must not be a symlink")
    if not stat.S_ISDIR(expected.st_mode):
        raise AssertionError("result generation root must be a directory")
    child = os.open(name, _directory_flags(), dir_fd=parent_fd)
    try:
        opened = os.fstat(child)
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if len({
            (expected.st_dev, expected.st_ino),
            (opened.st_dev, opened.st_ino),
            (current.st_dev, current.st_ino),
        }) != 1:
            raise AssertionError("result generation root changed while opening")
    except Exception:
        os.close(child)
        raise
    return child


def _json_bytes(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def _stage_bytes_at(directory_fd: int, name: str, content: bytes) -> str:
    temporary = f".{name}.{secrets.token_hex(12)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(temporary, flags, 0o600, dir_fd=directory_fd)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return temporary


def _read_regular_at(directory_fd: int, name: str, label: str) -> bytes:
    expected = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if stat.S_ISLNK(expected.st_mode):
        raise AssertionError(f"{label} must not be a symlink")
    if not stat.S_ISREG(expected.st_mode):
        raise AssertionError(f"{label} must be a regular file")
    descriptor = os.open(
        name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=directory_fd
    )
    try:
        opened = os.fstat(descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if len({
            (expected.st_dev, expected.st_ino),
            (opened.st_dev, opened.st_ino),
            (current.st_dev, current.st_ino),
        }) != 1:
            raise AssertionError(f"{label} changed while opening")
        chunks = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        os.close(descriptor)


def _validate_result_pair(
    results: dict[str, list[dict]], manifests: dict[str, list[dict]]
) -> None:
    for mode in ("forward", "lifecycle"):
        errors = validate_result_schema(mode, results[mode], manifests[mode])
        errors.extend(validate_id_set(manifests[mode], results[mode]))
        if errors:
            raise AssertionError(f"{mode} result validation: {'; '.join(errors)}")


def _validate_committed_result_semantics(
    results: dict[str, list[dict]], manifests: dict[str, list[dict]]
) -> None:
    """Re-score only the rows decoded from the committed descriptor chain."""

    _validate_result_pair(results, manifests)
    trigger_hits, taxonomy_hits, forward_errors = score_results(
        manifests["forward"], results["forward"]
    )
    errors = list(forward_errors)
    errors.extend(
        validate_decision_recording(manifests["forward"], results["forward"])
    )
    errors.extend(
        validate_lifecycle_results(
            manifests["lifecycle"], results["lifecycle"]
        )
    )
    if trigger_hits != len(manifests["forward"]):
        errors.append("forward trigger score is incomplete")
    if taxonomy_hits != len(manifests["forward"]):
        errors.append("forward taxonomy score is incomplete")
    if errors:
        raise AssertionError(
            "committed result semantic validation: " + "; ".join(errors)
        )


def _stable_metadata(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _fingerprint_regular_file_at(
    directory_fd: int, name: str, expected: os.stat_result, relative: str
) -> tuple[str, str, int, int, str]:
    descriptor = os.open(
        name,
        os.O_RDONLY
        | sharding_core._required_os_flag("O_NOFOLLOW")
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_CLOEXEC", 0),
        dir_fd=directory_fd,
    )
    slot = sharding_core._DescriptorSlot(descriptor)
    primary: BaseException | None = None
    result: tuple[str, str, int, int, str] | None = None
    try:
        opened = os.fstat(slot.descriptor)
        if _stable_metadata(opened) != _stable_metadata(expected):
            raise AssertionError("repository file changed while opening")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(slot.descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(after.st_mode) or len(
            {
                _stable_metadata(expected),
                _stable_metadata(opened),
                _stable_metadata(after),
                _stable_metadata(current),
            }
        ) != 1:
            raise AssertionError("repository file changed while reading")
        result = (
            relative,
            "file",
            stat.S_IMODE(after.st_mode),
            after.st_size,
            digest.hexdigest(),
        )
    except BaseException as error:
        primary = error
    _retire_authoritative_file(
        slot,
        primary=primary,
        label="repository fingerprint read or close failed",
    )
    assert result is not None
    return result


def _fingerprint_repository_directory_at(
    directory_fd: int, prefix: tuple[str, ...] = ()
) -> tuple[tuple[str, str, int, int, str], ...]:
    before = os.fstat(directory_fd)
    if not stat.S_ISDIR(before.st_mode):
        raise AssertionError("repository fingerprint anchor is not a directory")
    rows: list[tuple[str, str, int, int, str]] = []
    for name in sorted(os.listdir(directory_fd)):
        if not name or name in {".", ".."} or "/" in name:
            raise AssertionError("repository entry name is invalid")
        relative_parts = (*prefix, name)
        relative = "/".join(relative_parts)
        expected = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        mode = stat.S_IMODE(expected.st_mode)
        if stat.S_ISREG(expected.st_mode):
            rows.append(
                _fingerprint_regular_file_at(
                    directory_fd, name, expected, relative
                )
            )
            continue
        if stat.S_ISLNK(expected.st_mode):
            target = os.readlink(name, dir_fd=directory_fd)
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if _stable_metadata(current) != _stable_metadata(expected):
                raise AssertionError("repository symlink changed while reading")
            rows.append(
                (
                    relative,
                    "symlink",
                    mode,
                    len(os.fsencode(target)),
                    hashlib.sha256(os.fsencode(target)).hexdigest(),
                )
            )
            continue
        if stat.S_ISDIR(expected.st_mode):
            descriptor = os.open(
                name,
                os.O_RDONLY
                | sharding_core._required_os_flag("O_NOFOLLOW")
                | sharding_core._required_os_flag("O_DIRECTORY")
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_fd,
            )
            slot = sharding_core._DescriptorSlot(descriptor)
            primary: BaseException | None = None
            children: tuple[tuple[str, str, int, int, str], ...] | None = None
            try:
                opened = os.fstat(slot.descriptor)
                current = os.stat(
                    name, dir_fd=directory_fd, follow_symlinks=False
                )
                if len(
                    {
                        _stable_metadata(expected),
                        _stable_metadata(opened),
                        _stable_metadata(current),
                    }
                ) != 1:
                    raise AssertionError(
                        "repository directory changed while opening"
                    )
                children = _fingerprint_repository_directory_at(
                    slot.descriptor, relative_parts
                )
                after = os.fstat(slot.descriptor)
                current = os.stat(
                    name, dir_fd=directory_fd, follow_symlinks=False
                )
                if len(
                    {
                        _stable_metadata(opened),
                        _stable_metadata(after),
                        _stable_metadata(current),
                    }
                ) != 1:
                    raise AssertionError(
                        "repository directory changed while scanning"
                    )
            except BaseException as error:
                primary = error
            _retire_authoritative_file(
                slot,
                primary=primary,
                label="repository directory scan or close failed",
            )
            assert children is not None
            rows.append((relative, "directory", mode, 0, ""))
            rows.extend(children)
            continue
        rows.append((relative, "special", mode, expected.st_size, ""))
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _stable_metadata(current) != _stable_metadata(expected):
            raise AssertionError("repository special entry changed while scanning")
    after = os.fstat(directory_fd)
    if _stable_metadata(after) != _stable_metadata(before):
        raise AssertionError("repository directory changed during fingerprint")
    return tuple(rows)


def _capture_repository_delta(
    lease: ResultWriterLease,
) -> _RepositoryDeltaSnapshot:
    if type(lease) is not ResultWriterLease:
        raise TypeError("repository delta requires exact ResultWriterLease")
    lease._validate_live()
    root_metadata = os.fstat(lease._repository_slot.descriptor)
    fingerprints = (
        (
            ".",
            "directory",
            stat.S_IMODE(root_metadata.st_mode),
            0,
            "",
        ),
        *_fingerprint_repository_directory_at(
            lease._repository_slot.descriptor
        ),
    )
    lease._validate_live()
    return _RepositoryDeltaSnapshot(
        repository_key=lease._repository_key,
        fingerprints=fingerprints,
    )


def _assert_exact_result_repository_delta(
    before: _RepositoryDeltaSnapshot,
    committed: _RetainedCommittedResultPair,
    lease: ResultWriterLease,
) -> None:
    lease._validate_live()
    committed._validate_live()
    if before.repository_key != lease._repository_key:
        raise AssertionError("repository delta lease binding changed")
    relative_parent = committed.relative_parent
    expected = {row[0]: row for row in before.fingerprints}
    current = Path()
    for component in relative_parent.parts:
        current /= component
        relative = current.as_posix()
        expected.setdefault(relative, (relative, "directory", 0o700, 0, ""))

    generation_root = relative_parent / RESULT_GENERATION_DIRECTORY
    generation_root_text = generation_root.as_posix()
    expected.setdefault(
        generation_root_text,
        (generation_root_text, "directory", 0o700, 0, ""),
    )
    contents = {
        mode: _json_bytes(committed.results[mode])
        for mode in ("forward", "lifecycle")
    }
    digests = {
        mode: hashlib.sha256(contents[mode]).hexdigest()
        for mode in ("forward", "lifecycle")
    }
    generation = hashlib.sha256(
        (digests["forward"] + digests["lifecycle"]).encode("ascii")
    ).hexdigest()[:24]
    expected_names = {
        mode: f"{generation}-{mode}.json"
        for mode in ("forward", "lifecycle")
    }
    if committed.generation_names != expected_names:
        raise AssertionError("committed result generation binding changed")
    for mode in ("forward", "lifecycle"):
        generation_path = (
            generation_root / committed.generation_names[mode]
        ).as_posix()
        expected[generation_path] = (
            generation_path,
            "file",
            0o600,
            len(contents[mode]),
            digests[mode],
        )
    pointer_value = {
        "schema_version": 1,
        "generation": generation,
        "files": {
            mode: {
                "path": (
                    f"{RESULT_GENERATION_DIRECTORY}/"
                    f"{committed.generation_names[mode]}"
                ),
                "sha256": digests[mode],
            }
            for mode in ("forward", "lifecycle")
        },
    }
    pointer_content = _json_bytes(pointer_value)
    pointer_path = (relative_parent / RESULT_COMMIT_FILENAME).as_posix()
    expected[pointer_path] = (
        pointer_path,
        "file",
        0o600,
        len(pointer_content),
        hashlib.sha256(pointer_content).hexdigest(),
    )
    after = _capture_repository_delta(lease)
    if tuple(sorted(expected.values())) != after.fingerprints:
        raise AssertionError("unexpected repository delta after result commit")
    committed._validate_live()
    lease._validate_live()


def _retire_authoritative_file(
    slot: sharding_core._DescriptorSlot,
    *,
    primary: BaseException | None,
    label: str,
) -> None:
    close_error = sharding_core._retire_descriptor_capability(slot)
    sharding_core._raise_task_failures(
        primary=primary,
        close_errors=[close_error] if close_error is not None else [],
        label=label,
    )


def _stage_authoritative_bytes_at(
    directory_fd: int, name: str, content: bytes
) -> str:
    temporary = f".{name}.{secrets.token_hex(12)}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    slot: sharding_core._DescriptorSlot | None = None
    primary: BaseException | None = None
    try:
        descriptor = os.open(temporary, flags, 0o600, dir_fd=directory_fd)
        slot = sharding_core._DescriptorSlot(descriptor)
        view = memoryview(content)
        while view:
            written = os.write(slot.descriptor, view)
            view = view[written:]
        os.fsync(slot.descriptor)
    except BaseException as error:
        primary = error
    if slot is None:
        assert primary is not None
        raise primary
    close_error = sharding_core._retire_descriptor_capability(slot)
    if close_error is not None:
        sharding_core._raise_task_failures(
            primary=primary,
            close_errors=[close_error],
            label="result staging write or close failed",
        )
    if primary is not None:
        cleanup_error: BaseException | None = None
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except BaseException as error:
            cleanup_error = error
        sharding_core._raise_ordered_failures(
            "result staging write or cleanup failed",
            primary,
            [cleanup_error] if cleanup_error is not None else [],
        )
    return temporary


def _read_authoritative_regular_at(
    directory_fd: int, name: str, label: str
) -> bytes:
    expected = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(expected.st_mode):
        raise AssertionError(f"{label} must be a regular file")
    descriptor = os.open(
        name,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        dir_fd=directory_fd,
    )
    slot = sharding_core._DescriptorSlot(descriptor)
    primary: BaseException | None = None
    content = bytearray()
    try:
        opened = os.fstat(slot.descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        identities = {
            (expected.st_dev, expected.st_ino),
            (opened.st_dev, opened.st_ino),
            (current.st_dev, current.st_ino),
        }
        if not stat.S_ISREG(opened.st_mode) or len(identities) != 1:
            raise AssertionError(f"{label} changed while opening")
        while True:
            chunk = os.read(slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            content.extend(chunk)
    except BaseException as error:
        primary = error
    _retire_authoritative_file(
        slot, primary=primary, label=f"{label} read or close failed"
    )
    return bytes(content)


def _read_retained_authoritative_regular_at(
    directory_fd: int, name: str, label: str
) -> tuple[bytes, _RetainedAuthoritativeFile]:
    expected = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if not stat.S_ISREG(expected.st_mode):
        raise AssertionError(f"{label} must be a regular file")
    descriptor = os.open(
        name,
        os.O_RDONLY
        | sharding_core._required_os_flag("O_NOFOLLOW")
        | getattr(os, "O_CLOEXEC", 0),
        dir_fd=directory_fd,
    )
    slot = sharding_core._DescriptorSlot(descriptor)
    primary: BaseException | None = None
    content = bytearray()
    entry: _RetainedAuthoritativeFile | None = None
    try:
        opened = os.fstat(slot.descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(opened.st_mode) or len(
            {
                (expected.st_dev, expected.st_ino),
                (opened.st_dev, opened.st_ino),
                (current.st_dev, current.st_ino),
            }
        ) != 1:
            raise AssertionError(f"{label} changed while opening")
        while True:
            chunk = os.read(slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            content.extend(chunk)
        after = os.fstat(slot.descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if len(
            {
                _stable_metadata(expected),
                _stable_metadata(opened),
                _stable_metadata(after),
                _stable_metadata(current),
            }
        ) != 1:
            raise AssertionError(f"{label} changed while reading")
        identity = (after.st_dev, after.st_ino)
        sharding_core._reconcile_named_descriptor_at(
            directory_fd,
            name,
            slot.descriptor,
            identity,
            label=label,
            kind="file",
            mode=0o600,
        )
        entry = _RetainedAuthoritativeFile(name, identity, slot)
    except BaseException as error:
        primary = error
    if primary is not None:
        _retire_authoritative_file(
            slot, primary=primary, label=f"{label} read or close failed"
        )
    assert entry is not None
    return bytes(content), entry


def _decode_result_pointer(content: bytes) -> dict[str, object]:
    decoded = json.loads(content.decode("utf-8"))
    if not isinstance(decoded, dict) or set(decoded) != {
        "schema_version", "generation", "files"
    }:
        raise AssertionError("invalid result commit pointer fields")
    generation = decoded["generation"]
    if decoded["schema_version"] != 1 or type(generation) is not str or not re.fullmatch(
        r"[0-9a-f]{24}", generation
    ):
        raise AssertionError("invalid result commit pointer identity")
    files = decoded["files"]
    if not isinstance(files, dict) or set(files) != {"forward", "lifecycle"}:
        raise AssertionError("invalid result commit pointer files")
    return decoded


def _readback_result_pair_at(
    result_parent,
    generation_entry: sharding_core._RetainedDirectory,
    pointer_name: str,
    manifests: dict[str, list[dict]],
) -> _RetainedResultReadback:
    result_parent._validate_live()
    sharding_core._reconcile_named_descriptor_at(
        result_parent.descriptor,
        generation_entry.name,
        generation_entry.slot.descriptor,
        generation_entry.identity,
        label="result generation root",
        kind="directory",
        mode=0o700,
    )
    retained: list[_RetainedAuthoritativeFile] = []
    primary: BaseException | None = None
    readback: _RetainedResultReadback | None = None
    try:
        pointer_content, pointer_entry = _read_retained_authoritative_regular_at(
            result_parent.descriptor, pointer_name, "result commit pointer"
        )
        retained.append(pointer_entry)
        decoded = _decode_result_pointer(pointer_content)
        files = decoded["files"]
        generation = decoded["generation"]
        results: dict[str, list[dict]] = {}
        generation_entries: dict[str, _RetainedAuthoritativeFile] = {}
        for mode in ("forward", "lifecycle"):
            pointer_file = files[mode]
            if not isinstance(pointer_file, dict) or set(pointer_file) != {
                "path",
                "sha256",
            }:
                raise AssertionError(f"invalid {mode} result pointer entry")
            expected_name = f"{generation}-{mode}.json"
            if pointer_file["path"] != (
                f"{RESULT_GENERATION_DIRECTORY}/{expected_name}"
            ):
                raise AssertionError(f"{mode} result generation path mismatch")
            content, entry = _read_retained_authoritative_regular_at(
                generation_entry.slot.descriptor,
                expected_name,
                f"{mode} result generation",
            )
            retained.append(entry)
            generation_entries[mode] = entry
            if hashlib.sha256(content).hexdigest() != pointer_file["sha256"]:
                raise AssertionError(f"{mode} result generation hash mismatch")
            results[mode] = json.loads(content.decode("utf-8"))
        _validate_result_pair(results, manifests)
        result_parent._validate_live()
        readback = _RetainedResultReadback(
            results=results,
            pointer_entry=pointer_entry,
            generation_entries=generation_entries,
        )
    except BaseException as error:
        primary = error
    if primary is not None:
        sharding_core._retire_task_descriptors(
            [entry.slot for entry in reversed(retained)],
            primary=primary,
            label="result readback or descriptor close failed",
        )
    assert readback is not None
    return readback


def _persist_result_pair_retained(
    destinations: dict[str, Path],
    results: dict[str, list[dict]],
    manifests: dict[str, list[dict]],
    *,
    authority: ResultWriterAuthority,
    crash_at: str | None = None,
) -> _RetainedCommittedResultPair:
    """Commit and retain descriptor authority for the final acceptance gates."""

    if type(authority) is not ResultWriterAuthority:
        raise TypeError("persist_result_pair requires exact ResultWriterAuthority")
    _validate_result_pair(results, manifests)
    destinations = authority._consume(destinations)
    result_parent = authority._open_result_parent()
    parent = destinations["forward"].parent
    generation_entry: sharding_core._RetainedDirectory | None = None
    staged: list[tuple[int, str]] = []
    primary: BaseException | None = None
    pointer: Path | None = None
    decoded_results: dict[str, list[dict]] | None = None
    generation_names: dict[str, str] | None = None
    readback: _RetainedResultReadback | None = None
    try:
        parent_fd = result_parent.descriptor
        generation_slot, generation_identity = sharding_core._open_managed_directory_at(
            parent_fd,
            RESULT_GENERATION_DIRECTORY,
            label="result generation root",
            create=True,
        )
        generation_entry = sharding_core._RetainedDirectory(
            RESULT_GENERATION_DIRECTORY, generation_identity, generation_slot
        )
        generation_fd = generation_slot.descriptor
        os.fsync(parent_fd)

        contents = {
            mode: _json_bytes(results[mode]) for mode in ("forward", "lifecycle")
        }
        digests = {
            mode: hashlib.sha256(contents[mode]).hexdigest()
            for mode in ("forward", "lifecycle")
        }
        generation = hashlib.sha256(
            (digests["forward"] + digests["lifecycle"]).encode("ascii")
        ).hexdigest()[:24]
        generation_names = {
            mode: f"{generation}-{mode}.json" for mode in ("forward", "lifecycle")
        }
        for mode in ("forward", "lifecycle"):
            destination = generation_names[mode]
            try:
                existing = _read_authoritative_regular_at(
                    generation_fd, destination, "result generation"
                )
            except FileNotFoundError:
                existing = None
            if existing is not None:
                if existing != contents[mode]:
                    raise AssertionError("immutable result generation hash collision")
            else:
                temporary = _stage_authoritative_bytes_at(
                    generation_fd, destination, contents[mode]
                )
                staged.append((generation_fd, temporary))
                _inject_result_crash(crash_at, f"after_{mode}_write")
                os.replace(
                    temporary,
                    destination,
                    src_dir_fd=generation_fd,
                    dst_dir_fd=generation_fd,
                )
                staged.remove((generation_fd, temporary))
                os.fsync(generation_fd)
            _inject_result_crash(crash_at, f"after_{mode}_rename")

        pointer = parent / RESULT_COMMIT_FILENAME
        pointer_value = {
            "schema_version": 1,
            "generation": generation,
            "files": {
                mode: {
                    "path": f"{RESULT_GENERATION_DIRECTORY}/{generation_names[mode]}",
                    "sha256": digests[mode],
                }
                for mode in ("forward", "lifecycle")
            },
        }
        pointer_temporary = _stage_authoritative_bytes_at(
            parent_fd, pointer.name, _json_bytes(pointer_value)
        )
        staged.append((parent_fd, pointer_temporary))
        _inject_result_crash(crash_at, "after_pointer_write")
        os.replace(
            pointer_temporary,
            pointer.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        staged.remove((parent_fd, pointer_temporary))
        os.fsync(parent_fd)
        _inject_result_crash(crash_at, "after_pointer_rename")
        readback = _readback_result_pair_at(
            result_parent,
            generation_entry,
            pointer.name,
            manifests,
        )
        decoded_results = readback.results
        if decoded_results != results:
            raise AssertionError("committed result pair readback mismatch")
    except BaseException as error:
        primary = error

    if primary is None or not sharding_core.is_indeterminate_descriptor_close(primary):
        for directory_fd, temporary in staged:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            except BaseException as cleanup_error:
                if primary is None:
                    primary = cleanup_error
                else:
                    group_type = (
                        ExceptionGroup
                        if isinstance(primary, Exception)
                        and isinstance(cleanup_error, Exception)
                        else BaseExceptionGroup
                    )
                    primary = group_type(
                        "result persistence and staging cleanup failed",
                        [primary, cleanup_error],
                    )

    if primary is not None:
        close_slots = list(readback.retirement_slots()) if readback else []
        if generation_entry is not None:
            close_slots.append(generation_entry.slot)
        close_slots.extend(
            entry.slot for entry in reversed(result_parent._retained)
        )
        result_parent._closed = True
        sharding_core._retire_task_descriptors(
            close_slots,
            primary=primary,
            label="result persistence or descriptor close failed",
        )
    assert pointer is not None
    assert decoded_results is not None
    assert generation_entry is not None
    assert generation_names is not None
    assert readback is not None
    relative_parent = parent.relative_to(authority._lease._repository_root)
    return _RetainedCommittedResultPair(
        pointer=pointer,
        results=decoded_results,
        result_parent=result_parent,
        generation_entry=generation_entry,
        generation_names=generation_names,
        relative_parent=relative_parent,
        pointer_entry=readback.pointer_entry,
        generation_file_entries=readback.generation_entries,
    )


def persist_result_pair(
    destinations: dict[str, Path],
    results: dict[str, list[dict]],
    manifests: dict[str, list[dict]],
    *,
    authority: ResultWriterAuthority,
    crash_at: str | None = None,
) -> Path:
    """Commit a pair through immutable generations and one atomic pointer."""

    committed = _persist_result_pair_retained(
        destinations,
        results,
        manifests,
        authority=authority,
        crash_at=crash_at,
    )
    pointer = committed.pointer
    committed.close()
    return pointer


def resolve_committed_result_pair(
    pointer: Path, manifests: dict[str, list[dict]]
) -> dict[str, list[dict]]:
    """Resolve only the pair named by the authoritative commit pointer."""

    pointer = Path(pointer).absolute()
    parent_fd = _open_directory_path(pointer.parent, create=False)
    try:
        decoded = json.loads(
            _read_regular_at(parent_fd, pointer.name, "result commit pointer").decode(
                "utf-8"
            )
        )
    finally:
        os.close(parent_fd)
    if not isinstance(decoded, dict) or set(decoded) != {
        "schema_version", "generation", "files"
    }:
        raise AssertionError("invalid result commit pointer fields")
    if decoded["schema_version"] != 1 or not re.fullmatch(
        r"[0-9a-f]{24}", decoded["generation"] or ""
    ):
        raise AssertionError("invalid result commit pointer identity")
    files = decoded["files"]
    if not isinstance(files, dict) or set(files) != {"forward", "lifecycle"}:
        raise AssertionError("invalid result commit pointer files")
    results = {}
    parent_fd = _open_directory_path(pointer.parent, create=False)
    try:
        generation_fd = _open_child_directory(
            parent_fd, RESULT_GENERATION_DIRECTORY, create=False
        )
    except Exception:
        os.close(parent_fd)
        raise
    try:
        for mode in ("forward", "lifecycle"):
            entry = files[mode]
            if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
                raise AssertionError(f"invalid {mode} result pointer entry")
            expected_name = f"{decoded['generation']}-{mode}.json"
            if entry["path"] != f"{RESULT_GENERATION_DIRECTORY}/{expected_name}":
                raise AssertionError(f"{mode} result generation path mismatch")
            content = _read_regular_at(generation_fd, expected_name, "result generation")
            if hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise AssertionError(f"{mode} result generation hash mismatch")
            results[mode] = json.loads(content.decode("utf-8"))
    finally:
        os.close(generation_fd)
        os.close(parent_fd)
    _validate_result_pair(results, manifests)
    return results


def _observation_records(wiki_root: Path) -> list[dict]:
    records = []
    observations = wiki_root / "wiki" / "observations"
    if not observations.is_dir():
        return records
    for path in sorted(observations.glob("*.md")):
        metadata = _parse_frontmatter(path)
        if not metadata:
            continue
        records.append(
            {
                "run_id": metadata.get("run_id") or path.stem,
                "timestamp": metadata.get("timestamp") or "",
                "status": metadata.get("status") or "",
                "start_mode": metadata.get("start_mode") or "",
                "superseded_by": metadata.get("superseded_by"),
                "task_type": metadata.get("task_type"),
                "workflow_variant": metadata.get("workflow_variant"),
            }
        )
    return sorted(records, key=lambda row: (row["timestamp"], row["run_id"]))


class AppServer:
    def __init__(
        self,
        cwd: Path,
        runtime: CaseRuntime,
        popen_factory=subprocess.Popen,
    ):
        overrides = build_codex_config_overrides(
            runtime.transport_config,
            runtime.environment,
            runtime.disabled_skill_paths,
        )
        command = build_app_server_command(runtime.transport_config, overrides)
        environment = _validated_transport_environment(runtime)
        verify_codex_executable(runtime.transport_config)
        self.process = popen_factory(
            command,
            cwd=cwd,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        self.process.process_group_id = self.process.pid
        started_readers: list[threading.Thread] = []
        try:
            self.process_group_id = _process_group_id(self.process)
            self.process.process_group_id = self.process_group_id
            self.event_sink: CaseEventSink | None = None
            self.messages: queue.Queue[dict] = queue.Queue()
            self.stderr_tail: deque[str] = deque(maxlen=80)
            self.events: list[dict] = []
            self.agent_messages: list[str] = []
            self.command_executions: list[str] = []
            self.observation_command_diagnostics: list[dict] = []
            self.completed_turns: dict[str, dict] = {}
            self.active_command_executions: dict[str, str] = {}
            self.active_thread_id: str | None = None
            self.active_turn_id: str | None = None
            self._pending_thread_id: str | None = None
            self._pending_token_usage: dict[
                str, TokenUsage | CaseProtocolFailure
            ] = {}
            self.token_usage: TokenUsage | None = None
            self.model_started = False
            self._request_id = 0
            self._stdout_thread = threading.Thread(
                target=self._read_stdout, daemon=True
            )
            self._stderr_thread = threading.Thread(
                target=self._read_stderr, daemon=True
            )
            self._stdout_thread.start()
            started_readers.append(self._stdout_thread)
            self._stderr_thread.start()
            started_readers.append(self._stderr_thread)
        except BaseException as primary:
            cleanup_errors = []
            try:
                stop_process_group(
                    self.process, readers=tuple(started_readers)
                )
            except BaseException as error:
                cleanup_errors.append(error)
            _raise_case_and_auth_cleanup_failures(primary, cleanup_errors)

    def _read_stdout(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            try:
                self.messages.put(json.loads(line))
            except json.JSONDecodeError:
                self.messages.put({"_workflow_eval_protocol_failure": True})

    def _read_stderr(self) -> None:
        assert self.process.stderr is not None
        for line in self.process.stderr:
            self.stderr_tail.append(line.rstrip())

    def _send(self, message: dict) -> None:
        if self.process.poll() is not None:
            raise RuntimeError(
                "app-server exited: "
                f"stderr_tail={_bounded_sensitive_summaries(self.stderr_tail)!r}"
            )
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message, ensure_ascii=False) + "\n")
        self.process.stdin.flush()

    def _record(self, message: dict) -> None:
        self.events.append(message)
        method = message.get("method")
        params = message.get("params") or {}
        if method == "item/started":
            item = params.get("item") or {}
            if item.get("type") == "commandExecution":
                item_id = item.get("id")
                command = item.get("command")
                if isinstance(item_id, str) and isinstance(command, str):
                    self.active_command_executions[item_id] = command
        if method == "item/completed":
            item = params.get("item") or {}
            item_id = item.get("id")
            if isinstance(item_id, str):
                self.active_command_executions.pop(item_id, None)
            if item.get("type") == "agentMessage" and isinstance(item.get("text"), str):
                self.agent_messages.append(item["text"])
            if item.get("type") == "commandExecution":
                command = item.get("command") or ""
                self.command_executions.append(command)
                if "wiki_cli_audit.py" in command or " observe " in command:
                    self.observation_command_diagnostics.append(
                        {
                            "command": command,
                            "exit_code": item.get("exitCode"),
                            "output": item.get("aggregatedOutput"),
                        }
                    )
        if method == "turn/completed":
            turn = params.get("turn") or {}
            if isinstance(turn.get("id"), str):
                self.completed_turns[turn["id"]] = turn
        if method == "thread/tokenUsage/updated":
            raw_params = message.get("params")
            if not isinstance(raw_params, dict):
                raise CaseProtocolFailure(
                    "app-server token usage envelope is malformed",
                    model_started=bool(
                        getattr(self, "model_started", False)
                    ),
                )
            thread_id = raw_params.get("threadId")
            turn_id = raw_params.get("turnId")
            if (
                not isinstance(thread_id, str)
                or not thread_id
                or not isinstance(turn_id, str)
                or not turn_id
            ):
                return
            active = (
                thread_id == getattr(self, "active_thread_id", None)
                and turn_id == getattr(self, "active_turn_id", None)
            )
            pending = thread_id == getattr(
                self, "_pending_thread_id", None
            )
            if not active and not pending:
                return
            token_usage = raw_params.get("tokenUsage")
            total = token_usage.get("total") if isinstance(token_usage, dict) else None
            if active:
                self.token_usage = _normalize_token_usage(
                    total, app_server=True
                )
            else:
                try:
                    normalized: TokenUsage | CaseProtocolFailure = (
                        _normalize_token_usage(total, app_server=True)
                    )
                except CaseProtocolFailure:
                    normalized = CaseProtocolFailure(
                        "app-server pending token usage is malformed",
                        model_started=bool(
                            getattr(self, "model_started", False)
                        ),
                    )
                pending_usage = getattr(
                    self, "_pending_token_usage", None
                )
                if not isinstance(pending_usage, dict):
                    pending_usage = {}
                    self._pending_token_usage = pending_usage
                pending_usage[turn_id] = normalized

    def _receive(self, timeout: float) -> dict:
        try:
            message = self.messages.get(timeout=timeout)
        except queue.Empty as error:
            if self.process.poll() is not None:
                raise RuntimeError(
                    "app-server exited: "
                    f"stderr_tail={_bounded_sensitive_summaries(self.stderr_tail)!r}"
                ) from error
            last_event = _safe_protocol_label(
                self.events[-1].get("method") if self.events else None
            )
            raise TimeoutError(
                "app-server silence timeout: "
                f"wait_seconds={timeout:.3f}; pid={self.process.pid}; "
                f"last_event={last_event}; "
                "active_commands="
                f"{_bounded_sensitive_summaries(self.active_command_executions.values())!r}; "
                f"stderr_tail={_bounded_sensitive_summaries(self.stderr_tail)!r}"
            ) from error
        self._accept_received_message(message)
        return message

    def _accept_received_message(self, message: dict) -> None:
        if message == {"_workflow_eval_protocol_failure": True}:
            raise CaseProtocolFailure(
                "app-server emitted malformed JSON",
                model_started=bool(getattr(self, "model_started", False)),
            )
        try:
            self._record(message)
        except CaseTransportFailure:
            raise
        except (AttributeError, KeyError, TypeError, ValueError):
            raise CaseProtocolFailure(
                "app-server notification is malformed",
                model_started=bool(getattr(self, "model_started", False)),
            ) from None
        if (
            "id" in message
            and isinstance(message.get("method"), str)
            and "result" not in message
            and "error" not in message
        ):
            request_id = message["id"]
            safe_request_id = (
                str(request_id)
                if type(request_id) is int and 0 <= request_id <= MAX_TOKEN_COUNT
                else "redacted"
            )
            raise CaseProtocolFailure(
                "unsupported app-server request: "
                f"method={_safe_protocol_label(message['method'])} "
                f"id={safe_request_id}",
                model_started=bool(getattr(self, "model_started", False)),
            )

    def request(self, method: str, params: dict, timeout: float = 30) -> dict:
        self._request_id += 1
        request_id = self._request_id
        starts_model = method == "turn/start"
        if starts_model:
            self.model_started = True
            try:
                _emit_case_event(
                    self.event_sink,
                    "model-started",
                    self.process.pid,
                    self.process_group_id,
                )
            except BaseException:
                raise CaseTransportFailure(
                    "app-server model-start event sink failed",
                    model_started=True,
                    classification="event-sink",
                    retryable=False,
                ) from None
        model_started = bool(getattr(self, "model_started", False))
        try:
            self._send({"method": method, "id": request_id, "params": params})
        except BaseException:
            raise CaseTransportFailure(
                "app-server request send failed",
                model_started=model_started,
            ) from None
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                message = self._receive(max(0.01, deadline - time.monotonic()))
                if message.get("id") != request_id:
                    continue
                if "error" in message:
                    if model_started:
                        raise CaseModelFailure(
                            "app-server turn/start returned an error"
                        )
                    raise CaseProtocolFailure(
                        "app-server request returned an error",
                        model_started=False,
                    )
                return message.get("result") or {}
        except CaseTransportFailure:
            raise
        except (OSError, RuntimeError, TimeoutError):
            raise CaseTransportFailure(
                "app-server request response failed",
                model_started=model_started,
            ) from None
        raise CaseTransportFailure(
            "app-server request response timed out",
            model_started=model_started,
        )

    def notify(self, method: str, params: dict) -> None:
        try:
            self._send({"method": method, "params": params})
        except CaseTransportFailure:
            raise
        except BaseException:
            raise CaseTransportFailure(
                "app-server notification send failed",
                model_started=bool(getattr(self, "model_started", False)),
            ) from None

    def initialize(self) -> None:
        self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "observation_records_v2_eval",
                    "title": "Observation Records v2 Evaluation",
                    "version": "1.0.0",
                }
            },
        )
        self.notify("initialized", {})

    def start_thread(self, cwd: Path) -> str:
        result = self.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "ephemeral": True,
                "approvalPolicy": "never",
                "sandbox": "workspace-write",
                "config": {"features": {"multi_agent": True}},
            },
        )
        thread = result.get("thread") if isinstance(result, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(thread_id, str) or not thread_id:
            raise CaseProtocolFailure(
                "app-server thread/start response is malformed",
                model_started=False,
            )
        self.active_thread_id = thread_id
        self.active_turn_id = None
        self._pending_thread_id = None
        self._pending_token_usage = {}
        self.token_usage = None
        return thread_id

    def start_turn(
        self,
        thread_id: str,
        prompt: str,
        cwd: Path,
        writable_roots: tuple[Path, ...],
    ) -> str:
        self.active_turn_id = None
        self.token_usage = None
        self._pending_thread_id = thread_id
        self._pending_token_usage = {}
        try:
            result = self.request(
                "turn/start",
                {
                    "threadId": thread_id,
                    "cwd": str(cwd),
                    "approvalPolicy": "never",
                    "sandboxPolicy": {
                        "type": "workspaceWrite",
                        "writableRoots": [str(path) for path in writable_roots],
                        "networkAccess": False,
                        "excludeSlashTmp": False,
                        "excludeTmpdirEnvVar": False,
                    },
                    "input": [{"type": "text", "text": prompt}],
                },
            )
        except BaseException:
            self._pending_thread_id = None
            self._pending_token_usage = {}
            raise
        pending_usage = self._pending_token_usage
        self._pending_thread_id = None
        self._pending_token_usage = {}
        turn = result.get("turn") if isinstance(result, dict) else None
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            raise CaseProtocolFailure(
                "app-server turn/start response is malformed",
                model_started=True,
            )
        self.active_thread_id = thread_id
        self.active_turn_id = turn_id
        selected_usage = pending_usage.get(turn_id)
        if isinstance(selected_usage, CaseProtocolFailure):
            raise selected_usage
        self.token_usage = selected_usage
        return turn_id

    def steer(self, thread_id: str, turn_id: str, prompt: str) -> None:
        self.request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": [{"type": "text", "text": prompt}],
            },
        )

    def drain(self, timeout: float = 0.01) -> None:
        try:
            message = self.messages.get(timeout=timeout)
        except queue.Empty:
            return
        self._accept_received_message(message)
        while True:
            try:
                self._accept_received_message(self.messages.get_nowait())
            except queue.Empty:
                return

    def wait_turn(
        self, turn_id: str, timeout: float = APP_SERVER_TURN_TIMEOUT_SECONDS
    ) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if turn_id in self.completed_turns:
                turn = self.completed_turns[turn_id]
                if turn.get("status") != "completed":
                    raise CaseModelFailure(
                        "app-server turn ended without completion"
                    )
                return turn
            try:
                self._receive(max(0.01, deadline - time.monotonic()))
            except CaseTransportFailure:
                raise
            except (OSError, RuntimeError, TimeoutError):
                raise CaseTransportFailure(
                    "app-server turn response failed",
                    model_started=True,
                ) from None
        raise CaseTransportFailure(
            "app-server turn response timed out",
            model_started=True,
        )

    def close(self) -> None:
        readers = tuple(
            reader
            for reader in (
                getattr(self, "_stdout_thread", None),
                getattr(self, "_stderr_thread", None),
            )
            if isinstance(reader, threading.Thread)
        )
        stop_process_group(
            self.process,
            readers=readers,
        )


def _case_environment(
    case: dict,
    audit: PayloadAudit,
    wiki_root: Path,
    lifecycle: bool,
) -> dict[str, str]:
    environment = {
        "OBSERVATION_PAYLOAD_TMPDIR": str(audit.payload_dir),
        "OBSERVATION_AUDIT_LOG": str(audit.log_path),
        "OBSERVATION_AUDIT_TARGET_CLI": str(CENTRAL_CLI),
    }
    if lifecycle and case["setup"]["eval_override"].startswith("incomplete"):
        environment.update(
            {
                "OBSERVATION_EVAL": "1",
                "OBSERVATION_CLI_PATH": str(audit.wrapper_path),
            }
        )
        return environment
    environment.update(
        {
            "OBSERVATION_EVAL": "1",
            "OBSERVATION_WIKI_ROOT": str(wiki_root),
        }
    )
    if lifecycle and case["setup"]["cli"] == "unavailable":
        environment["OBSERVATION_CLI_PATH"] = str(audit.root / "missing-wiki-cli.py")
    else:
        environment["OBSERVATION_CLI_PATH"] = str(audit.wrapper_path)
    return environment


def install_evaluator_guidance(workspace: Path) -> None:
    path = workspace / "AGENTS.md"
    path.write_text(EVALUATOR_DEVELOPER_INSTRUCTIONS + "\n", encoding="utf-8")
    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Evaluation Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "Evaluation Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        "GIT_AUTHOR_DATE": "2026-01-01T00:00:01+00:00",
        "GIT_COMMITTER_DATE": "2026-01-01T00:00:01+00:00",
    }
    subprocess.run(["git", "add", "AGENTS.md"], cwd=workspace, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Install evaluator guidance"],
        cwd=workspace,
        check=True,
        capture_output=True,
        env=environment,
    )


def _case_requires_gate(case: dict) -> bool:
    return any(
        "scripts/gate.py" in turn.get("prompt", "") for turn in case["turns"]
    )


def build_case_fixture(case: dict, destination: Path) -> Path:
    include_gate = _case_requires_gate(case)
    workspace = build_fixture(
        case["id"],
        case["fixture"],
        destination,
        include_gate=include_gate,
        include_failure_script=case["id"] == "task-failure",
    )
    guidance_failure: BaseException | None = None
    try:
        install_evaluator_guidance(workspace)
    except BaseException:
        guidance_failure = CaseInfrastructureFailure(
            "isolated case fixture setup failed"
        )
    if guidance_failure is not None:
        cleanup_errors: list[BaseException] = []
        if include_gate:
            try:
                release_gate(case["id"])
            except BaseException:
                cleanup_errors.append(
                    CaseCleanupFailure("case gate release failed")
                )
        if cleanup_errors:
            raise _CaseFixtureSetupFailureGroup(
                "case fixture setup or cleanup failed",
                [guidance_failure, *cleanup_errors],
            )
        raise guidance_failure
    return workspace


def _capture_checkpoint(
    wiki_root: Path,
    role_map: dict[str, str],
    after_turn: int,
) -> tuple[dict, list[dict]]:
    records = _observation_records(wiki_root)
    return (
        {
            "after_turn": after_turn,
            "records": normalize_records(records, role_map),
        },
        records,
    )


def _wait_for_gate(
    server: AppServer,
    turn_id: str,
    case: dict,
    workspace: Path,
    wiki_root: Path,
) -> None:
    dispatch_when = case["turns"][1]["dispatch_when"]
    if dispatch_when == "after_single_file_mutation_without_run":
        predicate = lambda: after_single_file_mutation_without_run(workspace, wiki_root)
    elif dispatch_when == "after_draft_run":
        predicate = lambda: after_draft_run(wiki_root)
    else:
        raise ValueError(f"unsupported gate: {dispatch_when}")
    deadline = time.monotonic() + GATE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        server.drain(0.05)
        if turn_id in server.completed_turns:
            raise RuntimeError("evaluator exited before the declared gate")
        if predicate():
            return
    raise TimeoutError(f"checkpoint timed out for {case['id']}")


def execute_case_transport(
    case: dict,
    workspace: Path,
    runtime: CaseRuntime,
    wiki_root: Path,
    after_first_turn: Callable[[], None] | None = None,
    event_sink: CaseEventSink | None = None,
) -> CaseExecution:
    route = select_case_transport(case)
    if route == "exec":
        if after_first_turn is not None:
            raise ValueError("exec transport cannot accept a first-turn checkpoint")
        transport = ExecTransport(workspace, runtime)
        transport.event_sink = event_sink
        return transport.run(case["turns"][0]["prompt"])
    if after_first_turn is None:
        raise ValueError("app-server transport requires a first-turn checkpoint")
    try:
        server = AppServer(
            workspace, runtime
        )
    except (CaseInfrastructureFailure, CaseCleanupFailure, CaseTransportFailure):
        raise
    except BaseException as error:
        if _contains_process_survival_failure(error):
            raise
        raise CaseInfrastructureFailure(
            "app-server startup failed"
        ) from None
    server.event_sink = event_sink
    process = getattr(server, "process", None)
    pid = getattr(process, "pid", None)
    process_group_id = getattr(server, "process_group_id", pid)
    gate_released = False
    execution = None
    execution_error: BaseException | None = None
    try:
        _emit_case_event(
            event_sink, "process-started", pid, process_group_id
        )
    except BaseException:
        execution_error = CaseTransportFailure(
            "app-server process-start event sink failed",
            model_started=False,
            classification="event-sink",
            retryable=False,
        )
    if execution_error is None:
        try:
            server.initialize()
            thread_id = server.start_thread(workspace)
            turn_id = server.start_turn(
                thread_id,
                case["turns"][0]["prompt"],
                workspace,
                runtime.writable_roots,
            )
            _wait_for_gate(server, turn_id, case, workspace, wiki_root)
            after_first_turn()
            server.steer(thread_id, turn_id, case["turns"][1]["prompt"])
            release_gate(case["id"])
            gate_released = True
            server.wait_turn(turn_id)
            drain = getattr(server, "drain", None)
            if callable(drain):
                drain(0.05)
            if not server.agent_messages:
                raise RuntimeError("app-server final agent message is missing")
            if getattr(server, "token_usage", None) is None:
                raise CaseProtocolFailure("app-server token usage is missing")
            execution = CaseExecution(
                terminal_status="completed",
                final_text=server.agent_messages[-1],
                command_executions=tuple(server.command_executions),
                observation_command_diagnostics=tuple(
                    server.observation_command_diagnostics
                ),
                usage=server.token_usage,
            )
        except BaseException as error:
            execution_error = error

    cleanup_errors: list[BaseException] = []
    try:
        if not gate_released:
            try:
                release_gate(case["id"])
            except BaseException as error:
                cleanup_errors.append(error)
    finally:
        try:
            server.close()
        except BaseException as error:
            cleanup_errors.append(error)
        else:
            try:
                _emit_case_event(
                    event_sink, "process-stopped", pid, process_group_id
                )
            except BaseException:
                cleanup_errors.append(
                    CaseTransportFailure(
                        "app-server process-stop event sink failed",
                        model_started=bool(
                            getattr(server, "model_started", False)
                        ),
                        classification="event-sink",
                        retryable=False,
                    )
                )

    cleanup_failures = []
    for error in cleanup_errors:
        if isinstance(
            error, (ProcessSurvivalCleanupFailure, CaseTransportFailure)
        ):
            failure = error
        else:
            failure = CaseCleanupFailure(str(error))
            failure.__cause__ = error
        cleanup_failures.append(failure)
    errors = (
        ([execution_error] if execution_error is not None else [])
        + cleanup_failures
    )
    if len(errors) == 1:
        raise errors[0]
    if errors:
        group_type = (
            ExceptionGroup
            if all(isinstance(error, Exception) for error in errors)
            else BaseExceptionGroup
        )
        raise group_type("app-server execution or cleanup failed", errors)
    assert execution is not None
    return execution


def _validate_case_observations(
    case: dict,
    checkpoints: list[dict],
    store: dict,
) -> None:
    expected_checkpoints = case.get("expected_record_checkpoints")
    if expected_checkpoints is not None and checkpoints != expected_checkpoints:
        raise AssertionError(
            f"{case['id']}: checkpoint mismatch: expected {expected_checkpoints}, got {checkpoints}"
        )
    for field, actual in (
        ("expected_run_count", store["run_count"]),
        ("expected_draft_count", store["draft_count"]),
        ("expected_final_statuses", store["final_statuses"]),
    ):
        if field in case and case[field] is not None and case[field] != actual:
            raise AssertionError(
                f"{case['id']}: {field} expected {case[field]}, got {actual}"
            )


AUTH_CLEANUP_MAX_DEPTH = 32
AUTH_CLEANUP_MAX_ENTRIES = 4096


def _remove_tree_entry(
    parent_descriptor: int,
    name: str,
    *,
    depth: int,
    remaining: list[int],
    charged: bool = False,
) -> None:
    if depth > AUTH_CLEANUP_MAX_DEPTH:
        raise OSError("owned auth cleanup bound exceeded")
    if not charged:
        if remaining[0] <= 0:
            raise OSError("owned auth cleanup bound exceeded")
        remaining[0] -= 1
    metadata = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode):
        os.unlink(name, dir_fd=parent_descriptor)
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    child_descriptor = os.open(name, flags, dir_fd=parent_descriptor)
    try:
        child_names = []
        with os.scandir(child_descriptor) as entries:
            for entry in entries:
                if remaining[0] <= 0:
                    raise OSError("owned auth cleanup bound exceeded")
                remaining[0] -= 1
                child_names.append(entry.name)
        for child_name in sorted(child_names):
            _remove_tree_entry(
                child_descriptor,
                child_name,
                depth=depth + 1,
                remaining=remaining,
                charged=True,
            )
    finally:
        os.close(child_descriptor)
    os.rmdir(name, dir_fd=parent_descriptor)


def _remove_owned_auth_directory(path: Path) -> None:
    parent = path.parent
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        parent_descriptor = os.open(parent, flags)
    except OSError:
        raise CaseCleanupFailure("owned auth cleanup failed") from None
    try:
        try:
            os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            return
        _remove_tree_entry(
            parent_descriptor,
            path.name,
            depth=0,
            remaining=[AUTH_CLEANUP_MAX_ENTRIES],
        )
    except OSError:
        raise CaseCleanupFailure("owned auth cleanup failed") from None
    finally:
        os.close(parent_descriptor)


def _create_external_auth_owner(destination: Path) -> Path:
    owner: Path | None = None
    try:
        owner = Path(
            tempfile.mkdtemp(prefix="observing-workflows-auth-owner-")
        ).resolve(strict=True)
        forbidden_roots = (
            destination.resolve(strict=True),
            REPOSITORY_ROOT.resolve(strict=True),
        )
        if any(owner.is_relative_to(root) for root in forbidden_roots):
            owner.rmdir()
            raise OSError("auth owner is inside a forbidden root")
        owner.chmod(0o700)
        return owner
    except OSError:
        if owner is not None:
            try:
                owner.rmdir()
            except OSError:
                pass
        raise CaseInfrastructureFailure(
            "external auth owner setup failed"
        ) from None


def _raise_case_and_auth_cleanup_failures(
    primary: BaseException | None,
    cleanup_errors: list[BaseException],
) -> None:
    errors = ([primary] if primary is not None else []) + cleanup_errors
    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    group_type = (
        ExceptionGroup
        if all(isinstance(error, Exception) for error in errors)
        else BaseExceptionGroup
    )
    raise group_type("case execution or auth cleanup failed", errors)


def _contains_process_survival_failure(error: BaseException | None) -> bool:
    if error is None:
        return False
    if isinstance(error, ProcessSurvivalCleanupFailure):
        return True
    if any(
        _contains_process_survival_failure(child)
        for child in getattr(error, "exceptions", ())
    ):
        return True
    cause = getattr(error, "__cause__", None)
    if cause is not None and _contains_process_survival_failure(cause):
        return True
    return False


def _run_case_impl(
    case: dict,
    destination: Path,
    lifecycle: bool,
    runtime_factory,
    owned_auth_directories: list[Path],
    transport_runner,
    event_sink: CaseEventSink | None,
    execution_sink: Callable[[CaseExecution], None] | None,
    workspace_parent: Path | None,
) -> dict:
    gate_registered = False
    setup_primary: BaseException | None = None
    try:
        if workspace_parent is None:
            case_root = destination / ("lifecycle" if lifecycle else "forward")
            fixture_parent = case_root
            expected_workspace = None
        else:
            case_root = Path(destination)
            fixture_parent = Path(workspace_parent)
            if (
                not case_root.is_absolute()
                or not fixture_parent.is_absolute()
                or ".." in fixture_parent.parts
            ):
                raise ValueError("workspace parent must be absolute and canonical")
            try:
                canonical_parent = fixture_parent.resolve(strict=True)
                metadata = fixture_parent.lstat()
                canonical_destination = case_root.resolve(strict=True)
            except OSError:
                raise ValueError("workspace parent is unavailable") from None
            if (
                canonical_destination != case_root
                or canonical_parent != fixture_parent
                or stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISDIR(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o700
                or fixture_parent != case_root / "workspace"
            ):
                raise ValueError(
                    "workspace parent must be private and below the case root"
                )
            expected_workspace = fixture_parent / case["id"]
            if expected_workspace.exists() or expected_workspace.is_symlink():
                raise ValueError("canonical case workspace already exists")
        case_root.mkdir(exist_ok=True)
        workspace = build_case_fixture(case, fixture_parent)
        gate_registered = _case_requires_gate(case)
        if expected_workspace is not None and workspace != expected_workspace:
            raise ValueError("fixture builder returned a non-canonical workspace")
        if runtime_factory is None:
            wiki_root = case_root / f"{case['id']}-wiki-root"
            wiki_root.mkdir(mode=0o700)
            audit = build_payload_audit(case["id"], case_root, CENTRAL_CLI)
            source_codex_home = Path(
                os.environ.get("CODEX_HOME", Path.home() / ".codex")
            ).expanduser().resolve(strict=True)
            codex_executable = shutil.which("codex")
            if codex_executable is None:
                raise CaseInfrastructureFailure("Codex executable is unavailable")
            transport_config = resolve_transport_config(
                codex_executable=Path(codex_executable),
                source_codex_home=source_codex_home,
                requested_model=None,
                requested_reasoning_effort=None,
            )
            auth_owner = _create_external_auth_owner(destination)
            owned_auth_directories.insert(0, auth_owner)
            bootstrap = prepare_auth_bootstrap(
                source_codex_home=source_codex_home,
                coordinator_root=auth_owner,
            )
            owned_auth_directories.insert(0, bootstrap)
            case_codex_home = auth_owner / "case-codex-home"
            install_case_auth(
                bootstrap=bootstrap,
                case_codex_home=case_codex_home,
            )
            owned_auth_directories.insert(0, case_codex_home)
            environment = _case_environment(case, audit, wiki_root, lifecycle)
            environment["CODEX_HOME"] = str(case_codex_home)
            runtime = CaseRuntime(
                wiki_root,
                audit,
                environment,
                (wiki_root, audit.root),
                transport_config,
                audited_wrapper_path=audit.wrapper_path,
                audited_wrapper_content=ATTEMPT_AUDIT_WRAPPER,
            )
        else:
            runtime = runtime_factory(case, case_root, workspace, lifecycle)
            wiki_root = runtime.store_root
            audit = runtime.audit
        if runtime.audited_wrapper_content is not None:
            if runtime.audited_wrapper_path is None:
                raise ValueError("runtime wrapper content requires a wrapper path")
            runtime.audited_wrapper_path.write_text(
                runtime.audited_wrapper_content, encoding="utf-8"
            )
            runtime.audited_wrapper_path.chmod(0o700)
    except BaseException as error:
        if isinstance(
            error,
            (
                CaseCleanupFailure,
                CaseInfrastructureFailure,
                _CaseFixtureSetupFailureGroup,
            ),
        ):
            setup_primary = error
        else:
            setup_primary = CaseInfrastructureFailure(
                "isolated case setup failed"
            )
    if setup_primary is not None:
        cleanup_errors: list[BaseException] = []
        if gate_registered:
            try:
                release_gate(case["id"])
            except BaseException:
                cleanup_errors.append(
                    CaseCleanupFailure("case gate release failed")
                )
        _raise_case_and_auth_cleanup_failures(setup_primary, cleanup_errors)
        raise AssertionError("unreachable setup failure")
    role_map: dict[str, str] = {}
    checkpoints = []
    decisions = []
    previous_run_count = 0

    def capture_first_turn() -> None:
        nonlocal previous_run_count
        checkpoint, records = _capture_checkpoint(wiki_root, role_map, 1)
        checkpoints.append(checkpoint)
        if not lifecycle:
            decisions.append(
                decision_from_checkpoint(1, records, previous_run_count)
            )
            previous_run_count = len(records)

    route = select_case_transport(case)
    execution = transport_runner(
        case,
        workspace,
        runtime,
        wiki_root,
        capture_first_turn if route == "app-server" else None,
        event_sink=event_sink,
    )
    if execution_sink is not None:
        execution_sink(execution)
    final_turn_number = len(case["turns"])
    checkpoint, records = _capture_checkpoint(
        wiki_root, role_map, final_turn_number
    )
    checkpoints.append(checkpoint)
    if not lifecycle:
        decisions.append(
            decision_from_checkpoint(final_turn_number, records, previous_run_count)
        )
    final_text = execution.final_text

    assert execution.terminal_status == "completed"
    store = inspect_store(wiki_root)
    def verify_integrity() -> None:
        if runtime.integrity_command is not None:
            run_configured_integrity(
                runtime.integrity_command,
                runtime.environment,
                expected_records=store["run_count"],
            )
    if lifecycle and case["mode"] == "command-selection-only":
        selected_runtime_command = (
            runtime.selected_command if runtime.selected_command in final_text else None
        )
        if selected_runtime_command is None:
            raise AssertionError(
                f"{case['id']}: selected command missing; final message was {final_text!r}"
            )
        assert_observation_attempt_ledger(
            load_observation_attempt_ledger(audit),
            execution.command_executions,
            0,
            0,
            audit,
        )
        verify_integrity()
        return {
            "id": case["id"],
            "record_checkpoints": None,
            "run_count": None,
            "draft_count": None,
            "final_statuses": None,
            "failure_disclosed": None,
            "selected_command": case["expected_selected_command"],
        }

    try:
        _validate_case_observations(case, checkpoints, store)
    except AssertionError as error:
        raise AssertionError(
            f"{error}; evaluator final message: {final_text!r}"
        ) from error
    expected_start = case["expected_run_count"]
    if lifecycle and case.get("setup", {}).get("cli") == "unavailable":
        expected_start = 1
    expected_finish = len(case["expected_final_statuses"])
    try:
        assert_observation_attempt_ledger(
            load_observation_attempt_ledger(audit),
            execution.command_executions,
            expected_start,
            expected_finish,
            audit,
        )
    except AssertionError as error:
        raise AssertionError(
            f"{error}; observation command diagnostics: "
            f"{execution.observation_command_diagnostics!r}; "
            f"evaluator final message: {final_text!r}"
        ) from error
    verify_integrity()

    if lifecycle:
        disclosed = recording_failure_disclosed(final_text)
        if disclosed != case["expect_failure_disclosure"]:
            raise AssertionError(
                f"{case['id']}: failure disclosure expected "
                f"{case['expect_failure_disclosure']}, got {disclosed}"
            )
        return {
            "id": case["id"],
            "record_checkpoints": checkpoints,
            "run_count": store["run_count"],
            "draft_count": store["draft_count"],
            "final_statuses": store["final_statuses"],
            "failure_disclosed": disclosed,
            "selected_command": None,
        }

    validate_forward_decisions(case, decisions)
    return {
        "id": case["id"],
        "decisions": decisions,
        "record_checkpoints": checkpoints,
        "run_count": store["run_count"],
        "draft_count": store["draft_count"],
        "final_statuses": store["final_statuses"],
    }


def _run_case(
    case: dict,
    destination: Path,
    lifecycle: bool,
    runtime_factory=None,
    *,
    workspace_parent: Path | None = None,
    transport_runner=None,
    event_sink: CaseEventSink | None = None,
    execution_sink: Callable[[CaseExecution], None] | None = None,
) -> dict:
    if transport_runner is None:
        transport_runner = execute_case_transport
    owned_auth_directories: list[Path] = []
    result: dict | None = None
    primary: BaseException | None = None
    try:
        result = _run_case_impl(
            case,
            destination,
            lifecycle,
            runtime_factory,
            owned_auth_directories,
            transport_runner,
            event_sink,
            execution_sink,
            workspace_parent,
        )
    except BaseException as error:
        primary = error

    if _contains_process_survival_failure(primary):
        _raise_case_and_auth_cleanup_failures(primary, [])

    cleanup_errors: list[BaseException] = []
    for owned_path in owned_auth_directories:
        try:
            _remove_owned_auth_directory(owned_path)
        except BaseException as error:
            cleanup_errors.append(error)
            break
    _raise_case_and_auth_cleanup_failures(primary, cleanup_errors)
    assert result is not None
    return result


def run_suite(
    evaluator_root: Path,
    *,
    repository_root: Path,
    manifest_paths: dict[str, Path] | None = None,
    result_destinations: dict[str, Path] | None = None,
    runtime_factory=None,
    coordinator_role: str | None = None,
) -> tuple[list[dict], list[dict]]:
    if type(coordinator_role) is not str or coordinator_role != "serial-coordinator":
        raise ValueError("legacy formal run_suite requires serial-coordinator role")
    manifest_paths = manifest_paths or {
        "forward": evaluator_root
        / "tests/skill_evals/observing_workflows_cases.json",
        "lifecycle": evaluator_root
        / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    }
    result_destinations = result_destinations or {
        "forward": evaluator_root
        / "tests/skill_evals/observing_workflows_forward.json",
        "lifecycle": evaluator_root
        / "tests/skill_evals/observing_workflows_lifecycle_forward.json",
    }
    frozen_bytes = {name: path.read_bytes() for name, path in manifest_paths.items()}
    manifests = {
        name: json.loads(raw.decode("utf-8"))
        for name, raw in frozen_bytes.items()
    }
    validate_frozen_manifests(manifest_paths, manifests, frozen_bytes)
    _assert_frozen_manifest_bytes(manifest_paths, frozen_bytes)
    production = snapshot_production(evaluator_root)
    with tempfile.TemporaryDirectory(prefix="observing-workflows-task9-") as temporary:
        destination = Path(temporary).resolve(strict=True)
        results: dict[str, list[dict]] = {"forward": [], "lifecycle": []}
        for mode in ("forward", "lifecycle"):
            for index, case in enumerate(manifests[mode], 1):
                print(
                    f"[{mode} {index}/{len(manifests[mode])}] {case['id']}",
                    flush=True,
                )
                result = run_with_production_guard(
                    lambda case=case, mode=mode: _run_case(
                        case,
                        destination,
                        lifecycle=mode == "lifecycle",
                        runtime_factory=runtime_factory,
                    ),
                    lambda: assert_production_unchanged(production),
                )
                results[mode].append(result)
        for name, path in manifest_paths.items():
            if path.read_bytes() != frozen_bytes[name]:
                raise AssertionError(f"{name} manifest changed during evaluation")
        verify_all_integrity = getattr(runtime_factory, "verify_all_integrity", None)
        if verify_all_integrity is not None:
            run_with_production_guard(
                verify_all_integrity,
                lambda: assert_production_unchanged(production),
            )
        else:
            assert_production_unchanged(production)
        lease = ResultWriterLease.acquire(
            repository_root,
            role="serial-coordinator",
            run_kind="formal",
            run_lease=None,
        )
        primary: BaseException | None = None
        committed: dict[str, list[dict]] | None = None
        retained: _RetainedCommittedResultPair | None = None
        try:
            assert_production_unchanged(production)
            delta_before = _capture_repository_delta(lease)
            retained = _persist_result_pair_retained(
                result_destinations,
                results,
                manifests,
                authority=lease.authority(),
            )
            _validate_committed_result_semantics(retained.results, manifests)
            _assert_exact_result_repository_delta(delta_before, retained, lease)
            committed = retained.results
        except BaseException as error:
            primary = error
        close_errors: list[BaseException] = []
        if retained is not None:
            try:
                retained.close(primary)
            except BaseException as error:
                primary = error
        try:
            lease.close()
        except BaseException as error:
            close_errors.append(error)
        sharding_core._raise_ordered_failures(
            "formal evaluation or result-writer close failed", primary, close_errors
        )
        assert committed is not None
        return committed["forward"], committed["lifecycle"]


def _contains_cleanup_failure(error: BaseException) -> bool:
    if isinstance(error, CaseCleanupFailure):
        return True
    nested = getattr(error, "exceptions", ())
    return any(_contains_cleanup_failure(child) for child in nested)


def _contains_infrastructure_failure(error: BaseException) -> bool:
    if isinstance(error, CaseInfrastructureFailure):
        return True
    nested = getattr(error, "exceptions", ())
    return any(_contains_infrastructure_failure(child) for child in nested)


def _discovery_failure_summary(error: BaseException) -> dict[str, object]:
    rendered = str(error)
    return {
        "type": _safe_protocol_label(type(error).__name__),
        "chars": len(rendered),
        "sha256": hashlib.sha256(
            rendered.encode("utf-8", errors="replace")
        ).hexdigest(),
    }


def _assert_frozen_manifest_bytes(
    manifest_paths: dict[str, Path], frozen_bytes: dict[str, bytes]
) -> None:
    for mode in ("forward", "lifecycle"):
        if manifest_paths[mode].read_bytes() != frozen_bytes[mode]:
            raise AssertionError(f"{mode} manifest changed during evaluation")


def _write_discovery_report(destination: Path, report: dict[str, object]) -> None:
    report_path = destination / "discovery-sweep-report.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(report_path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(
            json.dumps(
                report, ensure_ascii=False, indent=2, sort_keys=True
            ).encode("utf-8")
            + b"\n"
        )
        output.flush()
        os.fsync(output.fileno())


def _raise_discovery_abort(
    reasons: list[str], errors: list[BaseException]
) -> None:
    if len(errors) == 1:
        cause = errors[0]
    else:
        group_type = (
            ExceptionGroup
            if all(isinstance(error, Exception) for error in errors)
            else BaseExceptionGroup
        )
        cause = group_type("discovery sweep hard failures", errors)
    raise DiscoverySweepAbort(
        "discovery sweep aborted: " + ", ".join(reasons)
    ) from cause


def run_discovery_sweep(
    repo_root: Path,
    *,
    manifest_paths: dict[str, Path] | None = None,
    runtime_factory=None,
    case_safety_check: Callable[[dict, str], None] | None = None,
    destination: Path | None = None,
) -> dict[str, object]:
    """Run every frozen case without publishing authoritative result artifacts."""

    manifest_paths = manifest_paths or {
        "forward": repo_root / "tests/skill_evals/observing_workflows_cases.json",
        "lifecycle": repo_root
        / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    }
    frozen_bytes = {name: path.read_bytes() for name, path in manifest_paths.items()}
    manifests = {
        name: json.loads(raw.decode("utf-8"))
        for name, raw in frozen_bytes.items()
    }
    validate_frozen_manifests(manifest_paths, manifests, frozen_bytes)
    try:
        _assert_frozen_manifest_bytes(manifest_paths, frozen_bytes)
    except BaseException as error:
        _raise_discovery_abort(["manifest integrity"], [error])

    if destination is None:
        destination = Path(
            tempfile.mkdtemp(prefix="observing-workflows-discovery-sweep-")
        )
    destination.mkdir(parents=True, exist_ok=True)
    destination = destination.resolve(strict=True)
    production = snapshot_production(repo_root)
    cases: list[dict[str, object]] = []

    for mode in ("forward", "lifecycle"):
        for index, case in enumerate(manifests[mode], 1):
            print(
                f"[sweep {mode} {index}/{len(manifests[mode])}] {case['id']}",
                flush=True,
            )
            case_error: BaseException | None = None
            try:
                _run_case(
                    case,
                    destination,
                    lifecycle=mode == "lifecycle",
                    runtime_factory=runtime_factory,
                )
            except BaseException as error:
                case_error = error

            safety_error: BaseException | None = None
            if case_safety_check is not None:
                try:
                    case_safety_check(case, mode)
                except BaseException as error:
                    safety_error = error

            production_error: BaseException | None = None
            try:
                assert_production_unchanged(production)
            except BaseException as error:
                production_error = error

            manifest_error: BaseException | None = None
            try:
                _assert_frozen_manifest_bytes(manifest_paths, frozen_bytes)
            except BaseException as error:
                manifest_error = error

            reasons = []
            hard_errors: list[BaseException] = []
            if case_error is not None:
                if _contains_cleanup_failure(case_error):
                    reasons.append("cleanup")
                if (
                    _contains_infrastructure_failure(case_error)
                    or not isinstance(case_error, Exception)
                ):
                    reasons.append("case infrastructure")
            if safety_error is not None:
                reasons.append("case safety")
            if production_error is not None:
                reasons.append("production fingerprint")
            if manifest_error is not None:
                reasons.append("manifest integrity")
            if reasons:
                hard_errors.extend(
                    error
                    for error in (
                        case_error,
                        safety_error,
                        production_error,
                        manifest_error,
                    )
                    if error is not None
                )
                _raise_discovery_abort(reasons, hard_errors)

            row: dict[str, object] = {
                "mode": mode,
                "id": case["id"],
                "status": "passed" if case_error is None else "failed",
            }
            if case_error is not None:
                row["failure"] = _discovery_failure_summary(case_error)
            cases.append(row)

    report: dict[str, object] = {
        "schema_version": 1,
        "authoritative": False,
        "complete": len(cases) == sum(len(rows) for rows in manifests.values()),
        "passed": sum(row["status"] == "passed" for row in cases),
        "failed": sum(row["status"] == "failed" for row in cases),
        "cases": cases,
    }
    _write_discovery_report(destination, report)
    return report


def run_infrastructure_preflight(repo_root: Path) -> None:
    case = {
        "id": "infrastructure-preflight",
        "turns": [
            {
                "prompt": (
                    "Discuss whether parser diagnostics should include line numbers. "
                    "Do not modify files."
                ),
                "dispatch_when": "immediate",
            }
        ],
        "fixture": "empty",
        "expected_decisions": [{"after_turn": 1, "triggered": False}],
        "task_type": None,
        "workflow_variant": None,
        "expected_record_checkpoints": [{"after_turn": 1, "records": []}],
        "expected_run_count": 0,
        "expected_final_statuses": [],
    }
    production = snapshot_production(repo_root)
    with tempfile.TemporaryDirectory(
        prefix="observing-workflows-task9-preflight-"
    ) as temporary:
        run_with_production_guard(
            lambda: _run_case(
                case, Path(temporary).resolve(strict=True), lifecycle=False
            ),
            lambda: assert_production_unchanged(production),
        )


def run_trigger_preflight(repo_root: Path) -> None:
    case = {
        "id": "trigger-infrastructure-preflight",
        "turns": [
            {
                "prompt": (
                    "The design and requirements are already approved. Implement JSON output "
                    "across src/parser.py, src/cli.py, and tests/test_cli.py now, run the "
                    "fixture tests, and do not pause for clarification."
                ),
                "dispatch_when": "immediate",
            }
        ],
        "fixture": "python-cli",
        "expected_decisions": [{"after_turn": 1, "triggered": True}],
        "task_type": "feature",
        "workflow_variant": "implementation-basic",
        "expected_record_checkpoints": [
            {
                "after_turn": 1,
                "records": [
                    {
                        "role": "run-1",
                        "status": "success",
                        "start_mode": "planned",
                        "superseded_by_role": None,
                    }
                ],
            }
        ],
        "expected_run_count": 1,
        "expected_final_statuses": ["success"],
    }
    production = snapshot_production(repo_root)
    with tempfile.TemporaryDirectory(
        prefix="observing-workflows-task9-trigger-preflight-"
    ) as temporary:
        run_with_production_guard(
            lambda: _run_case(
                case, Path(temporary).resolve(strict=True), lifecycle=False
            ),
            lambda: assert_production_unchanged(production),
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--repository-root", type=Path)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--trigger-preflight", action="store_true")
    arguments = parser.parse_args()
    repo_root = arguments.repo_root.resolve(strict=True)
    if arguments.preflight:
        run_infrastructure_preflight(repo_root)
        print("Task 9 app-server infrastructure preflight passed.")
        return 0
    if arguments.trigger_preflight:
        run_trigger_preflight(repo_root)
        print("Task 9 trigger infrastructure preflight passed.")
        return 0
    if arguments.repository_root is None:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--show-toplevel"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        rendered = completed.stdout.rstrip("\n")
        if completed.returncode != 0 or not rendered or "\n" in rendered:
            parser.error("formal evaluation requires an exact Git repository root")
        candidate = Path(rendered)
    else:
        candidate = arguments.repository_root
    try:
        repository_root, _ = sharding_core._canonical_git_repository_root(candidate)
    except (OSError, RuntimeError, TypeError, ValueError):
        parser.error("formal evaluation requires an exact Git repository root")
    run_suite(
        repo_root,
        repository_root=repository_root,
        coordinator_role="serial-coordinator",
    )
    print("Task 9 frozen evaluations completed and persisted atomically.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
