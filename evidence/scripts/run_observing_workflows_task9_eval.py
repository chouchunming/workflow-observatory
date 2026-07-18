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
import stat
import subprocess
import sys
import tempfile
import threading
import time
from typing import Literal, Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
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
    validate_id_set,
    validate_manifest_schema,
    validate_result_schema,
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
    writable_roots: list[Path]
    selected_command: str = CENTRAL_COMMAND
    disabled_skill_paths: tuple[Path, ...] = ()
    integrity_command: tuple[str, ...] | None = None
    audited_wrapper_path: Path | None = None
    audited_wrapper_content: str | None = None


TransportName = Literal["exec", "app-server"]


class CaseCleanupFailure(RuntimeError):
    """A case transport could not prove that its transient resources were cleaned."""


class CaseInfrastructureFailure(RuntimeError):
    """An isolated case could not establish its required runtime infrastructure."""


class DiscoverySweepAbort(RuntimeError):
    """A discovery sweep encountered a suite-integrity failure and must stop."""


@dataclass(frozen=True)
class CaseExecution:
    terminal_status: str
    final_text: str
    command_executions: tuple[str, ...]
    observation_command_diagnostics: tuple[dict[str, object], ...]


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
    environment: dict[str, str], disabled_skill_paths: tuple[Path, ...]
) -> tuple[str, ...]:
    overrides = [
        build_shell_environment_override(environment),
        'approval_policy="never"',
        'web_search="disabled"',
        "features.multi_agent=true",
    ]
    if disabled_skill_paths:
        overrides.append(build_disabled_skills_override(disabled_skill_paths))
    return tuple(overrides)


def build_exec_command(
    cwd: Path,
    writable_roots: list[Path],
    output_path: Path,
    overrides: tuple[str, ...],
) -> list[str]:
    command = [
        "codex", "exec", "--json", "--ephemeral", "--ignore-rules",
        "--sandbox", "workspace-write", "-C", str(cwd),
        "-o", str(output_path),
    ]
    for override in overrides:
        command.extend(("-c", override))
    for root in writable_roots:
        command.extend(("--add-dir", str(root)))
    command.append("-")
    return command


def parse_exec_jsonl(stdout: str, final_text: str) -> CaseExecution:
    active_commands: dict[str, str] = {}
    command_executions: list[str] = []
    observation_diagnostics: list[dict[str, object]] = []
    agent_messages: list[str] = []
    terminal_count = 0
    terminal_seen = False
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
    if agent_messages[-1].rstrip("\n") != final_text.rstrip("\n"):
        raise ValueError("codex exec final message mismatch")
    return CaseExecution(
        terminal_status="completed",
        final_text=agent_messages[-1],
        command_executions=tuple(command_executions),
        observation_command_diagnostics=tuple(observation_diagnostics),
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

    def run(
        self, prompt: str, timeout: float = EXEC_TURN_TIMEOUT_SECONDS
    ) -> CaseExecution:
        output_path = self.runtime.audit.root / "exec-final-message.txt"
        overrides = build_codex_config_overrides(
            self.runtime.environment, self.runtime.disabled_skill_paths
        )
        command = build_exec_command(
            self.cwd, self.runtime.writable_roots, output_path, overrides
        )
        self._remove_output(output_path)
        stdout = ""
        stderr = ""
        try:
            try:
                process = self.popen_factory(
                    command,
                    cwd=self.cwd,
                    env={**os.environ, **self.runtime.environment},
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except BaseException:
                raise CaseInfrastructureFailure(
                    "codex exec startup failed"
                ) from None
            sanitized_timeout: TimeoutError | None = None
            cleanup_failure: CaseCleanupFailure | None = None
            try:
                stdout, stderr = process.communicate(input=prompt, timeout=timeout)
            except subprocess.TimeoutExpired as error:
                event_summary = _exec_event_summary(error.stdout)
                stdout_summary = _bounded_sensitive_summaries(
                    (_coerce_diagnostic_text(error.stdout),), limit=1
                )
                stderr_summary = _bounded_sensitive_summaries(
                    (_coerce_diagnostic_text(error.stderr),), limit=1
                )
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        cleanup_failure = CaseCleanupFailure(
                            "codex exec cleanup failed after kill: "
                            f"pid={process.pid}; returncode={process.poll()}"
                        )
                sanitized_timeout = TimeoutError(
                    "codex exec timeout: "
                    f"pid={process.pid}; returncode={process.poll()}; "
                    f"events={event_summary!r}; "
                    f"stdout={stdout_summary!r}; stderr={stderr_summary!r}"
                )
            if cleanup_failure is not None:
                raise cleanup_failure from None
            if sanitized_timeout is not None:
                raise sanitized_timeout from None
            stdout = _coerce_diagnostic_text(stdout)
            stderr = _coerce_diagnostic_text(stderr)
            if process.returncode != 0:
                raise RuntimeError(
                    "codex exec failed: "
                    f"pid={process.pid}; returncode={process.returncode}; "
                    f"events={_exec_event_summary(stdout)!r}; "
                    f"stdout={_bounded_sensitive_summaries((stdout,), limit=1)!r}; "
                    f"stderr={_bounded_sensitive_summaries((stderr,), limit=1)!r}"
                )
            if not output_path.is_file():
                raise RuntimeError("codex exec final message is missing")
            return parse_exec_jsonl(
                stdout, output_path.read_text(encoding="utf-8").rstrip("\n")
            )
        finally:
            self._remove_output(output_path)

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


def persist_result_pair(
    destinations: dict[str, Path],
    results: dict[str, list[dict]],
    manifests: dict[str, list[dict]],
    *,
    crash_at: str | None = None,
) -> Path:
    """Commit a pair through immutable generations and one atomic pointer."""

    _validate_result_pair(results, manifests)
    parent = destinations["forward"].parent.absolute()
    if destinations["lifecycle"].parent.absolute() != parent:
        raise ValueError("paired result destinations must share one directory")
    parent_fd = _open_directory_path(parent, create=True)
    try:
        generation_fd = _open_child_directory(
            parent_fd, RESULT_GENERATION_DIRECTORY, create=True
        )
    except Exception:
        os.close(parent_fd)
        raise
    os.fsync(parent_fd)

    contents = {mode: _json_bytes(results[mode]) for mode in ("forward", "lifecycle")}
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
    staged: list[tuple[int, str]] = []
    try:
        for mode in ("forward", "lifecycle"):
            destination = generation_names[mode]
            try:
                existing = _read_regular_at(generation_fd, destination, "result generation")
            except FileNotFoundError:
                existing = None
            if existing is not None:
                if existing != contents[mode]:
                    raise AssertionError("immutable result generation hash collision")
            else:
                temporary = _stage_bytes_at(generation_fd, destination, contents[mode])
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
        pointer_temporary = _stage_bytes_at(
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
        if resolve_committed_result_pair(pointer, manifests) != results:
            raise AssertionError("committed result pair readback mismatch")
        return pointer
    finally:
        for directory_fd, temporary in staged:
            try:
                os.unlink(temporary, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
        os.close(generation_fd)
        os.close(parent_fd)


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
        environment: dict[str, str],
        disabled_skill_paths: tuple[Path, ...] = (),
    ):
        command = ["codex", "app-server", "--stdio"]
        for override in build_codex_config_overrides(
            environment, disabled_skill_paths
        ):
            command.extend(("-c", override))
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.messages: queue.Queue[dict] = queue.Queue()
        self.stderr_tail: deque[str] = deque(maxlen=80)
        self.events: list[dict] = []
        self.agent_messages: list[str] = []
        self.command_executions: list[str] = []
        self.observation_command_diagnostics: list[dict] = []
        self.completed_turns: dict[str, dict] = {}
        self.active_command_executions: dict[str, str] = {}
        self._request_id = 0
        self._stdout_thread = threading.Thread(target=self._read_stdout, daemon=True)
        self._stderr_thread = threading.Thread(target=self._read_stderr, daemon=True)
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _read_stdout(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            try:
                self.messages.put(json.loads(line))
            except json.JSONDecodeError:
                self.stderr_tail.append(f"non-JSON stdout: {line.rstrip()}")

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
        self._record(message)
        if (
            "id" in message
            and isinstance(message.get("method"), str)
            and "result" not in message
            and "error" not in message
        ):
            raise RuntimeError(
                "unsupported app-server request: "
                f"method={message['method']} id={message['id']}"
            )

    def request(self, method: str, params: dict, timeout: float = 30) -> dict:
        self._request_id += 1
        request_id = self._request_id
        self._send({"method": method, "id": request_id, "params": params})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = self._receive(max(0.01, deadline - time.monotonic()))
            if message.get("id") != request_id:
                continue
            if "error" in message:
                raise RuntimeError(f"{method} failed: {message['error']}")
            return message.get("result") or {}
        raise TimeoutError(f"timed out waiting for {method} response")

    def notify(self, method: str, params: dict) -> None:
        self._send({"method": method, "params": params})

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
        return result["thread"]["id"]

    def start_turn(
        self,
        thread_id: str,
        prompt: str,
        cwd: Path,
        writable_roots: list[Path],
    ) -> str:
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
        return result["turn"]["id"]

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
                    raise RuntimeError(f"turn ended with status {turn.get('status')}")
                return turn
            self._receive(max(0.01, deadline - time.monotonic()))
        raise TimeoutError("turn timed out")

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


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


def build_case_fixture(case: dict, destination: Path) -> Path:
    include_gate = any(
        "scripts/gate.py" in turn.get("prompt", "") for turn in case["turns"]
    )
    workspace = build_fixture(
        case["id"],
        case["fixture"],
        destination,
        include_gate=include_gate,
        include_failure_script=case["id"] == "task-failure",
    )
    install_evaluator_guidance(workspace)
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
) -> CaseExecution:
    route = select_case_transport(case)
    if route == "exec":
        if after_first_turn is not None:
            raise ValueError("exec transport cannot accept a first-turn checkpoint")
        return ExecTransport(workspace, runtime).run(case["turns"][0]["prompt"])
    if after_first_turn is None:
        raise ValueError("app-server transport requires a first-turn checkpoint")
    try:
        server = AppServer(
            workspace, runtime.environment, runtime.disabled_skill_paths
        )
    except CaseInfrastructureFailure:
        raise
    except BaseException:
        raise CaseInfrastructureFailure(
            "app-server startup failed"
        ) from None
    gate_released = False
    execution = None
    execution_error: BaseException | None = None
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
        if not server.agent_messages:
            raise RuntimeError("app-server final agent message is missing")
        execution = CaseExecution(
            terminal_status="completed",
            final_text=server.agent_messages[-1],
            command_executions=tuple(server.command_executions),
            observation_command_diagnostics=tuple(
                server.observation_command_diagnostics
            ),
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

    cleanup_failures = []
    for error in cleanup_errors:
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


def _run_case(
    case: dict,
    destination: Path,
    lifecycle: bool,
    runtime_factory=None,
) -> dict:
    try:
        case_root = destination / ("lifecycle" if lifecycle else "forward")
        case_root.mkdir(exist_ok=True)
        workspace = build_case_fixture(case, case_root)
        if runtime_factory is None:
            wiki_root = case_root / f"{case['id']}-wiki-root"
            wiki_root.mkdir(mode=0o700)
            audit = build_payload_audit(case["id"], case_root, CENTRAL_CLI)
            runtime = CaseRuntime(
                wiki_root,
                audit,
                _case_environment(case, audit, wiki_root, lifecycle),
                [wiki_root, audit.root],
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
    except (CaseCleanupFailure, CaseInfrastructureFailure):
        raise
    except BaseException:
        raise CaseInfrastructureFailure(
            "isolated case setup failed"
        ) from None
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
    execution = execute_case_transport(
        case,
        workspace,
        runtime,
        wiki_root,
        capture_first_turn if route == "app-server" else None,
    )
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


def run_suite(
    repo_root: Path,
    *,
    manifest_paths: dict[str, Path] | None = None,
    result_destinations: dict[str, Path] | None = None,
    runtime_factory=None,
) -> tuple[list[dict], list[dict]]:
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
    _assert_frozen_manifest_bytes(manifest_paths, frozen_bytes)
    production = snapshot_production(repo_root)
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
        assert_production_unchanged(production)
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
        persist_result_pair(
            result_destinations or {
                "forward": repo_root
                / "tests/skill_evals/observing_workflows_forward.json",
                "lifecycle": repo_root
                / "tests/skill_evals/observing_workflows_lifecycle_forward.json",
            },
            results,
            manifests,
        )
        return results["forward"], results["lifecycle"]


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
    run_suite(repo_root)
    print("Task 9 frozen evaluations completed and persisted atomically.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
