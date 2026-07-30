#!/usr/bin/env python3
"""Isolated worker runtime primitives for parallel Workflow Observatory evals."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time
from types import ModuleType
from typing import Callable, Protocol, Sequence

from scripts.run_observing_workflows_task9_eval import (
    AUTH_CLEANUP_MAX_DEPTH,
    AUTH_CLEANUP_MAX_ENTRIES,
    CaseCleanupFailure,
    CaseEventSink,
    CaseExecution,
    CaseInfrastructureFailure,
    CaseRuntime,
    CaseTransportFailure,
    ProcessSurvivalCleanupFailure,
    RuntimePayloadAudit,
    _contains_process_survival_failure,
    _raise_case_and_auth_cleanup_failures,
    build_embedded_audit_wrapper,
    inventory_external_skill_paths,
)
from scripts.workflow_eval_sharding import (
    Ack,
    CaseAssignment,
    CaseAuthOwnership,
    CasePaths,
    EpochPlan,
    FailureSummary,
    InstalledCaseAuth,
    MAX_PROTOCOL_RECORDS,
    ProgressMessage,
    ResolvedTransportConfig,
    RetryDecision,
    ShardTerminal,
    TombstoneReceipt,
    TokenUsage,
    WorkerDependencies,
    _DescriptorSlot,
    _atomic_write_record,
    _decode_epoch_plan_record,
    _decode_case_key,
    _decode_resume_plan_record,
    _read_canonical_record,
    _read_diagnostic_execution_scope,
    _register_progress_epoch_context,
    _require_exact_fields,
    _retire_descriptor_capability,
    _tombstone_receipt_from_payload as _receipt_from_payload,
    _validate_wait_timeout,
    _write_progress_with_deadline,
    canonical_run_root,
    canonical_config_bytes,
    decide_retry,
    install_case_auth,
    is_indeterminate_descriptor_close,
    paths_for_case,
    read_attempt_seal,
    read_case_auth_ownership as _read_case_auth_ownership,
    read_case_seal,
    read_progress,
    read_tombstone_receipt,
    scan_attempts,
    seal_case,
    seal_shard,
    stage_marketplace_for_case,
    wait_for_ack,
    write_attempt_start,
    write_attempt_terminal,
)


_MARKETPLACE_RELATIVE = Path("workflow-observatory")
_PLUGIN_RELATIVE = Path("plugins/workflow-observer")
_CLI_RELATIVE = _PLUGIN_RELATIVE / "scripts/workflow_observer_cli.py"


class RuntimeFactory(Protocol):
    @property
    def poisoned(self) -> bool: ...

    def __call__(
        self,
        *,
        assignment: CaseAssignment,
        manifest_case: dict[str, object],
        paths: CasePaths,
        transport_config: ResolvedTransportConfig,
    ) -> CaseRuntime: ...

    def cleanup_case(self, paths: CasePaths) -> TombstoneReceipt: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class DrivenCase:
    result: dict[str, object]
    execution: CaseExecution


@dataclass(frozen=True)
class _CapturedFailureFact:
    kind: str
    classification: str | None
    retryable: bool | None


class _CapturedEvaluatorFailure(RuntimeError):
    def __init__(self, facts: tuple[_CapturedFailureFact, ...]) -> None:
        if not facts or any(
            type(fact) is not _CapturedFailureFact for fact in facts
        ):
            raise TypeError("captured failure facts must be nonempty and exact")
        self.facts = facts
        super().__init__("captured evaluator failure")


def _captured_exception_type(
    evaluator: ModuleType, name: str
) -> type[BaseException] | None:
    candidate = getattr(evaluator, name, None)
    if (
        isinstance(candidate, type)
        and issubclass(candidate, BaseException)
    ):
        return candidate
    return None


def _normalize_captured_failure(
    *,
    evaluator: ModuleType,
    error: BaseException,
    survival_hint: bool,
) -> _CapturedEvaluatorFailure:
    survival_type = _captured_exception_type(
        evaluator, "ProcessSurvivalCleanupFailure"
    )
    cleanup_type = _captured_exception_type(
        evaluator, "CaseCleanupFailure"
    )
    transport_type = _captured_exception_type(
        evaluator, "CaseTransportFailure"
    )
    infrastructure_type = _captured_exception_type(
        evaluator, "CaseInfrastructureFailure"
    )
    facts: list[_CapturedFailureFact] = []

    def visit(current: BaseException) -> None:
        nested = getattr(current, "exceptions", ())
        if isinstance(nested, (tuple, list)) and nested:
            for child in nested:
                if isinstance(child, BaseException):
                    visit(child)
            return
        if survival_type is not None and isinstance(current, survival_type):
            facts.append(_CapturedFailureFact("survival", None, False))
        elif cleanup_type is not None and isinstance(current, cleanup_type):
            facts.append(_CapturedFailureFact("cleanup", None, False))
        elif transport_type is not None and isinstance(current, transport_type):
            classification = getattr(current, "classification", None)
            retryable = getattr(current, "retryable", None)
            facts.append(
                _CapturedFailureFact(
                    "transport",
                    classification if type(classification) is str else None,
                    retryable if type(retryable) is bool else None,
                )
            )
        elif (
            infrastructure_type is not None
            and isinstance(current, infrastructure_type)
        ):
            facts.append(_CapturedFailureFact("infrastructure", None, True))
        elif isinstance(current, TimeoutError):
            facts.append(_CapturedFailureFact("timeout", "timeout", False))
        elif isinstance(current, AssertionError):
            facts.append(_CapturedFailureFact("semantic", "semantic", False))
        else:
            facts.append(_CapturedFailureFact("unknown", None, None))

    visit(error)
    if survival_hint and not any(fact.kind == "survival" for fact in facts):
        facts.append(_CapturedFailureFact("survival", None, False))
    return _CapturedEvaluatorFailure(tuple(facts))


class CaseTransport(Protocol):
    def __call__(
        self,
        case: dict[str, object],
        workspace: Path,
        runtime: CaseRuntime,
        wiki_root: Path,
        after_first_turn: Callable[[], None] | None = None,
        event_sink: CaseEventSink | None = None,
    ) -> CaseExecution: ...


class CaseDriver(Protocol):
    def __call__(
        self,
        *,
        assignment: CaseAssignment,
        manifest_case: dict[str, object],
        paths: CasePaths,
        runtime_factory: RuntimeFactory,
        event_sink: CaseEventSink,
    ) -> DrivenCase: ...


def worker_exit_required(
    error: BaseException, factory: RuntimeFactory
) -> bool:
    if not isinstance(error, BaseException):
        raise TypeError("error must be a BaseException")
    captured_survival = any(
        isinstance(leaf, _CapturedEvaluatorFailure)
        and any(fact.kind == "survival" for fact in leaf.facts)
        for leaf in _worker_failure_leaves(error)
    )
    return (
        factory.poisoned
        or captured_survival
        or is_indeterminate_descriptor_close(error)
    )


def publish_progress_and_wait_for_ack(
    *,
    worker_root: Path,
    message: ProgressMessage,
    timeout: float,
    wakeup_sink: Callable[[dict[str, object]], None] | None = None,
) -> Ack:
    """Publish durable progress, emit a content-free wake-up, then block on ACK."""

    if wakeup_sink is not None and not callable(wakeup_sink):
        raise TypeError("wakeup_sink must be callable or None")
    timeout_value = _validate_wait_timeout(timeout)
    operation_deadline = time.monotonic() + timeout_value
    _, message_sha256 = _write_progress_with_deadline(
        worker_root,
        message,
        operation_deadline=operation_deadline,
    )
    wakeup = {
        "lane": message.lane,
        "seq": message.seq,
        "sha256": message_sha256,
    }
    if wakeup_sink is None:
        sys.stdout.write(
            json.dumps(
                wakeup,
                sort_keys=True,
                ensure_ascii=True,
                separators=(",", ":"),
            )
            + "\n"
        )
        sys.stdout.flush()
    else:
        wakeup_sink(wakeup)
    remaining = max(0.0, operation_deadline - time.monotonic())
    return wait_for_ack(worker_root, message, remaining)


def _load_captured_evaluator(snapshot_root: Path) -> ModuleType:
    try:
        snapshot = Path(snapshot_root).resolve(strict=True)
    except OSError:
        raise ValueError("captured evaluator root is unavailable") from None
    relative = Path("evidence/scripts/run_observing_workflows_task9_eval.py")
    candidates = (
        snapshot / relative,
        snapshot / "workflow-observatory" / relative,
    )
    matches = []
    for candidate in candidates:
        try:
            metadata = candidate.lstat()
        except OSError:
            continue
        if (
            stat.S_ISREG(metadata.st_mode)
            and not stat.S_ISLNK(metadata.st_mode)
            and stat.S_IMODE(metadata.st_mode) == 0o444
        ):
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError("captured evaluator source is missing or ambiguous")
    source = matches[0]
    try:
        if (
            source.resolve(strict=True) != source
            or not source.is_relative_to(snapshot)
        ):
            raise ValueError("captured evaluator source is non-canonical")
        source_bytes = source.read_bytes()
    except OSError:
        raise ValueError("captured evaluator source is unavailable") from None
    digest = hashlib.sha256(
        os.fsencode(str(source)) + b"\0" + source_bytes
    ).hexdigest()
    module_name = f"_workflow_eval_captured_{digest}"
    existing = sys.modules.get(module_name)
    if isinstance(existing, ModuleType):
        return existing
    module = ModuleType(module_name)
    module.__file__ = str(source)
    sys.modules[module_name] = module
    try:
        exec(compile(source_bytes, str(source), "exec"), module.__dict__)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    if not callable(getattr(module, "_run_case", None)):
        raise ValueError("captured evaluator entry point is missing")
    return module


class _ProductionCaseDriver:
    def __init__(
        self,
        *,
        evaluator: ModuleType,
        transport_config: ResolvedTransportConfig,
        transport_runner: CaseTransport,
    ) -> None:
        self._evaluator = evaluator
        self._transport_config = transport_config
        self._transport_runner = transport_runner

    def __call__(
        self,
        *,
        assignment: CaseAssignment,
        manifest_case: dict[str, object],
        paths: CasePaths,
        runtime_factory: RuntimeFactory,
        event_sink: CaseEventSink,
    ) -> DrivenCase:
        if runtime_factory.poisoned:
            raise ValueError("runtime factory is poisoned")
        expected_paths = paths_for_case(paths.root.parent.parent, assignment)
        if paths != expected_paths:
            raise ValueError("case driver paths differ from frozen assignment")
        if manifest_case.get("id") != assignment.key.case_id:
            raise ValueError("case driver manifest ID differs from assignment")
        workspace_parent = paths.workspace.parent
        _secure_directory(
            workspace_parent,
            anchor=paths.root.parent.parent,
        )
        if paths.workspace.exists() or paths.workspace.is_symlink():
            raise ValueError("canonical case workspace already exists")

        runtime_created = False
        execution: CaseExecution | None = None

        def runtime_adapter(case, case_root, workspace, lifecycle):
            nonlocal runtime_created
            if (
                case is not manifest_case
                or case_root != paths.root
                or workspace != paths.workspace
                or lifecycle != (assignment.key.mode == "lifecycle")
            ):
                raise ValueError("captured evaluator workspace binding changed")
            runtime = runtime_factory(
                assignment=assignment,
                manifest_case=manifest_case,
                paths=paths,
                transport_config=self._transport_config,
            )
            runtime_created = True
            return runtime

        def capture_execution(value: CaseExecution) -> None:
            nonlocal execution
            if execution is not None:
                raise ValueError("captured evaluator reported multiple executions")
            execution = value

        result: dict[str, object] | None = None
        primary: BaseException | None = None
        try:
            result = self._evaluator._run_case(
                case=manifest_case,
                destination=paths.root,
                lifecycle=assignment.key.mode == "lifecycle",
                runtime_factory=runtime_adapter,
                workspace_parent=workspace_parent,
                transport_runner=self._transport_runner,
                event_sink=event_sink,
                execution_sink=capture_execution,
            )
        except BaseException as error:
            primary = error

        if primary is not None:
            captured_detector = getattr(
                self._evaluator,
                "_contains_process_survival_failure",
                None,
            )
            captured_survival = False
            if callable(captured_detector):
                try:
                    captured_survival = captured_detector(primary)
                except BaseException:
                    # If the captured evaluator cannot prove quiescence, never
                    # scrub credentials from this worker process.
                    captured_survival = True
                if type(captured_survival) is not bool:
                    captured_survival = True
            primary = _normalize_captured_failure(
                evaluator=self._evaluator,
                error=primary,
                survival_hint=captured_survival,
            )
            if (
                captured_survival
                or _contains_process_survival_failure(primary)
                or worker_exit_required(primary, runtime_factory)
            ):
                raise primary

        cleanup_errors: list[BaseException] = []
        if runtime_created:
            try:
                runtime_factory.cleanup_case(paths)
            except BaseException as error:
                cleanup_errors.append(error)
        _raise_case_and_auth_cleanup_failures(primary, cleanup_errors)
        if runtime_factory.poisoned:
            raise AssertionError("poisoned factory returned from case cleanup")
        if result is None or execution is None:
            raise ValueError("captured evaluator omitted case result or execution")
        return DrivenCase(result=result, execution=execution)


def build_production_case_driver(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    transport_runner: CaseTransport | None = None,
) -> CaseDriver:
    if not isinstance(transport_config, ResolvedTransportConfig):
        raise TypeError("transport_config must be ResolvedTransportConfig")
    evaluator = _load_captured_evaluator(Path(snapshot_root))
    runner = (
        getattr(evaluator, "execute_case_transport")
        if transport_runner is None
        else transport_runner
    )
    if not callable(runner):
        raise TypeError("transport_runner must be callable")
    return _ProductionCaseDriver(
        evaluator=evaluator,
        transport_config=transport_config,
        transport_runner=runner,
    )


def _secure_directory(path: Path, *, anchor: Path | None = None) -> None:
    path = Path(path)
    anchor = path if anchor is None else Path(anchor)
    try:
        relative = path.relative_to(anchor)
        anchor_metadata = anchor.lstat()
    except ValueError:
        raise ValueError("isolated runtime path escapes its root") from None
    except OSError:
        raise ValueError("isolated runtime root is unavailable") from None
    if stat.S_ISLNK(anchor_metadata.st_mode) or not stat.S_ISDIR(
        anchor_metadata.st_mode
    ):
        raise ValueError("isolated runtime root must be a real non-symlink directory")
    try:
        anchor.chmod(0o700)
    except OSError:
        raise ValueError("could not secure isolated runtime root") from None

    current = anchor
    for part in relative.parts:
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            try:
                current.mkdir(mode=0o700)
                metadata = current.lstat()
            except OSError:
                raise ValueError(
                    "could not create isolated runtime directory"
                ) from None
        except OSError:
            raise ValueError("isolated runtime directory is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError(
                "isolated runtime path contains a symlink or non-directory"
            )
        try:
            current.chmod(0o700)
        except OSError:
            raise ValueError(
                "could not secure isolated runtime directory"
            ) from None


def _captured_marketplace_root(snapshot_root: Path) -> Path:
    snapshot = Path(snapshot_root)
    candidates = (snapshot, snapshot / _MARKETPLACE_RELATIVE)
    matches = [
        candidate
        for candidate in candidates
        if (candidate / _PLUGIN_RELATIVE).is_dir()
        and not (candidate / _PLUGIN_RELATIVE).is_symlink()
    ]
    if len(matches) != 1:
        raise ValueError("captured marketplace root is missing or ambiguous")
    marketplace = matches[0]
    try:
        metadata = marketplace.lstat()
    except OSError:
        raise ValueError("captured marketplace root is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("captured marketplace root must be a real directory")
    return marketplace


def _discover_auth_bootstrap(paths: CasePaths) -> Path:
    coordinator = paths.root.parent.parent / "coordinator"
    candidate = coordinator / "auth-bootstrap"
    try:
        metadata = coordinator.lstat()
        candidate_metadata = candidate.lstat()
    except OSError:
        raise ValueError("isolated auth bootstrap is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("isolated auth bootstrap is unavailable")
    if (
        stat.S_ISLNK(candidate_metadata.st_mode)
        or not stat.S_ISDIR(candidate_metadata.st_mode)
        or stat.S_IMODE(candidate_metadata.st_mode) != 0o700
    ):
        raise ValueError("isolated auth bootstrap is unsafe")
    return candidate


def _write_portable_store_config(home: Path, store: Path) -> None:
    config_path = home / "config.json"
    payload = json.dumps(
        {"schema_version": 1, "adapter": "portable", "root": str(store)},
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii") + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(config_path, flags, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        config_path.chmod(0o600)
    except OSError:
        raise ValueError("could not create isolated store config") from None


def _install_fixture_skills(staged_plugin: Path, workspace: Path) -> tuple[Path, ...]:
    skills_source = staged_plugin / "skills"
    try:
        source_metadata = skills_source.lstat()
        entries = sorted(skills_source.iterdir(), key=lambda path: path.name)
    except OSError:
        raise ValueError("captured plugin skills are unavailable") from None
    if stat.S_ISLNK(source_metadata.st_mode) or not stat.S_ISDIR(
        source_metadata.st_mode
    ):
        raise ValueError("captured plugin skills must be a real directory")

    skills_root = workspace / ".agents/skills"
    _secure_directory(skills_root, anchor=workspace)
    installed = []
    for source in entries:
        try:
            metadata = source.lstat()
        except OSError:
            raise ValueError("captured plugin skill is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("captured plugin skill must be a real directory")
        skill_file = source / "SKILL.md"
        try:
            skill_metadata = skill_file.lstat()
        except OSError:
            raise ValueError("captured plugin skill is missing SKILL.md") from None
        if stat.S_ISLNK(skill_metadata.st_mode) or not stat.S_ISREG(
            skill_metadata.st_mode
        ):
            raise ValueError("captured plugin SKILL.md must be a regular file")
        destination = skills_root / source.name
        if destination.exists() or destination.is_symlink():
            raise ValueError("fixture skill destination already exists")
        destination.symlink_to(source, target_is_directory=True)
        installed.append(destination / "SKILL.md")

    git_marker = workspace / ".git"
    if git_marker.exists() or git_marker.is_symlink():
        environment = {
            **os.environ,
            "GIT_AUTHOR_NAME": "Evaluation Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Evaluation Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:01+00:00",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:01+00:00",
        }
        try:
            subprocess.run(
                ["git", "add", ".agents/skills"],
                cwd=workspace,
                env=environment,
                text=True,
                capture_output=True,
                check=True,
                timeout=30,
            )
            subprocess.run(
                ["git", "commit", "-m", "Install isolated marketplace skills"],
                cwd=workspace,
                env=environment,
                text=True,
                capture_output=True,
                check=True,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            raise ValueError("could not commit isolated fixture skills") from None
    return tuple(installed)


def _prepare_case_directories(paths: CasePaths) -> None:
    run_root = canonical_run_root(paths.root.parent.parent)
    _secure_directory(run_root)
    for path in (
        paths.root,
        paths.cleanup,
        paths.attempts,
        paths.staging,
        paths.workspace,
        paths.store,
        paths.audit,
        paths.payload,
        paths.output,
        paths.home,
        paths.tmp,
        paths.config,
        paths.cache,
        paths.sealed,
    ):
        _secure_directory(path, anchor=run_root)
    if paths.codex_home != paths.root / "codex-home":
        raise ValueError("case Codex home differs from canonical path")
    if paths.codex_home.exists() or paths.codex_home.is_symlink():
        raise ValueError("isolated Codex home already exists")


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
    child_slot = _DescriptorSlot(child_descriptor)
    primary: BaseException | None = None
    try:
        child_names = []
        with os.scandir(child_slot.descriptor) as entries:
            for entry in entries:
                if remaining[0] <= 0:
                    raise OSError("owned auth cleanup bound exceeded")
                remaining[0] -= 1
                child_names.append(entry.name)
        for child_name in sorted(child_names):
            _remove_tree_entry(
                child_slot.descriptor,
                child_name,
                depth=depth + 1,
                remaining=remaining,
                charged=True,
            )
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(child_slot)
    close_errors = [close_error] if close_error is not None else []
    _raise_case_and_auth_cleanup_failures(primary, close_errors)
    os.rmdir(name, dir_fd=parent_descriptor)


def read_case_auth_ownership(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths
) -> CaseAuthOwnership:
    ownership, _ = _read_case_auth_ownership(
        plan=plan, assignment=assignment, paths=paths
    )
    return ownership


def _canonical_binding(paths: CasePaths, ownership: CaseAuthOwnership) -> str:
    try:
        metadata = paths.codex_home.lstat()
    except FileNotFoundError:
        return "missing"
    except OSError:
        raise ValueError("runtime cleanup canonical path is unavailable") from None
    if (
        stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and (metadata.st_dev, metadata.st_ino)
        == (ownership.codex_home_device, ownership.codex_home_inode)
    ):
        return "expected"
    return "replaced"


def _read_installed_ownership(
    *, installed: InstalledCaseAuth, paths: CasePaths
) -> tuple[CaseAuthOwnership, bytes]:
    payload, content = _read_canonical_record(
        paths.cleanup / "ownership.json", "case auth ownership"
    )
    _require_exact_fields(payload, CaseAuthOwnership, "case auth ownership")
    decoded = dict(payload)
    decoded["case"] = _decode_case_key(
        decoded.get("case"), "case auth ownership"
    )
    ownership = CaseAuthOwnership(**decoded)
    numeric = (
        ownership.schema_version,
        ownership.case_root_device,
        ownership.case_root_inode,
        ownership.codex_home_device,
        ownership.codex_home_inode,
    )
    if (
        any(type(value) is not int or value < 0 for value in numeric)
        or ownership.schema_version != 1
        or ownership.run_kind not in ("diagnostic", "discovery", "formal")
        or not isinstance(ownership.epoch_id, str)
        or ownership != installed.ownership
    ):
        raise ValueError("case auth ownership is stale or invalid")
    return ownership, content


def cleanup_case_auth(
    *, installed: InstalledCaseAuth, paths: CasePaths
) -> TombstoneReceipt:
    if not isinstance(installed, InstalledCaseAuth):
        raise TypeError("installed must be InstalledCaseAuth")
    path_assignment = CaseAssignment(
        key=installed.ownership.case,
        lane="E1",
        route="exec",
        manifest_sha256="0" * 64,
    )
    if paths != paths_for_case(paths.root.parent.parent, path_assignment):
        raise ValueError("runtime cleanup path is not canonical")
    durable_ownership, _ = _read_installed_ownership(
        installed=installed, paths=paths
    )
    if installed.state == "tombstoned":
        ownership, ownership_bytes = _read_installed_ownership(
            installed=installed, paths=paths
        )
        payload, _ = _read_canonical_record(
            paths.cleanup / "tombstone.json", "case auth tombstone"
        )
        receipt = _receipt_from_payload(payload)
        if (
            receipt.ownership_sha256
            != hashlib.sha256(ownership_bytes).hexdigest()
            or receipt.epoch_id != ownership.epoch_id
            or receipt.run_kind != ownership.run_kind
            or receipt.case != ownership.case
            or receipt.case_root_device != ownership.case_root_device
            or receipt.case_root_inode != ownership.case_root_inode
            or receipt.codex_home_device != ownership.codex_home_device
            or receipt.codex_home_inode != ownership.codex_home_inode
            or receipt.scrubbed is not True
            or receipt.empty is not True
            or receipt.canonical_binding not in ("expected", "missing", "replaced")
            or receipt.producer not in ("worker", "coordinator-recovery")
        ):
            raise ValueError("case auth tombstone is stale or invalid")
        if installed.descriptor_close_state == "indeterminate":
            error = installed.descriptor_close_error
            if not isinstance(error, BaseException):
                raise ValueError(
                    "indeterminate descriptor close has no stored error"
                )
            raise error
        if installed.descriptor_close_state == "owned":
            error = _retire_descriptor_capability(installed)
            if error is not None:
                raise error
        elif installed.descriptor_close_state != "closed":
            raise ValueError("runtime cleanup descriptor state is invalid")
        return receipt
    if (
        installed.state != "active"
        or installed.descriptor < 0
        or installed.descriptor_close_state != "owned"
        or installed.descriptor_close_error is not None
    ):
        raise ValueError("runtime cleanup ownership is unavailable")
    installed.state = "scrubbing"
    try:
        metadata = os.fstat(installed.descriptor)
        ownership = durable_ownership
        try:
            case_root_metadata = paths.root.lstat()
        except OSError:
            raise ValueError("runtime cleanup case root is unavailable") from None
        if (
            stat.S_ISLNK(case_root_metadata.st_mode)
            or not stat.S_ISDIR(case_root_metadata.st_mode)
            or stat.S_IMODE(case_root_metadata.st_mode) != 0o700
            or (case_root_metadata.st_dev, case_root_metadata.st_ino)
            != (ownership.case_root_device, ownership.case_root_inode)
        ):
            raise ValueError("runtime cleanup case-root ownership changed")
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_dev, metadata.st_ino)
            != (ownership.codex_home_device, ownership.codex_home_inode)
        ):
            raise ValueError("runtime cleanup Codex-home ownership changed")
        remaining = [AUTH_CLEANUP_MAX_ENTRIES - 1]
        if remaining[0] < 0:
            raise OSError("owned auth cleanup bound exceeded")
        child_names = []
        with os.scandir(installed.descriptor) as entries:
            for entry in entries:
                if remaining[0] <= 0:
                    raise OSError("owned auth cleanup bound exceeded")
                remaining[0] -= 1
                child_names.append(entry.name)
        for child_name in sorted(child_names):
            _remove_tree_entry(
                installed.descriptor,
                child_name,
                depth=1,
                remaining=remaining,
                charged=True,
            )
        after = os.fstat(installed.descriptor)
        with os.scandir(installed.descriptor) as entries:
            empty = not any(True for _ in entries)
        if (
            (after.st_dev, after.st_ino)
            != (ownership.codex_home_device, ownership.codex_home_inode)
            or stat.S_IMODE(after.st_mode) != 0o700
            or not empty
        ):
            raise ValueError("runtime cleanup did not scrub owned Codex home")
        os.fsync(installed.descriptor)
        durable_ownership, ownership_bytes = _read_installed_ownership(
            installed=installed, paths=paths
        )
        if durable_ownership != ownership:
            raise ValueError("runtime cleanup ownership changed")
        receipt = TombstoneReceipt(
            schema_version=1,
            epoch_id=ownership.epoch_id,
            run_kind=ownership.run_kind,
            case=ownership.case,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=ownership.case_root_device,
            case_root_inode=ownership.case_root_inode,
            codex_home_device=ownership.codex_home_device,
            codex_home_inode=ownership.codex_home_inode,
            scrubbed=True,
            empty=True,
            canonical_binding=_canonical_binding(paths, ownership),
            producer="worker",
        )
        _atomic_write_record(paths.cleanup / "tombstone.json", asdict(receipt))
        durable_payload, _ = _read_canonical_record(
            paths.cleanup / "tombstone.json", "case auth tombstone"
        )
        durable = _receipt_from_payload(durable_payload)
        if durable != receipt:
            raise ValueError("runtime cleanup tombstone verification failed")
    except BaseException:
        installed.state = "active"
        raise
    installed.state = "tombstoned"
    error = _retire_descriptor_capability(installed)
    if error is not None:
        raise error
    return receipt


class _ProductionRuntimeFactory:
    def __init__(
        self,
        *,
        snapshot_root: Path,
        transport_config: ResolvedTransportConfig,
        plan: EpochPlan,
    ) -> None:
        if (
            not isinstance(plan, EpochPlan)
            or type(plan.schema_version) is not int
            or plan.schema_version != 1
            or plan.run_kind not in ("diagnostic", "discovery", "formal")
            or not isinstance(plan.epoch_id, str)
            or len(plan.epoch_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in plan.epoch_id
            )
        ):
            raise TypeError("plan must be EpochPlan")
        self._marketplace = _captured_marketplace_root(snapshot_root)
        self._transport_config = transport_config
        self._plan = plan
        self._owned_cases: dict[Path, tuple[CasePaths, InstalledCaseAuth]] = {}
        self._closed = False
        self._poisoned = False
        self._terminal_error: BaseException | None = None

    @property
    def poisoned(self) -> bool:
        return self._poisoned

    @staticmethod
    def _contains_exception_identity(
        error: BaseException, candidate: BaseException
    ) -> bool:
        if error is candidate:
            return True
        if isinstance(error, BaseExceptionGroup):
            return any(
                _ProductionRuntimeFactory._contains_exception_identity(
                    nested, candidate
                )
                for nested in error.exceptions
            )
        return False

    @staticmethod
    def _compose_error_tree(
        primary: BaseException | None,
        cleanup_errors: list[BaseException],
    ) -> BaseException | None:
        try:
            _raise_case_and_auth_cleanup_failures(primary, cleanup_errors)
        except BaseException as error:
            return error
        return None

    def _retire_factory_owners(
        self, reported: BaseException | None
    ) -> list[BaseException]:
        close_errors: list[BaseException] = []
        owned = sorted(
            self._owned_cases.values(),
            key=lambda entry: entry[1].ownership.case,
        )
        for _, installed in owned:
            close_error: BaseException | None = None
            try:
                if installed.descriptor_close_state == "owned":
                    close_error = _retire_descriptor_capability(installed)
                elif installed.descriptor_close_state == "indeterminate":
                    close_error = installed.descriptor_close_error
                    if not isinstance(close_error, BaseException):
                        raise ValueError(
                            "indeterminate descriptor close has no stored error"
                        )
                elif installed.descriptor_close_state == "closed":
                    pass
                else:
                    raise ValueError(
                        "runtime factory descriptor state is invalid"
                    )
            except BaseException as error:
                close_error = error
            if close_error is None:
                continue
            if reported is not None and self._contains_exception_identity(
                reported, close_error
            ):
                continue
            close_errors.append(close_error)
        return close_errors

    def _poison(self, primary: BaseException) -> BaseException:
        if self._poisoned:
            if self._terminal_error is None:
                raise AssertionError("poisoned factory has no terminal error")
            return self._terminal_error
        self._poisoned = True
        self._closed = True
        close_errors = self._retire_factory_owners(primary)
        terminal = self._compose_error_tree(primary, close_errors)
        if terminal is None:
            raise AssertionError("factory poison produced no terminal error")
        self._terminal_error = terminal
        return terminal

    def __call__(
        self,
        *,
        assignment: CaseAssignment,
        manifest_case: dict[str, object],
        paths: CasePaths,
        transport_config: ResolvedTransportConfig,
    ) -> CaseRuntime:
        if self._poisoned:
            raise ValueError("runtime factory is poisoned")
        if transport_config != self._transport_config:
            raise ValueError("runtime transport config differs from sealed config")
        if self._closed:
            raise ValueError("runtime factory is closed")
        if assignment not in self._plan.assignments:
            raise ValueError("assignment is not bound to the epoch plan")
        if manifest_case.get("id") != assignment.key.case_id:
            raise ValueError("manifest case ID differs from assignment")
        expected_paths = paths_for_case(paths.root.parent.parent, assignment)
        if paths != expected_paths:
            raise ValueError("case paths differ from the frozen assignment")
        if paths.root in self._owned_cases:
            raise ValueError("case runtime ownership already exists")

        installed: InstalledCaseAuth | None = None
        try:
            _prepare_case_directories(paths)
            staged_marketplace = stage_marketplace_for_case(
                read_only_snapshot=self._marketplace,
                destination=paths.staging / _MARKETPLACE_RELATIVE,
            )
            staged_plugin = staged_marketplace / _PLUGIN_RELATIVE
            fixture_skills = _install_fixture_skills(
                staged_plugin, paths.workspace
            )
            cli = staged_marketplace / _CLI_RELATIVE
            try:
                cli_metadata = cli.lstat()
                original_cli = cli.read_bytes()
            except OSError:
                raise ValueError("staged marketplace CLI is unavailable") from None
            if stat.S_ISLNK(cli_metadata.st_mode) or not stat.S_ISREG(
                cli_metadata.st_mode
            ):
                raise ValueError("staged marketplace CLI must be a regular file")
            if stat.S_IMODE(cli_metadata.st_mode) != 0o700:
                raise ValueError("staged marketplace CLI must have mode 0700")

            _write_portable_store_config(paths.home, paths.store)
            bootstrap = _discover_auth_bootstrap(paths)
            installed = install_case_auth(
                bootstrap=bootstrap,
                plan=self._plan,
                assignment=assignment,
                paths=paths,
            )
            self._owned_cases[paths.root] = (paths, installed)

            audit = RuntimePayloadAudit(
                root=paths.audit,
                payload_dir=paths.payload,
                log_path=paths.audit / "payload-audit.jsonl",
                wrapper_path=cli,
            )
            environment = {
                "HOME": str(paths.home),
                "CODEX_HOME": str(paths.codex_home),
                "TMPDIR": str(paths.tmp),
                "XDG_CONFIG_HOME": str(paths.config),
                "XDG_CACHE_HOME": str(paths.cache),
                "PYTHONDONTWRITEBYTECODE": "1",
                "WORKFLOW_OBSERVATORY_HOME": str(paths.home),
                "OBSERVATION_PAYLOAD_TMPDIR": str(paths.payload),
                "OBSERVATION_AUDIT_LOG": str(audit.log_path),
                "OBSERVATION_EVAL": "1",
                "OBSERVATION_CLI_PATH": str(cli),
            }
            disabled_skill_paths = inventory_external_skill_paths(
                fixture_skill_paths=fixture_skills
            )
            setup = manifest_case.get("setup")
            force_start_unavailable = (
                isinstance(setup, dict) and setup.get("cli") == "unavailable"
            )
            runtime = CaseRuntime(
                store_root=paths.store,
                audit=audit,
                environment=environment,
                writable_roots=(
                    paths.workspace,
                    paths.store,
                    paths.audit,
                    paths.payload,
                    paths.output,
                    paths.home,
                    paths.tmp,
                    paths.config,
                    paths.cache,
                ),
                transport_config=transport_config,
                selected_command="workflow_observer_cli.py",
                disabled_skill_paths=disabled_skill_paths,
                integrity_command=(sys.executable, str(cli), "integrity"),
                audited_wrapper_path=cli,
                audited_wrapper_content=build_embedded_audit_wrapper(
                    original_cli,
                    force_start_unavailable=force_start_unavailable,
                ),
            )
            return runtime
        except BaseException as primary:
            if is_indeterminate_descriptor_close(primary):
                raise self._poison(primary)
            cleanup_errors: list[BaseException] = []
            if installed is not None:
                try:
                    cleanup_case_auth(installed=installed, paths=paths)
                except BaseException as error:
                    cleanup_errors.append(error)
            error = self._compose_error_tree(primary, cleanup_errors)
            if error is None:
                raise AssertionError("runtime setup produced no error")
            if is_indeterminate_descriptor_close(error):
                error = self._poison(error)
            raise error

    def cleanup_case(self, paths: CasePaths) -> TombstoneReceipt:
        if self._poisoned:
            raise ValueError("runtime factory is poisoned")
        if not isinstance(paths, CasePaths):
            raise TypeError("paths must be CasePaths")
        owned = self._owned_cases.get(paths.root)
        if owned is None or paths != owned[0]:
            raise ValueError("runtime cleanup ownership or canonical paths differ")
        try:
            return cleanup_case_auth(installed=owned[1], paths=paths)
        except BaseException as error:
            if is_indeterminate_descriptor_close(error):
                error = self._poison(error)
            raise error

    def close(self) -> None:
        if self._closed:
            if self._terminal_error is not None:
                raise self._terminal_error
            return
        self._closed = True
        close_errors = self._retire_factory_owners(None)
        self._terminal_error = self._compose_error_tree(None, close_errors)
        if self._terminal_error is not None:
            self._poisoned = is_indeterminate_descriptor_close(
                self._terminal_error
            )
            raise self._terminal_error


def build_production_runtime_factory(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> RuntimeFactory:
    """Bind one captured marketplace and sealed config to isolated case runtimes."""

    if not isinstance(transport_config, ResolvedTransportConfig):
        raise TypeError("transport_config must be ResolvedTransportConfig")
    return _ProductionRuntimeFactory(
        snapshot_root=Path(snapshot_root),
        transport_config=transport_config,
        plan=plan,
    )


def production_worker_dependencies(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> WorkerDependencies:
    if not isinstance(transport_config, ResolvedTransportConfig):
        raise TypeError("transport_config must be ResolvedTransportConfig")
    if type(plan) is not EpochPlan:
        raise TypeError("plan must be exact EpochPlan")
    runtime_factory = build_production_runtime_factory(
        snapshot_root=snapshot_root,
        transport_config=transport_config,
        plan=plan,
    )
    case_driver = build_production_case_driver(
        snapshot_root=snapshot_root,
        transport_config=transport_config,
    )
    return WorkerDependencies(
        runtime_factory=runtime_factory,
        case_driver=case_driver,
    )


def _worker_root(run_root: Path, lane: str) -> Path:
    return (
        run_root / "app-server"
        if lane == "APP"
        else run_root / "workers" / lane
    )


def _load_worker_manifests(
    snapshot_root: Path,
) -> dict[str, list[dict[str, object]]]:
    root = Path(snapshot_root)
    bases = (
        root / "evidence/tests/skill_evals",
        root / "plugins/workflow-observer/tests/skill_evals",
        root
        / "workflow-observatory/plugins/workflow-observer/tests/skill_evals",
    )
    matches = [
        base
        for base in bases
        if (base / "observing_workflows_cases.json").is_file()
        and (
            base / "observing_workflows_lifecycle_cases.json"
        ).is_file()
    ]
    if not matches:
        raise ValueError("captured worker manifests are unavailable")
    base = matches[0]
    try:
        forward_content = (
            base / "observing_workflows_cases.json"
        ).read_bytes()
        lifecycle_content = (
            base / "observing_workflows_lifecycle_cases.json"
        ).read_bytes()
        forward = json.loads(forward_content)
        lifecycle = json.loads(lifecycle_content)
    except (OSError, json.JSONDecodeError):
        raise ValueError("captured worker manifests are invalid") from None
    if (
        type(forward) is not list
        or type(lifecycle) is not list
        or any(type(row) is not dict for row in forward + lifecycle)
    ):
        raise ValueError("captured worker manifests are invalid")
    return {"forward": forward, "lifecycle": lifecycle}


def _load_worker_transport_config(
    *, run_root: Path, plan: EpochPlan
) -> ResolvedTransportConfig:
    payload, content = _read_canonical_record(
        run_root / "coordinator/transport-config.json",
        "sealed transport config",
    )
    _require_exact_fields(
        payload, ResolvedTransportConfig, "sealed transport config"
    )
    config = ResolvedTransportConfig(**payload)
    if (
        hashlib.sha256(content).hexdigest()
        != plan.fingerprints.transport_config_sha256
        or canonical_config_bytes(payload) != content
    ):
        raise ValueError("sealed transport config differs from the epoch plan")
    return config


def _worker_message(
    *,
    plan: EpochPlan,
    lane: str,
    seq: int,
    progress_type: str,
    case=None,
    attempt=None,
    status=None,
    classification=None,
    model_started=None,
    usage=None,
    attempt_terminal_sha256=None,
    case_commit_sha256=None,
    shard_commit_sha256=None,
    tombstone_receipt_sha256=None,
) -> ProgressMessage:
    return ProgressMessage(
        schema_version=1,
        epoch_id=plan.epoch_id,
        run_kind=plan.run_kind,
        lane=lane,
        seq=seq,
        type=progress_type,
        case=case,
        attempt=attempt,
        status=status,
        classification=classification,
        model_started=model_started,
        usage=usage,
        attempt_terminal_sha256=attempt_terminal_sha256,
        case_commit_sha256=case_commit_sha256,
        shard_commit_sha256=shard_commit_sha256,
        tombstone_receipt_sha256=tombstone_receipt_sha256,
    )


def _inventory_file_count(path: Path) -> int:
    try:
        entries = tuple(path.iterdir())
    except OSError:
        raise ValueError("worker evidence directory is unavailable") from None
    for entry in entries:
        metadata = entry.lstat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise ValueError("worker evidence directory contains a non-file")
    return len(entries)


def _audit_event_count(path: Path) -> int:
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return 0
    except (OSError, UnicodeDecodeError):
        raise ValueError("worker audit log is unavailable") from None
    return len(content.splitlines())


def _next_worker_sequence(worker_root: Path, lane: str) -> int:
    progress = Path(worker_root) / "progress"
    try:
        metadata = progress.lstat()
    except FileNotFoundError:
        return 1
    except OSError:
        raise ValueError("worker progress prefix is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("worker progress prefix is unsafe")
    progress_names: list[str] = []
    with os.scandir(progress) as entries:
        for entry in entries:
            if len(progress_names) >= MAX_PROTOCOL_RECORDS:
                raise ValueError(
                    "worker progress prefix exceeds its cap"
                )
            progress_names.append(entry.name)
    names = tuple(sorted(progress_names))
    expected = tuple(
        f"{sequence:06d}.json"
        for sequence in range(1, len(names) + 1)
    )
    if names != expected or len(names) > MAX_PROTOCOL_RECORDS:
        raise ValueError("worker progress prefix is invalid")
    for sequence, name in enumerate(names, start=1):
        message = read_progress(progress / name, lane, sequence)
        try:
            wait_for_ack(worker_root, message, 0.1)
        except TimeoutError:
            raise ValueError(
                "worker progress lacks its durable ACK"
            ) from None
    return len(names) + 1


def _reset_case_for_retry(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    paths: CasePaths,
) -> None:
    first = read_attempt_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=1,
        manifest_case=manifest_case,
    )
    receipt = read_tombstone_receipt(
        plan=plan, assignment=assignment, paths=paths
    )
    if (
        first.terminal.get("tombstone_receipt_sha256")
        != hashlib.sha256(
            canonical_config_bytes(asdict(receipt))
        ).hexdigest()
        or receipt.canonical_binding != "expected"
    ):
        raise ValueError("retry cleanup proof differs from attempt one")
    try:
        paths.codex_home.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("retry Codex home was not torn down")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    root_slot = _DescriptorSlot(os.open(paths.root, flags))
    primary: BaseException | None = None
    try:
        names = tuple(sorted(entry.name for entry in os.scandir(root_slot.descriptor)))
        allowed = {
            "attempts",
            "cleanup",
            "staging",
            "workspace",
            "store",
            "audit",
            "payload",
            "output",
            "home",
            "tmp",
            "config",
            "cache",
            "sealed",
        }
        if any(name not in allowed for name in names):
            raise ValueError("retry case contains an unknown entry")
        remaining = [AUTH_CLEANUP_MAX_ENTRIES]
        for name in names:
            if name in ("attempts", "cleanup"):
                continue
            if remaining[0] <= 0:
                raise OSError("retry reset bound exceeded")
            remaining[0] -= 1
            _remove_tree_entry(
                root_slot.descriptor,
                name,
                depth=1,
                remaining=remaining,
                charged=True,
            )
        cleanup_slot = _DescriptorSlot(
            os.open(paths.cleanup, flags)
        )
        cleanup_primary: BaseException | None = None
        try:
            cleanup_names = tuple(
                sorted(
                    entry.name
                    for entry in os.scandir(cleanup_slot.descriptor)
                )
            )
            if cleanup_names != ("ownership.json", "tombstone.json"):
                raise ValueError("retry cleanup proof is incomplete")
            for name in cleanup_names:
                metadata = os.stat(
                    name,
                    dir_fd=cleanup_slot.descriptor,
                    follow_symlinks=False,
                )
                if (
                    stat.S_ISLNK(metadata.st_mode)
                    or not stat.S_ISREG(metadata.st_mode)
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                ):
                    raise ValueError("retry cleanup proof is unsafe")
        except BaseException as error:
            cleanup_primary = error
        close_error = _retire_descriptor_capability(cleanup_slot)
        _raise_case_and_auth_cleanup_failures(
            cleanup_primary,
            [close_error] if close_error is not None else [],
        )
        try:
            os.stat(
                "cleanup-attempt-1",
                dir_fd=root_slot.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise ValueError("retry cleanup backup already exists")
        os.rename(
            "cleanup",
            "cleanup-attempt-1",
            src_dir_fd=root_slot.descriptor,
            dst_dir_fd=root_slot.descriptor,
        )
        os.mkdir(
            "cleanup", mode=0o700, dir_fd=root_slot.descriptor
        )
        os.fsync(root_slot.descriptor)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(root_slot)
    _raise_case_and_auth_cleanup_failures(
        primary, [close_error] if close_error is not None else []
    )


def _worker_failure_leaves(error: BaseException) -> tuple[BaseException, ...]:
    nested = getattr(error, "exceptions", ())
    if isinstance(nested, (tuple, list)) and nested:
        return tuple(
            leaf
            for child in nested
            if isinstance(child, BaseException)
            for leaf in _worker_failure_leaves(child)
        )
    return (error,)


def _classify_worker_failure(
    error: BaseException, *, model_started: bool
) -> str:
    leaves = _worker_failure_leaves(error)
    captured_facts = tuple(
        fact
        for leaf in leaves
        if isinstance(leaf, _CapturedEvaluatorFailure)
        for fact in leaf.facts
    )
    if any(fact.kind == "survival" for fact in captured_facts):
        return "surviving-process"
    if any(
        isinstance(leaf, ProcessSurvivalCleanupFailure)
        for leaf in leaves
    ):
        return "surviving-process"
    if any(fact.kind == "cleanup" for fact in captured_facts):
        return "cleanup"
    if any(isinstance(leaf, CaseCleanupFailure) for leaf in leaves):
        return "cleanup"
    for fact in captured_facts:
        if fact.kind != "transport":
            continue
        classification = fact.classification
        if (
            fact.retryable is False
            and classification == "pre-model-infrastructure"
        ):
            return "protocol"
        if classification in (
            "model",
            "pre-model-infrastructure",
            "timeout",
            "protocol",
            "post-start-transport",
        ):
            return classification
        return "protocol"
    for leaf in leaves:
        if isinstance(leaf, CaseTransportFailure):
            classification = leaf.classification
            if leaf.retryable is False and classification == (
                "pre-model-infrastructure"
            ):
                return "protocol"
            if classification in (
                "model",
                "pre-model-infrastructure",
                "timeout",
                "protocol",
                "post-start-transport",
            ):
                return classification
            return "protocol"
    if any(isinstance(leaf, TimeoutError) for leaf in leaves):
        return "timeout"
    if any(fact.kind == "timeout" for fact in captured_facts):
        return "timeout"
    if any(fact.kind == "infrastructure" for fact in captured_facts):
        return (
            "post-start-transport"
            if model_started
            else "pre-model-infrastructure"
        )
    if any(isinstance(leaf, CaseInfrastructureFailure) for leaf in leaves):
        return (
            "post-start-transport"
            if model_started
            else "pre-model-infrastructure"
        )
    if any(isinstance(leaf, AssertionError) for leaf in leaves):
        return "semantic"
    if any(fact.kind == "semantic" for fact in captured_facts):
        return "semantic"
    return "protocol" if model_started else "pre-model-infrastructure"


def _worker_failure_summary(
    error: BaseException, classification: str
) -> tuple[dict[str, object], FailureSummary]:
    rendered = str(error)
    error_type = type(error).__name__
    if not error_type or len(error_type) > 128 or not all(
        character.isalnum() or character in "._-"
        for character in error_type
    ):
        error_type = "WorkerFailure"
    payload = {
        "classification": classification,
        "type": error_type,
        "chars": min(len(rendered), 200),
        "sha256": hashlib.sha256(
            rendered.encode("utf-8", errors="replace")
        ).hexdigest(),
    }
    return payload, FailureSummary(**payload)


def _drive_worker_attempt(
    *,
    plan: EpochPlan,
    lane: str,
    sequence: int,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    paths: CasePaths,
    attempt: int,
    runtime_factory: RuntimeFactory,
    case_driver: CaseDriver,
) -> tuple[ShardTerminal, ProgressMessage, RetryDecision]:
    observed_model_start = False

    def event_sink(event, _pid, _pgid):
        nonlocal observed_model_start
        if event == "model-started":
            observed_model_start = True

    try:
        driven = case_driver(
            assignment=assignment,
            manifest_case=manifest_case,
            paths=paths,
            runtime_factory=runtime_factory,
            event_sink=event_sink,
        )
        if not isinstance(driven, DrivenCase):
            raise TypeError("case driver must return DrivenCase")
        if not observed_model_start:
            raise ValueError("successful worker case never started a model")
    except BaseException as error:
        if worker_exit_required(error, runtime_factory):
            raise
        try:
            verified_receipt = read_tombstone_receipt(
                plan=plan,
                assignment=assignment,
                paths=paths,
            )
        except (OSError, TypeError, ValueError):
            verified_receipt = None
        cleanup_passed = (
            verified_receipt is not None
            and verified_receipt.canonical_binding == "expected"
        )
        classification = _classify_worker_failure(
            error, model_started=observed_model_start
        )
        failure_payload, failure = _worker_failure_summary(
            error, classification
        )
        write_attempt_terminal(
            plan=plan,
            paths=paths,
            assignment=assignment,
            attempt=attempt,
            manifest_case=manifest_case,
            status="failed",
            classification=classification,
            model_started=observed_model_start,
            cleanup_passed=cleanup_passed,
            usage=None,
            failure=failure_payload,
        )
        attempt_seal = read_attempt_seal(
            plan=plan,
            paths=paths,
            assignment=assignment,
            attempt=attempt,
            manifest_case=manifest_case,
        )
        tombstone_sha256 = attempt_seal.terminal.get(
            "tombstone_receipt_sha256"
        )
        retry = decide_retry(
            classification=classification,
            attempt=attempt,
            model_started=observed_model_start,
            cleanup_passed=cleanup_passed,
            fingerprints_unchanged=True,
        )
        terminal = ShardTerminal(
            key=assignment.key,
            run_kind=plan.run_kind,
            status="failed",
            classification=classification,
            attempt_terminal_sha256=attempt_seal.terminal_sha256,
            case_commit_sha256=None,
            tombstone_receipt_sha256=tombstone_sha256,
            failure=failure,
        )
        progress = _worker_message(
            plan=plan,
            lane=lane,
            seq=sequence,
            progress_type="case-terminal",
            case=assignment.key,
            attempt=attempt,
            status="failed",
            classification=classification,
            model_started=observed_model_start,
            attempt_terminal_sha256=attempt_seal.terminal_sha256,
            tombstone_receipt_sha256=tombstone_sha256,
        )
        return terminal, progress, retry

    verified_receipt = read_tombstone_receipt(
        plan=plan,
        assignment=assignment,
        paths=paths,
    )
    if verified_receipt.canonical_binding != "expected":
        raise ValueError("worker cleanup did not retain expected tombstone")
    execution_usage = driven.execution.usage
    if not isinstance(execution_usage, TokenUsage):
        raise TypeError("case execution usage must be TokenUsage")
    usage = asdict(execution_usage)
    write_attempt_terminal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=attempt,
        manifest_case=manifest_case,
        status="success",
        classification="success",
        model_started=True,
        cleanup_passed=True,
        usage=usage,
        failure=None,
    )
    evidence = {
        "status": "success",
        "classification": "success",
        "model_started": True,
        "elapsed_milliseconds": 0,
        "usage": usage,
        "failure": None,
        "store_record_count": len(tuple(paths.store.rglob("*.md"))),
        "store_invalidated_count": len(
            tuple(paths.store.rglob("*.invalidated"))
        ),
        "audit_event_count": _audit_event_count(
            paths.audit / "payload-audit.jsonl"
        ),
        "payload_file_count": _inventory_file_count(paths.payload),
        "output_file_count": _inventory_file_count(paths.output),
        "process_cleanup_passed": True,
        "credential_cleanup_passed": True,
    }
    seal_case(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=attempt,
        result=driven.result,
        evidence=evidence,
        manifest_case=manifest_case,
        fault_injector=None,
    )
    attempt_seal = read_attempt_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=attempt,
        manifest_case=manifest_case,
    )
    case_seal = read_case_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    terminal = ShardTerminal(
        key=assignment.key,
        run_kind=plan.run_kind,
        status="success",
        classification="success",
        attempt_terminal_sha256=attempt_seal.terminal_sha256,
        case_commit_sha256=case_seal.commit_sha256,
        tombstone_receipt_sha256=case_seal.tombstone_receipt_sha256,
        failure=None,
    )
    progress = _worker_message(
        plan=plan,
        lane=lane,
        seq=sequence,
        progress_type="case-terminal",
        case=assignment.key,
        attempt=attempt,
        status="success",
        classification="success",
        model_started=True,
        usage=execution_usage,
        attempt_terminal_sha256=attempt_seal.terminal_sha256,
        case_commit_sha256=case_seal.commit_sha256,
        tombstone_receipt_sha256=case_seal.tombstone_receipt_sha256,
    )
    return (
        terminal,
        progress,
        decide_retry(
            classification="success",
            attempt=attempt,
            model_started=True,
            cleanup_passed=True,
            fingerprints_unchanged=True,
        ),
    )


def _run_worker_impl(
    *,
    lane: str,
    plan: EpochPlan,
    run_root: Path,
    snapshot_root: Path,
    resume,
    dependencies: WorkerDependencies | None = None,
) -> Path:
    if type(lane) is not str or lane not in ("E1", "E2", "E3", "APP"):
        raise ValueError("worker lane is invalid")
    if type(plan) is not EpochPlan:
        raise TypeError("plan must be exact EpochPlan")
    root = canonical_run_root(run_root)
    plan_payload, _plan_content = _read_canonical_record(
        root / "coordinator/epoch-plan.json",
        "sealed epoch plan",
        byte_cap=64 * 1024,
    )
    sealed_plan = _decode_epoch_plan_record(plan_payload)
    if sealed_plan != plan:
        raise ValueError("worker plan differs from sealed epoch plan")
    if resume is None:
        raise ValueError("worker resume plan is unavailable")
    if resume.invalid:
        raise ValueError("worker cannot launch an invalid resume plan")
    diagnostic_scope = None
    if plan.run_kind == "diagnostic":
        diagnostic_scope = _read_diagnostic_execution_scope(
            coordinator_root=root / "coordinator",
            plan=plan,
        )
        if lane != diagnostic_scope.lane:
            raise ValueError(
                "diagnostic worker lane differs from sealed scope"
            )
    transport_config = _load_worker_transport_config(
        run_root=root, plan=plan
    )
    bound_dependencies = (
        production_worker_dependencies(
            snapshot_root=snapshot_root,
            transport_config=transport_config,
            plan=plan,
        )
        if dependencies is None
        else dependencies
    )
    if type(bound_dependencies) is not WorkerDependencies:
        raise TypeError("dependencies must be exact WorkerDependencies")
    if (
        not callable(bound_dependencies.runtime_factory)
        or not callable(bound_dependencies.case_driver)
    ):
        raise TypeError("worker dependencies must be callable")
    manifests = _load_worker_manifests(snapshot_root)
    if (
        hashlib.sha256(
            json.dumps(
                manifests["forward"],
                indent=2,
                ensure_ascii=True,
            ).encode("utf-8")
            + b"\n"
        ).hexdigest()
        != plan.fingerprints.forward_manifest_sha256
        or hashlib.sha256(
            json.dumps(
                manifests["lifecycle"],
                indent=2,
                ensure_ascii=True,
            ).encode("utf-8")
            + b"\n"
        ).hexdigest()
        != plan.fingerprints.lifecycle_manifest_sha256
    ):
        raise ValueError("captured worker manifests differ from the epoch plan")
    _register_progress_epoch_context(plan=plan, manifests=manifests)
    lane_assignments = tuple(
        assignment for assignment in plan.assignments if assignment.lane == lane
    )
    execution_keys = {
        assignment.key for assignment in lane_assignments
    }
    if diagnostic_scope is not None:
        execution_keys = {diagnostic_scope.target}
    reusable_keys = set(resume.reusable).intersection(execution_keys)
    pending_keys = set(resume.pending).intersection(execution_keys)
    assignments = tuple(
        assignment
        for assignment in lane_assignments
        if assignment.key in pending_keys
    )
    worker_root = _worker_root(root, lane)
    _secure_directory(worker_root, anchor=root)
    sequence = _next_worker_sequence(worker_root, lane)
    terminals: list[ShardTerminal] = []
    case_paths: dict[object, CasePaths] = {}
    runtime_factory = bound_dependencies.runtime_factory
    try:
        for assignment in lane_assignments:
            if assignment.key not in reusable_keys:
                continue
            manifest_case = manifests[assignment.key.mode][
                assignment.key.ordinal - 1
            ]
            paths = paths_for_case(root, assignment)
            case_paths[assignment.key] = paths
            attempts = scan_attempts(
                paths, plan=plan, manifest_case=manifest_case
            )
            if len(attempts) not in (1, 2):
                raise ValueError("reusable case has invalid attempt count")
            attempt_seal = read_attempt_seal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=len(attempts),
                manifest_case=manifest_case,
            )
            case_seal = read_case_seal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
            )
            terminals.append(
                ShardTerminal(
                    key=assignment.key,
                    run_kind=plan.run_kind,
                    status="success",
                    classification="success",
                    attempt_terminal_sha256=attempt_seal.terminal_sha256,
                    case_commit_sha256=case_seal.commit_sha256,
                    tombstone_receipt_sha256=(
                        case_seal.tombstone_receipt_sha256
                    ),
                    failure=None,
                )
            )
        ready = _worker_message(
            plan=plan,
            lane=lane,
            seq=sequence,
            progress_type="lane-ready",
        )
        ack = publish_progress_and_wait_for_ack(
            worker_root=worker_root,
            message=ready,
            timeout=300.0,
        )
        sequence += 1
        if ack.decision != "continue":
            assignments = ()

        stop_assignments = False
        for assignment in assignments:
            manifest_case = manifests[assignment.key.mode][
                assignment.key.ordinal - 1
            ]
            if manifest_case.get("id") != assignment.key.case_id:
                raise ValueError("worker manifest ordinal binding changed")
            paths = paths_for_case(root, assignment)
            case_paths[assignment.key] = paths
            _secure_directory(paths.root, anchor=root)
            _secure_directory(paths.cleanup, anchor=root)
            _secure_directory(paths.attempts, anchor=root)
            while True:
                prior_attempts = scan_attempts(
                    paths, plan=plan, manifest_case=manifest_case
                )
                attempt = len(prior_attempts) + 1
                if attempt not in (1, 2):
                    raise ValueError(
                        "pending case exhausted its attempt budget"
                    )
                if attempt == 2:
                    prior = read_attempt_seal(
                        plan=plan,
                        paths=paths,
                        assignment=assignment,
                        attempt=1,
                        manifest_case=manifest_case,
                    )
                    prior_terminal = prior.terminal
                    prior_retry = decide_retry(
                        classification=prior_terminal["classification"],
                        attempt=1,
                        model_started=prior_terminal["model_started"],
                        cleanup_passed=prior_terminal["cleanup_passed"],
                        fingerprints_unchanged=True,
                    )
                    if (
                        not prior_retry.retry
                        or prior_retry.next_attempt != 2
                        or prior_retry.action != "reuse"
                    ):
                        raise ValueError(
                            "pending attempt two lacks retry authority"
                        )
                write_attempt_start(
                    plan=plan,
                    paths=paths,
                    assignment=assignment,
                    attempt=attempt,
                    manifest_case=manifest_case,
                )
                started = _worker_message(
                    plan=plan,
                    lane=lane,
                    seq=sequence,
                    progress_type="case-started",
                    case=assignment.key,
                    attempt=attempt,
                )
                ack = publish_progress_and_wait_for_ack(
                    worker_root=worker_root,
                    message=started,
                    timeout=300.0,
                )
                sequence += 1
                if ack.decision != "continue":
                    stop_assignments = True
                    break
                if attempt == 2:
                    _reset_case_for_retry(
                        plan=plan,
                        assignment=assignment,
                        manifest_case=manifest_case,
                        paths=paths,
                    )

                terminal, progress, retry = _drive_worker_attempt(
                    plan=plan,
                    lane=lane,
                    sequence=sequence,
                    assignment=assignment,
                    manifest_case=manifest_case,
                    paths=paths,
                    attempt=attempt,
                    runtime_factory=runtime_factory,
                    case_driver=bound_dependencies.case_driver,
                )
                ack = publish_progress_and_wait_for_ack(
                    worker_root=worker_root,
                    message=progress,
                    timeout=300.0,
                )
                sequence += 1
                if terminal.status == "failed" and retry.retry:
                    if ack.decision == "retry":
                        continue
                    if ack.decision == "continue":
                        raise ValueError(
                            "retryable failure received continue ACK"
                        )
                elif ack.decision == "retry":
                    raise ValueError(
                        "non-retryable terminal received retry ACK"
                    )
                terminals.append(terminal)
                if ack.decision != "continue":
                    stop_assignments = True
                break
            if stop_assignments:
                break

        shard_path = worker_root
        shard_status = (
            "failed"
            if terminals and terminals[-1].status == "failed"
            else "success"
        )
        should_seal_shard = (
            plan.run_kind != "diagnostic"
            and (
                len(terminals) == len(lane_assignments)
                or shard_status == "failed"
            )
        )
        if should_seal_shard:
            shard_path = seal_shard(
                worker_root=worker_root,
                plan=plan,
                lane=lane,
                terminals=terminals,
                manifests=manifests,
                case_paths=case_paths,
                fault_injector=None,
            )
            shard_content = shard_path.read_bytes()
            shard_message = _worker_message(
                plan=plan,
                lane=lane,
                seq=sequence,
                progress_type="shard-terminal",
                status=shard_status,
                shard_commit_sha256=hashlib.sha256(
                    shard_content
                ).hexdigest(),
            )
            publish_progress_and_wait_for_ack(
                worker_root=worker_root,
                message=shard_message,
                timeout=300.0,
            )
            sequence += 1
        stopped = _worker_message(
            plan=plan,
            lane=lane,
            seq=sequence,
            progress_type="worker-stopped",
        )
        publish_progress_and_wait_for_ack(
            worker_root=worker_root,
            message=stopped,
            timeout=300.0,
        )
        return shard_path
    finally:
        runtime_factory.close()


def run_worker(
    *,
    lane: str,
    plan: EpochPlan,
    run_root: Path,
    snapshot_root: Path,
    dependencies: WorkerDependencies | None = None,
) -> Path:
    from scripts.workflow_eval_sharding import ResumePlan

    return _run_worker_impl(
        lane=lane,
        plan=plan,
        run_root=run_root,
        snapshot_root=snapshot_root,
        resume=ResumePlan(
            run_kind=plan.run_kind,
            reusable=(),
            pending=tuple(
                assignment.key for assignment in plan.assignments
            ),
            invalid=(),
        ),
        dependencies=dependencies,
    )


def worker_main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one sealed parallel evaluation lane."
    )
    parser.add_argument(
        "--lane", required=True, choices=("E1", "E2", "E3", "APP")
    )
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--resume-plan-hex", required=True)
    arguments = parser.parse_args(argv)
    run_root = canonical_run_root(arguments.run_root)
    payload, _content = _read_canonical_record(
        run_root / "coordinator/epoch-plan.json",
        "sealed epoch plan",
        byte_cap=64 * 1024,
    )
    plan = _decode_epoch_plan_record(payload)
    if plan.epoch_id != arguments.epoch_id:
        raise ValueError("worker epoch argument differs from sealed plan")
    try:
        resume_content = bytes.fromhex(arguments.resume_plan_hex)
        resume_payload = json.loads(resume_content)
    except (ValueError, json.JSONDecodeError):
        raise ValueError("worker resume plan argument is invalid") from None
    if canonical_config_bytes(resume_payload) != resume_content:
        raise ValueError("worker resume plan argument is non-canonical")
    resume = _decode_resume_plan_record(resume_payload, plan=plan)
    _run_worker_impl(
        lane=arguments.lane,
        plan=plan,
        run_root=run_root,
        snapshot_root=arguments.snapshot_root,
        resume=resume,
        dependencies=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(worker_main())
