#!/usr/bin/env python3
"""Isolated worker runtime primitives for parallel Workflow Observatory evals."""

from __future__ import annotations

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
from typing import Callable, Protocol

from scripts.run_observing_workflows_task9_eval import (
    AUTH_CLEANUP_MAX_DEPTH,
    AUTH_CLEANUP_MAX_ENTRIES,
    CaseEventSink,
    CaseExecution,
    CaseRuntime,
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
    InstalledCaseAuth,
    ProgressMessage,
    ResolvedTransportConfig,
    TombstoneReceipt,
    _DescriptorSlot,
    _atomic_write_record,
    _decode_case_key,
    _read_canonical_record,
    _require_exact_fields,
    _retire_descriptor_capability,
    _tombstone_receipt_from_payload as _receipt_from_payload,
    _validate_wait_timeout,
    _write_progress_with_deadline,
    canonical_run_root,
    install_case_auth,
    is_indeterminate_descriptor_close,
    paths_for_case,
    read_case_auth_ownership as _read_case_auth_ownership,
    read_tombstone_receipt,
    stage_marketplace_for_case,
    wait_for_ack,
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
    return factory.poisoned or is_indeterminate_descriptor_close(error)


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
