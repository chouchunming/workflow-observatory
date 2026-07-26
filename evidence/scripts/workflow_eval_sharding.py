from dataclasses import asdict, dataclass, fields, replace
from contextlib import ExitStack
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import secrets
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import tomllib
from typing import Any, Callable, Literal, Mapping, Sequence


RunKind = Literal["diagnostic", "discovery", "formal"]
CoordinatorRole = Literal["serial-coordinator", "parallel-coordinator"]
EvalMode = Literal["forward", "lifecycle"]
LaneName = Literal["E1", "E2", "E3", "APP"]
Route = Literal["exec", "app-server"]
CleanupState = Literal["active", "scrubbing", "tombstoned"]
DescriptorCloseState = Literal["owned", "closing", "closed", "indeterminate"]
CaseSealStatus = Literal["success", "failed"]
OutcomeClass = Literal[
    "success",
    "semantic",
    "model",
    "pre-model-infrastructure",
    "cleanup",
    "production-mutation",
    "manifest-mutation",
    "timeout",
    "protocol",
    "post-start-transport",
    "surviving-process",
    "coordinator-crash",
]
FaultPoint = Literal[
    "after-result-replace",
    "after-evidence-replace",
    "before-case-commit",
    "after-case-commit",
    "before-shard-commit",
    "after-shard-commit",
]
FaultInjector = Callable[[FaultPoint], None]
ProgressType = Literal[
    "lane-ready",
    "case-started",
    "case-terminal",
    "shard-terminal",
    "worker-stopped",
]
AckDecision = Literal["continue", "stop-launches", "abort"]
MAX_ATTEMPT_START_BYTES = 4 * 1024
MAX_ATTEMPT_TERMINAL_BYTES = 8 * 1024
MAX_CASE_RESULT_BYTES = 64 * 1024
MAX_CASE_EVIDENCE_BYTES = 16 * 1024
MAX_CASE_COMMIT_BYTES = 8 * 1024
MAX_SHARD_COMMIT_BYTES = 64 * 1024
MAX_PROGRESS_BYTES = 4096
MAX_PROGRESS_STRING_CHARS = 256
MAX_TOKEN_COUNT = 2**63 - 1
MAX_PROTOCOL_RECORDS = 19
MAX_PROTOCOL_CRASH_TEMPS = 19
MAX_SEAL_COUNTER = 1_000_000
MAX_SEAL_ELAPSED_MILLISECONDS = 3_600_000
MAX_SEAL_FAILURE_CHARS = 2**63 - 1
MAX_SEAL_FAILURE_TYPE_CHARS = 128

DECISION_MANIFEST_FIELDS = frozenset(
    {
        "id",
        "turns",
        "fixture",
        "expected_decisions",
        "task_type",
        "workflow_variant",
        "expected_record_checkpoints",
        "expected_run_count",
        "expected_final_statuses",
    }
)
LIFECYCLE_MANIFEST_FIELDS = frozenset(
    {
        "id",
        "turns",
        "fixture",
        "mode",
        "setup",
        "expected_record_checkpoints",
        "expected_run_count",
        "expected_draft_count",
        "expected_final_statuses",
        "expect_failure_disclosure",
        "expected_selected_command",
    }
)
RESULT_SCHEMAS = {
    "forward": frozenset(
        {
            "id",
            "decisions",
            "record_checkpoints",
            "run_count",
            "draft_count",
            "final_statuses",
        }
    ),
    "lifecycle": frozenset(
        {
            "id",
            "record_checkpoints",
            "run_count",
            "draft_count",
            "final_statuses",
            "failure_disclosed",
            "selected_command",
        }
    ),
}
OBSERVED_DECISION_FIELDS = frozenset(
    {"after_turn", "triggered", "task_type", "workflow_variant"}
)
CHECKPOINT_FIELDS = frozenset({"after_turn", "records"})
NORMALIZED_RECORD_FIELDS = frozenset(
    {"role", "status", "start_mode", "superseded_by_role"}
)
TURN_FIELDS = frozenset({"prompt", "dispatch_when"})
EXPECTED_DECISION_FIELDS = frozenset({"after_turn", "triggered"})
SETUP_FIELDS = frozenset({"eval_override", "cli", "wiki_root"})
DISPATCH_VALUES = frozenset(
    {
        "immediate",
        "after_single_file_mutation_without_run",
        "after_draft_run",
    }
)
FIXTURE_VALUES = frozenset({"python-cli", "documentation", "wiki", "empty"})
LIFECYCLE_MODES = frozenset({"executable", "command-selection-only"})
_COMMAND_SELECTION_LIFECYCLE_KEY = (
    "lifecycle",
    8,
    "incomplete-eval-override",
)
SHARD_TERMINAL_FIELDS = frozenset(
    {
        "case",
        "status",
        "classification",
        "attempt_terminal_sha256",
        "case_commit_sha256",
        "tombstone_receipt_sha256",
        "failure",
    }
)
PROGRESS_FIELDS = {
    "schema_version",
    "epoch_id",
    "run_kind",
    "lane",
    "seq",
    "type",
    "case",
    "attempt",
    "status",
    "classification",
    "model_started",
    "usage",
    "attempt_terminal_sha256",
    "case_commit_sha256",
    "shard_commit_sha256",
    "tombstone_receipt_sha256",
}
ACK_FIELDS = {
    "schema_version",
    "epoch_id",
    "run_kind",
    "lane",
    "seq",
    "message_sha256",
    "decision",
}
_INDETERMINATE_CLOSE_MARKER = "_workflow_eval_indeterminate_descriptor_close"
_LEASE_CONSTRUCTOR_TOKEN = object()
_LEASE_REGISTRY_LOCK = threading.RLock()
_RUN_LEASES: dict[bytes, "RunCoordinatorLease"] = {}
_RESULT_WRITER_LEASES: dict[bytes, "ResultWriterLease"] = {}
_LEASE_PROCESS_POISON: BaseException | None = None


@dataclass
class _DescriptorSlot:
    descriptor: int
    descriptor_close_state: DescriptorCloseState = "owned"
    descriptor_close_error: BaseException | None = None


@dataclass(frozen=True)
class _RetainedDirectory:
    name: str
    identity: tuple[int, int]
    slot: _DescriptorSlot


def _require_lease_process_healthy() -> None:
    if _LEASE_PROCESS_POISON is not None:
        raise RuntimeError("lease process is poisoned by an indeterminate close") from (
            _LEASE_PROCESS_POISON
        )


def _close_lease_descriptors(
    slots: Sequence["_DescriptorSlot"], *, label: str
) -> None:
    _retire_task_descriptors(slots, primary=None, label=label)


def _retire_task_descriptors(
    slots: Sequence["_DescriptorSlot"],
    *,
    primary: BaseException | None,
    label: str,
) -> None:
    """Retire every independent slot once and poison on ambiguous close."""

    global _LEASE_PROCESS_POISON
    close_errors: list[BaseException] = []
    for slot in slots:
        error = _retire_descriptor_capability(slot)
        if error is not None:
            close_errors.append(error)
    _raise_task_failures(
        primary=primary, close_errors=close_errors, label=label
    )


def _raise_task_failures(
    *,
    primary: BaseException | None,
    close_errors: Sequence[BaseException],
    label: str,
) -> None:
    global _LEASE_PROCESS_POISON
    if primary is not None and is_indeterminate_descriptor_close(primary):
        _LEASE_PROCESS_POISON = primary
    if close_errors:
        errors = ([primary] if primary is not None else []) + list(close_errors)
        failure: BaseException
        if len(errors) == 1:
            failure = errors[0]
        else:
            group_type = (
                ExceptionGroup
                if all(isinstance(error, Exception) for error in errors)
                else BaseExceptionGroup
            )
            failure = group_type(label, errors)
        _LEASE_PROCESS_POISON = failure
        raise failure
    if primary is not None:
        raise primary


def _rollback_lease_acquisition(
    non_lock_slots: Sequence[_DescriptorSlot],
    lock_slot: _DescriptorSlot | None,
    *,
    primary: BaseException,
    label: str,
) -> None:
    ordered = list(reversed(tuple(non_lock_slots)))
    if lock_slot is not None:
        ordered.append(lock_slot)
    _retire_task_descriptors(ordered, primary=primary, label=label)
    raise AssertionError("lease rollback produced no error")


def _required_os_flag(name: str) -> int:
    value = getattr(os, name, 0)
    if not isinstance(value, int) or value == 0:
        raise RuntimeError(f"POSIX lease support requires {name}")
    return value


def _stat_identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _validate_owned_entry(
    metadata: os.stat_result,
    *,
    label: str,
    kind: Literal["directory", "file"],
    mode: int | None,
    require_owner: bool = True,
) -> None:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        raise RuntimeError("POSIX leases require os.geteuid")
    expected_kind = stat.S_ISDIR if kind == "directory" else stat.S_ISREG
    if not expected_kind(metadata.st_mode):
        raise ValueError(f"{label} must be a {kind}")
    if require_owner and metadata.st_uid != geteuid():
        raise PermissionError(f"{label} must be owned by the current uid")
    if mode is not None and stat.S_IMODE(metadata.st_mode) != mode:
        raise PermissionError(f"{label} must have mode {mode:04o}")


def _validate_trusted_parent(metadata: os.stat_result, *, label: str) -> None:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        raise RuntimeError("POSIX leases require os.geteuid")
    if not stat.S_ISDIR(metadata.st_mode):
        raise ValueError(f"{label} must be a directory")
    if metadata.st_uid not in {0, geteuid()}:
        raise PermissionError(f"{label} owner is not trusted")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & 0o022 and not mode & stat.S_ISVTX:
        raise PermissionError(f"{label} must be non-writable or sticky")


def _open_absolute_directory_anchor(
    path: Path, label: str, *, trusted_parent: bool = False
) -> tuple[_DescriptorSlot, tuple[int, int]]:
    nofollow = _required_os_flag("O_NOFOLLOW")
    directory = _required_os_flag("O_DIRECTORY")
    path = Path(path)
    try:
        expected = os.lstat(path)
    except OSError:
        raise ValueError(f"{label} must be an existing canonical directory") from None
    if trusted_parent:
        _validate_trusted_parent(expected, label=label)
    else:
        _validate_owned_entry(
            expected, label=label, kind="directory", mode=None
        )
    descriptor = os.open(
        path,
        os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0),
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    result: tuple[int, tuple[int, int]] | None = None
    try:
        opened = os.fstat(descriptor)
        current = os.lstat(path)
        identities = {
            _stat_identity(expected),
            _stat_identity(opened),
            _stat_identity(current),
        }
        if len(identities) != 1:
            raise RuntimeError(f"{label} changed while opening")
        if trusted_parent:
            _validate_trusted_parent(opened, label=label)
        else:
            _validate_owned_entry(
                opened, label=label, kind="directory", mode=None
            )
        result = slot, _stat_identity(opened)
    except BaseException as error:
        primary = error
    if primary is None:
        assert result is not None
        return result
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        f"{label} open or close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    raise AssertionError("directory-anchor validation produced no error")


def _open_managed_directory_at(
    parent_fd: int,
    name: str,
    *,
    label: str,
    create: bool,
    mode: int | None = 0o700,
) -> tuple[_DescriptorSlot, tuple[int, int]]:
    if not name or name in {".", ".."} or "/" in name:
        raise ValueError(f"invalid {label} name")
    nofollow = _required_os_flag("O_NOFOLLOW")
    directory = _required_os_flag("O_DIRECTORY")
    try:
        expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        if not create:
            raise
        os.mkdir(name, 0o700, dir_fd=parent_fd)
        expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _validate_owned_entry(expected, label=label, kind="directory", mode=mode)
    descriptor = os.open(
        name,
        os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0),
        dir_fd=parent_fd,
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    result: tuple[_DescriptorSlot, tuple[int, int]] | None = None
    try:
        opened = os.fstat(descriptor)
        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if len(
            {
                _stat_identity(expected),
                _stat_identity(opened),
                _stat_identity(current),
            }
        ) != 1:
            raise RuntimeError(f"{label} changed while opening")
        _validate_owned_entry(opened, label=label, kind="directory", mode=mode)
        result = slot, _stat_identity(opened)
    except BaseException as error:
        primary = error
    if primary is None:
        assert result is not None
        return result
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        f"{label} open or close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    raise AssertionError("managed-directory validation produced no error")


def _open_managed_lock_at(
    directory_fd: int, name: str, *, label: str
) -> tuple[_DescriptorSlot, tuple[int, int]]:
    if not name or name in {".", ".."} or "/" in name:
        raise ValueError(f"invalid {label} name")
    nofollow = _required_os_flag("O_NOFOLLOW")
    flags = os.O_RDWR | nofollow | getattr(os, "O_CLOEXEC", 0)
    slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    result: tuple[_DescriptorSlot, tuple[int, int]] | None = None
    try:
        try:
            expected = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
        except FileNotFoundError:
            descriptor = os.open(
                name,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=directory_fd,
            )
            slot = _DescriptorSlot(descriptor)
            expected = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
        else:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
            slot = _DescriptorSlot(descriptor)
        opened = os.fstat(slot.descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if len(
            {
                _stat_identity(expected),
                _stat_identity(opened),
                _stat_identity(current),
            }
        ) != 1:
            raise RuntimeError(f"{label} changed while opening")
        _validate_owned_entry(opened, label=label, kind="file", mode=0o600)
        result = slot, _stat_identity(opened)
    except BaseException as error:
        primary = error
    if primary is None:
        assert result is not None
        return result
    close_error = (
        _retire_descriptor_capability(slot) if slot is not None else None
    )
    _raise_ordered_failures(
        f"{label} open or close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    raise AssertionError("managed-lock validation produced no error")


def _reconcile_descriptor(
    descriptor: int,
    identity: tuple[int, int],
    *,
    label: str,
    kind: Literal["directory", "file"],
    mode: int | None,
    require_owner: bool = True,
) -> None:
    opened = os.fstat(descriptor)
    if _stat_identity(opened) != identity:
        raise RuntimeError(f"{label} descriptor identity changed")
    _validate_owned_entry(
        opened,
        label=label,
        kind=kind,
        mode=mode,
        require_owner=require_owner,
    )


def _reconcile_named_descriptor_at(
    parent_fd: int,
    name: str,
    descriptor: int,
    identity: tuple[int, int],
    *,
    label: str,
    kind: Literal["directory", "file"],
    mode: int | None,
) -> None:
    _reconcile_descriptor(descriptor, identity, label=label, kind=kind, mode=mode)
    current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if _stat_identity(current) != identity:
        raise RuntimeError(f"{label} name changed")
    _validate_owned_entry(current, label=label, kind=kind, mode=mode)


def _reconcile_trusted_descriptor(
    descriptor: int,
    identity: tuple[int, int],
    *,
    label: str,
) -> None:
    opened = os.fstat(descriptor)
    if _stat_identity(opened) != identity:
        raise RuntimeError(f"{label} descriptor identity changed")
    _validate_trusted_parent(opened, label=label)


def _reconcile_trusted_named_descriptor_at(
    parent_fd: int,
    entry: _RetainedDirectory,
    *,
    label: str,
) -> None:
    _reconcile_trusted_descriptor(
        entry.slot.descriptor, entry.identity, label=label
    )
    current = os.stat(
        entry.name, dir_fd=parent_fd, follow_symlinks=False
    )
    if _stat_identity(current) != entry.identity:
        raise RuntimeError(f"{label} name changed")
    _validate_trusted_parent(current, label=label)


def _open_trusted_directory_chain_at(
    anchor_fd: int,
    components: Sequence[str],
    *,
    label: str,
) -> tuple[_RetainedDirectory, ...]:
    nofollow = _required_os_flag("O_NOFOLLOW")
    directory = _required_os_flag("O_DIRECTORY")
    if any(not name or name in {".", ".."} or "/" in name for name in components):
        raise ValueError(f"invalid {label} component")
    retained: list[_RetainedDirectory] = []
    parent_fd = anchor_fd
    try:
        for name in components:
            expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            _validate_trusted_parent(expected, label=label)
            child_fd = os.open(
                name,
                os.O_RDONLY
                | nofollow
                | directory
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
            child_slot = _DescriptorSlot(child_fd)
            try:
                opened = os.fstat(child_fd)
                current = os.stat(
                    name, dir_fd=parent_fd, follow_symlinks=False
                )
                if len(
                    {
                        _stat_identity(expected),
                        _stat_identity(opened),
                        _stat_identity(current),
                    }
                ) != 1:
                    raise RuntimeError(f"{label} changed while opening")
                _validate_trusted_parent(opened, label=label)
            except BaseException as primary:
                close_error = _retire_descriptor_capability(child_slot)
                _raise_ordered_failures(
                    f"{label} open or close failed",
                    primary,
                    [close_error] if close_error is not None else [],
                )
                raise AssertionError("trusted-directory validation produced no error")
            retained.append(
                _RetainedDirectory(name, _stat_identity(opened), child_slot)
            )
            parent_fd = child_fd
        return tuple(retained)
    except BaseException as primary:
        close_errors: list[BaseException] = []
        for entry in reversed(retained):
            error = _retire_descriptor_capability(entry.slot)
            if error is not None:
                close_errors.append(error)
        _raise_ordered_failures(
            f"{label} traversal or close failed", primary, close_errors
        )
        raise AssertionError("trusted-directory traversal produced no error")


def _open_relative_directory_chain_at(
    anchor_fd: int,
    components: Sequence[str],
    *,
    label: str,
    create: bool,
    required_mode: int | None = None,
) -> tuple[_RetainedDirectory, ...]:
    nofollow = _required_os_flag("O_NOFOLLOW")
    directory = _required_os_flag("O_DIRECTORY")
    if any(not name or name in {".", ".."} or "/" in name for name in components):
        raise ValueError(f"invalid {label} component")
    retained: list[_RetainedDirectory] = []
    parent_fd = anchor_fd
    try:
        for name in components:
            created = False
            try:
                expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(name, 0o700, dir_fd=parent_fd)
                expected = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                created = True
            _validate_owned_entry(
                expected,
                label=label,
                kind="directory",
                mode=0o700 if created else required_mode,
            )
            child_fd = os.open(
                name,
                os.O_RDONLY
                | nofollow
                | directory
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
            child_slot = _DescriptorSlot(child_fd)
            try:
                opened = os.fstat(child_fd)
                current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if len(
                    {
                        _stat_identity(expected),
                        _stat_identity(opened),
                        _stat_identity(current),
                    }
                ) != 1:
                    raise RuntimeError(f"{label} changed while opening")
                _validate_owned_entry(
                    opened,
                    label=label,
                    kind="directory",
                    mode=0o700 if created else required_mode,
                )
            except BaseException as primary:
                close_error = _retire_descriptor_capability(child_slot)
                _raise_ordered_failures(
                    f"{label} open or close failed",
                    primary,
                    [close_error] if close_error is not None else [],
                )
                raise AssertionError("relative-directory validation produced no error")
            retained.append(
                _RetainedDirectory(name, _stat_identity(opened), child_slot)
            )
            parent_fd = child_fd
        return tuple(retained)
    except BaseException as primary:
        close_errors: list[BaseException] = []
        for entry in reversed(retained):
            error = _retire_descriptor_capability(entry.slot)
            if error is not None:
                close_errors.append(error)
        _raise_ordered_failures(
            f"{label} traversal or close failed", primary, close_errors
        )
        raise AssertionError("relative-directory traversal produced no error")


class _ResultParentCapability:
    """One-shot retained destination-parent chain rooted in a writer lease."""

    def __init__(
        self,
        token: object,
        lease: "ResultWriterLease",
        retained: tuple[_RetainedDirectory, ...],
    ) -> None:
        if token is not _LEASE_CONSTRUCTOR_TOKEN:
            raise TypeError("result parent capability cannot be constructed directly")
        self._lease = lease
        self._retained = retained
        self._owner_pid = os.getpid()
        self._closed = False

    @property
    def descriptor(self) -> int:
        self._validate_live()
        if self._retained:
            return self._retained[-1].slot.descriptor
        return self._lease._repository_slot.descriptor

    def _validate_live(self) -> None:
        if self._closed:
            raise RuntimeError("result parent capability is closed")
        if self._owner_pid != os.getpid():
            raise RuntimeError("result parent capability belongs to another process")
        self._lease._validate_live()
        parent_fd = self._lease._repository_slot.descriptor
        for entry in self._retained:
            _reconcile_named_descriptor_at(
                parent_fd,
                entry.name,
                entry.slot.descriptor,
                entry.identity,
                label="result parent",
                kind="directory",
                mode=None,
            )
            parent_fd = entry.slot.descriptor

    def close(self, primary: BaseException | None = None) -> None:
        if self._closed:
            if primary is not None:
                raise primary
            raise RuntimeError("result parent capability is closed")
        self._closed = True
        _retire_task_descriptors(
            tuple(entry.slot for entry in reversed(self._retained)),
            primary=primary,
            label="result parent operation or close failed",
        )


class _RecordDirectoryCapability:
    """Retained record directory rooted at one canonical run descriptor."""

    def __init__(
        self,
        *,
        anchor_path: Path,
        anchor_slot: _DescriptorSlot,
        anchor_identity: tuple[int, int],
        retained: tuple[_RetainedDirectory, ...],
        trusted_prefix_count: int,
        path: Path,
        label: str,
    ) -> None:
        if not retained:
            raise ValueError("record directory requires a retained path")
        if (
            type(trusted_prefix_count) is not int
            or trusted_prefix_count < 0
            or trusted_prefix_count > len(retained)
        ):
            raise ValueError("record directory trusted prefix is invalid")
        self._anchor_path = anchor_path
        self._anchor_slot = anchor_slot
        self._anchor_identity = anchor_identity
        self._retained = retained
        self._trusted_prefix_count = trusted_prefix_count
        self.path = path
        self.label = label
        self._owner_pid = os.getpid()
        self._closed = False

    def _validate_live(self) -> None:
        _require_lease_process_healthy()
        if self._closed:
            raise RuntimeError(f"{self.label} capability is closed")
        if self._owner_pid != os.getpid():
            raise RuntimeError(f"{self.label} capability belongs to another process")
        try:
            anchor_current = self._anchor_path.lstat()
        except OSError:
            raise ValueError(f"{self.label} anchor is unavailable") from None
        if _stat_identity(anchor_current) != self._anchor_identity:
            raise RuntimeError(f"{self.label} anchor name changed")
        _validate_trusted_parent(
            anchor_current, label=f"{self.label} anchor"
        )
        _reconcile_trusted_descriptor(
            self._anchor_slot.descriptor,
            self._anchor_identity,
            label=f"{self.label} anchor",
        )
        parent_fd = self._anchor_slot.descriptor
        for index, entry in enumerate(self._retained):
            if index < self._trusted_prefix_count:
                _reconcile_trusted_named_descriptor_at(
                    parent_fd, entry, label=self.label
                )
            else:
                _reconcile_named_descriptor_at(
                    parent_fd,
                    entry.name,
                    entry.slot.descriptor,
                    entry.identity,
                    label=self.label,
                    kind="directory",
                    mode=0o700,
                )
            parent_fd = entry.slot.descriptor

    def inventory(self) -> tuple[str, ...]:
        self._validate_live()
        before = os.fstat(self._retained[-1].slot.descriptor)
        try:
            names = tuple(sorted(os.listdir(self._retained[-1].slot.descriptor)))
        except OSError:
            raise ValueError(f"{self.label} is unavailable") from None
        after = os.fstat(self._retained[-1].slot.descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise RuntimeError(f"{self.label} changed while listing")
        self._validate_live()
        return names

    def close(self, primary: BaseException | None = None) -> None:
        if self._closed:
            if primary is not None:
                raise primary
            raise RuntimeError(f"{self.label} capability is closed")
        self._closed = True
        slots = tuple(
            entry.slot for entry in reversed(self._retained)
        ) + (self._anchor_slot,)
        _retire_task_descriptors(
            slots,
            primary=primary,
            label=f"{self.label} operation or close failed",
        )

    def __enter__(self) -> "_RecordDirectoryCapability":
        try:
            self._validate_live()
        except BaseException as primary:
            self.close(primary)
            raise AssertionError("record-directory entry produced no error")
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        error: BaseException | None,
        _traceback: object,
    ) -> bool:
        self.close(error)
        return False


class _RecordChildDirectoryCapability:
    """One retained child borrowed from a live record-directory capability."""

    def __init__(
        self,
        *,
        parent: _RecordDirectoryCapability,
        retained: tuple[_RetainedDirectory, ...],
        label: str,
    ) -> None:
        if len(retained) != 1:
            raise ValueError("record child requires exactly one retained directory")
        self._parent = parent
        self._retained = retained
        self.path = parent.path / retained[0].name
        self.label = label
        self._owner_pid = os.getpid()
        self._closed = False

    def _validate_live(self) -> None:
        _require_lease_process_healthy()
        if self._closed:
            raise RuntimeError(f"{self.label} capability is closed")
        if self._owner_pid != os.getpid():
            raise RuntimeError(f"{self.label} capability belongs to another process")
        self._parent._validate_live()
        entry = self._retained[0]
        _reconcile_named_descriptor_at(
            self._parent._retained[-1].slot.descriptor,
            entry.name,
            entry.slot.descriptor,
            entry.identity,
            label=self.label,
            kind="directory",
            mode=0o700,
        )

    def inventory(self) -> tuple[str, ...]:
        self._validate_live()
        descriptor = self._retained[-1].slot.descriptor
        before = os.fstat(descriptor)
        try:
            names = tuple(sorted(os.listdir(descriptor)))
        except OSError:
            raise ValueError(f"{self.label} is unavailable") from None
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise RuntimeError(f"{self.label} changed while listing")
        self._validate_live()
        return names

    def close(self, primary: BaseException | None = None) -> None:
        if self._closed:
            if primary is not None:
                raise primary
            raise RuntimeError(f"{self.label} capability is closed")
        self._closed = True
        _retire_task_descriptors(
            tuple(entry.slot for entry in reversed(self._retained)),
            primary=primary,
            label=f"{self.label} operation or close failed",
        )

    def __enter__(self) -> "_RecordChildDirectoryCapability":
        try:
            self._validate_live()
        except BaseException as primary:
            self.close(primary)
            raise AssertionError("record-child entry produced no error")
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        error: BaseException | None,
        _traceback: object,
    ) -> bool:
        self.close(error)
        return False


def _open_record_child_directory(
    parent: _RecordDirectoryCapability, name: str, *, label: str
) -> _RecordChildDirectoryCapability:
    parent._validate_live()
    retained = _open_relative_directory_chain_at(
        parent._retained[-1].slot.descriptor,
        (name,),
        label=label,
        create=False,
        required_mode=0o700,
    )
    capability = _RecordChildDirectoryCapability(
        parent=parent, retained=retained, label=label
    )
    try:
        capability._validate_live()
    except BaseException as primary:
        capability.close(primary)
        raise AssertionError("record-child acquisition produced no error")
    return capability


def _open_anchored_record_directory(
    *,
    anchor_path: Path,
    base_components: Sequence[str],
    record_components: Sequence[str],
    create: bool,
    label: str,
) -> _RecordDirectoryCapability:
    _require_lease_process_healthy()
    anchor_path = Path(anchor_path)
    if (
        not anchor_path.is_absolute()
        or anchor_path == Path("/")
        or any(
            component in {"", ".", ".."}
            for component in anchor_path.parts[1:]
        )
    ):
        raise ValueError(f"{label} run root must be an absolute canonical path")
    try:
        anchor_slot, anchor_identity = _open_absolute_directory_anchor(
            Path("/"), f"{label} filesystem root", trusted_parent=True
        )
    except BaseException as primary:
        _raise_task_failures(
            primary=primary,
            close_errors=[],
            label=f"{label} anchor acquisition failed",
        )
        raise AssertionError("record-directory anchor acquisition produced no error")
    trusted: tuple[_RetainedDirectory, ...] = ()
    run_root: tuple[_RetainedDirectory, ...] = ()
    base: tuple[_RetainedDirectory, ...] = ()
    record: tuple[_RetainedDirectory, ...] = ()
    try:
        run_components = anchor_path.parts[1:]
        trusted = _open_trusted_directory_chain_at(
            anchor_slot.descriptor,
            run_components[:-1],
            label=f"{label} run-root ancestor",
        )
        trusted_fd = (
            trusted[-1].slot.descriptor
            if trusted
            else anchor_slot.descriptor
        )
        run_root = _open_relative_directory_chain_at(
            trusted_fd,
            run_components[-1:],
            label=f"{label} run root",
            create=False,
            required_mode=0o700,
        )
        base = _open_relative_directory_chain_at(
            run_root[-1].slot.descriptor,
            base_components,
            label=f"{label} base",
            create=False,
            required_mode=0o700,
        )
        base_fd = (
            base[-1].slot.descriptor
            if base
            else run_root[-1].slot.descriptor
        )
        record = _open_relative_directory_chain_at(
            base_fd,
            record_components,
            label=label,
            create=create,
            required_mode=0o700,
        )
        retained = trusted + run_root + base + record
        capability = _RecordDirectoryCapability(
            anchor_path=Path("/"),
            anchor_slot=anchor_slot,
            anchor_identity=anchor_identity,
            retained=retained,
            trusted_prefix_count=len(trusted),
            path=anchor_path.joinpath(*base_components, *record_components),
            label=label,
        )
        capability._validate_live()
        return capability
    except BaseException as primary:
        retained = trusted + run_root + base + record
        slots = tuple(entry.slot for entry in reversed(retained)) + (anchor_slot,)
        _retire_task_descriptors(
            slots,
            primary=primary,
            label=f"{label} acquisition or close failed",
        )
        raise AssertionError("record-directory acquisition produced no error")


class RunCoordinatorLease:
    def __init__(
        self,
        token: object,
        *,
        run_root: Path,
        registry_key: bytes,
        epoch_id: str,
        run_kind: RunKind,
        owner_pid: int,
        parent_path: Path,
        parent_slot: _DescriptorSlot,
        parent_identity: tuple[int, int],
        run_slot: _DescriptorSlot,
        run_identity: tuple[int, int],
        coordinator_slot: _DescriptorSlot,
        coordinator_identity: tuple[int, int],
        lock_slot: _DescriptorSlot,
        lock_identity: tuple[int, int],
    ) -> None:
        if token is not _LEASE_CONSTRUCTOR_TOKEN:
            raise TypeError("RunCoordinatorLease cannot be constructed directly")
        self._run_root = run_root
        self._registry_key = registry_key
        self._epoch_id = epoch_id
        self._run_kind = run_kind
        self._owner_pid = owner_pid
        self._parent_path = parent_path
        self._parent_slot = parent_slot
        self._parent_identity = parent_identity
        self._run_slot = run_slot
        self._run_identity = run_identity
        self._coordinator_slot = coordinator_slot
        self._coordinator_identity = coordinator_identity
        self._lock_slot = lock_slot
        self._lock_identity = lock_identity
        self._closed = False
        self._writer_child = None

    @classmethod
    def acquire(
        cls, *, run_root: Path, epoch_id: str, run_kind: RunKind
    ) -> "RunCoordinatorLease":
        _require_lease_process_healthy()
        if cls is not RunCoordinatorLease:
            raise TypeError("RunCoordinatorLease subclasses are unsupported")
        if type(epoch_id) is not str or re.fullmatch(r"[0-9a-f]{64}", epoch_id) is None:
            raise ValueError("epoch_id must be 64 lowercase hexadecimal characters")
        if type(run_kind) is not str or run_kind not in (
            "diagnostic",
            "discovery",
            "formal",
        ):
            raise ValueError("invalid run kind")
        run_root = Path(run_root)
        if not run_root.is_absolute() or run_root != run_root.resolve(strict=False):
            raise ValueError("run_root must be an absolute canonical path")
        if not run_root.name:
            raise ValueError("run_root must name a managed directory")
        registry_key = os.fsencode(run_root)
        non_lock_slots: list[_DescriptorSlot] = []
        lock_slot: _DescriptorSlot | None = None
        with _LEASE_REGISTRY_LOCK:
            if _RESULT_WRITER_LEASES:
                raise RuntimeError("cannot acquire run lease after a result writer lease")
            if _RUN_LEASES:
                raise RuntimeError("run coordinator lease is already active")
            try:
                parent_slot, parent_identity = _open_absolute_directory_anchor(
                    run_root.parent, "run root parent", trusted_parent=True
                )
                non_lock_slots.append(parent_slot)
                run_slot, run_identity = _open_managed_directory_at(
                    parent_slot.descriptor,
                    run_root.name,
                    label="run root",
                    create=True,
                )
                non_lock_slots.append(run_slot)
                coordinator_slot, coordinator_identity = _open_managed_directory_at(
                    run_slot.descriptor,
                    "coordinator",
                    label="coordinator directory",
                    create=True,
                )
                non_lock_slots.append(coordinator_slot)
                lock_slot, lock_identity = _open_managed_lock_at(
                    coordinator_slot.descriptor,
                    "coordinator.lock",
                    label="coordinator lock",
                )
                fcntl.flock(
                    lock_slot.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB
                )
                if run_root.resolve(strict=True) != run_root:
                    raise RuntimeError("run_root changed during lease acquisition")
                lease = cls(
                    _LEASE_CONSTRUCTOR_TOKEN,
                    run_root=run_root,
                    registry_key=registry_key,
                    epoch_id=epoch_id,
                    run_kind=run_kind,
                    owner_pid=os.getpid(),
                    parent_path=run_root.parent,
                    parent_slot=parent_slot,
                    parent_identity=parent_identity,
                    run_slot=run_slot,
                    run_identity=run_identity,
                    coordinator_slot=coordinator_slot,
                    coordinator_identity=coordinator_identity,
                    lock_slot=lock_slot,
                    lock_identity=lock_identity,
                )
                _RUN_LEASES[registry_key] = lease
                return lease
            except BaseException as primary:
                _rollback_lease_acquisition(
                    non_lock_slots,
                    lock_slot,
                    primary=primary,
                    label="run coordinator lease acquisition rollback failed",
                )
                raise AssertionError("run lease rollback produced no error")

    def _validate_nominal(self) -> None:
        _require_lease_process_healthy()
        if self._closed:
            raise RuntimeError("run coordinator lease is closed")
        if self._owner_pid != os.getpid():
            raise RuntimeError("run coordinator lease belongs to another process")
        if _RUN_LEASES.get(self._registry_key) is not self:
            raise RuntimeError("run coordinator lease registry binding changed")

    def _validate_live(self) -> None:
        self._validate_nominal()
        _reconcile_descriptor(
            self._parent_slot.descriptor,
            self._parent_identity,
            label="run root parent",
            kind="directory",
            mode=None,
            require_owner=False,
        )
        _validate_trusted_parent(
            os.fstat(self._parent_slot.descriptor), label="run root parent"
        )
        current_parent = os.lstat(self._parent_path)
        if _stat_identity(current_parent) != self._parent_identity:
            raise RuntimeError("run root parent name changed")
        _reconcile_named_descriptor_at(
            self._parent_slot.descriptor,
            self._run_root.name,
            self._run_slot.descriptor,
            self._run_identity,
            label="run root",
            kind="directory",
            mode=0o700,
        )
        _reconcile_named_descriptor_at(
            self._run_slot.descriptor,
            "coordinator",
            self._coordinator_slot.descriptor,
            self._coordinator_identity,
            label="coordinator directory",
            kind="directory",
            mode=0o700,
        )
        _reconcile_named_descriptor_at(
            self._coordinator_slot.descriptor,
            "coordinator.lock",
            self._lock_slot.descriptor,
            self._lock_identity,
            label="coordinator lock",
            kind="file",
            mode=0o600,
        )

    @property
    def active(self) -> bool:
        if self._closed:
            _require_lease_process_healthy()
            return False
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            return True

    def close(self) -> None:
        with _LEASE_REGISTRY_LOCK:
            if self._closed:
                _require_lease_process_healthy()
                raise RuntimeError("run coordinator lease is closed")
            if self._writer_child is not None:
                raise RuntimeError("result writer lease must close before run lease")
            primary: BaseException | None = None
            if _LEASE_PROCESS_POISON is None:
                try:
                    self._validate_live()
                except BaseException as error:
                    primary = error
            _RUN_LEASES.pop(self._registry_key, None)
            self._closed = True
            _retire_task_descriptors(
                (
                    self._coordinator_slot,
                    self._run_slot,
                    self._parent_slot,
                    self._lock_slot,
                ),
                primary=primary,
                label="run coordinator lease close failed",
            )


def _canonical_git_repository_root(
    repository_root: Path,
) -> tuple[Path, tuple[int, int]]:
    repository_root = Path(repository_root)
    if not repository_root.is_absolute():
        raise ValueError("repository_root must be absolute")
    try:
        canonical = repository_root.resolve(strict=True)
    except OSError:
        raise ValueError("repository_root must exist") from None
    if canonical != repository_root:
        raise ValueError("repository_root must use its canonical spelling")
    metadata = os.lstat(repository_root)
    _validate_owned_entry(
        metadata, label="repository root", kind="directory", mode=None
    )
    try:
        completed = subprocess.run(
            ["git", "-C", str(repository_root), "rev-parse", "--show-toplevel"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("repository_root must be a supported Git worktree") from None
    if completed.returncode != 0:
        raise ValueError("repository_root must be a supported Git worktree")
    rendered = completed.stdout.rstrip("\n")
    if not rendered or "\n" in rendered:
        raise ValueError("Git returned an invalid worktree top-level")
    reported = Path(rendered)
    try:
        reported_canonical = reported.resolve(strict=True)
    except OSError:
        raise ValueError("Git worktree top-level is unavailable") from None
    if reported != repository_root or reported_canonical != repository_root:
        raise ValueError("repository_root must be the exact Git worktree top-level")
    return repository_root, _stat_identity(metadata)


def result_writer_lock_path(repository_root: Path) -> Path:
    canonical, _ = _canonical_git_repository_root(repository_root)
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise RuntimeError("POSIX leases require os.getuid")
    repository_key = hashlib.sha256(os.fsencode(canonical)).hexdigest()
    return Path(
        f"/var/tmp/workflow-observatory-result-locks-uid-{getuid()}",
        f"{repository_key}.lock",
    )


class ResultWriterAuthority:
    def __init__(self, token: object, lease: "ResultWriterLease") -> None:
        if token is not _LEASE_CONSTRUCTOR_TOKEN or type(lease) is not ResultWriterLease:
            raise TypeError("ResultWriterAuthority cannot be constructed directly")
        self._lease = lease
        self._owner_pid = os.getpid()
        self._consumed = False
        self._destinations: dict[str, Path] | None = None
        self._result_parent: _ResultParentCapability | None = None

    def _validate_nominal(self) -> None:
        if self._owner_pid != os.getpid():
            raise RuntimeError("result writer authority belongs to another process")
        self._lease._validate_nominal()
        if self._lease._authority is not self:
            raise RuntimeError("result writer authority binding changed")

    def _validate_live(self) -> None:
        self._validate_nominal()
        self._lease._validate_live()

    @property
    def repository_key(self) -> str:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            return self._lease._repository_key

    @property
    def role(self) -> CoordinatorRole:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            return self._lease._role

    @property
    def run_kind(self) -> Literal["formal"]:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            return "formal"

    @property
    def consumed(self) -> bool:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            return self._consumed

    def _consume(self, destinations: Mapping[str, Path]) -> dict[str, Path]:
        with _LEASE_REGISTRY_LOCK:
            self._validate_nominal()
            if self._consumed:
                raise RuntimeError("result writer authority was already consumed")
            frozen = _validate_result_destinations(
                destinations, repository_root=self._lease._repository_root
            )
            self._consumed = True
            self._destinations = frozen
            return frozen.copy()

    def _open_result_parent(self) -> _ResultParentCapability:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            if not self._consumed or self._destinations is None:
                raise RuntimeError("result writer authority was not consumed")
            if self._result_parent is not None:
                raise RuntimeError("result parent capability was already opened")
            relative_parent = self._destinations["forward"].parent.relative_to(
                self._lease._repository_root
            )
            try:
                retained = _open_relative_directory_chain_at(
                    self._lease._repository_slot.descriptor,
                    relative_parent.parts,
                    label="result parent",
                    create=True,
                )
            except BaseException as primary:
                _raise_task_failures(
                    primary=primary,
                    close_errors=[],
                    label="result parent acquisition failed",
                )
                raise AssertionError("result-parent acquisition produced no error")
            capability = _ResultParentCapability(
                _LEASE_CONSTRUCTOR_TOKEN, self._lease, retained
            )
            try:
                capability._validate_live()
            except BaseException as primary:
                capability.close(primary)
                raise AssertionError("result-parent validation produced no error")
            self._result_parent = capability
            return capability


class ResultWriterLease:
    def __init__(
        self,
        token: object,
        *,
        repository_root: Path,
        repository_identity: tuple[int, int],
        repository_key: str,
        registry_key: bytes,
        role: CoordinatorRole,
        owner_pid: int,
        repository_parent_slot: _DescriptorSlot,
        repository_parent_identity: tuple[int, int],
        repository_slot: _DescriptorSlot,
        lock_parent_slot: _DescriptorSlot,
        lock_parent_identity: tuple[int, int],
        root_name: str,
        root_slot: _DescriptorSlot,
        root_identity: tuple[int, int],
        lock_name: str,
        lock_slot: _DescriptorSlot,
        lock_identity: tuple[int, int],
        run_lease: RunCoordinatorLease | None,
    ) -> None:
        if token is not _LEASE_CONSTRUCTOR_TOKEN:
            raise TypeError("ResultWriterLease cannot be constructed directly")
        self._repository_root = repository_root
        self._repository_identity = repository_identity
        self._repository_key = repository_key
        self._registry_key = registry_key
        self._role = role
        self._owner_pid = owner_pid
        self._repository_parent_slot = repository_parent_slot
        self._repository_parent_identity = repository_parent_identity
        self._repository_slot = repository_slot
        self._repository_fd = repository_slot.descriptor
        self._parent_slot = lock_parent_slot
        self._parent_identity = lock_parent_identity
        self._root_name = root_name
        self._root_slot = root_slot
        self._root_identity = root_identity
        self._lock_name = lock_name
        self._lock_slot = lock_slot
        self._lock_identity = lock_identity
        self._run_lease = run_lease
        self._authority: ResultWriterAuthority | None = None
        self._closed = False

    @classmethod
    def acquire(
        cls,
        repository_root: Path,
        role: CoordinatorRole,
        run_kind: Literal["formal"],
        run_lease: RunCoordinatorLease | None = None,
    ) -> "ResultWriterLease":
        _require_lease_process_healthy()
        if cls is not ResultWriterLease:
            raise TypeError("ResultWriterLease subclasses are unsupported")
        if type(role) is not str or role not in (
            "serial-coordinator",
            "parallel-coordinator",
        ):
            raise ValueError("invalid result writer role")
        if type(run_kind) is not str or run_kind != "formal":
            raise ValueError("result writers require formal run kind")
        with _LEASE_REGISTRY_LOCK:
            if _RESULT_WRITER_LEASES:
                raise RuntimeError("a result writer lease is already active")
            if role == "serial-coordinator":
                if run_lease is not None:
                    raise TypeError("serial result writer requires run_lease=None")
                if _RUN_LEASES:
                    raise RuntimeError("serial result writer cannot nest under a run lease")
            else:
                if type(run_lease) is not RunCoordinatorLease:
                    raise TypeError("parallel result writer requires exact RunCoordinatorLease")
                run_lease._validate_live()
                if run_lease._run_kind != "formal":
                    raise ValueError("parallel result writer requires formal run lease")
                if run_lease._writer_child is not None:
                    raise RuntimeError("run lease already has a result writer child")

            canonical, repository_identity = _canonical_git_repository_root(
                repository_root
            )
            registry_key = os.fsencode(canonical)
            repository_key = hashlib.sha256(registry_key).hexdigest()
            lock_path = Path(
                f"/var/tmp/workflow-observatory-result-locks-uid-{os.getuid()}",
                f"{repository_key}.lock",
            )
            non_lock_slots: list[_DescriptorSlot] = []
            lock_slot: _DescriptorSlot | None = None
            try:
                repository_parent_slot, repository_parent_identity = (
                    _open_absolute_directory_anchor(
                        canonical.parent,
                        "repository parent",
                        trusted_parent=True,
                    )
                )
                non_lock_slots.append(repository_parent_slot)
                repository_slot, opened_repository_identity = (
                    _open_managed_directory_at(
                        repository_parent_slot.descriptor,
                        canonical.name,
                        label="repository root",
                        create=False,
                        mode=None,
                    )
                )
                non_lock_slots.append(repository_slot)
                if opened_repository_identity != repository_identity:
                    raise RuntimeError("repository changed while opening")

                lock_parent_slot, lock_parent_identity = (
                    _open_absolute_directory_anchor(
                        Path("/var/tmp").resolve(strict=True),
                        "result lock parent",
                        trusted_parent=True,
                    )
                )
                non_lock_slots.append(lock_parent_slot)
                root_slot, root_identity = _open_managed_directory_at(
                    lock_parent_slot.descriptor,
                    lock_path.parent.name,
                    label="result lock root",
                    create=True,
                )
                non_lock_slots.append(root_slot)
                lock_slot, lock_identity = _open_managed_lock_at(
                    root_slot.descriptor,
                    lock_path.name,
                    label="result writer lock",
                )
                fcntl.flock(
                    lock_slot.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB
                )
                _reconcile_named_descriptor_at(
                    repository_parent_slot.descriptor,
                    canonical.name,
                    repository_slot.descriptor,
                    repository_identity,
                    label="repository root",
                    kind="directory",
                    mode=None,
                )
                lease = cls(
                    _LEASE_CONSTRUCTOR_TOKEN,
                    repository_root=canonical,
                    repository_identity=repository_identity,
                    repository_key=repository_key,
                    registry_key=registry_key,
                    role=role,
                    owner_pid=os.getpid(),
                    repository_parent_slot=repository_parent_slot,
                    repository_parent_identity=repository_parent_identity,
                    repository_slot=repository_slot,
                    lock_parent_slot=lock_parent_slot,
                    lock_parent_identity=lock_parent_identity,
                    root_name=lock_path.parent.name,
                    root_slot=root_slot,
                    root_identity=root_identity,
                    lock_name=lock_path.name,
                    lock_slot=lock_slot,
                    lock_identity=lock_identity,
                    run_lease=run_lease,
                )
                _RESULT_WRITER_LEASES[registry_key] = lease
                if run_lease is not None:
                    run_lease._writer_child = lease
                return lease
            except BaseException as primary:
                _rollback_lease_acquisition(
                    non_lock_slots,
                    lock_slot,
                    primary=primary,
                    label="result writer lease acquisition rollback failed",
                )
                raise AssertionError("result writer rollback produced no error")

    def _validate_nominal(self) -> None:
        _require_lease_process_healthy()
        if self._closed:
            raise RuntimeError("result writer lease is closed")
        if self._owner_pid != os.getpid():
            raise RuntimeError("result writer lease belongs to another process")
        if _RESULT_WRITER_LEASES.get(self._registry_key) is not self:
            raise RuntimeError("result writer registry binding changed")
        if self._run_lease is not None:
            self._run_lease._validate_nominal()
            if self._run_lease._writer_child is not self:
                raise RuntimeError("parallel writer child binding changed")

    def _validate_live(self) -> None:
        self._validate_nominal()
        _reconcile_descriptor(
            self._repository_parent_slot.descriptor,
            self._repository_parent_identity,
            label="repository parent",
            kind="directory",
            mode=None,
            require_owner=False,
        )
        _validate_trusted_parent(
            os.fstat(self._repository_parent_slot.descriptor),
            label="repository parent",
        )
        _reconcile_named_descriptor_at(
            self._repository_parent_slot.descriptor,
            self._repository_root.name,
            self._repository_slot.descriptor,
            self._repository_identity,
            label="repository root",
            kind="directory",
            mode=None,
        )
        _reconcile_descriptor(
            self._parent_slot.descriptor,
            self._parent_identity,
            label="result lock parent",
            kind="directory",
            mode=None,
            require_owner=False,
        )
        _validate_trusted_parent(
            os.fstat(self._parent_slot.descriptor), label="result lock parent"
        )
        _reconcile_named_descriptor_at(
            self._parent_slot.descriptor,
            self._root_name,
            self._root_slot.descriptor,
            self._root_identity,
            label="result lock root",
            kind="directory",
            mode=0o700,
        )
        _reconcile_named_descriptor_at(
            self._root_slot.descriptor,
            self._lock_name,
            self._lock_slot.descriptor,
            self._lock_identity,
            label="result writer lock",
            kind="file",
            mode=0o600,
        )
        if self._run_lease is not None:
            self._run_lease._validate_live()

    def authority(self) -> ResultWriterAuthority:
        with _LEASE_REGISTRY_LOCK:
            self._validate_live()
            if self._authority is not None:
                raise RuntimeError("result writer authority was already issued")
            authority = ResultWriterAuthority(_LEASE_CONSTRUCTOR_TOKEN, self)
            self._authority = authority
            return authority

    def close(self) -> None:
        with _LEASE_REGISTRY_LOCK:
            if self._closed:
                _require_lease_process_healthy()
                raise RuntimeError("result writer lease is closed")
            primary: BaseException | None = None
            if _LEASE_PROCESS_POISON is None:
                try:
                    self._validate_live()
                except BaseException as error:
                    primary = error
            _RESULT_WRITER_LEASES.pop(self._registry_key, None)
            if self._run_lease is not None:
                if self._run_lease._writer_child is self:
                    self._run_lease._writer_child = None
                elif primary is None:
                    primary = RuntimeError("parallel writer child binding changed")
            self._closed = True
            _retire_task_descriptors(
                (
                    self._repository_slot,
                    self._repository_parent_slot,
                    self._root_slot,
                    self._parent_slot,
                    self._lock_slot,
                ),
                primary=primary,
                label="result writer lease close failed",
            )


def _validate_result_destinations(
    destinations: Mapping[str, Path], *, repository_root: Path
) -> dict[str, Path]:
    if type(destinations) is not dict:
        raise TypeError("result destinations must be an exact dict")
    snapshot = destinations.copy()
    keys = tuple(snapshot.keys())
    if any(type(key) is not str for key in keys) or set(keys) != {
        "forward", "lifecycle"
    }:
        raise ValueError("result destinations must contain forward and lifecycle")
    parents: list[Path] = []
    frozen: dict[str, Path] = {}
    concrete_path_type = type(Path("."))
    for mode in ("forward", "lifecycle"):
        value = snapshot[mode]
        if type(value) is not concrete_path_type:
            raise TypeError(f"{mode} result destination must be an exact Path")
        candidate = value
        if not candidate.is_absolute():
            raise ValueError(f"{mode} result destination must be absolute")
        if any(component in {"", ".", ".."} for component in candidate.parts[1:]):
            raise ValueError(f"{mode} result destination must be lexically normalized")
        try:
            relative = candidate.relative_to(repository_root)
        except ValueError:
            raise ValueError("result destinations must be contained in repository_root") from None
        if not relative.parts or relative == Path(".") or not candidate.name:
            raise ValueError("result destination must name a file inside repository_root")
        parents.append(candidate.parent)
        frozen[mode] = candidate
    if parents[0] != parents[1]:
        raise ValueError("paired result destinations must share one directory")
    return frozen


def is_indeterminate_descriptor_close(error: BaseException) -> bool:
    if not isinstance(error, BaseException):
        raise TypeError("error must be a BaseException")
    if getattr(error, _INDETERMINATE_CLOSE_MARKER, False) is True:
        return True
    if isinstance(error, BaseExceptionGroup):
        return any(
            is_indeterminate_descriptor_close(nested)
            for nested in error.exceptions
        )
    return False


@dataclass(frozen=True, order=True)
class CaseKey:
    mode: EvalMode
    ordinal: int
    case_id: str


@dataclass(frozen=True)
class CaseAssignment:
    key: CaseKey
    lane: LaneName
    route: Route
    manifest_sha256: str


@dataclass(frozen=True)
class AttemptPaths:
    root: Path
    start: Path
    terminal: Path


@dataclass(frozen=True)
class CasePaths:
    root: Path
    cleanup: Path
    attempts: Path
    staging: Path
    workspace: Path
    store: Path
    audit: Path
    payload: Path
    output: Path
    home: Path
    codex_home: Path
    tmp: Path
    config: Path
    cache: Path
    sealed: Path


@dataclass(frozen=True)
class InputFingerprints:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    archive_sha256: str
    marketplace_sha256: str
    evaluator_sha256: str
    transport_config_sha256: str
    forward_manifest_sha256: str
    lifecycle_manifest_sha256: str


@dataclass(frozen=True)
class EpochPlan:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    fingerprints: InputFingerprints
    assignments: tuple[CaseAssignment, ...]


_PROGRESS_EPOCH_CONTEXTS: dict[str, tuple[EpochPlan, bytes]] = {}


@dataclass(frozen=True)
class ResolvedTransportConfig:
    schema_version: int
    codex_version: str
    codex_executable_path: str
    codex_executable_sha256: str
    codex_executable_device: int
    codex_executable_inode: int
    codex_executable_size: int
    model: str
    model_reasoning_effort: str
    approval_policy: Literal["never"]
    sandbox_mode: Literal["workspace-write"]
    network_access: Literal[False]
    web_search: Literal["disabled"]
    multi_agent: Literal[True]
    exec_timeout_seconds: Literal[1200]
    app_server_timeout_seconds: Literal[600]
    gate_timeout_seconds: Literal[300]


@dataclass(frozen=True)
class BootstrapOwnership:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    bootstrap_device: int
    bootstrap_inode: int


@dataclass
class InstalledAuthBootstrap:
    path: Path
    ownership: BootstrapOwnership
    descriptor: int
    state: CleanupState
    descriptor_close_state: DescriptorCloseState
    descriptor_close_error: BaseException | None


@dataclass(frozen=True)
class BootstrapTombstoneReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    ownership_sha256: str
    bootstrap_device: int
    bootstrap_inode: int
    scrubbed: Literal[True]
    empty: Literal[True]
    canonical_binding: Literal["expected", "missing", "replaced"]
    producer: Literal["coordinator", "coordinator-recovery"]


@dataclass(frozen=True)
class CaseAuthOwnership:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    case: CaseKey
    case_root_device: int
    case_root_inode: int
    codex_home_device: int
    codex_home_inode: int


@dataclass
class InstalledCaseAuth:
    ownership: CaseAuthOwnership
    descriptor: int
    state: CleanupState
    descriptor_close_state: DescriptorCloseState
    descriptor_close_error: BaseException | None


@dataclass(frozen=True)
class TombstoneReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    case: CaseKey
    ownership_sha256: str
    case_root_device: int
    case_root_inode: int
    codex_home_device: int
    codex_home_inode: int
    scrubbed: Literal[True]
    empty: Literal[True]
    canonical_binding: Literal["expected", "missing", "replaced"]
    producer: Literal["worker", "coordinator-recovery"]


@dataclass(frozen=True)
class FailureSummary:
    classification: OutcomeClass
    type: str
    chars: int
    sha256: str


@dataclass(frozen=True)
class VerifiedTombstoneReceipt:
    receipt: TombstoneReceipt
    sha256: str


@dataclass(frozen=True)
class AttemptSeal:
    start: dict[str, object]
    terminal: dict[str, object]
    start_sha256: str
    terminal_sha256: str


@dataclass(frozen=True)
class CaseSeal:
    result: dict[str, object] | None
    evidence: dict[str, object]
    commit: dict[str, object]
    result_sha256: str | None
    evidence_sha256: str
    commit_sha256: str
    tombstone_receipt_sha256: str | None


@dataclass(frozen=True)
class ShardTerminal:
    key: CaseKey
    run_kind: RunKind
    status: CaseSealStatus
    classification: OutcomeClass
    attempt_terminal_sha256: str
    case_commit_sha256: str | None
    tombstone_receipt_sha256: str | None
    failure: FailureSummary | None


@dataclass(frozen=True)
class ShardSeal:
    status: CaseSealStatus
    terminals: tuple[ShardTerminal, ...]
    commit_sha256: str


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    reasoning_output_tokens: int
    total_tokens: int


@dataclass(frozen=True)
class ProgressMessage:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    lane: LaneName
    seq: int
    type: ProgressType
    case: CaseKey | None
    attempt: int | None
    status: CaseSealStatus | None
    classification: OutcomeClass | None
    model_started: bool | None
    usage: TokenUsage | None
    attempt_terminal_sha256: str | None
    case_commit_sha256: str | None
    shard_commit_sha256: str | None
    tombstone_receipt_sha256: str | None


@dataclass(frozen=True)
class Ack:
    schema_version: int
    epoch_id: str
    run_kind: RunKind
    lane: LaneName
    seq: int
    message_sha256: str
    decision: AckDecision


@dataclass(frozen=True)
class TeardownReceipt:
    schema_version: Literal[1]
    epoch_id: str
    run_kind: RunKind
    tombstone_receipts: tuple[tuple[CaseKey, str], ...]
    bootstrap_tombstone_receipt_sha256: str
    codex_homes_absent: Literal[True]
    bootstrap_absent: Literal[True]


def _open_regular_file(path: Path, label: str) -> tuple[int, os.stat_result]:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow and path.is_symlink():
        raise ValueError(f"{label} must be a regular non-symlink file")
    try:
        descriptor = os.open(path, flags | nofollow)
    except OSError:
        raise ValueError(f"{label} must be a regular non-symlink file") from None
    slot = _DescriptorSlot(descriptor)
    try:
        metadata = os.fstat(slot.descriptor)
    except BaseException as error:
        primary = error
    else:
        if stat.S_ISREG(metadata.st_mode):
            return slot.descriptor, metadata
        primary = ValueError(f"{label} must be a regular non-symlink file")
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        f"{label} validation or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    raise AssertionError("regular-file validation produced no error")


def _descriptor_sha256(descriptor: int) -> str:
    digest = hashlib.sha256()
    os.lseek(descriptor, 0, os.SEEK_SET)
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            break
        digest.update(chunk)
    return digest.hexdigest()


def _read_source_config(source_codex_home: Path) -> dict[str, object]:
    config_path = source_codex_home / "config.toml"
    try:
        descriptor, _ = _open_regular_file(config_path, "config.toml")
    except ValueError:
        return {}
    slot = _DescriptorSlot(descriptor)
    content = bytearray()
    primary: BaseException | None = None
    try:
        os.lseek(slot.descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            content.extend(chunk)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        "config read or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    try:
        decoded = tomllib.loads(bytes(content).decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError("config.toml is not valid UTF-8 TOML") from None
    if not isinstance(decoded, dict):
        raise ValueError("config.toml must contain a table")
    return decoded


def _required_config_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be an explicit non-empty string")
    return value


def resolve_transport_config(
    *,
    codex_executable: Path,
    source_codex_home: Path,
    requested_model: str | None,
    requested_reasoning_effort: str | None,
) -> ResolvedTransportConfig:
    try:
        executable = Path(codex_executable).expanduser().resolve(strict=True)
    except OSError:
        raise ValueError("Codex executable is unavailable") from None
    if not executable.is_absolute():
        raise ValueError("Codex executable must resolve to an absolute path")
    descriptor, metadata = _open_regular_file(executable, "Codex executable")
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    executable_sha256: str | None = None
    try:
        if metadata.st_mode & 0o111 == 0:
            raise ValueError("Codex executable is not executable")
        executable_sha256 = _descriptor_sha256(slot.descriptor)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        "Codex executable read or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    if executable_sha256 is None:
        raise AssertionError("Codex executable hash is unavailable")

    source_values = (
        _read_source_config(Path(source_codex_home))
        if requested_model is None or requested_reasoning_effort is None
        else {}
    )
    model = _required_config_string(
        requested_model if requested_model is not None else source_values.get("model"),
        "model",
    )
    reasoning = _required_config_string(
        requested_reasoning_effort
        if requested_reasoning_effort is not None
        else source_values.get("model_reasoning_effort"),
        "model_reasoning_effort",
    )

    try:
        completed = subprocess.run(
            [str(executable), "--version"],
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        raise ValueError("Codex executable version probe failed") from None
    codex_version = completed.stdout.strip()
    if completed.returncode != 0 or not codex_version:
        raise ValueError("Codex executable version probe failed")

    config = ResolvedTransportConfig(
        schema_version=1,
        codex_version=codex_version,
        codex_executable_path=str(executable),
        codex_executable_sha256=executable_sha256,
        codex_executable_device=metadata.st_dev,
        codex_executable_inode=metadata.st_ino,
        codex_executable_size=metadata.st_size,
        model=model,
        model_reasoning_effort=reasoning,
        approval_policy="never",
        sandbox_mode="workspace-write",
        network_access=False,
        web_search="disabled",
        multi_agent=True,
        exec_timeout_seconds=1200,
        app_server_timeout_seconds=600,
        gate_timeout_seconds=300,
    )
    verify_codex_executable(config)
    return config


def transport_config_bytes(config: ResolvedTransportConfig) -> bytes:
    if not isinstance(config, ResolvedTransportConfig):
        raise TypeError("config must be ResolvedTransportConfig")
    return canonical_config_bytes(asdict(config))


def verify_codex_executable(config: ResolvedTransportConfig) -> Path:
    path = Path(config.codex_executable_path)
    if not path.is_absolute():
        raise RuntimeError("sealed Codex executable path is not absolute")
    try:
        descriptor, metadata = _open_regular_file(path, "sealed Codex executable")
    except ValueError:
        raise RuntimeError("Codex executable identity changed") from None
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    actual_sha256: str | None = None
    try:
        actual_sha256 = _descriptor_sha256(slot.descriptor)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        "sealed executable read or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    if actual_sha256 is None:
        raise AssertionError("sealed executable hash is unavailable")
    actual_identity = (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        actual_sha256,
    )
    expected_identity = (
        config.codex_executable_device,
        config.codex_executable_inode,
        config.codex_executable_size,
        config.codex_executable_sha256,
    )
    if actual_identity != expected_identity or metadata.st_mode & 0o111 == 0:
        raise RuntimeError("Codex executable identity changed")
    return path


def _validate_private_auth(path: Path, label: str) -> int:
    try:
        descriptor, metadata = _open_regular_file(path, label)
    except ValueError:
        raise ValueError(f"safe {label} is required") from None
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        slot = _DescriptorSlot(descriptor)
        primary = ValueError(f"safe {label} is required")
        close_error = _retire_descriptor_capability(slot)
        _raise_ordered_failures(
            f"{label} validation or descriptor close failed",
            primary,
            [close_error] if close_error is not None else [],
        )
    return descriptor


def _copy_auth_descriptor(source_descriptor: int, destination: Path) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    destination_descriptor = os.open(destination, flags, 0o600)
    try:
        os.lseek(source_descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(source_descriptor, 1024 * 1024)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:
                written = os.write(destination_descriptor, view)
                view = view[written:]
        os.fsync(destination_descriptor)
    finally:
        os.close(destination_descriptor)


def _copy_auth_descriptor_at(
    source_descriptor: int, directory_descriptor: int, name: str
) -> None:
    if not isinstance(name, str) or not name or "/" in name or name in (".", ".."):
        raise ValueError("auth destination name is invalid")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    destination_descriptor = os.open(
        name, flags, 0o600, dir_fd=directory_descriptor
    )
    destination_slot = _DescriptorSlot(destination_descriptor)
    primary: BaseException | None = None
    try:
        os.fchmod(destination_slot.descriptor, 0o600)
        os.lseek(source_descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(source_descriptor, 1024 * 1024)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:
                written = os.write(destination_slot.descriptor, view)
                view = view[written:]
        os.fsync(destination_slot.descriptor)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(destination_slot)
    _raise_ordered_failures(
        "auth copy or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )


def _retire_descriptor_capability(owner: object) -> BaseException | None:
    """Invalidate one descriptor capability before its only close attempt."""

    close_state = getattr(owner, "descriptor_close_state", None)
    close_error = getattr(owner, "descriptor_close_error", None)
    if close_state == "closed":
        return None
    if close_state == "indeterminate":
        if not isinstance(close_error, BaseException):
            raise ValueError("indeterminate descriptor close has no stored error")
        return close_error
    if close_state != "owned" or close_error is not None:
        raise ValueError("descriptor close state is invalid")
    descriptor = getattr(owner, "descriptor", None)
    if type(descriptor) is not int or descriptor < 0:
        raise ValueError("owned descriptor capability is unavailable")

    local_descriptor = descriptor
    owner.descriptor = -1
    owner.descriptor_close_state = "closing"
    try:
        os.close(local_descriptor)
    except BaseException as error:
        setattr(error, _INDETERMINATE_CLOSE_MARKER, True)
        owner.descriptor_close_state = "indeterminate"
        owner.descriptor_close_error = error
        return error
    owner.descriptor_close_state = "closed"
    owner.descriptor_close_error = None
    return None


def _raise_ordered_failures(
    message: str,
    primary: BaseException | None,
    close_errors: Sequence[BaseException],
) -> None:
    errors = ([primary] if primary is not None else []) + list(close_errors)
    if not errors:
        return
    if len(errors) == 1:
        raise errors[0]
    group_type = (
        ExceptionGroup
        if all(isinstance(error, Exception) for error in errors)
        else BaseExceptionGroup
    )
    raise group_type(message, errors)


def _sanitize_setup_failure(error: BaseException, message: str) -> BaseException:
    if is_indeterminate_descriptor_close(error):
        return error
    if isinstance(error, (OSError, ValueError, TypeError)):
        sanitized = ValueError(message)
        sanitized.__suppress_context__ = True
        return sanitized
    return error


def _open_private_directory(path: Path, label: str) -> tuple[int, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ValueError(f"{label} must be a private directory") from None
    slot = _DescriptorSlot(descriptor)
    try:
        metadata = os.fstat(slot.descriptor)
    except BaseException as error:
        primary = error
    else:
        if (
            stat.S_ISDIR(metadata.st_mode)
            and stat.S_IMODE(metadata.st_mode) == 0o700
        ):
            return slot.descriptor, metadata
        primary = ValueError(f"{label} must be a private directory")
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        f"{label} validation or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    raise AssertionError("private-directory validation produced no error")


def _atomic_write_record(path: Path, payload: Mapping[str, Any]) -> bytes:
    content = canonical_config_bytes(payload)
    parent = path.parent
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    parent_descriptor, _ = _open_private_directory(parent, "record directory")
    parent_slot = _DescriptorSlot(parent_descriptor)
    temporary_slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        temporary_descriptor = os.open(
            temporary_name, flags, 0o600, dir_fd=parent_slot.descriptor
        )
        temporary_slot = _DescriptorSlot(temporary_descriptor)
        os.fchmod(temporary_slot.descriptor, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(temporary_slot.descriptor, view)
            view = view[written:]
        os.fsync(temporary_slot.descriptor)
    except BaseException as error:
        primary = error

    close_errors: list[BaseException] = []
    if temporary_slot is not None:
        error = _retire_descriptor_capability(temporary_slot)
        if error is not None:
            close_errors.append(error)
    indeterminate = (
        primary is not None
        and is_indeterminate_descriptor_close(primary)
    ) or any(
        is_indeterminate_descriptor_close(error) for error in close_errors
    )

    if primary is None and not indeterminate:
        try:
            os.replace(
                temporary_name,
                path.name,
                src_dir_fd=parent_slot.descriptor,
                dst_dir_fd=parent_slot.descriptor,
            )
            os.fsync(parent_slot.descriptor)
        except BaseException as error:
            primary = error
            indeterminate = is_indeterminate_descriptor_close(error)
    if primary is not None and not indeterminate:
        try:
            os.unlink(temporary_name, dir_fd=parent_slot.descriptor)
        except OSError:
            pass

    error = _retire_descriptor_capability(parent_slot)
    if error is not None:
        close_errors.append(error)
    _raise_ordered_failures(
        "record write or descriptor close failed", primary, close_errors
    )
    return content


def _read_canonical_record_at(
    *,
    parent_slot: _DescriptorSlot,
    parent_path: Path,
    parent_before: os.stat_result,
    name: str,
    label: str,
    byte_cap: int,
) -> tuple[dict[str, object], bytes]:
    if type(name) is not str or not name or "/" in name or name in (".", ".."):
        raise ValueError(f"{label} name is invalid")
    if type(byte_cap) is not int or byte_cap <= 0:
        raise ValueError("record byte cap must be a positive exact integer")
    file_slot: _DescriptorSlot | None = None
    before: os.stat_result | None = None
    primary: BaseException | None = None
    content = bytearray()
    after: os.stat_result | None = None
    try:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | _required_os_flag("O_NOFOLLOW")
        )
        try:
            descriptor = os.open(
                name, flags, dir_fd=parent_slot.descriptor
            )
        except OSError:
            raise ValueError(
                f"{label} must be a regular non-symlink file"
            ) from None
        file_slot = _DescriptorSlot(descriptor)
        before = os.fstat(file_slot.descriptor)
        named_before = os.stat(
            name, dir_fd=parent_slot.descriptor, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > byte_cap
            or (before.st_dev, before.st_ino)
            != (named_before.st_dev, named_before.st_ino)
        ):
            raise ValueError(f"{label} must be a canonical mode-0600 record")
        while True:
            chunk = os.read(file_slot.descriptor, min(1024 * 1024, byte_cap + 1))
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > byte_cap:
                raise ValueError(f"{label} exceeds its byte cap")
        after = os.fstat(file_slot.descriptor)
        named_after = os.stat(
            name, dir_fd=parent_slot.descriptor, follow_symlinks=False
        )
        parent_named_after = parent_path.lstat()
        if (
            (after.st_dev, after.st_ino)
            != (named_after.st_dev, named_after.st_ino)
            or (parent_before.st_dev, parent_before.st_ino)
            != (parent_named_after.st_dev, parent_named_after.st_ino)
            or not stat.S_ISDIR(parent_named_after.st_mode)
        ):
            raise ValueError(f"{label} changed while reading")
    except BaseException as error:
        primary = error
    slots = [file_slot] if file_slot is not None else []
    _retire_task_descriptors(
        slots,
        primary=primary,
        label=f"{label} read or descriptor close failed",
    )
    if before is None or after is None:
        raise AssertionError("canonical record read produced no metadata")
    if (
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or len(content) != before.st_size
    ):
        raise ValueError(f"{label} changed while reading")
    try:
        decoded = json.loads(bytes(content).decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError(f"{label} is not canonical ASCII JSON") from None
    if not isinstance(decoded, dict) or canonical_config_bytes(decoded) != bytes(content):
        raise ValueError(f"{label} is not canonical ASCII JSON")
    return decoded, bytes(content)


def _read_canonical_record(
    path: Path, label: str, *, byte_cap: int = 1024 * 1024
) -> tuple[dict[str, object], bytes]:
    _require_lease_process_healthy()
    if type(byte_cap) is not int or byte_cap <= 0:
        raise ValueError("record byte cap must be a positive exact integer")
    parent_descriptor, parent_before = _open_private_directory(
        path.parent, f"{label} parent"
    )
    parent_slot = _DescriptorSlot(parent_descriptor)
    result: tuple[dict[str, object], bytes] | None = None
    primary: BaseException | None = None
    try:
        result = _read_canonical_record_at(
            parent_slot=parent_slot,
            parent_path=path.parent,
            parent_before=parent_before,
            name=path.name,
            label=label,
            byte_cap=byte_cap,
        )
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [parent_slot],
        primary=primary,
        label=f"{label} read or descriptor close failed",
    )
    if result is None:
        raise AssertionError("canonical record read produced no result")
    return result


def _require_exact_fields(
    payload: Mapping[str, object], record_type: type, label: str
) -> None:
    expected = {field.name for field in fields(record_type)}
    if set(payload) != expected:
        raise ValueError(f"{label} has missing or unknown fields")


def _require_exact_field_names(
    payload: object, expected: frozenset[str], label: str
) -> dict[str, object]:
    if type(payload) is not dict or set(payload) != expected:
        raise ValueError(f"{label} has missing or unknown fields")
    return payload


def _is_sha256(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_plan_assignment(plan: EpochPlan, assignment: CaseAssignment) -> None:
    if not isinstance(plan, EpochPlan):
        raise TypeError("plan must be EpochPlan")
    if not isinstance(assignment, CaseAssignment):
        raise TypeError("assignment must be CaseAssignment")
    if (
        type(plan.schema_version) is not int
        or plan.schema_version != 1
        or plan.run_kind not in ("diagnostic", "discovery", "formal")
        or not isinstance(plan.epoch_id, str)
        or len(plan.epoch_id) != 64
        or any(character not in "0123456789abcdef" for character in plan.epoch_id)
        or assignment not in plan.assignments
    ):
        raise ValueError("assignment is not bound to the epoch plan")


def _decode_case_key(payload: object, label: str) -> CaseKey:
    if not isinstance(payload, dict):
        raise ValueError(f"{label} case is invalid")
    _require_exact_fields(payload, CaseKey, f"{label} case")
    key = CaseKey(
        mode=payload.get("mode"),
        ordinal=payload.get("ordinal"),
        case_id=payload.get("case_id"),
    )
    if (
        key.mode not in ("forward", "lifecycle")
        or type(key.ordinal) is not int
        or not isinstance(key.case_id, str)
    ):
        raise ValueError(f"{label} case is invalid")
    return key


def read_bootstrap_ownership(
    *, coordinator_root: Path, plan: EpochPlan
) -> BootstrapOwnership:
    if not isinstance(plan, EpochPlan):
        raise TypeError("plan must be EpochPlan")
    payload, _ = _read_canonical_record(
        Path(coordinator_root) / "cleanup/bootstrap-ownership.json",
        "bootstrap ownership",
    )
    _require_exact_fields(payload, BootstrapOwnership, "bootstrap ownership")
    ownership = BootstrapOwnership(**payload)
    if (
        type(ownership.schema_version) is not int
        or ownership.schema_version != 1
        or ownership.epoch_id != plan.epoch_id
        or ownership.run_kind != plan.run_kind
        or type(ownership.bootstrap_device) is not int
        or ownership.bootstrap_device < 0
        or type(ownership.bootstrap_inode) is not int
        or ownership.bootstrap_inode < 0
    ):
        raise ValueError("bootstrap ownership is stale or invalid")
    bootstrap = Path(coordinator_root) / "auth-bootstrap"
    descriptor, metadata = _open_private_directory(bootstrap, "auth bootstrap")
    slot = _DescriptorSlot(descriptor)
    close_error = _retire_descriptor_capability(slot)
    if close_error is not None:
        raise close_error
    if (metadata.st_dev, metadata.st_ino) != (
        ownership.bootstrap_device,
        ownership.bootstrap_inode,
    ):
        raise ValueError("bootstrap ownership is stale or invalid")
    return ownership


def prepare_auth_bootstrap(
    *, source_codex_home: Path, coordinator_root: Path, plan: EpochPlan
) -> InstalledAuthBootstrap:
    if (
        not isinstance(plan, EpochPlan)
        or type(plan.schema_version) is not int
        or plan.schema_version != 1
        or plan.run_kind not in ("diagnostic", "discovery", "formal")
        or not isinstance(plan.epoch_id, str)
        or len(plan.epoch_id) != 64
        or any(character not in "0123456789abcdef" for character in plan.epoch_id)
    ):
        raise TypeError("plan must be EpochPlan")
    source_slot = _DescriptorSlot(_validate_private_auth(
        Path(source_codex_home) / "auth.json", "auth.json"
    ))
    bootstrap_slot: _DescriptorSlot | None = None
    coordinator_slot: _DescriptorSlot | None = None
    cleanup_slot: _DescriptorSlot | None = None
    result_path: Path | None = None
    result_ownership: BootstrapOwnership | None = None
    primary: BaseException | None = None
    try:
        coordinator = Path(coordinator_root)
        try:
            if coordinator.resolve(strict=True) != coordinator:
                raise ValueError("coordinator root must be canonical")
        except OSError:
            raise ValueError("coordinator root must be canonical") from None
        coordinator_descriptor, _ = _open_private_directory(
            coordinator, "coordinator root"
        )
        coordinator_slot = _DescriptorSlot(coordinator_descriptor)
        cleanup = coordinator / "cleanup"
        try:
            os.mkdir("cleanup", 0o700, dir_fd=coordinator_slot.descriptor)
        except FileExistsError:
            pass
        else:
            os.fsync(coordinator_slot.descriptor)
        cleanup_descriptor, _ = _open_private_directory(
            cleanup, "coordinator cleanup directory"
        )
        cleanup_slot = _DescriptorSlot(cleanup_descriptor)
        bootstrap = coordinator / "auth-bootstrap"
        os.mkdir("auth-bootstrap", 0o700, dir_fd=coordinator_slot.descriptor)
        os.fsync(coordinator_slot.descriptor)
        bootstrap_descriptor, metadata = _open_private_directory(
            bootstrap, "auth bootstrap"
        )
        bootstrap_slot = _DescriptorSlot(bootstrap_descriptor)
        ownership = BootstrapOwnership(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            bootstrap_device=metadata.st_dev,
            bootstrap_inode=metadata.st_ino,
        )
        _atomic_write_record(
            cleanup / "bootstrap-ownership.json", asdict(ownership)
        )
        _copy_auth_descriptor_at(
            source_slot.descriptor, bootstrap_slot.descriptor, "auth.json"
        )
        os.fsync(bootstrap_slot.descriptor)
        result_path = bootstrap
        result_ownership = ownership
    except BaseException as error:
        primary = _sanitize_setup_failure(
            error, "could not create private auth bootstrap"
        )

    close_errors: list[BaseException] = []
    for slot in (
        cleanup_slot,
        coordinator_slot,
        source_slot,
    ):
        if slot is not None:
            error = _retire_descriptor_capability(slot)
            if error is not None:
                close_errors.append(error)
    if primary is not None or close_errors:
        if bootstrap_slot is not None:
            error = _retire_descriptor_capability(bootstrap_slot)
            if error is not None:
                close_errors.append(error)
        _raise_ordered_failures(
            "auth bootstrap setup or descriptor close failed",
            primary,
            close_errors,
        )
    if (
        result_path is None
        or result_ownership is None
        or bootstrap_slot is None
    ):
        raise AssertionError("auth bootstrap setup produced no result")
    return InstalledAuthBootstrap(
        path=result_path,
        ownership=result_ownership,
        descriptor=bootstrap_slot.descriptor,
        state="active",
        descriptor_close_state="owned",
        descriptor_close_error=None,
    )


def _decode_case_auth_ownership(
    *,
    payload: dict[str, object],
    content: bytes,
    plan: EpochPlan,
    assignment: CaseAssignment,
) -> tuple[CaseAuthOwnership, bytes]:
    _require_exact_fields(payload, CaseAuthOwnership, "case auth ownership")
    case = _decode_case_key(payload.get("case"), "case auth ownership")
    scalar = dict(payload)
    scalar["case"] = case
    ownership = CaseAuthOwnership(**scalar)
    if (
        type(ownership.schema_version) is not int
        or ownership.schema_version != 1
        or ownership.epoch_id != plan.epoch_id
        or ownership.run_kind != plan.run_kind
        or ownership.case != assignment.key
        or any(
            type(value) is not int or value < 0
            for value in (
                ownership.case_root_device,
                ownership.case_root_inode,
                ownership.codex_home_device,
                ownership.codex_home_inode,
            )
        )
    ):
        raise ValueError("case auth ownership is stale or invalid")
    return ownership, content


def read_case_auth_ownership(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths
) -> tuple[CaseAuthOwnership, bytes]:
    _validate_plan_assignment(plan, assignment)
    if paths != paths_for_case(paths.root.parent.parent, assignment):
        raise ValueError("case paths differ from the frozen assignment")
    payload, content = _read_canonical_record(
        paths.cleanup / "ownership.json", "case auth ownership"
    )
    return _decode_case_auth_ownership(
        payload=payload,
        content=content,
        plan=plan,
        assignment=assignment,
    )


def _tombstone_receipt_from_payload(
    payload: dict[str, object],
) -> TombstoneReceipt:
    _require_exact_fields(payload, TombstoneReceipt, "case auth tombstone")
    decoded = dict(payload)
    decoded["case"] = _decode_case_key(
        decoded.get("case"), "case auth tombstone"
    )
    receipt = TombstoneReceipt(**decoded)
    numeric = (
        receipt.schema_version,
        receipt.case_root_device,
        receipt.case_root_inode,
        receipt.codex_home_device,
        receipt.codex_home_inode,
    )
    if (
        any(type(value) is not int or value < 0 for value in numeric)
        or receipt.schema_version != 1
        or type(receipt.epoch_id) is not str
        or receipt.run_kind not in ("diagnostic", "discovery", "formal")
        or not _is_sha256(receipt.ownership_sha256)
        or receipt.scrubbed is not True
        or receipt.empty is not True
        or receipt.canonical_binding not in ("expected", "missing", "replaced")
        or receipt.producer not in ("worker", "coordinator-recovery")
    ):
        raise ValueError("case auth tombstone is invalid")
    return receipt


def _read_verified_tombstone_receipt(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
    required: bool,
) -> VerifiedTombstoneReceipt | None:
    _require_lease_process_healthy()
    _validate_plan_assignment(plan, assignment)
    if type(paths) is not CasePaths:
        raise TypeError("paths must be CasePaths")
    if paths != paths_for_case(paths.root.parent.parent, assignment):
        raise ValueError("case paths differ from the frozen assignment")
    cleanup_descriptor, cleanup_before = _open_private_directory(
        paths.cleanup, "case auth cleanup directory"
    )
    cleanup_slot = _DescriptorSlot(cleanup_descriptor)
    result: VerifiedTombstoneReceipt | None = None
    primary: BaseException | None = None
    try:
        if not required:
            try:
                os.stat(
                    "tombstone.json",
                    dir_fd=cleanup_slot.descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                result = None
            except OSError:
                raise ValueError("case auth tombstone is unavailable") from None
            else:
                required = True
        if required:
            ownership_payload, ownership_bytes = _read_canonical_record_at(
                parent_slot=cleanup_slot,
                parent_path=paths.cleanup,
                parent_before=cleanup_before,
                name="ownership.json",
                label="case auth ownership",
                byte_cap=1024 * 1024,
            )
            ownership, ownership_bytes = _decode_case_auth_ownership(
                payload=ownership_payload,
                content=ownership_bytes,
                plan=plan,
                assignment=assignment,
            )
            payload, content = _read_canonical_record_at(
                parent_slot=cleanup_slot,
                parent_path=paths.cleanup,
                parent_before=cleanup_before,
                name="tombstone.json",
                label="case auth tombstone",
                byte_cap=1024 * 1024,
            )
            receipt = _tombstone_receipt_from_payload(payload)
            expected_hash = hashlib.sha256(ownership_bytes).hexdigest()
            if (
                receipt.schema_version != 1
                or receipt.epoch_id != plan.epoch_id
                or receipt.run_kind != plan.run_kind
                or receipt.case != assignment.key
                or receipt.ownership_sha256 != expected_hash
                or receipt.case_root_device != ownership.case_root_device
                or receipt.case_root_inode != ownership.case_root_inode
                or receipt.codex_home_device != ownership.codex_home_device
                or receipt.codex_home_inode != ownership.codex_home_inode
                or receipt.scrubbed is not True
                or receipt.empty is not True
                or receipt.canonical_binding
                not in ("expected", "missing", "replaced")
                or receipt.producer not in ("worker", "coordinator-recovery")
            ):
                raise ValueError("case auth tombstone is stale or invalid")
            result = VerifiedTombstoneReceipt(
                receipt=receipt,
                sha256=hashlib.sha256(content).hexdigest(),
            )
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [cleanup_slot],
        primary=primary,
        label="case auth receipt read or descriptor close failed",
    )
    if required and result is None:
        raise AssertionError("verified tombstone read produced no result")
    return result


def read_verified_tombstone_receipt(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths
) -> VerifiedTombstoneReceipt:
    result = _read_verified_tombstone_receipt(
        plan=plan,
        assignment=assignment,
        paths=paths,
        required=True,
    )
    if result is None:
        raise AssertionError("verified tombstone read produced no result")
    return result


def read_tombstone_receipt(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths
) -> TombstoneReceipt:
    return read_verified_tombstone_receipt(
        plan=plan, assignment=assignment, paths=paths
    ).receipt


def _read_optional_verified_tombstone_receipt(
    *, plan: EpochPlan, assignment: CaseAssignment, paths: CasePaths
) -> VerifiedTombstoneReceipt | None:
    return _read_verified_tombstone_receipt(
        plan=plan,
        assignment=assignment,
        paths=paths,
        required=False,
    )


def install_case_auth(
    *, bootstrap: Path, plan: EpochPlan, assignment: CaseAssignment,
    paths: CasePaths
) -> InstalledCaseAuth:
    _validate_plan_assignment(plan, assignment)
    if paths != paths_for_case(paths.root.parent.parent, assignment):
        raise ValueError("case paths differ from the frozen assignment")
    bootstrap_path = Path(bootstrap)
    if bootstrap_path != paths.root.parent.parent / "coordinator/auth-bootstrap":
        raise ValueError("safe bootstrap auth.json is required")
    bootstrap_slot: _DescriptorSlot | None = None
    source_slot: _DescriptorSlot | None = None
    case_slot: _DescriptorSlot | None = None
    case_root_slot: _DescriptorSlot | None = None
    cleanup_slot: _DescriptorSlot | None = None
    result_ownership: CaseAuthOwnership | None = None
    primary: BaseException | None = None
    try:
        bootstrap_descriptor, bootstrap_metadata = _open_private_directory(
            bootstrap_path, "auth bootstrap"
        )
        bootstrap_slot = _DescriptorSlot(bootstrap_descriptor)
        bootstrap_ownership = read_bootstrap_ownership(
            coordinator_root=bootstrap_path.parent, plan=plan
        )
        if (
            bootstrap_metadata.st_dev,
            bootstrap_metadata.st_ino,
        ) != (
            bootstrap_ownership.bootstrap_device,
            bootstrap_ownership.bootstrap_inode,
        ):
            raise ValueError("safe bootstrap auth.json is required")
        auth_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        auth_flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        source_descriptor = os.open(
            "auth.json", auth_flags, dir_fd=bootstrap_slot.descriptor
        )
        source_slot = _DescriptorSlot(source_descriptor)
        source_metadata = os.fstat(source_slot.descriptor)
        if (
            not stat.S_ISREG(source_metadata.st_mode)
            or stat.S_IMODE(source_metadata.st_mode) != 0o600
        ):
            raise ValueError("safe bootstrap auth.json is required")

        case_root_descriptor, case_root_metadata = _open_private_directory(
            paths.root, "case root"
        )
        case_root_slot = _DescriptorSlot(case_root_descriptor)
        cleanup_descriptor, _ = _open_private_directory(
            paths.cleanup, "case cleanup directory"
        )
        cleanup_slot = _DescriptorSlot(cleanup_descriptor)
        os.fsync(case_root_slot.descriptor)
        try:
            os.stat(
                paths.codex_home.name,
                dir_fd=case_root_slot.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        else:
            raise ValueError("case Codex home already exists")
        os.mkdir(paths.codex_home.name, 0o700, dir_fd=case_root_slot.descriptor)
        os.fsync(case_root_slot.descriptor)
        case_descriptor, case_metadata = _open_private_directory(
            paths.codex_home, "case Codex home"
        )
        case_slot = _DescriptorSlot(case_descriptor)
        ownership = CaseAuthOwnership(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            case_root_device=case_root_metadata.st_dev,
            case_root_inode=case_root_metadata.st_ino,
            codex_home_device=case_metadata.st_dev,
            codex_home_inode=case_metadata.st_ino,
        )
        _atomic_write_record(paths.cleanup / "ownership.json", asdict(ownership))
        _copy_auth_descriptor_at(
            source_slot.descriptor, case_slot.descriptor, "auth.json"
        )
        os.fsync(case_slot.descriptor)
        result_ownership = ownership
    except BaseException as error:
        primary = _sanitize_setup_failure(
            error,
            "could not install safe case auth from safe bootstrap auth.json",
        )

    close_errors: list[BaseException] = []
    for slot in (
        cleanup_slot,
        case_root_slot,
        source_slot,
        bootstrap_slot,
    ):
        if slot is not None:
            error = _retire_descriptor_capability(slot)
            if error is not None:
                close_errors.append(error)
    if primary is not None or close_errors:
        if case_slot is not None:
            error = _retire_descriptor_capability(case_slot)
            if error is not None:
                close_errors.append(error)
        _raise_ordered_failures(
            "case auth setup or descriptor close failed",
            primary,
            close_errors,
        )
    if result_ownership is None or case_slot is None:
        raise AssertionError("case auth setup produced no result")
    return InstalledCaseAuth(
        ownership=result_ownership,
        descriptor=case_slot.descriptor,
        state="active",
        descriptor_close_state="owned",
        descriptor_close_error=None,
    )


def prepare_legacy_auth_bootstrap(
    *, source_codex_home: Path, coordinator_root: Path
) -> Path:
    """Task-9 compatibility path; parallel workers use durable ownership APIs."""

    source_descriptor = _validate_private_auth(
        Path(source_codex_home) / "auth.json", "auth.json"
    )
    bootstrap: Path | None = None
    try:
        coordinator = Path(coordinator_root)
        if coordinator.is_symlink() or not coordinator.is_dir():
            raise ValueError("coordinator root must be a directory")
        bootstrap = Path(
            tempfile.mkdtemp(prefix="auth-bootstrap-", dir=str(coordinator))
        )
        bootstrap.chmod(0o700)
        _copy_auth_descriptor(source_descriptor, bootstrap / "auth.json")
        return bootstrap
    except (OSError, ValueError):
        if bootstrap is not None:
            try:
                (bootstrap / "auth.json").unlink(missing_ok=True)
                bootstrap.rmdir()
            except OSError:
                pass
        raise ValueError("could not create private auth bootstrap") from None
    finally:
        os.close(source_descriptor)


def install_legacy_case_auth(*, bootstrap: Path, case_codex_home: Path) -> None:
    """Task-9 compatibility path; parallel workers use durable ownership APIs."""

    bootstrap_path = Path(bootstrap)
    if (
        bootstrap_path.is_symlink()
        or not bootstrap_path.is_dir()
        or stat.S_IMODE(bootstrap_path.stat().st_mode) != 0o700
    ):
        raise ValueError("safe bootstrap auth.json is required")
    source_descriptor = _validate_private_auth(
        bootstrap_path / "auth.json", "bootstrap auth.json"
    )
    case_home = Path(case_codex_home)
    created = False
    try:
        if case_home.exists() or case_home.is_symlink():
            raise ValueError("case Codex home already exists")
        case_home.mkdir(mode=0o700)
        created = True
        _copy_auth_descriptor(source_descriptor, case_home / "auth.json")
    except (OSError, ValueError):
        if created:
            try:
                (case_home / "auth.json").unlink(missing_ok=True)
                case_home.rmdir()
            except OSError:
                pass
        raise ValueError("could not install safe case auth") from None
    finally:
        os.close(source_descriptor)


_CASE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,127}$")
_DECLARED_STAGED_EXECUTABLES = frozenset(
    {
        PurePosixPath(
            "plugins/workflow-observer/scripts/workflow_observer_cli.py"
        ),
    }
)


def canonical_run_root(run_root: Path) -> Path:
    """Return an absolute run root only when its lexical path is canonical."""

    root = Path(run_root)
    if not root.is_absolute():
        raise ValueError("run root must be absolute and canonical")
    if ".." in root.parts:
        raise ValueError("run root contains a non-canonical parent traversal")
    try:
        canonical = root.resolve(strict=False)
    except OSError:
        raise ValueError("run root cannot be canonicalized") from None
    if canonical != root:
        raise ValueError("run root lexical/canonical mismatch or symlink alias")
    return canonical


def paths_for_case(run_root: Path, assignment: CaseAssignment) -> CasePaths:
    if not isinstance(assignment, CaseAssignment):
        raise TypeError("assignment must be a CaseAssignment")
    key = assignment.key
    if key.mode not in ("forward", "lifecycle"):
        raise ValueError("case mode is invalid")
    if type(key.ordinal) is not int or key.ordinal < 1 or key.ordinal > 99:
        raise ValueError("case ordinal must be in 1..99")
    if not _CASE_ID_PATTERN.fullmatch(key.case_id):
        raise ValueError("case ID is not a safe path component")
    bound_run_root = canonical_run_root(run_root)
    root = bound_run_root / "cases" / (
        f"{key.mode}-{key.ordinal:02d}-{key.case_id}"
    )
    return CasePaths(
        root=root,
        cleanup=root / "cleanup",
        attempts=root / "attempts",
        staging=root / "staging",
        workspace=root / "workspace" / key.case_id,
        store=root / "store",
        audit=root / "audit",
        payload=root / "payload",
        output=root / "output",
        home=root / "home",
        codex_home=root / "codex-home",
        tmp=root / "tmp",
        config=root / "config",
        cache=root / "cache",
        sealed=root / "sealed",
    )


def paths_for_attempt(case: CasePaths, attempt: Literal[1, 2]) -> AttemptPaths:
    if not isinstance(case, CasePaths):
        raise TypeError("case must be CasePaths")
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    root = case.attempts / f"{attempt:02d}"
    return AttemptPaths(
        root=root,
        start=root / "start.json",
        terminal=root / "terminal.json",
    )


def _ensure_private_directory(path: Path) -> None:
    path = Path(path)
    missing = []
    cursor = path
    while not cursor.exists():
        if cursor.is_symlink():
            raise ValueError("directory path contains a symlink")
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            raise ValueError("directory path has no existing parent")
        cursor = parent
    try:
        metadata = cursor.lstat()
    except OSError:
        raise ValueError("directory path is unavailable") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise ValueError("directory path contains a non-directory component")
    for directory in reversed(missing):
        try:
            directory.mkdir(mode=0o700)
            directory.chmod(0o700)
        except OSError:
            raise ValueError("could not create private directory") from None


def _copy_staged_file(
    source: Path, destination: Path, relative: PurePosixPath
) -> None:
    source_descriptor, source_metadata = _open_regular_file(
        source, f"captured marketplace file {relative.as_posix()}"
    )
    source_slot = _DescriptorSlot(source_descriptor)
    destination_slot: _DescriptorSlot | None = None
    mode = 0o700 if relative in _DECLARED_STAGED_EXECUTABLES else 0o600
    primary: BaseException | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        destination_descriptor = os.open(destination, flags, mode)
        destination_slot = _DescriptorSlot(destination_descriptor)
        while True:
            chunk = os.read(source_slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:
                written = os.write(destination_slot.descriptor, view)
                view = view[written:]
        os.fsync(destination_slot.descriptor)
        os.fchmod(destination_slot.descriptor, mode)
        current = os.fstat(source_slot.descriptor)
        if (
            current.st_dev,
            current.st_ino,
            current.st_size,
        ) != (
            source_metadata.st_dev,
            source_metadata.st_ino,
            source_metadata.st_size,
        ):
            raise ValueError("captured marketplace changed while staging")
    except BaseException as error:
        primary = (
            ValueError("could not stage captured marketplace file")
            if isinstance(error, OSError)
            and not is_indeterminate_descriptor_close(error)
            else error
        )
    close_errors: list[BaseException] = []
    for slot in (destination_slot, source_slot):
        if slot is not None:
            error = _retire_descriptor_capability(slot)
            if error is not None:
                close_errors.append(error)
    _raise_ordered_failures(
        "marketplace staging or descriptor close failed",
        primary,
        close_errors,
    )


def _copy_staged_directory(
    source: Path, destination: Path, relative: PurePosixPath
) -> None:
    try:
        entries = sorted(os.scandir(source), key=lambda entry: entry.name)
    except OSError:
        raise ValueError("could not scan captured marketplace") from None
    for entry in entries:
        child_relative = relative / entry.name
        try:
            metadata = entry.stat(follow_symlinks=False)
        except OSError:
            raise ValueError("captured marketplace entry is unavailable") from None
        source_child = source / entry.name
        destination_child = destination / entry.name
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError("captured marketplace contains a symlink")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o555:
                raise ValueError(
                    "captured marketplace directory must have mode 0555"
                )
            try:
                destination_child.mkdir(mode=0o700)
                destination_child.chmod(0o700)
            except OSError:
                raise ValueError("could not stage marketplace directory") from None
            _copy_staged_directory(
                source_child, destination_child, child_relative
            )
            continue
        if stat.S_ISREG(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o444:
                raise ValueError("captured marketplace file must have mode 0444")
            _copy_staged_file(source_child, destination_child, child_relative)
            continue
        raise ValueError("captured marketplace contains a special file")


def stage_marketplace_for_case(
    *, read_only_snapshot: Path, destination: Path
) -> Path:
    source = Path(read_only_snapshot)
    destination = Path(destination)
    try:
        source_metadata = source.lstat()
    except OSError:
        raise ValueError("captured marketplace root is unavailable") from None
    if stat.S_ISLNK(source_metadata.st_mode) or not stat.S_ISDIR(
        source_metadata.st_mode
    ):
        raise ValueError("captured marketplace root must be a real directory")
    if stat.S_IMODE(source_metadata.st_mode) != 0o555:
        raise ValueError("captured marketplace root must have mode 0555")
    if destination.exists() or destination.is_symlink():
        raise ValueError("marketplace staging destination already exists")
    _ensure_private_directory(destination.parent)
    try:
        destination.mkdir(mode=0o700)
        destination.chmod(0o700)
        _copy_staged_directory(source, destination, PurePosixPath())
    except BaseException as error:
        if not is_indeterminate_descriptor_close(error):
            try:
                shutil.rmtree(destination)
            except OSError:
                pass
        raise
    return destination


def _validated_attempt_file(path: Path, label: str) -> None:
    try:
        descriptor, metadata = _open_regular_file(path, label)
    except ValueError:
        raise ValueError(f"{label} must be a regular non-symlink file") from None
    slot = _DescriptorSlot(descriptor)
    content = bytearray()
    primary: BaseException | None = None
    try:
        while True:
            chunk = os.read(slot.descriptor, 1024 * 1024)
            if not chunk:
                break
            content.extend(chunk)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(slot)
    _raise_ordered_failures(
        f"{label} read or descriptor close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise ValueError(f"{label} must have mode 0600")
    try:
        decoded = json.loads(bytes(content).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError(f"{label} must contain valid JSON") from None
    if not isinstance(decoded, dict):
        raise ValueError(f"{label} must contain a JSON object")


def scan_attempts(
    case: CasePaths, *, plan: EpochPlan
) -> tuple[AttemptPaths, ...]:
    if not isinstance(case, CasePaths):
        raise TypeError("case must be CasePaths")
    assignments = getattr(plan, "assignments", None)
    if not isinstance(assignments, tuple):
        raise TypeError("plan must provide tuple assignments")
    run_root = canonical_run_root(case.root.parent.parent)
    matches = []
    for assignment in assignments:
        if not isinstance(assignment, CaseAssignment):
            continue
        expected = paths_for_case(run_root, assignment)
        if expected.root == case.root:
            matches.append((assignment, expected))
    if len(matches) != 1:
        raise ValueError("case paths do not identify exactly one planned case")
    _, canonical = matches[0]
    if case != canonical:
        raise ValueError("case paths differ from canonical planned paths")
    if not case.attempts.exists() and not case.attempts.is_symlink():
        return ()
    try:
        attempts_metadata = case.attempts.lstat()
    except OSError:
        raise ValueError("attempt root is unavailable") from None
    if stat.S_ISLNK(attempts_metadata.st_mode) or not stat.S_ISDIR(
        attempts_metadata.st_mode
    ):
        raise ValueError("attempt root must be a real directory")
    try:
        entries = sorted(os.scandir(case.attempts), key=lambda entry: entry.name)
    except OSError:
        raise ValueError("could not scan attempts") from None
    names = [entry.name for entry in entries]
    if any(name not in {"01", "02"} for name in names):
        raise ValueError("attempt directory name is invalid")
    if names not in ([], ["01"], ["01", "02"]):
        raise ValueError("attempt sequence contains a gap or duplicate")

    found = []
    for attempt_number, entry in enumerate(entries, start=1):
        try:
            metadata = entry.stat(follow_symlinks=False)
        except OSError:
            raise ValueError("attempt directory is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("attempt entry must be a real directory")
        paths = paths_for_attempt(case, attempt_number)
        try:
            child_names = sorted(child.name for child in os.scandir(paths.root))
        except OSError:
            raise ValueError("could not scan attempt directory") from None
        expected = ["start.json", "terminal.json"]
        if child_names != expected:
            raise ValueError("attempt is partial or contains unexpected files")
        _validated_attempt_file(paths.start, "attempt start")
        _validated_attempt_file(paths.terminal, "attempt terminal")
        found.append(paths)
    return tuple(found)


_ATTEMPT_START_FIELDS = frozenset(
    {
        "schema_version",
        "epoch_id",
        "run_kind",
        "case",
        "lane",
        "route",
        "attempt",
        "manifest_sha256",
        "manifest_case_sha256",
    }
)
_ATTEMPT_TERMINAL_FIELDS = _ATTEMPT_START_FIELDS | frozenset(
    {
        "start_sha256",
        "status",
        "classification",
        "model_started",
        "cleanup_passed",
        "usage",
        "failure",
        "tombstone_receipt_sha256",
    }
)
_EVIDENCE_INPUT_FIELDS = frozenset(
    {
        "status",
        "classification",
        "model_started",
        "elapsed_milliseconds",
        "usage",
        "failure",
        "store_record_count",
        "store_invalidated_count",
        "audit_event_count",
        "payload_file_count",
        "output_file_count",
        "process_cleanup_passed",
        "credential_cleanup_passed",
    }
)
_CASE_EVIDENCE_FIELDS = _EVIDENCE_INPUT_FIELDS | frozenset(
    {
        "schema_version",
        "epoch_id",
        "run_kind",
        "case",
        "lane",
        "route",
        "attempt",
        "manifest_sha256",
        "manifest_case_sha256",
        "archive_sha256",
        "marketplace_sha256",
        "evaluator_sha256",
        "transport_config_sha256",
        "attempt_start_sha256",
        "attempt_terminal_sha256",
        "result_sha256",
        "tombstone_receipt_sha256",
    }
)
_CASE_COMMIT_FIELDS = frozenset(
    {
        "schema_version",
        "epoch_id",
        "run_kind",
        "case",
        "lane",
        "route",
        "attempt",
        "status",
        "manifest_sha256",
        "manifest_case_sha256",
        "result_file",
        "result_sha256",
        "evidence_file",
        "evidence_sha256",
        "attempt_start_sha256",
        "attempt_terminal_sha256",
        "tombstone_receipt_sha256",
    }
)
_OUTCOME_CLASSES = frozenset(
    {
        "success",
        "semantic",
        "model",
        "pre-model-infrastructure",
        "cleanup",
        "production-mutation",
        "manifest-mutation",
        "timeout",
        "protocol",
        "post-start-transport",
        "surviving-process",
        "coordinator-crash",
    }
)
_FAILURE_TYPE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_.]{0,127}")
_RECORD_ROLE_PATTERN = re.compile(r"run-[1-9][0-9]*")


def _require_nonempty_string(value: object, label: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label} must be a nonempty exact string")
    return value


def _validate_manifest_records(records: object, label: str) -> None:
    if type(records) is not list:
        raise ValueError(f"{label} must be an exact list")
    for record in records:
        _require_exact_field_names(
            record, NORMALIZED_RECORD_FIELDS, "manifest normalized record"
        )
        role = _require_nonempty_string(record.get("role"), "manifest record role")
        superseded = record.get("superseded_by_role")
        if (
            _RECORD_ROLE_PATTERN.fullmatch(role) is None
            or type(record.get("status")) is not str
            or not record["status"].strip()
            or type(record.get("start_mode")) is not str
            or not record["start_mode"].strip()
            or (
                superseded is not None
                and (
                    type(superseded) is not str
                    or _RECORD_ROLE_PATTERN.fullmatch(superseded) is None
                )
            )
        ):
            raise ValueError("manifest normalized record is invalid")


def _validate_manifest_checkpoints(checkpoints: object) -> None:
    if type(checkpoints) is not list:
        raise ValueError("manifest checkpoints must be an exact list")
    for checkpoint in checkpoints:
        _require_exact_field_names(
            checkpoint, CHECKPOINT_FIELDS, "manifest checkpoint"
        )
        if type(checkpoint.get("after_turn")) is not int:
            raise ValueError("manifest checkpoint turn is invalid")
        _validate_manifest_records(
            checkpoint.get("records"), "manifest checkpoint records"
        )


def _validate_manifest_statuses(statuses: object) -> None:
    if type(statuses) is not list or any(
        type(status) is not str or not status.strip() for status in statuses
    ):
        raise ValueError("manifest final statuses are invalid")


def _validate_manifest_count(value: object, label: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{label} is invalid")


def _expected_lifecycle_mode(assignment: CaseAssignment) -> str:
    if not isinstance(assignment, CaseAssignment):
        raise TypeError("assignment must be a CaseAssignment")
    if assignment.key.mode != "lifecycle":
        raise ValueError("lifecycle mode requires a lifecycle assignment")
    identity = (
        assignment.key.mode,
        assignment.key.ordinal,
        assignment.key.case_id,
    )
    return (
        "command-selection-only"
        if identity == _COMMAND_SELECTION_LIFECYCLE_KEY
        else "executable"
    )


def _validate_manifest_case(
    manifest_case: dict[str, object], *, mode: EvalMode
) -> None:
    expected_fields = (
        DECISION_MANIFEST_FIELDS if mode == "forward" else LIFECYCLE_MANIFEST_FIELDS
    )
    _require_exact_field_names(manifest_case, expected_fields, "manifest case")
    _require_nonempty_string(manifest_case.get("id"), "manifest case ID")
    turns = manifest_case.get("turns")
    if type(turns) is not list or not turns:
        raise ValueError("manifest turns must be a nonempty exact list")
    for turn in turns:
        _require_exact_field_names(turn, TURN_FIELDS, "manifest turn")
        _require_nonempty_string(turn.get("prompt"), "manifest turn prompt")
        dispatch_when = turn.get("dispatch_when")
        if type(dispatch_when) is not str or dispatch_when not in DISPATCH_VALUES:
            raise ValueError("manifest turn dispatch is invalid")
    fixture = manifest_case.get("fixture")
    if type(fixture) is not str or fixture not in FIXTURE_VALUES:
        raise ValueError("manifest fixture is invalid")

    if mode == "forward":
        decisions = manifest_case.get("expected_decisions")
        if type(decisions) is not list:
            raise ValueError("manifest expected decisions must be an exact list")
        triggered = False
        for decision in decisions:
            _require_exact_field_names(
                decision, EXPECTED_DECISION_FIELDS, "manifest expected decision"
            )
            if (
                type(decision.get("after_turn")) is not int
                or type(decision.get("triggered")) is not bool
            ):
                raise ValueError("manifest expected decision is invalid")
            triggered = triggered or decision["triggered"]
        taxonomy = (
            manifest_case.get("task_type"),
            manifest_case.get("workflow_variant"),
        )
        if triggered:
            for value in taxonomy:
                _require_nonempty_string(value, "manifest decision taxonomy")
        elif taxonomy != (None, None):
            raise ValueError("untriggered manifest taxonomy must be null")
        _validate_manifest_checkpoints(
            manifest_case.get("expected_record_checkpoints")
        )
        _validate_manifest_count(
            manifest_case.get("expected_run_count"), "manifest expected run count"
        )
        _validate_manifest_statuses(manifest_case.get("expected_final_statuses"))
        return

    lifecycle_mode = manifest_case.get("mode")
    if type(lifecycle_mode) is not str or lifecycle_mode not in LIFECYCLE_MODES:
        raise ValueError("manifest lifecycle mode is invalid")
    setup = manifest_case.get("setup")
    _require_exact_field_names(setup, SETUP_FIELDS, "manifest setup")
    for field in SETUP_FIELDS:
        _require_nonempty_string(setup.get(field), f"manifest setup {field}")
    store_fields = (
        "expected_record_checkpoints",
        "expected_run_count",
        "expected_draft_count",
        "expected_final_statuses",
        "expect_failure_disclosure",
    )
    if lifecycle_mode == "command-selection-only":
        if any(manifest_case.get(field) is not None for field in store_fields):
            raise ValueError("command-selection manifest store fields must be null")
        _require_nonempty_string(
            manifest_case.get("expected_selected_command"),
            "manifest selected command",
        )
        return
    _validate_manifest_checkpoints(
        manifest_case.get("expected_record_checkpoints")
    )
    _validate_manifest_count(
        manifest_case.get("expected_run_count"), "manifest expected run count"
    )
    _validate_manifest_count(
        manifest_case.get("expected_draft_count"), "manifest expected draft count"
    )
    _validate_manifest_statuses(manifest_case.get("expected_final_statuses"))
    if (
        type(manifest_case.get("expect_failure_disclosure")) is not bool
        or manifest_case.get("expected_selected_command") is not None
    ):
        raise ValueError("executable lifecycle manifest fields are invalid")


def _validate_seal_context(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
) -> str:
    _require_lease_process_healthy()
    _validate_plan_assignment(plan, assignment)
    if not isinstance(paths, CasePaths):
        raise TypeError("paths must be CasePaths")
    expected = paths_for_case(paths.root.parent.parent, assignment)
    if paths != expected:
        raise ValueError("case paths differ from the frozen assignment")
    if type(manifest_case) is not dict:
        raise TypeError("manifest_case must be an exact dict")
    _validate_manifest_case(manifest_case, mode=assignment.key.mode)
    if type(manifest_case.get("id")) is not str or manifest_case.get(
        "id"
    ) != assignment.key.case_id:
        raise ValueError("manifest case ID differs from the assignment")
    if (
        assignment.key.mode == "lifecycle"
        and manifest_case.get("mode") != _expected_lifecycle_mode(assignment)
    ):
        raise ValueError("manifest lifecycle mode differs from the frozen case")
    if (
        assignment.manifest_sha256
        != (
            plan.fingerprints.forward_manifest_sha256
            if assignment.key.mode == "forward"
            else plan.fingerprints.lifecycle_manifest_sha256
        )
    ):
        raise ValueError("assignment manifest hash differs from the epoch plan")
    return hashlib.sha256(canonical_config_bytes(manifest_case)).hexdigest()


def _open_case_record_directory(
    *,
    paths: CasePaths,
    components: Sequence[str],
    create: bool,
    label: str,
) -> _RecordDirectoryCapability:
    run_root = paths.root.parent.parent
    try:
        base_components = paths.root.relative_to(run_root).parts
    except ValueError:
        raise ValueError("case root escapes the run root") from None
    if not base_components:
        raise ValueError("case root does not name a case")
    return _open_anchored_record_directory(
        anchor_path=run_root,
        base_components=base_components,
        record_components=components,
        create=create,
        label=label,
    )


def _validate_usage(value: object, *, nullable: bool) -> dict[str, int] | None:
    if value is None:
        if nullable:
            return None
        raise ValueError("usage is required")
    expected = {
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    }
    if type(value) is not dict or set(value) != expected:
        raise ValueError("usage has missing or unknown fields")
    if any(
        type(value[field]) is not int
        or value[field] < 0
        or value[field] > 2**63 - 1
        for field in expected
    ):
        raise ValueError("usage contains an invalid token count")
    if (
        value["cached_input_tokens"] > value["input_tokens"]
        or value["reasoning_output_tokens"] > value["output_tokens"]
        or value["total_tokens"]
        != value["input_tokens"] + value["output_tokens"]
    ):
        raise ValueError("usage token totals are inconsistent")
    return dict(value)


def _validate_failure(
    value: object, *, classification: object, nullable: bool
) -> dict[str, object] | None:
    if value is None:
        if nullable:
            return None
        raise ValueError("failure summary is required")
    if type(value) is not dict or set(value) != {
        "classification",
        "type",
        "chars",
        "sha256",
    }:
        raise ValueError("failure has missing or unknown fields")
    if (
        value["classification"] != classification
        or value["classification"] not in _OUTCOME_CLASSES - {"success"}
        or type(value["type"]) is not str
        or _FAILURE_TYPE_PATTERN.fullmatch(value["type"]) is None
        or type(value["chars"]) is not int
        or value["chars"] < 0
        or value["chars"] > MAX_SEAL_FAILURE_CHARS
        or not _is_sha256(value["sha256"])
    ):
        raise ValueError("failure summary is invalid")
    return dict(value)


def _validate_attempt_identity(
    payload: dict[str, object],
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case_sha256: str,
    label: str,
) -> None:
    case = _decode_case_key(payload.get("case"), label)
    if (
        payload.get("schema_version") != 1
        or type(payload.get("schema_version")) is not int
        or payload.get("epoch_id") != plan.epoch_id
        or payload.get("run_kind") != plan.run_kind
        or case != assignment.key
        or payload.get("lane") != assignment.lane
        or payload.get("route") != assignment.route
        or type(payload.get("attempt")) is not int
        or payload.get("attempt") != attempt
        or payload.get("manifest_sha256") != assignment.manifest_sha256
        or payload.get("manifest_case_sha256") != manifest_case_sha256
    ):
        raise ValueError(f"{label} is stale or invalid")


def _open_attempt_directory(
    paths: CasePaths, attempt: Literal[1, 2], *, create: bool
) -> tuple[AttemptPaths, _RecordDirectoryCapability]:
    attempt_paths = paths_for_attempt(paths, attempt)
    directory = _open_case_record_directory(
        paths=paths,
        components=("attempts", f"{attempt:02d}"),
        create=create,
        label="attempt directory",
    )
    return attempt_paths, directory


def _read_attempt_start_retained(
    *,
    directory: _RecordDirectoryCapability,
    plan: EpochPlan,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case_sha256: str,
) -> tuple[dict[str, object], str]:
    inventory = directory.inventory()
    if inventory not in (("start.json",), ("start.json", "terminal.json")):
        raise ValueError("attempt start inventory is invalid")
    payload, content = _read_canonical_record_retained(
        directory,
        "start.json",
        "attempt start",
        byte_cap=MAX_ATTEMPT_START_BYTES,
    )
    _require_exact_field_names(payload, _ATTEMPT_START_FIELDS, "attempt start")
    _validate_attempt_identity(
        payload,
        plan=plan,
        assignment=assignment,
        attempt=attempt,
        manifest_case_sha256=manifest_case_sha256,
        label="attempt start",
    )
    if directory.inventory() != inventory:
        raise RuntimeError("attempt inventory changed while reading start")
    return payload, hashlib.sha256(content).hexdigest()


def _read_attempt_seal_retained(
    *,
    directory: _RecordDirectoryCapability,
    plan: EpochPlan,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case_sha256: str,
) -> AttemptSeal:
    inventory = directory.inventory()
    if inventory != ("start.json", "terminal.json"):
        raise ValueError("attempt seal inventory is incomplete")
    start, start_sha256 = _read_attempt_start_retained(
        directory=directory,
        plan=plan,
        assignment=assignment,
        attempt=attempt,
        manifest_case_sha256=manifest_case_sha256,
    )
    terminal, terminal_content = _read_canonical_record_retained(
        directory,
        "terminal.json",
        "attempt terminal",
        byte_cap=MAX_ATTEMPT_TERMINAL_BYTES,
    )
    _require_exact_field_names(
        terminal, _ATTEMPT_TERMINAL_FIELDS, "attempt terminal"
    )
    _validate_attempt_identity(
        terminal,
        plan=plan,
        assignment=assignment,
        attempt=attempt,
        manifest_case_sha256=manifest_case_sha256,
        label="attempt terminal",
    )
    status = terminal.get("status")
    classification = terminal.get("classification")
    if status not in ("success", "failed") or classification not in _OUTCOME_CLASSES:
        raise ValueError("attempt terminal status is invalid")
    if type(terminal.get("model_started")) is not bool or type(
        terminal.get("cleanup_passed")
    ) is not bool:
        raise ValueError("attempt terminal booleans are invalid")
    if terminal.get("start_sha256") != start_sha256:
        raise ValueError("attempt terminal start hash differs")
    usage = _validate_usage(terminal.get("usage"), nullable=status == "failed")
    failure = _validate_failure(
        terminal.get("failure"),
        classification=classification,
        nullable=status == "success",
    )
    tombstone_sha256 = terminal.get("tombstone_receipt_sha256")
    if tombstone_sha256 is not None and not _is_sha256(tombstone_sha256):
        raise ValueError("attempt terminal tombstone hash is invalid")
    if status == "success":
        if (
            classification != "success"
            or terminal.get("model_started") is not True
            or terminal.get("cleanup_passed") is not True
            or usage is None
            or failure is not None
            or tombstone_sha256 is None
        ):
            raise ValueError("successful attempt terminal is invalid")
    elif classification == "success" or failure is None:
        raise ValueError("failed attempt terminal is invalid")
    if terminal.get("cleanup_passed") is True and tombstone_sha256 is None:
        raise ValueError("clean attempt terminal requires a tombstone hash")
    if directory.inventory() != inventory:
        raise RuntimeError("attempt inventory changed while reading seal")
    return AttemptSeal(
        start=start,
        terminal=terminal,
        start_sha256=start_sha256,
        terminal_sha256=hashlib.sha256(terminal_content).hexdigest(),
    )


def write_attempt_start(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case: dict[str, object],
) -> Path:
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    payload = {
        "schema_version": 1,
        "epoch_id": plan.epoch_id,
        "run_kind": plan.run_kind,
        "case": asdict(assignment.key),
        "lane": assignment.lane,
        "route": assignment.route,
        "attempt": attempt,
        "manifest_sha256": assignment.manifest_sha256,
        "manifest_case_sha256": manifest_case_sha256,
    }
    attempt_paths, directory = _open_attempt_directory(
        paths, attempt, create=True
    )
    with directory:
        inventory = directory.inventory()
        if "start.json" in inventory:
            existing, _ = _read_attempt_start_retained(
                directory=directory,
                plan=plan,
                assignment=assignment,
                attempt=attempt,
                manifest_case_sha256=manifest_case_sha256,
            )
            if existing != payload:
                raise ValueError("attempt start already differs")
            return attempt_paths.start
        if inventory:
            raise ValueError("attempt start cannot heal a partial inventory")
        _publish_immutable_json_retained(
            directory,
            "start.json",
            payload,
            byte_cap=MAX_ATTEMPT_START_BYTES,
        )
        return attempt_paths.start


def read_attempt_start(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case: dict[str, object],
) -> tuple[dict[str, object], str]:
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    _, directory = _open_attempt_directory(paths, attempt, create=False)
    with directory:
        return _read_attempt_start_retained(
            directory=directory,
            plan=plan,
            assignment=assignment,
            attempt=attempt,
            manifest_case_sha256=manifest_case_sha256,
        )


def write_attempt_terminal(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case: dict[str, object],
    status: CaseSealStatus,
    classification: OutcomeClass,
    model_started: bool,
    cleanup_passed: bool,
    usage: dict[str, object] | None,
    failure: dict[str, object] | None,
) -> Path:
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    if type(status) is not str or status not in ("success", "failed"):
        raise ValueError("attempt terminal status is invalid")
    if type(classification) is not str or classification not in _OUTCOME_CLASSES:
        raise ValueError("attempt terminal classification is invalid")
    if type(model_started) is not bool or type(cleanup_passed) is not bool:
        raise TypeError("attempt terminal booleans must be exact bools")
    validated_usage = _validate_usage(usage, nullable=status == "failed")
    validated_failure = _validate_failure(
        failure, classification=classification, nullable=status == "success"
    )
    if status == "success":
        if (
            classification != "success"
            or model_started is not True
            or cleanup_passed is not True
            or validated_usage is None
            or validated_failure is not None
        ):
            raise ValueError("successful attempt terminal is invalid")
    elif classification == "success" or validated_failure is None:
        raise ValueError("failed attempt terminal is invalid")
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    attempt_paths, directory = _open_attempt_directory(
        paths, attempt, create=False
    )
    with directory:
        start, start_sha256 = _read_attempt_start_retained(
            directory=directory,
            plan=plan,
            assignment=assignment,
            attempt=attempt,
            manifest_case_sha256=manifest_case_sha256,
        )
        verified_receipt = _read_verified_tombstone_receipt(
            plan=plan,
            assignment=assignment,
            paths=paths,
            required=cleanup_passed,
        )
        tombstone_sha256 = (
            verified_receipt.sha256 if verified_receipt is not None else None
        )
        if cleanup_passed and tombstone_sha256 is None:
            raise ValueError("clean attempt terminal requires a tombstone receipt")
        payload = {
            **start,
            "start_sha256": start_sha256,
            "status": status,
            "classification": classification,
            "model_started": model_started,
            "cleanup_passed": cleanup_passed,
            "usage": validated_usage,
            "failure": validated_failure,
            "tombstone_receipt_sha256": tombstone_sha256,
        }
        if len(canonical_config_bytes(payload)) > MAX_ATTEMPT_TERMINAL_BYTES:
            raise ValueError("attempt terminal exceeds its byte cap")
        inventory = directory.inventory()
        if "terminal.json" in inventory:
            seal = _read_attempt_seal_retained(
                directory=directory,
                plan=plan,
                assignment=assignment,
                attempt=attempt,
                manifest_case_sha256=manifest_case_sha256,
            )
            if seal.terminal != payload:
                raise ValueError("attempt terminal already differs")
            return attempt_paths.terminal
        if inventory != ("start.json",):
            raise ValueError("attempt terminal cannot heal a partial inventory")
        _publish_immutable_json_retained(
            directory,
            "terminal.json",
            payload,
            byte_cap=MAX_ATTEMPT_TERMINAL_BYTES,
        )
        return attempt_paths.terminal


def read_attempt_seal(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    manifest_case: dict[str, object],
) -> AttemptSeal:
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    _, directory = _open_attempt_directory(paths, attempt, create=False)
    with directory:
        return _read_attempt_seal_retained(
            directory=directory,
            plan=plan,
            assignment=assignment,
            attempt=attempt,
            manifest_case_sha256=manifest_case_sha256,
        )


def _validate_result_record(
    result: object,
    *,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
) -> dict[str, object]:
    if type(manifest_case) is not dict:
        raise TypeError("manifest_case must be an exact dict")
    _validate_manifest_case(manifest_case, mode=assignment.key.mode)
    if manifest_case.get("id") != assignment.key.case_id:
        raise ValueError("manifest case ID differs from the assignment")
    if (
        assignment.key.mode == "lifecycle"
        and manifest_case.get("mode") != _expected_lifecycle_mode(assignment)
    ):
        raise ValueError("manifest lifecycle mode differs from the frozen case")
    if type(result) is not dict:
        raise TypeError("result must be an exact dict")
    expected = RESULT_SCHEMAS[assignment.key.mode]
    _require_exact_field_names(result, expected, "case result")
    if (
        type(result.get("id")) is not str
        or not result["id"].strip()
        or result.get("id") != assignment.key.case_id
    ):
        raise ValueError("case result ID differs from the assignment")
    command_selection = (
        assignment.key.mode == "lifecycle"
        and _expected_lifecycle_mode(assignment) == "command-selection-only"
    )
    if command_selection:
        if (
            type(result.get("selected_command")) is not str
            or not result["selected_command"].strip()
            or any(
                result.get(field) is not None
                for field in (
                    "record_checkpoints",
                    "run_count",
                    "draft_count",
                    "final_statuses",
                    "failure_disclosed",
                )
            )
        ):
            raise ValueError("command-selection lifecycle result is invalid")
        return dict(result)
    for count_name in ("run_count", "draft_count"):
        if type(result.get(count_name)) is not int or result[count_name] < 0:
            raise ValueError("case result count is invalid")
    statuses = result.get("final_statuses")
    if type(statuses) is not list or any(
        type(value) is not str or not value.strip() for value in statuses
    ):
        raise ValueError("case result final statuses are invalid")
    checkpoints = result.get("record_checkpoints")
    if type(checkpoints) is not list:
        raise ValueError("case result checkpoints are invalid")
    for checkpoint in checkpoints:
        _require_exact_field_names(
            checkpoint, CHECKPOINT_FIELDS, "result checkpoint"
        )
        if type(checkpoint.get("after_turn")) is not int:
            raise ValueError("result checkpoint turn is invalid")
        records = checkpoint.get("records")
        if type(records) is not list:
            raise ValueError("result checkpoint records are invalid")
        for record in records:
            _require_exact_field_names(
                record, NORMALIZED_RECORD_FIELDS, "normalized record"
            )
            if (
                type(record.get("role")) is not str
                or _RECORD_ROLE_PATTERN.fullmatch(record["role"]) is None
                or type(record.get("status")) is not str
                or not record["status"].strip()
                or type(record.get("start_mode")) is not str
                or not record["start_mode"].strip()
                or (
                    record.get("superseded_by_role") is not None
                    and (
                        type(record["superseded_by_role"]) is not str
                        or _RECORD_ROLE_PATTERN.fullmatch(
                            record["superseded_by_role"]
                        )
                        is None
                    )
                )
            ):
                raise ValueError("normalized record is invalid")
    if assignment.key.mode == "forward":
        decisions = result.get("decisions")
        if type(decisions) is not list:
            raise ValueError("forward decisions are invalid")
        for decision in decisions:
            _require_exact_field_names(
                decision, OBSERVED_DECISION_FIELDS, "observed decision"
            )
            if (
                type(decision.get("after_turn")) is not int
                or type(decision.get("triggered")) is not bool
            ):
                raise ValueError("observed decision is invalid")
            taxonomy = (
                decision.get("task_type"), decision.get("workflow_variant")
            )
            if decision["triggered"]:
                if any(type(value) is not str or not value.strip() for value in taxonomy):
                    raise ValueError("triggered decision taxonomy is invalid")
            elif taxonomy != (None, None):
                raise ValueError("untriggered decision taxonomy is invalid")
    else:
        if (
            result.get("selected_command") is not None
            or type(result.get("failure_disclosed")) is not bool
        ):
            raise ValueError("executable lifecycle result is invalid")
    return dict(result)


def _validate_evidence_input(
    evidence: object, *, attempt_seal: AttemptSeal, result_sha256: str | None
) -> dict[str, object]:
    if type(evidence) is not dict:
        raise TypeError("evidence must be an exact dict")
    _require_exact_field_names(evidence, _EVIDENCE_INPUT_FIELDS, "case evidence")
    terminal = attempt_seal.terminal
    if (
        type(evidence.get("status")) is not str
        or evidence["status"] not in ("success", "failed")
        or type(evidence.get("classification")) is not str
        or evidence["classification"] not in _OUTCOME_CLASSES
        or type(evidence.get("model_started")) is not bool
    ):
        raise ValueError("case evidence outcome fields are invalid")
    for field in ("status", "classification", "model_started", "usage", "failure"):
        if evidence.get(field) != terminal.get(field):
            raise ValueError(f"case evidence {field} differs from attempt terminal")
    for field in (
        "store_record_count",
        "store_invalidated_count",
        "audit_event_count",
        "payload_file_count",
        "output_file_count",
    ):
        if (
            type(evidence.get(field)) is not int
            or evidence[field] < 0
            or evidence[field] > MAX_SEAL_COUNTER
        ):
            raise ValueError("case evidence counter is invalid")
    if (
        type(evidence.get("elapsed_milliseconds")) is not int
        or evidence["elapsed_milliseconds"] < 0
        or evidence["elapsed_milliseconds"] > MAX_SEAL_ELAPSED_MILLISECONDS
        or type(evidence.get("process_cleanup_passed")) is not bool
        or type(evidence.get("credential_cleanup_passed")) is not bool
        or (
            evidence["process_cleanup_passed"]
            and evidence["credential_cleanup_passed"]
        )
        != terminal.get("cleanup_passed")
    ):
        raise ValueError("case evidence cleanup or timing is invalid")
    _validate_usage(evidence.get("usage"), nullable=evidence["status"] == "failed")
    _validate_failure(
        evidence.get("failure"),
        classification=evidence.get("classification"),
        nullable=evidence["status"] == "success",
    )
    if evidence["status"] == "success" and result_sha256 is None:
        raise ValueError("successful case evidence requires a result")
    return dict(evidence)


def _validate_stored_case_evidence(
    evidence: dict[str, object],
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    manifest_case_sha256: str,
    attempt_seal: AttemptSeal,
    result_sha256: str | None,
    tombstone_receipt_sha256: str | None,
) -> None:
    case = _decode_case_key(evidence.get("case"), "case evidence")
    terminal = attempt_seal.terminal
    if (
        type(evidence.get("schema_version")) is not int
        or evidence.get("schema_version") != 1
        or type(evidence.get("epoch_id")) is not str
        or evidence.get("epoch_id") != plan.epoch_id
        or type(evidence.get("run_kind")) is not str
        or evidence.get("run_kind") != plan.run_kind
        or case != assignment.key
        or type(evidence.get("lane")) is not str
        or evidence.get("lane") != assignment.lane
        or type(evidence.get("route")) is not str
        or evidence.get("route") != assignment.route
        or type(evidence.get("attempt")) is not int
        or evidence.get("attempt") != terminal.get("attempt")
        or evidence.get("manifest_sha256") != assignment.manifest_sha256
        or evidence.get("manifest_case_sha256") != manifest_case_sha256
        or evidence.get("archive_sha256") != plan.fingerprints.archive_sha256
        or evidence.get("marketplace_sha256")
        != plan.fingerprints.marketplace_sha256
        or evidence.get("evaluator_sha256")
        != plan.fingerprints.evaluator_sha256
        or evidence.get("transport_config_sha256")
        != plan.fingerprints.transport_config_sha256
        or evidence.get("attempt_start_sha256") != attempt_seal.start_sha256
        or evidence.get("attempt_terminal_sha256") != attempt_seal.terminal_sha256
        or evidence.get("result_sha256") != result_sha256
        or evidence.get("tombstone_receipt_sha256")
        != tombstone_receipt_sha256
    ):
        raise ValueError("case evidence identity or dependency differs")
    caller_evidence = {
        field: evidence[field] for field in _EVIDENCE_INPUT_FIELDS
    }
    _validate_evidence_input(
        caller_evidence,
        attempt_seal=attempt_seal,
        result_sha256=result_sha256,
    )


def _encode_shard_terminal(terminal: ShardTerminal) -> dict[str, object]:
    if type(terminal) is not ShardTerminal:
        raise TypeError("terminal must be an exact ShardTerminal")
    if type(terminal.run_kind) is not str or terminal.run_kind not in (
        "diagnostic",
        "discovery",
        "formal",
    ):
        raise ValueError("shard terminal run kind is invalid")
    payload: dict[str, object] = {
        "case": asdict(terminal.key),
        "status": terminal.status,
        "classification": terminal.classification,
        "attempt_terminal_sha256": terminal.attempt_terminal_sha256,
        "case_commit_sha256": terminal.case_commit_sha256,
        "tombstone_receipt_sha256": terminal.tombstone_receipt_sha256,
        "failure": asdict(terminal.failure) if terminal.failure is not None else None,
    }
    _decode_shard_terminal(payload, run_kind=terminal.run_kind)
    return payload


def _decode_shard_terminal(
    payload: object, *, run_kind: RunKind
) -> ShardTerminal:
    if type(payload) is not dict:
        raise TypeError("shard terminal must be an exact dict")
    _require_exact_field_names(payload, SHARD_TERMINAL_FIELDS, "shard terminal")
    if type(run_kind) is not str or run_kind not in (
        "diagnostic",
        "discovery",
        "formal",
    ):
        raise ValueError("shard terminal run kind is invalid")
    key = _decode_case_key(payload.get("case"), "shard terminal")
    status = payload.get("status")
    classification = payload.get("classification")
    if (
        type(status) is not str
        or status not in ("success", "failed")
        or type(classification) is not str
        or classification not in _OUTCOME_CLASSES
        or not _is_sha256(payload.get("attempt_terminal_sha256"))
    ):
        raise ValueError("shard terminal outcome is invalid")
    case_commit_sha256 = payload.get("case_commit_sha256")
    tombstone_receipt_sha256 = payload.get("tombstone_receipt_sha256")
    if case_commit_sha256 is not None and not _is_sha256(case_commit_sha256):
        raise ValueError("shard terminal case commit hash is invalid")
    if tombstone_receipt_sha256 is not None and not _is_sha256(
        tombstone_receipt_sha256
    ):
        raise ValueError("shard terminal tombstone hash is invalid")
    failure_payload = _validate_failure(
        payload.get("failure"),
        classification=classification,
        nullable=status == "success",
    )
    if status == "success":
        if (
            classification != "success"
            or case_commit_sha256 is None
            or tombstone_receipt_sha256 is None
            or failure_payload is not None
        ):
            raise ValueError("successful shard terminal is invalid")
    elif classification == "success" or failure_payload is None:
        raise ValueError("failed shard terminal is invalid")
    failure = (
        FailureSummary(**failure_payload) if failure_payload is not None else None
    )
    return ShardTerminal(
        key=key,
        run_kind=run_kind,
        status=status,
        classification=classification,
        attempt_terminal_sha256=payload["attempt_terminal_sha256"],
        case_commit_sha256=case_commit_sha256,
        tombstone_receipt_sha256=tombstone_receipt_sha256,
        failure=failure,
    )


def _read_canonical_record_retained(
    directory: _RecordDirectoryCapability,
    name: str,
    label: str,
    *,
    byte_cap: int,
) -> tuple[dict[str, object], bytes]:
    directory._validate_live()
    parent_slot = directory._retained[-1].slot
    parent_before = os.fstat(parent_slot.descriptor)
    result = _read_canonical_record_at(
        parent_slot=parent_slot,
        parent_path=directory.path,
        parent_before=parent_before,
        name=name,
        label=label,
        byte_cap=byte_cap,
    )
    directory._validate_live()
    return result


def _publish_immutable_json_retained(
    directory: _RecordDirectoryCapability,
    name: str,
    payload: Mapping[str, Any],
    *,
    byte_cap: int,
) -> bytes:
    _require_lease_process_healthy()
    if type(name) is not str or not name or "/" in name or name in {".", ".."}:
        raise ValueError("record name is invalid")
    content = canonical_config_bytes(payload)
    if len(content) > byte_cap:
        raise ValueError(f"{name} exceeds its byte cap")
    directory._validate_live()
    parent_slot = directory._retained[-1].slot
    temporary_name = f".{name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    temporary_slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    cleanup_errors: list[BaseException] = []
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= _required_os_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0)
        temporary_descriptor = os.open(
            temporary_name, flags, 0o600, dir_fd=parent_slot.descriptor
        )
        temporary_slot = _DescriptorSlot(temporary_descriptor)
        os.fchmod(temporary_slot.descriptor, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(temporary_slot.descriptor, view)
            if written <= 0:
                raise OSError("record write made no progress")
            view = view[written:]
        os.fsync(temporary_slot.descriptor)
    except BaseException as error:
        primary = error

    close_error = None
    if temporary_slot is not None:
        close_error = _retire_descriptor_capability(temporary_slot)
    if close_error is not None:
        _raise_task_failures(
            primary=primary,
            close_errors=[close_error],
            label="record temp write or close failed",
        )
    if primary is None:
        try:
            directory._validate_live()
            os.link(
                temporary_name,
                name,
                src_dir_fd=parent_slot.descriptor,
                dst_dir_fd=parent_slot.descriptor,
                follow_symlinks=False,
            )
            os.fsync(parent_slot.descriptor)
            os.unlink(temporary_name, dir_fd=parent_slot.descriptor)
            os.fsync(parent_slot.descriptor)
            directory._validate_live()
        except FileExistsError:
            primary = ValueError(f"{name} already exists")
        except BaseException as error:
            primary = error
    if primary is not None and not is_indeterminate_descriptor_close(primary):
        try:
            os.unlink(temporary_name, dir_fd=parent_slot.descriptor)
        except FileNotFoundError:
            pass
        except OSError as error:
            cleanup_errors.append(error)
    _raise_ordered_failures(
        "record publication or cleanup failed", primary, cleanup_errors
    )
    durable, durable_content = _read_canonical_record_retained(
        directory, name, name, byte_cap=byte_cap
    )
    if durable != payload or durable_content != content:
        raise ValueError(f"{name} durable readback differs")
    return durable_content


def _ensure_private_record_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    try:
        metadata = path.lstat()
    except OSError:
        raise ValueError("record directory is unavailable") from None
    _validate_owned_entry(
        metadata, label="record directory", kind="directory", mode=0o700
    )


def _directory_inventory(path: Path, label: str) -> tuple[str, ...]:
    try:
        return tuple(sorted(entry.name for entry in os.scandir(path)))
    except OSError:
        raise ValueError(f"{label} is unavailable") from None


def _publish_immutable_json(
    path: Path, payload: Mapping[str, Any], *, byte_cap: int
) -> bytes:
    _require_lease_process_healthy()
    content = canonical_config_bytes(payload)
    if len(content) > byte_cap:
        raise ValueError(f"{path.name} exceeds its byte cap")
    parent_descriptor, _ = _open_private_directory(path.parent, "record directory")
    parent_slot = _DescriptorSlot(parent_descriptor)
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    temporary_slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= _required_os_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0)
        temporary_descriptor = os.open(
            temporary_name, flags, 0o600, dir_fd=parent_slot.descriptor
        )
        temporary_slot = _DescriptorSlot(temporary_descriptor)
        os.fchmod(temporary_slot.descriptor, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(temporary_slot.descriptor, view)
            if written <= 0:
                raise OSError("record write made no progress")
            view = view[written:]
        os.fsync(temporary_slot.descriptor)
        close_error = _retire_descriptor_capability(temporary_slot)
        if close_error is not None:
            raise close_error
        try:
            os.link(
                temporary_name,
                path.name,
                src_dir_fd=parent_slot.descriptor,
                dst_dir_fd=parent_slot.descriptor,
                follow_symlinks=False,
            )
        except FileExistsError:
            raise ValueError(f"{path.name} already exists") from None
        os.fsync(parent_slot.descriptor)
        os.unlink(temporary_name, dir_fd=parent_slot.descriptor)
        os.fsync(parent_slot.descriptor)
    except BaseException as error:
        primary = error
    if temporary_slot is not None and temporary_slot.descriptor_close_state == "owned":
        close_error = _retire_descriptor_capability(temporary_slot)
        if close_error is not None:
            if primary is None:
                primary = close_error
            else:
                primary = BaseExceptionGroup(
                    "record write and temp close failed", [primary, close_error]
                )
    _retire_task_descriptors(
        [parent_slot], primary=primary, label="record publish or close failed"
    )
    durable, durable_content = _read_canonical_record(
        path, path.name, byte_cap=byte_cap
    )
    if durable != payload or durable_content != content:
        raise ValueError(f"{path.name} durable readback differs")
    return durable_content


def _invoke_fault(
    fault_injector: FaultInjector | None, point: FaultPoint
) -> None:
    if fault_injector is None:
        return
    if not callable(fault_injector):
        raise TypeError("fault_injector must be callable or None")
    fault_injector(point)


def seal_case(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    attempt: Literal[1, 2],
    result: dict[str, object] | None,
    evidence: dict[str, object],
    manifest_case: dict[str, object],
    fault_injector: FaultInjector | None = None,
) -> Path:
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("attempt must be exactly 1 or 2")
    if fault_injector is not None and not callable(fault_injector):
        raise TypeError("fault_injector must be callable or None")
    attempt_seal = read_attempt_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=attempt,
        manifest_case=manifest_case,
    )
    verified_receipt = _read_optional_verified_tombstone_receipt(
        plan=plan, assignment=assignment, paths=paths
    )
    verified_receipt_sha256 = (
        verified_receipt.sha256 if verified_receipt is not None else None
    )
    if attempt_seal.terminal.get(
        "tombstone_receipt_sha256"
    ) != verified_receipt_sha256:
        raise ValueError("attempt terminal tombstone hash differs")
    if result is None:
        result_payload = None
        result_content = None
        result_sha256 = None
    else:
        result_payload = _validate_result_record(
            result,
            assignment=assignment,
            manifest_case=manifest_case,
        )
        result_content = canonical_config_bytes(result_payload)
        if len(result_content) > MAX_CASE_RESULT_BYTES:
            raise ValueError("case result exceeds its byte cap")
        result_sha256 = hashlib.sha256(result_content).hexdigest()
    evidence_payload = _validate_evidence_input(
        evidence, attempt_seal=attempt_seal, result_sha256=result_sha256
    )
    status = attempt_seal.terminal["status"]
    if (
        status == "success"
        and (
            verified_receipt is None
            or verified_receipt.receipt.canonical_binding != "expected"
        )
    ):
        raise ValueError("successful case requires an expected tombstone binding")
    if status == "success" and result_payload is None:
        raise ValueError("successful case requires a result")
    cleanup_complete = (
        evidence_payload["process_cleanup_passed"]
        and evidence_payload["credential_cleanup_passed"]
    )
    if cleanup_complete and (
        result_payload is None or verified_receipt is None
    ):
        raise ValueError("clean case requires result and tombstone receipt")
    stored_evidence = {
        "schema_version": 1,
        "epoch_id": plan.epoch_id,
        "run_kind": plan.run_kind,
        "case": asdict(assignment.key),
        "lane": assignment.lane,
        "route": assignment.route,
        "attempt": attempt,
        "manifest_sha256": assignment.manifest_sha256,
        "manifest_case_sha256": manifest_case_sha256,
        "archive_sha256": plan.fingerprints.archive_sha256,
        "marketplace_sha256": plan.fingerprints.marketplace_sha256,
        "evaluator_sha256": plan.fingerprints.evaluator_sha256,
        "transport_config_sha256": plan.fingerprints.transport_config_sha256,
        **evidence_payload,
        "attempt_start_sha256": attempt_seal.start_sha256,
        "attempt_terminal_sha256": attempt_seal.terminal_sha256,
        "result_sha256": result_sha256,
        "tombstone_receipt_sha256": verified_receipt_sha256,
    }
    evidence_content = canonical_config_bytes(stored_evidence)
    if len(evidence_content) > MAX_CASE_EVIDENCE_BYTES:
        raise ValueError("case evidence exceeds its byte cap")
    commit = {
        "schema_version": 1,
        "epoch_id": plan.epoch_id,
        "run_kind": plan.run_kind,
        "case": asdict(assignment.key),
        "lane": assignment.lane,
        "route": assignment.route,
        "attempt": attempt,
        "status": status,
        "manifest_sha256": assignment.manifest_sha256,
        "manifest_case_sha256": manifest_case_sha256,
        "result_file": "case-result.json" if result_payload is not None else None,
        "result_sha256": result_sha256,
        "evidence_file": "case-evidence.json",
        "evidence_sha256": hashlib.sha256(evidence_content).hexdigest(),
        "attempt_start_sha256": attempt_seal.start_sha256,
        "attempt_terminal_sha256": attempt_seal.terminal_sha256,
        "tombstone_receipt_sha256": verified_receipt_sha256,
    }
    if len(canonical_config_bytes(commit)) > MAX_CASE_COMMIT_BYTES:
        raise ValueError("case commit exceeds its byte cap")
    directory = _open_case_record_directory(
        paths=paths,
        components=("sealed",),
        create=True,
        label="case seal directory",
    )
    with directory:
        inventory = directory.inventory()
        if inventory:
            expected_inventory = (
                (
                    "case-commit.json",
                    "case-evidence.json",
                    "case-result.json",
                )
                if result_payload is not None
                else ("case-commit.json", "case-evidence.json")
            )
            if inventory != expected_inventory:
                raise ValueError("case seal inventory is partial or contains extras")
            existing = _read_case_seal_retained(
                directory=directory,
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
                manifest_case_sha256=manifest_case_sha256,
            )
            if (
                existing.result != result_payload
                or existing.evidence != stored_evidence
                or existing.commit != commit
            ):
                raise ValueError("case seal already differs")
            return paths.sealed / "case-commit.json"
        if result_payload is not None:
            _publish_immutable_json_retained(
                directory,
                "case-result.json",
                result_payload,
                byte_cap=MAX_CASE_RESULT_BYTES,
            )
            _invoke_fault(fault_injector, "after-result-replace")
        _publish_immutable_json_retained(
            directory,
            "case-evidence.json",
            stored_evidence,
            byte_cap=MAX_CASE_EVIDENCE_BYTES,
        )
        _invoke_fault(fault_injector, "after-evidence-replace")
        if result_payload is not None:
            durable_result, durable_result_content = (
                _read_canonical_record_retained(
                    directory,
                    "case-result.json",
                    "case result",
                    byte_cap=MAX_CASE_RESULT_BYTES,
                )
            )
            if (
                durable_result != result_payload
                or durable_result_content != result_content
            ):
                raise ValueError("case result changed before commit")
        durable_evidence, durable_evidence_content = (
            _read_canonical_record_retained(
                directory,
                "case-evidence.json",
                "case evidence",
                byte_cap=MAX_CASE_EVIDENCE_BYTES,
            )
        )
        if (
            durable_evidence != stored_evidence
            or durable_evidence_content != evidence_content
        ):
            raise ValueError("case evidence changed before commit")
        durable_attempt = read_attempt_seal(
            plan=plan,
            paths=paths,
            assignment=assignment,
            attempt=attempt,
            manifest_case=manifest_case,
        )
        if (
            durable_attempt.start_sha256 != attempt_seal.start_sha256
            or durable_attempt.terminal_sha256 != attempt_seal.terminal_sha256
        ):
            raise ValueError("attempt changed before case commit")
        durable_receipt = _read_optional_verified_tombstone_receipt(
            plan=plan, assignment=assignment, paths=paths
        )
        durable_receipt_sha256 = (
            durable_receipt.sha256 if durable_receipt is not None else None
        )
        if durable_receipt_sha256 != verified_receipt_sha256:
            raise ValueError("tombstone changed before case commit")
        directory._validate_live()
        _invoke_fault(fault_injector, "before-case-commit")
        _publish_immutable_json_retained(
            directory,
            "case-commit.json",
            commit,
            byte_cap=MAX_CASE_COMMIT_BYTES,
        )
        sealed = _read_case_seal_retained(
            directory=directory,
            plan=plan,
            paths=paths,
            assignment=assignment,
            manifest_case=manifest_case,
            manifest_case_sha256=manifest_case_sha256,
        )
        if sealed.commit != commit:
            raise ValueError("case seal durable readback differs")
        _invoke_fault(fault_injector, "after-case-commit")
        directory._validate_live()
        return paths.sealed / "case-commit.json"


def _read_case_seal_retained(
    *,
    directory: _RecordDirectoryCapability,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    manifest_case_sha256: str,
) -> CaseSeal:
    inventory = directory.inventory()
    names = set(inventory)
    if names not in (
        {"case-evidence.json", "case-commit.json"},
        {"case-result.json", "case-evidence.json", "case-commit.json"},
    ):
        raise ValueError("case seal inventory is partial or contains extras")
    commit, commit_content = _read_canonical_record_retained(
        directory,
        "case-commit.json",
        "case commit",
        byte_cap=MAX_CASE_COMMIT_BYTES,
    )
    if len(commit_content) > MAX_CASE_COMMIT_BYTES:
        raise ValueError("case commit exceeds its byte cap")
    _require_exact_field_names(commit, _CASE_COMMIT_FIELDS, "case commit")
    case = _decode_case_key(commit.get("case"), "case commit")
    if (
        commit.get("schema_version") != 1
        or type(commit.get("schema_version")) is not int
        or commit.get("epoch_id") != plan.epoch_id
        or commit.get("run_kind") != plan.run_kind
        or case != assignment.key
        or commit.get("lane") != assignment.lane
        or commit.get("route") != assignment.route
        or commit.get("manifest_sha256") != assignment.manifest_sha256
        or commit.get("manifest_case_sha256") != manifest_case_sha256
        or commit.get("evidence_file") != "case-evidence.json"
        or commit.get("status") not in ("success", "failed")
    ):
        raise ValueError("case commit is stale or invalid")
    attempt = commit.get("attempt")
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("case commit attempt is invalid")
    attempt_seal = read_attempt_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=attempt,
        manifest_case=manifest_case,
    )
    if (
        commit.get("attempt_start_sha256") != attempt_seal.start_sha256
        or commit.get("attempt_terminal_sha256") != attempt_seal.terminal_sha256
        or commit.get("status") != attempt_seal.terminal.get("status")
    ):
        raise ValueError("case commit attempt binding differs")
    evidence, evidence_content = _read_canonical_record_retained(
        directory,
        "case-evidence.json",
        "case evidence",
        byte_cap=MAX_CASE_EVIDENCE_BYTES,
    )
    if len(evidence_content) > MAX_CASE_EVIDENCE_BYTES:
        raise ValueError("case evidence exceeds its byte cap")
    _require_exact_field_names(evidence, _CASE_EVIDENCE_FIELDS, "case evidence")
    if commit.get("evidence_sha256") != hashlib.sha256(
        evidence_content
    ).hexdigest():
        raise ValueError("case evidence hash differs")
    result_file = commit.get("result_file")
    result_sha256 = commit.get("result_sha256")
    result: dict[str, object] | None
    if result_file is None and result_sha256 is None:
        if "case-result.json" in names:
            raise ValueError("case result inventory differs from commit")
        result = None
    elif result_file == "case-result.json" and _is_sha256(result_sha256):
        if "case-result.json" not in names:
            raise ValueError("case result is missing")
        result, result_content = _read_canonical_record_retained(
            directory,
            "case-result.json",
            "case result",
            byte_cap=MAX_CASE_RESULT_BYTES,
        )
        if (
            len(result_content) > MAX_CASE_RESULT_BYTES
            or hashlib.sha256(result_content).hexdigest() != result_sha256
        ):
            raise ValueError("case result hash or size differs")
        _validate_result_record(
            result,
            assignment=assignment,
            manifest_case=manifest_case,
        )
    else:
        raise ValueError("case result commit fields are invalid")
    verified_receipt = _read_optional_verified_tombstone_receipt(
        plan=plan, assignment=assignment, paths=paths
    )
    verified_receipt_sha256 = (
        verified_receipt.sha256 if verified_receipt is not None else None
    )
    if commit.get("tombstone_receipt_sha256") != verified_receipt_sha256:
        raise ValueError("case evidence dependency hash differs")
    _validate_stored_case_evidence(
        evidence,
        plan=plan,
        assignment=assignment,
        manifest_case_sha256=manifest_case_sha256,
        attempt_seal=attempt_seal,
        result_sha256=result_sha256,
        tombstone_receipt_sha256=verified_receipt_sha256,
    )
    if commit.get("status") == "success" and (
        result is None
        or verified_receipt is None
        or verified_receipt.receipt.canonical_binding != "expected"
    ):
        raise ValueError("successful case seal is incomplete")
    if (
        evidence.get("process_cleanup_passed") is True
        and evidence.get("credential_cleanup_passed") is True
        and (result is None or verified_receipt is None)
    ):
        raise ValueError("clean case seal is incomplete")
    if directory.inventory() != inventory:
        raise RuntimeError("case seal inventory changed while reading")
    return CaseSeal(
        result=result,
        evidence=evidence,
        commit=commit,
        result_sha256=result_sha256,
        evidence_sha256=hashlib.sha256(evidence_content).hexdigest(),
        commit_sha256=hashlib.sha256(commit_content).hexdigest(),
        tombstone_receipt_sha256=verified_receipt_sha256,
    )


def read_case_seal(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
) -> CaseSeal:
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    directory = _open_case_record_directory(
        paths=paths,
        components=("sealed",),
        create=False,
        label="case seal directory",
    )
    with directory:
        return _read_case_seal_retained(
            directory=directory,
            plan=plan,
            paths=paths,
            assignment=assignment,
            manifest_case=manifest_case,
            manifest_case_sha256=manifest_case_sha256,
        )


_SHARD_COMMIT_FIELDS = frozenset(
    {"schema_version", "epoch_id", "run_kind", "lane", "status", "terminals"}
)


def _validate_shard_context(
    *,
    worker_root: Path,
    plan: EpochPlan,
    lane: LaneName,
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
) -> tuple[tuple[CaseAssignment, ...], dict[CaseKey, CasePaths]]:
    _require_lease_process_healthy()
    concrete_path_type = type(Path("."))
    if type(worker_root) is not concrete_path_type or not worker_root.is_absolute():
        raise TypeError("worker_root must be an exact absolute Path")
    if type(lane) is not str or lane not in ("E1", "E2", "E3", "APP"):
        raise ValueError("shard lane is invalid")
    if type(manifests) is not dict or set(manifests) != {"forward", "lifecycle"}:
        raise ValueError("shard manifests must contain forward and lifecycle")
    if any(type(manifests[mode]) is not list for mode in ("forward", "lifecycle")):
        raise TypeError("shard manifests must contain exact lists")
    assignments = tuple(
        assignment for assignment in plan.assignments if assignment.lane == lane
    )
    expected_keys = {assignment.key for assignment in assignments}
    if type(case_paths) is not dict or set(case_paths) != expected_keys:
        raise ValueError("shard case_paths must contain the exact lane inventory")
    run_roots: set[Path] = set()
    frozen_paths: dict[CaseKey, CasePaths] = {}
    for assignment in assignments:
        paths = case_paths.get(assignment.key)
        if type(paths) is not CasePaths:
            raise TypeError("shard case path must be an exact CasePaths")
        run_root = paths.root.parent.parent
        if paths != paths_for_case(run_root, assignment):
            raise ValueError("shard case path differs from the frozen assignment")
        run_roots.add(run_root)
        frozen_paths[assignment.key] = paths
        manifest_rows = manifests[assignment.key.mode]
        index = assignment.key.ordinal - 1
        if index >= len(manifest_rows):
            raise ValueError("shard manifest row is missing")
        manifest_case = manifest_rows[index]
        if type(manifest_case) is not dict:
            raise TypeError("shard manifest row must be an exact dict")
        _validate_manifest_case(manifest_case, mode=assignment.key.mode)
        if manifest_case.get("id") != assignment.key.case_id:
            raise ValueError("shard manifest row ID differs from assignment")
    if len(run_roots) != 1:
        raise ValueError("shard case paths do not share one run root")
    run_root = next(iter(run_roots))
    expected_worker_root = (
        run_root / "app-server"
        if lane == "APP"
        else run_root / "workers" / lane
    )
    if worker_root != expected_worker_root:
        raise ValueError("worker_root differs from the frozen lane root")
    try:
        metadata = worker_root.lstat()
        resolved = worker_root.resolve(strict=True)
    except OSError:
        raise ValueError("worker_root is unavailable") from None
    _validate_owned_entry(
        metadata, label="worker root", kind="directory", mode=0o700
    )
    if resolved != worker_root:
        raise ValueError("worker_root must be canonical")
    return assignments, frozen_paths


def _find_attempt_for_shard_terminal(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
    manifest_case: dict[str, object],
    expected_sha256: str,
) -> AttemptSeal:
    directory = _open_case_record_directory(
        paths=paths,
        components=("attempts",),
        create=False,
        label="attempt root",
    )
    with directory:
        inventory = directory.inventory()
        if inventory not in (("01",), ("01", "02")):
            raise ValueError("shard attempt inventory is invalid")
        with ExitStack() as child_stack:
            children = {
                name: child_stack.enter_context(
                    _open_record_child_directory(
                        directory, name, label="shard attempt directory"
                    )
                )
                for name in inventory
            }
            if directory.inventory() != inventory:
                raise RuntimeError(
                    "shard attempt inventory changed while retaining children"
                )

            matches: list[AttemptSeal] = []
            manifest_case_sha256 = _validate_seal_context(
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
            )
            for attempt in (1, 2):
                child = children.get(f"{attempt:02d}")
                if child is None:
                    continue
                seal = _read_attempt_seal_retained(
                    directory=child,
                    plan=plan,
                    assignment=assignment,
                    attempt=attempt,
                    manifest_case_sha256=manifest_case_sha256,
                )
                if seal.terminal_sha256 == expected_sha256:
                    matches.append(seal)

            if directory.inventory() != inventory:
                raise RuntimeError(
                    "shard attempt inventory changed while scanning"
                )
            for child in children.values():
                child._validate_live()
            if len(matches) != 1:
                raise ValueError(
                    "shard terminal must identify exactly one attempt terminal"
                )
            return matches[0]


def _validate_shard_terminals(
    *,
    plan: EpochPlan,
    assignments: tuple[CaseAssignment, ...],
    terminals: Sequence[ShardTerminal],
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
) -> tuple[CaseSealStatus, tuple[ShardTerminal, ...]]:
    if type(terminals) not in (list, tuple):
        raise TypeError("terminals must be an exact list or tuple")
    frozen = tuple(terminals)
    if not frozen or len(frozen) > len(assignments):
        raise ValueError("shard terminal sequence length is invalid")
    for index, terminal in enumerate(frozen):
        if type(terminal) is not ShardTerminal:
            raise TypeError("shard terminal must be exact")
        assignment = assignments[index]
        if terminal.key != assignment.key or terminal.run_kind != plan.run_kind:
            raise ValueError("shard terminal order or run kind differs")
        _encode_shard_terminal(terminal)
        manifest_case = manifests[assignment.key.mode][assignment.key.ordinal - 1]
        attempt_seal = _find_attempt_for_shard_terminal(
            plan=plan,
            assignment=assignment,
            paths=case_paths[assignment.key],
            manifest_case=manifest_case,
            expected_sha256=terminal.attempt_terminal_sha256,
        )
        attempt_terminal = attempt_seal.terminal
        failure_payload = (
            asdict(terminal.failure) if terminal.failure is not None else None
        )
        if (
            attempt_terminal.get("status") != terminal.status
            or attempt_terminal.get("classification") != terminal.classification
            or attempt_terminal.get("failure") != failure_payload
            or attempt_terminal.get("tombstone_receipt_sha256")
            != terminal.tombstone_receipt_sha256
        ):
            raise ValueError("shard terminal differs from attempt terminal")
        case_seal: CaseSeal | None = None
        if terminal.case_commit_sha256 is not None:
            case_seal = read_case_seal(
                plan=plan,
                paths=case_paths[assignment.key],
                assignment=assignment,
                manifest_case=manifest_case,
            )
            if (
                case_seal.commit_sha256 != terminal.case_commit_sha256
                or case_seal.commit.get("status") != terminal.status
                or case_seal.evidence.get("classification")
                != terminal.classification
                or case_seal.evidence.get("failure") != failure_payload
                or case_seal.tombstone_receipt_sha256
                != terminal.tombstone_receipt_sha256
            ):
                raise ValueError("shard terminal differs from case seal")
        if terminal.tombstone_receipt_sha256 is not None:
            receipt = read_verified_tombstone_receipt(
                plan=plan,
                assignment=assignment,
                paths=case_paths[assignment.key],
            )
            if receipt.sha256 != terminal.tombstone_receipt_sha256:
                raise ValueError("shard terminal tombstone hash differs")
        else:
            receipt = _read_optional_verified_tombstone_receipt(
                plan=plan,
                assignment=assignment,
                paths=case_paths[assignment.key],
            )
            if receipt is not None:
                raise ValueError("shard terminal omitted an existing tombstone")
        if terminal.status == "success":
            if (
                terminal.case_commit_sha256 is None
                or terminal.tombstone_receipt_sha256 is None
                or case_seal is None
            ):
                raise ValueError("successful shard terminal is incomplete")
        elif index != len(frozen) - 1:
            raise ValueError("failed shard terminal must end the prefix")
    if frozen[-1].status == "success":
        if len(frozen) != len(assignments) or any(
            terminal.status != "success" for terminal in frozen
        ):
            raise ValueError("successful shard must contain the full lane")
        status: CaseSealStatus = "success"
    else:
        if any(terminal.status != "success" for terminal in frozen[:-1]):
            raise ValueError("failed shard prefix contains an earlier failure")
        status = "failed"
    return status, frozen


def _open_shard_record_directory(
    *,
    worker_root: Path,
    run_root: Path,
    create: bool,
) -> _RecordDirectoryCapability:
    try:
        worker_components = worker_root.relative_to(run_root).parts
    except ValueError:
        raise ValueError("worker root escapes the run root") from None
    if not worker_components:
        raise ValueError("worker root does not name a worker")
    return _open_anchored_record_directory(
        anchor_path=run_root,
        base_components=worker_components,
        record_components=("sealed",),
        create=create,
        label="shard seal directory",
    )


def _read_shard_seal_retained(
    *,
    directory: _RecordDirectoryCapability,
    plan: EpochPlan,
    lane: LaneName,
    assignments: tuple[CaseAssignment, ...],
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
) -> tuple[ShardSeal, dict[str, object]]:
    inventory = directory.inventory()
    if inventory != ("shard-commit.json",):
        raise ValueError("shard seal inventory is incomplete")
    payload, content = _read_canonical_record_retained(
        directory,
        "shard-commit.json",
        "shard commit",
        byte_cap=MAX_SHARD_COMMIT_BYTES,
    )
    _require_exact_field_names(payload, _SHARD_COMMIT_FIELDS, "shard commit")
    if (
        type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
        or type(payload.get("epoch_id")) is not str
        or payload.get("epoch_id") != plan.epoch_id
        or type(payload.get("run_kind")) is not str
        or payload.get("run_kind") != plan.run_kind
        or type(payload.get("lane")) is not str
        or payload.get("lane") != lane
        or type(payload.get("status")) is not str
        or payload.get("status") not in ("success", "failed")
        or type(payload.get("terminals")) is not list
    ):
        raise ValueError("shard commit identity is invalid")
    terminals = tuple(
        _decode_shard_terminal(terminal, run_kind=plan.run_kind)
        for terminal in payload["terminals"]
    )
    status, validated = _validate_shard_terminals(
        plan=plan,
        assignments=assignments,
        terminals=terminals,
        manifests=manifests,
        case_paths=case_paths,
    )
    if status != payload["status"]:
        raise ValueError("shard commit status differs from terminals")
    if directory.inventory() != inventory:
        raise RuntimeError("shard seal inventory changed while reading")
    return (
        ShardSeal(
            status=status,
            terminals=validated,
            commit_sha256=hashlib.sha256(content).hexdigest(),
        ),
        payload,
    )


def seal_shard(
    *,
    worker_root: Path,
    plan: EpochPlan,
    lane: LaneName,
    terminals: Sequence[ShardTerminal],
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
    fault_injector: FaultInjector | None = None,
) -> Path:
    if fault_injector is not None and not callable(fault_injector):
        raise TypeError("fault_injector must be callable or None")
    assignments, frozen_paths = _validate_shard_context(
        worker_root=worker_root,
        plan=plan,
        lane=lane,
        manifests=manifests,
        case_paths=case_paths,
    )
    status, frozen_terminals = _validate_shard_terminals(
        plan=plan,
        assignments=assignments,
        terminals=terminals,
        manifests=manifests,
        case_paths=frozen_paths,
    )
    payload = {
        "schema_version": 1,
        "epoch_id": plan.epoch_id,
        "run_kind": plan.run_kind,
        "lane": lane,
        "status": status,
        "terminals": [
            _encode_shard_terminal(terminal) for terminal in frozen_terminals
        ],
    }
    if len(canonical_config_bytes(payload)) > MAX_SHARD_COMMIT_BYTES:
        raise ValueError("shard commit exceeds its byte cap")
    run_root = next(iter(frozen_paths.values())).root.parent.parent
    sealed_root = worker_root / "sealed"
    commit_path = sealed_root / "shard-commit.json"
    directory = _open_shard_record_directory(
        worker_root=worker_root, run_root=run_root, create=True
    )
    with directory:
        inventory = directory.inventory()
        if inventory:
            if inventory != ("shard-commit.json",):
                raise ValueError(
                    "shard seal inventory is partial or contains extras"
                )
            existing, existing_payload = _read_shard_seal_retained(
                directory=directory,
                plan=plan,
                lane=lane,
                assignments=assignments,
                manifests=manifests,
                case_paths=frozen_paths,
            )
            if (
                existing_payload != payload
                or existing.terminals != frozen_terminals
            ):
                raise ValueError("shard commit already differs")
            return commit_path
        _invoke_fault(fault_injector, "before-shard-commit")
        _publish_immutable_json_retained(
            directory,
            "shard-commit.json",
            payload,
            byte_cap=MAX_SHARD_COMMIT_BYTES,
        )
        readback, _ = _read_shard_seal_retained(
            directory=directory,
            plan=plan,
            lane=lane,
            assignments=assignments,
            manifests=manifests,
            case_paths=frozen_paths,
        )
        if readback.terminals != frozen_terminals or readback.status != status:
            raise ValueError("shard seal durable readback differs")
        _invoke_fault(fault_injector, "after-shard-commit")
        directory._validate_live()
        return commit_path


def read_shard_seal(
    *,
    worker_root: Path,
    plan: EpochPlan,
    lane: LaneName,
    manifests: dict[EvalMode, list[dict[str, object]]],
    case_paths: dict[CaseKey, CasePaths],
) -> ShardSeal:
    assignments, frozen_paths = _validate_shard_context(
        worker_root=worker_root,
        plan=plan,
        lane=lane,
        manifests=manifests,
        case_paths=case_paths,
    )
    run_root = next(iter(frozen_paths.values())).root.parent.parent
    directory = _open_shard_record_directory(
        worker_root=worker_root, run_root=run_root, create=False
    )
    with directory:
        seal, _ = _read_shard_seal_retained(
            directory=directory,
            plan=plan,
            lane=lane,
            assignments=assignments,
            manifests=manifests,
            case_paths=frozen_paths,
        )
        return seal


FROZEN_LANE_CASES: tuple[tuple[LaneName, tuple[CaseKey, ...]], ...] = (
    (
        "E1",
        (
            CaseKey("forward", 1, "multi-file-feature"),
            CaseKey("forward", 5, "wiki-compile"),
            CaseKey("forward", 11, "chat"),
            CaseKey("forward", 14, "plan-only"),
            CaseKey("forward", 16, "single-file-copy"),
            CaseKey("forward", 19, "worker-with-parent-marker"),
            CaseKey("lifecycle", 1, "planned-success"),
            CaseKey("lifecycle", 6, "central-cli-unavailable"),
        ),
    ),
    (
        "E2",
        (
            CaseKey("forward", 2, "tested-bugfix"),
            CaseKey("forward", 4, "multi-file-docs"),
            CaseKey("forward", 6, "durable-query"),
            CaseKey("forward", 10, "parent-managed-subagent"),
            CaseKey("forward", 15, "single-file-typo"),
            CaseKey("forward", 17, "status-question"),
            CaseKey("lifecycle", 5, "task-failure"),
            CaseKey("lifecycle", 8, "incomplete-eval-override"),
        ),
    ),
    (
        "E3",
        (
            CaseKey("forward", 3, "reviewed-refactor"),
            CaseKey("forward", 7, "inbox-processing"),
            CaseKey("forward", 12, "read-only-search"),
            CaseKey("forward", 13, "answer-only"),
            CaseKey("forward", 18, "review-only"),
            CaseKey("forward", 20, "ambiguous-default-no-trigger"),
            CaseKey("lifecycle", 4, "parent-managed-subagent"),
            CaseKey("lifecycle", 7, "complete-eval-override"),
        ),
    ),
    (
        "APP",
        (
            CaseKey("forward", 8, "late-trigger"),
            CaseKey("forward", 9, "scope-supersession"),
            CaseKey("lifecycle", 2, "late-success"),
            CaseKey("lifecycle", 3, "scope-supersession"),
        ),
    ),
)


_PROGRESS_TYPES = {
    "lane-ready",
    "case-started",
    "case-terminal",
    "shard-terminal",
    "worker-stopped",
}
_ACK_DECISIONS = {"continue", "stop-launches", "abort"}
_PROTOCOL_IDENTITY_FIELDS = frozenset(
    {"schema_version", "epoch_id", "run_kind"}
)
_PROTOCOL_TEMP_PATTERN = re.compile(
    r"^\.[0-9]{6}\.json\.tmp-[0-9]+-[0-9a-f]{32}$"
)


def _protocol_worker_context(
    worker_root: Path, lane: LaneName
) -> tuple[Path, Path]:
    concrete_path_type = type(Path("."))
    if type(worker_root) is not concrete_path_type or not worker_root.is_absolute():
        raise TypeError("worker_root must be an exact absolute Path")
    if type(lane) is not str or lane not in ("E1", "E2", "E3", "APP"):
        raise ValueError("progress lane is invalid")
    if lane == "APP":
        if worker_root.name != "app-server":
            raise ValueError("worker_root differs from its progress lane")
        run_root = worker_root.parent
    else:
        if worker_root.name != lane or worker_root.parent.name != "workers":
            raise ValueError("worker_root differs from its progress lane")
        run_root = worker_root.parent.parent
    try:
        worker_metadata = worker_root.lstat()
        run_metadata = run_root.lstat()
        resolved_worker = worker_root.resolve(strict=True)
        resolved_run = run_root.resolve(strict=True)
    except OSError:
        raise ValueError("progress worker root is unavailable") from None
    _validate_owned_entry(
        worker_metadata,
        label="progress worker root",
        kind="directory",
        mode=0o700,
    )
    _validate_owned_entry(
        run_metadata,
        label="progress run root",
        kind="directory",
        mode=0o700,
    )
    if resolved_worker != worker_root or resolved_run != run_root:
        raise ValueError("progress worker root must be canonical")
    return worker_root, run_root


def _validate_progress_case(case: object, lane: LaneName) -> CaseKey:
    if type(case) is not CaseKey:
        raise TypeError("progress case must be an exact CaseKey")
    if (
        case.mode not in ("forward", "lifecycle")
        or type(case.ordinal) is not int
        or case.ordinal < 1
        or case.ordinal > 99
        or type(case.case_id) is not str
        or len(case.case_id) > MAX_PROGRESS_STRING_CHARS
        or _CASE_ID_PATTERN.fullmatch(case.case_id) is None
    ):
        raise ValueError("progress case is unsafe")
    lane_cases = dict(FROZEN_LANE_CASES).get(lane)
    if lane_cases is None or case not in lane_cases:
        raise ValueError("progress case is not assigned to its lane")
    return case


def _validated_token_usage(value: object, *, nullable: bool) -> TokenUsage | None:
    if value is None:
        if nullable:
            return None
        raise ValueError("progress usage is required")
    if type(value) is not TokenUsage:
        raise TypeError("progress usage must be an exact TokenUsage")
    validated = _validate_usage(asdict(value), nullable=False)
    if validated is None:
        raise AssertionError("non-null token usage validation returned null")
    return TokenUsage(**validated)


def _validate_progress_message(message: ProgressMessage) -> None:
    if type(message) is not ProgressMessage:
        raise TypeError("message must be an exact ProgressMessage")
    if (
        type(message.schema_version) is not int
        or message.schema_version != 1
        or not _is_sha256(message.epoch_id)
        or type(message.run_kind) is not str
        or message.run_kind not in ("diagnostic", "discovery", "formal")
        or type(message.lane) is not str
        or message.lane not in ("E1", "E2", "E3", "APP")
        or type(message.seq) is not int
        or message.seq < 1
        or message.seq > 999_999
        or type(message.type) is not str
        or message.type not in _PROGRESS_TYPES
    ):
        raise ValueError("progress identity is invalid")
    if message.status is not None and (
        type(message.status) is not str
        or message.status not in ("success", "failed")
    ):
        raise ValueError("progress status is invalid")
    if message.classification is not None and (
        type(message.classification) is not str
        or message.classification not in _OUTCOME_CLASSES
    ):
        raise ValueError("progress classification is invalid")
    if message.model_started is not None and type(message.model_started) is not bool:
        raise TypeError("progress model_started must be an exact bool or null")
    usage = _validated_token_usage(message.usage, nullable=True)
    digests = (
        message.attempt_terminal_sha256,
        message.case_commit_sha256,
        message.shard_commit_sha256,
        message.tombstone_receipt_sha256,
    )
    if any(digest is not None and not _is_sha256(digest) for digest in digests):
        raise ValueError("progress contains an invalid digest")

    common_nulls = (
        message.status,
        message.classification,
        message.model_started,
        usage,
        *digests,
    )
    if message.type in ("lane-ready", "worker-stopped"):
        if message.case is not None or message.attempt is not None or any(
            value is not None for value in common_nulls
        ):
            raise ValueError(f"{message.type} progress has unrelated fields")
        return
    if message.type == "case-started":
        _validate_progress_case(message.case, message.lane)
        if type(message.attempt) is not int or message.attempt not in (1, 2):
            raise ValueError("case-started attempt is invalid")
        if any(value is not None for value in common_nulls):
            raise ValueError("case-started progress has unrelated fields")
        return
    if message.type == "shard-terminal":
        if (
            message.case is not None
            or message.attempt is not None
            or message.status not in ("success", "failed")
            or message.classification is not None
            or message.model_started is not None
            or usage is not None
            or message.attempt_terminal_sha256 is not None
            or message.case_commit_sha256 is not None
            or message.shard_commit_sha256 is None
            or message.tombstone_receipt_sha256 is not None
        ):
            raise ValueError("shard-terminal progress has unrelated fields")
        return

    _validate_progress_case(message.case, message.lane)
    if type(message.attempt) is not int or message.attempt not in (1, 2):
        raise ValueError("case-terminal attempt is invalid")
    if (
        message.status not in ("success", "failed")
        or message.classification is None
        or type(message.model_started) is not bool
        or message.attempt_terminal_sha256 is None
        or message.shard_commit_sha256 is not None
    ):
        raise ValueError("case-terminal outcome is incomplete")
    if message.status == "success":
        if (
            message.classification != "success"
            or message.model_started is not True
            or usage is None
            or message.case_commit_sha256 is None
            or message.tombstone_receipt_sha256 is None
        ):
            raise ValueError("successful case-terminal progress is incomplete")
    elif message.classification == "success":
        raise ValueError("failed case-terminal classification is invalid")


def _encode_progress_message(message: ProgressMessage) -> dict[str, object]:
    _validate_progress_message(message)
    payload = asdict(message)
    _require_exact_field_names(payload, frozenset(PROGRESS_FIELDS), "progress")
    return payload


def _decode_progress_message(payload: object) -> ProgressMessage:
    decoded = _require_exact_field_names(
        payload, frozenset(PROGRESS_FIELDS), "progress"
    )
    case_payload = decoded.get("case")
    case = (
        None
        if case_payload is None
        else _decode_case_key(case_payload, "progress")
    )
    usage_payload = decoded.get("usage")
    usage: TokenUsage | None
    if usage_payload is None:
        usage = None
    else:
        validated = _validate_usage(usage_payload, nullable=False)
        if validated is None:
            raise AssertionError("decoded progress usage unexpectedly null")
        usage = TokenUsage(**validated)
    message = ProgressMessage(
        schema_version=decoded.get("schema_version"),
        epoch_id=decoded.get("epoch_id"),
        run_kind=decoded.get("run_kind"),
        lane=decoded.get("lane"),
        seq=decoded.get("seq"),
        type=decoded.get("type"),
        case=case,
        attempt=decoded.get("attempt"),
        status=decoded.get("status"),
        classification=decoded.get("classification"),
        model_started=decoded.get("model_started"),
        usage=usage,
        attempt_terminal_sha256=decoded.get("attempt_terminal_sha256"),
        case_commit_sha256=decoded.get("case_commit_sha256"),
        shard_commit_sha256=decoded.get("shard_commit_sha256"),
        tombstone_receipt_sha256=decoded.get("tombstone_receipt_sha256"),
    )
    _validate_progress_message(message)
    return message


def _register_progress_epoch_context(
    *, plan: EpochPlan, manifests: dict[EvalMode, list[dict[str, object]]]
) -> None:
    content = canonical_config_bytes(manifests)
    existing = _PROGRESS_EPOCH_CONTEXTS.get(plan.epoch_id)
    context = (plan, content)
    if existing is not None and existing != context:
        raise ValueError("epoch progress context differs for the same identity")
    _PROGRESS_EPOCH_CONTEXTS[plan.epoch_id] = context


def _resolve_progress_epoch_context(
    message: ProgressMessage,
) -> tuple[EpochPlan, dict[EvalMode, list[dict[str, object]]]]:
    context = _PROGRESS_EPOCH_CONTEXTS.get(message.epoch_id)
    if context is None:
        raise ValueError("progress epoch context is unavailable")
    plan, content = context
    if (
        plan.epoch_id != message.epoch_id
        or plan.run_kind != message.run_kind
    ):
        raise ValueError("progress differs from its epoch plan")
    decoded = json.loads(content)
    if (
        type(decoded) is not dict
        or set(decoded) != {"forward", "lifecycle"}
        or any(
            type(decoded.get(mode)) is not list
            for mode in ("forward", "lifecycle")
        )
    ):
        raise AssertionError("registered progress manifests are invalid")
    return plan, decoded


def _resolve_progress_case_context(
    *,
    run_root: Path,
    message: ProgressMessage,
    plan: EpochPlan,
    manifests: dict[EvalMode, list[dict[str, object]]],
) -> tuple[CaseAssignment, dict[str, object], CasePaths]:
    if message.case is None:
        raise AssertionError("case progress lacks its frozen case")
    matches = tuple(
        assignment
        for assignment in plan.assignments
        if assignment.key == message.case
    )
    if len(matches) != 1:
        raise ValueError("progress case is absent from the epoch plan")
    assignment = matches[0]
    if assignment.lane != message.lane:
        raise ValueError("progress case lane differs from the epoch plan")
    rows = manifests[assignment.key.mode]
    index = assignment.key.ordinal - 1
    if index < 0 or index >= len(rows) or type(rows[index]) is not dict:
        raise ValueError("progress manifest case is unavailable")
    manifest_case = rows[index]
    paths = paths_for_case(run_root, assignment)
    _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    return assignment, manifest_case, paths


def _read_progress_attempt(
    *,
    message: ProgressMessage,
    plan: EpochPlan,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    paths: CasePaths,
) -> tuple[AttemptSeal, CasePaths]:
    if message.case is None or message.attempt is None:
        raise AssertionError("case-terminal progress lacks a case binding")
    attempt_seal = _find_attempt_for_shard_terminal(
        plan=plan,
        assignment=assignment,
        paths=paths,
        manifest_case=manifest_case,
        expected_sha256=message.attempt_terminal_sha256,
    )
    terminal = attempt_seal.terminal
    if (
        terminal.get("attempt") != message.attempt
        or terminal.get("status") != message.status
        or terminal.get("classification") != message.classification
        or terminal.get("model_started") != message.model_started
        or terminal.get("usage")
        != (asdict(message.usage) if message.usage is not None else None)
        or terminal.get("tombstone_receipt_sha256")
        != message.tombstone_receipt_sha256
    ):
        raise ValueError("case-terminal progress differs from its durable attempt")
    return attempt_seal, paths


def _read_optional_protocol_record(
    path: Path, label: str, *, byte_cap: int
) -> tuple[dict[str, object], bytes] | None:
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        raise ValueError(f"{label} is unavailable") from None
    return _read_canonical_record(path, label, byte_cap=byte_cap)


def _validate_progress_case_commit(
    *,
    message: ProgressMessage,
    plan: EpochPlan,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    paths: CasePaths,
    attempt_seal: AttemptSeal,
) -> None:
    record = _read_optional_protocol_record(
        paths.sealed / "case-commit.json",
        "progress case commit",
        byte_cap=MAX_CASE_COMMIT_BYTES,
    )
    if message.case_commit_sha256 is None:
        if record is not None:
            raise ValueError("case-terminal progress omitted a durable case commit")
        try:
            paths.sealed.lstat()
        except FileNotFoundError:
            return
        except OSError:
            raise ValueError("progress case seal is unavailable") from None
        directory = _open_case_record_directory(
            paths=paths,
            components=("sealed",),
            create=False,
            label="progress case seal",
        )
        with directory:
            inventory = directory.inventory()
            if inventory not in (
                (),
                ("case-result.json",),
                ("case-evidence.json",),
                ("case-evidence.json", "case-result.json"),
            ):
                raise ValueError(
                    "uncommitted progress case seal is partial or contains extras"
                )
            result_sha256 = None
            if "case-result.json" in inventory:
                result, result_content = _read_canonical_record_retained(
                    directory,
                    "case-result.json",
                    "progress case result",
                    byte_cap=MAX_CASE_RESULT_BYTES,
                )
                _validate_result_record(
                    result,
                    assignment=assignment,
                    manifest_case=manifest_case,
                )
                result_sha256 = hashlib.sha256(result_content).hexdigest()
            if "case-evidence.json" in inventory:
                evidence, _ = _read_canonical_record_retained(
                    directory,
                    "case-evidence.json",
                    "progress case evidence",
                    byte_cap=MAX_CASE_EVIDENCE_BYTES,
                )
                _require_exact_field_names(
                    evidence,
                    _CASE_EVIDENCE_FIELDS,
                    "progress case evidence",
                )
                manifest_case_sha256 = _validate_seal_context(
                    plan=plan,
                    paths=paths,
                    assignment=assignment,
                    manifest_case=manifest_case,
                )
                verified_receipt = _read_optional_verified_tombstone_receipt(
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )
                verified_receipt_sha256 = (
                    verified_receipt.sha256
                    if verified_receipt is not None
                    else None
                )
                _validate_stored_case_evidence(
                    evidence,
                    plan=plan,
                    assignment=assignment,
                    manifest_case_sha256=manifest_case_sha256,
                    attempt_seal=attempt_seal,
                    result_sha256=result_sha256,
                    tombstone_receipt_sha256=verified_receipt_sha256,
                )
                if (
                    evidence.get("process_cleanup_passed") is True
                    and evidence.get("credential_cleanup_passed") is True
                    and (
                        result_sha256 is None
                        or verified_receipt is None
                    )
                ):
                    raise ValueError("clean pre-commit case seal is incomplete")
            if directory.inventory() != inventory:
                raise RuntimeError(
                    "uncommitted progress case seal changed while reading"
                )
        return
    if record is None:
        raise ValueError("case-terminal progress names a missing case commit")
    case_seal = read_case_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    commit = case_seal.commit
    if (
        case_seal.commit_sha256 != message.case_commit_sha256
        or commit.get("attempt") != message.attempt
        or commit.get("attempt_terminal_sha256")
        != message.attempt_terminal_sha256
        or commit.get("status") != message.status
        or case_seal.tombstone_receipt_sha256
        != message.tombstone_receipt_sha256
    ):
        raise ValueError("case-terminal progress case commit binding differs")


def _validate_progress_tombstone(
    *,
    message: ProgressMessage,
    terminal: dict[str, object],
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
) -> None:
    verified = _read_optional_verified_tombstone_receipt(
        plan=plan,
        assignment=assignment,
        paths=paths,
    )
    if message.tombstone_receipt_sha256 is None:
        if terminal.get("cleanup_passed") is True:
            raise ValueError("clean case-terminal progress omitted its receipt")
        if verified is not None:
            raise ValueError("case-terminal progress omitted a durable receipt")
        return
    if verified is None:
        raise ValueError("case-terminal progress names a missing receipt")
    if (
        verified.sha256 != message.tombstone_receipt_sha256
        or verified.receipt.case != message.case
    ):
        raise ValueError("case-terminal progress tombstone binding differs")
    if (
        message.status == "success"
        and verified.receipt.canonical_binding != "expected"
    ):
        raise ValueError("successful progress requires an expected tombstone")


def _validate_progress_shard(
    *,
    worker_root: Path,
    message: ProgressMessage,
    plan: EpochPlan,
    manifests: dict[EvalMode, list[dict[str, object]]],
    run_root: Path,
) -> None:
    if message.shard_commit_sha256 is None:
        raise AssertionError("shard-terminal progress lacks its digest")
    assignments = tuple(
        assignment
        for assignment in plan.assignments
        if assignment.lane == message.lane
    )
    case_paths = {
        assignment.key: paths_for_case(run_root, assignment)
        for assignment in assignments
    }
    shard = read_shard_seal(
        worker_root=worker_root,
        plan=plan,
        lane=message.lane,
        manifests=manifests,
        case_paths=case_paths,
    )
    if (
        shard.commit_sha256 != message.shard_commit_sha256
        or shard.status != message.status
    ):
        raise ValueError("shard-terminal progress differs from its durable commit")


def _validate_progress_durable(worker_root: Path, message: ProgressMessage) -> None:
    _, run_root = _protocol_worker_context(worker_root, message.lane)
    plan, manifests = _resolve_progress_epoch_context(message)
    if message.type == "case-terminal":
        assignment, manifest_case, paths = _resolve_progress_case_context(
            run_root=run_root,
            message=message,
            plan=plan,
            manifests=manifests,
        )
        attempt_seal, paths = _read_progress_attempt(
            message=message,
            plan=plan,
            assignment=assignment,
            manifest_case=manifest_case,
            paths=paths,
        )
        _validate_progress_case_commit(
            message=message,
            plan=plan,
            assignment=assignment,
            manifest_case=manifest_case,
            paths=paths,
            attempt_seal=attempt_seal,
        )
        _validate_progress_tombstone(
            message=message,
            terminal=attempt_seal.terminal,
            plan=plan,
            assignment=assignment,
            paths=paths,
        )
    elif message.type == "case-started":
        _resolve_progress_case_context(
            run_root=run_root,
            message=message,
            plan=plan,
            manifests=manifests,
        )
    elif message.type == "shard-terminal":
        _validate_progress_shard(
            worker_root=worker_root,
            message=message,
            plan=plan,
            manifests=manifests,
            run_root=run_root,
        )


def _write_protocol_record(
    path: Path, payload: Mapping[str, Any], *, byte_cap: int
) -> bytes:
    _require_lease_process_healthy()
    content = canonical_config_bytes(payload)
    if len(content) > byte_cap:
        raise ValueError(f"{path.name} exceeds its byte cap")
    _ensure_private_record_directory(path.parent)
    parent_descriptor, parent_metadata = _open_private_directory(
        path.parent,
        "protocol record directory",
    )
    parent_slot = _DescriptorSlot(parent_descriptor)
    try:
        live_parent = path.parent.lstat()
        _validate_owned_entry(
            live_parent,
            label="protocol record directory",
            kind="directory",
            mode=0o700,
        )
        if (
            live_parent.st_dev,
            live_parent.st_ino,
        ) != (
            parent_metadata.st_dev,
            parent_metadata.st_ino,
        ):
            raise RuntimeError("protocol record directory changed before publish")
    except BaseException as error:
        _retire_task_descriptors(
            [parent_slot],
            primary=error,
            label="protocol directory validation or close failed",
        )
    temporary_name = f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(16)}"
    temporary_slot: _DescriptorSlot | None = None
    temporary_created = False
    collision = False
    primary: BaseException | None = None
    close_errors: list[BaseException] = []
    cleanup_errors: list[BaseException] = []
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        flags |= _required_os_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0)
        temporary_descriptor = os.open(
            temporary_name,
            flags,
            0o600,
            dir_fd=parent_slot.descriptor,
        )
        temporary_created = True
        temporary_slot = _DescriptorSlot(temporary_descriptor)
        os.fchmod(temporary_slot.descriptor, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(temporary_slot.descriptor, view)
            if written <= 0:
                raise OSError("protocol record write made no progress")
            view = view[written:]
        os.fsync(temporary_slot.descriptor)
    except BaseException as error:
        primary = error

    if temporary_slot is not None:
        close_error = _retire_descriptor_capability(temporary_slot)
        if close_error is not None:
            close_errors.append(close_error)
    indeterminate = (
        primary is not None and is_indeterminate_descriptor_close(primary)
    ) or any(
        is_indeterminate_descriptor_close(error) for error in close_errors
    )

    if primary is None and not close_errors and not indeterminate:
        try:
            os.link(
                temporary_name,
                path.name,
                src_dir_fd=parent_slot.descriptor,
                dst_dir_fd=parent_slot.descriptor,
                follow_symlinks=False,
            )
            os.fsync(parent_slot.descriptor)
        except FileExistsError:
            collision = True
        except BaseException as error:
            primary = error
            indeterminate = is_indeterminate_descriptor_close(error)

    if temporary_created and not indeterminate:
        try:
            os.unlink(temporary_name, dir_fd=parent_slot.descriptor)
            os.fsync(parent_slot.descriptor)
        except FileNotFoundError as error:
            cleanup_errors.append(error)
        except OSError as error:
            cleanup_errors.append(error)

    parent_close_error = _retire_descriptor_capability(parent_slot)
    if parent_close_error is not None:
        close_errors.append(parent_close_error)
    _raise_ordered_failures(
        "protocol publication or cleanup failed",
        primary,
        [*close_errors, *cleanup_errors],
    )
    durable, durable_content = _read_canonical_record(
        path, path.name, byte_cap=byte_cap
    )
    if durable != payload or durable_content != content:
        if collision:
            raise ValueError(f"{path.name} already differs")
        raise ValueError(f"{path.name} durable readback differs")
    return durable_content


def _protocol_identity_payload(
    message: ProgressMessage,
) -> dict[str, object]:
    payload = {
        "schema_version": 1,
        "epoch_id": message.epoch_id,
        "run_kind": message.run_kind,
    }
    _require_exact_field_names(
        payload,
        _PROTOCOL_IDENTITY_FIELDS,
        "worker protocol identity",
    )
    return payload


def _read_worker_protocol_identity(
    worker_root: Path,
    message: ProgressMessage,
) -> None:
    payload, _ = _read_canonical_record(
        worker_root / "protocol-identity.json",
        "worker protocol identity",
        byte_cap=MAX_PROGRESS_BYTES,
    )
    _require_exact_field_names(
        payload,
        _PROTOCOL_IDENTITY_FIELDS,
        "worker protocol identity",
    )
    if (
        type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
        or payload.get("epoch_id") != message.epoch_id
        or payload.get("run_kind") != message.run_kind
    ):
        raise ValueError("worker protocol identity differs")


def _seal_worker_protocol_identity(
    worker_root: Path,
    message: ProgressMessage,
) -> None:
    _write_protocol_record(
        worker_root / "protocol-identity.json",
        _protocol_identity_payload(message),
        byte_cap=MAX_PROGRESS_BYTES,
    )
    _read_worker_protocol_identity(worker_root, message)


def write_progress(worker_root: Path, message: ProgressMessage) -> Path:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    _validate_progress_durable(bound_worker, message)
    _protocol_sequence_inventory(
        bound_worker / "progress",
        label="progress",
        deadline=None,
        publishing_seq=message.seq,
    )
    _seal_worker_protocol_identity(bound_worker, message)
    path = bound_worker / "progress" / f"{message.seq:06d}.json"
    _write_protocol_record(
        path, _encode_progress_message(message), byte_cap=MAX_PROGRESS_BYTES
    )
    return path


def read_progress(
    path: Path, expected_lane: LaneName, expected_seq: int
) -> ProgressMessage:
    concrete_path_type = type(Path("."))
    if type(path) is not concrete_path_type or not path.is_absolute():
        raise TypeError("progress path must be an exact absolute Path")
    if (
        type(expected_lane) is not str
        or expected_lane not in ("E1", "E2", "E3", "APP")
        or type(expected_seq) is not int
        or expected_seq < 1
        or expected_seq > 999_999
    ):
        raise ValueError("expected progress identity is invalid")
    expected_name = f"{expected_seq:06d}.json"
    if path.name != expected_name or path.parent.name != "progress":
        raise ValueError("progress path differs from its expected sequence")
    worker_root = path.parent.parent
    _protocol_worker_context(worker_root, expected_lane)
    payload, _ = _read_canonical_record(
        path, "progress", byte_cap=MAX_PROGRESS_BYTES
    )
    message = _decode_progress_message(payload)
    if message.lane != expected_lane or message.seq != expected_seq:
        raise ValueError("progress record identity differs")
    _read_worker_protocol_identity(worker_root, message)
    _validate_progress_durable(worker_root, message)
    return message


def _validate_wait_timeout(timeout: float) -> float:
    if (
        type(timeout) not in (int, float)
        or not math.isfinite(timeout)
        or timeout < 0
    ):
        raise ValueError("timeout must be a finite non-negative number")
    return float(timeout)


def _protocol_sequence_inventory(
    directory: Path,
    *,
    label: str,
    deadline: float | None,
    publishing_seq: int | None = None,
) -> tuple[int, ...]:
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError(f"timed out while scanning {label} records")
    try:
        metadata = directory.lstat()
    except FileNotFoundError:
        return ()
    except OSError:
        raise ValueError(f"{label} directory is unavailable") from None
    _validate_owned_entry(
        metadata, label=f"{label} directory", kind="directory", mode=0o700
    )
    descriptor, opened = _open_private_directory(
        directory,
        f"{label} directory",
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    sequences: list[int] = []
    crash_temp_count = 0
    try:
        if (
            metadata.st_dev,
            metadata.st_ino,
        ) != (
            opened.st_dev,
            opened.st_ino,
        ):
            raise RuntimeError(f"{label} directory changed before scanning")
        _validate_owned_entry(
            opened,
            label=f"{label} directory",
            kind="directory",
            mode=0o700,
        )
        with os.scandir(slot.descriptor) as entries:
            for entry in entries:
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timed out while scanning {label} records"
                    )
                name = entry.name
                if _PROTOCOL_TEMP_PATTERN.fullmatch(name):
                    crash_temp_count += 1
                    crash_temp_cap = MAX_PROTOCOL_CRASH_TEMPS - (
                        1 if publishing_seq is not None else 0
                    )
                    if crash_temp_count > crash_temp_cap:
                        raise ValueError(
                            f"{label} crash temporary inventory exceeds its cap"
                        )
                    try:
                        temporary_metadata = entry.stat(follow_symlinks=False)
                    except OSError:
                        raise ValueError(
                            f"{label} crash temporary record is unavailable"
                        ) from None
                    _validate_owned_entry(
                        temporary_metadata,
                        label=f"{label} crash temporary record",
                        kind="file",
                        mode=0o600,
                    )
                    continue
                if name.startswith(".") or ".tmp-" in name:
                    raise ValueError(
                        f"{label} directory contains an unknown crash temporary"
                    )
                if re.fullmatch(r"[0-9]{6}\.json", name) is None:
                    raise ValueError(
                        f"{label} directory contains an unsafe record"
                    )
                if len(sequences) >= MAX_PROTOCOL_RECORDS:
                    raise ValueError(
                        f"{label} record inventory exceeds its cap"
                    )
                sequence = int(name[:6])
                if sequence < 1:
                    raise ValueError(f"{label} sequence is invalid")
                sequences.append(sequence)
        after = os.fstat(slot.descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise RuntimeError(f"{label} inventory changed while scanning")
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(f"timed out while scanning {label} records")
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [slot],
        primary=primary,
        label=f"{label} inventory scan or close failed",
    )
    sequences.sort()
    if (
        publishing_seq is not None
        and publishing_seq not in sequences
        and len(sequences) >= MAX_PROTOCOL_RECORDS
    ):
        raise ValueError(f"{label} record inventory exceeds its cap")
    return tuple(sequences)


def _require_protocol_sequence_prefix(
    sequences: tuple[int, ...], *, expected_seq: int, label: str
) -> None:
    prefix_count = 0
    for sequence in sequences:
        if sequence > expected_seq:
            raise ValueError(f"{label} records are gapped or reordered")
        if sequence < expected_seq:
            prefix_count += 1
            if sequence != prefix_count:
                raise ValueError(f"{label} records are gapped or reordered")
    if prefix_count != expected_seq - 1:
        raise ValueError(f"{label} records are gapped or reordered")


def wait_for_progress(
    *,
    worker_root: Path,
    expected_lane: LaneName,
    expected_seq: int,
    timeout: float,
    expected_sha256: str | None = None,
) -> ProgressMessage:
    bound_worker, _ = _protocol_worker_context(worker_root, expected_lane)
    if (
        type(expected_seq) is not int
        or expected_seq < 1
        or expected_seq > 999_999
    ):
        raise ValueError("expected progress sequence is invalid")
    if expected_sha256 is not None and not _is_sha256(expected_sha256):
        raise ValueError("expected progress digest is invalid")
    timeout_value = _validate_wait_timeout(timeout)
    deadline = time.monotonic() + timeout_value
    progress_root = bound_worker / "progress"
    path = progress_root / f"{expected_seq:06d}.json"
    while True:
        sequences = _protocol_sequence_inventory(
            progress_root,
            label="progress",
            deadline=deadline,
        )
        _require_protocol_sequence_prefix(
            sequences,
            expected_seq=expected_seq,
            label="progress",
        )
        for sequence in range(1, expected_seq):
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for durable progress")
            read_progress(
                progress_root / f"{sequence:06d}.json",
                expected_lane,
                sequence,
            )
        if (
            _protocol_sequence_inventory(
                progress_root,
                label="progress",
                deadline=deadline,
            )
            != sequences
        ):
            raise RuntimeError("progress inventory changed while polling")
        if expected_seq in sequences:
            message = read_progress(path, expected_lane, expected_seq)
            if expected_sha256 is not None:
                _, content = _read_canonical_record(
                    path, "progress", byte_cap=MAX_PROGRESS_BYTES
                )
                if hashlib.sha256(content).hexdigest() != expected_sha256:
                    raise ValueError("progress wake-up digest differs")
            if (
                _protocol_sequence_inventory(
                    progress_root,
                    label="progress",
                    deadline=deadline,
                )
                != sequences
            ):
                raise RuntimeError("progress inventory changed while reading")
            return message
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for durable progress")
        time.sleep(min(0.01, remaining))


def _encode_ack(ack: Ack) -> dict[str, object]:
    if type(ack) is not Ack:
        raise TypeError("ack must be an exact Ack")
    if (
        type(ack.schema_version) is not int
        or ack.schema_version != 1
        or not _is_sha256(ack.epoch_id)
        or type(ack.run_kind) is not str
        or ack.run_kind not in ("diagnostic", "discovery", "formal")
        or type(ack.lane) is not str
        or ack.lane not in ("E1", "E2", "E3", "APP")
        or type(ack.seq) is not int
        or ack.seq < 1
        or ack.seq > 999_999
        or not _is_sha256(ack.message_sha256)
        or type(ack.decision) is not str
        or ack.decision not in _ACK_DECISIONS
    ):
        raise ValueError("ack identity or decision is invalid")
    payload = asdict(ack)
    _require_exact_field_names(payload, frozenset(ACK_FIELDS), "ack")
    return payload


def _decode_ack(payload: object) -> Ack:
    decoded = _require_exact_field_names(payload, frozenset(ACK_FIELDS), "ack")
    ack = Ack(
        schema_version=decoded.get("schema_version"),
        epoch_id=decoded.get("epoch_id"),
        run_kind=decoded.get("run_kind"),
        lane=decoded.get("lane"),
        seq=decoded.get("seq"),
        message_sha256=decoded.get("message_sha256"),
        decision=decoded.get("decision"),
    )
    _encode_ack(ack)
    return ack


def _durable_progress_hash(worker_root: Path, message: ProgressMessage) -> str:
    path = worker_root / "progress" / f"{message.seq:06d}.json"
    durable = read_progress(path, message.lane, message.seq)
    if durable != message:
        raise ValueError("durable progress differs from ACK message")
    _, content = _read_canonical_record(
        path, "progress", byte_cap=MAX_PROGRESS_BYTES
    )
    return hashlib.sha256(content).hexdigest()


def _read_ack_for_progress(
    worker_root: Path, message: ProgressMessage
) -> Ack:
    expected_hash = _durable_progress_hash(worker_root, message)
    path = worker_root / "acks" / f"{message.seq:06d}.json"
    payload, _ = _read_canonical_record(
        path, "ack", byte_cap=MAX_PROGRESS_BYTES
    )
    ack = _decode_ack(payload)
    if (
        ack.epoch_id != message.epoch_id
        or ack.run_kind != message.run_kind
        or ack.lane != message.lane
        or ack.seq != message.seq
        or ack.message_sha256 != expected_hash
    ):
        raise ValueError("ACK differs from durable progress")
    return ack


def write_ack(
    worker_root: Path, message: ProgressMessage, decision: AckDecision
) -> Path:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    _read_worker_protocol_identity(bound_worker, message)
    if type(decision) is not str or decision not in _ACK_DECISIONS:
        raise ValueError("ACK decision is invalid")
    ack = Ack(
        schema_version=1,
        epoch_id=message.epoch_id,
        run_kind=message.run_kind,
        lane=message.lane,
        seq=message.seq,
        message_sha256=_durable_progress_hash(bound_worker, message),
        decision=decision,
    )
    path = bound_worker / "acks" / f"{message.seq:06d}.json"
    _protocol_sequence_inventory(
        path.parent,
        label="ACK",
        deadline=None,
        publishing_seq=message.seq,
    )
    _write_protocol_record(path, _encode_ack(ack), byte_cap=MAX_PROGRESS_BYTES)
    return path


def wait_for_ack(
    worker_root: Path, message: ProgressMessage, timeout: float
) -> Ack:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    _read_worker_protocol_identity(bound_worker, message)
    _durable_progress_hash(bound_worker, message)
    timeout_value = _validate_wait_timeout(timeout)
    deadline = time.monotonic() + timeout_value
    ack_root = bound_worker / "acks"
    while True:
        sequences = _protocol_sequence_inventory(
            ack_root,
            label="ACK",
            deadline=deadline,
        )
        _require_protocol_sequence_prefix(
            sequences,
            expected_seq=message.seq,
            label="ACK",
        )
        for sequence in range(1, message.seq):
            if time.monotonic() >= deadline:
                raise TimeoutError("timed out waiting for progress ACK")
            prior_progress = read_progress(
                bound_worker / "progress" / f"{sequence:06d}.json",
                message.lane,
                sequence,
            )
            _read_ack_for_progress(bound_worker, prior_progress)
        if (
            _protocol_sequence_inventory(
                ack_root,
                label="ACK",
                deadline=deadline,
            )
            != sequences
        ):
            raise RuntimeError("ACK inventory changed while polling")
        if message.seq in sequences:
            ack = _read_ack_for_progress(bound_worker, message)
            if _protocol_sequence_inventory(
                ack_root,
                label="ACK",
                deadline=deadline,
            ) != sequences:
                raise RuntimeError("ACK inventory changed while reading")
            return ack
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("timed out waiting for progress ACK")
        time.sleep(min(0.01, remaining))


class ProgressAckLedger:
    """Sequence, idempotence, and cumulative launch-ceiling state."""

    def __init__(self, *, max_total_tokens: int | None) -> None:
        if max_total_tokens is not None and (
            type(max_total_tokens) is not int
            or max_total_tokens < 0
        ):
            raise ValueError(
                "max_total_tokens must be null or a non-negative exact integer"
            )
        self._max_total_tokens = max_total_tokens
        self._total_tokens = 0
        self._last_sequence = {
            lane: 0 for lane in ("E1", "E2", "E3", "APP")
        }
        self._accepted: dict[tuple[LaneName, int], tuple[str, AckDecision]] = {}
        self._active_cases: dict[LaneName, tuple[CaseKey, int] | None] = {
            lane: None for lane in ("E1", "E2", "E3", "APP")
        }
        self._completed_attempts: set[tuple[CaseKey, int]] = set()
        self._epoch_id: str | None = None
        self._run_kind: RunKind | None = None
        self._aborted = False
        self._stop_launches = max_total_tokens == 0
        self._exited: set[LaneName] = set()

    @property
    def total_tokens(self) -> int:
        return self._total_tokens

    @property
    def max_total_tokens(self) -> int | None:
        return self._max_total_tokens

    @property
    def stop_launches(self) -> bool:
        return self._stop_launches

    @property
    def aborted(self) -> bool:
        return self._aborted

    def accept_progress(self, message: ProgressMessage) -> AckDecision:
        payload = _encode_progress_message(message)
        if self._epoch_id is not None and (
            message.epoch_id != self._epoch_id
            or message.run_kind != self._run_kind
        ):
            raise ValueError("progress differs from the ledger protocol identity")
        message_sha256 = hashlib.sha256(
            canonical_config_bytes(payload)
        ).hexdigest()
        key = (message.lane, message.seq)
        previous = self._accepted.get(key)
        if previous is not None:
            previous_sha256, previous_decision = previous
            if previous_sha256 != message_sha256:
                raise ValueError("progress sequence already names a different message")
            return previous_decision
        expected_seq = self._last_sequence[message.lane] + 1
        if message.seq != expected_seq:
            raise ValueError("progress sequence is gapped or reordered")
        if message.lane in self._exited:
            raise ValueError("progress arrived after worker exit")

        active = self._active_cases[message.lane]
        case_attempt: tuple[CaseKey, int] | None = None
        if message.type in ("case-started", "case-terminal"):
            if message.case is None or message.attempt is None:
                raise AssertionError("case progress lacks its identity")
            case_attempt = (message.case, message.attempt)
            if case_attempt in self._completed_attempts:
                raise ValueError("completed case attempt cannot be replayed")
        if message.type == "case-started":
            if active is not None:
                raise ValueError("worker started a case before its prior terminal")
        if message.type == "case-terminal":
            if active is None:
                raise ValueError("case terminal lacks launch authority")
            if active != case_attempt:
                raise ValueError("case terminal differs from the active attempt")

        new_total = self._total_tokens
        reaches_ceiling = self._stop_launches
        if message.type == "case-terminal" and message.usage is not None:
            new_total += message.usage.total_tokens
            if (
                self._max_total_tokens is not None
                and new_total >= self._max_total_tokens
            ):
                reaches_ceiling = True

        decision: AckDecision
        if self._aborted:
            decision = "abort"
        elif message.type == "case-terminal" and message.status == "failed":
            self._aborted = True
            decision = "abort"
        elif message.type == "shard-terminal" and message.status == "failed":
            self._aborted = True
            decision = "abort"
        elif message.type == "worker-stopped" and (
            self._active_cases[message.lane] is not None
        ):
            self._aborted = True
            decision = "abort"
        elif reaches_ceiling:
            decision = "stop-launches"
        else:
            decision = "continue"

        self._total_tokens = new_total
        self._stop_launches = reaches_ceiling
        if message.type == "case-started" and decision == "continue":
            if message.case is None or message.attempt is None:
                raise AssertionError("case-started progress lacks its identity")
            self._active_cases[message.lane] = (
                message.case,
                message.attempt,
            )
        elif message.type == "case-terminal":
            if case_attempt is None:
                raise AssertionError("case terminal lacks its completed identity")
            self._completed_attempts.add(case_attempt)
            self._active_cases[message.lane] = None
        elif message.type == "worker-stopped":
            self._exited.add(message.lane)

        self._accepted[key] = (message_sha256, decision)
        self._last_sequence[message.lane] = message.seq
        if self._epoch_id is None:
            self._epoch_id = message.epoch_id
            self._run_kind = message.run_kind
        return decision

    def worker_exited(self, lane: LaneName) -> AckDecision:
        if type(lane) is not str or lane not in ("E1", "E2", "E3", "APP"):
            raise ValueError("worker exit lane is invalid")
        self._exited.add(lane)
        self._active_cases[lane] = None
        self._aborted = True
        return "abort"


def canonical_config_bytes(config: Mapping[str, Any]) -> bytes:
    return json.dumps(
        config, sort_keys=True, ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")


def component_digest(entries: Sequence[tuple[str, str]]) -> str:
    payload = json.dumps(
        sorted(entries), ensure_ascii=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def build_epoch_plan(
    *,
    run_kind: RunKind,
    manifests: dict[EvalMode, list[dict[str, object]]],
    fingerprints: InputFingerprints,
) -> EpochPlan:
    if run_kind not in ("diagnostic", "discovery", "formal"):
        raise ValueError(f"unsupported run kind: {run_kind}")
    if fingerprints.run_kind != run_kind:
        raise ValueError("plan and fingerprint run kinds differ")

    if set(manifests) != {"forward", "lifecycle"}:
        raise ValueError("manifests must contain exactly forward and lifecycle")

    lane_by_key = {
        key: lane for lane, lane_keys in FROZEN_LANE_CASES for key in lane_keys
    }
    assignments = []
    seen_keys = set()
    for mode in ("forward", "lifecycle"):
        manifest_sha256 = (
            fingerprints.forward_manifest_sha256
            if mode == "forward"
            else fingerprints.lifecycle_manifest_sha256
        )
        for ordinal, case in enumerate(manifests[mode], start=1):
            if type(case) is not dict:
                raise TypeError("manifest case must be an exact dict")
            _validate_manifest_case(case, mode=mode)
            key = CaseKey(mode, ordinal, str(case["id"]))
            if key in seen_keys:
                raise ValueError(f"duplicate case key: {key}")
            seen_keys.add(key)
            lane = lane_by_key.get(key)
            if lane is None:
                raise ValueError(f"case is absent from frozen lane mapping: {key}")
            turns = case.get("turns")
            if not isinstance(turns, list) or not turns:
                raise ValueError(f"case has no turns: {key}")
            route: Route = "exec" if len(turns) == 1 else "app-server"
            expected_route: Route = "app-server" if lane == "APP" else "exec"
            if route != expected_route:
                raise ValueError(f"case transport differs from frozen lane: {key}")
            if (
                mode == "lifecycle"
                and case.get("mode")
                != (
                    "command-selection-only"
                    if (mode, ordinal, key.case_id)
                    == _COMMAND_SELECTION_LIFECYCLE_KEY
                    else "executable"
                )
            ):
                raise ValueError(
                    f"lifecycle mode differs from the frozen case: {key}"
                )
            assignments.append(
                CaseAssignment(
                    key=key,
                    lane=lane,
                    route=route,
                    manifest_sha256=manifest_sha256,
                )
            )
    if seen_keys != set(lane_by_key):
        missing = sorted(set(lane_by_key) - seen_keys)
        raise ValueError(f"frozen case inventory is incomplete: missing={missing}")

    fingerprint_fields = asdict(fingerprints)
    fingerprint_fields.pop("epoch_id")
    identity = {
        "run_kind": run_kind,
        "fingerprints": fingerprint_fields,
        "assignments": [asdict(assignment) for assignment in assignments],
    }
    epoch_id = hashlib.sha256(canonical_config_bytes(identity)).hexdigest()
    bound_fingerprints = replace(fingerprints, epoch_id=epoch_id)
    plan = EpochPlan(
        schema_version=fingerprints.schema_version,
        epoch_id=epoch_id,
        run_kind=run_kind,
        fingerprints=bound_fingerprints,
        assignments=tuple(assignments),
    )
    _register_progress_epoch_context(plan=plan, manifests=manifests)
    return plan
