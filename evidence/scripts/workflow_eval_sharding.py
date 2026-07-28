from dataclasses import asdict, dataclass, fields, replace
from contextlib import ExitStack, contextmanager
import ctypes
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
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import weakref
import zipfile
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Protocol,
    Sequence,
)

if TYPE_CHECKING:
    from scripts.run_observing_workflows_task9_eval import (
        CaseEventSink,
        CaseExecution,
        CaseRuntime,
    )
    from scripts.run_observing_workflows_eval_worker import DrivenCase


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
AckDecision = Literal["continue", "retry", "stop-launches", "abort"]
CoordinatorPhase = Literal[
    "preflight",
    "running",
    "cancelling",
    "tearing-down",
    "validating",
    "validated",
    "commit-ready",
    "committed",
    "failed",
]
MAX_ATTEMPT_START_BYTES = 4 * 1024
MAX_ATTEMPT_TERMINAL_BYTES = 8 * 1024
MAX_CASE_RESULT_BYTES = 64 * 1024
MAX_CASE_EVIDENCE_BYTES = 16 * 1024
MAX_CASE_COMMIT_BYTES = 8 * 1024
MAX_SHARD_COMMIT_BYTES = 64 * 1024
MAX_PROGRESS_BYTES = 4096
MAX_PROGRESS_STRING_CHARS = 256
MAX_TOKEN_COUNT = 2**63 - 1
MAX_PROTOCOL_RECORDS = 35
MAX_PROTOCOL_CRASH_TEMPS = 19
MAX_PROTOCOL_PENDING_MARKERS = 19
MAX_PROTOCOL_IDENTITY_CRASH_TEMPS = 19
PROTOCOL_WORKER_LOCK_TIMEOUT_SECONDS = 0.2
PROTOCOL_WORKER_LOCK_POLL_SECONDS = 0.01
MAX_RETIRED_PROOF_RECORDS = 128
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


@dataclass(frozen=True)
class _RetainedUnlinkProof:
    parent_slot: _DescriptorSlot
    name: str
    label: str
    slot: _DescriptorSlot
    identity: tuple[int, int]
    size: int
    mtime_ns: int
    ctime_ns: int
    content: bytes


def _rename_exclusive_at(
    *,
    source_slot: _DescriptorSlot,
    source_name: str,
    destination_slot: _DescriptorSlot,
    destination_name: str,
) -> None:
    if (
        type(source_slot) is not _DescriptorSlot
        or type(destination_slot) is not _DescriptorSlot
        or any(
            type(name) is not str
            or not name
            or "/" in name
            or name in {".", ".."}
            for name in (source_name, destination_name)
        )
    ):
        raise TypeError("exclusive rename arguments are invalid")
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = getattr(library, "renameatx_np", None)
        flags = 0x00000004  # RENAME_EXCL
    elif sys.platform.startswith("linux"):
        rename = getattr(library, "renameat2", None)
        flags = 0x00000001  # RENAME_NOREPLACE
    else:
        rename = None
        flags = 0
    if rename is None:
        raise RuntimeError(
            "retained proof retirement requires no-clobber rename support"
        )
    rename.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    rename.restype = ctypes.c_int
    ctypes.set_errno(0)
    result = rename(
        source_slot.descriptor,
        os.fsencode(source_name),
        destination_slot.descriptor,
        os.fsencode(destination_name),
        flags,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(
            error_number,
            os.strerror(error_number),
            destination_name,
        )


def _read_retained_unlink_content(
    proof: "_RetainedUnlinkProof",
    *,
    byte_cap: int,
    require_original_ctime: bool,
) -> bytes:
    metadata = os.fstat(proof.slot.descriptor)
    _validate_owned_entry(
        metadata,
        label=proof.label,
        kind="file",
        mode=0o600,
    )
    os.lseek(proof.slot.descriptor, 0, os.SEEK_SET)
    content = bytearray()
    while True:
        chunk = os.read(
            proof.slot.descriptor,
            min(64 * 1024, byte_cap + 1 - len(content)),
        )
        if not chunk:
            break
        content.extend(chunk)
        if len(content) > byte_cap:
            raise ValueError(f"{proof.label} exceeds its byte cap")
    after = os.fstat(proof.slot.descriptor)
    if (
        _stat_identity(metadata) != proof.identity
        or _stat_identity(after) != proof.identity
        or after.st_size != proof.size
        or after.st_mtime_ns != proof.mtime_ns
        or (
            require_original_ctime
            and after.st_ctime_ns != proof.ctime_ns
        )
        or bytes(content) != proof.content
    ):
        raise ValueError(f"{proof.label} changed before retirement")
    return bytes(content)


def _retain_verified_unlink_proof(
    *,
    parent_slot: _DescriptorSlot,
    name: str,
    expected_content: bytes | None,
    label: str,
    byte_cap: int,
) -> _RetainedUnlinkProof:
    if (
        type(parent_slot) is not _DescriptorSlot
        or type(name) is not str
        or not name
        or "/" in name
        or name in {".", ".."}
        or (
            expected_content is not None
            and type(expected_content) is not bytes
        )
        or type(label) is not str
        or not label
        or type(byte_cap) is not int
        or byte_cap <= 0
    ):
        raise TypeError("retained unlink proof arguments are invalid")
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=parent_slot.descriptor)
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    proof: _RetainedUnlinkProof | None = None
    try:
        metadata = os.fstat(slot.descriptor)
        _validate_owned_entry(
            metadata,
            label=label,
            kind="file",
            mode=0o600,
        )
        if metadata.st_size > byte_cap:
            raise ValueError(f"{label} exceeds its byte cap")
        identity = _stat_identity(metadata)
        provisional = _RetainedUnlinkProof(
            parent_slot=parent_slot,
            name=name,
            label=label,
            slot=slot,
            identity=identity,
            size=metadata.st_size,
            mtime_ns=metadata.st_mtime_ns,
            ctime_ns=metadata.st_ctime_ns,
            content=b"",
        )
        os.lseek(slot.descriptor, 0, os.SEEK_SET)
        content = bytearray()
        while True:
            chunk = os.read(
                slot.descriptor,
                min(64 * 1024, byte_cap + 1 - len(content)),
            )
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > byte_cap:
                raise ValueError(f"{label} exceeds its byte cap")
        after = os.fstat(slot.descriptor)
        named = os.stat(
            name,
            dir_fd=parent_slot.descriptor,
            follow_symlinks=False,
        )
        if (
            _stat_identity(after) != identity
            or _stat_identity(named) != identity
            or after.st_size != metadata.st_size
            or after.st_mtime_ns != metadata.st_mtime_ns
            or after.st_ctime_ns != metadata.st_ctime_ns
            or len(content) != metadata.st_size
        ):
            raise ValueError(f"{label} changed while retaining")
        concrete = bytes(content)
        if (
            expected_content is not None
            and concrete != expected_content
        ):
            raise ValueError(f"{label} differs from its verified content")
        proof = replace(provisional, content=concrete)
    except BaseException as error:
        primary = error
    if primary is not None:
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label=f"{label} retention or close failed",
        )
    if proof is None:
        raise AssertionError("retained unlink proof produced no capability")
    return proof


def _validate_retired_proof_archive(
    archive_slot: _DescriptorSlot,
    *,
    additional_records: int,
) -> tuple[str, ...]:
    if (
        type(archive_slot) is not _DescriptorSlot
        or type(additional_records) is not int
        or additional_records < 0
        or additional_records > MAX_RETIRED_PROOF_RECORDS
    ):
        raise TypeError("retired proof archive arguments are invalid")
    before = os.fstat(archive_slot.descriptor)
    _validate_owned_entry(
        before,
        label="retired proof archive",
        kind="directory",
        mode=0o700,
    )
    inventory = tuple(sorted(os.listdir(archive_slot.descriptor)))
    if len(inventory) + additional_records > MAX_RETIRED_PROOF_RECORDS:
        raise ValueError("retired proof archive exceeds its record cap")
    for name in inventory:
        if (
            type(name) is not str
            or not re.fullmatch(r"[0-9a-f]{32}-.+", name)
            or "/" in name
        ):
            raise ValueError("retired proof archive has an invalid record")
        metadata = os.stat(
            name,
            dir_fd=archive_slot.descriptor,
            follow_symlinks=False,
        )
        _validate_owned_entry(
            metadata,
            label="retired proof record",
            kind="file",
            mode=0o600,
        )
        if metadata.st_size > 64 * 1024:
            raise ValueError("retired proof record exceeds its byte cap")
    after = os.fstat(archive_slot.descriptor)
    if (
        _stat_identity(before) != _stat_identity(after)
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ctime_ns != after.st_ctime_ns
        or tuple(sorted(os.listdir(archive_slot.descriptor))) != inventory
    ):
        raise RuntimeError("retired proof archive changed while scanning")
    return inventory


def _retire_retained_proofs(
    proofs: Sequence[_RetainedUnlinkProof],
    *,
    archive_slot: _DescriptorSlot,
) -> None:
    frozen = tuple(proofs)
    if (
        not frozen
        or any(type(proof) is not _RetainedUnlinkProof for proof in frozen)
        or type(archive_slot) is not _DescriptorSlot
        or len({(id(proof.parent_slot), proof.name) for proof in frozen})
        != len(frozen)
    ):
        raise ValueError("retained retirement proofs are invalid")
    initial_inventory = _validate_retired_proof_archive(
        archive_slot,
        additional_records=len(frozen),
    )
    staged: list[tuple[_RetainedUnlinkProof, str]] = []
    primary: BaseException | None = None
    try:
        for proof in frozen:
            _read_retained_unlink_content(
                proof,
                byte_cap=max(proof.size, 1),
                require_original_ctime=True,
            )
            named = os.stat(
                proof.name,
                dir_fd=proof.parent_slot.descriptor,
                follow_symlinks=False,
            )
            if _stat_identity(named) != proof.identity:
                raise ValueError(f"{proof.label} changed before retirement")
            archived_name = f"{secrets.token_hex(16)}-{proof.name}"
            _rename_exclusive_at(
                source_slot=proof.parent_slot,
                source_name=proof.name,
                destination_slot=archive_slot,
                destination_name=archived_name,
            )
            staged.append((proof, archived_name))
            archived = os.stat(
                archived_name,
                dir_fd=archive_slot.descriptor,
                follow_symlinks=False,
            )
            if _stat_identity(archived) != proof.identity:
                raise ValueError(f"{proof.label} changed before retirement")
            _read_retained_unlink_content(
                proof,
                byte_cap=max(proof.size, 1),
                require_original_ctime=False,
            )
        for proof, archived_name in staged:
            archived = os.stat(
                archived_name,
                dir_fd=archive_slot.descriptor,
                follow_symlinks=False,
            )
            if _stat_identity(archived) != proof.identity:
                raise ValueError(f"{proof.label} changed before retirement")
            _read_retained_unlink_content(
                proof,
                byte_cap=max(proof.size, 1),
                require_original_ctime=False,
            )
        if set(_validate_retired_proof_archive(
            archive_slot,
            additional_records=0,
        )) != set(initial_inventory) | {
            archived_name for _proof, archived_name in staged
        }:
            raise RuntimeError("retired proof archive changed during retirement")
        for parent_slot in {
            id(proof.parent_slot): proof.parent_slot for proof in frozen
        }.values():
            os.fsync(parent_slot.descriptor)
        os.fsync(archive_slot.descriptor)
    except BaseException as error:
        primary = error
        rollback_errors: list[BaseException] = []
        for proof, archived_name in reversed(staged):
            try:
                try:
                    os.stat(
                        archived_name,
                        dir_fd=archive_slot.descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    continue
                try:
                    os.stat(
                        proof.name,
                        dir_fd=proof.parent_slot.descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    _rename_exclusive_at(
                        source_slot=archive_slot,
                        source_name=archived_name,
                        destination_slot=proof.parent_slot,
                        destination_name=proof.name,
                    )
            except BaseException as rollback_error:
                rollback_errors.append(rollback_error)
        for parent_slot in {
            id(proof.parent_slot): proof.parent_slot for proof in frozen
        }.values():
            try:
                os.fsync(parent_slot.descriptor)
            except BaseException as rollback_error:
                rollback_errors.append(rollback_error)
        try:
            os.fsync(archive_slot.descriptor)
        except BaseException as rollback_error:
            rollback_errors.append(rollback_error)
        if rollback_errors:
            errors = [primary, *rollback_errors]
            group_type = (
                ExceptionGroup
                if all(isinstance(item, Exception) for item in errors)
                else BaseExceptionGroup
            )
            primary = group_type(
                "retained proof retirement and rollback failed",
                errors,
            )
    _retire_task_descriptors(
        [proof.slot for proof in frozen],
        primary=primary,
        label="retained proof retirement or descriptor close failed",
    )


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


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    next_attempt: Literal[2] | None
    action: Literal["reuse", "invalidate", "abort"]
    reason: str


@dataclass(frozen=True)
class ResumePlan:
    run_kind: RunKind
    reusable: tuple[CaseKey, ...]
    pending: tuple[CaseKey, ...]
    invalid: tuple[CaseKey, ...]


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


@dataclass(frozen=True)
class Aggregate:
    run_kind: RunKind
    forward_rows: tuple[dict[str, object], ...]
    lifecycle_rows: tuple[dict[str, object], ...]
    evidence_sha256: str


@dataclass(frozen=True)
class ProductionSnapshot:
    fingerprint: str
    entries: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ParallelOptions:
    run_kind: RunKind
    run_root: Path
    source_codex_home: Path
    codex_executable: Path
    requested_model: str | None = None
    requested_reasoning_effort: str | None = None
    resume_run_root: Path | None = None
    max_total_tokens: int | None = None


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
    ) -> "CaseRuntime": ...

    def cleanup_case(self, paths: CasePaths) -> TombstoneReceipt: ...

    def close(self) -> None: ...


class CaseDriver(Protocol):
    def __call__(
        self,
        *,
        assignment: CaseAssignment,
        manifest_case: dict[str, object],
        paths: CasePaths,
        runtime_factory: RuntimeFactory,
        event_sink: "CaseEventSink",
    ) -> "DrivenCase": ...


class CaseTransport(Protocol):
    def __call__(
        self,
        case: dict[str, object],
        workspace: Path,
        runtime: "CaseRuntime",
        wiki_root: Path,
        after_first_turn: Callable[[], None] | None = None,
        event_sink: "CaseEventSink | None" = None,
    ) -> "CaseExecution": ...


@dataclass(frozen=True)
class WorkerDependencies:
    runtime_factory: RuntimeFactory
    case_driver: CaseDriver


WorkerCommandFactory = Callable[
    [LaneName, EpochPlan, ParallelOptions, Path], Sequence[str]
]


@dataclass(frozen=True)
class CoordinatorDependencies:
    worker_command_factory: WorkerCommandFactory
    integrity_runner: Callable[..., dict[str, object]]


@dataclass(frozen=True)
class ParallelRunResult:
    run_kind: RunKind
    run_root: Path
    status: Literal["diagnostic", "validated", "committed", "failed"]
    validated: "ValidatedEpoch | None"


_COORDINATOR_GUARD_TOKEN = object()
_VALIDATED_EPOCH_TOKEN = object()
_FORMAL_COMMIT_TOKEN = object()
_QUIESCENT_AUTHORITY_TOKEN = object()


@dataclass(frozen=True)
class _CoordinatorGuardBinding:
    repository_root: Path
    repository_key: str
    baseline: ProductionSnapshot
    baseline_rows: tuple[tuple[str, str, int, int, str], ...]
    owner_pid: int


@dataclass(frozen=True)
class _ValidatedEpochBinding:
    plan: EpochPlan
    forward_bytes: bytes
    lifecycle_bytes: bytes
    manifest_bytes: tuple[bytes, bytes]
    forward_sha256: str
    lifecycle_sha256: str
    manifest_sha256: tuple[str, str]
    evidence_sha256: str
    teardown_receipt_sha256: str
    guard: object
    guard_binding: _CoordinatorGuardBinding
    repository_root: Path
    repository_key: str
    baseline: ProductionSnapshot
    baseline_rows: tuple[tuple[str, str, int, int, str], ...]
    owner_pid: int


@dataclass(frozen=True)
class _ValidatedEpochIssuance:
    binding: _ValidatedEpochBinding
    claim_state: Literal["validated", "issued"]
    issued_capability_ref: Any


@dataclass(frozen=True)
class _FormalCommitBinding:
    validated_ref: Any
    validated_binding: _ValidatedEpochBinding
    plan: EpochPlan
    forward_bytes: bytes
    lifecycle_bytes: bytes
    manifest_bytes: tuple[bytes, bytes]
    evidence_sha256: str
    teardown_receipt_sha256: str
    guard: object
    guard_binding: _CoordinatorGuardBinding
    repository_root: Path
    repository_key: str
    baseline: ProductionSnapshot
    baseline_rows: tuple[tuple[str, str, int, int, str], ...]
    owner_pid: int


@dataclass(frozen=True)
class _FormalCommitIssuance:
    binding: _FormalCommitBinding
    consumed: bool


class _NominalCapabilityRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._guards = weakref.WeakKeyDictionary()
        self._validated_epochs = weakref.WeakKeyDictionary()
        self._formal_commits = weakref.WeakKeyDictionary()

    @staticmethod
    def _lookup_locked(
        records: weakref.WeakKeyDictionary,
        capability: object,
        label: str,
        expected_type: type,
    ) -> object:
        try:
            snapshot = records[capability]
        except (KeyError, TypeError):
            raise RuntimeError(f"{label} was not module-issued") from None
        if type(snapshot) is not expected_type:
            raise RuntimeError(f"{label} issuance snapshot changed")
        return snapshot

    @staticmethod
    def _issue_locked(
        records: weakref.WeakKeyDictionary,
        capability: object,
        snapshot: object,
    ) -> None:
        if capability in records:
            raise RuntimeError("capability was already issued")
        records[capability] = snapshot

    def issue_guard(
        self,
        guard: object,
        binding: _CoordinatorGuardBinding,
    ) -> None:
        with self._lock:
            self._issue_locked(self._guards, guard, binding)

    def _guard_binding_locked(
        self, guard: object
    ) -> _CoordinatorGuardBinding:
        binding = self._lookup_locked(
            self._guards,
            guard,
            "CoordinatorGuard",
            _CoordinatorGuardBinding,
        )
        guard._validate_registry_binding(binding)
        return binding

    def guard_binding(
        self, guard: object
    ) -> _CoordinatorGuardBinding:
        with self._lock:
            return self._guard_binding_locked(guard)

    def issue_validated(
        self,
        validated: object,
        binding: _ValidatedEpochBinding,
    ) -> None:
        with self._lock:
            self._issue_locked(
                self._validated_epochs,
                validated,
                _ValidatedEpochIssuance(
                    binding=binding,
                    claim_state="validated",
                    issued_capability_ref=None,
                ),
            )

    def _validated_snapshot_locked(
        self, validated: object
    ) -> tuple[_ValidatedEpochIssuance, object | None]:
        snapshot = self._lookup_locked(
            self._validated_epochs,
            validated,
            "ValidatedEpoch",
            _ValidatedEpochIssuance,
        )
        issued_capability = (
            None
            if snapshot.issued_capability_ref is None
            else snapshot.issued_capability_ref()
        )
        if (
            snapshot.claim_state == "validated"
            and snapshot.issued_capability_ref is not None
        ) or (
            snapshot.claim_state == "issued"
            and snapshot.issued_capability_ref is None
        ):
            raise RuntimeError("ValidatedEpoch issuance state changed")
        validated._validate_registry_binding(snapshot.binding)
        guard_binding = self._guard_binding_locked(snapshot.binding.guard)
        if (
            guard_binding is not snapshot.binding.guard_binding
            or guard_binding.repository_root
            != snapshot.binding.repository_root
            or guard_binding.repository_key
            != snapshot.binding.repository_key
            or guard_binding.baseline is not snapshot.binding.baseline
            or guard_binding.baseline_rows
            is not snapshot.binding.baseline_rows
        ):
            raise RuntimeError("ValidatedEpoch guard binding changed")
        return snapshot, issued_capability

    def validated_binding(
        self, validated: object
    ) -> _ValidatedEpochBinding:
        with self._lock:
            snapshot, _issued_capability = (
                self._validated_snapshot_locked(validated)
            )
            return snapshot.binding

    def issue_formal(
        self,
        commit: object,
        binding: _FormalCommitBinding,
    ) -> None:
        with self._lock:
            self._issue_locked(
                self._formal_commits,
                commit,
                _FormalCommitIssuance(
                    binding=binding,
                    consumed=False,
                ),
            )

    def claim_formal(self, validated: object) -> object:
        with self._lock:
            snapshot, _issued_capability = (
                self._validated_snapshot_locked(validated)
            )
            if snapshot.binding.plan.run_kind != "formal":
                raise RuntimeError(
                    "only a formal validated epoch can claim commit"
                )
            if snapshot.claim_state != "validated":
                raise RuntimeError(
                    "formal commit capability was already claimed"
                )
            capability = FormalCommitCapability(
                _FORMAL_COMMIT_TOKEN,
                validated=validated,
                validated_binding=snapshot.binding,
            )
            self._validated_epochs[validated] = replace(
                snapshot,
                claim_state="issued",
                issued_capability_ref=weakref.ref(capability),
            )
            return capability

    def _formal_snapshot_locked(
        self, commit: object
    ) -> _FormalCommitIssuance:
        snapshot = self._lookup_locked(
            self._formal_commits,
            commit,
            "FormalCommitCapability",
            _FormalCommitIssuance,
        )
        validated = snapshot.binding.validated_ref()
        commit._validate_registry_binding(snapshot.binding)
        if type(validated) is not ValidatedEpoch:
            raise RuntimeError("FormalCommitCapability issuer changed")
        validated_snapshot, issued_capability = (
            self._validated_snapshot_locked(validated)
        )
        if (
            validated_snapshot.binding
            is not snapshot.binding.validated_binding
            or validated_snapshot.claim_state != "issued"
            or issued_capability is not commit
        ):
            raise RuntimeError(
                "FormalCommitCapability provenance binding changed"
            )
        return snapshot

    def formal_binding(
        self, commit: object
    ) -> _FormalCommitBinding:
        with self._lock:
            return self._formal_snapshot_locked(commit).binding

    def formal_preflight(
        self, commit: object
    ) -> _FormalCommitBinding:
        with self._lock:
            snapshot = self._formal_snapshot_locked(commit)
            if snapshot.consumed:
                raise RuntimeError(
                    "formal commit capability was already consumed"
                )
            return snapshot.binding

    def formal_consumed(self, commit: object) -> bool:
        with self._lock:
            return self._formal_snapshot_locked(commit).consumed

    def consume_formal(
        self,
        commit: object,
        expected_binding: _FormalCommitBinding | None,
    ) -> _FormalCommitBinding:
        with self._lock:
            snapshot = self._formal_snapshot_locked(commit)
            if (
                expected_binding is not None
                and snapshot.binding is not expected_binding
            ):
                raise RuntimeError(
                    "formal commit capability preflight binding changed"
                )
            if snapshot.consumed:
                raise RuntimeError(
                    "formal commit capability was already consumed"
                )
            self._formal_commits[commit] = replace(
                snapshot, consumed=True
            )
            return snapshot.binding


_CAPABILITY_REGISTRY = _NominalCapabilityRegistry()


class CoordinatorGuard:
    def __init__(
        self,
        token: object,
        *,
        repository_root: Path,
        repository_key: str,
        baseline: ProductionSnapshot,
        baseline_rows: tuple[tuple[str, str, int, int, str], ...],
    ) -> None:
        if token is not _COORDINATOR_GUARD_TOKEN:
            raise TypeError("CoordinatorGuard cannot be constructed directly")
        self._repository_root = repository_root
        self._repository_key = repository_key
        self._baseline = baseline
        self._baseline_rows = baseline_rows
        self._owner_pid = os.getpid()
        _CAPABILITY_REGISTRY.issue_guard(
            self,
            _CoordinatorGuardBinding(
                repository_root=repository_root,
                repository_key=repository_key,
                baseline=baseline,
                baseline_rows=baseline_rows,
                owner_pid=self._owner_pid,
            ),
        )

    @classmethod
    def capture(cls, repository_root: Path) -> "CoordinatorGuard":
        if cls is not CoordinatorGuard:
            raise TypeError("CoordinatorGuard subclasses are unsupported")
        canonical, _ = _canonical_git_repository_root(repository_root)
        rows = _capture_production_rows(canonical)
        snapshot = _production_snapshot_from_rows(rows)
        return cls(
            _COORDINATOR_GUARD_TOKEN,
            repository_root=canonical,
            repository_key=hashlib.sha256(os.fsencode(canonical)).hexdigest(),
            baseline=snapshot,
            baseline_rows=rows,
        )

    def _validate_registry_binding(
        self, binding: _CoordinatorGuardBinding
    ) -> None:
        if binding.owner_pid != os.getpid():
            raise RuntimeError("CoordinatorGuard belongs to another process")
        if (
            self._owner_pid != binding.owner_pid
            or self._repository_root != binding.repository_root
            or self._repository_key != binding.repository_key
            or self._baseline is not binding.baseline
            or self._baseline_rows is not binding.baseline_rows
            or type(self._baseline) is not ProductionSnapshot
            or not _is_sha256(self._baseline.fingerprint)
        ):
            raise RuntimeError("CoordinatorGuard provenance binding changed")

    def _validate_nominal(self) -> _CoordinatorGuardBinding:
        if type(self) is not CoordinatorGuard:
            raise TypeError("CoordinatorGuard must be exact")
        return _CAPABILITY_REGISTRY.guard_binding(self)

    @property
    def baseline(self) -> ProductionSnapshot:
        return self._validate_nominal().baseline

    def checkpoint(self, reason: str) -> ProductionSnapshot:
        binding = self._validate_nominal()
        _require_guard_reason(reason)
        rows = _capture_production_rows(binding.repository_root)
        if rows != binding.baseline_rows:
            raise AssertionError(f"production changed at coordinator checkpoint: {reason}")
        return _production_snapshot_from_rows(rows)

    def verify_exact_result_delta(
        self, expected: dict[str, str], reason: str
    ) -> ProductionSnapshot:
        binding = self._validate_nominal()
        _require_guard_reason(reason)
        frozen_expected = _validate_expected_result_delta(expected)
        current_rows = _capture_production_rows(binding.repository_root)
        allowed = {row[0]: row for row in binding.baseline_rows}
        current = {row[0]: row for row in current_rows}
        for relative, expected_sha256 in frozen_expected.items():
            path = PurePosixPath(relative)
            parent = path.parent
            while parent != PurePosixPath("."):
                parent_text = parent.as_posix()
                if parent_text not in allowed:
                    observed_parent = current.get(parent_text)
                    if (
                        observed_parent is None
                        or observed_parent[1:] != ("directory", 0o700, 0, "")
                    ):
                        raise AssertionError(
                            f"unexpected result parent at coordinator checkpoint: {reason}"
                        )
                    allowed[parent_text] = observed_parent
                parent = parent.parent
            observed = current.get(relative)
            if (
                observed is None
                or observed[1] != "file"
                or observed[2] != 0o600
                or observed[4] != expected_sha256
            ):
                raise AssertionError(
                    f"result delta hash or mode differs at coordinator checkpoint: {reason}"
                )
            allowed[relative] = observed
        if tuple(sorted(allowed.values())) != current_rows:
            raise AssertionError(
                f"unexpected production delta at coordinator checkpoint: {reason}"
            )
        return _production_snapshot_from_rows(current_rows)


class ValidatedEpoch:
    def __init__(
        self,
        token: object,
        *,
        plan: EpochPlan,
        forward_bytes: bytes,
        lifecycle_bytes: bytes,
        manifest_bytes: tuple[bytes, bytes],
        evidence_sha256: str,
        teardown_receipt_sha256: str,
        guard: CoordinatorGuard,
    ) -> None:
        if token is not _VALIDATED_EPOCH_TOKEN:
            raise TypeError("ValidatedEpoch cannot be constructed directly")
        self._plan = plan
        self._forward_bytes = forward_bytes
        self._lifecycle_bytes = lifecycle_bytes
        self._manifest_bytes = manifest_bytes
        self._forward_sha256 = hashlib.sha256(forward_bytes).hexdigest()
        self._lifecycle_sha256 = hashlib.sha256(lifecycle_bytes).hexdigest()
        self._manifest_sha256 = tuple(
            hashlib.sha256(content).hexdigest() for content in manifest_bytes
        )
        self._evidence_sha256 = evidence_sha256
        self._teardown_receipt_sha256 = teardown_receipt_sha256
        self._guard = guard
        self._owner_pid = os.getpid()
        guard_binding = guard._validate_nominal()
        binding = _ValidatedEpochBinding(
            plan=plan,
            forward_bytes=forward_bytes,
            lifecycle_bytes=lifecycle_bytes,
            manifest_bytes=manifest_bytes,
            forward_sha256=self._forward_sha256,
            lifecycle_sha256=self._lifecycle_sha256,
            manifest_sha256=self._manifest_sha256,
            evidence_sha256=evidence_sha256,
            teardown_receipt_sha256=teardown_receipt_sha256,
            guard=guard,
            guard_binding=guard_binding,
            repository_root=guard_binding.repository_root,
            repository_key=guard_binding.repository_key,
            baseline=guard_binding.baseline,
            baseline_rows=guard_binding.baseline_rows,
            owner_pid=self._owner_pid,
        )
        _CAPABILITY_REGISTRY.issue_validated(self, binding)

    def _issuance(self) -> _ValidatedEpochBinding:
        if type(self) is not ValidatedEpoch:
            raise TypeError("ValidatedEpoch must be exact")
        return _CAPABILITY_REGISTRY.validated_binding(self)

    def _validate_registry_binding(
        self, binding: _ValidatedEpochBinding
    ) -> None:
        if binding.owner_pid != os.getpid():
            raise RuntimeError("ValidatedEpoch belongs to another process")
        if (
            self._owner_pid != binding.owner_pid
            or self._plan is not binding.plan
            or self._forward_bytes is not binding.forward_bytes
            or self._lifecycle_bytes is not binding.lifecycle_bytes
            or self._manifest_bytes is not binding.manifest_bytes
            or self._forward_sha256 != binding.forward_sha256
            or self._lifecycle_sha256 != binding.lifecycle_sha256
            or self._manifest_sha256 != binding.manifest_sha256
            or self._evidence_sha256 != binding.evidence_sha256
            or self._teardown_receipt_sha256
            != binding.teardown_receipt_sha256
            or self._guard is not binding.guard
            or type(self._plan) is not EpochPlan
            or binding.plan.run_kind not in ("discovery", "formal")
            or not _is_sha256(binding.evidence_sha256)
            or not _is_sha256(binding.teardown_receipt_sha256)
            or type(self._guard) is not CoordinatorGuard
        ):
            raise RuntimeError("ValidatedEpoch provenance binding changed")

    def _validate_nominal(self) -> _ValidatedEpochBinding:
        return self._issuance()

    @property
    def run_kind(self) -> RunKind:
        return self._validate_nominal().plan.run_kind

    @property
    def epoch_id(self) -> str:
        return self._validate_nominal().plan.epoch_id

    @property
    def teardown_receipt_sha256(self) -> str:
        return self._validate_nominal().teardown_receipt_sha256

    def claim_formal_commit(self) -> "FormalCommitCapability":
        if type(self) is not ValidatedEpoch:
            raise TypeError("ValidatedEpoch must be exact")
        capability = _CAPABILITY_REGISTRY.claim_formal(self)
        if type(capability) is not FormalCommitCapability:
            raise RuntimeError("formal commit issuance returned an invalid type")
        return capability


class FormalCommitCapability:
    def __init__(
        self,
        token: object,
        *,
        validated: ValidatedEpoch,
        validated_binding: _ValidatedEpochBinding,
    ) -> None:
        if token is not _FORMAL_COMMIT_TOKEN or type(validated) is not ValidatedEpoch:
            raise TypeError("FormalCommitCapability cannot be constructed directly")
        self._validated = validated
        self._owner_pid = os.getpid()
        binding = _FormalCommitBinding(
            validated_ref=weakref.ref(validated),
            validated_binding=validated_binding,
            plan=validated_binding.plan,
            forward_bytes=validated_binding.forward_bytes,
            lifecycle_bytes=validated_binding.lifecycle_bytes,
            manifest_bytes=validated_binding.manifest_bytes,
            evidence_sha256=validated_binding.evidence_sha256,
            teardown_receipt_sha256=(
                validated_binding.teardown_receipt_sha256
            ),
            guard=validated_binding.guard,
            guard_binding=validated_binding.guard_binding,
            repository_root=validated_binding.repository_root,
            repository_key=validated_binding.repository_key,
            baseline=validated_binding.baseline,
            baseline_rows=validated_binding.baseline_rows,
            owner_pid=self._owner_pid,
        )
        _CAPABILITY_REGISTRY.issue_formal(self, binding)

    def _issuance(self) -> _FormalCommitBinding:
        if type(self) is not FormalCommitCapability:
            raise TypeError("FormalCommitCapability must be exact")
        return _CAPABILITY_REGISTRY.formal_binding(self)

    def _validate_registry_binding(
        self, binding: _FormalCommitBinding
    ) -> None:
        validated = binding.validated_ref()
        if binding.owner_pid != os.getpid():
            raise RuntimeError("FormalCommitCapability belongs to another process")
        if (
            self._owner_pid != binding.owner_pid
            or validated is None
            or self._validated is not validated
            or binding.plan.run_kind != "formal"
        ):
            raise RuntimeError("FormalCommitCapability provenance binding changed")

    def _validate_nominal(self) -> _FormalCommitBinding:
        return self._issuance()

    def _preflight(self) -> _FormalCommitBinding:
        if type(self) is not FormalCommitCapability:
            raise TypeError("FormalCommitCapability must be exact")
        return _CAPABILITY_REGISTRY.formal_preflight(self)

    @property
    def epoch_id(self) -> str:
        return self._validate_nominal().plan.epoch_id

    @property
    def run_kind(self) -> Literal["formal"]:
        self._validate_nominal()
        return "formal"

    @property
    def consumed(self) -> bool:
        if type(self) is not FormalCommitCapability:
            raise TypeError("FormalCommitCapability must be exact")
        return _CAPABILITY_REGISTRY.formal_consumed(self)

    def _consume(
        self,
        expected_binding: _FormalCommitBinding | None = None,
    ) -> _FormalCommitBinding:
        if type(self) is not FormalCommitCapability:
            raise TypeError("FormalCommitCapability must be exact")
        return _CAPABILITY_REGISTRY.consume_formal(
            self, expected_binding
        )


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


def _read_regular_bytes_at(
    *,
    parent_slot: _DescriptorSlot,
    parent_path: Path,
    parent_before: os.stat_result,
    name: str,
    label: str,
    byte_cap: int,
) -> bytes:
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
    return bytes(content)


def _read_canonical_record_at(
    *,
    parent_slot: _DescriptorSlot,
    parent_path: Path,
    parent_before: os.stat_result,
    name: str,
    label: str,
    byte_cap: int,
) -> tuple[dict[str, object], bytes]:
    content = _read_regular_bytes_at(
        parent_slot=parent_slot,
        parent_path=parent_path,
        parent_before=parent_before,
        name=name,
        label=label,
        byte_cap=byte_cap,
    )
    try:
        decoded = json.loads(content.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError(f"{label} is not canonical ASCII JSON") from None
    if not isinstance(decoded, dict) or canonical_config_bytes(decoded) != content:
        raise ValueError(f"{label} is not canonical ASCII JSON")
    return decoded, content


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


def _verified_tombstone_receipt_from_records(
    *,
    ownership: CaseAuthOwnership,
    ownership_bytes: bytes,
    payload: dict[str, object],
    content: bytes,
    plan: EpochPlan,
    assignment: CaseAssignment,
) -> VerifiedTombstoneReceipt:
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
        or receipt.canonical_binding not in ("expected", "missing", "replaced")
        or receipt.producer not in ("worker", "coordinator-recovery")
    ):
        raise ValueError("case auth tombstone is stale or invalid")
    return VerifiedTombstoneReceipt(
        receipt=receipt,
        sha256=hashlib.sha256(content).hexdigest(),
    )


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
            result = _verified_tombstone_receipt_from_records(
                ownership=ownership,
                ownership_bytes=ownership_bytes,
                payload=payload,
                content=content,
                plan=plan,
                assignment=assignment,
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


def _read_verified_tombstone_receipt_retained(
    *,
    directory: _RecordChildDirectoryCapability,
    plan: EpochPlan,
    assignment: CaseAssignment,
) -> VerifiedTombstoneReceipt:
    inventory = directory.inventory()
    if inventory != ("ownership.json", "tombstone.json"):
        raise ValueError("case auth cleanup inventory is invalid")
    ownership_payload, ownership_bytes = _read_canonical_record_retained(
        directory,
        "ownership.json",
        "case auth ownership",
        byte_cap=1024 * 1024,
    )
    ownership, ownership_bytes = _decode_case_auth_ownership(
        payload=ownership_payload,
        content=ownership_bytes,
        plan=plan,
        assignment=assignment,
    )
    payload, content = _read_canonical_record_retained(
        directory,
        "tombstone.json",
        "case auth tombstone",
        byte_cap=1024 * 1024,
    )
    result = _verified_tombstone_receipt_from_records(
        ownership=ownership,
        ownership_bytes=ownership_bytes,
        payload=payload,
        content=content,
        plan=plan,
        assignment=assignment,
    )
    if directory.inventory() != inventory:
        raise RuntimeError("case auth cleanup inventory changed while reading")
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


def scan_attempts(
    case: CasePaths,
    *,
    plan: EpochPlan,
    manifest_case: dict[str, object],
) -> tuple[AttemptPaths, ...]:
    if not isinstance(case, CasePaths):
        raise TypeError("case must be CasePaths")
    if not isinstance(plan, EpochPlan):
        raise TypeError("plan must be EpochPlan")
    run_root = canonical_run_root(case.root.parent.parent)
    matches = []
    for assignment in plan.assignments:
        if not isinstance(assignment, CaseAssignment):
            continue
        expected = paths_for_case(run_root, assignment)
        if expected.root == case.root:
            matches.append((assignment, expected))
    if len(matches) != 1:
        raise ValueError("case paths do not identify exactly one planned case")
    assignment, canonical = matches[0]
    if case != canonical:
        raise ValueError("case paths differ from canonical planned paths")
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=case,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    try:
        case.root.lstat()
    except FileNotFoundError:
        return ()
    except OSError:
        raise ValueError("case root is unavailable") from None

    case_directory = _open_case_record_directory(
        paths=case,
        components=(),
        create=False,
        label="case record directory",
    )
    with case_directory:
        case_inventory = case_directory.inventory()
        if "attempts" not in case_inventory:
            return ()

    directory = _open_case_record_directory(
        paths=case,
        components=("attempts",),
        create=False,
        label="attempt root",
    )
    with directory:
        inventory = directory.inventory()
        if inventory not in ((), ("01",), ("01", "02")):
            if "01" not in inventory:
                raise ValueError("attempt sequence contains a gap")
            raise ValueError("attempt directory name or sequence is invalid")
        with ExitStack() as child_stack:
            children = tuple(
                child_stack.enter_context(
                    _open_record_child_directory(
                        directory,
                        name,
                        label="attempt directory",
                    )
                )
                for name in inventory
            )
            if directory.inventory() != inventory:
                raise RuntimeError(
                    "attempt inventory changed while retaining children"
                )
            found: list[AttemptPaths] = []
            for attempt_number, child in enumerate(children, start=1):
                _read_attempt_seal_retained(
                    directory=child,
                    plan=plan,
                    assignment=assignment,
                    attempt=attempt_number,
                    manifest_case_sha256=manifest_case_sha256,
                )
                found.append(paths_for_attempt(case, attempt_number))
            if directory.inventory() != inventory:
                raise RuntimeError("attempt inventory changed while scanning")
            for child in children:
                child._validate_live()
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


def _read_regular_bytes_retained(
    directory: _RecordDirectoryCapability,
    name: str,
    label: str,
    *,
    byte_cap: int,
) -> bytes:
    directory._validate_live()
    parent_slot = directory._retained[-1].slot
    parent_before = os.fstat(parent_slot.descriptor)
    content = _read_regular_bytes_at(
        parent_slot=parent_slot,
        parent_path=directory.path,
        parent_before=parent_before,
        name=name,
        label=label,
        byte_cap=byte_cap,
    )
    directory._validate_live()
    return content


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


def decide_retry(
    *,
    classification: OutcomeClass,
    attempt: int,
    model_started: bool,
    cleanup_passed: bool,
    fingerprints_unchanged: bool,
) -> RetryDecision:
    if (
        type(classification) is not str
        or classification not in _OUTCOME_CLASSES
    ):
        raise ValueError("retry classification is invalid")
    if type(attempt) is not int or attempt not in (1, 2):
        raise ValueError("retry attempt must be exactly 1 or 2")
    if (
        type(model_started) is not bool
        or type(cleanup_passed) is not bool
        or type(fingerprints_unchanged) is not bool
    ):
        raise TypeError("retry predicates must be exact bools")
    if not cleanup_passed:
        return RetryDecision(
            retry=False,
            next_attempt=None,
            action="abort",
            reason="cleanup was not proved complete",
        )
    if not fingerprints_unchanged:
        return RetryDecision(
            retry=False,
            next_attempt=None,
            action="abort",
            reason="captured input fingerprints changed",
        )
    if classification == "success":
        if not model_started:
            return RetryDecision(
                retry=False,
                next_attempt=None,
                action="abort",
                reason="success without a model start is inconsistent",
            )
        return RetryDecision(
            retry=False,
            next_attempt=None,
            action="reuse",
            reason="successful attempt is reusable",
        )
    if classification in ("semantic", "model"):
        return RetryDecision(
            retry=False,
            next_attempt=None,
            action="invalidate",
            reason="semantic or model failure invalidates the case",
        )
    if (
        classification == "pre-model-infrastructure"
        and attempt == 1
        and not model_started
    ):
        return RetryDecision(
            retry=True,
            next_attempt=2,
            action="reuse",
            reason="one proved pre-model infrastructure retry is permitted",
        )
    return RetryDecision(
        retry=False,
        next_attempt=None,
        action="abort",
        reason="outcome is not eligible for retry",
    )


def _fingerprints_are_complete(fingerprints: InputFingerprints) -> bool:
    return (
        type(fingerprints) is InputFingerprints
        and type(fingerprints.schema_version) is int
        and fingerprints.schema_version == 1
        and type(fingerprints.run_kind) is str
        and fingerprints.run_kind in ("diagnostic", "discovery", "formal")
        and _is_sha256(fingerprints.epoch_id)
        and all(
            _is_sha256(value)
            for value in (
                fingerprints.archive_sha256,
                fingerprints.marketplace_sha256,
                fingerprints.evaluator_sha256,
                fingerprints.transport_config_sha256,
                fingerprints.forward_manifest_sha256,
                fingerprints.lifecycle_manifest_sha256,
            )
        )
    )


def _capture_exact_manifest_value(
    value: object,
    *,
    label: str,
    active: set[int],
) -> object:
    if value is None or type(value) in (str, int, bool):
        return value
    if type(value) is list:
        identity = id(value)
        if identity in active:
            raise ValueError(f"{label} contains a cycle")
        active.add(identity)
        try:
            return [
                _capture_exact_manifest_value(
                    item,
                    label=f"{label} item",
                    active=active,
                )
                for item in tuple(value)
            ]
        finally:
            active.remove(identity)
    if type(value) is dict:
        identity = id(value)
        if identity in active:
            raise ValueError(f"{label} contains a cycle")
        active.add(identity)
        try:
            items = tuple(value.items())
            if any(type(key) is not str for key, _item in items):
                raise ValueError(f"{label} keys must be exact strings")
            return {
                key: _capture_exact_manifest_value(
                    item,
                    label=f"{label}.{key}",
                    active=active,
                )
                for key, item in items
            }
        finally:
            active.remove(identity)
    raise ValueError(f"{label} contains a custom or unsupported value")


def _capture_resume_manifest_snapshot(
    manifests: dict[EvalMode, list[dict[str, object]]],
) -> tuple[bytes, bytes]:
    items = tuple(manifests.items())
    if (
        any(type(key) is not str for key, _rows in items)
        or {key for key, _rows in items} != {"forward", "lifecycle"}
        or len(items) != 2
    ):
        raise ValueError(
            "resume manifests must contain forward and lifecycle"
        )
    rows_by_mode = dict(items)
    captured: list[bytes] = []
    for mode in ("forward", "lifecycle"):
        rows = rows_by_mode[mode]
        if type(rows) is not list:
            raise ValueError("resume manifest values must be exact lists")
        frozen_rows = _capture_exact_manifest_value(
            rows,
            label=f"{mode} manifest",
            active=set(),
        )
        if type(frozen_rows) is not list or any(
            type(row) is not dict for row in frozen_rows
        ):
            raise ValueError("resume manifest rows must be exact dicts")
        captured.append(
            json.dumps(
                frozen_rows,
                ensure_ascii=True,
                indent=2,
                allow_nan=False,
            ).encode("ascii")
            + b"\n"
        )
    return captured[0], captured[1]


def _decode_resume_manifest_snapshot(
    snapshot: tuple[bytes, bytes],
) -> dict[EvalMode, list[dict[str, object]]]:
    forward = json.loads(snapshot[0])
    lifecycle = json.loads(snapshot[1])
    if type(forward) is not list or type(lifecycle) is not list:
        raise AssertionError("resume manifest snapshot decoded incorrectly")
    return {"forward": forward, "lifecycle": lifecycle}


def _resume_all_invalid(plan: EpochPlan) -> ResumePlan:
    return ResumePlan(
        run_kind=plan.run_kind,
        reusable=(),
        pending=(),
        invalid=tuple(assignment.key for assignment in plan.assignments),
    )


def _resume_all_pending(plan: EpochPlan) -> ResumePlan:
    return ResumePlan(
        run_kind=plan.run_kind,
        reusable=(),
        pending=tuple(assignment.key for assignment in plan.assignments),
        invalid=(),
    )


def _resume_run_root_has_cases(run_root: Path) -> bool:
    directory = _open_anchored_record_directory(
        anchor_path=run_root,
        base_components=(),
        record_components=(),
        create=False,
        label="resume run root",
    )
    with directory:
        inventory = directory.inventory()
        if "cases" not in inventory:
            return False
    cases = _open_anchored_record_directory(
        anchor_path=run_root,
        base_components=(),
        record_components=("cases",),
        create=False,
        label="resume cases root",
    )
    with cases:
        cases.inventory()
    return True


def _append_resume_disposition(
    *,
    disposition: Literal["reusable", "pending", "invalid"],
    key: CaseKey,
    reusable: list[CaseKey],
    pending: list[CaseKey],
    invalid: list[CaseKey],
) -> None:
    if disposition == "reusable":
        reusable.append(key)
    elif disposition == "pending":
        pending.append(key)
    elif disposition == "invalid":
        invalid.append(key)
    else:
        raise AssertionError("resume disposition is invalid")


def _classify_resume_case_retained(
    *,
    plan: EpochPlan,
    paths: CasePaths,
    assignment: CaseAssignment,
    manifest_case: dict[str, object],
    reusable: list[CaseKey],
    pending: list[CaseKey],
    invalid: list[CaseKey],
) -> None:
    manifest_case_sha256 = _validate_seal_context(
        plan=plan,
        paths=paths,
        assignment=assignment,
        manifest_case=manifest_case,
    )
    try:
        paths.root.lstat()
    except FileNotFoundError:
        pending.append(assignment.key)
        return
    except OSError:
        raise ValueError("case root is unavailable") from None

    with ExitStack() as retained:
        case_directory = retained.enter_context(
            _open_case_record_directory(
                paths=paths,
                components=(),
                create=False,
                label="resume case directory",
            )
        )
        case_inventory = case_directory.inventory()
        has_attempts = "attempts" in case_inventory
        has_seal = "sealed" in case_inventory
        if not has_attempts:
            disposition: Literal["reusable", "pending", "invalid"] = (
                "invalid" if has_seal else "pending"
            )
            if case_directory.inventory() != case_inventory:
                raise RuntimeError(
                    "case inventory changed before resume disposition"
                )
            _append_resume_disposition(
                disposition=disposition,
                key=assignment.key,
                reusable=reusable,
                pending=pending,
                invalid=invalid,
            )
            return

        attempts_directory = retained.enter_context(
            _open_record_child_directory(
                case_directory,
                "attempts",
                label="resume attempt root",
            )
        )
        attempt_inventory = attempts_directory.inventory()
        if attempt_inventory not in ((), ("01",), ("01", "02")):
            if "01" not in attempt_inventory:
                raise ValueError("attempt sequence contains a gap")
            raise ValueError("attempt directory name or sequence is invalid")
        if not attempt_inventory:
            disposition = "invalid" if has_seal else "pending"
            if attempts_directory.inventory() != attempt_inventory:
                raise RuntimeError(
                    "attempt inventory changed before resume disposition"
                )
            if case_directory.inventory() != case_inventory:
                raise RuntimeError(
                    "case inventory changed before resume disposition"
                )
            _append_resume_disposition(
                disposition=disposition,
                key=assignment.key,
                reusable=reusable,
                pending=pending,
                invalid=invalid,
            )
            return

        attempt_directories = tuple(
            retained.enter_context(
                _open_record_child_directory(
                    attempts_directory,
                    name,
                    label="resume attempt directory",
                )
            )
            for name in attempt_inventory
        )
        if attempts_directory.inventory() != attempt_inventory:
            raise RuntimeError(
                "attempt inventory changed while retaining resume hierarchy"
            )
        seals = tuple(
            _read_attempt_seal_retained(
                directory=directory,
                plan=plan,
                assignment=assignment,
                attempt=attempt_number,
                manifest_case_sha256=manifest_case_sha256,
            )
            for attempt_number, directory in enumerate(
                attempt_directories, start=1
            )
        )

        receipt: VerifiedTombstoneReceipt | None = None
        cleanup_directory: _RecordChildDirectoryCapability | None = None
        cleanup_inventory: tuple[str, ...] | None = None
        if any(
            seal.terminal.get("cleanup_passed") is True for seal in seals
        ):
            if "cleanup" not in case_inventory:
                raise ValueError("clean attempt has no cleanup directory")
            cleanup_directory = retained.enter_context(
                _open_record_child_directory(
                    case_directory,
                    "cleanup",
                    label="resume cleanup directory",
                )
            )
            cleanup_inventory = cleanup_directory.inventory()
            receipt = _read_verified_tombstone_receipt_retained(
                directory=cleanup_directory,
                plan=plan,
                assignment=assignment,
            )
            if receipt.receipt.canonical_binding != "expected":
                raise ValueError(
                    "attempt cleanup did not preserve canonical auth binding"
                )
            for seal in seals:
                if (
                    seal.terminal.get("cleanup_passed") is True
                    and seal.terminal.get("tombstone_receipt_sha256")
                    != receipt.sha256
                ):
                    raise ValueError("attempt cleanup receipt hash differs")

        sealed_directory: _RecordChildDirectoryCapability | None = None
        sealed_inventory: tuple[str, ...] | None = None
        if has_seal:
            sealed_directory = retained.enter_context(
                _open_record_child_directory(
                    case_directory,
                    "sealed",
                    label="resume case seal directory",
                )
            )
            sealed_inventory = sealed_directory.inventory()

        if len(seals) == 2:
            first = seals[0].terminal
            first_decision = decide_retry(
                classification=first["classification"],
                attempt=1,
                model_started=first["model_started"],
                cleanup_passed=first["cleanup_passed"],
                fingerprints_unchanged=True,
            )
            if (
                first.get("status") != "failed"
                or not first_decision.retry
                or first_decision.next_attempt != 2
                or first_decision.action != "reuse"
            ):
                disposition = "invalid"
            else:
                disposition = "pending"
        else:
            disposition = "pending"

        final_terminal = seals[-1].terminal
        final_attempt = len(seals)
        final_decision = decide_retry(
            classification=final_terminal["classification"],
            attempt=final_attempt,
            model_started=final_terminal["model_started"],
            cleanup_passed=final_terminal["cleanup_passed"],
            fingerprints_unchanged=True,
        )
        case_seal: CaseSeal | None = None
        if disposition != "invalid":
            if final_terminal.get("status") != "success":
                if (
                    len(seals) == 1
                    and final_decision.retry
                    and final_decision.next_attempt == 2
                    and final_decision.action == "reuse"
                    and not has_seal
                ):
                    disposition = "pending"
                else:
                    disposition = "invalid"
            elif (
                final_decision.action != "reuse"
                or sum(
                    seal.terminal.get("model_started") is True
                    for seal in seals
                )
                != 1
                or sealed_directory is None
            ):
                disposition = "invalid"
            else:
                case_seal = _read_case_seal_retained(
                    directory=sealed_directory,
                    plan=plan,
                    paths=paths,
                    assignment=assignment,
                    manifest_case=manifest_case,
                    manifest_case_sha256=manifest_case_sha256,
                )
                if (
                    case_seal.commit.get("status") != "success"
                    or case_seal.commit.get("attempt") != final_attempt
                ):
                    disposition = "invalid"
                else:
                    disposition = "reusable"

        durable_seals = tuple(
            _read_attempt_seal_retained(
                directory=directory,
                plan=plan,
                assignment=assignment,
                attempt=attempt_number,
                manifest_case_sha256=manifest_case_sha256,
            )
            for attempt_number, directory in enumerate(
                attempt_directories, start=1
            )
        )
        if durable_seals != seals:
            raise RuntimeError("attempt records changed before disposition")
        if receipt is not None:
            if cleanup_directory is None or cleanup_inventory is None:
                raise AssertionError("retained cleanup proof is incomplete")
            durable_receipt = _read_verified_tombstone_receipt_retained(
                directory=cleanup_directory,
                plan=plan,
                assignment=assignment,
            )
            if (
                durable_receipt != receipt
                or durable_receipt.receipt.canonical_binding != "expected"
                or cleanup_directory.inventory() != cleanup_inventory
            ):
                raise RuntimeError(
                    "cleanup proof changed before resume disposition"
                )
        if sealed_directory is not None:
            if sealed_inventory is None:
                raise AssertionError("retained case seal inventory is missing")
            if case_seal is not None:
                durable_case_seal = _read_case_seal_retained(
                    directory=sealed_directory,
                    plan=plan,
                    paths=paths,
                    assignment=assignment,
                    manifest_case=manifest_case,
                    manifest_case_sha256=manifest_case_sha256,
                )
                if durable_case_seal != case_seal:
                    raise RuntimeError(
                        "case seal changed before resume disposition"
                    )
            if sealed_directory.inventory() != sealed_inventory:
                raise RuntimeError(
                    "case seal inventory changed before resume disposition"
                )
        for directory in attempt_directories:
            if directory.inventory() != ("start.json", "terminal.json"):
                raise RuntimeError(
                    "attempt inventory changed before resume disposition"
                )
        if attempts_directory.inventory() != attempt_inventory:
            raise RuntimeError(
                "attempt inventory changed before resume disposition"
            )
        if case_directory.inventory() != case_inventory:
            raise RuntimeError(
                "case inventory changed before resume disposition"
            )
        _append_resume_disposition(
            disposition=disposition,
            key=assignment.key,
            reusable=reusable,
            pending=pending,
            invalid=invalid,
        )


def plan_resume(
    *,
    plan: EpochPlan,
    run_root: Path,
    current_fingerprints: InputFingerprints,
    manifests: dict[EvalMode, list[dict[str, object]]],
) -> ResumePlan:
    if type(plan) is not EpochPlan:
        raise TypeError("plan must be an exact EpochPlan")
    if type(current_fingerprints) is not InputFingerprints:
        raise TypeError(
            "current_fingerprints must be exact InputFingerprints"
        )
    if type(run_root) is not type(Path(".")):
        raise TypeError("run_root must be an exact Path")
    if type(manifests) is not dict:
        raise TypeError("manifests must be an exact dict")
    try:
        manifest_snapshot = _capture_resume_manifest_snapshot(manifests)
    except (TypeError, ValueError, OverflowError, RecursionError):
        manifest_snapshot = None
    canonical_root = canonical_run_root(run_root)
    if type(plan.assignments) is not tuple or any(
        type(assignment) is not CaseAssignment
        for assignment in plan.assignments
    ):
        raise ValueError("plan assignments must be an exact tuple")
    if not plan.assignments:
        raise ValueError("plan assignments must not be empty")
    if manifest_snapshot is None:
        return _resume_all_invalid(plan)
    if (
        type(plan.schema_version) is not int
        or plan.schema_version != 1
        or type(plan.epoch_id) is not str
        or not _is_sha256(plan.epoch_id)
        or type(plan.run_kind) is not str
        or plan.run_kind not in ("diagnostic", "discovery", "formal")
        or not _fingerprints_are_complete(plan.fingerprints)
        or not _fingerprints_are_complete(current_fingerprints)
        or current_fingerprints != plan.fingerprints
        or plan.fingerprints.epoch_id != plan.epoch_id
        or plan.fingerprints.run_kind != plan.run_kind
        or hashlib.sha256(manifest_snapshot[0]).hexdigest()
        != current_fingerprints.forward_manifest_sha256
        or hashlib.sha256(manifest_snapshot[1]).hexdigest()
        != current_fingerprints.lifecycle_manifest_sha256
    ):
        return _resume_all_invalid(plan)
    try:
        validation_manifests = _decode_resume_manifest_snapshot(
            manifest_snapshot
        )
        rebuilt = build_epoch_plan(
            run_kind=plan.run_kind,
            manifests=validation_manifests,
            fingerprints=current_fingerprints,
        )
    except (TypeError, ValueError):
        return _resume_all_invalid(plan)
    if rebuilt != plan:
        return _resume_all_invalid(plan)
    try:
        if not _resume_run_root_has_cases(canonical_root):
            return _resume_all_pending(plan)
    except (OSError, TypeError, ValueError, RuntimeError) as error:
        if is_indeterminate_descriptor_close(error):
            raise
        _require_lease_process_healthy()
        return _resume_all_invalid(plan)

    frozen_manifests = _decode_resume_manifest_snapshot(manifest_snapshot)
    reusable: list[CaseKey] = []
    pending: list[CaseKey] = []
    invalid: list[CaseKey] = []
    for assignment in plan.assignments:
        manifest_rows = frozen_manifests[assignment.key.mode]
        index = assignment.key.ordinal - 1
        if index < 0 or index >= len(manifest_rows):
            invalid.append(assignment.key)
            continue
        manifest_case = manifest_rows[index]
        if (
            type(manifest_case) is not dict
            or manifest_case.get("id") != assignment.key.case_id
        ):
            invalid.append(assignment.key)
            continue
        paths = paths_for_case(canonical_root, assignment)
        try:
            _classify_resume_case_retained(
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
                reusable=reusable,
                pending=pending,
                invalid=invalid,
            )
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            if is_indeterminate_descriptor_close(error):
                raise
            _require_lease_process_healthy()
            invalid.append(assignment.key)
    return ResumePlan(
        run_kind=plan.run_kind,
        reusable=tuple(reusable),
        pending=tuple(pending),
        invalid=tuple(invalid),
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
_ACK_DECISIONS = {"continue", "retry", "stop-launches", "abort"}
_PROTOCOL_IDENTITY_FIELDS = frozenset(
    {"schema_version", "epoch_id", "run_kind"}
)
_PROTOCOL_TEMP_PATTERN = re.compile(
    r"^\.[0-9]{6}\.json\.tmp-[0-9]+-[0-9a-f]{32}$"
)
_PROTOCOL_PENDING_PATTERN = re.compile(
    r"^\.([0-9]{6})\.json\.pending$"
)
_PROTOCOL_IDENTITY_TEMP_PATTERN = re.compile(
    r"^\.protocol-identity\.json\.tmp-[0-9]+-[0-9a-f]{32}$"
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
        hierarchy_metadata = (
            worker_root.parent.lstat() if lane != "APP" else None
        )
        resolved_worker = worker_root.resolve(strict=True)
        resolved_run = run_root.resolve(strict=True)
        resolved_hierarchy = (
            worker_root.parent.resolve(strict=True) if lane != "APP" else None
        )
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
    if hierarchy_metadata is not None:
        _validate_owned_entry(
            hierarchy_metadata,
            label="progress workers directory",
            kind="directory",
            mode=0o700,
        )
    if (
        resolved_worker != worker_root
        or resolved_run != run_root
        or (
            lane != "APP"
            and resolved_hierarchy != worker_root.parent
        )
    ):
        raise ValueError("progress worker root must be canonical")
    return worker_root, run_root


def _open_protocol_worker_directory(
    worker_root: Path,
    lane: LaneName,
) -> _RecordDirectoryCapability:
    bound_worker, run_root = _protocol_worker_context(worker_root, lane)
    try:
        worker_components = bound_worker.relative_to(run_root).parts
    except ValueError:
        raise ValueError("progress worker root escapes its run root") from None
    if not worker_components:
        raise ValueError("progress worker root does not name a worker")
    return _open_anchored_record_directory(
        anchor_path=run_root,
        base_components=worker_components,
        record_components=(),
        create=False,
        label="progress worker directory",
    )


def _open_protocol_record_directory(
    worker: _RecordDirectoryCapability,
    name: str,
    *,
    label: str,
    create: bool = True,
) -> _RecordChildDirectoryCapability:
    worker._validate_live()
    retained = _open_relative_directory_chain_at(
        worker._retained[-1].slot.descriptor,
        (name,),
        label=label,
        create=create,
        required_mode=0o700,
    )
    capability = _RecordChildDirectoryCapability(
        parent=worker,
        retained=retained,
        label=label,
    )
    try:
        capability._validate_live()
    except BaseException as primary:
        capability.close(primary)
        raise AssertionError("protocol record acquisition produced no error")
    return capability


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
    attempt_seal = read_attempt_seal(
        plan=plan,
        paths=paths,
        assignment=assignment,
        attempt=message.attempt,
        manifest_case=manifest_case,
    )
    attempts = _open_case_record_directory(
        paths=paths,
        components=("attempts",),
        create=False,
        label="progress attempt root",
    )
    with attempts:
        inventory = attempts.inventory()
        if inventory not in (("01",), ("01", "02")):
            raise ValueError("progress attempt inventory is invalid")
        if inventory == ("01", "02"):
            second = _open_record_child_directory(
                attempts,
                "02",
                label="progress attempt two",
            )
            with second:
                second_inventory = second.inventory()
                retry = decide_retry(
                    classification=attempt_seal.terminal["classification"],
                    attempt=attempt_seal.terminal["attempt"],
                    model_started=attempt_seal.terminal["model_started"],
                    cleanup_passed=attempt_seal.terminal["cleanup_passed"],
                    fingerprints_unchanged=True,
                )
                retry_history = (
                    message.attempt == 1
                    and message.status == "failed"
                    and retry.retry
                )
                if (
                    not retry_history
                    and second_inventory != ("start.json", "terminal.json")
                ):
                    raise ValueError(
                        "progress attempt two inventory is invalid"
                    )
            if attempts.inventory() != inventory:
                raise RuntimeError(
                    "progress attempt inventory changed while reading"
                )
    terminal = attempt_seal.terminal
    if (
        attempt_seal.terminal_sha256
        != message.attempt_terminal_sha256
        or terminal.get("attempt") != message.attempt
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
    retry = decide_retry(
        classification=attempt_seal.terminal["classification"],
        attempt=attempt_seal.terminal["attempt"],
        model_started=attempt_seal.terminal["model_started"],
        cleanup_passed=attempt_seal.terminal["cleanup_passed"],
        fingerprints_unchanged=True,
    )
    retry_backup = paths.root / "cleanup-attempt-1"
    if (
        message.case_commit_sha256 is None
        and message.status == "failed"
        and message.attempt == 1
        and retry.retry
        and _entry_exists_no_follow(
            retry_backup, "retry cleanup proof"
        )
    ):
        return
    record = _read_optional_protocol_record(
        paths.sealed / "case-commit.json",
        "progress case commit",
        byte_cap=MAX_CASE_COMMIT_BYTES,
    )
    if message.case_commit_sha256 is None:
        if record is not None:
            later = read_case_seal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
            )
            if not (
                message.status == "failed"
                and message.attempt == 1
                and retry.retry
                and later.commit.get("attempt") == 2
                and later.commit.get("status") == "success"
            ):
                raise ValueError(
                    "case-terminal progress omitted a durable case commit"
                )
            return
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


def _read_retry_tombstone_receipt(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
) -> VerifiedTombstoneReceipt:
    case = _open_case_record_directory(
        paths=paths,
        components=(),
        create=False,
        label="retry case directory",
    )
    with case:
        backup = _open_record_child_directory(
            case,
            "cleanup-attempt-1",
            label="retry cleanup proof",
        )
        with backup:
            return _read_verified_tombstone_receipt_retained(
                directory=backup,
                plan=plan,
                assignment=assignment,
            )


def _validate_progress_tombstone(
    *,
    message: ProgressMessage,
    terminal: dict[str, object],
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
) -> None:
    retry_backup = paths.root / "cleanup-attempt-1"
    try:
        retry_backup.lstat()
    except FileNotFoundError:
        verified = _read_optional_verified_tombstone_receipt(
            plan=plan,
            assignment=assignment,
            paths=paths,
        )
    except OSError:
        raise ValueError("retry cleanup proof is unavailable") from None
    else:
        verified = (
            _read_retry_tombstone_receipt(
                plan=plan,
                assignment=assignment,
                paths=paths,
            )
            if message.attempt == 1
            else _read_optional_verified_tombstone_receipt(
                plan=plan,
                assignment=assignment,
                paths=paths,
            )
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


def _retry_decision_for_progress(
    *,
    worker_root: Path,
    message: ProgressMessage,
) -> RetryDecision | None:
    if message.type != "case-terminal" or message.status != "failed":
        return None
    _validate_progress_durable(worker_root, message)
    _, run_root = _protocol_worker_context(worker_root, message.lane)
    plan, manifests = _resolve_progress_epoch_context(message)
    assignment, manifest_case, paths = _resolve_progress_case_context(
        run_root=run_root,
        message=message,
        plan=plan,
        manifests=manifests,
    )
    attempt_seal, _ = _read_progress_attempt(
        message=message,
        plan=plan,
        assignment=assignment,
        manifest_case=manifest_case,
        paths=paths,
    )
    terminal = attempt_seal.terminal
    return decide_retry(
        classification=terminal["classification"],
        attempt=terminal["attempt"],
        model_started=terminal["model_started"],
        cleanup_passed=terminal["cleanup_passed"],
        fingerprints_unchanged=True,
    )


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


def _read_optional_protocol_record_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    label: str,
    *,
    byte_cap: int,
) -> tuple[dict[str, object], bytes] | None:
    directory._validate_live()
    descriptor = directory._retained[-1].slot.descriptor
    try:
        metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
    except FileNotFoundError:
        directory._validate_live()
        return None
    except OSError:
        raise ValueError(f"{label} is unavailable") from None
    _validate_owned_entry(
        metadata,
        label=label,
        kind="file",
        mode=0o600,
    )
    return _read_canonical_record_retained(
        directory,
        name,
        label,
        byte_cap=byte_cap,
    )


def _protocol_pending_marker_name(name: str) -> str:
    if re.fullmatch(r"[0-9]{6}\.json", name) is None or name.startswith("000000"):
        raise ValueError("protocol sequence record name is invalid")
    return f".{name}.pending"


def _protocol_pending_marker_exists_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    *,
    label: str,
) -> bool:
    marker_name = _protocol_pending_marker_name(name)
    directory._validate_live()
    descriptor = directory._retained[-1].slot.descriptor
    try:
        metadata = os.stat(
            marker_name,
            dir_fd=descriptor,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        directory._validate_live()
        return False
    except OSError:
        raise ValueError(f"{label} pending marker is unavailable") from None
    _validate_owned_entry(
        metadata,
        label=f"{label} pending marker",
        kind="file",
        mode=0o600,
    )
    if metadata.st_size != 0:
        raise ValueError(f"{label} pending marker is unsafe")
    directory._validate_live()
    return True


def _require_protocol_record_committed_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    *,
    label: str,
) -> None:
    if _protocol_pending_marker_exists_retained(
        directory,
        name,
        label=label,
    ):
        raise ValueError(f"{label} publication is pending")


def _install_protocol_pending_marker_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    *,
    label: str,
    allow_existing: bool,
) -> None:
    marker_name = _protocol_pending_marker_name(name)
    directory._validate_live()
    parent_slot = directory._retained[-1].slot
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= _required_os_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0)
    try:
        marker_descriptor = os.open(
            marker_name,
            flags,
            0o600,
            dir_fd=parent_slot.descriptor,
        )
    except FileExistsError:
        if allow_existing and _protocol_pending_marker_exists_retained(
            directory,
            name,
            label=label,
        ):
            os.fsync(parent_slot.descriptor)
            directory._validate_live()
            if _protocol_pending_marker_exists_retained(
                directory,
                name,
                label=label,
            ):
                return
        raise ValueError(f"{label} publication is pending") from None

    marker_slot = _DescriptorSlot(marker_descriptor)
    primary: BaseException | None = None
    try:
        os.fchmod(marker_slot.descriptor, 0o600)
        os.fsync(marker_slot.descriptor)
    except BaseException as error:
        primary = error
    close_error = _retire_descriptor_capability(marker_slot)
    _raise_ordered_failures(
        f"{label} pending marker write or close failed",
        primary,
        [close_error] if close_error is not None else [],
    )
    os.fsync(parent_slot.descriptor)
    directory._validate_live()


def _remove_protocol_pending_marker_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    *,
    label: str,
) -> None:
    marker_name = _protocol_pending_marker_name(name)
    directory._validate_live()
    parent_slot = directory._retained[-1].slot
    try:
        os.unlink(marker_name, dir_fd=parent_slot.descriptor)
        os.fsync(parent_slot.descriptor)
        directory._validate_live()
        return
    except BaseException as primary:
        recovery_errors: list[BaseException] = []
        try:
            if not _protocol_pending_marker_exists_retained(
                directory,
                name,
                label=label,
            ):
                _install_protocol_pending_marker_retained(
                    directory,
                    name,
                    label=label,
                    allow_existing=True,
                )
        except BaseException as recovery_error:
            recovery_errors.append(recovery_error)
        _raise_ordered_failures(
            f"{label} commit-marker cleanup failed",
            primary,
            recovery_errors,
        )


def _write_protocol_record_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    payload: Mapping[str, Any],
    *,
    byte_cap: int,
) -> bytes:
    content = canonical_config_bytes(payload)
    if len(content) > byte_cap:
        raise ValueError(f"{name} exceeds its byte cap")
    existing = _read_optional_protocol_record_retained(
        directory,
        name,
        name,
        byte_cap=byte_cap,
    )
    if existing is not None:
        durable, durable_content = existing
        if durable != payload or durable_content != content:
            raise ValueError(f"{name} already differs")
        return durable_content
    try:
        return _publish_immutable_json_retained(
            directory,
            name,
            payload,
            byte_cap=byte_cap,
        )
    except ValueError as error:
        durable = _read_optional_protocol_record_retained(
            directory,
            name,
            name,
            byte_cap=byte_cap,
        )
        if durable is not None and durable == (dict(payload), content):
            return durable[1]
        raise error


@dataclass
class _ProtocolPublicationState:
    commit_boundary_entered: bool = False


def _write_durable_protocol_record_retained(
    directory: _RecordDirectoryCapability | _RecordChildDirectoryCapability,
    name: str,
    payload: Mapping[str, Any],
    *,
    label: str,
    byte_cap: int,
    publication_state: _ProtocolPublicationState,
) -> bytes:
    content = canonical_config_bytes(payload)
    if len(content) > byte_cap:
        raise ValueError(f"{name} exceeds its byte cap")
    _require_protocol_record_committed_retained(
        directory,
        name,
        label=label,
    )
    existing = _read_optional_protocol_record_retained(
        directory,
        name,
        name,
        byte_cap=byte_cap,
    )
    if existing is not None:
        durable, durable_content = existing
        if durable != payload or durable_content != content:
            raise ValueError(f"{name} already differs")
        return durable_content

    _install_protocol_pending_marker_retained(
        directory,
        name,
        label=label,
        allow_existing=False,
    )
    durable_content = _write_protocol_record_retained(
        directory,
        name,
        payload,
        byte_cap=byte_cap,
    )
    publication_state.commit_boundary_entered = True
    _remove_protocol_pending_marker_retained(
        directory,
        name,
        label=label,
    )
    return durable_content


def _restore_protocol_pending_after_failure_retained(
    worker: _RecordDirectoryCapability,
    *,
    directory_name: str,
    directory_label: str,
    record_name: str,
    record_label: str,
) -> tuple[BaseException, ...]:
    recovery_errors: list[BaseException] = []
    try:
        directory = _open_protocol_record_directory(
            worker,
            directory_name,
            label=directory_label,
            create=False,
        )
        with directory:
            _install_protocol_pending_marker_retained(
                directory,
                record_name,
                label=record_label,
                allow_existing=True,
            )
    except BaseException as recovery_error:
        recovery_errors.append(recovery_error)
    return tuple(recovery_errors)


def _protocol_identity_inventory_retained(
    worker: _RecordDirectoryCapability,
    *,
    publishing: bool,
) -> bool:
    worker._validate_live()
    descriptor = worker._retained[-1].slot.descriptor
    before = os.fstat(descriptor)
    crash_temp_count = 0
    final_present = False
    with os.scandir(descriptor) as entries:
        for entry in entries:
            name = entry.name
            if _PROTOCOL_IDENTITY_TEMP_PATTERN.fullmatch(name):
                crash_temp_count += 1
                if crash_temp_count > MAX_PROTOCOL_IDENTITY_CRASH_TEMPS:
                    raise ValueError(
                        "worker protocol identity crash temporary inventory "
                        "exceeds its cap"
                    )
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except OSError:
                    raise ValueError(
                        "worker protocol identity crash temporary is unavailable"
                    ) from None
                _validate_owned_entry(
                    metadata,
                    label="worker protocol identity crash temporary",
                    kind="file",
                    mode=0o600,
                )
                continue
            if (
                name.startswith(".protocol-identity")
                or "protocol-identity.json.tmp-" in name
            ):
                raise ValueError(
                    "worker protocol identity crash temporary is unsafe"
                )
            if name == "protocol-identity.json":
                if final_present:
                    raise AssertionError("worker protocol identity is duplicated")
                try:
                    metadata = entry.stat(follow_symlinks=False)
                except OSError:
                    raise ValueError(
                        "worker protocol identity is unavailable"
                    ) from None
                _validate_owned_entry(
                    metadata,
                    label="worker protocol identity",
                    kind="file",
                    mode=0o600,
                )
                final_present = True
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
        raise RuntimeError("worker protocol identity inventory changed while scanning")
    if (
        publishing
        and not final_present
        and crash_temp_count >= MAX_PROTOCOL_IDENTITY_CRASH_TEMPS
    ):
        raise ValueError(
            "worker protocol identity crash temporary inventory exceeds its cap"
        )
    worker._validate_live()
    return final_present


def _read_worker_protocol_identity_retained(
    worker: _RecordDirectoryCapability,
    message: ProgressMessage,
) -> None:
    if not _protocol_identity_inventory_retained(worker, publishing=False):
        raise ValueError("worker protocol identity is unavailable")
    payload, _ = _read_canonical_record_retained(
        worker,
        "protocol-identity.json",
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


def _seal_worker_protocol_identity_retained(
    worker: _RecordDirectoryCapability,
    message: ProgressMessage,
) -> None:
    if _protocol_identity_inventory_retained(worker, publishing=False):
        _read_worker_protocol_identity_retained(worker, message)
        return
    _protocol_identity_inventory_retained(worker, publishing=True)
    _write_protocol_record_retained(
        worker,
        "protocol-identity.json",
        _protocol_identity_payload(message),
        byte_cap=MAX_PROGRESS_BYTES,
    )
    _read_worker_protocol_identity_retained(worker, message)


def _bounded_protocol_lock_deadline(
    operation_deadline: float | None,
) -> float:
    now = time.monotonic()
    default_deadline = now + PROTOCOL_WORKER_LOCK_TIMEOUT_SECONDS
    if operation_deadline is None:
        return default_deadline
    if (
        type(operation_deadline) not in (int, float)
        or not math.isfinite(operation_deadline)
    ):
        raise ValueError("protocol operation deadline is invalid")
    return min(default_deadline, float(operation_deadline))


def _lock_protocol_worker(
    worker: _RecordDirectoryCapability,
    *,
    deadline: float,
) -> None:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("protocol worker lock deadline is invalid")
    worker._validate_live()
    descriptor = worker._retained[-1].slot.descriptor
    while True:
        try:
            fcntl.flock(
                descriptor,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
            break
        except (BlockingIOError, InterruptedError):
            remaining = float(deadline) - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "timed out acquiring protocol worker lock"
                ) from None
            time.sleep(min(PROTOCOL_WORKER_LOCK_POLL_SECONDS, remaining))
    worker._validate_live()


def _lock_protocol_worker_shared(
    worker: _RecordDirectoryCapability,
    *,
    deadline: float,
) -> None:
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("protocol worker read-lock deadline is invalid")
    worker._validate_live()
    descriptor = worker._retained[-1].slot.descriptor
    while True:
        try:
            fcntl.flock(
                descriptor,
                fcntl.LOCK_SH | fcntl.LOCK_NB,
            )
            break
        except (BlockingIOError, InterruptedError):
            remaining = float(deadline) - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    "timed out acquiring protocol worker read lock"
                ) from None
            time.sleep(min(PROTOCOL_WORKER_LOCK_POLL_SECONDS, remaining))
    worker._validate_live()


@contextmanager
def _shared_protocol_worker_lock(
    worker: _RecordDirectoryCapability,
    *,
    operation_deadline: float | None,
):
    acquired = False
    try:
        _lock_protocol_worker_shared(
            worker,
            deadline=_bounded_protocol_lock_deadline(operation_deadline),
        )
        acquired = True
        yield
    finally:
        if acquired:
            descriptor = worker._retained[-1].slot.descriptor
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            worker._validate_live()


def _write_progress_with_deadline(
    worker_root: Path,
    message: ProgressMessage,
    *,
    operation_deadline: float | None,
) -> tuple[Path, str]:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    worker = _open_protocol_worker_directory(bound_worker, message.lane)
    path = bound_worker / "progress" / f"{message.seq:06d}.json"
    message_sha256: str | None = None
    publication_state = _ProtocolPublicationState()
    try:
        with worker:
            try:
                _lock_protocol_worker(
                    worker,
                    deadline=_bounded_protocol_lock_deadline(operation_deadline),
                )
                _validate_progress_durable(bound_worker, message)
                worker._validate_live()
                progress = _open_protocol_record_directory(
                    worker,
                    "progress",
                    label="progress directory",
                )
                with progress:
                    _protocol_sequence_inventory_retained(
                        progress,
                        label="progress",
                        deadline=None,
                        publishing_seq=message.seq,
                    )
                    _seal_worker_protocol_identity_retained(worker, message)
                    progress_payload = _encode_progress_message(message)
                    message_sha256 = hashlib.sha256(
                        canonical_config_bytes(progress_payload)
                    ).hexdigest()
                    content = _write_durable_protocol_record_retained(
                        progress,
                        path.name,
                        progress_payload,
                        label="progress",
                        byte_cap=MAX_PROGRESS_BYTES,
                        publication_state=publication_state,
                    )
                    if hashlib.sha256(content).hexdigest() != message_sha256:
                        raise AssertionError(
                            "progress publication digest changed"
                        )
                    progress._validate_live()
                worker._validate_live()
            except BaseException as error:
                if not publication_state.commit_boundary_entered:
                    raise
                if (
                    message_sha256 is not None
                    and is_indeterminate_descriptor_close(error)
                ):
                    return path, message_sha256
                recovery_errors = (
                    _restore_protocol_pending_after_failure_retained(
                        worker,
                        directory_name="progress",
                        directory_label="progress directory",
                        record_name=path.name,
                        record_label="progress",
                    )
                )
                if recovery_errors:
                    if message_sha256 is None:
                        raise BaseExceptionGroup(
                            "progress post-commit failure and pending-marker "
                            "restoration failed",
                            [error, *recovery_errors],
                        )
                    return path, message_sha256
                publication_state.commit_boundary_entered = False
                raise error
            if message_sha256 is None:
                raise AssertionError(
                    "progress publication omitted its durable digest"
                )
        return path, message_sha256
    except BaseException:
        if (
            publication_state.commit_boundary_entered
            and message_sha256 is not None
        ):
            return path, message_sha256
        raise


def write_progress(worker_root: Path, message: ProgressMessage) -> Path:
    path, _ = _write_progress_with_deadline(
        worker_root,
        message,
        operation_deadline=None,
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
    worker = _open_protocol_worker_directory(worker_root, expected_lane)
    with worker:
        progress = _open_protocol_record_directory(
            worker,
            "progress",
            label="progress directory",
            create=False,
        )
        with progress:
            message = _read_progress_retained(
                worker,
                progress,
                expected_lane=expected_lane,
                expected_seq=expected_seq,
            )
        worker._validate_live()
        return message


def _read_progress_retained(
    worker: _RecordDirectoryCapability,
    progress: _RecordChildDirectoryCapability,
    *,
    expected_lane: LaneName,
    expected_seq: int,
    lock_deadline: float | None = None,
) -> ProgressMessage:
    with _shared_protocol_worker_lock(
        worker,
        operation_deadline=lock_deadline,
    ):
        return _read_progress_with_worker_lock_retained(
            worker,
            progress,
            expected_lane=expected_lane,
            expected_seq=expected_seq,
        )


def _read_progress_with_worker_lock_retained(
    worker: _RecordDirectoryCapability,
    progress: _RecordChildDirectoryCapability,
    *,
    expected_lane: LaneName,
    expected_seq: int,
) -> ProgressMessage:
    sequences = _protocol_sequence_inventory_retained(
        progress,
        label="progress",
        deadline=None,
    )
    _require_protocol_complete_prefix(sequences, label="progress")
    if expected_seq not in sequences:
        raise ValueError("progress records are gapped or reordered")
    message: ProgressMessage | None = None
    for sequence in sequences:
        durable = _read_single_progress_with_worker_lock_retained(
            worker,
            progress,
            expected_lane=expected_lane,
            expected_seq=sequence,
        )
        if sequence == expected_seq:
            message = durable
            break
    if message is None:
        raise AssertionError("progress prefix omitted its expected sequence")
    if (
        _protocol_sequence_inventory_retained(
            progress,
            label="progress",
            deadline=None,
        )
        != sequences
    ):
        raise RuntimeError("progress inventory changed while reading")
    return message


def _read_single_progress_with_worker_lock_retained(
    worker: _RecordDirectoryCapability,
    progress: _RecordChildDirectoryCapability,
    *,
    expected_lane: LaneName,
    expected_seq: int,
) -> ProgressMessage:
    expected_name = f"{expected_seq:06d}.json"
    _require_protocol_record_committed_retained(
        progress,
        expected_name,
        label="progress",
    )
    payload, _ = _read_canonical_record_retained(
        progress,
        expected_name,
        "progress",
        byte_cap=MAX_PROGRESS_BYTES,
    )
    message = _decode_progress_message(payload)
    if message.lane != expected_lane or message.seq != expected_seq:
        raise ValueError("progress record identity differs")
    _read_worker_protocol_identity_retained(worker, message)
    _validate_progress_durable(worker.path, message)
    _require_protocol_record_committed_retained(
        progress,
        expected_name,
        label="progress",
    )
    progress._validate_live()
    return message


def _validate_wait_timeout(timeout: float) -> float:
    if (
        type(timeout) not in (int, float)
        or not math.isfinite(timeout)
        or timeout < 0
    ):
        raise ValueError("timeout must be a finite non-negative number")
    return float(timeout)


def _protocol_sequence_inventory_retained(
    directory: _RecordChildDirectoryCapability,
    *,
    label: str,
    deadline: float | None,
    publishing_seq: int | None = None,
) -> tuple[int, ...]:
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError(f"timed out while scanning {label} records")
    directory._validate_live()
    descriptor = directory._retained[-1].slot.descriptor
    before = os.fstat(descriptor)
    sequences: list[int] = []
    crash_temp_count = 0
    pending_sequences: set[int] = set()
    with os.scandir(descriptor) as entries:
        for entry in entries:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"timed out while scanning {label} records")
            name = entry.name
            if _PROTOCOL_TEMP_PATTERN.fullmatch(name):
                crash_temp_count += 1
                if crash_temp_count > MAX_PROTOCOL_CRASH_TEMPS:
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
            pending_match = _PROTOCOL_PENDING_PATTERN.fullmatch(name)
            if pending_match is not None:
                if len(pending_sequences) >= MAX_PROTOCOL_PENDING_MARKERS:
                    raise ValueError(
                        f"{label} pending marker inventory exceeds its cap"
                    )
                try:
                    pending_metadata = entry.stat(follow_symlinks=False)
                except OSError:
                    raise ValueError(
                        f"{label} pending marker is unavailable"
                    ) from None
                _validate_owned_entry(
                    pending_metadata,
                    label=f"{label} pending marker",
                    kind="file",
                    mode=0o600,
                )
                if pending_metadata.st_size != 0:
                    raise ValueError(f"{label} pending marker is unsafe")
                pending_sequence = int(pending_match.group(1))
                if pending_sequence < 1:
                    raise ValueError(f"{label} pending sequence is invalid")
                pending_sequences.add(pending_sequence)
                continue
            if name.startswith(".") or ".tmp-" in name:
                raise ValueError(
                    f"{label} directory contains an unknown crash temporary"
                )
            if re.fullmatch(r"[0-9]{6}\.json", name) is None:
                raise ValueError(f"{label} directory contains an unsafe record")
            if len(sequences) >= MAX_PROTOCOL_RECORDS:
                raise ValueError(f"{label} record inventory exceeds its cap")
            try:
                record_metadata = entry.stat(follow_symlinks=False)
            except OSError:
                raise ValueError(f"{label} record is unavailable") from None
            _validate_owned_entry(
                record_metadata,
                label=f"{label} record",
                kind="file",
                mode=0o600,
            )
            sequence = int(name[:6])
            if sequence < 1:
                raise ValueError(f"{label} sequence is invalid")
            sequences.append(sequence)
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
        raise RuntimeError(f"{label} inventory changed while scanning")
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError(f"timed out while scanning {label} records")
    if len(set(sequences) | pending_sequences) > MAX_PROTOCOL_RECORDS:
        raise ValueError(f"{label} pending marker inventory exceeds its cap")
    if pending_sequences:
        raise ValueError(f"{label} publication is pending")
    sequences.sort()
    if (
        publishing_seq is not None
        and publishing_seq not in sequences
        and len(sequences) >= MAX_PROTOCOL_RECORDS
    ):
        raise ValueError(f"{label} record inventory exceeds its cap")
    if (
        publishing_seq is not None
        and publishing_seq not in sequences
        and crash_temp_count >= MAX_PROTOCOL_CRASH_TEMPS
    ):
        raise ValueError(
            f"{label} crash temporary inventory exceeds its cap"
        )
    directory._validate_live()
    return tuple(sequences)


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
    pending_sequences: set[int] = set()
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
                pending_match = _PROTOCOL_PENDING_PATTERN.fullmatch(name)
                if pending_match is not None:
                    if len(pending_sequences) >= MAX_PROTOCOL_PENDING_MARKERS:
                        raise ValueError(
                            f"{label} pending marker inventory exceeds its cap"
                        )
                    try:
                        pending_metadata = entry.stat(follow_symlinks=False)
                    except OSError:
                        raise ValueError(
                            f"{label} pending marker is unavailable"
                        ) from None
                    _validate_owned_entry(
                        pending_metadata,
                        label=f"{label} pending marker",
                        kind="file",
                        mode=0o600,
                    )
                    if pending_metadata.st_size != 0:
                        raise ValueError(f"{label} pending marker is unsafe")
                    pending_sequence = int(pending_match.group(1))
                    if pending_sequence < 1:
                        raise ValueError(
                            f"{label} pending sequence is invalid"
                        )
                    pending_sequences.add(pending_sequence)
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
    if len(set(sequences) | pending_sequences) > MAX_PROTOCOL_RECORDS:
        raise ValueError(f"{label} pending marker inventory exceeds its cap")
    if pending_sequences:
        raise ValueError(f"{label} publication is pending")
    sequences.sort()
    if (
        publishing_seq is not None
        and publishing_seq not in sequences
        and len(sequences) >= MAX_PROTOCOL_RECORDS
    ):
        raise ValueError(f"{label} record inventory exceeds its cap")
    return tuple(sequences)


def _require_protocol_complete_prefix(
    sequences: tuple[int, ...], *, label: str
) -> None:
    for expected_sequence, sequence in enumerate(sequences, start=1):
        if sequence != expected_sequence:
            raise ValueError(f"{label} records are gapped or reordered")


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
    worker = _open_protocol_worker_directory(bound_worker, expected_lane)
    with worker:
        with ExitStack() as retained:
            progress: _RecordChildDirectoryCapability | None = None
            while True:
                with _shared_protocol_worker_lock(
                    worker,
                    operation_deadline=deadline,
                ):
                    if progress is None:
                        try:
                            progress = retained.enter_context(
                                _open_protocol_record_directory(
                                    worker,
                                    "progress",
                                    label="progress directory",
                                    create=False,
                                )
                            )
                        except FileNotFoundError:
                            worker._validate_live()
                    sequences = (
                        ()
                        if progress is None
                        else _protocol_sequence_inventory_retained(
                            progress,
                            label="progress",
                            deadline=deadline,
                        )
                    )
                    _require_protocol_sequence_prefix(
                        sequences,
                        expected_seq=expected_seq,
                        label="progress",
                    )
                    if expected_seq in sequences:
                        if progress is None:
                            raise AssertionError(
                                "progress inventory exists without a directory"
                            )
                        message = _read_progress_with_worker_lock_retained(
                            worker,
                            progress,
                            expected_lane=expected_lane,
                            expected_seq=expected_seq,
                        )
                        if expected_sha256 is not None:
                            content = canonical_config_bytes(
                                _encode_progress_message(message)
                            )
                            if (
                                hashlib.sha256(content).hexdigest()
                                != expected_sha256
                            ):
                                raise ValueError(
                                    "progress wake-up digest differs"
                                )
                        worker._validate_live()
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


def _durable_progress_hash_retained(
    worker: _RecordDirectoryCapability,
    message: ProgressMessage,
    *,
    worker_lock_held: bool,
    lock_deadline: float | None = None,
) -> str:
    if not worker_lock_held:
        with _shared_protocol_worker_lock(
            worker,
            operation_deadline=lock_deadline,
        ):
            return _durable_progress_hash_retained(
                worker,
                message,
                worker_lock_held=True,
            )
    progress = _open_protocol_record_directory(
        worker,
        "progress",
        label="progress directory",
        create=False,
    )
    with progress:
        durable = _read_progress_with_worker_lock_retained(
            worker,
            progress,
            expected_lane=message.lane,
            expected_seq=message.seq,
        )
    if durable != message:
        raise ValueError("durable progress differs from ACK message")
    content = canonical_config_bytes(_encode_progress_message(durable))
    return hashlib.sha256(content).hexdigest()


def _read_ack_for_progress_retained(
    worker: _RecordDirectoryCapability,
    acks: _RecordChildDirectoryCapability,
    message: ProgressMessage,
    *,
    lock_deadline: float | None = None,
) -> Ack:
    with _shared_protocol_worker_lock(
        worker,
        operation_deadline=lock_deadline,
    ):
        return _read_ack_for_progress_with_worker_lock_retained(
            worker,
            acks,
            message,
        )


def _read_ack_for_progress_with_worker_lock_retained(
    worker: _RecordDirectoryCapability,
    acks: _RecordChildDirectoryCapability,
    message: ProgressMessage,
) -> Ack:
    expected_name = f"{message.seq:06d}.json"
    _require_protocol_record_committed_retained(
        acks,
        expected_name,
        label="ACK",
    )
    expected_hash = _durable_progress_hash_retained(
        worker,
        message,
        worker_lock_held=True,
    )
    worker._validate_live()
    payload, _ = _read_canonical_record_retained(
        acks,
        expected_name,
        "ack",
        byte_cap=MAX_PROGRESS_BYTES,
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
    _require_protocol_record_committed_retained(
        acks,
        expected_name,
        label="ACK",
    )
    acks._validate_live()
    return ack


def write_ack(
    worker_root: Path, message: ProgressMessage, decision: AckDecision
) -> Path:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    if type(decision) is not str or decision not in _ACK_DECISIONS:
        raise ValueError("ACK decision is invalid")
    path = bound_worker / "acks" / f"{message.seq:06d}.json"
    worker = _open_protocol_worker_directory(bound_worker, message.lane)
    publication_state = _ProtocolPublicationState()
    publication_completed = False
    try:
        with worker:
            try:
                _lock_protocol_worker(
                    worker,
                    deadline=_bounded_protocol_lock_deadline(None),
                )
                _read_worker_protocol_identity_retained(worker, message)
                message_sha256 = _durable_progress_hash_retained(
                    worker,
                    message,
                    worker_lock_held=True,
                )
                worker._validate_live()
                ack = Ack(
                    schema_version=1,
                    epoch_id=message.epoch_id,
                    run_kind=message.run_kind,
                    lane=message.lane,
                    seq=message.seq,
                    message_sha256=message_sha256,
                    decision=decision,
                )
                acks = _open_protocol_record_directory(
                    worker,
                    "acks",
                    label="ACK directory",
                )
                with acks:
                    _protocol_sequence_inventory_retained(
                        acks,
                        label="ACK",
                        deadline=None,
                        publishing_seq=message.seq,
                    )
                    _write_durable_protocol_record_retained(
                        acks,
                        path.name,
                        _encode_ack(ack),
                        label="ACK",
                        byte_cap=MAX_PROGRESS_BYTES,
                        publication_state=publication_state,
                    )
                    publication_completed = True
                    acks._validate_live()
                worker._validate_live()
            except BaseException as error:
                if not publication_state.commit_boundary_entered:
                    raise
                if (
                    publication_completed
                    and is_indeterminate_descriptor_close(error)
                ):
                    return path
                recovery_errors = (
                    _restore_protocol_pending_after_failure_retained(
                        worker,
                        directory_name="acks",
                        directory_label="ACK directory",
                        record_name=path.name,
                        record_label="ACK",
                    )
                )
                if recovery_errors:
                    return path
                publication_state.commit_boundary_entered = False
                raise error
        return path
    except BaseException:
        if (
            publication_state.commit_boundary_entered
            and publication_completed
        ):
            return path
        raise


def wait_for_ack(
    worker_root: Path, message: ProgressMessage, timeout: float
) -> Ack:
    _validate_progress_message(message)
    bound_worker, _ = _protocol_worker_context(worker_root, message.lane)
    timeout_value = _validate_wait_timeout(timeout)
    deadline = time.monotonic() + timeout_value
    worker = _open_protocol_worker_directory(bound_worker, message.lane)
    with worker:
        _read_worker_protocol_identity_retained(worker, message)
        _durable_progress_hash_retained(
            worker,
            message,
            worker_lock_held=False,
            lock_deadline=deadline,
        )
        worker._validate_live()
        with ExitStack() as retained:
            progress: _RecordChildDirectoryCapability | None = None
            acks: _RecordChildDirectoryCapability | None = None
            while True:
                if acks is None:
                    try:
                        acks = retained.enter_context(
                            _open_protocol_record_directory(
                                worker,
                                "acks",
                                label="ACK directory",
                                create=False,
                            )
                        )
                    except FileNotFoundError:
                        worker._validate_live()
                with _shared_protocol_worker_lock(
                    worker,
                    operation_deadline=deadline,
                ):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "timed out waiting for progress ACK"
                        )
                    _read_worker_protocol_identity_retained(worker, message)
                    sequences = (
                        ()
                        if acks is None
                        else _protocol_sequence_inventory_retained(
                            acks,
                            label="ACK",
                            deadline=deadline,
                        )
                    )
                    _require_protocol_sequence_prefix(
                        sequences,
                        expected_seq=message.seq,
                        label="ACK",
                    )
                    if acks is not None:
                        for sequence in range(1, message.seq):
                            if time.monotonic() >= deadline:
                                raise TimeoutError(
                                    "timed out waiting for progress ACK"
                                )
                            if progress is None:
                                progress = retained.enter_context(
                                    _open_protocol_record_directory(
                                        worker,
                                        "progress",
                                        label="progress directory",
                                        create=False,
                                    )
                                )
                            prior_progress = (
                                _read_progress_with_worker_lock_retained(
                                    worker,
                                    progress,
                                    expected_lane=message.lane,
                                    expected_seq=sequence,
                                )
                            )
                            worker._validate_live()
                            _read_ack_for_progress_with_worker_lock_retained(
                                worker,
                                acks,
                                prior_progress,
                            )
                        if (
                            _protocol_sequence_inventory_retained(
                                acks,
                                label="ACK",
                                deadline=deadline,
                            )
                            != sequences
                        ):
                            raise RuntimeError(
                                "ACK inventory changed while polling"
                            )
                    if message.seq in sequences:
                        if acks is None:
                            raise AssertionError(
                                "ACK inventory exists without a directory"
                            )
                        ack = (
                            _read_ack_for_progress_with_worker_lock_retained(
                                worker,
                                acks,
                                message,
                            )
                        )
                        if (
                            _protocol_sequence_inventory_retained(
                                acks,
                                label="ACK",
                                deadline=deadline,
                            )
                            != sequences
                        ):
                            raise RuntimeError(
                                "ACK inventory changed while reading"
                            )
                        worker._validate_live()
                        return ack
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for progress ACK")
                time.sleep(min(0.01, remaining))


@dataclass(frozen=True)
class _ProgressAckLedgerState:
    total_tokens: int
    last_sequence: dict[LaneName, int]
    accepted: dict[tuple[LaneName, int], tuple[str, AckDecision]]
    active_cases: dict[LaneName, tuple[CaseKey, int] | None]
    pending_retries: dict[LaneName, tuple[CaseKey, int] | None]
    completed_attempts: frozenset[tuple[CaseKey, int]]
    epoch_id: str | None
    run_kind: RunKind | None
    aborted: bool
    stop_launches: bool
    exited: frozenset[LaneName]


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
        self._state_lock = threading.RLock()
        self._state = _ProgressAckLedgerState(
            total_tokens=0,
            last_sequence={
                lane: 0 for lane in ("E1", "E2", "E3", "APP")
            },
            accepted={},
            active_cases={
                lane: None for lane in ("E1", "E2", "E3", "APP")
            },
            pending_retries={
                lane: None for lane in ("E1", "E2", "E3", "APP")
            },
            completed_attempts=frozenset(),
            epoch_id=None,
            run_kind=None,
            aborted=False,
            stop_launches=max_total_tokens == 0,
            exited=frozenset(),
        )

    @property
    def total_tokens(self) -> int:
        with self._state_lock:
            return self._state.total_tokens

    @property
    def max_total_tokens(self) -> int | None:
        with self._state_lock:
            return self._max_total_tokens

    @property
    def stop_launches(self) -> bool:
        with self._state_lock:
            return self._state.stop_launches

    @property
    def aborted(self) -> bool:
        with self._state_lock:
            return self._state.aborted

    @property
    def last_sequence(self) -> dict[LaneName, int]:
        with self._state_lock:
            return dict(self._state.last_sequence)

    @property
    def completed_attempts(self) -> frozenset[tuple[CaseKey, int]]:
        with self._state_lock:
            return self._state.completed_attempts

    @property
    def active_cases(
        self,
    ) -> dict[LaneName, tuple[CaseKey, int] | None]:
        with self._state_lock:
            return dict(self._state.active_cases)

    @property
    def pending_retries(
        self,
    ) -> dict[LaneName, tuple[CaseKey, int] | None]:
        with self._state_lock:
            return dict(self._state.pending_retries)

    @property
    def exited(self) -> frozenset[LaneName]:
        with self._state_lock:
            return self._state.exited

    @property
    def protocol_identity(self) -> tuple[str | None, RunKind | None]:
        with self._state_lock:
            return self._state.epoch_id, self._state.run_kind

    def _fork_for_replay(self) -> "ProgressAckLedger":
        with self._state_lock:
            fork = ProgressAckLedger(
                max_total_tokens=self._max_total_tokens
            )
            state = self._state
            fork._state = _ProgressAckLedgerState(
                total_tokens=state.total_tokens,
                last_sequence=dict(state.last_sequence),
                accepted=dict(state.accepted),
                active_cases=dict(state.active_cases),
                pending_retries=dict(state.pending_retries),
                completed_attempts=state.completed_attempts,
                epoch_id=state.epoch_id,
                run_kind=state.run_kind,
                aborted=state.aborted,
                stop_launches=state.stop_launches,
                exited=state.exited,
            )
            return fork

    def accept_progress(
        self,
        message: ProgressMessage,
        *,
        retry_decision: RetryDecision | None = None,
    ) -> AckDecision:
        with self._state_lock:
            self._require_outermost_transition_locked()
            payload = _encode_progress_message(message)
            if (
                retry_decision is not None
                and type(retry_decision) is not RetryDecision
            ):
                raise TypeError("retry decision must be exact or null")
            if retry_decision is None:
                return self._accept_progress_locked(message, payload)
            return self._accept_progress_locked(
                message, payload, retry_decision
            )

    def accept_durable_progress(
        self,
        *,
        worker_root: Path,
        message: ProgressMessage,
    ) -> AckDecision:
        retry_decision = _retry_decision_for_progress(
            worker_root=worker_root,
            message=message,
        )
        return self.accept_progress(
            message,
            retry_decision=retry_decision,
        )

    def _require_outermost_transition_locked(self) -> None:
        recursion_count = getattr(self._state_lock, "_recursion_count", None)
        if not callable(recursion_count):
            raise RuntimeError("ledger transition recursion state is unavailable")
        if recursion_count() != 1:
            raise RuntimeError("ledger transition is already active")

    def _accept_progress_locked(
        self,
        message: ProgressMessage,
        payload: dict[str, object],
        retry_decision: RetryDecision | None = None,
    ) -> AckDecision:
        state = self._state
        if state.epoch_id is not None and (
            message.epoch_id != state.epoch_id
            or message.run_kind != state.run_kind
        ):
            raise ValueError("progress differs from the ledger protocol identity")
        message_sha256 = hashlib.sha256(
            canonical_config_bytes(payload)
        ).hexdigest()
        key = (message.lane, message.seq)
        previous = state.accepted.get(key)
        if previous is not None:
            previous_sha256, previous_decision = previous
            if previous_sha256 != message_sha256:
                raise ValueError("progress sequence already names a different message")
            return previous_decision
        expected_seq = state.last_sequence[message.lane] + 1
        if message.seq != expected_seq:
            raise ValueError("progress sequence is gapped or reordered")
        if message.lane in state.exited:
            raise ValueError("progress arrived after worker exit")

        active = state.active_cases[message.lane]
        pending_retry = state.pending_retries[message.lane]
        case_attempt: tuple[CaseKey, int] | None = None
        if message.type in ("case-started", "case-terminal"):
            if message.case is None or message.attempt is None:
                raise AssertionError("case progress lacks its identity")
            case_attempt = (message.case, message.attempt)
            if case_attempt in state.completed_attempts:
                raise ValueError("completed case attempt cannot be replayed")
        if message.type == "case-started":
            if active is not None:
                raise ValueError("worker started a case before its prior terminal")
            if pending_retry is not None and case_attempt != pending_retry:
                raise ValueError("worker retry differs from its ACK authority")
            if pending_retry is None and message.attempt == 2:
                raise ValueError("attempt two lacks retry ACK authority")
        if message.type == "case-terminal":
            if active is None:
                raise ValueError("case terminal lacks launch authority")
            if active != case_attempt:
                raise ValueError("case terminal differs from the active attempt")

        new_total = state.total_tokens
        reaches_ceiling = state.stop_launches
        if message.type == "case-terminal" and message.usage is not None:
            new_total += message.usage.total_tokens
            if (
                self._max_total_tokens is not None
                and new_total >= self._max_total_tokens
            ):
                reaches_ceiling = True

        decision: AckDecision
        aborted = state.aborted
        if aborted:
            decision = "abort"
        elif message.type == "case-terminal" and message.status == "failed":
            if (
                retry_decision is not None
                and retry_decision.retry
                and retry_decision.next_attempt == 2
                and retry_decision.action == "reuse"
                and not reaches_ceiling
            ):
                decision = "retry"
            else:
                aborted = True
                decision = "abort"
        elif message.type == "shard-terminal" and message.status == "failed":
            aborted = True
            decision = "abort"
        elif message.type == "worker-stopped" and (
            state.active_cases[message.lane] is not None
            or state.pending_retries[message.lane] is not None
        ):
            aborted = True
            decision = "abort"
        elif reaches_ceiling:
            decision = "stop-launches"
        else:
            decision = "continue"

        active_cases = dict(state.active_cases)
        pending_retries = dict(state.pending_retries)
        completed_attempts = set(state.completed_attempts)
        exited = set(state.exited)
        if message.type == "case-started" and decision == "continue":
            if message.case is None or message.attempt is None:
                raise AssertionError("case-started progress lacks its identity")
            active_cases[message.lane] = (
                message.case,
                message.attempt,
            )
            pending_retries[message.lane] = None
        elif message.type == "case-terminal":
            if case_attempt is None:
                raise AssertionError("case terminal lacks its completed identity")
            completed_attempts.add(case_attempt)
            active_cases[message.lane] = None
            pending_retries[message.lane] = (
                (message.case, 2)
                if decision == "retry" and message.case is not None
                else None
            )
        elif message.type == "worker-stopped":
            exited.add(message.lane)

        accepted = dict(state.accepted)
        accepted[key] = (message_sha256, decision)
        last_sequence = dict(state.last_sequence)
        last_sequence[message.lane] = message.seq
        self._state = _ProgressAckLedgerState(
            total_tokens=new_total,
            last_sequence=last_sequence,
            accepted=accepted,
            active_cases=active_cases,
            pending_retries=pending_retries,
            completed_attempts=frozenset(completed_attempts),
            epoch_id=(
                message.epoch_id if state.epoch_id is None else state.epoch_id
            ),
            run_kind=(
                message.run_kind if state.run_kind is None else state.run_kind
            ),
            aborted=aborted,
            stop_launches=reaches_ceiling,
            exited=frozenset(exited),
        )
        return decision

    def worker_exited(self, lane: LaneName) -> AckDecision:
        if type(lane) is not str or lane not in ("E1", "E2", "E3", "APP"):
            raise ValueError("worker exit lane is invalid")
        with self._state_lock:
            self._require_outermost_transition_locked()
            state = self._state
            exited = set(state.exited)
            exited.add(lane)
            active_cases = dict(state.active_cases)
            active_cases[lane] = None
            self._state = _ProgressAckLedgerState(
                total_tokens=state.total_tokens,
                last_sequence=state.last_sequence,
                accepted=state.accepted,
                active_cases=active_cases,
                pending_retries=state.pending_retries,
                completed_attempts=state.completed_attempts,
                epoch_id=state.epoch_id,
                run_kind=state.run_kind,
                aborted=True,
                stop_launches=state.stop_launches,
                exited=frozenset(exited),
            )
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


def _require_guard_reason(reason: str) -> None:
    if type(reason) is not str or not reason or len(reason) > 256:
        raise ValueError("coordinator checkpoint reason must be a nonempty exact string")


def _capture_directory_rows(
    root: Path,
) -> tuple[tuple[str, str, int, int, str], ...]:
    from scripts import run_observing_workflows_task9_eval as evaluator

    slot, _ = _open_absolute_directory_anchor(
        root, "snapshot root", trusted_parent=False
    )
    rows: tuple[tuple[str, str, int, int, str], ...] | None = None
    primary: BaseException | None = None
    try:
        metadata = os.fstat(slot.descriptor)
        rows = (
            (
                ".",
                "directory",
                stat.S_IMODE(metadata.st_mode),
                0,
                "",
            ),
            *evaluator._fingerprint_repository_directory_at(slot.descriptor),
        )
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [slot],
        primary=primary,
        label="snapshot capture or descriptor close failed",
    )
    if rows is None:
        raise AssertionError("snapshot capture produced no rows")
    return rows


def _capture_production_rows(
    repository_root: Path,
) -> tuple[tuple[str, str, int, int, str], ...]:
    canonical, _ = _canonical_git_repository_root(repository_root)
    return _capture_directory_rows(canonical)


def _production_snapshot_from_rows(
    rows: tuple[tuple[str, str, int, int, str], ...],
) -> ProductionSnapshot:
    entries = tuple(
        (
            row[0],
            hashlib.sha256(
                canonical_config_bytes(
                    {
                        "kind": row[1],
                        "mode": row[2],
                        "size": row[3],
                        "sha256": row[4],
                    }
                )
            ).hexdigest(),
        )
        for row in rows
    )
    return ProductionSnapshot(
        fingerprint=hashlib.sha256(
            canonical_config_bytes({"entries": entries})
        ).hexdigest(),
        entries=entries,
    )


def _validate_expected_result_delta(expected: dict[str, str]) -> dict[str, str]:
    if type(expected) is not dict:
        raise TypeError("expected result delta must be an exact dict")
    items = tuple(expected.items())
    frozen: dict[str, str] = {}
    for relative, digest in items:
        if type(relative) is not str or type(digest) is not str:
            raise TypeError("expected result delta entries must be exact strings")
        path = PurePosixPath(relative)
        if (
            not relative
            or path.is_absolute()
            or path.as_posix() != relative
            or relative in {".", ".."}
            or any(part in {"", ".", ".."} for part in path.parts)
            or not _is_sha256(digest)
        ):
            raise ValueError("expected result delta entry is invalid")
        frozen[relative] = digest
    if len(frozen) != len(items):
        raise ValueError("expected result delta contains duplicate paths")
    return frozen


def _require_exact_path(path: Path, label: str) -> Path:
    concrete_path_type = type(Path("."))
    if type(path) is not concrete_path_type or not path.is_absolute():
        raise TypeError(f"{label} must be an exact absolute Path")
    if path != path.resolve(strict=False):
        raise ValueError(f"{label} must use its canonical spelling")
    return path


def _validate_snapshot_root(snapshot_root: Path) -> tuple[
    tuple[str, str, int, int, str], ...
]:
    root = _require_exact_path(snapshot_root, "snapshot_root")
    try:
        metadata = root.lstat()
    except OSError:
        raise ValueError("snapshot_root is unavailable") from None
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o555
        or metadata.st_uid != os.geteuid()
    ):
        raise ValueError("snapshot_root must be an owned mode-0555 directory")
    return _capture_directory_rows(root)


def _validate_epoch_inputs(
    *,
    plan: EpochPlan,
    run_root: Path,
    snapshot_root: Path,
    manifests: dict[str, list[dict]],
    current_fingerprints: InputFingerprints,
) -> tuple[
    Path,
    tuple[bytes, bytes],
    dict[EvalMode, list[dict[str, object]]],
    tuple[tuple[str, str, int, int, str], ...],
]:
    if type(plan) is not EpochPlan:
        raise TypeError("plan must be an exact EpochPlan")
    if plan.run_kind not in ("discovery", "formal"):
        raise ValueError("only discovery or formal epochs can be aggregated")
    if type(current_fingerprints) is not InputFingerprints:
        raise TypeError(
            "current_fingerprints must be exact InputFingerprints"
        )
    if (
        not _fingerprints_are_complete(plan.fingerprints)
        or not _fingerprints_are_complete(current_fingerprints)
        or current_fingerprints != plan.fingerprints
        or plan.epoch_id != plan.fingerprints.epoch_id
        or plan.run_kind != plan.fingerprints.run_kind
    ):
        raise ValueError("epoch input fingerprints differ")
    root = _require_exact_path(run_root, "run_root")
    descriptor, metadata = _open_private_directory(root, "run root")
    _retire_task_descriptors(
        [_DescriptorSlot(descriptor)],
        primary=None,
        label="run root validation or descriptor close failed",
    )
    if root.resolve(strict=True) != root or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise ValueError("run_root must be a canonical private directory")
    manifest_snapshot = _capture_resume_manifest_snapshot(manifests)
    if (
        hashlib.sha256(manifest_snapshot[0]).hexdigest()
        != current_fingerprints.forward_manifest_sha256
        or hashlib.sha256(manifest_snapshot[1]).hexdigest()
        != current_fingerprints.lifecycle_manifest_sha256
    ):
        raise ValueError("manifest capture differs from input fingerprints")
    frozen_manifests = _decode_resume_manifest_snapshot(manifest_snapshot)
    rebuilt = build_epoch_plan(
        run_kind=plan.run_kind,
        manifests=frozen_manifests,
        fingerprints=current_fingerprints,
    )
    if rebuilt != plan:
        raise ValueError("epoch plan differs from captured manifests")
    snapshot_rows = _validate_snapshot_root(snapshot_root)
    return root, manifest_snapshot, frozen_manifests, snapshot_rows


def _freeze_shard_paths(
    shard_paths: dict[LaneName, Path], *, run_root: Path
) -> dict[LaneName, Path]:
    if type(shard_paths) is not dict:
        raise TypeError("shard_paths must be an exact dict")
    items = tuple(shard_paths.items())
    if (
        any(type(lane) is not str for lane, _path in items)
        or {lane for lane, _path in items} != {"E1", "E2", "E3", "APP"}
        or len(items) != 4
    ):
        raise ValueError("shard_paths must contain the exact four lanes")
    frozen = dict(items)
    for lane in ("E1", "E2", "E3", "APP"):
        path = _require_exact_path(frozen[lane], f"{lane} shard path")
        worker_root = (
            run_root / "app-server"
            if lane == "APP"
            else run_root / "workers" / lane
        )
        if path != worker_root / "sealed" / "shard-commit.json":
            raise ValueError("shard path differs from the frozen lane path")
    return frozen


def _freeze_case_paths(
    case_paths: dict[CaseKey, CasePaths],
    *,
    plan: EpochPlan,
    run_root: Path,
) -> dict[CaseKey, CasePaths]:
    if type(case_paths) is not dict:
        raise TypeError("case_paths must be an exact dict")
    items = tuple(case_paths.items())
    expected_keys = tuple(assignment.key for assignment in plan.assignments)
    if (
        len(items) != len(expected_keys)
        or any(type(key) is not CaseKey for key, _paths in items)
        or {key for key, _paths in items} != set(expected_keys)
    ):
        raise ValueError("case_paths must contain every planned case exactly once")
    frozen = dict(items)
    for assignment in plan.assignments:
        paths = frozen[assignment.key]
        if type(paths) is not CasePaths:
            raise TypeError("case_paths values must be exact CasePaths")
        if paths != paths_for_case(run_root, assignment):
            raise ValueError("case_paths differ from the frozen epoch layout")
    return frozen


def _read_successful_shards(
    *,
    plan: EpochPlan,
    run_root: Path,
    manifests: dict[EvalMode, list[dict[str, object]]],
    shard_paths: dict[LaneName, Path],
    case_paths: dict[CaseKey, CasePaths],
) -> dict[CaseKey, ShardTerminal]:
    terminals: dict[CaseKey, ShardTerminal] = {}
    shard_hashes: set[str] = set()
    for lane in ("E1", "E2", "E3", "APP"):
        lane_assignments = tuple(
            assignment for assignment in plan.assignments if assignment.lane == lane
        )
        lane_paths = {
            assignment.key: case_paths[assignment.key]
            for assignment in lane_assignments
        }
        seal = read_shard_seal(
            worker_root=shard_paths[lane].parent.parent,
            plan=plan,
            lane=lane,
            manifests=manifests,
            case_paths=lane_paths,
        )
        if (
            seal.status != "success"
            or tuple(terminal.key for terminal in seal.terminals)
            != tuple(assignment.key for assignment in lane_assignments)
            or any(
                terminal.run_kind != plan.run_kind
                or terminal.status != "success"
                or terminal.classification != "success"
                for terminal in seal.terminals
            )
        ):
            raise ValueError("shard is not a complete all-green commit")
        if seal.commit_sha256 in shard_hashes:
            raise ValueError("shard commit hashes must be unique")
        shard_hashes.add(seal.commit_sha256)
        for terminal in seal.terminals:
            if terminal.key in terminals:
                raise ValueError("case appears in more than one shard")
            terminals[terminal.key] = terminal
    if len(shard_hashes) != 4 or len(terminals) != 28:
        raise ValueError("shard commits do not cover the complete epoch")
    return terminals


def _validate_attempt_truth(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
    manifest_case: dict[str, object],
    case_seal: CaseSeal,
    shard_terminal: ShardTerminal,
) -> None:
    attempt_paths = scan_attempts(
        paths, plan=plan, manifest_case=manifest_case
    )
    if len(attempt_paths) not in (1, 2):
        raise ValueError("validated case must contain one or two sealed attempts")
    attempts = tuple(
        read_attempt_seal(
            plan=plan,
            paths=paths,
            assignment=assignment,
            attempt=attempt_number,
            manifest_case=manifest_case,
        )
        for attempt_number in range(1, len(attempt_paths) + 1)
    )
    final = attempts[-1]
    if (
        final.terminal.get("status") != "success"
        or final.terminal.get("classification") != "success"
        or final.terminal.get("cleanup_passed") is not True
        or final.terminal_sha256 != shard_terminal.attempt_terminal_sha256
        or final.terminal_sha256 != case_seal.commit.get(
            "attempt_terminal_sha256"
        )
        or case_seal.commit.get("attempt") != len(attempts)
    ):
        raise ValueError("final attempt truth differs from case or shard commit")
    if sum(
        attempt.terminal.get("model_started") is True for attempt in attempts
    ) != 1:
        raise ValueError("validated case must have exactly one model-started attempt")
    if len(attempts) == 2:
        first = attempts[0].terminal
        decision = decide_retry(
            classification=first.get("classification"),
            attempt=1,
            model_started=first.get("model_started"),
            cleanup_passed=first.get("cleanup_passed"),
            fingerprints_unchanged=True,
        )
        if (
            first.get("status") != "failed"
            or not decision.retry
            or decision.next_attempt != 2
        ):
            raise ValueError("two-attempt case lacks a proved retryable first attempt")


def _validate_case_inventory_count(
    paths: CasePaths, component: str, expected: object
) -> None:
    if type(expected) is not int or expected < 0:
        raise ValueError(f"{component} count is invalid")
    directory = _open_case_record_directory(
        paths=paths,
        components=(component,),
        create=False,
        label=f"case {component} directory",
    )
    with directory:
        inventory = directory.inventory()
        parent_fd = directory._retained[-1].slot.descriptor
        for name in inventory:
            metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            _validate_owned_entry(
                metadata,
                label=f"case {component} entry",
                kind="file",
                mode=0o600,
            )
        if len(inventory) != expected:
            raise ValueError(f"case {component} count differs from sealed evidence")


def _validate_case_audit(paths: CasePaths, expected: object) -> None:
    if type(expected) is not int or expected < 0:
        raise ValueError("audit event count is invalid")
    directory = _open_case_record_directory(
        paths=paths,
        components=("audit",),
        create=False,
        label="case audit directory",
    )
    with directory:
        inventory = directory.inventory()
        if expected == 0:
            if inventory:
                raise ValueError("case audit inventory differs from sealed evidence")
            return
        if inventory != ("payload-audit.jsonl",):
            raise ValueError("case audit inventory is invalid")
        content = _read_regular_bytes_retained(
            directory,
            "payload-audit.jsonl",
            "case payload audit",
            byte_cap=4 * 1024 * 1024,
        )
        try:
            rows = [
                json.loads(line)
                for line in content.decode("utf-8").splitlines()
            ]
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError("case payload audit is invalid JSON lines") from None
        if len(rows) != expected or any(type(row) is not dict for row in rows):
            raise ValueError("case audit event count differs from sealed evidence")


def _integrity_environment(paths: CasePaths) -> dict[str, str]:
    cli = (
        paths.staging
        / "marketplace/plugins/workflow-observer/scripts/"
        "workflow_observer_cli.py"
    )
    return {
        "HOME": str(paths.home),
        "CODEX_HOME": str(paths.codex_home),
        "TMPDIR": str(paths.tmp),
        "XDG_CONFIG_HOME": str(paths.config),
        "XDG_CACHE_HOME": str(paths.cache),
        "PYTHONDONTWRITEBYTECODE": "1",
        "WORKFLOW_OBSERVATORY_HOME": str(paths.home),
        "OBSERVATION_PAYLOAD_TMPDIR": str(paths.payload),
        "OBSERVATION_AUDIT_LOG": str(paths.audit / "payload-audit.jsonl"),
        "OBSERVATION_EVAL": "1",
        "OBSERVATION_CLI_PATH": str(cli),
    }


def _validate_case_integrity(
    *,
    paths: CasePaths,
    evidence: dict[str, object],
    integrity_runner: Callable[..., dict],
) -> None:
    store = _open_case_record_directory(
        paths=paths,
        components=("store",),
        create=False,
        label="case store directory",
    )
    with store:
        inventory = store.inventory()
        environment = _integrity_environment(paths)
        command = (
            sys.executable,
            environment["OBSERVATION_CLI_PATH"],
            "integrity",
        )
        result = integrity_runner(
            command,
            environment,
            expected_records=evidence["store_record_count"],
        )
        if store.inventory() != inventory:
            raise RuntimeError("case store changed during integrity validation")
    if (
        type(result) is not dict
        or set(result) != {"records", "invalidated"}
        or any(type(result[field]) is not int or result[field] < 0 for field in result)
        or result["records"] != evidence["store_record_count"]
        or result["invalidated"] != evidence["store_invalidated_count"]
    ):
        raise ValueError("case integrity result differs from sealed evidence")


def _decode_bootstrap_tombstone_records(
    *,
    plan: EpochPlan,
    ownership_payload: dict[str, object],
    ownership_bytes: bytes,
    receipt_payload: dict[str, object],
    receipt_bytes: bytes,
) -> tuple[BootstrapTombstoneReceipt, str]:
    _require_exact_fields(
        ownership_payload, BootstrapOwnership, "bootstrap ownership"
    )
    ownership = BootstrapOwnership(**ownership_payload)
    _require_exact_fields(
        receipt_payload, BootstrapTombstoneReceipt, "bootstrap tombstone"
    )
    receipt = BootstrapTombstoneReceipt(**receipt_payload)
    if (
        type(ownership.schema_version) is not int
        or ownership.schema_version != 1
        or ownership.epoch_id != plan.epoch_id
        or ownership.run_kind != plan.run_kind
        or type(ownership.bootstrap_device) is not int
        or ownership.bootstrap_device < 0
        or type(ownership.bootstrap_inode) is not int
        or ownership.bootstrap_inode < 0
        or type(receipt.schema_version) is not int
        or receipt.schema_version != 1
        or type(receipt.epoch_id) is not str
        or receipt.epoch_id != plan.epoch_id
        or type(receipt.run_kind) is not str
        or receipt.run_kind != plan.run_kind
        or type(receipt.ownership_sha256) is not str
        or receipt.ownership_sha256
        != hashlib.sha256(ownership_bytes).hexdigest()
        or type(receipt.bootstrap_device) is not int
        or receipt.bootstrap_device != ownership.bootstrap_device
        or type(receipt.bootstrap_inode) is not int
        or receipt.bootstrap_inode != ownership.bootstrap_inode
        or receipt.scrubbed is not True
        or receipt.empty is not True
        or receipt.canonical_binding != "expected"
        or receipt.producer not in ("coordinator", "coordinator-recovery")
    ):
        raise ValueError("bootstrap tombstone is stale or invalid")
    return receipt, hashlib.sha256(receipt_bytes).hexdigest()


def _decode_bootstrap_tombstone(
    *,
    plan: EpochPlan,
    run_root: Path,
) -> tuple[BootstrapTombstoneReceipt, str]:
    cleanup = run_root / "coordinator" / "cleanup"
    ownership_payload, ownership_bytes = _read_canonical_record(
        cleanup / "bootstrap-ownership.json",
        "bootstrap ownership",
    )
    receipt_payload, receipt_bytes = _read_canonical_record(
        cleanup / "bootstrap-tombstone.json",
        "bootstrap tombstone",
    )
    return _decode_bootstrap_tombstone_records(
        plan=plan,
        ownership_payload=ownership_payload,
        ownership_bytes=ownership_bytes,
        receipt_payload=receipt_payload,
        receipt_bytes=receipt_bytes,
    )


def _validate_teardown_receipt(
    *,
    plan: EpochPlan,
    run_root: Path,
    teardown_receipt: Path,
    expected_tombstones: tuple[tuple[CaseKey, str], ...],
    case_paths: dict[CaseKey, CasePaths],
) -> str:
    path = _require_exact_path(teardown_receipt, "teardown_receipt")
    if path != run_root / "coordinator" / "teardown.json":
        raise ValueError("teardown receipt path differs from the coordinator path")
    payload, content = _read_canonical_record(
        path, "coordinator teardown receipt", byte_cap=64 * 1024
    )
    _require_exact_fields(payload, TeardownReceipt, "coordinator teardown receipt")
    encoded_tombstones = payload.get("tombstone_receipts")
    if type(encoded_tombstones) is not list:
        raise ValueError("teardown tombstone receipts must be an exact list")
    decoded_tombstones: list[tuple[CaseKey, str]] = []
    for item in encoded_tombstones:
        if type(item) is not list or len(item) != 2:
            raise ValueError("teardown tombstone binding is invalid")
        key = _decode_case_key(item[0], "teardown tombstone case")
        digest = item[1]
        if type(digest) is not str or not _is_sha256(digest):
            raise ValueError("teardown tombstone hash is invalid")
        decoded_tombstones.append((key, digest))
    _, bootstrap_sha256 = _decode_bootstrap_tombstone(
        plan=plan, run_root=run_root
    )
    if (
        type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
        or type(payload.get("epoch_id")) is not str
        or payload.get("epoch_id") != plan.epoch_id
        or type(payload.get("run_kind")) is not str
        or payload.get("run_kind") != plan.run_kind
        or tuple(decoded_tombstones) != expected_tombstones
        or len({key for key, _digest in decoded_tombstones}) != 28
        or len({digest for _key, digest in decoded_tombstones}) != 28
        or payload.get("bootstrap_tombstone_receipt_sha256")
        != bootstrap_sha256
        or payload.get("codex_homes_absent") is not True
        or payload.get("bootstrap_absent") is not True
    ):
        raise ValueError("coordinator teardown receipt is stale or incomplete")
    for paths in case_paths.values():
        try:
            paths.codex_home.lstat()
        except FileNotFoundError:
            pass
        else:
            raise ValueError("case Codex-home tombstone still exists")
    try:
        (run_root / "coordinator" / "auth-bootstrap").lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("coordinator auth bootstrap still exists")
    return hashlib.sha256(content).hexdigest()


def _encode_epoch_rows(rows: Sequence[dict[str, object]]) -> bytes:
    return json.dumps(
        list(rows),
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")


def _decode_epoch_rows(content: bytes) -> tuple[dict[str, object], ...]:
    rows = json.loads(content.decode("ascii"))
    if type(rows) is not list or any(type(row) is not dict for row in rows):
        raise RuntimeError("validated epoch row binding changed")
    return tuple(rows)


def validate_epoch_for_aggregation(
    *,
    plan: EpochPlan,
    run_root: Path,
    snapshot_root: Path,
    manifests: dict[str, list[dict]],
    shard_paths: dict[LaneName, Path],
    case_paths: dict[CaseKey, CasePaths],
    integrity_runner: Callable[..., dict],
    guard: CoordinatorGuard,
    current_fingerprints: InputFingerprints,
    teardown_receipt: Path,
) -> ValidatedEpoch:
    if type(guard) is not CoordinatorGuard:
        raise TypeError("guard must be an exact CoordinatorGuard")
    guard._validate_nominal()
    if not callable(integrity_runner):
        raise TypeError("integrity_runner must be callable")
    (
        bound_run_root,
        manifest_snapshot,
        frozen_manifests,
        snapshot_rows,
    ) = _validate_epoch_inputs(
        plan=plan,
        run_root=run_root,
        snapshot_root=snapshot_root,
        manifests=manifests,
        current_fingerprints=current_fingerprints,
    )
    frozen_shards = _freeze_shard_paths(
        shard_paths, run_root=bound_run_root
    )
    frozen_cases = _freeze_case_paths(
        case_paths, plan=plan, run_root=bound_run_root
    )
    shard_terminals = _read_successful_shards(
        plan=plan,
        run_root=bound_run_root,
        manifests=frozen_manifests,
        shard_paths=frozen_shards,
        case_paths=frozen_cases,
    )

    forward_rows: list[dict[str, object]] = []
    lifecycle_rows: list[dict[str, object]] = []
    sealed: list[tuple[CaseAssignment, CaseSeal, dict[str, object]]] = []
    commit_hashes: set[str] = set()
    attempt_hashes: set[str] = set()
    tombstone_hashes: set[str] = set()
    for assignment in plan.assignments:
        paths = frozen_cases[assignment.key]
        manifest_case = frozen_manifests[assignment.key.mode][
            assignment.key.ordinal - 1
        ]
        seal = read_case_seal(
            plan=plan,
            paths=paths,
            assignment=assignment,
            manifest_case=manifest_case,
        )
        terminal = shard_terminals[assignment.key]
        if (
            seal.result is None
            or seal.commit.get("status") != "success"
            or seal.evidence.get("status") != "success"
            or seal.evidence.get("classification") != "success"
            or seal.evidence.get("process_cleanup_passed") is not True
            or seal.evidence.get("credential_cleanup_passed") is not True
            or terminal.case_commit_sha256 != seal.commit_sha256
            or terminal.tombstone_receipt_sha256
            != seal.tombstone_receipt_sha256
        ):
            raise ValueError("case is not a complete all-green commit")
        _validate_attempt_truth(
            plan=plan,
            assignment=assignment,
            paths=paths,
            manifest_case=manifest_case,
            case_seal=seal,
            shard_terminal=terminal,
        )
        attempt_sha256 = seal.commit["attempt_terminal_sha256"]
        tombstone_sha256 = seal.tombstone_receipt_sha256
        if (
            seal.commit_sha256 in commit_hashes
            or attempt_sha256 in attempt_hashes
            or tombstone_sha256 in tombstone_hashes
        ):
            raise ValueError("case seal dependencies must be unique")
        commit_hashes.add(seal.commit_sha256)
        attempt_hashes.add(attempt_sha256)
        tombstone_hashes.add(tombstone_sha256)
        target = (
            forward_rows
            if assignment.key.mode == "forward"
            else lifecycle_rows
        )
        target.append(dict(seal.result))
        sealed.append((assignment, seal, manifest_case))
    if (
        len(commit_hashes) != 28
        or len(attempt_hashes) != 28
        or len(tombstone_hashes) != 28
        or sum(assignment.route == "exec" for assignment in plan.assignments) != 24
        or sum(
            assignment.route == "app-server" for assignment in plan.assignments
        )
        != 4
    ):
        raise ValueError("validated case seals or route table are incomplete")

    for _assignment, seal, _manifest_case in sealed:
        paths = frozen_cases[_assignment.key]
        _validate_case_integrity(
            paths=paths,
            evidence=seal.evidence,
            integrity_runner=integrity_runner,
        )
        _validate_case_inventory_count(
            paths, "payload", seal.evidence["payload_file_count"]
        )
        _validate_case_inventory_count(
            paths, "output", seal.evidence["output_file_count"]
        )
        _validate_case_audit(paths, seal.evidence["audit_event_count"])

    from scripts import run_observing_workflows_task9_eval as evaluator

    results = {"forward": forward_rows, "lifecycle": lifecycle_rows}
    evaluator._validate_committed_result_semantics(results, frozen_manifests)
    expected_tombstones = tuple(
        (
            assignment.key,
            shard_terminals[assignment.key].tombstone_receipt_sha256,
        )
        for assignment in plan.assignments
    )
    teardown_sha256 = _validate_teardown_receipt(
        plan=plan,
        run_root=bound_run_root,
        teardown_receipt=teardown_receipt,
        expected_tombstones=expected_tombstones,
        case_paths=frozen_cases,
    )
    for assignment, _seal, manifest_case in sealed:
        read_case_seal(
            plan=plan,
            paths=frozen_cases[assignment.key],
            assignment=assignment,
            manifest_case=manifest_case,
        )
    if (
        [row["id"] for row in forward_rows]
        != [row["id"] for row in frozen_manifests["forward"]]
        or [row["id"] for row in lifecycle_rows]
        != [row["id"] for row in frozen_manifests["lifecycle"]]
        or len(forward_rows) != 20
        or len(lifecycle_rows) != 8
        or _capture_directory_rows(snapshot_root) != snapshot_rows
    ):
        raise ValueError("validated epoch order or captured snapshot changed")
    guard.verify_exact_result_delta(
        {}, "validated epoch requires zero production delta"
    )
    forward_bytes = _encode_epoch_rows(forward_rows)
    lifecycle_bytes = _encode_epoch_rows(lifecycle_rows)
    evidence_sha256 = hashlib.sha256(
        canonical_config_bytes(
            {
                "epoch_id": plan.epoch_id,
                "run_kind": plan.run_kind,
                "case_commit_sha256": tuple(
                    seal.commit_sha256 for _assignment, seal, _manifest in sealed
                ),
                "teardown_receipt_sha256": teardown_sha256,
                "forward_sha256": hashlib.sha256(forward_bytes).hexdigest(),
                "lifecycle_sha256": hashlib.sha256(lifecycle_bytes).hexdigest(),
            }
        )
    ).hexdigest()
    return ValidatedEpoch(
        _VALIDATED_EPOCH_TOKEN,
        plan=plan,
        forward_bytes=forward_bytes,
        lifecycle_bytes=lifecycle_bytes,
        manifest_bytes=manifest_snapshot,
        evidence_sha256=evidence_sha256,
        teardown_receipt_sha256=teardown_sha256,
        guard=guard,
    )


def aggregate_committed_cases(validated: ValidatedEpoch) -> Aggregate:
    if type(validated) is not ValidatedEpoch:
        raise TypeError("aggregate requires an exact ValidatedEpoch")
    binding = validated._validate_nominal()
    return Aggregate(
        run_kind=binding.plan.run_kind,
        forward_rows=_decode_epoch_rows(binding.forward_bytes),
        lifecycle_rows=_decode_epoch_rows(binding.lifecycle_bytes),
        evidence_sha256=binding.evidence_sha256,
    )


def _expected_persisted_result_delta(
    *,
    repository_root: Path,
    destinations: dict[str, Path],
    results: dict[str, list[dict]],
) -> dict[str, str]:
    from scripts import run_observing_workflows_task9_eval as evaluator

    parent = destinations["forward"].parent
    relative_parent = parent.relative_to(repository_root)
    contents = {
        mode: evaluator._json_bytes(results[mode])
        for mode in ("forward", "lifecycle")
    }
    digests = {
        mode: hashlib.sha256(contents[mode]).hexdigest()
        for mode in ("forward", "lifecycle")
    }
    generation = hashlib.sha256(
        (digests["forward"] + digests["lifecycle"]).encode("ascii")
    ).hexdigest()[:24]
    names = {
        mode: f"{generation}-{mode}.json"
        for mode in ("forward", "lifecycle")
    }
    generation_root = relative_parent / evaluator.RESULT_GENERATION_DIRECTORY
    expected = {
        (generation_root / names[mode]).as_posix(): digests[mode]
        for mode in ("forward", "lifecycle")
    }
    pointer_value = {
        "schema_version": 1,
        "generation": generation,
        "files": {
            mode: {
                "path": (
                    f"{evaluator.RESULT_GENERATION_DIRECTORY}/{names[mode]}"
                ),
                "sha256": digests[mode],
            }
            for mode in ("forward", "lifecycle")
        },
    }
    pointer_content = evaluator._json_bytes(pointer_value)
    expected[
        (relative_parent / evaluator.RESULT_COMMIT_FILENAME).as_posix()
    ] = hashlib.sha256(pointer_content).hexdigest()
    return expected


def persist_validated_epoch(
    commit: FormalCommitCapability,
    *,
    authority: ResultWriterAuthority,
    destinations: dict[str, Path],
    guard: CoordinatorGuard,
) -> Path:
    if type(commit) is not FormalCommitCapability:
        raise TypeError("persist requires an exact FormalCommitCapability")
    binding = commit._preflight()
    if type(authority) is not ResultWriterAuthority:
        raise TypeError("persist requires an exact ResultWriterAuthority")
    if type(guard) is not CoordinatorGuard or guard is not binding.guard:
        raise TypeError("persist requires the validated epoch CoordinatorGuard")
    authority._validate_live()
    guard_binding = guard._validate_nominal()
    if guard_binding is not binding.guard_binding:
        raise RuntimeError("persist guard issuance binding changed")
    if (
        authority.run_kind != "formal"
        or authority.repository_key != binding.repository_key
    ):
        raise ValueError("result writer authority differs from validated epoch")
    frozen_destinations = _validate_result_destinations(
        destinations, repository_root=binding.repository_root
    )
    guard.checkpoint("before validated epoch persistence")
    results = {
        "forward": list(_decode_epoch_rows(binding.forward_bytes)),
        "lifecycle": list(_decode_epoch_rows(binding.lifecycle_bytes)),
    }
    manifests = _decode_resume_manifest_snapshot(binding.manifest_bytes)
    from scripts import run_observing_workflows_task9_eval as evaluator

    expected_delta = _expected_persisted_result_delta(
        repository_root=binding.repository_root,
        destinations=frozen_destinations,
        results=results,
    )
    commit._consume(binding)
    pointer = evaluator.persist_result_pair(
        frozen_destinations,
        results,
        manifests,
        authority=authority,
    )
    readback = evaluator.resolve_committed_result_pair(pointer, manifests)
    if readback != results:
        raise AssertionError("validated epoch result readback differs")
    guard.verify_exact_result_delta(
        expected_delta, "after validated epoch persistence"
    )
    return pointer


class QuiescentRunAuthority:
    def __init__(
        self,
        token: object,
        *,
        coordinator: "CoordinatorStateMachine",
    ) -> None:
        if (
            token is not _QUIESCENT_AUTHORITY_TOKEN
            or type(coordinator) is not CoordinatorStateMachine
        ):
            raise TypeError(
                "QuiescentRunAuthority cannot be constructed directly"
            )
        self._coordinator = coordinator
        self._owner_pid = os.getpid()
        self._epoch_id = coordinator._plan.epoch_id
        self._run_kind = coordinator._plan.run_kind
        self._consumed = False
        self._lease: RunCoordinatorLease | None = None

    def _validate_identity(self) -> "CoordinatorStateMachine":
        if type(self) is not QuiescentRunAuthority:
            raise TypeError("QuiescentRunAuthority must be exact")
        if self._owner_pid != os.getpid():
            raise RuntimeError(
                "quiescent run authority belongs to another process"
            )
        coordinator = self._coordinator
        if (
            type(coordinator) is not CoordinatorStateMachine
            or not any(
                candidate is self
                for candidate in coordinator._issued_authorities
            )
            or self._epoch_id != coordinator._plan.epoch_id
            or self._run_kind != coordinator._plan.run_kind
        ):
            raise RuntimeError("quiescent run authority binding changed")
        return coordinator

    def _validate_live(self) -> "CoordinatorStateMachine":
        coordinator = self._validate_identity()
        coordinator._require_healthy()
        if self._consumed:
            raise RuntimeError("quiescent run authority was already consumed")
        if coordinator._active_authority is not self:
            raise RuntimeError("quiescent run authority is no longer active")
        if coordinator._phase != "tearing-down":
            raise RuntimeError("run is not quiescent")
        if self._lease is None:
            raise RuntimeError(
                "quiescent run authority is not bound to a live run lease"
            )
        self._lease._validate_live()
        if (
            self._lease._epoch_id != self._epoch_id
            or self._lease._run_kind != self._run_kind
            or self._lease._run_root != coordinator._options.run_root
        ):
            raise RuntimeError("quiescent run authority lease binding changed")
        return coordinator

    def _bind_lease(self, lease: RunCoordinatorLease) -> None:
        if type(lease) is not RunCoordinatorLease:
            raise TypeError("quiescent authority requires exact run lease")
        lease._validate_live()
        if self._lease is None:
            self._lease = lease
        elif self._lease is not lease:
            raise RuntimeError(
                "quiescent authority was bound to another run lease"
            )

    def _consume(self, purpose: str) -> None:
        if purpose not in ("resume-launch", "teardown-receipt"):
            raise ValueError("invalid quiescent authority consumption purpose")
        coordinator = self._validate_live()
        self._consumed = True
        coordinator._active_authority = None

    def _poison(self, error: BaseException) -> None:
        coordinator = self._validate_identity()
        coordinator._poisoned = True
        coordinator._poison_error = error

    @property
    def epoch_id(self) -> str:
        self._validate_identity()
        return self._epoch_id

    @property
    def run_kind(self) -> RunKind:
        self._validate_identity()
        return self._run_kind

    @property
    def consumed(self) -> bool:
        self._validate_identity()
        return self._consumed


class CoordinatorStateMachine:
    def __init__(
        self,
        token: object,
        *,
        plan: EpochPlan,
        options: ParallelOptions,
        guard: CoordinatorGuard,
    ) -> None:
        if token is not _QUIESCENT_AUTHORITY_TOKEN:
            raise TypeError(
                "CoordinatorStateMachine cannot be constructed directly"
            )
        self._plan = plan
        self._options = options
        self._guard = guard
        self._phase: CoordinatorPhase = "preflight"
        self._workers: dict[LaneName, object] = {}
        self._worker_groups: dict[LaneName, int] = {}
        self._ledger = ProgressAckLedger(
            max_total_tokens=options.max_total_tokens
        )
        self._stop_launches = False
        self._cancel_reason: str | None = None
        self._active_authority: QuiescentRunAuthority | None = None
        self._issued_authorities: list[QuiescentRunAuthority] = []
        self._teardown_receipt: TeardownReceipt | None = None
        self._validated: ValidatedEpoch | None = None
        self._commit: FormalCommitCapability | None = None
        self._resume_snapshot_rows: (
            tuple[tuple[str, str, int, int, str], ...] | None
        ) = None
        self._poisoned = False
        self._poison_error: BaseException | None = None
        self._owner_pid = os.getpid()
        self._run_lease: RunCoordinatorLease | None = None

    @classmethod
    def create(
        cls,
        plan: EpochPlan,
        options: ParallelOptions,
        guard: CoordinatorGuard,
    ) -> "CoordinatorStateMachine":
        if cls is not CoordinatorStateMachine:
            raise TypeError("CoordinatorStateMachine subclasses are unsupported")
        if type(plan) is not EpochPlan:
            raise TypeError("plan must be an exact EpochPlan")
        if type(options) is not ParallelOptions:
            raise TypeError("options must be exact ParallelOptions")
        if type(guard) is not CoordinatorGuard:
            raise TypeError("guard must be an exact CoordinatorGuard")
        guard._validate_nominal()
        run_root = canonical_run_root(options.run_root)
        if (
            options.run_kind != plan.run_kind
            or type(options.run_kind) is not str
            or options.source_codex_home != Path(options.source_codex_home)
            or options.codex_executable != Path(options.codex_executable)
            or (
                options.resume_run_root is not None
                and canonical_run_root(options.resume_run_root) != run_root
            )
            or (
                options.max_total_tokens is not None
                and (
                    type(options.max_total_tokens) is not int
                    or options.max_total_tokens < 0
                )
            )
        ):
            raise ValueError("parallel options differ from the epoch plan")
        return cls(
            _QUIESCENT_AUTHORITY_TOKEN,
            plan=plan,
            options=replace(options, run_root=run_root),
            guard=guard,
        )

    def _require_healthy(self) -> None:
        if self._owner_pid != os.getpid():
            raise RuntimeError("coordinator belongs to another process")
        if self._poisoned:
            raise RuntimeError("coordinator is poisoned") from self._poison_error

    def _bind_run_lease(self, lease: RunCoordinatorLease) -> None:
        self._require_healthy()
        if type(lease) is not RunCoordinatorLease:
            raise TypeError("coordinator requires exact run lease")
        lease._validate_live()
        if (
            lease._run_root != self._options.run_root
            or lease._epoch_id != self._plan.epoch_id
            or lease._run_kind != self._plan.run_kind
        ):
            raise ValueError("coordinator run lease binding differs")
        if self._run_lease is not None and self._run_lease is not lease:
            raise RuntimeError("coordinator run lease was already bound")
        self._run_lease = lease

    @property
    def phase(self) -> CoordinatorPhase:
        self._require_healthy()
        return self._phase

    @property
    def stop_launches(self) -> bool:
        self._require_healthy()
        return self._stop_launches

    @property
    def total_tokens(self) -> int:
        self._require_healthy()
        return self._ledger.total_tokens

    def _seed_resume_protocol(
        self,
        *,
        ledger: ProgressAckLedger,
    ) -> None:
        self._require_healthy()
        if (
            self._phase != "tearing-down"
            or type(ledger) is not ProgressAckLedger
            or self._ledger.total_tokens != 0
            or ledger.max_total_tokens
            != self._options.max_total_tokens
        ):
            raise ValueError("resume protocol cannot seed this coordinator")
        epoch_id, run_kind = ledger.protocol_identity
        if (
            (epoch_id is None) != (run_kind is None)
            or (
                epoch_id is not None
                and (
                    epoch_id != self._plan.epoch_id
                    or run_kind != self._plan.run_kind
                )
            )
            or any(
                active is not None
                for active in ledger.active_cases.values()
            )
        ):
            raise ValueError("resume protocol differs from this coordinator")
        self._ledger = ledger
        self._stop_launches = (
            ledger.stop_launches or ledger.aborted
        )

    def register_worker(self, lane: LaneName, process: subprocess.Popen) -> None:
        self._require_healthy()
        if (
            type(lane) is not str
            or lane not in ("E1", "E2", "E3", "APP")
            or lane in self._workers
        ):
            raise ValueError("worker lane is invalid or already registered")
        if self._phase not in ("preflight", "running"):
            raise RuntimeError("workers cannot launch in the current phase")
        if self._stop_launches:
            raise RuntimeError("worker launches are stopped")
        pid = getattr(process, "pid", None)
        if type(pid) is not int or pid <= 0:
            raise TypeError("worker process must expose a positive PID")
        pgid = getattr(process, "pgid", None)
        if pgid is None:
            try:
                pgid = os.getpgid(pid)
            except OSError:
                pgid = pid
        if type(pgid) is not int or pgid <= 0:
            raise RuntimeError("worker process group is invalid")
        self._workers[lane] = process
        self._worker_groups[lane] = pgid
        self._phase = "running"

    @staticmethod
    def _readers_for(process: object) -> tuple[object, ...]:
        readers = getattr(process, "_coordinator_readers", ())
        if not isinstance(readers, (tuple, list)):
            raise RuntimeError("worker readers are invalid")
        return tuple(readers)

    @staticmethod
    def _join_readers(process: object) -> None:
        for reader in CoordinatorStateMachine._readers_for(process):
            join = getattr(reader, "join", None)
            alive = getattr(reader, "is_alive", None)
            if not callable(join) or not callable(alive):
                raise RuntimeError("worker reader is invalid")
            join(timeout=5.0)
            if alive():
                raise RuntimeError("worker reader survived cancellation")

    @staticmethod
    def _production_group_exists(process: object, pgid: int) -> bool:
        probe = getattr(process, "process_group_alive", None)
        if callable(probe):
            alive = probe()
            if type(alive) is not bool:
                raise RuntimeError("worker process-group probe is invalid")
            return alive
        if not isinstance(process, subprocess.Popen):
            return False
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            raise RuntimeError(
                "worker process-group quiescence is indeterminate"
            ) from None
        return True

    def _stop_worker(self, lane: LaneName, process: object) -> None:
        poll = getattr(process, "poll", None)
        wait = getattr(process, "wait", None)
        if not callable(poll) or not callable(wait):
            raise RuntimeError("worker process interface is invalid")
        pgid = self._worker_groups[lane]
        errors: list[BaseException] = []
        try:
            leader_alive = poll() is None
            group_alive = self._production_group_exists(process, pgid)
            if leader_alive or group_alive:
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                if leader_alive:
                    try:
                        wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        pass
                if self._production_group_exists(process, pgid):
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    deadline = time.monotonic() + 5.0
                    while (
                        self._production_group_exists(process, pgid)
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.01)
                if poll() is None:
                    wait(timeout=5.0)
            if poll() is None or self._production_group_exists(process, pgid):
                raise RuntimeError(
                    "worker process group survived cancellation"
                )
        except BaseException as error:
            errors.append(error)
        try:
            self._join_readers(process)
        except BaseException as error:
            errors.append(error)
        if not errors:
            return
        if len(errors) == 1:
            raise errors[0]
        group_type = (
            ExceptionGroup
            if all(isinstance(error, Exception) for error in errors)
            else BaseExceptionGroup
        )
        raise group_type(
            f"worker {lane} cancellation or reader join failed",
            errors,
        )

    def accept_progress(self, message: ProgressMessage) -> AckDecision:
        self._require_healthy()
        if self._phase != "running":
            raise RuntimeError("progress is invalid outside the running phase")
        try:
            if (
                type(message) is not ProgressMessage
                or message.epoch_id != self._plan.epoch_id
                or message.run_kind != self._plan.run_kind
                or message.lane not in self._workers
            ):
                raise ValueError("progress differs from the coordinator epoch")
            process = self._workers[message.lane]
            worker_root = getattr(process, "worker_root", None)
            if worker_root is None:
                worker_root = (
                    self._options.run_root / "app-server"
                    if message.lane == "APP"
                    else self._options.run_root
                    / "workers"
                    / message.lane
                )
            decision = (
                self._ledger.accept_durable_progress(
                    worker_root=Path(worker_root),
                    message=message,
                )
                if type(self._ledger) is ProgressAckLedger
                else self._ledger.accept_progress(message)
            )
            self._guard.checkpoint(
                f"before ACK {message.lane} sequence {message.seq}"
            )
            write_ack(Path(worker_root), message, decision)
            if decision in ("stop-launches", "abort"):
                self._stop_launches = True
            if decision == "abort":
                self.cancel(
                    f"progress requested abort for {message.lane} "
                    f"sequence {message.seq}"
                )
            return decision
        except BaseException as primary:
            if self._phase == "running":
                try:
                    self.cancel("malformed or unauthenticated progress")
                except BaseException as cancellation_error:
                    group_type = (
                        ExceptionGroup
                        if isinstance(primary, Exception)
                        and isinstance(cancellation_error, Exception)
                        else BaseExceptionGroup
                    )
                    raise group_type(
                        "progress failure and cancellation failure",
                        [primary, cancellation_error],
                    )
            raise

    def cancel(self, reason: str) -> None:
        self._require_healthy()
        if type(reason) is not str or not reason.strip():
            raise ValueError("cancellation reason must be nonempty")
        if self._phase in (
            "tearing-down",
            "validating",
            "validated",
            "commit-ready",
            "committed",
            "failed",
        ):
            raise RuntimeError("coordinator cannot cancel in the current phase")
        if self._phase == "cancelling":
            return
        self._phase = "cancelling"
        self._stop_launches = True
        self._cancel_reason = reason
        errors: list[BaseException] = []
        if self._run_lease is not None:
            try:
                self._run_lease._validate_live()
                ownership = (
                    self._options.run_root
                    / "coordinator/cleanup/bootstrap-ownership.json"
                )
                if _entry_exists_no_follow(
                    ownership, "bootstrap ownership"
                ):
                    _atomic_write_record(
                        self._options.run_root
                        / "coordinator/stop-launches.json",
                        {
                            "schema_version": 1,
                            "epoch_id": self._plan.epoch_id,
                            "run_kind": self._plan.run_kind,
                            "reason_sha256": hashlib.sha256(
                                reason.encode("utf-8")
                            ).hexdigest(),
                        },
                    )
            except BaseException as error:
                errors.append(error)
        for lane in ("E1", "E2", "E3", "APP"):
            process = self._workers.get(lane)
            if process is None:
                continue
            try:
                self._stop_worker(lane, process)
            except BaseException as error:
                errors.append(error)
        if errors:
            failure: BaseException
            if len(errors) == 1:
                failure = errors[0]
            else:
                group_type = (
                    ExceptionGroup
                    if all(isinstance(error, Exception) for error in errors)
                    else BaseExceptionGroup
                )
                failure = group_type("worker cancellation failed", errors)
            if is_indeterminate_descriptor_close(failure):
                self._poisoned = True
                self._poison_error = failure
            raise failure

    def workers_stopped(self) -> QuiescentRunAuthority:
        self._require_healthy()
        if self._phase not in ("preflight", "running", "cancelling"):
            raise RuntimeError("workers cannot become quiescent in this phase")
        if self._run_lease is None:
            raise RuntimeError(
                "quiescence requires a live run lease bound to the coordinator"
            )
        self._run_lease._validate_live()
        if (
            self._run_lease._run_root != self._options.run_root
            or self._run_lease._epoch_id != self._plan.epoch_id
            or self._run_lease._run_kind != self._plan.run_kind
        ):
            raise RuntimeError("coordinator run lease binding changed")
        if (
            self._active_authority is not None
            and not self._active_authority._consumed
        ):
            raise RuntimeError("quiescent authority was already issued")
        for process in self._workers.values():
            poll = getattr(process, "poll", None)
            if not callable(poll) or poll() is None:
                raise RuntimeError("worker is still running")
            lane = getattr(process, "lane", None)
            if lane is None:
                lane = next(
                    key
                    for key, candidate in self._workers.items()
                    if candidate is process
                )
            if self._production_group_exists(
                process, self._worker_groups[lane]
            ):
                raise RuntimeError("worker process group is still running")
            self._join_readers(process)
        _recover_durable_worker_groups(
            run_root=self._options.run_root,
            plan=self._plan,
        )
        authority = QuiescentRunAuthority(
            _QUIESCENT_AUTHORITY_TOKEN,
            coordinator=self,
        )
        self._issued_authorities.append(authority)
        authority._bind_lease(self._run_lease)
        self._active_authority = authority
        self._phase = "tearing-down"
        return authority

    def _resume_to_launch(self, authority: QuiescentRunAuthority) -> None:
        self._require_healthy()
        if type(authority) is not QuiescentRunAuthority:
            raise TypeError("resume requires exact quiescent authority")
        if (
            self._resume_snapshot_rows is None
            or _capture_directory_rows(self._options.run_root)
            != self._resume_snapshot_rows
        ):
            raise RuntimeError(
                "resume evidence changed before launch linearization"
            )
        authority._consume("resume-launch")
        self._resume_snapshot_rows = None
        self._workers.clear()
        self._worker_groups.clear()
        self._phase = "preflight"

    def begin_teardown(self) -> None:
        self._require_healthy()
        if self._phase != "tearing-down":
            raise RuntimeError("teardown requires quiescent workers")
        if (
            self._active_authority is None
            or self._active_authority._consumed
        ):
            raise RuntimeError("teardown requires live quiescent authority")
        self._active_authority._validate_live()

    def mark_torn_down(self, receipt: TeardownReceipt) -> None:
        self._require_healthy()
        if self._phase != "tearing-down":
            raise RuntimeError("teardown receipt is invalid in this phase")
        if type(receipt) is not TeardownReceipt:
            raise TypeError("receipt must be exact TeardownReceipt")
        if (
            receipt.epoch_id != self._plan.epoch_id
            or receipt.run_kind != self._plan.run_kind
            or self._active_authority is not None
        ):
            raise ValueError("teardown receipt differs from coordinator state")
        self._teardown_receipt = receipt
        if self._cancel_reason is not None:
            self._phase = "failed"

    def begin_validation(self) -> None:
        self._require_healthy()
        if self._phase != "tearing-down" or self._teardown_receipt is None:
            raise RuntimeError("validation requires committed teardown")
        self._phase = "validating"

    def mark_validated(self, validated: ValidatedEpoch) -> None:
        self._require_healthy()
        if self._phase != "validating" or type(validated) is not ValidatedEpoch:
            raise RuntimeError("validated capability is invalid in this phase")
        binding = validated._validate_nominal()
        if binding.plan is not self._plan:
            raise ValueError("validated epoch differs from coordinator plan")
        self._validated = validated
        self._phase = "validated"

    def mark_commit_ready(self, commit: FormalCommitCapability) -> None:
        self._require_healthy()
        if (
            self._phase != "validated"
            or self._plan.run_kind != "formal"
            or type(commit) is not FormalCommitCapability
        ):
            raise RuntimeError("formal commit is invalid in this phase")
        binding = commit._validate_nominal()
        if self._validated is None or binding.validated_ref() is not self._validated:
            raise ValueError("formal commit differs from validated epoch")
        self._commit = commit
        self._phase = "commit-ready"

    def mark_committed(self) -> None:
        self._require_healthy()
        if (
            self._phase != "commit-ready"
            or self._commit is None
            or not self._commit.consumed
        ):
            raise RuntimeError("commit completion is invalid in this phase")
        self._phase = "committed"


def _validate_quiescent_cleanup_access(
    *,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
    plan: EpochPlan | None = None,
    run_root: Path | None = None,
) -> CoordinatorStateMachine:
    if type(lease) is not RunCoordinatorLease:
        raise TypeError("cleanup requires exact RunCoordinatorLease")
    if type(authority) is not QuiescentRunAuthority:
        raise TypeError("cleanup requires exact QuiescentRunAuthority")
    lease._validate_live()
    authority._bind_lease(lease)
    coordinator = authority._validate_live()
    if (
        lease._epoch_id != authority.epoch_id
        or lease._run_kind != authority.run_kind
        or lease._run_root != coordinator._options.run_root
    ):
        raise RuntimeError("cleanup lease and quiescent authority differ")
    if plan is not None and (
        type(plan) is not EpochPlan
        or plan is not coordinator._plan
        or plan.epoch_id != lease._epoch_id
        or plan.run_kind != lease._run_kind
    ):
        raise ValueError("cleanup plan differs from live authority")
    if run_root is not None and canonical_run_root(run_root) != lease._run_root:
        raise ValueError("cleanup run root differs from live lease")
    return coordinator


def _poison_quiescent_on_indeterminate(
    authority: QuiescentRunAuthority,
    error: BaseException,
) -> None:
    if is_indeterminate_descriptor_close(error):
        global _LEASE_PROCESS_POISON
        _LEASE_PROCESS_POISON = error
        authority._poison(error)


def _scrub_coordinator_owned_directory(descriptor: int) -> None:
    from scripts.run_observing_workflows_eval_worker import _remove_tree_entry
    from scripts.run_observing_workflows_task9_eval import (
        AUTH_CLEANUP_MAX_ENTRIES,
    )

    remaining = [AUTH_CLEANUP_MAX_ENTRIES - 1]
    if remaining[0] < 0:
        raise OSError("owned auth cleanup bound exceeded")
    names: list[str] = []
    with os.scandir(descriptor) as entries:
        for entry in entries:
            if remaining[0] <= 0:
                raise OSError("owned auth cleanup bound exceeded")
            remaining[0] -= 1
            names.append(entry.name)
    for name in sorted(names):
        _remove_tree_entry(
            descriptor,
            name,
            depth=1,
            remaining=remaining,
            charged=True,
        )
    with os.scandir(descriptor) as entries:
        if any(True for _entry in entries):
            raise ValueError("owned auth directory is not empty after scrub")
    os.fsync(descriptor)


def _case_root_matches(
    paths: CasePaths, ownership: CaseAuthOwnership
) -> os.stat_result:
    try:
        metadata = paths.root.lstat()
    except OSError:
        raise ValueError("case evidence root is unavailable") from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_dev, metadata.st_ino)
        != (ownership.case_root_device, ownership.case_root_inode)
    ):
        raise ValueError("case evidence root ownership changed")
    return metadata


def _case_home_binding(
    paths: CasePaths, ownership: CaseAuthOwnership
) -> Literal["expected", "missing", "replaced"]:
    try:
        metadata = paths.codex_home.lstat()
    except FileNotFoundError:
        return "missing"
    except OSError:
        raise ValueError("case Codex home is unavailable") from None
    if (
        not stat.S_ISLNK(metadata.st_mode)
        and stat.S_ISDIR(metadata.st_mode)
        and stat.S_IMODE(metadata.st_mode) == 0o700
        and (metadata.st_dev, metadata.st_ino)
        == (ownership.codex_home_device, ownership.codex_home_inode)
    ):
        return "expected"
    return "replaced"


def _open_recovery_case_home(
    paths: CasePaths, ownership: CaseAuthOwnership
) -> _DescriptorSlot:
    _case_root_matches(paths, ownership)
    descriptor, metadata = _open_private_directory(
        paths.codex_home, "case Codex home"
    )
    slot = _DescriptorSlot(descriptor)
    if (metadata.st_dev, metadata.st_ino) != (
        ownership.codex_home_device,
        ownership.codex_home_inode,
    ):
        _retire_task_descriptors(
            [slot],
            primary=ValueError("case Codex-home ownership changed"),
            label="case recovery open or close failed",
        )
    return slot


def recover_case_auth_cleanup(
    *,
    plan: EpochPlan,
    assignment: CaseAssignment,
    paths: CasePaths,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> TombstoneReceipt:
    _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        plan=plan,
        run_root=paths.root.parent.parent,
    )
    _validate_plan_assignment(plan, assignment)
    if paths != paths_for_case(lease._run_root, assignment):
        raise ValueError("case recovery paths differ from the plan")
    initial_inventory = _directory_inventory(
        paths.cleanup, "case cleanup directory"
    )
    if initial_inventory not in (
        ("ownership.json",),
        ("ownership.json", "tombstone.json"),
    ):
        raise ValueError("case cleanup namespace is invalid")
    existing = _read_optional_verified_tombstone_receipt(
        plan=plan,
        assignment=assignment,
        paths=paths,
    )
    if existing is not None:
        ownership, _ = read_case_auth_ownership(
            plan=plan, assignment=assignment, paths=paths
        )
        binding = _case_home_binding(paths, ownership)
        if existing.receipt.canonical_binding != "expected":
            raise ValueError("case tombstone does not authorize teardown")
        if binding == "replaced":
            raise ValueError("case Codex-home name was replaced")
        if binding == "expected":
            descriptor, metadata = _open_private_directory(
                paths.codex_home, "case Codex-home tombstone"
            )
            slot = _DescriptorSlot(descriptor)
            primary: BaseException | None = None
            try:
                if (metadata.st_dev, metadata.st_ino) != (
                    ownership.codex_home_device,
                    ownership.codex_home_inode,
                ):
                    raise ValueError("case Codex-home tombstone changed")
                with os.scandir(slot.descriptor) as entries:
                    if any(True for _entry in entries):
                        raise ValueError("case Codex-home tombstone is not empty")
            except BaseException as error:
                primary = error
            try:
                _retire_task_descriptors(
                    [slot],
                    primary=primary,
                    label="case tombstone verification or close failed",
                )
            except BaseException as error:
                _poison_quiescent_on_indeterminate(authority, error)
                raise
        return existing.receipt

    ownership, ownership_bytes = read_case_auth_ownership(
        plan=plan, assignment=assignment, paths=paths
    )
    if _case_home_binding(paths, ownership) != "expected":
        raise ValueError("case recovery requires the recorded Codex home")
    slot = _open_recovery_case_home(paths, ownership)
    primary: BaseException | None = None
    receipt: TombstoneReceipt | None = None
    try:
        _scrub_coordinator_owned_directory(slot.descriptor)
        current = os.fstat(slot.descriptor)
        if (
            (current.st_dev, current.st_ino)
            != (ownership.codex_home_device, ownership.codex_home_inode)
            or stat.S_IMODE(current.st_mode) != 0o700
        ):
            raise ValueError("case recovery Codex-home identity changed")
        receipt = TombstoneReceipt(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=ownership.case_root_device,
            case_root_inode=ownership.case_root_inode,
            codex_home_device=ownership.codex_home_device,
            codex_home_inode=ownership.codex_home_inode,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="coordinator-recovery",
        )
        _atomic_write_record(paths.cleanup / "tombstone.json", asdict(receipt))
    except BaseException as error:
        primary = error
    try:
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label="case recovery scrub or close failed",
        )
    except BaseException as error:
        _poison_quiescent_on_indeterminate(authority, error)
        raise
    if receipt is None:
        raise AssertionError("case recovery produced no tombstone")
    if _directory_inventory(
        paths.cleanup, "case cleanup directory"
    ) != ("ownership.json", "tombstone.json"):
        raise RuntimeError("case cleanup namespace changed during recovery")
    return receipt


def teardown_case_auth(
    *,
    paths: CasePaths,
    receipt: TombstoneReceipt,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> None:
    coordinator = _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        run_root=paths.root.parent.parent,
    )
    if type(receipt) is not TombstoneReceipt:
        raise TypeError("case teardown requires exact TombstoneReceipt")
    assignments = [
        assignment
        for assignment in coordinator._plan.assignments
        if assignment.key == receipt.case
    ]
    if len(assignments) != 1:
        raise ValueError("case teardown receipt names an unknown case")
    assignment = assignments[0]
    if paths != paths_for_case(lease._run_root, assignment):
        raise ValueError("case teardown paths differ from the plan")
    if _directory_inventory(
        paths.cleanup, "case cleanup directory"
    ) != ("ownership.json", "tombstone.json"):
        raise ValueError("case cleanup namespace is invalid")
    verified = read_verified_tombstone_receipt(
        plan=coordinator._plan,
        assignment=assignment,
        paths=paths,
    )
    if (
        verified.receipt != receipt
        or receipt.canonical_binding != "expected"
    ):
        raise ValueError("case tombstone does not authorize teardown")
    ownership, _ = read_case_auth_ownership(
        plan=coordinator._plan,
        assignment=assignment,
        paths=paths,
    )
    _case_root_matches(paths, ownership)
    binding = _case_home_binding(paths, ownership)
    if binding == "missing":
        return
    if binding != "expected":
        raise ValueError("case Codex-home name was replaced")

    root_descriptor, root_metadata = _open_private_directory(
        paths.root, "case evidence root"
    )
    root_slot = _DescriptorSlot(root_descriptor)
    child_slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    try:
        if (root_metadata.st_dev, root_metadata.st_ino) != (
            ownership.case_root_device,
            ownership.case_root_inode,
        ):
            raise ValueError("case evidence root ownership changed")
        child_descriptor, child_metadata = _open_private_directory(
            paths.codex_home, "case Codex-home tombstone"
        )
        child_slot = _DescriptorSlot(child_descriptor)
        if (child_metadata.st_dev, child_metadata.st_ino) != (
            ownership.codex_home_device,
            ownership.codex_home_inode,
        ):
            raise ValueError("case Codex-home tombstone ownership changed")
        with os.scandir(child_slot.descriptor) as entries:
            if any(True for _entry in entries):
                raise ValueError("case Codex-home tombstone is not empty")
    except BaseException as error:
        primary = error
    if child_slot is not None:
        try:
            _retire_task_descriptors(
                [child_slot],
                primary=primary,
                label="case tombstone verification or close failed",
            )
            primary = None
        except BaseException as error:
            _poison_quiescent_on_indeterminate(authority, error)
            primary = error
    if primary is None:
        try:
            named = os.stat(
                paths.codex_home.name,
                dir_fd=root_slot.descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(named.st_mode)
                or (named.st_dev, named.st_ino)
                != (ownership.codex_home_device, ownership.codex_home_inode)
            ):
                raise ValueError("case Codex-home tombstone changed")
            os.rmdir(paths.codex_home.name, dir_fd=root_slot.descriptor)
            os.fsync(root_slot.descriptor)
        except BaseException as error:
            primary = error
    try:
        _retire_task_descriptors(
            [root_slot],
            primary=primary,
            label="case tombstone teardown or close failed",
        )
    except BaseException as error:
        _poison_quiescent_on_indeterminate(authority, error)
        raise


def _read_bootstrap_ownership_record(
    *, coordinator_root: Path, plan: EpochPlan
) -> tuple[BootstrapOwnership, bytes]:
    payload, content = _read_canonical_record(
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
    return ownership, content


def _bootstrap_binding(
    coordinator_root: Path, ownership: BootstrapOwnership
) -> Literal["expected", "missing", "replaced"]:
    path = Path(coordinator_root) / "auth-bootstrap"
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return "missing"
    except OSError:
        raise ValueError("auth bootstrap is unavailable") from None
    if (
        not stat.S_ISLNK(metadata.st_mode)
        and stat.S_ISDIR(metadata.st_mode)
        and stat.S_IMODE(metadata.st_mode) == 0o700
        and (metadata.st_dev, metadata.st_ino)
        == (ownership.bootstrap_device, ownership.bootstrap_inode)
    ):
        return "expected"
    return "replaced"


def _reopen_auth_bootstrap(
    *, plan: EpochPlan, coordinator_root: Path
) -> InstalledAuthBootstrap:
    ownership, _ownership_bytes = _read_bootstrap_ownership_record(
        coordinator_root=coordinator_root, plan=plan
    )
    if _directory_inventory(
        Path(coordinator_root) / "cleanup",
        "coordinator cleanup directory",
    ) != ("bootstrap-ownership.json",):
        raise ValueError("active bootstrap cleanup namespace is invalid")
    if _bootstrap_binding(coordinator_root, ownership) != "expected":
        raise ValueError("active bootstrap ownership changed")
    descriptor, metadata = _open_private_directory(
        Path(coordinator_root) / "auth-bootstrap",
        "active auth bootstrap",
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    try:
        if (metadata.st_dev, metadata.st_ino) != (
            ownership.bootstrap_device,
            ownership.bootstrap_inode,
        ):
            raise ValueError("active bootstrap ownership changed")
        auth_descriptor = _validate_private_auth(
            Path(coordinator_root) / "auth-bootstrap/auth.json",
            "active bootstrap auth.json",
        )
        _retire_task_descriptors(
            [_DescriptorSlot(auth_descriptor)],
            primary=None,
            label="active bootstrap auth close failed",
        )
    except BaseException as error:
        primary = error
    if primary is not None:
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label="active bootstrap reopen or close failed",
        )
    return InstalledAuthBootstrap(
        path=Path(coordinator_root) / "auth-bootstrap",
        ownership=ownership,
        descriptor=slot.descriptor,
        state="active",
        descriptor_close_state="owned",
        descriptor_close_error=None,
    )


def _write_bootstrap_tombstone(
    *,
    coordinator_root: Path,
    plan: EpochPlan,
    ownership: BootstrapOwnership,
    ownership_bytes: bytes,
    producer: Literal["coordinator", "coordinator-recovery"],
) -> BootstrapTombstoneReceipt:
    receipt = BootstrapTombstoneReceipt(
        schema_version=1,
        epoch_id=plan.epoch_id,
        run_kind=plan.run_kind,
        ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
        bootstrap_device=ownership.bootstrap_device,
        bootstrap_inode=ownership.bootstrap_inode,
        scrubbed=True,
        empty=True,
        canonical_binding="expected",
        producer=producer,
    )
    _atomic_write_record(
        Path(coordinator_root) / "cleanup/bootstrap-tombstone.json",
        asdict(receipt),
    )
    return receipt


def cleanup_auth_bootstrap(
    *,
    installed: InstalledAuthBootstrap,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> BootstrapTombstoneReceipt:
    coordinator = _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
    )
    if type(installed) is not InstalledAuthBootstrap:
        raise TypeError("bootstrap cleanup requires exact InstalledAuthBootstrap")
    coordinator_root = lease._run_root / "coordinator"
    if _directory_inventory(
        coordinator_root / "cleanup",
        "coordinator cleanup directory",
    ) != ("bootstrap-ownership.json",):
        raise ValueError("coordinator cleanup namespace is invalid")
    ownership, ownership_bytes = _read_bootstrap_ownership_record(
        coordinator_root=coordinator_root,
        plan=coordinator._plan,
    )
    if installed.ownership != ownership or installed.path != (
        coordinator_root / "auth-bootstrap"
    ):
        raise ValueError("installed bootstrap ownership changed")
    if installed.state != "active":
        raise RuntimeError("installed bootstrap is not active")
    if (
        installed.descriptor_close_state != "owned"
        or installed.descriptor < 0
    ):
        raise RuntimeError("installed bootstrap descriptor is unavailable")
    current = os.fstat(installed.descriptor)
    if (
        stat.S_IMODE(current.st_mode) != 0o700
        or (current.st_dev, current.st_ino)
        != (ownership.bootstrap_device, ownership.bootstrap_inode)
        or _bootstrap_binding(coordinator_root, ownership) != "expected"
    ):
        raise ValueError("installed bootstrap ownership changed")
    installed.state = "scrubbing"
    primary: BaseException | None = None
    receipt: BootstrapTombstoneReceipt | None = None
    try:
        _scrub_coordinator_owned_directory(installed.descriptor)
        receipt = _write_bootstrap_tombstone(
            coordinator_root=coordinator_root,
            plan=coordinator._plan,
            ownership=ownership,
            ownership_bytes=ownership_bytes,
            producer="coordinator",
        )
        installed.state = "tombstoned"
    except BaseException as error:
        installed.state = "active"
        primary = error
    if (
        primary is not None
        and not is_indeterminate_descriptor_close(primary)
    ):
        raise primary
    close_error = _retire_descriptor_capability(installed)
    errors = ([primary] if primary is not None else []) + (
        [close_error] if close_error is not None else []
    )
    if errors:
        failure: BaseException
        if len(errors) == 1:
            failure = errors[0]
        else:
            group_type = (
                ExceptionGroup
                if all(isinstance(error, Exception) for error in errors)
                else BaseExceptionGroup
            )
            failure = group_type("bootstrap cleanup failed", errors)
        _poison_quiescent_on_indeterminate(authority, failure)
        raise failure
    if receipt is None:
        raise AssertionError("bootstrap cleanup produced no tombstone")
    if _directory_inventory(
        coordinator_root / "cleanup",
        "coordinator cleanup directory",
    ) != (
        "bootstrap-ownership.json",
        "bootstrap-tombstone.json",
    ):
        raise RuntimeError("coordinator cleanup namespace changed")
    return receipt


def recover_auth_bootstrap_cleanup(
    *,
    plan: EpochPlan,
    coordinator_root: Path,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> BootstrapTombstoneReceipt:
    _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        plan=plan,
        run_root=Path(coordinator_root).parent,
    )
    coordinator_root = Path(coordinator_root)
    if coordinator_root != lease._run_root / "coordinator":
        raise ValueError("coordinator cleanup root differs from live lease")
    ownership, ownership_bytes = _read_bootstrap_ownership_record(
        coordinator_root=coordinator_root,
        plan=plan,
    )
    cleanup_inventory = _directory_inventory(
        coordinator_root / "cleanup",
        "coordinator cleanup directory",
    )
    if cleanup_inventory not in (
        ("bootstrap-ownership.json",),
        ("bootstrap-ownership.json", "bootstrap-tombstone.json"),
    ):
        raise ValueError("coordinator cleanup namespace is invalid")
    tombstone = coordinator_root / "cleanup/bootstrap-tombstone.json"
    try:
        tombstone.lstat()
    except FileNotFoundError:
        existing = None
    except OSError:
        raise ValueError("bootstrap tombstone is unavailable") from None
    else:
        existing, _digest = _decode_bootstrap_tombstone(
            plan=plan, run_root=lease._run_root
        )
    if existing is not None:
        binding = _bootstrap_binding(coordinator_root, ownership)
        if binding == "replaced":
            raise ValueError("auth bootstrap name was replaced")
        if binding == "expected":
            descriptor, metadata = _open_private_directory(
                coordinator_root / "auth-bootstrap",
                "auth bootstrap tombstone",
            )
            slot = _DescriptorSlot(descriptor)
            primary: BaseException | None = None
            try:
                if (metadata.st_dev, metadata.st_ino) != (
                    ownership.bootstrap_device,
                    ownership.bootstrap_inode,
                ):
                    raise ValueError("auth bootstrap tombstone changed")
                with os.scandir(slot.descriptor) as entries:
                    if any(True for _entry in entries):
                        raise ValueError("auth bootstrap tombstone is not empty")
            except BaseException as error:
                primary = error
            try:
                _retire_task_descriptors(
                    [slot],
                    primary=primary,
                    label="bootstrap verification or close failed",
                )
            except BaseException as error:
                _poison_quiescent_on_indeterminate(authority, error)
                raise
        return existing
    if _bootstrap_binding(coordinator_root, ownership) != "expected":
        raise ValueError("bootstrap recovery requires recorded ownership")
    descriptor, metadata = _open_private_directory(
        coordinator_root / "auth-bootstrap",
        "auth bootstrap",
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    receipt: BootstrapTombstoneReceipt | None = None
    try:
        if (metadata.st_dev, metadata.st_ino) != (
            ownership.bootstrap_device,
            ownership.bootstrap_inode,
        ):
            raise ValueError("auth bootstrap ownership changed")
        _scrub_coordinator_owned_directory(slot.descriptor)
        receipt = _write_bootstrap_tombstone(
            coordinator_root=coordinator_root,
            plan=plan,
            ownership=ownership,
            ownership_bytes=ownership_bytes,
            producer="coordinator-recovery",
        )
    except BaseException as error:
        primary = error
    try:
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label="bootstrap recovery or close failed",
        )
    except BaseException as error:
        _poison_quiescent_on_indeterminate(authority, error)
        raise
    if receipt is None:
        raise AssertionError("bootstrap recovery produced no tombstone")
    if _directory_inventory(
        coordinator_root / "cleanup",
        "coordinator cleanup directory",
    ) != (
        "bootstrap-ownership.json",
        "bootstrap-tombstone.json",
    ):
        raise RuntimeError("coordinator cleanup namespace changed")
    return receipt


def teardown_auth_bootstrap(
    *,
    coordinator_root: Path,
    receipt: BootstrapTombstoneReceipt,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> None:
    coordinator = _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        run_root=Path(coordinator_root).parent,
    )
    if type(receipt) is not BootstrapTombstoneReceipt:
        raise TypeError(
            "bootstrap teardown requires exact BootstrapTombstoneReceipt"
        )
    coordinator_root = Path(coordinator_root)
    if coordinator_root != lease._run_root / "coordinator":
        raise ValueError("coordinator cleanup root differs from live lease")
    if _directory_inventory(
        coordinator_root / "cleanup",
        "coordinator cleanup directory",
    ) != (
        "bootstrap-ownership.json",
        "bootstrap-tombstone.json",
    ):
        raise ValueError("coordinator cleanup namespace is invalid")
    durable, _digest = _decode_bootstrap_tombstone(
        plan=coordinator._plan, run_root=lease._run_root
    )
    if durable != receipt or receipt.canonical_binding != "expected":
        raise ValueError("bootstrap tombstone does not authorize teardown")
    ownership, _ = _read_bootstrap_ownership_record(
        coordinator_root=coordinator_root,
        plan=coordinator._plan,
    )
    binding = _bootstrap_binding(coordinator_root, ownership)
    if binding == "missing":
        return
    if binding != "expected":
        raise ValueError("auth bootstrap name was replaced")
    coordinator_descriptor, coordinator_metadata = _open_private_directory(
        coordinator_root, "coordinator root"
    )
    coordinator_slot = _DescriptorSlot(coordinator_descriptor)
    bootstrap_slot: _DescriptorSlot | None = None
    primary: BaseException | None = None
    try:
        bootstrap_descriptor, bootstrap_metadata = _open_private_directory(
            coordinator_root / "auth-bootstrap",
            "auth bootstrap tombstone",
        )
        bootstrap_slot = _DescriptorSlot(bootstrap_descriptor)
        if (bootstrap_metadata.st_dev, bootstrap_metadata.st_ino) != (
            ownership.bootstrap_device,
            ownership.bootstrap_inode,
        ):
            raise ValueError("auth bootstrap tombstone changed")
        with os.scandir(bootstrap_slot.descriptor) as entries:
            if any(True for _entry in entries):
                raise ValueError("auth bootstrap tombstone is not empty")
    except BaseException as error:
        primary = error
    if bootstrap_slot is not None:
        try:
            _retire_task_descriptors(
                [bootstrap_slot],
                primary=primary,
                label="bootstrap tombstone verification or close failed",
            )
            primary = None
        except BaseException as error:
            _poison_quiescent_on_indeterminate(authority, error)
            primary = error
    if primary is None:
        try:
            named = os.stat(
                "auth-bootstrap",
                dir_fd=coordinator_slot.descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISDIR(named.st_mode)
                or (named.st_dev, named.st_ino)
                != (ownership.bootstrap_device, ownership.bootstrap_inode)
            ):
                raise ValueError("auth bootstrap tombstone changed")
            os.rmdir(
                "auth-bootstrap", dir_fd=coordinator_slot.descriptor
            )
            os.fsync(coordinator_slot.descriptor)
            if (coordinator_metadata.st_dev, coordinator_metadata.st_ino) != (
                os.fstat(coordinator_slot.descriptor).st_dev,
                os.fstat(coordinator_slot.descriptor).st_ino,
            ):
                raise RuntimeError("coordinator root changed during teardown")
        except BaseException as error:
            primary = error
    try:
        _retire_task_descriptors(
            [coordinator_slot],
            primary=primary,
            label="bootstrap teardown or close failed",
        )
    except BaseException as error:
        _poison_quiescent_on_indeterminate(authority, error)
        raise


def write_teardown_receipt(
    *,
    plan: EpochPlan,
    run_root: Path,
    tombstones: Sequence[tuple[CaseKey, TombstoneReceipt]],
    bootstrap: BootstrapTombstoneReceipt,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> TeardownReceipt:
    _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        plan=plan,
        run_root=run_root,
    )
    if type(bootstrap) is not BootstrapTombstoneReceipt:
        raise TypeError("teardown requires exact bootstrap tombstone")
    if _directory_inventory(
        lease._run_root / "coordinator/cleanup",
        "coordinator cleanup directory",
    ) != (
        "bootstrap-ownership.json",
        "bootstrap-tombstone.json",
    ):
        raise ValueError("coordinator cleanup namespace is invalid")
    bindings = tuple(tombstones)
    keys = tuple(key for key, _receipt in bindings)
    plan_order = tuple(
        assignment.key
        for assignment in plan.assignments
        if assignment.key in set(keys)
    )
    if (
        keys != plan_order
        or len(set(keys)) != len(keys)
        or any(
            type(key) is not CaseKey
            or type(receipt) is not TombstoneReceipt
            or receipt.case != key
            or receipt.epoch_id != plan.epoch_id
            or receipt.run_kind != plan.run_kind
            or receipt.canonical_binding != "expected"
            for key, receipt in bindings
        )
    ):
        raise ValueError("teardown tombstone bindings are invalid")
    verified_bindings: list[tuple[CaseKey, str]] = []
    for key, receipt in bindings:
        assignment = next(
            item for item in plan.assignments if item.key == key
        )
        paths = paths_for_case(lease._run_root, assignment)
        verified = read_verified_tombstone_receipt(
            plan=plan, assignment=assignment, paths=paths
        )
        try:
            paths.codex_home.lstat()
        except FileNotFoundError:
            codex_home_absent = True
        except OSError:
            raise ValueError("case Codex-home absence is indeterminate") from None
        else:
            codex_home_absent = False
        ownership, _ownership_bytes = read_case_auth_ownership(
            plan=plan,
            assignment=assignment,
            paths=paths,
        )
        _case_root_matches(paths, ownership)
        if verified.receipt != receipt or not codex_home_absent:
            raise ValueError("case teardown is incomplete")
        verified_bindings.append((key, verified.sha256))
    bound_keys = set(keys)
    for assignment in plan.assignments:
        if assignment.key in bound_keys:
            continue
        paths = paths_for_case(lease._run_root, assignment)
        try:
            paths.codex_home.lstat()
        except FileNotFoundError:
            pass
        except OSError:
            raise ValueError(
                "case Codex-home absence is indeterminate"
            ) from None
        else:
            raise ValueError(
                "unowned case Codex home remains before teardown"
            )
    durable_bootstrap, bootstrap_sha256 = _decode_bootstrap_tombstone(
        plan=plan, run_root=lease._run_root
    )
    try:
        (lease._run_root / "coordinator/auth-bootstrap").lstat()
    except FileNotFoundError:
        bootstrap_absent = True
    except OSError:
        raise ValueError("bootstrap absence is indeterminate") from None
    else:
        bootstrap_absent = False
    if durable_bootstrap != bootstrap or not bootstrap_absent:
        raise ValueError("bootstrap teardown is incomplete")
    receipt = TeardownReceipt(
        schema_version=1,
        epoch_id=plan.epoch_id,
        run_kind=plan.run_kind,
        tombstone_receipts=tuple(verified_bindings),
        bootstrap_tombstone_receipt_sha256=bootstrap_sha256,
        codex_homes_absent=True,
        bootstrap_absent=True,
    )
    receipt_path = lease._run_root / "coordinator/teardown.json"
    try:
        receipt_path.lstat()
    except FileNotFoundError:
        existing = None
    except OSError:
        raise ValueError("coordinator teardown receipt is unavailable") from None
    else:
        payload, content = _read_canonical_record(
            receipt_path,
            "coordinator teardown receipt",
            byte_cap=64 * 1024,
        )
        _require_exact_fields(
            payload, TeardownReceipt, "coordinator teardown receipt"
        )
        if content != canonical_config_bytes(asdict(receipt)):
            raise ValueError(
                "existing coordinator teardown receipt differs"
            )
        existing = receipt
    authority._consume("teardown-receipt")
    if existing is not None:
        return existing
    try:
        _atomic_write_record(
            receipt_path,
            asdict(receipt),
        )
    except BaseException as error:
        _poison_quiescent_on_indeterminate(authority, error)
        raise
    return receipt


def _classify_resume_under_quiescence(
    *,
    plan: EpochPlan,
    run_root: Path,
    current_fingerprints: InputFingerprints,
    manifests: dict[EvalMode, list[dict[str, object]]],
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> ResumePlan:
    _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        plan=plan,
        run_root=run_root,
    )
    before = _capture_directory_rows(run_root)
    resume = plan_resume(
        plan=plan,
        run_root=run_root,
        current_fingerprints=current_fingerprints,
        manifests=manifests,
    )
    after = _capture_directory_rows(run_root)
    if after != before:
        raise RuntimeError("resume evidence changed during classification")
    coordinator = authority._validate_live()
    coordinator._resume_snapshot_rows = after
    return resume


def _resume_protocol_bindings(
    *,
    plan: EpochPlan,
    run_root: Path,
    manifests: dict[EvalMode, list[dict[str, object]]],
    max_total_tokens: int | None,
) -> ProgressAckLedger:
    if (
        type(plan) is not EpochPlan
        or type(run_root) is not type(Path("."))
        or not run_root.is_absolute()
        or type(manifests) is not dict
    ):
        raise TypeError("resume protocol replay arguments are invalid")
    lane_records: dict[
        LaneName, tuple[tuple[Path, ProgressMessage, Ack], ...]
    ] = {}
    for lane in ("E1", "E2", "E3", "APP"):
        worker_root = (
            Path(run_root) / "app-server"
            if lane == "APP"
            else Path(run_root) / "workers" / lane
        )
        progress_root = worker_root / "progress"
        try:
            metadata = progress_root.lstat()
        except FileNotFoundError:
            try:
                ack_metadata = (worker_root / "acks").lstat()
            except FileNotFoundError:
                pass
            except OSError:
                raise ValueError("resume ACK prefix is unavailable") from None
            else:
                if (
                    stat.S_ISLNK(ack_metadata.st_mode)
                    or not stat.S_ISDIR(ack_metadata.st_mode)
                    or _directory_inventory(
                        worker_root / "acks", "resume ACK prefix"
                    )
                ):
                    raise ValueError("resume ACK prefix lacks progress")
            lane_records[lane] = ()
            continue
        except OSError:
            raise ValueError("resume progress is unavailable") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(
            metadata.st_mode
        ):
            raise ValueError("resume progress is unsafe")
        progress_names: list[str] = []
        with os.scandir(progress_root) as entries:
            for entry in entries:
                if len(progress_names) >= MAX_PROTOCOL_RECORDS:
                    raise ValueError(
                        "resume progress prefix exceeds its cap"
                    )
                progress_names.append(entry.name)
        names = tuple(sorted(progress_names))
        expected = tuple(
            f"{sequence:06d}.json"
            for sequence in range(1, len(names) + 1)
        )
        if names != expected or len(names) > MAX_PROTOCOL_RECORDS:
            raise ValueError("resume progress prefix is invalid")
        ack_root = worker_root / "acks"
        try:
            ack_metadata = ack_root.lstat()
        except FileNotFoundError:
            raise ValueError(
                "resume progress lacks its durable ACK"
            ) from None
        except OSError:
            raise ValueError("resume ACK prefix is unavailable") from None
        if (
            stat.S_ISLNK(ack_metadata.st_mode)
            or not stat.S_ISDIR(ack_metadata.st_mode)
            or stat.S_IMODE(ack_metadata.st_mode) != 0o700
            or ack_metadata.st_uid != os.geteuid()
            or _directory_inventory(ack_root, "resume ACK prefix")
            != names
        ):
            raise ValueError("resume durable ACK prefix is invalid")
        records: list[tuple[Path, ProgressMessage, Ack]] = []
        for sequence, name in enumerate(names, start=1):
            message = read_progress(
                progress_root / name, lane, sequence
            )
            if (
                message.epoch_id != plan.epoch_id
                or message.run_kind != plan.run_kind
            ):
                raise ValueError("resume progress differs from the plan")
            ack_payload, _ack_content = _read_canonical_record(
                ack_root / name,
                "resume durable ACK",
                byte_cap=MAX_PROGRESS_BYTES,
            )
            ack = _decode_ack(ack_payload)
            expected_hash = hashlib.sha256(
                canonical_config_bytes(
                    _encode_progress_message(message)
                )
            ).hexdigest()
            if (
                ack.epoch_id != message.epoch_id
                or ack.run_kind != message.run_kind
                or ack.lane != message.lane
                or ack.seq != message.seq
                or ack.message_sha256 != expected_hash
            ):
                raise ValueError(
                    "resume durable ACK differs from progress"
                )
            records.append((worker_root, message, ack))
        lane_records[lane] = tuple(records)

    ledger = ProgressAckLedger(max_total_tokens=max_total_tokens)
    positions: dict[LaneName, int] = {
        lane: 0 for lane in ("E1", "E2", "E3", "APP")
    }
    decision_priority = {
        "continue": 0,
        "retry": 0,
        "stop-launches": 1,
        "abort": 2,
    }
    lane_priority = {
        lane: index
        for index, lane in enumerate(("E1", "E2", "E3", "APP"))
    }
    remaining = sum(len(records) for records in lane_records.values())
    while remaining:
        candidates: list[
            tuple[int, int, LaneName, ProgressAckLedger]
        ] = []
        for lane in ("E1", "E2", "E3", "APP"):
            index = positions[lane]
            records = lane_records[lane]
            if index >= len(records):
                continue
            worker_root, message, durable_ack = records[index]
            candidate = ledger._fork_for_replay()
            try:
                decision = candidate.accept_durable_progress(
                    worker_root=worker_root,
                    message=message,
                )
            except (TypeError, ValueError, RuntimeError) as error:
                raise ValueError(
                    "resume durable progress cannot be replayed"
                ) from error
            if decision == durable_ack.decision:
                candidates.append(
                    (
                        decision_priority[decision],
                        lane_priority[lane],
                        lane,
                        candidate,
                    )
                )
        if not candidates:
            raise ValueError(
                "resume durable ACK differs from exact ledger replay"
            )
        _priority, _lane_order, lane, ledger = min(candidates)
        positions[lane] += 1
        remaining -= 1

    sealed_attempts: set[tuple[CaseKey, int]] = set()
    for assignment in plan.assignments:
        manifest_case = manifests[assignment.key.mode][
            assignment.key.ordinal - 1
        ]
        paths = paths_for_case(run_root, assignment)
        attempts = scan_attempts(
            paths, plan=plan, manifest_case=manifest_case
        )
        sealed_attempts.update(
            (assignment.key, attempt)
            for attempt in range(1, len(attempts) + 1)
        )
    if ledger.completed_attempts != frozenset(sealed_attempts):
        raise ValueError(
            "resume durable progress differs from sealed attempts"
        )
    return ledger


def _resume_replay_requires_cleanup(
    *,
    ledger: ProgressAckLedger,
    resume: ResumePlan,
    plan: EpochPlan,
) -> bool:
    if (
        type(ledger) is not ProgressAckLedger
        or type(resume) is not ResumePlan
        or type(plan) is not EpochPlan
        or resume.run_kind != plan.run_kind
    ):
        raise TypeError("resume cleanup decision arguments are invalid")
    planned = {assignment.key for assignment in plan.assignments}
    if (
        any(key not in planned for key in resume.reusable)
        or any(key not in planned for key in resume.pending)
        or any(key not in planned for key in resume.invalid)
    ):
        raise ValueError("resume cleanup decision names an unknown case")
    return (
        bool(resume.invalid)
        or ledger.aborted
        or ledger.stop_launches
        or bool(ledger.exited)
        or any(active is not None for active in ledger.active_cases.values())
    )


def _rearm_bootstrap_before_resume(
    *,
    plan: EpochPlan,
    coordinator_root: Path,
    source_codex_home: Path,
    lease: RunCoordinatorLease,
    authority: QuiescentRunAuthority,
) -> InstalledAuthBootstrap:
    _validate_quiescent_cleanup_access(
        lease=lease,
        authority=authority,
        plan=plan,
        run_root=Path(coordinator_root).parent,
    )
    try:
        (Path(coordinator_root) / "auth-bootstrap").lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("resume bootstrap was not torn down")
    coordinator_descriptor, _metadata = _open_private_directory(
        Path(coordinator_root), "resume coordinator root"
    )
    coordinator_slot = _DescriptorSlot(coordinator_descriptor)
    cleanup = Path(coordinator_root) / "cleanup"
    try:
        cleanup_descriptor, _metadata = _open_private_directory(
            cleanup, "resume bootstrap cleanup"
        )
    except BaseException as error:
        _retire_task_descriptors(
            [coordinator_slot],
            primary=error,
            label="resume proof directory open or close failed",
        )
        raise AssertionError(
            "resume cleanup directory open unexpectedly returned"
        )
    cleanup_slot = _DescriptorSlot(cleanup_descriptor)
    retired_proofs = Path(coordinator_root) / "retired-proofs"
    try:
        _ensure_private_directory(retired_proofs)
        retired_descriptor, _metadata = _open_private_directory(
            retired_proofs, "resume retired proof archive"
        )
    except BaseException as error:
        _retire_task_descriptors(
            [cleanup_slot, coordinator_slot],
            primary=error,
            label="resume retired proof archive open or close failed",
        )
        raise AssertionError(
            "resume retired proof archive open unexpectedly returned"
        )
    retired_slot = _DescriptorSlot(retired_descriptor)
    proofs: list[_RetainedUnlinkProof] = []
    primary: BaseException | None = None
    try:
        inventory = tuple(sorted(os.listdir(cleanup_slot.descriptor)))
        if inventory != (
            "bootstrap-ownership.json",
            "bootstrap-tombstone.json",
        ):
            raise ValueError("resume bootstrap proof is incomplete")
        teardown_proof = _retain_verified_unlink_proof(
            parent_slot=coordinator_slot,
            name="teardown.json",
            expected_content=None,
            label="resume coordinator teardown receipt",
            byte_cap=64 * 1024,
        )
        proofs.append(teardown_proof)
        try:
            os.stat(
                "stop-launches.json",
                dir_fd=coordinator_slot.descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            stop_proof = None
        else:
            stop_proof = _retain_verified_unlink_proof(
                parent_slot=coordinator_slot,
                name="stop-launches.json",
                expected_content=None,
                label="resume coordinator stop marker",
                byte_cap=64 * 1024,
            )
            proofs.append(stop_proof)
        ownership_proof = _retain_verified_unlink_proof(
            parent_slot=cleanup_slot,
            name="bootstrap-ownership.json",
            expected_content=None,
            label="resume bootstrap ownership",
            byte_cap=64 * 1024,
        )
        proofs.append(ownership_proof)
        tombstone_proof = _retain_verified_unlink_proof(
            parent_slot=cleanup_slot,
            name="bootstrap-tombstone.json",
            expected_content=None,
            label="resume bootstrap tombstone",
            byte_cap=64 * 1024,
        )
        proofs.append(tombstone_proof)

        def decode_proof(
            proof: _RetainedUnlinkProof,
        ) -> dict[str, object]:
            try:
                decoded_record = json.loads(proof.content.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise ValueError(
                    f"{proof.label} is not canonical ASCII JSON"
                ) from None
            if (
                type(decoded_record) is not dict
                or canonical_config_bytes(decoded_record) != proof.content
            ):
                raise ValueError(
                    f"{proof.label} is not canonical ASCII JSON"
                )
            return decoded_record

        ownership_payload = decode_proof(ownership_proof)
        tombstone_payload = decode_proof(tombstone_proof)
        _bootstrap, bootstrap_sha256 = (
            _decode_bootstrap_tombstone_records(
                plan=plan,
                ownership_payload=ownership_payload,
                ownership_bytes=ownership_proof.content,
                receipt_payload=tombstone_payload,
                receipt_bytes=tombstone_proof.content,
            )
        )
        payload = decode_proof(teardown_proof)
        _require_exact_fields(
            payload,
            TeardownReceipt,
            "resume coordinator teardown receipt",
        )
        if stop_proof is not None:
            stop_payload = decode_proof(stop_proof)
            if (
                set(stop_payload)
                != {
                    "schema_version",
                    "epoch_id",
                    "run_kind",
                    "reason_sha256",
                }
                or type(stop_payload.get("schema_version")) is not int
                or stop_payload.get("schema_version") != 1
                or stop_payload.get("epoch_id") != plan.epoch_id
                or stop_payload.get("run_kind") != plan.run_kind
                or not _is_sha256(stop_payload.get("reason_sha256"))
            ):
                raise ValueError("resume coordinator stop marker is stale")
        encoded_tombstones = payload.get("tombstone_receipts")
        if type(encoded_tombstones) is not list:
            raise ValueError("resume teardown tombstones are invalid")
        decoded: list[tuple[CaseKey, str]] = []
        for item in encoded_tombstones:
            if type(item) is not list or len(item) != 2:
                raise ValueError("resume teardown tombstone is invalid")
            key = _decode_case_key(item[0], "resume teardown case")
            digest = item[1]
            if type(digest) is not str or not _is_sha256(digest):
                raise ValueError(
                    "resume teardown tombstone hash is invalid"
                )
            decoded.append((key, digest))
        decoded_tuple = tuple(decoded)
        plan_order = tuple(
            assignment.key
            for assignment in plan.assignments
            if assignment.key
            in {key for key, _digest in decoded_tuple}
        )
        if (
            type(payload.get("schema_version")) is not int
            or payload.get("schema_version") != 1
            or payload.get("epoch_id") != plan.epoch_id
            or payload.get("run_kind") != plan.run_kind
            or tuple(key for key, _digest in decoded_tuple) != plan_order
            or len({key for key, _digest in decoded_tuple})
            != len(decoded_tuple)
            or payload.get("bootstrap_tombstone_receipt_sha256")
            != bootstrap_sha256
            or payload.get("codex_homes_absent") is not True
            or payload.get("bootstrap_absent") is not True
        ):
            raise ValueError(
                "resume coordinator teardown receipt is stale"
            )
        assignment_by_key = {
            assignment.key: assignment for assignment in plan.assignments
        }
        for key, expected_digest in decoded_tuple:
            assignment = assignment_by_key.get(key)
            if assignment is None:
                raise ValueError(
                    "resume teardown names an unknown case"
                )
            paths = paths_for_case(lease._run_root, assignment)
            verified = read_verified_tombstone_receipt(
                plan=plan, assignment=assignment, paths=paths
            )
            if verified.sha256 != expected_digest:
                raise ValueError("resume teardown tombstone changed")
        for assignment in plan.assignments:
            try:
                paths_for_case(
                    lease._run_root, assignment
                ).codex_home.lstat()
            except FileNotFoundError:
                pass
            else:
                raise ValueError(
                    "resume case Codex home still exists"
                )
        _retire_retained_proofs(
            tuple(proofs),
            archive_slot=retired_slot,
        )
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [proof.slot for proof in proofs]
        + [retired_slot, cleanup_slot, coordinator_slot],
        primary=primary,
        label="resume proof reset or descriptor close failed",
    )
    return prepare_auth_bootstrap(
        source_codex_home=source_codex_home,
        coordinator_root=Path(coordinator_root),
        plan=plan,
    )


def build_production_case_driver(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    transport_runner: CaseTransport | None = None,
) -> CaseDriver:
    from scripts import run_observing_workflows_eval_worker as worker

    return worker.build_production_case_driver(
        snapshot_root=snapshot_root,
        transport_config=transport_config,
        transport_runner=transport_runner,
    )


def build_production_runtime_factory(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> RuntimeFactory:
    from scripts import run_observing_workflows_eval_worker as worker

    return worker.build_production_runtime_factory(
        snapshot_root=snapshot_root,
        transport_config=transport_config,
        plan=plan,
    )


def production_worker_dependencies(
    *,
    snapshot_root: Path,
    transport_config: ResolvedTransportConfig,
    plan: EpochPlan,
) -> WorkerDependencies:
    from scripts import run_observing_workflows_eval_worker as worker

    return worker.production_worker_dependencies(
        snapshot_root=snapshot_root,
        transport_config=transport_config,
        plan=plan,
    )


def _production_integrity_runner(
    command: Sequence[str],
    environment: Mapping[str, str],
    *,
    expected_records: int,
) -> dict[str, object]:
    completed = subprocess.run(
        tuple(command),
        env=dict(environment),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=300,
    )
    if completed.returncode != 0:
        raise RuntimeError("captured integrity command failed")
    match = re.fullmatch(
        r"healthy records=([0-9]+) invalidated=([0-9]+)\n?",
        completed.stdout,
    )
    if match is None or completed.stderr:
        raise ValueError("captured integrity command returned malformed output")
    result = {
        "records": int(match.group(1)),
        "invalidated": int(match.group(2)),
    }
    if result["records"] != expected_records:
        raise ValueError("captured integrity record count differs")
    return result


def production_coordinator_dependencies(
    *, snapshot_root: Path
) -> CoordinatorDependencies:
    snapshot = Path(snapshot_root)

    def worker_command_factory(
        lane: LaneName,
        plan: EpochPlan,
        options: ParallelOptions,
        bound_snapshot_root: Path,
    ) -> Sequence[str]:
        if (
            type(lane) is not str
            or lane not in ("E1", "E2", "E3", "APP")
            or type(plan) is not EpochPlan
            or type(options) is not ParallelOptions
            or Path(bound_snapshot_root) != snapshot
        ):
            raise ValueError("worker command inputs are invalid")
        return (
            sys.executable,
            "-m",
            "scripts.run_observing_workflows_eval_worker",
            "--lane",
            lane,
            "--run-root",
            str(options.run_root),
            "--snapshot-root",
            str(snapshot),
            "--epoch-id",
            plan.epoch_id,
        )

    return CoordinatorDependencies(
        worker_command_factory=worker_command_factory,
        integrity_runner=_production_integrity_runner,
    )


def run_worker(
    *,
    lane: LaneName,
    plan: EpochPlan,
    run_root: Path,
    snapshot_root: Path,
    dependencies: WorkerDependencies | None = None,
) -> Path:
    from scripts import run_observing_workflows_eval_worker as worker

    return worker.run_worker(
        lane=lane,
        plan=plan,
        run_root=run_root,
        snapshot_root=snapshot_root,
        dependencies=dependencies,
    )


def worker_main(argv: Sequence[str] | None = None) -> int:
    from scripts import run_observing_workflows_eval_worker as worker

    return worker.worker_main(argv)


def _snapshot_rows_for_parallel_plan(
    snapshot_root: Path,
) -> tuple[tuple[str, str], ...]:
    root = Path(snapshot_root)
    if not root.is_dir():
        raise ValueError("parallel snapshot root is unavailable")
    rows = _capture_directory_rows(root)
    return tuple(
        (row[0], row[4])
        for row in rows
        if row[1] == "file"
    )


_PARALLEL_EVALUATOR_ORIGINS = (
    "wiki_cli.py",
    "wiki_observations.py",
    "scripts/run_observing_workflows_task9_eval.py",
    "scripts/run_observing_workflows_eval_worker.py",
    "scripts/workflow_eval_sharding.py",
    "tests/observing_workflows_eval_harness.py",
    "tests/run_observing_workflows_eval.py",
)
_PARALLEL_ARCHIVE_RELATIVE = PurePosixPath(
    "evidence/dist/workflow-observatory-0.2.0-recovery.zip"
)


def _verified_parallel_archive_inputs(
    repository_root: Path,
) -> tuple[
    Path,
    tuple[str, str, str],
    dict[str, object],
    bytes,
]:
    from scripts.package_workflow_observatory import (
        ARCHIVE_ROOT,
        INVENTORY_MEMBER,
        PackageError,
        verify_archive,
    )

    archive_path = Path(repository_root) / _PARALLEL_ARCHIVE_RELATIVE
    try:
        archive_sha256 = verify_archive(archive_path)
        with zipfile.ZipFile(archive_path) as bundle:
            inventory_bytes = bundle.read(INVENTORY_MEMBER)
        inventory = json.loads(inventory_bytes)
    except (OSError, ValueError, PackageError, zipfile.BadZipFile) as error:
        raise ValueError(
            "verified evaluation archive is unavailable"
        ) from error
    if (
        type(inventory) is not dict
        or inventory.get("archive_root") != ARCHIVE_ROOT
        or type(inventory.get("marketplace_files")) is not dict
        or type(inventory.get("repository_evidence")) is not dict
        or type(inventory.get("members")) is not dict
    ):
        raise ValueError("verified evaluation archive inventory is invalid")
    marketplace = inventory["marketplace_files"]
    repository_evidence = inventory["repository_evidence"]
    marketplace_rows = tuple(
        (entry["member"], entry["packaged_sha256"])
        for _origin, entry in sorted(marketplace.items())
    )
    try:
        evaluator_rows = tuple(
            (
                repository_evidence[origin]["member"],
                repository_evidence[origin]["packaged_sha256"],
            )
            for origin in _PARALLEL_EVALUATOR_ORIGINS
        )
    except (KeyError, TypeError):
        raise ValueError(
            "verified archive lacks an evaluator member"
        ) from None
    return (
        archive_path,
        (
            archive_sha256,
            component_digest(marketplace_rows),
            component_digest(evaluator_rows),
        ),
        inventory,
        inventory_bytes,
    )


def _materialize_parallel_snapshot(
    *,
    repository_root: Path,
    snapshot_root: Path,
    expected_digests: tuple[str, str, str],
) -> None:
    (
        archive_path,
        archive_digests,
        inventory,
        inventory_bytes,
    ) = _verified_parallel_archive_inputs(repository_root)
    if archive_digests != expected_digests:
        raise RuntimeError(
            "verified archive changed before materialization"
        )
    try:
        snapshot_root.lstat()
    except FileNotFoundError:
        snapshot_root.mkdir(mode=0o700)
        snapshot_root.chmod(0o700)
        try:
            with zipfile.ZipFile(archive_path) as bundle:
                for info in bundle.infolist():
                    relative = PurePosixPath(info.filename)
                    destination_relative = PurePosixPath(*relative.parts[1:])
                    destination = snapshot_root / destination_relative
                    destination.parent.mkdir(
                        parents=True, mode=0o700, exist_ok=True
                    )
                    destination.write_bytes(bundle.read(info))
                    destination.chmod(0o444)
            directories = sorted(
                (
                    path
                    for path in snapshot_root.rglob("*")
                    if path.is_dir()
                ),
                key=lambda path: len(path.parts),
                reverse=True,
            )
            for directory in directories:
                directory.chmod(0o555)
            snapshot_root.chmod(0o555)
        except BaseException:
            try:
                snapshot_root.chmod(0o700)
                for path in snapshot_root.rglob("*"):
                    try:
                        path.chmod(0o700 if path.is_dir() else 0o600)
                    except OSError:
                        pass
                shutil.rmtree(snapshot_root)
            except OSError:
                pass
            raise
    except OSError:
        raise ValueError("parallel snapshot is unavailable") from None
    _validate_snapshot_root(snapshot_root)
    expected_rows = tuple(
        sorted(
            (
                member.removeprefix(
                    f"{inventory['archive_root']}/"
                ),
                digest,
            )
            for member, digest in inventory["members"].items()
        )
    ) + (
        (
            "SHA256SUMS.json",
            hashlib.sha256(inventory_bytes).hexdigest(),
        ),
    )
    actual_rows = tuple(
        sorted(
            (relative, digest)
            for relative, kind, _mode, _size, digest in (
                _capture_directory_rows(snapshot_root)
            )
            if kind == "file"
        )
    )
    if tuple(sorted(expected_rows)) != actual_rows:
        raise ValueError(
            "materialized snapshot differs from archive inventory"
        )


def _parallel_recovery_is_cleanup_only(
    *,
    has_ownership: bool,
    has_tombstone: bool,
    has_teardown: bool,
    has_stop_launches: bool,
    resume_requested: bool,
) -> bool:
    return (
        (has_tombstone and not has_teardown)
        or (has_stop_launches and not has_teardown)
        or (has_ownership and not resume_requested)
    )


def _parallel_plan_inputs(
    *,
    repository_root: Path,
    manifests: dict[EvalMode, list[dict[str, object]]],
    options: ParallelOptions,
) -> tuple[
    EpochPlan,
    ResolvedTransportConfig,
    Path,
    dict[str, list[dict[str, object]]],
    tuple[str, str, str],
]:
    if type(options) is not ParallelOptions:
        raise TypeError("options must be exact ParallelOptions")
    if type(manifests) is not dict or set(manifests) != {
        "forward",
        "lifecycle",
    }:
        raise ValueError("parallel manifests must contain exact modes")
    manifest_snapshot = _capture_resume_manifest_snapshot(manifests)
    frozen_manifests = _decode_resume_manifest_snapshot(manifest_snapshot)
    transport_config = resolve_transport_config(
        codex_executable=options.codex_executable,
        source_codex_home=options.source_codex_home,
        requested_model=options.requested_model,
        requested_reasoning_effort=options.requested_reasoning_effort,
    )
    run_root = canonical_run_root(options.run_root)
    if run_root.is_relative_to(repository_root):
        raise ValueError("parallel run root must be outside the repository")
    snapshot_root = (
        run_root / "coordinator/captured-snapshot"
    )
    _archive_path, capture_digests, _inventory, _inventory_bytes = (
        _verified_parallel_archive_inputs(repository_root)
    )
    archive_digest, marketplace_digest, evaluator_digest = capture_digests
    fingerprints = InputFingerprints(
        schema_version=1,
        epoch_id="",
        run_kind=options.run_kind,
        archive_sha256=archive_digest,
        marketplace_sha256=marketplace_digest,
        evaluator_sha256=evaluator_digest,
        transport_config_sha256=hashlib.sha256(
            transport_config_bytes(transport_config)
        ).hexdigest(),
        forward_manifest_sha256=hashlib.sha256(
            manifest_snapshot[0]
        ).hexdigest(),
        lifecycle_manifest_sha256=hashlib.sha256(
            manifest_snapshot[1]
        ).hexdigest(),
    )
    plan = build_epoch_plan(
        run_kind=options.run_kind,
        manifests=frozen_manifests,
        fingerprints=fingerprints,
    )
    return (
        plan,
        transport_config,
        snapshot_root,
        frozen_manifests,
        capture_digests,
    )


def _encode_epoch_plan_record(plan: EpochPlan) -> dict[str, object]:
    return asdict(plan)


def _encode_resume_plan_record(resume: ResumePlan) -> dict[str, object]:
    if type(resume) is not ResumePlan:
        raise TypeError("resume plan must be exact")
    return {
        "schema_version": 1,
        "run_kind": resume.run_kind,
        "reusable": [asdict(key) for key in resume.reusable],
        "pending": [asdict(key) for key in resume.pending],
        "invalid": [asdict(key) for key in resume.invalid],
    }


def _decode_resume_plan_record(
    payload: object, *, plan: EpochPlan
) -> ResumePlan:
    if type(payload) is not dict or set(payload) != {
        "schema_version",
        "run_kind",
        "reusable",
        "pending",
        "invalid",
    }:
        raise ValueError("sealed resume plan has invalid fields")
    groups: dict[str, tuple[CaseKey, ...]] = {}
    for name in ("reusable", "pending", "invalid"):
        encoded = payload.get(name)
        if type(encoded) is not list:
            raise ValueError("sealed resume plan group is invalid")
        groups[name] = tuple(
            _decode_case_key(item, "sealed resume plan")
            for item in encoded
        )
    resume = ResumePlan(
        run_kind=payload.get("run_kind"),
        reusable=groups["reusable"],
        pending=groups["pending"],
        invalid=groups["invalid"],
    )
    all_keys = resume.reusable + resume.pending + resume.invalid
    if (
        payload.get("schema_version") != 1
        or type(payload.get("schema_version")) is not int
        or resume.run_kind != plan.run_kind
        or len(set(all_keys)) != len(all_keys)
        or set(all_keys)
        != {assignment.key for assignment in plan.assignments}
    ):
        raise ValueError("sealed resume plan differs from the epoch plan")
    return resume


def _decode_epoch_plan_record(payload: object) -> EpochPlan:
    if type(payload) is not dict or set(payload) != {
        "schema_version",
        "epoch_id",
        "run_kind",
        "fingerprints",
        "assignments",
    }:
        raise ValueError("sealed epoch plan has invalid fields")
    fingerprints_payload = payload.get("fingerprints")
    assignments_payload = payload.get("assignments")
    if type(fingerprints_payload) is not dict or type(assignments_payload) is not list:
        raise ValueError("sealed epoch plan has invalid structure")
    _require_exact_fields(
        fingerprints_payload, InputFingerprints, "sealed input fingerprints"
    )
    fingerprints = InputFingerprints(**fingerprints_payload)
    assignments: list[CaseAssignment] = []
    for item in assignments_payload:
        if type(item) is not dict or set(item) != {
            "key",
            "lane",
            "route",
            "manifest_sha256",
        }:
            raise ValueError("sealed assignment has invalid fields")
        assignment = dict(item)
        assignment["key"] = _decode_case_key(
            assignment["key"], "sealed assignment"
        )
        assignments.append(CaseAssignment(**assignment))
    plan = EpochPlan(
        schema_version=payload["schema_version"],
        epoch_id=payload["epoch_id"],
        run_kind=payload["run_kind"],
        fingerprints=fingerprints,
        assignments=tuple(assignments),
    )
    if (
        type(plan.schema_version) is not int
        or plan.schema_version != 1
        or plan.epoch_id != fingerprints.epoch_id
        or plan.run_kind != fingerprints.run_kind
        or not _is_sha256(plan.epoch_id)
    ):
        raise ValueError("sealed epoch plan identity is invalid")
    return plan


def _parallel_worker_environment() -> dict[str, str]:
    allowed = (
        "PATH",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "SYSTEMROOT",
    )
    environment = {
        name: os.environ[name]
        for name in allowed
        if name in os.environ
    }
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    if any(
        "result" in name.lower()
        or "persist" in name.lower()
        or "writer" in name.lower()
        for name in environment
    ):
        raise RuntimeError("worker environment exposed persistence state")
    return environment


def _worker_lifetime_paths(
    run_root: Path, lane: LaneName
) -> tuple[Path, Path]:
    coordinator = Path(run_root) / "coordinator"
    return (
        coordinator / "worker-leases" / f"{lane}.lock",
        coordinator / "process-groups" / f"{lane}.json",
    )


def _acquire_worker_lifetime_lock(
    run_root: Path, lane: LaneName
) -> tuple[_DescriptorSlot, Path]:
    lock_path, record_path = _worker_lifetime_paths(run_root, lane)
    for directory in (lock_path.parent, record_path.parent):
        try:
            directory.mkdir(mode=0o700)
        except FileExistsError:
            pass
        metadata = directory.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != os.geteuid()
        ):
            raise ValueError("worker lifetime directory is unsafe")
    slot = _DescriptorSlot(
        os.open(
            lock_path,
            os.O_RDWR
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    )
    primary: BaseException | None = None
    try:
        metadata = os.fstat(slot.descriptor)
        named = lock_path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
            or (metadata.st_dev, metadata.st_ino)
            != (named.st_dev, named.st_ino)
        ):
            raise ValueError("worker lifetime lock is unsafe")
        fcntl.flock(slot.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException as error:
        primary = error
    if primary is not None:
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label="worker lifetime lock acquisition or close failed",
        )
    return slot, record_path


def _durable_process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        raise RuntimeError(
            "durable worker group quiescence is indeterminate"
        ) from None
    return True


def _recover_durable_worker_groups(
    *, run_root: Path, plan: EpochPlan
) -> None:
    lease_root = Path(run_root) / "coordinator/worker-leases"
    record_root = Path(run_root) / "coordinator/process-groups"
    for root, label in (
        (lease_root, "worker lifetime"),
        (record_root, "worker process-group"),
    ):
        try:
            metadata = root.lstat()
        except FileNotFoundError:
            continue
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != os.geteuid()
        ):
            raise ValueError(f"{label} directory is unsafe")
    allowed_locks = {f"{lane}.lock" for lane in ("E1", "E2", "E3", "APP")}
    allowed_records = {
        f"{lane}.json" for lane in ("E1", "E2", "E3", "APP")
    }
    lock_names = (
        set(entry.name for entry in os.scandir(lease_root))
        if lease_root.exists()
        else set()
    )
    record_names = (
        set(entry.name for entry in os.scandir(record_root))
        if record_root.exists()
        else set()
    )
    if not lock_names <= allowed_locks or not record_names <= allowed_records:
        raise ValueError("worker lifetime namespace contains an unknown entry")
    for lane in ("E1", "E2", "E3", "APP"):
        lock_path, record_path = _worker_lifetime_paths(run_root, lane)
        record: dict[str, object] | None = None
        if record_path.name in record_names:
            payload, _content = _read_canonical_record(
                record_path,
                "worker process-group record",
                byte_cap=4096,
            )
            if type(payload) is not dict or set(payload) != {
                "schema_version",
                "epoch_id",
                "run_kind",
                "lane",
                "pid",
                "pgid",
            }:
                raise ValueError("worker process-group record is invalid")
            if (
                payload.get("schema_version") != 1
                or type(payload.get("schema_version")) is not int
                or payload.get("epoch_id") != plan.epoch_id
                or payload.get("run_kind") != plan.run_kind
                or payload.get("lane") != lane
                or type(payload.get("pid")) is not int
                or payload["pid"] <= 0
                or type(payload.get("pgid")) is not int
                or payload["pgid"] <= 0
            ):
                raise ValueError("worker process-group record is stale")
            record = payload
        if lock_path.name not in lock_names:
            if record is not None:
                raise ValueError("worker process group lacks its lifetime lock")
            continue
        slot = _DescriptorSlot(
            os.open(
                lock_path,
                os.O_RDWR
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        )
        primary: BaseException | None = None
        locked_elsewhere = False
        try:
            metadata = os.fstat(slot.descriptor)
            named = lock_path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_uid != os.geteuid()
                or (metadata.st_dev, metadata.st_ino)
                != (named.st_dev, named.st_ino)
            ):
                raise ValueError("worker lifetime lock changed")
            try:
                fcntl.flock(
                    slot.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB
                )
            except BlockingIOError:
                locked_elsewhere = True
            if locked_elsewhere and record is None:
                raise RuntimeError(
                    "unrecorded worker may still be running"
                )
            if record is not None:
                pgid = record["pgid"]
                group_exists = _durable_process_group_exists(pgid)
                if group_exists and not locked_elsewhere:
                    try:
                        leader_group = os.getpgid(record["pid"])
                    except ProcessLookupError:
                        raise RuntimeError(
                            "unlocked durable worker group identity is "
                            "indeterminate"
                        ) from None
                    if leader_group != pgid:
                        raise RuntimeError(
                            "durable worker group identity changed"
                        )
                if group_exists:
                    try:
                        os.killpg(pgid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    deadline = time.monotonic() + 5.0
                    while (
                        _durable_process_group_exists(pgid)
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.01)
                    if _durable_process_group_exists(pgid):
                        try:
                            os.killpg(pgid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        deadline = time.monotonic() + 5.0
                        while (
                            _durable_process_group_exists(pgid)
                            and time.monotonic() < deadline
                        ):
                            time.sleep(0.01)
                if _durable_process_group_exists(pgid):
                    raise RuntimeError(
                        "durable worker process group survived cancellation"
                    )
            if locked_elsewhere:
                try:
                    fcntl.flock(
                        slot.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB
                    )
                except BlockingIOError:
                    raise RuntimeError(
                        "worker lifetime lock survived cancellation"
                    ) from None
        except BaseException as error:
            primary = error
        _retire_task_descriptors(
            [slot],
            primary=primary,
            label="worker lifetime recovery or close failed",
        )


def _drain_worker_stream(stream: object) -> None:
    if stream is None:
        return
    for _line in stream:
        pass


def _launch_parallel_workers(
    *,
    machine: CoordinatorStateMachine,
    plan: EpochPlan,
    options: ParallelOptions,
    snapshot_root: Path,
    dependencies: CoordinatorDependencies,
    resume: ResumePlan,
) -> None:
    if not callable(dependencies.worker_command_factory):
        raise TypeError("worker command factory must be callable")
    for lane in ("E1", "E2", "E3", "APP"):
        lifetime_slot, group_record_path = (
            _acquire_worker_lifetime_lock(options.run_root, lane)
        )
        process: subprocess.Popen | None = None
        try:
            command = tuple(
                dependencies.worker_command_factory(
                    lane, plan, options, snapshot_root
                )
            ) + (
                "--resume-plan-hex",
                canonical_config_bytes(
                    _encode_resume_plan_record(resume)
                ).hex(),
            )
        except BaseException as error:
            _retire_task_descriptors(
                [lifetime_slot],
                primary=error,
                label="worker lifetime command or close failed",
            )
            raise AssertionError(
                "worker command failure unexpectedly returned"
            )
        if not command or any(type(part) is not str for part in command):
            error = ValueError(
                "worker command must be a nonempty string sequence"
            )
            _retire_task_descriptors(
                [lifetime_slot],
                primary=error,
                label="worker command validation or close failed",
            )
            raise AssertionError(
                "invalid worker command unexpectedly returned"
            )
        lowered = "\0".join(command).lower()
        if any(
            forbidden in lowered
            for forbidden in (
                "result-destination",
                "result_destination",
                "writer-authority",
                "commit-capability",
            )
        ):
            error = RuntimeError(
                "worker command exposed persistence state"
            )
            _retire_task_descriptors(
                [lifetime_slot],
                primary=error,
                label="worker command isolation or close failed",
            )
            raise AssertionError(
                "unsafe worker command unexpectedly returned"
            )
        try:
            process = subprocess.Popen(
                command,
                cwd=Path(__file__).resolve().parents[1],
                env=_parallel_worker_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
                pass_fds=(lifetime_slot.descriptor,),
            )
            pgid = os.getpgid(process.pid)
            _atomic_write_record(
                group_record_path,
                {
                    "schema_version": 1,
                    "epoch_id": plan.epoch_id,
                    "run_kind": plan.run_kind,
                    "lane": lane,
                    "pid": process.pid,
                    "pgid": pgid,
                },
            )
            process.pgid = pgid
        except BaseException as error:
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5.0)
                except BaseException:
                    pass
            _retire_task_descriptors(
                [lifetime_slot],
                primary=error,
                label="worker launch or lifetime close failed",
            )
            raise AssertionError(
                "worker launch failure unexpectedly returned"
            )
        _retire_task_descriptors(
            [lifetime_slot],
            primary=None,
            label="worker lifetime handoff close failed",
        )
        process.worker_root = (
            options.run_root / "app-server"
            if lane == "APP"
            else options.run_root / "workers" / lane
        )
        readers = tuple(
            threading.Thread(
                target=_drain_worker_stream,
                args=(stream,),
                daemon=True,
            )
            for stream in (process.stdout, process.stderr)
        )
        process._coordinator_readers = readers
        for reader in readers:
            reader.start()
        machine.register_worker(lane, process)


def _supervise_parallel_workers(
    *,
    machine: CoordinatorStateMachine,
    options: ParallelOptions,
) -> None:
    sequences = {
        lane: machine._ledger._state.last_sequence[lane] + 1
        for lane in ("E1", "E2", "E3", "APP")
    }
    stopped: set[LaneName] = set()
    while len(stopped) != 4:
        progressed = False
        for lane in ("E1", "E2", "E3", "APP"):
            if lane in stopped:
                continue
            process = machine._workers[lane]
            worker_root = Path(process.worker_root)
            try:
                message = wait_for_progress(
                    worker_root=worker_root,
                    expected_lane=lane,
                    expected_seq=sequences[lane],
                    timeout=0.05,
                )
            except TimeoutError:
                poll = getattr(process, "poll")
                if poll() is not None:
                    machine._ledger.worker_exited(lane)
                    machine.cancel(
                        f"worker {lane} exited before durable terminal"
                    )
                    raise RuntimeError(
                        f"worker {lane} exited before durable terminal"
                    )
                continue
            decision = machine.accept_progress(message)
            progressed = True
            sequences[lane] += 1
            if message.type == "worker-stopped":
                return_code = process.wait(timeout=5.0)
                machine._join_readers(process)
                if type(return_code) is not int or return_code != 0:
                    machine.cancel(
                        f"worker {lane} failed after durable stop"
                    )
                    raise RuntimeError(
                        f"worker {lane} exited unsuccessfully"
                    )
                stopped.add(lane)
            if decision == "abort":
                raise RuntimeError("worker progress requested abort")
        if not progressed and machine.stop_launches:
            active = [
                lane
                for lane, process in machine._workers.items()
                if getattr(process, "poll")() is None
            ]
            if not active:
                raise RuntimeError(
                    "token launch ceiling left the epoch incomplete"
                )


def _reconcile_retry_cleanup_backup(paths: CasePaths) -> None:
    backup = paths.root / "cleanup-attempt-1"
    try:
        backup_metadata = backup.lstat()
    except FileNotFoundError:
        return
    except OSError:
        raise ValueError("retry cleanup backup is unavailable") from None
    if (
        stat.S_ISLNK(backup_metadata.st_mode)
        or not stat.S_ISDIR(backup_metadata.st_mode)
        or stat.S_IMODE(backup_metadata.st_mode) != 0o700
        or backup_metadata.st_uid != os.geteuid()
    ):
        raise ValueError("retry cleanup backup is unsafe")
    backup_inventory = _directory_inventory(
        backup, "retry cleanup backup"
    )
    if backup_inventory != ("ownership.json", "tombstone.json"):
        raise ValueError("retry cleanup backup is incomplete")
    for name in backup_inventory:
        metadata = (backup / name).lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
        ):
            raise ValueError("retry cleanup backup proof is unsafe")
    try:
        cleanup_metadata = paths.cleanup.lstat()
    except FileNotFoundError:
        cleanup_inventory: tuple[str, ...] = ()
        cleanup_missing = True
    except OSError:
        raise ValueError("retry cleanup namespace is unavailable") from None
    else:
        cleanup_missing = False
        if (
            stat.S_ISLNK(cleanup_metadata.st_mode)
            or not stat.S_ISDIR(cleanup_metadata.st_mode)
            or stat.S_IMODE(cleanup_metadata.st_mode) != 0o700
            or cleanup_metadata.st_uid != os.geteuid()
        ):
            raise ValueError("retry cleanup namespace is unsafe")
        cleanup_inventory = _directory_inventory(
            paths.cleanup, "retry cleanup namespace"
        )
    if cleanup_inventory:
        if cleanup_inventory not in (
            ("ownership.json",),
            ("ownership.json", "tombstone.json"),
        ):
            raise ValueError("retry cleanup namespace is invalid")
        return
    descriptor, _metadata = _open_private_directory(
        paths.root, "retry case root"
    )
    slot = _DescriptorSlot(descriptor)
    primary: BaseException | None = None
    try:
        if not cleanup_missing:
            os.rmdir("cleanup", dir_fd=slot.descriptor)
        os.rename(
            "cleanup-attempt-1",
            "cleanup",
            src_dir_fd=slot.descriptor,
            dst_dir_fd=slot.descriptor,
        )
        os.fsync(slot.descriptor)
    except BaseException as error:
        primary = error
    _retire_task_descriptors(
        [slot],
        primary=primary,
        label="retry cleanup restoration or close failed",
    )


def _cleanup_parallel_epoch(
    *,
    machine: CoordinatorStateMachine,
    lease: RunCoordinatorLease,
    plan: EpochPlan,
    bootstrap: InstalledAuthBootstrap | None,
    authority: QuiescentRunAuthority | None = None,
) -> tuple[TeardownReceipt, Path]:
    if authority is None:
        authority = machine.workers_stopped()
        machine.begin_teardown()
    else:
        _validate_quiescent_cleanup_access(
            lease=lease,
            authority=authority,
            plan=plan,
            run_root=lease._run_root,
        )
    cases_root = lease._run_root / "cases"
    expected_case_names = {
        paths_for_case(lease._run_root, assignment).root.name
        for assignment in plan.assignments
    }
    try:
        cases_metadata = cases_root.lstat()
    except FileNotFoundError:
        case_entries: tuple[object, ...] = ()
    except OSError:
        raise ValueError("case namespace is unavailable") from None
    else:
        if (
            stat.S_ISLNK(cases_metadata.st_mode)
            or not stat.S_ISDIR(cases_metadata.st_mode)
            or stat.S_IMODE(cases_metadata.st_mode) != 0o700
        ):
            raise ValueError("case namespace is unsafe")
        try:
            case_entries = tuple(os.scandir(cases_root))
        except OSError:
            raise ValueError("case namespace is unavailable") from None
    if len(case_entries) > len(plan.assignments):
        raise ValueError("case namespace exceeds the frozen plan")
    for entry in case_entries:
        try:
            metadata = entry.stat(follow_symlinks=False)
        except OSError:
            raise ValueError("case namespace entry is unavailable") from None
        if (
            entry.name not in expected_case_names
            or stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise ValueError("case namespace contains an unknown root")
    tombstones: list[tuple[CaseKey, TombstoneReceipt]] = []
    for assignment in plan.assignments:
        paths = paths_for_case(lease._run_root, assignment)
        _reconcile_retry_cleanup_backup(paths)
        try:
            paths.cleanup.lstat()
        except FileNotFoundError:
            try:
                paths.codex_home.lstat()
            except FileNotFoundError:
                pass
            except OSError:
                raise ValueError(
                    "case Codex-home absence is indeterminate"
                ) from None
            else:
                raise ValueError(
                    "case Codex home exists without cleanup ownership"
                )
            continue
        except OSError:
            raise ValueError("case cleanup namespace is unavailable") from None
        receipt = recover_case_auth_cleanup(
            plan=plan,
            assignment=assignment,
            paths=paths,
            lease=lease,
            authority=authority,
        )
        teardown_case_auth(
            paths=paths,
            receipt=receipt,
            lease=lease,
            authority=authority,
        )
        tombstones.append((assignment.key, receipt))
    if bootstrap is None:
        bootstrap_receipt = recover_auth_bootstrap_cleanup(
            plan=plan,
            coordinator_root=lease._run_root / "coordinator",
            lease=lease,
            authority=authority,
        )
    else:
        bootstrap_receipt = cleanup_auth_bootstrap(
            installed=bootstrap,
            lease=lease,
            authority=authority,
        )
    teardown_auth_bootstrap(
        coordinator_root=lease._run_root / "coordinator",
        receipt=bootstrap_receipt,
        lease=lease,
        authority=authority,
    )
    receipt = write_teardown_receipt(
        plan=plan,
        run_root=lease._run_root,
        tombstones=tuple(tombstones),
        bootstrap=bootstrap_receipt,
        lease=lease,
        authority=authority,
    )
    machine.mark_torn_down(receipt)
    return receipt, lease._run_root / "coordinator/teardown.json"


def _write_or_verify_coordinator_record(
    path: Path,
    payload: Mapping[str, object],
    label: str,
) -> None:
    expected = canonical_config_bytes(payload)
    try:
        path.lstat()
    except FileNotFoundError:
        _atomic_write_record(path, payload)
        return
    except OSError:
        raise ValueError(f"{label} is unavailable") from None
    current_payload, current = _read_canonical_record(path, label)
    if current != expected or current_payload != dict(payload):
        raise ValueError(f"{label} differs from the sealed coordinator record")


def _entry_exists_no_follow(path: Path, label: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        raise ValueError(f"{label} is unavailable") from None
    return True


def run_parallel_evaluation(
    *,
    repository_root: Path,
    manifests: dict[EvalMode, list[dict[str, object]]],
    result_destinations: dict[EvalMode, Path] | None,
    options: ParallelOptions,
    dependencies: CoordinatorDependencies | None = None,
) -> ParallelRunResult:
    repository, _identity = _canonical_git_repository_root(repository_root)
    if type(options) is not ParallelOptions:
        raise TypeError("options must be exact ParallelOptions")
    if options.run_kind in ("diagnostic", "discovery"):
        if result_destinations is not None:
            raise ValueError(
                "diagnostic and discovery require result_destinations=None"
            )
    elif options.run_kind == "formal":
        if type(result_destinations) is not dict:
            raise ValueError("formal parallel evaluation requires destinations")
    else:
        raise ValueError("invalid parallel run kind")
    (
        plan,
        transport_config,
        snapshot_root,
        frozen_manifests,
        capture_digests,
    ) = (
        _parallel_plan_inputs(
            repository_root=repository,
            manifests=manifests,
            options=options,
        )
    )
    run_root = canonical_run_root(options.run_root)
    bound_options = replace(options, run_root=run_root)
    coordinator_dependencies = (
        production_coordinator_dependencies(snapshot_root=snapshot_root)
        if dependencies is None
        else dependencies
    )
    if type(coordinator_dependencies) is not CoordinatorDependencies:
        raise TypeError("dependencies must be exact CoordinatorDependencies")
    if not callable(coordinator_dependencies.integrity_runner):
        raise TypeError("coordinator integrity runner must be callable")

    run_lease: RunCoordinatorLease | None = None
    writer_lease: ResultWriterLease | None = None
    writer_authority: ResultWriterAuthority | None = None
    machine: CoordinatorStateMachine | None = None
    bootstrap: InstalledAuthBootstrap | None = None
    body_error: BaseException | None = None
    result: ParallelRunResult | None = None
    try:
        run_lease = RunCoordinatorLease.acquire(
            run_root=run_root,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
        )
        guard = CoordinatorGuard.capture(repository)
        machine = CoordinatorStateMachine.create(
            plan, bound_options, guard
        )
        machine._bind_run_lease(run_lease)
        if plan.run_kind == "formal":
            writer_lease = ResultWriterLease.acquire(
                repository,
                "parallel-coordinator",
                "formal",
                run_lease=run_lease,
            )
            writer_authority = writer_lease.authority()

        coordinator_root = run_root / "coordinator"
        _materialize_parallel_snapshot(
            repository_root=repository,
            snapshot_root=snapshot_root,
            expected_digests=capture_digests,
        )
        _write_or_verify_coordinator_record(
            coordinator_root / "epoch-plan.json",
            _encode_epoch_plan_record(plan),
            "sealed epoch plan",
        )
        _write_or_verify_coordinator_record(
            coordinator_root / "transport-config.json",
            asdict(transport_config),
            "sealed transport config",
        )
        cleanup_root = coordinator_root / "cleanup"
        ownership_path = cleanup_root / "bootstrap-ownership.json"
        tombstone_path = cleanup_root / "bootstrap-tombstone.json"
        teardown_path = coordinator_root / "teardown.json"
        stop_launches_path = coordinator_root / "stop-launches.json"
        has_ownership = _entry_exists_no_follow(
            ownership_path, "bootstrap ownership"
        )
        has_tombstone = _entry_exists_no_follow(
            tombstone_path, "bootstrap tombstone"
        )
        has_teardown = _entry_exists_no_follow(
            teardown_path, "coordinator teardown receipt"
        )
        has_stop_launches = _entry_exists_no_follow(
            stop_launches_path, "coordinator stop marker"
        )
        cleanup_only = _parallel_recovery_is_cleanup_only(
            has_ownership=has_ownership,
            has_tombstone=has_tombstone,
            has_teardown=has_teardown,
            has_stop_launches=has_stop_launches,
            resume_requested=bound_options.resume_run_root is not None,
        )

        if cleanup_only:
            if not has_ownership:
                raise ValueError(
                    "cleanup-only recovery lacks bootstrap ownership"
                )
        elif has_ownership and not has_tombstone:
            bootstrap = _reopen_auth_bootstrap(
                plan=plan, coordinator_root=coordinator_root
            )
        elif not has_ownership:
            bootstrap = prepare_auth_bootstrap(
                source_codex_home=bound_options.source_codex_home,
                coordinator_root=coordinator_root,
                plan=plan,
            )

        launch_resume = ResumePlan(
            run_kind=plan.run_kind,
            reusable=(),
            pending=tuple(
                assignment.key for assignment in plan.assignments
            ),
            invalid=(),
        )
        teardown_authority: QuiescentRunAuthority | None = None
        if (
            not cleanup_only
            and bound_options.resume_run_root is not None
        ):
            resume_authority = machine.workers_stopped()
            if has_teardown:
                bootstrap = _rearm_bootstrap_before_resume(
                    plan=plan,
                    coordinator_root=coordinator_root,
                    source_codex_home=bound_options.source_codex_home,
                    lease=run_lease,
                    authority=resume_authority,
                )
            resume = _classify_resume_under_quiescence(
                plan=plan,
                run_root=run_root,
                current_fingerprints=plan.fingerprints,
                manifests=frozen_manifests,
                lease=run_lease,
                authority=resume_authority,
            )
            try:
                resume_ledger = _resume_protocol_bindings(
                    plan=plan,
                    run_root=run_root,
                    manifests=frozen_manifests,
                    max_total_tokens=bound_options.max_total_tokens,
                )
            except (TypeError, ValueError, RuntimeError):
                resume_ledger = None
                replay_requires_cleanup = True
            else:
                replay_requires_cleanup = (
                    _resume_replay_requires_cleanup(
                        ledger=resume_ledger,
                        resume=resume,
                        plan=plan,
                    )
                )
            if (
                resume_ledger is not None
                and not any(
                    active is not None
                    for active in resume_ledger.active_cases.values()
                )
            ):
                machine._seed_resume_protocol(ledger=resume_ledger)
            if replay_requires_cleanup:
                machine._cancel_reason = (
                    "resume snapshot or durable prefix requires cleanup"
                )
                machine._stop_launches = True
                cleanup_only = True
                teardown_authority = resume_authority
            else:
                launch_resume = resume
                machine._resume_to_launch(resume_authority)

        if not cleanup_only:
            guard.checkpoint("before parallel worker launch")
            _launch_parallel_workers(
                machine=machine,
                plan=plan,
                options=bound_options,
                snapshot_root=snapshot_root,
                dependencies=coordinator_dependencies,
                resume=launch_resume,
            )
            _supervise_parallel_workers(
                machine=machine,
                options=bound_options,
            )
        elif machine.phase == "preflight":
            machine.cancel("cleanup-only coordinator")

        _teardown, teardown_path = _cleanup_parallel_epoch(
            machine=machine,
            lease=run_lease,
            plan=plan,
            bootstrap=bootstrap,
            authority=teardown_authority,
        )
        if machine.phase == "failed":
            result = ParallelRunResult(
                run_kind=plan.run_kind,
                run_root=run_root,
                status="failed",
                validated=None,
            )
        elif plan.run_kind == "diagnostic":
            result = ParallelRunResult(
                run_kind=plan.run_kind,
                run_root=run_root,
                status="diagnostic",
                validated=None,
            )
        else:
            machine.begin_validation()
            case_paths = {
                assignment.key: paths_for_case(run_root, assignment)
                for assignment in plan.assignments
            }
            shard_paths = {
                lane: (
                    run_root / "app-server/sealed/shard-commit.json"
                    if lane == "APP"
                    else run_root
                    / "workers"
                    / lane
                    / "sealed/shard-commit.json"
                )
                for lane in ("E1", "E2", "E3", "APP")
            }
            validated = validate_epoch_for_aggregation(
                plan=plan,
                run_root=run_root,
                snapshot_root=snapshot_root,
                manifests=frozen_manifests,
                shard_paths=shard_paths,
                case_paths=case_paths,
                integrity_runner=coordinator_dependencies.integrity_runner,
                guard=guard,
                current_fingerprints=plan.fingerprints,
                teardown_receipt=teardown_path,
            )
            machine.mark_validated(validated)
            if plan.run_kind == "discovery":
                result = ParallelRunResult(
                    run_kind=plan.run_kind,
                    run_root=run_root,
                    status="validated",
                    validated=validated,
                )
            else:
                if writer_authority is None or result_destinations is None:
                    raise AssertionError(
                        "formal coordinator lacks writer authority"
                    )
                commit = validated.claim_formal_commit()
                machine.mark_commit_ready(commit)
                persist_validated_epoch(
                    commit,
                    authority=writer_authority,
                    destinations=result_destinations,
                    guard=guard,
                )
                machine.mark_committed()
                result = ParallelRunResult(
                    run_kind=plan.run_kind,
                    run_root=run_root,
                    status="committed",
                    validated=validated,
                )
    except BaseException as error:
        body_error = error
        if is_indeterminate_descriptor_close(error):
            close_errors: list[BaseException] = []
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                close_error = _retire_descriptor_capability(bootstrap)
                if close_error is not None:
                    close_errors.append(close_error)
                bootstrap = None
            _raise_task_failures(
                primary=error,
                close_errors=close_errors,
                label=(
                    "parallel coordinator indeterminate failure or "
                    "bootstrap close failed"
                ),
            )
            raise AssertionError(
                "indeterminate coordinator failure unexpectedly returned"
            )
        if machine is not None:
            try:
                if machine._phase in ("preflight", "running"):
                    machine.cancel("parallel coordinator failure")
                if (
                    run_lease is not None
                    and machine._phase == "cancelling"
                    and machine._active_authority is None
                ):
                    _cleanup_parallel_epoch(
                        machine=machine,
                        lease=run_lease,
                        plan=plan,
                        bootstrap=bootstrap,
                    )
            except BaseException as cleanup_error:
                if is_indeterminate_descriptor_close(cleanup_error):
                    raise cleanup_error from error
        if machine is not None and machine._phase != "committed":
            machine._stop_launches = True
            machine._phase = "failed"
        result = ParallelRunResult(
            run_kind=plan.run_kind,
            run_root=run_root,
            status="failed",
            validated=None,
        )
    finally:
        close_errors: list[BaseException] = []
        if writer_lease is not None:
            try:
                writer_lease.close()
            except BaseException as error:
                close_errors.append(error)
        if run_lease is not None:
            try:
                run_lease.close()
            except BaseException as error:
                close_errors.append(error)
        if close_errors:
            errors = (
                ([body_error] if body_error is not None else [])
                + close_errors
            )
            if len(errors) == 1:
                raise errors[0]
            group_type = (
                ExceptionGroup
                if all(isinstance(error, Exception) for error in errors)
                else BaseExceptionGroup
            )
            raise group_type(
                "parallel coordinator body or lease close failed",
                errors,
            )
    if result is None:
        raise AssertionError("parallel coordinator produced no result")
    return result
