#!/usr/bin/env python3
"""Private, disposable Claude session-to-observation bindings."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows is documented as skill-only.
    fcntl = None


_RUN_ID_RE = re.compile(r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
_MAX_SESSION_ID = 4096
_MAX_BINDING_BYTES = 4096
_BINDINGS_DIR = "session-bindings"
_LOCKS_DIR = ".locks"


class BindingError(ValueError):
    """A bounded failure that must never disclose binding input or paths."""


@dataclass
class _Layout:
    plugin_data_fd: int
    bindings_fd: int
    locks_fd: int


def _unavailable() -> BindingError:
    return BindingError("session binding unavailable")


def _invalid() -> BindingError:
    return BindingError("session binding is invalid")


def _same_identity(*entries: os.stat_result) -> bool:
    return len({(entry.st_dev, entry.st_ino) for entry in entries}) == 1


def _directory_flags() -> int:
    return os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


def _validate_inputs(
    plugin_data: Path | str, session_id: str
) -> tuple[Path, str]:
    try:
        root = Path(plugin_data)
    except (TypeError, ValueError) as error:
        raise _invalid() from error
    if (
        not root.is_absolute()
        or len(root.parts) <= 1
        or any(part in {".", ".."} for part in root.parts[1:])
    ):
        raise _invalid()
    if (
        not isinstance(session_id, str)
        or not session_id
        or len(session_id) > _MAX_SESSION_ID
        or "\0" in session_id
    ):
        raise _invalid()
    digest = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
    return root, digest


def _open_verified_child_directory(
    parent_fd: int, name: str, *, create: bool, private: bool
) -> int | None:
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        if not create:
            return None
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError:
            pass
        except OSError as error:
            raise _unavailable() from error
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as error:
            raise _unavailable() from error
    except OSError as error:
        raise _unavailable() from error
    if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise _invalid()
    if private and stat.S_IMODE(before.st_mode) != 0o700:
        raise _invalid()
    try:
        descriptor = os.open(name, _directory_flags(), dir_fd=parent_fd)
    except OSError as error:
        raise _invalid() from error
    try:
        opened = os.fstat(descriptor)
        after = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(after.st_mode)
            or not _same_identity(before, opened, after)
            or (private and stat.S_IMODE(opened.st_mode) != 0o700)
            or (private and stat.S_IMODE(after.st_mode) != 0o700)
        ):
            raise _invalid()
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


def _open_plugin_data(path: Path, *, create: bool) -> int | None:
    try:
        descriptor = os.open(path.anchor, _directory_flags())
    except OSError as error:
        raise _unavailable() from error
    try:
        components = path.parts[1:]
        for index, component in enumerate(components):
            child = _open_verified_child_directory(
                descriptor,
                component,
                create=create,
                private=index == len(components) - 1,
            )
            if child is None:
                os.close(descriptor)
                return None
            os.close(descriptor)
            descriptor = child
        return descriptor
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


@contextmanager
def _open_layout(path: Path, *, create: bool) -> Iterator[_Layout | None]:
    plugin_data_fd = _open_plugin_data(path, create=create)
    if plugin_data_fd is None:
        yield None
        return
    bindings_fd = -1
    locks_fd = -1
    try:
        bindings_fd = _open_verified_child_directory(
            plugin_data_fd, _BINDINGS_DIR, create=create, private=True
        )
        if bindings_fd is None:
            bindings_fd = -1
            yield None
            return
        locks_fd = _open_verified_child_directory(
            bindings_fd, _LOCKS_DIR, create=create, private=True
        )
        if locks_fd is None:
            locks_fd = -1
            yield None
            return
        yield _Layout(plugin_data_fd, bindings_fd, locks_fd)
    finally:
        for descriptor in (locks_fd, bindings_fd, plugin_data_fd):
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _entry_stat(directory_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise _unavailable() from error


@contextmanager
def _exclusive_lock(
    layout: _Layout, digest: str, *, create: bool
) -> Iterator[bool]:
    if fcntl is None:
        raise _unavailable()
    name = f"{digest}.lock"
    before = _entry_stat(layout.locks_fd, name)
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    if before is None:
        if not create:
            yield False
            return
        try:
            descriptor = os.open(
                name,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=layout.locks_fd,
            )
            os.fsync(layout.locks_fd)
        except FileExistsError:
            before = _entry_stat(layout.locks_fd, name)
            if before is None:
                raise _unavailable()
            try:
                descriptor = os.open(name, flags, dir_fd=layout.locks_fd)
            except OSError as error:
                raise _unavailable() from error
        except OSError as error:
            raise _unavailable() from error
    else:
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
        ):
            raise _invalid()
        try:
            descriptor = os.open(name, flags, dir_fd=layout.locks_fd)
        except OSError as error:
            raise _unavailable() from error
    try:
        opened = os.fstat(descriptor)
        after = os.stat(name, dir_fd=layout.locks_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(after.st_mode) != 0o600
            or not _same_identity(opened, after)
            or (before is not None and not _same_identity(before, opened, after))
        ):
            raise _invalid()
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except OSError as error:
            raise _unavailable() from error
        locked = os.fstat(descriptor)
        current = os.stat(name, dir_fd=layout.locks_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(locked.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or stat.S_IMODE(locked.st_mode) != 0o600
            or stat.S_IMODE(current.st_mode) != 0o600
            or not _same_identity(opened, locked, current)
        ):
            raise _invalid()
        yield True
    except BindingError:
        raise
    except OSError as error:
        raise _unavailable() from error
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(descriptor)


def _read_binding(layout: _Layout, digest: str) -> str | None:
    name = f"{digest}.json"
    before = _entry_stat(layout.bindings_fd, name)
    if before is None:
        return None
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise _invalid()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=layout.bindings_fd)
    except OSError as error:
        raise _invalid() from error
    try:
        opened = os.fstat(descriptor)
        after = os.stat(name, dir_fd=layout.bindings_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(after.st_mode) != 0o600
            or not _same_identity(before, opened, after)
        ):
            raise _invalid()
        chunks = bytearray()
        while len(chunks) <= _MAX_BINDING_BYTES:
            chunk = os.read(descriptor, min(1024, _MAX_BINDING_BYTES + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        if len(chunks) > _MAX_BINDING_BYTES:
            raise _invalid()
        current = os.stat(name, dir_fd=layout.bindings_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(current.st_mode)
            or stat.S_IMODE(current.st_mode) != 0o600
            or not _same_identity(opened, current)
        ):
            raise _invalid()
    except BindingError:
        raise
    except OSError as error:
        raise _unavailable() from error
    finally:
        os.close(descriptor)
    try:
        decoded = json.loads(bytes(chunks).decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise _invalid() from error
    if (
        not isinstance(decoded, dict)
        or set(decoded) != {"schema_version", "run_id", "state"}
        or type(decoded.get("schema_version")) is not int
        or decoded["schema_version"] != 1
        or not isinstance(decoded.get("run_id"), str)
        or _RUN_ID_RE.fullmatch(decoded["run_id"]) is None
        or decoded.get("state") != "active"
    ):
        raise _invalid()
    return decoded["run_id"]


def _write_binding(layout: _Layout, digest: str, run_id: str, state: str) -> None:
    payload = json.dumps(
        {"schema_version": 1, "run_id": run_id, "state": state},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") + b"\n"
    destination = f"{digest}.json"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    temporary: str | None = None
    created = False
    try:
        for _ in range(16):
            candidate = f".tmp-{digest}-{secrets.token_hex(8)}"
            try:
                descriptor = os.open(
                    candidate, flags, 0o600, dir_fd=layout.bindings_fd
                )
            except FileExistsError:
                continue
            temporary = candidate
            created = True
            break
        if descriptor < 0 or temporary is None:
            raise _unavailable()
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or stat.S_IMODE(opened.st_mode) != 0o600:
            raise _invalid()
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(
            temporary,
            destination,
            src_dir_fd=layout.bindings_fd,
            dst_dir_fd=layout.bindings_fd,
        )
        os.fsync(layout.bindings_fd)
        current = os.stat(destination, dir_fd=layout.bindings_fd, follow_symlinks=False)
        if not stat.S_ISREG(current.st_mode) or stat.S_IMODE(current.st_mode) != 0o600:
            raise _invalid()
    except BindingError:
        raise
    except OSError as error:
        raise _unavailable() from error
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if created and temporary is not None:
            try:
                os.unlink(temporary, dir_fd=layout.bindings_fd)
            except FileNotFoundError:
                pass
            except OSError:
                pass


def bind_session(
    plugin_data: Path | str,
    session_id: str,
    run_id: str,
    state: str = "active",
) -> None:
    """Atomically bind an opaque Claude session to an active observation."""

    root, digest = _validate_inputs(plugin_data, session_id)
    if (
        not isinstance(run_id, str)
        or _RUN_ID_RE.fullmatch(run_id) is None
        or state != "active"
    ):
        raise _invalid()
    with _open_layout(root, create=True) as layout:
        assert layout is not None
        with _exclusive_lock(layout, digest, create=True):
            _read_binding(layout, digest)
            _write_binding(layout, digest, run_id, state)


def lookup_session(plugin_data: Path | str, session_id: str) -> str | None:
    """Return the active run for a session, or ``None`` when no binding exists."""

    root, digest = _validate_inputs(plugin_data, session_id)
    with _open_layout(root, create=False) as layout:
        if layout is None:
            return None
        binding_exists = _entry_stat(layout.bindings_fd, f"{digest}.json") is not None
        with _exclusive_lock(layout, digest, create=False) as locked:
            if not locked:
                if binding_exists:
                    raise _invalid()
                return None
            return _read_binding(layout, digest)


def unbind_session(
    plugin_data: Path | str,
    session_id: str,
    *,
    expected_run_id: str,
) -> bool:
    """Remove only a binding that still points to ``expected_run_id``."""

    root, digest = _validate_inputs(plugin_data, session_id)
    if not isinstance(expected_run_id, str) or _RUN_ID_RE.fullmatch(expected_run_id) is None:
        raise _invalid()
    with _open_layout(root, create=False) as layout:
        if layout is None:
            return False
        binding_exists = _entry_stat(layout.bindings_fd, f"{digest}.json") is not None
        with _exclusive_lock(layout, digest, create=False) as locked:
            if not locked:
                if binding_exists:
                    raise _invalid()
                return False
            current = _read_binding(layout, digest)
            if current is None or current != expected_run_id:
                return False
            try:
                os.unlink(f"{digest}.json", dir_fd=layout.bindings_fd)
                os.fsync(layout.bindings_fd)
            except OSError as error:
                raise _unavailable() from error
            return True
