"""Validated domain model for LLM Wiki observation records.

This module owns observation schemas and filesystem-independent parsing.  The
CLI adapter lives in ``wiki_cli.py`` and calls these interfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import stat
import subprocess
import sys
from urllib.parse import urlsplit


TAXONOMY = {
    "feature": {"implementation-basic", "implementation-with-review"},
    "bugfix": {"implementation-basic", "implementation-with-review"},
    "refactor": {"implementation-basic", "implementation-with-review"},
    "documentation": {"implementation-basic", "implementation-with-review"},
    "maintenance": {"maintenance-basic", "implementation-with-review"},
    "compile": {"compile-basic", "compile-with-review"},
    "inbox-processing": {"compile-basic", "compile-with-review"},
    "query": {"research-basic"},
}
FINAL_STATUSES = {"success", "partial", "failed", "rolled-back", "superseded"}

_RUN_ID_RE = re.compile(r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
_WORKSPACE_ID_RE = re.compile(r"^[0-9a-f]{12}$")
_REVISION_RE = re.compile(r"^(?:[0-9a-f]{7,40}|unknown)$")
_TASK_REF_RE = re.compile(r"^\[\[([A-Za-z0-9][A-Za-z0-9._-]*)\]\]$")
_PAYLOAD_LIMIT = 64 * 1024
_SCALAR_LIMIT = 200
_MAX_INTEGER_DIGITS = 18
_ABSOLUTE_POSIX_PATH_RE = re.compile(r"(?<![A-Za-z0-9_./-])/(?!/)[^\s]+")
_ABSOLUTE_WINDOWS_PATH_RE = re.compile(r"(?i)(?<![A-Za-z0-9_])[A-Z]:[\\/][^\s]+")
_ABSOLUTE_NETWORK_PATH_RE = re.compile(
    r"(?:\\\\[^\s]+[\\/][^\s]+|(?<!:)//[^/\s]+/[^\s]+)"
)
_CREDENTIAL_ASSIGNMENT_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?:[a-z0-9]+[_-])*"
    r"(?:api[_-]?key|(?:secret[_-]?)?access[_-]?key(?:[_-]?id)?|"
    r"access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|"
    r"refresh[_-]?token|session[_-]?token|private[_-]?key|token|password|"
    r"passwd|secret|credentials?)"
    r"(?:[_-][a-z0-9]+)*"
    r"\s*[:=]\s*\S+"
)
_URI_USERINFO_RE = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/@]+@")
_LOCAL_FILE_URI_RE = re.compile(r"(?i)\bfile:(?://)?/[^\s]+")
_COMPLETION_HEADINGS = (
    "Execution evidence",
    "Outcome and observation",
    "Follow-up",
    "Metrics",
)
_INPUT_METRIC_FIELDS = (
    "verification",
    "review_rounds",
    "defects_found",
    "rework_count",
    "rework_reason",
)
_FINAL_METRIC_FIELDS = ("finished_at", "elapsed_seconds") + _INPUT_METRIC_FIELDS


class ObservationError(Exception):
    """An observation error mapped to the CLI validation/state/I/O contract."""

    def __init__(self, kind: str, message: str):
        if kind not in {"validation", "state", "io"}:
            raise ValueError(f"unsupported observation error kind: {kind}")
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class ObservationPaths:
    root: Path
    observations: Path
    locks: Path
    invalidations: Path

    @classmethod
    def from_root(cls, root: Path) -> "ObservationPaths":
        candidate = Path(root)
        if not candidate.exists():
            raise ObservationError("validation", "wiki root does not exist")
        if not candidate.is_dir():
            raise ObservationError("validation", "wiki root must be a directory")
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as error:
            raise ObservationError("io", str(error)) from error
        observations = resolved / "wiki" / "observations"
        locks = observations / ".locks"
        invalidations = observations / "invalidations"
        for path in (observations, locks, invalidations):
            try:
                resolved_path = path.resolve(strict=False)
                resolved_path.relative_to(resolved)
            except (OSError, ValueError) as error:
                raise ObservationError(
                    "validation", "observation symlink path escapes wiki root"
                ) from error
        for path in (resolved / "wiki", observations, locks, invalidations):
            if path.is_symlink():
                raise ObservationError(
                    "validation", "observation path must not contain symlinks"
                )
        return cls(resolved, observations, locks, invalidations)

    def record(self, run_id: str) -> Path:
        return self.observations / f"{run_id}.md"

    def invalidation(self, run_id: str) -> Path:
        return self.invalidations / f"{run_id}.md"


@dataclass(frozen=True)
class Provenance:
    project: str
    workspace: str
    workspace_id: str
    revision: str
    working_tree: str


@dataclass(frozen=True)
class StartRequest:
    title: str
    project: str
    workspace: str
    workspace_id: str
    revision: str
    working_tree: str
    agent_surface: str
    start_mode: str
    task_type: str
    workflow_variant: str
    task_ref: str | None
    sources: tuple[str, ...]


@dataclass(frozen=True)
class ScopePayload:
    goal: str
    included: str
    excluded: str


@dataclass(frozen=True)
class CompletionPayload:
    execution_verification: str
    artifacts: str
    outcome: str
    observation: str
    follow_up: str
    verification: str
    review_rounds: int | str
    defects_found: int | str
    rework_count: int | str
    rework_reason: str


@dataclass(frozen=True)
class ReportFilters:
    project: str | None = None
    workspace: str | None = None
    workspace_id: str | None = None
    task_type: str | None = None
    status: str | None = None
    since: date | None = None
    until: date | None = None


def _validation(message: str) -> ObservationError:
    return ObservationError("validation", message)


def _validate_scalar(value: object, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise _validation(f"{field} must be a string")
    if not allow_empty and not value.strip():
        raise _validation(f"{field} must not be empty")
    if len(value) > _SCALAR_LIMIT:
        raise _validation(f"{field} must not exceed 200 Unicode code points")
    if any(ord(character) < 32 for character in value):
        raise _validation(f"{field} contains a control character")
    if "---" in value:
        raise _validation(f"{field} contains a frontmatter delimiter")
    if any(
        pattern.search(value)
        for pattern in (
            _ABSOLUTE_POSIX_PATH_RE,
            _ABSOLUTE_WINDOWS_PATH_RE,
            _ABSOLUTE_NETWORK_PATH_RE,
            _CREDENTIAL_ASSIGNMENT_RE,
            _URI_USERINFO_RE,
            _LOCAL_FILE_URI_RE,
        )
    ):
        raise _validation(f"{field} contains sensitive path or credential text")
    return value


def _sanitize_workspace_name(value: str) -> str:
    sanitized = " ".join(value.replace("---", "-").split())[:_SCALAR_LIMIT]
    return sanitized or "workspace"


def _run_git(root: Path, *arguments: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(root), *arguments],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _normalize_remote(remote: str) -> tuple[str, str] | None:
    remote = remote.strip()
    if not remote:
        return None
    host = ""
    port: int | None = None
    path = ""
    scheme = "ssh"
    scp_match = re.fullmatch(r"(?:[^@/:]+@)?([^:/]+):(.+)", remote)
    if scp_match and "://" not in remote:
        host, path = scp_match.groups()
    else:
        parsed = urlsplit(remote)
        if not parsed.hostname:
            return None
        scheme = parsed.scheme.lower()
        host = parsed.hostname
        try:
            port = parsed.port
        except ValueError:
            return None
        path = parsed.path
    host = host.lower()
    default_ports = {"ssh": 22, "http": 80, "https": 443, "git": 9418}
    host_identity = host
    if port is not None and port != default_ports.get(scheme):
        host_identity += f":{port}"
    path = re.sub(r"/+", "/", path).lstrip("/").rstrip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    if not host_identity or not path:
        return None
    return f"{host_identity}/{path}", path.rsplit("/", 1)[-1]


def derive_provenance(
    subject_root: Path, project_override: str | None = None
) -> Provenance:
    candidate = Path(subject_root)
    if not candidate.exists():
        raise _validation("subject root does not exist")
    if not candidate.is_dir():
        raise _validation("subject root must be a directory")
    try:
        resolved_subject = candidate.resolve(strict=True)
    except OSError as error:
        raise ObservationError("io", str(error)) from error

    top_level_text = _run_git(resolved_subject, "rev-parse", "--show-toplevel")
    if top_level_text:
        try:
            workspace_root = Path(top_level_text).resolve(strict=True)
        except OSError:
            workspace_root = resolved_subject
    else:
        workspace_root = resolved_subject

    workspace = _sanitize_workspace_name(workspace_root.name)
    remote = _run_git(workspace_root, "remote", "get-url", "origin")
    normalized_remote = _normalize_remote(remote) if remote else None
    identity_input = normalized_remote[0] if normalized_remote else str(workspace_root)
    workspace_id = hashlib.sha256(identity_input.encode("utf-8")).hexdigest()[:12]

    if project_override is not None:
        project = _validate_scalar(project_override, "project")
    elif normalized_remote:
        project = _validate_scalar(normalized_remote[1], "project")
    else:
        project = workspace

    revision = _run_git(workspace_root, "rev-parse", "HEAD") or "unknown"
    if revision != "unknown" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", revision):
        revision = "unknown"
    revision = revision.lower()
    if top_level_text:
        status = _run_git(workspace_root, "status", "--porcelain")
        working_tree = "unknown" if status is None else ("dirty" if status else "clean")
    else:
        working_tree = "unknown"
    return Provenance(project, workspace, workspace_id, revision, working_tree)


def _validate_task_ref(task_ref: object) -> None:
    if task_ref is None:
        return
    if not isinstance(task_ref, str) or _TASK_REF_RE.fullmatch(task_ref) is None:
        raise _validation("task_ref must be a single safe task Wikilink")


def _validate_sources(sources: object) -> tuple[str, ...]:
    if not isinstance(sources, tuple):
        raise _validation("sources must be a tuple")
    if not all(isinstance(source, str) for source in sources):
        raise _validation("sources must contain only strings")
    if len(set(sources)) != len(sources):
        raise _validation("sources must be unique")
    for source in sources:
        if any(ord(character) < 32 for character in source) or "---" in source:
            raise _validation("sources contain a control character or frontmatter delimiter")
        if not isinstance(source, str) or "\\" in source:
            raise _validation("sources must be normalized raw paths")
        pure = PurePosixPath(source)
        if (
            pure.is_absolute()
            or len(pure.parts) < 2
            or pure.parts[0] != "raw"
            or any(part in {"", ".", ".."} for part in pure.parts)
            or pure.as_posix() != source
        ):
            raise _validation("sources must be normalized relative raw paths")
    return sources


def _start_request_errors(request: StartRequest) -> list[str]:
    errors: list[str] = []
    if not isinstance(request, StartRequest):
        return ["start request has the wrong type"]
    for field in ("title", "project", "workspace"):
        try:
            _validate_scalar(getattr(request, field), field)
        except ObservationError as error:
            errors.append(str(error))
    if not isinstance(request.workspace_id, str) or not _WORKSPACE_ID_RE.fullmatch(
        request.workspace_id
    ):
        errors.append("workspace_id must be 12 lowercase hex")
    if not isinstance(request.revision, str) or not _REVISION_RE.fullmatch(request.revision):
        errors.append("revision must be 7-40 lowercase hex or unknown")
    if (
        not isinstance(request.working_tree, str)
        or request.working_tree not in {"clean", "dirty", "unknown"}
    ):
        errors.append("working_tree must be clean, dirty, or unknown")
    if request.agent_surface not in {"codex", "claude"}:
        errors.append("agent_surface must be codex or claude")
    if not isinstance(request.start_mode, str) or request.start_mode not in {"planned", "late"}:
        errors.append("start_mode must be planned or late")
    if (
        not isinstance(request.task_type, str)
        or request.task_type not in TAXONOMY
        or not isinstance(request.workflow_variant, str)
        or request.workflow_variant not in TAXONOMY[request.task_type]
    ):
        errors.append("invalid taxonomy combination")
    try:
        _validate_task_ref(request.task_ref)
    except ObservationError as error:
        errors.append(str(error))
    try:
        _validate_sources(request.sources)
    except ObservationError as error:
        errors.append(str(error))
    return errors


def validate_start_request(request: StartRequest) -> None:
    errors = _start_request_errors(request)
    if errors:
        raise _validation(errors[0])


def _validate_payload_text(text: object) -> str:
    if not isinstance(text, str):
        raise _validation("payload must be UTF-8 text")
    if len(text.encode("utf-8")) > _PAYLOAD_LIMIT:
        raise _validation("payload must not exceed 64 KiB")
    for character in text:
        if ord(character) < 32 and character not in {"\n"}:
            raise _validation("payload contains a control character")
    if any(line.strip() == "---" for line in text.splitlines()):
        raise _validation("payload contains a frontmatter delimiter")
    return text


def parse_scope_payload(text: str) -> ScopePayload:
    text = _validate_payload_text(text)
    match = re.fullmatch(
        r"## Scope\n\n- Goal: ([^\n]*)\n- Included: ([^\n]*)\n- Excluded: ([^\n]*)\n?",
        text,
    )
    if match is None:
        raise _validation("Scope payload must contain only Goal, Included, and Excluded")
    goal, included, excluded = match.groups()
    for field, value in (("Goal", goal), ("Included", included), ("Excluded", excluded)):
        _validate_scalar(value, field)
    return ScopePayload(goal, included, excluded)


def _split_completion_sections(text: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?m)^## ([^\n]+)\n", text))
    names = tuple(match.group(1) for match in matches)
    if names != _COMPLETION_HEADINGS or not matches or matches[0].start() != 0:
        missing = [name for name in _COMPLETION_HEADINGS if name not in names]
        if missing:
            raise _validation(f"missing {missing[0]}")
        raise _validation("completion headings must be unique and in the fixed order")
    sections: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        sections[match.group(1)] = text[match.end():end].strip()
    return sections


def _parse_labeled_lines(section: str, fields: tuple[str, ...], context: str) -> dict[str, str]:
    values: dict[str, str] = {}
    lines = [line for line in section.splitlines() if line.strip()]
    for line in lines:
        match = re.fullmatch(r"- ([A-Za-z ]+):\s*(.*)", line)
        if match is None:
            raise _validation(f"invalid {context} content")
        label, value = match.groups()
        if label not in fields or label in values:
            raise _validation(f"duplicate or unexpected {context} label")
        _validate_scalar(value, label)
        values[label] = value
    for field in fields:
        if field not in values:
            raise _validation(f"missing {field}")
    return values


def _unquote_metric(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as error:
            raise _validation("invalid quoted metric value") from error
        if not isinstance(decoded, str):
            raise _validation("metric scalar must be a string")
        return decoded
    return value


def _parse_bounded_nonnegative_integer(value: str, field: str) -> int:
    if not re.fullmatch(r"[0-9]+", value) or len(value) > _MAX_INTEGER_DIGITS:
        raise _validation(f"{field} must be a bounded nonnegative integer")
    try:
        return int(value)
    except ValueError as error:
        raise _validation(f"{field} must be a bounded nonnegative integer") from error


def _parse_metrics(section: str, *, allow_derived: bool) -> tuple[dict[str, object], dict[str, object]]:
    match = re.fullmatch(r"```yaml\n(.*?)\n```", section, re.DOTALL)
    if match is None:
        raise _validation("Metrics must be one yaml code block")
    raw: dict[str, str] = {}
    for line in match.group(1).splitlines():
        field_match = re.fullmatch(r"([a-z_]+):\s*(.*)", line)
        if field_match is None:
            raise _validation("invalid Metrics line")
        key, value = field_match.groups()
        if key in raw:
            raise _validation(f"duplicate Metrics field {key}")
        raw[key] = value
    expected = _FINAL_METRIC_FIELDS if allow_derived else _INPUT_METRIC_FIELDS
    if tuple(raw) != expected:
        missing = [field for field in expected if field not in raw]
        if missing:
            raise _validation(f"missing Metrics field {missing[0]}")
        raise _validation("unexpected or out-of-order Metrics field")

    parsed: dict[str, object] = {}
    if allow_derived:
        parsed["finished_at"] = _unquote_metric(raw["finished_at"])
        elapsed = raw["elapsed_seconds"]
        parsed["elapsed_seconds"] = _parse_bounded_nonnegative_integer(
            elapsed, "elapsed_seconds"
        )
    verification = _unquote_metric(raw["verification"])
    if verification not in {"pass", "fail", "not-run", "unknown"}:
        raise _validation("invalid verification metric")
    parsed["verification"] = verification
    for field in ("review_rounds", "defects_found", "rework_count"):
        value = _unquote_metric(raw[field])
        if value == "unknown":
            parsed[field] = value
        elif re.fullmatch(r"[0-9]+", value):
            parsed[field] = _parse_bounded_nonnegative_integer(value, field)
        else:
            raise _validation(f"{field} must be a nonnegative integer or unknown")
    reason = _unquote_metric(raw["rework_reason"])
    _validate_scalar(reason, "rework_reason")
    parsed["rework_reason"] = reason
    rework_count = parsed["rework_count"]
    if rework_count == 0 and reason != "none":
        raise _validation("rework_count 0 requires rework_reason none")
    if isinstance(rework_count, int) and rework_count > 0 and reason == "none":
        raise _validation("positive rework_count cannot use rework_reason none")
    if rework_count == "unknown" and reason == "none":
        raise _validation("unknown rework_count cannot assert rework_reason none")
    derived = {key: parsed.pop(key) for key in ("finished_at", "elapsed_seconds") if key in parsed}
    return parsed, derived


def _parse_completion(text: str, *, allow_derived: bool) -> tuple[CompletionPayload, dict[str, object]]:
    text = _validate_payload_text(text)
    sections = _split_completion_sections(text)
    evidence = _parse_labeled_lines(
        sections["Execution evidence"], ("Verification", "Artifacts"), "Execution evidence"
    )
    outcome = _parse_labeled_lines(
        sections["Outcome and observation"], ("Outcome", "Observation"), "Outcome and observation"
    )
    follow_up = sections["Follow-up"]
    if not follow_up.startswith("- ") or not follow_up[2:].strip():
        raise _validation("Follow-up must contain a next action or None — no further action")
    follow_up_value = follow_up[2:].strip()
    _validate_scalar(follow_up_value, "Follow-up")
    metrics, derived = _parse_metrics(sections["Metrics"], allow_derived=allow_derived)
    return CompletionPayload(
        evidence["Verification"],
        evidence["Artifacts"],
        outcome["Outcome"],
        outcome["Observation"],
        follow_up_value,
        metrics["verification"],
        metrics["review_rounds"],
        metrics["defects_found"],
        metrics["rework_count"],
        metrics["rework_reason"],
    ), derived


def parse_completion_payload(text: str) -> CompletionPayload:
    payload, _ = _parse_completion(text, allow_derived=False)
    return payload


def _parse_frontmatter(content: str) -> tuple[dict[str, object], str]:
    if not content.startswith("---\n"):
        raise _validation("record is missing frontmatter")
    closing = content.find("\n---\n", 4)
    if closing < 0:
        raise _validation("record has malformed frontmatter")
    header = content[4:closing]
    body = content[closing + 5:].lstrip("\n")
    metadata: dict[str, object] = {}
    for line in header.splitlines():
        if not line or ":" not in line:
            raise _validation("record has malformed frontmatter")
        key, raw = line.split(":", 1)
        key = key.strip()
        raw = raw.strip()
        if not re.fullmatch(r"[a-z_]+", key) or key in metadata:
            raise _validation("record has malformed frontmatter")
        try:
            if raw.startswith("[") or raw.startswith('"'):
                value = json.loads(raw)
            elif re.fullmatch(r"-?[0-9]+", raw):
                if raw.startswith("-"):
                    raise _validation("record has an invalid integer frontmatter value")
                value = _parse_bounded_nonnegative_integer(raw, key)
            elif raw in {"null", "~"}:
                value = None
            else:
                value = raw.strip("'")
        except json.JSONDecodeError as error:
            raise _validation("record has unsafe frontmatter") from error
        metadata[key] = value
    return metadata, body


def read_record(paths: ObservationPaths, run_id: str) -> tuple[dict, str]:
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise _validation("run_id has an invalid format")
    path = paths.record(run_id)
    try:
        path.resolve(strict=False).relative_to(paths.observations.resolve(strict=False))
    except (OSError, ValueError) as error:
        raise _validation("record path escapes observations directory") from error
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise ObservationError("state", f"observation {run_id} does not exist") from error
    except (OSError, UnicodeError) as error:
        raise ObservationError("io", str(error)) from error
    try:
        metadata, body = _parse_frontmatter(content)
        if metadata.get("run_id") != run_id:
            raise _validation("record run_id does not match filename")
        return metadata, body
    except ObservationError:
        raise
    except Exception as error:  # defensive conversion at the persistence boundary
        raise _validation("record is malformed") from error


def _aware_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise _validation(f"{field} must be an aware ISO-8601 datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise _validation(f"{field} must be an aware ISO-8601 datetime") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _validation(f"{field} must be an aware ISO-8601 datetime")
    return parsed


def _secure_reference_file(
    base: Path, relative: PurePosixPath, wiki_root: Path
) -> Path:
    try:
        root_resolved = wiki_root.resolve(strict=True)
        if base.is_symlink():
            raise _validation("reference directory must not be a symlink")
        base_resolved = base.resolve(strict=True)
        base_resolved.relative_to(root_resolved)
        candidate = base.joinpath(*relative.parts)
        current = base
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise _validation("reference target must not be a symlink")
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(base_resolved)
        if not resolved.is_file():
            raise FileNotFoundError(str(candidate))
        return resolved
    except ObservationError:
        raise
    except (FileNotFoundError, NotADirectoryError):
        raise
    except (OSError, ValueError) as error:
        raise _validation("reference target escapes its required directory") from error


def _task_record_is_valid(path: Path, expected_id: str) -> bool:
    try:
        content = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return False
    if not content.startswith("---\n"):
        return False
    closing = content.find("\n---\n", 4)
    if closing < 0:
        return False
    fields: dict[str, object] = {}
    for line in content[4:closing].splitlines():
        if ":" not in line:
            return False
        key, value = line.split(":", 1)
        key = key.strip()
        if not re.fullmatch(r"[a-z_]+", key) or key in fields:
            return False
        raw = value.strip()
        if raw.startswith("[") and raw.endswith("]"):
            inner = raw[1:-1].strip()
            fields[key] = (
                []
                if not inner
                else [item.strip().strip('"').strip("'") for item in inner.split(",")]
            )
        else:
            fields[key] = raw.strip('"').strip("'")
    required = {"type", "id", "title", "status", "tags", "timestamp", "sources"}
    if required - set(fields):
        return False
    try:
        _validate_scalar(fields["title"], "task title")
        date.fromisoformat(fields["timestamp"])
    except (ObservationError, TypeError, ValueError):
        return False
    tags = fields["tags"]
    sources = fields["sources"]
    return (
        fields.get("type") == "task"
        and fields.get("id") == expected_id
        and fields.get("status") in {"pending", "waiting", "blocked", "done"}
        and isinstance(tags, list)
        and all(isinstance(tag, str) and tag for tag in tags)
        and isinstance(sources, list)
        and all(
            isinstance(source, str)
            and source.startswith("raw/")
            and ".." not in PurePosixPath(source).parts
            for source in sources
        )
    )


def _partial_outcome_has_incomplete_items(outcome: str) -> bool:
    match = re.search(
        r"(?:^|[.;]\s+)(?:Incomplete|Deferred|Remaining) Included items:\s*(.+?)(?:[.;]|$)",
        outcome,
    )
    if match is None:
        return False
    value = match.group(1).strip().lower()
    return value not in {"", "none", "none.", "nothing", "n/a", "unknown"}


def validate_record(
    metadata: dict,
    body: str,
    paths: ObservationPaths,
    _reference_chain: frozenset[str] | None = None,
) -> list[str]:
    errors: list[str] = []
    required = {
        "type", "title", "tags", "run_id", "timestamp", "project", "workspace",
        "workspace_id", "revision", "working_tree", "agent_surface", "task_type",
        "workflow_variant", "status", "start_mode", "sources",
    }
    if not isinstance(metadata, dict):
        return ["observation metadata must be an object"]
    allowed = required | {"task_ref", "superseded_by"}
    for field in sorted(set(metadata) - allowed):
        errors.append(f"unexpected frontmatter field `{field}`")
    for field in sorted(required - set(metadata)):
        errors.append(f"missing required field `{field}`")
    if required - set(metadata):
        return errors

    if metadata.get("type") != "observation":
        errors.append("type must be observation")
    tags = metadata.get("tags")
    if (
        not isinstance(tags, list)
        or not all(isinstance(tag, str) for tag in tags)
        or not {"observation", "workflow"}.issubset(set(tags))
    ):
        errors.append("tags must include observation and workflow")
    run_id = metadata.get("run_id")
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        errors.append("run_id has an invalid format")

    started: datetime | None = None
    try:
        started = _aware_datetime(metadata.get("timestamp"), "timestamp")
    except ObservationError as error:
        errors.append(str(error))

    sources = metadata.get("sources")
    request = StartRequest(
        metadata.get("title"), metadata.get("project"), metadata.get("workspace"),
        metadata.get("workspace_id"), metadata.get("revision"),
        metadata.get("working_tree"), metadata.get("agent_surface"),
        metadata.get("start_mode"), metadata.get("task_type"),
        metadata.get("workflow_variant"), metadata.get("task_ref"),
        tuple(sources) if isinstance(sources, list) else sources,
    )
    errors.extend(_start_request_errors(request))

    if isinstance(sources, list):
        for source in sources:
            if isinstance(source, str) and source.startswith("raw/"):
                try:
                    _secure_reference_file(
                        paths.root / "raw",
                        PurePosixPath(source).relative_to("raw"),
                        paths.root,
                    )
                except (FileNotFoundError, NotADirectoryError):
                    errors.append(f"source does not exist: {source}")
                except (ObservationError, ValueError):
                    errors.append(f"source must not be a symlink or escape raw: {source}")
    task_ref = metadata.get("task_ref")
    if isinstance(task_ref, str):
        match = _TASK_REF_RE.fullmatch(task_ref)
        if match:
            task_id = match.group(1)
            try:
                task_path = _secure_reference_file(
                    paths.root / "wiki" / "tasks",
                    PurePosixPath(f"{task_id}.md"),
                    paths.root,
                )
            except (FileNotFoundError, NotADirectoryError):
                errors.append("task_ref points to no task record")
            except ObservationError:
                errors.append("task_ref points to an invalid task record")
            else:
                if not _task_record_is_valid(task_path, task_id):
                    errors.append("task_ref points to an invalid task record")

    status = metadata.get("status")
    if not isinstance(status, str) or status not in FINAL_STATUSES | {"draft"}:
        errors.append(f"invalid status `{status}`")
    superseded_by = metadata.get("superseded_by")

    if status == "draft":
        if superseded_by is not None:
            errors.append("draft record must not contain superseded_by")
        try:
            parse_scope_payload(body)
        except ObservationError:
            errors.append("draft record must contain only Scope")
        return errors

    scope_marker = "\n## Execution evidence"
    if scope_marker not in body:
        errors.append("final record must contain Scope and completion sections")
        return errors
    scope_text, completion_tail = body.split(scope_marker, 1)
    try:
        parse_scope_payload(scope_text.rstrip() + "\n")
    except ObservationError as error:
        errors.append(str(error))
    completion: CompletionPayload | None = None
    derived: dict[str, object] = {}
    try:
        completion, derived = _parse_completion(
            "## Execution evidence" + completion_tail, allow_derived=True
        )
    except ObservationError as error:
        errors.append(str(error))

    if status == "superseded":
        if not isinstance(superseded_by, str) or _RUN_ID_RE.fullmatch(superseded_by) is None:
            errors.append("superseded status requires superseded_by")
        elif superseded_by == run_id:
            errors.append("superseded_by must not reference itself")
        elif _reference_chain and superseded_by in _reference_chain:
            errors.append("superseded_by must not create a cycle")
        else:
            try:
                _secure_reference_file(
                    paths.observations,
                    PurePosixPath(f"{superseded_by}.md"),
                    paths.root,
                )
                target_metadata, target_body = read_record(paths, superseded_by)
            except (FileNotFoundError, NotADirectoryError, ObservationError) as error:
                if isinstance(error, ObservationError) and error.kind != "state":
                    errors.append("superseded_by points to an invalid observation record")
                else:
                    errors.append("superseded_by points to no observation record")
            else:
                chain = (_reference_chain or frozenset()) | {
                    run_id if isinstance(run_id, str) else ""
                }
                target_errors = validate_record(
                    target_metadata, target_body, paths, frozenset(chain)
                )
                if target_metadata.get("run_id") != superseded_by or target_errors:
                    errors.append("superseded_by points to an invalid observation record")
    elif superseded_by is not None:
        errors.append("superseded_by is only valid for superseded status")

    if completion is not None:
        if status == "success" and completion.verification == "fail":
            errors.append("success status cannot have fail verification")
        if status == "partial":
            if completion.follow_up == "None — no further action":
                errors.append("partial status requires a follow-up action")
            if not _partial_outcome_has_incomplete_items(completion.outcome):
                errors.append("partial outcome must identify incomplete Included items")
        try:
            finished = _aware_datetime(derived.get("finished_at"), "finished_at")
            elapsed = derived.get("elapsed_seconds")
            if started is not None:
                if finished < started:
                    errors.append("finished_at must not be earlier than timestamp")
                elif elapsed != int((finished - started).total_seconds()):
                    errors.append("elapsed_seconds must equal finished_at minus timestamp")
        except ObservationError as error:
            errors.append(str(error))
    return errors


def _render_scope(scope: ScopePayload) -> str:
    if not isinstance(scope, ScopePayload):
        raise _validation("Scope payload has the wrong type")
    for field, value in (
        ("Scope Goal", scope.goal),
        ("Scope Included", scope.included),
        ("Scope Excluded", scope.excluded),
    ):
        _validate_scalar(value, field)
    text = (
        "## Scope\n\n"
        f"- Goal: {scope.goal}\n"
        f"- Included: {scope.included}\n"
        f"- Excluded: {scope.excluded}\n"
    )
    try:
        parse_scope_payload(text)
    except ObservationError as error:
        raise _validation(f"Scope payload: {error}") from error
    return text


def _draft_metadata(
    run_id: str, started: datetime, request: StartRequest
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "type": "observation",
        "title": request.title,
        "tags": ["observation", "workflow"],
        "run_id": run_id,
        "timestamp": started.isoformat(),
        "project": request.project,
        "workspace": request.workspace,
        "workspace_id": request.workspace_id,
        "revision": request.revision,
        "working_tree": request.working_tree,
        "agent_surface": request.agent_surface,
        "task_type": request.task_type,
        "workflow_variant": request.workflow_variant,
        "status": "draft",
        "start_mode": request.start_mode,
    }
    if request.task_ref is not None:
        metadata["task_ref"] = request.task_ref
    metadata["sources"] = list(request.sources)
    return metadata


def _render_frontmatter(metadata: dict[str, object]) -> str:
    lines = ["---"]
    for key, value in metadata.items():
        lines.append(
            f"{key}: {json.dumps(value, ensure_ascii=False, separators=(',', ':'))}"
        )
    lines.extend(["---", ""])
    return "\n".join(lines)


def _render_draft(
    run_id: str, started: datetime, request: StartRequest, scope_text: str
) -> tuple[dict[str, object], str]:
    metadata = _draft_metadata(run_id, started, request)
    return metadata, _render_frontmatter(metadata) + scope_text


def _validated_start_time(now: datetime | None) -> datetime:
    if now is None:
        value = datetime.now().astimezone()
    elif isinstance(now, datetime):
        value = now
    else:
        raise _validation("start time must be an aware datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise _validation("start time must be an aware datetime")
    return value.replace(microsecond=0)


def _canonical_observation_paths(paths: ObservationPaths) -> ObservationPaths:
    if not isinstance(paths, ObservationPaths):
        raise _validation("observation paths have the wrong type")
    canonical = ObservationPaths.from_root(paths.root)
    for supplied, expected in (
        (paths.observations, canonical.observations),
        (paths.locks, canonical.locks),
        (paths.invalidations, canonical.invalidations),
    ):
        try:
            if supplied.resolve(strict=False) != expected.resolve(strict=False):
                raise _validation("observation path escapes canonical wiki root")
        except OSError as error:
            raise ObservationError("io", str(error)) from error
    return canonical


def _raise_record_validation(errors: list[str]) -> None:
    if errors:
        raise _validation(errors[0])


def _assert_directory_identity(directory_fd: int, path: Path) -> None:
    try:
        held = os.fstat(directory_fd)
        current = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise _validation("observation directory changed during start") from error
    if (
        not stat.S_ISDIR(current.st_mode)
        or (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise _validation("observation directory changed during start")


def _close_preserving_error(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _unlink_preserving_error(name: str, directory_fd: int) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass


def _open_observation_directory(path: Path) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if path.is_symlink():
            raise _validation("observation directory changed to a symlink") from error
        raise ObservationError("io", str(error)) from error
    try:
        _assert_directory_identity(descriptor, path)
    except Exception:
        _close_preserving_error(descriptor)
        raise
    return descriptor


def _create_temporary_file(directory_fd: int):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    for _ in range(32):
        name = f".observation-{secrets.token_urlsafe(12)}.tmp"
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError:
            continue
        try:
            return name, os.fdopen(descriptor, "w", encoding="utf-8")
        except Exception:
            _close_preserving_error(descriptor)
            _unlink_preserving_error(name, directory_fd)
            raise
    raise OSError("could not allocate a unique temporary observation file")


def _assert_temporary_in_directory(
    temporary, temporary_name: str, directory_fd: int
) -> None:
    try:
        opened = os.fstat(temporary.fileno())
        entry = os.stat(
            temporary_name, dir_fd=directory_fd, follow_symlinks=False
        )
    except OSError as error:
        raise _validation("observation directory changed before temporary write") from error
    if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
        raise _validation("observation directory changed before temporary write")


def _cleanup_temporary(
    temporary_name: str | None, directory_fd: int, *, claimed: bool
) -> None:
    if temporary_name is None:
        return
    errors: list[OSError] = []
    for _ in range(2):
        try:
            os.unlink(temporary_name, dir_fd=directory_fd)
            return
        except FileNotFoundError:
            return
        except OSError as error:
            errors.append(error)
    if not claimed:
        raise ObservationError(
            "io", f"temporary cleanup failed: {errors[0]}"
        ) from errors[-1]


def _cleanup_before_error(
    temporary_name: str | None,
    directory_fd: int,
    original: ObservationError,
) -> None:
    try:
        _cleanup_temporary(temporary_name, directory_fd, claimed=False)
    except ObservationError as cleanup_error:
        raise ObservationError("io", f"{original}; {cleanup_error}") from original


def _rollback_claim(
    directory_fd: int, destination_name: str, operation: str = "start"
) -> None:
    try:
        os.unlink(destination_name, dir_fd=directory_fd)
        os.fsync(directory_fd)
    except OSError as error:
        raise ObservationError(
            "io", f"could not roll back failed {operation}: {error}"
        ) from error


def start_observation(
    paths: ObservationPaths,
    request: StartRequest,
    scope: ScopePayload,
    now: datetime | None = None,
) -> str:
    """Create one validated draft through an atomic exclusive hard-link claim."""

    validate_start_request(request)
    scope_text = _render_scope(scope)
    started = _validated_start_time(now)
    secure_paths = _canonical_observation_paths(paths)

    validation_id = f"obs-{started:%Y%m%d-%H%M%S}-000000"
    validation_metadata, _ = _render_draft(
        validation_id, started, request, scope_text
    )
    _raise_record_validation(
        validate_record(validation_metadata, scope_text, secure_paths)
    )

    try:
        secure_paths.observations.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ObservationError("io", str(error)) from error
    secure_paths = _canonical_observation_paths(secure_paths)
    directory_fd = _open_observation_directory(secure_paths.observations)
    committed = False
    try:
        for _ in range(32):
            _assert_directory_identity(directory_fd, secure_paths.observations)
            run_id = f"obs-{started:%Y%m%d-%H%M%S}-{secrets.token_hex(3)}"
            metadata, content = _render_draft(run_id, started, request, scope_text)
            _raise_record_validation(validate_record(metadata, scope_text, secure_paths))
            destination_name = f"{run_id}.md"
            temporary_name: str | None = None
            try:
                temporary_name, temporary = _create_temporary_file(directory_fd)
                with temporary:
                    _assert_directory_identity(
                        directory_fd, secure_paths.observations
                    )
                    _assert_temporary_in_directory(
                        temporary, temporary_name, directory_fd
                    )
                    os.fchmod(temporary.fileno(), 0o644)
                    temporary.write(content)
                    temporary.flush()
                    os.fsync(temporary.fileno())
            except (OSError, ObservationError) as error:
                mapped = (
                    error
                    if isinstance(error, ObservationError)
                    else ObservationError("io", str(error))
                )
                _cleanup_before_error(temporary_name, directory_fd, mapped)
                raise mapped

            try:
                _assert_directory_identity(directory_fd, secure_paths.observations)
            except ObservationError as error:
                _cleanup_before_error(temporary_name, directory_fd, error)
                raise
            try:
                os.link(
                    temporary_name,
                    destination_name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                _cleanup_temporary(temporary_name, directory_fd, claimed=False)
                continue
            except OSError as error:
                mapped = ObservationError("io", str(error))
                _cleanup_before_error(temporary_name, directory_fd, mapped)
                raise mapped from error

            try:
                os.fsync(directory_fd)
                _assert_directory_identity(directory_fd, secure_paths.observations)
            except (OSError, ObservationError) as error:
                try:
                    _rollback_claim(directory_fd, destination_name)
                finally:
                    _cleanup_temporary(temporary_name, directory_fd, claimed=False)
                if isinstance(error, ObservationError):
                    raise
                raise ObservationError("io", str(error)) from error

            _cleanup_temporary(temporary_name, directory_fd, claimed=True)
            committed = True
            return run_id
        raise ObservationError("io", "could not allocate a unique run id")
    finally:
        error_in_flight = sys.exc_info()[0] is not None
        try:
            os.close(directory_fd)
        except OSError as error:
            if not committed and not error_in_flight:
                raise ObservationError("io", str(error)) from error


def _validated_event_time(now: datetime | None, field: str) -> datetime:
    if now is None:
        value = datetime.now().astimezone()
    elif isinstance(now, datetime):
        value = now
    else:
        raise _validation(f"{field} must be an aware datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise _validation(f"{field} must be an aware datetime")
    return value.replace(microsecond=0)


def _yaml_metric_scalar(value: int | str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise _validation("metric value has the wrong type")
    if isinstance(value, int):
        return str(value)
    if re.fullmatch(r"[A-Za-z0-9-]+", value):
        return value
    return json.dumps(value, ensure_ascii=False)


def _validate_completion_payload(payload: CompletionPayload) -> None:
    if not isinstance(payload, CompletionPayload):
        raise _validation("completion payload has the wrong type")
    for field, value in (
        ("Verification", payload.execution_verification),
        ("Artifacts", payload.artifacts),
        ("Outcome", payload.outcome),
        ("Observation", payload.observation),
        ("Follow-up", payload.follow_up),
        ("rework_reason", payload.rework_reason),
    ):
        _validate_scalar(value, field)
    if (
        not isinstance(payload.verification, str)
        or payload.verification not in {"pass", "fail", "not-run", "unknown"}
    ):
        raise _validation("invalid verification metric")
    for field, value in (
        ("review_rounds", payload.review_rounds),
        ("defects_found", payload.defects_found),
        ("rework_count", payload.rework_count),
    ):
        if value == "unknown":
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or len(str(value)) > _MAX_INTEGER_DIGITS
        ):
            raise _validation(
                f"{field} must be a bounded nonnegative integer or unknown"
            )
    if payload.rework_count == 0 and payload.rework_reason != "none":
        raise _validation("rework_count 0 requires rework_reason none")
    if (
        isinstance(payload.rework_count, int)
        and not isinstance(payload.rework_count, bool)
        and payload.rework_count > 0
        and payload.rework_reason == "none"
    ):
        raise _validation("positive rework_count cannot use rework_reason none")
    if payload.rework_count == "unknown" and payload.rework_reason == "none":
        raise _validation("unknown rework_count cannot assert rework_reason none")


def _render_completion(
    payload: CompletionPayload, finished: datetime, elapsed_seconds: int
) -> str:
    _validate_completion_payload(payload)
    text = (
        "## Execution evidence\n\n"
        f"- Verification: {payload.execution_verification}\n"
        f"- Artifacts: {payload.artifacts}\n\n"
        "## Outcome and observation\n\n"
        f"- Outcome: {payload.outcome}\n"
        f"- Observation: {payload.observation}\n\n"
        "## Follow-up\n\n"
        f"- {payload.follow_up}\n\n"
        "## Metrics\n\n"
        "```yaml\n"
        f"finished_at: {json.dumps(finished.isoformat())}\n"
        f"elapsed_seconds: {elapsed_seconds}\n"
        f"verification: {_yaml_metric_scalar(payload.verification)}\n"
        f"review_rounds: {_yaml_metric_scalar(payload.review_rounds)}\n"
        f"defects_found: {_yaml_metric_scalar(payload.defects_found)}\n"
        f"rework_count: {_yaml_metric_scalar(payload.rework_count)}\n"
        f"rework_reason: {_yaml_metric_scalar(payload.rework_reason)}\n"
        "```\n"
    )
    _parse_completion(text, allow_derived=True)
    return text


def _render_completed_record(
    metadata: dict[str, object],
    scope_body: str,
    status: str,
    payload: CompletionPayload,
    superseded_by: str | None,
    finished: datetime,
    paths: ObservationPaths,
) -> str:
    started = _aware_datetime(metadata.get("timestamp"), "timestamp")
    if finished < started:
        raise _validation("finished_at must not be earlier than timestamp")
    elapsed_seconds = int((finished - started).total_seconds())
    completed_metadata = dict(metadata)
    completed_metadata["status"] = status
    completed_metadata.pop("superseded_by", None)
    if superseded_by is not None:
        completed_metadata["superseded_by"] = superseded_by
    completed_body = (
        scope_body.rstrip()
        + "\n\n"
        + _render_completion(payload, finished, elapsed_seconds)
    )
    _raise_record_validation(
        validate_record(completed_metadata, completed_body, paths)
    )
    return _render_frontmatter(completed_metadata) + completed_body


@dataclass
class _HeldRunLock:
    paths: ObservationPaths
    stream: object
    directory_fd: int
    name: str

    def verify(self) -> None:
        _assert_directory_identity(self.directory_fd, self.paths.locks)
        try:
            held = os.fstat(self.stream.fileno())
            entry = os.stat(
                self.name,
                dir_fd=self.directory_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise _validation("observation lock changed during transition") from error
        if (
            not stat.S_ISREG(entry.st_mode)
            or (held.st_dev, held.st_ino) != (entry.st_dev, entry.st_ino)
        ):
            raise _validation("observation lock changed during transition")

    def close(self) -> None:
        first_error: OSError | None = None
        try:
            self.stream.close()
        except OSError as error:
            first_error = error
        try:
            os.close(self.directory_fd)
        except OSError as error:
            if first_error is None:
                first_error = error
        if first_error is not None:
            raise first_error


def _open_run_lock(
    paths: ObservationPaths, run_id: str
) -> tuple[ObservationPaths, _HeldRunLock]:
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise _validation("run_id has an invalid format")
    secure_paths = _canonical_observation_paths(paths)
    try:
        observations_fd = _open_observation_directory(secure_paths.observations)
    except ObservationError as error:
        if error.kind == "io" and not os.path.lexists(secure_paths.observations):
            raise ObservationError(
                "state", f"observation {run_id} does not exist"
            ) from error
        raise
    locks_fd: int | None = None
    lock_descriptor: int | None = None
    lock_stream = None
    try:
        try:
            os.mkdir(".locks", 0o755, dir_fd=observations_fd)
        except FileExistsError:
            pass
        except OSError as error:
            raise ObservationError("io", str(error)) from error
        _assert_directory_identity(observations_fd, secure_paths.observations)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            locks_fd = os.open(
                ".locks", directory_flags, dir_fd=observations_fd
            )
        except OSError as error:
            raise _validation("observation locks path must be a directory") from error
        _assert_directory_identity(locks_fd, secure_paths.locks)

        flags = os.O_RDWR | os.O_CREAT
        flags |= getattr(os, "O_NOFOLLOW", 0)
        lock_name = f"{run_id}.lock"
        for attempt in range(4):
            try:
                lock_descriptor = os.open(
                    lock_name, flags, 0o600, dir_fd=locks_fd
                )
                break
            except FileNotFoundError as error:
                if attempt < 3:
                    _assert_directory_identity(locks_fd, secure_paths.locks)
                    continue
                raise ObservationError("io", str(error)) from error
            except OSError as error:
                try:
                    entry = os.stat(
                        lock_name, dir_fd=locks_fd, follow_symlinks=False
                    )
                except OSError:
                    entry = None
                if entry is not None and stat.S_ISLNK(entry.st_mode):
                    raise _validation("observation lock must not be a symlink") from error
                raise ObservationError("io", str(error)) from error
        if lock_descriptor is None:
            raise ObservationError("io", "could not open observation lock")
        try:
            lock_stream = os.fdopen(lock_descriptor, "a+", encoding="utf-8")
        except OSError as error:
            _close_preserving_error(lock_descriptor)
            raise ObservationError("io", str(error)) from error
        lock_descriptor = None
        try:
            opened_lock = os.fstat(lock_stream.fileno())
            if not stat.S_ISREG(opened_lock.st_mode):
                raise _validation("observation lock must be a regular file")
            os.fchmod(lock_stream.fileno(), 0o600)
        except OSError as error:
            _close_stream_preserving_error(lock_stream)
            lock_stream = None
            raise ObservationError("io", str(error)) from error
        try:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
        except OSError as error:
            _close_stream_preserving_error(lock_stream)
            lock_stream = None
            raise ObservationError("io", str(error)) from error
        held = _HeldRunLock(secure_paths, lock_stream, locks_fd, lock_name)
        held.verify()
        lock_stream = None
        locks_fd = None
        return secure_paths, held
    finally:
        if lock_descriptor is not None:
            _close_preserving_error(lock_descriptor)
        if lock_stream is not None:
            _close_stream_preserving_error(lock_stream)
        if locks_fd is not None:
            _close_preserving_error(locks_fd)
        _close_preserving_error(observations_fd)


def _close_stream_preserving_error(stream) -> None:
    try:
        stream.close()
    except OSError:
        pass


def _read_record_from_directory(
    paths: ObservationPaths, run_id: str, directory_fd: int
) -> tuple[dict[str, object], str, object]:
    name = f"{run_id}.md"
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    for _ in range(4):
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except FileNotFoundError as error:
            raise ObservationError(
                "state", f"observation {run_id} does not exist"
            ) from error
        except OSError as error:
            try:
                entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except OSError:
                entry = None
            if entry is not None and stat.S_ISLNK(entry.st_mode):
                raise _validation("observation record must not be a symlink") from error
            raise ObservationError("io", str(error)) from error
        try:
            stream = os.fdopen(descriptor, "r", encoding="utf-8")
        except OSError as error:
            _close_preserving_error(descriptor)
            raise ObservationError("io", str(error)) from error
        try:
            opened = os.fstat(stream.fileno())
            if not stat.S_ISREG(opened.st_mode):
                raise _validation("observation record must be a regular file")
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            entry = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                _close_stream_preserving_error(stream)
                continue
            content = stream.read()
            metadata, body = _parse_frontmatter(content)
            if metadata.get("run_id") != run_id:
                raise _validation("record run_id does not match filename")
            _raise_record_validation(validate_record(metadata, body, paths))
            return metadata, body, stream
        except ObservationError:
            _close_stream_preserving_error(stream)
            raise
        except (OSError, UnicodeError) as error:
            _close_stream_preserving_error(stream)
            raise ObservationError("io", str(error)) from error
    raise ObservationError("state", f"observation {run_id} changed while locking")


def _allocate_backup(directory_fd: int, record_name: str) -> str:
    for _ in range(32):
        backup_name = f".observation-backup-{secrets.token_urlsafe(12)}.tmp"
        try:
            os.link(
                record_name,
                backup_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            return backup_name
        except FileExistsError:
            continue
        except OSError as error:
            raise ObservationError("io", str(error)) from error
    raise ObservationError("io", "could not allocate an observation backup")


def _replace_completed_record(
    paths: ObservationPaths,
    directory_fd: int,
    run_id: str,
    content: str,
    verify_guard,
) -> None:
    temporary_name: str | None = None
    backup_name: str | None = None
    record_name = f"{run_id}.md"
    replaced = False
    try:
        try:
            temporary_name, temporary = _create_temporary_file(directory_fd)
            with temporary:
                _assert_directory_identity(directory_fd, paths.observations)
                _assert_temporary_in_directory(
                    temporary, temporary_name, directory_fd
                )
                os.fchmod(temporary.fileno(), 0o644)
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
        except (OSError, ObservationError) as error:
            mapped = (
                error
                if isinstance(error, ObservationError)
                else ObservationError("io", str(error))
            )
            _cleanup_before_error(temporary_name, directory_fd, mapped)
            raise mapped

        _assert_directory_identity(directory_fd, paths.observations)
        verify_guard()
        backup_name = _allocate_backup(directory_fd, record_name)
        try:
            os.replace(
                temporary_name,
                record_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            temporary_name = None
            replaced = True
            verify_guard()
            os.fsync(directory_fd)
            _assert_directory_identity(directory_fd, paths.observations)
        except (OSError, ObservationError) as error:
            if replaced and backup_name is not None:
                try:
                    os.replace(
                        backup_name,
                        record_name,
                        src_dir_fd=directory_fd,
                        dst_dir_fd=directory_fd,
                    )
                    backup_name = None
                    os.fsync(directory_fd)
                except OSError as rollback_error:
                    raise ObservationError(
                        "io", f"could not roll back failed finish: {rollback_error}"
                    ) from error
            if isinstance(error, ObservationError):
                raise
            raise ObservationError("io", str(error)) from error

        _cleanup_temporary(backup_name, directory_fd, claimed=True)
        backup_name = None
        try:
            os.fsync(directory_fd)
        except OSError:
            pass
    finally:
        error_in_flight = sys.exc_info()[0] is not None
        cleanup_error: ObservationError | None = None
        if temporary_name is not None:
            try:
                _cleanup_temporary(temporary_name, directory_fd, claimed=False)
            except ObservationError as error:
                cleanup_error = error
        if backup_name is not None:
            try:
                _cleanup_temporary(backup_name, directory_fd, claimed=replaced)
            except ObservationError as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None and not error_in_flight and not replaced:
            raise cleanup_error


def finish_observation(
    paths: ObservationPaths,
    run_id: str,
    status: str,
    payload: CompletionPayload,
    superseded_by: str | None = None,
    now: datetime | None = None,
) -> None:
    """Atomically transition one draft record to exactly one final status."""

    if not isinstance(status, str) or status not in FINAL_STATUSES:
        raise _validation(f"invalid final status `{status}`")
    if status == "superseded":
        if not isinstance(superseded_by, str) or _RUN_ID_RE.fullmatch(superseded_by) is None:
            raise _validation("superseded status requires superseded_by")
    elif superseded_by is not None:
        raise _validation("superseded_by is only valid for superseded status")
    finished = _validated_event_time(now, "finish time")
    secure_paths, run_lock = _open_run_lock(paths, run_id)
    observations_fd: int | None = None
    record_stream = None
    committed = False
    try:
        run_lock.verify()
        observations_fd = _open_observation_directory(secure_paths.observations)
        metadata, scope_body, record_stream = _read_record_from_directory(
            secure_paths, run_id, observations_fd
        )
        if metadata.get("status") != "draft":
            raise ObservationError("state", f"{run_id} is already final")
        content = _render_completed_record(
            metadata,
            scope_body,
            status,
            payload,
            superseded_by,
            finished,
            secure_paths,
        )
        run_lock.verify()
        _replace_completed_record(
            secure_paths,
            observations_fd,
            run_id,
            content,
            run_lock.verify,
        )
        committed = True
    finally:
        error_in_flight = sys.exc_info()[0] is not None
        close_error: OSError | None = None
        if record_stream is not None:
            try:
                record_stream.close()
            except OSError as error:
                close_error = error
        if observations_fd is not None:
            try:
                os.close(observations_fd)
            except OSError as error:
                if close_error is None:
                    close_error = error
        try:
            run_lock.close()
        except OSError as error:
            if close_error is None:
                close_error = error
        if close_error is not None and not committed and not error_in_flight:
            raise ObservationError("io", str(close_error)) from close_error


def _render_invalidation(run_id: str, reason: str, timestamp: datetime) -> str:
    metadata: dict[str, object] = {
        "type": "observation-invalidation",
        "title": f"Invalidate {run_id}",
        "tags": ["observation", "invalidation"],
        "timestamp": timestamp.isoformat(),
        "target_run_id": run_id,
        "reason": reason,
        "sources": [],
    }
    return _render_frontmatter(metadata)


def invalidate_observation(
    paths: ObservationPaths,
    run_id: str,
    reason: str,
    now: datetime | None = None,
) -> None:
    """Atomically create an immutable tombstone without editing the final record."""

    reason = _validate_scalar(reason, "invalidation reason")
    invalidated_at = _validated_event_time(now, "invalidation time")
    secure_paths, run_lock = _open_run_lock(paths, run_id)
    observations_fd: int | None = None
    invalidations_fd: int | None = None
    record_stream = None
    temporary_name: str | None = None
    claimed = False
    committed = False
    try:
        run_lock.verify()
        observations_fd = _open_observation_directory(secure_paths.observations)
        metadata, _, record_stream = _read_record_from_directory(
            secure_paths, run_id, observations_fd
        )
        if metadata.get("status") == "draft":
            raise ObservationError("state", f"{run_id} is still draft")
        try:
            os.mkdir("invalidations", 0o755, dir_fd=observations_fd)
        except FileExistsError:
            pass
        except OSError as error:
            raise ObservationError("io", str(error)) from error
        _assert_directory_identity(
            observations_fd, secure_paths.observations
        )
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            invalidations_fd = os.open(
                "invalidations", directory_flags, dir_fd=observations_fd
            )
        except OSError as error:
            raise _validation(
                "observation invalidations path must be a directory"
            ) from error
        _assert_directory_identity(
            invalidations_fd, secure_paths.invalidations
        )
        destination_name = f"{run_id}.md"
        content = _render_invalidation(run_id, reason, invalidated_at)
        try:
            temporary_name, temporary = _create_temporary_file(
                invalidations_fd
            )
            with temporary:
                _assert_directory_identity(
                    invalidations_fd, secure_paths.invalidations
                )
                _assert_temporary_in_directory(
                    temporary, temporary_name, invalidations_fd
                )
                os.fchmod(temporary.fileno(), 0o644)
                temporary.write(content)
                temporary.flush()
                os.fsync(temporary.fileno())
        except (OSError, ObservationError) as error:
            mapped = (
                error
                if isinstance(error, ObservationError)
                else ObservationError("io", str(error))
            )
            _cleanup_before_error(temporary_name, invalidations_fd, mapped)
            raise mapped

        _assert_directory_identity(
            invalidations_fd, secure_paths.invalidations
        )
        run_lock.verify()
        try:
            os.link(
                temporary_name,
                destination_name,
                src_dir_fd=invalidations_fd,
                dst_dir_fd=invalidations_fd,
                follow_symlinks=False,
            )
        except FileExistsError as error:
            raise ObservationError(
                "state", f"{run_id} is already invalidated"
            ) from error
        except OSError as error:
            raise ObservationError("io", str(error)) from error
        claimed = True
        try:
            run_lock.verify()
            os.fsync(invalidations_fd)
            _assert_directory_identity(
                invalidations_fd, secure_paths.invalidations
            )
        except (OSError, ObservationError) as error:
            _rollback_claim(
                invalidations_fd, destination_name, operation="invalidation"
            )
            claimed = False
            if isinstance(error, ObservationError):
                raise
            raise ObservationError("io", str(error)) from error
        committed = True
    finally:
        error_in_flight = sys.exc_info()[0] is not None
        close_error: OSError | None = None
        cleanup_error: ObservationError | None = None
        if temporary_name is not None and invalidations_fd is not None:
            try:
                _cleanup_temporary(
                    temporary_name, invalidations_fd, claimed=claimed
                )
            except ObservationError as error:
                cleanup_error = error
        if record_stream is not None:
            try:
                record_stream.close()
            except OSError as error:
                close_error = error
        for descriptor in (invalidations_fd, observations_fd):
            if descriptor is None:
                continue
            try:
                os.close(descriptor)
            except OSError as error:
                if close_error is None:
                    close_error = error
        try:
            run_lock.close()
        except OSError as error:
            if close_error is None:
                close_error = error
        if cleanup_error is not None and not committed and not error_in_flight:
            raise cleanup_error
        if close_error is not None and not committed and not error_in_flight:
            raise ObservationError("io", str(close_error)) from close_error


RATE_STATUSES = {"success", "partial", "failed", "rolled-back"}
_REPORT_STATUS_ORDER = (
    "draft",
    "success",
    "partial",
    "failed",
    "rolled-back",
    "superseded",
)
_REPORT_NUMERIC_FIELDS = (
    "elapsed_seconds",
    "defects_found",
    "rework_count",
    "review_rounds",
)


def _open_report_root(path: Path) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        if path.is_symlink():
            raise _validation("wiki root must not be a symlink") from error
        raise ObservationError("io", str(error)) from error
    try:
        try:
            opened = os.fstat(descriptor)
        except OSError as error:
            raise ObservationError("io", f"could not inspect wiki root: {error}") from error
        if not stat.S_ISDIR(opened.st_mode):
            raise _validation("wiki root must be a directory")
        return descriptor
    except Exception:
        _close_preserving_error(descriptor)
        raise


def _report_entry_stat(parent_fd: int, name: str, label: str):
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise ObservationError("io", f"could not inspect {label}: {error}") from error


def _open_report_child_directory(
    parent_fd: int,
    name: str,
    label: str,
    *,
    missing_ok: bool,
    expected=None,
) -> int | None:
    try:
        entry = expected or os.stat(
            name, dir_fd=parent_fd, follow_symlinks=False
        )
    except FileNotFoundError:
        if missing_ok:
            return None
        raise _validation(f"{label} changed during report discovery")
    except OSError as error:
        raise ObservationError("io", f"could not inspect {label}: {error}") from error
    if stat.S_ISLNK(entry.st_mode):
        raise _validation(f"{label} must not be a symlink")
    if not stat.S_ISDIR(entry.st_mode):
        raise _validation(f"{label} must be a directory")

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=parent_fd)
    except OSError as error:
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError:
            current = None
        if current is None or stat.S_ISLNK(current.st_mode):
            raise _validation(f"{label} changed during report discovery") from error
        raise ObservationError("io", f"could not open {label}: {error}") from error
    try:
        try:
            opened = os.fstat(descriptor)
        except OSError as error:
            raise ObservationError("io", f"could not inspect {label}: {error}") from error
        try:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError as error:
            raise _validation(f"{label} changed during report discovery") from error
        except OSError as error:
            raise ObservationError("io", f"could not inspect {label}: {error}") from error
        identities = {
            (entry.st_dev, entry.st_ino),
            (opened.st_dev, opened.st_ino),
            (current.st_dev, current.st_ino),
        }
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(current.st_mode)
            or len(identities) != 1
        ):
            raise _validation(f"{label} changed during report discovery")
        return descriptor
    except Exception:
        _close_preserving_error(descriptor)
        raise


def _report_entry_names(directory_fd: int, label: str) -> list[str]:
    try:
        names = os.listdir(directory_fd)
    except OSError as error:
        raise ObservationError("io", f"could not enumerate {label}: {error}") from error
    return sorted(names, key=lambda name: (name.casefold(), name))


def _read_report_regular_file(
    directory_fd: int, name: str, expected, label: str
) -> str:
    if stat.S_ISLNK(expected.st_mode):
        raise _validation(f"{label} must not be a symlink")
    if not stat.S_ISREG(expected.st_mode):
        raise _validation(f"{label} must be a regular file")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except OSError as error:
        try:
            current = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError:
            current = None
        if current is None or stat.S_ISLNK(current.st_mode):
            raise _validation(f"{label} changed during report discovery") from error
        raise ObservationError("io", f"could not open {label}: {error}") from error
    try:
        try:
            opened = os.fstat(descriptor)
        except OSError as error:
            raise ObservationError("io", f"could not inspect {label}: {error}") from error
        try:
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError as error:
            raise _validation(f"{label} changed during report discovery") from error
        except OSError as error:
            raise ObservationError("io", f"could not inspect {label}: {error}") from error
        identities = {
            (expected.st_dev, expected.st_ino),
            (opened.st_dev, opened.st_ino),
            (current.st_dev, current.st_ino),
        }
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or len(identities) != 1
        ):
            raise _validation(f"{label} changed during report discovery")
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                descriptor = -1
                return stream.read()
        except (OSError, UnicodeError) as error:
            raise ObservationError("io", f"could not read {label}: {error}") from error
    finally:
        if descriptor >= 0:
            _close_preserving_error(descriptor)


def _metrics_from_record(body: str, status: object) -> dict[str, object]:
    if status == "draft":
        return {}
    marker = "\n## Execution evidence"
    if marker not in body:
        raise _validation("final observation is missing completion metrics")
    _, completion_tail = body.split(marker, 1)
    payload, derived = _parse_completion(
        "## Execution evidence" + completion_tail, allow_derived=True
    )
    return {
        **derived,
        "verification": payload.verification,
        "review_rounds": payload.review_rounds,
        "defects_found": payload.defects_found,
        "rework_count": payload.rework_count,
        "rework_reason": payload.rework_reason,
    }


def _read_invalidation_target(content: str, filename: str) -> str:
    metadata, body = _parse_frontmatter(content)
    required = {
        "type",
        "title",
        "tags",
        "timestamp",
        "target_run_id",
        "reason",
        "sources",
    }
    missing = sorted(required - set(metadata))
    unexpected = sorted(set(metadata) - required)
    if missing:
        raise _validation(
            f"invalidation tombstone is missing required field `{missing[0]}`"
        )
    if unexpected:
        raise _validation(
            f"invalidation tombstone has unexpected field `{unexpected[0]}`"
        )
    target = metadata.get("target_run_id")
    if not isinstance(target, str) or _RUN_ID_RE.fullmatch(target) is None:
        raise _validation("invalidation target_run_id has an invalid format")
    if Path(filename).stem != target:
        raise _validation("invalidation target_run_id must match its filename")
    if metadata.get("type") != "observation-invalidation":
        raise _validation("invalidation tombstone has an invalid type")
    if metadata.get("title") != f"Invalidate {target}":
        raise _validation("invalidation tombstone has an invalid title")
    if metadata.get("tags") != ["observation", "invalidation"]:
        raise _validation("invalidation tombstone has invalid tags")
    if metadata.get("sources") != []:
        raise _validation("invalidation tombstone sources must be empty")
    _aware_datetime(metadata.get("timestamp"), "invalidation timestamp")
    _validate_scalar(metadata.get("reason"), "invalidation reason")
    if body:
        raise _validation("invalidation tombstone must not contain a body")
    return target


def collect_records(paths: ObservationPaths) -> tuple[list[dict], set[str]]:
    """Read direct, validated observation records and invalidation targets."""

    secure_paths = _canonical_observation_paths(paths)
    root_fd = _open_report_root(secure_paths.root)
    wiki_fd: int | None = None
    observations_fd: int | None = None
    invalidations_fd: int | None = None
    records: list[dict] = []
    invalidated: set[str] = set()
    try:
        wiki_fd = _open_report_child_directory(
            root_fd, "wiki", "wiki directory", missing_ok=True
        )
        if wiki_fd is None:
            return [], set()
        observations_fd = _open_report_child_directory(
            wiki_fd,
            "observations",
            "observation directory",
            missing_ok=True,
        )
        if observations_fd is None:
            return [], set()

        invalidations_entry = None
        for name in _report_entry_names(observations_fd, "observation records"):
            entry = _report_entry_stat(
                observations_fd, name, f"observation entry {name}"
            )
            if stat.S_ISLNK(entry.st_mode):
                raise _validation("observation storage must not contain symlinks")
            if name == "invalidations":
                if not stat.S_ISDIR(entry.st_mode):
                    raise _validation("invalidations path must be a directory")
                invalidations_entry = entry
                continue
            if name == ".locks" or stat.S_ISDIR(entry.st_mode):
                continue
            if Path(name).suffix != ".md":
                continue
            run_id = Path(name).stem
            if _RUN_ID_RE.fullmatch(run_id) is None:
                raise _validation("observation filename has an invalid run_id")
            content = _read_report_regular_file(
                observations_fd, name, entry, f"observation record {run_id}"
            )
            metadata, body = _parse_frontmatter(content)
            if metadata.get("run_id") != run_id:
                raise _validation("record run_id does not match filename")
            validation_errors = validate_record(metadata, body, secure_paths)
            if validation_errors:
                raise _validation(
                    f"invalid observation {run_id}: {validation_errors[0]}"
                )
            row = dict(metadata)
            row["metrics"] = _metrics_from_record(body, metadata.get("status"))
            records.append(row)

        records.sort(
            key=lambda row: (
                str(row.get("timestamp", "")),
                str(row.get("run_id", "")),
            )
        )
        by_id = {row["run_id"]: row for row in records}
        if invalidations_entry is None:
            return records, invalidated
        invalidations_fd = _open_report_child_directory(
            observations_fd,
            "invalidations",
            "invalidations directory",
            missing_ok=False,
            expected=invalidations_entry,
        )
        for name in _report_entry_names(invalidations_fd, "invalidations"):
            entry = _report_entry_stat(
                invalidations_fd, name, f"invalidation entry {name}"
            )
            if stat.S_ISLNK(entry.st_mode):
                raise _validation("invalidations storage must not contain symlinks")
            if stat.S_ISDIR(entry.st_mode) or Path(name).suffix != ".md":
                continue
            content = _read_report_regular_file(
                invalidations_fd,
                name,
                entry,
                f"invalidation tombstone {Path(name).stem}",
            )
            target = _read_invalidation_target(content, name)
            target_record = by_id.get(target)
            if target_record is None:
                raise _validation("invalidation points to no observation record")
            if target_record.get("status") == "draft":
                raise _validation("invalidation must reference a final observation")
            invalidated.add(target)
        return records, invalidated
    finally:
        for descriptor in (invalidations_fd, observations_fd, wiki_fd, root_fd):
            if descriptor is not None:
                _close_preserving_error(descriptor)


def _validate_report_filters(filters: ReportFilters) -> None:
    if not isinstance(filters, ReportFilters):
        raise _validation("report filters have the wrong type")
    for field in ("project", "workspace"):
        value = getattr(filters, field)
        if value is not None and (not isinstance(value, str) or not value):
            raise _validation(f"report {field} filter must be a nonempty string")
    if filters.workspace_id is not None and (
        not isinstance(filters.workspace_id, str)
        or _WORKSPACE_ID_RE.fullmatch(filters.workspace_id) is None
    ):
        raise _validation("report workspace_id filter must be 12 lowercase hex")
    if filters.task_type is not None and (
        not isinstance(filters.task_type, str)
        or filters.task_type not in TAXONOMY
    ):
        raise _validation("report task_type filter is invalid")
    if filters.status is not None and (
        not isinstance(filters.status, str)
        or filters.status not in FINAL_STATUSES | {"draft"}
    ):
        raise _validation("report status filter is invalid")
    for field in ("since", "until"):
        value = getattr(filters, field)
        if value is not None and (
            not isinstance(value, date) or isinstance(value, datetime)
        ):
            raise _validation(f"report {field} filter must be a date")
    if filters.since is not None and filters.until is not None:
        if filters.since > filters.until:
            raise _validation("report since date must not be after until date")


def _matches_report_filters(
    row: dict, filters: ReportFilters, local_timezone
) -> bool:
    for field in ("project", "workspace", "workspace_id", "task_type", "status"):
        expected = getattr(filters, field)
        if expected is not None and row.get(field) != expected:
            return False
    started = _aware_datetime(row.get("timestamp"), "timestamp")
    local_date = started.astimezone(local_timezone).date()
    if filters.since is not None and local_date < filters.since:
        return False
    if filters.until is not None and local_date > filters.until:
        return False
    return True


def _numeric_values(
    group: list[dict], field: str, invalidated: set[str]
) -> tuple[list[int], int]:
    values: list[int] = []
    missing = 0
    for row in group:
        if row.get("run_id") in invalidated or row.get("status") not in RATE_STATUSES:
            continue
        value = row.get("metrics", {}).get(field, "unknown")
        if isinstance(value, bool):
            missing += 1
        elif isinstance(value, int) and value >= 0:
            values.append(value)
        elif (
            isinstance(value, str)
            and len(value) <= _MAX_INTEGER_DIGITS
            and re.fullmatch(r"[0-9]+", value)
        ):
            values.append(int(value))
        else:
            missing += 1
    return values, missing


def _format_average(values: list[int]) -> str:
    if not values:
        return "unknown"
    average = sum(values) / len(values)
    if average.is_integer():
        return str(int(average))
    return f"{average:.1f}"


def _format_total(values: list[int]) -> str:
    return str(sum(values)) if values else "unknown"


def _report_group_key(row: dict) -> tuple[str, str, str, str]:
    return (
        str(row.get("project", "")),
        str(row.get("workspace_id", "")),
        str(row.get("task_type", "")),
        str(row.get("workflow_variant", "")),
    )


def render_observation_report(
    records: list[dict],
    invalidated: set[str],
    filters: ReportFilters,
    now: datetime,
) -> str:
    """Render deterministic descriptive aggregates without workflow advice."""

    _validate_report_filters(filters)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise _validation("report time must be an aware datetime")
    if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
        raise _validation("report records must be a list of objects")
    if not isinstance(invalidated, set) or not all(
        isinstance(run_id, str) for run_id in invalidated
    ):
        raise _validation("invalidated run IDs must be a set of strings")

    filtered = [
        row
        for row in records
        if _matches_report_filters(row, filters, now.tzinfo)
    ]
    filtered_invalidated = {
        row.get("run_id")
        for row in filtered
        if row.get("run_id") in invalidated
    }
    aggregate_rows = [
        row for row in filtered if row.get("run_id") not in invalidated
    ]
    lines = [
        "# Observation report",
        "",
        f"Generated at: {now.isoformat()}",
        f"Samples: {len(aggregate_rows)}",
        f"Invalidated: {len(filtered_invalidated)}",
    ]
    if not filtered:
        lines.extend(["", "No observation records matched the filters."])
        return "\n".join(lines) + "\n"

    groups: dict[tuple[str, str, str, str], list[dict]] = {}
    for row in filtered:
        groups.setdefault(_report_group_key(row), []).append(row)

    for key in sorted(
        groups,
        key=lambda item: tuple((part.casefold(), part) for part in item),
    ):
        project, workspace_id, task_type, workflow_variant = key
        group = sorted(
            groups[key],
            key=lambda row: (str(row.get("timestamp", "")), str(row.get("run_id", ""))),
        )
        group_invalidated = {
            row.get("run_id")
            for row in group
            if row.get("run_id") in invalidated
        }
        aggregate_group = [
            row for row in group if row.get("run_id") not in invalidated
        ]
        status_counts = {
            status: sum(row.get("status") == status for row in aggregate_group)
            for status in _REPORT_STATUS_ORDER
        }
        eligible = [
            row
            for row in aggregate_group
            if row.get("status") in RATE_STATUSES
        ]
        successes = sum(row.get("status") == "success" for row in eligible)
        final_sample = [
            row
            for row in aggregate_group
            if row.get("status") in FINAL_STATUSES
        ]
        numeric = {
            field: _numeric_values(group, field, invalidated)
            for field in _REPORT_NUMERIC_FIELDS
        }
        metric_denominator = len(eligible) * len(_REPORT_NUMERIC_FIELDS)
        missing_total = sum(missing for _, missing in numeric.values())
        missing_rate = (
            f"{100 * missing_total / metric_denominator:.1f}%"
            if metric_denominator
            else "0.0%"
        )
        drafts = [
            row for row in aggregate_group if row.get("status") == "draft"
        ]
        draft_ages = []
        stale_count = 0
        for row in drafts:
            started = _aware_datetime(row.get("timestamp"), "timestamp")
            age = now - started
            age_seconds = max(age.total_seconds(), 0.0)
            stale = age > timedelta(hours=24)
            stale_count += int(stale)
            label = f"{row.get('run_id')}={age_seconds / 3600:.1f}h"
            if stale:
                label += " (stale)"
            draft_ages.append(label)

        workspace_names = sorted(
            {str(row.get("workspace", "")) for row in group},
            key=lambda value: (value.casefold(), value),
        )
        elapsed_values, missing_elapsed = numeric["elapsed_seconds"]
        defect_values, missing_defects = numeric["defects_found"]
        rework_values, missing_rework = numeric["rework_count"]
        review_values, missing_reviews = numeric["review_rounds"]
        rate = (
            f"{successes}/{len(eligible)} ({100 * successes / len(eligible):.1f}%)"
            if eligible
            else "0/0"
        )
        counts = ", ".join(
            f"{status}={status_counts[status]}"
            for status in _REPORT_STATUS_ORDER
            if status_counts[status]
        ) or "none"
        lines.extend(
            [
                "",
                (
                    f"## Project: {project} | Workspace ID: {workspace_id} | "
                    f"Task type: {task_type} | Workflow variant: {workflow_variant}"
                ),
                "",
                f"Workspace: {', '.join(workspace_names)}",
                f"Samples: {len(aggregate_group)}",
                f"Status counts: {counts}",
                f"Invalidated: {len(group_invalidated)}",
                f"Success rate: {rate}",
                f"Average elapsed seconds: {_format_average(elapsed_values)}",
                f"Missing elapsed seconds: {missing_elapsed}",
                f"Total defects found: {_format_total(defect_values)}",
                f"Average defects found: {_format_average(defect_values)}",
                f"Missing defects found: {missing_defects}",
                f"Total rework count: {_format_total(rework_values)}",
                f"Average rework count: {_format_average(rework_values)}",
                f"Missing rework count: {missing_rework}",
                f"Average review rounds: {_format_average(review_values)}",
                f"Missing review rounds: {missing_reviews}",
                (
                    f"Missing metric values: {missing_total}/{metric_denominator} "
                    f"({missing_rate})"
                ),
                f"Drafts: {len(drafts)}",
                f"Stale drafts (>24h): {stale_count}",
                f"Draft ages: {', '.join(draft_ages) if draft_ages else 'none'}",
                f"small sample (n={len(final_sample)})"
                if len(final_sample) < 5
                else f"final sample (n={len(final_sample)})",
            ]
        )
    return "\n".join(lines) + "\n"
