"""Deterministic, production-safe fixtures for observing-workflows evaluations."""

from __future__ import annotations

import json
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import stat
import tempfile
import time


FIXTURE_KINDS = {"python-cli", "documentation", "wiki", "empty"}
_GATE_ROOTS: dict[str, Path] = {}
PRODUCTION_CHANGED_PATH_LIMIT = 8
PRODUCTION_PATH_PREFIX_LIMIT = 120
# Production mismatch exceptions are always kept strictly below this character cap.
PRODUCTION_MISMATCH_MESSAGE_LIMIT = 2048


PAYLOAD_AUDIT_WRAPPER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import stat
import sys

target = os.environ["OBSERVATION_AUDIT_TARGET_CLI"]
log_path = Path(os.environ["OBSERVATION_AUDIT_LOG"])
if "--help" in sys.argv or "-h" in sys.argv:
    os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
for flag in ("--scope-from-file", "--from-file"):
    if flag not in sys.argv:
        continue
    value = sys.argv[sys.argv.index(flag) + 1]
    payload = Path(value)
    details = os.stat(payload, follow_symlinks=False)
    row = {
        "flag": flag,
        "path": str(payload),
        "device": details.st_dev,
        "inode": details.st_ino,
        "mode": stat.S_IMODE(details.st_mode),
        "regular": stat.S_ISREG(details.st_mode),
    }
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, sort_keys=True) + "\n")
os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
'''


@dataclass(frozen=True)
class PayloadAudit:
    root: Path
    payload_dir: Path
    log_path: Path
    wrapper_path: Path
    target_cli: Path


def build_payload_audit(case_id: str, destination: Path, target_cli: Path) -> PayloadAudit:
    if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", case_id):
        raise ValueError("case id must be lowercase letters, digits, and hyphens")
    destination = Path(destination).resolve(strict=True)
    unresolved_target = Path(target_cli)
    if unresolved_target.is_symlink():
        raise ValueError("target CLI must be a regular non-symlink file")
    target_cli = unresolved_target.resolve(strict=True)
    if not target_cli.is_file():
        raise ValueError("target CLI must be a regular non-symlink file")
    root = destination / f"{case_id}-payload-audit"
    if root.exists():
        raise FileExistsError(root)
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    if not root.resolve().is_relative_to(destination):
        raise ValueError("payload audit path escapes destination")
    payload_dir = root / "tmp"
    payload_dir.mkdir(mode=0o700)
    payload_dir.chmod(0o700)
    wrapper_path = root / "wiki_cli_audit.py"
    wrapper_path.write_text(PAYLOAD_AUDIT_WRAPPER, encoding="utf-8")
    wrapper_path.chmod(0o700)
    return PayloadAudit(
        root=root,
        payload_dir=payload_dir,
        log_path=root / "payload-audit.jsonl",
        wrapper_path=wrapper_path,
        target_cli=target_cli,
    )


def payload_audit_environment(audit: PayloadAudit) -> dict[str, str]:
    return {
        "OBSERVATION_PAYLOAD_TMPDIR": str(audit.payload_dir),
        "OBSERVATION_AUDIT_LOG": str(audit.log_path),
        "OBSERVATION_AUDIT_TARGET_CLI": str(audit.target_cli),
    }


def assert_payload_audit(
    audit: PayloadAudit,
    expected_scope_calls: int,
    expected_completion_calls: int,
) -> None:
    rows = []
    errors = []
    if audit.log_path.exists():
        for line_number, line in enumerate(
            audit.log_path.read_text(encoding="utf-8").splitlines(), 1
        ):
            try:
                rows.append((line_number, json.loads(line)))
            except json.JSONDecodeError:
                errors.append(f"audit log line {line_number} is invalid JSON")
    expected_fields = {"flag", "path", "device", "inode", "mode", "regular"}
    scope_count = sum(
        isinstance(row, dict) and row.get("flag") == "--scope-from-file"
        for _, row in rows
    )
    completion_count = sum(
        isinstance(row, dict) and row.get("flag") == "--from-file"
        for _, row in rows
    )
    if scope_count != expected_scope_calls:
        errors.append(f"scope calls: expected {expected_scope_calls}, got {scope_count}")
    if completion_count != expected_completion_calls:
        errors.append(
            f"completion calls: expected {expected_completion_calls}, got {completion_count}"
        )
    expected_total = expected_scope_calls + expected_completion_calls
    if len(rows) != expected_total:
        errors.append(f"total calls: expected {expected_total}, got {len(rows)}")
    seen_paths = set()
    seen_inodes = set()
    for index, row in rows:
        if not isinstance(row, dict) or set(row) != expected_fields:
            errors.append(f"audit row {index} has invalid fields")
            continue
        if (
            row["flag"] not in {"--scope-from-file", "--from-file"}
            or not isinstance(row["path"], str)
            or not isinstance(row["device"], int)
            or isinstance(row["device"], bool)
            or not isinstance(row["inode"], int)
            or isinstance(row["inode"], bool)
            or not isinstance(row["mode"], int)
            or isinstance(row["mode"], bool)
            or not isinstance(row["regular"], bool)
        ):
            errors.append(f"audit row {index} has invalid values")
            continue
        path = row["path"]
        identity = (row["device"], row["inode"])
        if path in seen_paths:
            errors.append(f"audit row {index} reuses a payload path")
        if identity in seen_inodes:
            errors.append(f"audit row {index} reuses a payload inode")
        seen_paths.add(path)
        seen_inodes.add(identity)
        payload_path = Path(path)
        if (
            not payload_path.is_absolute()
            or not payload_path.resolve(strict=False).is_relative_to(
                audit.payload_dir.resolve(strict=True)
            )
        ):
            errors.append(f"audit row {index} payload is outside the case payload directory")
        if row["regular"] is not True or row["mode"] != 0o600:
            errors.append(f"audit row {index} is not a regular mode-0600 payload")
        if os.path.lexists(path):
            errors.append(f"audit row {index} payload still exists")
    leftovers = sorted(path.name for path in audit.payload_dir.iterdir())
    if leftovers:
        errors.append("payload directory is not empty: " + ", ".join(leftovers))
    if errors:
        raise AssertionError("; ".join(errors))


GATE_SCRIPT = '''#!/usr/bin/env python3
from pathlib import Path
import sys
import time

case_id = sys.argv[1]
gate_dir = Path(".eval-gates")
gate_dir.mkdir(exist_ok=True)
(gate_dir / f"{case_id}.ready").write_text("ready\\n", encoding="utf-8")
deadline = time.monotonic() + 15
release = gate_dir / f"{case_id}.release"
while not release.exists():
    if time.monotonic() >= deadline:
        raise SystemExit(2)
    time.sleep(0.01)
'''


FAIL_TASK_SCRIPT = '''#!/usr/bin/env python3
import sys

print("deterministic task failure", file=sys.stderr)
raise SystemExit(3)
'''


WIKI_INBOX_DASHBOARD = '''# Inbox

- [[inbox/cli-capture]]
- [[inbox/parser-capture]]
'''


WIKI_CLI_SCRIPT = '''#!/usr/bin/env python3
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
INBOX = ROOT / "wiki" / "inbox"
DASHBOARD = ROOT / "wiki" / "_inbox.md"


def field(path, key):
    prefix = f"{key}:"
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip()
    return None


def pending_captures():
    return [
        path for path in sorted(INBOX.glob("*.md"))
        if field(path, "status") == "pending"
    ]


def render_dashboard(paths):
    links = [f"- [[inbox/{path.stem}]]" for path in paths]
    return "# Inbox\\n\\n" + "\\n".join(links) + ("\\n" if links else "")


def run_inbox(check):
    paths = pending_captures()
    rendered = render_dashboard(paths)
    if check:
        if not DASHBOARD.is_file() or DASHBOARD.read_text(encoding="utf-8") != rendered:
            print("inbox dashboard is stale", file=sys.stderr)
            return 1
    else:
        DASHBOARD.write_text(rendered, encoding="utf-8")
    print(f"{len(paths)} pending capture(s)")
    return 0


def run_lint():
    required = [
        ROOT / "raw" / "source.md",
        ROOT / "wiki" / "_index.md",
        ROOT / "wiki" / "_overview.md",
        DASHBOARD,
    ]
    missing = [path.relative_to(ROOT).as_posix() for path in required if not path.is_file()]
    if missing:
        print("missing: " + ", ".join(missing), file=sys.stderr)
        return 1
    for folder in (ROOT / "wiki" / "concept", INBOX):
        for path in folder.glob("*.md"):
            text = path.read_text(encoding="utf-8")
            for key in ("type:", "title:", "tags:", "timestamp:", "sources:"):
                if not text.startswith("---\\n") or key not in text:
                    print(f"{path.relative_to(ROOT)}: invalid frontmatter", file=sys.stderr)
                    return 1
    if DASHBOARD.read_text(encoding="utf-8") != render_dashboard(pending_captures()):
        print("wiki/_inbox.md: stale dashboard", file=sys.stderr)
        return 1
    print("lint ok")
    return 0


arguments = sys.argv[1:]
if arguments and arguments[0] == "lint" and len(arguments) == 1:
    raise SystemExit(run_lint())
if arguments and arguments[0] == "inbox" and arguments[1:] in ([], ["--check"]):
    raise SystemExit(run_inbox(arguments[1:] == ["--check"]))
print("usage: wiki_cli.py lint | inbox [--check]", file=sys.stderr)
raise SystemExit(2)
'''


FIXTURE_FILES = {
    "python-cli": {
        "src/parser.py": "def parse(value):\n    return value.strip()\n",
        "src/cli.py": "from .parser import parse\n\ndef main(value):\n    return parse(value)\n",
        "tests/test_cli.py": "import unittest\n\nclass CliTests(unittest.TestCase):\n    def test_placeholder(self):\n        self.assertTrue(True)\n",
        "scripts/fail_task.py": FAIL_TASK_SCRIPT,
        "README.md": "# Example Python CLI\n",
    },
    "documentation": {
        "docs/index.md": "# Documentation\n\nSee [guide](guide.md) and [reference](reference.md).\n",
        "docs/guide.md": "# Guide\n\nRun `example --output text`. See [reference](reference.md).\n",
        "docs/usage.md": "# Usage\n\nSee [reference](reference.md).\n",
        "docs/reference.md": "# Reference\n\nRun `example --output text`. See [guide](guide.md).\n",
        "README.md": "# Documentatoin fixture\n\nSee the [guide](docs/guide.md).\n",
    },
    "wiki": {
        "raw/source.md": "# Raw source\n\nThe parser normalizes input. The CLI presents parsed output.\n",
        "wiki/concept/Existing.md": "---\ntype: concept\ntitle: Existing\ntags: [fixture]\ntimestamp: 2026-01-01\nsources: []\n---\n\n# Existing\n",
        "wiki/inbox/parser-capture.md": "---\ntype: inbox\ntitle: Parser capture\ntags: [fixture, parser]\ntimestamp: 2026-01-01\nsources: []\nstatus: pending\n---\n\nParser capture awaiting compilation.\n",
        "wiki/inbox/cli-capture.md": "---\ntype: inbox\ntitle: CLI capture\ntags: [fixture, cli]\ntimestamp: 2026-01-01\nsources: []\nstatus: pending\n---\n\nCLI capture awaiting compilation.\n",
        "wiki/_index.md": "# Wiki Index\n\n- [[Existing]]\n",
        "wiki/_overview.md": "# Overview\n\n- [[Existing]]\n",
        "wiki/_inbox.md": WIKI_INBOX_DASHBOARD,
        "wiki_cli.py": WIKI_CLI_SCRIPT,
        "README.md": "# Wiki fixture\n",
    },
    "empty": {
        "README.md": "# Empty fixture\n",
    },
}


def _run_git(root: Path, *arguments: str) -> str:
    environment = os.environ.copy()
    environment.update({
        "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
        "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
    })
    result = subprocess.run(
        ["git", *arguments], cwd=root, check=True, text=True,
        capture_output=True, env=environment,
    )
    return result.stdout.strip()


def _prune_gate_roots():
    for case_id, root in tuple(_GATE_ROOTS.items()):
        if not root.exists():
            del _GATE_ROOTS[case_id]


def build_fixture(
    case_id: str,
    fixture: str,
    destination: Path,
    *,
    include_gate: bool = True,
    include_failure_script: bool = True,
) -> Path:
    """Create one isolated Git-backed fixture strictly below destination."""
    if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", case_id):
        raise ValueError("case id must be lowercase letters, digits, and hyphens")
    if fixture not in FIXTURE_KINDS:
        raise ValueError(f"unknown fixture kind: {fixture}")
    _prune_gate_roots()
    if include_gate and case_id in _GATE_ROOTS:
        raise ValueError(f"active gate case already registered: {case_id}")
    destination = Path(destination).resolve(strict=True)
    root = destination / case_id
    if root.exists():
        raise FileExistsError(root)
    root.mkdir()
    if not root.resolve().is_relative_to(destination):
        raise ValueError("fixture path escapes destination")

    files = dict(FIXTURE_FILES[fixture])
    if not include_failure_script:
        files.pop("scripts/fail_task.py", None)
    if include_gate:
        files["scripts/gate.py"] = GATE_SCRIPT
    files[".gitignore"] = ".eval-gates/\n__pycache__/\n"
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    _run_git(root, "init", "-q")
    _run_git(root, "config", "user.name", "Observation Eval")
    _run_git(root, "config", "user.email", "observation-eval@example.invalid")
    _run_git(root, "add", ".")
    _run_git(root, "commit", "-q", "-m", "fixture baseline")
    if include_gate:
        _GATE_ROOTS[case_id] = root
    return root


def _parse_frontmatter(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    metadata = {}
    for line in parts[1].splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip().strip('"').strip("'") or None
    return metadata


def inspect_store(wiki_root: Path) -> dict:
    observations = Path(wiki_root) / "wiki" / "observations"
    records = []
    if observations.is_dir():
        for path in sorted(observations.glob("*.md")):
            metadata = _parse_frontmatter(path)
            if not metadata:
                continue
            records.append({
                "run_id": metadata.get("run_id") or path.stem,
                "timestamp": metadata.get("timestamp") or "",
                "status": metadata.get("status") or "",
                "start_mode": metadata.get("start_mode") or "",
                "superseded_by": metadata.get("superseded_by"),
            })
    statuses = [record["status"] for record in records]
    return {
        "records": sorted(records, key=lambda row: (row["timestamp"], row["run_id"])),
        "run_count": len(records),
        "draft_count": statuses.count("draft"),
        "final_statuses": sorted(status for status in statuses if status and status != "draft"),
    }


def normalize_records(records, role_map):
    """Replace raw IDs with stable per-case roles across ordered checkpoints."""
    unseen = sorted(
        (row for row in records if row["run_id"] not in role_map),
        key=lambda row: (row.get("timestamp", ""), row["run_id"]),
    )
    for row in unseen:
        role_map[row["run_id"]] = f"run-{len(role_map) + 1}"
    normalized = []
    for row in records:
        superseded_by = row.get("superseded_by")
        normalized.append({
            "role": role_map[row["run_id"]],
            "status": row.get("status"),
            "start_mode": row.get("start_mode"),
            "superseded_by_role": role_map.get(superseded_by) if superseded_by else None,
        })
    return sorted(normalized, key=lambda row: int(row["role"].split("-", 1)[1]))


def wait_for_checkpoint(case_id, predicate, timeout_seconds=15):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(min(0.01, max(timeout_seconds / 10, 0.001)))
    raise TimeoutError(f"checkpoint timed out for {case_id}")


def release_gate(case_id):
    _prune_gate_roots()
    try:
        root = _GATE_ROOTS[case_id]
    except KeyError as error:
        raise ValueError(f"unknown gate case: {case_id}") from error
    gate_dir = root / ".eval-gates"
    gate_dir.mkdir(exist_ok=True)
    (gate_dir / f"{case_id}.release").write_text("release\n", encoding="utf-8")
    del _GATE_ROOTS[case_id]


def after_draft_run(wiki_root: Path) -> bool:
    return inspect_store(wiki_root)["draft_count"] > 0


def after_single_file_mutation_without_run(workspace: Path, wiki_root: Path) -> bool:
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=workspace, check=True,
        text=True, capture_output=True,
    ).stdout.splitlines()
    changed_paths = [line for line in status if line.strip()]
    return len(changed_paths) == 1 and inspect_store(wiki_root)["run_count"] == 0


def _fingerprint_path(root: Path, relative: str) -> tuple:
    path = root / relative
    if not os.path.lexists(path):
        return relative, "missing"
    details = path.lstat()
    mode = stat.S_IMODE(details.st_mode)
    if path.is_symlink():
        target = os.readlink(path)
        digest = hashlib.sha256(target.encode("utf-8", "surrogateescape")).hexdigest()
        return relative, "symlink", mode, digest
    if path.is_file():
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return relative, "file", mode, details.st_size, digest.hexdigest()
    return relative, "other", mode


def _production_state(repo_root: Path) -> tuple[str, tuple[tuple, ...]]:
    status = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=repo_root,
        check=True,
        text=True, capture_output=True,
    ).stdout
    listed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    ).stdout.decode("utf-8", "surrogateescape").split("\0")
    relative_paths = {path for path in listed if path}
    observations = repo_root / "wiki" / "observations"
    if observations.is_dir():
        relative_paths.update(
            path.relative_to(repo_root).as_posix()
            for path in observations.rglob("*")
            if path.is_file() or path.is_symlink()
        )
    fingerprints = tuple(
        _fingerprint_path(repo_root, relative)
        for relative in sorted(relative_paths)
    )
    return status, fingerprints


def snapshot_production(repo_root: Path):
    root = Path(repo_root).resolve(strict=True)
    status, fingerprints = _production_state(root)
    return {
        "repo_root": root,
        "git_status": status,
        "fingerprints": fingerprints,
    }


def _status_sha256(status: str) -> str:
    return hashlib.sha256(
        status.encode("utf-8", "surrogateescape")
    ).hexdigest()


def _bounded_relative_path(relative: str) -> str:
    rendered = "".join(
        character if character.isprintable() else "?"
        for character in relative
    )
    if rendered == relative and len(rendered) <= PRODUCTION_PATH_PREFIX_LIMIT:
        return rendered
    digest = hashlib.sha256(
        relative.encode("utf-8", "surrogateescape")
    ).hexdigest()[:12]
    return rendered[:PRODUCTION_PATH_PREFIX_LIMIT] + f"…#{digest}"


def _production_path_changes(before_fingerprints, after_fingerprints):
    before = {fingerprint[0]: fingerprint for fingerprint in before_fingerprints}
    after = {fingerprint[0]: fingerprint for fingerprint in after_fingerprints}
    changes = []
    for relative in sorted(before.keys() | after.keys()):
        previous = before.get(relative)
        current = after.get(relative)
        if previous == current:
            continue
        previous_missing = previous is None or previous[1] == "missing"
        current_missing = current is None or current[1] == "missing"
        if previous_missing and not current_missing:
            category = "added"
        elif not previous_missing and current_missing:
            category = "removed"
        else:
            category = "modified"
        changes.append((relative, category))
    return changes


def _production_mismatch_message(snapshot, status, fingerprints) -> str:
    changes = _production_path_changes(snapshot["fingerprints"], fingerprints)
    counts = {
        category: sum(change_category == category for _, change_category in changes)
        for category in ("added", "removed", "modified")
    }
    summary = {
        "status_changed": status != snapshot["git_status"],
        "status_before_sha256": _status_sha256(snapshot["git_status"]),
        "status_after_sha256": _status_sha256(status),
        "added_path_count": counts["added"],
        "removed_path_count": counts["removed"],
        "modified_path_count": counts["modified"],
        "changed_paths": [
            {"category": category, "path": _bounded_relative_path(relative)}
            for relative, category in changes[:PRODUCTION_CHANGED_PATH_LIMIT]
        ],
    }
    prefix = "production repository changed during evaluation: "
    message = prefix + json.dumps(
        summary, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    while (
        len(message) >= PRODUCTION_MISMATCH_MESSAGE_LIMIT
        and summary["changed_paths"]
    ):
        summary["changed_paths"].pop()
        message = prefix + json.dumps(
            summary, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
    if len(message) >= PRODUCTION_MISMATCH_MESSAGE_LIMIT:
        raise RuntimeError("production mismatch summary exceeded diagnostic cap")
    return message


def assert_production_unchanged(snapshot):
    status, fingerprints = _production_state(snapshot["repo_root"])
    if status != snapshot["git_status"] or fingerprints != snapshot["fingerprints"]:
        raise AssertionError(_production_mismatch_message(snapshot, status, fingerprints))


def persist_results_atomically(destination: Path, rows: list[dict]):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=destination.parent,
            prefix=f".{destination.name}.", suffix=".tmp", delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(rows, temporary, ensure_ascii=False, indent=2)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
