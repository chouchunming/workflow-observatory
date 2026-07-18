#!/usr/bin/env python3
import os
import sys
import re
import argparse
import secrets
import stat
from datetime import date, datetime
from pathlib import Path

import wiki_observations
from wiki_observations import (
    ObservationError as ObservationDomainError,
    ObservationPaths,
    read_record as read_observation_record,
    validate_record as validate_observation_domain_record,
)

SYSTEM_PAGES = {"_index", "_overview", "_sources", "_source_triage", "_queries", "_maintenance", "_todo_list", "_inbox", "z_log"}
TEXT_EXTENSIONS = {".md", ".txt", ".html", ".htm", ".eml", ".csv", ".json", ".yaml", ".yml"}
TRIAGE_STATUSES = {
    "untriaged",
    "compile",
    "noise-ignore",
    "binary-extract-later",
    "pii-sensitive",
    "duplicate",
    "reference-only",
}
TASK_STATUSES = ("pending", "waiting", "blocked", "done")
TASKS_DIR = os.path.join("wiki", "tasks")
INBOX_DIR = os.path.join("wiki", "inbox")
INBOX_STATUSES = ("new", "compiled", "ignored")
OBSERVATIONS_DIR = os.path.join("wiki", "observations")


def is_observation_operational_path(path):
    """Return whether a repository-relative path is operational observation data."""
    candidate = Path(path)
    if candidate.is_absolute():
        try:
            candidate = candidate.resolve(strict=False).relative_to(Path.cwd().resolve())
        except (OSError, ValueError):
            return False
    normalized = candidate.as_posix().rstrip("/")
    return normalized == "wiki/observations" or normalized.startswith(
        "wiki/observations/"
    )


class ObservationArgumentParser(argparse.ArgumentParser):
    """Argparse adapter that preserves the observation error contract."""

    def error(self, message):
        raise ObservationDomainError("validation", message)


OBSERVATION_ERROR_CODES = {"validation": 2, "state": 3, "io": 4}


def fail_observation(error):
    prefix = {
        "validation": "observation validation error:",
        "state": "observation state error:",
        "io": "observation io error:",
    }[error.kind]
    print(f"{prefix} {error}", file=sys.stderr)
    return OBSERVATION_ERROR_CODES[error.kind]


def _read_observation_payload(path_value, label):
    if not isinstance(path_value, str) or not path_value:
        raise ObservationDomainError("validation", f"{label} path is required")
    path = Path(path_value)
    try:
        expected = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise ObservationDomainError(
            "io", f"could not inspect {label}: {error}"
        ) from error
    if stat.S_ISLNK(expected.st_mode):
        raise ObservationDomainError(
            "validation", f"{label} must not be a symlink"
        )
    if not stat.S_ISREG(expected.st_mode):
        raise ObservationDomainError(
            "validation", f"{label} must be a regular file"
        )
    if stat.S_IMODE(expected.st_mode) != 0o600:
        raise ObservationDomainError(
            "validation", f"{label} must have mode 0600"
        )

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        try:
            current = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as missing_error:
            raise ObservationDomainError(
                "validation", f"{label} changed while opening"
            ) from missing_error
        except OSError as inspect_error:
            raise ObservationDomainError(
                "io", f"could not inspect {label}: {inspect_error}"
            ) from inspect_error
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or (current.st_dev, current.st_ino)
            != (expected.st_dev, expected.st_ino)
        ):
            raise ObservationDomainError(
                "validation", f"{label} changed while opening"
            ) from error
        raise ObservationDomainError("io", f"could not open {label}: {error}") from error
    try:
        try:
            opened = os.fstat(descriptor)
            current = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as error:
            raise ObservationDomainError(
                "validation", f"{label} changed while opening"
            ) from error
        except OSError as error:
            raise ObservationDomainError(
                "io", f"could not inspect opened {label}: {error}"
            ) from error
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
            raise ObservationDomainError(
                "validation", f"{label} changed while opening"
            )
        if stat.S_IMODE(opened.st_mode) != 0o600:
            raise ObservationDomainError(
                "validation", f"{label} must have mode 0600"
            )
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                descriptor = -1
                return stream.read()
        except UnicodeError as error:
            raise ObservationDomainError(
                "validation", f"{label} must be UTF-8 text"
            ) from error
        except OSError as error:
            raise ObservationDomainError("io", f"could not read {label}: {error}") from error
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _observation_date(value):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ObservationDomainError(
            "validation", "report dates must use YYYY-MM-DD"
        ) from error


def build_observation_parser():
    parser = ObservationArgumentParser(
        prog=f"{Path(sys.argv[0]).name} observe",
        description="Record and report validated workflow observations",
    )
    parser.add_argument("--wiki-root", required=True, help="Central LLM Wiki root")
    subparsers = parser.add_subparsers(
        dest="observe_command",
        required=True,
        parser_class=ObservationArgumentParser,
    )

    start = subparsers.add_parser("start", help="Create a validated draft")
    start.add_argument("--title", required=True)
    start.add_argument("--subject-root", required=True)
    start.add_argument("--project")
    start.add_argument("--agent-surface", required=True, choices=("codex",))
    start.add_argument("--start-mode", required=True, choices=("planned", "late"))
    start.add_argument(
        "--task-type", required=True, choices=sorted(wiki_observations.TAXONOMY)
    )
    start.add_argument(
        "--workflow-variant",
        required=True,
        choices=sorted(
            {
                variant
                for variants in wiki_observations.TAXONOMY.values()
                for variant in variants
            }
        ),
    )
    start.add_argument("--scope-from-file", required=True)
    start.add_argument("--task")
    start.add_argument("--source", action="append", default=[])

    finish = subparsers.add_parser("finish", help="Finish one observation draft")
    finish.add_argument("run_id")
    finish.add_argument(
        "--status", required=True, choices=sorted(wiki_observations.FINAL_STATUSES)
    )
    finish.add_argument("--from-file", required=True)
    finish.add_argument("--superseded-by")

    invalidate = subparsers.add_parser(
        "invalidate", help="Create an immutable invalidation tombstone"
    )
    invalidate.add_argument("run_id")
    invalidate.add_argument("--reason", required=True)

    report = subparsers.add_parser("report", help="Render a read-only report")
    report.add_argument("--project")
    report.add_argument("--workspace")
    report.add_argument("--workspace-id")
    report.add_argument("--task-type")
    report.add_argument("--status")
    report.add_argument("--since", type=_observation_date)
    report.add_argument("--until", type=_observation_date)
    return parser


def run_observation_command(args):
    paths = wiki_observations.ObservationPaths.from_root(Path(args.wiki_root))
    if args.observe_command == "start":
        provenance = wiki_observations.derive_provenance(
            Path(args.subject_root), args.project
        )
        scope = wiki_observations.parse_scope_payload(
            _read_observation_payload(args.scope_from_file, "Scope payload")
        )
        task_ref = f"[[{args.task}]]" if args.task is not None else None
        request = wiki_observations.StartRequest(
            title=args.title,
            project=provenance.project,
            workspace=provenance.workspace,
            workspace_id=provenance.workspace_id,
            revision=provenance.revision,
            working_tree=provenance.working_tree,
            agent_surface=args.agent_surface,
            start_mode=args.start_mode,
            task_type=args.task_type,
            workflow_variant=args.workflow_variant,
            task_ref=task_ref,
            sources=tuple(args.source),
        )
        run_id = wiki_observations.start_observation(paths, request, scope)
        print(run_id)
        return 0
    if args.observe_command == "finish":
        payload = wiki_observations.parse_completion_payload(
            _read_observation_payload(args.from_file, "completion payload")
        )
        wiki_observations.finish_observation(
            paths,
            args.run_id,
            args.status,
            payload,
            superseded_by=args.superseded_by,
        )
        print(f"finished {args.run_id}")
        return 0
    if args.observe_command == "invalidate":
        wiki_observations.invalidate_observation(paths, args.run_id, args.reason)
        print(f"invalidated {args.run_id}")
        return 0
    records, invalidated = wiki_observations.collect_records(paths)
    filters = wiki_observations.ReportFilters(
        project=args.project,
        workspace=args.workspace,
        workspace_id=args.workspace_id,
        task_type=args.task_type,
        status=args.status,
        since=args.since,
        until=args.until,
    )
    sys.stdout.write(
        wiki_observations.render_observation_report(
            records, invalidated, filters, now=datetime.now().astimezone()
        )
    )
    return 0


def iter_raw_files():
    """Return every user source in raw/ without modifying the staging area."""
    raw_dir = "raw"
    if not os.path.isdir(raw_dir):
        return []

    files = []
    for root, _, names in os.walk(raw_dir):
        for name in names:
            if name.startswith(".") or name == ".gitkeep":
                continue
            path = os.path.join(root, name)
            files.append(os.path.relpath(path).replace("\\", "/"))
    return sorted(files, key=str.casefold)


GENERIC_RAW_SOURCE_RE = re.compile(
    r"^(?:未命名|無標題|Untitled)(?: \d+)?\.md$",
    re.IGNORECASE,
)


def find_unnamed_raw_sources():
    """Return raw Markdown sources that still have a generic capture name."""
    return [
        path
        for path in iter_raw_files()
        if GENERIC_RAW_SOURCE_RE.fullmatch(os.path.basename(path))
    ]


def collect_source_references():
    """Map raw source paths to the compiled pages that cite them."""
    references = {}
    if not os.path.isdir("wiki"):
        return references

    for root, _, files in os.walk("wiki"):
        for name in files:
            if not name.endswith(".md"):
                continue
            path = os.path.join(root, name)
            if is_observation_operational_path(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as file_obj:
                    metadata, _ = parse_frontmatter(file_obj.read())
            except OSError:
                continue
            if not metadata:
                continue
            sources = metadata.get("sources", [])
            if not isinstance(sources, list):
                sources = [sources]
            for source in sources:
                if isinstance(source, str) and source.startswith("raw/"):
                    references.setdefault(source, []).append(path.replace("\\", "/"))
    return references


def source_kind(path):
    return "text" if os.path.splitext(path)[1].lower() in TEXT_EXTENSIONS else "binary / extraction needed"


def parse_source_triage():
    """Read simple YAML records from the source-triage system page.

    The manifest deliberately uses one fenced YAML block per source so it stays
    editable in Obsidian without introducing a third-party YAML dependency.
    Sources omitted from the manifest are intentionally `untriaged`.
    """
    path = os.path.join("wiki", "_source_triage.md")
    if not os.path.exists(path):
        return {}, []

    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            content = file_obj.read()
    except OSError as error:
        return {}, [f"Could not read `{path}`: {error}"]

    records = {}
    errors = []
    blocks = re.findall(r"```ya?ml\s*\n(.*?)```", content, flags=re.DOTALL | re.IGNORECASE)
    for index, block in enumerate(blocks, 1):
        fields = {}
        for line in block.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            fields[key.strip()] = value.strip().split(" #", 1)[0].strip().strip('"').strip("'")

        source = fields.get("source")
        triage = fields.get("triage")
        if not source and not triage:
            continue
        if not source or not triage:
            errors.append(f"Triage record {index} must include both `source` and `triage`.")
            continue
        if not source.startswith("raw/"):
            errors.append(f"Triage record {index} has a non-raw source: `{source}`.")
            continue
        if triage not in TRIAGE_STATUSES:
            errors.append(f"Triage record {index} has invalid triage `{triage}` for `{source}`.")
            continue
        if source in records:
            errors.append(f"Triage manifest lists `{source}` more than once.")
            continue
        records[source] = fields

    return records, errors


def source_triage(raw_path, triage_records):
    """Return a declared triage state, or the safe default for new raw files."""
    return triage_records.get(raw_path, {}).get("triage", "untriaged")


def collect_task_records():
    """Return frontmatter records from the canonical per-task Markdown files."""
    records = []
    if not os.path.isdir(TASKS_DIR):
        return records

    for root, _, files in os.walk(TASKS_DIR):
        for name in files:
            if not name.endswith(".md") or name == "README.md":
                continue
            path = os.path.join(root, name)
            try:
                with open(path, "r", encoding="utf-8") as file_obj:
                    metadata, _ = parse_frontmatter(file_obj.read())
            except OSError:
                continue
            if not metadata or metadata.get("type") != "task":
                continue
            record = dict(metadata)
            record["_path"] = path.replace("\\", "/")
            records.append(record)

    priority_order = {"P1": 1, "P2": 2, "P3": 3, "P4": 4}
    return sorted(
        records,
        key=lambda item: (
            TASK_STATUSES.index(item.get("status")) if item.get("status") in TASK_STATUSES else len(TASK_STATUSES),
            priority_order.get(item.get("priority"), 99),
            item.get("title", "").casefold(),
        ),
    )


def validate_task_records(records):
    """Return diagnostics that would make a canonical task disappear or misrender."""
    errors = []
    for record in records:
        status = record.get("status")
        if status not in TASK_STATUSES:
            errors.append(
                (record.get("_path", "wiki/tasks/<unknown>"),
                 f"Invalid task status `{status or ''}`; expected one of: {', '.join(TASK_STATUSES)}")
            )
    return errors


def _list_directory_entries(path):
    """Return a deterministic, non-recursive directory listing."""
    return sorted(Path(path).iterdir(), key=lambda item: item.name.casefold())


def _safe_yaml_string(value, *, allow_quotes=True):
    """Return a safe double-quoted YAML scalar, or None for unsafe input."""
    if not isinstance(value, str) or "\n" in value or "\r" in value:
        return None
    if not allow_quotes and '"' in value:
        return None
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
def task_wikilinks(context):
    if not isinstance(context, list):
        context = [context] if context else []
    return ", ".join(f"[[{page}]]" for page in context)


def render_todo_dashboard():
    """Render the human-readable task dashboard from canonical task records."""
    records = collect_task_records()
    task_errors = validate_task_records(records)
    if task_errors:
        raise ValueError("; ".join(f"{path}: {message}" for path, message in task_errors))
    new_captures = [record for record in collect_inbox_records() if record.get("status") == "new"]
    grouped = {status: [] for status in TASK_STATUSES}
    for record in records:
        grouped[record["status"]].append(record)

    lines = [
        "---",
        "type: system",
        "title: 全域待辦清單",
        "tags: [system, todo, open-loop]",
        f"timestamp: {datetime.now().strftime('%Y-%m-%d')}",
        "sources: [\"raw/多工切換疲勞_第三階段_GTD.md\"]",
        "---",
        "",
        "# 待辦清單 (Global Todo & Open Loop Manager)",
        "",
        "> 此頁由 `python3 wiki_cli.py tasks` 從 `wiki/tasks/` 生成。請編輯單筆 task record，不要直接改這個 dashboard。",
        "> Record 格式說明：[[README|wiki/tasks README]]。",
        "> 尚未編譯的 capture 會在下方顯示；其 canonical records 位於 [[_inbox|Capture Inbox]]。",
    ]
    lines.extend(["", "## 待編譯 Inbox (Capture)", ""])
    if not new_captures:
        lines.append("_目前無待編譯 capture。_")
    else:
        for record in new_captures:
            page = os.path.splitext(os.path.basename(record["_path"]))[0]
            lines.append(f"- [[{page}|{record.get('capture_id', page)}]] · source: {record.get('source', 'unknown')}")
            lines.append("  - Action: 判斷應編譯為 task、summary，或標記 ignored。")
    sections = (
        ("pending", "現在可執行 (Pending)"),
        ("waiting", "等待外部回覆 (Waiting)"),
        ("blocked", "依賴尚未完成 (Blocked)"),
        ("done", "已完成 (Done)"),
    )
    for status, heading in sections:
        lines.extend(["", f"## {heading}", ""])
        if not grouped[status]:
            lines.append("_無項目。_")
            continue
        for record in grouped[status]:
            priority = record.get("priority", "—")
            task_id = record.get("id", "missing-id")
            lines.append(f"- [ ] **{record.get('title', task_id)}** · `{task_id}` · {priority}")
            if status == "done":
                lines[-1] = lines[-1].replace("- [ ]", "- [x]", 1)
            if record.get("next_action"):
                lines.append(f"  - Next: {record['next_action']}")
            if record.get("waiting_on"):
                lines.append(f"  - Waiting on: {record['waiting_on']}")
            if record.get("blocked_by"):
                lines.append(f"  - Blocked by: {record['blocked_by']}")
            if record.get("deadline"):
                lines.append(f"  - Deadline: {record['deadline']}")
            context = task_wikilinks(record.get("context", []))
            if context:
                lines.append(f"  - Context: {context}")
            record_page = os.path.splitext(os.path.basename(record["_path"]))[0]
            lines.append(f"  - Record: [[{record_page}|open task record]]")

    return "\n".join(lines) + "\n"


def todo_dashboard_is_current():
    path = os.path.join("wiki", "_todo_list.md")
    try:
        expected = render_todo_dashboard()
    except ValueError:
        return False
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            actual = file_obj.read()
    except OSError:
        return False
    return actual == expected


def write_todo_dashboard(check=False):
    try:
        rendered = render_todo_dashboard()
    except ValueError as error:
        print(f"Invalid task record: {error}")
        return False

    if check:
        path = os.path.join("wiki", "_todo_list.md")
        try:
            with open(path, "r", encoding="utf-8") as file_obj:
                current = file_obj.read()
        except OSError:
            current = ""
        if current == rendered:
            print("Todo dashboard is current.")
            return True
        print("Todo dashboard is stale. Run: python3 wiki_cli.py tasks")
        return False

    os.makedirs("wiki", exist_ok=True)
    path = os.path.join("wiki", "_todo_list.md")
    with open(path, "w", encoding="utf-8") as file_obj:
        file_obj.write(rendered)
    print(f"Updated todo dashboard: {path}")
    return True


def collect_inbox_records():
    records = []
    if not os.path.isdir(INBOX_DIR):
        return records
    for root, _, files in os.walk(INBOX_DIR):
        for name in files:
            if not name.endswith(".md") or name == "README.md":
                continue
            path = os.path.join(root, name)
            try:
                with open(path, "r", encoding="utf-8") as file_obj:
                    metadata, _ = parse_frontmatter(file_obj.read())
            except OSError:
                continue
            if not metadata or metadata.get("type") != "inbox":
                continue
            record = dict(metadata)
            record["_path"] = path.replace("\\", "/")
            records.append(record)
    return sorted(records, key=lambda item: item.get("captured_at", ""), reverse=True)


def mark_inbox_source_completed(external_refs, synced_at=None, refresh=True):
    refs = {
        value for value in external_refs if isinstance(value, str) and value
    }
    if not refs:
        return 0
    if synced_at is None:
        synced_at = datetime.now().astimezone().isoformat(timespec="seconds")
    if not isinstance(synced_at, str) or not synced_at or "\n" in synced_at:
        raise ValueError("synced_at must be single-line text")

    changed = 0
    for record in collect_inbox_records():
        if (
            record.get("source") != "reminders"
            or record.get("status") != "new"
            or record.get("external_ref") not in refs
        ):
            continue
        path = record["_path"]
        with open(path, "r", encoding="utf-8") as file_obj:
            content = file_obj.read()
        parts = content.split("---", 2)
        if len(parts) != 3:
            raise ValueError("invalid inbox frontmatter")
        lines = parts[1].splitlines()
        status_indexes = [
            index for index, line in enumerate(lines) if line.strip() == "status: new"
        ]
        if len(status_indexes) != 1:
            raise ValueError("invalid inbox status")
        index = status_indexes[0]
        lines[index:index + 1] = [
            "status: ignored",
            "reason: completed-at-source-before-compilation",
            f"source_completed_at: {synced_at}",
        ]
        updated = "---" + "\n".join(lines) + "\n---" + parts[2]
        temporary = f"{path}.tmp-{secrets.token_hex(4)}"
        try:
            with open(temporary, "x", encoding="utf-8") as file_obj:
                file_obj.write(updated)
                file_obj.flush()
                os.fsync(file_obj.fileno())
            os.chmod(temporary, stat.S_IMODE(os.stat(path).st_mode))
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        changed += 1

    if refresh and changed and not write_inbox_dashboard():
        raise OSError("dashboard refresh failed")
    return changed


def render_inbox_dashboard():
    records = collect_inbox_records()
    grouped = {status: [] for status in INBOX_STATUSES}
    for record in records:
        grouped.setdefault(record.get("status", "new"), []).append(record)
    lines = [
        "---", "type: system", "title: Capture Inbox", "tags: [system, inbox, capture]",
        f"timestamp: {datetime.now().strftime('%Y-%m-%d')}", "sources: []", "---", "",
        "# Capture Inbox", "",
        "> 先用 `python3 wiki_cli.py inbox add --text \"...\" --source manual` 捕捉；編譯後更新 record 的 `status` 與 `compiled_to`，再重建此頁。",
        "> Reminders 同步只建立或關閉尚未編譯的 capture；不會自動建立／完成正式 Todo，也不會回寫或完成 Apple Reminders。",
    ]
    for status in INBOX_STATUSES:
        lines.extend(["", f"## {status}", ""])
        if not grouped[status]:
            lines.append("_無項目。_")
            continue
        for record in grouped[status]:
            page = os.path.splitext(os.path.basename(record["_path"]))[0]
            lines.append(f"- [[{page}|{record.get('capture_id', page)}]] · {record.get('source', 'unknown')}")
            if record.get("compiled_to"):
                lines.append(f"  - Compiled to: {record['compiled_to']}")
            if record.get("reason"):
                lines.append(f"  - Reason: {record['reason']}")
    return "\n".join(lines) + "\n"


def write_inbox_dashboard(check=False):
    inbox_path = os.path.join("wiki", "_inbox.md")
    todo_path = os.path.join("wiki", "_todo_list.md")
    try:
        inbox_rendered = render_inbox_dashboard()
        todo_rendered = render_todo_dashboard()
    except (OSError, ValueError) as error:
        print(f"Could not render Inbox dashboards: {type(error).__name__}")
        return False

    if check:
        try:
            with open(inbox_path, "r", encoding="utf-8") as file_obj:
                current_inbox = file_obj.read()
            with open(todo_path, "r", encoding="utf-8") as file_obj:
                current_todo = file_obj.read()
        except OSError:
            current_inbox = ""
            current_todo = ""
        if current_inbox == inbox_rendered and current_todo == todo_rendered:
            print("Inbox dashboard is current.")
            return True
        print("Inbox or todo dashboard is stale. Run: python3 wiki_cli.py inbox")
        return False

    try:
        os.makedirs("wiki", exist_ok=True)
        with open(inbox_path, "w", encoding="utf-8") as file_obj:
            file_obj.write(inbox_rendered)
        with open(todo_path, "w", encoding="utf-8") as file_obj:
            file_obj.write(todo_rendered)
    except OSError:
        print("Could not update Inbox dashboards.")
        return False
    print(f"Updated inbox dashboard: {inbox_path}")
    print(f"Updated todo dashboard: {todo_path}")
    return True


def add_inbox_capture(text, source, external_ref=None, refresh=True):
    records = collect_inbox_records()
    if external_ref and any(record.get("external_ref") == external_ref for record in records):
        print(f"Inbox capture already exists for external_ref: {external_ref}")
        return False

    serialized_source = _safe_yaml_string(source)
    serialized_external_ref = _safe_yaml_string(external_ref or "")
    serialized_capture_source = _safe_yaml_string(f"capture:{source}")
    if not isinstance(text, str) or any(
        value is None
        for value in (serialized_source, serialized_external_ref, serialized_capture_source)
    ):
        print("Could not add inbox capture: source and external_ref must be single-line text.")
        return False

    now = datetime.now()
    captured_at = now.strftime("%Y-%m-%dT%H:%M:%S")
    capture_prefix = now.strftime("capture-%Y%m%d-%H%M%S")
    os.makedirs(INBOX_DIR, exist_ok=True)
    descriptor = None
    path = None
    capture_id = None
    for _ in range(100):
        capture_id = f"{capture_prefix}-{secrets.token_hex(4)}"
        path = os.path.join(INBOX_DIR, f"{capture_id}.md")
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            break
        except FileExistsError:
            continue
        except OSError:
            print("Could not add inbox capture: record creation failed.")
            return False
    if descriptor is None or path is None or capture_id is None:
        print("Could not add inbox capture: unique capture ID allocation failed.")
        return False

    body = text.strip()
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file_obj:
            file_obj.write(
                "---\n"
                "type: inbox\n"
                f"title: {capture_id}\n"
                f"capture_id: {capture_id}\n"
                f"captured_at: {captured_at}\n"
                f"source: {serialized_source}\n"
                f"external_ref: {serialized_external_ref}\n"
                "status: new\n"
                "tags: [inbox, capture]\n"
                f"timestamp: {now.strftime('%Y-%m-%d')}\n"
                f"sources: [{serialized_capture_source}]\n"
                "---\n\n"
                f"# {capture_id}\n\n{body}\n"
            )
    except OSError:
        try:
            os.unlink(path)
        except OSError:
            pass
        print("Could not add inbox capture: record write failed.")
        return False
    if refresh and not write_inbox_dashboard():
        print("Could not add inbox capture: dashboard refresh failed.")
        return False
    print(f"Added inbox capture: {path}")
    return True


def render_source_catalog():
    """Render the source registry from raw files and wiki frontmatter."""
    references = collect_source_references()
    triage_records, triage_errors = parse_source_triage()
    lines = [
        "# 原始來源目錄 (Source Catalog)",
        "",
        "> 此檔案由 `python3 wiki_cli.py sources` 重建。`raw/` 是不可變 staging 區；coverage 表示引用情況，triage 表示下一步處理決策。",
        "",
        "## 使用方式",
        "",
        "1. 先用 `python3 wiki_cli.py pending` 找出仍需決策或編譯的來源。",
        "2. 在 `wiki/_source_triage.md` 記錄處理決策；未列出的來源預設為 `untriaged`。",
        "3. 讀取一個來源、搜尋既有 wiki，然後新增或更新概念頁。",
        "4. 在頁面的 `sources` frontmatter 登記該 `raw/...` 路徑，再執行 `python3 wiki_cli.py sources`。",
        "",
        "## 來源覆蓋狀態",
        "",
    ]
    raw_files = iter_raw_files()
    if not raw_files:
        lines.append("_尚無原始來源。_")
    for raw_path in raw_files:
        pages = sorted(references.get(raw_path, []), key=str.casefold)
        coverage = "compiled" if pages else "uncovered"
        triage = source_triage(raw_path, triage_records)
        linked_pages = ", ".join(f"`{page}`" for page in pages) if pages else "—"
        lines.append(
            f"- **{coverage}** · triage: **{triage}** · `{raw_path}` · {source_kind(raw_path)} · pages: {linked_pages}"
        )
    lines.extend([
        "",
        "## 狀態語義",
        "",
        "- `compiled` 表示至少一頁 wiki frontmatter 引用了該來源；不代表內容已完整萃取或所有結論都已驗證。",
        "- `uncovered` 只表示尚無 frontmatter 引用；是否需要處理由 triage 決定。",
        "- `untriaged` 與 `compile` 且為 uncovered 的來源，才會由 `pending` 顯示。",
        "- `noise-ignore`、`duplicate`、`reference-only`、`pii-sensitive` 與 `binary-extract-later` 不會被當作一般編譯待辦。",
        "- 二進位檔案應先以可追溯的文字摘錄、OCR 或人工說明進行編譯；保留原檔在 `raw/`。",
        "- 同一來源可支持多個概念頁；同一概念頁也可列出多個來源。",
    ])
    if triage_errors:
        lines.extend(["", "## Triage manifest errors", ""])
        lines.extend(f"- ⚠️ {error}" for error in triage_errors)
    return "\n".join(lines) + "\n"


def source_catalog_is_current():
    """Return whether the generated source catalog matches the checked-in page."""
    path = os.path.join("wiki", "_sources.md")
    expected = render_source_catalog()
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            actual = file_obj.read()
    except OSError:
        return False
    return actual == expected


def write_source_catalog(check=False):
    triage_records, triage_errors = parse_source_triage()
    if check:
        if triage_errors:
            print("Source triage manifest has errors:")
            for error in triage_errors:
                print(f"- {error}")
            return False
        if source_catalog_is_current():
            print("Source catalog is current.")
            return True
        print("Source catalog is stale. Run: python3 wiki_cli.py sources")
        return False

    os.makedirs("wiki", exist_ok=True)
    path = os.path.join("wiki", "_sources.md")
    with open(path, "w", encoding="utf-8") as file_obj:
        file_obj.write(render_source_catalog())
    print(f"Updated source catalog: {path}")
    return True

def log_message(action, details):
    log_path = os.path.join("wiki", "z_log.md")
    today = datetime.now().strftime("%Y-%m-%d")
    log_line = f"\n## [{today}] {action} | {details}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(log_line)
    print(f"Logged: {action} | {details}")

def clean_and_tokenize(text):
    text = text.lower()
    # Find all English words, numbers, and Chinese characters
    words = re.findall(r'[a-zA-Z0-9\u4e00-\u9fa5]+', text)
    # Basic English and Chinese stopwords
    stopwords = {"this", "is", "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "的", "了", "在", "是", "和", "有"}
    return [w for w in words if w not in stopwords]

def search_wiki(query):
    # Check if the query uses case-insensitive AND logic
    # E.g., "名古屋 AND 住宿"
    is_and_query = False
    query_upper = query.upper()
    if " AND " in query_upper:
        is_and_query = True
        parts = re.split(r'\s+AND\s+', query, flags=re.IGNORECASE)
        query_tokens = []
        for part in parts:
            query_tokens.extend(clean_and_tokenize(part))
        required_token_groups = [clean_and_tokenize(part) for part in parts if clean_and_tokenize(part)]
    else:
        query_tokens = clean_and_tokenize(query)
        required_token_groups = []

    if not query_tokens:
        print("Empty search query.")
        return

    # 1. Collect all documents and tokenize them
    documents = []
    # Search in both wiki/ and raw/ folders
    for folder in ["wiki", "raw"]:
        if not os.path.isdir(folder):
            continue
        for root, _, files in os.walk(folder):
            for file in files:
                if not file.endswith((".md", ".txt")):
                    continue
                path = os.path.join(root, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        content = f.read()
                except Exception:
                    continue
                
                tokens = clean_and_tokenize(content)
                documents.append({
                    "path": path,
                    "content": content,
                    "tokens": tokens,
                    "token_count": len(tokens)
                })

    N = len(documents)
    if N == 0:
        print("No documents found to search.")
        return

    # 2. Compute Document Frequency (DF) for each query token using substring matching
    df = {}
    for q_t in query_tokens:
        df[q_t] = sum(1 for doc in documents if q_t in doc["content"].lower())

    # 3. Compute IDF for each query token (using natural logarithm)
    import math
    idf = {}
    for q_t in query_tokens:
        # Smooth IDF formulation
        idf[q_t] = math.log(1 + N / (df[q_t] + 1)) + 1

    # 4. Compute TF-IDF score for each document
    results = []
    for doc in documents:
        # Check AND query constraint using substring matching
        if is_and_query and required_token_groups:
            meets_and_condition = all(any(t in doc["content"].lower() for t in group) for group in required_token_groups)
            if not meets_and_condition:
                continue

        score = 0.0
        has_match = False
        for q_t in query_tokens:
            count = doc["content"].lower().count(q_t)
            if count > 0:
                has_match = True
                # Relative term frequency to prevent document length bias
                tf = count / doc["token_count"] if doc["token_count"] > 0 else 0
                score += tf * idf[q_t]
        
        if has_match:
            # Find a matching snippet for user-friendly output
            lines = doc["content"].splitlines()
            match_snippet = ""
            for line in lines:
                if any(q_t in line.lower() for q_t in query_tokens):
                    match_snippet = line.strip()
                    if len(match_snippet) > 80:
                        match_snippet = match_snippet[:80] + "..."
                    break
            results.append((score, doc["path"], match_snippet))

    # Sort by score descending
    results.sort(key=lambda x: x[0], reverse=True)

    print(f"\n🔍 Search results for: '{query}' (TF-IDF ranking)")
    print("-" * 60)
    if not results:
        print("No matches found.")
    for score, path, snippet in results[:10]:
        print(f"[{score:.4f}] {path}")
        print(f"    Snippet: {snippet}")
        print("-" * 60)

def parse_frontmatter(content):
    if not content.startswith("---"):
        return None, content
    parts = content.split("---", 2)
    if len(parts) < 3:
        return None, content
    
    yaml_text = parts[1]
    body = parts[2]
    metadata = {}
    
    lines = yaml_text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if not stripped:
            i += 1
            continue
        
        if ":" in line:
            k, v = line.split(":", 1)
            k = k.strip()
            v = v.strip()
            
            # Case 1: multi-line list with square brackets
            if v.startswith("[") and not v.endswith("]"):
                accumulated = [v]
                i += 1
                while i < len(lines):
                    next_line = lines[i].strip()
                    accumulated.append(next_line)
                    if next_line.endswith("]") or next_line == "]":
                        i += 1
                        break
                    i += 1
                full_v = " ".join(accumulated).strip()
                if full_v.startswith("[") and full_v.endswith("]"):
                    full_v = full_v[1:-1].strip()
                import re
                parts_list = re.split(r',\s*', full_v)
                items = []
                for item in parts_list:
                    item_str = item.strip().strip('"').strip("'")
                    if item_str:
                        items.append(item_str)
                metadata[k] = items
                continue
            
            # Case 2: inline list with square brackets
            elif v.startswith("[") and v.endswith("]"):
                full_v = v[1:-1].strip()
                import re
                parts_list = re.split(r',\s*', full_v)
                items = []
                for item in parts_list:
                    item_str = item.strip().strip('"').strip("'")
                    if item_str:
                        items.append(item_str)
                metadata[k] = items
                i += 1
                continue
                
            # Case 3: Block sequence list
            elif v == "" or v == "[]":
                items = []
                next_i = i + 1
                has_bullet = False
                while next_i < len(lines):
                    next_line = lines[next_i].strip()
                    if next_line.startswith("-"):
                        has_bullet = True
                        val = next_line[1:].strip().strip('"').strip("'")
                        items.append(val)
                        next_i += 1
                    elif next_line == "" or ":" in next_line:
                        break
                    else:
                        if has_bullet:
                            val = next_line.strip().strip('"').strip("'")
                            items.append(val)
                            next_i += 1
                        else:
                            break
                if has_bullet:
                    metadata[k] = items
                    i = next_i
                    continue
                else:
                    metadata[k] = []
                    i += 1
                    continue
            
            # Case 4: simple string/scalar
            else:
                v = v.strip().strip('"').strip("'")
                metadata[k] = v
                i += 1
                continue
        else:
            i += 1
            
    return metadata, body


def _validate_invalidation_tombstone(path, paths):
    errors = []
    try:
        if Path(path).is_symlink():
            raise ObservationDomainError(
                "validation", "invalidation tombstone must not be a symlink"
            )
        content = Path(path).read_text(encoding="utf-8")
        metadata, body = wiki_observations._parse_frontmatter(content)
    except ObservationDomainError as error:
        return [str(error)]
    except (OSError, UnicodeError) as error:
        return [f"could not read invalidation tombstone: {error}"]

    required = {
        "type",
        "title",
        "tags",
        "timestamp",
        "target_run_id",
        "reason",
        "sources",
    }
    for field in sorted(required - set(metadata)):
        errors.append(f"missing required frontmatter `{field}`")
    for field in sorted(set(metadata) - required):
        errors.append(f"unexpected frontmatter field `{field}`")
    if metadata.get("type") != "observation-invalidation":
        errors.append("type must be observation-invalidation")
    target = metadata.get("target_run_id")
    if not isinstance(target, str) or re.fullmatch(
        r"obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}", target
    ) is None:
        errors.append("target_run_id has an invalid format")
    elif Path(path).stem != target:
        errors.append("target_run_id must match the tombstone filename")
    if metadata.get("title") != f"Invalidate {target}":
        errors.append("title must identify target_run_id")
    if metadata.get("tags") != ["observation", "invalidation"]:
        errors.append("tags must be observation and invalidation")
    timestamp = metadata.get("timestamp")
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp)
        if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
            raise ValueError
    except (TypeError, ValueError):
        errors.append("timestamp must be an aware ISO-8601 datetime")
    try:
        wiki_observations._validate_scalar(
            metadata.get("reason"), "invalidation reason"
        )
    except ObservationDomainError as error:
        errors.append(str(error))
    if metadata.get("sources") != []:
        errors.append("sources must be an empty list")
    if body:
        errors.append("invalidation tombstone must not contain a body")

    if isinstance(target, str) and re.fullmatch(
        r"obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}", target
    ):
        try:
            target_metadata, target_body = read_observation_record(paths, target)
            target_errors = validate_observation_domain_record(
                target_metadata, target_body, paths
            )
        except ObservationDomainError:
            errors.append("target_run_id points to no valid observation record")
        else:
            if target_errors:
                errors.append("target_run_id points to no valid observation record")
            elif target_metadata.get("status") == "draft":
                errors.append("target_run_id must reference a final observation")
    return errors


def collect_observation_lint_errors():
    errors = []
    observation_root = Path(OBSERVATIONS_DIR)
    if not os.path.lexists(observation_root):
        return errors
    if observation_root.is_symlink():
        return [
            (
                observation_root.as_posix(),
                "Observation: observation root must not be a symlink",
            )
        ]
    if not observation_root.is_dir():
        return [
            (
                observation_root.as_posix(),
                "Observation: observation root must be a directory",
            )
        ]
    try:
        paths = ObservationPaths.from_root(Path.cwd())
    except ObservationDomainError as error:
        return [(observation_root.as_posix(), f"Observation: {error}")]

    try:
        root_entries = _list_directory_entries(observation_root)
    except OSError as error:
        return [
            (
                observation_root.as_posix(),
                f"Observation: could not enumerate observation storage: {error}",
            )
        ]

    record_paths = []
    tombstone_paths = []
    for path in root_entries:
        if path.name == ".locks":
            if path.is_symlink():
                errors.append(
                    (
                        path.as_posix(),
                        "Observation: .locks directory must not be a symlink",
                    )
                )
            elif not path.is_dir():
                errors.append(
                    (
                        path.as_posix(),
                        "Observation: .locks path must be a directory",
                    )
                )
            continue
        if path.name == "invalidations":
            if path.is_symlink():
                errors.append(
                    (
                        path.as_posix(),
                        "Observation: invalidations directory must not be a symlink",
                    )
                )
                continue
            if not path.is_dir():
                errors.append(
                    (
                        path.as_posix(),
                        "Observation: invalidations path must be a directory",
                    )
                )
                continue
            try:
                invalidation_entries = _list_directory_entries(path)
            except OSError as error:
                errors.append(
                    (
                        path.as_posix(),
                        f"Observation: could not enumerate invalidations: {error}",
                    )
                )
                continue
            for tombstone in invalidation_entries:
                if tombstone.is_symlink():
                    errors.append(
                        (
                            tombstone.as_posix(),
                            "Observation: unexpected symlink in invalidations storage",
                        )
                    )
                elif tombstone.is_dir():
                    errors.append(
                        (
                            tombstone.as_posix(),
                            "Observation: unexpected directory in invalidations storage",
                        )
                    )
                elif tombstone.suffix == ".md":
                    tombstone_paths.append(tombstone)
            continue
        if path.is_symlink():
            errors.append(
                (
                    path.as_posix(),
                    "Observation: unexpected symlink in observation operational storage",
                )
            )
        elif path.is_dir():
            errors.append(
                (
                    path.as_posix(),
                    "Observation: unexpected directory in observation operational storage",
                )
            )
        elif path.suffix == ".md":
            record_paths.append(path)

    for path in record_paths:
        run_id = path.stem
        try:
            metadata, body = read_observation_record(paths, run_id)
            messages = validate_observation_domain_record(metadata, body, paths)
        except ObservationDomainError as error:
            messages = [str(error)]
        errors.extend(
            (path.as_posix(), f"Observation: {message}") for message in messages
        )

    for path in tombstone_paths:
        messages = _validate_invalidation_tombstone(path, paths)
        errors.extend(
            (path.as_posix(), f"Observation: {message}") for message in messages
        )

    return errors


def perform_lint_checks():
    wiki_dir = "wiki"
    if not os.path.isdir(wiki_dir):
        return None

    errors_schema = collect_observation_lint_errors()
    errors_schema.extend(
        (
            path,
            "Raw source filename is unnamed; assign a descriptive filename before triage or compilation",
        )
        for path in find_unnamed_raw_sources()
    )
    broken_links = []
    all_pages = {}
    inbound_links = {}

    # 1. Collect all pages & check YAML schema
    for root, _, files in os.walk(wiki_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
            path = os.path.join(root, file)

            if is_observation_operational_path(path):
                continue

            page_name = os.path.splitext(file)[0]
            
            all_pages[page_name] = path
            inbound_links[page_name] = []

            # Exclude special files from schema checks
            if page_name in SYSTEM_PAGES:
                continue

            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception as e:
                errors_schema.append((path, f"Could not read file: {e}"))
                continue

            metadata, body = parse_frontmatter(content)
            if metadata is None:
                if content.startswith("---"):
                    errors_schema.append((path, "YAML frontmatter parsing failed"))
                else:
                    errors_schema.append((path, "Missing YAML Frontmatter"))
                continue
            elif not metadata:
                errors_schema.append((path, "Missing YAML Frontmatter"))
                continue

            required = ["type", "title", "tags", "timestamp", "sources"]
            missing = [r for r in required if r not in metadata]
            if missing:
                errors_schema.append((path, f"Missing required frontmatter properties: {', '.join(missing)}"))

            if metadata.get("type") == "task":
                task_record = dict(metadata)
                task_record["_path"] = path.replace("\\", "/")
                errors_schema.extend(validate_task_records([task_record]))

            # Check sources existence relative to repository root
            sources = metadata.get("sources", [])
            if isinstance(sources, list):
                for s in sources:
                    if isinstance(s, str) and s.startswith("raw/") and not os.path.exists(s):
                        errors_schema.append((path, f"Source reference not found on disk: {s}"))

    # 2. Check wikilinks & internal links
    outbound_warnings = []
    for page_name, path in all_pages.items():
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except Exception:
            continue

        wikilinks = re.findall(r'\[\[([^\]|#]+)(?:(?:#[^\]|]+)?(?:\|[^\]]+)?)?\]\]', content)
        outbound_count = 0
        for link in wikilinks:
            target = link.strip()
            if "#" in target:
                target = target.split("#")[0].strip()
            if not target:
                continue
            
            outbound_count += 1
            if target not in all_pages:
                broken_links.append((path, f"Broken link: [[{link}]] pointing to non-existent page"))
            else:
                inbound_links[target].append(page_name)

        if page_name not in SYSTEM_PAGES and outbound_count == 0:
            outbound_warnings.append(path)

    # 3. Detect orphan pages
    orphans = []
    for page_name, path in all_pages.items():
        if page_name in SYSTEM_PAGES:
            continue
        if not inbound_links[page_name]:
            orphans.append(path)

    # 4. Overview drift check
    overview_path = os.path.join(wiki_dir, "_overview.md")
    drift_warn = None
    if os.path.exists(overview_path):
        overview_mtime = os.path.getmtime(overview_path)
        newest_file = None
        newest_mtime = 0
        for name, path in all_pages.items():
            if name in SYSTEM_PAGES:
                continue
            mtime = os.path.getmtime(path)
            if mtime > newest_mtime:
                newest_mtime = mtime
                newest_file = path
        
        if newest_mtime > overview_mtime:
            drift_warn = f"Overview file `wiki/_overview.md` has drifted (older than newest wiki file `{newest_file}`). Please update overview."

    return errors_schema, broken_links, orphans, drift_warn, sorted(outbound_warnings)

def run_lint():
    results = perform_lint_checks()
    if results is None:
        print("Error: wiki directory not found.")
        sys.exit(1)
        
    errors_schema, broken_links, orphans, drift_warn, outbound_warnings = results
    
    is_clean = not (errors_schema or broken_links or orphans or drift_warn)
    status = "🟢 Green" if is_clean else "🟡 Yellow"
    
    print(f"# 知識庫健檢報告 (Lint Report) — {datetime.now().strftime('%Y-%m-%d')}")
    print(f"\n## 總體狀態: {status}\n")
    
    print("## 1. Schema 完整性檢查")
    if not errors_schema:
        print("* 🟢 通過 - 所有實體與概念頁面結構完整。")
    else:
        for path, err in errors_schema:
            print(f"* ❌ `{path}`: {err}")
            
    print("\n## 2. 連結健康度檢查")
    if not broken_links:
        print("* 🟢 通過 - 未發現死連結。")
    else:
        for path, err in broken_links:
            print(f"* ❌ `{path}` 中的 {err}")

    print("\n## 3. 孤兒頁面檢測")
    if not orphans:
        print("* 🟢 通過 - 所有頁面皆已與其餘主題建立連結。")
    else:
        for path in orphans:
            print(f"* ⚠️ `{path}` 是孤兒頁面，目前沒有入站連結。")

    print("\n## 4. 概述漂移警告")
    if not drift_warn:
        print("* 🟢 通過 - 概述與知識庫狀態同步。")
    else:
        print(f"* ⚠️ {drift_warn}")

    print("\n## 5. 零出站連結警告")
    if not outbound_warnings:
        print("* 🟢 通過 - 所有非系統頁面皆已建立出站連結。")
    else:
        for path in outbound_warnings:
            print(f"* ⚠️ `{path}` 沒有出站連結，目前未連結至任何其他頁面。")

def fileback_query(title, tags, page_type="concept"):
    import glob
    import json
    
    # 1. Find newest transcript.jsonl
    home = os.path.expanduser("~")
    pattern = os.path.join(home, ".gemini", "antigravity-cli", "brain", "*", ".system_generated", "logs", "transcript.jsonl")
    files = glob.glob(pattern)
    if not files:
        print("Error: Could not find any transcript.jsonl files under ~/.gemini/antigravity-cli/brain/")
        return
    
    files.sort(key=os.path.getmtime, reverse=True)
    newest_transcript = files[0]
    
    # 2. Extract last PLANNER_RESPONSE from MODEL
    target_content = None
    try:
        with open(newest_transcript, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading transcript file: {e}")
        return

    # Look backwards
    for line in reversed(lines):
        try:
            data = json.loads(line)
            if data.get("source") == "MODEL" and data.get("type") == "PLANNER_RESPONSE":
                content = data.get("content", "").strip()
                if content:
                    # Ignore command itself if it gets logged, or very short texts
                    if "wiki_cli.py" in content or "fileback" in content:
                        continue
                    target_content = content
                    break
        except Exception:
            continue
            
    if not target_content:
        print("Error: Could not find any assistant response in transcript logs to file back.")
        return
        
    # 3. Create filename
    filename = title.strip().replace(" ", "_")
    filename = "".join(c for c in filename if c.isalnum() or c in ("_", "-"))
    if not filename:
        print("Error: Invalid title provided.")
        return
        
    wiki_dir = "wiki"
    filepath = os.path.join(wiki_dir, f"{filename}.md")
    
    if os.path.exists(filepath):
        print(f"Error: Wiki file '{filepath}' already exists.")
        return
        
    # 4. Format content
    today = datetime.now().strftime("%Y-%m-%d")
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    yaml_tags = "[" + ", ".join(f'"{t}"' for t in tag_list) + "]"
    
    file_data = f"""---
type: {page_type}
title: "{title}"
tags: {yaml_tags}
timestamp: {today}
sources: ["conversation_history"]
---

# {title}

{target_content}
"""

    # Write to file
    try:
        os.makedirs(wiki_dir, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(file_data)
        print(f"Successfully filed back query response into: {filepath}")
    except Exception as e:
        print(f"Error writing to file: {e}")
        return
        
    # 5. Register in _index.md and _overview.md
    _index_path = os.path.join(wiki_dir, "_index.md")
    _overview_path = os.path.join(wiki_dir, "_overview.md")
    
    def insert_link(path, name):
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        lines = text.splitlines()
        insert_idx = -1
        for idx, l in enumerate(lines):
            if "z_log" in l:
                insert_idx = idx
                break
        if insert_idx != -1:
            lines.insert(insert_idx, f"* [[{name}]]")
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            print(f"Registered [[{name}]] in {path}")
            
    insert_link(_index_path, filename)
    insert_link(_overview_path, filename)
    
    # 6. Update _overview.md timestamp to prevent drift warning
    if os.path.exists(_overview_path):
        with open(_overview_path, "r", encoding="utf-8") as f:
            text = f.read()
        today_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        new_text = re.sub(
            r"最近更新於 \d{4}-\d{2}-\d{2} \d{2}:\d{2}",
            f"最近更新於 {today_time}",
            text
        )
        with open(_overview_path, "w", encoding="utf-8") as f:
            f.write(new_text)
        print("Updated _overview.md timestamp.")
        
    # 7. Log change
    log_message("ingest", f"Filed back query response into {filepath}")


def fileback_content(title, tags, page_type, content, sources):
    """File an explicit query result into the wiki without reading private tool transcripts."""
    content = content.strip()
    if not content:
        print("Error: fileback content is empty.")
        return

    filename = title.strip().replace(" ", "_")
    filename = "".join(char for char in filename if char.isalnum() or char in ("_", "-"))
    if not filename:
        print("Error: Invalid title provided.")
        return

    folder = "summary" if page_type in {"summary", "query"} else "concept"
    wiki_dir = os.path.join("wiki", folder)
    os.makedirs(wiki_dir, exist_ok=True)
    filepath = os.path.join(wiki_dir, f"{filename}.md")
    if os.path.exists(filepath):
        print(f"Error: Wiki file '{filepath}' already exists.")
        return

    tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()]
    source_list = [source.strip() for source in sources if source.strip()] or ["conversation"]
    yaml_tags = "[" + ", ".join(f'"{tag}"' for tag in tag_list) + "]"
    yaml_sources = "[" + ", ".join(f'"{source}"' for source in source_list) + "]"
    today = datetime.now().strftime("%Y-%m-%d")
    page_data = f'''---
type: {page_type}
title: "{title}"
tags: {yaml_tags}
timestamp: {today}
sources: {yaml_sources}
---

# {title}

{content}
'''
    with open(filepath, "w", encoding="utf-8") as file_obj:
        file_obj.write(page_data)

    query_index = os.path.join("wiki", "_queries.md")
    if os.path.exists(query_index):
        with open(query_index, "a", encoding="utf-8") as file_obj:
            file_obj.write(f"- [[{filename}]] · {today}\n")
    log_message("query", f"Filed explicit query output into {filepath}")
    write_source_catalog()
    print(f"Successfully filed back content into: {filepath}")

def generate_graph_web(open_browser=False):
    import json
    wiki_dir = "wiki"
    if not os.path.isdir(wiki_dir):
        print(f"Error: {wiki_dir} directory not found.")
        return
        
    nodes = []
    links = []
    file_map = {}
    
    for root, _, files in os.walk(wiki_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
            path = os.path.join(root, file)
            if is_observation_operational_path(path):
                continue
            name = os.path.splitext(file)[0]
            
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            except Exception:
                continue
                
            metadata, body = parse_frontmatter(content)
            if not metadata:
                metadata = {}
                
            title = metadata.get("title", name)
            page_type = metadata.get("type", "concept")
            tags = metadata.get("tags", [])
            timestamp = metadata.get("timestamp", "")
            
            if name == "_index":
                page_type = "index"
            elif name == "_overview":
                page_type = "overview"
            elif name == "z_log":
                page_type = "log"
                
            # Parse Google Maps links
            maps_links = []
            for line in body.splitlines():
                if "google.com/maps" in line or "google.co.jp/maps" in line:
                    match = re.search(r'\[([^\]]+)\]\((https?://[^\)]+)\)', line)
                    if match:
                        link_text = match.group(1)
                        url = match.group(2)
                        line_context = line.split("：")[0] if "：" in line else line.split(":")[0] if ":" in line else ""
                        line_context = line_context.replace("*", "").replace("-", "").strip()
                        line_context = re.sub(r'\[\[[^\]|]+\|([^\]]+)\]\]', r'\1', line_context)
                        line_context = re.sub(r'\[\[([^\]]+)\]\]', r'\1', line_context)
                        
                        full_text = f"{line_context} ({link_text})" if line_context else link_text
                        maps_links.append({
                            "text": full_text,
                            "url": url
                        })
                
            abs_path = os.path.abspath(path)
            travel_stats = None
            if name == "Nagoya_Family_Trip_Master_Plan":
                travel_stats = {
                    "total_mileage_km": 790,
                    "total_duration_hours": 11.3,
                    "daily_stats": [
                        {"day": "8/15 Day 1", "mileage": 0, "duration": 0},
                        {"day": "8/16 Day 2", "mileage": 0, "duration": 0},
                        {"day": "8/17 Day 3", "mileage": 65, "duration": 60},
                        {"day": "8/18 Day 4", "mileage": 85, "duration": 60},
                        {"day": "8/19 Day 5", "mileage": 45, "duration": 50},
                        {"day": "8/20 Day 6", "mileage": 95, "duration": 70},
                        {"day": "8/21 Day 7", "mileage": 230, "duration": 180},
                        {"day": "8/22 Day 8", "mileage": 20, "duration": 30},
                        {"day": "8/23 Day 9", "mileage": 105, "duration": 90},
                        {"day": "8/24 Day 10", "mileage": 70, "duration": 50},
                        {"day": "8/25 Day 11", "mileage": 45, "duration": 40},
                        {"day": "8/26 Day 12", "mileage": 30, "duration": 50}
                    ]
                }
            
            nodes.append({
                "id": name,
                "title": title,
                "type": page_type,
                "tags": tags if isinstance(tags, list) else [tags] if tags else [],
                "timestamp": timestamp,
                "maps_links": maps_links,
                "abs_path": abs_path,
                "travel_stats": travel_stats
            })
            file_map[name] = True
            
            outlinks = re.findall(r'\[\[([^\]|#]+)(?:(?:#[^\]|]+)?(?:\|[^\]]+)?)?\]\]', body)
            for out in outlinks:
                out_clean = out.strip().replace(" ", "_")
                links.append({
                    "source": name,
                    "target": out_clean
                })
                
    valid_links = []
    seen_links = set()
    for l in links:
        s, t = l["source"], l["target"]
        if s in file_map and t in file_map:
            link_key = tuple(sorted([s, t]))
            if link_key not in seen_links:
                seen_links.add(link_key)
                valid_links.append(l)
                
    data_js = json.dumps({"nodes": nodes, "links": valid_links}, indent=2, ensure_ascii=False)
    
    html_template = """<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LLM Wiki 知識圖譜視覺化</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        :root {
            --bg-color: #0b0f19;
            --panel-bg: rgba(15, 23, 42, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            
            --color-concept: #38bdf8;
            --color-summary: #fb923c;
            --color-entity: #c084fc;
            --color-special: #f43f5e;
            --color-default: #64748b;
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body, html {
            width: 100%;
            height: 100%;
            background-color: var(--bg-color);
            color: var(--text-primary);
            font-family: 'Inter', sans-serif;
            overflow: hidden;
        }
        
        .bg-grid {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: 0;
            background-image: 
                radial-gradient(circle at 10% 20%, rgba(56, 189, 248, 0.04) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(192, 132, 252, 0.04) 0%, transparent 40%),
                linear-gradient(rgba(255, 255, 255, 0.004) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255, 255, 255, 0.004) 1px, transparent 1px);
            background-size: 100% 100%, 100% 100%, 40px 40px, 40px 40px;
        }
        
        #graph-container {
            width: 100%;
            height: 100%;
            position: absolute;
            top: 0;
            left: 0;
            z-index: 1;
        }
        
        header {
            position: absolute;
            top: 24px;
            left: 24px;
            z-index: 10;
            pointer-events: none;
        }
        
        h1 {
            font-family: 'Outfit', sans-serif;
            font-size: 24px;
            font-weight: 700;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, #f8fafc 0%, #cbd5e1 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 4px;
        }
        
        .subtitle {
            font-size: 11px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1.5px;
        }
        
        .glass-panel {
            background: var(--panel-bg);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }
        
        #detail-panel {
            position: absolute;
            top: 24px;
            right: 24px;
            width: 360px;
            max-height: calc(100% - 48px);
            z-index: 10;
            padding: 24px;
            display: flex;
            flex-direction: column;
            gap: 20px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            transform: translateX(400px);
            opacity: 0;
            overflow-y: auto;
        }
        
        #detail-panel.active {
            transform: translateX(0);
            opacity: 1;
        }
        
        #control-panel {
            position: absolute;
            bottom: 24px;
            left: 24px;
            z-index: 10;
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 16px;
            width: 300px;
        }
        
        .search-container {
            position: relative;
            width: 100%;
        }
        
        .search-input {
            width: 100%;
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 10px 16px 10px 40px;
            color: var(--text-primary);
            font-family: inherit;
            font-size: 14px;
            outline: none;
            transition: all 0.2s;
        }
        
        .search-input:focus {
            border-color: var(--color-concept);
            box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.15);
        }
        
        .search-icon {
            position: absolute;
            left: 14px;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-secondary);
            pointer-events: none;
        }
        
        .legend-title {
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            margin-bottom: 8px;
            font-weight: 600;
        }
        
        .legend-item {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 13px;
            color: var(--text-primary);
            margin-bottom: 6px;
        }
        
        .legend-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }
        
        .meta-label {
            font-size: 11px;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 4px;
        }
        
        .meta-val {
            font-size: 14px;
            font-weight: 500;
            color: var(--text-primary);
        }
        
        .tag-pill {
            display: inline-block;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-color);
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 11px;
            color: var(--text-secondary);
            margin-right: 6px;
            margin-bottom: 6px;
        }
        
        .node {
            cursor: pointer;
            stroke-width: 2px;
            transition: stroke-width 0.1s;
        }
        
        .node-label {
            font-family: 'Outfit', sans-serif;
            font-weight: 500;
            pointer-events: none;
            fill: #e2e8f0;
            font-size: 11px;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.8), 0 0 10px rgba(0, 0, 0, 0.5);
        }
        
        .link {
            stroke-opacity: 0.15;
            stroke-width: 1.5px;
            transition: all 0.2s;
        }
        
        .close-btn {
            position: absolute;
            top: 16px;
            right: 16px;
            background: transparent;
            border: none;
            color: var(--text-secondary);
            cursor: pointer;
            font-size: 18px;
            transition: color 0.2s;
        }
        
        .close-btn:hover {
            color: var(--text-primary);
        }
        
        .connection-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-top: 8px;
            max-height: 200px;
            overflow-y: auto;
        }
        
        .connection-item {
            padding: 8px 12px;
            background: rgba(255, 255, 255, 0.02);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            font-size: 13px;
            color: #cbd5e1;
            cursor: pointer;
            transition: all 0.2s;
        }
        
        .connection-item:hover {
            background: rgba(56, 189, 248, 0.08);
            border-color: rgba(56, 189, 248, 0.3);
            color: #fff;
        }
        
        .timeline-item {
            position: relative;
            margin-bottom: 16px;
        }
        
        .timeline-item::before {
            content: '';
            position: absolute;
            left: -26px;
            top: 4px;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #38bdf8;
            border: 2px solid var(--bg-color);
            box-shadow: 0 0 8px #38bdf8;
        }
        
        .timeline-time {
            font-size: 12px;
            font-weight: 700;
            color: #fb923c;
            text-transform: uppercase;
            margin-bottom: 2px;
        }
        
        .timeline-desc {
            font-size: 13px;
            color: #cbd5e1;
            line-height: 1.4;
        }
    </style>
</head>
<body>
    <div class="bg-grid"></div>
    <div id="graph-container"></div>
    
    <header>
        <h1>LLM Wiki 知識圖譜</h1>
        <div class="subtitle">Personal Knowledge base Graph</div>
    </header>
    
    <!-- Control Panel -->
    <div id="control-panel" class="glass-panel">
        <div class="search-container">
            <span class="search-icon">🔍</span>
            <input type="text" class="search-input" placeholder="搜尋筆記..." oninput="filterGraph(this.value)">
        </div>
        
        <div>
            <div class="legend-title">主題類別</div>
            <div class="legend-item">
                <div class="legend-dot" style="background-color: var(--color-concept);"></div>
                <span>概念 (Concept)</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background-color: var(--color-summary);"></div>
                <span>摘要 (Summary)</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background-color: var(--color-entity);"></div>
                <span>實體 (Entity)</span>
            </div>
            <div class="legend-item">
                <div class="legend-dot" style="background-color: var(--color-special);"></div>
                <span>系統檔案 (Index/Log)</span>
            </div>
        </div>
    </div>
    
    <!-- Detail Sidebar -->
    <div id="detail-panel" class="glass-panel">
        <button class="close-btn" onclick="closeDetail()">✕</button>
        <div>
            <div class="meta-label">類別</div>
            <div id="detail-type" class="meta-val" style="font-weight: 700;">CONCEPT</div>
        </div>
        <div>
            <h2 id="detail-title" style="font-family: 'Outfit', sans-serif; font-size: 20px; font-weight: 600; margin-bottom: 8px;">Title</h2>
            <div id="detail-tags"></div>
        </div>
        <hr style="border: 0; border-top: 1px solid var(--border-color);">
        <div>
            <div class="meta-label">更新時間</div>
            <div id="detail-timestamp" class="meta-val" style="font-size: 13px;">2026-07-08</div>
        </div>
        <div>
            <div class="meta-label">關聯主題 (Backlinks & Outlinks)</div>
            <div id="detail-connections" class="connection-list"></div>
        </div>
        <div id="detail-maps-section" style="display: none; margin-top: 16px;">
            <div class="meta-label">🗺️ 推薦導航與景點地圖</div>
            <div id="detail-maps" class="connection-list"></div>
        </div>
        <div id="detail-timeline-section" style="display: none; margin-top: 16px;">
            <div class="meta-label">📅 行程時間軸 (Timeline)</div>
            <div id="detail-timeline" style="margin-top: 12px; position: relative; padding-left: 20px; border-left: 2px solid rgba(56, 189, 248, 0.2); max-height: 250px; overflow-y: auto;">
            </div>
        </div>
        <div id="detail-stats-section" style="display: none; margin-top: 16px;">
            <div class="meta-label">📊 自駕統計與行程儀表板</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 8px; margin-bottom: 12px;">
                <div class="glass-panel" style="padding: 10px; border-radius: 8px; text-align: center; background: rgba(56, 189, 248, 0.05); border: 1px solid rgba(56, 189, 248, 0.15);">
                    <div style="font-size: 11px; color: #38bdf8; font-weight: 700; text-transform: uppercase;">預估自駕里程</div>
                    <div id="stats-mileage" style="font-size: 18px; font-weight: 700; color: #fff; margin-top: 4px;">790 km</div>
                </div>
                <div class="glass-panel" style="padding: 10px; border-radius: 8px; text-align: center; background: rgba(251, 146, 60, 0.05); border: 1px solid rgba(251, 146, 60, 0.15);">
                    <div style="font-size: 11px; color: #fb923c; font-weight: 700; text-transform: uppercase;">預估行車時間</div>
                    <div id="stats-duration" style="font-size: 18px; font-weight: 700; color: #fff; margin-top: 4px;">11.3 hrs</div>
                </div>
            </div>
            <div id="stats-chart-container"></div>
        </div>
    </div>

    <script>
        const graphData = {graph_data_placeholder};
        
        const width = window.innerWidth;
        const height = window.innerHeight;
        
        const svg = d3.select("#graph-container")
            .append("svg")
            .attr("width", "100%")
            .attr("height", "100%")
            .call(d3.zoom().on("zoom", function (event) {
                g.attr("transform", event.transform);
            }))
            .append("g");
            
        const g = svg.append("g");
        
        const colors = {
            concept: "var(--color-concept)",
            summary: "var(--color-summary)",
            entity: "var(--color-entity)",
            index: "var(--color-special)",
            overview: "var(--color-special)",
            log: "var(--color-special)",
            default: "var(--color-default)"
        };
        
        let selectedNode = null;
        
        const simulation = d3.forceSimulation(graphData.nodes)
            .force("link", d3.forceLink(graphData.links).id(d => d.id).distance(120))
            .force("charge", d3.forceManyBody().strength(-200))
            .force("center", d3.forceCenter(width / 2, height / 2))
            .force("collision", d3.forceCollide().radius(30));
            
        const link = g.append("g")
            .selectAll("line")
            .data(graphData.links)
            .join("line")
            .attr("class", "link")
            .attr("stroke", "#94a3b8");
            
        const node = g.append("g")
            .selectAll("g")
            .data(graphData.nodes)
            .join("g")
            .attr("class", "node-group")
            .call(d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended));
                
        node.append("circle")
            .attr("r", d => (d.type === 'index' || d.type === 'overview') ? 14 : 9)
            .attr("fill", d => colors[d.type] || colors.default)
            .attr("stroke", d => "#0b0f19")
            .attr("stroke-width", 2)
            .attr("class", "node")
            .attr("id", d => `node-${d.id}`);
            
        node.append("text")
            .attr("dx", d => (d.type === 'index' || d.type === 'overview') ? 18 : 12)
            .attr("dy", 4)
            .attr("class", "node-label")
            .text(d => d.title);
            
        simulation.on("tick", () => {
            link
                .attr("x1", d => d.source.x)
                .attr("y1", d => d.source.y)
                .attr("x2", d => d.target.x)
                .attr("y2", d => d.target.y);
                
            node
                .attr("transform", d => `translate(${d.x}, ${d.y})`);
        });
        
        node.on("mouseover", function(event, d) {
            if (selectedNode) return;
            link.style("stroke-opacity", l => (l.source.id === d.id || l.target.id === d.id) ? 0.7 : 0.04)
                .style("stroke", l => (l.source.id === d.id || l.target.id === d.id) ? (colors[d.type] || colors.default) : "#94a3b8")
                .style("stroke-width", l => (l.source.id === d.id || l.target.id === d.id) ? 2.5 : 1.5);
                
            const neighbors = getNeighbors(d);
            d3.selectAll(".node").style("opacity", n => (n.id === d.id || neighbors.includes(n.id)) ? 1 : 0.2);
            d3.selectAll(".node-label").style("opacity", n => (n.id === d.id || neighbors.includes(n.id)) ? 1 : 0.1);
        });
        
        node.on("mouseout", function() {
            if (selectedNode) return;
            link.style("stroke-opacity", 0.15)
                .style("stroke", "#94a3b8")
                .style("stroke-width", 1.5);
            d3.selectAll(".node").style("opacity", 1);
            d3.selectAll(".node-label").style("opacity", 1);
        });
        
        node.on("click", function(event, d) {
            event.stopPropagation();
            showDetail(d);
        });
        
        node.on("dblclick", function(event, d) {
            event.stopPropagation();
            if (d.abs_path) {
                const obsidianUri = `obsidian://open?path=${encodeURIComponent(d.abs_path)}`;
                window.location.href = obsidianUri;
            }
        });
        
        function getNeighbors(node) {
            return graphData.links
                .filter(l => l.source.id === node.id || l.target.id === node.id)
                .map(l => l.source.id === node.id ? l.target.id : l.source.id);
        }
        
        function showDetail(d) {
            selectedNode = d;
            
            link.style("stroke-opacity", l => (l.source.id === d.id || l.target.id === d.id) ? 0.8 : 0.03)
                .style("stroke", l => (l.source.id === d.id || l.target.id === d.id) ? (colors[d.type] || colors.default) : "#94a3b8")
                .style("stroke-width", l => (l.source.id === d.id || l.target.id === d.id) ? 3 : 1.5);
                
            const neighbors = getNeighbors(d);
            d3.selectAll(".node").style("opacity", n => (n.id === d.id || neighbors.includes(n.id)) ? 1 : 0.15);
            d3.selectAll(".node-label").style("opacity", n => (n.id === d.id || neighbors.includes(n.id)) ? 1 : 0.1);
            
            const panel = document.getElementById("detail-panel");
            panel.classList.add("active");
            
            document.getElementById("detail-title").innerText = d.title;
            document.getElementById("detail-type").innerText = d.type.toUpperCase();
            document.getElementById("detail-type").style.color = colors[d.type] || colors.default;
            document.getElementById("detail-timestamp").innerText = d.timestamp || "N/A";
            
            const tagsContainer = document.getElementById("detail-tags");
            tagsContainer.innerHTML = "";
            if (d.tags && d.tags.length > 0) {
                d.tags.forEach(t => {
                    const pill = document.createElement("span");
                    pill.className = "tag-pill";
                    pill.innerText = t;
                    tagsContainer.appendChild(pill);
                });
            } else {
                tagsContainer.innerHTML = "<span style='color:var(--text-secondary); font-size:12px;'>無標籤</span>";
            }
            
            const connList = document.getElementById("detail-connections");
            connList.innerHTML = "";
            const neighborNodes = graphData.nodes.filter(n => neighbors.includes(n.id));
            if (neighborNodes.length > 0) {
                neighborNodes.forEach(n => {
                    const item = document.createElement("div");
                    item.className = "connection-item";
                    item.innerText = n.title;
                    item.onclick = () => {
                        showDetail(n);
                        zoomToNode(n);
                    };
                    connList.appendChild(item);
                });
            } else {
                connList.innerHTML = "<span style='color:var(--text-secondary); font-size:12px;'>無連結</span>";
            }
            
            const mapsSection = document.getElementById("detail-maps-section");
            const mapsContainer = document.getElementById("detail-maps");
            mapsContainer.innerHTML = "";
            if (d.maps_links && d.maps_links.length > 0) {
                mapsSection.style.display = "block";
                d.maps_links.forEach(link => {
                    const item = document.createElement("a");
                    item.className = "connection-item";
                    item.style.display = "block";
                    item.style.textDecoration = "none";
                    item.target = "_blank";
                    item.href = link.url;
                    item.innerText = link.text;
                    mapsContainer.appendChild(item);
                });
            } else {
                mapsSection.style.display = "none";
            }
            
            const timelineSection = document.getElementById("detail-timeline-section");
            const timelineContainer = document.getElementById("detail-timeline");
            timelineContainer.innerHTML = "";
            if (d.timeline && d.timeline.length > 0) {
                timelineSection.style.display = "block";
                d.timeline.forEach(item => {
                    const tItem = document.createElement("div");
                    tItem.className = "timeline-item";
                    
                    const tTime = document.createElement("div");
                    tTime.className = "timeline-time";
                    tTime.innerText = item.time;
                    
                    const tDesc = document.createElement("div");
                    tDesc.className = "timeline-desc";
                    tDesc.innerText = item.desc;
                    
                    tItem.appendChild(tTime);
                    tItem.appendChild(tDesc);
                    timelineContainer.appendChild(tItem);
                });
            } else {
                timelineSection.style.display = "none";
            }
            
            const statsSection = document.getElementById("detail-stats-section");
            const statsContainer = document.getElementById("stats-chart-container");
            statsContainer.innerHTML = "";
            if (d.travel_stats) {
                statsSection.style.display = "block";
                document.getElementById("stats-mileage").innerText = `${d.travel_stats.total_mileage_km} km`;
                document.getElementById("stats-duration").innerText = `${d.travel_stats.total_duration_hours} hrs`;
                
                const daily = d.travel_stats.daily_stats;
                const maxDuration = Math.max(...daily.map(day => day.duration));
                
                let svgHtml = `<svg width="100%" height="230" style="background: rgba(255,255,255,0.02); border: 1px solid var(--border-color); border-radius: 8px; padding: 10px; box-sizing: border-box;">`;
                
                daily.forEach((day, i) => {
                    const y = i * 17 + 8;
                    const barWidth = maxDuration > 0 ? (day.duration / maxDuration) * 120 : 0;
                    
                    svgHtml += `
                        <text x="5" y="${y + 11}" fill="#94a3b8" font-size="10" font-family="'Outfit', sans-serif">${day.day.split(" ")[0]}</text>
                        <rect x="50" y="${y + 2}" width="${barWidth}" height="9" rx="2" fill="url(#barGradient)" />
                        <text x="${50 + barWidth + 5}" y="${y + 10}" fill="#cbd5e1" font-size="9" font-family="'Outfit', sans-serif">${day.duration}m (${day.mileage}km)</text>
                    `;
                });
                
                svgHtml += `
                    <defs>
                        <linearGradient id="barGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                            <stop offset="0%" stop-color="#38bdf8" />
                            <stop offset="100%" stop-color="#fb923c" />
                        </linearGradient>
                    </defs>
                `;
                svgHtml += `</svg>`;
                statsContainer.innerHTML = svgHtml;
            } else {
                statsSection.style.display = "none";
            }
        }
        
        function zoomToNode(n) {
            const transform = d3.zoomIdentity
                .translate(width / 2 - n.x * 1.3, height / 2 - n.y * 1.3)
                .scale(1.3);
            d3.select("svg")
                .transition()
                .duration(750)
                .call(d3.zoom().transform, transform);
        }
        
        function closeDetail() {
            selectedNode = null;
            document.getElementById("detail-panel").classList.remove("active");
            link.style("stroke-opacity", 0.15)
                .style("stroke", "#94a3b8")
                .style("stroke-width", 1.5);
            d3.selectAll(".node").style("opacity", 1);
            d3.selectAll(".node-label").style("opacity", 1);
        }
        
        function filterGraph(query) {
            if (!query) {
                closeDetail();
                return;
            }
            const q = query.toLowerCase();
            const match = graphData.nodes.find(n => n.title.toLowerCase().includes(q) || n.id.toLowerCase().includes(q));
            if (match) {
                showDetail(match);
                zoomToNode(match);
            }
        }
        
        function dragstarted(event, d) {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }
        
        function dragged(event, d) {
            d.fx = event.x;
            d.fy = event.y;
        }
        
        function dragended(event, d) {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }
        
        window.addEventListener('resize', () => {
            const w = window.innerWidth;
            const h = window.innerHeight;
            d3.select("svg").attr("width", w).attr("height", h);
            simulation.force("center", d3.forceCenter(w / 2, h / 2)).alpha(0.3).restart();
        });
    </script>
</body>
</html>"""
    
    html_content = html_template.replace("{graph_data_placeholder}", data_js)
    
    output_path = "wiki_graph.html"
    try:
         with open(output_path, "w", encoding="utf-8") as f:
             f.write(html_content)
         print(f"Interactive graph generated successfully at: {output_path}")
         print("You can open this file in your web browser to explore the knowledge graph!")
         log_message("query", f"Generated interactive knowledge graph visualization at {output_path}")
         if open_browser:
             import webbrowser
             abs_path = os.path.abspath(output_path)
             webbrowser.open(f"file://{abs_path}")
             print(f"Opened {output_path} in your default browser.")
    except Exception as e:
         print(f"Error writing graph HTML: {e}")

def render_slides(open_browser=False, theme_override=None):
    import subprocess
    wiki_dir = "wiki"
    if not os.path.isdir(wiki_dir):
        print(f"Error: {wiki_dir} directory not found.")
        return
        
    slide_files = []
    for root, _, files in os.walk(wiki_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
            path = os.path.join(root, file)
            if is_observation_operational_path(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                if "marp: true" in content:
                    slide_files.append(path)
            except Exception:
                continue
                
    if not slide_files:
        print("No Marp slide files (containing 'marp: true') found in wiki/.")
        return
        
    print(f"Found {len(slide_files)} slide files. Compiling to HTML and PDF...")
    
    for path in slide_files:
        base_name = os.path.splitext(path)[0]
        html_out = f"{base_name}.html"
        pdf_out = f"{base_name}.pdf"
        
        # 1. Render to HTML
        print(f"Rendering {path} to HTML...")
        cmd_html = ["npx", "-y", "@marp-team/marp-cli", "--no-stdin"]
        if theme_override:
            cmd_html.extend(["--theme", theme_override])
        cmd_html.extend([path, "-o", html_out])
        try:
            subprocess.run(cmd_html, check=True)
            print(f"🟢 Successfully generated HTML: {html_out}")
        except Exception as e:
            print(f"❌ Error rendering to HTML for {path}: {e}")
            
        # 2. Render to PDF
        print(f"Rendering {path} to PDF...")
        cmd_pdf = ["npx", "-y", "@marp-team/marp-cli", "--no-stdin"]
        if theme_override:
            cmd_pdf.extend(["--theme", theme_override])
        cmd_pdf.extend([path, "--pdf", "-o", pdf_out])
        try:
            subprocess.run(cmd_pdf, check=True)
            print(f"🟢 Successfully generated PDF: {pdf_out}")
        except Exception as e:
            print(f"❌ Error rendering to PDF for {path}: {e}")
            
    if open_browser and slide_files:
        import webbrowser
        first_html = f"{os.path.splitext(slide_files[0])[0]}.html"
        if os.path.exists(first_html):
            abs_path = os.path.abspath(first_html)
            webbrowser.open(f"file://{abs_path}")
            print(f"Opened {first_html} in browser.")

def check_and_log_deletions():
    import subprocess
    from datetime import datetime
    try:
        res = subprocess.run(["git", "diff", "--name-status", "HEAD"], capture_output=True, text=True)
        deleted_files = []
        for line in res.stdout.splitlines():
            if line.startswith("D\t") or line.startswith("D "):
                filepath = line[2:].strip()
                if filepath.startswith("wiki/"):
                    deleted_files.append(filepath)
                    
        if deleted_files:
            print("🔍 Detected manual removal of the following wiki files:")
            for path in deleted_files:
                print(f"  - {path}")
            
            z_log_path = "wiki/z_log.md"
            if os.path.exists(z_log_path):
                with open(z_log_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                today_str = datetime.now().strftime("%Y-%m-%d")
                log_entries = []
                for path in deleted_files:
                    entry_header = f"## [{today_str}] delete | Detected manual removal of {path}"
                    if entry_header not in content:
                        log_entries.append(entry_header)
                        
                if log_entries:
                    with open(z_log_path, 'a', encoding='utf-8') as f:
                        f.write("\n" + "\n\n".join(log_entries) + "\n")
                    print("📝 Logged deletions in wiki/z_log.md")
    except Exception as e:
        print(f"⚠️ Warning during deletion detection: {e}")

def publish_pipeline(commit_message=None, theme_override=None):
    import subprocess
    print("🚀 Starting Automated Wiki Publish Pipeline...")
    print("-" * 50)
    
    # 0. Check and Log Deletions
    check_and_log_deletions()

    # 1. Run Lint
    print("📋 Step 1/4: Running Lint & Health Checks...")
    results = perform_lint_checks()
    if results is None:
        print("❌ Pipeline aborted: wiki directory not found.")
        sys.exit(1)
        
    errors_schema, broken_links, orphans, drift_warn, outbound_warnings = results
    is_clean = not (errors_schema or broken_links or orphans or drift_warn)
    
    if outbound_warnings:
        print("⚠️ Outbound Link Warnings (Non-blocking):")
        for path in outbound_warnings:
            print(f"  - `{path}` has 0 outbound links")
            
    if not is_clean:
        print("❌ Pipeline aborted: Lint checks failed. Please fix the following errors first:")
        if errors_schema:
            for path, err in errors_schema:
                print(f"  - Schema Error in {path}: {err}")
        if broken_links:
            for path, err in broken_links:
                print(f"  - Broken Link in {path}: {err}")
        if orphans:
            for path in orphans:
                print(f"  - Orphan Page: {path}")
        if drift_warn:
            print(f"  - Drift Warning: {drift_warn}")
        sys.exit(1)
        
    print("🟢 Step 1 Passed: Wiki health check is clean.")
    print("-" * 50)
    
    # 2. Render Slides
    print("🎨 Step 2/4: Compiling Marp Slide Decks...")
    render_slides(open_browser=False, theme_override=theme_override)
    print("🟢 Step 2 Passed: Slide decks compiled successfully.")
    print("-" * 50)
    
    # 3. Generate Graph
    print("🕸️ Step 3/4: Regenerating Knowledge Graph...")
    generate_graph_web(open_browser=False)
    print("🟢 Step 3 Passed: Knowledge graph updated.")
    print("-" * 50)
    
    # 4. Git Commit
    print("💾 Step 4/4: Committing Changes to Git...")
    try:
        subprocess.run(["git", "add", "-A"], check=True)
        
        if not commit_message:
            today_str = datetime.now().strftime("%Y-%m-%d %H:%M")
            commit_message = f"chore(wiki): automated pipeline publish - {today_str}"
            
        status_res = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status_res.stdout.strip():
            print("🟢 No changes to commit. Wiki is already up-to-date!")
            print("-" * 50)
            print("🎉 Wiki Publish Pipeline finished successfully!")
            return
            
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        print(f"🟢 Successfully committed changes with message: '{commit_message}'")
    except Exception as e:
        print(f"❌ Error committing to Git: {e}")
        sys.exit(1)
        
    print("-" * 50)
    print("🎉 Wiki Publish Pipeline finished successfully!")

def find_pending_files():
    """List uncovered raw sources that still need a triage or compile decision."""
    if not os.path.isdir("raw"):
        print("Error: raw/ is not a directory.")
        return
    referenced_sources = collect_source_references()
    triage_records, triage_errors = parse_source_triage()
    eligible = {"untriaged", "compile"}
    unreferenced = [
        path for path in iter_raw_files()
        if path not in referenced_sources and source_triage(path, triage_records) in eligible
    ]

    print("# 待處理原始檔案列表 (Pending Raw Files)")
    print("-" * 50)
    if triage_errors:
        print("Triage manifest errors:")
        for error in triage_errors:
            print(f"- {error}")
        print("-" * 50)
        return
    if not unreferenced:
        print("🟢 沒有需要 triage 或編譯的 uncovered raw 來源。")
    else:
        groups = {status: [] for status in sorted(eligible)}
        for path in sorted(unreferenced, key=str.casefold):
            groups[source_triage(path, triage_records)].append(path)
        print(f"共發現 {len(unreferenced)} 個需要處理的 uncovered 來源：")
        for status in ("untriaged", "compile"):
            if not groups[status]:
                continue
            print(f"\n## {status}")
            for idx, path in enumerate(groups[status], 1):
                print(f"  {idx}. `{path}` ({source_kind(path)})")
    print("-" * 50)

def heal_links(old_concept, new_concept):
    import os
    import re
    wiki_dir = "wiki"
    
    old_escaped = re.escape(old_concept)
    pattern = re.compile(r'\[\[\s*' + old_escaped + r'\s*((?:#[^\]|]+)?(?:\|[^\]]+)?)\]\]')
    
    count = 0
    for root, dirs, files in os.walk(wiki_dir):
        for file in files:
            if not file.endswith(".md"):
                continue
            path = os.path.join(root, file)
            if is_observation_operational_path(path):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
                    
                new_content, subs = pattern.subn(r'[[' + new_concept.replace('\\', r'\\') + r'\1]]', content)
                
                if subs > 0:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(new_content)
                    print(f"✅ Updated {subs} link(s) in {path}")
                    count += subs
            except Exception as e:
                print(f"❌ Error updating links in {path}: {e}")
                
    if count == 0:
        print(f"No links found pointing to '{old_concept}'.")
    else:
        print(f"🎉 Successfully healed {count} link(s) globally.")

def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "observe":
        try:
            observe_args = build_observation_parser().parse_args(arguments[1:])
            return run_observation_command(observe_args)
        except ObservationDomainError as error:
            return fail_observation(error)

    parser = argparse.ArgumentParser(description="LLM Wiki CLI helper tool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # log subparser
    log_parser = subparsers.add_parser("log", help="Append an entry to log.md")
    log_parser.add_argument("action", help="The action type (e.g., ingest, query)")
    log_parser.add_argument("details", help="Log details/description")

    # search subparser
    search_parser = subparsers.add_parser("search", help="Search the wiki")
    search_parser.add_argument("query", help="Search query string")

    # lint subparser
    subparsers.add_parser("lint", help="Run health check on wiki pages")

    # pending subparser
    subparsers.add_parser("pending", help="List all raw files that are not yet ingested in the wiki")

    # sources subparser
    sources_parser = subparsers.add_parser("sources", help="Rebuild or validate wiki/_sources.md from raw files and wiki frontmatter")
    sources_parser.add_argument("--check", action="store_true", help="Fail if the source catalog is stale or source triage is invalid")

    # tasks subparser
    tasks_parser = subparsers.add_parser("tasks", help="Rebuild or validate wiki/_todo_list.md from canonical task records")
    tasks_parser.add_argument("--check", action="store_true", help="Fail if the todo dashboard is stale")

    subparsers.add_parser(
        "observe",
        add_help=False,
        help="Record and report validated workflow observations",
    )

    inbox_parser = subparsers.add_parser("inbox", help="Rebuild, validate, or add local capture inbox records")
    inbox_parser.add_argument("--check", action="store_true", help="Fail if the inbox dashboard is stale")
    inbox_parser.add_argument("add", nargs="?", help="Use `add` with --text to create a capture record")
    inbox_parser.add_argument("--text", help="Capture text when using `inbox add`")
    inbox_parser.add_argument("--source", default="manual", help="Capture source, e.g. manual, siri, reminders")
    inbox_parser.add_argument("--external-ref", help="Stable external identifier used to prevent duplicate imports")

    # fileback subparser
    fileback_parser = subparsers.add_parser("fileback", help="File an explicit query result into the wiki")
    fileback_parser.add_argument("--title", required=True, help="Title of the new wiki page")
    fileback_parser.add_argument("--tags", required=True, help="Comma-separated tags for the page")
    fileback_parser.add_argument("--type", default="query", choices=["concept", "summary", "entity", "query"], help="YAML frontmatter type")
    fileback_parser.add_argument("--from-file", help="Read the result body from a UTF-8 Markdown/text file")
    fileback_parser.add_argument("--stdin", action="store_true", help="Read the result body from standard input")
    fileback_parser.add_argument("--source", action="append", default=[], help="Source path or conversation label; repeatable")

    # graph subparser
    graph_parser = subparsers.add_parser("graph", help="Generate interactive HTML knowledge graph")
    graph_parser.add_argument("--open", action="store_true", help="Open the generated graph in the default browser")

    # render-slides subparser
    slides_parser = subparsers.add_parser("render-slides", help="Render all Marp markdown slides to HTML and PDF")
    slides_parser.add_argument("--open", action="store_true", help="Open the first rendered HTML slide in the browser")
    slides_parser.add_argument("--theme", choices=["gaia", "uncover", "default"], help="Override Marp theme")

    # publish subparser
    publish_parser = subparsers.add_parser("publish", help="Run automated publish pipeline (lint -> render-slides -> graph -> git commit)")
    publish_parser.add_argument("--message", help="Custom git commit message")
    publish_parser.add_argument("--theme", choices=["gaia", "uncover", "default"], help="Override Marp theme for slide decks")

    # heal subparser
    heal_parser = subparsers.add_parser("heal", help="Globally rewrite all Wikilinks from an old concept to a new concept")
    heal_parser.add_argument("old_concept", help="The exact old concept name to search for")
    heal_parser.add_argument("new_concept", help="The exact new concept name to replace with")

    args = parser.parse_args(arguments)

    if args.command == "log":
        log_message(args.action, args.details)
    elif args.command == "search":
        search_wiki(args.query)
    elif args.command == "lint":
        run_lint()
    elif args.command == "pending":
        find_pending_files()
    elif args.command == "sources":
        if not write_source_catalog(check=args.check):
            sys.exit(1)
    elif args.command == "tasks":
        if not write_todo_dashboard(check=args.check):
            sys.exit(1)
    elif args.command == "inbox":
        if args.add:
            if args.add != "add" or not args.text:
                print("Use: python3 wiki_cli.py inbox add --text \"...\" [--source manual] [--external-ref ID]")
                sys.exit(1)
            if not add_inbox_capture(args.text, args.source, args.external_ref):
                sys.exit(1)
        elif not write_inbox_dashboard(check=args.check):
            sys.exit(1)
    elif args.command == "fileback":
        if args.from_file:
            try:
                with open(args.from_file, "r", encoding="utf-8") as file_obj:
                    fileback_content(args.title, args.tags, args.type, file_obj.read(), args.source)
            except OSError as error:
                print(f"Error reading --from-file: {error}")
        elif args.stdin:
            fileback_content(args.title, args.tags, args.type, sys.stdin.read(), args.source)
        else:
            print("Error: Transcript-based fileback is deprecated and no longer supported. Please use --from-file or --stdin.")
            sys.exit(1)
    elif args.command == "graph":
        generate_graph_web(args.open)
    elif args.command == "render-slides":
        render_slides(args.open, args.theme)
    elif args.command == "publish":
        publish_pipeline(args.message, args.theme)
    elif args.command == "heal":
        heal_links(args.old_concept, args.new_concept)
    return 0

if __name__ == "__main__":
    sys.exit(main())
