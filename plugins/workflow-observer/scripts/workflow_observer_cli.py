#!/usr/bin/env python3
"""Adapter-neutral command line interface for Workflow Observatory."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Sequence

from store_config import (
    AdapterSemantics,
    ConfigError,
    StoreConfig,
    adapter_semantics,
    load_store_config,
)
import wiki_observations
from episode_schema import EpisodeSchemaError, parse_v2_supplement
from policy_artifacts import PolicyError, load_policy_set
from canonical_json import CanonicalizationError, canonicalize
from snapshot_input import (
    SNAPSHOT_ANALYZER_FILES,
    SnapshotInputError,
    SnapshotQuery,
    acquire_snapshot_input,
    canonical_interval,
    validate_snapshot_query,
)
from snapshot_store import create_learning_snapshot
from wiki_observations import ObservationError, ObservationPaths


_RUN_ID_PATTERN = r"obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}"
_RUN_ID_RE = re.compile(rf"^{_RUN_ID_PATTERN}$")
_LOCK_RE = re.compile(rf"^{_RUN_ID_PATTERN}\.lock$")
_RECORD_RE = re.compile(rf"^{_RUN_ID_PATTERN}\.md$")


class CliArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ObservationError("validation", message)


def _report_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ObservationError(
            "validation", "report dates must use YYYY-MM-DD"
        ) from error


def _snapshot_date(value: str) -> date:
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        raise ObservationError(
            "validation", "snapshot dates must use YYYY-MM-DD"
        )
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ObservationError(
            "validation", "snapshot dates must use YYYY-MM-DD"
        ) from error


def build_parser() -> argparse.ArgumentParser:
    parser = CliArgumentParser(description="Record local workflow observations")
    subparsers = parser.add_subparsers(
        dest="command", required=True, parser_class=CliArgumentParser
    )

    start = subparsers.add_parser("start")
    start.add_argument("--title", required=True)
    start.add_argument("--subject-root", required=True)
    start.add_argument("--project")
    start.add_argument("--agent-surface", required=True, choices=("codex",))
    start.add_argument("--start-mode", required=True, choices=("planned", "late"))
    start.add_argument("--task-type", required=True, choices=sorted(wiki_observations.TAXONOMY))
    start.add_argument(
        "--workflow-variant",
        required=True,
        choices=sorted(
            variant
            for variants in wiki_observations.TAXONOMY.values()
            for variant in variants
        ),
    )
    start.add_argument("--scope-from-file", required=True)
    start.add_argument("--episode-schema-version", type=int, choices=(1, 2), default=1)
    start.add_argument("--workflow-generation")
    start.add_argument("--task")
    start.add_argument("--source", action="append", default=[])

    finish = subparsers.add_parser("finish")
    finish.add_argument("run_id")
    finish.add_argument("--status", required=True, choices=sorted(wiki_observations.FINAL_STATUSES))
    finish.add_argument("--from-file", required=True)
    finish.add_argument("--episode-from-file")
    finish.add_argument("--superseded-by")

    invalidate = subparsers.add_parser("invalidate")
    invalidate.add_argument("run_id")
    invalidate.add_argument("--reason", required=True)

    report = subparsers.add_parser("report")
    report.add_argument("--project")
    report.add_argument("--workspace")
    report.add_argument("--workspace-id")
    report.add_argument("--task-type")
    report.add_argument("--status")
    report.add_argument("--since", type=_report_date)
    report.add_argument("--until", type=_report_date)

    _add_snapshot_query_arguments(subparsers.add_parser("snapshot-input"))
    _add_snapshot_query_arguments(subparsers.add_parser("snapshot"))

    subparsers.add_parser("validate")
    subparsers.add_parser("integrity")
    return parser


def _add_snapshot_query_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--since", required=True, type=_snapshot_date)
    parser.add_argument("--until", required=True, type=_snapshot_date)
    parser.add_argument("--timezone", required=True)
    parser.add_argument("--project")
    parser.add_argument("--workspace")
    parser.add_argument("--workspace-id")
    parser.add_argument("--task-type")
    parser.add_argument("--as-of")


def _read_private_payload(path_value: str, label: str) -> str:
    if not isinstance(path_value, str) or not path_value:
        raise ObservationError("validation", f"{label} path is required")
    path = Path(path_value)
    try:
        before = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise ObservationError("io", f"could not inspect {label}: {error}") from error
    if stat.S_ISLNK(before.st_mode):
        raise ObservationError("validation", f"{label} must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise ObservationError("validation", f"{label} must be a regular file")
    if stat.S_IMODE(before.st_mode) != 0o600:
        raise ObservationError("validation", f"{label} must have mode 0600")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        try:
            current = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as missing_error:
            raise ObservationError("validation", f"{label} changed while opening") from missing_error
        except OSError as inspect_error:
            raise ObservationError("io", f"could not inspect {label}: {inspect_error}") from inspect_error
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise ObservationError("validation", f"{label} changed while opening") from error
        raise ObservationError("io", f"could not open {label}: {error}") from error
    try:
        try:
            opened = os.fstat(descriptor)
            after = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as error:
            raise ObservationError("validation", f"{label} changed while opening") from error
        except OSError as error:
            raise ObservationError("io", f"could not inspect opened {label}: {error}") from error
        identities = {
            (before.st_dev, before.st_ino),
            (opened.st_dev, opened.st_ino),
            (after.st_dev, after.st_ino),
        }
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(after.st_mode)
            or len(identities) != 1
        ):
            raise ObservationError("validation", f"{label} changed while opening")
        if stat.S_IMODE(opened.st_mode) != 0o600:
            raise ObservationError("validation", f"{label} must have mode 0600")
        try:
            with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
                descriptor = -1
                return stream.read()
        except UnicodeError as error:
            raise ObservationError("validation", f"{label} must be UTF-8 text") from error
        except OSError as error:
            raise ObservationError("io", f"could not read {label}: {error}") from error
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _directory_open_flags() -> int:
    return os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


def _read_draft_schema(paths: ObservationPaths, run_id: str) -> int:
    """Securely classify a complete draft before selecting its lifecycle core."""

    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise ObservationError("validation", "run_id has an invalid format")
    secure_paths = wiki_observations._canonical_observation_paths(paths)
    directory_fd = wiki_observations._open_observation_directory(
        secure_paths.observations
    )
    descriptor = -1
    name = f"{run_id}.md"
    try:
        wiki_observations._assert_directory_identity(
            directory_fd, secure_paths.observations
        )
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError as error:
            raise ObservationError(
                "state", f"observation {run_id} does not exist"
            ) from error
        except OSError as error:
            raise ObservationError("io", str(error)) from error
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise ObservationError(
                "validation", "observation record must be a regular file"
            )
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except OSError as error:
            raise ObservationError(
                "validation", "observation record changed while opening"
            ) from error
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise ObservationError(
                "validation", "observation record changed while opening"
            )

        frontmatter = bytearray()
        delimiter = b"\n---\n"
        while not frontmatter.endswith(delimiter):
            if len(frontmatter) >= 64 * 1024:
                raise ObservationError(
                    "validation", "observation frontmatter is too large"
                )
            chunk = os.read(descriptor, 1)
            if not chunk:
                raise ObservationError(
                    "validation", "record has malformed frontmatter"
                )
            frontmatter.extend(chunk)

        content = bytearray(frontmatter)
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            content.extend(chunk)

        after = os.fstat(descriptor)
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        wiki_observations._assert_directory_identity(
            directory_fd, secure_paths.observations
        )
        if (
            not stat.S_ISREG(after.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or len({
                (before.st_dev, before.st_ino),
                (opened.st_dev, opened.st_ino),
                (after.st_dev, after.st_ino),
                (current.st_dev, current.st_ino),
            }) != 1
        ):
            raise ObservationError(
                "validation", "observation record changed while reading schema"
            )
        try:
            text = bytes(content).decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ObservationError(
                "validation", "observation record must be UTF-8 text"
            ) from error
        from artifact_schema import load_artifact_policy_set, parse_markdown_envelope

        envelope = parse_markdown_envelope(
            text,
            expected_human_type="observation",
            policies=load_artifact_policy_set(
                Path(__file__).resolve().parents[1] / "policies"
            ),
        )
        metadata = envelope.metadata
        if metadata.get("run_id") != run_id:
            raise ObservationError(
                "validation", "record run_id does not match filename"
            )
        if metadata.get("status") != "draft":
            raise ObservationError("state", f"{run_id} is already final")
        return envelope.artifact.schema_version
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass
        try:
            os.close(directory_fd)
        except OSError:
            pass


def _walk_portable_directory(
    path: Path, *, create: bool, private_target: bool = False
) -> bool:
    """Walk an absolute directory without following any symlink component."""

    candidate = Path(path)
    if not candidate.is_absolute():
        raise ObservationError("validation", "portable store root must be absolute")
    components = candidate.parts[1:]
    if any(component in {".", ".."} for component in components):
        raise ObservationError("validation", "portable store root must be normalized")
    descriptor = -1
    try:
        try:
            descriptor = os.open(candidate.anchor, _directory_open_flags())
        except OSError as error:
            raise ObservationError("io", f"could not open filesystem root: {error}") from error
        for index, component in enumerate(components):
            created = False
            try:
                expected = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    return False
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    created = True
                    expected = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                except FileExistsError:
                    expected = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                except OSError as error:
                    raise ObservationError(
                        "io", f"could not create portable store directory: {error}"
                    ) from error
            except OSError as error:
                raise ObservationError(
                    "io", f"could not inspect portable store path: {error}"
                ) from error
            if stat.S_ISLNK(expected.st_mode):
                raise ObservationError(
                    "validation", "portable store path contains a symlink"
                )
            if not stat.S_ISDIR(expected.st_mode):
                raise ObservationError(
                    "validation", "portable store path component must be a directory"
                )
            try:
                child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
            except OSError as error:
                try:
                    current = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                except OSError as inspect_error:
                    raise ObservationError(
                        "io", f"could not inspect portable store path: {inspect_error}"
                    ) from inspect_error
                if (
                    stat.S_ISLNK(current.st_mode)
                    or not stat.S_ISDIR(current.st_mode)
                    or (current.st_dev, current.st_ino)
                    != (expected.st_dev, expected.st_ino)
                ):
                    raise ObservationError(
                        "validation", "portable store path changed while opening"
                    ) from error
                raise ObservationError(
                    "io", f"could not open portable store path: {error}"
                ) from error
            try:
                opened = os.fstat(child)
                current = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or not stat.S_ISDIR(current.st_mode)
                    or len({
                        (expected.st_dev, expected.st_ino),
                        (opened.st_dev, opened.st_ino),
                        (current.st_dev, current.st_ino),
                    }) != 1
                ):
                    raise ObservationError(
                        "validation", "portable store path changed while opening"
                    )
                if created or (private_target and index == len(components) - 1):
                    os.fchmod(child, 0o700)
            except ObservationError:
                os.close(child)
                raise
            except OSError as error:
                os.close(child)
                raise ObservationError(
                    "io", f"could not secure portable store directory: {error}"
                ) from error
            os.close(descriptor)
            descriptor = child
        return True
    finally:
        if descriptor >= 0:
            try:
                os.close(descriptor)
            except OSError:
                pass


def initialize_portable_root(root: Path) -> ObservationPaths:
    root = Path(root)
    for path in (
        root,
        root / "wiki",
        root / "wiki" / "observations",
        root / "wiki" / "observations" / ".locks",
        root / "wiki" / "observations" / "invalidations",
    ):
        _walk_portable_directory(path, create=True, private_target=True)
    return ObservationPaths.from_root(root)


def _portable_paths(config: StoreConfig, *, missing_ok: bool) -> ObservationPaths | None:
    if not _walk_portable_directory(config.root, create=False):
        if missing_ok:
            return None
        raise ObservationError("state", "portable observation store does not exist")
    return ObservationPaths.from_root(config.root)


def _start_request(args: argparse.Namespace) -> tuple[wiki_observations.StartRequest, wiki_observations.ScopePayload]:
    provenance = wiki_observations.derive_provenance(Path(args.subject_root), args.project)
    scope = wiki_observations.parse_scope_payload(
        _read_private_payload(args.scope_from_file, "Scope payload")
    )
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
        task_ref=f"[[{args.task}]]" if args.task is not None else None,
        sources=tuple(args.source),
        episode_schema_version=args.episode_schema_version,
        workflow_generation=args.workflow_generation,
    )
    return request, scope


def _filters(args: argparse.Namespace) -> wiki_observations.ReportFilters:
    return wiki_observations.ReportFilters(
        project=args.project,
        workspace=args.workspace,
        workspace_id=args.workspace_id,
        task_type=args.task_type,
        status=args.status,
        since=args.since,
        until=args.until,
    )


def _empty_report(filters: wiki_observations.ReportFilters) -> str:
    return wiki_observations.render_observation_report(
        [], set(), filters, datetime.now().astimezone()
    )


def _snapshot_query(args: argparse.Namespace) -> SnapshotQuery:
    interval = canonical_interval(args.since, args.until, args.timezone)
    query = SnapshotQuery(
        interval=interval,
        lifecycle_as_of=(
            args.as_of
            if args.as_of is not None
            else interval["until_exclusive"]
        ),
        project=args.project,
        workspace=args.workspace,
        workspace_id=args.workspace_id,
        task_type=args.task_type,
    )
    validate_snapshot_query(query)
    return query


def _snapshot_policy_set():
    plugin_root = Path(__file__).resolve().parents[1]
    try:
        return load_policy_set(
            plugin_root / "policies",
            analyzer_files=SNAPSHOT_ANALYZER_FILES,
            canonicalizer_files=("scripts/canonical_json.py",),
        )
    except (PolicyError, CanonicalizationError, OSError) as error:
        kind = "io" if isinstance(error, OSError) else "validation"
        raise SnapshotInputError(kind, f"could not load snapshot policy: {error}") from error


def _run_bundled(
    args: argparse.Namespace,
    config: StoreConfig,
    semantics: AdapterSemantics,
) -> int:
    if args.command == "start":
        paths = (
            initialize_portable_root(config.root)
            if semantics.name == "portable"
            else ObservationPaths.from_root(config.root)
        )
        request, scope = _start_request(args)
        print(wiki_observations.start_observation(
            paths, request, scope, semantics=semantics
        ))
        return 0
    if args.command in {"snapshot-input", "snapshot"}:
        query = _snapshot_query(args)
        policy_set = _snapshot_policy_set()
        paths = (
            _portable_paths(config, missing_ok=False)
            if semantics.name == "portable"
            else ObservationPaths.from_root(config.root)
        )
        assert paths is not None
        def acquire():
            return acquire_snapshot_input(
                paths, semantics, query, policy_set
            )

        if args.command == "snapshot-input":
            acquired = acquire()
            sys.stdout.write(acquired.manifest_bytes.decode("utf-8") + "\n")
            return 0
        home = Path(os.environ.get(
            "WORKFLOW_OBSERVATORY_HOME",
            Path.home() / ".codex/workflow-observatory",
        )).expanduser()
        published = create_learning_snapshot(
            acquire=acquire,
            query=query,
            policy_set=policy_set,
            home=home,
            generated_at=datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        )
        response = {
            "created": published.created,
            "snapshot": dict(published.artifact),
        }
        sys.stdout.write(canonicalize(response).decode("utf-8") + "\n")
        return 0
    paths = (
        _portable_paths(
            config,
            missing_ok=args.command in {"report", "validate", "integrity"},
        )
        if semantics.name == "portable"
        else ObservationPaths.from_root(config.root)
    )
    if args.command == "finish":
        assert paths is not None
        payload = wiki_observations.parse_completion_payload(
            _read_private_payload(args.from_file, "completion payload")
        )
        episode_v2 = None
        if args.episode_from_file is not None:
            try:
                episode_v2 = parse_v2_supplement(
                    _read_private_payload(
                        args.episode_from_file, "Episode supplement"
                    ),
                    wiki_observations._episode_projection_policy(),
                )
            except EpisodeSchemaError as error:
                raise ObservationError("validation", str(error)) from error
        wiki_observations.finish_observation(
            paths,
            args.run_id,
            args.status,
            payload,
            superseded_by=args.superseded_by,
            episode_v2=episode_v2,
            semantics=semantics,
        )
        return 0
    if args.command == "invalidate":
        assert paths is not None
        wiki_observations.invalidate_observation(
            paths,
            args.run_id,
            args.reason,
            semantics=semantics,
        )
        return 0
    if args.command == "report":
        if paths is None:
            sys.stdout.write(_empty_report(_filters(args)))
        else:
            records, invalidated = wiki_observations.collect_records(
                paths, semantics
            )
            sys.stdout.write(wiki_observations.render_observation_report(
                records, invalidated, _filters(args), datetime.now().astimezone()
            ))
        return 0
    return _run_read_only(args.command, paths, semantics)


def _normalized_args(args: argparse.Namespace) -> list[str]:
    if args.command == "start":
        values = [
            "start", "--title", args.title, "--subject-root", args.subject_root,
            "--agent-surface", args.agent_surface, "--start-mode", args.start_mode,
            "--task-type", args.task_type, "--workflow-variant", args.workflow_variant,
            "--scope-from-file", args.scope_from_file,
        ]
        if args.project is not None:
            values.extend(["--project", args.project])
        if args.episode_schema_version != 1:
            values.extend([
                "--episode-schema-version", str(args.episode_schema_version)
            ])
        if args.workflow_generation is not None:
            values.extend(["--workflow-generation", args.workflow_generation])
        if args.task is not None:
            values.extend(["--task", args.task])
        for source in args.source:
            values.extend(["--source", source])
        return values
    if args.command == "finish":
        values = ["finish", args.run_id, "--status", args.status, "--from-file", args.from_file]
        if args.superseded_by is not None:
            values.extend(["--superseded-by", args.superseded_by])
        if args.episode_from_file is not None:
            values.extend(["--episode-from-file", args.episode_from_file])
        return values
    values = ["report"]
    for option in ("project", "workspace", "workspace_id", "task_type", "status"):
        value = getattr(args, option)
        if value is not None:
            values.extend([f"--{option.replace('_', '-')}", value])
    for option in ("since", "until"):
        value = getattr(args, option)
        if value is not None:
            values.extend([f"--{option}", value.isoformat()])
    return values


def _delegated_error(completed: subprocess.CompletedProcess[str]) -> int:
    kinds = {2: "validation", 3: "state", 4: "io"}
    kind = kinds.get(completed.returncode, "io")
    message = completed.stderr.strip()
    for prefix in (
        "observation validation error:",
        "observation state error:",
        "observation io error:",
    ):
        if message.startswith(prefix):
            message = message[len(prefix):].strip()
            break
    if not message:
        message = f"delegated LLM Wiki CLI failed with exit code {completed.returncode}"
    print(f"workflow observer {kind} error: {message}", file=sys.stderr)
    return 1 if kind == "io" else 2


def _delegate(args: argparse.Namespace, config: StoreConfig) -> int:
    assert config.cli_path is not None
    command = [
        sys.executable,
        str(config.cli_path),
        "observe",
        "--wiki-root",
        str(config.root),
        *_normalized_args(args),
    ]
    try:
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
    except OSError as error:
        raise ObservationError("io", f"could not run configured LLM Wiki CLI: {error}") from error
    if completed.returncode != 0:
        return _delegated_error(completed)
    if args.command in {"start", "report"}:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    return 0


def _entry_kind(entry: os.DirEntry[str]) -> int:
    try:
        return entry.stat(follow_symlinks=False).st_mode
    except OSError as error:
        raise ObservationError("io", f"could not inspect integrity entry: {error}") from error


def _strict_directory(path: Path, kind: str) -> list[os.DirEntry[str]]:
    try:
        return list(os.scandir(path))
    except OSError as error:
        raise ObservationError("io", f"could not scan {kind}: {error}") from error


def _check_integrity_layout(paths: ObservationPaths) -> None:
    observations = paths.observations
    if not observations.exists():
        return
    for entry in _strict_directory(observations, "observation directory"):
        mode = _entry_kind(entry)
        if stat.S_ISLNK(mode):
            raise ObservationError("validation", "observation layout must not contain symlinks")
        if entry.name in {".locks", "invalidations"}:
            if not stat.S_ISDIR(mode):
                raise ObservationError("validation", f"{entry.name} must be a directory")
            continue
        if not stat.S_ISREG(mode) or _RECORD_RE.fullmatch(entry.name) is None:
            raise ObservationError("validation", f"unexpected observation entry: {entry.name}")

    if paths.locks.exists():
        for entry in _strict_directory(paths.locks, "lock directory"):
            mode = _entry_kind(entry)
            if stat.S_ISLNK(mode) or not stat.S_ISREG(mode) or _LOCK_RE.fullmatch(entry.name) is None:
                raise ObservationError("validation", f"unexpected lock entry: {entry.name}")
            if stat.S_IMODE(mode) & 0o077:
                raise ObservationError("validation", f"unsafe lock permissions: {entry.name}")
    if paths.invalidations.exists():
        for entry in _strict_directory(paths.invalidations, "invalidation directory"):
            mode = _entry_kind(entry)
            if stat.S_ISLNK(mode) or not stat.S_ISREG(mode) or _RECORD_RE.fullmatch(entry.name) is None:
                raise ObservationError("validation", f"unexpected invalidation entry: {entry.name}")


def _run_read_only(
    command: str,
    paths: ObservationPaths | None,
    semantics: AdapterSemantics,
) -> int:
    if paths is None:
        records, invalidated = [], set()
    else:
        records, invalidated = wiki_observations.collect_records(paths, semantics)
        if command == "integrity":
            _check_integrity_layout(paths)
    label = "healthy" if command == "integrity" else "valid"
    print(f"{label} records={len(records)} invalidated={len(invalidated)}")
    return 0


def _fail(error: ObservationError) -> int:
    print(f"workflow observer {error.kind} error: {error}", file=sys.stderr)
    return 1 if error.kind == "io" else 2


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        config = load_store_config()
        semantics = adapter_semantics(config)
        if config.adapter == "llmwiki":
            if args.command in {"validate", "integrity"}:
                paths = ObservationPaths.from_root(config.root)
                return _run_read_only(args.command, paths, semantics)
            if args.command == "start" and args.episode_schema_version == 1:
                return _delegate(args, config)
            if args.command == "finish":
                paths = ObservationPaths.from_root(config.root)
                if _read_draft_schema(paths, args.run_id) == 1:
                    return _delegate(args, config)
            return _run_bundled(args, config, semantics)
        return _run_bundled(args, config, semantics)
    except ConfigError as error:
        return _fail(ObservationError("validation", str(error)))
    except (PolicyError, EpisodeSchemaError, CanonicalizationError) as error:
        return _fail(ObservationError("validation", str(error)))
    except ObservationError as error:
        return _fail(error)
    except OSError as error:
        return _fail(ObservationError("io", str(error)))


if __name__ == "__main__":
    raise SystemExit(main())
