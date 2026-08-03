#!/usr/bin/env python3
"""Build and verify a deterministic Workflow Observatory distribution archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
from typing import Iterable, Sequence
import zipfile


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_LIVE_MARKETPLACE_ROOT = REPOSITORY_ROOT.parent
_LIVE_MARKETPLACE_MANIFEST = (
    _LIVE_MARKETPLACE_ROOT / ".agents/plugins/marketplace.json"
)
_FROZEN_MARKETPLACE_ROOT = REPOSITORY_ROOT / "marketplace/workflow-observatory"
MARKETPLACE_ROOT = (
    _LIVE_MARKETPLACE_ROOT
    if _LIVE_MARKETPLACE_MANIFEST.is_file()
    or _LIVE_MARKETPLACE_MANIFEST.is_symlink()
    else _FROZEN_MARKETPLACE_ROOT
)
_LIVE_STAGING_EXCLUSIONS = frozenset(
    {".git", ".codegraph", ".superpowers", "dist", "evidence"}
)
ARCHIVE_ROOT = "workflow-observatory"
INVENTORY_MEMBER = f"{ARCHIVE_ROOT}/SHA256SUMS.json"
ZIP_TIMESTAMP = (2026, 7, 15, 0, 0, 0)
TEXT_SUFFIXES = {".json", ".md", ".py", ".txt", ".yaml", ".yml"}
FORWARD_MANIFEST = "observing_workflows_cases.json"
LIFECYCLE_MANIFEST = "observing_workflows_lifecycle_cases.json"
SOURCE_MANIFEST_HASHES = {
    FORWARD_MANIFEST: "f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2",
    LIFECYCLE_MANIFEST: "d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b",
}
FORBIDDEN_ARCHIVE_BYTES = (
    b"/private/var/" + b"folders/",
)
_USER_HOME_PATTERN = re.compile(
    rb"/" + rb"Users/" + rb"[^/\s`\"']+"
)
_SYNTHETIC_USER_PATHS = (
    b"/" + b"Users/" + b"alice/repo",
    b"/" + b"Users/" + b"alice/private/repo",
    b"/" + b"Users/" + b"alice/private-project",
    b"/" + b"Users/" + b"alice/private/story",
    b"/" + b"Users/" + b"alice/private.txt",
)
PATH_NORMALIZATION_DECLARATIONS = {
    "repository-root": frozenset({
        "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
        "docs/superpowers/specs/2026-07-12-observation-records-design.md",
        "scripts/run_observing_workflows_task9_eval.py",
        "skills/observing-workflows/SKILL.md",
    }),
    "codex-home": frozenset({
        "docs/parallel-evaluation-mvp-implementation-plan.md",
        "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
        "docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md",
        "marketplace/workflow-observatory/docs/"
        "parallel-evaluation-mvp-implementation-plan.md",
        "skills/observing-workflows/README.md",
    }),
    "user-home": frozenset({
        "docs/parallel-evaluation-mvp-implementation-plan.md",
        "marketplace/workflow-observatory/docs/"
        "parallel-evaluation-mvp-implementation-plan.md",
        "plugins/workflow-observer/tests/skill_evals/"
        "observing_workflows_lifecycle_cases.json",
        "marketplace/workflow-observatory/plugins/workflow-observer/tests/"
        "skill_evals/observing_workflows_lifecycle_cases.json",
        "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    }),
    "temporary-root": frozenset({
        ".superpowers/sdd/workflow-observatory-task-6-report.md",
    }),
}
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RECOGNIZED_NORMALIZATIONS = frozenset(PATH_NORMALIZATION_DECLARATIONS) | {
    "manifest-hash:f3bd3b758e5f",
    "manifest-hash:17626afd2a24",
}


class PackageError(RuntimeError):
    """The requested archive is unsafe, incomplete, or inconsistent."""


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _contains_personal_path(data: bytes) -> bool:
    candidate = data
    for fixture in _SYNTHETIC_USER_PATHS:
        candidate = re.sub(
            re.escape(fixture) + rb"(?=$|[\s`\"'])",
            b"",
            candidate,
        )
    return _USER_HOME_PATTERN.search(candidate) is not None or any(
        marker in candidate for marker in FORBIDDEN_ARCHIVE_BYTES
    )


def _is_allowed_marketplace_file(relative: PurePosixPath) -> bool:
    value = relative.as_posix()
    if value in {
        ".gitignore",
        ".agents/plugins/marketplace.json",
        "README.md",
        "ROADMAP.md",
        "TODO.md",
        "LICENSE",
        "NOTICE.md",
        "docs/parallel-evaluation-mvp-implementation-plan.md",
        "docs/parallel-evaluation-plan.md",
        "docs/release-acceptance.md",
        "docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md",
        "docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md",
        "plugins/workflow-observer/.codex-plugin/plugin.json",
        "plugins/workflow-observer/README.md",
    }:
        return True
    prefix = "plugins/workflow-observer/"
    if not value.startswith(prefix):
        return False
    nested = value[len(prefix):]
    if nested.startswith("scripts/"):
        return nested.endswith(".py") or nested == "scripts/core_source.json"
    if nested.startswith("skills/"):
        parts = PurePosixPath(nested).parts
        return len(parts) == 3 and parts[-1] == "SKILL.md"
    if nested.startswith("policies/"):
        policy = PurePosixPath(nested)
        return len(policy.parts) == 2 and policy.suffix == ".json"
    if nested.startswith("tests/skill_evals/"):
        return nested.endswith(".json")
    if nested == "tests/fixtures/jcs_conformance_vectors.json":
        return True
    if nested.startswith("tests/"):
        return nested.endswith(".py")
    if nested.startswith("docs/"):
        return nested.endswith(".md")
    return False


def _stage_live_marketplace(source_root: Path, staging_parent: Path) -> Path:
    source_root = Path(source_root).absolute()
    staging_parent = Path(staging_parent).absolute()
    if not source_root.is_dir() or source_root.is_symlink():
        raise PackageError(
            f"live marketplace root must be a real directory: {source_root}"
        )
    destination = staging_parent / "marketplace"
    if destination.exists() or destination.is_symlink():
        raise PackageError(f"live marketplace staging path exists: {destination}")

    def ignore_development_roots(directory: str, names: list[str]) -> list[str]:
        if Path(directory).absolute() != source_root:
            return []
        return sorted(_LIVE_STAGING_EXCLUSIONS.intersection(names))

    try:
        shutil.copytree(
            source_root,
            destination,
            symlinks=True,
            ignore=ignore_development_roots,
        )
    except OSError as error:
        raise PackageError(f"could not stage live marketplace: {error}") from error
    return destination


def _assert_regular_no_symlink(path: Path, root: Path, label: str) -> None:
    try:
        relative = path.absolute().relative_to(root.absolute())
    except ValueError as error:
        raise PackageError(f"{label} escapes repository root: {path}") from error
    cursor = root.absolute()
    for part in relative.parts:
        cursor = cursor / part
        try:
            details = cursor.lstat()
        except FileNotFoundError as error:
            raise PackageError(f"missing {label}: {path}") from error
        if stat.S_ISLNK(details.st_mode):
            raise PackageError(f"symlink is forbidden for {label}: {path}")
    if not stat.S_ISREG(path.lstat().st_mode):
        raise PackageError(f"non-regular {label}: {path}")


def _marketplace_files(source_root: Path) -> list[tuple[str, bytes, str]]:
    source_root = source_root.absolute()
    if not source_root.is_dir() or source_root.is_symlink():
        raise PackageError(f"marketplace root must be a real directory: {source_root}")
    rows: list[tuple[str, bytes, str]] = []
    for path in sorted(source_root.rglob("*"), key=lambda item: item.as_posix()):
        details = path.lstat()
        relative = PurePosixPath(path.relative_to(source_root).as_posix())
        if stat.S_ISLNK(details.st_mode):
            raise PackageError(f"symlink is forbidden in marketplace: {relative}")
        if stat.S_ISDIR(details.st_mode):
            continue
        if relative.parts[0] == "evidence" or relative.as_posix() == "SHA256SUMS.json":
            continue
        if not stat.S_ISREG(details.st_mode):
            raise PackageError(f"non-regular marketplace file: {relative}")
        if "__pycache__" in relative.parts or relative.suffix == ".pyc":
            continue
        if not _is_allowed_marketplace_file(relative):
            raise PackageError(f"unexpected marketplace file: {relative}")
        data = path.read_bytes()
        member = f"{ARCHIVE_ROOT}/{relative.as_posix()}"
        rows.append((member, data, relative.as_posix()))
    required = {
        f"{ARCHIVE_ROOT}/.agents/plugins/marketplace.json",
        f"{ARCHIVE_ROOT}/plugins/workflow-observer/.codex-plugin/plugin.json",
        f"{ARCHIVE_ROOT}/README.md",
        f"{ARCHIVE_ROOT}/ROADMAP.md",
        f"{ARCHIVE_ROOT}/TODO.md",
        f"{ARCHIVE_ROOT}/docs/parallel-evaluation-mvp-implementation-plan.md",
        f"{ARCHIVE_ROOT}/docs/parallel-evaluation-plan.md",
        f"{ARCHIVE_ROOT}/docs/release-acceptance.md",
        f"{ARCHIVE_ROOT}/LICENSE",
        f"{ARCHIVE_ROOT}/NOTICE.md",
    }
    found = {member for member, _data, _relative in rows}
    missing = sorted(required - found)
    if missing:
        raise PackageError("missing required marketplace files: " + ", ".join(missing))
    return rows


def default_evidence(repository_root: Path = REPOSITORY_ROOT) -> tuple[Path, ...]:
    repository_root = Path(repository_root).absolute()
    explicit = [
        "AGENTS.md",
        "wiki/concept/Workflow_Observation_and_Process_Knowledge.md",
        "docs/superpowers/specs/2026-07-12-observation-records-design.md",
        "docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md",
        "docs/superpowers/plans/2026-07-12-observation-records.md",
        "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
        "docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md",
        "docs/superpowers/plans/2026-07-15-workflow-telemetry-best-practices-research.md",
        ".superpowers/sdd/workflow-observatory-task-6-report.md",
        "wiki_cli.py",
        "wiki_observations.py",
        "scripts/__init__.py",
        "scripts/run_observing_workflows_eval_worker.py",
        "scripts/run_observing_workflows_task9_eval.py",
        "scripts/package_workflow_observatory.py",
        "scripts/check_parallel_eval_frozen_boundary.py",
        "scripts/workflow_eval_sharding.py",
        "skills/observing-workflows/SKILL.md",
        "skills/observing-workflows/README.md",
        "skills/observing-workflows/agents/openai.yaml",
        "tests/observing_workflows_eval_harness.py",
        "tests/run_parallel_eval_no_model_coordinator.py",
        "tests/run_parallel_eval_no_model_worker.py",
        "tests/run_observing_workflows_eval.py",
        "tests/test_parallel_eval_no_model_integration.py",
        "tests/test_parallel_eval_frozen_boundary.py",
        "tests/test_workflow_eval_sharding.py",
        "tests/test_inbox_flow.py",
        "tests/test_knowledge_loop.py",
        "tests/test_task_records.py",
    ]
    paths = {repository_root / relative for relative in explicit}
    tests_root = repository_root / "tests"
    for pattern in ("test_observation*.py", "test_observing_workflows*.py"):
        paths.update(tests_root.glob(pattern))
    paths.update((tests_root / "skill_evals").glob("*.json"))
    marketplace_root = repository_root / "marketplace/workflow-observatory"
    if marketplace_root.is_dir():
        for path in marketplace_root.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            relative = PurePosixPath(path.relative_to(marketplace_root).as_posix())
            if _is_allowed_marketplace_file(relative):
                paths.add(path)
    return tuple(sorted(paths, key=lambda path: path.as_posix()))


def _evidence_files(
    evidence: Sequence[Path], repository_root: Path
) -> list[tuple[str, bytes, str]]:
    rows = []
    seen = set()
    for candidate in evidence:
        path = Path(candidate).absolute()
        _assert_regular_no_symlink(path, repository_root, "repository evidence")
        relative = path.relative_to(repository_root).as_posix()
        if relative in seen:
            raise PackageError(f"duplicate repository evidence: {relative}")
        if relative.startswith("raw/") or relative.startswith("wiki/observations/"):
            raise PackageError(f"private evidence class is forbidden: {relative}")
        seen.add(relative)
        rows.append(
            (f"{ARCHIVE_ROOT}/evidence/{relative}", path.read_bytes(), relative)
        )
    return sorted(rows, key=lambda row: row[0])


_TEXT_PATH_BOUNDARY = rb"(?=$|/|[\s`\"'()<>\[\]{},;:])"


def _replace_path_prefix(
    data: bytes, source: bytes, target: bytes
) -> tuple[bytes, int]:
    return re.subn(
        re.escape(source) + _TEXT_PATH_BOUNDARY,
        target,
        data,
    )


def _normalize_text(data: bytes, origin: str) -> tuple[bytes, list[str]]:
    replacements = (
        (str(REPOSITORY_ROOT).encode("utf-8"), b"${LLMWIKI_ROOT}", "repository-root"),
        (
            str(Path.home() / ".codex").encode("utf-8"),
            b"${CODEX_HOME}",
            "codex-home",
        ),
        (str(Path.home()).encode("utf-8"), b"${HOME}", "user-home"),
    )
    normalized = data
    applied = []
    for source, target, label in replacements:
        updated, count = _replace_path_prefix(normalized, source, target)
        if count:
            if origin not in PATH_NORMALIZATION_DECLARATIONS[label]:
                raise PackageError(
                    f"undeclared path normalization `{label}`: {origin}"
                )
            normalized = updated
            applied.append(label)
    temporary_pattern = re.compile(
        rb"/private/var/" rb"folders/[^\s`\"']+/T/"
    )
    if temporary_pattern.search(normalized):
        if origin not in PATH_NORMALIZATION_DECLARATIONS["temporary-root"]:
            raise PackageError(
                f"undeclared path normalization `temporary-root`: {origin}"
            )
        normalized = temporary_pattern.sub(b"${TMPDIR}/", normalized)
        applied.append("temporary-root")
    return normalized, applied


def _normalize_entries(
    rows: Iterable[tuple[str, bytes, str, str]]
) -> tuple[dict[str, bytes], dict[str, dict[str, object]]]:
    entries: dict[str, bytes] = {}
    metadata: dict[str, dict[str, object]] = {}
    for member, source, origin, kind in rows:
        suffix = PurePosixPath(member).suffix.lower()
        packaged = source
        normalizations: list[str] = []
        if suffix in TEXT_SUFFIXES or PurePosixPath(member).name == "LICENSE":
            packaged, normalizations = _normalize_text(source, origin)
        elif _contains_personal_path(source):
            raise PackageError(f"personal path in non-text archive member: {member}")
        if member in entries:
            raise PackageError(f"duplicate archive member: {member}")
        entries[member] = packaged
        metadata[member] = {
            "kind": kind,
            "origin": origin,
            "source_sha256": _sha256(source),
            "normalizations": normalizations,
        }

    packaged_hashes = {}
    for filename, source_hash in SOURCE_MANIFEST_HASHES.items():
        matches = {
            _sha256(data)
            for member, data in entries.items()
            if PurePosixPath(member).name == filename
        }
        if not matches:
            raise PackageError(f"missing frozen manifest from archive inputs: {filename}")
        if len(matches) != 1:
            raise PackageError(f"normalized frozen manifest copies diverge: {filename}")
        packaged_hashes[source_hash] = matches.pop()

    for member, data in tuple(entries.items()):
        suffix = PurePosixPath(member).suffix.lower()
        if suffix not in TEXT_SUFFIXES:
            continue
        updated = data
        applied = metadata[member]["normalizations"]
        assert isinstance(applied, list)
        for source_hash, packaged_hash in packaged_hashes.items():
            source = source_hash.encode("ascii")
            if source in updated and source_hash != packaged_hash:
                updated = updated.replace(source, packaged_hash.encode("ascii"))
                applied.append(f"manifest-hash:{source_hash[:12]}")
        entries[member] = updated

    for member, data in entries.items():
        if _contains_personal_path(data):
            raise PackageError(f"personal path remains after normalization: {member}")
        metadata[member]["packaged_sha256"] = _sha256(data)
    return entries, metadata


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
        + b"\n"
    )


def _inventory(
    entries: dict[str, bytes], metadata: dict[str, dict[str, object]]
) -> dict[str, object]:
    members = {member: _sha256(data) for member, data in sorted(entries.items())}
    marketplace_files = {}
    repository_evidence = {}
    for member in sorted(entries):
        details = metadata[member]
        row = {
            "member": member,
            "source_sha256": details["source_sha256"],
            "packaged_sha256": details["packaged_sha256"],
            "normalized": bool(details["normalizations"]),
            "normalizations": details["normalizations"],
        }
        if details["kind"] == "marketplace":
            marketplace_files[str(details["origin"])] = row
        else:
            repository_evidence[str(details["origin"])] = row
    return {
        "schema_version": 1,
        "archive_root": ARCHIVE_ROOT,
        "zip_timestamp": "2026-07-15T00:00:00Z",
        "members": members,
        "marketplace_files": marketplace_files,
        "repository_evidence": repository_evidence,
    }


def _zip_info(member: str) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(member, ZIP_TIMESTAMP)
    info.create_system = 3
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = (stat.S_IFREG | 0o644) << 16
    return info


def _write_zip(path: Path, entries: dict[str, bytes], inventory: bytes) -> None:
    with zipfile.ZipFile(
        path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as bundle:
        for member in sorted(entries):
            bundle.writestr(_zip_info(member), entries[member])
        bundle.writestr(_zip_info(INVENTORY_MEMBER), inventory)


def verify_archive(path: Path) -> str:
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise PackageError(f"archive is not a regular file: {path}")
    try:
        with zipfile.ZipFile(path) as bundle:
            infos = bundle.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise PackageError("archive contains duplicate members")
            if INVENTORY_MEMBER not in names:
                raise PackageError("archive inventory is missing")
            for info in infos:
                pure = PurePosixPath(info.filename)
                if (
                    pure.is_absolute()
                    or ".." in pure.parts
                    or not info.filename.startswith(f"{ARCHIVE_ROOT}/")
                    or info.is_dir()
                ):
                    raise PackageError(f"unsafe archive member: {info.filename}")
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                    raise PackageError(f"non-regular archive member: {info.filename}")
                if (mode & 0o777) != 0o644 or info.date_time != ZIP_TIMESTAMP:
                    raise PackageError(f"non-deterministic metadata: {info.filename}")
            try:
                inventory = json.loads(bundle.read(INVENTORY_MEMBER))
            except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as error:
                raise PackageError("archive inventory is invalid") from error
            if not isinstance(inventory, dict) or set(inventory) != {
                "schema_version",
                "archive_root",
                "zip_timestamp",
                "members",
                "marketplace_files",
                "repository_evidence",
            }:
                raise PackageError("archive inventory fields are invalid")
            if (
                inventory["schema_version"] != 1
                or inventory["archive_root"] != ARCHIVE_ROOT
                or inventory["zip_timestamp"] != "2026-07-15T00:00:00Z"
            ):
                raise PackageError("archive inventory identity is invalid")
            members = inventory["members"]
            if not isinstance(members, dict) or set(names) != {
                *members.keys(),
                INVENTORY_MEMBER,
            }:
                raise PackageError("archive member inventory is incomplete")
            for member, expected in members.items():
                data = bundle.read(member)
                if _sha256(data) != expected:
                    raise PackageError(f"archive member hash mismatch: {member}")
                if _contains_personal_path(data):
                    raise PackageError(f"personal path found in archive: {member}")
            for section in ("marketplace_files", "repository_evidence"):
                mapping = inventory[section]
                if not isinstance(mapping, dict):
                    raise PackageError(f"invalid inventory section: {section}")
                for origin, entry in mapping.items():
                    if not isinstance(origin, str) or not isinstance(entry, dict):
                        raise PackageError(f"invalid inventory entry: {section}")
                    required = {
                        "member",
                        "source_sha256",
                        "packaged_sha256",
                        "normalized",
                        "normalizations",
                    }
                    if set(entry) != required or entry["member"] not in members:
                        raise PackageError(f"invalid completeness entry: {origin}")
                    source_digest = entry["source_sha256"]
                    packaged_digest = entry["packaged_sha256"]
                    normalized = entry["normalized"]
                    normalizations = entry["normalizations"]
                    if (
                        not isinstance(source_digest, str)
                        or _DIGEST_PATTERN.fullmatch(source_digest) is None
                        or not isinstance(packaged_digest, str)
                        or _DIGEST_PATTERN.fullmatch(packaged_digest) is None
                        or type(normalized) is not bool
                        or not isinstance(normalizations, list)
                        or any(
                            not isinstance(label, str)
                            or label not in _RECOGNIZED_NORMALIZATIONS
                            for label in normalizations
                        )
                        or len(normalizations) != len(set(normalizations))
                        or normalized is not bool(normalizations)
                        or (source_digest == packaged_digest) is normalized
                    ):
                        raise PackageError(
                            f"invalid completeness metadata: {origin}"
                        )
                    if entry["packaged_sha256"] != members[entry["member"]]:
                        raise PackageError(f"completeness digest mismatch: {origin}")
            marketplace_files = inventory["marketplace_files"]
            repository_evidence = inventory["repository_evidence"]
            if set(marketplace_files) & set(repository_evidence):
                raise PackageError("completeness mapping origins overlap")
            mapped_members = [
                entry["member"]
                for mapping in (marketplace_files, repository_evidence)
                for entry in mapping.values()
            ]
            if (
                len(mapped_members) != len(set(mapped_members))
                or set(mapped_members) != set(members)
            ):
                raise PackageError(
                    "completeness mapping must cover every member exactly once"
                )
            evidence_prefix = f"{ARCHIVE_ROOT}/evidence/"
            for origin, entry in marketplace_files.items():
                if entry["member"] != f"{ARCHIVE_ROOT}/{origin}":
                    raise PackageError(
                        f"origin-member mapping mismatch: {origin}"
                    )
            for origin, entry in repository_evidence.items():
                if entry["member"] != f"{evidence_prefix}{origin}":
                    raise PackageError(
                        f"origin-member mapping mismatch: {origin}"
                    )
            if any(
                entry["member"].startswith(evidence_prefix)
                for entry in marketplace_files.values()
            ) or any(
                not entry["member"].startswith(evidence_prefix)
                for entry in repository_evidence.values()
            ):
                raise PackageError("completeness mapping member class mismatch")
    except (OSError, zipfile.BadZipFile) as error:
        raise PackageError(f"could not read archive: {path}") from error
    return _sha256(path.read_bytes())


def build_archive(
    source_root: Path,
    destination: Path,
    evidence: Sequence[Path],
) -> str:
    source_root = Path(source_root).absolute()
    destination = Path(destination).absolute()
    repository_root = REPOSITORY_ROOT.absolute()
    marketplace_rows = [
        (member, data, relative, "marketplace")
        for member, data, relative in _marketplace_files(source_root)
    ]
    evidence_rows = [
        (member, data, relative, "evidence")
        for member, data, relative in _evidence_files(evidence, repository_root)
    ]
    entries, metadata = _normalize_entries((*marketplace_rows, *evidence_rows))
    inventory = _json_bytes(_inventory(entries, metadata))
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        _write_zip(temporary, entries, inventory)
        archive_fd = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(archive_fd)
        finally:
            os.close(archive_fd)
        digest = verify_archive(temporary)
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        if verify_archive(destination) != digest:
            raise PackageError("archive readback digest mismatch")
        return digest
    finally:
        temporary.unlink(missing_ok=True)


def _build_default_archive(destination: Path) -> str:
    evidence = default_evidence(REPOSITORY_ROOT)
    if MARKETPLACE_ROOT != _LIVE_MARKETPLACE_ROOT:
        return build_archive(MARKETPLACE_ROOT, destination, evidence)
    with tempfile.TemporaryDirectory(
        prefix="workflow-observatory-marketplace-stage-"
    ) as temporary:
        staged = _stage_live_marketplace(MARKETPLACE_ROOT, Path(temporary))
        return build_archive(staged, destination, evidence)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version")
    parser.add_argument("--verify", type=Path)
    arguments = parser.parse_args(argv)
    if arguments.verify is not None:
        print(verify_archive(arguments.verify))
        return 0
    if not arguments.version:
        parser.error("--version is required when building")
    destination = (
        REPOSITORY_ROOT
        / "dist"
        / f"workflow-observatory-{arguments.version}.zip"
    )
    digest = _build_default_archive(destination)
    print(f"{digest}  {destination.relative_to(REPOSITORY_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
