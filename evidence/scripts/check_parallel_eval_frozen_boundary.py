"""Fail-closed boundary checks for the parallel evaluation implementation."""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Mapping, Sequence


FROZEN_BASE = "2f617fea833e583af9cae87308cfde2e620fcd82"

ALLOWED_IMPLEMENTATION_PATHS: frozenset[str] = frozenset(
    {
        "README.md",
        "ROADMAP.md",
        "TODO.md",
        "docs/parallel-evaluation-mvp-implementation-plan.md",
        "docs/parallel-evaluation-plan.md",
        "evidence/scripts/check_parallel_eval_frozen_boundary.py",
        "evidence/scripts/package_workflow_observatory.py",
        "evidence/scripts/run_observing_workflows_eval_worker.py",
        "evidence/scripts/run_observing_workflows_task9_eval.py",
        "evidence/scripts/workflow_eval_sharding.py",
        "evidence/tests/run_parallel_eval_no_model_coordinator.py",
        "evidence/tests/run_parallel_eval_no_model_worker.py",
        "evidence/tests/test_observing_workflows_task9_eval.py",
        "evidence/tests/test_parallel_eval_frozen_boundary.py",
        "evidence/tests/test_parallel_eval_no_model_integration.py",
        "evidence/tests/test_workflow_eval_sharding.py",
        "plugins/workflow-observer/tests/run_marketplace_eval.py",
        "plugins/workflow-observer/tests/test_eval_runner_hygiene.py",
        "plugins/workflow-observer/tests/test_package_archive.py",
        "plugins/workflow-observer/tests/test_parallel_eval_runner.py",
        "evidence/marketplace/workflow-observatory/README.md",
        "evidence/marketplace/workflow-observatory/ROADMAP.md",
        "evidence/marketplace/workflow-observatory/TODO.md",
        "evidence/marketplace/workflow-observatory/docs/parallel-evaluation-mvp-implementation-plan.md",
        "evidence/marketplace/workflow-observatory/docs/parallel-evaluation-plan.md",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_eval_runner_hygiene.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_package_archive.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_parallel_eval_runner.py",
    }
)

_MANIFEST_NAMES = (
    "observing_workflows_cases.json",
    "observing_workflows_lifecycle_cases.json",
)
_MANIFEST_PREFIXES = (
    "evidence/tests/skill_evals",
    "plugins/workflow-observer/tests/skill_evals",
    "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals",
)
FROZEN_MANIFEST_PATHS: tuple[str, ...] = tuple(
    f"{prefix}/{name}" for prefix in _MANIFEST_PREFIXES for name in _MANIFEST_NAMES
)
FROZEN_BYTE_PATHS: tuple[str, ...] = (
    *FROZEN_MANIFEST_PATHS,
    "SHA256SUMS.json",
    "evidence/tests/run_observing_workflows_eval.py",
)
FROZEN_AST_BINDINGS: dict[str, tuple[str, ...]] = {
    "evidence/tests/run_observing_workflows_eval.py": (
        "DECISION_MANIFEST_FIELDS",
        "LIFECYCLE_MANIFEST_FIELDS",
        "RESULT_SCHEMAS",
    ),
    "evidence/scripts/run_observing_workflows_task9_eval.py": (
        "EXEC_TURN_TIMEOUT_SECONDS",
        "APP_SERVER_TURN_TIMEOUT_SECONDS",
        "GATE_TIMEOUT_SECONDS",
        "FROZEN_MANIFEST_HASHES",
        "FROZEN_MANIFEST_IDS",
    ),
}

assert ALLOWED_IMPLEMENTATION_PATHS.isdisjoint(FROZEN_BYTE_PATHS)

_FORBIDDEN_RESULT_BASENAMES = frozenset(
    {
        "observing_workflows_forward.json",
        "observing_workflows_lifecycle_forward.json",
        "observing_workflows_results_commit.json",
    }
)
_FORBIDDEN_RESULT_GENERATIONS_COMPONENT = ".observing_workflows_result_generations"


@dataclass(frozen=True)
class _TreeEntry:
    kind: str
    mode: int
    payload: bytes


def _special_kind(mode: int) -> str:
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISSOCK(mode):
        return "socket"
    if stat.S_ISCHR(mode):
        return "character-device"
    if stat.S_ISBLK(mode):
        return "block-device"
    return "unsupported"


def _read_regular_file(path: Path) -> bytes:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("entry changed kind while being read")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        os.close(descriptor)


def _read_tree(root: Path, label: str) -> tuple[dict[str, _TreeEntry], list[str]]:
    if not root.is_dir():
        raise ValueError(f"tree is not a directory: {root}")
    files: dict[str, _TreeEntry] = {}
    errors: list[str] = []

    def visit(directory: Path, relative_directory: Path) -> None:
        try:
            entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        except OSError as error:
            relative = relative_directory.as_posix() or "."
            errors.append(f"{label} tree directory read failed: {relative}: {error}")
            return
        for directory_entry in entries:
            relative = relative_directory / directory_entry.name
            if relative.parts and relative.parts[0] == ".git":
                continue
            relative_path = relative.as_posix()
            try:
                entry_stat = directory_entry.stat(follow_symlinks=False)
            except OSError as error:
                errors.append(
                    f"{label} tree entry stat failed: {relative_path}: {error}"
                )
                continue
            mode = entry_stat.st_mode
            permissions = stat.S_IMODE(mode)
            entry_path = Path(directory_entry.path)
            if stat.S_ISDIR(mode):
                visit(entry_path, relative)
            elif stat.S_ISREG(mode):
                try:
                    payload = _read_regular_file(entry_path)
                except (OSError, ValueError) as error:
                    errors.append(
                        f"{label} tree regular file read failed: "
                        f"{relative_path}: {error}"
                    )
                    continue
                files[relative_path] = _TreeEntry("regular", permissions, payload)
            elif stat.S_ISLNK(mode):
                try:
                    payload = os.fsencode(os.readlink(entry_path))
                except OSError as error:
                    errors.append(
                        f"{label} tree symlink read failed: {relative_path}: {error}"
                    )
                    continue
                files[relative_path] = _TreeEntry("symlink", permissions, payload)
            else:
                kind = _special_kind(mode)
                files[relative_path] = _TreeEntry(kind, permissions, b"")
                errors.append(
                    f"{label} tree contains unsupported special file: "
                    f"{relative_path} ({kind})"
                )

    visit(root, Path())
    return files, errors


def _git(repository: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"git {' '.join(arguments)} failed: {detail}")
    return completed.stdout


def _read_git_tree(repository: Path, revision: str) -> dict[str, _TreeEntry]:
    raw_entries = _git(repository, "ls-tree", "-r", "-z", revision)
    files: dict[str, _TreeEntry] = {}
    for raw_entry in raw_entries.split(b"\0"):
        if not raw_entry:
            continue
        try:
            metadata, raw_path = raw_entry.split(b"\t", 1)
            raw_mode, raw_kind, object_id = metadata.split(b" ", 2)
        except ValueError as error:
            raise ValueError(f"invalid git tree entry: {raw_entry!r}") from error
        path = raw_path.decode("utf-8", errors="surrogateescape")
        mode = int(raw_mode, 8)
        object_kind = raw_kind.decode("ascii")
        if object_kind == "blob":
            kind = "symlink" if mode == 0o120000 else "regular"
            payload = _git(repository, "cat-file", "-p", object_id.decode("ascii"))
        else:
            kind = "gitlink" if mode == 0o160000 else object_kind
            payload = object_id
        files[path] = _TreeEntry(kind, mode, payload)
    return files


def _literal_value(node: ast.AST) -> object:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Tuple):
        return tuple(_literal_value(item) for item in node.elts)
    if isinstance(node, ast.List):
        return [_literal_value(item) for item in node.elts]
    if isinstance(node, ast.Set):
        return frozenset(_literal_value(item) for item in node.elts)
    if isinstance(node, ast.Dict):
        return {
            _literal_value(key): _literal_value(value)
            for key, value in zip(node.keys, node.values, strict=True)
        }
    if isinstance(node, ast.UnaryOp):
        value = _literal_value(node.operand)
        if isinstance(node.op, ast.UAdd):
            return +value  # type: ignore[operator]
        if isinstance(node.op, ast.USub):
            return -value  # type: ignore[operator]
    if isinstance(node, ast.BinOp):
        left = _literal_value(node.left)
        right = _literal_value(node.right)
        if isinstance(node.op, ast.Add):
            return left + right  # type: ignore[operator]
        if isinstance(node.op, ast.Sub):
            return left - right  # type: ignore[operator]
        if isinstance(node.op, ast.Mult):
            return left * right  # type: ignore[operator]
        if isinstance(node.op, ast.FloorDiv):
            return left // right  # type: ignore[operator]
    raise ValueError(f"binding is not a supported literal: {ast.dump(node)}")


def _literal_bindings(source: bytes, names: tuple[str, ...]) -> dict[str, object]:
    module = ast.parse(source.decode("utf-8"))
    wanted = set(names)
    bindings: dict[str, object] = {}
    for statement in module.body:
        name: str | None = None
        value: ast.AST | None = None
        if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
            target = statement.targets[0]
            if isinstance(target, ast.Name):
                name = target.id
                value = statement.value
        elif isinstance(statement, ast.AnnAssign) and isinstance(
            statement.target, ast.Name
        ):
            name = statement.target.id
            value = statement.value
        if name in wanted and value is not None:
            bindings[name] = _literal_value(value)
    missing = sorted(wanted - bindings.keys())
    if missing:
        raise ValueError(f"missing literal bindings: {', '.join(missing)}")
    return bindings


def _compare_file_maps(
    base_files: Mapping[str, _TreeEntry], head_files: Mapping[str, _TreeEntry]
) -> list[str]:
    if not ALLOWED_IMPLEMENTATION_PATHS.isdisjoint(FROZEN_BYTE_PATHS):
        overlap = sorted(ALLOWED_IMPLEMENTATION_PATHS.intersection(FROZEN_BYTE_PATHS))
        return [f"allowlist overlaps frozen byte paths: {path}" for path in overlap]

    errors: list[str] = []
    base_paths = set(base_files)
    head_paths = set(head_files)
    all_paths = base_paths | head_paths

    for path in sorted(all_paths):
        if base_files.get(path) != head_files.get(path) and path not in ALLOWED_IMPLEMENTATION_PATHS:
            errors.append(f"unauthorized changed path: {path}")

    for path in FROZEN_BYTE_PATHS:
        if path not in base_files:
            errors.append(f"frozen byte path missing from base: {path}")
        if path not in head_files:
            errors.append(f"frozen byte path missing from head: {path}")
        if (
            path in base_files
            and path in head_files
            and base_files[path] != head_files[path]
        ):
            errors.append(f"frozen byte path changed: {path}")

    discovered_manifests = {
        path for path in all_paths if Path(path).name in _MANIFEST_NAMES
    }
    for path in sorted(discovered_manifests - set(FROZEN_MANIFEST_PATHS)):
        errors.append(f"unexpected seventh frozen manifest path: {path}")

    for path in sorted(head_paths - base_paths):
        path_parts = Path(path).parts
        if Path(path).name in _FORBIDDEN_RESULT_BASENAMES:
            errors.append(f"forbidden result artifact path added: {path}")
        if _FORBIDDEN_RESULT_GENERATIONS_COMPONENT in path_parts:
            errors.append(f"forbidden result-generation path added: {path}")

    for path, names in FROZEN_AST_BINDINGS.items():
        if path not in base_files or path not in head_files:
            errors.append(f"frozen AST source missing: {path}")
            continue
        try:
            base_bindings = _literal_bindings(base_files[path].payload, names)
        except (SyntaxError, UnicodeDecodeError, ValueError) as error:
            errors.append(f"invalid base AST bindings in {path}: {error}")
            continue
        try:
            head_bindings = _literal_bindings(head_files[path].payload, names)
        except (SyntaxError, UnicodeDecodeError, ValueError) as error:
            errors.append(f"invalid head AST bindings in {path}: {error}")
            continue
        for name in names:
            if base_bindings[name] != head_bindings[name]:
                errors.append(f"frozen AST binding changed: {path}:{name}")

    return errors


def compare_git_range(repository: Path, base: str, head: str) -> list[str]:
    """Compare tracked blobs at two revisions against the frozen boundary."""

    repository = repository.resolve()
    repository = Path(
        _git(repository, "rev-parse", "--show-toplevel")
        .decode("utf-8", errors="surrogateescape")
        .strip()
    )
    return _compare_file_maps(
        _read_git_tree(repository, base),
        _read_git_tree(repository, head),
    )


def compare_trees(base_tree: Path, head_tree: Path) -> list[str]:
    """Compare two materialized trees against the frozen boundary."""

    base_files, base_errors = _read_tree(base_tree, "base")
    head_files, head_errors = _read_tree(head_tree, "head")
    return [*base_errors, *head_errors, *_compare_file_maps(base_files, head_files)]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    git_range = subparsers.add_parser("git-range", help="compare two Git revisions")
    git_range.add_argument("--repository", type=Path, default=Path.cwd())
    git_range.add_argument("--base", default=FROZEN_BASE)
    git_range.add_argument("--head", default="HEAD")

    trees = subparsers.add_parser("trees", help="compare two materialized trees")
    trees.add_argument("--base-tree", type=Path, required=True)
    trees.add_argument("--head-tree", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] not in {"git-range", "trees"}:
        arguments.insert(0, "git-range")
    args = _parser().parse_args(arguments)
    try:
        if args.mode == "git-range":
            errors = compare_git_range(args.repository, args.base, args.head)
        else:
            errors = compare_trees(args.base_tree, args.head_tree)
    except (OSError, ValueError) as error:
        print(f"frozen boundary check failed: {error}", file=sys.stderr)
        return 2

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print("parallel evaluation frozen boundary: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
