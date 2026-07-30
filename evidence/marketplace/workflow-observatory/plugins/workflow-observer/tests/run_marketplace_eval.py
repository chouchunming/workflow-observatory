#!/usr/bin/env python3
"""Run revision 6 frozen evaluations against an isolated marketplace copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


sys.dont_write_bytecode = True


TESTS_ROOT = Path(__file__).resolve().parent
PLUGIN_ROOT = TESTS_ROOT.parent
MARKETPLACE_ROOT = PLUGIN_ROOT.parents[1]
_SOURCE_REPOSITORY = MARKETPLACE_ROOT.parents[1]
REPOSITORY_ROOT = (
    _SOURCE_REPOSITORY
    if (_SOURCE_REPOSITORY / "scripts/run_observing_workflows_task9_eval.py").is_file()
    else MARKETPLACE_ROOT / "evidence"
)
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from scripts.run_observing_workflows_task9_eval import (
    CaseRuntime,
    RuntimePayloadAudit,
    _run_case,
    build_embedded_audit_wrapper,
    inventory_external_skill_paths,
    run_configured_integrity,
    run_discovery_sweep,
    run_suite,
    run_with_production_guard,
)
from scripts.workflow_eval_sharding import (
    ParallelOptions,
    run_parallel_evaluation,
)
from tests.observing_workflows_eval_harness import inspect_store
from tests.observing_workflows_eval_harness import snapshot_production, assert_production_unchanged


FROZEN_MANIFEST_HASHES = {
    "forward": "f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2",
    "lifecycle": "d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b",
}
MANIFEST_PATHS = {
    "forward": TESTS_ROOT / "skill_evals/observing_workflows_cases.json",
    "lifecycle": TESTS_ROOT / "skill_evals/observing_workflows_lifecycle_cases.json",
}
RESULT_PATHS = {
    "forward": TESTS_ROOT / "skill_evals/observing_workflows_forward.json",
    "lifecycle": TESTS_ROOT / "skill_evals/observing_workflows_lifecycle_forward.json",
}
DIAGNOSTIC_CASE_ID = "reviewed-refactor"


def exact_git_repository_root(start: Path) -> Path:
    completed = subprocess.run(
        ["git", "-C", str(start), "rev-parse", "--show-toplevel"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    rendered = completed.stdout.rstrip("\n")
    if completed.returncode != 0 or not rendered or "\n" in rendered:
        raise RuntimeError(
            "formal marketplace evaluation requires a supported Git worktree"
        )
    repository_root = Path(rendered)
    if (
        not repository_root.is_absolute()
        or repository_root.resolve(strict=True) != repository_root
    ):
        raise RuntimeError(
            "formal marketplace evaluation requires the exact canonical Git root"
        )
    return repository_root


def validate_marketplace_manifest_hashes() -> None:
    for mode, path in MANIFEST_PATHS.items():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != FROZEN_MANIFEST_HASHES[mode]:
            raise AssertionError(f"{mode} marketplace manifest hash mismatch")


def _git_commit_fixture_skills(workspace: Path) -> None:
    subprocess.run(["git", "add", ".agents/skills"], cwd=workspace, check=True)
    environment = os.environ.copy()
    environment.update(
        {
            "GIT_AUTHOR_NAME": "Evaluation Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Evaluation Fixture",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:01+00:00",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:01+00:00",
        }
    )
    subprocess.run(
        ["git", "commit", "-m", "Install isolated marketplace skills"],
        cwd=workspace,
        check=True,
        capture_output=True,
        env=environment,
    )


def _build_marketplace_runtime(
    case: dict, case_root: Path, workspace: Path, lifecycle: bool
) -> CaseRuntime:
    del lifecycle
    install_root = case_root / f"{case['id']}-marketplace-install"
    shutil.copytree(
        MARKETPLACE_ROOT,
        install_root,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    installed_plugin = install_root / "plugins/workflow-observer"
    skills_root = workspace / ".agents/skills"
    skills_root.mkdir(parents=True)
    for skill in sorted((installed_plugin / "skills").iterdir()):
        if skill.is_dir():
            (skills_root / skill.name).symlink_to(skill, target_is_directory=True)
    _git_commit_fixture_skills(workspace)

    cli = installed_plugin / "scripts/workflow_observer_cli.py"
    original_cli = cli.read_bytes()
    audit_root = case_root / f"{case['id']}-payload-audit"
    audit_root.mkdir(mode=0o700)
    payload_dir = audit_root / "tmp"
    payload_dir.mkdir(mode=0o700)
    audit = RuntimePayloadAudit(
        root=audit_root,
        payload_dir=payload_dir,
        log_path=audit_root / "payload-audit.jsonl",
        wrapper_path=cli,
    )
    home = case_root / f"{case['id']}-workflow-home"
    home.mkdir(mode=0o700)
    environment = {
        "WORKFLOW_OBSERVATORY_HOME": str(home),
        "OBSERVATION_PAYLOAD_TMPDIR": str(audit.payload_dir),
        "OBSERVATION_AUDIT_LOG": str(audit.log_path),
    }
    fixture_skill_paths = tuple(skills_root.glob("*/SKILL.md"))
    return CaseRuntime(
        store_root=home / "store",
        audit=audit,
        environment=environment,
        writable_roots=(home, audit.root),
        selected_command="workflow_observer_cli.py",
        disabled_skill_paths=inventory_external_skill_paths(
            fixture_skill_paths=fixture_skill_paths
        ),
        integrity_command=(sys.executable, str(cli), "integrity"),
        audited_wrapper_path=cli,
        audited_wrapper_content=build_embedded_audit_wrapper(
            original_cli,
            force_start_unavailable=(
                case.get("setup", {}).get("cli") == "unavailable"
            ),
        ),
    )


class MarketplaceRuntimeFactory:
    def __init__(self):
        self.runtimes: list[CaseRuntime] = []
        self.verified_runtimes = 0

    def __call__(
        self, case: dict, case_root: Path, workspace: Path, lifecycle: bool
    ) -> CaseRuntime:
        runtime = _build_marketplace_runtime(case, case_root, workspace, lifecycle)
        self.runtimes.append(runtime)
        return runtime

    def verify_all_integrity(self) -> None:
        for runtime in self.runtimes:
            assert runtime.integrity_command is not None
            store = inspect_store(runtime.store_root)
            run_configured_integrity(
                runtime.integrity_command,
                runtime.environment,
                expected_records=store["run_count"],
            )

    def verify_case_safety(self, case: dict, mode: str) -> None:
        if len(self.runtimes) != self.verified_runtimes + 1:
            raise AssertionError(
                f"{mode} {case['id']}: isolated runtime setup did not complete"
            )
        runtime = self.runtimes[self.verified_runtimes]
        assert runtime.integrity_command is not None
        store = inspect_store(runtime.store_root)
        run_configured_integrity(
            runtime.integrity_command,
            runtime.environment,
            expected_records=store["run_count"],
        )
        payload_leftovers = sum(1 for _ in runtime.audit.payload_dir.iterdir())
        if payload_leftovers:
            raise AssertionError(
                f"{mode} {case['id']}: payload cleanup left "
                f"{payload_leftovers} path(s)"
            )
        if (runtime.audit.root / "exec-final-message.txt").exists():
            raise AssertionError(
                f"{mode} {case['id']}: exec output cleanup did not complete"
            )
        self.verified_runtimes += 1


def run_preflight() -> None:
    case = {
        "id": "marketplace-preflight",
        "turns": [{
            "prompt": (
                "The design is approved. Implement JSON output across src/parser.py, "
                "src/cli.py, and tests/test_cli.py, run tests, and do not pause."
            ),
            "dispatch_when": "immediate",
        }],
        "fixture": "python-cli",
        "expected_decisions": [{"after_turn": 1, "triggered": True}],
        "task_type": "feature",
        "workflow_variant": "implementation-basic",
        "expected_record_checkpoints": [{
            "after_turn": 1,
            "records": [{
                "role": "run-1", "status": "success", "start_mode": "planned",
                "superseded_by_role": None,
            }],
        }],
        "expected_run_count": 1,
        "expected_final_statuses": ["success"],
    }
    production = snapshot_production(REPOSITORY_ROOT)
    with tempfile.TemporaryDirectory(prefix="workflow-observatory-preflight-") as temporary:
        factory = MarketplaceRuntimeFactory()
        def execute_preflight():
            result = _run_case(
                case,
                Path(temporary).resolve(strict=True),
                lifecycle=False,
                runtime_factory=factory,
            )
            factory.verify_all_integrity()
            return result

        run_with_production_guard(
            execute_preflight,
            lambda: assert_production_unchanged(production),
        )


def _find_diagnostic_case(case_id: str) -> tuple[dict, bool]:
    if case_id != DIAGNOSTIC_CASE_ID:
        raise LookupError(
            f"diagnostic case is fixed to {DIAGNOSTIC_CASE_ID}: {case_id}"
        )
    for mode, path in MANIFEST_PATHS.items():
        cases = json.loads(path.read_text(encoding="utf-8"))
        for case in cases:
            if case["id"] == case_id:
                return case, mode == "lifecycle"
    raise LookupError(f"unknown diagnostic case: {case_id}")


def run_diagnostic_case(case_id: str) -> dict:
    case, lifecycle = _find_diagnostic_case(case_id)
    destination = Path(
        tempfile.mkdtemp(prefix=f"workflow-observatory-diagnostic-{case_id}-")
    ).resolve(strict=True)
    print(f"Diagnostic workspace retained at {destination}", flush=True)
    factory = MarketplaceRuntimeFactory()
    production = snapshot_production(REPOSITORY_ROOT)

    def execute_diagnostic() -> dict:
        result = _run_case(
            case,
            destination,
            lifecycle=lifecycle,
            runtime_factory=factory,
        )
        factory.verify_all_integrity()
        return result

    return run_with_production_guard(
        execute_diagnostic,
        lambda: assert_production_unchanged(production),
    )


def run_parallel_mode(arguments: argparse.Namespace) -> int:
    if arguments.resume_run_root is None:
        run_root = Path(
            tempfile.mkdtemp(
                prefix=f"workflow-observatory-parallel-{arguments.parallel}-"
            )
        ).resolve(strict=True)
        resume_run_root = None
    else:
        run_root = arguments.resume_run_root.expanduser().resolve(strict=True)
        resume_run_root = run_root
    source_codex_home = Path(
        os.environ.get("CODEX_HOME", Path.home() / ".codex")
    ).expanduser().resolve(strict=True)
    selected_codex = shutil.which("codex")
    if selected_codex is None:
        raise RuntimeError("Codex executable is unavailable")
    codex_executable = Path(selected_codex).resolve(strict=True)
    manifests = {
        name: json.loads(path.read_text(encoding="utf-8"))
        for name, path in MANIFEST_PATHS.items()
    }
    result = run_parallel_evaluation(
        repository_root=exact_git_repository_root(REPOSITORY_ROOT),
        manifests=manifests,
        result_destinations=(
            RESULT_PATHS if arguments.parallel == "formal" else None
        ),
        options=ParallelOptions(
            run_kind=arguments.parallel,
            run_root=run_root,
            source_codex_home=source_codex_home,
            codex_executable=codex_executable,
            resume_run_root=resume_run_root,
        ),
    )
    print(
        json.dumps(
            {
                "authoritative": (
                    result.run_kind == "formal"
                    and result.status == "committed"
                ),
                "run_kind": result.run_kind,
                "run_root": str(result.run_root),
                "status": result.status,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 1 if result.status == "failed" else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--diagnostic-case")
    mode.add_argument("--sweep", action="store_true")
    mode.add_argument(
        "--parallel",
        choices=("diagnostic", "discovery", "formal"),
    )
    parser.add_argument("--resume-run-root", type=Path)
    arguments = parser.parse_args()
    if arguments.resume_run_root is not None and arguments.parallel is None:
        parser.error("--resume-run-root requires --parallel")
    diagnostic_requested = arguments.diagnostic_case is not None
    if (
        diagnostic_requested
        and arguments.diagnostic_case != DIAGNOSTIC_CASE_ID
    ):
        parser.error(
            "diagnostic case is fixed to "
            f"{DIAGNOSTIC_CASE_ID}: {arguments.diagnostic_case}"
        )
    validate_marketplace_manifest_hashes()
    if arguments.preflight:
        run_preflight()
        print("Workflow Observatory marketplace preflight passed (one start, one finish).")
        return 0
    if diagnostic_requested:
        try:
            result = run_diagnostic_case(arguments.diagnostic_case)
        except LookupError as error:
            parser.error(str(error))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.sweep:
        destination = Path(
            tempfile.mkdtemp(prefix="workflow-observatory-discovery-sweep-")
        ).resolve(strict=True)
        print(f"Discovery sweep workspace retained at {destination}", flush=True)
        factory = MarketplaceRuntimeFactory()
        report = run_discovery_sweep(
            REPOSITORY_ROOT,
            manifest_paths=MANIFEST_PATHS,
            runtime_factory=factory,
            case_safety_check=factory.verify_case_safety,
            destination=destination,
        )
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    if arguments.parallel is not None:
        return run_parallel_mode(arguments)
    run_suite(
        REPOSITORY_ROOT,
        repository_root=exact_git_repository_root(REPOSITORY_ROOT),
        manifest_paths=MANIFEST_PATHS,
        result_destinations=RESULT_PATHS,
        runtime_factory=MarketplaceRuntimeFactory(),
        coordinator_role="serial-coordinator",
    )
    print(
        "Revision 6 frozen evaluations passed and paired results were committed "
        "through the atomic result manifest."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
