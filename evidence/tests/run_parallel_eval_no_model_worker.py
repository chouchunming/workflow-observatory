#!/usr/bin/env python3
"""Test-only real-process worker that seals cases without starting a model."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence

from scripts import run_observing_workflows_eval_worker as worker
from scripts import workflow_eval_sharding as sharding
from scripts.run_observing_workflows_task9_eval import CaseExecution


@contextmanager
def _worker_writer_poison(run_root: Path, lane: str):
    original_acquire = sharding.ResultWriterLease.__dict__["acquire"]

    def poisoned_acquire(cls, *_args, **_kwargs):
        violations = (
            run_root / "coordinator/worker-writer-violations"
        )
        violations.mkdir(parents=True, mode=0o700, exist_ok=True)
        violations.chmod(0o700)
        sharding._atomic_write_record(
            violations / f"{lane}.json",
            {
                "lane": lane,
                "pid": os.getpid(),
                "type": "result-writer-acquire",
            },
        )
        raise AssertionError(
            "worker attempted to acquire result writer lease"
        )

    sharding.ResultWriterLease.acquire = classmethod(poisoned_acquire)
    try:
        yield
    finally:
        sharding.ResultWriterLease.acquire = original_acquire


class _NoModelRuntimeFactory:
    poisoned = False

    def __call__(self, **_kwargs):
        raise AssertionError("the no-model case driver owns the fake runtime")

    def cleanup_case(self, _paths):
        raise AssertionError("the no-model case driver owns auth cleanup")

    def close(self) -> None:
        return None


def _result_for_manifest(
    assignment: sharding.CaseAssignment,
    manifest_case: dict[str, object],
) -> dict[str, object]:
    if assignment.key.mode == "forward":
        decisions = [
            {
                **decision,
                "task_type": (
                    manifest_case["task_type"]
                    if decision["triggered"]
                    else None
                ),
                "workflow_variant": (
                    manifest_case["workflow_variant"]
                    if decision["triggered"]
                    else None
                ),
            }
            for decision in manifest_case["expected_decisions"]
        ]
        return {
            "id": manifest_case["id"],
            "decisions": decisions,
            "record_checkpoints": manifest_case[
                "expected_record_checkpoints"
            ],
            "run_count": manifest_case["expected_run_count"],
            "draft_count": 0,
            "final_statuses": manifest_case["expected_final_statuses"],
        }
    return {
        "id": manifest_case["id"],
        "record_checkpoints": manifest_case[
            "expected_record_checkpoints"
        ],
        "run_count": manifest_case["expected_run_count"],
        "draft_count": manifest_case["expected_draft_count"],
        "final_statuses": manifest_case["expected_final_statuses"],
        "failure_disclosed": manifest_case[
            "expect_failure_disclosure"
        ],
        "selected_command": manifest_case[
            "expected_selected_command"
        ],
    }


class _NoModelCaseDriver:
    def __init__(
        self,
        *,
        plan: sharding.EpochPlan,
        snapshot_root: Path,
        collision_case_id: str | None,
        execute_staged_cli_probe: bool,
    ) -> None:
        self._plan = plan
        self._marketplace = worker._captured_marketplace_root(snapshot_root)
        self._collision_case_id = collision_case_id
        self._execute_staged_cli_probe = execute_staged_cli_probe

    def __call__(
        self,
        *,
        assignment,
        manifest_case,
        paths,
        runtime_factory,
        event_sink,
    ):
        if runtime_factory.poisoned:
            raise ValueError("no-model runtime factory is poisoned")
        if assignment not in self._plan.assignments:
            raise ValueError("no-model assignment differs from the epoch plan")
        if manifest_case.get("id") != assignment.key.case_id:
            raise ValueError("no-model manifest differs from the assignment")
        if paths != sharding.paths_for_case(
            paths.root.parent.parent, assignment
        ):
            raise ValueError("no-model case paths are non-canonical")

        worker._prepare_case_directories(paths)
        staged = sharding.stage_marketplace_for_case(
            read_only_snapshot=self._marketplace,
            destination=paths.staging / "marketplace",
            expected_marketplace_sha256=(
                self._plan.fingerprints.marketplace_sha256
            ),
        )
        if self._execute_staged_cli_probe:
            probe = subprocess.run(
                [
                    sys.executable,
                    str(
                        staged
                        / "plugins/workflow-observer/scripts/"
                        "workflow_observer_cli.py"
                    ),
                ],
                cwd=paths.workspace,
                env={
                    "HOME": str(paths.home),
                    "PATH": os.environ.get("PATH", ""),
                    "TMPDIR": str(paths.tmp),
                },
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5,
                check=False,
            )
            if probe.returncode != 0:
                raise ValueError("staged CLI execution probe failed")
        worker._write_portable_store_config(paths.home, paths.store)
        installed = sharding.install_case_auth(
            bootstrap=worker._discover_auth_bootstrap(paths),
            plan=self._plan,
            assignment=assignment,
            paths=paths,
        )
        event_sink("model-started", os.getpid(), os.getpgrp())
        worker.cleanup_case_auth(installed=installed, paths=paths)

        if assignment.key.case_id == self._collision_case_id:
            owned = paths.root / "collision-owned-codex-home"
            paths.codex_home.rename(owned)
            paths.codex_home.mkdir(mode=0o700)
            marker = paths.codex_home / "replacement-marker"
            marker.write_text("replacement\n", encoding="ascii")
            marker.chmod(0o600)

        environment_record = {
            "case": asdict(assignment.key),
            "lane": assignment.lane,
            "pid": os.getpid(),
            "root": str(paths.root),
            "workspace": str(paths.workspace),
            "store": str(paths.store),
            "home": str(paths.home),
            "tmp": str(paths.tmp),
            "config": str(paths.config),
            "cache": str(paths.cache),
            "staged_marketplace": str(staged),
        }
        marker = paths.output / "no-model-environment.json"
        marker.write_text(
            json.dumps(
                environment_record,
                sort_keys=True,
                ensure_ascii=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="ascii",
        )
        marker.chmod(0o600)
        usage = sharding.TokenUsage(
            input_tokens=1,
            cached_input_tokens=0,
            output_tokens=1,
            reasoning_output_tokens=0,
            total_tokens=2,
        )
        return worker.DrivenCase(
            result=_result_for_manifest(assignment, manifest_case),
            execution=CaseExecution(
                terminal_status="completed",
                final_text="schema-valid no-model fixture",
                command_executions=(),
                observation_command_diagnostics=(),
                usage=usage,
            ),
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one test-only no-model parallel lane."
    )
    parser.add_argument(
        "--lane", required=True, choices=("E1", "E2", "E3", "APP")
    )
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--snapshot-root", required=True, type=Path)
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--resume-plan-hex", required=True)
    parser.add_argument("--inject-collision", action="store_true")
    parser.add_argument("--execute-staged-cli-probe", action="store_true")
    arguments = parser.parse_args(argv)

    run_root = sharding.canonical_run_root(arguments.run_root)
    payload, _content = sharding._read_canonical_record(
        run_root / "coordinator/epoch-plan.json",
        "sealed epoch plan",
        byte_cap=64 * 1024,
    )
    plan = sharding._decode_epoch_plan_record(payload)
    if plan.epoch_id != arguments.epoch_id:
        raise ValueError("test worker epoch differs from the sealed plan")
    try:
        resume_content = bytes.fromhex(arguments.resume_plan_hex)
        resume_payload = json.loads(resume_content)
    except (ValueError, json.JSONDecodeError):
        raise ValueError("test worker resume plan is invalid") from None
    if sharding.canonical_config_bytes(resume_payload) != resume_content:
        raise ValueError("test worker resume plan is non-canonical")
    resume = sharding._decode_resume_plan_record(
        resume_payload, plan=plan
    )
    lane_assignments = [
        assignment
        for assignment in plan.assignments
        if assignment.lane == arguments.lane
    ]
    collision_case_id = (
        lane_assignments[0].key.case_id
        if arguments.inject_collision
        else None
    )
    dependencies = sharding.WorkerDependencies(
        runtime_factory=_NoModelRuntimeFactory(),
        case_driver=_NoModelCaseDriver(
            plan=plan,
            snapshot_root=arguments.snapshot_root,
            collision_case_id=collision_case_id,
            execute_staged_cli_probe=(
                arguments.execute_staged_cli_probe
            ),
        ),
    )
    with _worker_writer_poison(run_root, arguments.lane):
        worker._run_worker_impl(
            lane=arguments.lane,
            plan=plan,
            run_root=run_root,
            snapshot_root=arguments.snapshot_root,
            resume=resume,
            dependencies=dependencies,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
