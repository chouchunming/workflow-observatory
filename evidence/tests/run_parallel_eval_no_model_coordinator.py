#!/usr/bin/env python3
"""Test-only coordinator for the real-process no-model integration boundary."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
from typing import Sequence

from scripts import workflow_eval_sharding as sharding


LANES = ("E1", "E2", "E3", "APP")


def _load_manifests(repository_root: Path):
    root = repository_root / "evidence/tests/skill_evals"
    return {
        "forward": json.loads(
            (root / "observing_workflows_cases.json").read_text(
                encoding="utf-8"
            )
        ),
        "lifecycle": json.loads(
            (
                root
                / "observing_workflows_lifecycle_cases.json"
            ).read_text(encoding="utf-8")
        ),
    }


def _encoded_key(key: sharding.CaseKey) -> str:
    return f"{key.mode}:{key.ordinal}:{key.case_id}"


class _IntegrityProbe:
    def __init__(self, run_root: Path) -> None:
        self._run_root = run_root
        self.environment_roots: list[str] = []
        self.teardown_preceded_validation = True
        self.bootstrap_absent_during_validation = True
        self.production_integrity_delegations = 0

    def __call__(self, command, environment, *, expected_records):
        teardown = self._run_root / "coordinator/teardown.json"
        bootstrap = self._run_root / "coordinator/auth-bootstrap"
        self.teardown_preceded_validation = (
            self.teardown_preceded_validation and teardown.is_file()
        )
        self.bootstrap_absent_during_validation = (
            self.bootstrap_absent_during_validation
            and not bootstrap.exists()
        )
        case_root = Path(environment["HOME"]).parent
        if not case_root.is_dir():
            raise ValueError("case evidence root is unavailable at validation")
        for name in (
            "workspace",
            "store",
            "home",
            "tmp",
            "config",
            "cache",
            "output",
        ):
            if not (case_root / name).is_dir():
                raise ValueError(
                    f"case environment is unavailable at validation: {name}"
                )
        if Path(environment["CODEX_HOME"]).exists():
            raise ValueError("case Codex home survived coordinator teardown")
        if command[-1] != "integrity":
            raise ValueError("integrity command shape changed")
        marker = case_root / "output/no-model-environment.json"
        payload = json.loads(marker.read_text(encoding="ascii"))
        if Path(payload["root"]) != case_root:
            raise ValueError("case environment marker is stale")
        self.environment_roots.append(str(case_root))
        result = sharding._production_integrity_runner(
            command,
            environment,
            expected_records=expected_records,
        )
        self.production_integrity_delegations += 1
        return result


def _inject_active_ownership(
    *,
    repository_root: Path,
    manifests,
    options: sharding.ParallelOptions,
) -> sharding.EpochPlan:
    plan, _transport, _snapshot, _frozen, _digests = (
        sharding._parallel_plan_inputs(
            repository_root=repository_root,
            manifests=manifests,
            options=options,
        )
    )
    options.run_root.mkdir(mode=0o700)
    cases = options.run_root / "cases"
    cases.mkdir(mode=0o700)
    assignment = plan.assignments[0]
    paths = sharding.paths_for_case(options.run_root, assignment)
    paths.root.mkdir(mode=0o700)
    paths.cleanup.mkdir(mode=0o700)
    paths.codex_home.mkdir(mode=0o700)
    credential = paths.codex_home / "auth.json"
    credential.write_text('{"fixture":"recovery"}\n', encoding="ascii")
    credential.chmod(0o600)
    root_metadata = paths.root.stat()
    home_metadata = paths.codex_home.stat()
    ownership = sharding.CaseAuthOwnership(
        schema_version=1,
        epoch_id=plan.epoch_id,
        run_kind=plan.run_kind,
        case=assignment.key,
        case_root_device=root_metadata.st_dev,
        case_root_inode=root_metadata.st_ino,
        codex_home_device=home_metadata.st_dev,
        codex_home_inode=home_metadata.st_ino,
    )
    sharding._atomic_write_record(
        paths.cleanup / "ownership.json", asdict(ownership)
    )
    return plan


def _process_summary(
    *,
    run_root: Path,
    plan: sharding.EpochPlan,
    result: sharding.ParallelRunResult,
    probe: _IntegrityProbe,
    writer_lease_acquisitions: int,
    writer_authority_issuances: int,
    transport_binding_paths: list[str],
    launched_lanes: list[str],
) -> dict[str, object]:
    lane_pids = {}
    all_workers_joined = True
    lane_case_keys = {}
    for lane in LANES:
        record = run_root / f"coordinator/process-groups/{lane}.json"
        if record.is_file():
            payload = json.loads(record.read_text(encoding="ascii"))
            lane_pids[lane] = payload["pid"]
            try:
                os.waitpid(payload["pid"], os.WNOHANG)
            except ChildProcessError:
                pass
            else:
                all_workers_joined = False
        worker_root = (
            run_root / "app-server"
            if lane == "APP"
            else run_root / "workers" / lane
        )
        terminal_keys = []
        for path in sorted((worker_root / "progress").glob("*.json")):
            payload = json.loads(path.read_text(encoding="ascii"))
            if (
                payload.get("type") == "case-terminal"
                and payload.get("status") == "success"
            ):
                terminal_keys.append(
                    _encoded_key(
                        sharding._decode_case_key(
                            payload["case"], "test progress case"
                        )
                    )
                )
        lane_case_keys[lane] = terminal_keys

    sealed_keys = []
    for assignment in plan.assignments:
        paths = sharding.paths_for_case(run_root, assignment)
        if (paths.sealed / "case-commit.json").is_file():
            sealed_keys.append(_encoded_key(assignment.key))

    aggregate = {}
    if result.validated is not None:
        rows = sharding.aggregate_committed_cases(result.validated)
        aggregate = {
            "forward": len(rows.forward_rows),
            "lifecycle": len(rows.lifecycle_rows),
        }

    recovery_producer = None
    first_paths = sharding.paths_for_case(
        run_root, plan.assignments[0]
    )
    tombstone = first_paths.cleanup / "tombstone.json"
    if tombstone.is_file():
        recovery_producer = json.loads(
            tombstone.read_text(encoding="ascii")
        ).get("producer")
    replacement = first_paths.codex_home / "replacement-marker"
    worker_violation_root = (
        run_root / "coordinator/worker-writer-violations"
    )
    worker_writer_violations = (
        sorted(
            str(path.relative_to(run_root))
            for path in worker_violation_root.rglob("*")
        )
        if worker_violation_root.exists()
        else []
    )
    diagnostic_scope = None
    if plan.run_kind == "diagnostic":
        diagnostic_scope = (
            sharding._encode_diagnostic_execution_scope(
                sharding._read_diagnostic_execution_scope(
                    coordinator_root=run_root / "coordinator",
                    plan=plan,
                )
            )
        )
    return {
        "status": result.status,
        "sealed_keys": sealed_keys,
        "launched_lanes": launched_lanes,
        "lane_pids": lane_pids,
        "process_group_lanes": sorted(lane_pids),
        "all_workers_joined": all_workers_joined,
        "lane_case_keys": lane_case_keys,
        "diagnostic_scope": diagnostic_scope,
        "aggregate": aggregate,
        "writer_lease_acquisitions": writer_lease_acquisitions,
        "writer_authority_issuances": writer_authority_issuances,
        "validation_environment_count": len(probe.environment_roots),
        "validation_environment_roots": probe.environment_roots,
        "teardown_preceded_validation": (
            probe.teardown_preceded_validation
        ),
        "bootstrap_absent_during_validation": (
            probe.bootstrap_absent_during_validation
        ),
        "production_integrity_delegations": (
            probe.production_integrity_delegations
        ),
        "sealed_codex_executable_path": (
            transport_binding_paths[0]
            if (
                transport_binding_paths
                and len(set(transport_binding_paths)) == 1
            )
            else None
        ),
        "transport_binding_launch_count": len(transport_binding_paths),
        "worker_writer_violations": worker_writer_violations,
        "recovery_producer": recovery_producer,
        "collision_replacement_retained": replacement.is_file(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a test-only real-process no-model epoch."
    )
    parser.add_argument("--repository-root", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--source-codex-home", required=True, type=Path)
    parser.add_argument("--codex-executable", required=True, type=Path)
    parser.add_argument(
        "--run-kind",
        required=True,
        choices=("diagnostic", "discovery", "formal"),
    )
    parser.add_argument("--forward-result", required=True, type=Path)
    parser.add_argument("--lifecycle-result", required=True, type=Path)
    parser.add_argument("--inject-active-ownership", action="store_true")
    parser.add_argument("--inject-collision", action="store_true")
    arguments = parser.parse_args(argv)

    repository_root = arguments.repository_root.resolve(strict=True)
    run_root = arguments.run_root.resolve(strict=False)
    manifests = _load_manifests(repository_root)
    options = sharding.ParallelOptions(
        run_kind=arguments.run_kind,
        run_root=run_root,
        source_codex_home=arguments.source_codex_home.resolve(
            strict=True
        ),
        codex_executable=arguments.codex_executable.resolve(strict=True),
        requested_model="no-model-integration",
        requested_reasoning_effort="minimal",
        resume_run_root=(
            run_root if arguments.inject_active_ownership else None
        ),
    )
    injected_plan = None
    if arguments.inject_active_ownership:
        injected_plan = _inject_active_ownership(
            repository_root=repository_root,
            manifests=manifests,
            options=options,
        )

    probe = _IntegrityProbe(run_root)
    captured_plan: list[sharding.EpochPlan] = []
    transport_binding_paths: list[str] = []
    launched_lanes: list[str] = []
    expected_lanes = (
        ("E3",)
        if arguments.run_kind == "diagnostic"
        else LANES
    )

    def worker_command_factory(
        lane, plan, bound_options, snapshot_root
    ):
        if not captured_plan:
            captured_plan.append(plan)
        elif captured_plan[0] != plan:
            raise ValueError("test coordinator observed multiple plans")
        transport_payload, transport_content = sharding._read_canonical_record(
            bound_options.run_root / "coordinator/transport-config.json",
            "test coordinator sealed transport config",
            byte_cap=64 * 1024,
        )
        try:
            sealed_transport = sharding.ResolvedTransportConfig(
                **transport_payload
            )
        except TypeError:
            raise ValueError(
                "test coordinator sealed transport fields changed"
            ) from None
        if (
            sharding.transport_config_bytes(sealed_transport)
            != transport_content
        ):
            raise ValueError(
                "test coordinator sealed transport bytes changed"
            )
        sealed_executable = sharding.verify_codex_executable(
            sealed_transport
        )
        expected_executable = arguments.codex_executable.resolve(
            strict=True
        )
        if sealed_executable != expected_executable:
            raise ValueError(
                "test coordinator did not seal the configured sentinel"
            )
        launched_lanes.append(lane)
        transport_binding_paths.append(str(sealed_executable))
        worker_root = (
            bound_options.run_root / "app-server"
            if lane == "APP"
            else bound_options.run_root / "workers" / lane
        )
        worker_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if lane != "APP":
            worker_root.parent.chmod(0o700)
        worker_root.chmod(0o700)
        command = [
            sys.executable,
            "-m",
            "tests.run_parallel_eval_no_model_worker",
            "--lane",
            lane,
            "--run-root",
            str(bound_options.run_root),
            "--snapshot-root",
            str(snapshot_root),
            "--epoch-id",
            plan.epoch_id,
        ]
        if arguments.inject_collision and lane == "E1":
            command.append("--inject-collision")
        return tuple(command)

    dependencies = sharding.CoordinatorDependencies(
        worker_command_factory=worker_command_factory,
        integrity_runner=probe,
    )
    writer_lease_acquisitions = 0
    writer_authority_issuances = 0
    original_acquire = sharding.ResultWriterLease.__dict__["acquire"]
    original_acquire_function = original_acquire.__func__
    original_authority = sharding.ResultWriterLease.__dict__["authority"]

    def counted_acquire(cls, *args, **kwargs):
        nonlocal writer_lease_acquisitions
        lease = original_acquire_function(cls, *args, **kwargs)
        writer_lease_acquisitions += 1
        return lease

    def counted_authority(self, *args, **kwargs):
        nonlocal writer_authority_issuances
        authority = original_authority(self, *args, **kwargs)
        writer_authority_issuances += 1
        return authority

    sharding.ResultWriterLease.acquire = classmethod(counted_acquire)
    sharding.ResultWriterLease.authority = counted_authority
    try:
        result = sharding.run_parallel_evaluation(
            repository_root=repository_root,
            manifests=manifests,
            result_destinations=(
                {
                    "forward": arguments.forward_result.resolve(
                        strict=False
                    ),
                    "lifecycle": arguments.lifecycle_result.resolve(
                        strict=False
                    ),
                }
                if arguments.run_kind == "formal"
                else None
            ),
            options=options,
            dependencies=dependencies,
        )
    finally:
        sharding.ResultWriterLease.acquire = original_acquire
        sharding.ResultWriterLease.authority = original_authority

    plan_payload, _content = sharding._read_canonical_record(
        run_root / "coordinator/epoch-plan.json",
        "test coordinator sealed plan",
        byte_cap=64 * 1024,
    )
    plan = sharding._decode_epoch_plan_record(plan_payload)
    if injected_plan is not None and plan != injected_plan:
        raise ValueError("recovery plan changed between injection and run")
    if captured_plan and captured_plan[0] != plan:
        raise ValueError("launched worker plan differs from sealed plan")
    if tuple(launched_lanes) != expected_lanes:
        raise ValueError("test coordinator observed unexpected launch lanes")
    summary = _process_summary(
        run_root=run_root,
        plan=plan,
        result=result,
        probe=probe,
        writer_lease_acquisitions=writer_lease_acquisitions,
        writer_authority_issuances=writer_authority_issuances,
        transport_binding_paths=transport_binding_paths,
        launched_lanes=launched_lanes,
    )
    sys.stdout.write(
        json.dumps(
            summary,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
