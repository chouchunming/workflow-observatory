import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from tests import run_parallel_eval_no_model_coordinator as coordinator_runner
from tests import run_parallel_eval_no_model_worker as worker_runner
from scripts import workflow_eval_sharding as sharding

EVIDENCE_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = (
    EVIDENCE_ROOT
    / "dist/workflow-observatory-0.2.0-recovery.zip"
)
FIXTURES = EVIDENCE_ROOT / "tests/skill_evals"
LANES = ("E1", "E2", "E3", "APP")


def _key(payload):
    return (
        f"{payload['mode']}:{payload['ordinal']}:"
        f"{payload['case_id']}"
    )


class NoModelHarnessUnitTests(unittest.TestCase):
    def test_integrity_probe_delegates_to_production_runner(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary).resolve()
            coordinator = run_root / "coordinator"
            coordinator.mkdir()
            (coordinator / "teardown.json").write_text(
                "{}\n", encoding="ascii"
            )
            case_root = run_root / "case"
            for name in (
                "workspace",
                "store",
                "home",
                "tmp",
                "config",
                "cache",
                "output",
            ):
                (case_root / name).mkdir(parents=True, exist_ok=True)
            (case_root / "output/no-model-environment.json").write_text(
                json.dumps({"root": str(case_root)}) + "\n",
                encoding="ascii",
            )
            command = ("python", "captured-cli.py", "integrity")
            environment = {
                "HOME": str(case_root / "home"),
                "CODEX_HOME": str(case_root / "codex-home"),
            }
            parsed = {"records": 7, "invalidated": 3}
            probe = coordinator_runner._IntegrityProbe(run_root)

            with mock.patch.object(
                coordinator_runner.sharding,
                "_production_integrity_runner",
                return_value=parsed,
            ) as production_runner:
                result = probe(
                    command,
                    environment,
                    expected_records=7,
                )

            self.assertIs(parsed, result)
            production_runner.assert_called_once_with(
                command,
                environment,
                expected_records=7,
            )

    def test_worker_writer_poison_records_and_rejects_acquisition(self):
        poison = getattr(worker_runner, "_worker_writer_poison", None)
        self.assertIsNotNone(poison)
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary).resolve()
            with poison(run_root, "E1"):
                with self.assertRaisesRegex(
                    AssertionError,
                    "worker attempted to acquire result writer lease",
                ):
                    sharding.ResultWriterLease.acquire(
                        run_root,
                        "parallel-coordinator",
                        "formal",
                    )
            marker = (
                run_root
                / "coordinator/worker-writer-violations/E1.json"
            )
            self.assertEqual(
                {
                    "lane": "E1",
                    "pid": os.getpid(),
                    "type": "result-writer-acquire",
                },
                json.loads(marker.read_text(encoding="ascii")),
            )


class ParallelNoModelIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.repository = self.root / "fixture-repository"
        subprocess.run(
            ["git", "init", "-q", str(self.repository)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        (self.repository / "baseline.txt").write_text(
            "fixture baseline\n", encoding="utf-8"
        )
        archive_destination = (
            self.repository
            / "evidence/dist/workflow-observatory-0.2.0-recovery.zip"
        )
        archive_destination.parent.mkdir(parents=True)
        shutil.copy2(ARCHIVE, archive_destination)
        fixture_destination = (
            self.repository / "evidence/tests/skill_evals"
        )
        fixture_destination.mkdir(parents=True)
        for name in (
            "observing_workflows_cases.json",
            "observing_workflows_lifecycle_cases.json",
        ):
            shutil.copy2(FIXTURES / name, fixture_destination / name)
        self.results = self.repository / "evidence/results"
        self.results.mkdir(parents=True)
        (self.results / ".keep").write_text("", encoding="utf-8")

        self.source_codex_home = self.root / "source-codex-home"
        self.source_codex_home.mkdir(mode=0o700)
        (self.source_codex_home / "auth.json").write_text(
            '{"fixture":"no-model"}\n', encoding="utf-8"
        )
        (self.source_codex_home / "auth.json").chmod(0o600)

        self.sentinel_marker = self.root / "sentinel-codex-invoked"
        sentinel_bin = self.root / "sentinel-bin"
        sentinel_bin.mkdir()
        self.sentinel_codex = (sentinel_bin / "codex").resolve()
        self.sentinel_codex.write_text(
            "#!/bin/sh\n"
            'if [ "$#" -eq 1 ] && [ "$1" = "--version" ]; then\n'
            "  printf '%s\\n' 'codex-cli no-model-sentinel'\n"
            "  exit 0\n"
            "fi\n"
            f"printf invoked > {self.sentinel_marker}\n"
            "exit 97\n",
            encoding="utf-8",
        )
        self.sentinel_codex.chmod(0o700)
        self.environment = dict(os.environ)
        self.environment["PATH"] = (
            str(sentinel_bin)
            + os.pathsep
            + self.environment.get("PATH", "")
        )

    def _run_coordinator(self, run_kind, *, extra=()):
        run_root = self.root / f"{run_kind}-run"
        command = [
            sys.executable,
            "-m",
            "tests.run_parallel_eval_no_model_coordinator",
            "--repository-root",
            str(self.repository),
            "--run-root",
            str(run_root),
            "--source-codex-home",
            str(self.source_codex_home),
            "--codex-executable",
            str(self.sentinel_codex),
            "--run-kind",
            run_kind,
            "--forward-result",
            str(self.results / f"{run_kind}-forward.json"),
            "--lifecycle-result",
            str(self.results / f"{run_kind}-lifecycle.json"),
            *extra,
        ]
        completed = subprocess.run(
            command,
            cwd=EVIDENCE_ROOT,
            env=self.environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
        self.assertEqual(
            0,
            completed.returncode,
            completed.stdout + completed.stderr,
        )
        try:
            summary = json.loads(completed.stdout)
        except json.JSONDecodeError:
            self.fail(completed.stdout + completed.stderr)
        self.assertEqual(
            str(self.sentinel_codex),
            summary.get("sealed_codex_executable_path"),
        )
        self.assertEqual(
            1 if run_kind == "diagnostic" else 4,
            summary.get("transport_binding_launch_count"),
        )
        self.assertEqual([], summary.get("worker_writer_violations"))
        self.assertEqual(
            (
                28
                if run_kind in ("discovery", "formal") and not extra
                else 0
            ),
            summary.get("production_integrity_delegations"),
            summary,
        )
        return run_root, summary

    def _result_inventory(self):
        return sorted(
            str(path.relative_to(self.results))
            for path in self.results.rglob("*")
        )

    def test_diagnostic_real_process_runs_only_reviewed_refactor(self):
        run_root, summary = self._run_coordinator("diagnostic")
        plan = json.loads(
            (run_root / "coordinator/epoch-plan.json").read_text(
                encoding="ascii"
            )
        )

        self.assertEqual("diagnostic", summary["status"])
        self.assertEqual(["E3"], summary["launched_lanes"])
        self.assertEqual(
            ["forward:3:reviewed-refactor"],
            summary["sealed_keys"],
        )
        self.assertEqual(
            {
                "schema_version": 1,
                "epoch_id": plan["epoch_id"],
                "run_kind": "diagnostic",
                "target": {
                    "mode": "forward",
                    "ordinal": 3,
                    "case_id": "reviewed-refactor",
                },
                "lane": "E3",
            },
            summary["diagnostic_scope"],
        )
        self.assertEqual(["E3"], summary["process_group_lanes"])
        self.assertEqual(1, summary["transport_binding_launch_count"])
        self.assertEqual(0, summary["writer_lease_acquisitions"])
        self.assertEqual(0, summary["writer_authority_issuances"])
        self.assertFalse(self.sentinel_marker.exists())
        self.assertFalse(
            (run_root / "workers/E3/sealed/shard-commit.json").exists()
        )
        self.assertEqual([".keep"], self._result_inventory())

    def test_real_processes_cover_all_28_cases(self):
        run_root, summary = self._run_coordinator("formal")

        plan = json.loads(
            (run_root / "coordinator/epoch-plan.json").read_text(
                encoding="ascii"
            )
        )
        planned = [_key(row["key"]) for row in plan["assignments"]]
        self.assertEqual(28, len(planned))
        self.assertEqual(28, len(set(planned)))
        self.assertEqual("committed", summary["status"], summary)
        self.assertEqual(planned, summary["sealed_keys"])
        self.assertEqual({"forward": 20, "lifecycle": 8}, summary["aggregate"])

        lane_pids = summary["lane_pids"]
        self.assertEqual(set(LANES), set(lane_pids))
        self.assertEqual(4, len(set(lane_pids.values())))
        self.assertTrue(summary["all_workers_joined"])
        self.assertNotIn(os.getpid(), lane_pids.values())
        self.assertEqual(1, summary.get("writer_lease_acquisitions"))
        self.assertEqual(1, summary.get("writer_authority_issuances"))

        expected_lane_counts = {"E1": 8, "E2": 8, "E3": 8, "APP": 4}
        for lane in LANES:
            expected = [
                _key(row["key"])
                for row in plan["assignments"]
                if row["lane"] == lane
            ]
            self.assertEqual(
                expected_lane_counts[lane], len(expected), lane
            )
            self.assertEqual(expected, summary["lane_case_keys"][lane])
            worker_root = (
                run_root / "app-server"
                if lane == "APP"
                else run_root / "workers" / lane
            )
            progress = sorted((worker_root / "progress").glob("*.json"))
            acks = sorted((worker_root / "acks").glob("*.json"))
            expected_records = 19 if lane != "APP" else 11
            self.assertEqual(expected_records, len(progress), lane)
            self.assertEqual(
                [path.name for path in progress],
                [path.name for path in acks],
                lane,
            )
            terminal_payloads = [
                json.loads(path.read_text(encoding="ascii"))
                for path in progress
                if json.loads(path.read_text(encoding="ascii"))["type"]
                == "case-terminal"
            ]
            self.assertEqual(expected_lane_counts[lane], len(terminal_payloads))
            self.assertTrue(
                all(
                    payload["tombstone_receipt_sha256"]
                    for payload in terminal_payloads
                )
            )

        self.assertEqual(28, summary["validation_environment_count"])
        self.assertEqual(
            28, len(set(summary["validation_environment_roots"]))
        )
        self.assertTrue(summary["teardown_preceded_validation"])
        self.assertTrue(summary["bootstrap_absent_during_validation"])
        self.assertTrue(
            (run_root / "coordinator/teardown.json").is_file()
        )
        self.assertFalse(
            (run_root / "coordinator/auth-bootstrap").exists()
        )

        case_roots = sorted((run_root / "cases").iterdir())
        self.assertEqual(28, len(case_roots))
        environment_fields = {
            name: set()
            for name in (
                "root",
                "workspace",
                "store",
                "home",
                "tmp",
                "config",
                "cache",
                "staged_marketplace",
            )
        }
        for case_root in case_roots:
            tombstone = json.loads(
                (case_root / "cleanup/tombstone.json").read_text(
                    encoding="ascii"
                )
            )
            self.assertEqual("worker", tombstone["producer"])
            self.assertFalse((case_root / "codex-home").exists())
            marker = case_root / "output/no-model-environment.json"
            environment = json.loads(marker.read_text(encoding="ascii"))
            self.assertEqual(
                case_root,
                Path(environment["root"]),
            )
            self.assertEqual(
                lane_pids[environment["lane"]], environment["pid"]
            )
            for field in environment_fields:
                environment_fields[field].add(environment[field])
        self.assertTrue(
            all(len(values) == 28 for values in environment_fields.values())
        )

        pointer = self.results / "observing_workflows_results_commit.json"
        self.assertTrue(pointer.is_file())
        generation = json.loads(pointer.read_text(encoding="utf-8"))
        generation_root = (
            self.results / ".observing_workflows_result_generations"
        )
        self.assertEqual(
            {
                generation["files"]["forward"]["path"].split("/", 1)[1],
                generation["files"]["lifecycle"]["path"].split("/", 1)[1],
            },
            {path.name for path in generation_root.iterdir()},
        )
        self.assertFalse(self.sentinel_marker.exists())

    def test_discovery_twin_validates_without_result_writer(self):
        run_root, summary = self._run_coordinator("discovery")

        self.assertEqual("validated", summary["status"], summary)
        self.assertEqual(28, len(summary["sealed_keys"]))
        self.assertEqual({"forward": 20, "lifecycle": 8}, summary["aggregate"])
        self.assertEqual(0, summary.get("writer_lease_acquisitions"))
        self.assertEqual(0, summary.get("writer_authority_issuances"))
        self.assertEqual(4, len(set(summary["lane_pids"].values())))
        self.assertTrue(summary["all_workers_joined"])
        self.assertEqual(28, summary["validation_environment_count"])
        self.assertTrue(summary["teardown_preceded_validation"])
        self.assertTrue(summary["bootstrap_absent_during_validation"])
        self.assertTrue(
            (run_root / "coordinator/teardown.json").is_file()
        )
        self.assertEqual([".keep"], self._result_inventory())
        self.assertFalse(self.sentinel_marker.exists())

    def test_recovery_scrubs_active_ownership_only_case(self):
        run_root, summary = self._run_coordinator(
            "formal", extra=("--inject-active-ownership",)
        )

        self.assertEqual("failed", summary["status"], summary)
        self.assertEqual(1, summary.get("writer_lease_acquisitions"))
        self.assertEqual(1, summary.get("writer_authority_issuances"))
        self.assertEqual(4, len(set(summary["lane_pids"].values())))
        self.assertTrue(summary["all_workers_joined"])
        self.assertLess(len(summary["sealed_keys"]), 28)
        self.assertEqual(
            "coordinator-recovery",
            summary["recovery_producer"],
            summary,
        )
        case_roots = list((run_root / "cases").iterdir())
        recovered = []
        for case_root in case_roots:
            tombstone_path = case_root / "cleanup/tombstone.json"
            if not tombstone_path.is_file():
                continue
            tombstone_payload = json.loads(
                tombstone_path.read_text(encoding="ascii")
            )
            if tombstone_payload["producer"] == "coordinator-recovery":
                recovered.append((case_root, tombstone_payload))
        self.assertEqual(1, len(recovered))
        case_root, tombstone = recovered[0]
        self.assertFalse((case_root / "codex-home").exists())
        self.assertTrue(tombstone["scrubbed"])
        self.assertEqual("expected", tombstone["canonical_binding"])
        self.assertEqual([".keep"], self._result_inventory())
        self.assertFalse(self.sentinel_marker.exists())

    def test_namespace_collision_fails_closed_and_retains_replacement(self):
        run_root, summary = self._run_coordinator(
            "formal", extra=("--inject-collision",)
        )

        self.assertEqual("failed", summary["status"], summary)
        self.assertEqual(1, summary.get("writer_lease_acquisitions"))
        self.assertEqual(1, summary.get("writer_authority_issuances"))
        self.assertTrue(summary["collision_replacement_retained"])
        self.assertEqual(4, len(set(summary["lane_pids"].values())))
        self.assertTrue(summary["all_workers_joined"])
        replacements = list(
            (run_root / "cases").glob(
                "*/codex-home/replacement-marker"
            )
        )
        self.assertEqual(1, len(replacements))
        replacement = replacements[0]
        self.assertEqual(
            "replacement\n", replacement.read_text(encoding="ascii")
        )
        self.assertEqual([".keep"], self._result_inventory())
        self.assertFalse(self.sentinel_marker.exists())

    def test_production_worker_cli_exposes_no_test_driver_flag(self):
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_observing_workflows_eval_worker",
                "--help",
            ],
            cwd=EVIDENCE_ROOT,
            env=self.environment,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertNotIn("test-driver", completed.stdout)
        self.assertNotIn("no-model", completed.stdout)
        self.assertFalse(self.sentinel_marker.exists())


if __name__ == "__main__":
    unittest.main()
