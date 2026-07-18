import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from tests.observing_workflows_eval_harness import (
    PayloadAudit,
    after_draft_run,
    after_single_file_mutation_without_run,
    assert_payload_audit,
    assert_production_unchanged,
    build_payload_audit,
    build_fixture,
    inspect_store,
    normalize_records,
    payload_audit_environment,
    persist_results_atomically,
    release_gate,
    snapshot_production,
    wait_for_checkpoint,
)


class PayloadAuditTests(unittest.TestCase):
    def _write_target_cli(self, destination: Path) -> Path:
        target = destination / "target_cli.py"
        target.write_text(
            "import sys\n"
            "print('target stdout')\n"
            "print('target stderr', file=sys.stderr)\n"
            "raise SystemExit(7)\n",
            encoding="utf-8",
        )
        return target

    def _write_audit_rows(self, audit: PayloadAudit, rows: list[dict]) -> None:
        audit.log_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    def _row(self, flag: str, path: Path, *, mode=0o600, device=1, inode=1):
        return {
            "flag": flag,
            "path": str(path),
            "device": device,
            "inode": inode,
            "mode": mode,
            "regular": True,
        }

    def test_wrapper_audits_separate_deleted_payloads_without_changing_target_result(self):
        with tempfile.TemporaryDirectory() as destination:
            destination_path = Path(destination)
            target = self._write_target_cli(destination_path)
            audit = build_payload_audit("payload-case", destination_path, target)

            self.assertEqual(0o700, audit.root.stat().st_mode & 0o777)
            self.assertEqual(0o700, audit.payload_dir.stat().st_mode & 0o777)
            self.assertEqual(0o700, audit.wrapper_path.stat().st_mode & 0o777)
            audit_environment = payload_audit_environment(audit)
            self.assertNotIn("TMPDIR", audit_environment)
            self.assertEqual(
                str(audit.payload_dir),
                audit_environment["OBSERVATION_PAYLOAD_TMPDIR"],
            )
            environment = dict(os.environ)
            environment.update(audit_environment)
            for index, flag in enumerate(("--scope-from-file", "--from-file"), 1):
                payload = audit.payload_dir / f"payload-{index}.json"
                payload.write_text("secret payload must not be audited\n", encoding="utf-8")
                payload.chmod(0o600)
                result = subprocess.run(
                    [sys.executable, str(audit.wrapper_path), flag, str(payload)],
                    text=True,
                    capture_output=True,
                    env=environment,
                )
                self.assertEqual(7, result.returncode)
                self.assertEqual("target stdout\n", result.stdout)
                self.assertEqual("target stderr\n", result.stderr)
                payload.unlink()

            assert_payload_audit(audit, expected_scope_calls=1, expected_completion_calls=1)
            rows = [json.loads(line) for line in audit.log_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                {"flag", "path", "device", "inode", "mode", "regular"},
                set(rows[0]),
            )

    def test_wrapper_does_not_audit_help_call_that_cannot_consume_payload(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            target = self._write_target_cli(root)
            audit = build_payload_audit("help-case", root, target)
            payload = audit.payload_dir / "unused-payload.json"
            payload.write_text("not consumed\n", encoding="utf-8")
            payload.chmod(0o600)
            environment = dict(os.environ)
            environment.update(payload_audit_environment(audit))

            result = subprocess.run(
                [
                    sys.executable,
                    str(audit.wrapper_path),
                    "start",
                    "--scope-from-file",
                    str(payload),
                    "--help",
                ],
                text=True,
                capture_output=True,
                env=environment,
            )
            payload.unlink()

            self.assertEqual(7, result.returncode)
            assert_payload_audit(audit, 0, 0)

    def test_build_rejects_unsafe_ids_and_symlink_target(self):
        with tempfile.TemporaryDirectory() as destination:
            destination_path = Path(destination)
            target = self._write_target_cli(destination_path)
            symlink = destination_path / "target-link.py"
            symlink.symlink_to(target)

            with self.assertRaisesRegex(ValueError, "case id"):
                build_payload_audit("../escape", destination_path, target)
            with self.assertRaisesRegex(ValueError, "non-symlink"):
                build_payload_audit("symlink-case", destination_path, symlink)

    def test_rejects_non_0600_payload(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("mode-case", root, self._write_target_cli(root))
            self._write_audit_rows(
                audit,
                [self._row("--scope-from-file", root / "gone", mode=0o644)],
            )

            with self.assertRaisesRegex(AssertionError, "not a regular mode-0600 payload"):
                assert_payload_audit(audit, 1, 0)

    def test_rejects_reused_path_and_inode(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("reuse-case", root, self._write_target_cli(root))
            gone = root / "gone"
            self._write_audit_rows(
                audit,
                [
                    self._row("--scope-from-file", gone, device=2, inode=3),
                    self._row("--from-file", gone, device=2, inode=3),
                ],
            )

            with self.assertRaisesRegex(
                AssertionError,
                "audit row 2 reuses a payload path; audit row 2 reuses a payload inode",
            ):
                assert_payload_audit(audit, 1, 1)

    def test_rejects_surviving_observed_path(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("survivor-case", root, self._write_target_cli(root))
            surviving = root / "surviving-payload"
            surviving.write_text("still here\n", encoding="utf-8")
            surviving.chmod(0o600)
            details = surviving.stat()
            self._write_audit_rows(
                audit,
                [self._row(
                    "--scope-from-file",
                    surviving,
                    device=details.st_dev,
                    inode=details.st_ino,
                )],
            )

            with self.assertRaisesRegex(AssertionError, "payload still exists"):
                assert_payload_audit(audit, 1, 0)

    def test_rejects_payload_outside_case_tmpdir(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("outside-case", root, self._write_target_cli(root))
            self._write_audit_rows(
                audit,
                [self._row("--scope-from-file", root / "deleted-outside-tmpdir")],
            )

            with self.assertRaisesRegex(
                AssertionError,
                "audit row 1 payload is outside the case payload directory",
            ):
                assert_payload_audit(audit, 1, 0)

    def test_rejects_wrong_call_counts(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("counts-case", root, self._write_target_cli(root))

            with self.assertRaisesRegex(
                AssertionError,
                "scope calls: expected 1, got 0; completion calls: expected 2, got 0",
            ):
                assert_payload_audit(audit, 1, 2)

    def test_rejects_unknown_flag_even_when_expected_counts_match(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("unknown-flag-case", root, self._write_target_cli(root))
            self._write_audit_rows(
                audit,
                [self._row("--unexpected-file", audit.payload_dir / "deleted")],
            )

            with self.assertRaisesRegex(
                AssertionError,
                "total calls: expected 0, got 1; audit row 1 has invalid values",
            ):
                assert_payload_audit(audit, 0, 0)

    def test_rejects_invalid_typed_row_with_deterministic_diagnostic(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("typed-row-case", root, self._write_target_cli(root))
            row = self._row("--scope-from-file", audit.payload_dir / "deleted")
            row["path"] = []
            self._write_audit_rows(audit, [row])

            with self.assertRaisesRegex(
                AssertionError,
                "audit row 1 has invalid values",
            ):
                assert_payload_audit(audit, 1, 0)

    def test_rejects_nonempty_no_wrapper_tmpdir(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("leftover-case", root, self._write_target_cli(root))
            (audit.payload_dir / "orphan").write_text("leftover\n", encoding="utf-8")

            with self.assertRaisesRegex(
                AssertionError,
                "payload directory is not empty: orphan",
            ):
                assert_payload_audit(audit, 0, 0)

    def test_rejects_malformed_audit_log_with_deterministic_diagnostic(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("malformed-case", root, self._write_target_cli(root))
            audit.log_path.write_text("not-json\n", encoding="utf-8")

            with self.assertRaisesRegex(
                AssertionError,
                "audit log line 1 is invalid JSON",
            ):
                assert_payload_audit(audit, 0, 0)

    def test_rejects_non_object_audit_row_with_deterministic_diagnostic(self):
        with tempfile.TemporaryDirectory() as destination:
            root = Path(destination)
            audit = build_payload_audit("row-shape-case", root, self._write_target_cli(root))
            audit.log_path.write_text("[]\n", encoding="utf-8")

            with self.assertRaisesRegex(
                AssertionError,
                "audit row 1 has invalid fields",
            ):
                assert_payload_audit(audit, 0, 0)


class FixtureTests(unittest.TestCase):
    def test_builds_are_isolated_and_remain_below_destination(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_destination = Path(first)
            second_destination = Path(second)
            first_root = build_fixture("fixture-one", "python-cli", first_destination)
            second_root = build_fixture("fixture-two", "python-cli", second_destination)

            self.assertNotEqual(first_root, second_root)
            self.assertTrue((first_root / ".git").is_dir())
            self.assertTrue((second_root / ".git").is_dir())
            self.assertTrue(first_root.resolve().is_relative_to(first_destination.resolve()))
            self.assertTrue(second_root.resolve().is_relative_to(second_destination.resolve()))
            self.assertTrue(all(
                path.resolve().is_relative_to(first_destination.resolve())
                for path in first_destination.rglob("*")
            ))

            (first_root / "src" / "parser.py").write_text("changed = True\n", encoding="utf-8")
            self.assertNotEqual(
                (first_root / "src" / "parser.py").read_text(encoding="utf-8"),
                (second_root / "src" / "parser.py").read_text(encoding="utf-8"),
            )

    def test_case_id_cannot_escape_destination(self):
        with tempfile.TemporaryDirectory() as destination:
            with self.assertRaisesRegex(ValueError, "case id"):
                build_fixture("../escape", "empty", Path(destination))

    def test_all_fixture_kinds_have_an_initial_commit(self):
        with tempfile.TemporaryDirectory() as destination:
            for fixture in ("python-cli", "documentation", "wiki", "empty"):
                root = build_fixture(f"case-{fixture}", fixture, Path(destination))
                revision = subprocess.run(
                    ["git", "rev-parse", "HEAD"], cwd=root, check=True,
                    text=True, capture_output=True,
                ).stdout.strip()
                self.assertEqual(40, len(revision))

    def test_fixture_templates_cover_prompt_required_artifacts_and_commands(self):
        with tempfile.TemporaryDirectory() as destination:
            destination_path = Path(destination)
            python_root = build_fixture("artifact-python", "python-cli", destination_path)
            docs_root = build_fixture("artifact-docs", "documentation", destination_path)
            wiki_root = build_fixture("artifact-wiki", "wiki", destination_path)

            python_tests = subprocess.run(
                [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
                cwd=python_root, text=True, capture_output=True,
            )
            self.assertEqual(0, python_tests.returncode, python_tests.stderr)
            failure = subprocess.run(
                [sys.executable, "scripts/fail_task.py"],
                cwd=python_root, text=True, capture_output=True,
            )
            self.assertEqual(3, failure.returncode)
            self.assertEqual("deterministic task failure\n", failure.stderr)

            self.assertTrue((docs_root / "docs" / "guide.md").is_file())
            self.assertIn(
                "Documentatoin",
                (docs_root / "README.md").read_text(encoding="utf-8"),
            )

            required_wiki_paths = (
                "raw/source.md",
                "wiki/_index.md",
                "wiki/_overview.md",
                "wiki/_inbox.md",
                "wiki/inbox/parser-capture.md",
                "wiki/inbox/cli-capture.md",
                "wiki_cli.py",
            )
            for relative in required_wiki_paths:
                with self.subTest(relative=relative):
                    self.assertTrue((wiki_root / relative).is_file())
            inbox_check = subprocess.run(
                [sys.executable, "wiki_cli.py", "inbox", "--check"],
                cwd=wiki_root, text=True, capture_output=True,
            )
            self.assertEqual(0, inbox_check.returncode, inbox_check.stderr)
            self.assertIn("2 pending capture(s)", inbox_check.stdout)
            lint = subprocess.run(
                [sys.executable, "wiki_cli.py", "lint"],
                cwd=wiki_root, text=True, capture_output=True,
            )
            self.assertEqual(0, lint.returncode, lint.stderr)
            self.assertEqual("lint ok\n", lint.stdout)


class RecordInspectionTests(unittest.TestCase):
    def test_role_mapping_is_stable_and_supersession_uses_roles(self):
        role_map = {}
        first = normalize_records([{
            "run_id": "obs-b",
            "timestamp": "2026-01-02T00:00:00+00:00",
            "status": "draft",
            "start_mode": "planned",
            "superseded_by": None,
        }], role_map)
        second = normalize_records([{
            "run_id": "obs-a",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "status": "success",
            "start_mode": "planned",
            "superseded_by": None,
        }, {
            "run_id": "obs-b",
            "timestamp": "2026-01-02T00:00:00+00:00",
            "status": "superseded",
            "start_mode": "planned",
            "superseded_by": "obs-a",
        }], role_map)

        self.assertEqual("run-1", first[0]["role"])
        self.assertEqual(
            [
                {"role": "run-1", "status": "superseded", "start_mode": "planned", "superseded_by_role": "run-2"},
                {"role": "run-2", "status": "success", "start_mode": "planned", "superseded_by_role": None},
            ],
            second,
        )
        self.assertNotIn("obs-a", json.dumps(second))
        self.assertNotIn("obs-b", json.dumps(second))

    def test_inspect_store_counts_records_and_exposes_checkpoint_predicates(self):
        with tempfile.TemporaryDirectory() as temporary:
            wiki_root = Path(temporary)
            records = wiki_root / "wiki" / "observations"
            records.mkdir(parents=True)
            (records / "obs-one.md").write_text(
                "---\nrun_id: obs-one\ntimestamp: 2026-01-01T00:00:00+00:00\n"
                "status: draft\nstart_mode: late\nsuperseded_by:\n---\n",
                encoding="utf-8",
            )

            inspected = inspect_store(wiki_root)

            self.assertEqual(1, inspected["run_count"])
            self.assertEqual(1, inspected["draft_count"])
            self.assertEqual([], inspected["final_statuses"])
            self.assertTrue(after_draft_run(wiki_root))

    def test_single_file_mutation_predicate_requires_no_observation(self):
        with tempfile.TemporaryDirectory() as destination, tempfile.TemporaryDirectory() as store:
            workspace = build_fixture("late-case", "python-cli", Path(destination))
            wiki_root = Path(store)
            (workspace / "src" / "parser.py").write_text("changed = True\n", encoding="utf-8")

            self.assertTrue(after_single_file_mutation_without_run(workspace, wiki_root))
            observations = wiki_root / "wiki" / "observations"
            observations.mkdir(parents=True)
            (observations / "obs-one.md").write_text(
                "---\nrun_id: obs-one\nstatus: draft\nstart_mode: late\n---\n",
                encoding="utf-8",
            )
            self.assertFalse(after_single_file_mutation_without_run(workspace, wiki_root))


class GateTests(unittest.TestCase):
    def test_gate_timeout_is_reported(self):
        with self.assertRaisesRegex(TimeoutError, "timeout-case"):
            wait_for_checkpoint("timeout-case", lambda: False, timeout_seconds=0.02)

    def test_release_unblocks_fixture_gate(self):
        with tempfile.TemporaryDirectory() as destination:
            root = build_fixture("release-case", "python-cli", Path(destination))
            gate = subprocess.Popen(
                [sys.executable, str(root / "scripts" / "gate.py"), "release-case"],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            ready = root / ".eval-gates" / "release-case.ready"
            wait_for_checkpoint("release-case", ready.exists, timeout_seconds=2)
            release_gate("release-case")
            stdout, stderr = gate.communicate(timeout=2)

            self.assertEqual((0, "", ""), (gate.returncode, stdout, stderr))

    def test_active_case_id_collision_is_rejected_and_release_permits_reuse(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            build_fixture("collision-case", "empty", Path(first))
            with self.assertRaisesRegex(ValueError, "active gate case"):
                build_fixture("collision-case", "empty", Path(second))

            release_gate("collision-case")
            reused = build_fixture("collision-case", "empty", Path(second))

            self.assertTrue(reused.is_dir())

    def test_non_gate_case_id_can_be_reused_in_another_mode_destination(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            first_root = build_fixture(
                "shared-case", "empty", Path(first), include_gate=False
            )
            second_root = build_fixture(
                "shared-case", "empty", Path(second), include_gate=False
            )

            self.assertTrue(first_root.is_dir())
            self.assertTrue(second_root.is_dir())

    def test_non_gate_reuse_does_not_overwrite_an_active_gate_registration(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            gate_root = build_fixture("mixed-case", "empty", Path(first))
            non_gate_root = build_fixture(
                "mixed-case", "empty", Path(second), include_gate=False
            )

            release_gate("mixed-case")

            self.assertTrue(
                (gate_root / ".eval-gates" / "mixed-case.release").is_file()
            )
            self.assertFalse((non_gate_root / ".eval-gates").exists())

    def test_missing_fixture_root_is_pruned_before_case_id_reuse(self):
        with tempfile.TemporaryDirectory() as first:
            build_fixture("pruned-case", "empty", Path(first))

        with tempfile.TemporaryDirectory() as second:
            reused = build_fixture("pruned-case", "empty", Path(second))

            self.assertTrue(reused.is_dir())


class ProductionIntegrityTests(unittest.TestCase):
    def _production_error_payload(self, error: BaseException) -> dict:
        prefix = "production repository changed during evaluation: "
        message = str(error)
        self.assertTrue(message.startswith(prefix), message)
        return json.loads(message.removeprefix(prefix))

    def test_snapshot_reports_transient_add_tracked_modification_and_removal(self):
        content_secret = "FILE_CONTENT_SECRET"
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-change-summary", "empty", Path(destination))
            modified = repo / "modify-me.txt"
            removed = repo / "remove-me.txt"
            modified.write_text("original modified\n", encoding="utf-8")
            removed.write_text("original removed\n", encoding="utf-8")
            subprocess.run(
                ["git", "add", "modify-me.txt", "remove-me.txt"],
                cwd=repo, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "commit", "-m", "add production guard fixtures"],
                cwd=repo, check=True, capture_output=True,
            )
            snapshot = snapshot_production(repo)

            (repo / "added.txt").write_text(content_secret, encoding="utf-8")
            modified.write_text(content_secret, encoding="utf-8")
            removed.unlink()

            with self.assertRaises(AssertionError) as caught:
                assert_production_unchanged(snapshot)

        message = str(caught.exception)
        payload = self._production_error_payload(caught.exception)
        self.assertTrue(payload["status_changed"])
        self.assertRegex(payload["status_before_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(payload["status_after_sha256"], r"^[0-9a-f]{64}$")
        self.assertNotEqual(
            payload["status_before_sha256"], payload["status_after_sha256"]
        )
        self.assertEqual(1, payload["added_path_count"])
        self.assertEqual(1, payload["removed_path_count"])
        self.assertEqual(1, payload["modified_path_count"])
        self.assertEqual(
            [
                {"category": "added", "path": "added.txt"},
                {"category": "modified", "path": "modify-me.txt"},
                {"category": "removed", "path": "remove-me.txt"},
            ],
            payload["changed_paths"],
        )
        self.assertNotIn(content_secret, message)
        self.assertNotIn(str(repo), message)
        self.assertLess(len(message), 2048)

    def test_snapshot_change_summary_is_deterministic_and_path_bounded(self):
        content_secret = "LONG_PATH_CONTENT_SECRET"
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-path-summary", "empty", Path(destination))
            snapshot = snapshot_production(repo)
            for index in reversed(range(10)):
                name = f"{index:02d}-" + (chr(97 + index) * 180) + ".txt"
                (repo / name).write_text(content_secret, encoding="utf-8")

            messages = []
            for _ in range(2):
                with self.assertRaises(AssertionError) as caught:
                    assert_production_unchanged(snapshot)
                messages.append(str(caught.exception))

        self.assertEqual(messages[0], messages[1])
        payload = self._production_error_payload(AssertionError(messages[0]))
        self.assertEqual(10, payload["added_path_count"])
        self.assertEqual(0, payload["removed_path_count"])
        self.assertEqual(0, payload["modified_path_count"])
        self.assertLessEqual(len(payload["changed_paths"]), 8)
        rendered_paths = [entry["path"] for entry in payload["changed_paths"]]
        self.assertEqual(sorted(rendered_paths), rendered_paths)
        self.assertTrue(all(len(path) <= 134 for path in rendered_paths))
        self.assertTrue(all("…#" in path for path in rendered_paths))
        self.assertNotIn(content_secret, messages[0])
        self.assertNotIn(str(repo), messages[0])
        self.assertLess(len(messages[0]), 2048)

    def test_snapshot_unchanged_state_still_passes(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-unchanged", "empty", Path(destination))
            snapshot = snapshot_production(repo)

            assert_production_unchanged(snapshot)

    def test_snapshot_detects_second_change_to_already_dirty_tracked_file(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-dirty-content", "empty", Path(destination))
            readme = repo / "README.md"
            readme.write_text("first dirty value\n", encoding="utf-8")
            snapshot = snapshot_production(repo)

            readme.write_text("other dirty value\n", encoding="utf-8")

            with self.assertRaisesRegex(AssertionError, "production repository changed"):
                assert_production_unchanged(snapshot)

    def test_snapshot_detects_existing_observation_content_change(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-observation-content", "empty", Path(destination))
            observations = repo / "wiki" / "observations"
            observations.mkdir(parents=True)
            record = observations / "obs-existing.md"
            record.write_text("first record\n", encoding="utf-8")
            snapshot = snapshot_production(repo)

            record.write_text("other record\n", encoding="utf-8")

            with self.assertRaisesRegex(AssertionError, "production repository changed"):
                assert_production_unchanged(snapshot)

    def test_snapshot_detects_git_or_observation_name_changes(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production", "empty", Path(destination))
            observations = repo / "wiki" / "observations"
            observations.mkdir(parents=True)
            snapshot = snapshot_production(repo)
            assert_production_unchanged(snapshot)

            (observations / "obs-new.md").write_text("record\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "production repository changed"):
                assert_production_unchanged(snapshot)

    def test_results_are_written_only_after_integrity_check(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-results", "empty", Path(destination))
            result_path = repo / "results" / "baseline.json"
            rows = [{"id": "case", "decisions": []}]
            snapshot = snapshot_production(repo)

            self.assertFalse(result_path.exists())
            assert_production_unchanged(snapshot)
            self.assertFalse(result_path.exists())
            persist_results_atomically(result_path, rows)

            self.assertEqual(rows, json.loads(result_path.read_text(encoding="utf-8")))
            self.assertEqual([], list(result_path.parent.glob(".*.tmp")))

    def test_existing_result_survives_failed_integrity_check_until_later_success(self):
        with tempfile.TemporaryDirectory() as destination:
            repo = build_fixture("production-deferred-results", "empty", Path(destination))
            result_path = repo / "results" / "baseline.json"
            original = [{"id": "case", "decisions": []}]
            replacement = [{"id": "case", "decisions": [{"after_turn": 1}]}]
            persist_results_atomically(result_path, original)
            snapshot = snapshot_production(repo)

            readme = repo / "README.md"
            original_readme = readme.read_text(encoding="utf-8")
            readme.write_text("production mutation\n", encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "production repository changed"):
                assert_production_unchanged(snapshot)
            self.assertEqual(
                original,
                json.loads(result_path.read_text(encoding="utf-8")),
            )

            readme.write_text(original_readme, encoding="utf-8")
            assert_production_unchanged(snapshot)
            persist_results_atomically(result_path, replacement)

            self.assertEqual(
                replacement,
                json.loads(result_path.read_text(encoding="utf-8")),
            )


if __name__ == "__main__":
    unittest.main()
