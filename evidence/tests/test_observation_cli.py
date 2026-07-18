from datetime import date
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import wiki_cli
from wiki_observations import ObservationError, derive_provenance


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CLI = REPOSITORY_ROOT / "wiki_cli.py"
SCOPE = """## Scope

- Goal: Verify the cross-workspace CLI.
- Included: Start, finish, report, and invalidate.
- Excluded: None.
"""
COMPLETION = """## Execution evidence

- Verification: CLI integration tests passed.
- Artifacts: wiki_cli.py and tests.

## Outcome and observation

- Outcome: Cross-workspace lifecycle completed.
- Observation: Machine-readable output remained stable.

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 1
defects_found: 0
rework_count: 0
rework_reason: none
```
"""


class ObservationCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.wiki_root = self.root / "central-wiki"
        (self.wiki_root / "wiki" / "tasks").mkdir(parents=True)
        (self.wiki_root / "raw").mkdir()
        self.subject_root = self.root / "example-project"
        self.subject_root.mkdir()
        subprocess.run(
            ["git", "init", "-q", str(self.subject_root)], check=True
        )
        (self.subject_root / "README.md").write_text("fixture\n", encoding="utf-8")
        subprocess.run(
            ["git", "-C", str(self.subject_root), "add", "README.md"], check=True
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(self.subject_root),
                "-c",
                "user.name=Observation Test",
                "-c",
                "user.email=observation@example.invalid",
                "commit",
                "-q",
                "-m",
                "fixture",
            ],
            check=True,
        )
        self.external_dir = self.root / "external-cwd"
        self.external_dir.mkdir()
        self.scope_file = self.root / "scope.md"
        self.scope_file.write_text(SCOPE, encoding="utf-8")
        self.scope_file.chmod(0o600)
        self.completion_file = self.root / "completion.md"
        self.completion_file.write_text(COMPLETION, encoding="utf-8")
        self.completion_file.chmod(0o600)
        self.provenance = derive_provenance(self.subject_root)
        self.valid_start_args = (
            "--title",
            "Cross-workspace CLI",
            "--subject-root",
            str(self.subject_root),
            "--agent-surface",
            "codex",
            "--start-mode",
            "planned",
            "--task-type",
            "feature",
            "--workflow-variant",
            "implementation-with-review",
            "--scope-from-file",
            str(self.scope_file),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def run_cli(self, command, *arguments, cwd=None):
        return subprocess.run(
            [
                sys.executable,
                str(CLI),
                "observe",
                "--wiki-root",
                str(self.wiki_root),
                command,
                *arguments,
            ],
            cwd=cwd or self.external_dir,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def start(self):
        result = self.run_cli("start", *self.valid_start_args)
        self.assertEqual((0, ""), (result.returncode, result.stderr))
        return result.stdout.strip()

    def test_start_stdout_is_only_run_id_from_external_cwd(self):
        result = self.run_cli(
            "start", *self.valid_start_args, cwd=self.external_dir
        )

        self.assertEqual(0, result.returncode)
        self.assertRegex(result.stdout, r"^obs-\d{8}-\d{6}-[0-9a-f]{6}\n$")
        self.assertEqual("", result.stderr)
        run_id = result.stdout.strip()
        self.assertTrue(
            (self.wiki_root / "wiki" / "observations" / f"{run_id}.md").exists()
        )

    def test_start_derives_provenance_without_persisting_subject_root(self):
        run_id = self.start()
        record = (
            self.wiki_root / "wiki" / "observations" / f"{run_id}.md"
        ).read_text(encoding="utf-8")

        self.assertIn('workspace: "example-project"', record)
        self.assertIn(f'workspace_id: "{self.provenance.workspace_id}"', record)
        self.assertNotIn(str(self.subject_root), record)

    def test_external_cwd_start_finish_report_and_invalidate(self):
        run_id = self.start()

        finished = self.run_cli(
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion_file),
            cwd=self.external_dir,
        )
        self.assertEqual(
            (0, f"finished {run_id}\n", ""),
            (finished.returncode, finished.stdout, finished.stderr),
        )

        report_before = self.run_cli(
            "report", "--workspace-id", self.provenance.workspace_id
        )
        self.assertEqual((0, ""), (report_before.returncode, report_before.stderr))
        self.assertIn("Success rate: 1/1 (100.0%)", report_before.stdout)

        invalidated = self.run_cli(
            "invalidate", run_id, "--reason", "temporary smoke fixture"
        )
        self.assertEqual(
            (0, f"invalidated {run_id}\n", ""),
            (invalidated.returncode, invalidated.stdout, invalidated.stderr),
        )
        report_after = self.run_cli(
            "report", "--workspace-id", self.provenance.workspace_id
        )
        self.assertIn("Invalidated: 1", report_after.stdout)
        self.assertIn("Success rate: 0/0", report_after.stdout)

    def test_invalid_observe_argument_uses_only_contract_prefix(self):
        result = self.run_cli("start", "--workspace-id", "BAD")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(
            result.stderr.startswith("observation validation error:")
        )
        self.assertNotIn("usage:", result.stderr.lower())

    def test_state_and_io_errors_use_fixed_exit_contract(self):
        missing_run = "obs-20260714-120000-aaaaaa"
        state = self.run_cli(
            "finish",
            missing_run,
            "--status",
            "success",
            "--from-file",
            str(self.completion_file),
        )
        self.assertEqual((3, ""), (state.returncode, state.stdout))
        self.assertTrue(state.stderr.startswith("observation state error:"))

        io_error = self.run_cli(
            "start",
            *self.valid_start_args[:-1],
            str(self.root / "missing-scope.md"),
        )
        self.assertEqual((4, ""), (io_error.returncode, io_error.stdout))
        self.assertTrue(io_error.stderr.startswith("observation io error:"))

    def test_payload_mode_and_date_errors_are_validation_failures(self):
        self.scope_file.chmod(0o644)
        mode_error = self.run_cli("start", *self.valid_start_args)
        self.assertEqual((2, ""), (mode_error.returncode, mode_error.stdout))
        self.assertIn("mode 0600", mode_error.stderr)

        date_error = self.run_cli("report", "--since", "2026-99-99")
        self.assertEqual((2, ""), (date_error.returncode, date_error.stdout))
        self.assertTrue(date_error.stderr.startswith("observation validation error:"))
        self.assertNotIn("usage:", date_error.stderr.lower())

    def test_superseded_transition_is_exposed_by_cli(self):
        first = self.start()
        second = self.start()

        superseded = self.run_cli(
            "finish",
            first,
            "--status",
            "superseded",
            "--superseded-by",
            second,
            "--from-file",
            str(self.completion_file),
        )

        self.assertEqual(
            (0, f"finished {first}\n", ""),
            (superseded.returncode, superseded.stdout, superseded.stderr),
        )


class SecurePayloadReaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.payload = self.root / "payload.md"
        self.payload.write_text(SCOPE, encoding="utf-8")
        self.payload.chmod(0o600)

    def tearDown(self):
        self.temporary.cleanup()

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO fixtures unavailable")
    def test_regular_payload_swapped_to_fifo_cannot_block(self):
        backup = self.root / "payload.backup"
        real_open = os.open
        swapped = False

        def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal swapped
            if Path(path) == self.payload and not swapped:
                swapped = True
                self.payload.rename(backup)
                os.mkfifo(self.payload, 0o600)
                self.payload.chmod(0o600)
                if not flags & os.O_NONBLOCK:
                    raise AssertionError("payload FIFO open would block")
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with mock.patch("wiki_cli.os.open", side_effect=swap_before_open):
            with self.assertRaises(ObservationError) as raised:
                wiki_cli._read_observation_payload(str(self.payload), "Scope payload")

        self.assertTrue(swapped)
        self.assertEqual("validation", raised.exception.kind)
        self.assertIn("changed while opening", str(raised.exception))

    def test_regular_payload_swapped_to_symlink_is_validation(self):
        backup = self.root / "payload.backup"
        outside = self.root / "outside.md"
        outside.write_text(SCOPE, encoding="utf-8")
        outside.chmod(0o600)
        real_open = os.open
        swapped = False

        def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal swapped
            if Path(path) == self.payload and not swapped:
                swapped = True
                self.payload.rename(backup)
                os.symlink(outside, self.payload)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with mock.patch("wiki_cli.os.open", side_effect=swap_before_open):
            with self.assertRaises(ObservationError) as raised:
                wiki_cli._read_observation_payload(str(self.payload), "Scope payload")

        self.assertTrue(swapped)
        self.assertEqual("validation", raised.exception.kind)
        self.assertIn("changed while opening", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
