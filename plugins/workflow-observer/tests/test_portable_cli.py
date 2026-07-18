import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
CLI = PLUGIN_ROOT / "scripts/workflow_observer_cli.py"
sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))
import workflow_observer_cli

SCOPE = """## Scope

- Goal: Verify portable lifecycle
- Included: CLI behavior
- Excluded: None
"""

COMPLETION = """## Execution evidence

- Verification: focused tests pass
- Artifacts: observation record

## Outcome and observation

- Outcome: Portable lifecycle completed
- Observation: Adapter behavior stayed deterministic

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 0
defects_found: 0
rework_count: 0
rework_reason: none
```
"""


def write_private(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o600)


def run_cli(home: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "WORKFLOW_OBSERVATORY_HOME": str(home),
    }
    return subprocess.run(
        [sys.executable, str(CLI), *arguments],
        cwd=PLUGIN_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )


class PortableCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.home = self.base / "home"
        self.subject = self.base / "subject with spaces;safe"
        self.subject.mkdir()
        self.scope = self.base / "scope.md"
        self.completion = self.base / "completion.md"
        write_private(self.scope, SCOPE)
        write_private(self.completion, COMPLETION)

    def tearDown(self):
        self.temporary.cleanup()

    def start(self, **overrides):
        fields = {
            "title": "Example",
            "task_type": "maintenance",
            "workflow_variant": "maintenance-basic",
        }
        fields.update(overrides)
        return run_cli(
            self.home,
            "start",
            "--title",
            fields["title"],
            "--subject-root",
            str(self.subject),
            "--agent-surface",
            "codex",
            "--start-mode",
            "planned",
            "--task-type",
            fields["task_type"],
            "--workflow-variant",
            fields["workflow_variant"],
            "--scope-from-file",
            str(self.scope),
        )

    def test_start_finish_validate_report(self):
        started = self.start()
        self.assertEqual("", started.stderr)
        run_id = started.stdout.strip()
        self.assertRegex(run_id, r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")

        finished = run_cli(
            self.home,
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        self.assertEqual((0, "", ""), (finished.returncode, finished.stdout, finished.stderr))
        validated = run_cli(self.home, "validate")
        self.assertEqual((0, "valid records=1 invalidated=0\n", ""),
                         (validated.returncode, validated.stdout, validated.stderr))
        report = run_cli(self.home, "report")
        self.assertEqual(0, report.returncode)
        self.assertIn("maintenance-basic", report.stdout)
        self.assertEqual("", report.stderr)

    def test_start_creates_only_private_portable_layout(self):
        self.assertEqual(0, self.start().returncode)
        root = self.home / "store"
        self.assertEqual(
            [
                "wiki",
                "wiki/observations",
                "wiki/observations/.locks",
                "wiki/observations/invalidations",
            ],
            sorted(
                str(path.relative_to(root))
                for path in root.rglob("*")
                if path.is_dir()
            ),
        )
        for directory in (root, root / "wiki", root / "wiki/observations",
                          root / "wiki/observations/.locks",
                          root / "wiki/observations/invalidations"):
            self.assertEqual(0, stat.S_IMODE(directory.stat().st_mode) & 0o077)

    def test_missing_store_read_only_commands_do_not_create_it(self):
        expected = {
            "report": "# Observation report\n",
            "validate": "valid records=0 invalidated=0\n",
            "integrity": "healthy records=0 invalidated=0\n",
        }
        for command, prefix in expected.items():
            with self.subTest(command=command):
                result = run_cli(self.home, command)
                self.assertEqual(0, result.returncode)
                self.assertTrue(result.stdout.startswith(prefix), result.stdout)
                self.assertEqual("", result.stderr)
                self.assertFalse((self.home / "store").exists())

    def test_finish_missing_store_is_state_error(self):
        result = run_cli(
            self.home,
            "finish",
            "obs-20260715-120000-abcdef",
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith("workflow observer state error:"))
        self.assertFalse((self.home / "store").exists())

    def test_missing_store_report_still_validates_filters(self):
        result = run_cli(self.home, "report", "--task-type", "not-a-task-type")
        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith("workflow observer validation error:"))
        self.assertFalse((self.home / "store").exists())

    def test_integrity_rejects_unexpected_entries_and_permissive_locks(self):
        run_id = self.start().stdout.strip()
        observations = self.home / "store/wiki/observations"
        unexpected = observations / "backup.tmp"
        unexpected.write_text("artifact", encoding="utf-8")
        rejected = run_cli(self.home, "integrity")
        self.assertEqual(2, rejected.returncode)
        self.assertEqual("", rejected.stdout)
        self.assertIn("workflow observer validation error:", rejected.stderr)
        unexpected.unlink()

        lock = observations / ".locks" / f"{run_id}.lock"
        lock.touch()
        lock.chmod(0o644)
        rejected = run_cli(self.home, "integrity")
        self.assertEqual(2, rejected.returncode)
        self.assertIn("lock permissions", rejected.stderr)

        lock.chmod(0o600)
        accepted = run_cli(self.home, "integrity")
        self.assertEqual((0, "healthy records=1 invalidated=0\n", ""),
                         (accepted.returncode, accepted.stdout, accepted.stderr))

    def test_integrity_rejects_existing_root_through_symlink_ancestor(self):
        outside = self.base / "outside"
        store = outside / "store"
        store.mkdir(parents=True)
        link = self.base / "linked-parent"
        link.symlink_to(outside, target_is_directory=True)
        self.home.mkdir()
        (self.home / "config.json").write_text(
            '{"schema_version":1,"adapter":"portable","root":"'
            + str(link / "store")
            + '"}',
            encoding="utf-8",
        )

        result = run_cli(self.home, "integrity")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith("workflow observer validation error:"))
        self.assertIn("symlink", result.stderr)
        self.assertEqual([], list(store.iterdir()))

    def test_start_rejects_symlink_ancestor_without_chmod_or_creation(self):
        outside = self.base / "outside"
        outside.mkdir(mode=0o755)
        outside.chmod(0o755)
        link = self.base / "linked-parent"
        link.symlink_to(outside, target_is_directory=True)
        self.home.mkdir()
        (self.home / "config.json").write_text(
            '{"schema_version":1,"adapter":"portable","root":"'
            + str(link / "new-store")
            + '"}',
            encoding="utf-8",
        )

        result = self.start()

        self.assertEqual(2, result.returncode)
        self.assertIn("symlink", result.stderr)
        self.assertFalse((outside / "new-store").exists())
        self.assertEqual(0o755, stat.S_IMODE(outside.stat().st_mode))

    def test_integrity_rejects_dangling_store_symlink(self):
        self.home.mkdir()
        store = self.base / "dangling-store"
        store.symlink_to(self.base / "missing-target", target_is_directory=True)
        (self.home / "config.json").write_text(
            '{"schema_version":1,"adapter":"portable","root":"'
            + str(store)
            + '"}',
            encoding="utf-8",
        )

        result = run_cli(self.home, "integrity")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("symlink", result.stderr)
        self.assertTrue(store.is_symlink())
        self.assertFalse((self.base / "missing-target").exists())

    @unittest.skipUnless(getattr(os, "O_NONBLOCK", 0), "O_NONBLOCK unavailable")
    def test_payload_open_uses_nonblocking_flag(self):
        opened_flags = []
        real_open = os.open

        def recording_open(path, flags, *arguments, **keywords):
            opened_flags.append(flags)
            return real_open(path, flags, *arguments, **keywords)

        with mock.patch.object(workflow_observer_cli.os, "open", side_effect=recording_open):
            self.assertEqual(
                SCOPE,
                workflow_observer_cli._read_private_payload(str(self.scope), "Scope payload"),
            )

        self.assertTrue(opened_flags[0] & os.O_NONBLOCK)

    def test_payload_revalidates_opened_descriptor_mode(self):
        real_fstat = os.fstat

        def insecure_fstat(descriptor):
            current = real_fstat(descriptor)
            fields = list(current)
            fields[0] = (current.st_mode & ~0o777) | 0o644
            return os.stat_result(fields)

        with mock.patch.object(workflow_observer_cli.os, "fstat", side_effect=insecure_fstat):
            with self.assertRaisesRegex(
                workflow_observer_cli.ObservationError,
                "must have mode 0600",
            ):
                workflow_observer_cli._read_private_payload(
                    str(self.scope), "Scope payload"
                )


if __name__ == "__main__":
    unittest.main()
