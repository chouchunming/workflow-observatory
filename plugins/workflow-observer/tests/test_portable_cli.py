import hashlib
import json
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
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))
from episode_schema import parse_episode_block
from canonical_json import canonicalize, strict_json_loads
from store_config import PORTABLE_SEMANTICS
from workflow_evolution_fixtures import FakeObservationStore, load_projection_policy
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
        record = (
            self.home / "store/wiki/observations" / f"{run_id}.md"
        ).read_text(encoding="utf-8")
        frontmatter, body = record.split("---\n", 2)[1:]
        self.assertNotIn("schema_version:", frontmatter)
        self.assertNotIn("workflow_generation:", frontmatter)
        self.assertNotIn("## Episode data\n", body)
        validated = run_cli(self.home, "validate")
        self.assertEqual((0, "valid records=1 invalidated=0\n", ""),
                         (validated.returncode, validated.stdout, validated.stderr))
        report = run_cli(self.home, "report")
        self.assertEqual(0, report.returncode)
        self.assertIn("maintenance-basic", report.stdout)
        self.assertEqual("", report.stderr)
        self.assertEqual(
            record,
            (self.home / "store/wiki/observations" / f"{run_id}.md").read_text(
                encoding="utf-8"
            ),
            "classification/validation/reporting must not rewrite v1 bytes",
        )

    def test_invalidate_writes_exact_five_key_v2_without_reason(self):
        from wiki_observations import (
            InvalidationEvidence,
            ObservationPaths,
            collect_record_documents,
        )

        started = self.start()
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = run_cli(
            self.home,
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        root = self.home / "store"
        reason_sentinel = "reason-must-not-enter-explicit-v2-bytes"

        invalidated = run_cli(
            self.home,
            "invalidate",
            run_id,
            "--reason",
            reason_sentinel,
        )
        self.assertEqual(
            (0, "", ""),
            (invalidated.returncode, invalidated.stdout, invalidated.stderr),
        )

        tombstone = root / "wiki/observations/invalidations" / f"{run_id}.md"
        self.assertEqual(
            0o600,
            stat.S_IMODE(tombstone.stat().st_mode),
            "Portable invalidation must be mode-0600 at publication",
        )
        tombstone_bytes = tombstone.read_bytes()
        lines = tombstone_bytes.decode("utf-8").splitlines()
        self.assertEqual(
            [
                "---",
                "type: observation-invalidation",
                "artifact_type: observation-invalidation",
                "schema_version: 2",
                f"run_id: {run_id}",
            ],
            lines[:5],
        )
        self.assertRegex(
            lines[5],
            r"^timestamp: [0-9]{4}-[0-9]{2}-[0-9]{2}T"
            r"[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
        )
        self.assertEqual(["---"], lines[6:])
        self.assertNotIn(reason_sentinel.encode("utf-8"), tombstone_bytes)
        timestamp = lines[5].removeprefix("timestamp: ")
        collection = collect_record_documents(
            ObservationPaths.from_root(root),
            PORTABLE_SEMANTICS,
        )
        self.assertEqual(
            (InvalidationEvidence(
                run_id,
                timestamp,
                hashlib.sha256(tombstone_bytes).hexdigest(),
            ),),
            collection.invalidations,
        )

    def test_invalidate_validates_target_before_tombstone_write(self):
        started = self.start()
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        root = self.home / "store"
        absent_run_id = "obs-20260811-010203-abcdef"

        rejected = run_cli(
            self.home,
            "invalidate",
            absent_run_id,
            "--reason",
            "target is absent",
        )
        self.assertEqual(2, rejected.returncode, rejected.stderr)
        self.assertEqual("", rejected.stdout)
        self.assertIn("does not exist", rejected.stderr)

        self.assertFalse(
            (root / "wiki/observations/invalidations" / f"{absent_run_id}.md")
            .exists()
        )

        finished = run_cli(
            self.home,
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        record = root / "wiki/observations" / f"{run_id}.md"
        record.write_bytes(record.read_bytes().replace(
            b'type: "observation"\n',
            b'type: "observation"\nartifact_type: workflow-observation\n',
            1,
        ))
        malformed = record.read_bytes()

        rejected = run_cli(
            self.home,
            "invalidate",
            run_id,
            "--reason",
            "malformed target",
        )
        self.assertEqual(2, rejected.returncode, rejected.stderr)
        self.assertEqual("", rejected.stdout)
        self.assertIn(
            "explicit Markdown artifact_type requires schema_version",
            rejected.stderr,
        )

        self.assertFalse(
            (root / "wiki/observations/invalidations" / f"{run_id}.md").exists()
        )
        self.assertEqual(malformed, record.read_bytes())

    def test_invalidate_requires_reason_before_store_mutation(self):
        started = self.start()
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        root = self.home / "store"

        rejected = run_cli(self.home, "invalidate", run_id)

        self.assertEqual(2, rejected.returncode, rejected.stderr)
        self.assertEqual("", rejected.stdout)
        self.assertIn("following arguments are required: --reason", rejected.stderr)
        self.assertFalse(
            (root / "wiki/observations/invalidations" / f"{run_id}.md").exists()
        )

    def test_invalidation_schema_adversaries_fail_closed(self):
        started = self.start()
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = run_cli(
            self.home,
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        tombstone = (
            self.home
            / "store/wiki/observations/invalidations"
            / f"{run_id}.md"
        )
        cases = {
            "legacy extra schema-like field": (
                "---\n"
                "type: observation-invalidation\n"
                f"title: Invalidate {run_id}\n"
                'tags: ["observation","invalidation"]\n'
                "timestamp: 2026-08-11T01:02:03Z\n"
                f"target_run_id: {run_id}\n"
                "reason: duplicate observation\n"
                "sources: []\n"
                "artifact_type: observation-invalidation\n"
                "---\n"
            ),
            "explicit v2 missing artifact_type": (
                "---\n"
                "type: observation-invalidation\n"
                "schema_version: 2\n"
                f"run_id: {run_id}\n"
                "timestamp: 2026-08-11T01:02:03Z\n"
                "---\n"
            ),
            "human type and artifact type mismatch": (
                "---\n"
                "type: observation\n"
                "artifact_type: observation-invalidation\n"
                "schema_version: 2\n"
                f"run_id: {run_id}\n"
                "timestamp: 2026-08-11T01:02:03Z\n"
                "---\n"
            ),
            "unknown schema version": (
                "---\n"
                "type: observation-invalidation\n"
                "artifact_type: observation-invalidation\n"
                "schema_version: 3\n"
                f"run_id: {run_id}\n"
                "timestamp: 2026-08-11T01:02:03Z\n"
                "---\n"
            ),
            "duplicate run_id": (
                "---\n"
                "type: observation-invalidation\n"
                "artifact_type: observation-invalidation\n"
                "schema_version: 2\n"
                f"run_id: {run_id}\n"
                f"run_id: {run_id}\n"
                "timestamp: 2026-08-11T01:02:03Z\n"
                "---\n"
            ),
            "duplicate schema_version": (
                "---\n"
                "type: observation-invalidation\n"
                "artifact_type: observation-invalidation\n"
                "schema_version: 2\n"
                "schema_version: 2\n"
                f"run_id: {run_id}\n"
                "timestamp: 2026-08-11T01:02:03Z\n"
                "---\n"
            ),
        }
        for name, content in cases.items():
            with self.subTest(name=name):
                tombstone.write_text(content, encoding="utf-8")
                rejected = run_cli(self.home, "validate")
                self.assertEqual(2, rejected.returncode, rejected.stderr)
                self.assertEqual("", rejected.stdout)
                self.assertTrue(rejected.stderr.startswith(
                    "workflow observer validation error:"
                ))

    def test_v2_start_and_finish_write_one_canonical_episode_block(self):
        supplement = self.base / "episode.json"
        write_private(supplement, json.dumps({
            "schema_version": 2,
            "execution": {
                "input_tokens": None,
                "output_tokens": None,
                "cache_read_tokens": None,
                "cost_amount": None,
                "cost_currency": None,
                "measurement_source": "unavailable",
            },
            "quality": {"test_failures": 0, "timeout_count": 0},
            "decisions": [],
        }))
        started = run_cli(
            self.home, "start", "--title", "v2", "--subject-root", str(self.subject),
            "--agent-surface", "codex", "--start-mode", "planned",
            "--task-type", "maintenance", "--workflow-variant", "maintenance-basic",
            "--scope-from-file", str(self.scope), "--episode-schema-version", "2",
            "--workflow-generation", "maintenance-basic@2",
        )
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = run_cli(
            self.home, "finish", run_id, "--status", "success",
            "--from-file", str(self.completion), "--episode-from-file", str(supplement),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        record = (
            self.home / "store/wiki/observations" / f"{run_id}.md"
        ).read_text(encoding="utf-8")
        self.assertIn("schema_version: 2\n", record)
        self.assertIn('workflow_generation: "maintenance-basic@2"\n', record)
        self.assertEqual(1, record.count("## Episode data\n"))
        _human, episode = parse_episode_block(
            record.split("---\n", 2)[2].lstrip(), load_projection_policy()
        )
        self.assertEqual(2, episode["schema_version"])
        validated = run_cli(self.home, "validate")
        report = run_cli(self.home, "report")
        self.assertEqual((0, ""), (validated.returncode, validated.stderr))
        self.assertEqual((0, ""), (report.returncode, report.stderr))
        self.assertEqual(
            record,
            (self.home / "store/wiki/observations" / f"{run_id}.md").read_text(
                encoding="utf-8"
            ),
            "classification/validation/reporting must not rewrite v2 bytes",
        )

    def test_v2_complete_lifecycle_reads_human_completion_metrics(self):
        completion = self.base / "v2-completion.md"
        write_private(
            completion,
            COMPLETION.replace(
                "review_rounds: 0\ndefects_found: 0\nrework_count: 0\n"
                "rework_reason: none",
                "review_rounds: 2\ndefects_found: 3\nrework_count: 1\n"
                "rework_reason: corrected-readback",
            ),
        )
        supplement = self.base / "v2-episode.json"
        write_private(supplement, json.dumps({
            "schema_version": 2,
            "execution": {
                "input_tokens": None,
                "output_tokens": None,
                "cache_read_tokens": None,
                "cost_amount": None,
                "cost_currency": None,
                "measurement_source": "unavailable",
            },
            "quality": {"test_failures": 7, "timeout_count": 0},
            "decisions": [],
        }))
        started = run_cli(
            self.home, "start", "--title", "v2 readback",
            "--subject-root", str(self.subject), "--agent-surface", "codex",
            "--start-mode", "planned", "--task-type", "maintenance",
            "--workflow-variant", "maintenance-basic", "--scope-from-file",
            str(self.scope), "--episode-schema-version", "2",
            "--workflow-generation", "maintenance-basic@2",
        )
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()

        finished = run_cli(
            self.home, "finish", run_id, "--status", "success",
            "--from-file", str(completion), "--episode-from-file",
            str(supplement),
        )
        validated = run_cli(self.home, "validate")
        integrity = run_cli(self.home, "integrity")
        report = run_cli(self.home, "report")

        self.assertEqual((0, "", ""),
                         (finished.returncode, finished.stdout, finished.stderr))
        self.assertEqual((0, "valid records=1 invalidated=0\n", ""),
                         (validated.returncode, validated.stdout, validated.stderr))
        self.assertEqual((0, "healthy records=1 invalidated=0\n", ""),
                         (integrity.returncode, integrity.stdout, integrity.stderr))
        self.assertEqual(0, report.returncode, report.stderr)
        self.assertIn("Total defects found: 3", report.stdout)
        self.assertIn("Total rework count: 1", report.stdout)
        self.assertIn("Average review rounds: 2", report.stdout)
        self.assertNotIn("Total defects found: 7", report.stdout)

    def test_rejected_v1_v2_combinations_preserve_record_bytes(self):
        valid_supplement = {
            "schema_version": 2,
            "execution": {
                "input_tokens": None,
                "output_tokens": None,
                "cache_read_tokens": None,
                "cost_amount": None,
                "cost_currency": None,
                "measurement_source": "unavailable",
            },
            "quality": {"test_failures": 0, "timeout_count": 0},
            "decisions": [],
        }
        valid_episode = self.base / "valid-episode.json"
        write_private(valid_episode, json.dumps(valid_supplement))
        invalid_episode = self.base / "invalid-episode.json"
        invalid_decision = {
            "sequence": 1,
            "phase": "not-a-phase",
            "actor_role": "implementer",
            "decision_type": "reject",
            "reason_code": "integrity-risk",
            "result": "supported",
            "summary": "Rejected an unsafe mutation at the evidence boundary",
        }
        write_private(
            invalid_episode,
            json.dumps({**valid_supplement, "decisions": [invalid_decision]}),
        )

        def records() -> dict[str, bytes]:
            observations = self.home / "store/wiki/observations"
            return {
                path.name: path.read_bytes()
                for path in observations.glob("obs-*.md")
            } if observations.exists() else {}

        def start_v2(title: str) -> str:
            result = run_cli(
                self.home, "start", "--title", title,
                "--subject-root", str(self.subject), "--agent-surface", "codex",
                "--start-mode", "planned", "--task-type", "maintenance",
                "--workflow-variant", "implementation-with-review",
                "--scope-from-file", str(self.scope),
                "--episode-schema-version", "2", "--workflow-generation",
                "implementation-with-review@2",
            )
            self.assertEqual(0, result.returncode, result.stderr)
            return result.stdout.strip()

        v2_missing = start_v2("v2 missing supplement")
        v1_with_supplement = self.start(title="v1 with supplement").stdout.strip()
        v2_invalid = start_v2("v2 invalid decision")
        cases = (
            (
                "v2 finish without supplement",
                lambda: run_cli(
                    self.home, "finish", v2_missing, "--status", "success",
                    "--from-file", str(self.completion),
                ),
            ),
            (
                "v1 start with workflow generation",
                lambda: run_cli(
                    self.home, "start", "--title", "invalid v1 generation",
                    "--subject-root", str(self.subject), "--agent-surface", "codex",
                    "--start-mode", "planned", "--task-type", "maintenance",
                    "--workflow-variant", "maintenance-basic", "--scope-from-file",
                    str(self.scope), "--workflow-generation", "maintenance-basic@2",
                ),
            ),
            (
                "v1 finish with supplement",
                lambda: run_cli(
                    self.home, "finish", v1_with_supplement, "--status", "success",
                    "--from-file", str(self.completion), "--episode-from-file",
                    str(valid_episode),
                ),
            ),
            (
                "v2 invalid Decision Event",
                lambda: run_cli(
                    self.home, "finish", v2_invalid, "--status", "success",
                    "--from-file", str(self.completion), "--episode-from-file",
                    str(invalid_episode),
                ),
            ),
        )
        for name, rejected_call in cases:
            with self.subTest(name=name):
                before = records()
                rejected = rejected_call()
                self.assertEqual(2, rejected.returncode, rejected.stderr)
                self.assertTrue(rejected.stderr.startswith(
                    "workflow observer validation error:"
                ))
                self.assertEqual(before, records())

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

    def test_snapshot_publishes_canonical_path_free_artifact_without_mutation(self):
        with FakeObservationStore("portable") as store:
            home = store.store_root.parent
            before = {
                path.relative_to(store.store_root): path.read_bytes()
                for path in store.store_root.rglob("*")
                if path.is_file()
            }
            arguments = (
                "snapshot",
                "--since", "2026-08-02",
                "--until", "2026-08-02",
                "--timezone", "Asia/Taipei",
                "--as-of", "2026-08-02T16:00:00Z",
            )

            first = run_cli(home, *arguments)
            second = run_cli(home, *arguments)

            self.assertEqual((0, ""), (first.returncode, first.stderr))
            self.assertEqual((0, ""), (second.returncode, second.stderr))
            first_response = strict_json_loads(first.stdout.encode("utf-8"))
            second_response = strict_json_loads(second.stdout.encode("utf-8"))
            self.assertEqual(
                {"created", "snapshot"}, set(first_response)
            )
            self.assertIs(True, first_response["created"])
            self.assertIs(False, second_response["created"])
            self.assertEqual(
                first_response["snapshot"], second_response["snapshot"]
            )
            self.assertEqual(
                canonicalize(first_response).decode("utf-8") + "\n",
                first.stdout,
            )
            self.assertNotIn(str(home), first.stdout)
            snapshot = first_response["snapshot"]
            self.assertEqual(2, snapshot["schema_version"])
            self.assertEqual(2, snapshot["core"]["schema_version"])
            self.assertEqual(
                "learning-snapshot-core",
                snapshot["core"]["artifact_type"],
            )
            self.assertEqual(0, snapshot["core"]["sampled_by_policy_n"])
            path = (
                home / "learning/snapshots"
                / f"{snapshot['snapshot_id']}.json"
            )
            self.assertEqual(canonicalize(snapshot), path.read_bytes())
            after = {
                path.relative_to(store.store_root): path.read_bytes()
                for path in store.store_root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
