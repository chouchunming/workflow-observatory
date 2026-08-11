import json
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE_ROOT = PLUGIN_ROOT.parents[1]
_SOURCE_REPOSITORY = MARKETPLACE_ROOT.parents[1]
REPOSITORY_ROOT = (
    _SOURCE_REPOSITORY
    if (_SOURCE_REPOSITORY / "wiki_cli.py").is_file()
    else MARKETPLACE_ROOT / "evidence"
)
CLI = PLUGIN_ROOT / "scripts/workflow_observer_cli.py"
sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))

from store_config import LLMWIKI_SEMANTICS, PORTABLE_SEMANTICS
from canonical_json import canonicalize

SCOPE_TEXT = """## Scope

- Goal: Compare adapters
- Included: Shared lifecycle matrix
- Excluded: None
"""
TASK_TEXT = """---
type: task
id: example
title: Example task
status: pending
tags: [workflow]
timestamp: 2026-08-02
sources: []
---
"""
EPISODE_TEXT = json.dumps({
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
})
COMPLETION_TEXT = """## Execution evidence

- Verification: matrix pass
- Artifacts: sanitized record

## Outcome and observation

- Outcome: Adapter lifecycle completed
- Observation: Identical inputs preserved the contract

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


def write_private(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


class AdapterConformanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.subject = self.base / "subject argv ; $safe"
        self.subject.mkdir()
        self.scope = self.base / "scope.md"
        self.completion = self.base / "completion.md"
        self.episode = self.base / "episode.json"
        write_private(self.scope, SCOPE_TEXT)
        write_private(self.completion, COMPLETION_TEXT)
        write_private(self.episode, EPISODE_TEXT)

        self.homes = {
            "portable": self.base / "portable-home",
            "llmwiki": self.base / "llmwiki-home",
        }
        self.llm_root = self.base / "temporary llm wiki"
        self.llm_root.mkdir()
        shutil.copy2(REPOSITORY_ROOT / "wiki_cli.py", self.llm_root / "wiki_cli.py")
        shutil.copy2(
            REPOSITORY_ROOT / "wiki_observations.py",
            self.llm_root / "wiki_observations.py",
        )
        self.homes["llmwiki"].mkdir()
        (self.homes["llmwiki"] / "config.json").write_text(
            json.dumps({
                "schema_version": 1,
                "adapter": "llmwiki",
                "cli_path": str(self.llm_root / "wiki_cli.py"),
                "wiki_root": str(self.llm_root),
            }),
            encoding="utf-8",
        )
        (self.llm_root / "wiki/observations/.locks").mkdir(parents=True)
        (self.llm_root / "wiki/observations/invalidations").mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def run_cli(self, adapter: str, *arguments: str) -> subprocess.CompletedProcess[str]:
        environment = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "WORKFLOW_OBSERVATORY_HOME": str(self.homes[adapter]),
        }
        return subprocess.run(
            [sys.executable, str(CLI), *arguments],
            cwd=PLUGIN_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def start(
        self,
        adapter: str,
        *,
        title="Matrix Example",
        task_type="maintenance",
        variant="maintenance-basic",
        task: str | None = None,
        schema_version: int = 1,
    ):
        arguments = [
            "start",
            "--title", title,
            "--subject-root", str(self.subject),
            "--agent-surface", "codex",
            "--start-mode", "planned",
            "--task-type", task_type,
            "--workflow-variant", variant,
            "--scope-from-file", str(self.scope),
        ]
        if task is not None:
            arguments.extend(["--task", task])
        if schema_version == 2:
            arguments.extend([
                "--episode-schema-version", "2",
                "--workflow-generation", f"{variant}@2",
            ])
        return self.run_cli(adapter, *arguments)

    def _select_delegate_probe(self, *, exit_code: int = 0) -> Path:
        marker = self.llm_root / "delegate-probe.log"
        probe = self.llm_root / "delegate_probe.py"
        probe.write_text(
            "from pathlib import Path\n"
            "import sys\n"
            "marker = Path(__file__).with_name('delegate-probe.log')\n"
            "with marker.open('a', encoding='utf-8') as stream:\n"
            "    stream.write(' '.join(sys.argv[1:]) + '\\n')\n"
            "if 'start' in sys.argv:\n"
            "    print('obs-20260802-120000-abcdef')\n"
            "if 'report' in sys.argv:\n"
            "    print('delegated report')\n"
            f"raise SystemExit({exit_code})\n",
            encoding="utf-8",
        )
        config_path = self.homes["llmwiki"] / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["cli_path"] = str(probe)
        config_path.write_text(json.dumps(config), encoding="utf-8")
        return marker

    def test_selected_adapters_validate_equivalent_current_task_layouts(self):
        self.assertEqual("portable", PORTABLE_SEMANTICS.name)
        self.assertEqual("llmwiki", LLMWIKI_SEMANTICS.name)
        portable_root = self.homes["portable"] / "store"
        portable_task = portable_root / "wiki/tasks/example.md"
        portable_task.parent.mkdir(parents=True)
        portable_task.write_text(TASK_TEXT, encoding="utf-8")
        llmwiki_task = self.llm_root / "wiki/tasks/records/example.md"
        llmwiki_task.parent.mkdir(parents=True)
        llmwiki_task.write_text(TASK_TEXT, encoding="utf-8")

        started = self.start("portable", task="example")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        portable_record = portable_root / "wiki/observations" / f"{run_id}.md"
        llmwiki_record = self.llm_root / "wiki/observations" / f"{run_id}.md"
        llmwiki_record.write_bytes(portable_record.read_bytes())
        self.assertEqual(portable_record.read_bytes(), llmwiki_record.read_bytes())

        portable = self.run_cli("portable", "validate")
        llmwiki = self.run_cli("llmwiki", "validate")

        self.assertEqual(
            (0, "valid records=1 invalidated=0\n", ""),
            (portable.returncode, portable.stdout, portable.stderr),
        )
        self.assertEqual(
            (0, "valid records=1 invalidated=0\n", ""),
            (llmwiki.returncode, llmwiki.stdout, llmwiki.stderr),
        )

    def test_record_documents_bind_exact_record_and_reference_bytes(self):
        from wiki_observations import (
            ObservationPaths,
            ReferenceEvidence,
            collect_record_documents,
            collect_records,
        )

        portable_root = self.homes["portable"] / "store"
        task = portable_root / "wiki/tasks/example.md"
        task.parent.mkdir(parents=True)
        task.write_text(TASK_TEXT, encoding="utf-8")
        started = self.start("portable", task="example")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        record = portable_root / "wiki/observations" / f"{run_id}.md"

        collection = collect_record_documents(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        )

        self.assertEqual(1, len(collection.records))
        document = collection.records[0]
        self.assertEqual(run_id, document.run_id)
        self.assertEqual(hashlib.sha256(record.read_bytes()).hexdigest(),
                         document.source_sha256)
        self.assertEqual(
            (ReferenceEvidence(
                "task",
                "[[example]]",
                hashlib.sha256(task.read_bytes()).hexdigest(),
            ),),
            document.references,
        )
        records, invalidated = collect_records(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        )
        self.assertEqual([run_id], [row["run_id"] for row in records])
        self.assertEqual(set(), invalidated)

    def test_portable_observation_can_be_invalidated_after_reader_refactor(self):
        from wiki_observations import ObservationPaths, invalidate_observation

        started = self.start("portable")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "portable", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        portable_root = self.homes["portable"] / "store"

        invalidate_observation(
            ObservationPaths.from_root(portable_root),
            run_id,
            "duplicate observation",
        )

        tombstone = portable_root / "wiki/observations/invalidations" / f"{run_id}.md"
        self.assertTrue(tombstone.is_file())
        validation = self.run_cli("portable", "validate")
        self.assertEqual(
            (0, "valid records=1 invalidated=1\n", ""),
            (validation.returncode, validation.stdout, validation.stderr),
        )

    def test_existing_legacy_tombstone_is_readable_and_never_rewritten(self):
        from wiki_observations import (
            InvalidationEvidence,
            ObservationError,
            ObservationPaths,
            collect_record_documents,
            invalidate_observation,
        )

        started = self.start("portable")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "portable", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        portable_root = self.homes["portable"] / "store"
        legacy_bytes = (
            "---\n"
            "type: observation-invalidation\n"
            f"title: Invalidate {run_id}\n"
            'tags: ["observation","invalidation"]\n'
            "timestamp: 2026-08-02T23:17:45+08:00\n"
            f"target_run_id: {run_id}\n"
            "reason: legacy-reason-remains-byte-identical\n"
            "sources: []\n"
            "---\n"
        ).encode("utf-8")
        tombstone = (
            portable_root / "wiki/observations/invalidations" / f"{run_id}.md"
        )
        tombstone.write_bytes(legacy_bytes)

        collection = collect_record_documents(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        )
        self.assertEqual(
            (InvalidationEvidence(
                run_id,
                "2026-08-02T15:17:45Z",
                hashlib.sha256(legacy_bytes).hexdigest(),
            ),),
            collection.invalidations,
        )
        with self.assertRaisesRegex(ObservationError, "already invalidated"):
            invalidate_observation(
                ObservationPaths.from_root(portable_root),
                run_id,
                "new reason must not replace legacy bytes",
                now=datetime(2026, 8, 11, 1, 2, 3, tzinfo=timezone.utc),
            )
        self.assertEqual(legacy_bytes, tombstone.read_bytes())

    def test_legacy_and_explicit_v2_invalidation_evidence_has_adapter_parity(self):
        from wiki_observations import ObservationPaths, collect_record_documents

        started = self.start("portable")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "portable", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        portable_root = self.homes["portable"] / "store"
        portable_record = portable_root / "wiki/observations" / f"{run_id}.md"
        llm_record = self.llm_root / "wiki/observations" / f"{run_id}.md"
        llm_record.write_bytes(portable_record.read_bytes())
        legacy_bytes = (
            "---\n"
            "type: observation-invalidation\n"
            f"title: Invalidate {run_id}\n"
            'tags: ["observation","invalidation"]\n'
            "timestamp: 2026-08-02T23:17:45+08:00\n"
            f"target_run_id: {run_id}\n"
            "reason: legacy parity\n"
            "sources: []\n"
            "---\n"
        ).encode("utf-8")
        v2_bytes = (
            "---\n"
            "type: observation-invalidation\n"
            "artifact_type: observation-invalidation\n"
            "schema_version: 2\n"
            f"run_id: {run_id}\n"
            "timestamp: 2026-08-02T15:17:45Z\n"
            "---\n"
        ).encode("utf-8")
        portable_tombstone = (
            portable_root / "wiki/observations/invalidations" / f"{run_id}.md"
        )
        llm_tombstone = (
            self.llm_root / "wiki/observations/invalidations" / f"{run_id}.md"
        )
        portable_tombstone.write_bytes(legacy_bytes)
        llm_tombstone.write_bytes(v2_bytes)

        legacy = collect_record_documents(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        ).invalidations[0]
        explicit = collect_record_documents(
            ObservationPaths.from_root(self.llm_root), LLMWIKI_SEMANTICS
        ).invalidations[0]
        self.assertEqual((legacy.run_id, legacy.timestamp),
                         (explicit.run_id, explicit.timestamp))
        self.assertEqual(hashlib.sha256(legacy_bytes).hexdigest(),
                         legacy.source_sha256)
        self.assertEqual(hashlib.sha256(v2_bytes).hexdigest(),
                         explicit.source_sha256)

        portable_tombstone.write_bytes(v2_bytes)
        portable_v2 = collect_record_documents(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        ).invalidations
        llmwiki_v2 = collect_record_documents(
            ObservationPaths.from_root(self.llm_root), LLMWIKI_SEMANTICS
        ).invalidations
        self.assertEqual(portable_v2, llmwiki_v2)

    def test_llmwiki_finish_classifies_complete_draft_before_delegation(self):
        started = self.start("llmwiki")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        record = self.llm_root / "wiki/observations" / f"{run_id}.md"
        original = record.read_bytes()
        record.write_bytes(original.replace(
            b'type: "observation"\n',
            b'type: "observation"\nartifact_type: workflow-observation\n',
            1,
        ))
        malformed = record.read_bytes()
        marker = self._select_delegate_probe()

        rejected = self.run_cli(
            "llmwiki", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )

        self.assertEqual(2, rejected.returncode, rejected.stderr)
        self.assertEqual("", rejected.stdout)
        self.assertIn(
            "explicit Markdown artifact_type requires schema_version",
            rejected.stderr,
        )
        self.assertFalse(marker.exists(), "malformed draft reached the delegate")
        self.assertEqual(malformed, record.read_bytes())

    def test_invalidation_evidence_binds_timestamp_and_exact_tombstone_bytes(self):
        from wiki_observations import ObservationPaths, collect_record_documents

        started = self.start("portable")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "portable", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        portable_root = self.homes["portable"] / "store"
        tombstone_bytes = (
            "---\n"
            "type: observation-invalidation\n"
            f"title: Invalidate {run_id}\n"
            "tags: [\"observation\",\"invalidation\"]\n"
            "timestamp: 2026-08-02T23:17:45+08:00\n"
            f"target_run_id: {run_id}\n"
            "reason: duplicate observation\n"
            "sources: []\n"
            "---\n"
        ).encode("utf-8")
        tombstone = (
            portable_root / "wiki/observations/invalidations" / f"{run_id}.md"
        )
        tombstone.write_bytes(tombstone_bytes)

        collection = collect_record_documents(
            ObservationPaths.from_root(portable_root), PORTABLE_SEMANTICS
        )

        self.assertTrue(
            hasattr(collection, "invalidations"),
            "collection must expose immutable invalidation evidence rows",
        )
        from wiki_observations import InvalidationEvidence

        expected_sha256 = hashlib.sha256(tombstone_bytes).hexdigest()
        self.assertEqual(
            (InvalidationEvidence(
                run_id,
                "2026-08-02T15:17:45Z",
                expected_sha256,
            ),),
            collection.invalidations,
        )
        self.assertEqual(frozenset({run_id}), collection.invalidated)
        self.assertEqual(
            ((run_id, expected_sha256),),
            collection.invalidation_sha256,
        )

    def test_llmwiki_obsolete_task_layout_fails_closed(self):
        portable_root = self.homes["portable"] / "store"
        portable_task = portable_root / "wiki/tasks/example.md"
        portable_task.parent.mkdir(parents=True)
        portable_task.write_text(TASK_TEXT, encoding="utf-8")
        obsolete_task = self.llm_root / "wiki/tasks/example.md"
        obsolete_task.parent.mkdir(parents=True, exist_ok=True)
        obsolete_task.write_text(TASK_TEXT, encoding="utf-8")

        started = self.start("portable", task="example")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        source = portable_root / "wiki/observations" / f"{run_id}.md"
        (self.llm_root / "wiki/observations" / f"{run_id}.md").write_bytes(
            source.read_bytes()
        )

        result = self.run_cli("llmwiki", "validate")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("task_ref points to no task record", result.stderr)

    def test_llmwiki_schema_v1_lifecycle_remains_delegated_and_report_is_bundled(self):
        started = self.start("llmwiki")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        marker = self._select_delegate_probe()

        finished = self.run_cli(
            "llmwiki", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        another_start = self.start("llmwiki")
        report = self.run_cli("llmwiki", "report")

        self.assertEqual(0, finished.returncode, finished.stderr)
        self.assertEqual(0, another_start.returncode, another_start.stderr)
        self.assertEqual("obs-20260802-120000-abcdef\n", another_start.stdout)
        self.assertEqual((0, ""), (report.returncode, report.stderr))
        self.assertIn("maintenance-basic", report.stdout)
        self.assertIn("draft=1", report.stdout)
        calls = marker.read_text(encoding="utf-8")
        self.assertIn(f"finish {run_id}", calls)
        self.assertIn(" start ", f" {calls}")
        self.assertNotIn(" report", calls)

    def test_llmwiki_report_v2_lifecycle_uses_bundled_core(self):
        marker = self._select_delegate_probe(exit_code=91)

        started = self.start("llmwiki", title="LLMWiki v2", schema_version=2)

        self.assertEqual(0, started.returncode, started.stderr)
        self.assertFalse(marker.exists())
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "llmwiki", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
            "--episode-from-file", str(self.episode),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        validation = self.run_cli("llmwiki", "validate")
        integrity = self.run_cli("llmwiki", "integrity")
        report = self.run_cli("llmwiki", "report")

        self.assertEqual(
            (0, 0, 0),
            (validation.returncode, integrity.returncode, report.returncode),
            (validation.stderr, integrity.stderr, report.stderr),
        )
        self.assertEqual("valid records=1 invalidated=0\n", validation.stdout)
        self.assertEqual("healthy records=1 invalidated=0\n", integrity.stdout)
        self.assertIn("maintenance-basic", report.stdout)
        self.assertIn("success=1", report.stdout)
        self.assertFalse(marker.exists(), "report invoked the configured delegate")
        record = (self.llm_root / "wiki/observations" / f"{run_id}.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("schema_version: 2\n", record)
        self.assertIn("## Episode data\n", record)

    def test_llmwiki_invalidate_uses_bundled_exact_v2_without_delegate(self):
        marker = self._select_delegate_probe(exit_code=91)
        started = self.start("llmwiki", title="Invalidate v2", schema_version=2)
        self.assertEqual(0, started.returncode, started.stderr)
        self.assertFalse(marker.exists())
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "llmwiki", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
            "--episode-from-file", str(self.episode),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)

        absent_run_id = "obs-20260811-010203-abcdef"
        absent = self.run_cli(
            "llmwiki", "invalidate", absent_run_id,
            "--reason", "absent LLMWiki target",
        )
        self.assertEqual(2, absent.returncode, absent.stderr)
        self.assertEqual("", absent.stdout)
        self.assertIn("does not exist", absent.stderr)
        self.assertFalse(
            (self.llm_root / "wiki/observations/invalidations"
             / f"{absent_run_id}.md").exists()
        )
        self.assertFalse(marker.exists(), "absent target reached the delegate")

        reason_sentinel = "llmwiki-reason-must-not-enter-v2-bytes"
        invalidated = self.run_cli(
            "llmwiki", "invalidate", run_id,
            "--reason", reason_sentinel,
        )
        self.assertEqual(
            (0, "", ""),
            (invalidated.returncode, invalidated.stdout, invalidated.stderr),
        )
        tombstone = (
            self.llm_root / "wiki/observations/invalidations" / f"{run_id}.md"
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
        self.assertFalse(marker.exists(), "invalidation reached the delegate")
        validation = self.run_cli("llmwiki", "validate")
        self.assertEqual(
            (0, "valid records=1 invalidated=1\n", ""),
            (validation.returncode, validation.stdout, validation.stderr),
        )
        self.assertFalse(marker.exists(), "validation reached the delegate")

    def test_llmwiki_report_current_task_layout_uses_selected_semantics(self):
        portable_root = self.homes["portable"] / "store"
        portable_task = portable_root / "wiki/tasks/example.md"
        portable_task.parent.mkdir(parents=True)
        portable_task.write_text(TASK_TEXT, encoding="utf-8")
        llmwiki_task = self.llm_root / "wiki/tasks/records/example.md"
        llmwiki_task.parent.mkdir(parents=True)
        llmwiki_task.write_text(TASK_TEXT, encoding="utf-8")
        self.assertFalse((self.llm_root / "wiki/tasks/example.md").exists())

        started = self.start("portable", task="example")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        finished = self.run_cli(
            "portable", "finish", run_id, "--status", "success",
            "--from-file", str(self.completion),
        )
        self.assertEqual(0, finished.returncode, finished.stderr)
        source = portable_root / "wiki/observations" / f"{run_id}.md"
        (self.llm_root / "wiki/observations" / f"{run_id}.md").write_bytes(
            source.read_bytes()
        )
        marker = self._select_delegate_probe(exit_code=91)

        validation = self.run_cli("llmwiki", "validate")
        report = self.run_cli("llmwiki", "report")

        self.assertEqual(
            (0, 0),
            (validation.returncode, report.returncode),
            (validation.stderr, report.stderr),
        )
        self.assertEqual("valid records=1 invalidated=0\n", validation.stdout)
        self.assertIn("maintenance-basic", report.stdout)
        self.assertIn("success=1", report.stdout)
        self.assertFalse(marker.exists(), "report invoked the configured delegate")

    def test_shared_adapter_conformance_matrix(self):
        results = {}
        for adapter in self.homes:
            with self.subTest(adapter=adapter):
                started = self.start(adapter)
                self.assertEqual((0, ""), (started.returncode, started.stderr))
                run_id = started.stdout.strip()
                self.assertRegex(run_id, r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
                finished = self.run_cli(
                    adapter, "finish", run_id, "--status", "success",
                    "--from-file", str(self.completion),
                )
                self.assertEqual((0, "", ""),
                                 (finished.returncode, finished.stdout, finished.stderr))

                validation = self.run_cli(adapter, "validate")
                integrity = self.run_cli(adapter, "integrity")
                report = self.run_cli(adapter, "report")
                taxonomy = self.start(adapter, task_type="query", variant="maintenance-basic")
                too_long = self.start(adapter, title="x" * 201)
                double_finish = self.run_cli(
                    adapter, "finish", run_id, "--status", "success",
                    "--from-file", str(self.completion),
                )

                self.assertEqual("valid records=1 invalidated=0\n", validation.stdout)
                self.assertEqual("healthy records=1 invalidated=0\n", integrity.stdout)
                self.assertEqual((0, 0), (validation.returncode, integrity.returncode))
                self.assertIn("maintenance-basic", report.stdout)
                self.assertIn("success=1", report.stdout)
                self.assertNotIn(str(self.subject), report.stdout)
                self.assertEqual((2, 2, 2),
                                 (taxonomy.returncode, too_long.returncode,
                                  double_finish.returncode))
                self.assertEqual("", taxonomy.stdout)
                self.assertEqual("", too_long.stdout)
                self.assertEqual("", double_finish.stdout)
                self.assertIn("taxonomy", taxonomy.stderr)
                self.assertIn("200 Unicode code points", too_long.stderr)
                self.assertTrue(double_finish.stderr.startswith(
                    "workflow observer state error:"))
                records_root = (
                    self.homes[adapter] / "store"
                    if adapter == "portable"
                    else Path(json.loads((self.homes[adapter] / "config.json").read_text())["wiki_root"])
                )
                record_text = next((records_root / "wiki/observations").glob("obs-*.md")).read_text()
                self.assertNotIn(str(self.subject), record_text)
                results[adapter] = {
                    "validate": validation.stdout,
                    "integrity": integrity.stdout,
                    "status": "success" if "success=1" in report.stdout else "missing",
                    "group": "maintenance-basic" in report.stdout,
                    "taxonomy_code": taxonomy.returncode,
                    "scalar_code": too_long.returncode,
                    "double_finish_code": double_finish.returncode,
                }

        self.assertEqual(results["portable"], results["llmwiki"])

    def test_llmwiki_integrity_rejects_unexpected_layout_entry(self):
        root = Path(json.loads(
            (self.homes["llmwiki"] / "config.json").read_text(encoding="utf-8")
        )["wiki_root"])
        (root / "wiki/observations/record.backup").write_text(
            "unexpected", encoding="utf-8"
        )

        result = self.run_cli("llmwiki", "integrity")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("unexpected observation entry", result.stderr)

    def test_llmwiki_delegated_io_error_normalizes_to_exit_one(self):
        started = self.start("llmwiki")
        self.assertEqual(0, started.returncode)
        missing_payload = self.base / "missing-completion.md"

        result = self.run_cli(
            "llmwiki",
            "finish",
            started.stdout.strip(),
            "--status",
            "success",
            "--from-file",
            str(missing_payload),
        )

        self.assertEqual(1, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith("workflow observer io error:"))

    def test_snapshot_input_cli_has_canonical_adapter_neutral_semantics(self):
        started = self.start("portable")
        self.assertEqual(0, started.returncode, started.stderr)
        run_id = started.stdout.strip()
        portable_record = (
            self.homes["portable"] / "store/wiki/observations" / f"{run_id}.md"
        )
        llmwiki_record = self.llm_root / "wiki/observations" / f"{run_id}.md"
        llmwiki_record.write_bytes(portable_record.read_bytes())

        arguments = (
            "snapshot-input",
            "--since", "2020-01-01",
            "--until", "2030-12-31",
            "--timezone", "UTC",
        )
        portable = self.run_cli("portable", *arguments)
        llmwiki = self.run_cli("llmwiki", *arguments)

        self.assertEqual((0, ""), (portable.returncode, portable.stderr))
        self.assertEqual((0, ""), (llmwiki.returncode, llmwiki.stderr))
        portable_bundle = json.loads(portable.stdout)
        llmwiki_bundle = json.loads(llmwiki.stdout)
        self.assertEqual(
            {"adapter", "store_identity", "semantic_bundle"},
            set(portable_bundle),
        )
        self.assertEqual(
            canonicalize(portable_bundle).decode("utf-8") + "\n",
            portable.stdout,
        )
        self.assertEqual(
            canonicalize(llmwiki_bundle).decode("utf-8") + "\n",
            llmwiki.stdout,
        )
        self.assertEqual(
            portable_bundle["semantic_bundle"],
            llmwiki_bundle["semantic_bundle"],
        )
        self.assertEqual("portable", portable_bundle["adapter"]["name"])
        self.assertEqual("llmwiki", llmwiki_bundle["adapter"]["name"])
        self.assertNotIn(str(self.base), portable.stdout)
        self.assertNotIn(str(self.base), llmwiki.stdout)

    def test_snapshot_input_cli_rejects_noncanonical_as_of_without_stdout(self):
        result = self.run_cli(
            "portable",
            "snapshot-input",
            "--since", "2026-08-02",
            "--until", "2026-08-02",
            "--timezone", "UTC",
            "--as-of", "2026-08-03T00:00:00+00:00",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith(
            "workflow observer validation error:"
        ))

    def test_snapshot_input_cli_rejects_explicit_empty_as_of(self):
        result = self.run_cli(
            "portable",
            "snapshot-input",
            "--since", "2026-08-02",
            "--until", "2026-08-02",
            "--timezone", "UTC",
            "--as-of", "",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("lifecycle_as_of", result.stderr)

    def test_snapshot_input_cli_requires_extended_iso_dates(self):
        result = self.run_cli(
            "portable",
            "snapshot-input",
            "--since", "20260802",
            "--until", "2026-08-02",
            "--timezone", "UTC",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("snapshot dates must use YYYY-MM-DD", result.stderr)

    def test_snapshot_input_cli_rejects_sensitive_filter_before_store_access(self):
        result = self.run_cli(
            "portable",
            "snapshot-input",
            "--since", "2026-08-02",
            "--until", "2026-08-02",
            "--timezone", "UTC",
            "--project", "/Users/alice/private-project",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("sensitive path or credential", result.stderr)


if __name__ == "__main__":
    unittest.main()
