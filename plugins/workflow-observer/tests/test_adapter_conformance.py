import json
import hashlib
import os
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

    def test_llmwiki_schema_v1_lifecycle_and_report_remain_delegated(self):
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
        self.assertEqual((0, "delegated report\n", ""),
                         (report.returncode, report.stdout, report.stderr))
        calls = marker.read_text(encoding="utf-8")
        self.assertIn(f"finish {run_id}", calls)
        self.assertIn(" start ", f" {calls}")
        self.assertIn(" report", calls)

    def test_llmwiki_schema_v2_lifecycle_uses_bundled_core(self):
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
        self.assertFalse(marker.exists())
        record = (self.llm_root / "wiki/observations" / f"{run_id}.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("schema_version: 2\n", record)
        self.assertIn("## Episode data\n", record)

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


if __name__ == "__main__":
    unittest.main()
