import json
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
SCOPE_TEXT = """## Scope

- Goal: Compare adapters
- Included: Shared lifecycle matrix
- Excluded: None
"""
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
        write_private(self.scope, SCOPE_TEXT)
        write_private(self.completion, COMPLETION_TEXT)

        self.homes = {
            "portable": self.base / "portable-home",
            "llmwiki": self.base / "llmwiki-home",
        }
        llm_root = self.base / "temporary llm wiki"
        llm_root.mkdir()
        shutil.copy2(REPOSITORY_ROOT / "wiki_cli.py", llm_root / "wiki_cli.py")
        shutil.copy2(REPOSITORY_ROOT / "wiki_observations.py", llm_root / "wiki_observations.py")
        self.homes["llmwiki"].mkdir()
        (self.homes["llmwiki"] / "config.json").write_text(
            json.dumps({
                "schema_version": 1,
                "adapter": "llmwiki",
                "cli_path": str(llm_root / "wiki_cli.py"),
                "wiki_root": str(llm_root),
            }),
            encoding="utf-8",
        )
        (llm_root / "wiki/observations/.locks").mkdir(parents=True)
        (llm_root / "wiki/observations/invalidations").mkdir()

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

    def start(self, adapter: str, *, title="Matrix Example", task_type="maintenance",
              variant="maintenance-basic"):
        return self.run_cli(
            adapter,
            "start",
            "--title", title,
            "--subject-root", str(self.subject),
            "--agent-surface", "codex",
            "--start-mode", "planned",
            "--task-type", task_type,
            "--workflow-variant", variant,
            "--scope-from-file", str(self.scope),
        )

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
