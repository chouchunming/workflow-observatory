import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
CODEX_CLI = PLUGIN_ROOT / "scripts/workflow_observer_cli.py"
CLAUDE_CLI = PLUGIN_ROOT / "scripts/claude_workflow_observer_cli.py"

SCOPE = """## Scope

- Goal: Verify platform-isolated observation homes
- Included: CLI lifecycle and surface labels
- Excluded: Session binding
"""

COMPLETION = """## Execution evidence

- Verification: focused tests pass
- Artifacts: isolated observation record

## Outcome and observation

- Outcome: Claude lifecycle completed
- Observation: Platform homes stayed isolated

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


class ClaudeWorkflowObserverCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.user_home = self.base / "user-home"
        self.user_home.mkdir()
        self.subject = self.base / "subject"
        self.subject.mkdir()
        self.scope = self.base / "scope.md"
        self.completion = self.base / "completion.md"
        write_private(self.scope, SCOPE)
        write_private(self.completion, COMPLETION)

    def tearDown(self):
        self.temporary.cleanup()

    def run_cli(
        self,
        cli: Path,
        *arguments: str,
        observer_home: Path | str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = {
            **os.environ,
            "HOME": str(self.user_home),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        environment.pop("WORKFLOW_OBSERVATORY_HOME", None)
        if observer_home is not None:
            environment["WORKFLOW_OBSERVATORY_HOME"] = str(observer_home)
        return subprocess.run(
            [sys.executable, str(cli), *arguments],
            cwd=PLUGIN_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def start(
        self,
        cli: Path,
        surface: str,
        *,
        observer_home: Path | str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return self.run_cli(
            cli,
            "start",
            "--title",
            f"{surface} example",
            "--subject-root",
            str(self.subject),
            "--agent-surface",
            surface,
            "--start-mode",
            "planned",
            "--task-type",
            "feature",
            "--workflow-variant",
            "implementation-with-review",
            "--scope-from-file",
            str(self.scope),
            observer_home=observer_home,
        )

    @staticmethod
    def records(home: Path) -> list[Path]:
        return sorted((home / "store/wiki/observations").glob("obs-*.md"))

    def test_platform_defaults_are_isolated_and_surfaces_are_fixed(self):
        codex_home = self.user_home / ".codex/workflow-observatory"
        claude_home = self.user_home / ".claude/workflow-observatory"

        codex_started = self.start(CODEX_CLI, "codex")
        self.assertEqual((0, ""), (codex_started.returncode, codex_started.stderr))
        self.assertEqual(1, len(self.records(codex_home)))
        self.assertFalse(claude_home.exists())

        claude_started = self.start(CLAUDE_CLI, "claude")
        self.assertEqual((0, ""), (claude_started.returncode, claude_started.stderr))
        run_id = claude_started.stdout.strip()
        self.assertEqual(1, len(self.records(codex_home)))
        self.assertEqual(1, len(self.records(claude_home)))
        self.assertIn(
            'agent_surface: "claude"',
            self.records(claude_home)[0].read_text(encoding="utf-8"),
        )

        finished = self.run_cli(
            CLAUDE_CLI,
            "finish",
            run_id,
            "--status",
            "success",
            "--from-file",
            str(self.completion),
        )
        report = self.run_cli(CLAUDE_CLI, "report")
        integrity = self.run_cli(CLAUDE_CLI, "integrity")
        self.assertEqual((0, "", ""), (finished.returncode, finished.stdout, finished.stderr))
        self.assertEqual((0, ""), (report.returncode, report.stderr))
        self.assertIn("success=1", report.stdout)
        self.assertEqual(
            (0, "healthy records=1 invalidated=0\n", ""),
            (integrity.returncode, integrity.stdout, integrity.stderr),
        )

        relabeled_claude = self.start(CLAUDE_CLI, "codex")
        relabeled_codex = self.start(CODEX_CLI, "claude")
        for rejected in (relabeled_claude, relabeled_codex):
            self.assertEqual(2, rejected.returncode)
            self.assertEqual("", rejected.stdout)
            self.assertIn("invalid choice", rejected.stderr)
        self.assertEqual(1, len(self.records(codex_home)))
        self.assertEqual(1, len(self.records(claude_home)))

    def test_absolute_environment_overrides_remain_isolated(self):
        codex_override = self.base / "codex-override"
        claude_override = self.base / "claude-override"

        codex_started = self.start(
            CODEX_CLI, "codex", observer_home=codex_override
        )
        claude_started = self.start(
            CLAUDE_CLI, "claude", observer_home=claude_override
        )

        self.assertEqual((0, 0), (codex_started.returncode, claude_started.returncode))
        self.assertEqual(1, len(self.records(codex_override)))
        self.assertEqual(1, len(self.records(claude_override)))
        self.assertFalse((self.user_home / ".codex").exists())
        self.assertFalse((self.user_home / ".claude").exists())

    def test_relative_environment_override_is_rejected_without_writes(self):
        result = self.start(
            CLAUDE_CLI,
            "claude",
            observer_home="relative-observer-home",
        )

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertIn("observation home must be absolute", result.stderr)
        self.assertFalse((PLUGIN_ROOT / "relative-observer-home").exists())
        self.assertFalse((self.user_home / ".claude").exists())

    def test_codex_only_llmwiki_delegate_fails_closed_for_claude(self):
        claude_home = self.user_home / ".claude/workflow-observatory"
        llmwiki_root = self.base / "codex-only-wiki"
        llmwiki_root.mkdir()
        fake_cli = llmwiki_root / "wiki_cli.py"
        fake_cli.write_text(
            """import pathlib
import sys

arguments = sys.argv[1:]
surface = arguments[arguments.index('--agent-surface') + 1]
if surface != 'codex':
    print("observation validation error: argument --agent-surface: invalid choice: 'claude'", file=sys.stderr)
    raise SystemExit(2)
observations = pathlib.Path(arguments[arguments.index('--wiki-root') + 1]) / 'wiki/observations'
observations.mkdir(parents=True, exist_ok=True)
(observations / 'codex-substitute.md').write_text('agent_surface: codex\\n', encoding='utf-8')
print('obs-20260719-120000-abcdef')
""",
            encoding="utf-8",
        )
        claude_home.mkdir(parents=True)
        (claude_home / "config.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "adapter": "llmwiki",
                    "cli_path": str(fake_cli),
                    "wiki_root": str(llmwiki_root),
                }
            ),
            encoding="utf-8",
        )

        result = self.start(CLAUDE_CLI, "claude")

        self.assertEqual(2, result.returncode)
        self.assertEqual("", result.stdout)
        self.assertTrue(result.stderr.startswith("workflow observer validation error:"))
        self.assertFalse((claude_home / "store").exists())
        self.assertFalse((claude_home / "sessions").exists())
        self.assertFalse((claude_home / "session-bindings").exists())
        self.assertFalse((llmwiki_root / "wiki").exists())
        self.assertEqual(["config.json"], sorted(path.name for path in claude_home.iterdir()))


if __name__ == "__main__":
    unittest.main()
