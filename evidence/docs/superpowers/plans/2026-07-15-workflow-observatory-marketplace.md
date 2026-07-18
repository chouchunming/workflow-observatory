# Workflow Observatory Marketplace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revision: v2 — Task 6 hybrid evaluator amendment approved 2026-07-17.

**Goal:** Build a shareable `workflow-observatory` Codex marketplace whose `workflow-observer` plugin records eligible workflows locally, optionally targets LLM Wiki, and separates telemetry, learning, and improvement responsibilities.

**Architecture:** A repo-local marketplace contains one plugin and four sibling skills. An adapter-neutral CLI selects either a bundled portable observation core or an explicitly configured LLM Wiki CLI. The automatic observer is a compact router; telemetry owns exact recording mechanics, while learning and improving run only on demand. The frozen evaluator uses `codex exec` for 24 one-turn cases and app-server only for the four cases that require in-flight `turn/steer`, adapting both into one validation result.

**Tech Stack:** Python 3.11+, standard library, Markdown frontmatter, Codex plugin and skill manifests, `unittest`, JSON, ZIP, SHA-256.

## Global Constraints

- Canonical marketplace root: `marketplace/workflow-observatory/`; archive root: `workflow-observatory/`.
- Marketplace: `workflow-observatory`; display name: `Workflow Observatory`; plugin: `workflow-observer`.
- Skills: `workflow-observer`, `workflow-telemetry`, `workflow-learning`, `workflow-improving`.
- Default local-only data root: `~/.codex/workflow-observatory/`; no network transport.
- Payload limit: 65,536 bytes; scalar limit: 200 Unicode code points.
- Never record full prompts, transcripts, secrets, credentials, or subject absolute paths.
- One eligible top-level task creates exactly one start and at most one finish; workers inherit the parent marker.
- Learning needs at least five comparable final records; improving needs explicit user approval before mutation.
- Frozen decision manifest SHA-256: `f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`; frozen lifecycle manifest SHA-256: `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`.
- The 24 one-turn cases must use `codex exec --json --ephemeral`; the four two-turn cases must use app-server; any other turn count fails before model execution.
- Each `codex exec` turn has a fixed 20-minute wall-clock budget; app-server turns retain their separate 10-minute timeout. Either timeout remains fail-closed and cannot publish partial results.
- Model-bearing diagnostics and formal evaluation require an explicit protected no-write window. Diagnostics never persist authoritative results.
- `raw/` is out of marketplace implementation and evaluation scope. The required post-Task 6 LLM Wiki handoff may add one immutable session source through the repository's approved ingest workflow; it never edits or deletes existing raw bytes. Do not commit, publish, install globally, or edit a personal marketplace without explicit approval.

## File map

- `marketplace/workflow-observatory/.agents/plugins/marketplace.json`: marketplace catalog.
- `marketplace/workflow-observatory/plugins/workflow-observer/.codex-plugin/plugin.json`: plugin manifest.
- `.../scripts/wiki_observations.py`: parity copy of the validated domain core.
- `.../scripts/store_config.py`: local adapter configuration.
- `.../scripts/workflow_observer_cli.py`: adapter-neutral CLI.
- `.../skills/*/SKILL.md`: router, telemetry, learning, and improvement skills.
- `.../tests/`: portable manifest, runtime, adapter, privacy, and packaging tests.
- `scripts/run_observing_workflows_task9_eval.py`: transport-neutral frozen-case evaluator and atomic paired-result store.
- `tests/test_observing_workflows_task9_eval.py`: deterministic transport, guard, ledger, and result-store regressions.
- `scripts/package_workflow_observatory.py`: deterministic archive builder.
- `dist/workflow-observatory-0.1.0.zip`: verified deliverable.

---

### Task 1: Scaffold and lock marketplace manifests

**Files:**
- Create: `marketplace/workflow-observatory/.agents/plugins/marketplace.json`
- Create: `marketplace/workflow-observatory/README.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/.codex-plugin/plugin.json`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/README.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_manifests.py`

**Interfaces:**
- Consumes: approved marketplace design.
- Produces: the canonical marketplace and plugin roots.

- [ ] **Step 1: Write the failing identity test**

```python
import json
from pathlib import Path
import unittest

MARKET_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE = MARKET_ROOT / ".agents/plugins/marketplace.json"
PLUGIN_MANIFEST = PLUGIN_ROOT / ".codex-plugin/plugin.json"

class ManifestTests(unittest.TestCase):
    def test_marketplace_and_plugin_identity(self):
        market = json.loads(MARKETPLACE.read_text())
        plugin = json.loads(PLUGIN_MANIFEST.read_text())
        self.assertEqual("workflow-observatory", market["name"])
        self.assertEqual("Workflow Observatory", market["interface"]["displayName"])
        self.assertEqual("workflow-observer", plugin["name"])
        self.assertEqual("./skills/", plugin["skills"])
        entry = market["plugins"][0]
        self.assertEqual("./plugins/workflow-observer", entry["source"]["path"])
        self.assertEqual("AVAILABLE", entry["policy"]["installation"])
        self.assertEqual("ON_INSTALL", entry["policy"]["authentication"])
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_manifests.py' -v`

Expected: FAIL because the manifests are absent.

- [ ] **Step 3: Scaffold with the official helper**

```bash
python3 ${CODEX_HOME}/skills/.system/plugin-creator/scripts/create_basic_plugin.py workflow-observer --path marketplace/workflow-observatory/plugins --marketplace-path marketplace/workflow-observatory/.agents/plugins/marketplace.json --marketplace-name workflow-observatory --with-skills --with-scripts --with-marketplace --category Productivity
```

Set version `0.1.0`, license `MIT`, author `Workflow Observatory Contributors`, description `Local-first workflow observation, learning, and user-approved improvement for Codex.`, `skills: "./skills/"`, interface display name `Workflow Observer`, category `Productivity`, capabilities `Write` and `Analysis`, and brand color `#315C6D`. Do not add apps, MCP servers, hooks, assets, or product gating.

- [ ] **Step 4: Verify GREEN and review**

```bash
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/plugin-creator/scripts/validate_plugin.py marketplace/workflow-observatory/plugins/workflow-observer
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_manifests.py' -v
```

Expected: valid plugin and passing identity test. Stop for reviewer approval; do not commit.

---

### Task 2: Package the portable core and configuration

**Files:**
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/scripts/wiki_observations.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/scripts/core_source.json`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/scripts/store_config.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_core_parity.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_store_config.py`

**Interfaces:**
- Produces: `StoreConfig`, `ConfigError`, `load_store_config()`, `parse_store_config()`, and a hash-identified domain core.

- [ ] **Step 1: Write RED parity/config tests**

```python
import hashlib
import json
import os
import sys
from pathlib import Path
import unittest

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))
from store_config import ConfigError, StoreConfig, load_store_config, parse_store_config

BUNDLED_CORE = PLUGIN_ROOT / "scripts/wiki_observations.py"
CORE_SOURCE = PLUGIN_ROOT / "scripts/core_source.json"

def test_bundled_core_matches_declared_hash(self):
    declared = json.loads(CORE_SOURCE.read_text(encoding="utf-8"))
    self.assertEqual(declared["sha256"], hashlib.sha256(BUNDLED_CORE.read_bytes()).hexdigest())

def test_repository_copy_matches_when_source_is_configured(self):
    source_root = os.environ.get("LLMWIKI_SOURCE_ROOT")
    if source_root is None:
        self.skipTest("development source root not configured")
    self.assertEqual((Path(source_root) / "wiki_observations.py").read_bytes(),
                     BUNDLED_CORE.read_bytes())

def test_missing_config_selects_portable_store(self):
    config = load_store_config(home=Path("/tmp/example-home"), environ={})
    self.assertEqual(StoreConfig("portable", Path("/tmp/example-home/store"), None), config)

def test_llmwiki_requires_existing_cli(self):
    with self.assertRaisesRegex(ConfigError, "cli_path does not exist"):
        parse_store_config({"schema_version": 1, "adapter": "llmwiki",
            "cli_path": "/missing/wiki_cli.py", "wiki_root": "/missing/wiki"})
```

Run:

```bash
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_core_parity.py' -v
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_store_config.py' -v
```

Expected: FAIL on missing bundled modules.

- [ ] **Step 2: Copy the validated core mechanically**

Run: `cp wiki_observations.py marketplace/workflow-observatory/plugins/workflow-observer/scripts/wiki_observations.py`

Calculate SHA-256 from the copied bytes. Write `core_source.json` with schema version `1`, source `llmwiki/wiki_observations.py`, and the lowercase digest returned by `hashlib.sha256(BUNDLED_CORE.read_bytes()).hexdigest()`. The development gate runs with `LLMWIKI_SOURCE_ROOT=$PWD`; clean-room verification checks the declared hash without needing the original repository.

- [ ] **Step 3: Implement configuration**

```python
@dataclass(frozen=True)
class StoreConfig:
    adapter: Literal["portable", "llmwiki"]
    root: Path
    cli_path: Path | None

def load_store_config(home=None, environ=None):
    env = dict(os.environ if environ is None else environ)
    base = Path(env.get("WORKFLOW_OBSERVATORY_HOME",
        home or Path.home() / ".codex/workflow-observatory")).expanduser()
    path = base / "config.json"
    if not path.exists():
        return StoreConfig("portable", base / "store", None)
    return parse_store_config(json.loads(path.read_text(encoding="utf-8")))
```

`parse_store_config()` accepts only schema version `1`, rejects unknown keys, validates adapter names, requires existing LLM Wiki root/CLI paths, and prevents symlink escape. Absolute paths remain local configuration and never enter records.

- [ ] **Step 4: Verify GREEN and review**

Repeat both Task 2 commands. Expected: PASS. Stop for reviewer approval; do not commit.

---

### Task 3: Implement the adapter-neutral CLI

**Files:**
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_portable_cli.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_adapter_conformance.py`

**Interfaces:**
- Consumes: `StoreConfig` and bundled `wiki_observations` API.
- Produces: `main(argv: Sequence[str] | None = None) -> int` with `start`, `finish`, `report`, `validate`, and `integrity`.

`validate` and `integrity` are read-only for both adapters and use the bundled core against the selected root because the existing LLM Wiki CLI has no matching subcommands. `validate` prints `valid records=<N> invalidated=<M>` after schema/lifecycle validation. `integrity` prints `healthy records=<N> invalidated=<M>` after validation plus strict layout checks: only run-ID Markdown records, `.locks` containing private regular run-ID lock files, and `invalidations` containing regular run-ID tombstones are allowed; symlinks, malformed names, other directories/files, and temporary/backup artifacts fail. A missing portable root is a healthy empty read-only result and is not created. Only portable `start` initializes storage; missing-root `finish` is a state error. LLM Wiki `start`, `finish`, and `report` delegate through configured argv; its `validate` and `integrity` use the same bundled read-only implementation as portable storage.

- [ ] **Step 1: Write the RED lifecycle test**

```python
import os
from pathlib import Path
import subprocess
import sys

PLUGIN_ROOT = Path(__file__).resolve().parents[1]

def run_cli(home: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "WORKFLOW_OBSERVATORY_HOME": str(home)}
    return subprocess.run(
        [sys.executable, str(PLUGIN_ROOT / "scripts/workflow_observer_cli.py"),
         *arguments],
        cwd=PLUGIN_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )

def test_start_finish_validate_report(self):
    started = run_cli(self.home, "start", "--title", "Example", "--subject-root",
        str(self.subject), "--agent-surface", "codex", "--start-mode", "planned",
        "--task-type", "maintenance", "--workflow-variant", "maintenance-basic",
        "--scope-from-file", str(self.scope))
    run_id = started.stdout.strip()
    self.assertRegex(run_id, r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
    self.assertEqual(0, run_cli(self.home, "finish", run_id, "--status", "success",
        "--from-file", str(self.completion)).returncode)
    self.assertEqual(0, run_cli(self.home, "validate").returncode)
    self.assertIn("maintenance-basic", run_cli(self.home, "report").stdout)
```

Add one conformance matrix that runs portable and temporary LLM Wiki adapters with identical payloads and compares normalized run count, status, taxonomy rejection, 200-code-point rejection, double-finish rejection, report grouping, and absence of subject paths. Run it; expected: FAIL because the CLI is absent.

- [ ] **Step 2: Implement argv-safe selection**

```python
def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    config = load_store_config()
    if config.adapter == "llmwiki":
        command = [sys.executable, str(config.cli_path), "observe",
                   "--wiki-root", str(config.root), *normalized_args(args)]
        return subprocess.run(command, check=False).returncode
    initialize_portable_root(config.root)
    return run_portable(args, ObservationPaths.from_root(config.root))
```

Portable initialization creates only `wiki/observations/.locks` and `wiki/observations/invalidations` under a private data root. Delegation uses argv arrays, never shell strings. Validation/state errors exit `2`, I/O errors exit `1`, and stdout contains only run IDs or requested reports.

- [ ] **Step 3: Verify GREEN and review**

Run:

```bash
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_portable_cli.py' -v
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -p 'test_adapter_conformance.py' -v
```

Expected: both adapters pass. Stop for reviewer approval; do not commit.

---

### Task 4: Split observer and telemetry contracts

**Files:**
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-observer/SKILL.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-telemetry/SKILL.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_skill_contracts.py`
- Modify: `skills/observing-workflows/README.md`

**Interfaces:**
- Produces: one automatic router and one complete recording contract.

- [ ] **Step 1: Write RED contract tests**

```python
def test_observer_routes_once_to_telemetry(self):
    text = observer_skill()
    self.assertIn("Read `../workflow-telemetry/SKILL.md` before the first real start", text)
    self.assertIn("exactly one start and at most one finish", text)
    self.assertIn("Do not inspect the draft or run help after start", text)

def test_telemetry_owns_limits_and_enums(self):
    text = telemetry_skill()
    for token in ("65536 bytes", "200 Unicode code points", "maintenance-basic",
                  "success", "partial", "failed", "rolled-back", "superseded"):
        self.assertIn(token, text)
```

Run the module. Expected: FAIL because both skills are absent.

- [ ] **Step 2: Write the compact automatic router**

Its procedure is: decide once; read telemetry; start once before mutation; retain only run ID; propagate the exact parent marker; never inspect draft or run help after start; create one bounded completion payload; finish at most once; disclose failures. Frozen evaluation does not retry a rejected payload-bearing call.

- [ ] **Step 3: Write the complete telemetry contract**

Include the task/variant matrix, five final statuses, exact size limits, Scope/completion templates, secure unique mode-0600 files, adapter command resolution, partial/superseded rules, cleanup, and sanitization. Forbid prompts, transcripts, secrets, subject paths, payload reuse, combined probes, draft inspection, and help after start.

- [ ] **Step 4: Document migration and verify GREEN**

The old README states that `observing-workflows` remains active until marketplace installation checks pass and that old/new automatic descriptions must never run together. Validate both skills with `quick_validate.py`, run the contract module, and stop for review. Do not commit.

---

### Task 5: Add learning and improving skills

**Files:**
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-learning/SKILL.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-improving/SKILL.md`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_learning_improving.py`

**Interfaces:**
- Consumes: read-only validated reports.
- Produces: descriptive learning and approval-gated improvement procedures.

- [ ] **Step 1: Write RED policy tests**

```python
def test_learning_requires_five_records(self):
    self.assertIn("at least 5 comparable final records", learning_skill())
    self.assertIn("small sample", learning_skill())

def test_improving_requires_evidence_and_approval(self):
    text = improving_skill()
    self.assertIn("cite observation group keys", text)
    self.assertIn("explicit user approval", text)
    self.assertNotIn("automatically apply", text.lower())
```

- [ ] **Step 2: Implement bounded procedures**

Learning calls only report/validate, excludes drafts and invalidations from rates, labels groups below five, and separates observation from inference. Improving cites project/workspace/task/variant keys, proposes one bounded change with rollback and measurement criteria, and stops for approval before mutation.

- [ ] **Step 3: Verify GREEN and review**

Run Task 5 tests and all four official skill validations. Expected: PASS without network or record writes. Stop for reviewer approval; do not commit.

---

### Task 6: Port and harden the frozen hybrid evaluation

Task 6A through 6D are one release gate. Do not begin Task 7 until every deterministic test, the non-persisting diagnostic, the non-authoritative 20+8 discovery sweep, independent review, the protected 20+8 formal run, and the required LLM Wiki handoff below are complete.

Historical constraints remain evidence, not work to repeat: the accepted isolated preflight remains accepted; revision 5 failed on unqualified `python`; earlier revision-6 attempts were invalidated by concurrent writes, timed out, or exposed an app-server custom-tool transport stall. None produced an authoritative result pair.

**July 19 execution amendment:** the user explicitly accepted Task 6 under a
composite boundary consisting of 26 consecutive protected formal passes plus
targeted passes for `complete-eval-override` and
`incomplete-eval-override`. The fixture-isolation repair received deterministic
coverage and independent review, and both frozen manifests remained unchanged.
This amendment authorizes Tasks 7–8 without claiming that the evidence is one
uninterrupted formal 28/28 run. No authoritative atomic result pair or commit
pointer was produced; the original all-green publication rule remains the
default for future evaluator epochs.

#### Task 6A: Introduce a transport-neutral result and fail-closed routing

**Files:**
- Modify: `scripts/run_observing_workflows_task9_eval.py`
- Modify: `tests/test_observing_workflows_task9_eval.py`

**Interfaces:**
- Produces: `TransportName = Literal["exec", "app-server"]`.
- Produces: `CaseExecution(terminal_status, final_text, command_executions, observation_command_diagnostics)`.
- Produces: `select_case_transport(case: dict) -> TransportName`.

- [ ] **Step 1: Write RED routing and common-result tests**

Add these tests to `Task9EvalRunnerTests`:

```python
def test_transport_selection_is_derived_only_from_turn_count(self):
    self.assertEqual("exec", task9_eval.select_case_transport({"turns": [{}]}))
    self.assertEqual(
        "app-server", task9_eval.select_case_transport({"turns": [{}, {}]})
    )
    for turns in ([], [{}, {}, {}]):
        with self.subTest(turns=len(turns)):
            with self.assertRaisesRegex(ValueError, "unsupported turn count"):
                task9_eval.select_case_transport({"turns": turns})

def test_frozen_manifests_route_exactly_24_exec_and_4_app_server(self):
    repository = Path(__file__).resolve().parents[1]
    paths = (
        repository / "tests/skill_evals/observing_workflows_cases.json",
        repository / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    )
    cases = [row for path in paths for row in json.loads(path.read_text())]
    routes = [task9_eval.select_case_transport(case) for case in cases]
    self.assertEqual(24, routes.count("exec"))
    self.assertEqual(4, routes.count("app-server"))
    self.assertEqual(
        {
            ("late-trigger", "app-server"),
            ("late-success", "app-server"),
            ("scope-supersession", "app-server"),
        },
        {(case["id"], route) for case, route in zip(cases, routes) if route == "app-server"},
    )
```

The set intentionally collapses the two manifest-local `scope-supersession` IDs; the route count proves there are four cases.

- [ ] **Step 2: Run the focused tests and observe RED**

```bash
python3 -m unittest \
  tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_transport_selection_is_derived_only_from_turn_count \
  tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests.test_frozen_manifests_route_exactly_24_exec_and_4_app_server -v
```

Expected: both tests fail because `select_case_transport` does not exist.

- [ ] **Step 3: Add the minimal common types and router**

Add `Literal` to the typing imports and place this beside `CaseRuntime`:

```python
TransportName = Literal["exec", "app-server"]


@dataclass(frozen=True)
class CaseExecution:
    terminal_status: str
    final_text: str
    command_executions: tuple[str, ...]
    observation_command_diagnostics: tuple[dict[str, object], ...]


def select_case_transport(case: dict) -> TransportName:
    turn_count = len(case.get("turns", ()))
    if turn_count == 1:
        return "exec"
    if turn_count == 2:
        return "app-server"
    raise ValueError(f"unsupported turn count: {turn_count}")
```

- [ ] **Step 4: Re-run the focused tests and review the boundary**

Expected: PASS. Confirm routing reads no case ID, prompt, expected result, or mutable side table. Do not commit.

#### Task 6B: Add the bounded `codex exec` transport

**Files:**
- Modify: `scripts/run_observing_workflows_task9_eval.py`
- Modify: `tests/test_observing_workflows_task9_eval.py`

**Interfaces:**
- Produces: `build_codex_config_overrides(environment, disabled_skill_paths) -> tuple[str, ...]` used by both transports.
- Produces: `build_exec_command(cwd, writable_roots, output_path, overrides) -> list[str]`.
- Produces: `parse_exec_jsonl(stdout: str, final_text: str) -> CaseExecution`.
- Produces: `ExecTransport(cwd: Path, runtime: CaseRuntime, popen_factory=subprocess.Popen)` and `run(prompt: str, timeout: float = EXEC_TURN_TIMEOUT_SECONDS) -> CaseExecution`.

- [ ] **Step 1: Write RED command-construction and JSONL parser tests**

Use the documented Codex JSONL event names and verify that the frozen prompt is never an argv element:

```python
def test_exec_command_is_ephemeral_json_fail_closed_and_prompt_free(self):
    root = Path("/fixture")
    output = Path("/audit/final.txt")
    overrides = ('approval_policy="never"', 'web_search="disabled"')
    command = task9_eval.build_exec_command(
        root, [Path("/store"), Path("/audit")], output, overrides
    )
    self.assertEqual(["codex", "exec"], command[:2])
    for flag in ("--json", "--ephemeral", "--ignore-rules"):
        self.assertIn(flag, command)
    self.assertIn("workspace-write", command)
    self.assertEqual("-", command[-1])
    self.assertNotIn("synthetic secret prompt", command)
    self.assertEqual(2, command.count("--add-dir"))

def test_exec_jsonl_normalizes_completed_turn(self):
    stdout = "\n".join((
        '{"type":"thread.started","thread_id":"thread-1"}',
        '{"type":"turn.started"}',
        '{"type":"item.started","item":{"id":"cmd-1","type":"command_execution","command":"python3 -m unittest","status":"in_progress"}}',
        '{"type":"item.completed","item":{"id":"cmd-1","type":"command_execution","command":"python3 -m unittest","aggregated_output":"OK","exit_code":0,"status":"completed"}}',
        '{"type":"item.completed","item":{"id":"msg-1","type":"agent_message","text":"done"}}',
        '{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}',
    ))
    result = task9_eval.parse_exec_jsonl(stdout, "done")
    self.assertEqual("completed", result.terminal_status)
    self.assertEqual("done", result.final_text)
    self.assertEqual(("python3 -m unittest",), result.command_executions)
```

Add this table-driven failure test; use hashed/bounded exceptions so none of the sentinel strings can escape:

```python
def test_exec_jsonl_rejects_incomplete_or_failed_protocol_without_leaking(self):
    secret = "PROMPT_SECRET command-secret stderr-secret tool-output-secret"
    cases = {
        "malformed": "not-json " + secret,
        "error": json.dumps({"type": "error", "message": secret}),
        "turn-failed": json.dumps({"type": "turn.failed", "error": {"message": secret}}),
        "missing-terminal": json.dumps({
            "type": "item.completed",
            "item": {"id": "msg", "type": "agent_message", "text": "done"},
        }),
        "active-command": "\n".join((
            json.dumps({"type": "item.started", "item": {
                "id": "cmd", "type": "command_execution", "command": secret,
            }}),
            json.dumps({"type": "item.completed", "item": {
                "id": "msg", "type": "agent_message", "text": "done",
            }}),
            json.dumps({"type": "turn.completed", "usage": {}}),
        )),
        "missing-agent": json.dumps({"type": "turn.completed", "usage": {}}),
    }
    for label, stdout in cases.items():
        with self.subTest(label=label):
            with self.assertRaises((ValueError, RuntimeError)) as caught:
                task9_eval.parse_exec_jsonl(stdout, "done")
            self.assertNotIn(secret, str(caught.exception))

def test_exec_jsonl_rejects_final_message_disagreement(self):
    stdout = "\n".join((
        json.dumps({"type": "item.completed", "item": {
            "id": "msg", "type": "agent_message", "text": "event-final",
        }}),
        json.dumps({"type": "turn.completed", "usage": {}}),
    ))
    with self.assertRaisesRegex(ValueError, "final message mismatch"):
        task9_eval.parse_exec_jsonl(stdout, "file-final")
```

- [ ] **Step 2: Run the parser tests and observe RED**

```bash
python3 -m unittest tests.test_observing_workflows_task9_eval.Task9EvalRunnerTests -k exec -v
```

Expected: new exec tests fail because the builder, parser, and transport do not exist; existing app-server tests remain green.

- [ ] **Step 3: Share the exact config overrides**

Replace app-server's ad hoc `-c` construction and use this helper from both transports:

```python
def build_codex_config_overrides(
    environment: dict[str, str], disabled_skill_paths: tuple[Path, ...]
) -> tuple[str, ...]:
    overrides = [
        build_shell_environment_override(environment),
        'approval_policy="never"',
        'web_search="disabled"',
        "features.multi_agent=true",
    ]
    if disabled_skill_paths:
        overrides.append(build_disabled_skills_override(disabled_skill_paths))
    return tuple(overrides)
```

Both processes continue to read the same Codex model and reasoning configuration; neither transport adds a transport-specific model override. App-server still sets `approvalPolicy: never`, `workspaceWrite`, and `networkAccess: false` in protocol requests as a second fail-closed layer.

- [ ] **Step 4: Implement the exec command and strict event normalizer**

`build_exec_command` must emit this shape, appending one `-c VALUE` per common override and one `--add-dir PATH` per case-local writable root:

```python
def build_exec_command(
    cwd: Path,
    writable_roots: list[Path],
    output_path: Path,
    overrides: tuple[str, ...],
) -> list[str]:
    command = [
        "codex", "exec", "--json", "--ephemeral", "--ignore-rules",
        "--sandbox", "workspace-write", "-C", str(cwd),
        "-o", str(output_path),
    ]
    for override in overrides:
        command.extend(("-c", override))
    for root in writable_roots:
        command.extend(("--add-dir", str(root)))
    command.append("-")
    return command
```

Implement the strict event normalizer as follows; unknown informational event types may pass, but malformed lifecycle state fails:

```python
def parse_exec_jsonl(stdout: str, final_text: str) -> CaseExecution:
    active_commands: dict[str, str] = {}
    command_executions: list[str] = []
    observation_diagnostics: list[dict[str, object]] = []
    agent_messages: list[str] = []
    terminal_count = 0
    for line_number, line in enumerate(stdout.splitlines(), 1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(
                "malformed codex exec JSONL: "
                f"line={line_number}; summary={_sensitive_text_summary(line)!r}"
            ) from error
        if not isinstance(event, dict):
            raise ValueError(f"codex exec event is not an object: line={line_number}")
        event_type = event.get("type")
        if event_type in ("error", "turn.failed"):
            raise RuntimeError(
                "codex exec protocol failure: "
                f"type={_safe_protocol_label(event_type)}; "
                f"summary={_sensitive_text_summary(line)!r}"
            )
        if event_type == "turn.completed":
            terminal_count += 1
            continue
        if event_type not in ("item.started", "item.completed"):
            continue
        item = event.get("item")
        if not isinstance(item, dict):
            raise ValueError(f"codex exec item is not an object: line={line_number}")
        item_id = item.get("id")
        item_type = item.get("type")
        if event_type == "item.started" and item_type == "command_execution":
            if not isinstance(item_id, str) or not isinstance(item.get("command"), str):
                raise ValueError("codex exec command start is malformed")
            if item_id in active_commands:
                raise ValueError("codex exec command started twice")
            active_commands[item_id] = item["command"]
        elif event_type == "item.completed" and item_type == "command_execution":
            if not isinstance(item_id, str) or item_id not in active_commands:
                raise ValueError("codex exec command completed without a start")
            command = active_commands.pop(item_id)
            if item.get("command") != command or item.get("status") != "completed":
                raise ValueError("codex exec command completion is inconsistent")
            command_executions.append(command)
            if "workflow_observer_cli.py" in command or " observe " in command:
                observation_diagnostics.append({
                    "command": command,
                    "exit_code": item.get("exit_code"),
                    "output": item.get("aggregated_output"),
                })
        elif event_type == "item.completed" and item_type == "agent_message":
            text = item.get("text")
            if not isinstance(text, str):
                raise ValueError("codex exec agent message is malformed")
            agent_messages.append(text)
    if active_commands:
        raise RuntimeError(
            "codex exec ended with active commands: "
            f"count={len(active_commands)}; ids="
            f"{[_safe_protocol_label(value) for value in list(active_commands)[-6:]]!r}"
        )
    if terminal_count != 1:
        raise RuntimeError(f"codex exec terminal events: expected 1, got {terminal_count}")
    if not agent_messages:
        raise RuntimeError("codex exec final agent message is missing")
    if agent_messages[-1].rstrip("\n") != final_text.rstrip("\n"):
        raise ValueError("codex exec final message mismatch")
    return CaseExecution(
        terminal_status="completed",
        final_text=agent_messages[-1],
        command_executions=tuple(command_executions),
        observation_command_diagnostics=tuple(observation_diagnostics),
    )
```

It returns only `CaseExecution`; it never returns raw protocol events.

The active-command error keeps the total count but emits at most the deterministic last six safe IDs. Add a 20-command, 80-character-ID regression asserting `count=20`, sentinel redaction, and `len(str(exception)) < 1024`.

- [ ] **Step 5: Implement bounded subprocess lifecycle and cleanup**

Implement `ExecTransport` with an injectable `popen_factory`. Its constructor stores `cwd`, `runtime`, and the factory. `run` creates `runtime.audit.root / "exec-final-message.txt"`, calls `communicate(input=prompt, timeout=timeout)` so the prompt is stdin-only, and uses `parse_exec_jsonl` only after exit zero. On timeout it terminates, waits five seconds, kills if necessary, and raises a message built only from PID, return code, event-name/count summaries, character counts, and SHA-256 digests from `_bounded_sensitive_summaries`. On nonzero exit it applies the same redaction. It unlinks the final-message file in `finally`, whether parsing succeeds or fails.

The implementation skeleton is:

```python
def _coerce_diagnostic_text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value if isinstance(value, str) else ""


def _exec_event_summary(value: str | bytes | None) -> dict[str, object]:
    counts: dict[str, int] = {}
    last_event = "none"
    active_commands: set[str] = set()
    for line in _coerce_diagnostic_text(value).splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        event_type = _safe_protocol_label(event.get("type"))
        last_event = event_type
        counts[event_type] = counts.get(event_type, 0) + 1
        item = event.get("item") if isinstance(event.get("item"), dict) else {}
        item_id = item.get("id")
        if isinstance(item_id, str) and item.get("type") == "command_execution":
            if event.get("type") == "item.started":
                active_commands.add(item_id)
            elif event.get("type") == "item.completed":
                active_commands.discard(item_id)
    return {
        "event_count": sum(counts.values()),
        "last_event": last_event,
        "active_command_count": len(active_commands),
        "event_types": dict(sorted(counts.items())[-6:]),
    }


class ExecTransport:
    def __init__(
        self,
        cwd: Path,
        runtime: CaseRuntime,
        popen_factory=subprocess.Popen,
    ):
        self.cwd = cwd
        self.runtime = runtime
        self.popen_factory = popen_factory

    def run(
        self, prompt: str, timeout: float = EXEC_TURN_TIMEOUT_SECONDS
    ) -> CaseExecution:
        output_path = self.runtime.audit.root / "exec-final-message.txt"
        overrides = build_codex_config_overrides(
            self.runtime.environment, self.runtime.disabled_skill_paths
        )
        command = build_exec_command(
            self.cwd, self.runtime.writable_roots, output_path, overrides
        )
        process = self.popen_factory(
            command,
            cwd=self.cwd,
            env={**os.environ, **self.runtime.environment},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = ""
        stderr = ""
        try:
            try:
                stdout, stderr = process.communicate(input=prompt, timeout=timeout)
            except subprocess.TimeoutExpired as error:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                raise TimeoutError(
                    "codex exec timeout: "
                    f"pid={process.pid}; returncode={process.poll()}; "
                    f"events={_exec_event_summary(error.stdout)!r}; "
                    f"stdout={_sensitive_text_summary(_coerce_diagnostic_text(error.stdout))!r}; "
                    f"stderr={_sensitive_text_summary(_coerce_diagnostic_text(error.stderr))!r}"
                ) from error
            if process.returncode != 0:
                raise RuntimeError(
                    "codex exec failed: "
                    f"pid={process.pid}; returncode={process.returncode}; "
                    f"events={_exec_event_summary(stdout)!r}; "
                    f"stdout={_sensitive_text_summary(stdout)!r}; "
                    f"stderr={_sensitive_text_summary(stderr)!r}"
                )
            if not output_path.is_file():
                raise RuntimeError("codex exec final message is missing")
            return parse_exec_jsonl(
                stdout, output_path.read_text(encoding="utf-8").rstrip("\n")
            )
        finally:
            output_path.unlink(missing_ok=True)
```

Never interpolate subprocess output itself. Six retained event-type labels keep the valid worst-case exception below the existing 1,024-character diagnostic bound; add a regression that constructs six 80-character safe labels and asserts both sentinel redaction and `len(str(exception)) < 1024`.

Use one fake process for the success and timeout lifecycle tests:

```python
class FakeExecProcess:
    pid = 4321

    def __init__(self, *, stdout="", stderr="", returncode=0, timeout=False):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.timeout = timeout
        self.calls = []

    def communicate(self, *, input, timeout):
        self.calls.append(("communicate", input, timeout))
        if self.timeout:
            raise subprocess.TimeoutExpired("codex", timeout, self.stdout, self.stderr)
        return self.stdout, self.stderr

    def poll(self):
        return self.returncode

    def terminate(self):
        self.calls.append(("terminate",))

    def wait(self, timeout):
        self.calls.append(("wait", timeout))
        if self.timeout and not any(call[0] == "kill" for call in self.calls):
            raise subprocess.TimeoutExpired("codex", timeout)
        return self.returncode

    def kill(self):
        self.calls.append(("kill",))

def test_exec_transport_sends_prompt_only_on_stdin_and_deletes_output(self):
    stdout = "\n".join((
        json.dumps({"type": "item.completed", "item": {
            "id": "msg", "type": "agent_message", "text": "done",
        }}),
        json.dumps({"type": "turn.completed", "usage": {}}),
    ))
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        audit_root = root / "audit"
        payload_dir = audit_root / "tmp"
        store = root / "store"
        payload_dir.mkdir(parents=True)
        store.mkdir()
        audit = task9_eval.RuntimePayloadAudit(
            root=audit_root,
            payload_dir=payload_dir,
            log_path=audit_root / "audit.jsonl",
            wrapper_path=audit_root / "workflow_observer_cli.py",
        )
        runtime = task9_eval.CaseRuntime(
            store_root=store,
            audit=audit,
            environment={},
            writable_roots=[store, audit_root],
        )
        process = FakeExecProcess(stdout=stdout)

        def popen_factory(command, **kwargs):
            self.assertNotIn("PROMPT_SECRET", command)
            Path(command[command.index("-o") + 1]).write_text("done", encoding="utf-8")
            return process

        transport = task9_eval.ExecTransport(root, runtime, popen_factory)
        result = transport.run("PROMPT_SECRET")
        self.assertEqual("done", result.final_text)
        self.assertEqual(
        ("communicate", "PROMPT_SECRET", task9_eval.EXEC_TURN_TIMEOUT_SECONDS),
            process.calls[0],
        )
        self.assertFalse((audit_root / "exec-final-message.txt").exists())

def test_exec_transport_timeout_terminates_then_kills_without_leaking(self):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        audit_root = root / "audit"
        payload_dir = audit_root / "tmp"
        store = root / "store"
        payload_dir.mkdir(parents=True)
        store.mkdir()
        runtime = task9_eval.CaseRuntime(
            store_root=store,
            audit=task9_eval.RuntimePayloadAudit(
                root=audit_root,
                payload_dir=payload_dir,
                log_path=audit_root / "audit.jsonl",
                wrapper_path=audit_root / "workflow_observer_cli.py",
            ),
            environment={},
            writable_roots=[store, audit_root],
        )
        process = FakeExecProcess(
            stdout="PROMPT_SECRET", stderr="STDERR_SECRET", timeout=True
        )
        transport = task9_eval.ExecTransport(root, runtime, lambda *a, **k: process)
        with self.assertRaises(TimeoutError) as caught:
            transport.run("PROMPT_SECRET", timeout=0.01)
        self.assertEqual(
            ["communicate", "terminate", "wait", "kill", "wait"],
            [call[0] for call in process.calls],
        )
        self.assertNotIn("PROMPT_SECRET", str(caught.exception))
        self.assertNotIn("STDERR_SECRET", str(caught.exception))
        self.assertFalse((audit_root / "exec-final-message.txt").exists())

def test_exec_transport_cleans_output_on_nonzero_exit_and_parse_failure(self):
    for label, process in (
        ("nonzero", FakeExecProcess(stderr="STDERR_SECRET", returncode=7)),
        ("parse", FakeExecProcess(stdout="not-json PROMPT_SECRET")),
    ):
        with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            audit_root = root / "audit"
            payload_dir = audit_root / "tmp"
            store = root / "store"
            payload_dir.mkdir(parents=True)
            store.mkdir()
            runtime = task9_eval.CaseRuntime(
                store_root=store,
                audit=task9_eval.RuntimePayloadAudit(
                    root=audit_root,
                    payload_dir=payload_dir,
                    log_path=audit_root / "audit.jsonl",
                    wrapper_path=audit_root / "workflow_observer_cli.py",
                ),
                environment={},
                writable_roots=[store, audit_root],
            )

            def popen_factory(command, **kwargs):
                Path(command[command.index("-o") + 1]).write_text(
                    "file-final", encoding="utf-8"
                )
                return process

            transport = task9_eval.ExecTransport(root, runtime, popen_factory)
            with self.assertRaises((ValueError, RuntimeError)) as caught:
                transport.run("PROMPT_SECRET")
            self.assertNotIn("PROMPT_SECRET", str(caught.exception))
            self.assertNotIn("STDERR_SECRET", str(caught.exception))
            self.assertFalse((audit_root / "exec-final-message.txt").exists())
```

- [ ] **Step 6: Run exec and existing app-server tests**

```bash
python3 -m unittest tests.test_observing_workflows_task9_eval -v
```

Expected: all focused runner tests pass without launching `codex` or writing production files. Do not commit.

#### Task 6C: Integrate both transports into one case validator

**Files:**
- Modify: `scripts/run_observing_workflows_task9_eval.py`
- Modify: `tests/test_observing_workflows_task9_eval.py`
- Modify: `marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py`

**Interfaces:**
- Produces: `execute_case_transport(case: dict, workspace: Path, runtime: CaseRuntime, wiki_root: Path, after_first_turn: Callable[[], None] | None = None) -> CaseExecution`.
- Preserves: `_run_case(...) -> dict`, `run_suite(...) -> tuple[list[dict], list[dict]]`, the invocation ledger, store-integrity checks, production guard, and paired-result API.

- [ ] **Step 1: Write RED integration tests with fake transports**

Use fakes that fail if the wrong transport is constructed:

```python
def test_execute_case_transport_uses_exec_for_one_turn(self):
    expected = task9_eval.CaseExecution("completed", "done", (), ())
    calls = []

    class FakeExec:
        def __init__(self, cwd, runtime, popen_factory=subprocess.Popen):
            calls.append(("construct-exec", cwd, runtime))

        def run(self, prompt, timeout=task9_eval.EXEC_TURN_TIMEOUT_SECONDS):
            calls.append(("run-exec", prompt))
            return expected

    with mock.patch.object(task9_eval, "ExecTransport", FakeExec), \
         mock.patch.object(task9_eval, "AppServer", side_effect=AssertionError("app-server used")):
        result = task9_eval.execute_case_transport(
            {"turns": [{"prompt": "one"}]}, Path("/fixture"), mock.sentinel.runtime,
            Path("/store"), None,
        )
    self.assertIs(expected, result)
    self.assertEqual("run-exec", calls[-1][0])

def test_execute_case_transport_steers_two_turn_case(self):
    calls = []

    class FakeServer:
        def __init__(self, cwd, environment, disabled_skill_paths=()):
            self.agent_messages = ["done"]
            self.command_executions = ["python3 fixture.py"]
            self.observation_command_diagnostics = []
        def initialize(self): calls.append("initialize")
        def start_thread(self, cwd): calls.append("thread"); return "thread-1"
        def start_turn(self, *args): calls.append("turn"); return "turn-1"
        def steer(self, *args): calls.append("steer")
        def wait_turn(self, *args): calls.append("wait"); return {"status": "completed"}
        def close(self): calls.append("close")

    case = {"id": "late-trigger", "turns": [
        {"prompt": "first"}, {"prompt": "second", "dispatch_when": "after_draft_run"},
    ]}
    with mock.patch.object(task9_eval, "AppServer", FakeServer), \
         mock.patch.object(task9_eval, "ExecTransport", side_effect=AssertionError("exec used")), \
         mock.patch.object(task9_eval, "_wait_for_gate", side_effect=lambda *a: calls.append("gate")), \
         mock.patch.object(task9_eval, "release_gate", side_effect=lambda *a: calls.append("release")):
        result = task9_eval.execute_case_transport(
            case, Path("/fixture"), mock.Mock(environment={}, disabled_skill_paths=(),
            writable_roots=[]), Path("/store"), lambda: calls.append("checkpoint"),
        )
    self.assertEqual(
        ["initialize", "thread", "turn", "gate", "checkpoint", "steer", "release", "wait", "close"],
        calls,
    )
    self.assertEqual("done", result.final_text)
```

Add these non-model suite-order and fail-fast tests:

```python
def test_run_suite_routes_frozen_cases_in_order_before_one_persist(self):
    repository = Path(__file__).resolve().parents[1]
    paths = {
        "forward": repository / "tests/skill_evals/observing_workflows_cases.json",
        "lifecycle": repository / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    }
    calls = []

    def fake_case(case, destination, lifecycle, runtime_factory=None):
        calls.append(("lifecycle" if lifecycle else "forward", case["id"],
                      task9_eval.select_case_transport(case)))
        return {"id": case["id"]}

    with tempfile.TemporaryDirectory() as temporary, \
         mock.patch.object(task9_eval, "_run_case", side_effect=fake_case), \
         mock.patch.object(task9_eval, "snapshot_production", return_value="baseline"), \
         mock.patch.object(task9_eval, "assert_production_unchanged"), \
         mock.patch.object(task9_eval, "persist_result_pair") as persist:
        task9_eval.run_suite(
            repository,
            manifest_paths=paths,
            result_destinations={
                "forward": Path(temporary) / "forward.json",
                "lifecycle": Path(temporary) / "lifecycle.json",
            },
        )
    self.assertEqual(28, len(calls))
    self.assertEqual(24, sum(route == "exec" for _, _, route in calls))
    self.assertEqual(4, sum(route == "app-server" for _, _, route in calls))
    persist.assert_called_once()

def test_run_suite_transport_failure_never_persists(self):
    repository = Path(__file__).resolve().parents[1]
    paths = {
        "forward": repository / "tests/skill_evals/observing_workflows_cases.json",
        "lifecycle": repository / "tests/skill_evals/observing_workflows_lifecycle_cases.json",
    }
    calls = []

    def fail_third(case, destination, lifecycle, runtime_factory=None):
        calls.append(case["id"])
        if len(calls) == 3:
            raise RuntimeError("transport failed")
        return {"id": case["id"]}

    with tempfile.TemporaryDirectory() as temporary, \
         mock.patch.object(task9_eval, "_run_case", side_effect=fail_third), \
         mock.patch.object(task9_eval, "snapshot_production", return_value="baseline"), \
         mock.patch.object(task9_eval, "assert_production_unchanged"), \
         mock.patch.object(task9_eval, "persist_result_pair") as persist:
        with self.assertRaisesRegex(RuntimeError, "transport failed"):
            task9_eval.run_suite(
                repository,
                manifest_paths=paths,
                result_destinations={
                    "forward": Path(temporary) / "forward.json",
                    "lifecycle": Path(temporary) / "lifecycle.json",
                },
            )
    self.assertEqual(3, len(calls))
    persist.assert_not_called()
```

The route assertion must inspect all frozen cases without launching a model:

```python
def test_frozen_route_ids_are_stable(self):
    repository = Path(__file__).resolve().parents[1]
    forward, lifecycle = (
        json.loads((repository / path).read_text(encoding="utf-8"))
        for path in (
            "tests/skill_evals/observing_workflows_cases.json",
            "tests/skill_evals/observing_workflows_lifecycle_cases.json",
        )
    )
    self.assertEqual(
        [("forward", "late-trigger"), ("forward", "scope-supersession"),
         ("lifecycle", "late-success"), ("lifecycle", "scope-supersession")],
        [(mode, case["id"]) for mode, cases in (("forward", forward), ("lifecycle", lifecycle))
         for case in cases if task9_eval.select_case_transport(case) == "app-server"],
    )
```

- [ ] **Step 2: Run integration tests and observe RED**

Expected: failures identify the current `_run_case` hard dependency on `AppServer`.

- [ ] **Step 3: Extract app-server execution and return the common result**

Move the current initialize/start/gate/checkpoint/steer/release/wait/close sequence into `execute_case_transport`. It rejects a callback for exec, requires one for app-server, and always cleans up:

```python
def execute_case_transport(
    case: dict,
    workspace: Path,
    runtime: CaseRuntime,
    wiki_root: Path,
    after_first_turn: Callable[[], None] | None = None,
) -> CaseExecution:
    route = select_case_transport(case)
    if route == "exec":
        if after_first_turn is not None:
            raise ValueError("exec transport cannot accept a first-turn checkpoint")
        return ExecTransport(workspace, runtime).run(case["turns"][0]["prompt"])
    if after_first_turn is None:
        raise ValueError("app-server transport requires a first-turn checkpoint")
    server = AppServer(workspace, runtime.environment, runtime.disabled_skill_paths)
    gate_released = False
    try:
        server.initialize()
        thread_id = server.start_thread(workspace)
        turn_id = server.start_turn(
            thread_id, case["turns"][0]["prompt"], workspace,
            runtime.writable_roots,
        )
        _wait_for_gate(server, turn_id, case, workspace, wiki_root)
        after_first_turn()
        server.steer(thread_id, turn_id, case["turns"][1]["prompt"])
        release_gate(case["id"])
        gate_released = True
        server.wait_turn(turn_id)
        if not server.agent_messages:
            raise RuntimeError("app-server final agent message is missing")
        return CaseExecution(
            terminal_status="completed",
            final_text=server.agent_messages[-1],
            command_executions=tuple(server.command_executions),
            observation_command_diagnostics=tuple(
                server.observation_command_diagnostics
            ),
        )
    finally:
        if not gate_released:
            release_gate(case["id"])
        server.close()
```

Retain fail-fast rejection of unsupported server requests. Add this cleanup regression:

```python
def test_app_server_transport_releases_gate_and_closes_when_steer_fails(self):
    server = mock.Mock()
    server.start_thread.return_value = "thread-1"
    server.start_turn.return_value = "turn-1"
    server.steer.side_effect = RuntimeError("steer failed")
    runtime = mock.Mock(environment={}, disabled_skill_paths=(), writable_roots=[])
    case = {"id": "late-trigger", "turns": [
        {"prompt": "first"}, {"prompt": "second", "dispatch_when": "after_draft_run"},
    ]}
    with mock.patch.object(task9_eval, "AppServer", return_value=server), \
         mock.patch.object(task9_eval, "_wait_for_gate"), \
         mock.patch.object(task9_eval, "release_gate") as release:
        with self.assertRaisesRegex(RuntimeError, "steer failed"):
            task9_eval.execute_case_transport(
                case, Path("/fixture"), runtime, Path("/store"), lambda: None
            )
    release.assert_called_once_with("late-trigger")
    server.close.assert_called_once_with()
```

- [ ] **Step 4: Route one-turn execution through `ExecTransport`**

Refactor `_run_case` around this callback and final capture; keep its existing decision calculations verbatim:

```python
def capture_first_turn() -> None:
    nonlocal previous_run_count
    checkpoint, records = _capture_checkpoint(wiki_root, role_map, 1)
    checkpoints.append(checkpoint)
    if not lifecycle:
        decisions.append(decision_from_checkpoint(1, records, previous_run_count))
        previous_run_count = len(records)

route = select_case_transport(case)
execution = execute_case_transport(
    case,
    workspace,
    runtime,
    wiki_root,
    capture_first_turn if route == "app-server" else None,
)
final_turn_number = len(case["turns"])
checkpoint, records = _capture_checkpoint(
    wiki_root, role_map, final_turn_number
)
checkpoints.append(checkpoint)
if not lifecycle:
    decisions.append(
        decision_from_checkpoint(final_turn_number, records, previous_run_count)
    )
final_text = execution.final_text
```

Pass `execution.command_executions` and `execution.observation_command_diagnostics` into the existing ledger and diagnostic paths. Assert `execution.terminal_status == "completed"` before reading the store.

- [ ] **Step 5: Keep evaluator instructions transport-neutral**

Write `EVALUATOR_DEVELOPER_INSTRUCTIONS` to a case-local `AGENTS.md` and commit it only inside the temporary fixture, so both Codex surfaces receive the same durable repository guidance. Remove the duplicate app-server-only `developerInstructions` field. Use this helper from `build_case_fixture` after `build_fixture` returns:

```python
def install_evaluator_guidance(workspace: Path) -> None:
    path = workspace / "AGENTS.md"
    path.write_text(EVALUATOR_DEVELOPER_INSTRUCTIONS + "\n", encoding="utf-8")
    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "Evaluation Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "Evaluation Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        "GIT_AUTHOR_DATE": "2026-01-01T00:00:01+00:00",
        "GIT_COMMITTER_DATE": "2026-01-01T00:00:01+00:00",
    }
    subprocess.run(["git", "add", "AGENTS.md"], cwd=workspace, check=True)
    subprocess.run(
        ["git", "commit", "-m", "Install evaluator guidance"],
        cwd=workspace, check=True, capture_output=True, env=environment,
    )
```

The regression asserts `AGENTS.md` bytes equal `(EVALUATOR_DEVELOPER_INSTRUCTIONS + "\n").encode()` and `git status --porcelain` is empty before marketplace skills are installed. Fixture-only commits are test setup and never touch the production repository.

- [ ] **Step 6: Run deterministic suites and inspect frozen bytes**

```bash
python3 -m unittest tests.test_observing_workflows_task9_eval -v
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -v
shasum -a 256 \
  marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals/observing_workflows_cases.json \
  marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals/observing_workflows_lifecycle_cases.json
```

Expected: deterministic suites pass; hashes exactly match the Global Constraints; no model process runs; no result generation or commit pointer appears. Do not commit.

#### Task 6D: Gate the marketplace diagnostic and formal run

**Files:**
- Modify: `marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py`
- Modify: `tests/test_observing_workflows_task9_eval.py`
- Modify after successful formal run: `.superpowers/sdd/workflow-observatory-task-6-report.md`
- Modify after successful formal run: `.superpowers/sdd/progress.md`
- Create only after all 28 cases pass: content-addressed result generations and `observing_workflows_results_commit.json` below the marketplace `tests/skill_evals/` result store.

**Interfaces:**
- Preserves: `--preflight` for historical compatibility but does not rerun the already accepted preflight.
- Restricts: `--diagnostic-case reviewed-refactor` to one non-persisting exec diagnostic.
- Adds: `--sweep` for one retained, non-authoritative 20+8 discovery run that never invokes paired-result persistence.
- Produces: revision-6 paired results only through the existing atomic commit-manifest protocol.

- [ ] **Step 1: Write RED marketplace gates**

Retain the existing success/failure tests that prove diagnostic runs leave fixed result paths, generation contents, and the authoritative pointer byte-identical. Add this restriction test around the marketplace namespace:

```python
def test_marketplace_diagnostic_allows_only_reviewed_refactor_exec_case(self):
    repository = Path(__file__).resolve().parents[1]
    runner = repository / (
        "marketplace/workflow-observatory/plugins/workflow-observer/"
        "tests/run_marketplace_eval.py"
    )
    namespace = runpy.run_path(str(runner))
    case, lifecycle = namespace["_find_diagnostic_case"]("reviewed-refactor")
    self.assertFalse(lifecycle)
    self.assertEqual("exec", task9_eval.select_case_transport(case))
    for rejected in ("multi-file-feature", "late-trigger", "scope-supersession"):
        with self.subTest(rejected=rejected):
            with self.assertRaisesRegex(LookupError, "diagnostic case is fixed"):
                namespace["_find_diagnostic_case"](rejected)
```

In the existing guarded diagnostic test, replace its `_run_case` lambda with:

```python
def fake_diagnostic(case, *args, **kwargs):
    self.assertEqual("reviewed-refactor", case["id"])
    self.assertEqual("exec", task9_eval.select_case_transport(case))
    return {"id": case["id"]}

runtime_globals["_run_case"] = fake_diagnostic
```

Keep the formal 24/4 order assertion in `test_run_suite_routes_frozen_cases_in_order_before_one_persist`; the marketplace main delegates to that exact `run_suite` implementation.

- [ ] **Step 2: Implement the narrow diagnostic gate**

Add `DIAGNOSTIC_CASE_ID = "reviewed-refactor"`. `_find_diagnostic_case` rejects every other ID before fixture creation or transport startup. Keep production snapshot/verification, `MarketplaceRuntimeFactory.verify_all_integrity()`, retained diagnostic workspace, and the non-persistence tests unchanged.

- [ ] **Step 3: Run the complete deterministic verification set**

```bash
python3 -m unittest tests.test_observing_workflows_task9_eval -v
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -v
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/plugin-creator/scripts/validate_plugin.py marketplace/workflow-observatory/plugins/workflow-observer
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-observer
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-telemetry
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-learning
/tmp/skill-validator-venv/bin/python ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py marketplace/workflow-observatory/plugins/workflow-observer/skills/workflow-improving
```

Expected: all tests and five official validations pass. Confirm no authoritative result pointer was created. Stop for independent code review; fix findings and repeat deterministic verification before any model-bearing run.

- [ ] **Step 4: Run one protected non-persisting diagnostic**

First establish an explicit no-write window with every other LLM Wiki session. Snapshot the authoritative result store, then run exactly:

```bash
python3 marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py \
  --diagnostic-case reviewed-refactor
```

Expected: the case uses exec, creates one successful `implementation-with-review` observation, passes its reviewer gate and fixture tests, passes configured-store integrity and the production fingerprint, and does not add or change a result generation or commit pointer. Preserve the diagnostic evidence in the Task 6 report, then obtain independent acceptance of that evidence.

The first protected attempt on 2026-07-17 failed safely and did not persist results: Codex emitted a visible review item but no `turn.completed`, so `ExecTransport` terminated it at the 600-second bound; the production guard also detected a transient repository fingerprint mismatch, but the original generic assertion did not retain path-level evidence. Before retrying, add deterministic tests and bounded, content-free diagnostics for the last protocol event/item/status sequence and for production changed-path categories. These diagnostics must retain the existing 1,024-character exception bound, never include prompt, message, tool output, or file content, and must not accept `item.completed` as a terminal turn. Obtain independent review of the instrumentation before one new protected diagnostic attempt.

Operational acceptance is staged before any further synthetic model run. Phase A runs at least two sequential, ordinary LLM Wiki ingests and requires both to preserve raw bytes, create exactly one observation start and one successful finish, leave no new draft or actively held `flock`, cite sources correctly, and pass source-catalog plus lint checks. A mode-0600 per-run lock inode may remain by design after its advisory lock is released. Only after Phase A passes may Phase B exercise corner cases such as review/rework, failed verification and cleanup, duplicate finish, or concurrent-writer handling. The frozen 20+8 formal suite remains deferred and must not block ordinary ingest work; pilot evidence validates production usefulness but does not claim the frozen benchmark score.

- [ ] **Step 5: Run one protected non-authoritative discovery sweep**

After deterministic tests and independent review pass, establish a new explicit no-write window and run:

```bash
python3 marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py \
  --sweep
```

The sweep snapshots production and both frozen manifests before case 1, runs all 20 forward cases and all 8 lifecycle cases in frozen order, retains its isolated workspace and sanitized report, and continues after ordinary isolated case assertion or model failures. It hard-aborts on any manifest mutation, production-fingerprint change, configured-store integrity failure, incomplete runtime setup, payload/output cleanup failure, or transport cleanup failure. It never calls `persist_result_pair`, never creates or advances the authoritative commit pointer, and labels its report `authoritative: false`. Freeze implementation for the entire sweep; after all 28 attempts, diagnose from retained artifacts and batch fixes before rerunning deterministic gates. Repeat the discovery sweep only after that batch is independently reviewed. A complete discovery sweep is diagnostic evidence, not acceptance and not a frozen score.

- [ ] **Step 6: Run the protected frozen formal suite once**

Reconfirm the no-write window and run:

```bash
python3 marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py
```

Expected: forward cases complete `20/20`, lifecycle cases complete `8/8`, literal `python3 <resolved-cli-path>` is used, configured integrity passes for every accumulated case-local store, production remains unchanged, and the two result generations become visible through one fsync'd atomic commit manifest. Any case, transport, integrity, manifest, or production-fingerprint failure stops the suite and leaves the previous authoritative pair visible.

- [ ] **Step 7: Resolve and review the committed pair**

Use `resolve_committed_result_pair()` in a focused verification test to reopen the pointer, constrain both generation paths, re-hash both files, validate both schemas and exact ID sets, and recompute 20/20 plus 8/8. Record paths, hashes, transport counts, diagnostic result, formal scores, and previous failed-attempt history in the Task 6 report and progress file. Run the complete deterministic suites once more and stop for final Task 6 review. Do not commit.

### Required post-Task 6 LLM Wiki checkpoint

After Task 6 review passes, pause marketplace implementation before Task 7. Ingest this session through the repository's current approved immutable-raw workflow, then create canonical task records for:

- collision-proof session capture under `raw/sessions/YYYY/MM/DD/<semantic-topic-slug>.md` with multiple sessions per day;
- periodic `synthesize sessions` and `monthly synthesis YYYY-MM` workflows with bounded domain classification;
- an immutable-raw-compatible metadata design, preferring a mutable sidecar/index/manifest over editing raw session frontmatter;
- retention/export/delete policy, `schema_version`, telemetry health events, value-based sampling, and multi-writer conflict recovery.

Regenerate `_todo_list.md`, run `sources --check`, `tasks --check`, and `lint`, review the ingest and tasks, and only then begin Task 7. Do not implement those future improvements during the handoff.

---

### Task 7: Build deterministic packaging

**Files:**
- Create: `scripts/package_workflow_observatory.py`
- Create: `marketplace/workflow-observatory/plugins/workflow-observer/tests/test_package_archive.py`
- Create: `marketplace/workflow-observatory/NOTICE.md`
- Create after all gates pass: `dist/workflow-observatory-0.1.0.zip`

**Interfaces:**
- Produces: `build_archive(source_root: Path, destination: Path, evidence: Sequence[Path]) -> str`, `verify_archive(path: Path) -> str`, deterministic ZIP, and `SHA256SUMS.json`.

- [ ] **Step 1: Write RED content/security tests**

```python
import json
import shutil
import sys
import tempfile
from pathlib import Path
import unittest
import zipfile

REPOSITORY_ROOT = Path(__file__).resolve().parents[5]
MARKETPLACE_ROOT = REPOSITORY_ROOT / "marketplace/workflow-observatory"
sys.path.insert(0, str(REPOSITORY_ROOT / "scripts"))
from package_workflow_observatory import PackageError, build_archive

class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = self.root / "workflow-observatory"
        shutil.copytree(MARKETPLACE_ROOT, self.source)
        self.archive = self.root / "workflow-observatory-0.1.0.zip"
        self.outside = self.root / "outside.txt"
        self.outside.write_text("outside", encoding="utf-8")
        self.evidence = (
            REPOSITORY_ROOT / "wiki/concept/Workflow_Observation_and_Process_Knowledge.md",
            REPOSITORY_ROOT / "docs/superpowers/specs/2026-07-12-observation-records-design.md",
            REPOSITORY_ROOT / "docs/superpowers/plans/2026-07-12-observation-records.md",
            REPOSITORY_ROOT / "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
            REPOSITORY_ROOT / "docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md",
            REPOSITORY_ROOT / "docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md",
            *sorted((REPOSITORY_ROOT / "tests").glob("test_observation*.py")),
            *sorted((REPOSITORY_ROOT / "tests").glob("test_observing_workflows*.py")),
            REPOSITORY_ROOT / "tests/observing_workflows_eval_harness.py",
            REPOSITORY_ROOT / "scripts/run_observing_workflows_task9_eval.py",
        )

    def test_archive_contains_evidence(self):
        build_archive(self.source, self.archive, self.evidence)
        with zipfile.ZipFile(self.archive) as bundle:
            names = set(bundle.namelist())
        for suffix in ("marketplace.json", "plugin.json",
            "Workflow_Observation_and_Process_Knowledge.md",
            "observation-records-design.md", "observation-records-v2.md",
            "workflow-observatory-marketplace-design.md",
            "workflow-observatory-marketplace.md",
            "run_observing_workflows_task9_eval.py",
            "observing_workflows_eval_harness.py", "SHA256SUMS.json"):
            self.assertTrue(any(name.endswith(suffix) for name in names), suffix)

        inventory = next(name for name in names if name.endswith("SHA256SUMS.json"))
        with zipfile.ZipFile(self.archive) as bundle:
            mapping = json.loads(bundle.read(inventory))
        self.assertEqual(
            {path.as_posix() for path in self.evidence},
            set(mapping["repository_evidence"].keys()),
        )

    def test_archive_rejects_symlinks(self):
        linked = self.source / "linked"
        linked.symlink_to(self.outside)
        with self.assertRaisesRegex(PackageError, "symlink"):
            build_archive(self.source, self.archive, self.evidence)
```

- [ ] **Step 2: Implement deterministic packaging**

Use an explicit allowlist; include every repository evidence path named above plus every marketplace-local test and frozen manifest; reject symlinks and non-regular files; sort POSIX paths; assign every ZIP member timestamp `(2026, 7, 15, 0, 0, 0)` and mode `0644`; hash every member except the inventory; serialize sorted JSON; atomically replace only after readback validation. `SHA256SUMS.json` must contain a completeness mapping from repository path to portable archive member and digest. Deterministically normalize author-specific absolute paths in archive copies, record both source and packaged digests, and fail if normalization changes anything outside the declared path fields.

- [ ] **Step 3: Build and clean-room verify**

```bash
python3 scripts/package_workflow_observatory.py --version 0.1.0
python3 scripts/package_workflow_observatory.py --verify dist/workflow-observatory-0.1.0.zip
```

Extract to a fresh temporary directory and rerun plugin validation, four skill validations, packaged tests, result scoring, and inventory verification. A second build must have the same SHA-256.

- [ ] **Step 4: Review Task 7**

Confirm the archive excludes personal config, absolute author paths, raw inputs, secrets, production observations, caches, and temporary payloads. Do not commit.

---

### Task 8: Migration gate and final verification

**Files:**
- Modify: `marketplace/workflow-observatory/README.md`
- Modify: `marketplace/workflow-observatory/plugins/workflow-observer/README.md`
- Modify: `wiki/tasks/package-observation-workflows-marketplace.md`
- Regenerate: `wiki/_todo_list.md`
- Install only after approval: local marketplace and `workflow-observer` plugin.

**Interfaces:**
- Produces: documented installation, double-trigger-safe migration, and final evidence.

- [ ] **Step 1: Document portable installation**

Document `codex plugin marketplace add <extracted-root>`, `codex plugin add workflow-observer@workflow-observatory`, portable storage, explicit LLM Wiki config, uninstall/data retention, and clean-room verification.

- [ ] **Step 2: Enforce the migration gate**

After explicit install approval, compare old/new contracts, install the marketplace, use a new thread for discovery, prove one trigger and one exclusion, then disable the old global skill. Never leave both automatic descriptions active for normal work.

- [ ] **Step 3: Run final checks**

```bash
python3 -m unittest discover -s tests -v
python3 -m unittest discover -s marketplace/workflow-observatory/plugins/workflow-observer/tests -v
python3 wiki_cli.py sources --check
python3 wiki_cli.py tasks
python3 wiki_cli.py tasks --check
python3 wiki_cli.py lint
python3 scripts/package_workflow_observatory.py --verify dist/workflow-observatory-0.1.0.zip
git diff --check
git status --short
```

Expected: tests pass, sources/tasks current, lint Green, archive valid, and no raw or unplanned change.

- [ ] **Step 4: Close only proven work**

Record archive path/SHA-256, revision 6 scores, official validators, adapter conformance, and installation state. Regenerate `_todo_list.md`. If installation is not approved, leave deployment open. Final reviewer verifies design acceptance criteria and historical failed attempts; do not commit or publish.
