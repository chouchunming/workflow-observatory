# Observation Records v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Supersedes:** `docs/superpowers/plans/2026-07-12-observation-records.md`. Old task numbers and `.superpowers/sdd/progress.md` completion entries do not apply to this plan.

**Goal:** Build a secure, Markdown-first observation lifecycle and reporting CLI, then install a globally discoverable skill that records eligible top-level Codex implementation work in this central LLM Wiki.

**Architecture:** Move observation schema, storage, locking, validation, and reporting into a focused `wiki_observations.py` module. Keep `wiki_cli.py` as the argparse and exit-code adapter. Store immutable run records under `wiki/observations/`, invalidation tombstones under `wiki/observations/invalidations/`, and use a canonical repository skill copied to the global personal-skills directory only after baseline evaluation.

**Tech Stack:** Python 3 standard library (`argparse`, `dataclasses`, `datetime`, `fcntl`, `hashlib`, `json`, `os`, `pathlib`, `secrets`, `subprocess`, `tempfile`, `time`, `unittest`), Markdown records, JSON skill-eval fixtures.

## Global Constraints

- Treat `docs/superpowers/specs/2026-07-12-observation-records-design.md` as fixed input. Stop and return to design review if implementation exposes a contradiction.
- Treat existing observation code and `tests/test_observation_records.py` as migration input; preserve unrelated dirty-worktree changes.
- Never modify, rename, or delete files under `raw/`.
- Do not run `publish`, commit, push, or edit generated dashboards by hand.
- Observation writes use an explicit wiki root; non-observe CLI commands retain current-working-directory behavior.
- The CLI derives provenance from an explicit, non-persisted subject root; the skill never reimplements remote normalization or workspace hashing.
- Observation records are operational records, not source-compilation evidence or concept graph nodes.
- Unknown metrics remain `unknown`; never coerce them to zero.
- Do not store full prompts, transcripts, credentials, personal data, secrets, or unsanitized absolute paths.
- One observation represents one top-level user-authorized task. Parent agents aggregate subagent evidence; workers do not create child observations.
- Skill installation outside this repository requires explicit user approval.

---

## File Structure

- Create `wiki_observations.py`: observation enums, dataclasses, subject-workspace provenance derivation, path resolution, payload parsing, lifecycle, tombstones, locking, validation, and report rendering.
- Modify `wiki_cli.py`: remove migrated observation logic, register `observe start|finish|invalidate|report`, and map typed errors to the specified stdout/stderr/exit-code contract.
- Modify `AGENTS.md`: define `wiki/observations/` as operational records and document exclusions.
- Replace the old-schema fixtures in `tests/test_observation_records.py`: schema, payload, provenance, and taxonomy tests.
- Create `tests/test_observation_lifecycle.py`: start, finish, supersede, invalidate, concurrency, atomicity, and stale-draft tests.
- Create `tests/test_observation_report.py`: filtering, grouping, missing values, small samples, stale drafts, and invalidated-record tests.
- Create `tests/test_observation_cli.py`: cross-CWD root behavior and stdout/stderr/exit-code contract.
- Create `tests/test_observation_integration.py`: lint, source coverage, graph-warning, and overview-drift exclusions.
- Create `tests/skill_evals/observing_workflows_cases.json`: frozen 20-case trigger/taxonomy manifest.
- Create `tests/skill_evals/observing_workflows_lifecycle_cases.json`: frozen lifecycle integration manifest.
- Create `tests/skill_evals/observing_workflows_baseline.json` and `tests/skill_evals/observing_workflows_forward.json`: captured eval decisions.
- Create `tests/skill_evals/observing_workflows_lifecycle_forward.json`: inspected lifecycle outcomes from temporary stores.
- Create `tests/run_observing_workflows_eval.py`: validate exact result schemas and ID sets, then print trigger, taxonomy, recording, or lifecycle accuracy for the explicit mode.
- Create `tests/observing_workflows_eval_harness.py`: build and clean deterministic temporary task workspaces, expose checkpoint predicates, and inspect temporary observation stores; agent dispatch remains orchestration work outside this Python module.
- Create `tests/test_observing_workflows_eval_harness.py`: fixture isolation, normalized-role, gate timeout/release, snapshot, and deferred-result-write tests.
- Create `skills/observing-workflows/SKILL.md` and `skills/observing-workflows/agents/openai.yaml`: canonical personal skill.
- Create global installed copy `~/.codex/skills/observing-workflows/` only after approval.

### Task 0: Review Gate and Migration Baseline

**Files:**
- Review: `docs/superpowers/specs/2026-07-12-observation-records-design.md`
- Review: `docs/superpowers/plans/2026-07-13-observation-records-v2.md`
- Review: `wiki_cli.py`
- Review: `tests/test_observation_records.py`

**Interfaces:**
- Produces no code interface.
- Produces an explicit review verdict that the spec and v2 plan are internally consistent and that old SDD progress is ignored.

- [ ] **Step 1: Confirm the spec and plan have no placeholders or whitespace errors**

Run:

```bash
grep -nEi 'TB[D]|TO[D]O|implement la[t]er|fill in de[t]ails|similar to Ta[s]k' docs/superpowers/specs/2026-07-12-observation-records-design.md docs/superpowers/plans/2026-07-13-observation-records-v2.md
git diff --check -- docs/superpowers/specs/2026-07-12-observation-records-design.md docs/superpowers/plans/2026-07-13-observation-records-v2.md
```

Expected: grep has no matches; `git diff --check` exits `0`.

- [ ] **Step 2: Record the migration baseline**

Run:

```bash
python3 -m unittest tests.test_observation_records -v
python3 -m py_compile wiki_cli.py
```

Expected: current old-schema observation tests pass; Python compilation passes. Save the exact test count in the task report so later tasks prove migration rather than accidental deletion.

- [ ] **Step 3: Review the fixed interfaces before code changes**

Confirm the implementation tasks below define `ObservationPaths`, `Provenance`, `StartRequest`, `CompletionPayload`, `ReportFilters`, `ObservationError`, `derive_provenance`, `start_observation`, `finish_observation`, `invalidate_observation`, and `render_observation_report` exactly once. Expected: review verdict `approved`; otherwise amend this plan before Task 1.

### Task 1: Freeze Skill Evaluation Cases and Capture Baseline

**Files:**
- Create: `tests/skill_evals/observing_workflows_cases.json`
- Create: `tests/skill_evals/observing_workflows_lifecycle_cases.json`
- Create: `tests/skill_evals/observing_workflows_baseline.json`
- Create: `tests/run_observing_workflows_eval.py`
- Create: `tests/observing_workflows_eval_harness.py`
- Create: `tests/test_observing_workflows_eval_harness.py`

**Interfaces:**
- Turn rows: `{prompt, dispatch_when}` where `dispatch_when` is `immediate`, `after_single_file_mutation_without_run`, or `after_draft_run`.
- Expected decision rows: `{after_turn, triggered}`; observed decision rows additionally contain `{task_type, workflow_variant}`.
- Normalized record rows: `{role, status, start_mode, superseded_by_role}` where role is `run-1`, `run-2`, and so on; `superseded_by_role` is null unless status is `superseded`.
- Record checkpoint rows: `{after_turn, records}` using only normalized record rows and never raw run IDs.
- Decision manifest rows: `{id, turns, fixture, expected_decisions, task_type, workflow_variant, expected_record_checkpoints, expected_run_count, expected_final_statuses}`.
- Decision result rows: `{id, decisions, record_checkpoints, run_count, draft_count, final_statuses}`; baseline rows contain `decisions` only because no task mutation or skill lifecycle is executed.
- Lifecycle manifest rows: `{id, turns, fixture, mode, setup, expected_record_checkpoints, expected_run_count, expected_draft_count, expected_final_statuses, expect_failure_disclosure, expected_selected_command}`; store fields may be null for command-selection-only mode.
- Lifecycle result rows: `{id, record_checkpoints, run_count, draft_count, final_statuses, failure_disclosed, selected_command}`; command-selection-only rows use null store fields and executable rows use null `selected_command`.
- Produces `validate_id_set(manifest: list[dict], results: list[dict]) -> list[str]`.
- Produces `score_results(manifest: list[dict], results: list[dict]) -> tuple[int, int, list[str]]`.
- Produces `validate_decision_recording(manifest: list[dict], results: list[dict]) -> list[str]` for forward results; baseline scoring skips this check because baseline rows have no lifecycle fields.
- Produces `validate_lifecycle_results(manifest: list[dict], results: list[dict]) -> list[str]`.
- Produces `build_fixture(case_id: str, fixture: str, destination: Path) -> Path`, `inspect_store(wiki_root: Path) -> dict`, `normalize_records(records, role_map) -> list[dict]`, `wait_for_checkpoint(case_id, predicate, timeout_seconds=15)`, `release_gate(case_id)`, `snapshot_production(repo_root: Path)`, `assert_production_unchanged(snapshot)`, and `persist_results_atomically(destination: Path, rows: list[dict])`.

- [ ] **Step 1: Write failing scorer and lifecycle-validator tests before creating the skill**

Add the scorer tests below to `tests/run_observing_workflows_eval.py`; add the fixture, role-normalization, gate timeout/release, production snapshot, and deferred-result-write tests specified in Step 3 to `tests/test_observing_workflows_eval_harness.py`.

```python
class EvalScoreTests(unittest.TestCase):
    def test_score_detects_trigger_and_taxonomy_mismatches(self):
        manifest = [{
            "id": "feature",
            "expected_decisions": [{"after_turn": 1, "triggered": True}],
            "task_type": "feature",
            "workflow_variant": "implementation-basic",
        }]
        results = [{
            "id": "feature",
            "decisions": [{"after_turn": 1, "triggered": False, "task_type": None, "workflow_variant": None}],
        }]
        trigger_hits, taxonomy_hits, errors = score_results(manifest, results)
        self.assertEqual((0, 0), (trigger_hits, taxonomy_hits))
        self.assertEqual(["feature: trigger mismatch"], errors)

    def test_lifecycle_validator_detects_store_and_disclosure_mismatches(self):
        manifest = [{
            "id": "failed-task",
            "expected_record_checkpoints": [],
            "expected_run_count": 1,
            "expected_draft_count": 0,
            "expected_final_statuses": ["failed"],
            "expect_failure_disclosure": False,
            "expected_selected_command": None,
        }]
        results = [{
            "id": "failed-task",
            "record_checkpoints": [],
            "run_count": 1,
            "draft_count": 1,
            "final_statuses": [],
            "failure_disclosed": False,
            "selected_command": None,
        }]
        self.assertEqual(
            ["failed-task: draft count mismatch", "failed-task: final statuses mismatch"],
            validate_lifecycle_results(manifest, results),
        )

    def test_decision_recording_validator_requires_final_records(self):
        manifest = [{
            "id": "feature",
            "expected_record_checkpoints": [{
                "after_turn": 1,
                "records": [{"role": "run-1", "status": "draft", "start_mode": "planned", "superseded_by_role": None}],
            }],
            "expected_run_count": 1,
            "expected_final_statuses": ["success"],
        }]
        results = [{
            "id": "feature",
            "record_checkpoints": [{
                "after_turn": 1,
                "records": [{"role": "run-1", "status": "draft", "start_mode": "planned", "superseded_by_role": None}],
            }],
            "run_count": 1,
            "draft_count": 1,
            "final_statuses": [],
        }]
        self.assertEqual(
            ["feature: draft records remain", "feature: final statuses mismatch"],
            validate_decision_recording(manifest, results),
        )

    def test_result_ids_must_match_manifest_exactly(self):
        manifest = [{"id": "one"}, {"id": "two"}]
        results = [{"id": "one"}, {"id": "one"}, {"id": "extra"}]
        self.assertEqual(
            ["duplicate result id: one", "missing result id: two", "extra result id: extra"],
            validate_id_set(manifest, results),
        )
```

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.run_observing_workflows_eval.EvalScoreTests tests.test_observing_workflows_eval_harness -v`

Expected: FAIL because scorer/validators and the eval harness do not exist.

- [ ] **Step 3: Implement the scorer and freeze twenty synthetic cases**

Implement:

```python
def score_results(manifest, results):
    by_id = {row["id"]: row for row in results}
    trigger_hits = taxonomy_hits = 0
    errors = []
    for case in manifest:
        actual = by_id.get(case["id"])
        expected_trigger_sequence = [(row["after_turn"], row["triggered"]) for row in case["expected_decisions"]]
        actual_trigger_sequence = [(row["after_turn"], row["triggered"]) for row in actual["decisions"]] if actual else []
        if actual is None or actual_trigger_sequence != expected_trigger_sequence:
            errors.append(f"{case['id']}: trigger mismatch")
            continue
        trigger_hits += 1
        triggered_rows = [row for row in actual["decisions"] if row["triggered"]]
        if not triggered_rows or all(
            row["task_type"] == case["task_type"]
            and row["workflow_variant"] == case["workflow_variant"]
            for row in triggered_rows
        ):
            taxonomy_hits += 1
        else:
            errors.append(f"{case['id']}: taxonomy mismatch")
    return trigger_hits, taxonomy_hits, errors
```

Implement `validate_id_set`, `validate_decision_recording`, and `validate_lifecycle_results` as deterministic exact comparisons keyed by case ID. Every mode first requires the manifest/result ID sets to match exactly and rejects duplicate, missing, and extra IDs. Before comparison, `normalize_records` assigns `run-1`, `run-2`, and later roles when a record is first observed and preserves that per-case role map across checkpoints; multiple unseen records in one inspection are ordered by `(timestamp, run_id)`. Replace raw `superseded_by` IDs with `superseded_by_role`; raw IDs must never enter result JSON. Decision recording compares every ordered normalized checkpoint, then requires zero final drafts and compares final `run_count` plus sorted `final_statuses`; lifecycle validation does the same and additionally compares `failure_disclosed`, while command-selection-only cases compare `selected_command` and must not execute it.

The runner must require exactly one explicit mode: `--baseline <manifest> <results>`, `--forward <manifest> <results>`, or `--lifecycle <manifest> <results>`. Baseline validates every turn's decision/taxonomy fields only. Forward rejects any row missing `decisions`, `record_checkpoints`, `run_count`, `draft_count`, or `final_statuses` and always runs recording validation. Lifecycle requires the exact lifecycle result schema above. Do not infer mode from which fields happen to be present.

Implement `build_fixture` with deterministic in-code templates for `python-cli`, `documentation`, `wiki`, and `empty` fixtures. Each call writes only below the caller-provided temporary destination, initializes a local Git repository with a deterministic identity and initial commit, and returns its workspace root. Templates contain the minimum parser/CLI/tests, linked docs, or raw/wiki files needed by their assigned turns. Multi-turn fixtures also contain a gate command that writes a ready marker and waits at most 15 seconds for a release marker.

Add harness tests proving two builds do not share files, no path escapes the supplied destination, role mappings remain stable across checkpoints, supersession links normalize to roles, gate timeout is reported, and release unblocks the gate. The harness exposes read-only predicates for `after_single_file_mutation_without_run` and `after_draft_run`, but never launches or messages an agent. The current agent/subagent workflow dispatches turns and records harness-inspected checkpoints. If a gate times out, the evaluator exits early, or its agent identity changes, record an explicit case failure and do not retry.

Before an eval suite, snapshot production Git status and production observation record names. Accumulate case results only in memory or a system temporary file. After every case is cleaned, assert the production snapshot is unchanged; only then atomically write the declared baseline/forward/lifecycle result JSON into the repository. Result paths are therefore never created or modified during the production-integrity comparison, even when a prior result file already exists.

The manifest must contain exactly ten trigger cases (`multi-file-feature`, `tested-bugfix`, `reviewed-refactor`, `multi-file-docs`, `wiki-compile`, `durable-query`, `inbox-processing`, `late-trigger`, `scope-supersession`, `parent-managed-subagent`) and ten exclusions (`chat`, `read-only-search`, `answer-only`, `plan-only`, `single-file-typo`, `single-file-copy`, `status-question`, `review-only`, `worker-with-parent-marker`, `ambiguous-default-no-trigger`). Assign every case one of the frozen fixture kinds and use the taxonomy matrix from the spec verbatim.

For recording expectations, every exclusion has `expected_run_count: 0` and `expected_final_statuses: []`. Every trigger except `scope-supersession` has one final `success` record. `scope-supersession` has two final records with sorted statuses `["success", "superseded"]`. All forward decision cases require `draft_count: 0`.

All cases except `late-trigger` and `scope-supersession` use one `immediate` turn. Freeze the two multi-turn cases as follows:

- `late-trigger`: turn 1 requests an untested single-file fix and instructs the evaluator to enter the fixture gate immediately after that mutation; it expects no trigger. Turn 2 requests code plus tests with `dispatch_when: after_single_file_mutation_without_run` and expects a bugfix trigger. The orchestrator queues turn 2 to the same running agent before releasing the gate. Its final normalized checkpoint requires `run-1` to be `success` with `start_mode: late`.
- `scope-supersession`: turn 1 starts the original multi-file feature and instructs the evaluator to enter the fixture gate before the first code mutation; it expects a planned trigger. Turn 2 materially replaces its Scope with `dispatch_when: after_draft_run`. The orchestrator queues turn 2 to the same running agent before releasing the gate. Checkpoints require planned draft `run-1` before turn 2, then `run-1` status `superseded` with `superseded_by_role: run-2`, and `run-2` status `success`.

Use these frozen expectations when writing the JSON rows:

| id | synthetic prompt summary | trigger | task type | workflow variant |
|---|---|---:|---|---|
| multi-file-feature | Add a feature across parser, CLI, and tests | true | feature | implementation-with-review |
| tested-bugfix | Fix a reproduced bug and run its tests | true | bugfix | implementation-basic |
| reviewed-refactor | Refactor two modules with reviewer gate | true | refactor | implementation-with-review |
| multi-file-docs | Update three linked documentation files | true | documentation | implementation-basic |
| wiki-compile | Compile one raw source and lint the Wiki | true | compile | compile-basic |
| durable-query | Research and file a durable summary back | true | query | research-basic |
| inbox-processing | Promote several captures and regenerate dashboards | true | inbox-processing | compile-basic |
| late-trigger | A one-file fix expands to code plus tests | true | bugfix | implementation-basic |
| scope-supersession | Replace an active implementation scope with a new feature | true | feature | implementation-with-review |
| parent-managed-subagent | Lead delegates code and review while owning one run | true | feature | implementation-with-review |
| chat | Discuss an idea without changing files | false | null | null |
| read-only-search | Search files and report findings | false | null | null |
| answer-only | Explain an API without modifying anything | false | null | null |
| plan-only | Write no files; provide an implementation plan in chat | false | null | null |
| single-file-typo | Correct one typo without tests | false | null | null |
| single-file-copy | Change one sentence in one file without validation | false | null | null |
| status-question | Report current task status | false | null | null |
| review-only | Review a diff without edits | false | null | null |
| worker-with-parent-marker | Worker receives the exact parent observation marker | false | null | null |
| ambiguous-default-no-trigger | Ambiguous request with no clear mutation threshold | false | null | null |

Freeze a separate lifecycle manifest before the skill exists. It must contain `planned-success`, `late-success`, `scope-supersession`, `parent-managed-subagent`, `task-failure`, `central-cli-unavailable`, `complete-eval-override`, and `incomplete-eval-override`, with a fixture kind assigned to every row. The expected outcomes must assert actual temporary-store run counts, draft counts, sorted final-status multisets, and whether the final response discloses a recording failure. Mark `incomplete-eval-override` as command-selection-only: it verifies selection of the fixed central command but must never execute that command. This manifest is not part of the 10/10 decision score and must never be extended during Task 9.

- [ ] **Step 4: Run GREEN for scorer and harness units**

Run: `python3 -m unittest tests.run_observing_workflows_eval.EvalScoreTests tests.test_observing_workflows_eval_harness -v`

Expected: exact ID/schema validation, normalized roles/links, fixture isolation, gate timeout/release, production snapshots, and deferred result persistence pass.

- [ ] **Step 5: Capture baseline decisions without the skill**

For each frozen case, build its assigned temporary fixture and dispatch its turns to one isolated decision-only evaluator with CWD set to that fixture. In baseline mode, preserve turn order but treat `dispatch_when` as an immediate sequencing marker because no mutation or observation lifecycle is executed. Accumulate the decision after every turn in memory or a system temporary result file; do not execute requested mutations, expose expected values, create the skill, or write the repository result yet. Clean every fixture in `finally`.

- [ ] **Step 6: Verify production integrity and persist baseline**

Compare production Git status and observation record names with the pre-suite snapshot. Only after they match, atomically write `tests/skill_evals/observing_workflows_baseline.json` from the accumulated temporary result.

- [ ] **Step 7: Run GREEN for result-file validation**

Run: `python3 tests/run_observing_workflows_eval.py --baseline tests/skill_evals/observing_workflows_cases.json tests/skill_evals/observing_workflows_baseline.json`

Expected: command parses all 20 rows and prints baseline trigger/taxonomy scores; non-perfect baseline is allowed and preserved as evidence.

### Task 2: Observation Domain Model and Validation

**Files:**
- Create: `wiki_observations.py`
- Modify: `tests/test_observation_records.py`

**Interfaces:**
- Produces `ObservationPaths.from_root(root: Path) -> ObservationPaths`.
- Produces dataclasses `Provenance`, `StartRequest`, `ScopePayload`, `CompletionPayload`, `ReportFilters`.
- Produces `ObservationError(kind: str, message: str)` where kind is `validation`, `state`, or `io`.
- Produces `derive_provenance(subject_root: Path, project_override: str | None = None) -> Provenance`, `validate_start_request(request: StartRequest) -> None`, `parse_scope_payload(text: str) -> ScopePayload`, `parse_completion_payload(text: str) -> CompletionPayload`, `read_record(paths: ObservationPaths, run_id: str) -> tuple[dict, str]`, and `validate_record(metadata: dict, body: str, paths: ObservationPaths) -> list[str]`.

- [ ] **Step 1: Write failing schema and parser tests**

```python
def test_taxonomy_and_provenance_are_validated(self):
    taxonomy_error = make_start_request(task_type="feature", workflow_variant="compile-basic")
    with self.assertRaisesRegex(ObservationError, "invalid taxonomy combination"):
        validate_start_request(taxonomy_error)
    workspace_error = make_start_request(workspace_id="BAD")
    with self.assertRaisesRegex(ObservationError, "workspace_id must be 12 lowercase hex"):
        validate_start_request(workspace_error)

def test_completion_requires_fixed_sections(self):
    with self.assertRaisesRegex(ObservationError, "missing Outcome and observation"):
        parse_completion_payload("## Execution evidence\n- Verification: None.\n- Artifacts: None.")

def test_provenance_is_derived_without_persisting_subject_root(self):
    provenance = derive_provenance(self.subject_root)
    self.assertEqual("example-project", provenance.project)
    self.assertEqual("example-project", provenance.workspace)
    self.assertRegex(provenance.workspace_id, r"^[0-9a-f]{12}$")
    self.assertNotIn(str(self.subject_root), repr(provenance))
```

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_records -v`

Expected: FAIL because `wiki_observations` and the new request/payload interfaces do not exist.

- [ ] **Step 3: Implement enums, dataclasses, remote normalization, and strict parsers**

Use `pathlib.Path`, frozen dataclasses, the exact taxonomy matrix, 64-KiB payload cap, 200-code-point scalar cap, aware ISO timestamps, `workspace_id` hashing, `start_mode`, status invariants, required/empty sources, and safe YAML-like serialization. Keep existing `wiki_cli.parse_frontmatter` behavior unchanged; the new module may call it through an injected parser to avoid a circular import.

Start with these exact public types:

```python
TAXONOMY = {
    "feature": {"implementation-basic", "implementation-with-review"},
    "bugfix": {"implementation-basic", "implementation-with-review"},
    "refactor": {"implementation-basic", "implementation-with-review"},
    "documentation": {"implementation-basic", "implementation-with-review"},
    "maintenance": {"maintenance-basic", "implementation-with-review"},
    "compile": {"compile-basic", "compile-with-review"},
    "inbox-processing": {"compile-basic", "compile-with-review"},
    "query": {"research-basic"},
}
FINAL_STATUSES = {"success", "partial", "failed", "rolled-back", "superseded"}

class ObservationError(Exception):
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind

@dataclass(frozen=True)
class ObservationPaths:
    root: Path
    observations: Path
    locks: Path
    invalidations: Path

    @classmethod
    def from_root(cls, root):
        resolved = Path(root).resolve(strict=True)
        observations = resolved / "wiki" / "observations"
        return cls(resolved, observations, observations / ".locks", observations / "invalidations")

@dataclass(frozen=True)
class StartRequest:
    title: str
    project: str
    workspace: str
    workspace_id: str
    revision: str
    working_tree: str
    agent_surface: str
    start_mode: str
    task_type: str
    workflow_variant: str
    task_ref: str | None
    sources: tuple[str, ...]
```

Implement `Provenance`, `ScopePayload`, `CompletionPayload`, and `ReportFilters` as frozen dataclasses whose fields exactly match the spec. `Provenance` contains only project, workspace, workspace ID, revision, and working-tree state—never subject root. `derive_provenance` resolves an existing subject directory, canonicalizes a Git-contained path to its worktree top-level, performs the spec's remote normalization/hash and Git fallbacks, and applies only a validated explicit project override. Parser functions must raise `ObservationError("validation", message)` rather than return partial results.

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_records -v`

Expected: all migrated schema/parser tests pass; old unsupported signatures are removed from tests.

### Task 3: Planned and Late Start Lifecycle

**Files:**
- Modify: `wiki_observations.py`
- Create: `tests/test_observation_lifecycle.py`

**Interfaces:**
- Consumes `ObservationPaths`, `StartRequest`, and `ScopePayload`.
- Produces `start_observation(paths: ObservationPaths, request: StartRequest, scope: ScopePayload, now: datetime | None = None) -> str`.

- [ ] **Step 1: Write failing start tests**

```python
def test_same_second_starts_keep_true_timestamp_and_unique_ids(self):
    now = datetime(2026, 7, 13, 10, 0, tzinfo=timezone(timedelta(hours=8)))
    first = start_observation(self.paths, self.request, self.scope, now=now)
    second = start_observation(self.paths, self.request, self.scope, now=now)
    self.assertNotEqual(first, second)
    self.assertRegex(first, r"^obs-20260713-100000-[0-9a-f]{6}$")
    self.assertEqual(now.isoformat(), read_record(self.paths, first)[0]["timestamp"])
    self.assertEqual(now.isoformat(), read_record(self.paths, second)[0]["timestamp"])

def test_start_fsync_failure_leaves_no_record_or_temporary(self):
    with mock.patch("wiki_observations.os.fsync", side_effect=OSError("disk failure")):
        with self.assertRaisesRegex(ObservationError, "disk failure") as raised:
            start_observation(self.paths, self.request, self.scope, now=self.started)
    self.assertEqual("io", raised.exception.kind)
    self.assertEqual([], list(self.paths.observations.iterdir()))

def test_start_link_collision_retries(self):
    self.paths.observations.mkdir(parents=True)
    occupied = self.paths.observations / "obs-20260713-100000-a1b2c3.md"
    occupied.write_text("occupied", encoding="utf-8")
    with mock.patch("wiki_observations.secrets.token_hex", side_effect=["a1b2c3", "d4e5f6"]):
        run_id = start_observation(self.paths, self.request, self.scope, now=self.started)
    self.assertEqual("obs-20260713-100000-d4e5f6", run_id)

def test_start_noncollision_link_error_is_io_and_cleans_temporary(self):
    with mock.patch("wiki_observations.os.link", side_effect=PermissionError("denied")):
        with self.assertRaisesRegex(ObservationError, "denied") as raised:
            start_observation(self.paths, self.request, self.scope, now=self.started)
    self.assertEqual("io", raised.exception.kind)
    self.assertEqual([], list(self.paths.observations.iterdir()))
```

Also patch `NamedTemporaryFile` with an `OSError` to cover temporary creation failure, and use a fake temporary-file context whose `write()` raises `OSError` to cover write failure after path allocation. Assert the observation directory remains empty in both cases. After every failure case, assert no temporary path remains and none is discoverable as an observation record. These tests cover temporary creation, write, `fsync`, collision retry, and non-collision link failure separately.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_lifecycle.StartLifecycleTests -v`

Expected: FAIL because `start_observation` is not implemented.

- [ ] **Step 3: Implement secure start**

Resolve every destination under the canonical wiki root, reject symlink/traversal escapes, generate suffixes with `secrets.token_hex(3)`, render the immutable Scope into a same-directory temporary file, then atomically claim the final name without overwrite. Persist `planned` or `late` without inventing earlier elapsed time; a crash or failed write must not leave a truncated draft.

Use this creation loop:

```python
def start_observation(paths, request, scope, now=None):
    validate_start_request(request)
    started = (now or datetime.now().astimezone()).replace(microsecond=0)
    paths.observations.mkdir(parents=True, exist_ok=True)
    for _ in range(32):
        run_id = f"{started:%Y%m%d-%H%M%S}-{secrets.token_hex(3)}"
        run_id = f"obs-{run_id}"
        destination = paths.observations / f"{run_id}.md"
        content = render_draft(run_id, started, request, scope)
        temporary_path = None
        try:
            try:
                with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=paths.observations, delete=False) as temporary:
                    temporary_path = Path(temporary.name)
                    os.fchmod(temporary.fileno(), 0o644)
                    temporary.write(content)
                    temporary.flush()
                    os.fsync(temporary.fileno())
            except OSError as error:
                raise ObservationError("io", str(error)) from error
            try:
                os.link(temporary_path, destination)
            except FileExistsError:
                continue
            except OSError as error:
                raise ObservationError("io", str(error)) from error
            else:
                return run_id
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
    raise ObservationError("io", "could not allocate a unique run id")
```

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_lifecycle.StartLifecycleTests -v`

Expected: unique IDs, exact timestamps, path-security, planned/late, temporary-write failure, `fsync` failure, link collision retry, non-collision link error mapping, and cleanup cases pass.

### Task 4: Atomic Finish, Supersession, and Invalidation

**Files:**
- Modify: `wiki_observations.py`
- Modify: `tests/test_observation_lifecycle.py`

**Interfaces:**
- Produces `finish_observation(paths, run_id, status, payload, superseded_by=None, now=None) -> None`.
- Produces `invalidate_observation(paths, run_id, reason, now=None) -> None`.

- [ ] **Step 1: Write failing concurrency and state tests**

```python
def test_only_one_competing_finish_succeeds(self):
    run_id = start_observation(self.paths, self.request, self.scope, now=self.started)
    outcomes = run_competing_finishes(self.paths, run_id, self.payload)
    self.assertEqual(1, outcomes.count("finished"))
    self.assertEqual(1, outcomes.count("state-error"))
    self.assertEqual("success", read_record(self.paths, run_id)[0]["status"])

def test_draft_cannot_be_invalidated(self):
    run_id = start_observation(self.paths, self.request, self.scope, now=self.started)
    with self.assertRaisesRegex(ObservationError, "still draft"):
        invalidate_observation(self.paths, run_id, "invalid fixture")

def test_invalidation_preserves_original_and_excludes_it(self):
    run_id = self.create_finished_run()
    before = self.paths.record(run_id).read_bytes()
    invalidate_observation(self.paths, run_id, "invalid fixture")
    self.assertEqual(before, self.paths.record(run_id).read_bytes())
    self.assertTrue(self.paths.invalidation(run_id).exists())
```

Define `run_competing_finishes` with `multiprocessing.get_context("spawn")`, a module-level `_finish_worker(paths_root, run_id, payload_text, queue)` function, and a `Queue`. Start two independent child processes, have each reconstruct `ObservationPaths` and the completion payload, then put exactly `"finished"` or `"state-error"` on the queue. Join both with a bounded timeout and fail the test if either remains alive or exits nonzero. Do not use threads: `flock` semantics must be exercised across processes.

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_lifecycle.FinishLifecycleTests -v`

Expected: FAIL because locking, finish, supersession, and tombstones are absent.

- [ ] **Step 3: Implement atomic state transitions**

Use a stable `.locks/<run-id>.lock` with `fcntl.flock(LOCK_EX)`, re-read after locking, validate all content before writing, write a same-directory `NamedTemporaryFile(delete=False)`, flush and `os.fsync`, then `os.replace`. Require `superseded_by` only for `superseded`; create invalidation tombstones through a same-directory temporary file followed by an exclusive atomic claim, and never mutate the original record.

The write path must follow this structure:

```python
def finish_observation(paths, run_id, status, payload, superseded_by=None, now=None):
    paths.locks.mkdir(parents=True, exist_ok=True)
    lock_path = paths.locks / f"{run_id}.lock"
    with open(lock_path, "a+", encoding="utf-8") as lock_stream:
        fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
        metadata, body = read_record(paths, run_id)
        if metadata["status"] != "draft":
            raise ObservationError("state", f"{run_id} is already final")
        completed = render_completed(metadata, body, status, payload, superseded_by, now)
        validate_completed_content(completed, paths)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=paths.observations, delete=False) as temporary:
            temporary.write(completed)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        try:
            os.replace(temporary_path, paths.observations / f"{run_id}.md")
        finally:
            temporary_path.unlink(missing_ok=True)

def invalidate_observation(paths, run_id, reason, now=None):
    metadata, _ = read_record(paths, run_id)
    if metadata["status"] == "draft":
        raise ObservationError("state", f"{run_id} is still draft")
    paths.invalidations.mkdir(parents=True, exist_ok=True)
    destination = paths.invalidations / f"{run_id}.md"
    content = render_invalidation(run_id, reason, now or datetime.now().astimezone())
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=paths.invalidations, delete=False) as temporary:
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        os.link(temporary_path, destination)
    except FileExistsError as error:
        raise ObservationError("state", f"{run_id} is already invalidated") from error
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
```

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_lifecycle -v`

Expected: success/partial/failed/rolled-back/superseded, competing finish, crash simulation, invalidation, and stale-draft tests pass.

### Task 5: Operational-Record Integration

**Files:**
- Modify: `AGENTS.md`
- Modify: `wiki_cli.py`
- Create: `tests/test_observation_integration.py`

**Interfaces:**
- Consumes `validate_record` and invalidation discovery.
- Extends existing lint without changing its public return tuple.

- [ ] **Step 1: Write failing integration tests**

```python
def test_observations_are_linted_but_not_graph_or_source_coverage_nodes(self):
    self.write_valid_observation(source="raw/source.md")
    references = wiki_cli.collect_source_references()
    self.assertNotIn("raw/source.md", references)
    errors, broken, orphans, drift, outbound = wiki_cli.perform_lint_checks()
    self.assertEqual([], errors)
    self.assertNotIn("wiki/observations/example.md", orphans)
    self.assertNotIn("wiki/observations/example.md", outbound)
```

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_integration -v`

Expected: FAIL because observations currently participate in generic coverage/graph checks.

- [ ] **Step 3: Implement exclusions and update the operating contract**

Skip `wiki/observations/` in raw coverage collection, orphan/outbound warnings, and overview drift; still invoke dedicated observation/tombstone validation. Add the operational-record rules from the spec to `AGENTS.md` without changing raw triage or task semantics.

Use one path predicate in every generic Wiki scan:

```python
def is_observation_operational_path(path):
    normalized = Path(path).as_posix()
    return normalized == "wiki/observations" or normalized.startswith("wiki/observations/")

# In source coverage and generic graph/drift loops:
if is_observation_operational_path(path):
    continue
```

Call `validate_record` and tombstone validation in a separate observation-specific lint pass so exclusions do not suppress schema errors.

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_integration tests.test_knowledge_loop tests.test_task_records -v`

Expected: observation integration and existing knowledge-loop/task tests pass.

### Task 6: Filtered Descriptive Reporting

**Files:**
- Modify: `wiki_observations.py`
- Create: `tests/test_observation_report.py`

**Interfaces:**
- Produces `collect_records(paths) -> tuple[list[dict], set[str]]` where the set contains invalidated run IDs.
- Produces `render_observation_report(records, invalidated, filters: ReportFilters, now: datetime) -> str`.

- [ ] **Step 1: Write failing aggregation tests**

```python
def test_report_excludes_unknown_superseded_and_invalidated_from_rate(self):
    records = [
        record("ok", "success", elapsed="60"),
        record("partial", "partial", elapsed="unknown"),
        record("old", "superseded", elapsed="5"),
        record("bad", "success", elapsed="10"),
    ]
    report = render_observation_report(records, {"bad"}, ReportFilters(), now=self.now)
    self.assertIn("Success rate: 1/2 (50.0%)", report)
    self.assertIn("Average elapsed seconds: 60", report)
    self.assertIn("Missing elapsed seconds: 1", report)
    self.assertIn("Invalidated: 1", report)
```

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_report -v`

Expected: FAIL because the v2 report engine does not exist.

- [ ] **Step 3: Implement deterministic filters and report rendering**

Group by project/workspace ID/task type/workflow variant, apply inclusive local-date filters, status/task filters, numeric-only aggregates, missing counts, invalidated counts, stale draft ages, and `small sample (n=N)` for fewer than five final records. Never rank or recommend workflows.

Implement aggregation around this denominator:

```python
RATE_STATUSES = {"success", "partial", "failed", "rolled-back"}

def success_fraction(group, invalidated):
    eligible = [row for row in group if row["run_id"] not in invalidated and row["status"] in RATE_STATUSES]
    successes = sum(row["status"] == "success" for row in eligible)
    return successes, len(eligible)

def numeric_values(group, field, invalidated):
    values = []
    missing = 0
    for row in group:
        if row["run_id"] in invalidated or row["status"] in {"draft", "superseded"}:
            continue
        value = row.get("metrics", {}).get(field, "unknown")
        if value == "unknown":
            missing += 1
        else:
            values.append(int(value))
    return values, missing
```

Inject `now` and the local timezone into report rendering; never call `datetime.now()` inside grouping helpers.

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_report -v`

Expected: all grouping, date-boundary, missing-value, invalidation, stale, and small-sample tests pass.

### Task 7: CLI Adapter and Cross-Workspace Contract

**Files:**
- Modify: `wiki_cli.py`
- Create: `tests/test_observation_cli.py`

**Interfaces:**
- Consumes all public functions from `wiki_observations.py`.
- Produces `observe start`, `finish`, `invalidate`, and `report` subcommands with exit codes `0`, `2`, `3`, and `4`.

- [ ] **Step 1: Write failing subprocess tests**

Test setup creates a Git-backed `self.subject_root`; `self.valid_start_args` includes its required `--subject-root` and does not include derived workspace, workspace ID, revision, or working-tree flags.

```python
def test_start_stdout_is_only_run_id_from_external_cwd(self):
    result = self.run_cli("start", *self.valid_start_args, cwd=self.external_dir)
    self.assertEqual(0, result.returncode)
    self.assertRegex(result.stdout, r"^obs-\d{8}-\d{6}-[0-9a-f]{6}\n$")
    self.assertEqual("", result.stderr)
    self.assertTrue((self.wiki_root / "wiki" / "observations" / f"{result.stdout.strip()}.md").exists())

def test_start_derives_provenance_from_subject_root_without_persisting_path(self):
    result = self.run_cli("start", *self.valid_start_args, cwd=self.external_dir)
    self.assertEqual(0, result.returncode)
    record = (self.wiki_root / "wiki" / "observations" / f"{result.stdout.strip()}.md").read_text()
    self.assertIn('workspace: "example-project"', record)
    self.assertNotIn(str(self.subject_root), record)

def test_external_cwd_start_finish_report_and_invalidate(self):
    started = self.run_cli("start", *self.valid_start_args, cwd=self.external_dir)
    run_id = started.stdout.strip()
    finished = self.run_cli("finish", run_id, "--status", "success", "--from-file", str(self.completion_file), cwd=self.external_dir)
    self.assertEqual((0, f"finished {run_id}\n", ""), (finished.returncode, finished.stdout, finished.stderr))
    report_before = self.run_cli("report", "--workspace-id", self.workspace_id, cwd=self.external_dir)
    self.assertIn("Success rate: 1/1 (100.0%)", report_before.stdout)
    invalidated = self.run_cli("invalidate", run_id, "--reason", "temporary smoke fixture", cwd=self.external_dir)
    self.assertEqual((0, f"invalidated {run_id}\n"), (invalidated.returncode, invalidated.stdout))
    report_after = self.run_cli("report", "--workspace-id", self.workspace_id, cwd=self.external_dir)
    self.assertIn("Invalidated: 1", report_after.stdout)
    self.assertIn("Success rate: 0/0", report_after.stdout)

def test_invalid_observe_argument_uses_only_contract_prefix(self):
    result = self.run_cli("start", "--workspace-id", "BAD")
    self.assertEqual(2, result.returncode)
    self.assertTrue(result.stderr.startswith("observation validation error:"))
    self.assertNotIn("usage:", result.stderr.lower())
```

- [ ] **Step 2: Run RED**

Run: `python3 -m unittest tests.test_observation_cli -v`

Expected: FAIL because the old CLI lacks v2 arguments and exit-code mapping.

- [ ] **Step 3: Replace the old observation adapter**

Remove migrated observation constants/helpers from `wiki_cli.py`; add `--wiki-root`, required start `--subject-root`, optional `--project`, start mode, Scope/completion file arguments, superseded-by, invalidation, and report filters. Remove public start flags for workspace, workspace ID, revision, and working-tree state. The adapter calls `derive_provenance`, constructs `StartRequest`, and never serializes subject root. Catch `ObservationError.kind` and write only the specified fixed stderr prefix; preserve non-observe command behavior. Build the observe parser and all its subparsers with this adapter so argparse never writes its own `usage:` text for observe failures:

```python
class ObservationArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ObservationError("validation", message)
```

The `try/except ObservationError` boundary must include observe `parse_args()` as well as command dispatch. If the existing CLI parses top-level arguments before dispatch, pre-detect the `observe` command and parse that branch with `ObservationArgumentParser`; do not allow an observe parsing error to escape through the legacy top-level parser.

Use this single error mapper:

```python
ERROR_CODES = {"validation": 2, "state": 3, "io": 4}

def fail_observation(error):
    prefix = {
        "validation": "observation validation error:",
        "state": "observation state error:",
        "io": "observation io error:",
    }[error.kind]
    print(f"{prefix} {error}", file=sys.stderr)
    return ERROR_CODES[error.kind]
```

The `main()` observe branch must return an integer and the module footer must call `sys.exit(main())`; successful start prints only the run ID, successful finish prints only `finished <run-id>`, and report writes only report text.

- [ ] **Step 4: Run GREEN**

Run: `python3 -m unittest tests.test_observation_cli tests.test_task2_cli_search_log tests.test_task3_cli_lint -v`

Expected: CLI contract and legacy CLI regression tests pass.

### Task 8: Canonical Skill and Parent-Managed Workflow

**Files:**
- Create: `skills/observing-workflows/SKILL.md`
- Create: `skills/observing-workflows/agents/openai.yaml`

**Interfaces:**
- Skill invokes only the v2 CLI contract.
- Parent marker format is exactly `Observation managed by parent run <run-id>; do not start a child observation.`

- [ ] **Step 1: Initialize the canonical skill**

Run:

```bash
python3 ${CODEX_HOME}/skills/.system/skill-creator/scripts/init_skill.py observing-workflows --path skills --interface display_name="Observing Workflows" --interface short_description="Record substantial Codex workflow outcomes" --interface default_prompt="Use $observing-workflows to record this substantial implementation task."
```

Expected: canonical skill directory and `agents/openai.yaml` are created with no placeholder resources.

- [ ] **Step 2: Write the skill from the frozen trigger contract**

The SKILL.md must remain under 500 words and include: exclusions-first decision, top-level-only rule, parent marker, planned/late start, subject-root handoff to CLI-owned provenance derivation, secure `0600` payload files, Scope and completion templates, material-scope supersession, cleanup/finally, failure disclosure, and the absolute central CLI command. Do not duplicate CLI schema internals.

Use this frontmatter and structure:

`````markdown
---
name: observing-workflows
description: Use when Codex is about to perform substantial implementation work involving multiple files, tests or lint, or two or more implementation steps.
---

# Observing Workflows

Record one central observation for each eligible top-level user-authorized task.

Central command: `python3 "${LLMWIKI_ROOT}/wiki_cli.py" observe --wiki-root "${LLMWIKI_ROOT}"`.

## Decide

Exclude chat, read-only work, answer-only work, planning without implementation, simple untested single-file edits, and any worker prompt containing `Observation managed by parent run`.

Otherwise trigger for multi-file work, work requiring tests/lint, or work with at least two implementation steps. If eligibility appears only after work starts, use `start_mode: late`.

## Run

Before the first mutation, identify the current subject workspace root, create a unique mode-0600 Scope payload, and invoke central `start` with `--subject-root`; the CLI derives project, workspace ID, revision, and working-tree state. If syntax must be checked, run `start --help` or `finish --help` as a standalone command before creating a payload and never combine help with a payload-bearing call. Capture only the run ID from stdout. Always delete temporary payloads in cleanup.

Use the required start flags on the first real attempt: `<command> start --title <title> --subject-root <root> --agent-surface codex --start-mode <planned|late> --task-type <type> --workflow-variant <variant> --scope-from-file <path>`. Omit optional `--project`, `--task`, and `--source` unless the explicit label or canonical central referent exists. Finish with `<command> finish <run-id> --status <status> --from-file <path>`, adding `--superseded-by` only for supersession.

Immediately before that command, the skill must expose the exact repository taxonomy: feature/bugfix/refactor/documentation pair only with implementation-basic or implementation-with-review; maintenance only with maintenance-basic or implementation-with-review; compile/inbox-processing only with compile-basic or compile-with-review; query only with research-basic. A taxonomy rejection creates no run and remains a disclosed recording failure even if a legal retry later succeeds; the retry must not expand the authorized task scope.

At the completion call site, list the exact final statuses: success, partial, failed, rolled-back, or superseded. Explicitly reject `completed`; a rejected finish does not transition the draft, and a later legal finish does not erase the failed recording attempt from evaluation evidence.

For controlled forward evaluation only, use `OBSERVATION_EVAL=1` together with both `OBSERVATION_CLI_PATH` and `OBSERVATION_WIKI_ROOT` to replace the central script/root; otherwise ignore any override and use the central command. The evaluator points these variables at a temporary wiki root.

Use these fixed payload shapes (replace bracketed values with sanitized, truthful content):

```markdown
## Scope

- Goal: [goal]
- Included: [included work]
- Excluded: [excluded work or None.]
```

````markdown
## Execution evidence

- Verification: [command and result, or None.]
- Artifacts: [sanitized labels, or None.]

## Outcome and observation

- Outcome: [outcome]
- Observation: [workflow observation]

## Follow-up

- [next action, task reference, or None — no further action]

## Metrics

```yaml
verification: [pass|fail|not-run|unknown]
review_rounds: [non-negative integer|unknown]
defects_found: [non-negative integer|unknown]
rework_count: [non-negative integer|unknown]
rework_reason: [sanitized reason|none|unknown]
```
````

For `partial`, Follow-up must name an actual next action or task reference; it cannot use `None — no further action`.

Pass `Observation managed by parent run <run-id>; do not start a child observation.` to every subagent and aggregate worker evidence into the parent completion.

At completion, create a sanitized mode-0600 completion payload and finish with the truthful final status. For material replacement of Scope, start the replacement first, then finish the prior run as superseded. If observation start or finish fails, continue only within the original authorization and disclose the recording failure in the final response.
`````

- [ ] **Step 3: Validate the canonical skill**

Run:

```bash
python3 ${CODEX_HOME}/skills/.system/skill-creator/scripts/generate_openai_yaml.py skills/observing-workflows --interface display_name="Observing Workflows" --interface short_description="Record substantial Codex workflow outcomes" --interface default_prompt="Use $observing-workflows to record this substantial implementation task."
python3 ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py skills/observing-workflows
```

Expected: validation succeeds and regenerated metadata has `policy.allow_implicit_invocation: true` or the default equivalent.

### Task 8.1: Review Remediation Gate

**Files:**
- Create: `.superpowers/sdd/task-0-v2-report.md`
- Modify: `.superpowers/sdd/progress.md`
- Modify: `wiki/tasks/implement-observation-records-v2.md`
- Modify: `tests/test_task2_cli_search_log.py`
- Modify: `tests/observing_workflows_eval_harness.py`
- Modify: `tests/test_observing_workflows_eval_harness.py`

**Interfaces:**
- Preserve the historical Task 0 count as ledger-derived evidence while explicitly stating that its exact contemporaneous output is unavailable and cannot be independently reproduced after migration.
- Produce frozen `PayloadAudit(root, payload_dir, log_path, wrapper_path, target_cli)` plus `build_payload_audit(case_id, destination, target_cli)`, `payload_audit_environment(audit)`, and `assert_payload_audit(audit, expected_scope_calls, expected_completion_calls)`.
- Keep the frozen decision/lifecycle manifests and exact result schemas unchanged.

- [ ] **Step 1: Capture the three regressions before implementation**

Run:

```bash
test -f .superpowers/sdd/task-0-v2-report.md
python3 -m unittest tests.test_task2_cli_search_log -v
grep -RIn "assert_payload_audit\|PayloadAudit" tests/observing_workflows_eval_harness.py tests/test_observing_workflows_eval_harness.py
```

Expected: the report check fails; the legacy test passes but appends a production `test_action` entry, which is the isolation failure; grep has no matches.

- [ ] **Step 2: Record Task 0 without fabricating historical output**

Create a report that contains the ledger-recorded count `19`, the fixed-interface approval, the current placeholder/diff checks, and this exact limitation:

```markdown
The contemporaneous verbose baseline output was not preserved and cannot be independently reproduced after the v2 schema migration. The count is retained as ledger-derived historical evidence, not presented as a newly rerun result.
```

Update progress and the canonical task record to say `complete with historical audit limitation` instead of implying independently reproducible Task 0 evidence.

- [ ] **Step 3: Isolate the legacy CLI log/search test**

Use a per-test temporary current working directory and invoke the repository CLI by absolute path:

```python
self.repository_root = Path(__file__).resolve().parents[1]
self.cli = self.repository_root / "wiki_cli.py"
self.temporary = tempfile.TemporaryDirectory()
self.root = Path(self.temporary.name)
(self.root / "wiki").mkdir()
(self.root / "wiki" / "z_log.md").write_text("# Log\n", encoding="utf-8")

subprocess.run(
    ["python3", str(self.cli), "log", "test_action", test_details],
    cwd=self.root,
    check=True,
)
```

Snapshot the production `wiki/z_log.md` bytes in setup and assert they remain identical in cleanup. Run the test twice and compare the production log hash before and after both runs.

- [ ] **Step 4: Add isolated payload-audit tests**

Add tests that build a case-local payload directory and wrapper, invoke a fake target CLI with separate Scope/completion mode-0600 files, delete them, and prove audit acceptance. Add rejection tests for mode `0644`, reused path/inode, a surviving observed path, wrong call counts, and a non-empty no-wrapper payload directory. Expose it as `OBSERVATION_PAYLOAD_TMPDIR`; do not replace process-wide `TMPDIR`.

For non-help calls, the wrapper must append JSON lines containing only `flag`, `path`, `device`, `inode`, `mode`, and `regular`, then `os.execv` the target CLI so stdout, stderr, and exit code are unchanged. Calls containing `-h` or `--help` must be delegated without audit because argparse will not consume the payload. `assert_payload_audit` must raise `AssertionError` with deterministic diagnostics and must never inspect payload content.

- [ ] **Step 5: Implement the minimal payload-audit harness**

Add the frozen carrier and public helpers:

```python
from dataclasses import dataclass


PAYLOAD_AUDIT_WRAPPER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import stat
import sys

target = os.environ["OBSERVATION_AUDIT_TARGET_CLI"]
log_path = Path(os.environ["OBSERVATION_AUDIT_LOG"])
if "--help" in sys.argv or "-h" in sys.argv:
    os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
for flag in ("--scope-from-file", "--from-file"):
    if flag not in sys.argv:
        continue
    value = sys.argv[sys.argv.index(flag) + 1]
    payload = Path(value)
    details = os.stat(payload, follow_symlinks=False)
    row = {
        "flag": flag,
        "path": str(payload),
        "device": details.st_dev,
        "inode": details.st_ino,
        "mode": stat.S_IMODE(details.st_mode),
        "regular": stat.S_ISREG(details.st_mode),
    }
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, sort_keys=True) + "\n")
os.execv(sys.executable, [sys.executable, target, *sys.argv[1:]])
'''


@dataclass(frozen=True)
class PayloadAudit:
    root: Path
    payload_dir: Path
    log_path: Path
    wrapper_path: Path
    target_cli: Path


def build_payload_audit(case_id: str, destination: Path, target_cli: Path) -> PayloadAudit:
    if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", case_id):
        raise ValueError("case id must be lowercase letters, digits, and hyphens")
    destination = Path(destination).resolve(strict=True)
    target_cli = Path(target_cli).resolve(strict=True)
    if not target_cli.is_file() or target_cli.is_symlink():
        raise ValueError("target CLI must be a regular non-symlink file")
    root = destination / f"{case_id}-payload-audit"
    if root.exists():
        raise FileExistsError(root)
    root.mkdir(mode=0o700)
    if not root.resolve().is_relative_to(destination):
        raise ValueError("payload audit path escapes destination")
    payload_dir = root / "tmp"
    payload_dir.mkdir(mode=0o700)
    wrapper_path = root / "wiki_cli_audit.py"
    wrapper_path.write_text(PAYLOAD_AUDIT_WRAPPER, encoding="utf-8")
    wrapper_path.chmod(0o700)
    return PayloadAudit(
        root=root,
        payload_dir=payload_dir,
        log_path=root / "payload-audit.jsonl",
        wrapper_path=wrapper_path,
        target_cli=target_cli,
    )


def payload_audit_environment(audit: PayloadAudit) -> dict[str, str]:
    return {
        "OBSERVATION_PAYLOAD_TMPDIR": str(audit.payload_dir),
        "OBSERVATION_AUDIT_LOG": str(audit.log_path),
        "OBSERVATION_AUDIT_TARGET_CLI": str(audit.target_cli),
    }


def assert_payload_audit(
    audit: PayloadAudit,
    expected_scope_calls: int,
    expected_completion_calls: int,
) -> None:
    rows = []
    if audit.log_path.exists():
        for line in audit.log_path.read_text(encoding="utf-8").splitlines():
            rows.append(json.loads(line))
    errors = []
    expected_fields = {"flag", "path", "device", "inode", "mode", "regular"}
    scope_count = sum(row.get("flag") == "--scope-from-file" for row in rows)
    completion_count = sum(row.get("flag") == "--from-file" for row in rows)
    if scope_count != expected_scope_calls:
        errors.append(f"scope calls: expected {expected_scope_calls}, got {scope_count}")
    if completion_count != expected_completion_calls:
        errors.append(
            f"completion calls: expected {expected_completion_calls}, got {completion_count}"
        )
    seen_paths = set()
    seen_inodes = set()
    for index, row in enumerate(rows, 1):
        if set(row) != expected_fields:
            errors.append(f"audit row {index} has invalid fields")
            continue
        path = row["path"]
        identity = (row["device"], row["inode"])
        if path in seen_paths:
            errors.append(f"audit row {index} reuses a payload path")
        if identity in seen_inodes:
            errors.append(f"audit row {index} reuses a payload inode")
        seen_paths.add(path)
        seen_inodes.add(identity)
        if row["regular"] is not True or row["mode"] != 0o600:
            errors.append(f"audit row {index} is not a regular mode-0600 payload")
        if os.path.lexists(path):
            errors.append(f"audit row {index} payload still exists")
    leftovers = sorted(path.name for path in audit.payload_dir.iterdir())
    if leftovers:
        errors.append("payload directory is not empty: " + ", ".join(leftovers))
    if errors:
        raise AssertionError("; ".join(errors))
```

Reject unsafe case IDs and destination escapes, create directories with mode `0700`, create the wrapper with mode `0700`, require unique paths and `(device, inode)` pairs, require regular mode-0600 inputs, require exact call counts, and require every observed path plus all entries in `payload_dir` to be absent at inspection time.

- [ ] **Step 6: Verify remediation without touching raw or global skill state**

Run:

```bash
python3 -m unittest tests.test_task2_cli_search_log tests.test_observing_workflows_eval_harness -v
python3 -m unittest tests.run_observing_workflows_eval.EvalScoreTests tests.test_observing_workflows_eval_harness -v
python3 -m unittest discover -s tests -q
python3 wiki_cli.py tasks
python3 wiki_cli.py tasks --check
python3 wiki_cli.py lint
git diff --check -- tests/test_task2_cli_search_log.py tests/observing_workflows_eval_harness.py tests/test_observing_workflows_eval_harness.py .superpowers/sdd/task-0-v2-report.md .superpowers/sdd/progress.md wiki/tasks/implement-observation-records-v2.md
```

Expected: all tests pass; two consecutive focused runs leave production `wiki/z_log.md` byte-identical; task dashboard is current; lint is Green; no global skill, forward result, production observation, commit, publish action, or additional raw change is created.

### Task 9: Install Skill and Run Forward Evaluation

**Files:**
- Create after approval: `~/.codex/skills/observing-workflows/`
- Create: `tests/skill_evals/observing_workflows_forward.json`
- Create: `tests/skill_evals/observing_workflows_lifecycle_forward.json`

**Interfaces:**
- Consumes canonical skill and frozen manifest.
- Produces an installed copy identical to canonical source, a 20-case decision result file, and lifecycle results inspected from temporary stores.

- [ ] **Step 1: Request approval and install the canonical skill**

After explicit approval, perform the global installation with escalated permission because the destination is outside the workspace. If the destination already exists, first run `diff -qr`; stop on any difference and never overwrite it. If it is absent, copy only the canonical directory, then compare it:

```bash
if test -e ${CODEX_HOME}/skills/observing-workflows; then
  diff -qr skills/observing-workflows ${CODEX_HOME}/skills/observing-workflows
else
  cp -R skills/observing-workflows ${CODEX_HOME}/skills/observing-workflows
  diff -qr skills/observing-workflows ${CODEX_HOME}/skills/observing-workflows
fi
```

Expected: recursive diff exits `0`.

- [ ] **Step 2: Run isolated forward evaluations**

Use the same frozen 20 cases and ordered turns. For each case, the Python harness creates a fresh fixture project workspace and temporary wiki root, then returns CWD, environment, gate command, and checkpoint predicates; it does not launch an agent. The current agent/subagent orchestration launches exactly one evaluator for that case, sends the initial turn, waits at most 15 seconds for the declared gate/predicate, queues follow-ups to that same still-running agent, then releases the gate. Early evaluator exit, timeout, or agent-ID change is a case failure and is never retried. Set `OBSERVATION_EVAL=1`, `OBSERVATION_CLI_PATH`, and `OBSERVATION_WIKI_ROOT` for every decision evaluator process. Do not disclose expected decisions to evaluators. Accumulate per-turn decisions, normalized record checkpoints, final `run_count`, `draft_count`, and `final_statuses` outside the repository; parent/subagent cases must use the exact parent marker. Clean both temporary roots in `finally`, verify the production snapshot is unchanged, and do not add or modify cases during forward evaluation.

For every executable case whose CLI is available, set `OBSERVATION_CLI_PATH` to the case-local payload-audit wrapper, pass its target/audit variables and `OBSERVATION_PAYLOAD_TMPDIR`, and call `assert_payload_audit` before persisting results. Do not replace the evaluator process-wide `TMPDIR`. Use `expected_run_count` as the exact Scope-call count and the length of `expected_final_statuses` as the exact completion-call count. For `central-cli-unavailable`, keep the selected CLI unavailable and require zero wrapper calls plus an empty isolated payload directory. The command-selection-only incomplete-override case does not execute a CLI and uses no audit. Audit failures fail the case and are never retried; the audit log is deleted with the evaluation temporary root and is not added to result JSON.

- [ ] **Step 3: Run frozen lifecycle integration evaluations**

Run every executable case and its ordered turns from `observing_workflows_lifecycle_cases.json` through the same gate/orchestration boundary: Python prepares and inspects, while the current agent/subagent workflow launches, queues follow-ups, and releases gates. Apply complete overrides only where the case setup requests them. The incomplete-override case is command-selection-only: prove it selects the fixed central command and leaves the temporary root untouched, but do not execute the selected production command. For executable cases, inspect normalized per-turn filesystem checkpoints, final records, and final-response disclosure rather than trusting evaluator self-report. Clean all fixtures in `finally`, verify the production snapshot is unchanged, and accumulate only observed results outside the repository.

- [ ] **Step 4: Persist verified eval results**

After all forward and lifecycle cases pass production-integrity comparison, atomically write the accumulated temporary JSON to `tests/skill_evals/observing_workflows_forward.json` and `tests/skill_evals/observing_workflows_lifecycle_forward.json`. Validate both files immediately after replacement. No result file may be written or updated before the production snapshot assertion succeeds.

- [ ] **Step 5: Score decision and lifecycle results**

Run:

```bash
python3 tests/run_observing_workflows_eval.py --forward tests/skill_evals/observing_workflows_cases.json tests/skill_evals/observing_workflows_forward.json
python3 tests/run_observing_workflows_eval.py --lifecycle tests/skill_evals/observing_workflows_lifecycle_cases.json tests/skill_evals/observing_workflows_lifecycle_forward.json
```

Expected: `Trigger accuracy: 20/20`, `Taxonomy accuracy: 20/20`, `Recording accuracy: 20/20`, `Lifecycle: 8/8`, no mismatches; all ten trigger cases have the expected final records and all ten exclusions have zero records.

### Task 10: Final Verification and Maintenance

**Files:**
- Modify only if verification exposes a defect in files already named above.

**Interfaces:**
- Produces no new interface.

- [ ] **Step 1: Run all tests**

Run: `python3 -m unittest discover -s tests -v`

Expected: all tests pass with no traceback or warning noise.

- [ ] **Step 2: Validate generated Wiki entry points**

Run:

```bash
python3 wiki_cli.py sources --check
python3 wiki_cli.py tasks
python3 wiki_cli.py tasks --check
python3 wiki_cli.py lint
```

Expected: source catalog and task dashboard are current; lint has no observation regression. Report unrelated pre-existing warnings separately.

- [ ] **Step 3: Run central and cross-CWD smoke tests**

Run the end-to-end temporary-root test rather than polluting the production observation store:

```bash
python3 -m unittest tests.test_observation_cli.ObservationCliTests.test_external_cwd_start_finish_report_and_invalidate -v
```

Expected: PASS; every CLI command follows the stdout/stderr/exit-code contract, the invalidated record remains immutable but exits aggregates, and temporary input files are deleted in cleanup.

- [ ] **Step 4: Inspect final scope**

Run:

```bash
git diff --check
git status --short
```

Expected: only the files named in this plan plus pre-existing user changes are present; no raw files were created, edited, renamed, or deleted by this implementation.
