# Observe 記錄機制 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provide a Markdown-first, semi-automated CLI for starting, finishing, validating, and reporting LLM Wiki workflow observations.

**Architecture:** Store one immutable-by-default observation Markdown record in `wiki/observations/` per run. Add focused helpers to `wiki_cli.py` for parsing, writing, linting, and summarizing those records; expose them through an `observe` command group. Keep task records as the canonical open-loop system, and accept raw-source references only as optional evidence.

**Tech Stack:** Python 3 standard library (`argparse`, `datetime`, `os`, `re`, `unittest`); repository Markdown and YAML-like frontmatter conventions.

## Global Constraints

- Do not modify, rename, or delete files under `raw/`.
- Observation records live only in `wiki/observations/`; one completed work run has one record.
- Observation frontmatter must retain the page contract: `type`, `title`, `tags`, `timestamp`, and `sources`.
- Supported task types are exactly `compile`, `maintenance`, `query`, and `inbox-processing`.
- Initial workflow variants are exactly `compile-basic`, `compile-with-review`, and `maintenance-basic`.
- Supported final statuses are exactly `success`, `partial`, `failed`, and `rolled-back`.
- Unknown metric values remain the string `unknown`; reports must exclude them from numeric aggregates and count them as missing.
- Do not make recommendation or causal claims from observation reports.
- Do not run `publish`, commit, or modify unrelated existing changes.

---

## File Structure

- Modify `wiki_cli.py`: define the observation schema and record lifecycle, integrate schema validation into lint, render a descriptive report, and register the `observe` CLI group.
- Create `tests/test_observation_records.py`: isolated filesystem tests for lifecycle, validation, lint, and report behavior.
- Modify `docs/superpowers/specs/2026-07-12-observation-records-design.md`: add the omitted `run_id` required by `observe finish <run-id>`; no behavioral change beyond making the approved design unambiguous.

### Task 1: Specify and validate the record format

**Files:**
- Modify: `wiki_cli.py:20-25` and new observation helpers before `perform_lint_checks`
- Create: `tests/test_observation_records.py`
- Modify: `docs/superpowers/specs/2026-07-12-observation-records-design.md`

**Interfaces:**
- Produces `OBSERVATIONS_DIR`, `OBSERVATION_TASK_TYPES`, `OBSERVATION_WORKFLOW_VARIANTS`, and `OBSERVATION_STATUSES` constants.
- Produces `collect_observation_records() -> list[dict]` and `validate_observation_record(metadata: dict, body: str) -> list[str]`.
- A completed record’s Metrics code block is parsed by `parse_observation_metrics(body: str) -> dict`.

- [ ] **Step 1: Write the failing unit tests for record discovery and schema errors**

```python
def test_validate_observation_record_rejects_invalid_status_and_unknown_task_link(self):
    path = self.write_observation(status="invalid", task_ref="[[missing-task]]")
    metadata, body = wiki_cli.parse_frontmatter(open(path, encoding="utf-8").read())
    errors = wiki_cli.validate_observation_record(metadata, body)
    self.assertIn("invalid status `invalid`", "\n".join(errors))
    self.assertIn("task_ref points to no task record", "\n".join(errors))
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `python3 -m unittest tests.test_observation_records.TestObservationRecords.test_validate_observation_record_rejects_invalid_status_and_unknown_task_link -v`

Expected: FAIL because the observation validation helpers do not exist.

- [ ] **Step 3: Add the constants, discovery function, Metrics parser, and validation helper**

Implement these rules exactly:

```python
OBSERVATIONS_DIR = os.path.join("wiki", "observations")
OBSERVATION_TASK_TYPES = ("compile", "maintenance", "query", "inbox-processing")
OBSERVATION_WORKFLOW_VARIANTS = ("compile-basic", "compile-with-review", "maintenance-basic")
OBSERVATION_STATUSES = ("success", "partial", "failed", "rolled-back")
```

Validate `type: observation`; all page-contract frontmatter; `run_id`; task type; workflow variant; status; ISO-8601 start time; optional `task_ref` against `collect_task_records()`; raw-only source paths; exactly one Metrics block for completed records; valid finish timestamp; nonnegative integer elapsed seconds equal to finish minus start; and the four allowed verification values (`pass`, `fail`, `not-run`, `unknown`). Return diagnostics rather than raising for malformed files.

- [ ] **Step 4: Amend the design spec to state the record identity**

Add `run_id: "obs-YYYYMMDD-HHMMSS"` to the frontmatter example and specify that it is the filename stem and the argument accepted by `observe finish`.

- [ ] **Step 5: Run the observation test to verify it passes**

Run: `python3 -m unittest tests.test_observation_records -v`

Expected: PASS for record collection and invalid-schema cases.

### Task 2: Implement `observe start` and `observe finish`

**Files:**
- Modify: `wiki_cli.py` observation helper section and `main()` CLI registration/dispatch
- Modify: `tests/test_observation_records.py`

**Interfaces:**
- Consumes `validate_observation_record()` and supported-value constants from Task 1.
- Produces `start_observation(title: str, task_type: str, workflow_variant: str, task_ref: str | None, sources: list[str]) -> str | None`.
- Produces `finish_observation(run_id: str, status: str, verification: str, review_rounds: str, defects_found: str, rework: str) -> bool`.

- [ ] **Step 1: Write failing lifecycle tests**

```python
def test_start_then_finish_writes_a_complete_record_once(self):
    run_id = wiki_cli.start_observation("Compile source", "compile", "compile-basic", None, ["raw/source.md"])
    self.assertTrue(os.path.exists(f"wiki/observations/{run_id}.md"))
    self.assertTrue(wiki_cli.finish_observation(run_id, "success", "pass", "unknown", "0", "none"))
    self.assertFalse(wiki_cli.finish_observation(run_id, "success", "pass", "unknown", "0", "none"))
```

- [ ] **Step 2: Run the lifecycle test to verify it fails**

Run: `python3 -m unittest tests.test_observation_records.TestObservationRecords.test_start_then_finish_writes_a_complete_record_once -v`

Expected: FAIL because lifecycle functions do not exist.

- [ ] **Step 3: Implement start and finish with deterministic file semantics**

`start_observation` must create `wiki/observations/`, generate a collision-free `obs-YYYYMMDD-HHMMSS` run ID, reject unsupported task type/workflow variant and non-`raw/` sources, and write the fixed Scope, Execution evidence, Metrics placeholder, Outcome and observation, and Follow-up sections. `finish_observation` must reject an unknown ID, a non-final status, and a record that already contains a completed Metrics block. It writes `finished_at`, recalculated `elapsed_seconds`, validation result, and supplied metrics; it never edits scope or previous evidence.

- [ ] **Step 4: Register CLI grammar and dispatch**

Implement:

```text
python3 wiki_cli.py observe start --title TITLE --task-type TYPE --workflow-variant VARIANT [--task TASK_ID] [--source RAW_PATH ...]
python3 wiki_cli.py observe finish RUN_ID --status STATUS [--verification VALUE] [--review-rounds VALUE] [--defects-found VALUE] [--rework VALUE]
```

Make missing `start`/`finish` subcommands, invalid arguments, nonexistent run IDs, and duplicate finish operations return nonzero without partially writing a record.

- [ ] **Step 5: Run lifecycle and CLI tests to verify they pass**

Run: `python3 -m unittest tests.test_observation_records -v`

Expected: PASS, including duplicate-finish and invalid-input tests.

### Task 3: Add observation lint integration and reporting

**Files:**
- Modify: `wiki_cli.py:663-773` and `main()` CLI registration/dispatch
- Modify: `tests/test_observation_records.py`

**Interfaces:**
- Consumes `collect_observation_records()`, `validate_observation_record()`, and `parse_observation_metrics()`.
- Produces `render_observation_report(records: list[dict]) -> str` and `run_observation_report() -> bool`.
- Extends `perform_lint_checks()` to include observation validation errors in its existing schema-error list.

- [ ] **Step 1: Write failing lint and report tests**

```python
def test_report_excludes_unknown_elapsed_time_and_discloses_missing_values(self):
    self.write_complete_observation("obs-a", elapsed_seconds="60", status="success")
    self.write_complete_observation("obs-b", elapsed_seconds="unknown", status="partial")
    report = wiki_cli.render_observation_report(wiki_cli.collect_observation_records())
    self.assertIn("Samples: 2", report)
    self.assertIn("Average elapsed seconds: 60", report)
    self.assertIn("Missing elapsed seconds: 1", report)
```

- [ ] **Step 2: Run the reporting test to verify it fails**

Run: `python3 -m unittest tests.test_observation_records.TestObservationRecords.test_report_excludes_unknown_elapsed_time_and_discloses_missing_values -v`

Expected: FAIL because reporting functions do not exist.

- [ ] **Step 3: Implement lint integration and report grouping**

Extend the existing schema phase so each `wiki/observations/*.md` record appends every validation diagnostic as `(path, diagnostic)`. `observe report` must group rows by `(task_type, workflow_variant, status)`, report total samples, successes over all final statuses, mean elapsed seconds over numeric values only, records with a non-`none` rework value, and field-specific missing counts. If no records exist, print a clear empty-state message and return success. Do not write any wiki pages and do not suggest a preferred workflow.

- [ ] **Step 4: Add the report parser and dispatch**

Implement `python3 wiki_cli.py observe report` with no write-capable arguments. Make `argparse` reject a report invocation that includes start/finish-only options.

- [ ] **Step 5: Run all observation tests and existing test suite**

Run: `python3 -m unittest discover -s tests -v`

Expected: all tests PASS.

### Task 4: Verify command-line behavior and repository maintenance

**Files:**
- Modify: `wiki_cli.py` only if verification exposes an implementation defect
- Modify: `tests/test_observation_records.py` only if a regression test is required

**Interfaces:**
- Consumes the complete observation lifecycle and lint/report functions.
- Produces no new interface.

- [ ] **Step 1: Exercise the user-facing help and empty report**

Run:

```bash
python3 wiki_cli.py observe --help
python3 wiki_cli.py observe start --help
python3 wiki_cli.py observe finish --help
python3 wiki_cli.py observe report
```

Expected: help lists the three subcommands and report exits successfully with an explicit no-records message when the repository has none.

- [ ] **Step 2: Run source catalog synchronization check**

Run: `python3 wiki_cli.py sources --check`

Expected: source catalog is current, or report only pre-existing discrepancies unrelated to observation records.

- [ ] **Step 3: Run lint and inspect changes**

Run:

```bash
python3 wiki_cli.py lint
git diff --check
git status --short
```

Expected: lint reports no observation-schema regressions; whitespace check passes; status contains only intended observation implementation files plus already-existing user changes.
