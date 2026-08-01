# Workflow Evolution Foundation v0.2 Design

Date: 2026-08-02  
Status: Draft for external review  
Review mode: Architecture, then adversarial  

## Section status

- Section 1 — Product boundary and components: Approved
- Section 2 — Episode v2 and Decision Events: Approved
- Section 3 — Learning Snapshot, comparability, and improvement candidates: Draft
- Error behavior, experiment lifecycle, and verification design: Not yet designed

This document is not implementation-ready. Sections marked Approved are the
review baseline and must not be silently changed. Section 3 is the current
review target. Later sections and an implementation plan require separate
approval.

## Purpose

Extend Workflow Observatory from local workflow telemetry into an
approval-gated feedback loop:

```text
Knowledge
    → Workflow
    → Agent execution
    → Episode
    → Learning Snapshot
    → Evolution Proposal
    → user-approved experiment
    → formal acceptance or rollback
```

The first milestone analyzes the bounded history from 2026-07-15 through
2026-08-02 and may create only read-only learning and proposal artifacts. It
does not modify a workflow, create a pull request, or execute an experiment.

## Existing decisions

- Extend the existing `workflow-observer` plugin rather than create a separate
  evolution plugin or an LLMWiki-only prototype.
- Treat one top-level observation as one Episode; do not create a duplicate
  Episode store.
- Preserve `run_id` as the Episode identity.
- Keep schema v1 records unchanged and readable.
- Keep execution facts separate from post-hoc quality scores.
- Keep learning read-only and improvement approval-gated.
- Preserve the distinction between discovery, targeted diagnostic, and formal
  acceptance evidence.

## Current evidence and blocking compatibility fault

The bounded inventory reports 88 LLMWiki observation files, but that count is
not yet valid learning evidence. The Marketplace adapter fails its required
validation gate before a Learning Snapshot can be produced.

The failure is reproducible:

1. The LLMWiki adapter's `report` operation delegates to the configured
   LLMWiki CLI.
2. Its `validate` and `integrity` operations instead use the Marketplace's
   bundled storage core.
3. The bundled core resolves task references under `wiki/tasks/<id>.md`.
4. The current LLMWiki task contract resolves canonical records under
   `wiki/tasks/records/<id>.md`.
5. Validation therefore reports a missing task for an observation whose task
   record exists in the current canonical directory.

This is a Data Trust Gate defect, not evidence that the workflow itself is
better or worse. Formal learning must stop until validation, integrity, and
reporting use compatible semantics.

## Section 1 — Product boundary and components

### Data Trust Gate

The gate reports the selected adapter, store identity, schema capabilities,
and record count. Validation, integrity, and reporting must use the same
storage semantics. A failure stops learning without producing a partial
snapshot.

### Versioned Episode

An observation becomes the versioned Episode envelope. Schema v2 adds
structured execution, quality, and optional decision data while preserving the
existing human-readable evidence sections.

### Learning Snapshot

A Learning Snapshot is read-only, content-addressed, reproducible, and bounded
by an explicit time window. It records exact group keys, input hashes, missing
values, observed counts, and cautious inferences.

### Evolution Proposal

One proposal references one or more validated snapshots and contains exactly
one bounded, reversible experiment. Creating a proposal does not authorize a
workflow edit or experiment.

### Acceptance boundary

After separate user approval, an experiment enters a new implementation cycle.
Only a fully green formal acceptance run from one fixed implementation
generation can support adoption. Discovery and targeted diagnostic evidence
remain non-authoritative.

### First-milestone non-goals

- Background scheduling
- Automatic workflow mutation
- Automatic pull requests
- Cross-user benchmarking
- Full prompt, transcript, tool-argument, or tool-result capture
- Claims that observational correlation establishes causality

## Section 2 — Episode v2 and Decision Events

### Record shape

Episode v2 remains one Markdown observation record:

```text
obs-<date>-<time>-<id>.md
├── frontmatter: identity, taxonomy, status, and schema version
├── human evidence: Scope, Execution, Outcome, and Follow-up
└── machine data: one canonical JSON Episode block
```

Keeping the structured block in the atomically finalized record avoids a
Markdown-versus-sidecar consistency boundary.

### Machine-readable Episode data

```json
{
  "schema_version": 2,
  "execution": {
    "elapsed_seconds": 1200,
    "input_tokens": null,
    "output_tokens": null,
    "cache_read_tokens": null,
    "cost_amount": null,
    "cost_currency": null,
    "measurement_source": "tool-derived"
  },
  "quality": {
    "verification": "pass",
    "review_rounds": 2,
    "defects_found": 3,
    "rework_count": 2,
    "test_failures": 1,
    "timeout_count": 0
  },
  "decisions": []
}
```

Unavailable token and cost values are `null`. Estimated values must not be
represented as measurements.

### Decision Event

Decision Events record only consequential choices that change the execution
path. They do not record every tool call.

```json
{
  "sequence": 1,
  "phase": "planning",
  "actor_role": "planner",
  "decision_type": "split-task",
  "reason_code": "complexity-threshold",
  "result": "supported",
  "summary": "Split evaluator work into isolated coordinator and worker units."
}
```

Constraints:

- At most 12 Decision Events per Episode.
- `phase`, `actor_role`, `decision_type`, `reason_code`, and `result` use
  versioned, low-cardinality enumerations.
- `summary` is limited to 200 Unicode code points.
- Prompts, transcripts, tool arguments, credentials, absolute paths, and large
  error bodies are prohibited.
- Routine successful work may contain no Decision Events.
- Decision Events are supplied at Episode finish and written in the same
  atomic lifecycle transition; v0.2 does not introduce an event append API.

### Facts and judgments

Episode data contains execution facts and source-attributed agent or tool
reports. Human, reviewer, or evaluator quality scores are immutable evaluation
artifacts that reference `run_id`; they do not rewrite the Episode.

### Compatibility

- Schema v1 records remain unchanged.
- The system never fabricates Decision Events for v1 history.
- Learning may consume common v1/v2 fields only when their metric semantics are
  identical.
- Decision analysis consumes v2 Episodes only.
- Migration may create a derived representation but never edit historical
  records in place.

## Section 3 — Learning Snapshot, comparability, and improvement candidates

### Data Trust Gate

Before producing a snapshot, the analyzer must:

1. Report the selected adapter, store identity, adapter version, and storage
   capability fingerprint.
2. Prove that validation, integrity, and report semantics are compatible.
3. Validate input Episodes, invalidations, and canonical task/source
   references.
4. Freeze a bounded time window.
5. Freeze the analyzer version, query, grouping rules, and metric-semantics
   version.

Any failure stops the run. No partial snapshot is emitted.

### Snapshot identity

```json
{
  "snapshot_id": "sha256:<digest>",
  "schema_version": 1,
  "analyzer_version": "0.2.0",
  "adapter_identity": "<adapter>",
  "store_identity": "<opaque-local-identity>",
  "window": {
    "since": "2026-07-15",
    "until": "2026-08-02"
  },
  "input_manifest_sha256": "<digest>",
  "authoritative": false
}
```

The snapshot identity is derived from canonical query and time-window data,
the analyzer and metric-semantics versions, sorted input run IDs, Episode
content hashes, the invalidation set, and grouping configuration. Identical
inputs must produce an identical identity.

Learning Snapshots are descriptive evidence. `authoritative: false` prevents a
snapshot from being mistaken for experiment acceptance.

### Comparable cohort

The cohort key is:

```text
project
+ workspace
+ workspace_id
+ task_type
+ workflow_variant
+ workflow_generation
+ metric_semantics_version
```

Revision, model/runtime identity, environment fingerprint, and Episode schema
version are retained as provenance. They must not become unbounded metric
labels.

Comparability rules:

- Include only final, non-invalidated Episodes.
- Exclude drafts from every count, rate, trend, and inference.
- Combine v1/v2 common fields only when field semantics are identical.
- Analyze Decision Events only for v2 Episodes.
- Do not merge different workflow generations into one baseline.
- Split inconsistent workspace labels or mark the data non-comparable.

### Descriptive output

Each cohort reports:

- Final status counts and comparable sample size
- Missing-value counts with explicit denominators
- Median, p25, and p75 elapsed time
- Success, partial, failed, and rolled-back counts
- Review, defect, rework, test-failure, and timeout distributions
- Low-cardinality reason-code frequencies
- Decision Event patterns for v2 Episodes

The mean may be reported but never alone because interruptions and long-lived
Episodes can distort it.

For fewer than five comparable final Episodes, output descriptive counts only.
At five or more, the analyzer may produce a cautious recurring-pattern
hypothesis. No sample size permits the analyzer to claim causality or declare a
winning workflow from observational records.

### Improvement-candidate classes

The snapshot identifies candidates but does not prescribe a change:

1. Data health: adapter mismatch, stale drafts, schema mismatch, missing
   metrics, or invalid references.
2. Reliability: failed, partial, or rolled-back outcomes; integrity aborts;
   repeated timeouts; or duplicate lifecycle attempts.
3. Quality: high defect or rework counts, recurring reviewer rejection codes,
   or verification failures.
4. Efficiency: high median duration, repeated review loops, decision reversals,
   or measured token/cache inefficiency.
5. Decision patterns: recurring task-split, rejection, rollback, or stop
   patterns associated with later outcomes.

Candidates may be ordered by recurrence, severity, confidence, actionability,
and reversibility. The system does not calculate a composite workflow score
unless a later approved design defines its meaning and weighting.

### Bounded historical baseline

After the Data Trust Gate is repaired, the first snapshot may inspect the
validated records from 2026-07-15 through 2026-08-02. Candidate cohorts include
LLMWiki compile-basic, compile-with-review, feature-with-review, and
maintenance-basic records, plus Workflow Observatory feature-with-review
records. These names identify analysis candidates, not conclusions.

## Unresolved questions for Section 3 review

1. Can every supported Codex surface expose a stable, privacy-safe model/runtime
   identity? If not, the value remains unavailable rather than inferred.
2. Workflow generation should come from an explicit workflow version. A Git
   revision is provenance and must not silently substitute for that version.
3. Median, quartiles, counts, and missingness are the minimum statistical
   output. Confidence intervals should be added only if they materially improve
   decisions without implying false precision.

## External review request

Review Section 3 first in architecture mode, then adversarial mode. Check:

- compatibility with Approved Sections 1 and 2;
- adapter and lifecycle consistency;
- reproducibility and content-addressing boundaries;
- v1/v2 mixed-analysis safety;
- statistical and causal overclaiming;
- schema drift, duplicate counting, and missing-data behavior;
- privacy, cardinality, and approval-gate bypasses;
- testability, rollback, and formal-acceptance boundaries.

Return one of:

- Approved
- Approved with required changes
- Not approved

Separate required corrections from optional improvements.
