# Workflow Evolution Foundation v0.2 Design

Date: 2026-08-02  
Status: Draft for external review  
Review mode: Architecture, then adversarial  

## Section status

- Section 1 — Product boundary and components: Approved
- Section 2 — Episode v2 and Decision Events: Approved baseline
- Section 2 additive amendment — Workflow generation: Draft for approval
- Section 3 — Learning Snapshot, comparability, and improvement candidates: Revised draft
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

### Additive amendment — Workflow generation

Episode v2 frontmatter adds optional `workflow_generation`. It identifies the
explicit version of the workflow contract used by the Episode and is immutable
after Episode start. It is a validated, bounded scalar and must never be
inferred from a Git revision. Its value contains 1–200 ASCII characters,
matches `[a-z0-9][a-z0-9._:@+-]*`, and cannot be the reserved projection states
`unknown` or `unavailable`.

Examples include a versioned contract identifier:

```yaml
workflow_generation: implementation-with-review@2
```

or a content-derived workflow artifact identifier:

```yaml
workflow_generation: wf-sha256-8b28c2f6d53e6d7aab326852acc038915e834c6b530e868f294221300f2641f2
```

The field becomes required for new v2 Episodes once their producing workflow
declares explicit versioning support in the versioned producer-capability
registry. Omission after that declaration is a gate failure. For v1 history
and v2 producers without an explicit version, canonical projection represents
generation availability as `unavailable` with a null value. `unknown` is not a
generation, and records without an auditable generation mapping are
descriptive-only for workflow comparison. A fixed, reviewed migration mapping
may supply a generation in a derived view; it does not edit the Episode or
create another sample.

## Section 3 — Learning Snapshot, comparability, and improvement candidates

This section supersedes the first Section 3 draft. It does not alter the
Approved Sections 1 and 2 except for the explicit workflow-generation
amendment above.

### Canonical adapter input boundary

The analyzer consumes one adapter-neutral, machine-readable operation named
`snapshot-input`. The v0.2 `workflow-learning` acquisition contract replaces
its human-report parsing path with this operation. Human-readable `report`
remains available for people but is not an analyzer input.

Within the selected adapter implementation, `snapshot-input` performs one
consistent sequence:

```text
store resolution
    → structural integrity
    → Episode and invalidation validation
    → task/source reference validation
    → invalidation and supersession resolution
    → canonical Episode projection
    → source-byte hashing
    → input manifest generation
```

The analyzer must not parse the human-readable report, access adapter record
files directly, or combine validation from one storage core with enumeration
from another. Portable and LLMWiki adapters must expose the same canonical
projection and pass a shared conformance fixture.

The canonical input bundle includes:

- adapter semantics and projection versions;
- the absolute analysis interval;
- sorted Episode projections with `run_id`, status, schema availability, and
  Episode source-byte hash;
- resolved invalidations and supersession relationships;
- reference-validation identities and hashes needed to reproduce the gate;
- one manifest hash over the complete selection-relevant input.

A `run_id` identifies exactly one Episode. A migration-derived representation
is a view of that Episode and cannot become a second sample. Conflicting source
records or projections for one `run_id` fail the gate.

### Stable-read rule

The analyzer obtains input manifest A, computes the candidate snapshot in
memory or private temporary storage, then asks the same selected adapter to
recompute selection-relevant manifest B. A and B must be byte-identical.

```text
manifest A
    → deterministic analysis
    → manifest B
    → exact equality
    → atomic snapshot publication
```

If an Episode is finalized, invalidated, superseded, manually changed, or has a
validated reference change between A and B, the run aborts and emits no
Learning Snapshot. An unrelated record outside the frozen selection does not
change the selection-relevant manifest. A later version may replace the double
read with an adapter read epoch or snapshot lease; v0.2 does not require one.

### Absolute analysis interval

The canonical selection basis is Episode `started_at`. A requested local date
range is converted once into a UTC half-open interval. Runtime-local timezone
must never participate in Episode selection.

For a request covering 2026-07-15 through 2026-08-02 in Asia/Taipei, the
canonical window is:

```json
{
  "basis": "started_at",
  "since_inclusive": "2026-07-14T16:00:00Z",
  "until_exclusive": "2026-08-02T16:00:00Z",
  "requested_timezone": "Asia/Taipei",
  "requested_dates": {
    "since": "2026-07-15",
    "until_inclusive": "2026-08-02"
  }
}
```

The timezone name must be a validated IANA identifier. The canonical UTC
instants, not the machine timezone, determine the input manifest.

### Snapshot envelope and canonical core

The artifact separates local or volatile provenance from the semantic core:

```text
Learning Snapshot envelope
├── generated_at
├── privacy-safe opaque store identity
├── snapshot_id
├── artifact_sha256
└── canonical snapshot core
    ├── artifact and analyzer schema versions
    ├── query and absolute interval
    ├── adapter projection and metric-semantics versions
    ├── input manifest and exclusion ledger
    ├── outcome cohorts and lifecycle-health counts
    ├── aggregates and per-metric missingness
    └── deterministic candidate evidence
```

`store_identity` is an opaque, path-free envelope value. It does not enter the
semantic core, so relocating identical valid data does not change the semantic
snapshot identity. `generated_at` is also excluded from that identity.

The canonical core uses UTF-8 JSON with lexicographically sorted object keys,
no insignificant whitespace, UTC timestamps ending in `Z`, integers for
counts, explicit JSON nulls, and normalized non-exponent decimal strings for
derived numeric values. Floating-point JSON numbers are prohibited. The
quantile policy is identified by a versioned semantics ID and computes exact
rational interpolation before rendering a normalized decimal string.

The identifier is:

```text
snapshot_id = SHA-256(
  UTF-8("workflow-observatory:learning-snapshot-core:v1\0")
  + canonical-json(snapshot-core-without-snapshot-id)
)
```

`artifact_sha256` separately covers the canonical complete artifact with the
`artifact_sha256` field omitted. Human-readable Markdown is a deterministic
rendering of the canonical core and is not a second authority. Any LLM-written
narrative is a separate annotation that cites `snapshot_id`; it is not part of
the snapshot core and cannot reuse the snapshot identity as its own identity.

The Learning Snapshot schema fixes `artifact_type` to `learning-snapshot` and
`authoritative` to `false`. Changing that boolean changes the artifact hash but
still cannot create formal acceptance: formal results use a distinct artifact
type, schema, and publication capability that the learning path cannot claim.

### Workflow generation and base cohorts

The outcome cohort key is:

```text
project
+ workspace
+ workspace_id
+ task_type
+ workflow_variant
+ workflow_generation
```

`workflow_generation` must be an explicit version supplied under the Section 2
amendment or an auditable fixed migration mapping. Git revision remains
provenance and cannot substitute for workflow generation.

Episodes with unavailable generation may appear in a separately labeled
legacy descriptive collection. They must not be treated as one shared
generation and cannot support workflow-comparison or recurring-pattern
inference.

Revision, model/runtime identity, environment fingerprint, and Episode schema
version remain provenance rather than unbounded metric labels. Unknown
model/runtime identity remains null and is never inferred. If a cohort contains
multiple known runtime generations without an approved compatibility policy,
the snapshot emits `heterogeneous-runtime-provenance` and limits that cohort to
descriptive output.

### Outcome analysis and lifecycle health

The snapshot separates two denominator families.

Outcome analysis includes only non-invalidated Episodes with these statuses:

- `success`
- `partial`
- `failed`
- `rolled-back`

Lifecycle health separately reports:

- active and stale `draft` Episodes;
- `superseded` Episodes;
- invalidated Episodes;
- schema-adoption and generation-availability coverage.

Draft, superseded, and invalidated Episodes never enter workflow outcome,
quality, duration, success-rate, or Decision Event effectiveness denominators.
They may motivate only separately labeled lifecycle-health measurement or
recovery work.

Every cohort reports explicit counts such as:

```json
{
  "outcome_episode_n": 4,
  "superseded_episode_n": 1,
  "draft_episode_n": 2,
  "invalidated_episode_n": 0
}
```

Fewer than five comparable outcome Episodes permits descriptive counts only.
At five or more, deterministic evidence may support a cautious recurring-
pattern hypothesis. No sample size establishes causality or a winning
workflow.

### Per-metric semantics and missingness

Metric semantics are field-specific, not a global cohort key:

```json
{
  "metric_semantics": {
    "elapsed_seconds": "wall-clock-elapsed@1",
    "review_rounds": "formal-review-cycle@1",
    "test_failures": "confirmed-test-failure@1"
  }
}
```

Each metric reports a complete eligibility partition:

```json
{
  "eligible_episode_n": 12,
  "observed_n": 6,
  "not_recorded_n": 2,
  "unsupported_by_schema_n": 4,
  "not_applicable_n": 0
}
```

The four category counts must sum to `eligible_episode_n`:

- `observed` contains a legal value, including a legal zero.
- `not_recorded` means the Episode schema supports the metric but stores null
  or explicit unknown.
- `unsupported_by_schema` means that Episode schema has no such field.
- `not_applicable` follows a deterministic rule in the metric-semantics
  registry.

An invalid value is a Data Trust Gate failure and cannot appear as another
missingness bucket. v1 absence is never converted to zero or ordinary v2
missingness. Episodes may contribute to one common metric while remaining
ineligible for another whose semantics differ.

### Deterministic descriptive output

For each eligible cohort and metric, the deterministic core may report:

- exact outcome and lifecycle counts;
- missingness partitions with denominators;
- p25, p50, and p75 using `linear-rational-quantile@1`;
- review, defect, rework, test-failure, and timeout distributions;
- low-cardinality reason-code support;
- v2 Decision Event support.

Confidence intervals are outside v0.2. A later amendment must freeze the
interval method, minimum sample size, quantile policy, and missing-value
treatment before adding them.

### Decision Event analysis

Decision recurrence is measured primarily in distinct Episodes, never raw
event count. Every pattern reports:

```json
{
  "event_count": 10,
  "episode_count_with_event": 3,
  "eligible_episode_n": 12
}
```

Within one Episode, repeated occurrences of a reason code are not independent
samples. Sequence support is the number of distinct Episodes containing that
sequence. `summary` is never a group key, label, or frequency input; only the
versioned low-cardinality Decision Event fields are analyzable.

The v0.2 policy `decision-pattern-support@1` requires both at least three
distinct supporting Episodes and support in at least 40 percent of eligible
Episodes before producing a recurring Decision Event hypothesis. This policy
is independent of the cohort-level five-outcome-Episode threshold.

### Trust Gate Diagnostic versus Learning Snapshot

A gate failure may emit a privacy-minimized immutable `trust-gate-diagnostic`
describing the failure class, adapter semantics, and bounded identifiers. It
does not contain aggregates, is not a Learning Snapshot, and cannot be consumed
by `workflow-improving` or used to create an Evolution Proposal.

```text
Gate failure
    → Trust Gate Diagnostic or error
    → no Learning Snapshot
    → no Evolution Proposal
```

After a successful gate, snapshot data-health evidence may include supported-
but-missing metrics, stale drafts, schema-adoption coverage, invalidation
counts, and unavailable legacy generation. Adapter mismatch, invalid
references, and store change belong to Trust Gate Diagnostics, not snapshot
candidates.

Duplicate lifecycle attempts and historical integrity aborts are out of scope
for v0.2 because no approved immutable health-event artifact currently records
them. A later design may include them only through a separately approved,
privacy-minimized health-event schema present in the input manifest.

### Deterministic improvement candidates

A successful snapshot may identify deterministic evidence candidates in these
classes:

1. Lifecycle health: stale drafts, schema adoption, invalidation volume, or
   generation availability.
2. Outcome reliability: failed, partial, or rolled-back outcome distributions
   and measured timeout patterns.
3. Quality: defect, rework, verification, or low-cardinality reviewer reason
   patterns.
4. Efficiency: elapsed-time, review-loop, or measured token/cache patterns.
5. Decision patterns: task split, rejection, rollback, stop, or other approved
   low-cardinality patterns meeting the separate support policy.

v0.2 does not rank candidates. It emits stable `candidate_id` values sorted in
ascending byte order, the exact deterministic evidence, and
`evidence_strength` of `descriptive` or `recurring`. It does not assign a
probabilistic confidence, actionability score, composite workflow score, or
LLM-selected priority. `workflow-improving` acts only after a user selects one
candidate from a cited snapshot.

Each `candidate_id` is the lowercase hexadecimal SHA-256 of the UTF-8 domain
separator `workflow-observatory:learning-candidate:v1\0` followed by the
canonical JSON bytes of that candidate's class, cohort identity, metric or
pattern semantics ID, denominators, observed values, and evidence strength.
Human narrative and envelope provenance do not enter this identity.

### Post-hoc evaluation artifacts

Workflow Evolution Foundation v0.2 Learning Snapshots do not consume post-hoc
human, reviewer, or evaluator score artifacts. A later design may add them only
after defining their schema, evaluator identity, `run_id` join rules,
duplicate/conflict rules, input hashes, and metric-specific denominators. This
preserves the Section 2 separation between execution facts and judgments.

### Bounded historical baseline

After the Data Trust Gate implementation is repaired, the first snapshot may
inspect validated inputs selected by the canonical UTC interval corresponding
to 2026-07-15 through 2026-08-02 in Asia/Taipei. Candidate collections include
LLMWiki compile-basic, compile-with-review, feature-with-review, and
maintenance-basic records, plus Workflow Observatory feature-with-review
records. Generation or runtime heterogeneity may restrict any collection to
descriptive output. These names identify analysis candidates, not conclusions.

## Section 3 acceptance tests for the later implementation plan

1. Identical canonical inputs and rules produce identical snapshot-core bytes
   and `snapshot_id`.
2. Different machine timezones do not change the input manifest for the same
   absolute interval.
3. A selection-relevant store change between manifests A and B aborts without
   leaving a snapshot.
4. Portable and LLMWiki adapters produce identical canonical projections for
   equivalent fixtures.
5. A v1 absent field is neither zero nor ordinary v2 missingness.
6. Draft, superseded, and invalidated Episodes do not enter outcome
   denominators; stale drafts appear only in lifecycle health.
7. Decision Event recurrence uses distinct Episode support and the versioned
   support policy.
8. One original Episode and its migration-derived view count once by `run_id`.
9. A gate failure produces no Learning Snapshot or Evolution Proposal.
10. Modifying `authoritative` cannot transform a learning artifact into formal
    acceptance.
11. LLM narrative variation cannot change or share a semantic snapshot
    identity without its own annotation identity.
12. Without explicit user approval, the workflow does not edit a workflow or
    skill, create a branch or pull request, or execute an experiment.

## Resolved Section 3 questions

- Model/runtime identity is optional and never inferred. Known incompatible or
  ungoverned heterogeneity forces descriptive-only output.
- Workflow generation is explicit under the Section 2 amendment; Git revision
  is provenance only.
- Confidence intervals are excluded from v0.2.
- Evaluation artifacts are excluded from v0.2 snapshots.
- Candidates are stable but unranked in v0.2.

## External review request

Review the Section 2 additive amendment and revised Section 3 first in
architecture mode, then adversarial mode. Check:

- compatibility with the Approved Sections 1 and 2 baseline;
- canonical adapter acquisition and stable-read behavior;
- semantic content addressing and artifact authority;
- absolute-window reproducibility;
- outcome versus lifecycle denominators;
- field-specific metric semantics and missingness;
- Decision Event Episode-level support;
- Trust Gate Diagnostic isolation;
- v1/v2 derived-view deduplication;
- privacy, cardinality, and approval-gate bypasses;
- coverage of the twelve acceptance-test requirements.

Return one of:

- Approved
- Approved with required changes
- Not approved

Separate required corrections from optional improvements.
