# Workflow Observatory Roadmap

This roadmap contains only Workflow Observatory product work. Personal LLM Wiki
tasks and production observations are intentionally excluded.

## Current public release line

- Maintain stable release `v0.1.0` and public prerelease `v0.2.0-rc1`; the
  unreleased Phase 1 checkpoint is not on this release line.
- Keep the completed deterministic archive and clean-room gate passing on
  Python 3.11+ and current Codex for future releases.
- Maintain the public MIT-licensed marketplace with its specifications, plans,
  complete test evidence, and SHA-256 inventory.
- Keep the implemented sharded evaluator coordinator, ordered leases, sealed
  worker evidence, cleanup recovery, and opt-in Marketplace CLI passing their
  deterministic gates.
- Preserve the deterministic no-model proof of one coordinator, four real
  workers, and 28 sealed cases in 8/8/8/4 lanes without a Codex invocation or
  discovery writer call.
- Preserve one final aggregate gate that validates all 20 forward and 8
  lifecycle case results, exact frozen manifest hashes, store integrity,
  payload cleanup, and production isolation.
- The protected real-model formal epoch remains pending; independent review and
  explicit approval are required, and deterministic protocol coverage is not a
  real-model 28/28 result.

The implemented worker layout, isolation model, retry policy, and RED/GREEN record
are documented in
[`docs/parallel-evaluation-plan.md`](docs/parallel-evaluation-plan.md).

## Completed milestone — Workflow Evolution Foundation v0.2

Workflow Evolution Foundation v0.2, including Task 11's 15-case matrix and
bounded fake-store historical dry run, is complete.
All 11 milestone tasks and the supported-interpreter execution completed before
the v0.2 checkpoint was included in public prerelease
[`v0.2.0-rc1`](https://github.com/chouchunming/workflow-observatory/releases/tag/v0.2.0-rc1).

The bounded foundation turns privacy-minimized observations into reproducible,
non-authoritative learning evidence without changing workflows automatically:

```text
Episode v2
    -> canonical snapshot-input
    -> Data Trust Gate
    -> immutable Learning Snapshot
    -> stable observational candidates
```

Implementation was divided into 11 independently reviewable TDD units:

1. Add RFC 8785 JCS identity primitives and deterministic code-artifact
   manifests.
2. Freeze policy and registry inputs, including exclusive effective-boundary
   forms for producer capabilities.
3. Add Episode v2 supplements, explicit workflow generations, and canonical
   projection while preserving v1 compatibility.
4. Keep Episode v2 lifecycle and CLI behavior backward compatible.
5. Make reference validation use the selected adapter's storage semantics.
6. Add one adapter-neutral `snapshot-input` operation with absolute UTC
   intervals and privacy-safe source hashes.
7. Add deterministic cohorts, lifecycle health, per-metric missingness, and
   rational quantiles.
8. Count Decision patterns by distinct Episodes and emit stable, unranked
   candidate evidence.
9. Require manifest A/B stable reads and atomic Learning Snapshot publication.
10. Update learning, telemetry, improving, packaging, and user-facing
    contracts without enabling mutation.
11. Passed the 15-case non-model acceptance matrix across isolated fake stores
    and the supported CPython 3.11–3.14 matrix.

The completed Task 10 contract routes learning through canonical
`snapshot-input` and immutable `snapshot` publication under
`$WORKFLOW_OBSERVATORY_HOME/learning/snapshots/`. Schema v1 remains readable;
Episode v2 remains an explicit opt-in for attributable sanitized measurements
and an applicable workflow generation. All result-affecting policies and code
identities are closed into the snapshot, and source tests use only fake roots.
Candidate evidence remains observational, unranked, and non-authoritative.

The following remain outside this milestone and require later approved designs:

- Evolution Proposal schema and experiment lifecycle;
- execution of approval gates, workflow changes, branches, or pull requests;
- post-hoc evaluation artifacts and immutable health-event history/streams;
- background scheduling, network transport, and automatic live-data analysis;
- causal or cross-runtime claims without an explicit comparability policy.

Any future Evolution Proposal must reference `snapshot_id + candidate_id` so
its evidence remains bound to one immutable Learning Snapshot. A bounded live
data baseline is a separate approval gate after the fake-store acceptance suite.

See the approved
[v0.2 design](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md)
and
[amended implementation plan](https://github.com/chouchunming/workflow-observatory/blob/53780705ec878af9ad6cde14358121f8ebcb1205/docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md).

## Workflow Observatory v0.3 Phase 1 checkpoint

Phase 1 is an unreleased implementation checkpoint, not a v0.3 release or
publication.
The commit-pinned implementation baseline is
`53d45af5344dc5fc231723802dad70fa5a0b564a`.
It implements the explicit artifact schema registry and policies, pure derived
migrations, the exact invalidation v2 writer with legacy reads, Snapshot Input
and Learning Snapshot v2 zero-sampling semantics, v1/v2 Learning Snapshot
readback and publication dispatch, and the fixed 12-case acceptance matrix.

Phase 1 preserves existing observation v1, invalidation v1, and Learning
Snapshot v1/v2 artifact bytes byte-for-byte. Readback and pure derived
migrations never rewrite those artifacts in place.
Zero-sampling fields are schema semantics only: they state that no record/sample
selection policy ran; Phase 1 does not select, retain, drop, or sample records.

The checkpoint does not implement the cooperative lock/CAS/maintenance
transaction, a durable health-event sink/store/reporting path,
retention/export/delete/restore/purge operations, observation v3 or sampling
decisions, or a Windows lock backend.

- macOS runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Linux native runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Windows backend/runtime verification: not implemented or run; Windows support is not certified.

Linux certification completed on native Linux aarch64 with the unchanged
12-case CPython 3.11–3.14 matrix, the complete plugin suite, and two
byte-identical exact-inventory package builds. The exact commands and continuing
evidence contract remain in the
[Phase 1 verification boundary](README.md#phase-1-verification-boundary) and the
[fixed 12-case test](plugins/workflow-observer/tests/test_schema_migration_acceptance.py).

## Next design unit — same-machine writer safety

The next design unit is the Phase 2 cross-platform same-machine writer-safety
plan. Phase 1 does not authorize Phase 2 code. That design must cover
per-resource advisory locks, content-hash compare-and-swap, bounded lock
timeouts, stale-owner handling, a maintenance lease for cross-file operations,
and the Windows lock backend and verification boundary.

Later operability phases must separately design the durable health-event
sink/store/reporting path; retention/export/delete/restore/purge operations;
and observation v3 sampling decisions.

Roadmap and backlog items describe future design work; they are not
implementation authority.

## Later — learning and ecosystem expansion

- Define `episode-projection@3` with bounded runtime provenance, an explicit
  compatibility policy, and reviewed heterogeneous-runtime analysis before
  making any cross-runtime exclusion, candidate, or comparison claim. Require
  separate approval of the Episode producer contract, projection policy, Task
  3 and Task 6 boundaries, metric and comparability policies, fixtures and
  acceptance coverage, and mixed v2/v3 behavior.
- Add reviewed human-readable narrative and annotation artifacts over Learning
  Snapshots only after their independent identity and provenance contract is
  approved.
- Add post-hoc evaluator inputs only after evaluator identity, `run_id` join,
  duplicate/conflict, and metric-denominator rules are designed.
- Add approval-gated improvement experiments with explicit hypotheses,
  rollback criteria, and before/after evidence.
- Evaluate W3C Trace Context-compatible opaque parent identifiers for
  cross-process integrations without putting sensitive data into baggage.
- Study useful ideas from OpenTelemetry-oriented skills: progressive
  disclosure, cardinality budgets, sampling trade-offs, durable queues,
  collector health, and production-safe configuration tests.
- Explore optional adapters and export formats without changing the local-only,
  content-minimized default or enabling background network transport.

## Release principles

- No automatic workflow mutation; user approval remains mandatory.
- No full prompt, transcript, credential, or secret capture.
- No partial evaluator shard or unreferenced result generation is authoritative.
- A portable installation must not depend on the original author's paths,
  credentials, LLM Wiki, or observation history.
