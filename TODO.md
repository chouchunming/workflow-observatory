# Workflow Observatory Todo

This is the public product backlog. It intentionally excludes personal Wiki
tasks, production observations, credentials, and local configuration.

## Stable v0.1.0 release

- [x] Separate automatic routing, telemetry, learning, and improving concerns.
- [x] Add portable and explicit LLM Wiki storage adapters.
- [x] Freeze the 20 forward and 8 lifecycle evaluation manifests.
- [x] Preserve the July 19 composite acceptance without relabeling it as an
  authoritative atomic 28/28 result.
- [x] Package the approved specifications, implementation plans, test suites,
  acceptance report, and SHA-256 completeness inventory.
- [x] Complete clean-room validation of the plugin, four skills, 76
  plugin-local tests, and 268 packaged repository tests.
- [x] Implement the isolated parallel coordinator, production worker boundary,
  ordered leases, sealed evidence, cleanup recovery, and opt-in Marketplace
  CLI; prove deterministic 8/8/8/4 coverage without invoking a model.
- [x] Publish the public MIT repository.
- [x] Publish the stable v0.1.0 release archive.

## Workflow Evolution Foundation v0.2 — completed prerelease checkpoint

Workflow Evolution Foundation v0.2, including Task 11's 15-case matrix and
bounded fake-store historical dry run, is complete.

- [x] Preserve schema-v1 readability while adding optional, attributable
  Episode v2 measurements and explicit workflow generations.
- [x] Implement canonical `snapshot-input`, closed policy identities,
  deterministic unranked candidates, stable A/B acquisition, and immutable
  local `snapshot` publication.
- [x] Route learning through the sanitized snapshot response and require one
  user-selected `snapshot_id` + `candidate_id` pair for candidate inspection,
  without granting proposal or mutation authority.
- [x] Package direct immutable policy JSON files, the JCS fixture, and the exact
  approved v0.2 design and plan in the current source archive inventory.
- [x] Complete Task 11's 15-case acceptance matrix and bounded historical dry
  run against isolated fake portable and fake LLM Wiki roots across the
  supported CPython 3.11–3.14 matrix.

## Workflow Observatory v0.3 Phase 1 checkpoint

Phase 1 is an unreleased implementation checkpoint, not a v0.3 release or
publication.
The commit-pinned implementation baseline is
`53d45af5344dc5fc231723802dad70fa5a0b564a`.

- [x] Implement the explicit artifact schema registry and policies, pure
  derived migrations, the exact invalidation v2 writer with legacy reads,
  Snapshot Input and Learning Snapshot v2 zero-sampling semantics, v1/v2
  Learning Snapshot readback and publication dispatch, and the fixed 12-case
  acceptance matrix.

Phase 1 preserves existing observation v1, invalidation v1, and Learning
Snapshot v1/v2 artifact bytes byte-for-byte. Readback and pure derived
migrations never rewrite those artifacts in place.
Zero-sampling fields are schema semantics only: they state that no record/sample
selection policy ran; Phase 1 does not select, retain, drop, or sample records.

The checkpoint does not implement the cooperative lock/CAS/maintenance
transaction, a durable health-event sink/store/reporting path,
retention/export/delete/restore/purge operations, observation v3 or sampling
decisions, or a Windows lock backend. These capabilities are tracked outside
the Phase 1 completion checklist.

- macOS runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Linux native runtime verification: pending; Linux support is not yet certified.
- Windows backend/runtime verification: not implemented or run; Windows support is not certified.

The next design unit is the Phase 2 cross-platform same-machine writer-safety
plan. Phase 1 does not authorize Phase 2 code.

See the [fixed 12-case test](plugins/workflow-observer/tests/test_schema_migration_acceptance.py)
and the [exact platform commands, pass criteria, and external evidence contract](README.md#phase-1-verification-boundary).

## Pending platform verification

- [ ] In a native Linux environment, run the exact commit-pinned 12-case
  CPython matrix, complete plugin suite, and two exact-inventory package gates;
  record the distribution, kernel, architecture, Python versions, candidate
  commit, implementation baseline, exit statuses, test counts, archive hashes,
  and byte-comparison result outside the package at
  `evidence/dist/phase1-acceptance/linux/<candidate-commit>/`.

## Separate evaluator gate

- [ ] Obtain explicit approval before one protected real-model formal epoch
  after independent review; do not treat the deterministic no-model gate as a
  real-model 28/28 result.

## Backlog and design gates — not implementation authority

Roadmap and backlog items describe future design work; they are not
implementation authority.

- [ ] Add per-resource advisory locks, content-hash compare-and-swap, bounded
  lock timeouts, stale-owner handling, a maintenance lease, and a reviewed
  Windows lock backend.
- [ ] Add retention, export, delete, restore, and purge policy and operations.
- [ ] Define and add immutable health-event history for rejected validation,
  dropped records, duplicate finish attempts, cleanup failure, schema mismatch,
  and lock contention.
- [ ] Add value-based sampling that always retains failures, rework, rollback,
  and rare paths.

## Later

- [ ] Design the Evolution Proposal schema; any future proposal must cite both
  `snapshot_id` and `candidate_id`.
- [ ] Define post-hoc evaluation artifacts and their identity, join, conflict,
  and denominator rules before admitting them as learning input.
- [ ] Add approval-gated workflow experiments with rollback criteria only after
  the proposal and experiment lifecycle is separately approved.
- [ ] Add background scheduling only after its local authority, concurrency,
  and privacy boundaries are designed.
- [ ] Execute formal acceptance and any live-data baseline only behind their
  separate review and approval gates.
- [ ] Evaluate opaque W3C Trace Context-compatible parent identifiers.
- [ ] Evaluate optional OpenTelemetry export adapters while preserving the
  local-only, content-minimized default.

See [ROADMAP.md](ROADMAP.md) for sequencing and release principles.
