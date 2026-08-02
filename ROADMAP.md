# Workflow Observatory Roadmap

This roadmap contains only Workflow Observatory product work. Personal LLM Wiki
tasks and production observations are intentionally excluded.

## Now — 0.1.x release hardening

- Keep the completed deterministic archive and clean-room gate passing on
  Python 3.11+ and current Codex for every 0.1.x release.
- Publish the public MIT-licensed marketplace with its specifications, plans,
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

## Approved next milestone — Workflow Evolution Foundation v0.2

Status: design and implementation plan approved as of 2026-08-02.
Implementation started on 2026-08-02; Tasks 1 through 3 are complete on the
design branch and are not included in the current release.

The bounded foundation turns privacy-minimized observations into reproducible,
non-authoritative learning evidence without changing workflows automatically:

```text
Episode v2
    -> canonical snapshot-input
    -> Data Trust Gate
    -> immutable Learning Snapshot
    -> stable observational candidates
```

Implementation is divided into 11 independently reviewable TDD units:

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
11. Pass the 15-case non-model acceptance matrix across isolated fake stores
    and the supported CPython 3.11–3.14 matrix.

The following remain outside this milestone and require later approved designs:

- Evolution Proposal schema and experiment lifecycle;
- execution of approval gates, workflow changes, branches, or pull requests;
- post-hoc evaluation artifacts and immutable health-event streams;
- background scheduling, network transport, and automatic live-data analysis;
- causal or cross-runtime claims without an explicit comparability policy.

Any future Evolution Proposal must reference `snapshot_id + candidate_id` so
its evidence remains bound to one immutable Learning Snapshot. A bounded live
data baseline is a separate approval gate after the fake-store acceptance suite.

See the approved
[v0.2 design](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md)
and
[implementation plan](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md).

## Next — concurrency and operability

- Add explicit schema versions to all observation, invalidation, and analysis
  records with tested migration rules.
- Define retention, export, and delete policies for the portable store; local
  retention remains the default until the user chooses otherwise.
- Record health counters/events for validation rejection, dropped records,
  duplicate finishes, cleanup failures, schema mismatches, lock contention,
  and compare-and-swap conflicts.
- Add value-based sampling: retain failures, rework, rollback, and rare paths;
  sample only high-volume repeated successes.
- Harden same-machine multi-writer behavior with per-resource advisory locks,
  content-hash compare-and-swap, bounded lock timeouts, stale-owner handling,
  and a maintenance lease for cross-file operations.

## Later — learning and ecosystem expansion

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
