# Workflow Observatory Roadmap

This roadmap contains only Workflow Observatory product work. Personal LLM Wiki
tasks and production observations are intentionally excluded.

## Now — 0.1.x release hardening

- Keep the completed deterministic archive and clean-room gate passing on
  Python 3.11+ and current Codex for every 0.1.x release.
- Publish the public MIT-licensed marketplace with its specifications, plans,
  complete test evidence, and SHA-256 inventory.
- Add a sharded evaluator coordinator so independent frozen cases can run in
  parallel without making partial worker output authoritative.
- Preserve one final aggregate gate that validates all 20 forward and 8
  lifecycle case results, exact frozen manifest hashes, store integrity,
  payload cleanup, and production isolation.

The proposed worker layout, isolation model, retry policy, and RED/GREEN steps
are documented in
[`docs/parallel-evaluation-plan.md`](docs/parallel-evaluation-plan.md).

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

- Add reviewed telemetry-to-learning summaries once at least five comparable
  finalized records exist; keep observation facts separate from quality scores.
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
