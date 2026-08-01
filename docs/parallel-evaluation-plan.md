# Parallel Evaluation Acceleration Plan

Status: Implemented and deterministically verified; model-bearing rollout is
pending independent review and explicit approval.

## Goal

Reduce the wall-clock time of the frozen 20 forward plus 8 lifecycle real-model
evaluation without weakening manifests, privacy, cleanup, production isolation,
or the atomic authoritative result boundary.

The July 19 release acceptance is historical composite evidence: 26 consecutive
formal passes followed by targeted passes for `complete-eval-override` and
`incomplete-eval-override`. It produced no authoritative atomic result pair and
must not be relabeled as a single-run 28/28 result.

## Constraints discovered during review

- All 24 one-turn cases are semantically independent and can run in separate
  worker processes.
- Four cases require app-server `turn/steer` and remain serial:
  forward `late-trigger`, forward `scope-supersession`, lifecycle
  `late-success`, and lifecycle `scope-supersession`.
- Threads in one evaluator process are unsafe because the harness has a
  process-global gate registry, forward and lifecycle share a
  `scope-supersession` ID, and the marketplace runtime factory has an ordered
  mutable runtime cursor.
- Worker artifacts, progress, caches, reports, and Codex state must stay outside
  the repository because the production fingerprint intentionally covers all
  Git-visible files and observation records.
- Workers can never call paired-result persistence. Only one coordinator may
  aggregate and atomically publish a complete result pair.
- The earlier 15-minute exec-timeout drift is resolved: approved spec, plan,
  implementation, and tests now use the same 20-minute fail-closed bound.

## Four-lane layout

Each lane preserves relative frozen order. The coordinator restores the full
forward-then-lifecycle aggregate order.

| Lane | Frozen cases |
|---|---|
| Exec E1 | F1 `multi-file-feature`; F5 `wiki-compile`; F11 `chat`; F14 `plan-only`; F16 `single-file-copy`; F19 `worker-with-parent-marker`; L1 `planned-success`; L6 `central-cli-unavailable` |
| Exec E2 | F2 `tested-bugfix`; F4 `multi-file-docs`; F6 `durable-query`; F10 `parent-managed-subagent`; F15 `single-file-typo`; F17 `status-question`; L5 `task-failure`; L8 `incomplete-eval-override` |
| Exec E3 | F3 `reviewed-refactor`; F7 `inbox-processing`; F12 `read-only-search`; F13 `answer-only`; F18 `review-only`; F20 `ambiguous-default-no-trigger`; L4 `parent-managed-subagent`; L7 `complete-eval-override` |
| Serial app-server | F8 `late-trigger`; F9 `scope-supersession`; L2 `late-success`; L3 `scope-supersession` |

This mapping is run-local coordinator metadata, not a third frozen manifest.
Transport selection continues to derive only from each case's turn count.

## Isolation model

The coordinator creates one mode-0700 evaluation root outside the repository:

```text
run-root/
  captured-input/
  coordinator/
  workers/E1|E2|E3/
  app-server/
  cases/<mode>-<ordinal>-<id>/
```

Before launch, it captures and hashes one immutable marketplace/evaluator
snapshot plus both frozen manifests. Every case receives unique roots for:

- fixture workspace;
- configured observation store and `WORKFLOW_OBSERVATORY_HOME`;
- audit and payload files;
- `HOME`, `CODEX_HOME`, `TMPDIR`, config, and cache state;
- sealed result/evidence output.

Workers run as separate processes with `PYTHONDONTWRITEBYTECODE=1`. Both exec
and app-server receive the same isolated environment. Transports run in their
own process groups; termination must stop, wait, kill if necessary, re-wait,
join readers, and prove that no recorded child PID remains alive.

Recommended caps are three concurrent exec root cases, one serial app-server
case, and at most one reviewer/delegation-heavy case at a time. Keep bounded
diagnostics and a recommended 512 MiB case-root quota. A cost ceiling may stop
new launches but may not truncate an in-flight case and call it valid.

## Production guard

One coordinator owns a single production baseline. It rechecks that exact
baseline:

1. before workers launch;
2. after every case completion or failure notification;
3. before and after cancellation cleanup;
4. before aggregation;
5. immediately before persistence.

Do not add broad exclusions. A maintenance/result lock keyed by canonical
repository path may coordinate evaluators, but it does not replace fingerprint
verification. After persistence, only the exact named generation files and
commit pointer with expected hashes may differ from the baseline.

## Sealed case and shard evidence

Each successful case writes, using temp-file + fsync + atomic replace:

1. `case-result.json`, exactly matching the existing forward/lifecycle result
   row schema;
2. `case-evidence.json`, containing only bounded IDs, hashes, route, attempt,
   timing, store counts, audit counts, cleanup state, and sanitized failure
   classification;
3. `case-commit.json`, pointing to both files and hashes.

Do not include prompts, final agent text, raw commands/tool output, credentials,
payload contents, or absolute subject paths. A worker writes a sealed
`shard-commit.json` only after every assigned case has a terminal result.

The coordinator rejects missing, duplicate, partial, extra, stale-config,
stale-code, stale-manifest, symlinked, or hash-mismatched artifacts. It joins all
workers, reopens every case store, reruns configured integrity, requires the
expected records and invalidated count, and repeats payload/output/audit/process
cleanup checks before aggregation.

## Retry and resume policy

- Semantic assertion or model failure invalidates the formal epoch. Never retry
  it inside the same epoch or publish partial shards.
- Cleanup failure, manifest/production mutation, timeout, malformed protocol,
  post-start transport failure, or ambiguous surviving process hard-aborts the
  epoch.
- A pre-model infrastructure failure may receive one bounded retry only when
  typed evidence proves `model_started: false`, cleanup passed, and every
  manifest/config/code/production fingerprint is unchanged.
- After coordinator crash, reuse only sealed successful cases from the exact
  same epoch. A started case without a terminal commit invalidates the epoch;
  only cases proven never started may resume.
- Formal acceptance requires exactly one model-started attempt per case.

## Serial authoritative aggregation gate

After all workers stop, one non-model coordinator gate must:

1. revalidate frozen bytes, hashes, schemas, IDs, and order;
2. revalidate evaluator, marketplace, model/reasoning config, and captured
   snapshot fingerprints;
3. verify exact 24 exec and 4 app-server routing;
4. require 28 unique sealed cases with no extras;
5. rerun every configured-store integrity check;
6. verify payload, output, audit, Codex-state, and child-process cleanup;
7. reconstruct the ordered 20+8 result pair and validate it;
8. verify production unchanged;
9. call the existing atomic paired-result commit exactly once;
10. resolve, rehash, and rescore the committed pointer before accepting only the
    exact computed production delta.

No serial model rerun is needed. The serial aggregation gate is the only
authoritative boundary; shards are never authoritative independently.

## Marketplace CLI and authority

The Marketplace runner exposes an opt-in
`--parallel {diagnostic,discovery,formal}` mode and an optional
`--resume-run-root` for the exact retained run root. With `--parallel` absent,
the existing preflight, fixed single-case diagnostic, serial discovery sweep,
and default serial formal paths keep their prior behavior and serial writer
authority.

Every parallel invocation requires both
`--archive /absolute/path/to/original-release.zip` and
`--expected-archive-sha256 <64-lowercase-hex>` from an externally trusted
release descriptor or independent channel. The archive path must identify an
absolute, regular, non-symlink file outside the extracted release; the release
does not embed a recursive archive copy. This applies equally to diagnostic,
discovery, and formal: authority differs by mode, but each mode materializes and
executes archive-owned code. Before any archive verifier, ZIP inspection, or
snapshot extraction, the coordinator hashes the raw complete archive and
compares it with the trusted value. The epoch seals separate full expected and
observed fields, and live coordinator/protocol sources must match the trusted
archive identities before planning or worker launch.

Production worker processes use Python isolated mode with only the captured
snapshot's `evidence/` root inserted for evaluator imports. Each worker receives
the sealed evaluator identity at spawn and recomputes the exact component
digest from the read-only snapshot before its first `lane-ready` publication.

The CLI passes the two frozen manifests and a sealed `RunKind` to the reviewed
production coordinator. Diagnostic and discovery pass no result destinations
and never acquire or claim result-writer authority. Discovery remains
non-authoritative even after complete validation. Formal uses the coordinator's
ordered per-run then repository writer leases; only the validated formal
capability can be claimed and consumed for one paired-result publication.

The deterministic no-model integration gate starts one coordinator and four
real worker processes, seals all 28 cases in 8/8/8/4 lanes, and proves zero
sentinel Codex invocations and zero discovery writer calls. It does not establish
a real-model discovery or formal 28/28 result.

## Fixed single-case parallel diagnostic contract

`--parallel diagnostic` is a non-authoritative rollout gate, not a shortened
epoch or a user-selectable case runner. It must execute exactly
`forward/3 reviewed-refactor`, whose frozen lane is E3. The ordinal `3` names
one frozen case; it does not mean three cases.

The coordinator must still load both complete frozen manifests and build the
same full 28-assignment diagnostic `EpochPlan`. Manifest bytes, frozen lane
mapping, input fingerprints, and epoch identity therefore remain unchanged.
Filtering a manifest before plan construction is forbidden because it would
create a different inventory and weaken the frozen-plan checks.

The target is a closed production constant, not a `ParallelOptions` field and
not a CLI selector. Before launching a diagnostic worker, the coordinator
derives the unique matching `CaseAssignment` from the full plan and atomically
writes or verifies:

```json
{
  "schema_version": 1,
  "epoch_id": "<full diagnostic epoch ID>",
  "run_kind": "diagnostic",
  "target": {
    "mode": "forward",
    "ordinal": 3,
    "case_id": "reviewed-refactor"
  },
  "lane": "E3"
}
```

This exact closed record lives at
`coordinator/diagnostic-scope.json`. A missing target, duplicate target,
different lane, extra field, non-canonical encoding, unsafe path, or changed
record fails closed before model launch. Discovery and formal runs reject a
diagnostic scope record. Because there is only one legal diagnostic target,
the existing full-plan epoch identity remains sufficient; a resume cannot
select a different case under the same epoch.

For diagnostic runs, the coordinator launches only the E3 worker and supervises
exactly its registered process. Discovery and formal retain the exact four-lane
launch requirement. Worker recovery and process-group cleanup continue to
iterate over the closed four-lane namespace so an unexpected retained process
in any lane is still detected and terminated.

The diagnostic worker reads and validates the sealed scope against its full
plan before case execution. The E3 worker filters the full pending/reusable resume
classification to the one target without relabeling the other 27 assignments
as successful, reusable, invalid, or skipped. The existing `ResumePlan` schema
does not change. A resumed diagnostic may reuse only the target's valid sealed
case evidence; absent non-target cases remain absent and cannot affect the
diagnostic outcome.

A successful diagnostic retains the target's attempt and case seals plus its
cleanup evidence, then reports `status: diagnostic`. It does not write a
partial shard seal, enter 28-case aggregation, construct `ValidatedEpoch`,
acquire result-writer authority, claim a commit capability, or update frozen
result files. A target failure returns `status: failed` after the existing
fail-closed cancellation and cleanup path. The retained run root remains the
operator-visible recovery and review artifact.

The Marketplace CLI keeps all existing modes. Legacy
`--diagnostic-case reviewed-refactor` remains the existing serial one-case
path. New `--parallel diagnostic` always uses the sealed production constant;
no arbitrary parallel case selector is exposed.

Implementation acceptance requires test-first proof that:

1. the full 28-case diagnostic plan and epoch fingerprints are preserved;
2. a fresh diagnostic writes the exact scope and launches only E3;
3. only `forward/3 reviewed-refactor` reaches the worker case driver;
4. success and failure cannot create or persist a partial authoritative result;
5. resume reuses only the same target and rejects missing or tampered scope;
6. discovery and formal still launch E1, E2, E3, and APP and reject diagnostic
   scope state;
7. process cleanup, production guards, frozen hashes, boundary checks, clean
   archive suites, reproducible packaging, and official validators remain
   green.

The deterministic tests must use sentinel/no-model runtimes. Real-model
diagnostic execution remains a separate explicit rollout step after code review
and deterministic acceptance.

## Expected acceleration and cost

- Sequential timeout bound at the implemented 20-minute exec and 10-minute
  app-server limits: 520 minutes.
- Three balanced exec shards overlapped with the serial app-server lane: about
  160 minutes worst-case plus 10–20 minutes aggregation.
- Against observed 2–3 hour sequential runs, the practical target is 50–80
  minutes, depending on reviewer-heavy cases and service rate limits.
- The 28 model tasks remain unchanged. Coordinator/worker bootstrap is expected
  to add roughly 3–10% tokens while substantially reducing wall-clock time.

## RED/GREEN implementation record

Create focused modules rather than expanding the current evaluator file:

- `scripts/workflow_eval_sharding.py`
- `scripts/run_observing_workflows_eval_worker.py`
- `tests/test_workflow_eval_sharding.py`
- plugin-local `tests/test_parallel_eval_runner.py`

Required RED tests include exact case coverage, stable out-of-order aggregation,
worker persistence prohibition, missing/duplicate/partial/stale shard rejection,
same-ID mode isolation, typed retry policy, ambiguous-resume rejection,
process-group cleanup, post-failure all-store integrity, coordinator lock
exclusivity, crash consistency, non-authoritative discovery shards, and exact
post-commit production delta.

Implementation followed this order: schema/planner, worker isolation, case
sealing, coordinator cancellation/guards, aggregation, resume policy,
Marketplace CLI, deterministic suites, and official validators. The
model-bearing worker-path diagnostic, parallel discovery sweep, independent
review, and protected parallel formal epoch remain separate approval-gated
rollout steps.
