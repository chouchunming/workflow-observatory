# Workflow Observatory Marketplace Design

Date: 2026-07-15
Status: Approved, including the Task 6 hybrid-evaluator amendment

## Purpose

Package the current observation workflow as a shareable Codex marketplace that records privacy-minimized workflow evidence, learns from sufficiently large samples, and proposes user-approved improvements. Installation must not depend on the author's LLM Wiki path or send observation data off-device.

The marketplace is named `workflow-observatory` with display name **Workflow Observatory**. Its first plugin is `workflow-observer`. Future functionality extends the marketplace through additional plugins only when a capability no longer belongs in the observer plugin.

## Distribution structure

```text
workflow-observatory/
├── .agents/plugins/marketplace.json
├── README.md
└── plugins/
    └── workflow-observer/
        ├── .codex-plugin/plugin.json
        ├── README.md
        ├── skills/
        │   ├── workflow-observer/SKILL.md
        │   ├── workflow-telemetry/SKILL.md
        │   ├── workflow-learning/SKILL.md
        │   └── workflow-improving/SKILL.md
        ├── scripts/
        ├── tests/
        └── docs/
```

`marketplace.json` uses local source `./plugins/workflow-observer`, category `Productivity`, installation policy `AVAILABLE`, and authentication policy `ON_INSTALL`. The plugin manifest and outer directory are both named `workflow-observer`.

## Skill boundaries

### workflow-observer

This is the only automatic entry point. It decides eligibility, creates one parent lifecycle per top-level authorized task, routes every eligible record through `workflow-telemetry`, propagates the parent marker to workers, and discloses recording failures. It retains the smallest non-negotiable contract: one legal start, at most one legal finish, no payload-bearing probes, no help or draft inspection after start, and no child observations.

### workflow-telemetry

This owns schemas, legal enumerations, privacy sanitization, secure payload handling, lifecycle transitions, storage adapters, and adapter conformance rules. The observer must read this skill before constructing the first real command. Telemetry records evidence; it never interprets workflow quality.

### workflow-learning

This reads finalized records and produces descriptive aggregates or recurring-pattern hypotheses. It requires at least five comparable final records before presenting a trend. Smaller groups are labeled `small sample`; drafts and invalidated records are excluded from rate calculations. It does not change skills or recommend a winning workflow.

### workflow-improving

This converts learning output into evidence-linked proposals or bounded experiments. Every proposal cites the observation groups that motivated it and states uncertainty. It cannot edit a skill, start an experiment, publish data, or change a workflow without explicit user approval.

## Data flow

```text
authorized task
    → workflow-observer
    → workflow-telemetry
    → selected local store adapter
    → workflow-learning (on demand or scheduled review)
    → workflow-improving (on explicit request)
    → user approval before mutation
```

Normal task overhead includes only observer and telemetry. Learning and improving do not auto-run per task.

## Storage adapters

The default portable adapter stores records under `~/.codex/workflow-observatory/`. Its configuration is local to that directory and is not packaged into shared archives. An optional LLM Wiki adapter points to an explicitly configured Wiki root and preserves the current observation schema and CLI behavior.

Both adapters implement the same operations: start, finish, validate, list/report, and integrity check. They must pass one shared conformance suite. Adapter selection is explicit. If the selected adapter fails, the system does not silently fall back to another store because that would split history or create duplicates; the authorized task continues and the final response discloses the recording failure.

## Privacy and security

- Local-only by default; no network transport is required.
- Never store full prompts, transcripts, credentials, secrets, or unnecessary personal data.
- Observation records omit subject absolute paths; local configuration may contain the selected store path.
- Scope and completion payloads use unique mode-0600 temporary files and are deleted in cleanup.
- Records use validated fixed schemas, a 64 KiB payload limit, a 200-Unicode-code-point scalar limit, legal enums, atomic writes, and exclusive lifecycle transitions. Telemetry exposes these exact limits at payload construction time.
- Learning and improving consume sanitized records only.

## Error behavior

Observation failure never expands the original task's authority and does not block authorized implementation. A rejected start creates no run. A rejected finish leaves the draft unchanged. A later legal retry does not erase failed-call evidence from evaluation. Marketplace installation, adapter initialization, and analysis failure are reported separately from task success.

## Multi-writer limitation

The first release protects observation lifecycle transitions but does not make the entire LLM Wiki a multi-writer transactional store. Multiple sessions may safely read concurrently and may usually write unrelated records, but concurrent edits to the same concept/task/inbox record, generated singleton pages such as `_todo_list.md` or `_sources.md`, append-only logs, and multi-file migrations can still cause lost updates, stale derived pages, interleaved logs, or invalidated verification.

The first release therefore documents one-writer rules for shared pages and maintenance operations, preserves production-fingerprint invalidation in evaluation, and records concurrency hardening as a separate open loop. Full hardening requires per-resource advisory locks, content-hash compare-and-swap before atomic replacement, a maintenance lease for cross-file operations, lock timeout/stale-owner policy, and telemetry health events for contention and conflicts. Local `flock` coordinates processes on one machine; it is not a distributed lock across separate devices synchronized by Dropbox or another file-sync service.

## Migration and compatibility

The existing `observing-workflows` behavior becomes `workflow-observer` plus `workflow-telemetry`. Current LLM Wiki records remain valid and are not moved. The LLM Wiki adapter calls the existing CLI through configuration rather than a hard-coded author path. Installation documentation includes an explicit migration check that compares the old and new skill contract before disabling the old global skill, preventing double triggering.

## Frozen evaluator and hybrid transport

Task 6 evaluates the marketplace against the existing ordered 20-case decision manifest and 8-case lifecycle manifest. Their bytes, prompts, expected values, and order remain frozen. The evaluator rejects either input unless its SHA-256 is exactly:

- decision: `f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`
- lifecycle: `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`

The hybrid design responds to a reproduced app-server boundary failure: after a valid implementation, independent reviewer approval, and passing fixture tests, app-server emitted an in-progress custom `exec` tool call but never emitted the corresponding bidirectional tool request or a terminal turn event. A harness request handler cannot repair a request that never arrives. The supported non-interactive client already owns that tool loop, so it becomes the lower-risk path for cases that do not require steering.

Transport selection is derived from each frozen case's `turns` length rather than stored in a second routing manifest:

- The 24 one-turn cases run through the supported non-interactive `codex exec --json --ephemeral` interface. This path owns subprocess startup, JSONL event collection, a fixed 20-minute wall-clock turn timeout, termination and cleanup, and final-output capture. The 20-minute value is a bounded budget selected after a delegated-review case exceeded the prior 15-minute budget; it is not a guarantee that every run will finish. Reaching it still terminates the case and prevents result publication. The app-server transport retains its separate 10-minute turn timeout. The exec path uses the same model and reasoning settings, isolated fixture, case-local store, audited workflow CLI, disabled external skills, environment overrides, approval policy, sandbox, and writable roots as the existing evaluator. It does not add a Python SDK or another runtime dependency.
- The four two-turn cases continue through app-server because they require an in-flight checkpoint and `turn/steer`: forward `late-trigger` and `scope-supersession`, plus lifecycle `late-success` and `scope-supersession`. Assigning any multi-turn case to the exec transport fails before model execution. An unsupported turn count also fails manifest validation.

Both transports adapt their protocol-specific messages into one evaluator-owned case result containing terminal status, final agent text, command-execution events, observation-command diagnostics, and any required record checkpoints. Case validation, the audited invocation ledger, payload cleanup, configured-store integrity, expected decisions and lifecycles, failure-disclosure checks, and result schemas operate only on this common representation. A transport change therefore cannot weaken or silently skip a frozen assertion.

The exec adapter fails closed on a nonzero process exit, malformed non-JSON stdout, a missing terminal event or final agent message, an incomplete command execution, timeout, or protocol/schema mismatch. The app-server adapter retains fail-fast rejection of every unsupported server-initiated request and accepts only responses and notifications that the harness understands. Neither adapter grants an approval. Timeout and process-exit diagnostics contain only bounded counts, lengths, event names, process state, and SHA-256 summaries; they never reproduce raw prompts, commands, paths, stderr, tool arguments, or results.

Every diagnostic, discovery-sweep, and formal case remains inside the production-fingerprint guard, including cleanup and configured-store verification. The guard preserves both a case failure and a concurrent production mutation. Before the formal run, one explicitly selected `reviewed-refactor` exec diagnostic must pass without invoking paired-result persistence; this is transport evidence only and never counts as a frozen formal result.

After deterministic review and the retained diagnostic, a non-authoritative discovery sweep runs all 20 decision cases and all 8 lifecycle cases in frozen order. Ordinary isolated case failures are sanitized, retained, and do not stop later attempts. Manifest mutation, production-fingerprint change, configured-store integrity failure, incomplete runtime setup, payload/output cleanup failure, or transport cleanup failure aborts immediately. The implementation is frozen throughout a sweep, fixes are batched only after all 28 attempts, and the sweep never invokes paired-result persistence or publishes a score. Its retained report is explicitly `authoritative: false`.

After the discovery evidence is reviewed and any batched remediation passes deterministic review, the formal evaluator runs all 20 decision cases and then all 8 lifecycle cases in frozen order. A failure in either transport terminates this formal ordered suite and cannot publish results. Only a completely successful run may write the two immutable, content-addressed result generations and atomically replace the single authoritative commit manifest. Generation hashing, schema and ID-set revalidation, directory-fd and no-symlink protections, crash consistency, and final all-store integrity checks are unchanged by the hybrid transport. No fixed result file, partial generation, diagnostic or sweep output, or unreferenced generation is authoritative.

## July 19 release-acceptance amendment

For the 0.1.0 release only, the user explicitly accepted composite Task 6
evidence: 26 consecutive protected formal passes plus targeted passes for
`complete-eval-override` and `incomplete-eval-override` after the former's
fixture-isolation repair. The deterministic suites and independent review
passed after that repair, and both frozen manifest hashes remained unchanged.

This release decision authorizes Tasks 7–8 but does not alter the evaluator's
authoritative-result semantics. No uninterrupted 28-case result pair or commit
pointer was produced, and the composite evidence must not be described as an
atomic formal 28/28 run. Future authoritative publication still requires one
complete aggregate satisfying the preceding paragraph.

## Verification

- Validate the marketplace and plugin manifests with the official plugin validator.
- Validate all four skills with the official skill validator.
- Run privacy, schema, secure-payload, lifecycle, atomicity, and adapter conformance tests.
- Test the transport-neutral result contract, exec JSONL failure modes and timeout cleanup, app-server request rejection and steer behavior, and fail-closed routing before a model run.
- Run a non-persisting `reviewed-refactor` exec diagnostic, obtain independent review, run and review the non-authoritative 20+8 discovery sweep, then run the frozen formal trigger and lifecycle evaluations against isolated fixtures and stores.
- Verify one-start/one-finish call counts and production-store immutability.
- Build a versioned zip containing the concept, every approved Observation Records and Workflow Observatory design spec and superseding plan, all runtime/packaging scripts, all unit/integration/CLI/adapter/security/packaging and frozen skill-eval tests, manifests, skills, documentation, and a SHA-256 completeness inventory. The inventory maps every repository evidence path to its archive member. Fixtures with author-specific absolute paths receive deterministic portable archive copies rather than being omitted.
- Extract the archive into a clean temporary directory and repeat manifest, skill, test, and inventory validation.

## Non-goals for the first release

- Hosted telemetry, accounts, synchronization, or remote analytics.
- Automatic workflow changes or autonomous experiments.
- Cross-user benchmarking.
- Empty placeholder plugins beyond `workflow-observer`.
- Rewriting or relocating existing LLM Wiki observations.
- Whole-Wiki multi-writer transactions or cross-device distributed locking.

## Acceptance criteria

The first release is complete when another Codex user can install the marketplace, record an eligible task into the portable local store without author-specific paths, select the LLM Wiki adapter explicitly if desired, validate and report records, uninstall without deleting their data, and verify the distributed archive from its included inventory and tests.
