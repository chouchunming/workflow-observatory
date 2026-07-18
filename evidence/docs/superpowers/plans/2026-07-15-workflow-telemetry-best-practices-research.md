# Workflow Telemetry Best-Practices Research and Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to execute this plan task-by-task. The user's `$orchestrator` invocation authorizes subagents only within the cost-aware limits below.

**Status:** Research and independent review complete; two immutable raw sources delivered. Wiki ingest and integration are deferred to the separate user-designated session.

**Goal:** Research mature workflow-telemetry practices, turn the evidence into explicit Workflow Observatory v1 architecture decisions, and integrate the accepted decisions without conflicting with the active observation session.

**Architecture:** Keep research artifacts outside `wiki/` and separate operational telemetry, workflow-learning records, and audit/integrity records as distinct data purposes. The root orchestrator performs batched source research and owns all architecture decisions; subagents are reserved for one narrowly bounded evidence probe already completed and one no-Web final review. During research, stage working material under `/private/tmp/workflow-telemetry-research/`; after the research review passes, write two immutable, descriptively named `raw/` evidence inputs exactly once. A different user-designated session owns all later triage, wiki compilation, canonical-document integration, generated-page rebuilds, and lint.

**Tech Stack:** Official web standards and product documentation, Markdown evidence ledgers, `wiki_cli.py`, and read-only Git inspection.

## Global Constraints

- Do not modify `wiki/`, `marketplace/workflow-observatory/`, the active marketplace design spec, or the active marketplace implementation plan during the research phase.
- Do not finish or invalidate observation run `obs-20260715-194320-497362`; only its owning session may perform its terminal transition.
- Do not treat an observation lock file as session ownership. The lock protects individual state transitions, not the full lifespan of a session.
- Do not run `python3 wiki_cli.py fileback` during the research phase. It creates a page, appends `wiki/_queries.md` and `wiki/z_log.md`, and rebuilds `wiki/_sources.md`.
- Do not run the writing forms of `python3 wiki_cli.py tasks`, `sources`, or `inbox` until the integration window.
- The user explicitly authorizes final raw-only delivery before a later ingest session. Do not run `pending`, source-catalog generation, triage, compilation, or lint after those raw writes in this session.
- Treat the staged `/private/tmp` artifacts as mutable working files and the eventual `raw/` copies as immutable evidence. Complete source-link, date, and internal-consistency checks before the one-time raw writes.
- Preserve all unrelated dirty-worktree changes. Re-read every integration target immediately before editing it.
- Do not commit, publish, push, install, or export without separate user authorization.
- Use direct official sources for technical claims. Label experimental conventions and vendor-specific practices instead of presenting them as stable standards.

## Cost-Aware Orchestration Strategy

Measured on 2026-07-15 in the current Codex environment:

| Probe | Runtime model / effort | Input | Cached input | Output | Total |
|---|---|---:|---:|---:|---:|
| Fixed-string baseline | `gpt-5.6-sol` / `xhigh` | 19,916 | 9,984 | 8 | 19,924 |
| Five-claim OTel/W3C research | `gpt-5.6-sol` / `xhigh` | 285,132 | 223,488 | 3,374 | 288,506 |
| Six-artifact no-Web review | `gpt-5.6-sol` / `xhigh` | 272,313 | 237,568 | 3,105 | 275,418 |

- Treat approximately 20,000 total tokens as the observed fixed cost of spawning even a trivial subagent in this environment.
- Do not fan out broad research tracks. The root orchestrator performs Web research with batched queries and carries findings directly into the evidence ledger.
- Treat the completed OTel/W3C worker result as unverified input until the root reopens and checks every cited official source.
- Do not spawn another Web-research subagent for Tasks 1–6. A projected three-worker run at the observed research cost would be approximately 865,518 total tokens before root integration and review.
- The single Task 6 review is complete. Do not spawn another subagent in this research run. It showed that a no-Web/read-only brief can still cost almost as much as Web research when an xhigh agent reads several artifacts over multiple cycles.
- For future independent review, consolidate the evidence and decisions into one bounded packet before spawning, request one single-pass response, and time-box immediately. Do not infer low cost from a no-Web constraint.
- After every subagent, read the corresponding local session `token_count` event and append model, effort, input, cached input, output, reasoning output, total tokens, and response-cycle count to `/private/tmp/workflow-telemetry-research/token-usage.md`.
- The current collaboration API does not expose a model-selection parameter. Do not claim that a cheaper model or lower reasoning effort was selected unless runtime logs prove it.
- Keep root Web work source-bounded: batch related searches, open only pages that resolve a named decision, and stop at the evidence-saturation rule in Task 1.

## File and Ownership Map

### Research-stage artifacts

- Create: `/private/tmp/workflow-telemetry-research/research-contract.md`
- Create: `/private/tmp/workflow-telemetry-research/evidence-ledger.md`
- Create: `/private/tmp/workflow-telemetry-research/decision-matrix.md`
- Create: `/private/tmp/workflow-telemetry-research/architecture-options.md`
- Create: `/private/tmp/workflow-telemetry-research/lifecycle-validation.md`
- Create: `/private/tmp/workflow-telemetry-research/integration-proposal.md`
- Create: `/private/tmp/workflow-telemetry-research/token-usage.md`

### Raw source deliverables after research verification

- Create once: `raw/Workflow_Telemetry_Best_Practices_Evidence_Ledger_2026-07-15.md`
- Create once: `raw/Workflow_Telemetry_Best_Practices_Research_Report_2026-07-15.md`

### Deferred integration targets owned by the later ingest session

- Modify: `docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md`
- Modify: `docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md`
- Modify: `wiki/tasks/adopt-workflow-telemetry-best-practices.md`
- Modify only if stable reusable principles change: `wiki/concept/Workflow_Observation_and_Process_Knowledge.md`
- Create through the repository-supported fileback workflow: `wiki/summary/Workflow_Telemetry_Best_Practices_Research.md`
- Rebuild once, by the integration owner: `wiki/_queries.md`, `wiki/_sources.md`, `wiki/_todo_list.md`, and any other generated page made stale by the accepted changes

---

### Task 0: Establish the Concurrency Gate

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/research-contract.md`
- Read: `wiki/observations/obs-20260715-194320-497362.md`
- Read: the four canonical integration targets listed above

**Produces:** A written ownership boundary, target-file fingerprints, and the conditions that permit integration.

- [ ] Record the active observation run ID and name its current session as the repository integration owner.
- [ ] Record this research session as read-only with respect to repository integration targets.
- [ ] Record the current content hashes of the design spec, implementation plan, telemetry task, and workflow-observation concept.
- [ ] State that research may proceed in `/private/tmp`, but repository integration requires an explicit handoff or confirmed completion of the active session.
- [ ] Before integration, compare the target hashes. If any target changed, discard the stale integration diff, re-read the latest file, and regenerate the proposal.

**Acceptance:** Research can proceed without adding modifications under `wiki/`, `marketplace/`, or the active design and implementation documents.

### Task 1: Define the Decision Contract

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/research-contract.md`

**Produces:** Exact research questions, options, non-goals, and evidence requirements.

- [ ] Define three data purposes: operational telemetry, workflow-learning records, and audit/integrity records.
- [ ] Define the decisions the research must resolve: canonical record, task/run/session boundary, context propagation, storage model, schema evolution, evaluation attachment, privacy, retention, deletion, export, sampling, and failure semantics.
- [ ] For every decision, list the candidate options and the evidence needed to distinguish them.
- [ ] Record v1 non-goals: no hosted collector requirement, no automatic external export, no default full-prompt or tool-payload capture, no general-purpose APM platform, and no causal claim from early workflow statistics.
- [ ] Define the research stopping rule: all decisions have sufficient evidence, and two consecutive additional representative tools introduce no decision-changing pattern.

**Acceptance:** Every later search query and collected source maps to at least one named architecture decision.

### Task 2: Build the Evidence Ledger

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/evidence-ledger.md`

**Produces:** A reproducible claim-to-source ledger with maturity and applicability labels.

- [ ] Have the root orchestrator perform all remaining Web research using batched queries; do not delegate additional Web-research workers.
- [ ] Reopen and verify the five official sources returned by the completed OTel/W3C probe before accepting any of its claims.
- [ ] Research OpenTelemetry traces, metrics, logs/events, semantic-convention stability, schema evolution, baggage, sampling, and telemetry self-observation from official documentation.
- [ ] Research W3C Trace Context from the normative specification.
- [ ] Research OpenTelemetry GenAI conventions and OpenInference, recording whether each relevant convention is stable or experimental.
- [ ] Select two representative open-source LLM-observability systems and one or two representative commercial agent-tracing systems; use their official schema and privacy documentation.
- [ ] Select one representative workflow-engine model and authoritative process-mining guidance to examine lifecycle, retry, rework, rollback, and case identity.
- [ ] Research privacy, logging, minimization, retention, and re-identification risks from NIST, OWASP, or equivalent primary guidance.
- [ ] Record each claim with source title, direct URL, publisher, document version/status, access date, evidence class, paraphrased support, local-first applicability, and conflicting evidence.
- [ ] Use only these evidence classes: `normative-standard`, `stable-official-guidance`, `experimental-convention`, `cross-tool-practice`, `vendor-specific`, and `local-design-inference`.
- [ ] Require two independent implementations before labeling a product behavior `cross-tool-practice`.

**Acceptance:** No recommendation relies on an unattributed claim, and vendor-specific behavior is not presented as an industry standard.

### Task 3: Create the Decision-First Comparison Matrix

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/decision-matrix.md`

**Produces:** One disposition for each architecture decision, with traceable rationale.

- [ ] For each decision, compare options by evidence maturity, local-first fit, privacy impact, implementation cost, interoperability, and workflow-learning value.
- [ ] Assign exactly one disposition: `adopt-v1`, `preserve-compatibility`, `experiment-first`, `reject`, or `unresolved`.
- [ ] For every proposed v1 field, identify its consumer, signal type, expected cardinality, privacy class, retention class, missing-value meaning, and whether it can be derived.
- [ ] Remove fields with no identifiable consumer or decision value.
- [ ] Separate facts captured during execution from human or automated evaluation scores attached afterward.
- [ ] Separate diagnostic usefulness from evidence that a workflow is effective.

**Acceptance:** Every disposition links to named evidence, and every retained v1 field has an explicit consumer and lifecycle.

### Task 4: Compare Architecture Options

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/architecture-options.md`

**Produces:** A recommended architecture plus important rejected alternatives.

- [ ] Evaluate a single durable observation record as the canonical model.
- [ ] Evaluate an OTel-compatible span/event stream as canonical storage with derived workflow summaries.
- [ ] Evaluate a minimal portable event envelope with separate workflow summary, evaluation score, and audit/health records, plus an optional OTel adapter.
- [ ] Compare offline operation, context propagation, crash recovery, multi-writer behavior, privacy, schema migration, export compatibility, learning suitability, and implementation complexity.
- [ ] Do not equate OTel compatibility with requiring the OTel SDK, OTLP, a collector, or a network service.
- [ ] State the recommended option, rejected alternatives, assumptions, and intentional compatibility seams.

**Acceptance:** The recommendation identifies which representation is canonical, which records are derived, and which interoperability mechanisms remain adapters.

### Task 5: Validate Lifecycle Invariants

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/lifecycle-validation.md`

**Produces:** Pass/fail mappings for representative workflow lifecycles.

- [ ] Model a successful single-agent task.
- [ ] Model a failed worker followed by a successful retry without double-counting the top-level task.
- [ ] Model two reviewer-requested rework rounds.
- [ ] Model a late child event after the parent has reached a terminal state.
- [ ] Model observation validation failure while the authorized user task succeeds.
- [ ] Model rollback, cancellation, abandonment, and an interrupted run that never reaches finish.
- [ ] Model duplicate finish and concurrent writers.
- [ ] Model multiple runs grouped into a session.
- [ ] Model operation with content capture disabled.
- [ ] Verify that causality is preserved, incomplete does not imply success, retry is not a new top-level outcome, terminal transitions are idempotent, late events cannot silently rewrite verified results, telemetry health does not overwrite workflow outcome, and the core works without a network or collector.

**Acceptance:** Every scenario has an unambiguous representation and expected result; remaining ambiguity is recorded as an unresolved design decision.

### Task 6: Produce the Integration Proposal

**Files:**

- Create: `/private/tmp/workflow-telemetry-research/integration-proposal.md`

**Produces:** A reviewable proposal that the integration owner can apply without repeating the research.

- [ ] Write an executive summary, scope, and non-goals.
- [ ] Present the claim-evidence table and maturity distinctions.
- [ ] Present the three data-purpose model and recommended architecture.
- [ ] Present the decision matrix and v1 minimal schema.
- [ ] Present lifecycle invariants, privacy policy, retention/delete/export defaults, and telemetry-health behavior.
- [ ] List rejected alternatives and reasons.
- [ ] List open questions that require operational evidence rather than more document research.
- [ ] Provide section-specific proposed changes for the design spec and marketplace implementation plan.
- [ ] Identify which findings belong only in the research summary and which stable principles justify changing the concept page.
- [ ] After the root self-review, dispatch at most one no-Web reviewer to inspect the six staged research artifacts for unsupported claims, maturity-label errors, internal contradictions, missing decision coverage, and lifecycle gaps.
- [ ] Resolve every Critical or Important reviewer finding in the staged artifacts and record the reviewer token usage in `token-usage.md`.
- [ ] Verify the evidence ledger and integration proposal are final and contain no secrets or subject absolute paths; verify the ledger has complete direct-source URLs/access dates and the report points to the exact companion raw path; then materialize both at the named raw paths exactly once.
- [ ] Stop after raw delivery. Leave triage, wiki compilation, generated pages, and repository integration to the later ingest session.

**Acceptance:** A reviewer can accept or reject each proposed repository change independently and trace it to evidence.

### Task 7: Perform the Single-Owner Integration

**Gate:** Deferred to the separate ingest session designated by the user. This research session must not execute Task 7.

**Files:**

- Modify: `docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md`
- Modify: `docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md`
- Modify: `wiki/tasks/adopt-workflow-telemetry-best-practices.md`
- Modify conditionally: `wiki/concept/Workflow_Observation_and_Process_Knowledge.md`
- Create: `wiki/summary/Workflow_Telemetry_Best_Practices_Research.md`

**Produces:** Accepted research decisions incorporated into canonical project documentation and the LLM Wiki.

- [ ] Re-read all targets and compare them with Task 0 fingerprints.
- [ ] If another session changed a target, regenerate the proposed edit against the latest content rather than applying the stale text.
- [ ] The later ingest session records both raw sources in `wiki/_source_triage.md` and chooses the correct triage state before compilation.
- [ ] Update the design spec's data flow, storage, privacy, error behavior, multi-writer limitation, migration, verification, non-goals, and acceptance criteria as supported by accepted decisions.
- [ ] Update the implementation plan tasks affected by portable core/configuration, observer/telemetry contracts, evaluation, migration, and final verification.
- [ ] File back the durable research report through the supported explicit-input workflow only after exclusive ownership of generated pages is established.
- [ ] Update the concept page only with stable reusable principles, not vendor-specific findings or unresolved recommendations.
- [ ] Update the task record with accepted conclusions, remaining experiments, next physical action, and correct status.
- [ ] Have one integration owner rebuild all stale generated pages exactly once.

**Acceptance:** No newer content is overwritten, no unrelated file enters the integration diff, and the research/task/spec/plan tell the same architectural story.

### Task 8: Verify and Hand Off

**Files:**

- Check: all Task 7 integration targets and generated pages

**Produces:** Validation evidence and a concise list of remaining open loops.

- [ ] Run `python3 wiki_cli.py tasks` once as the integration owner.
- [ ] Run `python3 wiki_cli.py sources` once as the integration owner.
- [ ] Run `python3 wiki_cli.py tasks --check`; expect `Todo dashboard is current.`
- [ ] Run `python3 wiki_cli.py sources --check`; expect `Source catalog is current.`
- [ ] Run `python3 wiki_cli.py lint`; confirm no regression caused by this work and report unrelated pre-existing warnings separately.
- [ ] Run `git diff --check`; expect no whitespace errors.
- [ ] Review the path-limited diff for the plan's integration targets and generated pages.
- [ ] Confirm the original observation owner, not the research session, performs any terminal transition for `obs-20260715-194320-497362`.

**Acceptance:** Evidence, decisions, schema requirements, implementation steps, wiki summary, task state, and generated pages are synchronized without disturbing unrelated work.

## Final Acceptance Criteria

- Every v1 recommendation is evidence-linked and maturity-labeled.
- Every v1 field has a consumer, cardinality expectation, privacy class, and retention class.
- Operational telemetry, workflow-learning records, evaluation scores, and audit/health records have explicit boundaries.
- Stable standards are distinguished from experimental conventions and vendor practices.
- The selected architecture handles the lifecycle invariants without relying on free-text interpretation.
- The research session's only repository writes are this uniquely named staging plan and the two verified immutable raw sources; it makes no wiki, marketplace, spec, or active implementation-plan edits.
- The final evidence ledger and research report enter `raw/` only after research review and pre-write verification; they are never used as mutable working documents.
- Repository integration occurs only after ownership handoff and is performed by one designated session.
- Generated singleton pages are rebuilt once, after canonical pages are final.
- No unrelated dirty-worktree changes are modified, staged, committed, or reverted.
