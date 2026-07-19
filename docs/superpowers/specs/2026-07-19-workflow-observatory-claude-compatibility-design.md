# Workflow Observatory Claude Compatibility Design

Date: 2026-07-19

Status: approved architecture; written specification awaiting user review

## Purpose

Add an explicit Claude Code adapter to the public MIT-licensed Workflow
Observatory while preserving its existing Codex behavior, shared local storage
core, privacy boundary, lifecycle rules, and clean-room release evidence.

The first Claude-capable release is an alpha. It may pass deterministic
validators and fixture-driven tests, but it must state publicly:

> Claude support is alpha and has not yet been validated in a real Claude Code
> runtime. If you validate it, find a compatibility issue, or implement a fix,
> please submit a pull request with sanitized reproduction evidence.

This statement remains in the root README, Claude plugin README, release notes,
and GitHub pre-release description until a maintainer records successful real
Claude Code installation and behavioral validation. Passing
`claude plugin validate` alone does not remove the statement.

## Scope

This design covers one self-contained marketplace repository with:

- separate Codex and Claude marketplace/plugin metadata;
- separate Codex and Claude skill adapters;
- a shared Python observation schema, lifecycle, and storage core;
- a narrow Claude `SubagentStart` hook for parent-marker propagation;
- deterministic packaging, native manifest validation, and behavioral fixtures;
- explicit degraded modes and an unverified-alpha release policy.

It does not add remote telemetry, background network transport, automatic
workflow changes, prompt or transcript capture, full retention management,
schema migration machinery, or the broader multi-writer CAS/lease work already
listed on the public roadmap. It does not implement the rejected packaging
alternatives described at the end of this document.

## Binding principles

1. Codex and Claude manifests are different public interfaces. Neither is
   described, tested, or packaged as a substitute for the other.
2. The domain/storage core is shared. Platform detection and runtime behavior
   live in explicit, thin adapters.
3. Observation failure never broadens task authority and never triggers silent
   fallback to another store or another agent-surface label.
4. Full prompts, transcripts, final assistant messages, credentials, secrets,
   subject absolute paths, and unnecessary personal data are never recorded.
5. One stable top-level scope has at most one active lifecycle. Material scope
   replacement starts a replacement before superseding the prior run.
6. Hooks assist propagation; they are never the source of truth for
   eligibility, observation start, observation finish, or task success.
7. Existing Codex behavior is a compatibility contract, not a migration target.
8. The alpha label reports what was actually validated and does not turn
   deterministic fixtures into a claim of real Claude runtime compatibility.

## Architecture

### One physical plugin, two native platform surfaces

The repository continues to ship one `workflow-observer` plugin directory. It
contains native metadata and skills for each platform plus the shared core:

```text
.agents/plugins/marketplace.json                 # Codex marketplace only
.claude-plugin/marketplace.json                  # Claude marketplace only
plugins/workflow-observer/
├── .codex-plugin/plugin.json                    # Codex manifest only
├── .claude-plugin/plugin.json                   # Claude manifest only
├── adapters/
│   ├── codex/skills/<skill>/SKILL.md
│   └── claude/
│       ├── hooks/hooks.json
│       └── skills/<skill>/SKILL.md
├── scripts/                                     # shared core and thin adapters
└── tests/
```

The Codex manifest points only to `adapters/codex/skills/`. The Claude manifest
points only to `adapters/claude/skills/` and its Claude hook configuration.
There is no plugin-root `skills/` directory after migration. This avoids Claude
Code's normal behavior of scanning default skills in addition to custom skill
paths and prevents duplicate or cross-platform skill discovery.

The native marketplace files both identify `workflow-observer` and resolve to
the same self-contained plugin root, but they use their own documented schemas.
Tests validate each file only with its native validator and schema assertions.
No compatibility claim is inferred from their similar names or source paths.

### Shared core

`wiki_observations.py` remains the canonical domain and filesystem core. It
continues to own:

- bounded schemas and fixed enumerations;
- privacy validation and provenance minimization;
- run ID generation and record rendering;
- lifecycle validation, supersession, invalidation, reports, and integrity;
- exclusive start claims and per-run lifecycle locks;
- unique temporary files, regular-file and directory identity checks;
- file and directory `fsync`, atomic replacement, and rollback behavior.

The allowed `agent_surface` values become exactly `codex` and `claude`.
Existing `codex` records and their validation behavior remain unchanged.
`claude` is persisted only when the Claude adapter explicitly supplies it.
Unknown surfaces still fail validation.

The public roadmap's stronger cross-resource advisory locking, content-hash
compare-and-swap, bounded lock timeouts, stale-owner handling, and maintenance
lease remain future work. This feature preserves the current per-record atomic
and exclusive transition guarantees without claiming those future guarantees.

### Thin platform adapters

The adapter-neutral CLI keeps the existing commands: `start`, `finish`,
`validate`, `integrity`, and `report`. Thin platform entry points select a
surface and its default local home before invoking the shared parser/core:

| Surface | Default home | Recorded surface |
|---|---|---|
| Codex | `~/.codex/workflow-observatory` | `codex` |
| Claude | `~/.claude/workflow-observatory` | `claude` |

`WORKFLOW_OBSERVATORY_HOME` remains an explicit absolute local override for
both. A config file at the selected home keeps schema version 1 and explicitly
selects `portable` or `llmwiki`. Adapter selection never falls back after an
error.

The Codex entry point preserves its existing argv, interpreter guidance,
default home, output, errors, and record bytes except where a test explicitly
proves that accepting `claude` does not change Codex behavior. Claude skills use
the Claude entry point and `${CLAUDE_PLUGIN_ROOT}` to locate bundled code. They
do not infer a platform by searching for home directories or installed tools.

### Portable and LLM Wiki storage

Portable Claude mode is the fully supported storage mode for the alpha's
deterministic test boundary. It writes to the configured Claude home and uses
the same core as Codex portable mode.

The LLM Wiki adapter remains explicit. A configured external LLM Wiki CLI must
accept `--agent-surface claude` to record a Claude run. If it accepts only
Codex, the delegated start fails closed, creates no replacement portable run,
does not relabel the event as Codex, and reports the adapter failure while the
authorized task continues. This repository does not modify a user's live LLM
Wiki or assume it has been upgraded.

## Claude skill behavior

Claude receives platform-specific versions of the existing four concerns:

- `workflow-observer`: best-effort automatic eligibility and one-run routing;
- `workflow-telemetry`: Claude command resolution, schemas, payloads, adapters,
  lifecycle, and cleanup;
- `workflow-learning`: on-demand read-only validated report analysis;
- `workflow-improving`: explicit-request, evidence-linked proposals with a
  fresh approval gate before action.

The substantive privacy, taxonomy, status, lifecycle, learning, and approval
rules stay aligned across platforms. Command examples, runtime variables,
default homes, agent surface, hook degradation, and installation instructions
remain platform-specific.

Automatic skill discovery is best effort on both platforms. It is not a hard
runtime hook and is not represented as 100% guaranteed invocation. Planning
without implementation, read-only work, answer-only work, simple untested
single-file edits, chat, status questions, and parent-managed workers remain
excluded.

## Claude parent propagation hook

### Purpose and boundary

The initial Claude hook surface contains only `SubagentStart`. It improves
parent-marker propagation without deciding eligibility or starting/finishing
observations. The adapter does not install `UserPromptSubmit`, `Stop`,
`SubagentStop`, `SessionEnd`, transcript-reading, or prompt-based hooks.

This narrow choice matters because official Claude hook inputs can include the
complete user prompt, transcript paths, or final assistant message. The
observer does not need those values and must not process them into telemetry.

### Session binding

After a successful Claude observation start, the Claude adapter creates a
private runtime binding from the opaque Claude session ID to the active run ID.
The session ID is SHA-256 hashed before it is used as a filename. The binding
contains only its schema version, active run ID, and bounded lifecycle state;
it contains no prompt, transcript, task text, cwd, subject path, or hook input.

Runtime bindings live beneath `${CLAUDE_PLUGIN_DATA}` because they are
disposable adapter state, not observation history. Directories use mode 0700,
files use mode 0600, symlinks and non-regular files fail closed, and updates use
the same unique-temporary-file, flush, `fsync`, atomic-replace, and
directory-`fsync` discipline as other durable local transitions. A per-session
lock serializes bind, supersede, finish, and lookup operations.

Lifecycle rules are:

1. Rejected start: create no binding.
2. Successful start: bind the session to the returned run ID.
3. Successful finish: remove the binding and sync the directory.
4. Rejected finish: retain the binding because the draft remains active.
5. Scope replacement: start the replacement, atomically bind the new run, then
   finish the old run as superseded.
6. Invalid, missing, permissive, or symlinked binding state: emit no marker,
   disclose a bounded adapter error, and do not create a child observation.

### Injected context

On `SubagentStart`, the command hook reads only the event name and opaque
`session_id` needed for lookup. It ignores and never writes `cwd`,
`transcript_path`, `agent_transcript_path`, prompts, messages, tool inputs, and
other free text. With a valid active binding it returns exactly:

```text
Observation managed by parent run <run-id>; do not start a child observation.
```

The subagent and every descendant must propagate that marker unchanged. The
hook emits no marker when no active binding exists. Hook failure cannot block
subagent creation, so Claude skills retain the existing explicit instruction to
put the same marker into every worker prompt. The hook is defense in depth, not
an authority and not a duplicate-start mechanism.

## Capability and degraded-mode matrix

| Capability | Codex adapter | Claude full mode | Claude skill-only degraded mode |
|---|---|---|---|
| Native marketplace and manifest | Existing Codex schemas | New Claude schemas | New Claude schemas |
| Platform skills | Codex adapter skills | Claude adapter skills | Claude adapter skills |
| Portable store | Full; Codex home | Full; Claude home | Full; Claude home |
| Shared schema/privacy/core | Full | Full with `agent_surface: claude` | Same |
| Atomic start/finish and per-run locking | Preserved | Preserved | Preserved |
| Parent propagation | Explicit marker contract | Contract plus `SubagentStart` injection | Contract only |
| Hooks | Not required | Best-effort assist | Disabled, unavailable, or failed |
| LLM Wiki | Existing behavior | Only with a Claude-aware external CLI | Same fail-closed rule |
| Windows | Existing interpreter fallback | Provisional skill-only unless hook launcher is proven | `py -3` skill path |
| Uninstall | Observation store preserved | Observation store preserved | Observation store preserved |
| Learning and improvement | Existing read/approval gates | Same shared report contract | Same |
| Runtime validation claim | Existing Codex evidence | Alpha: not yet real-runtime validated | Explicitly degraded |

Claude enters skill-only degraded mode when any of these applies:

- managed policy disables plugin hooks;
- the documented hook interpreter/launcher is unavailable;
- `${CLAUDE_PLUGIN_DATA}` or the session ID is unavailable or invalid;
- runtime binding initialization or integrity validation fails;
- the platform is Windows and the static hook launch path has not been proven;
- the user deliberately disables hooks while keeping plugin skills enabled.

Degradation never switches stores, changes `agent_surface`, starts a replacement
run, or blocks the authorized task. The response discloses that hook-assisted
propagation was unavailable. Skills continue to require explicit parent-marker
propagation.

The provisional Claude Code version target is 2.1.153 or later because that is
the locally available validator baseline. The alpha documentation must call
this target unverified. If real runtime testing shows any required skill,
manifest, `${CLAUDE_PLUGIN_ROOT}`, `${CLAUDE_PLUGIN_DATA}`, session substitution,
or `SubagentStart` behavior needs a later version, the implementation raises the
minimum version rather than adding an unsafe fallback.

## Privacy, retention, and uninstall behavior

Observation records remain local by default. They contain bounded summaries,
fixed enums, derived provenance, metrics, opaque IDs, and evidence labels—not
full prompts, transcripts, credentials, secrets, hook messages, or subject
absolute paths.

Claude observation history lives under `~/.claude/workflow-observatory/store`
or an explicitly configured absolute root. It does not live under
`${CLAUDE_PLUGIN_ROOT}` because installed plugin versions are cached and
ephemeral. It does not live under `${CLAUDE_PLUGIN_DATA}` because Claude Code
deletes plugin data by default after uninstalling the plugin from its final
scope. Removing the plugin therefore does not remove observation history.

Runtime session bindings may live under `${CLAUDE_PLUGIN_DATA}` and may be
deleted on uninstall because they are disposable propagation state. Their
deletion can leave a truthful draft in the separate observation store; it must
never delete, finalize, relabel, or invalidate that draft automatically.

The alpha documents manual store export/deletion only at the same level as the
existing release. It does not claim that the roadmap's full retention, export,
delete, or schema-migration policy has been implemented.

## Error handling

- Validation failures return bounded validation errors without raw hook input,
  prompts, commands, paths, stderr, or record content.
- A rejected start creates no observation and no session binding.
- A rejected finish leaves both draft and active binding unchanged.
- Hook lookup failure produces no context marker and never starts a child run.
- LLM Wiki rejection never falls back to portable storage.
- A Codex-only external CLI never receives a record falsely labeled Claude or
  a Claude record falsely labeled Codex.
- Package validation failure blocks release construction but does not edit
  source evidence or user stores.
- Real Claude runtime validation that fails after alpha publication is reported
  as an alpha compatibility defect, not rewritten as a successful test.

## Checkout-packager prerequisite

Before any Claude feature implementation, the source-checkout baseline must be
made green with a focused TDD repair. In an ordinary Git clone, the released
plugin suite currently runs 76 tests with four failures, eight errors, and one
skip because the clean-room packager recursively scans the repository root and
rejects `.git/HEAD` as an unexpected marketplace file.

The repair must ignore the root `.git` metadata tree as repository metadata,
without broadening the public-file allowlist or hiding other unexpected files.
A regression test must fail first in a real or representative cloned layout,
then prove:

- `.git/**` is excluded from marketplace inputs;
- another unexpected root or plugin file is still rejected;
- symlink rejection, personal-path rejection, completeness inventory, archive
  verification, and byte reproducibility remain unchanged;
- the documented source-checkout test command passes from a normal clone.

This prerequisite receives its own reviewable commit before Claude adapter
work. It is not described as a Claude compatibility fix.

## Verification design

### Deterministic tests required for the alpha

All test processes run under `caffeinate` in the development workflow.

1. **Native metadata:** validate Codex metadata with Codex tooling and Claude
   metadata with `claude plugin validate --strict`; assert neither test feeds
   one platform's manifest into the other platform's validator.
2. **Core surface:** RED/GREEN tests accept exactly `codex` and `claude`, reject
   unknown surfaces, round-trip Claude records, and prove existing Codex record
   bytes and normalized reports do not change.
3. **Default homes:** isolated empty-home tests prove Codex writes only beneath
   the Codex home and Claude only beneath the Claude home; explicit config and
   override behavior remain deterministic.
4. **Portable lifecycle:** run start, finish, partial, failed, rolled-back,
   superseded, double-finish, report, validate, and integrity matrices for both
   surfaces through the shared core.
5. **Privacy:** reject prompt/transcript/path/credential payloads; prove hook
   fixtures containing sensitive unused fields do not write or echo them.
6. **Session binding:** test modes, symlinks, identity races, unique temporary
   files, locks, atomic replacement, directory sync, successful cleanup,
   rejected-finish retention, and supersession ordering.
7. **Hook contract:** fixture-driven `SubagentStart` tests emit exactly one
   unchanged marker for an active binding and none for missing/invalid state.
   Hook-disabled and hook-error fixtures continue without duplicate starts.
8. **LLM Wiki degradation:** a Codex-only fake external CLI rejects Claude
   start; the adapter creates no portable record, no session binding, and no
   relabeled Codex record.
9. **Codex non-regression:** preserve current skill trigger contracts, CLI argv,
   default home, taxonomy, privacy, frozen forward/lifecycle manifests, adapter
   conformance, integrity, package contents, and installation documentation.
10. **Clean-room packaging:** include both native metadata trees, both skill
    adapters, hooks, shared core, tests, public disclaimer, approved spec/plan,
    and SHA-256 mapping. A clean extraction passes validators and deterministic
    tests, then rebuilds two byte-identical archives.

### Fresh-agent Claude evaluations

The repository defines but does not claim to have executed a real-runtime
Claude evaluation suite for the alpha. Cases run in fresh, isolated Claude Code
sessions with a fresh home/store and cover:

- eligible multi-file implementation starts once before mutation;
- chat, read-only, plan-only, simple single-file, and parent-managed exclusions;
- successful, partial, failed, and superseded completion behavior;
- one top-level run with subagents and unchanged descendant markers;
- hooks enabled, hooks disabled, hook failure, and skill-only degradation;
- no prompt/transcript/path leakage into records, bindings, stdout, or stderr;
- no silent portable fallback from an incompatible LLM Wiki adapter;
- install, reload, update, resume, and uninstall-with-store-preservation flows.

Each run records Claude Code version, OS, install scope, marketplace source,
case ID, sanitized result, and artifact SHA-256. It never stores prompts,
transcripts, credentials, absolute subject paths, or unsanitized debug logs.
The aggregate is authoritative only when every required case passes from fresh
state. Partial shards and individually retried successes are not represented as
an atomic full pass.

Because the first publication is explicitly unverified in a real Claude Code
runtime, these cases ship as pending evaluation definitions. Their absence is
visible in release acceptance and cannot be converted into a pass by the
deterministic suite. A later beta may remove the unverified label only after the
aggregate real-runtime gate succeeds and its sanitized evidence is reviewed.

## Codex non-regression boundary

The existing Codex release remains installable through its native marketplace.
Moving Codex skills into an explicit adapter directory must not change their
names, descriptions, automatic eligibility, telemetry payloads, CLI behavior,
learning thresholds, approval gates, or migration warning against enabling two
automatic observers.

The frozen 20 forward and 8 lifecycle manifests retain their approved bytes and
digests unless a separately approved Codex behavior change requires a new
versioned manifest. Claude cases use separate manifests; they do not rewrite
Codex evidence. The historical July 19 composite acceptance remains described
with its original non-atomic boundary.

## Clean-room release and public labeling

The first dual-platform artifact is versioned `0.2.0-alpha.1` and published as a
GitHub pre-release. The user has authorized that eventual upload, but no push or
publication occurs until the user reviews this written specification and later
approves the implementation plan, implementation, and verification results.
The current design-writing stage does not authorize a push or publication.

The deterministic archive inventory includes every distributed Codex and
Claude file and every public evidence file required to reproduce the claimed
checks. Raw observations, configuration, credentials, caches, runtime bindings,
plugin data, temporary payloads, Git metadata, and author-specific paths are
excluded.

Release acceptance separates three statements:

- **Codex:** the exact non-regression evidence that actually passed;
- **Claude deterministic:** validators and fixture-driven tests that actually
  passed, with versions and counts;
- **Claude runtime:** `NOT YET VALIDATED` until the fresh-agent aggregate gate
  has run successfully.

No README badge, release title, marketplace description, or changelog may say
"Claude compatible," "works with Claude," or equivalent without the adjacent
alpha/unverified qualification.

## Contribution and pull-request policy

The public README and Claude plugin documentation invite users to validate the
alpha and submit pull requests. A compatibility report or PR should include:

- Claude Code version, OS, installation scope, and plugin source/ref;
- the sanitized case ID or minimal reproduction steps;
- expected and actual bounded behavior;
- deterministic test additions for every fix;
- confirmation that prompts, transcripts, credentials, personal paths, stores,
  and unsanitized debug logs are not attached.

Fix PRs follow TDD: add a failing deterministic regression when possible, show
the expected failure, implement the smallest fix, and run native validators,
Claude-focused tests, Codex non-regression tests, and clean-room packaging.
Changes to privacy, lifecycle, retention, adapter fallback, or surface labeling
also require a design update and explicit maintainer review.

Successful external real-runtime results may contribute to the aggregate gate,
but a single report does not remove the unverified label. Maintainers verify
sanitized evidence, reproduce where possible, and record the exact acceptance
boundary before changing release status.

## Alternatives considered

### Alternative B: two generated platform plugin trees

Generate separate Codex and Claude plugin trees from a canonical source core.
This gives the strongest discovery isolation and native package boundaries, but
adds generated-file drift, parity machinery, duplicate review surfaces, and a
build prerequisite before the repository is directly installable.

This remains a documented fallback if native tooling proves that one physical
plugin root cannot be validated safely. The alpha does not create placeholder
directories, manifests, generators, or incomplete tests for Alternative B.

### Alternative C: shared skills plus dual manifests

Keep one skill set and branch inside each skill on the detected platform. This
looks smaller but interleaves homes, commands, variables, hooks, and degraded
modes. Claude normally scans default skills in addition to custom paths, which
also risks duplicate discovery. The result would weaken Codex non-regression
and encourage the false idea that a shared Markdown shape makes platform
manifests or runtime instructions interchangeable.

Alternative C is rejected. The alpha does not implement placeholder branches,
unused platform detection, or speculative shared frontmatter.

## Acceptance criteria for the written design

This specification is ready for implementation planning only when the user
confirms that it correctly defines:

- one physical plugin with explicit Codex and Claude metadata/skill adapters;
- a shared core with exact `codex` and `claude` surfaces;
- portable defaults, fail-closed LLM Wiki behavior, and degraded modes;
- privacy, retention, uninstall, parent propagation, and atomic binding rules;
- the checkout-packager prerequisite and Codex non-regression boundary;
- deterministic alpha tests and pending fresh-agent Claude runtime tests;
- `0.2.0-alpha.1` labeling and the explicit real-runtime disclaimer;
- the contribution/PR policy and non-implemented Alternatives B and C.

After that review, the next and only brainstorming transition is a separate,
detailed implementation plan. No code, push, publication, installation, or
runtime/global skill change is authorized by this specification.

## Primary references

Official Claude Code documentation inspected for this design:

- <https://code.claude.com/docs/en/plugins>
- <https://code.claude.com/docs/en/plugins-reference>
- <https://code.claude.com/docs/en/plugin-marketplaces>
- <https://code.claude.com/docs/en/skills>
- <https://code.claude.com/docs/en/hooks>
- <https://code.claude.com/docs/en/hooks-guide>
- <https://code.claude.com/docs/en/settings>
- <https://code.claude.com/docs/en/security>
