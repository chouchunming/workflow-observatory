# Workflow Observatory Marketplace — Task 6 Report

Status: **revision 6 formal case-20 remediation verified; protected formal retry pending**

## Frozen inputs

The marketplace contains byte-identical ordered 20-case and 8-case manifests.
The runner embeds and enforces these SHA-256 values:

- decision: `f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`
- lifecycle: `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`

No frozen turn, decision, or expected lifecycle value was rewritten.

## Important review findings closed

1. **Configured-store integrity.** Every marketplace case invokes the copied
   plugin's actual audited `workflow_observer_cli.py integrity` command after the
   case. Exit must be zero, stderr empty, and stdout exactly
   `healthy records=N invalidated=M`; `N` must match the inspected store. The
   runtime factory repeats these checks for every accumulated store immediately
   before persistence, so malformed or extra store entries prevent a commit.

2. **Skill isolation.** Each case copies the marketplace locally and exposes
   only its four skill symlinks in the fixture. The runner inventories every
   external `SKILL.md` below the full `CODEX_HOME` (including global skills,
   installed plugins, and plugin caches), `~/.agents/skills`, and
   `/etc/codex/skills`, then passes explicit `skills.config` disable entries to
   app-server. Fixture skill paths are excluded from that disable inventory.

3. **Honest invocation ledger.** The marketplace runtime now has exactly one
   workflow CLI entrypoint: the selected `workflow_observer_cli.py`. Its
   generated source embeds the original production CLI bytes, compiles them
   under a non-`__main__` namespace, and calls `main(argv)` in the same process.
   Normal sibling libraries remain, but there is no target executable, target
   environment variable, audit sidecar, alternate main entrypoint, or subprocess
   capability to discover. The wrapper records one row in `finally` for every
   invocation and preserves the embedded CLI's stdout, stderr, exit, and errors.
   It records every payload-flag occurrence in argv
   order, its argv index and selected following value, payload identity/mode/text
   or capture error, and target exit/error. Validation counts start and finish
   invocations rather than successful payloads; it rejects payloadless finish,
   repeated payload flags, argv/payload binding mismatches, reused or undeleted
   payloads, post-start help/draft inspection, repeated calls beyond the frozen
   expectation, and completion scalars over 200 Unicode code points. Direct
   selected-wrapper `report` and `finish` calls are therefore ledgered rather
   than bypassing audit. Filesystem discovery regressions confirm there is no
   `.target-*.py`, target sidecar, or second workflow CLI to find.

4. **Production fingerprint in `finally`.** Every formal case and every
   preflight is executed through `run_with_production_guard`. The production
   Git-visible/observation fingerprint runs on success and failure. If both the
   case and fingerprint fail, an exception group retains both causes. The same
   guard wraps the final all-store integrity pass before persistence.

5. **Crash-consistent paired results.** Fixed result paths are no longer
   authoritative. Each pair is written as two immutable, content-addressed
   generation files and fsync'd. A single fsync'd, atomically replaced commit
   manifest is the sole visibility point and contains both relative paths and
   SHA-256 hashes. Readers resolve only that manifest, constrain paths to the
   generation directory, hash-check both files, and revalidate both schemas and
   ID sets. Unreferenced generation files are ignored. Crash injection after
   each generation write/rename and pointer write/rename proves that every
   pre-commit interruption leaves the previous committed pair visible; after
   pointer commit, the new complete pair is visible. Tampering with either
   generation fails hash validation.
   Parent and generation directories are traversed component-by-component with
   `O_DIRECTORY|O_NOFOLLOW`, lstat/fstat identity rechecks, and dir-fd-relative
   mkdir/open/rename/unlink operations. Generation and pointer files are opened
   with `O_NOFOLLOW` and verified regular before reading. The resolver never
   uses `.resolve()` to turn a symlink into a trusted path. Regressions prove a
   symlinked ancestor, symlinked generation root, and symlinked pointer are
   rejected without writing through to the external target.

The implementation plan now documents this authoritative commit-manifest
protocol rather than claiming impossible two-fixed-file atomic replacement.

## TDD and verification evidence

RED was observed as an import failure for the new review-remediation APIs before
implementation. Reviewer counterexamples then drove the implementation:

- payloadless repeated finish;
- repeated `--from-file` occurrences;
- payload/argv index and selected-value mismatch;
- payload pre-read failure and missing target while preserving target exit;
- malformed integrity stdout and nonzero exit;
- user/global/plugin-cache/installed-plugin skill discovery;
- simultaneous case and production-fingerprint failures;
- crashes at all six pair-commit boundaries and generation hash tampering.

The earlier review remediation passed `21` focused tests; its then-current full
marketplace suite passed `51` tests with one expected configured-source parity
skip.

## Invalidated authorized preflight and bytecode hygiene

One later user-authorized preflight attempt is not accepted as Task 6 evidence.
Its production fingerprint was invalidated by two independent mutations: a
concurrent session created
`raw/Workflow_Improvement_Market_Landscape_and_Palantir_Fit_2026-07-15.md`, and
the marketplace runner's own top-level repository-runner import created
`scripts/__pycache__/run_observing_workflows_task9_eval.cpython-314.pyc`.
Consequently, this attempt produced no accepted preflight result, formal run, or
model result, and no revision-5 result was persisted.

The self-pollution cause is now covered by a bare-invocation regression. The
marketplace runner sets `sys.dont_write_bytecode = True` before importing the
repository runner or harness, so `python3 .../run_marketplace_eval.py --help`
does not create the production runner cache. The generated runner `.pyc` was
removed while the pre-existing `scripts/__pycache__/__init__.cpython-314.pyc`
was preserved. No preflight or model command was run while making this fix.

## Accepted marketplace preflight

After the hygiene fix passed independent review and the production fingerprint
remained unchanged through a 30-second quiescence window, one replacement
preflight was authorized and run. It completed with exit `0` and reported:

```text
Workflow Observatory marketplace preflight passed (one start, one finish).
```

The accepted preflight used isolated marketplace skills and case-local store,
payload, audit, and output paths. It passed the lifecycle ledger, payload
cleanup, store-integrity, and production-fingerprint gates. It did not run the
formal 20+8 suites or persist revision-5 results.

## Formal revision 5 failure

After the accepted preflight, the one authorized formal revision 5 run stopped
at forward case 2/20, `tested-bugfix`. The evaluated Codex session used
unqualified `python` for the observation start, but that executable was absent
from the isolated environment. The start therefore failed, the expected
observation record was absent, and the evaluator terminated at that case.

Fail-fast persistence worked as designed: no fixed result document, generation
file, or authoritative `observing_workflows_results_commit.json` was created.
The frozen manifests, expected outputs, and fixed result destinations remain
unchanged. This failure is preserved as revision 5 history; revision 6 is a new
skill revision and has not been run.

## Revision 6 interpreter-contract remediation

Root cause was the telemetry wording "the current Python interpreter," which
did not specify the executable name and allowed Codex to choose `python`. A new
contract regression first failed against that wording. The telemetry contract
now requires literal `python3 <resolved-cli-path>` on Codex for Unix, macOS,
and Linux and explicitly forbids unqualified `python`. Windows alone may use
the documented `py -3 <resolved-cli-path>` fallback after one non-payload
availability resolution before payload creation. Expanded start and finish
examples retain the same prefix across the lifecycle.

The marketplace runner now labels its next formal execution and success output
as revision 6. The implementation plan's Task 6 and Task 8 targets also name
revision 6 while preserving the accepted preflight and revision 5 failure.
No preflight, formal suite, or model command was run during this remediation.

Revision 6 remediation verification passed `10` focused contract/hygiene tests
and the full `54`-test marketplace suite with one expected configured-source
parity skip. All four skills passed the official skill validator, and the
plugin passed the official plugin validator. Static checks passed runner
`--help`, AST parsing, both frozen SHA-256 values, per-file whitespace checks,
absence of result files/commit pointer, and absence of marketplace `.pyc`
files. Repository-wide `git diff --check` separately reports pre-existing
trailing whitespace in `cron_gmail.log`; this task did not change that file.

## Invalidated revision 6 formal attempt

After revision 6 passed independent review and a 30-second quiescence check,
one formal run was authorized. It completed forward case 1 and was executing
case 2, `tested-bugfix`, when the production-fingerprint guard detected
concurrent repository writes and terminated the suite. Read-only forensics
identified a new raw source plus updates to `wiki/_source_triage.md` and
`wiki/tasks/codex-tui-diff-upstream-pr.md` from another workflow during the
formal window.

This is an external concurrent-mutation invalidation, not a revision-6 skill
failure. The runner published no fixed result document, generation, or
authoritative commit pointer. Revision 6 must not be treated as evaluated until
a replacement formal run completes inside an explicitly protected no-write
window.

## Protected replacement timeout and diagnostic hardening

After the concurrent ingest workflow reported completion, the repository
fingerprint remained stable through a protected quiescence check. A replacement
revision-6 formal run then stopped in forward case 1, `multi-file-feature`,
after app-server remained alive but emitted no message for the 600-second turn
budget. The production fingerprint did not report a mutation. The temporary
case directory was removed by the existing `TemporaryDirectory`, and the old
timeout contained no last-event, active-command, or stderr context, so this
attempt cannot establish whether the agent, a tool, or the transport stalled.
It published no result generation or commit pointer.

TDD diagnostics now track active command executions and report the app-server
PID, last protocol event, and bounded SHA-256/length summaries for at most three
active commands and stderr lines on silence. Raw command, path, token, and stderr
text is not copied into timeout or process-exit exceptions. The
marketplace runner also accepts one explicit `--diagnostic-case`; it validates
the frozen manifests, retains its isolated workspace, runs the production guard
and configured-store integrity checks, and never invokes paired-result
persistence. An initial ad-hoc diagnostic launch from the managed shell sandbox
failed before initialization because app-server could not initialize SQLite
state below `~/.codex`; it did not enter a model turn. The supported diagnostic
runner was then executed after the user approved that outer sandbox boundary.
The exact `multi-file-feature` case passed with one planned
`implementation-with-review` run and one successful finish. This makes the old
silence non-reproducible; it does not turn the diagnostic into a formal result.

Current Codex app-server schemas confirm that the protocol is bidirectional and
can send client requests such as
`item/commandExecution/requestApproval`,
`item/fileChange/requestApproval`, and
`item/permissions/requestApproval`. Although both evaluator thread and turn
request `approvalPolicy: never`, the harness previously treated any such server
request as an ordinary event, sent no response, and then waited until the
silence timeout. A RED regression reproduced that deterministic deadlock path in
both normal receive and two-turn gate polling. Both paths now share one message
classifier and fail fast on every unhandled server-initiated JSON-RPC request,
recording only its method and id; responses and notifications remain accepted,
and no action is silently approved. Success/failure diagnostic regressions prove
the production verifier always runs, configured-store integrity runs on
success, the formal suite/result persistor is never invoked, the authoritative
generation directory and commit pointer remain unchanged on success, and both
remain absent on failure. The full focused runner suite passes 30 tests; all 54
marketplace plugin tests pass with one expected skip. Independent re-review and
a new protected formal run remain required.

## Revision 6 case-3 reviewer-gate failure

After the server-request/diagnostic hardening passed independent review, a new
protected formal revision-6 run completed forward cases 1
(`multi-file-feature`) and 2 (`tested-bugfix`). Case 2 specifically proved the
literal `python3` lifecycle contract that had failed revision 5. Forward case 3,
`reviewed-refactor`, then failed with one draft record instead of the expected
successful finish. Its implementation and three fixture tests passed, and
`git diff --check` passed, but the evaluator ran `scripts/gate.py` as its
"reviewer gate". That script created its ready marker, received no release
within 15 seconds, and exited 2. The agent correctly did not claim reviewer
approval; its attempted partial observation completion was rejected and was not
retried. No result generation or commit pointer was published, and the
production guard reported no concurrent mutation.

Root cause is fixture affordance leakage rather than the frozen manifest or
telemetry taxonomy. `scripts/gate.py` is a harness choreography barrier for the
two prompts that explicitly name the command (`late-trigger` and
`scope-supersession`). `build_fixture()` nevertheless exposed it in every
fixture. The frozen case-3 wording "require a reviewer gate" therefore made the
unreleasable choreography script look like the intended independent-review
mechanism. Case 1 used a reviewer successfully because its wording did not
invite that script.

A RED regression now requires a case-aware fixture builder. The runner includes
`scripts/gate.py` only when a frozen turn explicitly names that path; all other
fixture content, prompts, expected outputs, and frozen manifest bytes remain
unchanged. The focused evaluator suite passes 31 tests, the lower-level harness
passes 29 tests including real gate release/timeout behavior, and all 54
marketplace tests pass with one expected skip. The required independent review
approved the case-aware fixture selection, preserved explicit two-turn gate
behavior, unchanged frozen hashes, and the recorded failure history.

## Post-remediation case-3 app-server stall

The reviewed case-aware fixture replacement formal again completed forward
cases 1 and 2. In case 3 the false choreography gate was absent, the independent
reviewer approved the refactor, and four tests passed. The parent agent then
announced that it would run one final cleanliness check before finishing the
observation. App-server never emitted `turn/completed`; the 600-second turn
budget expired with `last_event=item/completed` and no active command
executions. No production mutation was reported and no result generation or
commit pointer exists.

Read-only forensics against the evaluator process in Codex's local structured
log identified the exact boundary. The reviewer child received its interrupt,
the parent reported reviewer approval, and the next raw model output was a
`CustomToolCall` named `exec` with empty input and `status=in_progress`. No
`item/tool/call` server request followed, so the evaluator client had nothing it
could answer or reject. Parent-thread activity then ceased until timeout. This
is an app-server/custom-tool transport stall after successful skill behavior,
not an observation lifecycle, reviewer, fixture, shell-command, approval, or
production-concurrency failure.

Further narrow app-server patches are not justified: the last custom call never
reached the bidirectional request layer. The recommended architecture change is
a hybrid runner: use `codex exec --json --ephemeral` for the 24 single-turn
cases so the official noninteractive client owns custom-tool execution, and
retain app-server only for the four cases that require in-flight steering.
Frozen manifests, isolated skills/store/payload audit, production guards,
result validation, and atomic paired-result persistence would remain unchanged.
This is a material evaluator refactor and requires user authorization, RED/GREEN
coverage, a single reviewed `reviewed-refactor` transport diagnostic, and a new
protected formal. The alternative is to record Task 6 as blocked by upstream
app-server infrastructure, which prevents Tasks 7 and 8 from claiming complete
formal evidence.

## Hybrid transport and first complete discovery sweep

The user authorized the hybrid architecture. The runner now derives transport
only from turn count: 24 single-turn cases use `codex exec --json --ephemeral`,
and the four two-turn steering cases retain app-server. Frozen prompts,
expectations, order, and manifest bytes remain unchanged. Deterministic tests
cover exact 24/4 routing, terminal protocol requirements, cleanup, production
guards, and one final persistence call only after a complete formal suite.

After deterministic review, a protected non-authoritative discovery sweep ran
all 20 forward and 8 lifecycle cases without a hard abort. Its retained report
was `authoritative: false`, `complete: true`, `passed: 21`, `failed: 7`; its
SHA-256 is
`0070ff8408998150a0621191ece050836ec4d06dd5c9ce4cb07ba3febf28349c`.
No result generation or authoritative commit pointer was created.

The seven failures formed four evidence-backed root-cause groups:

- `durable-query` completed its Wiki output and one successful run but selected
  `documentation/implementation-basic` instead of `query/research-basic`.
- Forward `late-trigger` and both `scope-supersession` variants, plus lifecycle
  `late-success`, reached the fixed 15-second checkpoint deadline before any
  mutation or draft existed. Cleanup and the production guard still passed.
- Forward `parent-managed-subagent` reached the 15-minute exec limit after its
  implementation and tests were complete and one parent draft existed; the
  retained fixture's eight tests pass, but the parent had not finished the run.
- Lifecycle `task-failure` correctly stored final status `failed`; the evaluator
  falsely classified a final response that mentioned both the task failure and
  its observation as disclosure of a recording-infrastructure failure.

## Batched discovery remediation

TDD regressions first failed at each observed boundary. The minimal remediation:

- extends the bounded exec limit from 15 to 20 minutes for delegated review
  tail latency;
- replaces the 15-second two-turn checkpoint deadline with a bounded five-minute
  condition wait while preserving early exit, turn-completed rejection, cleanup,
  and the ten-minute post-steer turn bound;
- states that research/query intent remains `query/research-basic` when its
  durable output is a comparison, answer, or Markdown summary, reserving
  `documentation` for documentation-as-task;
- replaces the recording-disclosure keyword cross-product with explicit active,
  passive, unavailable-command, and start/finish failure patterns, while
  rejecting task-failure statements and `recorded as/with status` qualifiers.

The evaluator suite passes 87 tests. The marketplace suite passes 56 tests with
one expected development-source skip. Plugin, four marketplace skills, the
repository legacy skill, and the installed legacy skill pass their official
validators. Forward and lifecycle manifests retain SHA-256
`f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`
and `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`.

Independent review first found three passive-voice false negatives, then one
`recorded as successful` false positive. Each finding received a failing
regression before repair. The final reviewer verdict is Ready with no remaining
blocking finding.

## Post-repair targeted model evidence

Five non-persisting targeted cases then passed under marketplace runtimes and
production/store/payload guards:

- forward `durable-query`: one success, draft zero,
  `query/research-basic`;
- lifecycle `task-failure`: one final `failed` run, draft zero, recording-failure
  disclosure false;
- forward `late-trigger`: turn one untriggered, turn two late-triggered, one
  success;
- lifecycle `scope-supersession`: two runs, final statuses `success` and
  `superseded`, draft zero;
- forward `parent-managed-subagent`: one success, draft zero,
  `feature/implementation-with-review`.

Both targeted groups exited zero, repeated all configured-store integrity
checks, removed payloads and exec output, preserved the production fingerprint,
and created no authoritative result artifact. A new protected complete discovery
sweep is required before the frozen formal run.

## Replacement discovery sweep and second remediation

The protected replacement discovery sweep completed every frozen case without a
hard abort. Its retained non-authoritative report is at
`${TMPDIR}/workflow-observatory-discovery-sweep-4xj7thqg/discovery-sweep-report.json`.
It reported `complete: true`, `passed: 25`, and `failed: 3`; the report SHA-256
is `4a16f9b28435bdb1a3687022fbc3bef5f4b1541082bba68e44ac2a4a005a8280`.
The production fingerprint and both frozen manifests remained unchanged, and no
authoritative result generation or commit pointer was created.

The remaining failures were:

- forward `wiki-compile` completed the concept page, raw citation, index update,
  and lint but did not initially start an observation;
- forward `scope-supersession` exited before the declared draft gate;
- lifecycle `scope-supersession` implemented the replacement `--stats` scope and
  passed seven tests but left only the original `--summary` draft.

Read-only forensics found two skill-contract gaps. The automatic router described
only software implementation even though its taxonomy and frozen cases include
Wiki compile and inbox workflows. More importantly, the router and telemetry
skill simultaneously required exactly one start and prohibited a replacement
lifecycle, contradicting the required material-scope supersession sequence. The
contracts now:

- explicitly trigger compile and inbox workflows that update Wiki pages or
  generated catalogs;
- start before entering a controlled fixture gate while keeping the gate before
  the first task mutation;
- use one lifecycle per stable scope, with material replacement as the sole
  second-start path: start replacement, supersede prior, then finish replacement;
- select the replacement run's taxonomy and review variant from the new scope.

A lifecycle targeted run then passed with two records, final statuses `success`
and `superseded`, draft zero, no failure disclosure, and configured-store
integrity. A forward targeted run revealed one narrower adapter issue: the agent
correctly triggered `wiki-compile` but supplied `--source raw/source.md` to the
portable adapter. That adapter's observation store does not contain the subject
workspace's raw tree, so schema validation correctly rejected the start. The
skill now permits `--task` and `--source` only for the `llmwiki` adapter when the
exact referent exists under that adapter's configured Wiki root, and requires
both options to be omitted for the portable adapter.

After that repair, forward `wiki-compile` passed with one planned
`compile/compile-basic` success and draft zero. The retained forward
`scope-supersession` model run also created the correct two records and passed
seven tests plus independent review: the original scope was
`implementation-basic/superseded`, while the replacement was
`implementation-with-review/success`. The evaluator alone rejected the first
historical scope because it applied the case's final taxonomy expectation to
every triggered scope. The scorer now preserves trigger-timing validation but
applies case-level taxonomy to the final triggered scope. A regression proves a
legal basic predecessor is accepted and a wrong final scope remains fail-closed.

Current deterministic evidence is `90` evaluator tests passing and `61`
marketplace tests passing with one expected development-source skip. The plugin,
all four marketplace skills, the repository legacy skill, and the installed
legacy skill pass their official validators. Python AST parsing and scoped
`git diff --check` pass. Repository and marketplace manifest copies retain the
exact frozen SHA-256 values listed above, and the marketplace result store still
contains only the two manifests.

Fresh independent review reported no findings and a **Ready** verdict for the
protected formal gate. The reviewer independently passed all `90` evaluator
tests and all `15` marketplace skill-contract tests, confirmed the root and
marketplace frozen manifests are byte-identical at the required hashes, and
verified that wrong-final taxonomy remains fail-closed. The reviewer did not run
model evaluation, edit files, or publish results. The protected formal 20+8 run
is the next gate.

## Formal case-4 zsh payload-cleanup failure

The reviewed protected formal run passed forward cases 1 through 3, including
the formerly stalled `reviewed-refactor`, then failed closed in forward case 4,
`multi-file-docs`. The observation start command successfully created a run but
its generated zsh wrapper assigned the exit code to `status`. In zsh, `status`
is a read-only special parameter; the assignment terminated the wrapper and the
Scope payload remained in the dedicated payload directory. The agent later
finished the record successfully, but the attempt ledger correctly rejected the
case because the first payload still existed and the payload directory was not
empty.

The production fingerprint did not change, configured storage was not treated
as authoritative, and no result generation or commit pointer was published.
This is a shell-portability and cleanup failure, not a store, lock, taxonomy, or
production-concurrency failure.

Two RED contract tests now require shell wrappers to store exit codes in
`exit_code`, never zsh's read-only `status`, and to keep the cleanup trap active
across every command after payload creation until deletion. The minimal skill
wording is synchronized to marketplace telemetry, repository legacy, and
installed legacy skills. The focused tests pass; the full deterministic suites
now pass `91` evaluator tests and `62` marketplace tests with one expected skip.
A fresh focused review and a non-persisting `multi-file-docs` model diagnostic
are required before another protected formal run.

Fresh focused review reported no findings and a **Ready** verdict. The reviewer
confirmed that `exit_code` is portable across POSIX-style sh/bash/zsh wrappers,
that the cleanup-trap lifetime strengthens rather than weakens the existing
payload guarantee, and that adapter and no-retry behavior remain unchanged.

The non-persisting targeted `multi-file-docs` run then passed with one planned
`documentation/implementation-basic` success, draft zero, configured-store
integrity, an empty payload directory, and an unchanged production fingerprint.
Its retained isolated workspace is
`${TMPDIR}/workflow-observatory-targeted-docs-o4ib_6wp`.
The marketplace telemetry, repository legacy, and installed legacy skills also
pass the official skill validator after this remediation. A complete protected
formal retry remains required because an authoritative pair can only be
published by one ordered all-green 20+8 execution.

## Formal case-20 ambiguity-default failure

The next protected formal retry passed all first `19` forward cases, including
the zsh-cleanup case and every earlier discovery remediation, then failed closed
at forward case 20, `ambiguous-default-no-trigger`. Its frozen prompt asked the
agent to improve parsing only if it saw an opportunity and explicitly supplied
no specific change or validation requirement. The agent voluntarily expanded
that optional request into three modified files and five tests, then created one
successful observation. The frozen expectation requires no observation for this
uncertain authorization boundary.

No lifecycle case ran, no production-fingerprint failure occurred, and no
authoritative result generation or commit pointer was published. The failure is
an automatic-trigger boundary gap: the marketplace router lacked the legacy
skill's uncertainty default and did not prevent an agent from manufacturing
eligibility by voluntarily expanding an open-ended optional request.

Two RED contract checks now require an open-ended "improve if useful" request
with no specific change or validation requirement to remain uncertain and
default to no observation, even if the agent itself chooses multiple files or
tests. Explicit authorized work, compile/inbox routing, and genuine later scope
authorization remain unchanged. Focused tests pass; current full deterministic
suites pass `91` evaluator tests and `63` marketplace tests with one expected
skip. Fresh focused review and a targeted non-persisting ambiguity case are
required before another complete protected formal run.

Fresh focused review reported no findings and a **Ready** verdict. The reviewer
confirmed that the rule is narrowly conjunctive, preserves explicit
compile/inbox eligibility, and permits a later concrete user authorization to
trigger a late observation. Both frozen manifest copies remained byte-identical
at the required hashes.

The targeted non-persisting `ambiguous-default-no-trigger` case then passed with
`triggered: false`, run count zero, draft count zero, no observation records,
configured-store integrity, an unchanged production fingerprint, and no result
persistence. Its retained workspace is
`${TMPDIR}/workflow-observatory-targeted-ambiguous-ycljv0an`.
The marketplace router, repository legacy, and installed legacy skills pass the
official validator after this remediation. Another complete protected formal
run remains required.

## Composite release acceptance after lifecycle fixture isolation

The next protected formal execution passed all 20 forward cases and the first
six lifecycle cases consecutively. Lifecycle case 7,
`complete-eval-override`, then failed for a fixture-isolation reason: every
Python-CLI fixture exposed `scripts/fail_task.py`, so the agent executed an
unrelated failure helper and honestly finalized the observed task as failed.
Lifecycle case 8 did not run. No production mutation, result generation, or
authoritative commit pointer was published.

TDD narrowed the failure helper to the exact frozen `task-failure` case. A new
regression proves every other fixture removes it while that one case retains
it. Focused verification passed, the full evaluator suite passed 92 tests, and
the marketplace suite passed 63 tests with one expected development-source
skip. Independent review reported no findings and a Ready verdict. Both frozen
manifest copies remained byte-identical at:

- forward: `f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2`;
- lifecycle: `d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b`.

Two non-persisting targeted validations then passed:

- lifecycle `complete-eval-override`: one successful final record and zero
  drafts;
- lifecycle `incomplete-eval-override`: the expected incomplete command
  selection remained fail-closed and passed its frozen assertion.

The user explicitly accepted the resulting composite boundary for Task 6:
26 consecutive protected formal passes plus the two targeted lifecycle passes.
This covers all 20 forward and 8 lifecycle cases for the 0.1.0 release decision,
but it is not one uninterrupted formal 28/28 run. No authoritative atomic result
pair or commit pointer exists, and packaging must preserve that distinction.
Task 6 is complete only under this explicit release waiver; future authoritative
formal publication still requires a complete all-green aggregate.
