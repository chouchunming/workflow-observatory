---
name: workflow-telemetry
description: Use when workflow-observer routes an eligible task here or when a user explicitly requests Workflow Observatory recording mechanics, schemas, lifecycle handling, or storage-adapter resolution.
---

# Workflow Telemetry

Record sanitized workflow evidence through one explicitly selected local
adapter. Do not interpret workflow quality, run learning, or change a workflow.

## Resolve the adapter command

Resolve `../../scripts/workflow_observer_cli.py` relative to this `SKILL.md` and
call that absolute file `<resolved-cli-path>`. Do not hard-code an author path.

On Codex for Unix, macOS, and Linux, `<command>` is exactly
`python3 <resolved-cli-path>`. Never use unqualified `python`. Invoke the two
values as an argv array, never a shell string.

On Windows only, `<command>` may be `py -3 <resolved-cli-path>` when a
non-payload-bearing availability check shows that `python3` is unavailable.
Document that Windows fallback. Resolve interpreter availability
once before creating a payload; do not change interpreters during a lifecycle.

The CLI loads `$WORKFLOW_OBSERVATORY_HOME/config.json`; without that file it
uses the portable store at `~/.codex/workflow-observatory/store`. Configuration
may explicitly select either `portable` or `llmwiki`. The LLM Wiki adapter must
name an existing Wiki root and a CLI inside that root. Never silently fall back
to another adapter after selection or failure: that can split history or create
duplicates. Continue the authorized task and disclose the recording failure.

Both adapters are local-only and provide start, finish, validate, report, and integrity. Require both to pass the shared adapter conformance suite with the
same schema, taxonomy, privacy, lifecycle, and normalized reporting behavior.
The storage core enforces atomic record writes and exclusive lifecycle transitions;
do not bypass the CLI or edit records directly.

Use the resolved `<command>` below as the Python-and-CLI argv prefix. Run help
only as a standalone command before creating any payload. Never combine help,
validate, integrity, or report with a payload-bearing call.

## Validate before construction

Every payload must be UTF-8, no more than 65536 bytes, and contain no
frontmatter delimiter or prohibited control character. Every scalar must be
non-empty, contain at most 200 Unicode code points, and omit control characters,
frontmatter delimiters, credentials, secrets, and absolute local or network
paths.

Store only concise sanitized facts. Forbid full prompts, transcripts,
credentials, secrets, unnecessary personal data, and subject absolute paths.
The start command needs `--subject-root` only to derive local provenance; never
copy it into a record scalar or payload. Do not reuse raw user text.

## Opt into Episode v2 only with real measurements

Schema v1 remains the default. Use the optional private v2 supplement only
when sanitized structured measurements and an applicable explicit workflow generation
are both available for this run. Do not infer a generation
from the workflow variant, Git revision, agent surface, or command context.
Otherwise keep schema v1 and omit every v2 option.

For an eligible v2 run, append
`--episode-schema-version 2 --workflow-generation <generation>` to `start`.
At finish, place the exact v2 JSON supplement in a separate unique mode-0600
regular file and append `--episode-from-file <unique-episode-path>`. Apply the
same private-file construction, scalar, privacy, and `finally` cleanup rules as
the lifecycle payloads. Let the CLI validate the closed supplement schema; do
not edit the stored Episode directly.

Never fabricate token or cost measurements, estimate them from unrelated
values, or convert absence to zero. Use only attributable tool-derived or
agent-reported values accepted by the v2 schema; when those execution values
are unavailable, keep them null with the schema's `unavailable` measurement
source. A v2 Episode records evidence only. It does not authorize a workflow
edit, branch or pull request, experiment, proposal, or learning run.

Choose exactly one legal task/variant pair:

- `feature`, `bugfix`, `refactor`, or `documentation`: `implementation-basic`
  or `implementation-with-review`
- `maintenance`: `maintenance-basic` or `implementation-with-review`
- `compile` or `inbox-processing`: `compile-basic` or `compile-with-review`
- `query`: `research-basic`

A research or query task remains `query` with `research-basic` when its durable
output is a comparison, answer, or Markdown summary. Use `documentation` only
when the authorized task itself is to create or maintain documentation.

Select a review variant only when the authorized task instructions or an
already-applicable workflow explicitly require a distinct reviewer, review
gate, or delegated independent review. Multiple files, tests, lint, link
checks, or ordinary self-verification do not by themselves imply a review
variant. Otherwise choose the legal basic variant for the task type.

The only start modes are `planned` and `late`; the only agent surface is
`codex`. The five final statuses are `success`, `partial`, `failed`,
`rolled-back`, and `superseded`. Never use `completed`.

## Create private payloads

For each call, atomically create a unique regular temporary file with mode 0600
using an exclusive secure-file API. During controlled evaluation, use the
existing directory named by `OBSERVATION_PAYLOAD_TMPDIR`; a missing dedicated
directory is a recording failure. Do not replace process-wide `TMPDIR`.
Otherwise use a secure local temporary directory. Never use symlinks, predictable
names, shared files, or payload reuse. Delete each payload in a `finally` block,
whether validation or the CLI call succeeds or fails.

When a shell wrapper records an exit code, use `exit_code`; never assign to
zsh's read-only special parameter `status`. Keep the cleanup trap active across
every command after payload creation until the payload has been removed.

Use this exact Scope shape, with every bracketed value replaced by a sanitized
scalar:

```markdown
## Scope

- Goal: [goal]
- Included: [included work]
- Excluded: [excluded work or None.]
```

Start once for each stable authorized scope, before the first mutation in that
scope:

```text
python3 <resolved-cli-path> start --title <title> --subject-root <root> --agent-surface codex --start-mode <planned|late> --task-type <type> --workflow-variant <variant> --scope-from-file <unique-path>
```

The example is the required Codex command on Unix, macOS, and Linux. On Windows
only, substitute the documented `py -3` prefix selected before payload creation.

Add `--project` only when the caller has an explicit project label. Add `--task`
or `--source` only when the selected adapter is `llmwiki` and the exact canonical
referent exists under that adapter's configured Wiki root. Omit both options for
the portable adapter even when the subject workspace contains similarly named
task or raw files. Capture stdout only as the run ID. A rejected start creates
no run. Do not automatically retry rejected payload-bearing calls in frozen
evaluation; retain failed-call evidence in the evaluator.

## Complete one lifecycle

Do not perform combined probes, draft inspection, or help after start. Do not
read the draft to construct completion evidence. Construct one new unique
completion payload from evidence already available in the task:

````markdown
## Execution evidence

- Verification: [command and result, or None.]
- Artifacts: [sanitized labels, or None.]

## Outcome and observation

- Outcome: [outcome]
- Observation: [workflow observation]

## Follow-up

- [next action, task reference, or None — no further action]

## Metrics

```yaml
verification: [pass|fail|not-run|unknown]
review_rounds: [non-negative integer|unknown]
defects_found: [non-negative integer|unknown]
rework_count: [non-negative integer|unknown]
rework_reason: [sanitized reason|none|unknown]
```
````

If `rework_count` is `0`, use `rework_reason: none`; if it is positive, give a
sanitized reason; if it is `unknown`, do not assert `none`. For `partial`, the
Follow-up must name an actual next action or task reference and cannot be
`None — no further action`.

Finish each run at most once:

```text
python3 <resolved-cli-path> finish <run-id> --status <status> --from-file <unique-path> [--episode-from-file <unique-episode-path>]
```

The same platform-specific prefix chosen before the start must be used for the
finish.

Use `--superseded-by <replacement-run-id>` only with `superseded`, and require a
real replacement run that is already active. Never use it for another status.
For a material scope replacement, start the replacement run before finishing
the prior run as `superseded`. Choose the replacement run's taxonomy and review
variant from the new authorized scope, then finish the replacement run with its
truthful outcome. This is the only path that creates another lifecycle for one
top-level task. A rejected finish leaves the draft unchanged; do not issue a
second completion-bearing call in frozen evaluation. Always clean up the
payload and disclose the recording failure.
