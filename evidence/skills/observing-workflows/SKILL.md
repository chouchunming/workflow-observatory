---
name: observing-workflows
description: Use when Codex is about to perform substantial implementation work involving multiple files, tests or lint, or two or more implementation steps; also use for a compile or inbox workflow that updates Wiki pages or generated catalogs.
---

# Observing Workflows

Record one central observation for each eligible top-level user-authorized task.

Central command: `python3 "${LLMWIKI_ROOT}/wiki_cli.py" observe --wiki-root "${LLMWIKI_ROOT}"`.

## Decide

Exclude chat, read-only work, answer-only work, planning without implementation, simple untested single-file edits, and any worker prompt containing `Observation managed by parent run`.

Otherwise trigger for multi-file work, tests/lint, or at least two implementation steps. If uncertain, do not observe. If eligibility appears after work starts, use `start_mode: late`.

Treat a compile or inbox workflow that updates Wiki pages or generated catalogs
as eligible even though it is knowledge-base maintenance rather than software
implementation.

Treat an open-ended request to improve something if useful that names no
specific change or validation requirement as uncertain. Default to no
observation, and do not manufacture eligibility by voluntarily expanding it
into multiple files or tests.

## Run

Before mutation, identify the subject root, securely create a unique mode-0600 Scope payload, and invoke `start` with `--subject-root`; the CLI derives provenance. Run help only standalone before creating a payload. Capture only the run ID. Delete payloads in `finally`. When a shell wrapper records an exit code, use `exit_code`; never assign to zsh's read-only special parameter `status`. Keep the cleanup trap active across every command after payload creation until the payload has been removed.

During evaluation, pass `OBSERVATION_PAYLOAD_TMPDIR` explicitly to the secure temporary-file API; never replace process-wide `TMPDIR`. A missing dedicated directory under a complete override is a recording failure.

For controlled forward evaluation only, use `OBSERVATION_EVAL=1` together with both `OBSERVATION_CLI_PATH` and `OBSERVATION_WIKI_ROOT` to replace the central script/root; otherwise ignore any override and use the central command. The evaluator points these variables at a temporary wiki root.

First real start: `<command> start --title <title> --subject-root <root> --agent-surface codex --start-mode <planned|late> --task-type <type> --workflow-variant <variant> --scope-from-file <path>`. Add `--project` only for an explicit project label. Add `--task` or `--source` only when the selected adapter is `llmwiki` and the exact canonical referent exists under that adapter's configured Wiki root. Omit both options for the portable adapter even when the subject workspace contains similarly named task or raw files. Finish: `<command> finish <run-id> --status <status> --from-file <path>`; add `--superseded-by` only for supersession. Never probe with payload-bearing commands.

In controlled evaluation, complete the observation start before entering a
required fixture gate, while still entering that gate before the first task
mutation.

Choose only these pairs:

- `feature`, `bugfix`, `refactor`, or `documentation`: `implementation-basic` or `implementation-with-review`
- `maintenance`: `maintenance-basic` or `implementation-with-review`
- `compile` or `inbox-processing`: `compile-basic` or `compile-with-review`
- `query`: `research-basic`

Never use `maintenance-basic` with another task type.

A research or query task remains `query` with `research-basic` when its durable
output is a comparison, answer, or Markdown summary. Use `documentation` only
when the authorized task itself is to create or maintain documentation.

Select a review variant only when the authorized task instructions or an
already-applicable workflow explicitly require a distinct reviewer, review
gate, or delegated independent review. Multiple files, tests, lint, link
checks, or ordinary self-verification do not by themselves imply a review
variant. Otherwise choose the legal basic variant for the task type.

Use these fixed payload shapes with sanitized, truthful values. Keep every
scalar at most 200 Unicode code points and count the final sanitized value
before creating the payload:

```markdown
## Scope

- Goal: [goal]
- Included: [included work]
- Excluded: [excluded work or None.]
```

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

For `partial`, Follow-up must name an actual next action or task reference; it cannot use `None — no further action`.

Pass `Observation managed by parent run <run-id>; do not start a child observation.` to every subagent and aggregate worker evidence into the parent completion. A marked worker that delegates must propagate the unchanged marker; no descendant starts a run.

At completion, atomically create a unique, sanitized mode-0600 completion payload. Finish exactly once using `success`, `partial`, `failed`, `rolled-back`, or `superseded`; never `completed`. A material Scope replacement is the only exception: start one replacement run first, then finish the prior run as `superseded` with `--superseded-by`. If start or finish fails, continue within the original authorization and disclose the recording failure.
