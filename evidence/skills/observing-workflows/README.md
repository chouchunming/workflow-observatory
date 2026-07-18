# Observing Workflows

`observing-workflows` is a Codex skill for recording durable, privacy-minimized outcomes from substantial implementation work in the LLM Wiki observation store.

The canonical source lives in this repository. The installed copy lives at:

```text
~/.codex/skills/observing-workflows/
```

## When it applies

The skill is eligible for a top-level task that involves multiple files, tests or lint, or at least two implementation steps. It excludes chat, read-only analysis, answer-only work, planning without implementation, simple untested single-file edits, and worker tasks already managed by a parent observation.

Automatic discovery is best effort; it is not a guaranteed runtime hook.

## What it records

For an eligible task, Codex creates one central observation run before the first mutation and finalizes it with a truthful status. Records contain sanitized scope, execution evidence, outcome, follow-up, and small workflow metrics. They must not contain full prompts, transcripts, credentials, secrets, or unnecessary personal data.

Material scope replacement creates a new run and supersedes the old one. Parent agents own the observation and pass an exact marker to workers so delegated work is not double-counted.

See [`SKILL.md`](SKILL.md) for the normative agent procedure and payload contracts.

## Valid workflow pairs

- `feature`, `bugfix`, `refactor`, `documentation`: `implementation-basic` or `implementation-with-review`
- `maintenance`: `maintenance-basic` or `implementation-with-review`
- `compile`, `inbox-processing`: `compile-basic` or `compile-with-review`
- `query`: `research-basic`

In particular, `maintenance-basic` is not a general fallback. If an invalid pair is rejected, no run was created; record that failed attempt and retry with a valid pair without changing the task's scope.

Finish with exactly one of `success`, `partial`, `failed`, `rolled-back`, or `superseded`. `completed` is not a valid status.

## Installation

Copy the canonical directory, then verify that the installed copy is identical:

```bash
cp -R skills/observing-workflows ~/.codex/skills/observing-workflows
diff -qr skills/observing-workflows ~/.codex/skills/observing-workflows
```

If the destination already exists and differs, inspect the diff instead of overwriting it.

## Marketplace migration

The legacy `observing-workflows` skill remains active until marketplace installation checks pass in a clean test thread. Compare the legacy contract
with `workflow-observer` plus `workflow-telemetry`, validate one eligible trigger
and one exclusion, and only then disable the legacy installed skill.

To prevent duplicate observations, the old and new automatic descriptions must never be active simultaneously during normal work. Keep the old skill active
while validating an isolated marketplace install, then switch in one controlled
step; if validation fails, leave the old skill active and remove or disable the
new automatic entry point.

## Validation

From the LLM Wiki repository, validate the skill metadata with:

```bash
python3 ${CODEX_HOME}/skills/.system/skill-creator/scripts/quick_validate.py skills/observing-workflows
```

The validator requires PyYAML in its Python environment. The repository's frozen decision and lifecycle evaluations are defined under `tests/skill_evals/` and must run only against isolated temporary fixtures and Wiki roots.

## Evaluation overrides

`OBSERVATION_EVAL=1`, `OBSERVATION_CLI_PATH`, and `OBSERVATION_WIKI_ROOT` may redirect observation writes only when all required evaluation variables are supplied. `OBSERVATION_PAYLOAD_TMPDIR` names the case-specific directory used only for Scope and completion payloads; it must not replace the evaluator's process-wide `TMPDIR`. These overrides are for controlled isolated evaluation, not normal use.

## Repository policy

- Treat `skills/observing-workflows/` as canonical.
- Keep the installed copy byte-for-byte equivalent to the canonical directory.
- Do not write evaluation records into the production observation store.
- Do not commit, publish, or overwrite a differing installed copy without explicit authorization.
