# Workflow Observer

Workflow Observer is the automatic entry point for eligible Codex workflow
observations. It coordinates local telemetry while keeping learning and
improvement separate from normal task execution.

## Eligibility and lifecycle

One authorized top-level task receives at most one stable-scope lifecycle.
Subagents inherit an opaque parent marker and do not create child observations.
Material scope replacement starts a replacement run and supersedes the prior
run. Read-only answers, chat, plans, status questions, ordinary single-file
copy/typo edits, and open-ended suggestions with no concrete change or
validation requirement do not trigger recording.

Observation failure never expands task authority. A rejected start creates no
run; a rejected finish leaves the draft unchanged; recording failure is
disclosed without blocking the authorized task.

## Privacy and storage

The default portable adapter is local-only. Records contain bounded summaries,
fixed enums, metrics, opaque IDs, and provenance—not full prompts, transcripts,
secrets, credentials, or subject absolute paths. Payloads must be unique
mode-0600 regular files and are deleted after the single payload-bearing call.

The optional LLM Wiki adapter must be selected explicitly and never counts
observation sources as compiled knowledge coverage.

## Developer verification

From a source checkout containing this plugin:

```bash
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
python3 plugins/workflow-observer/scripts/workflow_observer_cli.py integrity
```

The release archive also contains the frozen 20 forward and 8 lifecycle
manifests, repository evaluator/harness tests, specifications, plans, historical
failure evidence, and a machine-readable completeness inventory. Model-bearing
evaluations are not required for ordinary installation.

The plugin does not send observation data off-device or change workflows
without explicit user approval.
