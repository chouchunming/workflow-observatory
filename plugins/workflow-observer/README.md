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

Schema v1 remains readable and is the default recording format. Telemetry may
opt into the private Episode v2 supplement only when sanitized structured
measurements and an applicable explicit workflow generation are available; it
never invents token or cost values.

## Reproducible learning boundary

`snapshot-input` produces one canonical adapter-neutral input over an explicit
`--since`, `--until`, and `--timezone` interval. `snapshot` performs the stable
A/B acquisition check, deterministic policy-bound analysis, and immutable
publication. The local artifact is
`$WORKFLOW_OBSERVATORY_HOME/learning/snapshots/<snapshot_id>.json`, defaulting to
`~/.codex/workflow-observatory/learning/snapshots/<snapshot_id>.json`.

Learning reads only the sanitized `snapshot` response, not raw records or human
`report` output. Every result-affecting policy and registry and the analyzer and
canonicalizer code identities are included in the snapshot closure. Candidates
are stable, unranked observational evidence. Improving requires the user to
select both an exact `snapshot_id` and `candidate_id`, verifies membership, and
stops before Evolution Proposal design, workflow edits, branches, pull
requests, or experiments.

## Workflow Observatory v0.3 Phase 1 checkpoint

Phase 1 is an implementation checkpoint, not a v0.3 release or publication.
It implements the explicit artifact schema registry and policies, pure derived
migrations, the exact invalidation v2 writer with legacy reads, Snapshot Input
and Learning Snapshot v2 zero-sampling semantics, v1/v2 Learning Snapshot
readback and publication dispatch, and the fixed 12-case acceptance matrix.

The checkpoint does not implement the cooperative lock/CAS/maintenance
transaction, a durable health-event sink/store/reporting path,
retention/export/delete/restore/purge operations, observation v3 or sampling
decisions, or a Windows lock backend.

- macOS runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Linux native runtime verification: pending; Linux support is not yet certified.
- Windows backend/runtime verification: not implemented or run; Windows support is not certified.

The next design unit is the Phase 2 cross-platform same-machine writer-safety
plan. Phase 1 does not authorize Phase 2 code.

## Developer verification

From a source checkout containing this plugin:

```bash
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
python3 plugins/workflow-observer/scripts/workflow_observer_cli.py integrity
```

The v0.2 suites create only isolated temporary fake portable and fake LLM Wiki
roots. Do not point learning tests at a live observation store. The source
archive allowlist includes only direct policy `.json` files, the JCS
conformance fixture, and the exact approved v0.2 design and plan in addition to
the existing package surface.

The release archive also contains the frozen 20 forward and 8 lifecycle
manifests, repository evaluator/harness tests, specifications, plans, historical
failure evidence, and a machine-readable completeness inventory. Model-bearing
evaluations are not required for ordinary installation.

The plugin does not send observation data off-device. v0.2 snapshots grant no
authority to change workflows, create proposals, open branches or pull
requests, or execute experiments.
