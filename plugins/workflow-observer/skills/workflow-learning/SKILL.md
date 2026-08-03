---
name: workflow-learning
description: Use only when a user asks to analyze existing Workflow Observatory records or requests a workflow-learning review.
---

# Workflow Learning

Perform an on-demand, read-only analysis of observation evidence by publishing
one immutable, sanitized Learning Snapshot. Do not run this skill as part of
each observed task, mutate observation evidence, or change a workflow.

## Publish one bounded snapshot

1. Resolve `../../scripts/workflow_observer_cli.py` relative to this `SKILL.md`
   and invoke it as an argv array. Use the adapter selected by Workflow
   Observatory configuration; do not bypass the CLI or silently fall back.
2. Require explicit `--since`, `--until`, and `--timezone` values; ask the user
   to choose any value the request does not establish. Add only the narrowest
   applicable `--project`, `--workspace`, `--workspace-id`, or `--task-type`
   filters from the request.
3. For bounded learning, run only `snapshot`:

```text
python3 <resolved-cli-path> snapshot --since <YYYY-MM-DD> --until <YYYY-MM-DD> --timezone <IANA-timezone> [filters]
```

The `snapshot` operation performs the canonical `snapshot-input` acquisition,
Data Trust Gate, deterministic analysis, stable-read verification, and
publication. Do not invoke `snapshot-input` as a separate analysis path. If
the CLI fails, report its normalized failure and stop.

4. Read only the canonical JSON response returned by `snapshot`. Its
   `snapshot` value is the validated sanitized artifact. Do not parse observation records,
   invalidations, policy files, or adapter storage. Do not run or parse human `report`
   output.

The local immutable copy is stored under
`$WORKFLOW_OBSERVATORY_HOME/learning/snapshots/<snapshot_id>.json`; without the
environment override, the home is `~/.codex/workflow-observatory`. This path is
for locating the published artifact, not for bypassing the CLI contract.

## Present observational evidence

Report the exact `snapshot_id`, bounded query and policy identities from the
Learning Snapshot, then list its candidates as unranked observational
evidence. Preserve each exact `candidate_id`, evidence fields, denominators,
missingness, and policy references. Do not reorder candidates into a ranking.
Do not name a winning workflow or add evidence not present in the artifact.

Do not claim causality, effectiveness, or generality beyond the exact snapshot.
Do not recommend or create an Evolution Proposal, edit a workflow or skill,
or use network services. Do not create a branch or pull request or start an experiment.
Do not start, finish, invalidate, edit, or publish observations.
