---
name: workflow-learning
description: Use only when a user asks to analyze existing Workflow Observatory records or requests a workflow-learning review.
---

# Workflow Learning

Produce an on-demand, read-only description of validated Workflow Observatory
history. Do not run this skill as part of each observed task and do not mutate a
workflow or observation store.

## Read validated reports

1. Resolve `../../scripts/workflow_observer_cli.py` relative to this `SKILL.md`
   and invoke it with the current Python interpreter as an argv array. Use the
   adapter selected by Workflow Observatory configuration; do not bypass the
   CLI, silently fall back, or access record files directly.
2. Run `validate` first. If validation fails, report that failure and stop the
   learning review. Do not draw conclusions from an invalid store.
3. After successful validation, run only `report`, adding the narrowest
   applicable `--project`, `--workspace`, `--workspace-id`, `--task-type`, and
   `--status` filters supported by the request. Always use bounded `--since` and `--until` dates for a reproducible time window; ask the user to choose the review
   window if the request does not establish one.
4. Do not start, finish, invalidate, edit, or publish observations. Do not use
   network services. Treat sanitized report output as the only evidence.

## Build comparable groups

Use only final, non-invalidated records for every descriptive or inferential
output. The only permitted statuses are `success`, `partial`, `failed`, `rolled-back`, and `superseded`.

Exclude drafts from every observed count, status count, sample, comparison, rate, trend, and inference. Ignore every report draft count and draft summary completely; do not reproduce or discuss them. Exclude invalidated records from every observed count, status count, sample, comparison, rate, trend, and inference. Use the report's `final sample` count rather than its broader sample count when deciding whether a group is large enough.

Group records by the exact five-part group key: project, workspace, workspace
ID, task type, and workflow variant. Do not merge groups when any one key value
differs, including a workspace label associated with the same workspace ID.
Preserve the literal key values in the result so another reader can reproduce
the comparison.

The report heading groups on workspace ID and prints workspace labels on its
`Workspace:` line. If it represents more than one workspace label, rerun `report --workspace <exact-label>` separately for each label before counting or
inferring. If an exact label cannot be established, label the output
non-comparable and provide no inference.

Require at least 5 comparable final records in one exact group before
presenting a trend or recurring-pattern hypothesis. For every smaller group,
write `small sample (n=<count>)` and report descriptive counts only. Do not present a trend for a small sample and do not combine unlike groups to cross the
threshold.

## Report without overclaiming

For each exact group, use this separation:

```markdown
### Group
- Project: ...
- Workspace: ...
- Workspace ID: ...
- Task type: ...
- Workflow variant: ...
- Time window: ... through ...
- Comparable final records: n

### Observed counts
- Final-status counts: only success, partial, failed, rolled-back, and superseded
- Final-record metric counts: values and denominators for permitted final statuses only
- Missing final-record metric values and the small-sample label, when applicable

### Inference
- A cautious recurring-pattern hypothesis, or `None — descriptive counts only`
```

Keep Observed counts separate from Inference. State missing data and uncertainty.
If the validated report does not establish that a count or metric uses only
permitted final, non-invalidated records, omit it or label it unavailable.
Do not claim causality, workflow effectiveness, or generality beyond the exact
group and time window. Do not name a winning workflow, recommend a workflow
change, edit a skill, or initiate an experiment.
