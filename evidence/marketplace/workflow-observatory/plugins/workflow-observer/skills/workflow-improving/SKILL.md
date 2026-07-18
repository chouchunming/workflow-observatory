---
name: workflow-improving
description: Use only when a user explicitly asks for an improvement proposal based on cited Workflow Observatory learning output.
---

# Workflow Improving

Turn an evidence-linked learning result into a proposal for user consideration.
This skill is explicit-request only. It does not run for each task and does not
collect, reinterpret, or modify observation records.

## Check the evidence boundary

1. Consume cited workflow-learning output supplied or identified by the user.
   Do not substitute uncited memory, raw record access, or a new analysis.
2. Confirm the learning output cites its observation groups and separates
   observed counts from inference. If it does not, stop and request a valid
   workflow-learning result.
3. For every motivating group, cite observation group keys exactly: project,
   workspace, workspace ID, task type, and workflow variant. Also cite its
   comparable final record count and time window.
4. Preserve the learning output's missing-data notes and uncertainty. A small
   sample may motivate further measurement, but it is not evidence of a trend.

## Propose one bounded option

Propose exactly one bounded change or experiment. Do not provide a menu of
changes and do not bundle independent interventions. Use this form:

```markdown
## Evidence
- Exact group key(s): ...
- Comparable final record count(s): ...
- Time window(s): ...
- Observed counts: ...
- Inference and uncertainty: ...

## One proposed change or experiment
- Change: one reversible, narrowly scoped intervention
- Scope: affected workflow and excluded work
- Measurement: baseline, metric, comparison window, and decision threshold
- Rollback: exact condition and steps that restore the prior state
- Uncertainty: plausible alternatives and limits of the evidence

## Approval gate
- No action has been taken. Explicit user approval is required to proceed.
```

Do not claim causality from observational evidence. Do not declare a winner or
represent the proposal as proven. The measurement must compare like-for-like
groups and the rollback must be practical before the experiment begins.

## Stop before action

Stop after presenting the proposal. Require explicit user approval before any
edit, run, experiment, or publish action, including editing a skill, changing a
workflow, starting the proposed experiment, or publishing observation data.
Before any edit, run, experiment, or publish action, obtain fresh explicit user approval for that specific action.
Never apply the proposal on your own. Approval for analysis is not approval for
mutation, execution, or publication.
