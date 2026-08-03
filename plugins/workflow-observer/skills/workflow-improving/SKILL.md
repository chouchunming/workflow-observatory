---
name: workflow-improving
description: Use only when a user explicitly asks to inspect one selected Workflow Observatory learning candidate.
---

# Workflow Improving

Inspect one user-selected candidate from one immutable Learning Snapshot. This
skill is explicit-request only. It does not run for each task, perform a new
analysis, or modify observation records.

## Require one exact evidence pair

Require the user to select both identifiers in this exact form:

```text
snapshot_id= followed by exactly 64 lowercase hexadecimal characters
candidate_id= followed by exactly 64 lowercase hexadecimal characters
```

Reject missing, uppercase, shortened, prefixed, or additional identifier
values. Do not choose either identifier for the user and do not substitute a
candidate from memory or a different snapshot.

## Verify membership

Use the sanitized Learning Snapshot returned by `workflow-learning`, or its
immutable local artifact, and verify all of the following before responding:

- the artifact's exact `snapshot_id` equals the selected `snapshot_id`;
- one and only one entry in `core.candidates` has the selected `candidate_id`;
- the candidate exists in that exact Learning Snapshot and its evidence,
  denominator, missingness, policy, and cohort fields are preserved unchanged.

Do not parse raw observation records, rerun learning, rank candidates, or join
other evidence. If validation or membership fails, report the mismatch and
stop.

## Return evidence and stop

Return the selected pair and the candidate's exact observational evidence with
its uncertainty and policy boundary. State clearly: this inspection does not create an Evolution Proposal;
proposal design and creation remain deferred.

Do not claim causality. Do not declare a winner. Stop after the evidence inspection.
Do not edit a workflow or skill. Do not create a branch or pull request or start an experiment.
Do not publish data or apply any candidate. A v0.2 candidate grants no
mutation, execution, proposal, experiment, or publication authority.
