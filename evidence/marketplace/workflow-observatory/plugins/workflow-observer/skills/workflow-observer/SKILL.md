---
name: workflow-observer
description: Use when Codex is about to perform substantial top-level implementation work involving multiple files, tests or lint, or at least two implementation steps; also use for a compile or inbox workflow that updates Wiki pages or generated catalogs.
---

# Workflow Observer

Create one privacy-minimized parent observation for each stable eligible scope.
This is the only automatic Workflow Observatory entry point; telemetry supplies
every recording detail.

## Route each stable scope

1. Decide eligibility once. Exclude chat, read-only or answer-only work,
   planning without implementation, a simple untested single-file edit, and
   any worker prompt containing `Observation managed by parent run`. Treat a
   compile or inbox workflow that updates Wiki pages or generated catalogs as
   eligible even though it is knowledge-base maintenance rather than software
   implementation.
   Treat an open-ended request to improve something if useful that names no
   specific change or validation requirement as uncertain. Default to no
   observation, and do not manufacture eligibility by voluntarily expanding it
   into multiple files or tests.
2. Read `../workflow-telemetry/SKILL.md` before the first real start. Follow its
   adapter, schema, privacy, payload, lifecycle, and cleanup contract exactly.
3. Before mutation, make one sanitized Scope payload and issue one start for
   each stable top-level scope. Each run receives at most one finish. A rejected
   start creates no run; continue the authorized task and disclose the recording
   failure.
4. On a successful start, retain only the run ID. Do not inspect the draft or run help after start. Do not issue payload-bearing probes.
5. Give every worker the exact marker
   `Observation managed by parent run <run-id>; do not start a child observation.`
   Require descendants to propagate it unchanged. Do not start a child observation. Aggregate worker evidence into the parent completion.
6. A material scope replacement is the only exception: start one replacement
   run first, then finish the prior run as `superseded` with
   `--superseded-by`. The replacement run uses the taxonomy and review
   requirement of the new authorized scope.
7. At the end, make one new bounded completion payload and finish the active run
   at most once with a truthful telemetry status. Disclose start or finish
   failure separately from the task outcome.

In controlled evaluation, complete the observation start before entering a
required fixture gate, while still entering that gate before the first task
mutation. Do not automatically retry a rejected payload-bearing call. Never
retry one by changing taxonomy, payload text, status, or adapter. Observation
failure does not expand authority or block the original task.
