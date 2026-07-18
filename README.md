# Workflow Observatory

Workflow Observatory is a local-first Codex marketplace for privacy-minimized
workflow observation, evidence-based learning, and user-approved improvement.
It is MIT licensed and does not require the author's LLM Wiki or filesystem.

The marketplace currently contains one plugin, `workflow-observer`, with four
skills:

- `workflow-observer`: automatic eligibility and parent-lifecycle router.
- `workflow-telemetry`: schemas, privacy limits, secure payloads, and adapters.
- `workflow-learning`: descriptive analysis after at least five comparable
  finalized records.
- `workflow-improving`: evidence-linked proposals and experiments that require
  explicit user approval before mutation.

Observation data stays local by default under
`~/.codex/workflow-observatory/`. The plugin never records full prompts,
transcripts, credentials, or secrets, and never changes a workflow
automatically.

## Install from GitHub

```bash
codex plugin marketplace add chouchunming/workflow-observatory
codex plugin add workflow-observer@workflow-observatory
```

Start a new Codex thread after installation so skill discovery uses the new
snapshot. During migration, compare the old `observing-workflows` contract with
the new observer/telemetry pair, prove one eligible trigger and one exclusion,
then disable the old automatic skill. Do not leave both automatic descriptions
enabled for normal work.

## Install an extracted release locally

```bash
codex plugin marketplace add /absolute/path/to/workflow-observatory
codex plugin add workflow-observer@workflow-observatory
```

Remove the plugin with:

```bash
codex plugin remove workflow-observer@workflow-observatory
```

Removal does not delete the portable observation store. Delete or export local
data separately according to your own retention policy.

## Storage

With no configuration file, the portable adapter writes beneath
`~/.codex/workflow-observatory/store/`. To choose another absolute local root,
create `~/.codex/workflow-observatory/config.json`:

```json
{
  "schema_version": 1,
  "adapter": "portable",
  "root": "/absolute/private/path/to/workflow-observatory-store"
}
```

An existing LLM Wiki can be selected explicitly:

```json
{
  "schema_version": 1,
  "adapter": "llmwiki",
  "wiki_root": "/absolute/path/to/llmwiki",
  "cli_path": "/absolute/path/to/llmwiki/wiki_cli.py"
}
```

The CLI must be a real file inside the configured Wiki root. Adapter failure is
reported; the plugin never silently falls back to a second store.

## Verification evidence

Release archives contain:

- the installable marketplace and full plugin test suite;
- approved concept, specifications, superseding plans, and Task 6 report;
- repository observation, lifecycle, security, adapter, and frozen evaluator
  tests;
- `TODO.md`, `ROADMAP.md`, the parallel-evaluation plan, and the explicit
  composite release-acceptance boundary;
- `SHA256SUMS.json`, mapping every packaged file and repository evidence path
  to source and portable SHA-256 digests.

Author-specific paths are normalized only in archive copies. Production raw
inputs, observations, credentials, local configuration, caches, and temporary
payloads are excluded.

See [TODO.md](TODO.md), [ROADMAP.md](ROADMAP.md), and
[docs/release-acceptance.md](docs/release-acceptance.md) for the public work and
verification boundary, and
[plugins/workflow-observer/README.md](plugins/workflow-observer/README.md) for
plugin behavior and developer checks.

## Design, plan, and test evidence

The extracted release keeps reviewable repository evidence under `evidence/`:

- [Observation Records design](evidence/docs/superpowers/specs/2026-07-12-observation-records-design.md)
  and [Workflow Observatory marketplace design](evidence/docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md);
- [Observation Records v2 plan](evidence/docs/superpowers/plans/2026-07-13-observation-records-v2.md)
  and [marketplace implementation plan](evidence/docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md);
- [Task 6 acceptance history](evidence/.superpowers/sdd/workflow-observatory-task-6-report.md);
- [repository test suite](evidence/tests/) and the plugin-local
  [portable test suite](plugins/workflow-observer/tests/).

From an extracted release, reproduce the non-model gates with:

```bash
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
(cd evidence && python3 -m unittest discover -s tests -p 'test_*.py')
python3 evidence/scripts/package_workflow_observatory.py --verify \
  /absolute/path/to/workflow-observatory-0.1.0.zip
```
