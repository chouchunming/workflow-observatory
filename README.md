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

## Workflow Evolution Foundation v0.2

The v0.2 foundation design and implementation plan are approved as of
2026-08-02.
Implementation started on 2026-08-02; Tasks 1 and 2 are complete on the design
branch.
These capabilities are not part of the current released plugin.

The bounded milestone will extend the existing observation layer through this
evidence path:

```text
Episode v2
    -> canonical adapter snapshot-input
    -> Data Trust Gate
    -> immutable, content-addressed Learning Snapshot
    -> deterministic observational candidates
```

The milestone preserves the local-first privacy boundary and separates outcome
analysis from lifecycle health. It also defines stable-read checks, explicit
workflow generations, versioned analysis policies, per-metric missingness, and
Episode-level Decision Event support. Its 11 reviewable implementation tasks
end in a 15-case non-model acceptance matrix against isolated fake stores.

Evolution Proposal artifacts, experiment execution, formal acceptance,
post-hoc evaluation inputs, and automatic workflow mutation remain deferred.
A future proposal must cite both `snapshot_id` and `candidate_id`, and no live
data baseline will run without separate approval.

Review the approved
[design](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md)
and
[implementation plan](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md).

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

## Parallel evaluator boundary

The Marketplace evaluator keeps its existing preflight, single-case diagnostic,
serial discovery sweep, and default serial formal behavior when `--parallel` is
absent. Parallel execution is opt-in:

```text
run_marketplace_eval.py --parallel {diagnostic,discovery,formal} \
  --archive /absolute/path/to/original-release.zip \
  --expected-archive-sha256 <trusted-archive-sha256>
run_marketplace_eval.py --parallel discovery \
  --archive /absolute/path/to/original-release.zip \
  --expected-archive-sha256 <trusted-archive-sha256> \
  --resume-run-root /absolute/private/path/to/retained-run
```

Every parallel mode requires an externally trusted full archive SHA-256. Obtain
it from a trusted release descriptor or independent channel; the archive's own
`SHA256SUMS.json` cannot authenticate itself. The coordinator hashes the raw ZIP
bytes and compares the full expected and observed identities before invoking
the archive verifier or opening the ZIP, then seals both identities into the
epoch. Diagnostic and discovery remain non-authoritative, but they are not
exempt because they still execute archive-owned code.

`--archive` must name the original absolute, regular, non-symlink ZIP path.
Keep that release artifact outside the extracted tree; an extracted release is
not expected to contain a nested copy of itself.

The coordinator also verifies that every live coordinator/protocol source
matches its trusted archive member. Production workers start with isolated
Python imports rooted only in the captured snapshot and attest the complete
sealed evaluator identity before `lane-ready` or any model-capable work.

`--parallel diagnostic` runs only the fixed non-authoritative
`forward/3 reviewed-refactor` case through the reviewed coordinator/worker
path. It cannot select another case or persist results. Discovery and formal
continue to use the complete frozen 20+8 inventory.

Parallel diagnostic and discovery runs are non-authoritative: they pass no
result destinations and cannot claim the formal commit capability. Only a
validated `formal` epoch may use the coordinator-held ordered leases and consume
that one-shot capability to publish the paired result.

The deterministic 28-case no-model gate passed with one coordinator, four real
worker processes, 8/8/8/4 sealed lane coverage, no Codex invocation, and no
discovery writer call. This is protocol and isolation evidence, not a real-model
28/28 result. A real-model diagnostic, discovery sweep, and protected formal
epoch still require their separate review and approval gates.

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
export WORKFLOW_OBSERVATORY_EVAL_ARCHIVE=/absolute/path/to/original-release.zip
export WORKFLOW_OBSERVATORY_EVAL_ARCHIVE_SHA256='64-lowercase-hex-from-trusted-channel'
python3 -c 'import hashlib,os,pathlib; p=pathlib.Path(os.environ["WORKFLOW_OBSERVATORY_EVAL_ARCHIVE"]); expected=os.environ["WORKFLOW_OBSERVATORY_EVAL_ARCHIVE_SHA256"]; observed=hashlib.sha256(p.read_bytes()).hexdigest(); assert observed == expected, (expected, observed)'
python3 evidence/scripts/package_workflow_observatory.py --verify \
  "$WORKFLOW_OBSERVATORY_EVAL_ARCHIVE"
python3 -m unittest discover -s plugins/workflow-observer/tests -p 'test_*.py'
(cd evidence && python3 -m unittest discover -v -s tests -p 'test_*.py')
```

The exported digest must come from an independent trusted release descriptor or
channel; replace the quoted placeholder before running the commands. The
subsequent archive verifier checks structure and internal
consistency; it does not establish the archive's authenticity.
