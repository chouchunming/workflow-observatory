# Workflow Observatory

Workflow Observatory is a local-first Codex marketplace for privacy-minimized
workflow observation, evidence-based learning, and user-approved improvement.
It is MIT licensed and does not require the author's LLM Wiki or filesystem.

The marketplace currently contains one plugin, `workflow-observer`, with four
skills:

- `workflow-observer`: automatic eligibility and parent-lifecycle router.
- `workflow-telemetry`: schemas, privacy limits, secure payloads, and adapters.
- `workflow-learning`: bounded, reproducible Learning Snapshots over validated
  local evidence.
- `workflow-improving`: inspection of one user-selected snapshot/candidate
  pair, stopping before proposal design or mutation.

Observation data stays local by default under
`~/.codex/workflow-observatory/`. The plugin never records full prompts,
transcripts, credentials, or secrets, and never changes a workflow
automatically.

## Workflow Evolution Foundation v0.2

Workflow Evolution Foundation v0.2, including Task 11's 15-case matrix and
bounded fake-store historical dry run, is complete. These capabilities are
included in public prerelease
[`v0.2.0-rc1`](https://github.com/chouchunming/workflow-observatory/releases/tag/v0.2.0-rc1).
See also the release SHA (`workflow-observatory-0.2.0-rc1-65ec366.zip.sha256`)
published with the archive.

The current stable GitHub release is v0.1.0; v0.2.0-rc1 is a public prerelease.
The GitHub source-install commands below use the repository's published default
branch. They do not install the unreleased Phase 1 checkpoint from this design
branch.

The bounded milestone extended the existing observation layer through this
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
ended in a 15-case non-model acceptance matrix against isolated fake stores.

`snapshot-input` is the adapter-neutral, canonical machine-readable acquisition
boundary. `snapshot` reruns that acquisition for a stable-read check, performs
deterministic analysis, and publishes one immutable Learning Snapshot beneath
`$WORKFLOW_OBSERVATORY_HOME/learning/snapshots/<snapshot_id>.json` (by default,
`~/.codex/workflow-observatory/learning/snapshots/<snapshot_id>.json`). Human
`report` output is not learning input.

Schema-v1 observations remain unchanged and readable. Episode v2 is optional
and may be selected only with sanitized structured measurements and an
applicable explicit workflow generation; unavailable token or cost data is
never fabricated or converted to zero. Every result-affecting policy and
registry, plus the canonicalizer and analyzer code identities, is closed into
the snapshot identity. Candidate rows are deterministic, unranked,
non-authoritative observational evidence—not causal findings or permission to
change a workflow.

All v0.2 learning and publication tests use temporary fake portable or fake LLM
Wiki roots. They do not analyze the user's live store. The source archive
inventory includes the immutable policy JSON files, JCS conformance fixture,
and the exact approved v0.2 design and implementation plan as current
marketplace documentation; it does not relabel them as historical `evidence/`.

`episode-projection@2` fixes `runtime_provenance` to JSON null. v0.2 neither
infers runtime identities nor emits runtime-heterogeneity exclusions or
candidates. Null means runtime unavailable, not a shared runtime, and does not
split cohorts. Bounded runtime provenance and heterogeneous-runtime analysis
are deferred to a separately approved `episode-projection@3` contract.

Evolution Proposal artifacts, experiment execution, formal acceptance,
post-hoc evaluation inputs, and automatic workflow mutation remain deferred.
A future proposal must cite both `snapshot_id` and `candidate_id`, and no live
data baseline will run without separate approval.

Review the approved
[design](https://github.com/chouchunming/workflow-observatory/blob/eaa09257e4c1a774aa627286f3dcd6b1928c7dbe/docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md)
and
[amended implementation plan](https://github.com/chouchunming/workflow-observatory/blob/53780705ec878af9ad6cde14358121f8ebcb1205/docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md).

## Workflow Observatory v0.3 Phase 1 checkpoint

Phase 1 is an unreleased implementation checkpoint, not a v0.3 release or
publication.
Its commit-pinned implementation baseline is
`53d45af5344dc5fc231723802dad70fa5a0b564a`.
It implements the explicit artifact schema registry and policies, pure derived
migrations, the exact invalidation v2 writer with legacy reads, Snapshot Input
and Learning Snapshot v2 zero-sampling semantics, v1/v2 Learning Snapshot
readback and publication dispatch, and the fixed 12-case acceptance matrix.

Phase 1 preserves existing observation v1, invalidation v1, and Learning
Snapshot v1/v2 artifact bytes byte-for-byte. Readback and pure derived
migrations never rewrite those artifacts in place.
Zero-sampling fields are schema semantics only: they state that no record/sample
selection policy ran; Phase 1 does not select, retain, drop, or sample records.

The checkpoint does not implement the cooperative lock/CAS/maintenance
transaction, a durable health-event sink/store/reporting path,
retention/export/delete/restore/purge operations, observation v3 or sampling
decisions, or a Windows lock backend.

- macOS runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Linux native runtime verification: completed for the Phase 1 matrix on CPython 3.11–3.14.
- Windows backend/runtime verification: not implemented or run; Windows support is not certified.

The executable contract is the
[fixed 12-case schema migration acceptance test](plugins/workflow-observer/tests/test_schema_migration_acceptance.py).
Roadmap and backlog items describe future design work; they are not
implementation authority.
The next design unit is the Phase 2 cross-platform same-machine writer-safety
plan. Phase 1 does not authorize Phase 2 code.

### Phase 1 verification boundary

Run the macOS gates from a clean checkout of one reviewed checkpoint whose
history contains the implementation baseline:

```bash
test "$(git merge-base 53d45af5344dc5fc231723802dad70fa5a0b564a HEAD)" = \
  53d45af5344dc5fc231723802dad70fa5a0b564a
for py in 3.11 3.12 3.13 3.14; do
  caffeinate -i -m uv run --no-project --python "$py" \
    python -m unittest \
    plugins/workflow-observer/tests/test_schema_migration_acceptance.py -v
done
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py' -v
mkdir -p evidence/dist/phase1-verify-a evidence/dist/phase1-verify-b
rm -f evidence/dist/phase1-verify-{a,b}/workflow-observatory-0.3.0-phase1-verify.zip
caffeinate -i -m python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-a/
caffeinate -i -m python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/
cmp evidence/dist/phase1-verify-a/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/workflow-observatory-0.3.0-phase1-verify.zip
```

Linux certification must use a native Linux environment and an explicitly
pinned reviewed checkpoint. It remains pending; these commands are the gate,
not a claim that it has run:

```bash
PHASE1_CANDIDATE_COMMIT="${PHASE1_CANDIDATE_COMMIT:?set reviewed checkpoint SHA}"
git switch --detach "$PHASE1_CANDIDATE_COMMIT"
test "$(git rev-parse HEAD)" = "$PHASE1_CANDIDATE_COMMIT"
test "$(git merge-base 53d45af5344dc5fc231723802dad70fa5a0b564a HEAD)" = \
  53d45af5344dc5fc231723802dad70fa5a0b564a
uname -a
python3 --version
uv --version
for py in 3.11 3.12 3.13 3.14; do
  uv run --no-project --python "$py" python -m unittest \
    plugins/workflow-observer/tests/test_schema_migration_acceptance.py -v
done
python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py' -v
mkdir -p evidence/dist/phase1-verify-a evidence/dist/phase1-verify-b
rm -f evidence/dist/phase1-verify-{a,b}/workflow-observatory-0.3.0-phase1-verify.zip
python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-a/
python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/
cmp evidence/dist/phase1-verify-a/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/workflow-observatory-0.3.0-phase1-verify.zip
```

Pass criterion: 12/12 on every requested interpreter with no skip, the complete
plugin suite with no failure, and two exact-inventory archives with
byte-identical ZIP bytes.

Write platform evidence outside the package inventory at
`evidence/dist/phase1-acceptance/<platform>/<candidate-commit>/`. Record the
candidate commit, implementation baseline, timestamp, distribution, kernel,
architecture, Python versions, exact commands, test counts, skips, failures,
and exit statuses, archive filenames and SHA-256 hashes, and byte-comparison
result. That ignored local directory may be copied to independently published
acceptance evidence; it is not part of the release archive.

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

Removal does not delete the portable observation store. This release provides
no product delete or export operation. Manual filesystem deletion or copying is
an operator action, not a product delete/export operation.

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

A ZIP archive hash must be recorded outside the ZIP in local or independently
published acceptance evidence; `SHA256SUMS.json` authenticates members but
cannot authenticate its containing archive.

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
