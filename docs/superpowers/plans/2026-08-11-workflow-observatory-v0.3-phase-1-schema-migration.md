# Workflow Observatory v0.3 Phase 1 Schema and Migration Foundation Implementation Plan

Date: 2026-08-11
Status: Ready for technical review; implementation is not authorized
Design authority: `251495e93dbe705a9124fb6de2b7c123ea428651`

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `subagent-driven-development` (recommended) or `executing-plans` to implement
> this plan task-by-task. Use `test-driven-development` for every behavior
> change and `verification-before-completion` before each checkpoint.

**Goal:** Establish explicit artifact schema identities, the bounded health
event schema/enums required by later phases, and pure, auditable derived
migrations for existing observation, invalidation, and Learning Snapshot
artifacts without rewriting historical bytes or enabling later v0.3 writer,
health-recorder, retention, or sampling behavior.

**Architecture:** Add one immutable artifact-schema registry and one immutable
migration registry beside the existing v0.2 analysis policies. A small pure
module validates Markdown/JSON envelopes, dispatches exact legacy shapes, and
produces normalized derived artifacts bound to source bytes and migration
policy identity. A schema-only health-event policy freezes the initial event,
error, resource, and evidence enums but does not create or persist events.
Existing observation writers keep their v1/v2 modes. New invalidations become
explicit schema v2. Snapshot Input and Learning Snapshot advance to schema v2
so later sampling can represent policy-caused absence, while Phase 1 records no
sampling decision and retains every selected Episode.

**Tech Stack:** Python 3.11+ standard library, `unittest`, RFC 8785 JCS through
the existing `canonical_json.py`, descriptor-bound evidence reads already used
by the selected adapters, Markdown/YAML-compatible frontmatter, CodeGraph
1.5.0 as advisory impact analysis, and deterministic ZIP packaging tests.

## Global Constraints

- The approved design is
  `docs/superpowers/specs/2026-08-11-workflow-observatory-concurrency-operability-v0.3-design.md`
  at commit `251495e93dbe705a9124fb6de2b7c123ea428651`.
- This plan covers Phase 1 only. It freezes health-event schema/enums, but does
  not authorize or implement native lock adapters, compare-and-swap writes,
  maintenance transactions, a health-event producer/sink/store, retention,
  export, delete, restore, purge, observation schema v3, or sampling decisions.
- Work in a linked worktree backed by the durable canonical clone. Do not let a
  temporary directory contain the only Git object database or only completed
  work.
- Run Task 0 once per linked worktree. Before every later task, run CodeGraph
  `sync`, inspect the exact symbols with `explore`, and run `affected` on the
  changed paths. CodeGraph is advisory; explicit source inspection and tests
  remain authoritative.
- On macOS, run every test, build, evaluation, and package operation as a direct
  child of `caffeinate -i -m`. Keep one separately tracked orchestration-scoped
  `caffeinate -i` assertion while a multi-step execution is active, and release
  it before hand-off or interruption.
- Use TDD. A missing import is RED-A only for a new module. Add the smallest
  interface necessary to obtain RED-B on the intended semantic assertion before
  implementing GREEN.
- Commit each independently reviewable task after focused verification. Before
  review, pause, hand-off, compaction, machine transfer, or worktree cleanup,
  push the WIP branch when separately authorized; otherwise create and verify a
  Git bundle on durable storage.
- Never modify historical observation, invalidation, Learning Snapshot, frozen
  marketplace evidence, or v0.1/v0.2 release bytes in place.
- Observation v1 and v2 writer behavior stays byte-compatible. Phase 1 adds no
  default observation v3 writer and performs no live-store migration.
- Preserve the existing v0.2 analysis-policy identity. The new artifact schema
  and migration policies are a separate immutable policy set; do not add them
  to `policy_artifacts._POLICY_FILES` or silently change v0.2 snapshot identity.
- Every migration is pure: no clock, environment, Git state, network, mutable
  global, unbound path, or unrestricted human-text input.
- Use only fake Portable and fake LLMWiki roots under `TemporaryDirectory`.
  Tests must not inspect or mutate the user's live store.
- Python 3.11 is the minimum runtime. Run the fixed Phase 1 acceptance class on
  CPython 3.11, 3.12, 3.13, and 3.14. Run the full repository regression suite
  on the primary runtime.
- Formal Phase 1 acceptance is one clean run on one frozen commit. Targeted
  reruns are diagnostics and do not combine into formal acceptance.
- Do not push, merge, publish a release, or alter GitHub state without separate
  user authorization.

## Program Phase Boundaries

| Phase | Deliverable | Entry gate | Exit gate |
|---|---|---|---|
| 1 | Explicit schema registry, pure migration, Snapshot v2 compatibility | This plan approved and execution separately authorized | Fixed 12-case matrix, full regression, package inventory, adversarial review |
| 2 | Same-machine lock/CAS/maintenance safety plus typed notifications to an injected non-durable health sink | Phase 1 accepted; separate Phase 2 plan approved | Native POSIX tests, injected Windows contract, bounded real-process schedule and crash matrix |
| 3 | Immutable health evidence | Phase 2 accepted; event source contract reviewed; separate plan approved | No-recursion durable sink, stable aggregation, persisted Phase 2 fault cases |
| 4 | Retention/export/delete/restore/purge | Phase 3 accepted; lifecycle policy and confirmation UX approved | Deterministic export, reversible delete/restore, drift-safe confirmed purge and receipts |
| 5 | Value-based tail sampling | Phase 4 accepted; sampling policy and denominator semantics approved | Forced-retain matrix, deterministic selection, cleanup safety, Learning Snapshot sampling semantics |

Later phases may extend the registry created here only with new immutable
registry versions and focused compatibility plans. They may not edit the Phase
1 policy bytes under an existing identity.

## File and Responsibility Map

**Create in Phase 1:**

- `plugins/workflow-observer/policies/artifact_schema_registry.json` — exact
  artifact type/version dispatch and schema identities.
- `plugins/workflow-observer/policies/artifact_migration_registry.json` — exact
  source-to-target derived-migration identities and handler names.
- `plugins/workflow-observer/policies/health_event_schema.json` — exact
  health-event v1 envelope, event types, resource/error enums, evidence keys,
  limits, and hash domain for later phases; no recorder implementation.
- `plugins/workflow-observer/scripts/artifact_schema.py` — immutable policy
  loader, Markdown envelope parser, exact artifact classifier, JSON envelope
  validator, and normalized availability validation.
- `plugins/workflow-observer/scripts/artifact_migration.py` — pure migration
  dispatcher and content-addressed derived artifact.
- `plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json` —
  language-neutral source bytes, policy identities, canonical projections, and
  expected hashes.
- `plugins/workflow-observer/tests/test_artifact_schema.py`
- `plugins/workflow-observer/tests/test_artifact_migration.py`
- `plugins/workflow-observer/tests/test_schema_migration_acceptance.py` — fixed
  12-case Phase 1 acceptance matrix.

**Modify in Phase 1:**

- `plugins/workflow-observer/scripts/wiki_observations.py` — call the central
  classifier, preserve v1/v2 observations, and write explicit invalidation v2.
- `plugins/workflow-observer/scripts/episode_schema.py` — expose validated
  legacy/v2 observation projections to the migration dispatcher without a
  second parser.
- `plugins/workflow-observer/scripts/snapshot_input.py` — emit schema-v2
  semantic bundles, artifact-policy closure, and migration manifests.
- `plugins/workflow-observer/scripts/learning_snapshot.py` — build exact
  Learning Snapshot core v2 with sampling-ready zero-state fields.
- `plugins/workflow-observer/scripts/snapshot_store.py` — publish/read both
  Learning Snapshot v1 and v2 by exact schema identity and domain separator.
- `plugins/workflow-observer/scripts/workflow_observer_cli.py` — keep existing
  commands while exposing schema-v2 invalidation output and Snapshot v2.
- `plugins/workflow-observer/scripts/core_source.json` — refresh only after the
  last lifecycle-core modification.
- `plugins/workflow-observer/tests/test_episode_v2.py`
- `plugins/workflow-observer/tests/test_portable_cli.py`
- `plugins/workflow-observer/tests/test_adapter_conformance.py`
- `plugins/workflow-observer/tests/test_snapshot_input.py`
- `plugins/workflow-observer/tests/test_learning_snapshot.py`
- `plugins/workflow-observer/tests/test_snapshot_publication.py`
- `plugins/workflow-observer/tests/test_core_parity.py`
- `plugins/workflow-observer/tests/test_package_archive.py`
- `evidence/scripts/package_workflow_observatory.py` — exact paths for approved
  v0.3 documents, policies, migration fixture, modules, and tests.
- `plugins/workflow-observer/README.md`, `README.md`, `ROADMAP.md`, and
  `TODO.md` — implemented/deferred/platform status only after tests pass.

## Required Interfaces and Exact Contracts

`artifact_schema.py` exports:

```python
@dataclass(frozen=True)
class ArtifactSchemaRef:
    artifact_type: str
    schema_version: int
    schema_identity: str
    reader_contract: str
    writer_contract: str

@dataclass(frozen=True)
class MarkdownEnvelope:
    metadata: Mapping[str, object]
    body: str
    artifact: ArtifactSchemaRef

@dataclass(frozen=True, init=False)
class ArtifactPolicySet:
    @property
    def schema_registry(self) -> Mapping[str, object]: ...
    @property
    def migration_registry(self) -> Mapping[str, object]: ...
    def identities(self) -> Mapping[str, Mapping[str, str]]: ...

def load_artifact_policy_set(policy_root: Path) -> ArtifactPolicySet: ...

def parse_markdown_envelope(
    text: str,
    *,
    expected_human_type: str,
    policies: ArtifactPolicySet,
) -> MarkdownEnvelope: ...

def classify_json_artifact(
    value: object,
    *,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
) -> ArtifactSchemaRef: ...

def validate_health_event_document(
    value: object,
    *,
    policies: ArtifactPolicySet,
    require_digest: bool,
) -> Mapping[str, object]: ...
```

The ellipses above specify signatures only; production functions must contain
complete validated behavior. `ArtifactPolicySet` deep-freezes validated
documents, returns defensive copies, and binds these exact Phase 1 identities:

```text
artifact-schema-registry@1
sha256:1bba0c5635ed2cedf4885861243947c89d3f9ba98e358b049ff3a61c0a40e7d6

artifact-migration-registry@1
sha256:0c6bbdb88de176725c065f885a4393b73db19ee769f04f486ade013121e0fe90

health-event-schema@1
sha256:5abab8b18858e95535b31185eb65d273e8ec4758034e5cfe85492528fcaba516
```

These digests were recomputed from the exact root keys, array orders, rows,
and scalar values below using the approved current `canonical_json.py` during
plan authoring. Task 2 freezes the same values in tests before production code
may consume them.

The schema registry has exact root keys `artifact_type`, `schema_version`,
`registry_version`, and `schemas`. Its schema rows, in this exact array order,
are:

| artifact type | version | schema identity | reader contract | writer contract |
|---|---:|---|---|---|
| `artifact-migration-registry` | 1 | `artifact-migration-registry@1` | `strict-canonical-json` | `immutable-policy` |
| `artifact-schema-registry` | 1 | `artifact-schema-registry@1` | `strict-canonical-json` | `immutable-policy` |
| `derived-artifact` | 1 | `derived-artifact@1` | `strict-canonical-json` | `derived-only` |
| `episode-projection` | 2 | `episode-projection@2` | `strict-canonical-json` | `derived-only` |
| `health-event` | 1 | `health-event@1` | `strict-canonical-json` | `disabled-until-phase-3` |
| `health-event-schema` | 1 | `health-event-schema@1` | `strict-canonical-json` | `immutable-policy` |
| `learning-snapshot` | 1 | `learning-snapshot@1` | `legacy-exact-shape` | `legacy-read-only` |
| `learning-snapshot` | 2 | `learning-snapshot@2` | `strict-canonical-json` | `new-default` |
| `learning-snapshot-core` | 2 | `learning-snapshot-core@2` | `strict-canonical-json` | `embedded-only` |
| `observation-invalidation` | 1 | `observation-invalidation@1` | `legacy-exact-shape` | `legacy-read-only` |
| `observation-invalidation` | 2 | `observation-invalidation@2` | `explicit-markdown-envelope` | `new-default` |
| `snapshot-input` | 1 | `snapshot-input@1` | `legacy-exact-shape` | `legacy-read-only` |
| `snapshot-input` | 2 | `snapshot-input@2` | `strict-canonical-json` | `new-default` |
| `workflow-observation` | 1 | `workflow-observation@1` | `legacy-exact-shape` | `explicit-v1-compatibility` |
| `workflow-observation` | 2 | `workflow-observation@2` | `explicit-episode-v2` | `explicit-v2-compatibility` |

The migration registry has exact root keys `artifact_type`, `schema_version`,
`registry_version`, and `migrations`. Its rows, in this exact array order, are:

| source | target | migration identity | handler |
|---|---|---|---|
| `learning-snapshot@1` | `learning-snapshot-core@2` | `learning-snapshot-v1-to-core-v2@1` | `learning-snapshot-v1` |
| `observation-invalidation@1` | `observation-invalidation@2` | `observation-invalidation-v1-to-v2@1` | `observation-invalidation-v1` |
| `workflow-observation@1` | `episode-projection@2` | `workflow-observation-v1-to-episode-projection@1` | `workflow-observation-v1` |
| `workflow-observation@2` | `episode-projection@2` | `workflow-observation-v2-to-episode-projection@1` | `workflow-observation-v2` |

Each migration row has exact keys `source_artifact_type`,
`source_schema_version`, `target_contract`, `target_schema_version`,
`migration_identity`, and `handler`. Every source and target must resolve to a
schema-registry identity; projection/embedded-only targets are registered but
can never be mistaken for independently writable artifacts.

`health_event_schema.json` has exact root keys `artifact_type`,
`schema_version`, `schema_identity`, `event_schema_identity`, `event_types`,
`error_classes`, `resource_kinds`, `limits`, and `hash_domain`. Its fixed
scalar values are:

```json
{
  "artifact_type": "health-event-schema",
  "schema_version": 1,
  "schema_identity": "health-event-schema@1",
  "event_schema_identity": "health-event@1",
  "error_classes": [
    "conflict", "integrity", "state", "timeout", "unsupported", "validation"
  ],
  "resource_kinds": [
    "artifact", "export", "health", "learning-snapshot", "maintenance",
    "observation", "observation-invalidation", "purge", "sampling", "store"
  ],
  "limits": {
    "identifier_max_utf8_bytes": 128,
    "integer_max": 9007199254740991
  },
  "hash_domain": "workflow-observatory:health-event:v1"
}
```

Its `event_types` rows stay in the approved design order and bind these exact
evidence schemas; the named fields are required and no other evidence field is
allowed:

```json
{
  "event_type": "cas-conflict",
  "evidence": {
    "required": ["attempt"],
    "properties": {"attempt": {"type": "positive-integer"}}
  }
}
```

| event type | evidence field | value contract |
|---|---|---|
| `validation-rejected` | `reason_code` | enum `artifact`, `policy`, `reference`, `request` |
| `schema-mismatch` | `expected_schema`, `observed_schema` | bounded identifiers |
| `duplicate-finish` | `attempt` | positive integer |
| `record-dropped` | `reason_code` | enum `invalid`, `storage`, `unsupported` |
| `payload-cleanup-failed` | `attempt` | positive integer |
| `lock-contended` | `wait_milliseconds` | non-negative integer |
| `lock-timeout` | `timeout_milliseconds` | positive integer |
| `stale-owner-recovered` | `metadata_present` | Boolean |
| `cas-conflict` | `attempt` | positive integer |
| `maintenance-lease-timeout` | `timeout_milliseconds` | positive integer |
| `maintenance-recovery-blocked` | `reason_code` | enum `conflict`, `corrupt-staging`, `durability`, `missing-staging`, `multiple-active` |
| `sampling-decision-failed` | `reason_code` | enum `cleanup`, `policy`, `validation` |
| `export-aborted` | `reason_code` | enum `drift`, `integrity`, `validation` |
| `purge-aborted` | `reason_code` | enum `drift`, `expired`, `integrity`, `validation` |

A future health event has exactly the envelope keys approved by the design:
`artifact_type`, `schema_version`, `event_id`, `occurred_at`, `event_type`,
`operation_id`, `run_id`, `resource_kind`, `resource_key`, `evidence`,
`error_class`, `policy_identity`, and `event_sha256`. Phase 1 validates supplied
documents and the digest relationship only; it does not choose IDs/timestamps,
publish events, report health, or invoke this validator from product failures.
The hash operation appends one NUL byte to the policy's `hash_domain` before
domain-separated SHA-256.

`artifact_migration.py` exports:

```python
@dataclass(frozen=True, init=False)
class DerivedArtifact:
    @property
    def canonical_document(self) -> Mapping[str, object]: ...
    @property
    def canonical_bytes(self) -> bytes: ...

def migrate_artifact(
    *,
    source_bytes: bytes,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
    observation_projection_policy: Mapping[str, object] | None = None,
) -> DerivedArtifact: ...
```

Every derived document has exact top-level keys:

```json
{
  "artifact_type": "derived-artifact",
  "schema_version": 1,
  "source": {
    "artifact_type": "workflow-observation",
    "schema_version": 1,
    "source_sha256": "..."
  },
  "migration": {
    "migration_identity": "workflow-observation-v1-to-episode-projection@1",
    "migration_registry_sha256": "sha256:..."
  },
  "target": {
    "contract": "episode-projection@2",
    "schema_version": 2,
    "value": {}
  }
}
```

`canonical_document` is recursively immutable internally and defensively
copied on access. `canonical_bytes` is computed once with the existing RFC
8785 canonicalizer. The source hash is SHA-256 over the exact input bytes.

## Task 0: Per-Worktree Execution Preflight

- [ ] Verify the durable branch and current worktree.

```bash
git rev-parse --show-toplevel
git rev-parse --git-common-dir
git status --short --branch
```

Expected: the top-level is the linked v0.3 worktree, the common Git directory
belongs to the durable canonical clone rather than a temporary directory, and
no unexpected product-code edits exist.

- [ ] Initialize or refresh CodeGraph and keep it advisory.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 --version
if [ ! -d .codegraph ]; then
  npx --yes @colbymchenry/codegraph@1.5.0 init
else
  npx --yes @colbymchenry/codegraph@1.5.0 sync .
fi
npx --yes @colbymchenry/codegraph@1.5.0 status
git check-ignore -q .codegraph
```

Expected: version `1.5.0`, a complete local index when supported, and ignored
`.codegraph/`. If indexing is unavailable, record the limitation and use `rg`
plus tests; do not weaken any test gate.

- [ ] Start one tracked orchestration assertion before execution and record its
  PID in the execution log.

```bash
caffeinate -i &
ORCHESTRATION_CAFFEINATE_PID=$!
kill -0 "$ORCHESTRATION_CAFFEINATE_PID"
```

Expected: `kill -0` succeeds. The integrator must terminate this exact PID at
every stop or final hand-off.

## Task 1: Close the Exact Package Gate for Approved Phase 1 Sources

**Files:**

- Modify: `evidence/scripts/package_workflow_observatory.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`

- [ ] Refresh advisory impact context.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore _marketplace_allowed_files
npx --yes @colbymchenry/codegraph@1.5.0 explore build_archive
```

Expected: the packager and package test are the primary affected files. If
CodeGraph cannot resolve Python symbols, use `rg -n` to inspect the exact
allowlist and continue with the explicit tests.

- [ ] Add a focused RED test that requires the two exact approved Phase 1
  documents in the live archive inventory:

```python
def test_archive_contains_approved_v03_phase1_documents(self):
    build_archive(self.source, self.archive, self.evidence)
    marketplace = self._inventory()["marketplace_files"]
    self.assertIn(
        "docs/superpowers/specs/"
        "2026-08-11-workflow-observatory-concurrency-operability-v0.3-design.md",
        marketplace,
    )
    self.assertIn(
        "docs/superpowers/plans/"
        "2026-08-11-workflow-observatory-v0.3-phase-1-schema-migration.md",
        marketplace,
    )
```

- [ ] Run RED and retain the complete failure.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

Expected: failure reports the approved v0.3 design and/or plan as unexpected
marketplace files. No archive is accepted by a broad docs wildcard.

- [ ] Add only the two exact relative paths to the packager allowlist. Do not
  add `docs/superpowers/**`, modify frozen `evidence/marketplace/`, or alter the
  historical `SHA256SUMS.json`.

- [ ] Run GREEN and an allowlist adversarial case.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

Expected: all package tests pass, including an existing or new assertion that
an unlisted sibling Markdown file remains rejected.

- [ ] Inspect affected paths and checkpoint.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add evidence/scripts/package_workflow_observatory.py \
  plugins/workflow-observer/tests/test_package_archive.py
git commit -m "test: admit approved workflow schema foundation docs"
```

Expected: one documentation-packaging unit; no runtime artifact behavior has
changed.

## Task 2: Add the Immutable Artifact Policy Set

**Files:**

- Create: `plugins/workflow-observer/policies/artifact_schema_registry.json`
- Create: `plugins/workflow-observer/policies/artifact_migration_registry.json`
- Create: `plugins/workflow-observer/policies/health_event_schema.json`
- Create: `plugins/workflow-observer/scripts/artifact_schema.py`
- Create: `plugins/workflow-observer/tests/test_artifact_schema.py`
- Modify: `plugins/workflow-observer/tests/test_manifests.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`

- [ ] Refresh impact context.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore PolicySet
npx --yes @colbymchenry/codegraph@1.5.0 explore strict_json_loads
npx --yes @colbymchenry/codegraph@1.5.0 explore build_code_manifest
```

- [ ] Write RED-A tests importing `ArtifactPolicySet`,
  `load_artifact_policy_set`, `classify_json_artifact`, and
  `validate_health_event_document`. Add fixtures that assert all three exact
  policy versions/hashes printed in this plan and that the existing v0.2
  `PolicySet.core_identity()` is unchanged.

- [ ] Add RED-B adversarial assertions for:

```text
duplicate JSON key
unknown top-level key
unknown artifact type/version pair
schema identity inconsistent with type/version
migration source or target missing from schema registry
duplicate dispatch key
same policy version with changed bytes
constructor-input mutation
property-return mutation
registry symlink or unsafe relative path
health event with unknown event/error/resource enum
health event with missing, extra, or wrong-typed event-specific evidence
health event containing an unrestricted error/path/hostname/PID field
health event with stale or self-inconsistent event_sha256
```

- [ ] Run RED-A, add only the named interfaces, then run RED-B.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_schema.py -v
```

Expected RED-A: import/interface failure. Expected RED-B: the first semantic
policy assertion fails while the interface imports successfully.

- [ ] Implement the three canonical policy documents with exact-key schemas.
  Use integer schema versions, bounded ASCII identifiers, sorted unique
  entries, strict JSON ingress, RFC 8785 canonical bytes, and SHA-256-prefixed
  identities. `ArtifactPolicySet` must recursively freeze internal values and
  return defensive copies.

The schema registry must distinguish legacy recognition from new writers:

```json
{
  "artifact_type": "artifact-schema-registry",
  "schema_version": 1,
  "registry_version": "artifact-schema-registry@1",
  "schemas": [
    {
      "artifact_type": "workflow-observation",
      "schema_version": 1,
      "schema_identity": "workflow-observation@1",
      "reader_contract": "legacy-exact-shape",
      "writer_contract": "explicit-v1-compatibility"
    }
  ]
}
```

The complete sorted registry must contain only the Phase 1 entries listed in
Required Interfaces. The migration registry must bind every handler to one
source schema, one target contract, one migration identity, and one handler
name. Validate cross-document references before constructing the value object.
Validate the health schema's exact event order, evidence mini-schema, limits,
and enums before exposing it through `ArtifactPolicySet`.

- [ ] Implement health-event validation as a pure schema operation only. It
  accepts an already supplied mapping, checks the exact 13-key envelope,
  canonical UTC second timestamp, bounded opaque identifiers, nullable
  `run_id`, exact event-specific evidence, fixed resource/error/policy enums,
  and the domain-separated `event_sha256` relationship. It must not generate
  IDs/timestamps, read a store, write a file, or catch/report product failures.

- [ ] Ensure artifact policies do not alter v0.2 analysis identity.

```python
before = load_policy_set(...).core_identity()
load_artifact_policy_set(...)
after = load_policy_set(...).core_identity()
self.assertEqual(before, after)
```

- [ ] Add the exact new policy/module/test paths to manifest and package tests;
  policy directory wildcard behavior remains restricted to direct `.json`
  children and still rejects nested or non-JSON files.

- [ ] Run GREEN and regressions.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_policy_artifacts.py \
  plugins/workflow-observer/tests/test_manifests.py \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

Expected: all named tests pass and the existing v0.2 policy identity fixture is
byte-for-byte unchanged.

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/policies/artifact_schema_registry.json \
  plugins/workflow-observer/policies/artifact_migration_registry.json \
  plugins/workflow-observer/policies/health_event_schema.json \
  plugins/workflow-observer/scripts/artifact_schema.py \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_manifests.py \
  plugins/workflow-observer/tests/test_package_archive.py
git commit -m "feat: add immutable artifact schema policies"
```

## Task 3: Centralize Exact Markdown and JSON Artifact Classification

**Files:**

- Modify: `plugins/workflow-observer/scripts/artifact_schema.py`
- Modify: `plugins/workflow-observer/scripts/episode_schema.py`
- Modify: `plugins/workflow-observer/scripts/wiki_observations.py`
- Modify: `plugins/workflow-observer/tests/test_artifact_schema.py`
- Modify: `plugins/workflow-observer/tests/test_episode_v2.py`
- Modify: `plugins/workflow-observer/tests/test_portable_cli.py`

- [ ] Refresh CodeGraph and inspect existing frontmatter and Episode dispatch.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore parse_frontmatter
npx --yes @colbymchenry/codegraph@1.5.0 explore validate_record
npx --yes @colbymchenry/codegraph@1.5.0 explore canonical_episode_projection
```

- [ ] Add RED tests for the exact legacy classification matrix:

```text
observation without schema_version/artifact_type + complete v1 shape
    -> workflow-observation@1
observation schema_version 2 without artifact_type + valid Episode block
    -> workflow-observation@2
new Markdown artifact with matching explicit type/artifact_type/version
    -> registered schema
absent version with extra or ambiguous fields
    -> Data Trust Gate failure
type/artifact_type mismatch
    -> failure
unknown version or duplicate frontmatter key
    -> failure
Episode envelope/embedded schema disagreement
    -> failure
```

Use sentinels in title, Scope, Outcome, Follow-up, and `rework_reason`; no
classifier return value may include these values except the existing bounded
Episode Decision summary already admitted by `episode-projection@2`.

- [ ] Run RED.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_episode_v2.py -v
```

Expected: legacy exact-shape and mismatch assertions fail before central
classification is wired into both readers.

- [ ] Implement `parse_markdown_envelope()` over the existing strict
  frontmatter parser. Do not introduce a second YAML parser. The central
  classifier chooses a registry entry only after the full historical record
  validator succeeds. It returns metadata/body plus an `ArtifactSchemaRef`,
  never a best-effort partial classification.

- [ ] Refactor `wiki_observations.validate_record()` and the Episode reader to
  consume the same classification result. Preserve all v1/v2 rendering bytes,
  report output, validation behavior, and selected-adapter task layout.

- [ ] Run focused and compatibility GREEN.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

Expected: all pass; v1 output has no added fields and existing v2 output remains
canonical.

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/scripts/artifact_schema.py \
  plugins/workflow-observer/scripts/episode_schema.py \
  plugins/workflow-observer/scripts/wiki_observations.py \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/test_portable_cli.py
git commit -m "refactor: centralize artifact schema classification"
```

## Task 4: Add Pure Observation and Invalidation Derived Migrations

**Files:**

- Create: `plugins/workflow-observer/scripts/artifact_migration.py`
- Create: `plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json`
- Create: `plugins/workflow-observer/tests/test_artifact_migration.py`
- Modify: `plugins/workflow-observer/scripts/artifact_schema.py`
- Modify: `plugins/workflow-observer/tests/test_manifests.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`

- [ ] Refresh CodeGraph context.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore canonical_episode_projection
npx --yes @colbymchenry/codegraph@1.5.0 explore hash_canonical
```

- [ ] Write RED-A imports for `DerivedArtifact` and `migrate_artifact`, then
  RED-B vectors for these three source artifacts:

```text
workflow-observation v1 -> episode-projection@2
workflow-observation v2 -> episode-projection@2
observation-invalidation v1 -> observation-invalidation@2 projection
```

The fixture stores base64 or UTF-8 exact source bytes, source SHA-256,
migration identity, canonical derived UTF-8 hex, and derived SHA-256. It must
contain no private paths or live records.

- [ ] Add purity and adversarial tests:

```text
same source bytes + same policies -> identical canonical bytes
source whitespace change -> source hash and derived bytes change
clock/environment/Git changes -> no output change
legacy absence remains unavailable, never zero
run_id is unchanged
human-text privacy sentinel is absent
unknown/ambiguous schema fails before migration
wrong expected artifact type fails
source bytes remain unchanged
constructor/property mutation cannot alter DerivedArtifact
```

- [ ] Run RED-A, add only interfaces, then run RED-B.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_migration.py -v
```

- [ ] Implement only the observation-v1, observation-v2, and
  invalidation-v1 handlers in this task. Reuse the validated classifier and
  `canonical_episode_projection`; do not parse Markdown or Episode JSON a
  second time. Each handler accepts already-bound source bytes and policy
  values and returns a schema-validated value without file access.

For invalidation v1, normalize the exact legacy tombstone into:

```json
{
  "artifact_type": "observation-invalidation",
  "schema_version": 2,
  "run_id": "obs-...",
  "timestamp": "2026-08-11T00:00:00Z"
}
```

- [ ] Keep the Learning Snapshot migration registry entry declared but reject
  dispatch with an explicit unsupported-handler error until Task 6 installs
  and tests its handler. Task 4 tests must not claim Learning Snapshot
  migration works.

- [ ] Run GREEN and packaging tests.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_migration.py \
  plugins/workflow-observer/tests/test_artifact_schema.py \
  plugins/workflow-observer/tests/test_manifests.py \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

Expected: all implemented handlers pass; an explicit test confirms the
declared-but-not-installed Learning Snapshot handler fails closed.

- [ ] Inspect and commit this independently reviewable migration unit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/scripts/artifact_migration.py \
  plugins/workflow-observer/scripts/artifact_schema.py \
  plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json \
  plugins/workflow-observer/tests/test_artifact_migration.py \
  plugins/workflow-observer/tests/test_manifests.py \
  plugins/workflow-observer/tests/test_package_archive.py
git commit -m "feat: add pure observation artifact migrations"
```

## Task 5: Gate Store Records and Write Explicit Invalidation v2

**Files:**

- Modify: `plugins/workflow-observer/scripts/wiki_observations.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/scripts/core_source.json`
- Modify: `plugins/workflow-observer/tests/test_portable_cli.py`
- Modify: `plugins/workflow-observer/tests/test_adapter_conformance.py`
- Modify: `plugins/workflow-observer/tests/test_core_parity.py`

- [ ] Refresh CodeGraph and inspect invalidation lifecycle paths.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore invalidate_observation
npx --yes @colbymchenry/codegraph@1.5.0 explore collect_record_documents
npx --yes @colbymchenry/codegraph@1.5.0 explore InvalidationEvidence
```

- [ ] Add RED tests that new invalidations have this exact frontmatter and no
  additional machine fields:

```yaml
---
type: observation-invalidation
artifact_type: observation-invalidation
schema_version: 2
run_id: obs-20260811-010203-abcdef
timestamp: 2026-08-11T01:02:03Z
---
```

Retain any existing bounded human body required by the invalidation contract.
Assert that legacy unversioned tombstones remain readable and produce the same
`InvalidationEvidence(run_id, timestamp, source_sha256)` semantics as their
derived v2 form.

- [ ] Add adversarial RED cases:

```text
legacy invalidation with extra schema-like field
explicit v2 missing artifact_type
human type/artifact type mismatch
unknown schema version
duplicate run_id or schema_version frontmatter
tombstone target absent
Portable and LLMWiki differing normalized evidence
```

- [ ] Run RED.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

- [ ] Route observation and invalidation discovery through the central schema
  gate. New `invalidate` writes explicit v2 only after target validation, using
  the existing atomic/no-clobber lifecycle behavior. Do not rewrite an existing
  legacy tombstone. Continue binding timestamp and SHA-256 to the same secure
  read bytes.

- [ ] Regenerate `core_source.json` from the final bundled core sources using
  the repository's existing manifest procedure; never hand-edit a digest.

- [ ] Run GREEN, core parity, and v1/v2 compatibility.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_core_parity.py \
  plugins/workflow-observer/tests/test_episode_v2.py -v
```

Expected: all tests pass, new invalidations are v2, legacy tombstones remain
readable, and packaged/core source hashes match the live implementation.

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/scripts/wiki_observations.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/scripts/core_source.json \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_core_parity.py
git commit -m "feat: write explicit observation invalidations v2"
```

## Task 6: Advance Snapshot Input and Learning Snapshot to Schema v2

**Files:**

- Modify: `plugins/workflow-observer/scripts/artifact_migration.py`
- Modify: `plugins/workflow-observer/scripts/snapshot_input.py`
- Modify: `plugins/workflow-observer/scripts/learning_snapshot.py`
- Modify: `plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json`
- Modify: `plugins/workflow-observer/tests/test_artifact_migration.py`
- Modify: `plugins/workflow-observer/tests/test_snapshot_input.py`
- Modify: `plugins/workflow-observer/tests/test_learning_snapshot.py`
- Modify: `plugins/workflow-observer/tests/test_manifests.py`

- [ ] Refresh CodeGraph and inspect the v0.2 identity boundaries.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore SnapshotInput
npx --yes @colbymchenry/codegraph@1.5.0 explore build_learning_snapshot_core
npx --yes @colbymchenry/codegraph@1.5.0 explore SNAPSHOT_ANALYZER_FILES
```

- [ ] Add RED tests for Snapshot Input v2. Its canonical representation has
  exact top-level keys `artifact_type`, `schema_version`, `adapter`,
  `store_identity`, and `semantic_bundle`. `artifact_type` is
  `snapshot-input`, both the envelope and embedded semantic-bundle
  `schema_version` are integer `2`, and disagreement fails closed. The semantic
  bundle retains the existing query, policy, Episode, invalidation, reference,
  and input-manifest semantics and adds exact fields:

```json
{
  "artifact_type": "snapshot-input",
  "schema_version": 2,
  "artifact_policy_set": {
    "artifact_schema_registry": {
      "version": "artifact-schema-registry@1",
      "sha256": "sha256:1bba0c5635ed2cedf4885861243947c89d3f9ba98e358b049ff3a61c0a40e7d6"
    },
    "artifact_migration_registry": {
      "version": "artifact-migration-registry@1",
      "sha256": "sha256:0c6bbdb88de176725c065f885a4393b73db19ee769f04f486ade013121e0fe90"
    }
  },
  "migration_manifest": []
}
```

The exact new fields live in the semantic bundle and therefore enter
`input_manifest_sha256`. The separately validated health-event schema is not a
Snapshot result input and must not enter Snapshot identity. Adapter identity
and privacy-safe store identity remain envelope provenance as in v0.2.

- [ ] Populate `migration_manifest` with one row per selected physical source
  that required a derived migration. Each row contains only:

```json
{
  "artifact_type": "workflow-observation",
  "migration_identity": "workflow-observation-v1-to-episode-projection@1",
  "run_id": "obs-...",
  "source_schema_version": 1,
  "source_sha256": "...",
  "target_contract": "episode-projection@2"
}
```

Rows are unique by `(artifact_type, run_id, source_sha256,
migration_identity)`, byte-order sorted by JCS row bytes, and never contain
human text or paths. A reviewed logical view does not create a second Episode.

- [ ] Add RED tests for Learning Snapshot core v2. Preserve all v0.2 fields and
  add exact top-level fields:

```json
{
  "artifact_type": "learning-snapshot-core",
  "schema_version": 2,
  "sampled_by_policy_n": 0,
  "sampling_summary": {
    "full_retained_episode_n": 0,
    "sampled_minimal_episode_n": 0,
    "sampling_policy_identities": []
  }
}
```

`full_retained_episode_n` equals selected Episode count after invalidation
resolution under Phase 1. `sampled_minimal_episode_n` and
`sampled_by_policy_n` are exactly zero. No sampling-decision artifact exists.
Every existing metric availability partition adds `sampled_by_policy_n: 0` so
the categories remain exhaustive and do not reinterpret v0.2 absence.

The enclosing Learning Snapshot v2 artifact has exact top-level keys
`artifact_type`, `schema_version`, `authoritative`, `generated_at`,
`store_identity`, `adapter`, `snapshot_id`, `core`, and `artifact_sha256`.
`artifact_type` is `learning-snapshot`; envelope and core schema versions are
both integer `2`. Historical v1 artifacts have no top-level schema version and
are recognized only after their complete approved v0.2 shape and core v1 have
validated; absence is not a general fallback.

- [ ] Add RED tests for the Learning Snapshot v1 derived migration. It must:

```text
validate the complete v1 artifact and identities
retain the original snapshot_id only as source identity evidence
derive schema-v2 sampling fields as explicit zero because v1 predated sampling
bind exact source bytes and migration policy identity
not republish or rewrite the v1 artifact
produce the fourth frozen migration vector
```

- [ ] Run RED.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_migration.py \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_learning_snapshot.py -v
```

- [ ] Implement Snapshot Input v2 using `ArtifactPolicySet` supplied explicitly
  by the acquisition caller. Reuse the deeply immutable `SnapshotInput` value
  object and revalidate the v2 manifest hash at construction. Do not reopen
  source files in the analyzer.

- [ ] Implement Learning Snapshot core v2 and its validator with exact keys,
  integer non-negative counts, exhaustive per-metric availability partitions,
  and these cross-field equations:

```text
sampled_by_policy_n == sampled_minimal_episode_n == 0
full_retained_episode_n == selected outcome/lifecycle Episode population
observed_n + not_recorded_n + unsupported_by_schema_n
  + not_applicable_n + sampled_by_policy_n == eligible_episode_n
```

- [ ] Install the previously declared `learning-snapshot-v1` migration handler,
  add the fourth fixture vector, and remove the Task 4 fail-closed expectation.
  The handler consumes already-validated v1 artifact bytes and returns a
  derived schema-v2 core; it does not call the Snapshot publisher.

- [ ] Add `artifact_schema.py` and `artifact_migration.py` to the exact analyzer
  artifact source manifest because both now affect Snapshot Input and Learning
  Snapshot results. Assert the full sorted source list in the manifest test.

- [ ] Run GREEN and v0.2 regressions.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_artifact_migration.py \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_learning_snapshot.py \
  plugins/workflow-observer/tests/test_manifests.py \
  plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py -v
```

Expected: new v2 tests pass, all existing v0.2 acceptance cases remain green,
and v0.2 artifacts/identities are readable without reinterpretation.

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/scripts/artifact_migration.py \
  plugins/workflow-observer/scripts/snapshot_input.py \
  plugins/workflow-observer/scripts/learning_snapshot.py \
  plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json \
  plugins/workflow-observer/tests/test_artifact_migration.py \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_learning_snapshot.py \
  plugins/workflow-observer/tests/test_manifests.py
git commit -m "feat: add schema v2 learning snapshot semantics"
```

## Task 7: Publish and Read Learning Snapshot v1/v2 Safely

**Files:**

- Modify: `plugins/workflow-observer/scripts/snapshot_store.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_snapshot_publication.py`
- Modify: `plugins/workflow-observer/tests/test_portable_cli.py`
- Modify: `plugins/workflow-observer/tests/test_adapter_conformance.py`

- [ ] Refresh CodeGraph.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore create_learning_snapshot
npx --yes @colbymchenry/codegraph@1.5.0 explore validate_learning_artifact_bytes
npx --yes @colbymchenry/codegraph@1.5.0 explore read_learning_artifact
```

- [ ] Add RED tests for version-dispatched identity:

```text
learning-snapshot v1 -> existing v1 domain and existing validator
learning-snapshot v2 -> v2 domain and exact v2 validator
new snapshot command -> publishes v2 only
existing valid v1 file -> remains readable and byte-identical
filename ID -> recomputed schema-specific snapshot_id
artifact_sha256 -> recomputed over exact canonical artifact
unknown version/type mismatch -> reject
v1 bytes labeled v2 or v2 bytes labeled v1 -> reject
v2 envelope/core schema disagreement -> reject
```

The v2 semantic ID domain is exactly:

```python
b"workflow-observatory:learning-snapshot-core:v2\0"
```

The v1 domain remains:

```python
b"workflow-observatory:learning-snapshot-core:v1\0"
```

- [ ] Run RED.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_publication.py -v
```

- [ ] Dispatch only after strict JSON ingress and exact artifact classification.
  Reuse the existing descriptor-bound, no-follow, bounded single-open readback,
  manifest A/B stable-read, mode `0700` directory, mode `0600` file, and
  hard-link no-clobber publication. Do not duplicate or weaken these routines.

- [ ] Recompute every cross-field semantic invariant during v2 readback,
  including sampling-zero equations and existing Decision recurrence/candidate
  evidence graph validation. Hashes prove byte identity; validators prove
  meaning.

- [ ] Add a concurrent same-v2-ID publication test and a v1/v2 sibling test.
  Exactly one v2 publisher returns `created=true`; a second returns false after
  secure validation. A historical v1 artifact remains independently readable
  and is never overwritten by v2.

- [ ] Run GREEN and CLI/adapter regressions.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_publication.py \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/scripts/snapshot_store.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/tests/test_snapshot_publication.py \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py
git commit -m "feat: publish versioned learning snapshots"
```

## Task 8: Freeze the 12-Case Phase 1 Acceptance Matrix

**Files:**

- Create: `plugins/workflow-observer/tests/test_schema_migration_acceptance.py`
- Modify: `plugins/workflow-observer/tests/workflow_evolution_fixtures.py`
- Modify: `plugins/workflow-observer/tests/test_adapter_conformance.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`

- [ ] Refresh CodeGraph and inspect existing fake-store builders and matrix
  execution conventions.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore FakeObservationStore
npx --yes @colbymchenry/codegraph@1.5.0 explore WorkflowEvolutionAcceptanceTests
npx --yes @colbymchenry/codegraph@1.5.0 explore ArchiveTests
```

- [ ] Freeze exactly these 12 acceptance cases in one test class. Each case
  runs against fake Portable and fake LLMWiki semantics when adapter behavior
  is relevant:

| Case | Required result |
|---:|---|
| 01 | Observation v1 exact legacy shape classifies and migrates deterministically without changing source bytes |
| 02 | Observation v2 exact Episode shape classifies and migrates to one `episode-projection@2` sample |
| 03 | Ambiguous absent-version observation fails the Data Trust Gate |
| 04 | All 14 health-event schemas validate exact evidence; unknown event/schema and envelope/embedded mismatch fail closed |
| 05 | Legacy invalidation v1 and explicit invalidation v2 normalize to equivalent bounded evidence while preserving different source hashes |
| 06 | New invalidate writes v2; existing legacy tombstone remains byte-identical and readable |
| 07 | Snapshot Input v2 binds artifact policy identities and sorted migration manifest with no privacy sentinel |
| 08 | Learning Snapshot v1 derived migration produces the fixed v2 core and never republishes/re-writes v1 |
| 09 | New Learning Snapshot v2 records exhaustive zero sampling fields and preserves v0.2 denominators |
| 10 | Secure readback accepts valid v1/v2 siblings and rejects schema-label, filename-ID, digest, and noncanonical-byte mismatches |
| 11 | Two concurrent v2 publishers never overwrite; exactly one creates and no temporary residue remains |
| 12 | Portable/LLMWiki canonical semantic bytes match; package inventory contains the approved design, plan, policies, migration fixture, modules, and acceptance test |

- [ ] Add an acceptance fixture guard that records the case IDs and rejects
  duplicate, missing, reordered, or newly appended cases unless this plan is
  amended and re-approved:

```python
EXPECTED_CASE_IDS = tuple(f"test_{number:02d}" for number in range(1, 13))
```

The 12 methods are named exactly `test_01` through `test_12`; each method uses
its docstring for the descriptive case title. Discovery must equal this tuple
exactly, so adding a thirteenth case or silently removing one fails the guard.

- [ ] Assert isolation before semantic assertions:

```text
all roots reside below the test TemporaryDirectory
no resolved path equals or descends from configured live roots
no network call or external command is used
fixtures contain no author-specific home path, credentials, prompts, or transcripts
all produced files use expected private modes
```

An isolation, integrity, fixture-provenance, or cleanup failure aborts the
formal test class; it is not reported as an ordinary semantic case failure.

- [ ] Run the class on the primary runtime first.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_schema_migration_acceptance.py -v
```

Expected: 12 tests pass with no skip on macOS/Linux. This is development
evidence until the complete fixed commit is run at Task 9.

- [ ] Run the same fixed class on CPython 3.11–3.14 without changing source or
  fixture bytes between interpreters:

```bash
for py in 3.11 3.12 3.13 3.14; do
  caffeinate -i -m uv run --no-project --python "$py" \
    python -m unittest \
    plugins/workflow-observer/tests/test_schema_migration_acceptance.py -v
done
```

Expected: 12/12 pass on every interpreter. An unavailable interpreter is an
environment blocker for Phase 1 acceptance, not a test skip.

- [ ] Run adapter and package cross-checks.

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

- [ ] Inspect and commit.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add plugins/workflow-observer/tests/test_schema_migration_acceptance.py \
  plugins/workflow-observer/tests/workflow_evolution_fixtures.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_package_archive.py
git commit -m "test: freeze workflow schema migration acceptance"
```

## Task 9: Document, Verify, Review, and Checkpoint Phase 1

**Files:**

- Modify: `plugins/workflow-observer/README.md`
- Modify: `README.md`
- Modify: `ROADMAP.md`
- Modify: `TODO.md`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`

- [ ] Refresh CodeGraph and inspect all user-facing schema/version claims.

```bash
npx --yes @colbymchenry/codegraph@1.5.0 sync .
npx --yes @colbymchenry/codegraph@1.5.0 explore snapshot
rg -n "schema|migration|snapshot|sampling|Windows|supported|v0\.3" \
  README.md ROADMAP.md TODO.md plugins/workflow-observer/README.md
```

- [ ] Update documentation with only verified Phase 1 claims:

```text
Implemented:
  explicit Phase 1 schema registry
  pure derived migrations
  invalidation v2 writer with legacy read compatibility
  Snapshot Input / Learning Snapshot v2 zero-sampling semantics
  v1/v2 Learning Snapshot readback

Not implemented in Phase 1:
  lock/CAS/maintenance transaction
  durable health evidence
  retention/export/delete/restore/purge
  observation v3 or sampling decisions
  Windows lock backend

Next authorized design unit:
  Phase 2 cross-platform same-machine writer safety plan
```

Do not claim Windows support or a v0.3 release. Keep the exact wording that
Windows verification is deferred to the phase that implements the backend.

- [ ] Extend the exact package inventory assertion to cover:

```text
approved v0.3 design and Phase 1 plan
three artifact/health policy JSON files
artifact_schema.py and artifact_migration.py
artifact_migration_vectors.json
three new unit/acceptance test modules
updated README, ROADMAP, and TODO
```

No wildcard may admit unknown documents, nested policies, caches, observations,
local configs, absolute paths, `.git`, `.codegraph`, `.superpowers`, `dist`, or
historical frozen evidence as live source.

- [ ] Run formatting/static hygiene.

```bash
caffeinate -i -m python3 -m compileall -q \
  plugins/workflow-observer/scripts plugins/workflow-observer/tests
git diff --check
```

Expected: no whitespace error or syntax failure.

- [ ] Run the complete fixed Phase 1 interpreter matrix on unchanged candidate
  content.

```bash
for py in 3.11 3.12 3.13 3.14; do
  caffeinate -i -m uv run --no-project --python "$py" \
    python -m unittest \
    plugins/workflow-observer/tests/test_schema_migration_acceptance.py -v
done
```

Expected: 12/12 pass on each interpreter, no skip.

- [ ] Run the complete primary-runtime repository suite once, without stopping
  at an earlier focused failure:

```bash
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py' -v
```

Expected: all available tests pass. A failure invalidates formal acceptance;
fixes require a new clean full run from the beginning on the new commit.

- [ ] Build and verify the production archive twice.

```bash
mkdir -p evidence/dist/phase1-verify-a evidence/dist/phase1-verify-b
rm -f evidence/dist/phase1-verify-a/\
workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/\
workflow-observatory-0.3.0-phase1-verify.zip
caffeinate -i -m python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-a/
caffeinate -i -m python3 evidence/scripts/package_workflow_observatory.py \
  --version 0.3.0-phase1-verify
cp evidence/dist/workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/
cmp evidence/dist/phase1-verify-a/\
workflow-observatory-0.3.0-phase1-verify.zip \
  evidence/dist/phase1-verify-b/\
workflow-observatory-0.3.0-phase1-verify.zip
```

Expected: package verification succeeds, archives are byte-identical, and
their inventories contain exactly the approved Phase 1 sources. These are
local verification artifacts, not a release.

- [ ] Request adversarial code review against the approved design and this
  plan. The review must examine schema ambiguity, policy/hash closure, legacy
  byte preservation, privacy sentinels, migration purity, version dispatch,
  package allowlists, and test isolation. Fix every Critical/Important finding
  with focused RED/GREEN evidence and rerun the full gates above.

- [ ] Update documentation/tests, inspect the final diff, and commit the Phase
  1 acceptance record.

```bash
git diff --check
git diff --name-only | npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
git add README.md ROADMAP.md TODO.md \
  plugins/workflow-observer/README.md \
  plugins/workflow-observer/tests/test_package_archive.py
git commit -m "docs: record workflow schema foundation acceptance"
```

- [ ] Establish the durable interruption checkpoint.

```bash
git status --short --branch
git log -1 --oneline
```

If remote WIP push has been separately authorized:

```bash
git push -u origin design/concurrency-operability-v0.3
```

Otherwise create a durable bundle from the canonical clone storage:

```bash
CHECKPOINT_DIR="${HOME}/Developer/workflow-observatory-checkpoints"
BUNDLE="${CHECKPOINT_DIR}/workflow-observatory-v0.3-phase1-\
$(git rev-parse --short=12 HEAD).bundle"
mkdir -p "$CHECKPOINT_DIR"
git bundle create "$BUNDLE" HEAD ^origin/main
git bundle verify "$BUNDLE"
```

Expected: every Phase 1 commit is reachable from the durable clone and either
the authorized remote WIP branch or the verified bundle.

- [ ] Release the exact orchestration-scoped sleep assertion and confirm it is
  gone:

```bash
kill "$ORCHESTRATION_CAFFEINATE_PID"
wait "$ORCHESTRATION_CAFFEINATE_PID" || true
if kill -0 "$ORCHESTRATION_CAFFEINATE_PID" 2>/dev/null; then
  echo "orchestration caffeinate still alive" >&2
  exit 1
fi
```

Do not kill unrelated `caffeinate` processes.

## Phase 1 Exit Criteria

Phase 1 is accepted only when all of the following are simultaneously true on
one checkpointed commit:

- the two registries and health-event schema validate with the exact frozen
  identities and all 14 event-specific evidence contracts;
- existing observation v1/v2 and Learning Snapshot v1 bytes remain unchanged;
- new invalidations are explicit v2 and legacy invalidations remain readable;
- pure migrations are deterministic, privacy-bounded, and source-hash bound;
- Snapshot Input and Learning Snapshot v2 encode explicit zero sampling state;
- one physical `run_id` remains one Episode sample;
- v1/v2 snapshot readback recomputes schema-specific identities and meaning;
- the fixed 12-case matrix passes on CPython 3.11–3.14;
- the complete primary-runtime suite passes cleanly;
- two local package builds are byte-identical and exact-inventory verified;
- no test touched a live store and no release/push/merge occurred without
  separate authorization;
- adversarial review has no unresolved Critical or Important finding;
- the work is durably checkpointed and the orchestration sleep assertion is
  released.

Only then may the maintainer approve writing the separate Phase 2
cross-platform writer-safety implementation plan. Phase 1 acceptance does not
itself authorize Phase 2 code.

## Plan-Authoring Verification Record

On 2026-08-11, before this plan was checkpointed:

- the approved v0.3 design was mapped to the five phase entry/exit gates;
- the Phase 1 file map, interfaces, types, version dispatch, TDD steps, exact
  commands, and interruption checkpoint were reviewed for internal consistency;
- the three canonical policy hashes were independently recomputed with the
  current approved RFC 8785 canonicalizer;
- placeholder scanning found no unfinished implementation instruction;
- Markdown code fences were balanced and `git diff --check` was clean;
- all 281 existing non-package plugin tests passed;
- the focused package check failed only at the intended exact allowlist gate:
  `unexpected marketplace file: docs/superpowers/plans/2026-08-11-workflow-observatory-v0.3-phase-1-schema-migration.md`.

That package failure is Task 1's required RED evidence. It is not a product
regression to bypass before plan approval.

## Execution Choice After Plan Approval

1. **Subagent-driven development (recommended):** a fresh worker implements one
   task at a time, then a spec-compliance reviewer and code-quality reviewer
   inspect the checkpoint before the next task. Shared-worktree writes remain
   serialized by the coordinator.
2. **Inline execution:** continue in one session with the same RED/GREEN,
   review, verification, and durable-checkpoint boundaries.

Do not begin either path until the user approves this Phase 1 plan and
separately authorizes Task 0/Task 1 execution.
