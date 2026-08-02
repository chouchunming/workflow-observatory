# Workflow Evolution Foundation v0.2 Implementation Plan

Status: Revised after final focused external re-review; focused approval of the
Decision recurrence cohort-size amendment is required before Task 1. The
underlying design remains approved.

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `subagent-driven-development` (recommended) or `executing-plans` to implement
> this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add backward-compatible Episode v2 records, one adapter-neutral
canonical snapshot input, reproducible Learning Snapshots, and deterministic
improvement candidates without authorizing proposals or workflow mutation.

**Architecture:** Keep the existing observation lifecycle core and introduce
focused pure-Python modules for canonical JSON, immutable policy artifacts,
Episode v2 projection, snapshot acquisition, deterministic analysis, and
atomic artifact publication. The CLI selects one adapter semantics profile and
uses it for the complete `snapshot-input` gate; the higher-level `snapshot`
operation performs manifest A/B stable-read verification before publishing a
local immutable artifact. Existing schema-v1 records and human `report` remain
readable, but `workflow-learning` moves to the canonical machine-readable
snapshot operation.

**Tech Stack:** Python 3.11+ standard library, `unittest`, RFC 8785 JCS under
the design's no-floating-number restriction, Markdown skills and documentation,
and CodeGraph 1.5.0 as advisory impact analysis.

## Global Constraints

- The approved design is
  `docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md`.
- Use an isolated linked worktree backed by the durable canonical clone.
- Complete Task 0 once in every newly created linked worktree. Before each
  later task, run `npx --yes @colbymchenry/codegraph@1.5.0 sync .`, query the
  exact symbols being changed with `codegraph explore`, and pipe changed paths
  to `codegraph affected --stdin` after the change. CodeGraph is advisory only;
  unavailable, stale, or unsupported-language results never replace the
  explicit tests below.
- Run every test/build/evaluation command as a direct child of
  `caffeinate -i -m`. Keep one tracked orchestration-scoped `caffeinate -i`
  assertion while workers or multi-step orchestration are active, and release
  it before hand-off.
- Use TDD. Every task must demonstrate the named focused test failing for the
  intended missing behavior before production changes, then passing afterward.
- For trust-critical Tasks 2, 3, 5, 6, and 9, an initial import/interface
  failure is only RED-A. Add the smallest interface skeleton without product
  behavior, rerun, and capture RED-B where the intended semantic assertion
  fails. Implement GREEN only after RED-B proves the test exercises the trust
  boundary rather than merely a missing module.
- Commit each task after its focused verification. Push the WIP branch at every
  review, pause, hand-off, or context-compaction boundary.
- Python 3.11 is the minimum runtime. The v0.2 acceptance matrix covers CPython
  3.11, 3.12, 3.13, and 3.14; a later minor is not formally supported until it
  runs the same vector/suite. Add no third-party runtime dependency.
- Keep schema-v1 record bytes unchanged and readable. Never migrate or rewrite
  an existing observation in place.
- Use only fake portable and fake LLMWiki roots under `TemporaryDirectory` in
  tests. Never run learning tests against the user's live Wiki or portable
  store.
- Do not create Evolution Proposal artifacts, edit workflows or skills based on
  candidates, create PRs, execute experiments, or publish formal acceptance.
- Trust-gate failure emits a normalized error in this milestone; persistent
  Trust Gate Diagnostic artifacts remain optional and are not implemented.
- Snapshot acquisition reads only Episodes, invalidations, validated
  references, and the approved policy set. Post-hoc human/reviewer/evaluator
  artifacts are excluded from v0.2 input and cannot affect a denominator.
- v0.2 publishes canonical JSON only. It produces no Markdown rendering and no
  LLM-authored narrative, so no second authoritative representation exists.
- `snapshot_id`, `candidate_id`, policy hashes, manifest hashes, and artifact
  hashes use one canonicalizer implementation and the exact domain separators
  defined below.
- Analyzer and canonicalizer code identities use one deterministic JCS
  artifact manifest format. They never hash tar/zip bytes or depend on
  traversal order.
- The effective-boundary discriminated union is strict. Exactly one of
  `started_at` or `producer_generation` is legal. A producer-generation
  boundary uses exact identity equality only; lexical, numeric-suffix, and
  semantic-version ordering are forbidden.
- Any future Evolution Proposal must cite both `snapshot_id` and
  `candidate_id`; this plan updates the contract but does not implement proposal
  creation.
- Do not edit historical packaged evidence under `evidence/marketplace/`, the
  top-level `SHA256SUMS.json`, release version `0.1.0`, or GitHub release state.
  Current top-level README/ROADMAP/TODO may evolve independently; never restore
  obsolete byte-equality by copying them into the frozen 0.1 evidence tree.

## Task 0: Per-Worktree Execution Preflight

This is an execution preflight, not a twelfth implementation unit. Run it once
after creating each linked worktree and before Task 1:

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

Expected: CodeGraph reports version `1.5.0`, the worktree has its own complete
index, and `.codegraph/` is ignored by a local Git exclude or repository ignore
rule. If the ignore check fails, add `.codegraph/` to the worktree's local Git
exclude before continuing; do not commit the index. If CodeGraph cannot index
the worktree, record that advisory limitation and continue with explicit
searches and tests rather than treating a partial graph as authoritative.

## File and Responsibility Map

**Create:**

- `plugins/workflow-observer/scripts/canonical_json.py` — the sole JCS
  serializer, strict JSON ingress parser, and domain-separated hash helper.
- `plugins/workflow-observer/scripts/policy_artifacts.py` — policy validation,
  effective-boundary union, deterministic source manifest, and policy-set
  identity.
- `plugins/workflow-observer/scripts/episode_schema.py` — Episode v2 supplement,
  JSON block, projection, schema capability, and Decision Event validation.
- `plugins/workflow-observer/scripts/snapshot_input.py` — absolute interval,
  adapter semantics, validated record acquisition, invalidation resolution,
  source/reference hashes, and canonical input manifest.
- `plugins/workflow-observer/scripts/learning_snapshot.py` — lifecycle/outcome
  denominators, missingness, rational quantiles, Decision support, and stable
  candidate evidence.
- `plugins/workflow-observer/scripts/snapshot_store.py` — manifest A/B
  orchestration and private atomic Learning Snapshot publication.
- `plugins/workflow-observer/policies/*.json` — immutable projection,
  producer-capability, generation-mapping, metric, quantile, Decision support,
  staleness, and candidate-emission policies.
- `plugins/workflow-observer/tests/test_canonical_json.py`
- `plugins/workflow-observer/tests/fixtures/jcs_conformance_vectors.json`
- `plugins/workflow-observer/tests/test_policy_artifacts.py`
- `plugins/workflow-observer/tests/test_episode_v2.py`
- `plugins/workflow-observer/tests/test_snapshot_input.py`
- `plugins/workflow-observer/tests/test_learning_snapshot.py`
- `plugins/workflow-observer/tests/test_snapshot_publication.py`
- `plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py`
- `plugins/workflow-observer/tests/workflow_evolution_fixtures.py` — shared
  fake-root record builders, policy loaders, fixed timestamps, and timezone
  context manager; test support only, never packaged as production behavior.

**Modify:**

- `plugins/workflow-observer/scripts/wiki_observations.py` — preserve v1 while
  accepting/rendering/validating v2 and adapter-specific task-record layout.
- `plugins/workflow-observer/scripts/workflow_observer_cli.py` — add v2 start
  and finish inputs, `snapshot-input`, and `snapshot`.
- `plugins/workflow-observer/scripts/store_config.py` — expose a selected
  adapter semantics profile without silent fallback.
- `plugins/workflow-observer/scripts/core_source.json` — refresh the bundled
  lifecycle-core digest only after the final core change.
- `plugins/workflow-observer/skills/workflow-telemetry/SKILL.md` — document the
  opt-in Episode v2 payload and privacy boundary.
- `plugins/workflow-observer/skills/workflow-learning/SKILL.md` — consume only
  the bounded canonical snapshot operation.
- `plugins/workflow-observer/skills/workflow-improving/SKILL.md` — require the
  compound snapshot/candidate reference and retain the approval gate.
- `plugins/workflow-observer/tests/test_portable_cli.py`
- `plugins/workflow-observer/tests/test_adapter_conformance.py`
- `plugins/workflow-observer/tests/test_core_parity.py`
- `plugins/workflow-observer/tests/test_learning_improving.py`
- `plugins/workflow-observer/tests/test_skill_contracts.py`
- `plugins/workflow-observer/tests/test_package_archive.py`
- `evidence/scripts/package_workflow_observatory.py` — allow immutable policy
  JSON and the two exact approved v0.2 documents in future marketplace
  archives without rewriting historical evidence.
- `plugins/workflow-observer/README.md`, `README.md`, `ROADMAP.md`, and
  `TODO.md` — describe the bounded v0.2 foundation and remaining proposal work.

---

### Task 1: Canonical JSON and Content-Derived Identity Primitive

**Files:**

- Create: `plugins/workflow-observer/scripts/canonical_json.py`
- Create: `plugins/workflow-observer/tests/test_canonical_json.py`
- Create: `plugins/workflow-observer/tests/fixtures/jcs_conformance_vectors.json`

**Interfaces:**

- Produces: `canonicalize(value: object) -> bytes`
- Produces: `strict_json_loads(data: str | bytes) -> object`
- Produces: `hash_canonical(domain: bytes, value: object) -> str`
- Produces: `CanonicalizationError(ValueError)`
- Consumes: Python JSON-domain values only; floats and non-string mapping keys
  are rejected.

- [ ] **Step 1: Write the failing RFC 8785 and rejection tests**

Create the shared language-neutral vector first:

```json
{
  "schema_version": 1,
  "domain_utf8_hex": "776f726b666c6f772d6f627365727661746f72793a6a63732d636f6e666f726d616e63653a763100",
  "canonical_utf8_hex": "7b22636f6e74726f6c223a225c625c745c6e5c665c725c7530303066222c226e6573746564223a5b7b2261223a747275652c227a223a6e756c6c7d5d2c2271756f7465223a225c225c5c222c22c3a9223a22e58e9fe6a8a3222c22f09f9880223a22656d6f6a69227d",
  "domain_hash": "d14ca7c9b6fc637da0e029621ec12d6e9e5f272fe6c938675b5e25fe60e1bd3a"
}
```

The test constructs the Unicode/control input in code, loads the expected bytes
and digest from this file, and separately constructs a lone surrogate that
cannot legally appear in the I-JSON fixture.

```python
import json
import sys
from pathlib import Path
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
VECTOR = Path(__file__).resolve().parent / "fixtures/jcs_conformance_vectors.json"
sys.path.insert(0, str(SCRIPTS))

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)


class CanonicalJsonTests(unittest.TestCase):
    def test_shared_unicode_and_escape_vector(self):
        vector = json.loads(VECTOR.read_text(encoding="utf-8"))
        value = {
            "😀": "emoji",
            "é": "原樣",
            "control": "\b\t\n\f\r\u000f",
            "quote": "\"\\",
            "nested": [{"z": None, "a": True}],
        }
        encoded = canonicalize(value)
        self.assertEqual(bytes.fromhex(vector["canonical_utf8_hex"]), encoded)
        self.assertEqual(
            vector["domain_hash"],
            hash_canonical(bytes.fromhex(vector["domain_utf8_hex"]), value),
        )

    def test_rejects_non_i_json_or_forbidden_numbers(self):
        for value in (
            1.5,
            {"x": float("nan")},
            {"x": 2**53},
            {"x": "\ud800"},
            {1: "non-string key"},
        ):
            with self.subTest(value=repr(value)):
                with self.assertRaises(CanonicalizationError):
                    canonicalize(value)

    def test_hash_is_domain_separated(self):
        value = {"a": 1}
        self.assertEqual(64, len(hash_canonical(b"a\0", value)))
        self.assertNotEqual(
            hash_canonical(b"a\0", value),
            hash_canonical(b"b\0", value),
        )

    def test_strict_json_rejects_ambiguous_or_non_i_json_input(self):
        invalid = (
            '{"x":1,"x":2}',
            '{"x":NaN}',
            '{"x":Infinity}',
            '{"x":-Infinity}',
            '{"x":"\\ud800"}',
            b'{"x":"\xff"}',
        )
        for payload in invalid:
            with self.subTest(payload=repr(payload)):
                with self.assertRaises(CanonicalizationError):
                    strict_json_loads(payload)
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_canonical_json.py -v
```

Expected: import failure because `canonical_json.py` does not exist.

- [ ] **Step 3: Implement the restricted JCS serializer and hash helper**

```python
from __future__ import annotations

import hashlib
import json

_MIN_SAFE_INTEGER = -(2**53) + 1
_MAX_SAFE_INTEGER = (2**53) - 1


class CanonicalizationError(ValueError):
    pass


def _validate_string(value: str) -> None:
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise CanonicalizationError("lone surrogate is not valid I-JSON")


def _string_bytes(value: str) -> bytes:
    _validate_string(value)
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _key_order(value: str) -> bytes:
    _validate_string(value)
    return value.encode("utf-16-be")


def _encode(value: object) -> bytes:
    if value is None:
        return b"null"
    if value is True:
        return b"true"
    if value is False:
        return b"false"
    if isinstance(value, int):
        if not _MIN_SAFE_INTEGER <= value <= _MAX_SAFE_INTEGER:
            raise CanonicalizationError("integer is outside the I-JSON safe range")
        return str(value).encode("ascii")
    if isinstance(value, float):
        raise CanonicalizationError("JSON floating-point numbers are prohibited")
    if isinstance(value, str):
        return _string_bytes(value)
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise CanonicalizationError("JSON object keys must be strings")
        keys = sorted(value, key=_key_order)
        return b"{" + b",".join(
            _string_bytes(key) + b":" + _encode(value[key]) for key in keys
        ) + b"}"
    if isinstance(value, list):
        return b"[" + b",".join(_encode(item) for item in value) + b"]"
    raise CanonicalizationError(f"unsupported JSON value: {type(value).__name__}")


def canonicalize(value: object) -> bytes:
    return _encode(value)


def strict_json_loads(data: str | bytes) -> object:
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise CanonicalizationError("JSON input is not valid UTF-8") from error
    elif isinstance(data, str):
        text = data
    else:
        raise CanonicalizationError("JSON input must be text or UTF-8 bytes")

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise CanonicalizationError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    def reject_constant(token):
        raise CanonicalizationError(f"non-finite JSON number: {token}")

    try:
        value = json.loads(
            text,
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (CanonicalizationError, json.JSONDecodeError) as error:
        if isinstance(error, CanonicalizationError):
            raise
        raise CanonicalizationError("invalid JSON input") from error
    canonicalize(value)
    return value


def hash_canonical(domain: bytes, value: object) -> str:
    if not isinstance(domain, bytes) or not domain.endswith(b"\0"):
        raise CanonicalizationError("hash domain must be NUL-terminated bytes")
    return hashlib.sha256(domain + canonicalize(value)).hexdigest()
```

`strict_json_loads` is the only production JSON ingress for v0.2 policy and
registry files, Episode v2 supplements, embedded Episode blocks, persisted
Learning Snapshot readback, and any persisted code/source manifest JSON. A
caller may not use ordinary `json.loads()` before handing an already-collapsed
object to the canonicalizer.

- [ ] **Step 4: Run the focused test and existing plugin tests**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_canonical_json.py -v
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py'
```

Expected: new tests pass; existing baseline remains green with its documented
single optional parity skip.

- [ ] **Step 5: Commit the reviewable unit**

```bash
git add plugins/workflow-observer/scripts/canonical_json.py \
  plugins/workflow-observer/tests/test_canonical_json.py \
  plugins/workflow-observer/tests/fixtures/jcs_conformance_vectors.json
git commit -m "feat: add canonical workflow artifact hashing"
```

---

### Task 2: Immutable Policy Closure and Code Artifact Manifests

**Files:**

- Create: `plugins/workflow-observer/scripts/policy_artifacts.py`
- Create: `plugins/workflow-observer/policies/episode_projection.json`
- Create: `plugins/workflow-observer/policies/producer_capabilities.json`
- Create: `plugins/workflow-observer/policies/workflow_generation_mapping.json`
- Create: `plugins/workflow-observer/policies/metric_semantics.json`
- Create: `plugins/workflow-observer/policies/quantile_policy.json`
- Create: `plugins/workflow-observer/policies/decision_support_policy.json`
- Create: `plugins/workflow-observer/policies/lifecycle_health_policy.json`
- Create: `plugins/workflow-observer/policies/candidate_emission_policy.json`
- Create: `plugins/workflow-observer/tests/test_policy_artifacts.py`

**Interfaces:**

- Consumes: `canonical_json.canonicalize` and `hash_canonical` from Task 1.
- Produces: `PolicyError(ValueError)`.
- Produces: `validate_effective_boundary(value: object) -> dict[str, str]`
- Produces: `effective_boundary_applies(boundary: Mapping, *, started_at: str, producer_generation: str | None) -> bool`.
- Produces: `RegularFileEvidence(content: bytes, sha256: str, executable: bool, device: int, inode: int)`.
- Produces: `read_regular_file_evidence(root: Path, relative_posix_path: str, *, max_bytes: int) -> RegularFileEvidence`.
- Produces: `build_code_manifest(root: Path, relative_paths: Sequence[str]) -> dict`
- Produces: `load_policy_set(policy_root: Path, analyzer_files: Sequence[str], canonicalizer_files: Sequence[str]) -> PolicySet`
- Produces: immutable `PolicySet.core_identity() -> dict[str, dict[str, str]]`.

- [ ] **Step 1: Write failing boundary, manifest, and policy-hash tests**

```python
def test_effective_boundary_is_exactly_one_variant(self):
    self.assertEqual(
        {"type": "started_at", "from": "2026-08-03T00:00:00Z"},
        validate_effective_boundary({
            "type": "started_at", "from": "2026-08-03T00:00:00Z"
        }),
    )
    invalid = (
        {},
        {"type": "unknown", "from": "x"},
        {"type": "started_at", "from": "producer@3"},
        {"type": "producer_generation", "from": "2026-08-03T00:00:00Z"},
        {"type": "started_at", "from": "2026-08-03T00:00:00Z", "other": 1},
    )
    for value in invalid:
        with self.subTest(value=value), self.assertRaises(PolicyError):
            validate_effective_boundary(value)


def test_producer_generation_boundary_is_valid_and_uses_exact_identity(self):
    boundary = validate_effective_boundary({
        "type": "producer_generation",
        "from": "producer@3",
    })
    self.assertEqual(
        {"type": "producer_generation", "from": "producer@3"},
        boundary,
    )
    self.assertTrue(effective_boundary_applies(
        boundary,
        started_at="2026-08-03T00:00:00Z",
        producer_generation="producer@3",
    ))
    for generation in (None, "producer@2", "producer@10"):
        with self.subTest(generation=generation):
            self.assertFalse(effective_boundary_applies(
                boundary,
                started_at="2026-08-03T00:00:00Z",
                producer_generation=generation,
            ))


def test_code_manifest_is_order_independent_and_binds_mode(self):
    left = build_code_manifest(self.root, ["b.py", "a.py"])
    right = build_code_manifest(self.root, ["a.py", "b.py"])
    self.assertEqual(left, right)
    os.chmod(self.root / "a.py", 0o755)
    self.assertNotEqual(left, build_code_manifest(self.root, ["a.py", "b.py"]))


def test_code_manifest_binds_exact_file_bytes(self):
    before = build_code_manifest(self.root, ["a.py"])
    (self.root / "a.py").write_text("changed\n", encoding="utf-8")
    after = build_code_manifest(self.root, ["a.py"])
    self.assertNotEqual(before, after)


def test_code_manifest_rejects_unsafe_members(self):
    for member in (
        "", ".", "/absolute.py", "../escape.py", "a/../b.py",
        "a\\b.py", "C:/drive.py", "//server/share.py", "nul\0name.py",
    ):
        with self.subTest(member=member), self.assertRaises(PolicyError):
            build_code_manifest(self.root, [member])


def test_code_manifest_rejects_swap_before_final_open(self):
    replacement = self.root / "replacement.py"
    replacement.write_text("replacement\n", encoding="utf-8")
    real_open = os.open
    swapped = False

    def swap_then_open(path, flags, *args, **kwargs):
        nonlocal swapped
        if path == "a.py" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            os.replace(replacement, self.root / "a.py")
        return real_open(path, flags, *args, **kwargs)

    with mock.patch("policy_artifacts.os.open", side_effect=swap_then_open):
        with self.assertRaisesRegex(PolicyError, "changed during read"):
            build_code_manifest(self.root, ["a.py"])


def test_duplicate_key_rejected_in_policy(self):
    (self.policy_root / "quantile_policy.json").write_text(
        '{"schema_version":1,"schema_version":1}',
        encoding="utf-8",
    )
    with self.assertRaisesRegex(PolicyError, "duplicate JSON key"):
        self.load_policy_set()


def test_metric_and_candidate_policy_schemas_are_closed(self):
    policies = self.load_policy_set()
    metrics = policies.documents["metric_semantics"]["metrics"]
    self.assertEqual("category-counts", metrics["verification"]["aggregation"])
    self.assertEqual("missingness-only", metrics["cost_amount"]["aggregation"])
    self.assertIsNone(metrics["cost_amount"]["candidate_type"])
    rules = policies.documents["candidate_emission_policy"]["rules"]
    self.assertEqual(
        sorted(rules, key=lambda row: row["candidate_type"].encode("utf-8")),
        rules,
    )
    self.assertEqual(
        {"single-event", "contiguous-adjacent-pair"},
        set(policies.documents["decision_support_policy"]["pattern_kinds"]),
    )
    self.assertEqual(
        5,
        policies.documents["decision_support_policy"]
        ["decision_recurring_minimum_outcome_episodes"],
    )
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_policy_artifacts.py -v
```

Expected: import failure for `policy_artifacts`.
Then add only the named classes/functions with `NotImplementedError` bodies and
rerun. RED-B is valid only when boundary, unsafe-path, swap-race, duplicate-key,
manifest, metric-type, and candidate-rule assertions execute and fail for their
intended semantics rather than import or fixture errors.

- [ ] **Step 3: Add exact immutable policy documents**

Use schema version `1` and these version labels:

```json
{
  "episode_projection": "episode-projection@2",
  "producer_capabilities": "producer-capabilities@1",
  "workflow_generation_mapping": "workflow-generation-mapping@1",
  "metric_semantics": "metric-semantics@1",
  "quantile_policy": "linear-rational-quantile@1",
  "decision_support_policy": "decision-pattern-support@1",
  "lifecycle_health_policy": "draft-staleness@1",
  "candidate_emission_policy": "candidate-emission@1"
}
```

`producer_capabilities.json` initially contains an empty `entries` array. The
first producer declaration is added only with the producer that can actually
emit `workflow_generation`; this prevents a policy committed today from
retroactively invalidating an unupgraded producer. The projection document
owns the bounded Decision enumerations and the v2 field capability map. The
generation mapping contains an empty mapping and remains present in every
policy set.

The projection policy fixes these v0.2 enumerations:

```json
{
  "measurement_source": ["agent-reported", "tool-derived", "unavailable"],
  "phase": ["implementation", "planning", "recovery", "review", "verification"],
  "actor_role": ["coordinator", "implementer", "planner", "reviewer", "tester"],
  "decision_type": ["change-scope", "reject", "resume", "retry", "rollback", "split-task", "stop"],
  "reason_code": ["api-design", "complexity-threshold", "dependency", "integrity-risk", "test-failure", "timeout", "user-direction", "verification-failure"],
  "result": ["inconclusive", "rejected", "superseded", "supported"]
}
```

It fixes `max_decisions` to `12`, `max_scalar_codepoints` to `200`, and marks
the common v1/v2 metrics `elapsed_seconds`, `verification`, `review_rounds`,
`defects_found`, and `rework_count`. It marks token/cost, `test_failures`,
`timeout_count`, and Decisions unsupported in schema v1 and supported in schema
v2.

The remaining policy documents use these exact values:

```json
{
  "quantiles": ["0.25", "0.50", "0.75"],
  "decision_min_episode_support": 3,
  "decision_min_support_ratio": "0.40",
  "decision_recurring_minimum_outcome_episodes": 5,
  "draft_stale_after_seconds": 86400,
  "candidate_classes": ["decision-pattern", "efficiency", "lifecycle-health", "outcome-reliability", "quality"],
  "candidate_order": "candidate-id-ascending-byte-order",
  "candidate_ranking": "none"
}
```

Store each value only in its owning policy file rather than duplicating this
combined explanatory shape.

`metric_semantics.json` has exact top-level keys `schema_version`, `version`,
`metrics`, and `not_applicable_rules`. Its `metrics` object contains exactly
these entries; every entry has the exact keys shown:

```json
{
  "elapsed_seconds": {"semantics_id":"wall-clock-elapsed@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"elapsed-time-distribution"},
  "verification": {"semantics_id":"verification-result@1","value_type":"enum","aggregation":"category-counts","candidate_type":"verification-non-pass"},
  "review_rounds": {"semantics_id":"formal-review-cycle@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"review-round-distribution"},
  "defects_found": {"semantics_id":"confirmed-defect@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"defect-observed"},
  "rework_count": {"semantics_id":"confirmed-rework@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"rework-observed"},
  "input_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"input-token-distribution"},
  "output_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"output-token-distribution"},
  "cache_read_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"cache-read-token-distribution"},
  "cost_amount": {"semantics_id":"measured-cost@1","value_type":"normalized-decimal-string","aggregation":"missingness-only","candidate_type":null},
  "test_failures": {"semantics_id":"confirmed-test-failure@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"test-failure-observed"},
  "timeout_count": {"semantics_id":"confirmed-timeout@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"timeout-observed"}
}
```

`verification` categories are exactly `pass`, `fail`, `not-run`, and
`unknown`; they produce category counts and never numeric quantiles.
`cost_currency` is the required three-letter companion of an observed
`cost_amount`, not a cohort key or independently aggregated metric. v0.2
reports only the cost missingness partition: it never converts a decimal cost
to float, combines currencies, produces a cost distribution, or emits a cost
candidate. `not_applicable_rules` is empty, so `not_applicable_n` is
deterministically zero for every v0.2 metric; changing that requires new policy
bytes/version and changes snapshot identity.

`decision_support_policy.json` additionally fixes pattern kinds to
`single-event` and `contiguous-adjacent-pair`, the analyzable event-key fields
to `phase`, `actor_role`, `decision_type`, `reason_code`, and `result`, minimum
distinct-Episode support to `3`, minimum support ratio to `0.40`, and the
containing cohort's minimum comparable outcome count for recurring evidence to
`5`. These are three separate fields in the immutable hashed policy. The first
two are pattern-support thresholds; the third is the independent cohort-size
threshold and must not be inferred from `comparative_inference_eligible`.

`candidate_emission_policy.json` contains exact-key rule rows with
`candidate_type`, `class`, `source_kind`, `source`, `predicate`,
`minimum_denominator`, `cardinality`, `evidence_fields`, and
`policy_identity_keys`. It fixes these rules:

| Candidate type | Class | Trigger | Cardinality |
|---|---|---|---|
| `stale-drafts` | `lifecycle-health` | `stale_draft_n > 0` | one per cohort |
| `schema-adoption-gap` | `lifecycle-health` | metric `unsupported_by_schema_n > 0` | one per cohort/metric |
| `metric-missingness` | `lifecycle-health` | metric `not_recorded_n > 0` | one per cohort/metric |
| `invalidated-episodes` | `lifecycle-health` | `invalidated_episode_n > 0` | one per cohort |
| `generation-unavailable` | `lifecycle-health` | unavailable-generation Episode count `> 0` | one per descriptive legacy collection |
| `non-success-outcomes` | `outcome-reliability` | failed + partial + rolled-back `> 0` | one per cohort |
| `verification-non-pass` | `quality` | any observed verification category other than `pass` | one per cohort/metric |
| `defect-observed` | `quality` | any observed value `> 0` | one per cohort/metric |
| `rework-observed` | `quality` | any observed value `> 0` | one per cohort/metric |
| `test-failure-observed` | `quality` | any observed value `> 0` | one per cohort/metric |
| `timeout-observed` | `outcome-reliability` | any observed value `> 0` | one per cohort/metric |
| `elapsed-time-distribution` | `efficiency` | `observed_n > 0` | one per cohort/metric |
| `review-round-distribution` | `efficiency` | `observed_n > 0` | one per cohort/metric |
| `input-token-distribution` | `efficiency` | `observed_n > 0` | one per cohort/metric |
| `output-token-distribution` | `efficiency` | `observed_n > 0` | one per cohort/metric |
| `cache-read-token-distribution` | `efficiency` | `observed_n > 0` | one per cohort/metric |
| `decision-single-event` | `decision-pattern` | decision support policy satisfied | one per cohort/pattern |
| `decision-adjacent-pair` | `decision-pattern` | decision support policy satisfied | one per cohort/pattern |

The table is explanatory; the normative machine-readable document has exact
top-level keys `schema_version`, `version`, `candidate_classes`,
`candidate_order`, `candidate_ranking`, and `rules`. `rules` is sorted by
`candidate_type` UTF-8 bytes and contains exactly these rows:

```json
[
  {"candidate_type":"cache-read-token-distribution","class":"efficiency","source_kind":"metric","source":"cache_read_tokens","predicate":"observed-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"decision-adjacent-pair","class":"decision-pattern","source_kind":"decision","source":"contiguous-adjacent-pair","predicate":"decision-support-satisfied","minimum_denominator":"decision-pattern-support@1","cardinality":"one-per-cohort-pattern","evidence_fields":["counts","pattern"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","decision_support_policy"]},
  {"candidate_type":"decision-single-event","class":"decision-pattern","source_kind":"decision","source":"single-event","predicate":"decision-support-satisfied","minimum_denominator":"decision-pattern-support@1","cardinality":"one-per-cohort-pattern","evidence_fields":["counts","pattern"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","decision_support_policy"]},
  {"candidate_type":"defect-observed","class":"quality","source_kind":"metric","source":"defects_found","predicate":"positive-observed-value","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"elapsed-time-distribution","class":"efficiency","source_kind":"metric","source":"elapsed_seconds","predicate":"observed-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"generation-unavailable","class":"lifecycle-health","source_kind":"lifecycle","source":"generation_unavailable_episode_n","predicate":"positive-count","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-legacy-collection","evidence_fields":["counts"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","producer_capability_registry","workflow_generation_mapping"]},
  {"candidate_type":"input-token-distribution","class":"efficiency","source_kind":"metric","source":"input_tokens","predicate":"observed-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"invalidated-episodes","class":"lifecycle-health","source_kind":"lifecycle","source":"invalidated_episode_n","predicate":"positive-count","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort","evidence_fields":["counts"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract"]},
  {"candidate_type":"metric-missingness","class":"lifecycle-health","source_kind":"metric","source":"any-metric","predicate":"not-recorded-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry"]},
  {"candidate_type":"non-success-outcomes","class":"outcome-reliability","source_kind":"outcome","source":"non_success_outcome_n","predicate":"positive-count","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort","evidence_fields":["counts"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract"]},
  {"candidate_type":"output-token-distribution","class":"efficiency","source_kind":"metric","source":"output_tokens","predicate":"observed-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"review-round-distribution","class":"efficiency","source_kind":"metric","source":"review_rounds","predicate":"observed-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"rework-observed","class":"quality","source_kind":"metric","source":"rework_count","predicate":"positive-observed-value","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"schema-adoption-gap","class":"lifecycle-health","source_kind":"metric","source":"any-metric","predicate":"unsupported-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry"]},
  {"candidate_type":"stale-drafts","class":"lifecycle-health","source_kind":"lifecycle","source":"stale_draft_n","predicate":"positive-count","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort","evidence_fields":["counts"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","lifecycle_health_policy"]},
  {"candidate_type":"test-failure-observed","class":"quality","source_kind":"metric","source":"test_failures","predicate":"positive-observed-value","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"timeout-observed","class":"outcome-reliability","source_kind":"metric","source":"timeout_count","predicate":"positive-observed-value","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["missingness","observed_values","quantiles"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry","quantile_policy"]},
  {"candidate_type":"verification-non-pass","class":"quality","source_kind":"metric","source":"verification","predicate":"non-pass-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["category_counts","missingness"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry"]}
]
```

`minimum_denominator` is `one-observed-episode@1` for non-Decision rules and
`decision-pattern-support@1` for Decision rules. The former means at least one
eligible Episode in the rule's source collection, not at least one observed
metric value; the rule predicate supplies any stronger observed-value
requirement. `any-metric` is a selector expanded over concrete metric keys and
is prohibited in emitted candidate evidence. Rule rows name the exact evidence
fields defined in Task 8. Zero-only defect, rework, test-failure, and timeout
values do not emit candidates. All non-Decision candidates are `descriptive`
in v0.2; only a Decision pattern satisfying both its independent support policy
and Task 7 comparative-inference eligibility is `recurring`. Adding a rule,
changing a predicate, or changing evidence fields requires new
candidate-policy bytes/version.

- [ ] **Step 4: Implement manifest validation and policy loading**

```python
@dataclass(frozen=True)
class PolicySet:
    documents: Mapping[str, Mapping[str, object]]
    identities: Mapping[str, Mapping[str, str]]

    def core_identity(self) -> dict[str, dict[str, str]]:
        return {name: dict(self.identities[name]) for name in sorted(self.identities)}


def build_code_manifest(root: Path, relative_paths: Sequence[str]) -> dict:
    rows = []
    seen = set()
    for raw in relative_paths:
        normalized = validate_relative_posix_artifact_path(raw)
        if normalized in seen:
            raise PolicyError("artifact member path is duplicated")
        seen.add(normalized)
        evidence = read_regular_file_evidence(
            root,
            normalized,
            max_bytes=1_048_576,
        )
        rows.append({
            "path": normalized,
            "sha256": evidence.sha256,
            "executable": evidence.executable,
        })
    rows.sort(key=lambda row: row["path"].encode("utf-8"))
    return {"schema_version": 1, "files": rows}
```

`validate_relative_posix_artifact_path` rejects empty strings, NUL, backslash,
POSIX absolute paths, drive-qualified or UNC paths, empty components, `.`, and
`..`; the normalized UTF-8 POSIX spelling must equal the caller's input.

`validate_effective_boundary` accepts exactly the two shapes shown in the
approved design. `started_at.from` must be a canonical second-precision UTC
`Z` instant. `producer_generation.from` must match
`[a-z0-9][a-z0-9._:@+-]{0,199}` and may not be `unknown` or `unavailable`.
`effective_boundary_applies` compares a started-at boundary by parsed UTC
instant and compares a producer-generation boundary by exact string equality.
It returns false when the caller has no explicit producer generation and never
infers ordering from lexical form, numeric suffix, SemVer, Git revision, or
wall clock. The initial v0.2 producer registry remains empty. A future
non-empty producer-generation declaration may be activated only by a producer
path that supplies its own validated explicit identity to this function; old
Episodes without that provenance are never retroactively assigned one.

`read_regular_file_evidence` uses descriptor-relative traversal. Open the root
directory and every directory component with `O_DIRECTORY | O_NOFOLLOW |
O_CLOEXEC`, then inspect and open the final component with `dir_fd` and
`O_NOFOLLOW`. Compare pre-open `stat(..., follow_symlinks=False)`, opened
`fstat`, and post-read `fstat` device/inode/type/size plus modification identity.
Read bounded bytes exactly once from that final descriptor; derive `content`,
SHA-256, and executable mode from those descriptor-bound bytes and metadata.
Never call `Path.read_bytes()` after validation. A component swap, final-file
swap, in-place mutation during the read, oversized file, short read, symlink,
or non-regular file fails closed. If the platform cannot provide equivalent
`dir_fd` and no-follow guarantees, this capability fails closed rather than
falling back to path-based reads. Device and inode are returned as local
evidence but are not serialized into the portable code manifest identity.

`load_policy_set` must parse each JSON document with Task 1
`strict_json_loads`,
validate its exact allowed keys and version, hash its JCS form, build the two
source manifests, and return these core keys:

```python
(
    "analyzer_artifact",
    "canonicalizer_artifact",
    "canonical_projection_contract",
    "producer_capability_registry",
    "workflow_generation_mapping",
    "metric_semantics_registry",
    "quantile_policy",
    "decision_support_policy",
    "lifecycle_health_policy",
    "candidate_emission_policy",
)
```

Every core identity row has exactly `version` and `sha256`. The digest value is
the literal prefix `sha256:` plus 64 lowercase hexadecimal characters computed
as `hashlib.sha256(canonicalize(document_or_code_manifest)).hexdigest()`.
Individual file rows inside a code manifest use the raw 64-character digest
without that prefix. Version labels may not be reused with different bytes.

At the completed implementation boundary, the manifest root is the installed
`workflow-observer` plugin root, never a repository or author-specific path.
The canonicalizer artifact manifest contains exactly
`scripts/canonical_json.py`. The analyzer artifact manifest contains exactly
these result-affecting sources, expressed relative to plugin root and sorted by
UTF-8 path bytes:

```text
scripts/episode_schema.py
scripts/learning_snapshot.py
scripts/policy_artifacts.py
scripts/snapshot_input.py
scripts/store_config.py
scripts/wiki_observations.py
```

`snapshot_store.py` and `workflow_observer_cli.py` are envelope/publication
orchestration and do not enter semantic analysis identity. Their bytes remain
covered by the release/package inventory and tests. If later code moves a
result-affecting decision into either file, the analyzer manifest must be
expanded in the same reviewed change.

- [ ] **Step 5: Run focused and full plugin tests**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_policy_artifacts.py -v
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py'
```

Expected: both commands pass.

- [ ] **Step 6: Commit the policy unit**

```bash
git add plugins/workflow-observer/scripts/policy_artifacts.py \
  plugins/workflow-observer/policies \
  plugins/workflow-observer/tests/test_policy_artifacts.py
git commit -m "feat: close workflow analysis policy identities"
```

---

### Task 3: Episode v2 Schema, Supplement, and Canonical Projection

**Files:**

- Create: `plugins/workflow-observer/scripts/episode_schema.py`
- Create: `plugins/workflow-observer/tests/test_episode_v2.py`
- Create: `plugins/workflow-observer/tests/workflow_evolution_fixtures.py`

**Interfaces:**

- Consumes: Task 1 canonicalizer/strict parser and Task 2 projection/policy
  documents.
- Produces: `EpisodeSchemaError(ValueError)`.
- Produces: `EpisodeV2Supplement`
- Produces: `parse_v2_supplement(text: str, projection: Mapping) -> EpisodeV2Supplement`
- Produces: `build_episode_v2(*, elapsed_seconds: int, completion_metrics: Mapping, supplement: EpisodeV2Supplement) -> dict`
- Produces: `render_episode_block(data: Mapping) -> str`
- Produces: `parse_episode_block(body: str, projection: Mapping) -> tuple[str, dict | None]`
- Produces: `canonical_episode_projection(metadata: Mapping, body: str, projection: Mapping) -> dict`
- Test fixtures produce: `DECISION`, `V2_SUPPLEMENT`, `V1_METADATA`, `V1_BODY`,
  `EXPECTED_CANONICAL_EPISODE_KEYS`, `PRIVACY_SENTINEL`,
  `v1_body_with_privacy_sentinel()`,
  `load_projection_policy()`, `temporary_timezone(name)`, and
  `FakeObservationStore` with explicit portable/LLMWiki layout selection.

- [ ] **Step 1: Write failing v1/v2, privacy, and Decision tests**

```python
def test_v2_round_trip_is_canonical(self):
    supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
    episode = build_episode_v2(
        elapsed_seconds=120,
        completion_metrics={
            "verification": "pass",
            "review_rounds": 1,
            "defects_found": 0,
            "rework_count": 0,
        },
        supplement=supplement,
    )
    block = render_episode_block(episode)
    human_body, parsed = parse_episode_block("human\n\n" + block, self.projection)
    self.assertEqual("human\n", human_body)
    self.assertEqual(episode, parsed)
    self.assertEqual(canonicalize(episode), canonicalize(parsed))


def test_v1_projection_does_not_fabricate_v2_fields(self):
    projected = canonical_episode_projection(V1_METADATA, V1_BODY, self.projection)
    self.assertEqual(1, projected["episode_schema_version"])
    self.assertEqual("unsupported_by_schema", projected["metrics"]["test_failures"]["availability"])
    self.assertEqual([], projected["decisions"])


def test_projection_has_exact_privacy_safe_keys(self):
    metadata = {
        **V1_METADATA,
        "title": PRIVACY_SENTINEL,
        "task_ref": "[[private-task]]",
        "sources": ["raw/private-source.md"],
    }
    projected = canonical_episode_projection(
        metadata,
        v1_body_with_privacy_sentinel(),
        self.projection,
    )
    self.assertEqual(EXPECTED_CANONICAL_EPISODE_KEYS, set(projected))
    encoded = canonicalize(projected)
    self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), encoded)
    self.assertNotIn(b"private-task", encoded)
    self.assertNotIn(b"private-source", encoded)


def test_duplicate_key_rejected_in_episode_supplement(self):
    payload = V2_SUPPLEMENT.replace(
        '"schema_version": 2,',
        '"schema_version": 2, "schema_version": 2,',
        1,
    )
    with self.assertRaisesRegex(EpisodeSchemaError, "duplicate JSON key"):
        parse_v2_supplement(payload, self.projection)


def test_duplicate_key_rejected_in_episode_block(self):
    body = (
        'human\n\n## Episode data\n\n```json\n'
        '{"schema_version":2,"schema_version":2}'
        '\n```\n'
    )
    with self.assertRaisesRegex(EpisodeSchemaError, "duplicate JSON key"):
        parse_episode_block(body, self.projection)


def test_decisions_are_bounded_and_reject_sensitive_shapes(self):
    payload = json.loads(V2_SUPPLEMENT)
    payload["decisions"] = payload["decisions"] * 13
    with self.assertRaises(EpisodeSchemaError):
        parse_v2_supplement(json.dumps(payload), self.projection)
    for prohibited in ("/Users/alice/repo", "api_key=secret", "https://host/path"):
        payload["decisions"] = [{**DECISION, "summary": prohibited}]
        with self.assertRaises(EpisodeSchemaError):
            parse_v2_supplement(json.dumps(payload), self.projection)
```

Define the named constants and helpers in
`workflow_evolution_fixtures.py`. `temporary_timezone` saves/restores `TZ` and
calls `time.tzset()` where available. `FakeObservationStore` creates only
`TemporaryDirectory` descendants, writes exact v1/v2 Markdown fixtures with
mode 0600, places tasks under the selected adapter's canonical relative
directory, and exposes their expected raw SHA-256 values.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_episode_v2.py -v
```

Expected: import failure for `episode_schema`.
Then add only the importable Episode interfaces and rerun. RED-B must reach the
v1/v2, strict-ingress, privacy, and bounded-Decision assertions and fail for
missing semantics rather than parser import or fixture errors.

- [ ] **Step 3: Implement the exact v2 supplement input contract**

`parse_v2_supplement` must call Task 1 `strict_json_loads` on the original
UTF-8 text before schema validation. It may not accept a pre-parsed mapping or
use ordinary `json.loads()` at this ingress.

The mode-0600 supplement file has this exact shape; fields not known are JSON
null and no extra keys are accepted:

```json
{
  "schema_version": 2,
  "execution": {
    "input_tokens": null,
    "output_tokens": null,
    "cache_read_tokens": null,
    "cost_amount": null,
    "cost_currency": null,
    "measurement_source": "unavailable"
  },
  "quality": {
    "test_failures": 0,
    "timeout_count": 0
  },
  "decisions": []
}
```

Token fields are null or safe non-negative integers. `cost_amount` is null or a
non-negative string matching
`(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?`; therefore exponent form, leading zeroes,
and redundant trailing fractional zeroes are invalid. `cost_currency` is null
or three ASCII uppercase letters, and it is non-null exactly when
`cost_amount` is non-null. If all token fields and `cost_amount` are null,
`measurement_source` must be `unavailable`; otherwise it is `tool-derived` or
`agent-reported`.
Quality fields are null or safe non-negative integers. The projection policy
contains the exact low-cardinality Decision enums, and summaries reuse the
existing 200-code-point sanitized scalar rules plus explicit absolute-path,
URL, credential-assignment, and control-character rejection.

- [ ] **Step 4: Implement canonical Episode construction and body parsing**

`build_episode_v2` derives `elapsed_seconds` and the four existing quality
values from the authoritative lifecycle completion. It must not accept those
values from the supplement. `render_episode_block` appends exactly:

````markdown
## Episode data

```json
{"decisions":[],"execution":{},"quality":{},"schema_version":2}
```
````

The real JSON line is `canonicalize(data).decode("utf-8")`. A v1 body has no
Episode block. A v2 body has exactly one final Episode block after Metrics;
duplicate, reordered, non-canonical, or trailing content is invalid.
`parse_episode_block` sends the extracted original JSON bytes through
`strict_json_loads` before comparing their canonical bytes; duplicate keys are
therefore rejected before object construction can collapse them.

`canonical_episode_projection` returns exactly these top-level keys and no
others:

```json
{
  "run_id": "obs-...",
  "episode_schema_version": 2,
  "started_at": "2026-08-02T00:00:00Z",
  "finished_at": "2026-08-02T00:02:00Z",
  "project": "workflow-observatory",
  "workspace": "workflow-observatory",
  "workspace_id": "0123456789ab",
  "revision": "0123456789abcdef",
  "working_tree": "clean",
  "agent_surface": "codex",
  "task_type": "maintenance",
  "workflow_variant": "implementation-with-review",
  "workflow_generation": {"availability": "observed", "value": "implementation-with-review@2"},
  "status": "success",
  "metrics": {},
  "runtime_provenance": null,
  "decisions": []
}
```

Every projected metric named in `metric_semantics.json` is present exactly
once. Its value object has exact keys `availability`, `value`, and `unit`.
`availability` is one of `observed`, `not_recorded`,
`unsupported_by_schema`, or `not_applicable`; `unit` is JSON null except that
an observed `cost_amount` uses its validated three-letter currency. A metric
that is not observed has JSON-null `value` and `unit`. Verification retains its
bounded enum string; integer metrics retain safe non-negative integers; cost
retains its normalized decimal string without conversion. `runtime_provenance`
is JSON null in projection version `episode-projection@2` because neither v1
nor the bounded v2 supplement records a versioned model/runtime identity; it
is never inferred.

`started_at` and a final Episode's `finished_at` are canonical
second-precision UTC `Z` instants. A draft has JSON-null `finished_at`; no other
status may omit it. The v2 Episode block retains validated
`measurement_source` as source attribution for human/audit readback, but
`episode-projection@2` deliberately does not copy it into the Learning Snapshot
because v0.2 defines no comparison, grouping, candidate, or denominator
semantics for that shared execution qualifier. It is not inferred or silently
converted into a per-metric label. An analyzer that consumes it requires a new
projection policy version and reviewed metric semantics.

Each Decision object has exactly `sequence`, `phase`, `actor_role`,
`decision_type`, `reason_code`, `result`, and `summary`. `summary` remains
bounded evidence but never enters a cohort, count, candidate trigger, pattern
key, or candidate ID.

The projection explicitly excludes `title`, `start_mode`, `task_ref`,
`sources`, `superseded_by`, Scope, Execution evidence, Outcome and observation,
Follow-up, `rework_reason`, and all other human body text. Reference identities
and hashes belong only to Task 6 `reference_manifest`; reference file bodies
never enter either representation. `PRIVACY_SENTINEL` is a fixed unique string,
and `v1_body_with_privacy_sentinel()` places it in otherwise valid Scope,
Outcome, Follow-up, and `rework_reason` fields so the test proves exclusion
rather than parser rejection.

- [ ] **Step 5: Run focused and existing record tests**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_episode_v2.py -v
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

Expected: all tests pass; no record lifecycle behavior has changed yet.

- [ ] **Step 6: Commit the Episode schema unit**

```bash
git add plugins/workflow-observer/scripts/episode_schema.py \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/workflow_evolution_fixtures.py
git commit -m "feat: define workflow Episode v2 projection"
```

---

### Task 4: Backward-Compatible Episode v2 Lifecycle and CLI

**Files:**

- Modify: `plugins/workflow-observer/scripts/wiki_observations.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_episode_v2.py`
- Modify: `plugins/workflow-observer/tests/test_portable_cli.py`

**Interfaces:**

- Extends: `StartRequest` with `episode_schema_version: int = 1` and
  `workflow_generation: str | None = None`.
- Extends: `finish_observation(paths: ObservationPaths, run_id: str, status: str, payload: CompletionPayload, superseded_by: str | None = None, now: datetime | None = None, episode_v2: EpisodeV2Supplement | None = None) -> None`.
- CLI adds: `start --episode-schema-version {1,2} [--workflow-generation VALUE]`.
- CLI adds: `finish RUN_ID --status STATUS --from-file MODE_0600_MARKDOWN [--episode-from-file MODE_0600_JSON]`.

- [ ] **Step 1: Add failing lifecycle tests**

Add one complete happy-path test using the existing `PortableCliTests` fixture:

```python
def test_v2_start_and_finish_write_one_canonical_episode_block(self):
    supplement = self.base / "episode.json"
    write_private(supplement, json.dumps({
        "schema_version": 2,
        "execution": {
            "input_tokens": None,
            "output_tokens": None,
            "cache_read_tokens": None,
            "cost_amount": None,
            "cost_currency": None,
            "measurement_source": "unavailable",
        },
        "quality": {"test_failures": 0, "timeout_count": 0},
        "decisions": [],
    }))
    started = run_cli(
        self.home, "start", "--title", "v2", "--subject-root", str(self.subject),
        "--agent-surface", "codex", "--start-mode", "planned",
        "--task-type", "maintenance", "--workflow-variant", "maintenance-basic",
        "--scope-from-file", str(self.scope), "--episode-schema-version", "2",
        "--workflow-generation", "maintenance-basic@2",
    )
    self.assertEqual(0, started.returncode, started.stderr)
    run_id = started.stdout.strip()
    finished = run_cli(
        self.home, "finish", run_id, "--status", "success",
        "--from-file", str(self.completion), "--episode-from-file", str(supplement),
    )
    self.assertEqual(0, finished.returncode, finished.stderr)
    record = (self.home / "store/wiki/observations" / f"{run_id}.md").read_text()
    self.assertIn("schema_version: 2\n", record)
    self.assertIn('workflow_generation: "maintenance-basic@2"\n', record)
    self.assertEqual(1, record.count("## Episode data\n"))
    _human, episode = parse_episode_block(
        record.split("---\n", 2)[2].lstrip(), load_projection_policy()
    )
    self.assertEqual(2, episode["schema_version"])
```

Add four table-driven rejection tests that preserve the record's bytes before
and after the rejected call:

- v2 finish with no `--episode-from-file`;
- v1 start with `--workflow-generation`;
- v1 finish with `--episode-from-file`;
- v2 supplement containing an invalid Decision Event.

Also retain the existing v1 happy-path test and assert its frontmatter does not
gain `schema_version` or `workflow_generation`, and its body does not gain an
Episode block.

The v2 happy path invokes:

```text
start --episode-schema-version 2 --workflow-generation implementation-with-review@2
finish RUN_ID --from-file COMPLETION_FILE --episode-from-file SUPPLEMENT_FILE
```

- [ ] **Step 2: Run the named tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/test_portable_cli.py -v
```

Expected: CLI rejects the new arguments and v2 lifecycle assertions fail.

- [ ] **Step 3: Extend start validation and draft frontmatter**

For v1, preserve the current frontmatter exactly. For v2, add these fields in
the stable frontmatter order immediately after `run_id`:

```yaml
schema_version: 2
workflow_generation: "implementation-with-review@2"
```

Omit `workflow_generation` only when no applicable producer capability requires
it. Validate it against `[a-z0-9][a-z0-9._:@+-]{0,199}` and reject reserved
`unknown` and `unavailable`. Never infer it from Git revision.

- [ ] **Step 4: Extend atomic finish without creating a second write boundary**

Parse and validate the private supplement before acquiring the per-run finish
transition. During `_render_completed_record`, derive the full Episode JSON,
append its canonical block, validate the complete record, and pass the one
combined byte string through the existing temporary-file/fsync/replace path.
Do not write a sidecar and do not mutate the draft when parsing or validation
fails.

- [ ] **Step 5: Run v1/v2 lifecycle and CLI regressions**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

Expected: both v1 and v2 paths pass.

- [ ] **Step 6: Commit the lifecycle unit**

```bash
git add plugins/workflow-observer/scripts/wiki_observations.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/tests/test_episode_v2.py \
  plugins/workflow-observer/tests/test_portable_cli.py
git commit -m "feat: write backward-compatible Episode v2 records"
```

---

### Task 5: One Selected Adapter Semantics for Reference Validation

**Files:**

- Modify: `plugins/workflow-observer/scripts/store_config.py`
- Modify: `plugins/workflow-observer/scripts/wiki_observations.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_store_config.py`
- Modify: `plugins/workflow-observer/tests/test_adapter_conformance.py`

**Interfaces:**

- Produces: `AdapterSemantics(name, projection_version, task_records_relative)`.
- Produces: `ReferenceEvidence(kind: str, identity: str, sha256: str)`.
- Produces: `ReferenceResolver(paths: ObservationPaths, semantics: AdapterSemantics)`
  whose task/source/supersession methods validate and hash the same securely
  opened bytes.
- Produces: `RecordDocument(run_id, metadata, body, source_sha256, references)`
  and `collect_record_documents(paths, semantics) -> ObservationCollection`.
- Portable semantics: `wiki/tasks`.
- LLMWiki semantics: `wiki/tasks/records`.
- Extends: `validate_record(metadata: dict, body: str, paths: ObservationPaths, reference_chain: frozenset[str] | None = None, semantics: AdapterSemantics = PORTABLE_SEMANTICS, resolver: ReferenceResolver | None = None) -> list[str]`.
- Extends: `collect_records(paths: ObservationPaths, semantics: AdapterSemantics = PORTABLE_SEMANTICS) -> tuple[list[dict], set[str]]`.
- Extends: Task 4 `start_observation` and `finish_observation` with an explicit
  `semantics: AdapterSemantics = PORTABLE_SEMANTICS` final parameter.
- Preserves: existing delegated v1 start/finish/report behavior.
- Adds: v2 LLMWiki start/finish through the bundled atomic core with the
  selected LLMWiki semantics. Human `report` remains delegated; canonical
  snapshot acquisition never delegates or combines cores.

- [ ] **Step 1: Add the failing current-layout LLMWiki reference fixture**

Create equivalent portable and LLMWiki roots. Put the canonical task at
`wiki/tasks/example.md` for portable and `wiki/tasks/records/example.md` for
LLMWiki, create byte-equivalent observations referencing `[[example]]`, and
assert both validate under their selected semantics. Also assert LLMWiki fails
closed when only the obsolete `wiki/tasks/example.md` path exists.

In `test_store_config.py`, add
`test_duplicate_key_rejected_in_store_config`: write a config with duplicate
`adapter` keys and require `ConfigError` before adapter selection. This keeps
configuration ingress on the same Task 1 strict parser as the analysis inputs.

- [ ] **Step 2: Run the adapter test and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

Expected: LLMWiki current-layout reference is reported missing because the
bundled core still assumes the legacy `wiki/tasks/{task_id}.md` shape.

- [ ] **Step 3: Add immutable adapter semantics to config resolution**

```python
@dataclass(frozen=True)
class AdapterSemantics:
    name: Literal["portable", "llmwiki"]
    projection_version: str
    task_records_relative: PurePosixPath


def adapter_semantics(config: StoreConfig) -> AdapterSemantics:
    if config.adapter == "portable":
        return AdapterSemantics("portable", "episode-projection@2", PurePosixPath("wiki/tasks"))
    return AdapterSemantics("llmwiki", "episode-projection@2", PurePosixPath("wiki/tasks/records"))
```

No environment probe or fallback may change the selected semantics after
configuration is loaded.
`load_store_config` reads original UTF-8 bytes through `strict_json_loads` and
translates `CanonicalizationError` to `ConfigError`; it never calls ordinary
`json.loads()` on configuration bytes.

- [ ] **Step 4: Thread semantics through every reference check used by snapshots**

Replace the hard-coded `paths.root / "wiki" / "tasks"` task resolver with the
selected normalized relative directory. The validation function must receive
semantics explicitly; do not use a process-global current adapter. Existing
portable callers pass portable semantics. The `snapshot-input` path added next
will pass the one selected profile through integrity, validation, enumeration,
hashing, and projection.

`ReferenceResolver` performs the existing no-symlink component walk, opens the
final regular file once with `O_NOFOLLOW`, confirms before/opened/after
device/inode identity, reads bounded bytes from that descriptor, validates the
task or supersession target from those exact bytes when applicable, and
computes the SHA-256 before closing. The same `ReferenceEvidence` object
satisfies validation and manifest hashing; do not validate one open and hash a
second open. Reference kinds are the fixed values `source`, `task`, and
`supersession-target`.

Refactor the current secure observation scan into
`collect_record_documents`. It retains exact observation/tombstone bytes long
enough to compute source hashes, returns the validated metadata/body plus
reference evidence, and preserves current sorting. Existing `collect_records`
becomes a compatibility projection over that collection, so human report
behavior does not fork into a second scanner.

For a selected LLMWiki adapter, dispatch schema-v1 lifecycle commands exactly
as before. Dispatch schema-v2 `start` directly to the bundled atomic core with
LLMWiki semantics. On `finish`, securely read only the target draft's
frontmatter to choose its recorded schema; a v2 draft finishes through the same
bundled core and a v1 draft delegates. Reject schema ambiguity before consuming
the completion payload. This permits v2 LLMWiki records without requiring an
unversioned external CLI extension and never switches semantics within one
lifecycle.

- [ ] **Step 5: Run adapter, portable CLI, and config tests**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_store_config.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_portable_cli.py -v
```

Expected: all tests pass with no adapter fallback.

- [ ] **Step 6: Commit the adapter-semantics unit**

```bash
git add plugins/workflow-observer/scripts/store_config.py \
  plugins/workflow-observer/scripts/wiki_observations.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/tests/test_store_config.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py
git commit -m "fix: unify selected adapter reference semantics"
```

---

### Task 6: Canonical `snapshot-input` Acquisition

**Files:**

- Create: `plugins/workflow-observer/scripts/snapshot_input.py`
- Create: `plugins/workflow-observer/tests/test_snapshot_input.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_adapter_conformance.py`

**Interfaces:**

- Consumes: selected `AdapterSemantics`, Task 5 `ObservationCollection`,
  Episode projection, and `PolicySet`.
- Produces: `SnapshotInputError(ObservationError)` normalized as validation,
  state, or I/O by the existing CLI boundary.
- Produces: `SnapshotQuery` with absolute UTC half-open interval and optional
  exact project/workspace/workspace-ID/task-type filters.
- Produces: `canonical_interval(since: date, until_inclusive: date, timezone_name: str) -> dict`.
- Produces: `acquire_snapshot_input(paths, semantics, query, policy_set) -> SnapshotInput`.
- Produces: `canonical_reference_manifest(evidence: Iterable[ReferenceEvidence]) -> list[dict[str, str]]`.
- Produces: `derive_store_identity(paths, semantics) -> str | None` using the
  path-free device/inode rule consumed later by Task 9.
- CLI adds: `snapshot-input --since YYYY-MM-DD --until YYYY-MM-DD --timezone IANA_NAME [filters] [--as-of UTC_Z]`.
- CLI stdout: one canonical JSON bundle plus one trailing newline; errors go to
  stderr and return the existing normalized exit code.

`SnapshotQuery` has exactly `interval`, `lifecycle_as_of`, `project`,
`workspace`, `workspace_id`, and `task_type` fields. The last four are string or
JSON null; `interval` is the canonical object returned by
`canonical_interval`, and `lifecycle_as_of` is a second-precision UTC `Z`
string.

- [ ] **Step 1: Write failing interval, hashing, deduplication, and parity tests**

In `SnapshotInputTests.setUp`, create one policy set and two equivalent
`FakeObservationStore` instances. Define `self.acquire(adapter="portable")` as
a thin call to the public `acquire_snapshot_input`; define
`self.add_reviewed_generation_mapping()` to add a reviewed logical mapping for
the one existing physical Episode without creating a second source record.

```python
def test_taipei_dates_become_fixed_utc_half_open_interval(self):
    self.assertEqual({
        "basis": "started_at",
        "since_inclusive": "2026-07-14T16:00:00Z",
        "until_exclusive": "2026-08-02T16:00:00Z",
        "requested_timezone": "Asia/Taipei",
        "requested_dates": {
            "since": "2026-07-15",
            "until_inclusive": "2026-08-02",
        },
    }, canonical_interval(date(2026, 7, 15), date(2026, 8, 2), "Asia/Taipei"))


def test_runtime_timezone_does_not_change_bundle(self):
    with temporary_timezone("UTC"):
        left = self.acquire()
    with temporary_timezone("America/Los_Angeles"):
        right = self.acquire()
    self.assertEqual(left.manifest_bytes, right.manifest_bytes)


def test_derived_view_does_not_duplicate_run_id(self):
    self.add_reviewed_generation_mapping()
    acquired = self.acquire()
    self.assertEqual(1, len(acquired.semantic_bundle["episodes"]))
    self.assertEqual(
        1,
        acquired.semantic_bundle["record_counts"]["selected_episode_n"],
    )


def test_duplicate_physical_sources_for_run_id_fail_gate(self):
    self.store.write_second_physical_record_for_existing_run_id()
    with self.assertRaisesRegex(SnapshotInputError, "duplicate physical Episode"):
        self.acquire()


def test_portable_and_llmwiki_equivalent_fixtures_project_identically(self):
    portable = self.acquire(adapter="portable")
    llmwiki = self.acquire(adapter="llmwiki")
    self.assertEqual(portable.semantic_bundle, llmwiki.semantic_bundle)


def test_adapter_provenance_cites_analyzer_artifact(self):
    acquired = self.acquire()
    analyzer = acquired.semantic_bundle["policy_set"]["analyzer_artifact"]
    self.assertEqual(
        analyzer["sha256"].removeprefix("sha256:"),
        acquired.adapter["implementation_sha256"],
    )
    self.assertNotIn("adapter", acquired.semantic_bundle)


def test_snapshot_input_excludes_human_text_and_reference_bodies(self):
    self.store.write_valid_record_with_privacy_sentinel(PRIVACY_SENTINEL)
    acquired = self.acquire()
    self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), acquired.manifest_bytes)


def test_reference_manifest_collapses_identical_rows_and_rejects_conflicts(self):
    same = ReferenceEvidence("task", "example", "a" * 64)
    self.assertEqual(
        [{"kind": "task", "identity": "example", "sha256": "a" * 64}],
        canonical_reference_manifest([same, same]),
    )
    with self.assertRaisesRegex(SnapshotInputError, "conflicting reference identity"):
        canonical_reference_manifest([
            same,
            ReferenceEvidence("task", "example", "b" * 64),
        ])
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py -v
```

Expected: `snapshot_input` import or CLI-command failure.
Then add only the importable acquisition interfaces and CLI parser entry and
rerun. RED-B must reach the UTC interval, logical-view count, duplicate-source
gate, and adapter parity assertions before acquisition behavior is implemented.

- [ ] **Step 3: Implement absolute interval and explicit lifecycle `as_of`**

Use `zoneinfo.ZoneInfo`. Convert local midnight at `since` and local midnight
after `until_inclusive` to UTC `Z` instants once. Reject an unknown IANA zone or
an end date before the start. Round-trip each localized midnight through UTC
and back to the requested zone; if its local date or `00:00:00` wall time does
not survive exactly, fail closed rather than silently normalizing a skipped or
ambiguous civil boundary. Historical queries set
`lifecycle_as_of = until_exclusive`. A live query accepts only an explicit
second-precision UTC `--as-of` value that is greater than or equal to
`until_exclusive`; it never calls `datetime.now()` inside analysis semantics.
Select an Episode exactly when `since_inclusive <= started_at <
until_exclusive`; never select by finish or invalidation timestamp.

- [ ] **Step 4: Acquire one validated canonical bundle under selected semantics**

`SnapshotInput` separates envelope provenance from the semantic bundle. Its
canonical CLI representation has exactly `adapter`, `store_identity`, and
`semantic_bundle` top-level fields. `adapter` contains selected adapter name
and fixes `implementation_version` to
`workflow-observer-snapshot-adapter@1`. It also contains
`implementation_sha256`, the raw 64-character digest copied from the selected
policy set's `analyzer_artifact.sha256` after removing its `sha256:` prefix.
The adapter fields are envelope provenance and are never copied into semantic
identity; the analyzer artifact identity is already closed independently in
the semantic policy set. Equivalent portable and LLMWiki fixtures therefore
differ in envelope adapter name but not semantic bundle bytes.

The nested semantic bundle shape is:

```json
{
  "schema_version": 1,
  "projection_version": "episode-projection@2",
  "query": {},
  "lifecycle_as_of": "2026-08-02T16:00:00Z",
  "policy_set": {},
  "schema_capabilities": {},
  "record_counts": {},
  "episodes": [],
  "invalidations": [],
  "reference_manifest": [],
  "input_manifest_sha256": "64-lowercase-hex"
}
```

Each Episode row contains its one canonical projection and
`source_sha256` over the exact observation bytes. Invalidation rows contain
target `run_id`, exact tombstone hash, and timestamp. Reference rows contain a
bounded kind, opaque relative identity, and exact content hash; they never
contain an absolute path or file body. Sort Episode and invalidation rows by
`run_id`, and reference rows by `(kind, identity)` UTF-8 bytes.
Treat `(kind, identity)` as the unique reference-manifest key. Identical
`(kind, identity, sha256)` evidence reused by multiple Episodes collapses to
one row. The same `(kind, identity)` observed with different hashes is a Data
Trust Gate failure; a snapshot can never claim that one reference identity
simultaneously denotes multiple byte sequences.
Resolve invalidations before analysis selection: any valid tombstone targeting
a selected Episode is included and applied regardless of the tombstone's own
timestamp. A tombstone for an unselected Episode does not enter the
selection-relevant manifest.

One physical Episode record plus a reviewed generation mapping still projects
to exactly one Episode sample for its `run_id`; the mapping enriches that
projection and is not another representation in `episodes`. Two physical
observation records with the same `run_id`, or conflicting canonical
projections for one `run_id`, are a Data Trust Gate failure rather than a
deduplication opportunity.

`schema_capabilities` is copied from the hashed projection policy and reports
the exact v1/v2 supported-field sets. `record_counts` contains non-negative
integers for `selected_episode_n`, `draft_episode_n`, `final_episode_n`, and
`selected_invalidation_n`; its draft plus final counts must equal selected
Episodes. These are gate diagnostics inside the semantic input, not outcome
denominators.

Compute `input_manifest_sha256` as:

```python
hash_canonical(
    b"workflow-observatory:snapshot-input-manifest:v1\0",
    bundle_without_input_manifest_sha256,
)
```

The selected adapter's integrity, record validation, reference validation,
invalidation resolution, projection, and hashing all execute within this one
function. Do not call or parse human `report`.

- [ ] **Step 5: Add the CLI command without exposing local paths**

The command loads config once, selects semantics once, loads one immutable
policy set, acquires one `SnapshotInput`, and writes its complete path-free
canonical representation plus one newline to stdout. `store_identity` and
adapter implementation identity appear only beside `semantic_bundle`, never
inside it or its manifest hash.
Translate `PolicyError`, `EpisodeSchemaError`, and `CanonicalizationError` to
the CLI's normalized validation error before any stdout is written.

- [ ] **Step 6: Run snapshot-input and adapter parity tests**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py \
  plugins/workflow-observer/tests/test_portable_cli.py -v
```

Expected: all tests pass and equivalent adapter fixtures have identical
canonical semantic bytes.

- [ ] **Step 7: Commit the acquisition unit**

```bash
git add plugins/workflow-observer/scripts/snapshot_input.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/tests/test_snapshot_input.py \
  plugins/workflow-observer/tests/test_adapter_conformance.py
git commit -m "feat: add canonical workflow snapshot input"
```

---

### Task 7: Deterministic Cohorts, Lifecycle Health, Missingness, and Quantiles

**Files:**

- Create: `plugins/workflow-observer/scripts/learning_snapshot.py`
- Create: `plugins/workflow-observer/tests/test_learning_snapshot.py`

**Interfaces:**

- Consumes: canonical `SnapshotInput` and immutable `PolicySet`.
- Produces: `LearningSnapshotError(ValueError)` for policy or semantic
  inconsistencies that passed neither acquisition nor analysis validation.
- Produces: `build_snapshot_core(snapshot_input: SnapshotInput, policy_set: PolicySet) -> dict`.
- Produces: `linear_rational_quantile(values: Sequence[int], numerator: int, denominator: int) -> str | None`.
- Produces: explicit outcome and lifecycle denominators and per-metric
  eligibility partitions.

- [ ] **Step 1: Write failing denominator and missingness tests**

The test class defines deterministic data constructors only:
`self.bundle(outcomes=(), drafts=(), superseded=0, invalidated=frozenset(),
as_of="2026-08-02T16:00:00Z")` returns a validated `SnapshotInput`, and
`self.metric_partition(v1_absent, v2_null, v2_values)` calls
`build_snapshot_core` then selects the named `test_failures` metric. These
helpers do not reproduce production grouping or classification logic.
`self.metric_output(name, values)` and `self.cost_metric_output(values)` build
validated Episode projections through shared fixtures, call
`build_snapshot_core`, and select the requested public metric result; they do
not implement aggregation inside the test harness.

```python
def test_outcome_and_lifecycle_denominators_are_separate(self):
    core = build_snapshot_core(self.bundle(
        outcomes=["success", "failed", "partial", "rolled-back"],
        drafts=["active", "stale"],
        superseded=1,
        invalidated={"superseded-run"},
    ), self.policies)
    cohort = core["cohorts"][0]
    self.assertEqual(4, cohort["outcome_episode_n"])
    self.assertEqual(1, cohort["superseded_episode_n"])
    self.assertEqual(2, cohort["draft_episode_n"])
    self.assertEqual(1, cohort["invalidated_episode_n"])
    self.assertEqual("descriptive", cohort["evidence_strength"])


def test_v1_absence_is_not_zero_or_v2_not_recorded(self):
    metric = self.metric_partition(v1_absent=4, v2_null=2, v2_values=[0, 1])
    self.assertEqual({
        "eligible_episode_n": 8,
        "observed_n": 2,
        "not_recorded_n": 2,
        "unsupported_by_schema_n": 4,
        "not_applicable_n": 0,
    }, metric["missingness"])
    self.assertEqual([0, 1], metric["observed_values"])


def test_historical_staleness_uses_bound_as_of(self):
    left = build_snapshot_core(self.bundle(as_of="2026-08-02T16:00:00Z"), self.policies)
    right = build_snapshot_core(self.bundle(as_of="2026-08-02T16:00:00Z"), self.policies)
    later = build_snapshot_core(self.bundle(as_of="2026-08-03T16:00:00Z"), self.policies)
    self.assertEqual(left, right)
    self.assertNotEqual(left, later)


def test_verification_uses_category_counts_not_quantiles(self):
    metric = self.metric_output("verification", ["pass", "fail", "pass"])
    self.assertEqual(
        {"fail": 1, "not-run": 0, "pass": 2, "unknown": 0},
        metric["category_counts"],
    )
    self.assertIsNone(metric["observed_values"])
    self.assertIsNone(metric["quantiles"])


def test_cost_is_missingness_only_and_never_combines_currencies(self):
    metric = self.cost_metric_output([
        ("1.25", "USD"),
        ("2.5", "EUR"),
        (None, None),
    ])
    self.assertEqual("missingness-only", metric["aggregation"])
    self.assertEqual(2, metric["missingness"]["observed_n"])
    self.assertEqual(1, metric["missingness"]["not_recorded_n"])
    self.assertIsNone(metric["observed_values"])
    self.assertIsNone(metric["category_counts"])
    self.assertIsNone(metric["quantiles"])
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_snapshot.py -v
```

Expected: import failure for `learning_snapshot`.

- [ ] **Step 3: Implement base cohorts and generation comparability**

The returned core has exactly these top-level keys before Task 8 adds candidate
evidence:

```text
schema_version
analyzer_version
query
lifecycle_health_policy
analysis_policy_set
input_manifest
exclusion_ledger
cohorts
decision_patterns
candidates
```

Use integer `schema_version: 1` and string
`analyzer_version: workflow-learning-analyzer@0.2.0`. `input_manifest` contains
the selected manifest digest plus sorted `(run_id, source_sha256)` Episode rows,
sorted invalidation rows, and sorted reference rows required to reproduce the
gate. It does not contain record bodies. `decision_patterns` and `candidates`
are empty arrays until Task 8. The core never contains `generated_at`, adapter
name/implementation, store identity, an absolute path, or narrative.
`lifecycle_health_policy` contains exactly `policy_id`, `policy_sha256`,
`as_of`, and integer `stale_after_seconds`; `as_of` comes from the input bundle,
not the runtime clock.

Group outcomes by the exact six-part key:

```python
(
    project,
    workspace,
    workspace_id,
    task_type,
    workflow_variant,
    workflow_generation,
)
```

Every cohort and legacy descriptive collection exposes the exact handoff fields
`comparative_inference_eligible` and `comparative_inference_exclusions`.
Eligibility is true only when the exclusions array is empty. The array contains
only the sorted bounded values `generation-unavailable` and
`heterogeneous-runtime-provenance`; it is derived from the same facts that
produce the exclusion ledger and cannot be independently overridden by Task 8.
Sample size and Decision support remain separate gates: a cohort can be
comparable but too small for a particular inference.

Final non-invalidated `success`, `partial`, `failed`, and `rolled-back`
Episodes enter outcome analysis. Draft, superseded, and invalidated records
enter only overlapping lifecycle predicates. Generation `unavailable` uses a
separately labeled legacy descriptive collection and never treats all missing
generations as one workflow generation. Fewer than five comparable outcome
Episodes remains descriptive.

The core includes an `exclusion_ledger` sorted by `(run_id, reason,
excluded_from)` UTF-8 bytes. Reasons are the bounded values `draft`,
`superseded`, `invalidated`, `generation-unavailable`, and
`heterogeneous-runtime-provenance`; `excluded_from` is either `outcome-analysis`
or `comparative-inference`. A run may have more than one lifecycle predicate
but appears at most once for each exact reason/scope pair. Generation-unavailable
legacy Episodes remain eligible for separately labeled descriptive outcome
counts but are excluded from comparative inference. Known multiple runtime
generations without an approved compatibility policy add
`heterogeneous-runtime-provenance` and force descriptive output; unknown
runtime remains JSON null and is never inferred.

- [ ] **Step 4: Implement metric eligibility and exact rational quartiles**

For each metric, use its field-specific semantics registry to classify every
eligible Episode into exactly one of `observed`, `not_recorded`,
`unsupported_by_schema`, or `not_applicable`. Assert the four counts sum to the
eligible denominator. Invalid values must already have failed acquisition.

Every metric result has the exact keys `metric`, `semantics_id`, `value_type`,
`aggregation`, `missingness`, `observed_values`, `category_counts`, and
`quantiles`. For `integer-quantiles`, `observed_values` is the sorted integer
array, `category_counts` is null, and `quantiles` has exact keys `p25`, `p50`,
and `p75`. For `category-counts`, `observed_values` and `quantiles` are null and
`category_counts` contains every allowed enum category, including zero counts,
sorted by UTF-8 key bytes. For `missingness-only`, all three result fields are
null: values contribute only to the four-bucket missingness partition and are
not reproduced in the analysis output.

For p25/p50/p75, sort integers, use index
`(n - 1) * numerator / denominator`, interpolate with `fractions.Fraction`, and
render an exact normalized non-exponent decimal string. Because the approved
quartile denominators divide powers of two, any non-terminating decimal is a
policy/configuration error, not a rounded float.

Never call the quantile function for `verification` or `cost_amount`.
`verification` produces only the four fixed category counts. `cost_amount`
produces missingness only, retains no decimal values in aggregates, never
converts through binary float, and never groups or combines currencies. A
mixed-currency cohort is therefore valid for cost missingness but has no cost
distribution or candidate.

- [ ] **Step 5: Run focused tests and deterministic repeat checks**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_snapshot.py -v
```

Expected: all denominator, missingness, staleness, and quantile tests pass.

- [ ] **Step 6: Commit the deterministic analysis unit**

```bash
git add plugins/workflow-observer/scripts/learning_snapshot.py \
  plugins/workflow-observer/tests/test_learning_snapshot.py
git commit -m "feat: build deterministic workflow learning cohorts"
```

---

### Task 8: Episode-Level Decision Support and Stable Candidates

**Files:**

- Modify: `plugins/workflow-observer/scripts/learning_snapshot.py`
- Modify: `plugins/workflow-observer/tests/test_learning_snapshot.py`

**Interfaces:**

- Extends: `build_snapshot_core` with Decision pattern evidence and unranked
  candidates.
- Produces: `candidate_id(candidate_evidence: Mapping) -> str`.
- Uses: `decision-pattern-support@1` minimum three distinct Episodes and at
  least 40 percent of eligible Episodes, plus a separately owned minimum of
  five comparable outcome Episodes before recurring evidence is allowed.

- [ ] **Step 1: Add failing Episode-support and candidate-identity tests**

Define `self.core_with_decisions(events_by_run_id)` by placing the supplied
Decision arrays into five otherwise equivalent v2 Episode projections and
calling public `build_snapshot_core`. Define `self.lifecycle_candidate(as_of)`
from one stale-draft fixture and `self.core_with_multiple_candidates()` from
one fixture that deterministically triggers two candidate classes.
`self.core_with_recurring_decision_pattern(...)` accepts explicit
`outcome_episode_n` and `supporting_episode_n`, builds that many valid
projections sharing one supported pattern, and may vary only the named
generation or runtime provenance through the Task 7 fixture boundary.
`self.core_with_equal_missingness(metrics)` assigns equal missingness to the
named concrete metrics. No helper computes IDs, support, comparability, wildcard
expansion, or candidate eligibility itself.

```python
def test_one_episode_cannot_dominate_decision_recurrence(self):
    core = self.core_with_decisions({"a": [DECISION] * 10, "b": [], "c": [], "d": [], "e": []})
    pattern = core["decision_patterns"][0]
    self.assertEqual(10, pattern["event_count"])
    self.assertEqual(1, pattern["episode_count_with_event"])
    self.assertEqual(5, pattern["eligible_episode_n"])
    self.assertEqual("descriptive", pattern["evidence_strength"])


def test_candidate_ids_bind_policy_and_staleness_as_of(self):
    first = self.lifecycle_candidate(as_of="2026-08-02T16:00:00Z")
    second = self.lifecycle_candidate(as_of="2026-08-03T16:00:00Z")
    self.assertNotEqual(first["candidate_id"], second["candidate_id"])


def test_candidates_are_unranked_and_byte_sorted(self):
    candidates = self.core_with_multiple_candidates()["candidates"]
    self.assertEqual(
        sorted(candidates, key=lambda item: item["candidate_id"].encode("ascii")),
        candidates,
    )
    for candidate in candidates:
        self.assertNotIn("priority", candidate)
        self.assertNotIn("confidence", candidate)
        self.assertNotIn("actionability", candidate)


def test_decision_sequences_are_only_single_events_and_adjacent_pairs(self):
    second = {**DECISION, "sequence": 2, "reason_code": "dependency"}
    third = {**DECISION, "sequence": 3, "reason_code": "timeout"}
    core = self.core_with_decisions({
        "a": [DECISION, second, third],
        "b": [], "c": [], "d": [], "e": [],
    })
    patterns = {
        (row["pattern_kind"], tuple(item["reason_code"] for item in row["pattern"]))
        for row in core["decision_patterns"]
    }
    self.assertIn(("single-event", ("complexity-threshold",)), patterns)
    self.assertIn(("contiguous-adjacent-pair", ("complexity-threshold", "dependency")), patterns)
    self.assertIn(("contiguous-adjacent-pair", ("dependency", "timeout")), patterns)
    self.assertNotIn(("contiguous-adjacent-pair", ("complexity-threshold", "timeout")), patterns)
    self.assertFalse(any(len(pattern) > 2 for _kind, pattern in patterns))


def test_zero_only_adverse_metrics_emit_no_quality_or_timeout_candidate(self):
    core = self.core_with_zero_quality_and_timeout_metrics()
    types = {candidate["candidate_type"] for candidate in core["candidates"]}
    self.assertTrue({
        "defect-observed", "rework-observed", "test-failure-observed", "timeout-observed"
    }.isdisjoint(types))


def test_unavailable_generation_decision_pattern_remains_descriptive(self):
    core = self.core_with_recurring_decision_pattern(
        outcome_episode_n=5,
        supporting_episode_n=3,
        workflow_generation={"availability": "unavailable", "value": None},
    )
    self.assertEqual(
        "descriptive",
        core["decision_patterns"][0]["evidence_strength"],
    )
    self.assertFalse(any(
        candidate["class"] == "decision-pattern"
        for candidate in core["candidates"]
    ))


def test_heterogeneous_runtime_cohort_emits_no_decision_candidate(self):
    core = self.core_with_recurring_decision_pattern(
        outcome_episode_n=5,
        supporting_episode_n=3,
        runtime_generations=["runtime@1", "runtime@2"],
    )
    self.assertIn(
        "heterogeneous-runtime-provenance",
        {row["reason"] for row in core["exclusion_ledger"]},
    )
    self.assertEqual(
        "descriptive",
        core["decision_patterns"][0]["evidence_strength"],
    )
    self.assertFalse(any(
        candidate["class"] == "decision-pattern"
        for candidate in core["candidates"]
    ))


def test_small_comparable_cohort_cannot_emit_recurring_decision_candidate(self):
    core = self.core_with_recurring_decision_pattern(
        outcome_episode_n=4,
        supporting_episode_n=3,
    )
    pattern = core["decision_patterns"][0]
    self.assertEqual(3, pattern["episode_count_with_event"])
    self.assertEqual(
        {"numerator": 3, "denominator": 4},
        pattern["support_fraction"],
    )
    self.assertEqual("descriptive", pattern["evidence_strength"])
    self.assertFalse(any(
        candidate["class"] == "decision-pattern"
        for candidate in core["candidates"]
    ))


def test_five_comparable_outcomes_can_emit_recurring_decision_candidate(self):
    core = self.core_with_recurring_decision_pattern(
        outcome_episode_n=5,
        supporting_episode_n=3,
    )
    pattern = core["decision_patterns"][0]
    self.assertEqual(3, pattern["episode_count_with_event"])
    self.assertEqual(
        {"numerator": 3, "denominator": 5},
        pattern["support_fraction"],
    )
    self.assertEqual("recurring", pattern["evidence_strength"])
    candidates = [
        candidate for candidate in core["candidates"]
        if candidate["class"] == "decision-pattern"
    ]
    self.assertEqual(1, len(candidates))


def test_any_metric_candidates_bind_concrete_metric_identity(self):
    core = self.core_with_equal_missingness(
        metrics=("input_tokens", "output_tokens"),
    )
    candidates = [
        item for item in core["candidates"]
        if item["candidate_type"] == "metric-missingness"
    ]
    self.assertEqual(
        {"input_tokens", "output_tokens"},
        {item["source"]["identity"] for item in candidates},
    )
    self.assertTrue(all(
        item["evidence"]["missingness"]["observed_n"] == 0
        for item in candidates
    ))
    self.assertEqual(2, len({item["candidate_id"] for item in candidates}))
```

- [ ] **Step 2: Run named tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_snapshot.py -v
```

Expected: Decision pattern and candidates are absent.

- [ ] **Step 3: Implement distinct-Episode support**

Decision analysis uses only non-invalidated v2 outcome Episodes in the cohort;
that full set, including Episodes with zero Decisions, is
`eligible_episode_n`. Order each Episode's validated events by `sequence` and
generate exactly two pattern kinds: each single event and each contiguous
adjacent pair. Do not generate arbitrary-length sequences, non-contiguous
subsequences, or combinations.

Each event key contains exactly the five versioned low-cardinality fields
`phase`, `actor_role`, `decision_type`, `reason_code`, and `result`.
`summary` and `sequence` never enter a pattern key, group, count, metric label,
candidate trigger, or candidate ID. A pattern row has exact keys
`pattern_kind`, `pattern`, `event_count`, `episode_count_with_event`,
`eligible_episode_n`, `support_fraction`, and `evidence_strength`. `pattern` is
a one- or two-element array of exact event-key objects. `event_count` counts all
occurrences, but the same pattern repeated within one Episode contributes only
one to `episode_count_with_event`. `support_fraction` has exact integer keys
`numerator` and `denominator`; compare it to the policy's normalized decimal
threshold through `fractions.Fraction`, never float or rounded decimal output.
Below the independent `3 Episodes + 0.40` policy the row is `descriptive`; at
or above both thresholds it is eligible for the comparability gate described
next. Pattern rows remain sorted by JCS bytes of
`(pattern_kind, pattern)`.

A Decision pattern is `recurring` if and only if all four conditions hold:

1. `episode_count_with_event >= decision_min_episode_support`;
2. `support_fraction >= decision_min_support_ratio`;
3. the containing cohort has `comparative_inference_eligible: true` from Task
   7; and
4. `outcome_episode_n >= decision_recurring_minimum_outcome_episodes`.

The three numeric thresholds are read from the immutable, hashed
`decision_support_policy`; none may be duplicated as an implementation
constant. Failure of any condition leaves the pattern `descriptive` and emits
no `decision-pattern` candidate. In particular, four comparable outcome
Episodes with three supporting Episodes remain descriptive, while five
comparable outcomes with three supporting Episodes cross the cohort-size and
support boundaries. A cohort whose exclusions contain
`generation-unavailable` or `heterogeneous-runtime-provenance` may still
expose deterministic Decision pattern counts, but every pattern remains
`descriptive`. Task 8 consumes the Task 7 comparability fields directly and
may not recompute, weaken, or override them.

- [ ] **Step 4: Emit deterministic unranked candidates**

Evaluate only the exact ordered rule rows in `candidate_emission_policy.json`;
no code-default or LLM judgment may create an additional candidate. Each rule
produces at most the cardinality declared in Task 2. A candidate evidence
object has exactly these top-level keys:

```json
{
  "candidate_type": "...",
  "class": "...",
  "cohort": {},
  "source": {},
  "policy_identities": {},
  "denominators": {},
  "evidence": {},
  "evidence_strength": "descriptive"
}
```

`cohort` contains the exact six cohort-key fields. `source` has exact keys
`kind`, `identity`, and `semantics_id`. `denominators` always has exact keys
`eligible_episode_n`, `outcome_episode_n`, and `supporting_episode_n`, using
JSON null when a denominator is not applicable. `evidence` always has exact
keys `counts`, `missingness`, `observed_values`, `category_counts`, `quantiles`,
and `pattern`, using JSON null for fields not named by the rule's
`evidence_fields`. `policy_identities` contains exactly the version/hash rows
named by that rule's `policy_identity_keys`; no mutable label is substituted.
For lifecycle candidates it includes the exact staleness policy and `as_of` in
evidence. Non-Decision candidates remain `descriptive` in v0.2. A Decision
candidate is emitted only for a `recurring` pattern row.

For a metric rule, the policy `source` is a selector, never the emitted source
identity. A concrete source such as `input_tokens` selects exactly that metric.
`any-metric` iterates every concrete key in `metric_semantics.json` in UTF-8
byte order and evaluates the predicate independently. Every emitted metric
candidate sets `source.kind` to `metric`, `source.identity` to that concrete
metric key, and `source.semantics_id` to that metric entry's exact semantics
ID. Consequently `input_tokens` and `output_tokens` remain different candidate
evidence even when their semantics IDs and missingness counts are equal; the
concrete identity is included in `candidate_id` input.

`one-observed-episode@1` means the rule's applicable source collection has at
least one eligible Episode. It does **not** mean a metric has
`observed_n > 0`. The rule predicate remains the authoritative additional
threshold: `observed-count-positive` and `positive-observed-value` require
observed metric values, while missingness, unsupported-schema, stale,
invalidation, generation, and outcome rules may trigger with metric
`observed_n == 0` when their own named count is positive.

For the two Decision rules, `decision-support-satisfied` is the exact
four-condition predicate defined in Step 3: the pattern meets the hashed
distinct-Episode and support-ratio thresholds, the cohort is eligible for
comparative inference, and the cohort meets the hashed minimum of five
comparable outcome Episodes. The 18 policy rules remain unchanged; this
predicate cannot be weakened by a code default.

Build all values from deterministic snapshot fields only. Compute:

```python
candidate_id = hash_canonical(
    b"workflow-observatory:learning-candidate:v1\0",
    candidate_evidence,
)
```

Then add `candidate_id` and sort candidates by its ASCII bytes. Do not add LLM
narrative, priority, probability, causal wording, actionability, or proposal
data.

- [ ] **Step 5: Run the full learning-snapshot test module**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_snapshot.py -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit the candidate unit**

```bash
git add plugins/workflow-observer/scripts/learning_snapshot.py \
  plugins/workflow-observer/tests/test_learning_snapshot.py
git commit -m "feat: emit stable workflow learning candidates"
```

---

### Task 9: Manifest A/B Stable Read and Atomic Snapshot Publication

**Files:**

- Create: `plugins/workflow-observer/scripts/snapshot_store.py`
- Create: `plugins/workflow-observer/tests/test_snapshot_publication.py`
- Modify: `plugins/workflow-observer/scripts/workflow_observer_cli.py`
- Modify: `plugins/workflow-observer/tests/test_portable_cli.py`

**Interfaces:**

- Produces: `create_learning_snapshot(*, acquire, query, policy_set, home, generated_at) -> PublishedSnapshot`.
- Produces: `PublishedSnapshot(snapshot_id: str, path: Path, artifact: Mapping, created: bool)`.
- Produces: `SnapshotPublicationError(ObservationError)` and
  `validate_learning_artifact(artifact: Mapping) -> None`.
- Produces: `validate_learning_artifact_bytes(raw: bytes, *, expected_snapshot_id: str | None = None) -> Mapping`.
- Produces: `read_learning_artifact(snapshot_dir: Path, expected_snapshot_id: str) -> Mapping`.
- CLI adds: `snapshot` with the same bounded query flags as `snapshot-input`.
- Publishes: one file whose name is the computed 64-character lowercase
  `snapshot_id` plus `.json` beneath
  `$WORKFLOW_OBSERVATORY_HOME/learning/snapshots/`.
- Prints: one canonical path-free JSON response with exactly two top-level
  fields, Boolean `created` and object `snapshot`, plus one trailing newline.
  The nested object is the complete sanitized artifact so skills never open
  store files directly.

- [ ] **Step 1: Write failing stable-read, atomicity, and idempotence tests**

The publication test fixture defines `self.acquire_then_mutate_selection()` as
a callable whose first invocation returns fixture A and whose second invocation
finalizes one selected draft through the public lifecycle before returning
fixture B. `self.publish(generated_at)` calls only public
`create_learning_snapshot`; `FIXED_NOW` is
`2026-08-03T00:01:00Z` from `workflow_evolution_fixtures.py`.

```python
def test_store_change_between_manifests_aborts_without_artifact(self):
    acquire = self.acquire_then_mutate_selection()
    with self.assertRaisesRegex(SnapshotPublicationError, "changed during analysis"):
        create_learning_snapshot(
            acquire=acquire,
            query=self.query,
            policy_set=self.policies,
            home=self.home,
            generated_at=FIXED_NOW,
        )
    self.assertEqual([], list((self.home / "learning/snapshots").glob("*.json")))


def test_identical_core_reuses_existing_immutable_artifact(self):
    first = self.publish(generated_at="2026-08-03T00:01:00Z")
    second = self.publish(generated_at="2026-08-04T00:01:00Z")
    self.assertTrue(first.created)
    self.assertFalse(second.created)
    self.assertEqual(first.snapshot_id, second.snapshot_id)
    self.assertEqual(first.path.read_bytes(), second.path.read_bytes())


def test_authoritative_tamper_cannot_become_acceptance(self):
    artifact = strict_json_loads(self.publish().path.read_bytes())
    artifact["authoritative"] = True
    with self.assertRaises(SnapshotPublicationError):
        validate_learning_artifact(artifact)


def test_duplicate_key_rejected_in_snapshot_artifact(self):
    artifact = self.publish().path.read_bytes()
    ambiguous = artifact.replace(
        b'{"artifact_type":',
        b'{"artifact_type":"learning-snapshot","artifact_type":',
        1,
    )
    with self.assertRaisesRegex(SnapshotPublicationError, "duplicate JSON key"):
        validate_learning_artifact_bytes(ambiguous)


def test_artifact_readback_recomputes_snapshot_identity(self):
    artifact = strict_json_loads(self.publish().path.read_bytes())
    artifact["core"]["analyzer_version"] = "tampered"
    raw = artifact_bytes_with_recomputed_file_digest(artifact)
    with self.assertRaisesRegex(SnapshotPublicationError, "snapshot identity mismatch"):
        validate_learning_artifact_bytes(raw)


def test_artifact_readback_recomputes_file_digest(self):
    artifact = strict_json_loads(self.publish().path.read_bytes())
    artifact["artifact_sha256"] = "0" * 64
    with self.assertRaisesRegex(SnapshotPublicationError, "artifact digest mismatch"):
        validate_learning_artifact_bytes(canonicalize(artifact))


def test_artifact_readback_rejects_noncanonical_bytes(self):
    raw = self.publish().path.read_bytes()
    with self.assertRaisesRegex(SnapshotPublicationError, "not canonical JCS"):
        validate_learning_artifact_bytes(b" " + raw)


def test_artifact_readback_rejects_symlink_and_filename_identity_mismatch(self):
    published = self.publish()
    with self.assertRaisesRegex(SnapshotPublicationError, "snapshot filename mismatch"):
        validate_learning_artifact_bytes(
            published.path.read_bytes(),
            expected_snapshot_id="0" * 64,
        )
    link = published.path.parent / ("f" * 64 + ".json")
    link.symlink_to(published.path.name)
    with self.assertRaisesRegex(SnapshotPublicationError, "unsafe snapshot target"):
        read_learning_artifact(published.path.parent, "f" * 64)


def test_snapshot_rejects_narrative_and_annotation_fields(self):
    artifact = strict_json_loads(self.publish().path.read_bytes())
    for field in ("narrative", "annotation", "summary_markdown"):
        with self.subTest(field=field):
            tampered = {**artifact, field: "different text"}
            with self.assertRaises(SnapshotPublicationError):
                validate_learning_artifact(tampered)
    self.assertEqual([], list((self.home / "learning/annotations").glob("*")))


def test_concurrent_publishers_never_overwrite(self):
    results = self.publish_from_two_processes_released_by_one_barrier()
    self.assertEqual([False, True], sorted(result.created for result in results))
    self.assertEqual(1, len({result.snapshot_id for result in results}))
    final_path = results[0].path
    read_learning_artifact(final_path.parent, results[0].snapshot_id)
    self.assertEqual([], list(final_path.parent.glob(".snapshot-*.tmp")))
```

The concurrency fixture launches two independent Python processes against the
same fake store and snapshots directory, waits until both are ready, then
releases one barrier. A sequential double call is retained as the idempotence
test but is not accepted as evidence for the concurrent publisher contract.

- [ ] **Step 2: Run publication tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_publication.py -v
```

Expected: import failure for `snapshot_store`.
Then add only the importable publisher interfaces and rerun. RED-B must reach
stable-read, strict artifact validation, no-narrative, idempotence, and true
concurrent-publisher assertions before publication behavior is implemented.

- [ ] **Step 3: Build the semantic core, envelope, and identities in memory**

Call `acquire()` for manifest A, build the core, and compute:

```python
snapshot_id = hash_canonical(
    b"workflow-observatory:learning-snapshot-core:v1\0",
    snapshot_core,
)
```

Catch `LearningSnapshotError` only at this orchestration boundary and translate
it to `SnapshotPublicationError("validation", message)`; do not expose a Python
traceback or continue with a partial core.

The envelope fixes `artifact_type` to `learning-snapshot`, `authoritative` to
false, includes second-precision UTC `generated_at`, privacy-safe opaque
`store_identity`, adapter implementation provenance, `snapshot_id`, and the
core. Reuse Task 6 `derive_store_identity`: when the selected root has nonzero
device/inode identity, it returns the SHA-256 of the NUL-terminated domain
`workflow-observatory:store-identity:v1` followed by adapter name, decimal
`st_dev`, and decimal `st_ino` separated by NUL bytes; otherwise it returns JSON
null and never infers an identity from an absolute path. Compute `artifact_sha256`
as `hashlib.sha256(canonicalize(artifact_without_artifact_sha256)).hexdigest()`.
It has no semantic domain separator because it is a byte-integrity digest of
the already typed complete artifact, not a reusable semantic identifier.
`validate_learning_artifact_bytes` parses persisted bytes only through Task 1
`strict_json_loads`. Persisted files contain exactly JCS bytes without a
trailing newline; require `raw == canonicalize(parsed_artifact)` before any
identity is trusted. Require the exact artifact keys, then call
`validate_learning_artifact`. Recompute `snapshot_id` from `core` with the
snapshot-core domain separator and compare it to the claimed value. Remove
`artifact_sha256`, recompute SHA-256 over JCS of the remaining complete
artifact, and compare it to the claimed digest. When
`expected_snapshot_id` is supplied, require it to equal the recomputed ID so
the filename cannot disagree with content. Unknown narrative or annotation
fields, duplicate keys, non-I-JSON input, stale IDs, stale file digests, and
non-canonical bytes fail before an artifact can be reused.

`artifact_bytes_with_recomputed_file_digest` is a test-only helper that removes
and recomputes only `artifact_sha256`; it intentionally leaves a stale
`snapshot_id` so the focused test proves the semantic identity check rather
than failing first on generic file integrity.

- [ ] **Step 4: Reacquire B and publish only after exact manifest equality**

Call the same `acquire()` with the same selected adapter, query, and policy set.
Require exact canonical manifest bytes A == B. On mismatch, delete any private
temporary analysis file and return a normalized state error. After equality,
publish with this exact no-clobber sequence:

1. Create one unique mode-0600 temporary file inside the same mode-0700
   snapshots directory.
2. Write the complete canonical artifact bytes and `fsync` its open descriptor.
3. Call `os.link(temp, target)` without a preceding existence check.
4. On success, this publisher alone returns `created=true`. On `EEXIST`, never
   overwrite: securely read the existing target through
   `read_learning_artifact(snapshot_dir, snapshot_id)`. That function validates
   the 64-lowercase-hex ID, opens the snapshots directory, then reuses Task 2
   descriptor-relative `read_regular_file_evidence` (or an exactly equivalent
   same-descriptor/no-follow bounded read) for `<snapshot_id>.json`; a symlink,
   non-regular file, component swap, read mutation, or oversized artifact fails
   closed. Pass the exact filename ID into
   `validate_learning_artifact_bytes`.
5. If the existing artifact has the same `snapshot_id` and byte-identical
   canonical core, return that existing artifact as `created=false`; otherwise
   fail closed as an identity collision.
6. `fsync` the snapshots directory after successful publication, unlink the
   private temporary file in every exit path, then `fsync` the directory again
   so temp removal is durable. Never call `os.replace()` for the final target.

The temporary file and target must be on the same filesystem. If hard-link
no-clobber publication or equivalent guarantees are unavailable, fail closed;
do not degrade to check-then-replace or direct target writes.

- [ ] **Step 5: Add the bounded `snapshot` CLI operation**

The CLI loads config/policies once, creates a privacy-safe store identity from
filesystem identity without emitting a path, and invokes the publisher. It
writes the canonical response containing the returned immutable artifact to
stdout. It does not call `report`, does not persist LLM narrative, and does not
mutate observations, invalidations, tasks, sources, workflows, branches, or
PRs.

- [ ] **Step 6: Run publication, CLI, and security regressions**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_snapshot_publication.py \
  plugins/workflow-observer/tests/test_portable_cli.py \
  plugins/workflow-observer/tests/test_snapshot_input.py -v
```

Expected: all tests pass; the store-change fixture leaves no artifact.

- [ ] **Step 7: Commit the publication unit**

```bash
git add plugins/workflow-observer/scripts/snapshot_store.py \
  plugins/workflow-observer/scripts/workflow_observer_cli.py \
  plugins/workflow-observer/tests/test_snapshot_publication.py \
  plugins/workflow-observer/tests/test_portable_cli.py
git commit -m "feat: publish stable workflow learning snapshots"
```

---

### Task 10: Learning, Telemetry, Improving, and Distribution Contracts

**Files:**

- Modify: `plugins/workflow-observer/skills/workflow-telemetry/SKILL.md`
- Modify: `plugins/workflow-observer/skills/workflow-learning/SKILL.md`
- Modify: `plugins/workflow-observer/skills/workflow-improving/SKILL.md`
- Modify: `plugins/workflow-observer/tests/test_learning_improving.py`
- Modify: `plugins/workflow-observer/tests/test_skill_contracts.py`
- Modify: `plugins/workflow-observer/tests/test_package_archive.py`
- Modify: `evidence/scripts/package_workflow_observatory.py`
- Modify: `plugins/workflow-observer/README.md`
- Modify: `README.md`
- Modify: `ROADMAP.md`
- Modify: `TODO.md`

**Interfaces:**

- Workflow telemetry may opt into v2 only when it has sanitized structured
  measurements and an applicable explicit workflow generation.
- Workflow learning calls only `snapshot` for bounded analysis and reads the
  resulting sanitized artifact through the CLI contract.
- Workflow improving requires a user-selected `(snapshot_id, candidate_id)`
  pair and still stops before proposal creation in this milestone.

- [ ] **Step 1: Write failing skill and packaging contract tests**

Add exact assertions:

```python
self.assertIn("snapshot-input", learning_skill())
self.assertIn("Learning Snapshot", learning_skill())
self.assertNotIn("Run `validate` first", learning_skill())
self.assertNotIn("run only `report`", learning_skill())
self.assertIn("snapshot_id", improving_skill())
self.assertIn("candidate_id", improving_skill())
self.assertIn("both", improving_skill())
self.assertIn("does not create an Evolution Proposal", improving_skill())
```

Extend archive tests to require every `plugins/workflow-observer/policies/*.json`,
the approved design, and this plan in the future source archive inventory.
First correct the extracted/source layout selector in
`test_package_archive.py`: when `_LAYOUT_ROOT/.agents/plugins/marketplace.json`
exists, use `_LAYOUT_ROOT` as `MARKETPLACE_ROOT` and `_LAYOUT_ROOT/evidence` as
`REPOSITORY_ROOT`. Fall back to the frozen
`evidence/marketplace/workflow-observatory` only when testing from an extracted
archive that has no live top-level marketplace manifest. This ensures source
tests package current plugin bytes rather than silently repackaging historical
evidence.

- [ ] **Step 2: Run contract tests and verify RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_improving.py \
  plugins/workflow-observer/tests/test_skill_contracts.py \
  plugins/workflow-observer/tests/test_package_archive.py -v
```

Expected: old learning/report wording and policy-directory archive rejection
make the new assertions fail.

- [ ] **Step 3: Update the skills without broadening authority**

Telemetry documents the optional private v2 supplement and forbids fabricated
token/cost measurements. Learning requires explicit bounded dates/timezone,
runs the canonical snapshot operation, reports the exact snapshot ID and
unranked candidates, and never parses records or human `report`. Improving
requires the user to select one exact pair:

```text
snapshot_id= followed by exactly 64 lowercase hexadecimal characters
candidate_id= followed by exactly 64 lowercase hexadecimal characters
```

It verifies the candidate exists in that snapshot and then stops with a clear
statement that proposal design/creation remains deferred. No skill may edit a
workflow, create a branch/PR, or initiate an experiment from v0.2 output.

- [ ] **Step 4: Allow immutable policies and approved docs in packaging**

Update `_is_allowed_marketplace_file` so only `.json` regular files directly
under `plugins/workflow-observer/policies/` are allowed. Also allow exactly
`plugins/workflow-observer/tests/fixtures/jcs_conformance_vectors.json`,
`docs/superpowers/specs/2026-08-02-workflow-evolution-foundation-v0.2-design.md`
and
`docs/superpowers/plans/2026-08-02-workflow-evolution-foundation-v0.2.md`
as marketplace documentation; do not pretend these top-level files are members
of the historical `evidence/` snapshot. Keep symlink, unexpected-file,
personal-path, completeness, and reproducibility checks unchanged.

- [ ] **Step 5: Update user/developer documentation and remaining roadmap**

Document `snapshot-input`, `snapshot`, local artifact location, v1/v2
compatibility, policy closure, fake-root test boundary, and the fact that
candidate evidence is observational. Keep Evolution Proposal schema,
experiments, post-hoc evaluation artifacts, health-event history, background
scheduling, and formal acceptance execution in the roadmap/TODO rather than
claiming them complete.

- [ ] **Step 6: Run skill, package, and plugin regressions**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_learning_improving.py \
  plugins/workflow-observer/tests/test_skill_contracts.py \
  plugins/workflow-observer/tests/test_package_archive.py -v
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py'
```

Expected: all tests pass with only explicitly documented optional skips.

- [ ] **Step 7: Commit the contract and distribution unit**

```bash
git add plugins/workflow-observer/skills \
  plugins/workflow-observer/tests/test_learning_improving.py \
  plugins/workflow-observer/tests/test_skill_contracts.py \
  plugins/workflow-observer/tests/test_package_archive.py \
  evidence/scripts/package_workflow_observatory.py \
  plugins/workflow-observer/README.md README.md ROADMAP.md TODO.md
git commit -m "docs: route learning through reproducible snapshots"
```

---

### Task 11: Fifteen-Case Acceptance Matrix and Bounded Historical Dry Run

**Files:**

- Create: `plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py`
- Modify: `plugins/workflow-observer/tests/test_core_parity.py`
- Modify: `plugins/workflow-observer/scripts/core_source.json`

**Interfaces:**

- Consumes: all Tasks 1–10.
- Produces: one named automated test for each of the fifteen approved
  acceptance requirements.
- Produces: a fake-root historical baseline artifact for 2026-07-15 through
  2026-08-02 Asia/Taipei; it is test evidence, not analysis of the user's live
  records.

- [ ] **Step 1: Add the explicit fifteen-test acceptance class**

Create `WorkflowEvolutionAcceptanceTests` with the exact method names and
assertion contracts below. Reuse only constructors from the reviewed
`workflow_evolution_fixtures.py`; every test invokes public CLI or public
module interfaces rather than private filesystem edits after acquisition
begins.

| Test method | Required fixture and assertion |
|---|---|
| `test_01_identical_inputs_produce_identical_core_and_snapshot_id` | Publish twice from the same fake bundle and fixed `generated_at`; securely read back both results, recompute both identities, and assert equal JCS core bytes and IDs. |
| `test_02_machine_timezone_does_not_change_manifest` | Acquire the same Taipei date query under `TZ=UTC` and `TZ=America/Los_Angeles`; assert equal manifest bytes. |
| `test_03_store_change_aborts_without_snapshot` | Finalize a selected draft between acquisition A and B; assert normalized state error and an empty snapshots directory. |
| `test_04_adapter_fixtures_project_identically` | Build equivalent portable and current-layout LLMWiki roots; assert equal semantic bundle bytes. |
| `test_05_v1_absence_is_not_zero_or_v2_missing` | Mix four v1, two v2-null, and two v2-observed values; assert the exact 4/2/2/0 missingness partition, then give `input_tokens` and `output_tokens` equal zero-observed missingness and assert distinct concrete source identities and candidate IDs. |
| `test_06_lifecycle_records_do_not_enter_outcome_denominator` | Mix four outcomes, drafts, superseded, and invalidated records; assert outcome `n=4` and separate overlapping lifecycle counts. |
| `test_07_decision_recurrence_uses_distinct_episodes` | Put ten identical events in one of five Episodes; assert event count 10, Episode support 1, and descriptive strength. With no comparability exclusions, assert four outcomes with three supporting Episodes remain descriptive and emit no Decision candidate, while five outcomes with three supporting Episodes become recurring and emit exactly one Decision candidate. Repeat a numerically supported pattern in generation-unavailable and heterogeneous-runtime cohorts; assert descriptive patterns and no Decision candidates. |
| `test_08_reviewed_mapping_derived_view_counts_once` | Present one physical Episode plus its reviewed workflow-generation mapping; assert exactly one canonical projected Episode for that `run_id`. |
| `test_09_gate_failure_produces_no_snapshot_or_proposal` | Break a task reference; assert no snapshot directory member and no proposal artifact anywhere in the fake home. |
| `test_10_authoritative_tamper_is_not_acceptance` | Change learning artifact `authoritative` to true; assert schema rejection even after recomputing a generic file digest. |
| `test_11_snapshot_rejects_narrative_and_annotation_fields` | Add each of `narrative`, `annotation`, and `summary_markdown` to an otherwise valid artifact; assert validation failure and no annotation artifact or directory member. |
| `test_12_no_approval_causes_no_external_mutation` | Snapshot a fake Git subject and workflow file before/after learning; assert byte equality and mock `subprocess.run` rejects `git branch`, `gh pr`, or workflow-edit calls. |
| `test_13_lifecycle_as_of_is_frozen_in_identity` | Rebuild at later wall clock with same bound `as_of`; assert same ID, then change explicit `as_of` and assert a different ID. |
| `test_14_shared_jcs_vector_and_lone_surrogate` | Use emoji, non-ASCII keys, control characters, quote, and backslash vector; assert fixed bytes/hash and lone-surrogate failure. |
| `test_15_policy_hash_and_effective_boundary_are_closed` | Change one policy byte and assert identity/core change; assert a `started_at` declaration applies only on/after its instant, a `producer_generation` declaration matches only the exact explicit identity, and all invalid union shapes fail. |

Test 12 never points at the source checkout as a mutation target.
Test 08 does not create a second physical source record; the Task 6 focused
conflict test separately proves that duplicate physical sources for one
`run_id` fail the gate. v0.2 publishes no annotation artifact. A later approved
annotation schema must define its own artifact identity, cite `snapshot_id`,
and may not reuse `snapshot_id` as the annotation's artifact identity.

- [ ] **Step 2: Run only the acceptance class and verify any missing coverage is RED**

Run:

```bash
caffeinate -i -m python3 -m unittest \
  plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py -v
```

Expected before final fixture work: one or more named acceptance tests fail for
missing integration coverage, not for import or test-harness errors.

- [ ] **Step 3: Complete only the integration glue required by the failures**

Add shared fake-root fixture builders inside the test module or a private test
helper in the same file. Do not add new product behavior at this step. If a
failure exposes missing product behavior, return to the owning Task 1–10,
write its focused regression there, fix it, commit that task correction, then
rerun this matrix.

- [ ] **Step 4: Refresh lifecycle core identity after final core bytes settle**

The v2 lifecycle extension makes Workflow Observatory the canonical owner of
its bundled core. Change `core_source.json` to schema version `2`, set `source`
to the exact relative identity
`workflow-observatory/plugins/workflow-observer/scripts/wiki_observations.py`,
and set `sha256` to the exact 64-character lowercase digest computed from that
file's final bytes. Update
`test_core_parity.py` to assert schema/source/hash and remove the obsolete
`LLMWIKI_SOURCE_ROOT` byte-equality skip. Adapter conformance fixtures now prove
the supported v1 contract against LLMWiki semantics. Do not modify the
historical evidence copy or top-level release inventory.
Load `core_source.json` through Task 1 `strict_json_loads` in the parity test
and add a duplicate-key fixture that fails before hash comparison. Any later
production reader of this or another persisted code/source manifest must use
the same strict ingress rather than ordinary `json.loads()`.

- [ ] **Step 5: Run a fake-root bounded baseline twice**

Create a temporary fixture containing v1 and v2 Episodes across the approved
2026-07-15 through 2026-08-02 interval, plus excluded before/after records.
Invoke the public `snapshot` CLI twice with `TZ=UTC` and
`TZ=America/Los_Angeles`, using `--timezone Asia/Taipei`. Assert the same
`snapshot_id`, same core bytes, one immutable artifact, correct unsupported-v1
missingness, no proposal, and no mutation outside the fake home.

- [ ] **Step 6: Run targeted tests selected by CodeGraph, then the complete non-model gate**

Run:

```bash
git diff --name-only origin/main...HEAD | \
  npx --yes @colbymchenry/codegraph@1.5.0 affected --stdin
caffeinate -i -m uv python install 3.11 3.12 3.13 3.14
for py in 3.11 3.12 3.13 3.14; do
  caffeinate -i -m uv run --no-project --python "$py" \
    python -m unittest \
      plugins/workflow-observer/tests/test_canonical_json.py \
      plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py -v
done
caffeinate -i -m python3 -m unittest discover \
  -s plugins/workflow-observer/tests -p 'test_*.py'
: "${WORKFLOW_OBSERVATORY_EVAL_ARCHIVE:?set an absolute trusted archive path}"
: "${WORKFLOW_OBSERVATORY_EVAL_ARCHIVE_SHA256:?set its independently trusted SHA-256}"
(
  cd evidence
  caffeinate -i -m python3 -m unittest discover -s tests -p 'test_*.py'
)
git diff --check origin/main...HEAD
```

The loop is the support claim: both JCS conformance and the complete 15-case
v0.2 acceptance class run on every listed CPython. Full plugin and evidence
regression discovery runs once on the primary `python3` runtime. Evidence tests
that execute archived evaluator code require the two pre-set environment
variables above; obtain the digest from an independent trusted release
descriptor or channel, never by trusting the archive's own inventory or by
computing both sides inside this gate.

Expected:

- the shared JCS vector and all fifteen acceptance cases pass under CPython
  3.11–3.14;
- all plugin tests pass with only explicitly documented optional skips;
- all evidence/repository non-model tests pass;
- no production Wiki, portable store, workflow, branch, PR, or release is
  touched;
- `git diff --check` reports no error.

- [ ] **Step 7: Perform plan-to-spec self-review before the final commit**

Create a temporary checklist mapping every approved design acceptance item and
each approved implementation-plan clarification to its named test and owning
task. Verify all fifteen rows have fresh evidence. Scan for placeholders and
unintended scope:

```bash
rg -n '\b(TBD|FIXME|implement later|similar to Task)\b|# TODO\b' \
  plugins/workflow-observer/scripts \
  plugins/workflow-observer/policies \
  plugins/workflow-observer/tests \
  plugins/workflow-observer/skills
git diff --name-only origin/main...HEAD | \
  rg '^(evidence/marketplace/|SHA256SUMS.json$)' && exit 1 || true
```

Expected: no placeholders in the new implementation and no historical package
evidence/release inventory changes.

- [ ] **Step 8: Commit and push the verified implementation boundary**

```bash
git add plugins/workflow-observer/tests/test_workflow_evolution_acceptance.py \
  plugins/workflow-observer/tests/test_core_parity.py \
  plugins/workflow-observer/scripts/core_source.json
git commit -m "test: verify workflow evolution foundation acceptance"
git push origin feature/workflow-evolution-foundation-v0.2
```

The push is a WIP/review checkpoint only. Do not merge, create a PR, publish a
release, install over the user's active plugin, or run against live observation
data without a separate explicit instruction.

## Execution Review Gates

After Tasks 1–2, review identity and policy closure before Episode work. After
Tasks 3–5, review v1 compatibility, v2 privacy, and adapter semantics before
analysis work. After Tasks 6–9, run an adversarial review of stable reads,
denominators, identity, and mutation boundaries before changing skills. Task 11
is the final non-model acceptance gate; a later live-data baseline requires
separate authorization and is never substituted for these fake-root tests.
