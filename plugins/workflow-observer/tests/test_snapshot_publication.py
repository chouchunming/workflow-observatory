from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
for module_root in (PLUGIN_ROOT / "scripts", PLUGIN_ROOT / "tests"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import canonicalize, hash_canonical, strict_json_loads
from artifact_schema import load_artifact_policy_set
from learning_snapshot import candidate_id
from policy_artifacts import PolicySet, load_policy_set
from snapshot_input import (
    SNAPSHOT_ANALYZER_FILES,
    SnapshotQuery,
    acquire_snapshot_input,
)
from snapshot_store import (
    SnapshotPublicationError,
    create_learning_snapshot,
    read_learning_artifact,
    validate_learning_artifact_bytes,
)
from store_config import PORTABLE_SEMANTICS
import wiki_observations
from wiki_observations import ObservationPaths
from workflow_evolution_fixtures import FakeObservationStore


FIXED_NOW = "2026-08-03T00:01:00Z"
_SNAPSHOT_CORE_DOMAINS = {
    1: b"workflow-observatory:learning-snapshot-core:v1\0",
    2: b"workflow-observatory:learning-snapshot-core:v2\0",
}
_COMPLETION = """## Execution evidence

- Verification: publication regression passed
- Artifacts: immutable learning snapshot

## Outcome and observation

- Outcome: Fixture draft finalized
- Observation: Manifest B differs from manifest A

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 0
defects_found: 0
rework_count: 0
rework_reason: none
```
"""


def artifact_bytes_with_recomputed_file_digest(artifact):
    rebuilt = deepcopy(artifact)
    rebuilt.pop("artifact_sha256", None)
    digest = hashlib.sha256(canonicalize(rebuilt)).hexdigest()
    rebuilt["artifact_sha256"] = digest
    return canonicalize(rebuilt)


def artifact_bytes_with_recomputed_identities(artifact):
    rebuilt = deepcopy(artifact)
    schema_version = rebuilt.get("schema_version", 1)
    rebuilt["snapshot_id"] = hash_canonical(
        _SNAPSHOT_CORE_DOMAINS[schema_version], rebuilt["core"]
    )
    return artifact_bytes_with_recomputed_file_digest(rebuilt)


def decision_candidate_for_pattern(core, pattern):
    cohort = next(
        row for row in core["cohorts"]
        if all(row[name] == value for name, value in pattern["cohort"].items())
    )
    candidate = {
        "candidate_type": (
            "decision-single-event"
            if pattern["pattern_kind"] == "single-event"
            else "decision-adjacent-pair"
        ),
        "class": "decision-pattern",
        "cohort": deepcopy(pattern["cohort"]),
        "source": {
            "kind": "decision",
            "identity": pattern["pattern_kind"],
            "semantics_id": "decision-pattern-support@1",
        },
        "policy_identities": {
            name: deepcopy(core["analysis_policy_set"][name])
            for name in (
                "candidate_emission_policy",
                "canonical_projection_contract",
                "decision_support_policy",
            )
        },
        "denominators": {
            "eligible_episode_n": pattern["eligible_episode_n"],
            "outcome_episode_n": cohort["outcome_episode_n"],
            "supporting_episode_n": pattern["episode_count_with_event"],
        },
        "evidence": {
            "counts": {
                "event_count": pattern["event_count"],
                "episode_count_with_event": pattern[
                    "episode_count_with_event"
                ],
            },
            "missingness": None,
            "observed_values": None,
            "category_counts": None,
            "quantiles": None,
            "pattern": deepcopy(pattern["pattern"]),
        },
        "evidence_strength": "recurring",
    }
    candidate["candidate_id"] = candidate_id(candidate)
    return candidate


def recompute_candidate_identity(candidate):
    evidence = deepcopy(candidate)
    evidence.pop("candidate_id", None)
    candidate["candidate_id"] = candidate_id(evidence)


class SnapshotPublicationTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeObservationStore("portable")
        self.addCleanup(self.store.close)
        self.home = self.store.store_root.parent
        self.policies = load_policy_set(
            PLUGIN_ROOT / "policies",
            analyzer_files=SNAPSHOT_ANALYZER_FILES,
            canonicalizer_files=("scripts/canonical_json.py",),
        )
        self.artifact_policies = load_artifact_policy_set(
            PLUGIN_ROOT / "policies"
        )
        interval = {
            "basis": "started_at",
            "since_inclusive": "2026-08-01T16:00:00Z",
            "until_exclusive": "2026-08-03T16:00:00Z",
            "requested_timezone": "Asia/Taipei",
            "requested_dates": {
                "since": "2026-08-02",
                "until_inclusive": "2026-08-03",
            },
        }
        self.query = SnapshotQuery(
            interval=interval,
            lifecycle_as_of="2026-08-03T16:00:00Z",
            project=None,
            workspace=None,
            workspace_id=None,
            task_type=None,
        )

    def acquire(self):
        return acquire_snapshot_input(
            ObservationPaths.from_root(self.store.store_root),
            PORTABLE_SEMANTICS,
            self.query,
            self.policies,
            self.artifact_policies,
        )

    def historical_v1_artifact_bytes(self):
        fixture = strict_json_loads(
            (PLUGIN_ROOT / "tests/fixtures/artifact_migration_vectors.json")
            .read_bytes()
        )
        vector = next(
            row for row in fixture["vectors"]
            if row["name"] == "learning-snapshot-v1"
        )
        encoded = vector["source_bytes_base64"]
        insertion = vector.get("source_bytes_base64_tail_insertion", "")
        if insertion:
            encoded = encoded[:-4] + insertion + encoded[-4:]
        artifact_bytes = base64.b64decode(encoded, validate=True)
        self.assertEqual(
            vector["source_sha256"], hashlib.sha256(artifact_bytes).hexdigest()
        )
        artifact = strict_json_loads(artifact_bytes)
        historical_policies = PolicySet(
            self.policies.documents,
            artifact["core"]["analysis_policy_set"],
        )
        return artifact["snapshot_id"], artifact_bytes, historical_policies

    def publish(self, generated_at=FIXED_NOW):
        return create_learning_snapshot(
            acquire=self.acquire,
            query=self.query,
            policy_set=self.policies,
            home=self.home,
            generated_at=generated_at,
        )

    def artifact_with_recurring_decision_patterns(
        self,
        *,
        supporting_n=3,
        eligible_n=5,
        outcome_n=5,
        comparative_eligible=True,
        evidence_strength="recurring",
        include_candidate=True,
        include_pair=False,
    ):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        core = artifact["core"]
        single = core["decision_patterns"][0]
        cohort = next(
            row for row in core["cohorts"]
            if all(
                row[name] == value
                for name, value in single["cohort"].items()
            )
        )
        cohort["comparative_inference_eligible"] = comparative_eligible
        cohort["comparative_inference_exclusions"] = (
            [] if comparative_eligible else ["generation-unavailable"]
        )
        cohort["evidence_strength"] = (
            "recurring"
            if comparative_eligible and outcome_n >= 5
            else "descriptive"
        )
        cohort["outcome_episode_n"] = outcome_n
        cohort["outcome_counts"] = {
            "failed": 0,
            "partial": 0,
            "rolled-back": 0,
            "success": outcome_n,
        }
        single.update({
            "event_count": supporting_n,
            "episode_count_with_event": supporting_n,
            "eligible_episode_n": eligible_n,
            "support_fraction": {
                "numerator": supporting_n,
                "denominator": eligible_n,
            },
            "evidence_strength": evidence_strength,
        })
        patterns = [single]
        if include_pair:
            pair = deepcopy(single)
            pair["pattern_kind"] = "contiguous-adjacent-pair"
            pair["pattern"] = pair["pattern"] * 2
            patterns.append(pair)
            core["decision_patterns"].append(pair)
            core["decision_patterns"].sort(key=lambda row: canonicalize([
                row["cohort"], row["pattern_kind"], row["pattern"]
            ]))
        if include_candidate:
            core["candidates"].extend(
                decision_candidate_for_pattern(core, pattern)
                for pattern in patterns
            )
        core["candidates"].sort(key=lambda row: row["candidate_id"])
        return artifact, patterns

    def policy_set_with_different_decision_identity(self):
        documents = self.policies.documents
        decision = documents["decision_support_policy"]
        decision["decision_min_support_ratio"] = "0.600"
        identities = self.policies.core_identity()
        identities["decision_support_policy"] = {
            "version": decision["version"],
            "sha256": "sha256:" + hashlib.sha256(
                canonicalize(decision)
            ).hexdigest(),
        }
        return PolicySet(documents, identities)

    def acquire_then_mutate_selection(self):
        run_id = "obs-20260802-000000-abcdef"
        final = self.store.v1_path.read_bytes()
        draft = final.replace(b'status: "success"', b'status: "draft"')
        draft = draft.split(b"\n## Execution evidence", 1)[0].rstrip() + b"\n"
        self.store._write_private(self.store.v1_path, draft)
        calls = 0

        def acquire():
            nonlocal calls
            calls += 1
            if calls == 2:
                wiki_observations.finish_observation(
                    ObservationPaths.from_root(self.store.store_root),
                    run_id,
                    "success",
                    wiki_observations.parse_completion_payload(_COMPLETION),
                    semantics=PORTABLE_SEMANTICS,
                )
            return self.acquire()

        return acquire

    def publish_from_two_processes_released_by_one_barrier(self):
        barrier = self.store.base / "publish.release"
        worker_source = r'''
import os
from pathlib import Path
import sys
import time

plugin_root = Path(os.environ["PLUGIN_ROOT"])
for module_root in (plugin_root / "scripts", plugin_root / "tests"):
    sys.path.insert(0, str(module_root))

from artifact_schema import load_artifact_policy_set
from canonical_json import canonicalize
from policy_artifacts import load_policy_set
from snapshot_input import SNAPSHOT_ANALYZER_FILES, SnapshotQuery, acquire_snapshot_input
from snapshot_store import create_learning_snapshot
from store_config import PORTABLE_SEMANTICS
from wiki_observations import ObservationPaths

policy_set = load_policy_set(
    plugin_root / "policies",
    analyzer_files=SNAPSHOT_ANALYZER_FILES,
    canonicalizer_files=("scripts/canonical_json.py",),
)
artifact_policy_set = load_artifact_policy_set(plugin_root / "policies")
interval = {
    "basis": "started_at",
    "since_inclusive": "2026-08-01T16:00:00Z",
    "until_exclusive": "2026-08-03T16:00:00Z",
    "requested_timezone": "Asia/Taipei",
    "requested_dates": {"since": "2026-08-02", "until_inclusive": "2026-08-03"},
}
query = SnapshotQuery(
    interval=interval,
    lifecycle_as_of="2026-08-03T16:00:00Z",
    project=None,
    workspace=None,
    workspace_id=None,
    task_type=None,
)
store_root = Path(os.environ["STORE_ROOT"])
ready = Path(os.environ["READY"])
release = Path(os.environ["RELEASE"])
ready.write_text("ready", encoding="ascii")
deadline = time.monotonic() + 10
while not release.exists():
    if time.monotonic() >= deadline:
        raise RuntimeError("publication barrier timed out")
    time.sleep(0.01)

def acquire():
    return acquire_snapshot_input(
        ObservationPaths.from_root(store_root),
        PORTABLE_SEMANTICS,
        query,
        policy_set,
        artifact_policy_set,
    )

published = create_learning_snapshot(
    acquire=acquire,
    query=query,
    policy_set=policy_set,
    home=Path(os.environ["HOME_ROOT"]),
    generated_at="2026-08-03T00:01:00Z",
)
Path(os.environ["RESULT"]).write_bytes(canonicalize({
    "created": published.created,
    "snapshot_id": published.snapshot_id,
    "path": str(published.path),
}))
'''
        processes = []
        result_paths = []
        ready_paths = []
        for index in range(2):
            ready = self.store.base / f"publisher-{index}.ready"
            result = self.store.base / f"publisher-{index}.json"
            environment = {
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                "PLUGIN_ROOT": str(PLUGIN_ROOT),
                "STORE_ROOT": str(self.store.store_root),
                "HOME_ROOT": str(self.home),
                "READY": str(ready),
                "RELEASE": str(barrier),
                "RESULT": str(result),
            }
            processes.append(subprocess.Popen(
                [sys.executable, "-c", worker_source],
                cwd=PLUGIN_ROOT,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ))
            ready_paths.append(ready)
            result_paths.append(result)
        try:
            deadline = time.monotonic() + 10
            while not all(path.exists() for path in ready_paths):
                if time.monotonic() >= deadline:
                    self.fail("publishers did not reach the barrier")
                time.sleep(0.01)
            barrier.touch()
            results = []
            for process, result_path in zip(
                processes, result_paths, strict=True
            ):
                stdout, stderr = process.communicate(timeout=20)
                self.assertEqual(0, process.returncode, stderr or stdout)
                row = strict_json_loads(result_path.read_bytes())
                results.append(SimpleNamespace(
                    created=row["created"],
                    snapshot_id=row["snapshot_id"],
                    path=Path(row["path"]),
                ))
            return results
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=5)

    def test_store_change_between_manifests_aborts_without_artifact(self):
        acquire = self.acquire_then_mutate_selection()
        with self.assertRaisesRegex(
            SnapshotPublicationError, "changed during analysis"
        ):
            create_learning_snapshot(
                acquire=acquire,
                query=self.query,
                policy_set=self.policies,
                home=self.home,
                generated_at=FIXED_NOW,
            )
        self.assertEqual(
            [], list((self.home / "learning/snapshots").glob("*.json"))
        )

    def test_identical_core_reuses_existing_immutable_artifact(self):
        first = self.publish(generated_at="2026-08-03T00:01:00Z")
        second = self.publish(generated_at="2026-08-04T00:01:00Z")
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.snapshot_id, second.snapshot_id)
        self.assertEqual(first.path.read_bytes(), second.path.read_bytes())

    def test_stale_publisher_temps_do_not_block_valid_reuse(self):
        first = self.publish()
        snapshot_dir = first.path.parent
        stale_unrelated = snapshot_dir / (".snapshot-" + "a" * 32 + ".tmp")
        stale_unrelated.write_bytes(b"abandoned")
        stale_unrelated.chmod(0o600)
        stale_hard_link = snapshot_dir / (".snapshot-" + "b" * 32 + ".tmp")
        os.link(first.path, stale_hard_link)

        reused = self.publish(generated_at="2026-08-04T00:01:00Z")

        self.assertFalse(reused.created)
        self.assertEqual(first.snapshot_id, reused.snapshot_id)
        self.assertTrue(stale_unrelated.exists())
        self.assertTrue(stale_hard_link.exists())

    def test_directory_creation_is_descriptor_relative_and_parent_durable(self):
        original_mkdir = os.mkdir
        original_fsync = os.fsync
        with (
            patch("snapshot_store.os.mkdir", wraps=original_mkdir) as mkdir,
            patch("snapshot_store.os.fsync", wraps=original_fsync) as fsync,
        ):
            self.publish()

        self.assertEqual(2, mkdir.call_count)
        self.assertEqual(
            ["learning", "snapshots"],
            [call.args[0] for call in mkdir.call_args_list],
        )
        self.assertTrue(all(
            call.kwargs.get("dir_fd") is not None
            for call in mkdir.call_args_list
        ))
        self.assertGreaterEqual(fsync.call_count, 5)

    def test_published_envelope_has_exact_path_free_fields(self):
        published = self.publish()
        artifact = published.artifact
        self.assertEqual({
            "artifact_type",
            "schema_version",
            "authoritative",
            "generated_at",
            "store_identity",
            "adapter",
            "snapshot_id",
            "core",
            "artifact_sha256",
        }, set(artifact))
        self.assertEqual("learning-snapshot", artifact["artifact_type"])
        self.assertEqual(2, artifact["schema_version"])
        self.assertEqual(2, artifact["core"]["schema_version"])
        self.assertEqual(
            hash_canonical(_SNAPSHOT_CORE_DOMAINS[2], artifact["core"]),
            artifact["snapshot_id"],
        )
        self.assertNotEqual(
            hash_canonical(_SNAPSHOT_CORE_DOMAINS[1], artifact["core"]),
            artifact["snapshot_id"],
        )
        self.assertIs(False, artifact["authoritative"])
        self.assertEqual(FIXED_NOW, artifact["generated_at"])
        self.assertNotIn(str(self.home), canonicalize(artifact).decode("utf-8"))
        self.assertEqual(0o600, published.path.stat().st_mode & 0o777)
        self.assertFalse(published.path.read_bytes().endswith(b"\n"))

    def test_oversized_artifact_fails_before_publication(self):
        with patch("snapshot_store._MAX_ARTIFACT_BYTES", 1):
            with self.assertRaisesRegex(
                SnapshotPublicationError, "artifact is too large"
            ):
                self.publish()
        self.assertEqual(
            [], list((self.home / "learning/snapshots").glob("*.json"))
        )

    def test_authoritative_tamper_cannot_become_acceptance(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["authoritative"] = True
        with self.assertRaises(SnapshotPublicationError):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_file_digest(artifact),
                self.policies,
            )

    def test_adapter_tamper_cannot_inject_path_metadata(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["adapter"]["name"] = str(self.home)
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "adapter.*invalid"
        ):
            validate_learning_artifact_bytes(raw, self.policies)

    def test_adapter_digest_must_bind_analyzer_artifact(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["adapter"]["implementation_sha256"] = "0" * 64
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "adapter.*analyzer"
        ):
            validate_learning_artifact_bytes(raw, self.policies)

    def test_recursive_core_validation_rejects_path_and_unknown_fields(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        malformed = []

        query = deepcopy(artifact)
        query["core"]["query"] = {
            "narrative": "/Users/alice/private/story"
        }
        malformed.append(query)

        cohort = deepcopy(artifact)
        cohort["core"]["cohorts"][0]["narrative"] = "unsupported"
        malformed.append(cohort)

        candidate = deepcopy(artifact)
        candidate["core"]["candidates"][0]["annotation"] = "unsupported"
        malformed.append(candidate)

        for item in malformed:
            with self.subTest(core=item["core"]):
                with self.assertRaisesRegex(
                    SnapshotPublicationError,
                    "core|query|cohort|candidate|exact fields",
                ):
                    validate_learning_artifact_bytes(
                        artifact_bytes_with_recomputed_identities(item),
                        self.policies,
                    )

    def test_recursive_core_validation_rejects_embedded_sensitive_path(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        candidate = artifact["core"]["candidates"][0]
        candidate["candidate_type"] = "leak /Users/alice/private/story"
        evidence = dict(candidate)
        del evidence["candidate_id"]
        candidate["candidate_id"] = candidate_id(evidence)
        artifact["core"]["candidates"].sort(
            key=lambda row: row["candidate_id"]
        )
        with self.assertRaisesRegex(
            SnapshotPublicationError, "sensitive path or credential"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_decision_pattern_kind_must_bind_pattern_length(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["decision_patterns"][0][
            "pattern_kind"
        ] = "contiguous-adjacent-pair"

        with self.assertRaisesRegex(
            SnapshotPublicationError, "core structure is invalid"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_decision_pattern_support_must_bind_counts(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["decision_patterns"][0]["support_fraction"][
            "numerator"
        ] = 0

        with self.assertRaisesRegex(
            SnapshotPublicationError, "core structure is invalid"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_one_of_one_self_declared_recurring_is_rejected(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns(
            supporting_n=1,
            eligible_n=1,
            outcome_n=5,
        )

        with self.assertRaisesRegex(
            SnapshotPublicationError, "decision pattern evidence strength"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_three_of_four_with_four_outcomes_is_not_recurring(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns(
            supporting_n=3,
            eligible_n=4,
            outcome_n=4,
        )

        with self.assertRaisesRegex(
            SnapshotPublicationError, "decision pattern evidence strength"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_three_of_five_without_comparative_eligibility_is_not_recurring(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns(
            supporting_n=3,
            eligible_n=5,
            outcome_n=5,
            comparative_eligible=False,
        )

        with self.assertRaisesRegex(
            SnapshotPublicationError, "decision pattern evidence strength"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_three_of_five_eligible_outcomes_accepts_one_recurring_candidate(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns(
            supporting_n=3,
            eligible_n=5,
            outcome_n=5,
            comparative_eligible=True,
        )

        validated = validate_learning_artifact_bytes(
            artifact_bytes_with_recomputed_identities(artifact),
            self.policies,
        )

        decision_candidates = [
            candidate for candidate in validated["core"]["candidates"]
            if candidate["source"]["kind"] == "decision"
        ]
        self.assertEqual(1, len(decision_candidates))
        self.assertEqual("recurring", decision_candidates[0]["evidence_strength"])

    def test_recurring_qualified_evidence_relabelled_descriptive_is_rejected(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns(
            supporting_n=3,
            eligible_n=5,
            outcome_n=5,
            comparative_eligible=True,
            evidence_strength="descriptive",
            include_candidate=False,
        )

        with self.assertRaisesRegex(
            SnapshotPublicationError, "decision pattern evidence strength"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_supplied_policy_set_identity_mismatch_is_rejected(self):
        artifact, _patterns = self.artifact_with_recurring_decision_patterns()

        with self.assertRaisesRegex(
            SnapshotPublicationError, "analysis policy set does not match"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                policy_set=self.policy_set_with_different_decision_identity(),
            )

    def test_orphan_decision_candidate_is_rejected(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        core = artifact["core"]
        candidate = decision_candidate_for_pattern(
            core, core["decision_patterns"][0]
        )
        core["candidates"].append(candidate)
        core["candidates"].sort(key=lambda row: row["candidate_id"])

        with self.assertRaisesRegex(
            SnapshotPublicationError, "core structure is invalid"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_decision_candidate_must_bind_pattern_evidence(self):
        mutations = {
            "counts": lambda candidate: candidate["evidence"]["counts"].update(
                event_count=candidate["evidence"]["counts"]["event_count"] + 1
            ),
            "denominators": lambda candidate: candidate["denominators"].update(
                eligible_episode_n=(
                    candidate["denominators"]["eligible_episode_n"] + 1
                )
            ),
            "cohort": lambda candidate: candidate["cohort"].update(
                project="different-project"
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(binding=label):
                artifact, _patterns = (
                    self.artifact_with_recurring_decision_patterns()
                )
                decision = next(
                    row for row in artifact["core"]["candidates"]
                    if row["class"] == "decision-pattern"
                )
                mutate(decision)
                recompute_candidate_identity(decision)
                artifact["core"]["candidates"].sort(
                    key=lambda row: row["candidate_id"]
                )
                with self.assertRaisesRegex(
                    SnapshotPublicationError, "core structure is invalid"
                ):
                    validate_learning_artifact_bytes(
                        artifact_bytes_with_recomputed_identities(artifact),
                        self.policies,
                    )

    def test_valid_single_event_and_adjacent_pair_patterns_are_accepted(self):
        artifact, patterns = self.artifact_with_recurring_decision_patterns(
            include_pair=True
        )

        validated = validate_learning_artifact_bytes(
            artifact_bytes_with_recomputed_identities(artifact),
            self.policies,
        )

        self.assertEqual(
            {"single-event", "contiguous-adjacent-pair"},
            {pattern["pattern_kind"] for pattern in patterns},
        )
        self.assertEqual(artifact["core"], validated["core"])

    def test_cohorts_must_have_deterministic_array_order(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["cohorts"].reverse()

        with self.assertRaisesRegex(
            SnapshotPublicationError, "core structure is invalid"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_valid_producer_cohort_order_is_accepted(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        workflow_cohort = artifact["core"]["cohorts"][-1]
        later_workflow_cohort = deepcopy(workflow_cohort)
        later_workflow_cohort.update({
            "workspace": "z-workspace",
            "task_type": "compile",
            "workflow_variant": "compile-with-review",
        })
        artifact["core"]["cohorts"].append(later_workflow_cohort)

        validated = validate_learning_artifact_bytes(
            artifact_bytes_with_recomputed_identities(artifact),
            self.policies,
        )

        self.assertEqual(artifact["core"], validated["core"])

    def test_duplicate_key_rejected_in_snapshot_artifact(self):
        artifact = self.publish().path.read_bytes()
        ambiguous = artifact.replace(
            b'{"adapter":',
            b'{"adapter":{},"adapter":',
            1,
        )
        with self.assertRaisesRegex(
            SnapshotPublicationError, "duplicate JSON key"
        ):
            validate_learning_artifact_bytes(ambiguous, self.policies)

    def test_historical_v1_readback_is_byte_identical_and_uses_v1_domain(self):
        (
            snapshot_id,
            historical_bytes,
            historical_policies,
        ) = self.historical_v1_artifact_bytes()
        self.assertEqual(
            "9376242a607a79bdd7495427562c28ff6c4b63b6ee73dcdc29ee7dcc63109712",
            snapshot_id,
        )
        self.assertEqual(
            "d7063bc9ed597388e958b3451edbbd72104124082cf143cb67deec2311b3e5ec",
            hashlib.sha256(historical_bytes).hexdigest(),
        )
        snapshot_dir = self.home / "learning/snapshots"
        snapshot_dir.mkdir(parents=True, mode=0o700)
        snapshot_dir.chmod(0o700)
        path = snapshot_dir / f"{snapshot_id}.json"
        path.write_bytes(historical_bytes)
        path.chmod(0o600)

        readback = read_learning_artifact(
            snapshot_dir, snapshot_id, historical_policies
        )

        self.assertEqual(historical_bytes, canonicalize(readback))
        self.assertEqual(historical_bytes, path.read_bytes())
        self.assertNotIn("schema_version", {
            key: value for key, value in readback.items() if key != "core"
        })
        self.assertEqual(1, readback["core"]["schema_version"])

    def test_version_classification_rejects_unknown_mismatch_and_relabeling(self):
        v2 = strict_json_loads(self.publish().path.read_bytes())
        _v1_id, v1_bytes, _historical_policies = (
            self.historical_v1_artifact_bytes()
        )
        v1 = strict_json_loads(v1_bytes)
        cases = {}

        unknown = deepcopy(v2)
        unknown["schema_version"] = 3
        cases["unknown"] = artifact_bytes_with_recomputed_file_digest(unknown)

        wrong_type = deepcopy(v2)
        wrong_type["artifact_type"] = "health-event"
        cases["type mismatch"] = artifact_bytes_with_recomputed_file_digest(
            wrong_type
        )

        v1_labeled_v2 = deepcopy(v1)
        v1_labeled_v2["schema_version"] = 2
        cases["v1 labeled v2"] = artifact_bytes_with_recomputed_file_digest(
            v1_labeled_v2
        )

        v2_labeled_v1 = deepcopy(v2)
        del v2_labeled_v1["schema_version"]
        cases["v2 labeled v1"] = artifact_bytes_with_recomputed_file_digest(
            v2_labeled_v1
        )

        for label, raw in cases.items():
            with self.subTest(label=label), self.assertRaises(
                SnapshotPublicationError
            ):
                validate_learning_artifact_bytes(raw, self.policies)

    def test_v2_envelope_core_schema_disagreement_is_rejected(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["schema_version"] = 1
        with self.assertRaisesRegex(SnapshotPublicationError, "schema"):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_file_digest(artifact),
                self.policies,
            )

    def test_v2_readback_recomputes_sampling_equations(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["sampling_summary"]["full_retained_episode_n"] += 1
        with self.assertRaisesRegex(
            SnapshotPublicationError, "retained population"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_v2_readback_recomputes_metric_sampling_partition(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        missingness = artifact["core"]["cohorts"][0]["metrics"][0][
            "missingness"
        ]
        missingness["sampled_by_policy_n"] = 1
        with self.assertRaisesRegex(SnapshotPublicationError, "sampling|partition"):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact),
                self.policies,
            )

    def test_artifact_readback_recomputes_snapshot_identity(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["snapshot_id"] = "0" * 64
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "snapshot identity.*inconsistent|mismatch"
        ):
            validate_learning_artifact_bytes(raw, self.policies)

    def test_artifact_readback_recomputes_file_digest(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["artifact_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            SnapshotPublicationError, "artifact digest.*inconsistent|mismatch"
        ):
            validate_learning_artifact_bytes(
                canonicalize(artifact), self.policies
            )

    def test_artifact_readback_rejects_noncanonical_bytes(self):
        raw = self.publish().path.read_bytes()
        with self.assertRaisesRegex(
            SnapshotPublicationError, "not canonical JCS"
        ):
            validate_learning_artifact_bytes(b" " + raw, self.policies)

    def test_artifact_readback_rejects_symlink_and_filename_identity_mismatch(self):
        published = self.publish()
        with self.assertRaisesRegex(
            SnapshotPublicationError, "snapshot filename mismatch"
        ):
            validate_learning_artifact_bytes(
                published.path.read_bytes(),
                self.policies,
                expected_snapshot_id="0" * 64,
            )
        link = published.path.parent / ("f" * 64 + ".json")
        link.symlink_to(published.path.name)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "unsafe snapshot target"
        ):
            read_learning_artifact(
                published.path.parent, "f" * 64, self.policies
            )

    def test_artifact_readback_requires_mode_0600(self):
        published = self.publish()
        published.path.chmod(0o644)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "mode-0600"
        ):
            read_learning_artifact(
                published.path.parent, published.snapshot_id, self.policies
            )

    def test_artifact_readback_rejects_post_open_filename_swap(self):
        published = self.publish()
        original_bytes = published.path.read_bytes()
        displaced = published.path.with_suffix(".displaced")
        original_read = os.read
        swapped = False

        def read_then_swap(descriptor, maximum):
            nonlocal swapped
            content = original_read(descriptor, maximum)
            if not swapped:
                swapped = True
                published.path.rename(displaced)
                published.path.write_bytes(original_bytes)
                published.path.chmod(0o600)
            return content

        with patch("snapshot_store.os.read", side_effect=read_then_swap):
            with self.assertRaisesRegex(
                SnapshotPublicationError, "changed during read"
            ):
                read_learning_artifact(
                    published.path.parent, published.snapshot_id, self.policies
                )

    def test_artifact_readback_fifo_swap_is_nonblocking(self):
        published = self.publish()
        original_open = os.open
        swapped = False

        def swap_to_fifo(path, flags, *args, **kwargs):
            nonlocal swapped
            if path == published.path.name and not swapped:
                self.assertTrue(
                    flags & os.O_NONBLOCK,
                    "snapshot target open must be nonblocking",
                )
                swapped = True
                published.path.unlink()
                os.mkfifo(published.path, mode=0o600)
            return original_open(path, flags, *args, **kwargs)

        with patch("snapshot_store.os.open", side_effect=swap_to_fifo):
            with self.assertRaisesRegex(
                SnapshotPublicationError, "changed during read"
            ):
                read_learning_artifact(
                    published.path.parent, published.snapshot_id, self.policies
                )

    def test_snapshot_rejects_narrative_and_annotation_fields(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        for field in ("narrative", "annotation", "summary_markdown"):
            with self.subTest(field=field):
                tampered = {**artifact, field: "different text"}
                with self.assertRaises(SnapshotPublicationError):
                    validate_learning_artifact_bytes(
                        artifact_bytes_with_recomputed_file_digest(tampered),
                        self.policies,
                    )
        self.assertEqual(
            [], list((self.home / "learning/annotations").glob("*"))
        )

    def test_concurrent_same_v2_id_publishers_never_overwrite(self):
        results = self.publish_from_two_processes_released_by_one_barrier()
        self.assertEqual(
            [False, True], sorted(result.created for result in results)
        )
        self.assertEqual(1, len({result.snapshot_id for result in results}))
        final_path = results[0].path
        read_learning_artifact(
            final_path.parent, results[0].snapshot_id, self.policies
        )
        self.assertEqual([], list(final_path.parent.glob(".snapshot-*.tmp")))

    def test_v1_v2_sibling_artifacts_remain_independent(self):
        v1_id, v1_bytes, historical_policies = (
            self.historical_v1_artifact_bytes()
        )
        learning_dir = self.home / "learning"
        learning_dir.mkdir(mode=0o700)
        learning_dir.chmod(0o700)
        snapshot_dir = learning_dir / "snapshots"
        snapshot_dir.mkdir(mode=0o700)
        snapshot_dir.chmod(0o700)
        v1_path = snapshot_dir / f"{v1_id}.json"
        v1_path.write_bytes(v1_bytes)
        v1_path.chmod(0o600)

        v2 = self.publish()

        self.assertNotEqual(v1_id, v2.snapshot_id)
        self.assertEqual(v1_bytes, v1_path.read_bytes())
        self.assertEqual(1, read_learning_artifact(
            snapshot_dir, v1_id, historical_policies
        )["core"]["schema_version"])
        self.assertEqual(2, read_learning_artifact(
            snapshot_dir, v2.snapshot_id, self.policies
        )["schema_version"])
        self.assertEqual(
            sorted([v1_id, v2.snapshot_id]),
            sorted(path.stem for path in snapshot_dir.glob("*.json")),
        )


if __name__ == "__main__":
    unittest.main()
