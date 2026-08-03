from __future__ import annotations

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
from learning_snapshot import candidate_id
from policy_artifacts import load_policy_set
from snapshot_input import (
    SNAPSHOT_ANALYZER_FILES,
    SnapshotQuery,
    acquire_snapshot_input,
)
from snapshot_store import (
    SnapshotPublicationError,
    create_learning_snapshot,
    read_learning_artifact,
    validate_learning_artifact,
    validate_learning_artifact_bytes,
)
from store_config import PORTABLE_SEMANTICS
import wiki_observations
from wiki_observations import ObservationPaths
from workflow_evolution_fixtures import FakeObservationStore


FIXED_NOW = "2026-08-03T00:01:00Z"
_SNAPSHOT_CORE_DOMAIN = b"workflow-observatory:learning-snapshot-core:v1\0"
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
    rebuilt["snapshot_id"] = hash_canonical(
        _SNAPSHOT_CORE_DOMAIN, rebuilt["core"]
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
        )

    def publish(self, generated_at=FIXED_NOW):
        return create_learning_snapshot(
            acquire=self.acquire,
            query=self.query,
            policy_set=self.policies,
            home=self.home,
            generated_at=generated_at,
        )

    def artifact_with_recurring_decision_patterns(self, *, include_pair=False):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        core = artifact["core"]
        single = core["decision_patterns"][0]
        single["evidence_strength"] = "recurring"
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
        core["candidates"].extend(
            decision_candidate_for_pattern(core, pattern) for pattern in patterns
        )
        core["candidates"].sort(key=lambda row: row["candidate_id"])
        return artifact, patterns

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
            "authoritative",
            "generated_at",
            "store_identity",
            "adapter",
            "snapshot_id",
            "core",
            "artifact_sha256",
        }, set(artifact))
        self.assertEqual("learning-snapshot", artifact["artifact_type"])
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
            validate_learning_artifact(artifact)

    def test_adapter_tamper_cannot_inject_path_metadata(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["adapter"]["name"] = str(self.home)
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "adapter identity is invalid"
        ):
            validate_learning_artifact_bytes(raw)

    def test_adapter_digest_must_bind_analyzer_artifact(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["adapter"]["implementation_sha256"] = "0" * 64
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "adapter.*analyzer"
        ):
            validate_learning_artifact_bytes(raw)

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
                    SnapshotPublicationError, "core structure is invalid"
                ):
                    validate_learning_artifact_bytes(
                        artifact_bytes_with_recomputed_identities(item)
                    )

    def test_recursive_core_validation_rejects_embedded_sensitive_path(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        candidate = artifact["core"]["candidates"][0]
        candidate["candidate_type"] = "leak /Users/alice/private/story"
        evidence = dict(candidate)
        del evidence["candidate_id"]
        candidate["candidate_id"] = candidate_id(evidence)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "sensitive path or credential"
        ):
            validate_learning_artifact_bytes(
                artifact_bytes_with_recomputed_identities(artifact)
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
                artifact_bytes_with_recomputed_identities(artifact)
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
                artifact_bytes_with_recomputed_identities(artifact)
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
                artifact_bytes_with_recomputed_identities(artifact)
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
                        artifact_bytes_with_recomputed_identities(artifact)
                    )

    def test_valid_single_event_and_adjacent_pair_patterns_are_accepted(self):
        artifact, patterns = self.artifact_with_recurring_decision_patterns(
            include_pair=True
        )

        validated = validate_learning_artifact_bytes(
            artifact_bytes_with_recomputed_identities(artifact)
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
                artifact_bytes_with_recomputed_identities(artifact)
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
            artifact_bytes_with_recomputed_identities(artifact)
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
            validate_learning_artifact_bytes(ambiguous)

    def test_artifact_readback_recomputes_snapshot_identity(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["core"]["analyzer_version"] = "tampered"
        raw = artifact_bytes_with_recomputed_file_digest(artifact)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "snapshot identity mismatch"
        ):
            validate_learning_artifact_bytes(raw)

    def test_artifact_readback_recomputes_file_digest(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        artifact["artifact_sha256"] = "0" * 64
        with self.assertRaisesRegex(
            SnapshotPublicationError, "artifact digest mismatch"
        ):
            validate_learning_artifact_bytes(canonicalize(artifact))

    def test_artifact_readback_rejects_noncanonical_bytes(self):
        raw = self.publish().path.read_bytes()
        with self.assertRaisesRegex(
            SnapshotPublicationError, "not canonical JCS"
        ):
            validate_learning_artifact_bytes(b" " + raw)

    def test_artifact_readback_rejects_symlink_and_filename_identity_mismatch(self):
        published = self.publish()
        with self.assertRaisesRegex(
            SnapshotPublicationError, "snapshot filename mismatch"
        ):
            validate_learning_artifact_bytes(
                published.path.read_bytes(),
                expected_snapshot_id="0" * 64,
            )
        link = published.path.parent / ("f" * 64 + ".json")
        link.symlink_to(published.path.name)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "unsafe snapshot target"
        ):
            read_learning_artifact(published.path.parent, "f" * 64)

    def test_artifact_readback_requires_mode_0600(self):
        published = self.publish()
        published.path.chmod(0o644)
        with self.assertRaisesRegex(
            SnapshotPublicationError, "mode-0600"
        ):
            read_learning_artifact(
                published.path.parent, published.snapshot_id
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
                    published.path.parent, published.snapshot_id
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
                    published.path.parent, published.snapshot_id
                )

    def test_snapshot_rejects_narrative_and_annotation_fields(self):
        artifact = strict_json_loads(self.publish().path.read_bytes())
        for field in ("narrative", "annotation", "summary_markdown"):
            with self.subTest(field=field):
                tampered = {**artifact, field: "different text"}
                with self.assertRaises(SnapshotPublicationError):
                    validate_learning_artifact(tampered)
        self.assertEqual(
            [], list((self.home / "learning/annotations").glob("*"))
        )

    def test_concurrent_publishers_never_overwrite(self):
        results = self.publish_from_two_processes_released_by_one_barrier()
        self.assertEqual(
            [False, True], sorted(result.created for result in results)
        )
        self.assertEqual(1, len({result.snapshot_id for result in results}))
        final_path = results[0].path
        read_learning_artifact(final_path.parent, results[0].snapshot_id)
        self.assertEqual([], list(final_path.parent.glob(".snapshot-*.tmp")))


if __name__ == "__main__":
    unittest.main()
