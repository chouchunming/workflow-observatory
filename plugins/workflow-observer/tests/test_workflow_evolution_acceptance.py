from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
for module_root in (PLUGIN_ROOT / "scripts", PLUGIN_ROOT / "tests"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)
from episode_schema import (
    build_episode_v2,
    parse_v2_supplement,
    render_episode_block,
    synthetic_episode_projection as canonical_episode_projection,
)
from learning_snapshot import build_snapshot_core
from policy_artifacts import (
    PolicyError,
    PolicySet,
    effective_boundary_applies,
    load_policy_set,
    validate_effective_boundary,
)
from snapshot_input import (
    SNAPSHOT_ANALYZER_FILES,
    SnapshotInput,
    SnapshotInputError,
    SnapshotQuery,
    acquire_snapshot_input,
)
from snapshot_store import (
    SnapshotPublicationError,
    create_learning_snapshot,
    read_learning_artifact,
    validate_learning_artifact_bytes,
)
from store_config import LLMWIKI_SEMANTICS, PORTABLE_SEMANTICS, StoreConfig
import wiki_observations
from wiki_observations import ObservationPaths
import workflow_observer_cli
from workflow_evolution_fixtures import (
    DECISION,
    FakeObservationStore,
    V1_BODY,
    V1_METADATA,
    V2_METADATA,
    V2_SUPPLEMENT,
    temporary_timezone,
)


FIXED_GENERATED_AT = "2026-08-03T00:01:00Z"
SNAPSHOT_CORE_DOMAIN = b"workflow-observatory:learning-snapshot-core:v1\0"
INPUT_MANIFEST_DOMAIN = b"workflow-observatory:snapshot-input-manifest:v1\0"
GENERATION = "implementation-with-review@2"
COMPLETION = """## Execution evidence

- Verification: acceptance fixture passed
- Artifacts: immutable learning snapshot

## Outcome and observation

- Outcome: Fixture draft finalized
- Observation: Stable-read mutation was detected

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


class WorkflowEvolutionAcceptanceTests(unittest.TestCase):
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
            lifecycle_as_of=interval["until_exclusive"],
            project=None,
            workspace=None,
            workspace_id=None,
            task_type=None,
        )
        self._sequence = 10

    def acquire(self):
        return acquire_snapshot_input(
            ObservationPaths.from_root(self.store.store_root),
            PORTABLE_SEMANTICS,
            self.query,
            self.policies,
        )

    def publish(self):
        return create_learning_snapshot(
            acquire=self.acquire,
            query=self.query,
            policy_set=self.policies,
            home=self.home,
            generated_at=FIXED_GENERATED_AT,
        )

    def _publish_same_bundle_twice(self):
        return [self.publish(), self.publish()]

    def _run_id(self):
        self._sequence += 1
        return f"obs-20260802-{self._sequence:06d}-{self._sequence:06x}"

    def _completion_body(self, completion_metrics):
        body = V1_BODY
        for old, new in {
            "verification: pass": (
                f"verification: {completion_metrics['verification']}"
            ),
            "review_rounds: 1": (
                f"review_rounds: {completion_metrics['review_rounds']}"
            ),
            "defects_found: 0": (
                f"defects_found: {completion_metrics['defects_found']}"
            ),
            "rework_count: 0": (
                f"rework_count: {completion_metrics['rework_count']}"
            ),
        }.items():
            body = body.replace(old, new)
        return body

    def _projection(
        self,
        *,
        status="success",
        schema_version=2,
        generation=GENERATION,
        started_at="2026-08-02T08:00:00+08:00",
        completion_metrics=None,
        supplement_execution=None,
        supplement_quality=None,
        supplement_decisions=None,
    ):
        run_id = self._run_id()
        completion = {
            "verification": "pass",
            "review_rounds": 1,
            "defects_found": 0,
            "rework_count": 0,
        }
        completion.update(completion_metrics or {})
        metadata = deepcopy(V2_METADATA if schema_version == 2 else V1_METADATA)
        metadata.update({
            "run_id": run_id,
            "status": status,
            "timestamp": started_at,
        })
        if schema_version == 2:
            metadata["schema_version"] = 2
            if generation is None:
                metadata.pop("workflow_generation", None)
            else:
                metadata["workflow_generation"] = generation
        else:
            metadata.pop("schema_version", None)
            metadata.pop("workflow_generation", None)

        if status == "draft":
            body = V1_BODY.split("\n## Execution evidence", 1)[0].rstrip() + "\n"
        else:
            body = self._completion_body(completion)
            if schema_version == 2:
                supplement_data = json.loads(V2_SUPPLEMENT)
                supplement_data["execution"].update(supplement_execution or {})
                supplement_data["quality"].update(supplement_quality or {})
                if supplement_decisions is not None:
                    supplement_data["decisions"] = supplement_decisions
                has_execution_value = any(
                    supplement_data["execution"][name] is not None
                    for name in (
                        "input_tokens",
                        "output_tokens",
                        "cache_read_tokens",
                        "cost_amount",
                    )
                )
                supplement_data["execution"]["measurement_source"] = (
                    "tool-derived" if has_execution_value else "unavailable"
                )
                supplement = parse_v2_supplement(
                    json.dumps(supplement_data), self.policies.documents
                )
                episode = build_episode_v2(
                    elapsed_seconds=120,
                    completion_metrics=completion,
                    supplement=supplement,
                )
                body = body.rstrip() + "\n\n" + render_episode_block(episode)

        projected = canonical_episode_projection(
            metadata, body, self.policies.documents
        )
        projected["source_sha256"] = hashlib.sha256(
            canonicalize(projected)
        ).hexdigest()
        return projected

    def _policies_with_mapping(self, mapping):
        documents = self.policies.documents
        document = documents["workflow_generation_mapping"]
        document["mapping"] = dict(mapping)
        identities = self.policies.identities
        identities["workflow_generation_mapping"] = {
            "version": document["version"],
            "sha256": "sha256:" + hashlib.sha256(
                canonicalize(document)
            ).hexdigest(),
        }
        return PolicySet(documents, identities)

    def _snapshot_input(
        self,
        episodes,
        *,
        invalidated=frozenset(),
        as_of="2026-08-02T16:00:00Z",
        policies=None,
    ):
        selected_policies = self.policies if policies is None else policies
        invalidations = [
            {
                "run_id": run_id,
                "source_sha256": hashlib.sha256(
                    f"invalidate:{run_id}".encode("ascii")
                ).hexdigest(),
                "timestamp": "2026-08-02T12:00:00Z",
            }
            for run_id in sorted(invalidated)
        ]
        bundle = {
            "schema_version": 1,
            "projection_version": "episode-projection@2",
            "query": {
                "interval": {
                    "basis": "started_at",
                    "since_inclusive": "2026-07-31T16:00:00Z",
                    "until_exclusive": "2026-08-02T16:00:00Z",
                    "requested_timezone": "Asia/Taipei",
                    "requested_dates": {
                        "since": "2026-08-01",
                        "until_inclusive": "2026-08-02",
                    },
                },
                "lifecycle_as_of": as_of,
                "project": None,
                "workspace": None,
                "workspace_id": None,
                "task_type": None,
            },
            "lifecycle_as_of": as_of,
            "policy_set": selected_policies.core_identity(),
            "schema_capabilities": deepcopy(
                self.policies.documents["episode_projection"]
                ["schema_capabilities"]
            ),
            "record_counts": {
                "selected_episode_n": len(episodes),
                "draft_episode_n": sum(
                    episode["status"] == "draft" for episode in episodes
                ),
                "final_episode_n": sum(
                    episode["status"] != "draft" for episode in episodes
                ),
                "selected_invalidation_n": len(invalidations),
            },
            "episodes": sorted(episodes, key=lambda row: row["run_id"]),
            "invalidations": invalidations,
            "reference_manifest": [],
        }
        bundle["input_manifest_sha256"] = hash_canonical(
            INPUT_MANIFEST_DOMAIN, bundle
        )
        return SnapshotInput(
            adapter={
                "name": "portable",
                "implementation_version": (
                    "workflow-observer-snapshot-adapter@1"
                ),
                "implementation_sha256": (
                    selected_policies.core_identity()
                    ["analyzer_artifact"]["sha256"][7:]
                ),
            },
            store_identity="f" * 64,
            semantic_bundle=bundle,
            reviewed_generation_mapping=(
                selected_policies.documents["workflow_generation_mapping"]
            ),
        )

    def _metric(self, core, name):
        return next(
            metric for metric in core["cohorts"][0]["metrics"]
            if metric["metric"] == name
        )

    def test_01_identical_inputs_produce_identical_core_and_snapshot_id(self):
        published = self._publish_same_bundle_twice()
        self.assertEqual(2, len(published))
        artifacts = [
            read_learning_artifact(
                result.path.parent, result.snapshot_id, self.policies
            )
            for result in published
        ]
        recomputed_ids = [
            hash_canonical(SNAPSHOT_CORE_DOMAIN, artifact["core"])
            for artifact in artifacts
        ]

        self.assertEqual(
            canonicalize(artifacts[0]["core"]),
            canonicalize(artifacts[1]["core"]),
        )
        self.assertEqual(
            [result.snapshot_id for result in published],
            recomputed_ids,
        )
        self.assertEqual(recomputed_ids[0], recomputed_ids[1])

    def test_02_machine_timezone_does_not_change_manifest(self):
        def add_historical_record(kind, run_id, day):
            key = "v1" if kind == "v1" else "v2"
            old_run_id = (
                "obs-20260802-000000-abcdef"
                if key == "v1"
                else "obs-20260802-000001-fedcba"
            )
            raw = self.store.expected_raw_bytes[key]
            raw = raw.replace(old_run_id.encode("ascii"), run_id.encode("ascii"))
            raw = raw.replace(
                b"2026-08-02T08:00:00+08:00",
                f"{day}T08:00:00+08:00".encode("ascii"),
            )
            raw = raw.replace(
                b"2026-08-02T08:02:00+08:00",
                f"{day}T08:02:00+08:00".encode("ascii"),
            )
            self.store._write_private(
                self.store.observations / f"{run_id}.md", raw
            )

        add_historical_record(
            "v1", "obs-20260715-080000-a1b2c3", "2026-07-15"
        )
        add_historical_record(
            "v2", "obs-20260720-080000-b2c3d4", "2026-07-20"
        )
        add_historical_record(
            "v1", "obs-20260714-080000-c3d4e5", "2026-07-14"
        )
        add_historical_record(
            "v2", "obs-20260803-080000-d4e5f6", "2026-08-03"
        )
        with temporary_timezone("UTC"):
            utc = self.acquire()
        with temporary_timezone("America/Los_Angeles"):
            los_angeles = self.acquire()

        self.assertEqual(utc.manifest_bytes, los_angeles.manifest_bytes)

        outside = self.store.base / "outside-fake-home.txt"
        outside.write_bytes(b"must remain unchanged\n")
        store_before = {
            path.relative_to(self.store.store_root): path.read_bytes()
            for path in self.store.store_root.rglob("*")
            if path.is_file()
        }
        arguments = [
            "snapshot",
            "--since", "2026-07-15",
            "--until", "2026-08-02",
            "--timezone", "Asia/Taipei",
            "--as-of", "2026-08-02T16:00:00Z",
        ]

        def run_snapshot(machine_timezone):
            stdout = io.StringIO()
            stderr = io.StringIO()
            config = StoreConfig("portable", self.store.store_root, None)
            with (
                temporary_timezone(machine_timezone),
                mock.patch(
                    "workflow_observer_cli.load_store_config",
                    return_value=config,
                ),
                mock.patch.dict(
                    "os.environ",
                    {"WORKFLOW_OBSERVATORY_HOME": str(self.home)},
                ),
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                exit_code = workflow_observer_cli.main(arguments)
            self.assertEqual((0, ""), (exit_code, stderr.getvalue()))
            return strict_json_loads(stdout.getvalue())

        first = run_snapshot("UTC")
        second = run_snapshot("America/Los_Angeles")
        first_snapshot = first["snapshot"]
        second_snapshot = second["snapshot"]
        self.assertIs(True, first["created"])
        self.assertIs(False, second["created"])
        self.assertEqual(
            first_snapshot["snapshot_id"], second_snapshot["snapshot_id"]
        )
        self.assertEqual(
            canonicalize(first_snapshot["core"]),
            canonicalize(second_snapshot["core"]),
        )
        selected_ids = {
            row["run_id"]
            for row in first_snapshot["core"]["input_manifest"]["episodes"]
        }
        self.assertIn("obs-20260715-080000-a1b2c3", selected_ids)
        self.assertIn("obs-20260720-080000-b2c3d4", selected_ids)
        self.assertNotIn("obs-20260714-080000-c3d4e5", selected_ids)
        self.assertNotIn("obs-20260803-080000-d4e5f6", selected_ids)
        unsupported_v1 = sum(
            next(
                metric for metric in cohort["metrics"]
                if metric["metric"] == "input_tokens"
            )["missingness"]["unsupported_by_schema_n"]
            for cohort in first_snapshot["core"]["cohorts"]
        )
        self.assertEqual(2, unsupported_v1)
        snapshot_dir = self.home / "learning" / "snapshots"
        self.assertEqual(1, len(list(snapshot_dir.glob("*.json"))))
        read_learning_artifact(
            snapshot_dir, first_snapshot["snapshot_id"], self.policies
        )
        self.assertEqual([], list(self.home.rglob("*proposal*")))
        self.assertEqual(b"must remain unchanged\n", outside.read_bytes())
        store_after = {
            path.relative_to(self.store.store_root): path.read_bytes()
            for path in self.store.store_root.rglob("*")
            if path.is_file()
        }
        self.assertEqual(store_before, store_after)

    def test_03_store_change_aborts_without_snapshot(self):
        self.store.v2_path.unlink()
        final = self.store.v1_path.read_bytes()
        draft = final.replace(b'status: "success"', b'status: "draft"')
        draft = draft.split(b"\n## Execution evidence", 1)[0].rstrip() + b"\n"
        self.store._write_private(self.store.v1_path, draft)
        calls = 0

        def acquire_then_finalize():
            nonlocal calls
            calls += 1
            if calls == 2:
                wiki_observations.finish_observation(
                    ObservationPaths.from_root(self.store.store_root),
                    "obs-20260802-000000-abcdef",
                    "success",
                    wiki_observations.parse_completion_payload(COMPLETION),
                    semantics=PORTABLE_SEMANTICS,
                )
            return self.acquire()

        with self.assertRaisesRegex(
            SnapshotPublicationError, "changed during analysis"
        ) as caught:
            create_learning_snapshot(
                acquire=acquire_then_finalize,
                query=self.query,
                policy_set=self.policies,
                home=self.home,
                generated_at=FIXED_GENERATED_AT,
            )

        self.assertEqual("state", caught.exception.kind)
        snapshot_dir = self.home / "learning" / "snapshots"
        self.assertEqual(
            [], list(snapshot_dir.glob("*.json")) if snapshot_dir.exists() else []
        )

    def test_04_adapter_fixtures_project_identically(self):
        with FakeObservationStore("portable") as portable, FakeObservationStore(
            "llmwiki"
        ) as llmwiki:
            portable_input = acquire_snapshot_input(
                ObservationPaths.from_root(portable.store_root),
                PORTABLE_SEMANTICS,
                self.query,
                self.policies,
            )
            llmwiki_input = acquire_snapshot_input(
                ObservationPaths.from_root(llmwiki.store_root),
                LLMWIKI_SEMANTICS,
                self.query,
                self.policies,
            )

        self.assertEqual(
            canonicalize(portable_input.semantic_bundle),
            canonicalize(llmwiki_input.semantic_bundle),
        )

    def test_05_v1_absence_is_not_zero_or_v2_missing(self):
        v1_episodes = [self._projection(schema_version=1) for _ in range(4)]
        mapping = {episode["run_id"]: GENERATION for episode in v1_episodes}
        policies = self._policies_with_mapping(mapping)
        for episode in v1_episodes:
            episode["workflow_generation"] = {
                "availability": "observed",
                "value": GENERATION,
            }
        episodes = v1_episodes + [
            self._projection(supplement_quality={"test_failures": None})
            for _ in range(2)
        ] + [
            self._projection(supplement_quality={"test_failures": value})
            for value in (0, 1)
        ]
        core = build_snapshot_core(
            self._snapshot_input(episodes, policies=policies), policies
        )
        metric = self._metric(core, "test_failures")
        self.assertEqual({
            "eligible_episode_n": 8,
            "observed_n": 2,
            "not_recorded_n": 2,
            "unsupported_by_schema_n": 4,
            "not_applicable_n": 0,
        }, metric["missingness"])

        zero_observed = self._projection(supplement_execution={
            "input_tokens": None,
            "output_tokens": None,
            "cache_read_tokens": 1,
        })
        missing_core = build_snapshot_core(
            self._snapshot_input([zero_observed]), self.policies
        )
        input_tokens = self._metric(missing_core, "input_tokens")
        output_tokens = self._metric(missing_core, "output_tokens")
        self.assertEqual(
            input_tokens["missingness"], output_tokens["missingness"]
        )
        self.assertEqual(0, input_tokens["missingness"]["observed_n"])
        self.assertEqual(0, output_tokens["missingness"]["observed_n"])
        candidates = [
            candidate for candidate in missing_core["candidates"]
            if candidate["source"]["kind"] == "metric"
            and candidate["source"]["identity"] in {
                "input_tokens", "output_tokens"
            }
        ]
        self.assertEqual(
            {"input_tokens", "output_tokens"},
            {candidate["source"]["identity"] for candidate in candidates},
        )
        self.assertEqual(2, len(candidates))
        self.assertEqual(2, len({row["candidate_id"] for row in candidates}))
        candidate_by_metric = {
            candidate["source"]["identity"]: candidate
            for candidate in candidates
        }
        input_candidate_missingness = (
            candidate_by_metric["input_tokens"]["evidence"]["missingness"]
        )
        output_candidate_missingness = (
            candidate_by_metric["output_tokens"]["evidence"]["missingness"]
        )
        self.assertEqual(
            input_candidate_missingness, output_candidate_missingness
        )
        self.assertEqual(0, input_candidate_missingness["observed_n"])
        self.assertEqual(0, output_candidate_missingness["observed_n"])
        self.assertEqual(
            input_tokens["missingness"],
            input_candidate_missingness,
        )
        self.assertEqual(
            output_tokens["missingness"],
            output_candidate_missingness,
        )

    def test_06_lifecycle_records_do_not_enter_outcome_denominator(self):
        outcomes = [
            self._projection(status=status)
            for status in ("success", "failed", "partial", "rolled-back")
        ]
        drafts = [
            self._projection(
                status="draft", started_at="2026-08-01T16:00:00Z"
            ),
            self._projection(
                status="draft", started_at="2026-08-01T15:59:59Z"
            ),
        ]
        superseded = self._projection(status="superseded")
        acquired = self._snapshot_input(
            outcomes + drafts + [superseded],
            invalidated=frozenset({superseded["run_id"]}),
        )
        core = build_snapshot_core(acquired, self.policies)
        cohort = core["cohorts"][0]

        self.assertEqual(4, cohort["outcome_episode_n"])
        self.assertEqual(2, cohort["draft_episode_n"])
        self.assertEqual(1, cohort["superseded_episode_n"])
        self.assertEqual(1, cohort["invalidated_episode_n"])
        self.assertEqual(1, cohort["active_draft_n"])
        self.assertEqual(1, cohort["stale_draft_n"])
        self.assertEqual({
            "failed": 1,
            "partial": 1,
            "rolled-back": 1,
            "success": 1,
        }, cohort["outcome_counts"])

    def test_07_decision_recurrence_uses_distinct_episodes(self):
        ten_events = [
            {**deepcopy(DECISION), "sequence": sequence}
            for sequence in range(1, 11)
        ]
        dominated_episodes = [
            self._projection(supplement_decisions=ten_events)
        ] + [
            self._projection(supplement_decisions=[]) for _ in range(4)
        ]
        dominated = build_snapshot_core(
            self._snapshot_input(dominated_episodes), self.policies
        )
        single = next(
            row for row in dominated["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        )
        self.assertEqual(10, single["event_count"])
        self.assertEqual(1, single["episode_count_with_event"])
        self.assertEqual("descriptive", single["evidence_strength"])
        self.assertTrue(all(
            episode["runtime_provenance"] is None
            for episode in dominated_episodes
        ))

        def decision_core(outcome_n, supporting_n, *, generation=GENERATION):
            episodes = [
                self._projection(
                    generation=generation,
                    supplement_decisions=(
                        [deepcopy(DECISION)] if index < supporting_n else []
                    ),
                )
                for index in range(outcome_n)
            ]
            return build_snapshot_core(
                self._snapshot_input(episodes), self.policies
            )

        four = decision_core(4, 3)
        four_pattern = next(
            row for row in four["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        )
        self.assertEqual([], four["exclusion_ledger"])
        self.assertEqual(3, four_pattern["episode_count_with_event"])
        self.assertEqual("descriptive", four_pattern["evidence_strength"])
        self.assertFalse(any(
            candidate["class"] == "decision-pattern"
            for candidate in four["candidates"]
        ))

        five = decision_core(5, 3)
        five_pattern = next(
            row for row in five["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        )
        self.assertEqual("recurring", five_pattern["evidence_strength"])
        self.assertEqual(1, len([
            candidate for candidate in five["candidates"]
            if candidate["class"] == "decision-pattern"
        ]))

        unavailable = decision_core(5, 3, generation=None)
        self.assertTrue(unavailable["decision_patterns"])
        self.assertTrue(all(
            pattern["evidence_strength"] == "descriptive"
            for pattern in unavailable["decision_patterns"]
        ))
        self.assertFalse(any(
            candidate["class"] == "decision-pattern"
            for candidate in unavailable["candidates"]
        ))

    def test_08_reviewed_mapping_derived_view_counts_once(self):
        self.store.v2_path.unlink()
        run_id = "obs-20260802-000000-abcdef"
        policies = self._policies_with_mapping({
            run_id: "implementation-with-review@1"
        })
        acquired = acquire_snapshot_input(
            ObservationPaths.from_root(self.store.store_root),
            PORTABLE_SEMANTICS,
            self.query,
            policies,
        )

        physical_records = list(self.store.observations.glob("*.md"))
        self.assertEqual([self.store.v1_path], physical_records)
        self.assertEqual(1, len(acquired.semantic_bundle["episodes"]))
        self.assertEqual(
            1,
            acquired.semantic_bundle["record_counts"]["selected_episode_n"],
        )
        self.assertEqual(
            run_id, acquired.semantic_bundle["episodes"][0]["run_id"]
        )
        self.assertEqual(
            {
                "availability": "observed",
                "value": "implementation-with-review@1",
            },
            acquired.semantic_bundle["episodes"][0]["workflow_generation"],
        )

    def test_09_gate_failure_produces_no_snapshot_or_proposal(self):
        self.store.v2_path.unlink()
        broken = self.store.v1_path.read_bytes().replace(
            b"sources: []\n---\n",
            b'task_ref: "[[missing-task]]"\nsources: []\n---\n',
            1,
        )
        self.store._write_private(self.store.v1_path, broken)

        with self.assertRaisesRegex(
            SnapshotInputError, "task_ref points to no task record"
        ):
            self.publish()

        snapshot_dir = self.home / "learning" / "snapshots"
        self.assertEqual(
            [], list(snapshot_dir.iterdir()) if snapshot_dir.exists() else []
        )
        self.assertEqual([], list(self.home.rglob("*proposal*")))

    def test_10_authoritative_tamper_is_not_acceptance(self):
        published = self.publish()
        artifact = strict_json_loads(published.path.read_bytes())
        artifact["authoritative"] = True
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = hashlib.sha256(
            canonicalize(artifact)
        ).hexdigest()

        with self.assertRaisesRegex(
            SnapshotPublicationError, "cannot be authoritative"
        ):
            validate_learning_artifact_bytes(
                canonicalize(artifact), self.policies
            )

    def test_11_snapshot_rejects_narrative_and_annotation_fields(self):
        published = self.publish()
        artifact = read_learning_artifact(
            published.path.parent, published.snapshot_id, self.policies
        )
        for field in ("narrative", "annotation", "summary_markdown"):
            with self.subTest(field=field):
                tampered = {**artifact, field: "unsupported text"}
                tampered.pop("artifact_sha256")
                tampered["artifact_sha256"] = hashlib.sha256(
                    canonicalize(tampered)
                ).hexdigest()
                with self.assertRaisesRegex(
                    SnapshotPublicationError,
                    "learning snapshot artifact has wrong fields",
                ):
                    validate_learning_artifact_bytes(
                        canonicalize(tampered), self.policies
                    )

        self.assertEqual(
            [],
            [
                path for path in self.home.rglob("*")
                if "annotation" in path.name.lower()
            ],
        )

    def test_12_no_approval_causes_no_external_mutation(self):
        subject = self.store.base / "fake-git-subject"
        workflow = subject / ".github" / "workflows" / "observer.yml"
        git_head = subject / ".git" / "HEAD"
        workflow.parent.mkdir(parents=True)
        git_head.parent.mkdir(parents=True)
        workflow.write_bytes(b"name: fixture\non: workflow_dispatch\n")
        git_head.write_bytes(b"ref: refs/heads/fixture\n")
        before = {
            "workflow": workflow.read_bytes(),
            "head": git_head.read_bytes(),
        }

        def reject_external_mutation(command, *args, **kwargs):
            words = [str(word) for word in command]
            prohibited = (
                words[:2] == ["git", "branch"]
                or words[:2] == ["gh", "pr"]
                or any("workflow" in word.lower() for word in words)
            )
            if prohibited:
                raise AssertionError("external mutation is not approved")
            return subprocess.CompletedProcess(words, 0, "", "")

        with mock.patch(
            "subprocess.run", side_effect=reject_external_mutation
        ) as run:
            self.publish()
            run.assert_not_called()
            for command in (
                ["git", "branch", "proposal"],
                ["gh", "pr", "create"],
                ["tool", "edit-workflow", str(workflow)],
            ):
                with self.subTest(command=command):
                    with self.assertRaisesRegex(
                        AssertionError, "not approved"
                    ):
                        run(command)

        self.assertEqual(before["workflow"], workflow.read_bytes())
        self.assertEqual(before["head"], git_head.read_bytes())
        self.assertNotEqual(PLUGIN_ROOT, subject)
        self.assertNotIn(PLUGIN_ROOT, subject.parents)

    def test_13_lifecycle_as_of_is_frozen_in_identity(self):
        self.store.v2_path.unlink()
        final = self.store.v1_path.read_bytes()
        draft = final.replace(b'status: "success"', b'status: "draft"')
        draft = draft.split(b"\n## Execution evidence", 1)[0].rstrip() + b"\n"
        self.store._write_private(self.store.v1_path, draft)

        def query_at(as_of):
            return SnapshotQuery(
                interval=self.query.interval,
                lifecycle_as_of=as_of,
                project=None,
                workspace=None,
                workspace_id=None,
                task_type=None,
            )

        def publish_at(query, generated_at):
            def acquire():
                return acquire_snapshot_input(
                    ObservationPaths.from_root(self.store.store_root),
                    PORTABLE_SEMANTICS,
                    query,
                    self.policies,
                )

            return create_learning_snapshot(
                acquire=acquire,
                query=query,
                policy_set=self.policies,
                home=self.home,
                generated_at=generated_at,
            )

        bound = query_at("2026-08-03T16:00:00Z")
        first = publish_at(bound, "2026-08-03T16:00:01Z")
        later_wall_clock = publish_at(bound, "2026-08-04T16:00:01Z")
        later_as_of = publish_at(
            query_at("2026-08-04T16:00:00Z"),
            "2026-08-04T16:00:01Z",
        )

        self.assertEqual(first.snapshot_id, later_wall_clock.snapshot_id)
        self.assertNotEqual(first.snapshot_id, later_as_of.snapshot_id)

    def test_14_shared_jcs_vector_and_lone_surrogate(self):
        vector_path = (
            PLUGIN_ROOT / "tests" / "fixtures" / "jcs_conformance_vectors.json"
        )
        vector = strict_json_loads(vector_path.read_bytes())
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
        with self.assertRaises(CanonicalizationError):
            canonicalize({"lone": "\ud800"})

    def test_15_policy_hash_and_effective_boundary_are_closed(self):
        documents = self.policies.documents
        lifecycle = documents["lifecycle_health_policy"]
        before_policy_bytes = canonicalize(lifecycle)
        lifecycle["draft_stale_after_seconds"] = 86401
        after_policy_bytes = canonicalize(lifecycle)
        differing_bytes = sum(
            left != right
            for left, right in zip(before_policy_bytes, after_policy_bytes)
        )
        self.assertEqual(len(before_policy_bytes), len(after_policy_bytes))
        self.assertEqual(1, differing_bytes)

        identities = self.policies.identities
        identities["lifecycle_health_policy"] = {
            "version": lifecycle["version"],
            "sha256": "sha256:" + hashlib.sha256(
                after_policy_bytes
            ).hexdigest(),
        }
        changed_policies = PolicySet(documents, identities)
        episode = self._projection(
            status="draft", started_at="2026-08-01T15:59:59Z"
        )
        original_core = build_snapshot_core(
            self._snapshot_input([episode]), self.policies
        )
        changed_core = build_snapshot_core(
            self._snapshot_input([episode], policies=changed_policies),
            changed_policies,
        )
        self.assertNotEqual(
            self.policies.core_identity()["lifecycle_health_policy"],
            changed_policies.core_identity()["lifecycle_health_policy"],
        )
        self.assertNotEqual(
            canonicalize(original_core), canonicalize(changed_core)
        )

        started_at = validate_effective_boundary({
            "type": "started_at",
            "from": "2026-08-03T00:00:00Z",
        })
        self.assertFalse(effective_boundary_applies(
            started_at,
            started_at="2026-08-02T23:59:59Z",
            producer_generation=None,
        ))
        self.assertTrue(effective_boundary_applies(
            started_at,
            started_at="2026-08-03T00:00:00Z",
            producer_generation=None,
        ))

        producer = validate_effective_boundary({
            "type": "producer_generation",
            "from": "producer@3",
        })
        self.assertTrue(effective_boundary_applies(
            producer,
            started_at="2026-08-03T00:00:00Z",
            producer_generation="producer@3",
        ))
        for generation in (None, "producer@2", "producer@10"):
            with self.subTest(generation=generation):
                self.assertFalse(effective_boundary_applies(
                    producer,
                    started_at="2026-08-03T00:00:00Z",
                    producer_generation=generation,
                ))

        invalid = (
            {},
            {"type": "unknown", "from": "x"},
            {"type": "started_at", "from": "producer@3"},
            {
                "type": "producer_generation",
                "from": "2026-08-03T00:00:00Z",
            },
            {
                "type": "started_at",
                "from": "2026-08-03T00:00:00Z",
                "other": 1,
            },
        )
        for value in invalid:
            with self.subTest(boundary=value):
                with self.assertRaises(PolicyError):
                    validate_effective_boundary(value)


if __name__ == "__main__":
    unittest.main()
