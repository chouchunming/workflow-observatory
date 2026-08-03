from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
for module_root in (PLUGIN_ROOT / "scripts", PLUGIN_ROOT / "tests"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import canonicalize, hash_canonical
from episode_schema import (
    build_episode_v2,
    canonical_episode_projection,
    parse_v2_supplement,
    render_episode_block,
)
from learning_snapshot import (
    LearningSnapshotError,
    build_snapshot_core,
    linear_rational_quantile,
)
from policy_artifacts import load_policy_set
from snapshot_input import SNAPSHOT_ANALYZER_FILES, SnapshotInput
from workflow_evolution_fixtures import (
    V1_BODY,
    V1_METADATA,
    V2_METADATA,
    V2_SUPPLEMENT,
)


_INPUT_MANIFEST_DOMAIN = b"workflow-observatory:snapshot-input-manifest:v1\0"
_GENERATION = "implementation-with-review@2"


class LearningSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.policies = load_policy_set(
            PLUGIN_ROOT / "policies",
            analyzer_files=SNAPSHOT_ANALYZER_FILES,
            canonicalizer_files=("scripts/canonical_json.py",),
        )
        self.projection = self.policies.documents
        self._sequence = 0

    def _run_id(self) -> str:
        self._sequence += 1
        return f"obs-20260802-{self._sequence:06d}-{self._sequence:06x}"

    def _completion_body(self, completion_metrics: dict[str, object]) -> str:
        body = V1_BODY
        replacements = {
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
        }
        for old, new in replacements.items():
            body = body.replace(old, new)
        return body

    def _projection(
        self,
        *,
        status: str = "success",
        schema_version: int = 2,
        generation: str | None = _GENERATION,
        started_at: str = "2026-08-02T08:00:00+08:00",
        completion_metrics: dict[str, object] | None = None,
        supplement_execution: dict[str, object] | None = None,
        supplement_quality: dict[str, object] | None = None,
        runtime_generation: str | None = None,
    ) -> dict:
        run_id = self._run_id()
        completion = {
            "verification": "pass",
            "review_rounds": 1,
            "defects_found": 0,
            "rework_count": 0,
        }
        if completion_metrics is not None:
            completion.update(completion_metrics)
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
                    json.dumps(supplement_data), self.projection
                )
                episode = build_episode_v2(
                    elapsed_seconds=120,
                    completion_metrics=completion,
                    supplement=supplement,
                )
                body = body.rstrip() + "\n\n" + render_episode_block(episode)

        projected = canonical_episode_projection(metadata, body, self.projection)
        if schema_version == 1 and generation is not None:
            projected["workflow_generation"] = {
                "availability": "observed",
                "value": generation,
            }
        projected["runtime_provenance"] = runtime_generation
        projected["source_sha256"] = hashlib.sha256(
            canonicalize(projected)
        ).hexdigest()
        return projected

    def _snapshot_input(
        self,
        episodes: list[dict],
        *,
        invalidated: frozenset[str] = frozenset(),
        as_of: str = "2026-08-02T16:00:00Z",
    ) -> SnapshotInput:
        invalidations = [
            {
                "run_id": run_id,
                "source_sha256": hashlib.sha256(
                    f"invalidate:{run_id}".encode("ascii")
                ).hexdigest(),
                "timestamp": "2026-08-02T12:00:00Z",
            }
            for run_id in sorted(invalidated, key=lambda item: item.encode("utf-8"))
        ]
        bundle = {
            "schema_version": 1,
            "projection_version": "episode-projection@2",
            "query": {
                "interval": {
                    "basis": "started_at",
                    "since_inclusive": "2026-08-01T16:00:00Z",
                    "until_exclusive": "2026-08-02T16:00:00Z",
                    "requested_timezone": "Asia/Taipei",
                    "requested_dates": {
                        "since": "2026-08-02",
                        "until_inclusive": "2026-08-02",
                    },
                },
                "project": None,
                "workspace": None,
                "workspace_id": None,
                "task_type": None,
            },
            "lifecycle_as_of": as_of,
            "policy_set": self.policies.core_identity(),
            "schema_capabilities": deepcopy(
                self.projection["episode_projection"]["schema_capabilities"]
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
            "episodes": sorted(
                episodes, key=lambda episode: episode["run_id"].encode("utf-8")
            ),
            "invalidations": invalidations,
            "reference_manifest": [
                {
                    "kind": "task",
                    "identity": "fixture-task",
                    "sha256": "a" * 64,
                }
            ],
        }
        bundle["input_manifest_sha256"] = hash_canonical(
            _INPUT_MANIFEST_DOMAIN, bundle
        )
        return SnapshotInput(
            adapter={
                "name": "portable",
                "implementation_version": "workflow-observer-snapshot-adapter@1",
                "implementation_sha256": (
                    self.policies.core_identity()["analyzer_artifact"]["sha256"][7:]
                ),
            },
            store_identity="f" * 64,
            semantic_bundle=bundle,
        )

    def bundle(
        self,
        outcomes=(),
        drafts=(),
        superseded=0,
        invalidated=frozenset(),
        as_of="2026-08-02T16:00:00Z",
    ) -> SnapshotInput:
        self._sequence = 0
        episodes = [self._projection(status=status) for status in outcomes]
        labels: dict[str, str] = {}
        for draft_state in drafts:
            started_at = {
                "active": "2026-08-01T16:00:00Z",
                "stale": "2026-08-01T15:59:59Z",
            }[draft_state]
            episode = self._projection(status="draft", started_at=started_at)
            labels[f"{draft_state}-draft"] = episode["run_id"]
            episodes.append(episode)
        for index in range(superseded):
            episode = self._projection(status="superseded")
            if index == 0:
                labels["superseded-run"] = episode["run_id"]
            episodes.append(episode)
        invalidated_ids = frozenset(labels.get(item, item) for item in invalidated)
        return self._snapshot_input(
            episodes,
            invalidated=invalidated_ids,
            as_of=as_of,
        )

    def metric_partition(self, v1_absent, v2_null, v2_values):
        episodes = [
            self._projection(schema_version=1)
            for _ in range(v1_absent)
        ]
        episodes.extend(
            self._projection(
                supplement_quality={"test_failures": None},
            )
            for _ in range(v2_null)
        )
        episodes.extend(
            self._projection(
                supplement_quality={"test_failures": value},
            )
            for value in v2_values
        )
        core = build_snapshot_core(self._snapshot_input(episodes), self.policies)
        return self._metric(core["cohorts"][0], "test_failures")

    def metric_output(self, name, values):
        episodes = []
        for value in values:
            completion = {name: value} if name in {
                "verification",
                "review_rounds",
                "defects_found",
                "rework_count",
            } else None
            quality = {name: value} if name in {
                "test_failures",
                "timeout_count",
            } else None
            execution = {name: value} if name in {
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
            } else None
            episodes.append(self._projection(
                completion_metrics=completion,
                supplement_execution=execution,
                supplement_quality=quality,
            ))
        core = build_snapshot_core(self._snapshot_input(episodes), self.policies)
        return self._metric(core["cohorts"][0], name)

    def cost_metric_output(self, values):
        episodes = [
            self._projection(supplement_execution={
                "cost_amount": amount,
                "cost_currency": currency,
            })
            for amount, currency in values
        ]
        core = build_snapshot_core(self._snapshot_input(episodes), self.policies)
        return self._metric(core["cohorts"][0], "cost_amount")

    def _metric(self, cohort, name):
        return next(metric for metric in cohort["metrics"] if metric["metric"] == name)

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
        self.assertEqual(1, cohort["active_draft_n"])
        self.assertEqual(1, cohort["stale_draft_n"])
        self.assertEqual("descriptive", cohort["evidence_strength"])
        self.assertEqual(
            {"failed": 1, "partial": 1, "rolled-back": 1, "success": 1},
            cohort["outcome_counts"],
        )

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
        left = build_snapshot_core(
            self.bundle(drafts=["active"], as_of="2026-08-02T16:00:00Z"),
            self.policies,
        )
        right = build_snapshot_core(
            self.bundle(drafts=["active"], as_of="2026-08-02T16:00:00Z"),
            self.policies,
        )
        later = build_snapshot_core(
            self.bundle(drafts=["active"], as_of="2026-08-03T16:00:00Z"),
            self.policies,
        )
        self.assertEqual(left, right)
        self.assertNotEqual(left, later)
        self.assertEqual(1, left["cohorts"][0]["active_draft_n"])
        self.assertEqual(1, later["cohorts"][0]["stale_draft_n"])

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

    def test_integer_quantiles_are_exact_normalized_decimal_strings(self):
        metric = self.metric_output("test_failures", [0, 2])
        self.assertEqual([0, 2], metric["observed_values"])
        self.assertEqual(
            {"p25": "0.5", "p50": "1", "p75": "1.5"},
            metric["quantiles"],
        )
        self.assertIsNone(linear_rational_quantile([], 1, 2))
        self.assertEqual("7", linear_rational_quantile([7], 1, 2))
        with self.assertRaisesRegex(LearningSnapshotError, "terminating decimal"):
            linear_rational_quantile([0, 1], 1, 3)

    def test_core_has_exact_boundary_and_manifest_fields(self):
        acquired = self.bundle(outcomes=["success"])
        core = build_snapshot_core(acquired, self.policies)
        self.assertEqual({
            "schema_version",
            "analyzer_version",
            "query",
            "lifecycle_health_policy",
            "analysis_policy_set",
            "input_manifest",
            "exclusion_ledger",
            "cohorts",
            "decision_patterns",
            "candidates",
        }, set(core))
        self.assertEqual(1, core["schema_version"])
        self.assertEqual(
            "workflow-learning-analyzer@0.2.0", core["analyzer_version"]
        )
        self.assertEqual([], core["decision_patterns"])
        self.assertEqual([], core["candidates"])
        self.assertEqual({
            "input_manifest_sha256",
            "episodes",
            "invalidations",
            "reference_manifest",
        }, set(core["input_manifest"]))
        self.assertNotIn("adapter", canonicalize(core).decode("utf-8"))
        self.assertNotIn("store_identity", canonicalize(core).decode("utf-8"))

    def test_input_manifest_preserves_validated_reference_identity_bound(self):
        acquired = self.bundle(outcomes=["success"])
        bundle = deepcopy(acquired.semantic_bundle)
        bundle["reference_manifest"][0]["identity"] = "r" * 201
        del bundle["input_manifest_sha256"]
        bundle["input_manifest_sha256"] = hash_canonical(
            _INPUT_MANIFEST_DOMAIN, bundle
        )
        rebuilt = SnapshotInput(
            acquired.adapter,
            acquired.store_identity,
            bundle,
        )

        core = build_snapshot_core(rebuilt, self.policies)

        self.assertEqual(
            "r" * 201,
            core["input_manifest"]["reference_manifest"][0]["identity"],
        )

    def test_generation_unavailable_episodes_are_separate_legacy_collections(self):
        episodes = [
            self._projection(schema_version=1, generation=None),
            self._projection(schema_version=1, generation=None),
        ]
        core = build_snapshot_core(self._snapshot_input(episodes), self.policies)
        self.assertEqual(2, len(core["cohorts"]))
        self.assertEqual(
            ["legacy-generation-unavailable", "legacy-generation-unavailable"],
            [cohort["collection"] for cohort in core["cohorts"]],
        )
        self.assertEqual(
            [1, 1], [cohort["outcome_episode_n"] for cohort in core["cohorts"]]
        )
        for cohort in core["cohorts"]:
            self.assertFalse(cohort["comparative_inference_eligible"])
            self.assertEqual(
                ["generation-unavailable"],
                cohort["comparative_inference_exclusions"],
            )
            self.assertEqual(1, cohort["generation_unavailable_episode_n"])

    def test_heterogeneous_runtime_and_lifecycle_exclusions_are_ledgered(self):
        first = self._projection(runtime_generation="runtime@1")
        second = self._projection(runtime_generation="runtime@2")
        draft = self._projection(status="draft")
        superseded = self._projection(status="superseded")
        core = build_snapshot_core(
            self._snapshot_input(
                [first, second, draft, superseded],
                invalidated=frozenset({superseded["run_id"]}),
            ),
            self.policies,
        )
        cohort = core["cohorts"][0]
        self.assertFalse(cohort["comparative_inference_eligible"])
        self.assertEqual(
            ["heterogeneous-runtime-provenance"],
            cohort["comparative_inference_exclusions"],
        )
        self.assertEqual("descriptive", cohort["evidence_strength"])
        self.assertEqual(
            sorted(
                core["exclusion_ledger"],
                key=lambda row: tuple(
                    row[name].encode("utf-8")
                    for name in ("run_id", "reason", "excluded_from")
                ),
            ),
            core["exclusion_ledger"],
        )
        self.assertIn({
            "run_id": superseded["run_id"],
            "reason": "invalidated",
            "excluded_from": "outcome-analysis",
        }, core["exclusion_ledger"])
        self.assertIn({
            "run_id": superseded["run_id"],
            "reason": "superseded",
            "excluded_from": "outcome-analysis",
        }, core["exclusion_ledger"])
        for episode in (first, second):
            self.assertIn({
                "run_id": episode["run_id"],
                "reason": "heterogeneous-runtime-provenance",
                "excluded_from": "comparative-inference",
            }, core["exclusion_ledger"])

    def test_five_comparable_outcomes_are_recurring_strength(self):
        core = build_snapshot_core(
            self.bundle(outcomes=["success"] * 5), self.policies
        )
        cohort = core["cohorts"][0]
        self.assertTrue(cohort["comparative_inference_eligible"])
        self.assertEqual("recurring", cohort["evidence_strength"])


if __name__ == "__main__":
    unittest.main()
