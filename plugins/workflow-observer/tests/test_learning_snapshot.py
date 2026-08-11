from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
for module_root in (PLUGIN_ROOT / "scripts", PLUGIN_ROOT / "tests"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import canonicalize, hash_canonical
from episode_schema import (
    build_episode_v2,
    parse_v2_supplement,
    render_episode_block,
    synthetic_episode_projection as canonical_episode_projection,
)
from learning_snapshot import (
    LearningSnapshotError,
    build_snapshot_core,
    candidate_id,
    linear_rational_quantile,
)
from policy_artifacts import PolicySet, load_policy_set
from snapshot_input import SNAPSHOT_ANALYZER_FILES, SnapshotInput
from workflow_evolution_fixtures import (
    DECISION,
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
        supplement_decisions: list[dict[str, object]] | None = None,
        metadata_overrides: dict[str, object] | None = None,
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
        metadata.update(metadata_overrides or {})
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
                    json.dumps(supplement_data), self.projection
                )
                episode = build_episode_v2(
                    elapsed_seconds=120,
                    completion_metrics=completion,
                    supplement=supplement,
                )
                body = body.rstrip() + "\n\n" + render_episode_block(episode)

        projected = canonical_episode_projection(metadata, body, self.projection)
        projected["source_sha256"] = hashlib.sha256(
            canonicalize(projected)
        ).hexdigest()
        return projected

    def _policies_with_generation_mapping(
        self,
        mapping: dict[str, str],
    ) -> PolicySet:
        documents = self.policies.documents
        mapping_document = documents["workflow_generation_mapping"]
        mapping_document["mapping"] = dict(mapping)
        identities = self.policies.identities
        identities["workflow_generation_mapping"] = {
            "version": mapping_document["version"],
            "sha256": "sha256:" + hashlib.sha256(
                canonicalize(mapping_document)
            ).hexdigest(),
        }
        return PolicySet(documents, identities)

    def _snapshot_input(
        self,
        episodes: list[dict],
        *,
        invalidated: frozenset[str] = frozenset(),
        as_of: str = "2026-08-02T16:00:00Z",
        policies: PolicySet | None = None,
    ) -> SnapshotInput:
        selected_policies = self.policies if policies is None else policies
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
                    "identity": "[[fixture-task]]",
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

    def _mixed_v1_v2_snapshot_input(
        self,
        v1_absent: int,
        v2_null: int,
        v2_values: list[int],
    ) -> tuple[SnapshotInput, PolicySet]:
        v1_episodes = [
            self._projection(schema_version=1)
            for _ in range(v1_absent)
        ]
        mapping = {
            episode["run_id"]: _GENERATION for episode in v1_episodes
        }
        policies = self._policies_with_generation_mapping(mapping)
        for episode in v1_episodes:
            episode["workflow_generation"] = {
                "availability": "observed",
                "value": mapping[episode["run_id"]],
            }
        episodes = list(v1_episodes)
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
        return self._snapshot_input(episodes, policies=policies), policies

    def metric_partition(self, v1_absent, v2_null, v2_values):
        acquired, policies = self._mixed_v1_v2_snapshot_input(
            v1_absent,
            v2_null,
            v2_values,
        )
        core = build_snapshot_core(
            acquired,
            policies,
        )
        return self._metric(core["cohorts"][0], "test_failures")

    def metric_output(self, name, values):
        episodes = []
        for value in values:
            status = (
                "failed"
                if name == "verification" and value == "fail"
                else "success"
            )
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
                status=status,
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

    def core_with_decisions(self, events_by_run_id):
        episodes = []
        for events in events_by_run_id.values():
            ordered = [
                {**deepcopy(event), "sequence": sequence}
                for sequence, event in enumerate(events, start=1)
            ]
            episodes.append(self._projection(supplement_decisions=ordered))
        return build_snapshot_core(
            self._snapshot_input(episodes), self.policies
        )

    def core_with_recurring_decision_pattern(
        self,
        *,
        outcome_episode_n,
        supporting_episode_n,
        workflow_generation={
            "availability": "observed",
            "value": _GENERATION,
        },
        policies=None,
    ):
        generation = (
            workflow_generation["value"]
            if workflow_generation["availability"] == "observed"
            else None
        )
        episodes = [
            self._projection(
                generation=generation,
                supplement_decisions=[deepcopy(DECISION)] if index < supporting_episode_n else [],
            )
            for index in range(outcome_episode_n)
        ]
        selected_policies = self.policies if policies is None else policies
        return build_snapshot_core(
            self._snapshot_input(episodes, policies=selected_policies),
            selected_policies,
        )

    def lifecycle_candidate(self, as_of):
        core = build_snapshot_core(
            self.bundle(drafts=["stale"], as_of=as_of), self.policies
        )
        return next(
            candidate for candidate in core["candidates"]
            if candidate["candidate_type"] == "stale-drafts"
        )

    def core_with_multiple_candidates(self):
        episode = self._projection(
            status="failed",
            completion_metrics={
                "verification": "fail",
                "defects_found": 1,
                "rework_count": 0,
            },
            supplement_quality={"test_failures": 1, "timeout_count": 1},
        )
        return build_snapshot_core(
            self._snapshot_input([episode]), self.policies
        )

    def core_with_zero_quality_and_timeout_metrics(self):
        episode = self._projection(
            completion_metrics={"defects_found": 0, "rework_count": 0},
            supplement_quality={"test_failures": 0, "timeout_count": 0},
        )
        return build_snapshot_core(
            self._snapshot_input([episode]), self.policies
        )

    def core_with_equal_missingness(self, metrics):
        execution = {"cache_read_tokens": 1}
        execution.update({
            name: None for name in metrics
            if name in {"input_tokens", "output_tokens", "cache_read_tokens"}
        })
        quality = {
            name: None for name in metrics
            if name in {"test_failures", "timeout_count"}
        }
        episode = self._projection(
            supplement_execution=execution,
            supplement_quality=quality,
        )
        return build_snapshot_core(
            self._snapshot_input([episode]), self.policies
        )

    def _policies_with_decision_thresholds(
        self, *, minimum_episode_support, minimum_support_ratio, minimum_outcomes
    ):
        documents = self.policies.documents
        decision = documents["decision_support_policy"]
        decision["decision_min_episode_support"] = minimum_episode_support
        decision["decision_min_support_ratio"] = minimum_support_ratio
        decision["decision_recurring_minimum_outcome_episodes"] = minimum_outcomes
        identities = self.policies.identities
        identities["decision_support_policy"] = {
            "version": decision["version"],
            "sha256": "sha256:" + hashlib.sha256(
                canonicalize(decision)
            ).hexdigest(),
        }
        return PolicySet(documents, identities)

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
        self.assertIsInstance(core["decision_patterns"], list)
        self.assertIsInstance(core["candidates"], list)
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
        bundle["reference_manifest"][0].update({
            "kind": "source",
            "identity": "raw/" + "r" * 197,
        })
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
            "raw/" + "r" * 197,
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

    def test_episode_projection_v2_runtime_provenance_is_json_null(self):
        episode = self._projection(schema_version=2)

        self.assertEqual(
            "episode-projection@2",
            self.projection["episode_projection"]["version"],
        )
        self.assertEqual(2, episode["episode_schema_version"])
        self.assertIsNone(episode["runtime_provenance"])

    def test_null_runtime_adds_no_runtime_exclusion(self):
        core = build_snapshot_core(
            self._snapshot_input([self._projection(), self._projection()]),
            self.policies,
        )

        self.assertEqual(
            [], core["cohorts"][0]["comparative_inference_exclusions"]
        )
        self.assertFalse(any(
            "runtime" in row["reason"] for row in core["exclusion_ledger"]
        ))

    def test_null_runtime_does_not_split_otherwise_equal_cohort(self):
        first = self._projection()
        second = self._projection()

        core = build_snapshot_core(
            self._snapshot_input([first, second]),
            self.policies,
        )

        self.assertIsNone(first["runtime_provenance"])
        self.assertIsNone(second["runtime_provenance"])
        self.assertEqual(1, len(core["cohorts"]))
        self.assertEqual(2, core["cohorts"][0]["outcome_episode_n"])

    def test_runtime_is_not_inferred_from_revision_surface_cli_or_environment(self):
        with (
            mock.patch.dict(
                os.environ,
                {
                    "CODEX_MODEL": "environment-runtime",
                    "OPENAI_MODEL": "other-environment-runtime",
                },
                clear=False,
            ),
            mock.patch.object(
                sys,
                "argv",
                ["workflow-observer", "snapshot", "--model", "cli-runtime"],
            ),
        ):
            episode = self._projection(metadata_overrides={
                "revision": "abcdef0123456789",
                "agent_surface": "codex",
                "environment_fingerprint": "runtime-looking-fingerprint",
                "model": "runtime-looking-extra-field",
            })

        acquired = self._snapshot_input([episode])
        core = build_snapshot_core(acquired, self.policies)
        validated = acquired.semantic_bundle["episodes"][0]

        self.assertEqual("abcdef0123456789", validated["revision"])
        self.assertEqual("codex", validated["agent_surface"])
        self.assertIsNone(validated["runtime_provenance"])
        self.assertEqual(
            [], core["cohorts"][0]["comparative_inference_exclusions"]
        )

    def test_mapped_v1_v2_fixture_preserves_projection_v2_null_runtime_contract(self):
        acquired, policies = self._mixed_v1_v2_snapshot_input(
            v1_absent=1,
            v2_null=1,
            v2_values=[0],
        )
        episodes = acquired.semantic_bundle["episodes"]

        self.assertEqual(
            [1, 2, 2],
            [row["episode_schema_version"] for row in episodes],
        )
        self.assertTrue(all(
            row["runtime_provenance"] is None for row in episodes
        ))
        self.assertTrue(all(
            row["workflow_generation"] == {
                "availability": "observed",
                "value": _GENERATION,
            }
            for row in episodes
        ))

        core = build_snapshot_core(acquired, policies)
        self.assertEqual(1, len(core["cohorts"]))
        self.assertEqual(3, core["cohorts"][0]["outcome_episode_n"])

    def test_lifecycle_exclusions_are_sorted_and_ledgered(self):
        first = self._projection()
        second = self._projection()
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
        self.assertTrue(cohort["comparative_inference_eligible"])
        self.assertEqual([], cohort["comparative_inference_exclusions"])
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

    def test_each_cohort_dimension_splits_independently(self):
        cases = (
            (
                "project",
                {"metadata_overrides": {"project": "project-a"}},
                {"metadata_overrides": {"project": "project-b"}},
            ),
            (
                "workspace",
                {"metadata_overrides": {"workspace": "workspace-a"}},
                {"metadata_overrides": {"workspace": "workspace-b"}},
            ),
            (
                "workspace_id",
                {"metadata_overrides": {"workspace_id": "aaaaaaaaaaaa"}},
                {"metadata_overrides": {"workspace_id": "bbbbbbbbbbbb"}},
            ),
            (
                "task_type",
                {"metadata_overrides": {
                    "task_type": "compile",
                    "workflow_variant": "compile-with-review",
                }},
                {"metadata_overrides": {
                    "task_type": "inbox-processing",
                    "workflow_variant": "compile-with-review",
                }},
            ),
            (
                "workflow_variant",
                {"metadata_overrides": {
                    "workflow_variant": "maintenance-basic",
                }},
                {"metadata_overrides": {
                    "workflow_variant": "implementation-with-review",
                }},
            ),
            (
                "workflow_generation",
                {"generation": "implementation-with-review@2"},
                {"generation": "implementation-with-review@3"},
            ),
        )
        for dimension, left_options, right_options in cases:
            with self.subTest(dimension=dimension):
                self._sequence = 0
                core = build_snapshot_core(
                    self._snapshot_input([
                        self._projection(**left_options),
                        self._projection(**right_options),
                    ]),
                    self.policies,
                )
                self.assertEqual(2, len(core["cohorts"]))
                self.assertEqual(
                    [1, 1],
                    [row["outcome_episode_n"] for row in core["cohorts"]],
                )

    def test_invalidated_final_episode_is_excluded_from_outcomes_and_metrics(self):
        invalidated = self._projection(status="success")
        retained = self._projection(status="success")
        core = build_snapshot_core(
            self._snapshot_input(
                [invalidated, retained],
                invalidated=frozenset({invalidated["run_id"]}),
            ),
            self.policies,
        )
        cohort = core["cohorts"][0]
        self.assertEqual(1, cohort["outcome_episode_n"])
        self.assertEqual(
            {"failed": 0, "partial": 0, "rolled-back": 0, "success": 1},
            cohort["outcome_counts"],
        )
        self.assertEqual(
            1,
            self._metric(cohort, "elapsed_seconds")["missingness"]
            ["eligible_episode_n"],
        )

    def test_returned_core_does_not_alias_snapshot_or_policy_inputs(self):
        acquired = self.bundle(outcomes=["success"])
        expected = build_snapshot_core(acquired, self.policies)
        mutated = build_snapshot_core(acquired, self.policies)

        mutated["query"]["interval"]["requested_dates"]["since"] = "1999-01-01"
        mutated["analysis_policy_set"].clear()
        mutated["input_manifest"]["reference_manifest"].clear()
        mutated["cohorts"][0]["metrics"][0]["missingness"]["observed_n"] = 99

        self.assertEqual(expected, build_snapshot_core(acquired, self.policies))

    def test_exclusion_ledger_has_one_row_per_exact_reason_scope_pair(self):
        superseded = self._projection(status="superseded")
        core = build_snapshot_core(
            self._snapshot_input(
                [superseded],
                invalidated=frozenset({superseded["run_id"]}),
            ),
            self.policies,
        )
        expected = [
            {
                "run_id": superseded["run_id"],
                "reason": "invalidated",
                "excluded_from": "outcome-analysis",
            },
            {
                "run_id": superseded["run_id"],
                "reason": "superseded",
                "excluded_from": "outcome-analysis",
            },
        ]
        self.assertEqual(expected, core["exclusion_ledger"])
        keys = [
            (row["run_id"], row["reason"], row["excluded_from"])
            for row in core["exclusion_ledger"]
        ]
        self.assertEqual(len(keys), len(set(keys)))

    def test_five_comparable_outcomes_are_recurring_strength(self):
        core = build_snapshot_core(
            self.bundle(outcomes=["success"] * 5), self.policies
        )
        cohort = core["cohorts"][0]
        self.assertTrue(cohort["comparative_inference_eligible"])
        self.assertEqual("recurring", cohort["evidence_strength"])

    def test_one_episode_cannot_dominate_decision_recurrence(self):
        core = self.core_with_decisions({
            "a": [DECISION] * 10,
            "b": [],
            "c": [],
            "d": [],
            "e": [],
        })
        pattern = next(
            row for row in core["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        )
        self.assertEqual({
            "cohort",
            "pattern_kind",
            "pattern",
            "event_count",
            "episode_count_with_event",
            "eligible_episode_n",
            "support_fraction",
            "evidence_strength",
        }, set(pattern))
        self.assertEqual(10, pattern["event_count"])
        self.assertEqual(1, pattern["episode_count_with_event"])
        self.assertEqual(5, pattern["eligible_episode_n"])
        self.assertEqual(
            {"numerator": 1, "denominator": 5},
            pattern["support_fraction"],
        )
        self.assertEqual("descriptive", pattern["evidence_strength"])

    def test_candidate_ids_bind_policy_and_staleness_as_of(self):
        first = self.lifecycle_candidate(as_of="2026-08-02T16:00:00Z")
        second = self.lifecycle_candidate(as_of="2026-08-03T16:00:00Z")
        self.assertNotEqual(first["candidate_id"], second["candidate_id"])
        self.assertIn("2026-08-02T16:00:00Z", json.dumps(first["evidence"]))
        self.assertIn(
            "lifecycle_health_policy", first["policy_identities"]
        )

    def test_candidate_id_hashes_exact_evidence_before_id(self):
        candidate = self.lifecycle_candidate(as_of="2026-08-02T16:00:00Z")
        evidence = {
            key: deepcopy(value)
            for key, value in candidate.items()
            if key != "candidate_id"
        }
        self.assertEqual(candidate_id(evidence), candidate["candidate_id"])
        self.assertEqual(
            hash_canonical(
                b"workflow-observatory:learning-candidate:v1\0", evidence
            ),
            candidate["candidate_id"],
        )

    def test_candidates_are_unranked_and_byte_sorted(self):
        candidates = self.core_with_multiple_candidates()["candidates"]
        self.assertGreater(len(candidates), 1)
        self.assertEqual(
            sorted(
                candidates,
                key=lambda item: item["candidate_id"].encode("ascii"),
            ),
            candidates,
        )
        for candidate in candidates:
            self.assertNotIn("priority", candidate)
            self.assertNotIn("confidence", candidate)
            self.assertNotIn("actionability", candidate)
            self.assertNotIn("narrative", candidate)
            self.assertNotIn("proposal", candidate)

    def test_candidate_has_exact_nested_evidence_shapes(self):
        candidate = self.lifecycle_candidate(as_of="2026-08-02T16:00:00Z")
        self.assertEqual({
            "candidate_type",
            "class",
            "cohort",
            "source",
            "policy_identities",
            "denominators",
            "evidence",
            "evidence_strength",
            "candidate_id",
        }, set(candidate))
        self.assertEqual({
            "collection",
            "legacy_collection_id",
            "project",
            "workspace",
            "workspace_id",
            "task_type",
            "workflow_variant",
            "workflow_generation",
        }, set(candidate["cohort"]))
        self.assertEqual(
            {"kind", "identity", "semantics_id"},
            set(candidate["source"]),
        )
        self.assertEqual({
            "eligible_episode_n",
            "outcome_episode_n",
            "supporting_episode_n",
        }, set(candidate["denominators"]))
        self.assertEqual({
            "counts",
            "missingness",
            "observed_values",
            "category_counts",
            "quantiles",
            "pattern",
        }, set(candidate["evidence"]))
        self.assertEqual(
            {"counts"},
            {
                name for name, value in candidate["evidence"].items()
                if value is not None
            },
        )
        stale_rule = next(
            rule
            for rule in self.projection["candidate_emission_policy"]["rules"]
            if rule["candidate_type"] == "stale-drafts"
        )
        self.assertEqual(
            set(stale_rule["policy_identity_keys"]),
            set(candidate["policy_identities"]),
        )

    def test_decision_sequences_are_only_single_events_and_adjacent_pairs(self):
        first = {**DECISION, "reason_code": "complexity-threshold"}
        second = {**DECISION, "sequence": 2, "reason_code": "dependency"}
        third = {**DECISION, "sequence": 3, "reason_code": "timeout"}
        core = self.core_with_decisions({
            "a": [first, second, third],
            "b": [],
            "c": [],
            "d": [],
            "e": [],
        })
        patterns = {
            (
                row["pattern_kind"],
                tuple(item["reason_code"] for item in row["pattern"]),
            )
            for row in core["decision_patterns"]
        }
        self.assertIn(("single-event", ("complexity-threshold",)), patterns)
        self.assertIn(
            (
                "contiguous-adjacent-pair",
                ("complexity-threshold", "dependency"),
            ),
            patterns,
        )
        self.assertIn(
            ("contiguous-adjacent-pair", ("dependency", "timeout")),
            patterns,
        )
        self.assertNotIn(
            (
                "contiguous-adjacent-pair",
                ("complexity-threshold", "timeout"),
            ),
            patterns,
        )
        self.assertFalse(any(len(pattern) > 2 for _kind, pattern in patterns))
        self.assertEqual(
            sorted(
                core["decision_patterns"],
                key=lambda row: canonicalize([
                    row["cohort"], row["pattern_kind"], row["pattern"]
                ]),
            ),
            core["decision_patterns"],
        )

    def test_decision_keys_exclude_summary_and_sequence(self):
        second = {
            **DECISION,
            "sequence": 2,
            "summary": "Different narrative with the same bounded event key",
        }
        core = self.core_with_decisions({
            "a": [DECISION, second], "b": [], "c": [], "d": [], "e": [],
        })
        singles = [
            row for row in core["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        ]
        self.assertEqual(1, len(singles))
        self.assertEqual(2, singles[0]["event_count"])
        self.assertEqual({
            "phase", "actor_role", "decision_type", "reason_code", "result",
        }, set(singles[0]["pattern"][0]))

    def test_zero_only_adverse_metrics_emit_no_quality_or_timeout_candidate(self):
        core = self.core_with_zero_quality_and_timeout_metrics()
        types = {candidate["candidate_type"] for candidate in core["candidates"]}
        self.assertTrue({
            "defect-observed",
            "rework-observed",
            "test-failure-observed",
            "timeout-observed",
        }.isdisjoint(types))

    def test_unavailable_generation_decision_pattern_remains_descriptive(self):
        core = self.core_with_recurring_decision_pattern(
            outcome_episode_n=5,
            supporting_episode_n=3,
            workflow_generation={"availability": "unavailable", "value": None},
        )
        self.assertTrue(core["decision_patterns"])
        self.assertTrue(all(
            row["evidence_strength"] == "descriptive"
            for row in core["decision_patterns"]
        ))
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

    def test_decision_thresholds_come_from_bound_policy(self):
        policies = self._policies_with_decision_thresholds(
            minimum_episode_support=2,
            minimum_support_ratio="0.41",
            minimum_outcomes=5,
        )
        core = self.core_with_recurring_decision_pattern(
            outcome_episode_n=5,
            supporting_episode_n=2,
            policies=policies,
        )
        self.assertEqual(
            {"numerator": 2, "denominator": 5},
            core["decision_patterns"][0]["support_fraction"],
        )
        self.assertEqual(
            "descriptive", core["decision_patterns"][0]["evidence_strength"]
        )

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

    def test_candidate_types_are_closed_to_the_eighteen_policy_rules(self):
        core = self.core_with_multiple_candidates()
        policy_types = {
            rule["candidate_type"]
            for rule in self.projection["candidate_emission_policy"]["rules"]
        }
        self.assertEqual(18, len(policy_types))
        self.assertTrue(all(
            candidate["candidate_type"] in policy_types
            for candidate in core["candidates"]
        ))
        self.assertFalse(any(
            "runtime" in candidate["candidate_type"]
            or "runtime" in candidate["source"]["identity"]
            for candidate in core["candidates"]
        ))

    def test_legacy_generation_candidates_bind_distinct_collection_identity(self):
        episodes = [
            self._projection(schema_version=1, generation=None),
            self._projection(schema_version=1, generation=None),
        ]
        core = build_snapshot_core(
            self._snapshot_input(episodes), self.policies
        )
        candidates = [
            candidate for candidate in core["candidates"]
            if candidate["candidate_type"] == "generation-unavailable"
        ]
        self.assertEqual(2, len(candidates))
        self.assertEqual(
            {cohort["legacy_collection_id"] for cohort in core["cohorts"]},
            {
                candidate["cohort"]["legacy_collection_id"]
                for candidate in candidates
            },
        )
        self.assertTrue(all(
            candidate["cohort"]["collection"]
                == "legacy-generation-unavailable"
            for candidate in candidates
        ))
        self.assertEqual(2, len({
            candidate["candidate_id"] for candidate in candidates
        }))

    def test_global_decision_patterns_and_candidates_bind_cohort_identity(self):
        episodes = []
        for project in ("project-a", "project-b"):
            episodes.extend(
                self._projection(
                    metadata_overrides={"project": project},
                    supplement_decisions=[deepcopy(DECISION)] if index < 3 else [],
                )
                for index in range(5)
            )
        core = build_snapshot_core(
            self._snapshot_input(episodes), self.policies
        )
        patterns = [
            row for row in core["decision_patterns"]
            if row["pattern_kind"] == "single-event"
        ]
        self.assertEqual(2, len(patterns))
        self.assertEqual(
            {"project-a", "project-b"},
            {row["cohort"]["project"] for row in patterns},
        )
        self.assertEqual(2, len({canonicalize(row) for row in patterns}))
        self.assertEqual(
            sorted(
                patterns,
                key=lambda row: canonicalize([
                    row["cohort"], row["pattern_kind"], row["pattern"]
                ]),
            ),
            patterns,
        )
        candidates = [
            candidate for candidate in core["candidates"]
            if candidate["candidate_type"] == "decision-single-event"
        ]
        self.assertEqual(2, len(candidates))
        self.assertEqual(
            {canonicalize(row["cohort"]) for row in patterns},
            {canonicalize(row["cohort"]) for row in candidates},
        )
        self.assertEqual(2, len({
            candidate["candidate_id"] for candidate in candidates
        }))

    def test_snapshot_fails_closed_if_candidate_ids_collide(self):
        with mock.patch(
            "learning_snapshot.candidate_id", return_value="0" * 64
        ):
            with self.assertRaisesRegex(
                LearningSnapshotError, "candidate IDs are duplicated"
            ):
                self.core_with_multiple_candidates()


if __name__ == "__main__":
    unittest.main()
