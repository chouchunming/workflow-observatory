import copy
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PLUGIN_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

from canonical_json import canonicalize
from policy_artifacts import (
    PolicyError,
    build_code_manifest,
    effective_boundary_applies,
    load_policy_set,
    read_regular_file_evidence,
    validate_policy_documents,
    validate_effective_boundary,
)


COMMON_METRICS = (
    "elapsed_seconds",
    "verification",
    "review_rounds",
    "defects_found",
    "rework_count",
)
V2_METRICS = (
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cost_amount",
    "test_failures",
    "timeout_count",
)

CANDIDATE_RULES = [
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
    {"candidate_type":"verification-non-pass","class":"quality","source_kind":"metric","source":"verification","predicate":"non-pass-count-positive","minimum_denominator":"one-observed-episode@1","cardinality":"one-per-cohort-metric","evidence_fields":["category_counts","missingness"],"policy_identity_keys":["candidate_emission_policy","canonical_projection_contract","metric_semantics_registry"]},
]


def policy_documents():
    metric_capabilities_v1 = {
        name: name in COMMON_METRICS for name in COMMON_METRICS + V2_METRICS
    }
    metric_capabilities_v2 = {
        name: True for name in COMMON_METRICS + V2_METRICS
    }
    metrics = {
        "elapsed_seconds": {"semantics_id":"wall-clock-elapsed@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"elapsed-time-distribution"},
        "verification": {"semantics_id":"verification-result@1","value_type":"enum","aggregation":"category-counts","candidate_type":"verification-non-pass"},
        "review_rounds": {"semantics_id":"formal-review-cycle@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"review-round-distribution"},
        "defects_found": {"semantics_id":"confirmed-defect@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"defect-observed"},
        "rework_count": {"semantics_id":"confirmed-rework@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"rework-observed"},
        "input_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"input-token-distribution"},
        "output_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"output-token-distribution"},
        "cache_read_tokens": {"semantics_id":"measured-token-count@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"cache-read-token-distribution"},
        "cost_amount": {"semantics_id":"measured-cost@1","value_type":"normalized-decimal-string","aggregation":"missingness-only","candidate_type":None},
        "test_failures": {"semantics_id":"confirmed-test-failure@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"test-failure-observed"},
        "timeout_count": {"semantics_id":"confirmed-timeout@1","value_type":"nonnegative-integer","aggregation":"integer-quantiles","candidate_type":"timeout-observed"},
    }
    return {
        "episode_projection": {
            "schema_version": 1,
            "version": "episode-projection@2",
            "max_decisions": 12,
            "max_scalar_codepoints": 200,
            "enumerations": {
                "measurement_source": ["agent-reported", "tool-derived", "unavailable"],
                "phase": ["implementation", "planning", "recovery", "review", "verification"],
                "actor_role": ["coordinator", "implementer", "planner", "reviewer", "tester"],
                "decision_type": ["change-scope", "reject", "resume", "retry", "rollback", "split-task", "stop"],
                "reason_code": ["api-design", "complexity-threshold", "dependency", "integrity-risk", "test-failure", "timeout", "user-direction", "verification-failure"],
                "result": ["inconclusive", "rejected", "superseded", "supported"],
            },
            "schema_capabilities": {
                "1": {"metrics": metric_capabilities_v1, "decisions": False},
                "2": {"metrics": metric_capabilities_v2, "decisions": True},
            },
        },
        "producer_capabilities": {
            "schema_version": 1,
            "version": "producer-capabilities@1",
            "entries": [],
        },
        "workflow_generation_mapping": {
            "schema_version": 1,
            "version": "workflow-generation-mapping@1",
            "mapping": {},
        },
        "metric_semantics": {
            "schema_version": 1,
            "version": "metric-semantics@1",
            "metrics": metrics,
            "not_applicable_rules": [],
        },
        "quantile_policy": {
            "schema_version": 1,
            "version": "linear-rational-quantile@1",
            "quantiles": ["0.25", "0.50", "0.75"],
        },
        "decision_support_policy": {
            "schema_version": 1,
            "version": "decision-pattern-support@1",
            "pattern_kinds": ["single-event", "contiguous-adjacent-pair"],
            "event_key_fields": ["phase", "actor_role", "decision_type", "reason_code", "result"],
            "decision_min_episode_support": 3,
            "decision_min_support_ratio": "0.40",
            "decision_recurring_minimum_outcome_episodes": 5,
        },
        "lifecycle_health_policy": {
            "schema_version": 1,
            "version": "draft-staleness@1",
            "draft_stale_after_seconds": 86400,
        },
        "candidate_emission_policy": {
            "schema_version": 1,
            "version": "candidate-emission@1",
            "candidate_classes": ["decision-pattern", "efficiency", "lifecycle-health", "outcome-reliability", "quality"],
            "candidate_order": "candidate-id-ascending-byte-order",
            "candidate_ranking": "none",
            "rules": copy.deepcopy(CANDIDATE_RULES),
        },
    }


class PolicyArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "a.py").write_text("a = 1\n", encoding="utf-8")
        (self.root / "b.py").write_text("b = 2\n", encoding="utf-8")
        self.policy_root = self.root / "policies"
        self.policy_root.mkdir()
        self.documents = policy_documents()
        self.write_policy_documents(self.documents)

    def write_policy_documents(self, documents):
        for name, document in documents.items():
            (self.policy_root / f"{name}.json").write_text(
                json.dumps(document, ensure_ascii=False, separators=(",", ":"))
                + "\n",
                encoding="utf-8",
            )

    def _semantic_call(self, label, operation):
        try:
            return operation()
        except NotImplementedError:
            self.fail(f"missing {label} semantics")

    def load_policy_set(self):
        return self._semantic_call(
            "policy loading",
            lambda: load_policy_set(
                self.policy_root,
                analyzer_files=["a.py"],
                canonicalizer_files=["b.py"],
            ),
        )

    def test_public_structural_validator_closes_document_envelopes(self):
        validate_policy_documents(self.documents)
        mutations = {
            "missing family": lambda documents: documents.pop("quantile_policy"),
            "extra family": lambda documents: documents.__setitem__(
                "future_policy", {}
            ),
            "extra key": lambda documents: documents["quantile_policy"]
                .__setitem__("future", True),
            "schema version": lambda documents: documents["quantile_policy"]
                .__setitem__("schema_version", 2),
            "version label": lambda documents: documents["quantile_policy"]
                .__setitem__("version", "linear-rational-quantile@2"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                documents = policy_documents()
                mutate(documents)
                with self.assertRaises(PolicyError):
                    validate_policy_documents(documents)

    def test_public_structural_validator_allows_only_reviewed_generation_mapping(self):
        documents = policy_documents()
        documents["workflow_generation_mapping"]["mapping"] = {
            "obs-20260802-000000-abcdef": "implementation-with-review@1"
        }
        with self.assertRaisesRegex(PolicyError, "not defined"):
            validate_policy_documents(documents)
        validate_policy_documents(
            documents, allow_reviewed_generation_mapping=True
        )

        invalid_mappings = (
            {"legacy": "implementation-with-review@1"},
            {"obs-20260802-000000-abcdef": "Unknown Generation"},
            [],
        )
        for mapping in invalid_mappings:
            with self.subTest(mapping=mapping):
                malformed = policy_documents()
                malformed["workflow_generation_mapping"]["mapping"] = mapping
                with self.assertRaises(PolicyError):
                    validate_policy_documents(
                        malformed, allow_reviewed_generation_mapping=True
                    )

    def test_effective_boundary_is_exactly_one_variant(self):
        actual = self._semantic_call(
            "effective boundary",
            lambda: validate_effective_boundary({
                "type": "started_at",
                "from": "2026-08-03T00:00:00Z",
            }),
        )
        self.assertEqual(
            {"type": "started_at", "from": "2026-08-03T00:00:00Z"},
            actual,
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

    def test_started_at_boundary_compares_parsed_utc_instants(self):
        boundary = self._semantic_call(
            "started-at boundary",
            lambda: validate_effective_boundary({
                "type": "started_at",
                "from": "2026-08-03T00:00:00Z",
            }),
        )
        self.assertFalse(effective_boundary_applies(
            boundary,
            started_at="2026-08-02T23:59:59Z",
            producer_generation=None,
        ))
        self.assertTrue(effective_boundary_applies(
            boundary,
            started_at="2026-08-03T00:00:00Z",
            producer_generation=None,
        ))

    def test_producer_generation_boundary_is_valid_and_uses_exact_identity(self):
        boundary = self._semantic_call(
            "producer-generation boundary",
            lambda: validate_effective_boundary({
                "type": "producer_generation",
                "from": "producer@3",
            }),
        )
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
        left = self._semantic_call(
            "code manifest",
            lambda: build_code_manifest(self.root, ["b.py", "a.py"]),
        )
        right = build_code_manifest(self.root, ["a.py", "b.py"])
        self.assertEqual(left, right)
        os.chmod(self.root / "a.py", 0o755)
        self.assertNotEqual(left, build_code_manifest(self.root, ["a.py", "b.py"]))

    def test_code_manifest_binds_exact_file_bytes(self):
        before = self._semantic_call(
            "manifest byte binding",
            lambda: build_code_manifest(self.root, ["a.py"]),
        )
        (self.root / "a.py").write_text("changed\n", encoding="utf-8")
        after = build_code_manifest(self.root, ["a.py"])
        self.assertNotEqual(before, after)

    def test_code_manifest_rejects_unsafe_members(self):
        unsafe = (
            "", ".", "/absolute.py", "../escape.py", "a/../b.py",
            "a\\b.py", "C:/drive.py", "//server/share.py", "nul\0name.py",
        )
        for member in unsafe:
            with self.subTest(member=member):
                try:
                    build_code_manifest(self.root, [member])
                except NotImplementedError:
                    self.fail("missing unsafe artifact-path semantics")
                except PolicyError:
                    continue
                self.fail("unsafe artifact path was accepted")

    def test_code_manifest_rejects_duplicate_members(self):
        with self.assertRaisesRegex(PolicyError, "duplicated"):
            self._semantic_call(
                "duplicate manifest-member",
                lambda: build_code_manifest(self.root, ["a.py", "a.py"]),
            )

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

        try:
            with mock.patch("policy_artifacts.os.open", side_effect=swap_then_open):
                with self.assertRaisesRegex(PolicyError, "changed during read"):
                    build_code_manifest(self.root, ["a.py"])
        except NotImplementedError:
            self.fail("missing final-file swap-race semantics")

    def test_regular_file_evidence_rejects_symlinks_and_oversize(self):
        (self.root / "link.py").symlink_to(self.root / "a.py")
        try:
            with self.assertRaises(PolicyError):
                read_regular_file_evidence(self.root, "link.py", max_bytes=1024)
            with self.assertRaisesRegex(PolicyError, "maximum"):
                read_regular_file_evidence(self.root, "a.py", max_bytes=1)
        except NotImplementedError:
            self.fail("missing regular-file evidence semantics")

    def test_regular_file_evidence_returns_descriptor_bound_metadata(self):
        evidence = self._semantic_call(
            "regular-file evidence",
            lambda: read_regular_file_evidence(self.root, "a.py", max_bytes=1024),
        )
        expected = (self.root / "a.py").stat()
        self.assertEqual(b"a = 1\n", evidence.content)
        self.assertEqual(hashlib.sha256(evidence.content).hexdigest(), evidence.sha256)
        self.assertEqual(expected.st_dev, evidence.device)
        self.assertEqual(expected.st_ino, evidence.inode)
        self.assertEqual(bool(expected.st_mode & 0o111), evidence.executable)

    def test_duplicate_key_rejected_in_policy(self):
        (self.policy_root / "quantile_policy.json").write_text(
            '{"schema_version":1,"schema_version":1}',
            encoding="utf-8",
        )
        try:
            with self.assertRaisesRegex(PolicyError, "duplicate JSON key"):
                load_policy_set(
                    self.policy_root,
                    analyzer_files=["a.py"],
                    canonicalizer_files=["b.py"],
                )
        except NotImplementedError:
            self.fail("missing strict duplicate-key policy ingress")

    def test_metric_policy_rejects_wrong_value_type_shape(self):
        document = self.documents["metric_semantics"]
        document["metrics"]["verification"]["value_type"] = ["enum"]
        (self.policy_root / "metric_semantics.json").write_text(
            json.dumps(document, separators=(",", ":")),
            encoding="utf-8",
        )
        try:
            with self.assertRaisesRegex(PolicyError, "metric_semantics"):
                load_policy_set(
                    self.policy_root,
                    analyzer_files=["a.py"],
                    canonicalizer_files=["b.py"],
                )
        except NotImplementedError:
            self.fail("missing metric value-type schema semantics")

    def test_candidate_policy_rejects_non_closed_rule(self):
        document = self.documents["candidate_emission_policy"]
        document["rules"][0]["extra"] = "not-closed"
        (self.policy_root / "candidate_emission_policy.json").write_text(
            json.dumps(document, separators=(",", ":")),
            encoding="utf-8",
        )
        try:
            with self.assertRaisesRegex(PolicyError, "candidate_emission_policy"):
                load_policy_set(
                    self.policy_root,
                    analyzer_files=["a.py"],
                    canonicalizer_files=["b.py"],
                )
        except NotImplementedError:
            self.fail("missing closed candidate-rule semantics")

    def test_metric_and_candidate_policy_schemas_are_closed(self):
        policies = self.load_policy_set()
        metrics = policies.documents["metric_semantics"]["metrics"]
        self.assertEqual("category-counts", metrics["verification"]["aggregation"])
        self.assertEqual("missingness-only", metrics["cost_amount"]["aggregation"])
        self.assertIsNone(metrics["cost_amount"]["candidate_type"])
        self.assertEqual([], policies.documents["metric_semantics"]["not_applicable_rules"])
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

    def test_policy_identities_bind_jcs_documents_and_code_manifests(self):
        policies = self.load_policy_set()
        identities = policies.core_identity()
        self.assertEqual(
            (
                "analyzer_artifact",
                "candidate_emission_policy",
                "canonical_projection_contract",
                "canonicalizer_artifact",
                "decision_support_policy",
                "lifecycle_health_policy",
                "metric_semantics_registry",
                "producer_capability_registry",
                "quantile_policy",
                "workflow_generation_mapping",
            ),
            tuple(identities),
        )
        expected_documents = {
            "canonical_projection_contract": "episode_projection",
            "producer_capability_registry": "producer_capabilities",
            "workflow_generation_mapping": "workflow_generation_mapping",
            "metric_semantics_registry": "metric_semantics",
            "quantile_policy": "quantile_policy",
            "decision_support_policy": "decision_support_policy",
            "lifecycle_health_policy": "lifecycle_health_policy",
            "candidate_emission_policy": "candidate_emission_policy",
        }
        for identity_name, document_name in expected_documents.items():
            with self.subTest(identity_name=identity_name):
                document = policies.documents[document_name]
                self.assertEqual(
                    {
                        "version": document["version"],
                        "sha256": "sha256:" + hashlib.sha256(
                            canonicalize(document)
                        ).hexdigest(),
                    },
                    identities[identity_name],
                )
        self.assertEqual("workflow-learning-analyzer@0.2.0", identities["analyzer_artifact"]["version"])
        self.assertEqual("rfc8785-jcs@1", identities["canonicalizer_artifact"]["version"])
        for row in identities.values():
            self.assertEqual({"version", "sha256"}, set(row))
            self.assertRegex(row["sha256"], r"^sha256:[0-9a-f]{64}$")

    def test_policy_set_documents_are_defensive_copies(self):
        policies = self.load_policy_set()
        documents = policies.documents
        documents["lifecycle_health_policy"]["draft_stale_after_seconds"] = 1
        self.assertEqual(
            86400,
            policies.documents["lifecycle_health_policy"]
            ["draft_stale_after_seconds"],
        )

    def test_policy_set_identities_are_defensive_copies(self):
        policies = self.load_policy_set()
        before = policies.core_identity()
        identities = policies.identities
        identities["analyzer_artifact"]["version"] = "tampered"
        self.assertEqual(before, policies.core_identity())
        self.assertEqual(
            "workflow-learning-analyzer@0.2.0",
            policies.identities["analyzer_artifact"]["version"],
        )

    def test_policy_set_internal_documents_are_deeply_immutable(self):
        policies = self.load_policy_set()
        with self.assertRaises(TypeError):
            policies._documents["lifecycle_health_policy"] \
                ["draft_stale_after_seconds"] = 1
        self.assertEqual(
            86400,
            policies.documents["lifecycle_health_policy"]
            ["draft_stale_after_seconds"],
        )

    def test_policy_set_internal_identities_are_deeply_immutable(self):
        policies = self.load_policy_set()
        before = policies.core_identity()
        with self.assertRaises(TypeError):
            policies._identities["analyzer_artifact"]["version"] = "tampered"
        self.assertEqual(before, policies.core_identity())

    def test_same_version_policy_mutations_are_rejected_for_all_families(self):
        def mutate_projection(documents):
            documents["episode_projection"]["max_decisions"] = 13

        def mutate_producer(documents):
            documents["producer_capabilities"]["entries"] = [
                {"producer": "future-producer"}
            ]

        def mutate_mapping(documents):
            documents["workflow_generation_mapping"]["mapping"] = {
                "legacy": "generation@1"
            }

        def mutate_metrics(documents):
            documents["metric_semantics"]["metrics"]["elapsed_seconds"] \
                ["semantics_id"] = "different-elapsed@1"

        def mutate_quantiles(documents):
            documents["quantile_policy"]["quantiles"] = [
                "0.20", "0.50", "0.75"
            ]

        def mutate_decision(documents):
            documents["decision_support_policy"] \
                ["decision_min_episode_support"] = 4

        def mutate_lifecycle(documents):
            documents["lifecycle_health_policy"] \
                ["draft_stale_after_seconds"] = 172800

        def mutate_candidate(documents):
            documents["candidate_emission_policy"]["rules"][0] \
                ["predicate"] = "different-predicate"

        mutations = {
            "episode_projection": mutate_projection,
            "producer_capabilities": mutate_producer,
            "workflow_generation_mapping": mutate_mapping,
            "metric_semantics": mutate_metrics,
            "quantile_policy": mutate_quantiles,
            "decision_support_policy": mutate_decision,
            "lifecycle_health_policy": mutate_lifecycle,
            "candidate_emission_policy": mutate_candidate,
        }
        for name, mutate in mutations.items():
            with self.subTest(policy=name):
                documents = policy_documents()
                mutate(documents)
                self.write_policy_documents(documents)
                with self.assertRaises(PolicyError):
                    self.load_policy_set()

    def test_schema_version_requires_exact_json_integer_one(self):
        for schema_version in (True, False, 1.0, "1", None):
            with self.subTest(schema_version=schema_version):
                documents = policy_documents()
                documents["lifecycle_health_policy"] \
                    ["schema_version"] = schema_version
                self.write_policy_documents(documents)
                with self.assertRaises(PolicyError):
                    self.load_policy_set()

    def test_policy_set_is_frozen_and_core_identity_returns_copies(self):
        policies = self.load_policy_set()
        with self.assertRaises((AttributeError, TypeError)):
            policies.identities = {}
        first = policies.core_identity()
        first["analyzer_artifact"]["version"] = "tampered"
        self.assertEqual(
            "workflow-learning-analyzer@0.2.0",
            policies.core_identity()["analyzer_artifact"]["version"],
        )


if __name__ == "__main__":
    unittest.main()
