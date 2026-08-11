from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
POLICY_ROOT = PLUGIN_ROOT / "policies"
SCRIPTS = PLUGIN_ROOT / "scripts"
TESTS = PLUGIN_ROOT / "tests"
FIXTURE = TESTS / "fixtures" / "artifact_migration_vectors.json"
for module_root in (SCRIPTS, TESTS):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import artifact_migration
from artifact_migration import DerivedArtifact, migrate_artifact
from artifact_schema import ArtifactSchemaError, load_artifact_policy_set
from canonical_json import canonicalize, hash_canonical
from policy_artifacts import PolicySet
from workflow_evolution_fixtures import PRIVACY_SENTINEL, load_projection_policy


INSTALLED_HANDLERS = {
    "learning-snapshot-v1",
    "observation-invalidation-v1",
    "workflow-observation-v1",
    "workflow-observation-v2",
}


class ArtifactMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_bytes = FIXTURE.read_bytes()
        cls.fixture = json.loads(cls.fixture_bytes)
        cls.vectors = {
            row["name"]: row for row in cls.fixture["vectors"]
        }

    def setUp(self):
        self.policies = load_artifact_policy_set(POLICY_ROOT)
        self.projection = load_projection_policy()

    @staticmethod
    def source_bytes(row: dict) -> bytes:
        encoded = row["source_bytes_base64"]
        insertion = row.get("source_bytes_base64_tail_insertion", "")
        if insertion:
            encoded = encoded[:-4] + insertion + encoded[-4:]
        return base64.b64decode(encoded, validate=True)

    def migrate_vector(self, name: str) -> DerivedArtifact:
        row = self.vectors[name]
        source = self.source_bytes(row)
        learning_policy_set = None
        if row["source_artifact_type"] == "learning-snapshot":
            source_document = json.loads(source)
            learning_policy_set = PolicySet(
                self.projection,
                source_document["core"]["analysis_policy_set"],
            )
        return migrate_artifact(
            source_bytes=source,
            expected_artifact_type=row["source_artifact_type"],
            policies=self.policies,
            learning_policy_set=learning_policy_set,
            observation_projection_policy=(
                self.projection
                if row["source_artifact_type"] == "workflow-observation"
                else None
            ),
        )

    def test_exports_approved_interfaces_and_exact_four_handlers(self):
        self.assertTrue(isinstance(DerivedArtifact, type))
        self.assertTrue(callable(migrate_artifact))
        self.assertEqual(INSTALLED_HANDLERS, set(artifact_migration._HANDLERS))

    def test_fixture_freezes_synthetic_exact_vectors_and_policy_identity(self):
        self.assertEqual(1, self.fixture["fixture_version"])
        self.assertEqual(
            self.policies.identities()["artifact_migration_registry"],
            self.fixture["migration_registry_identity"],
        )
        self.assertEqual(
            {
                "learning-snapshot-v1",
                "workflow-observation-v1",
                "workflow-observation-v2",
                "observation-invalidation-v1",
            },
            set(self.vectors),
        )
        self.assertNotIn(b"/Users/", self.fixture_bytes)
        self.assertNotIn(b"C:\\\\", self.fixture_bytes)
        for row in self.fixture["vectors"]:
            source = self.source_bytes(row)
            expected = (
                bytes.fromhex(row["canonical_derived_utf8_hex"])
                if "canonical_derived_utf8_hex" in row
                else base64.b64decode(
                    row["canonical_derived_bytes_base64"], validate=True
                )
            )
            self.assertEqual(row["source_sha256"], hashlib.sha256(source).hexdigest())
            self.assertEqual(row["derived_sha256"], hashlib.sha256(expected).hexdigest())

    def test_fixed_vectors_match_exact_canonical_bytes_and_digests(self):
        for name, row in self.vectors.items():
            with self.subTest(name=name):
                derived = self.migrate_vector(name)
                expected = (
                    bytes.fromhex(row["canonical_derived_utf8_hex"])
                    if "canonical_derived_utf8_hex" in row
                    else base64.b64decode(
                        row["canonical_derived_bytes_base64"], validate=True
                    )
                )
                self.assertEqual(expected, derived.canonical_bytes)
                self.assertEqual(row["derived_sha256"], hashlib.sha256(derived.canonical_bytes).hexdigest())
                self.assertEqual(expected, canonicalize(derived.canonical_document))
                document = derived.canonical_document
                self.assertEqual(
                    {"artifact_type", "schema_version", "source", "migration", "target"},
                    set(document),
                )
                self.assertEqual(row["source_sha256"], document["source"]["source_sha256"])
                self.assertEqual(row["source_schema_version"], document["source"]["schema_version"])
                self.assertEqual(row["migration_identity"], document["migration"]["migration_identity"])
                self.assertEqual(
                    self.fixture["migration_registry_identity"]["sha256"],
                    document["migration"]["migration_registry_sha256"],
                )
                self.assertEqual(row["target_contract"], document["target"]["contract"])
                self.assertEqual(row["target_schema_version"], document["target"]["schema_version"])

    def test_invalidation_projection_has_exact_four_machine_fields(self):
        value = self.migrate_vector("observation-invalidation-v1").canonical_document[
            "target"
        ]["value"]
        self.assertEqual(
            {"artifact_type", "schema_version", "run_id", "timestamp"},
            set(value),
        )
        self.assertNotIn("type", value)
        self.assertEqual("obs-20260811-010203-abcdef", value["run_id"])

    def test_legacy_absence_remains_unavailable_and_never_zero(self):
        value = self.migrate_vector("workflow-observation-v1").canonical_document[
            "target"
        ]["value"]
        self.assertEqual(
            {"availability": "unavailable", "value": None},
            value["workflow_generation"],
        )
        for name in (
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cost_amount",
            "test_failures",
            "timeout_count",
        ):
            metric = value["metrics"][name]
            self.assertEqual("unsupported_by_schema", metric["availability"])
            self.assertIsNone(metric["value"])

    def test_run_id_is_preserved_for_all_handlers(self):
        expected = {
            "workflow-observation-v1": "obs-20260802-000000-abcdef",
            "workflow-observation-v2": "obs-20260802-000001-fedcba",
            "observation-invalidation-v1": "obs-20260811-010203-abcdef",
        }
        for name, run_id in expected.items():
            with self.subTest(name=name):
                value = self.migrate_vector(name).canonical_document["target"]["value"]
                self.assertEqual(run_id, value["run_id"])

    def test_learning_snapshot_v1_migration_preserves_only_source_identity(self):
        row = self.vectors["learning-snapshot-v1"]
        source = self.source_bytes(row)
        source_document = json.loads(source)
        derived = self.migrate_vector("learning-snapshot-v1")
        document = derived.canonical_document

        self.assertEqual(
            source_document["snapshot_id"],
            document["source"]["snapshot_id"],
        )
        self.assertNotIn("snapshot_id", document["target"]["value"])
        core = document["target"]["value"]
        self.assertEqual("learning-snapshot-core", core["artifact_type"])
        self.assertEqual(2, core["schema_version"])
        self.assertEqual(0, core["sampled_by_policy_n"])
        self.assertEqual(
            {
                "full_retained_episode_n": 0,
                "sampled_minimal_episode_n": 0,
                "sampling_policy_identities": [],
            },
            core["sampling_summary"],
        )
        self.assertEqual(
            row["source_sha256"], hashlib.sha256(source).hexdigest()
        )

    def test_same_inputs_are_byte_identical_and_canonicalize_once(self):
        row = self.vectors["workflow-observation-v2"]
        source = self.source_bytes(row)
        with mock.patch(
            "artifact_migration.canonicalize",
            wraps=artifact_migration.canonicalize,
        ) as encoder:
            first = migrate_artifact(
                source_bytes=source,
                expected_artifact_type="workflow-observation",
                policies=self.policies,
                observation_projection_policy=self.projection,
            )
            first_bytes = first.canonical_bytes
            self.assertEqual(first_bytes, first.canonical_bytes)
        self.assertEqual(1, encoder.call_count)
        second = migrate_artifact(
            source_bytes=source,
            expected_artifact_type="workflow-observation",
            policies=self.policies,
            observation_projection_policy=self.projection,
        )
        self.assertEqual(first_bytes, second.canonical_bytes)

    def test_source_whitespace_changes_hash_and_derived_bytes_only(self):
        row = self.vectors["workflow-observation-v1"]
        source = base64.b64decode(row["source_bytes_base64"])
        changed = source.replace(
            b'type: "observation"', b'type:  "observation"', 1
        )
        first = self.migrate_vector("workflow-observation-v1")
        second = migrate_artifact(
            source_bytes=changed,
            expected_artifact_type="workflow-observation",
            policies=self.policies,
            observation_projection_policy=self.projection,
        )
        self.assertNotEqual(first.canonical_bytes, second.canonical_bytes)
        self.assertNotEqual(
            first.canonical_document["source"]["source_sha256"],
            second.canonical_document["source"]["source_sha256"],
        )
        self.assertEqual(
            first.canonical_document["target"],
            second.canonical_document["target"],
        )

    def test_clock_environment_git_and_filesystem_are_not_observed(self):
        row = self.vectors["workflow-observation-v2"]
        source = base64.b64decode(row["source_bytes_base64"])
        expected = self.migrate_vector("workflow-observation-v2").canonical_bytes
        failure = AssertionError("migration attempted hidden I/O")
        with (
            mock.patch.dict(os.environ, {"TZ": "Pacific/Kiritimati", "GIT_DIR": "/synthetic"}),
            mock.patch("os.getenv", side_effect=failure),
            mock.patch("time.time", side_effect=failure),
            mock.patch("time.monotonic", side_effect=failure),
            mock.patch("subprocess.run", side_effect=failure),
            mock.patch("builtins.open", side_effect=failure),
            mock.patch("pathlib.Path.open", side_effect=failure),
            mock.patch("pathlib.Path.read_bytes", side_effect=failure),
            mock.patch("pathlib.Path.read_text", side_effect=failure),
        ):
            actual = migrate_artifact(
                source_bytes=source,
                expected_artifact_type="workflow-observation",
                policies=self.policies,
                observation_projection_policy=self.projection,
            ).canonical_bytes
        self.assertEqual(expected, actual)

    def test_migration_uses_central_classifier_and_projection_once(self):
        row = self.vectors["workflow-observation-v2"]
        source = base64.b64decode(row["source_bytes_base64"])
        with (
            mock.patch(
                "artifact_migration.parse_markdown_envelope",
                wraps=artifact_migration.parse_markdown_envelope,
            ) as classifier,
            mock.patch(
                "artifact_migration.canonical_episode_projection",
                wraps=artifact_migration.canonical_episode_projection,
            ) as projector,
        ):
            migrate_artifact(
                source_bytes=source,
                expected_artifact_type="workflow-observation",
                policies=self.policies,
                observation_projection_policy=self.projection,
            )
        classifier.assert_called_once()
        projector.assert_called_once()

    def test_human_privacy_sentinel_is_excluded_from_derived_bytes(self):
        row = self.vectors["workflow-observation-v1"]
        source = base64.b64decode(row["source_bytes_base64"]).replace(
            b"Private evidence", PRIVACY_SENTINEL.encode("utf-8"), 1
        )
        self.assertIn(PRIVACY_SENTINEL.encode("utf-8"), source)
        derived = migrate_artifact(
            source_bytes=source,
            expected_artifact_type="workflow-observation",
            policies=self.policies,
            observation_projection_policy=self.projection,
        )
        self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), derived.canonical_bytes)

    def test_unknown_and_ambiguous_schema_fail_before_handler(self):
        row = self.vectors["workflow-observation-v1"]
        source = base64.b64decode(row["source_bytes_base64"])
        cases = {
            "ambiguous": source.replace(
                b'type: "observation"\n',
                b'type: "observation"\nartifact_type: "workflow-observation"\n',
                1,
            ),
            "unknown": source.replace(
                b'type: "observation"\n',
                b'type: "observation"\nschema_version: 3\n',
                1,
            ),
        }
        for name, invalid in cases.items():
            with self.subTest(name=name), mock.patch(
                "artifact_migration.canonical_episode_projection",
                side_effect=AssertionError("handler ran before classification"),
            ):
                with self.assertRaises(ArtifactSchemaError):
                    migrate_artifact(
                        source_bytes=invalid,
                        expected_artifact_type="workflow-observation",
                        policies=self.policies,
                        observation_projection_policy=self.projection,
                    )

    def test_wrong_expected_artifact_type_fails_before_handler(self):
        row = self.vectors["workflow-observation-v1"]
        source = base64.b64decode(row["source_bytes_base64"])
        with mock.patch(
            "artifact_migration.canonical_episode_projection",
            side_effect=AssertionError("wrong-type source reached handler"),
        ):
            with self.assertRaisesRegex(ArtifactSchemaError, "human type"):
                migrate_artifact(
                    source_bytes=source,
                    expected_artifact_type="observation-invalidation",
                    policies=self.policies,
                )

    def test_source_and_projection_policy_inputs_remain_unchanged(self):
        row = self.vectors["workflow-observation-v2"]
        source = base64.b64decode(row["source_bytes_base64"])
        source_before = bytes(source)
        projection_before = copy.deepcopy(self.projection)
        migrate_artifact(
            source_bytes=source,
            expected_artifact_type="workflow-observation",
            policies=self.policies,
            observation_projection_policy=self.projection,
        )
        self.assertEqual(source_before, source)
        self.assertEqual(projection_before, self.projection)

    def test_derived_artifact_constructor_and_properties_are_defensive(self):
        original = self.migrate_vector("observation-invalidation-v1")
        constructor_input = original.canonical_document
        copied = DerivedArtifact(constructor_input)
        constructor_input["source"]["source_sha256"] = "0" * 64
        returned = copied.canonical_document
        returned["target"]["value"]["run_id"] = "obs-20260811-999999-abcdef"
        self.assertEqual(original.canonical_bytes, copied.canonical_bytes)
        self.assertEqual(
            "obs-20260811-010203-abcdef",
            copied.canonical_document["target"]["value"]["run_id"],
        )
        with self.assertRaises(TypeError):
            copied._canonical_document["target"] = {}
        with self.assertRaises(AttributeError):
            copied.canonical_bytes = b"tampered"

    def test_learning_snapshot_v1_requires_complete_exact_legacy_shape(self):
        rows = self.policies.migration_registry["migrations"]
        learning = next(
            row for row in rows if row["handler"] == "learning-snapshot-v1"
        )
        self.assertEqual("learning-snapshot-core@2", learning["target_contract"])
        self.assertIn("learning-snapshot-v1", artifact_migration._HANDLERS)
        invalid = (
            b'{"artifact_type":"learning-snapshot","authoritative":false}'
        )
        with self.assertRaisesRegex(ValueError, "exact|legacy|fields"):
            migrate_artifact(
                source_bytes=invalid,
                expected_artifact_type="learning-snapshot",
                policies=self.policies,
            )

    def test_learning_snapshot_v1_rejects_rehashed_explicit_schema_label(self):
        row = self.vectors["learning-snapshot-v1"]
        artifact = json.loads(self.source_bytes(row))
        artifact["schema_version"] = 1
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = hashlib.sha256(
            canonicalize(artifact)
        ).hexdigest()

        with self.assertRaisesRegex(ValueError, "legacy|exact|version"):
            migrate_artifact(
                source_bytes=canonicalize(artifact),
                expected_artifact_type="learning-snapshot",
                policies=self.policies,
            )

    def test_learning_snapshot_v1_rejects_rehashed_nested_adversarial_core(self):
        row = self.vectors["learning-snapshot-v1"]
        artifact = json.loads(self.source_bytes(row))
        artifact["core"]["exclusion_ledger"].append({
            "run_id": "not-a-run-id",
            "reason": "draft",
            "excluded_from": "outcome-analysis",
        })
        artifact["snapshot_id"] = hash_canonical(
            b"workflow-observatory:learning-snapshot-core:v1\0",
            artifact["core"],
        )
        artifact.pop("artifact_sha256")
        artifact["artifact_sha256"] = hashlib.sha256(
            canonicalize(artifact)
        ).hexdigest()
        learning_policy_set = PolicySet(
            self.projection,
            artifact["core"]["analysis_policy_set"],
        )

        with self.assertRaisesRegex(
            ValueError, "core|ledger|structure|run_id"
        ):
            migrate_artifact(
                source_bytes=canonicalize(artifact),
                expected_artifact_type="learning-snapshot",
                policies=self.policies,
                learning_policy_set=learning_policy_set,
            )

    def test_learning_snapshot_migration_never_reads_writes_or_publishes(self):
        row = self.vectors["learning-snapshot-v1"]
        source = self.source_bytes(row)
        source_before = bytes(source)
        failure = AssertionError("learning migration attempted hidden I/O")
        with (
            mock.patch("builtins.open", side_effect=failure),
            mock.patch("pathlib.Path.open", side_effect=failure),
            mock.patch("pathlib.Path.read_bytes", side_effect=failure),
            mock.patch("pathlib.Path.read_text", side_effect=failure),
            mock.patch("pathlib.Path.write_bytes", side_effect=failure),
            mock.patch("pathlib.Path.write_text", side_effect=failure),
            mock.patch("os.replace", side_effect=failure),
        ):
            derived = migrate_artifact(
                source_bytes=source,
                expected_artifact_type="learning-snapshot",
                policies=self.policies,
                learning_policy_set=PolicySet(
                    self.projection,
                    json.loads(source)["core"]["analysis_policy_set"],
                ),
            )
        self.assertEqual(source_before, source)
        self.assertEqual(
            row["derived_sha256"],
            hashlib.sha256(derived.canonical_bytes).hexdigest(),
        )

    def test_invalid_argument_types_and_missing_projection_policy_fail_closed(self):
        row = self.vectors["workflow-observation-v1"]
        source = base64.b64decode(row["source_bytes_base64"])
        with self.assertRaisesRegex(ValueError, "source_bytes"):
            migrate_artifact(
                source_bytes=bytearray(source),
                expected_artifact_type="workflow-observation",
                policies=self.policies,
                observation_projection_policy=self.projection,
            )
        with self.assertRaisesRegex(ValueError, "projection policy"):
            migrate_artifact(
                source_bytes=source,
                expected_artifact_type="workflow-observation",
                policies=self.policies,
            )


if __name__ == "__main__":
    unittest.main()
