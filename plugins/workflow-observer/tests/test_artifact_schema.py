import copy
import hashlib
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import MappingProxyType
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
POLICY_ROOT = PLUGIN_ROOT / "policies"
SCRIPTS = PLUGIN_ROOT / "scripts"
TESTS = PLUGIN_ROOT / "tests"
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(TESTS))

from artifact_schema import (
    ArtifactPolicySet,
    ArtifactSchemaError,
    classify_json_artifact,
    load_artifact_policy_set,
    parse_markdown_envelope,
    validate_health_event_document,
)
from canonical_json import canonicalize, hash_canonical, strict_json_loads
from policy_artifacts import load_policy_set
from wiki_observations import ObservationPaths, validate_record
from workflow_evolution_fixtures import (
    FakeObservationStore,
    PRIVACY_SENTINEL,
    V1_BODY,
    V1_METADATA,
    v1_body_with_privacy_sentinel,
)


SCHEMA_ROWS = [
    {
        "artifact_type": "artifact-migration-registry",
        "schema_version": 1,
        "schema_identity": "artifact-migration-registry@1",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "immutable-policy",
    },
    {
        "artifact_type": "artifact-schema-registry",
        "schema_version": 1,
        "schema_identity": "artifact-schema-registry@1",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "immutable-policy",
    },
    {
        "artifact_type": "derived-artifact",
        "schema_version": 1,
        "schema_identity": "derived-artifact@1",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "derived-only",
    },
    {
        "artifact_type": "episode-projection",
        "schema_version": 2,
        "schema_identity": "episode-projection@2",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "derived-only",
    },
    {
        "artifact_type": "health-event",
        "schema_version": 1,
        "schema_identity": "health-event@1",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "disabled-until-phase-3",
    },
    {
        "artifact_type": "health-event-schema",
        "schema_version": 1,
        "schema_identity": "health-event-schema@1",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "immutable-policy",
    },
    {
        "artifact_type": "learning-snapshot",
        "schema_version": 1,
        "schema_identity": "learning-snapshot@1",
        "reader_contract": "legacy-exact-shape",
        "writer_contract": "legacy-read-only",
    },
    {
        "artifact_type": "learning-snapshot",
        "schema_version": 2,
        "schema_identity": "learning-snapshot@2",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "new-default",
    },
    {
        "artifact_type": "learning-snapshot-core",
        "schema_version": 2,
        "schema_identity": "learning-snapshot-core@2",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "embedded-only",
    },
    {
        "artifact_type": "observation-invalidation",
        "schema_version": 1,
        "schema_identity": "observation-invalidation@1",
        "reader_contract": "legacy-exact-shape",
        "writer_contract": "legacy-read-only",
    },
    {
        "artifact_type": "observation-invalidation",
        "schema_version": 2,
        "schema_identity": "observation-invalidation@2",
        "reader_contract": "explicit-markdown-envelope",
        "writer_contract": "new-default",
    },
    {
        "artifact_type": "snapshot-input",
        "schema_version": 1,
        "schema_identity": "snapshot-input@1",
        "reader_contract": "legacy-exact-shape",
        "writer_contract": "legacy-read-only",
    },
    {
        "artifact_type": "snapshot-input",
        "schema_version": 2,
        "schema_identity": "snapshot-input@2",
        "reader_contract": "strict-canonical-json",
        "writer_contract": "new-default",
    },
    {
        "artifact_type": "workflow-observation",
        "schema_version": 1,
        "schema_identity": "workflow-observation@1",
        "reader_contract": "legacy-exact-shape",
        "writer_contract": "explicit-v1-compatibility",
    },
    {
        "artifact_type": "workflow-observation",
        "schema_version": 2,
        "schema_identity": "workflow-observation@2",
        "reader_contract": "explicit-episode-v2",
        "writer_contract": "explicit-v2-compatibility",
    },
]

SCHEMA_REGISTRY = {
    "artifact_type": "artifact-schema-registry",
    "schema_version": 1,
    "registry_version": "artifact-schema-registry@1",
    "schemas": SCHEMA_ROWS,
}

MIGRATION_ROWS = [
    {
        "source_artifact_type": "learning-snapshot",
        "source_schema_version": 1,
        "target_contract": "learning-snapshot-core@2",
        "target_schema_version": 2,
        "migration_identity": "learning-snapshot-v1-to-core-v2@1",
        "handler": "learning-snapshot-v1",
    },
    {
        "source_artifact_type": "observation-invalidation",
        "source_schema_version": 1,
        "target_contract": "observation-invalidation@2",
        "target_schema_version": 2,
        "migration_identity": "observation-invalidation-v1-to-v2@1",
        "handler": "observation-invalidation-v1",
    },
    {
        "source_artifact_type": "workflow-observation",
        "source_schema_version": 1,
        "target_contract": "episode-projection@2",
        "target_schema_version": 2,
        "migration_identity": "workflow-observation-v1-to-episode-projection@1",
        "handler": "workflow-observation-v1",
    },
    {
        "source_artifact_type": "workflow-observation",
        "source_schema_version": 2,
        "target_contract": "episode-projection@2",
        "target_schema_version": 2,
        "migration_identity": "workflow-observation-v2-to-episode-projection@1",
        "handler": "workflow-observation-v2",
    },
]

MIGRATION_REGISTRY = {
    "artifact_type": "artifact-migration-registry",
    "schema_version": 1,
    "registry_version": "artifact-migration-registry@1",
    "migrations": MIGRATION_ROWS,
}


def enum_property(values):
    return {"type": "enum", "values": values}


def evidence_row(event_type, properties):
    return {
        "event_type": event_type,
        "evidence": {
            "required": list(properties),
            "properties": properties,
        },
    }


HEALTH_EVENT_ROWS = [
    evidence_row(
        "validation-rejected",
        {"reason_code": enum_property(["artifact", "policy", "reference", "request"])},
    ),
    evidence_row(
        "schema-mismatch",
        {
            "expected_schema": {"type": "bounded-identifier"},
            "observed_schema": {"type": "bounded-identifier"},
        },
    ),
    evidence_row("duplicate-finish", {"attempt": {"type": "positive-integer"}}),
    evidence_row(
        "record-dropped",
        {"reason_code": enum_property(["invalid", "storage", "unsupported"])},
    ),
    evidence_row(
        "payload-cleanup-failed",
        {"attempt": {"type": "positive-integer"}},
    ),
    evidence_row(
        "lock-contended",
        {"wait_milliseconds": {"type": "nonnegative-integer"}},
    ),
    evidence_row(
        "lock-timeout",
        {"timeout_milliseconds": {"type": "positive-integer"}},
    ),
    evidence_row(
        "stale-owner-recovered",
        {"metadata_present": {"type": "boolean"}},
    ),
    evidence_row("cas-conflict", {"attempt": {"type": "positive-integer"}}),
    evidence_row(
        "maintenance-lease-timeout",
        {"timeout_milliseconds": {"type": "positive-integer"}},
    ),
    evidence_row(
        "maintenance-recovery-blocked",
        {
            "reason_code": enum_property(
                [
                    "conflict",
                    "corrupt-staging",
                    "durability",
                    "missing-staging",
                    "multiple-active",
                ]
            )
        },
    ),
    evidence_row(
        "sampling-decision-failed",
        {"reason_code": enum_property(["cleanup", "policy", "validation"])},
    ),
    evidence_row(
        "export-aborted",
        {"reason_code": enum_property(["drift", "integrity", "validation"])},
    ),
    evidence_row(
        "purge-aborted",
        {"reason_code": enum_property(["drift", "expired", "integrity", "validation"])},
    ),
]

HEALTH_EVENT_SCHEMA = {
    "artifact_type": "health-event-schema",
    "schema_version": 1,
    "schema_identity": "health-event-schema@1",
    "event_schema_identity": "health-event@1",
    "event_types": HEALTH_EVENT_ROWS,
    "error_classes": [
        "conflict",
        "integrity",
        "state",
        "timeout",
        "unsupported",
        "validation",
    ],
    "resource_kinds": [
        "artifact",
        "export",
        "health",
        "learning-snapshot",
        "maintenance",
        "observation",
        "observation-invalidation",
        "purge",
        "sampling",
        "store",
    ],
    "limits": {
        "identifier_max_utf8_bytes": 128,
        "integer_max": 9007199254740991,
    },
    "hash_domain": "workflow-observatory:health-event:v1",
}

POLICY_DOCUMENTS = {
    "artifact_schema_registry.json": SCHEMA_REGISTRY,
    "artifact_migration_registry.json": MIGRATION_REGISTRY,
    "health_event_schema.json": HEALTH_EVENT_SCHEMA,
}

EXPECTED_IDENTITIES = {
    "artifact_schema_registry": {
        "version": "artifact-schema-registry@1",
        "sha256": "sha256:1bba0c5635ed2cedf4885861243947c89d3f9ba98e358b049ff3a61c0a40e7d6",
    },
    "artifact_migration_registry": {
        "version": "artifact-migration-registry@1",
        "sha256": "sha256:0c6bbdb88de176725c065f885a4393b73db19ee769f04f486ade013121e0fe90",
    },
    "health_event_schema": {
        "version": "health-event-schema@1",
        "sha256": "sha256:5abab8b18858e95535b31185eb65d273e8ec4758034e5cfe85492528fcaba516",
    },
}


class EqualitySpoof:
    def __init__(self):
        self.mutable = []

    def __eq__(self, other):
        return True


def render_markdown(metadata, body=""):
    lines = ["---"]
    for key, value in metadata.items():
        lines.append(
            f"{key}: {json.dumps(value, ensure_ascii=False, separators=(',', ':'))}"
        )
    lines.extend(["---", ""])
    return "\n".join(lines) + body


class ArtifactSchemaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.policy_root = self.root / "policies"
        self.policy_root.mkdir()
        self.documents = copy.deepcopy(POLICY_DOCUMENTS)
        self.write_documents()

    def write_documents(self):
        for filename, document in self.documents.items():
            (self.policy_root / filename).write_text(
                json.dumps(document, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    def load(self):
        return load_artifact_policy_set(self.policy_root)

    def event(self, *, event_type="cas-conflict", evidence=None):
        value = {
            "artifact_type": "health-event",
            "schema_version": 1,
            "event_id": "health-20260811-040000-abcdef",
            "occurred_at": "2026-08-11T04:00:00Z",
            "event_type": event_type,
            "operation_id": "op-abcdef",
            "run_id": None,
            "resource_kind": "observation",
            "resource_key": "sha256-abcdef",
            "evidence": {"attempt": 1} if evidence is None else evidence,
            "error_class": "state",
            "policy_identity": "writer-safety@1",
        }
        value["event_sha256"] = hash_canonical(
            b"workflow-observatory:health-event:v1\0", value
        )
        return value

    def test_01_loader_binds_exact_policy_identities(self):
        policies = self.load()
        self.assertEqual(EXPECTED_IDENTITIES, policies.identities())
        for name, document in (
            ("artifact_schema_registry", SCHEMA_REGISTRY),
            ("artifact_migration_registry", MIGRATION_REGISTRY),
            ("health_event_schema", HEALTH_EVENT_SCHEMA),
        ):
            self.assertEqual(
                EXPECTED_IDENTITIES[name]["sha256"],
                "sha256:" + hashlib.sha256(canonicalize(document)).hexdigest(),
            )

    def test_checked_in_policy_documents_match_the_frozen_contract(self):
        for filename, expected in POLICY_DOCUMENTS.items():
            with self.subTest(filename=filename):
                actual = strict_json_loads((POLICY_ROOT / filename).read_bytes())
                self.assertEqual(expected, actual)

    def test_v02_policy_core_identity_is_unchanged(self):
        arguments = {
            "policy_root": POLICY_ROOT,
            "analyzer_files": ["scripts/learning_snapshot.py"],
            "canonicalizer_files": ["scripts/canonical_json.py"],
        }
        before = canonicalize(load_policy_set(**arguments).core_identity())
        self.load()
        after = canonicalize(load_policy_set(**arguments).core_identity())
        self.assertEqual(before, after)

    def test_strict_json_ingress_rejects_duplicate_keys(self):
        (self.policy_root / "artifact_schema_registry.json").write_text(
            '{"artifact_type":"artifact-schema-registry",'
            '"artifact_type":"artifact-schema-registry"}',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            self.load()

    def test_policy_documents_reject_unknown_top_level_keys(self):
        self.documents["artifact_schema_registry.json"]["future"] = True
        self.write_documents()
        with self.assertRaisesRegex(ValueError, "exact allowed keys"):
            self.load()

    def test_classifier_rejects_unknown_pair_and_inconsistent_identity(self):
        policies = self.load()
        with self.assertRaisesRegex(ValueError, "unknown artifact schema"):
            classify_json_artifact(
                {"artifact_type": "health-event", "schema_version": 2},
                expected_artifact_type="health-event",
                policies=policies,
            )
        with self.assertRaisesRegex(ValueError, "schema identity"):
            classify_json_artifact(
                {
                    "artifact_type": "health-event",
                    "schema_version": 1,
                    "schema_identity": "health-event@2",
                },
                expected_artifact_type="health-event",
                policies=policies,
            )

    def test_markdown_classifier_accepts_only_exact_legacy_observation_matrix(self):
        policies = self.load()
        with FakeObservationStore("portable") as store:
            cases = (
                (store.v1_path.read_text(encoding="utf-8"), 1),
                (store.v2_path.read_text(encoding="utf-8"), 2),
            )
            for text, version in cases:
                with self.subTest(version=version):
                    envelope = parse_markdown_envelope(
                        text,
                        expected_human_type="observation",
                        policies=policies,
                    )
                    self.assertEqual(
                        f"workflow-observation@{version}",
                        envelope.artifact.schema_identity,
                    )
                    self.assertEqual(version, envelope.artifact.schema_version)
                    self.assertEqual("observation", envelope.metadata["type"])

    def test_markdown_classifier_accepts_matching_explicit_registered_artifact(self):
        metadata = {
            "type": "observation-invalidation",
            "artifact_type": "observation-invalidation",
            "schema_version": 2,
            "run_id": "obs-20260811-040000-abcdef",
            "timestamp": "2026-08-11T04:00:00Z",
        }
        envelope = parse_markdown_envelope(
            render_markdown(metadata),
            expected_human_type="observation-invalidation",
            policies=self.load(),
        )
        self.assertEqual("observation-invalidation@2", envelope.artifact.schema_identity)
        self.assertEqual(metadata, envelope.metadata)
        self.assertEqual("", envelope.body)

    def test_explicit_invalidation_rejects_legacy_labels(self):
        metadata = {
            "type": "observation-invalidation",
            "artifact_type": "observation-invalidation",
            "schema_version": 2,
            "run_id": "obs-20260811-040000-abcdef",
            "timestamp": "2026-08-11T04:00:00Z",
            "title": "Invalidate obs-20260811-040000-abcdef",
            "tags": ["observation", "invalidation"],
            "target_run_id": "obs-20260811-040000-abcdef",
            "reason": "invalid source evidence",
            "sources": [],
        }
        with self.assertRaisesRegex(ArtifactSchemaError, "unexpected field"):
            parse_markdown_envelope(
                render_markdown(metadata),
                expected_human_type="observation-invalidation",
                policies=self.load(),
            )

    def test_markdown_classifier_accepts_exact_legacy_invalidation(self):
        metadata = {
            "type": "observation-invalidation",
            "title": "Invalidate obs-20260811-040000-abcdef",
            "tags": ["observation", "invalidation"],
            "timestamp": "2026-08-11T04:00:00Z",
            "target_run_id": "obs-20260811-040000-abcdef",
            "reason": "invalid source evidence",
            "sources": [],
        }
        envelope = parse_markdown_envelope(
            render_markdown(metadata),
            expected_human_type="observation-invalidation",
            policies=self.load(),
        )
        self.assertEqual("observation-invalidation@1", envelope.artifact.schema_identity)

    def test_markdown_classifier_fails_closed_on_ambiguous_or_mismatched_envelopes(self):
        policies = self.load()
        explicit_invalidation = {
            "type": "observation-invalidation",
            "artifact_type": "observation-invalidation",
            "schema_version": 2,
            "run_id": "obs-20260811-040000-abcdef",
            "timestamp": "2026-08-11T04:00:00Z",
        }
        invalid = {
            "absent version with extra field": render_markdown(
                {**V1_METADATA, "future": True}, V1_BODY
            ),
            "artifact without version": render_markdown(
                {**V1_METADATA, "artifact_type": "workflow-observation"},
                V1_BODY,
            ),
            "type mismatch": render_markdown(
                {**explicit_invalidation, "artifact_type": "workflow-observation"}
            ),
            "unknown version": render_markdown(
                {**explicit_invalidation, "schema_version": 3}
            ),
            "historically invalid body": render_markdown(
                V1_METADATA,
                V1_BODY.replace("## Scope", "## Not Scope", 1),
            ),
        }
        for name, text in invalid.items():
            human_type = (
                "observation-invalidation"
                if "invalidation" in text
                else "observation"
            )
            with self.subTest(name=name), self.assertRaises(ArtifactSchemaError):
                parse_markdown_envelope(
                    text,
                    expected_human_type=human_type,
                    policies=policies,
                )

    def test_markdown_classifier_rejects_duplicate_frontmatter_key(self):
        text = render_markdown(V1_METADATA, V1_BODY).replace(
            "type: \"observation\"\n",
            "type: \"observation\"\ntype: \"observation\"\n",
            1,
        )
        with self.assertRaisesRegex(ArtifactSchemaError, "frontmatter"):
            parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=self.load(),
            )

    def test_markdown_classifier_rejects_episode_envelope_disagreement(self):
        policies = self.load()
        with FakeObservationStore("portable") as store:
            v2_text = store.v2_path.read_text(encoding="utf-8")
            _v2_metadata, v2_body = v2_text.split("\n---\n", 1)
            mismatches = (
                render_markdown(V1_METADATA, v2_body.lstrip("\n")),
                render_markdown({**V1_METADATA, "schema_version": 2}, V1_BODY),
            )
            for text in mismatches:
                with self.subTest(text=text[:80]), self.assertRaisesRegex(
                    ArtifactSchemaError,
                    "Episode|schema",
                ):
                    parse_markdown_envelope(
                        text,
                        expected_human_type="observation",
                        policies=policies,
                    )

    def test_pure_classifier_preserves_lifecycle_duration_validation(self):
        text = render_markdown(
            V1_METADATA,
            V1_BODY.replace("elapsed_seconds: 120", "elapsed_seconds: 121", 1),
        )
        with self.assertRaisesRegex(ArtifactSchemaError, "elapsed_seconds"):
            parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=self.load(),
            )

    def test_pure_classifier_rejects_fractional_draft_timestamps(self):
        policies = self.load()
        draft_body = V1_BODY.split("\n## Execution evidence", 1)[0].rstrip() + "\n"
        for version in (1, 2):
            metadata = {
                **V1_METADATA,
                "status": "draft",
                "timestamp": "2026-08-02T08:00:00.123456+08:00",
            }
            if version == 2:
                metadata["schema_version"] = 2
            with self.subTest(version=version), self.assertRaisesRegex(
                ArtifactSchemaError,
                "second-precision",
            ):
                parse_markdown_envelope(
                    render_markdown(metadata, draft_body),
                    expected_human_type="observation",
                    policies=policies,
                )

    def test_pure_classifier_accepts_utc_second_draft_timestamps(self):
        policies = self.load()
        draft_body = V1_BODY.split("\n## Execution evidence", 1)[0].rstrip() + "\n"
        for version in (1, 2):
            metadata = {
                **V1_METADATA,
                "status": "draft",
                "timestamp": "2026-08-02T00:00:00Z",
            }
            if version == 2:
                metadata["schema_version"] = 2
            with self.subTest(version=version):
                envelope = parse_markdown_envelope(
                    render_markdown(metadata, draft_body),
                    expected_human_type="observation",
                    policies=policies,
                )
            self.assertEqual(version, envelope.artifact.schema_version)

    def test_markdown_classifier_uses_one_existing_frontmatter_parse(self):
        import wiki_observations

        text = render_markdown(V1_METADATA, V1_BODY)
        with mock.patch.object(
            wiki_observations,
            "_parse_frontmatter",
            wraps=wiki_observations._parse_frontmatter,
        ) as parser:
            parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=self.load(),
            )
        parser.assert_called_once_with(text)

    def test_markdown_classifier_performs_no_hidden_policy_or_file_loading(self):
        policies = self.load()
        with FakeObservationStore("portable") as store:
            text = store.v2_path.read_text(encoding="utf-8")

        failure = AssertionError("classifier performed hidden policy I/O")
        with (
            mock.patch(
                "wiki_observations._episode_projection_policy",
                side_effect=failure,
            ),
            mock.patch("policy_artifacts.load_policy_set", side_effect=failure),
            mock.patch("artifact_schema.load_artifact_policy_set", side_effect=failure),
            mock.patch("pathlib.Path.read_text", side_effect=failure),
        ):
            envelope = parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=policies,
            )

        self.assertEqual("workflow-observation@2", envelope.artifact.schema_identity)

    def test_persisted_consumer_applies_policy_after_pure_classification(self):
        with FakeObservationStore("portable") as store:
            text = store.v2_path.read_text(encoding="utf-8").replace(
                '"measurement_source":"tool-derived"',
                '"measurement_source":"future-source"',
                1,
            )
            envelope = parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=self.load(),
            )
            errors = validate_record(
                envelope.metadata,
                envelope.body,
                ObservationPaths.from_root(store.store_root),
                artifact=envelope.artifact,
            )

        self.assertIn(
            "measurement_source is not allowed by projection policy",
            errors,
        )

    def test_pure_classification_excludes_reference_acquisition(self):
        metadata = {
            **V1_METADATA,
            "title": PRIVACY_SENTINEL,
            "task_ref": "[[missing-task]]",
            "sources": ["raw/missing-source.md"],
        }
        text = render_markdown(metadata, v1_body_with_privacy_sentinel())
        import wiki_observations

        with mock.patch.object(
            wiki_observations,
            "ReferenceResolver",
            side_effect=AssertionError("classifier acquired filesystem evidence"),
        ):
            envelope = parse_markdown_envelope(
                text,
                expected_human_type="observation",
                policies=self.load(),
            )
        self.assertNotIn(PRIVACY_SENTINEL, repr(envelope.artifact))

        store_root = self.root / "fake-store"
        (store_root / "wiki" / "observations").mkdir(parents=True)
        errors = validate_record(
            envelope.metadata,
            envelope.body,
            ObservationPaths.from_root(store_root),
        )
        self.assertIn("source does not exist: raw/missing-source.md", errors)
        self.assertIn("task_ref points to no task record", errors)

    def test_registry_rejects_inconsistent_schema_identity(self):
        self.documents["artifact_schema_registry.json"]["schemas"][4][
            "schema_identity"
        ] = "health-event@2"
        self.write_documents()
        with self.assertRaisesRegex(ValueError, "schema identity"):
            self.load()

    def test_migration_registry_closes_cross_document_references(self):
        for field in ("source_artifact_type", "target_contract"):
            with self.subTest(field=field):
                self.documents = copy.deepcopy(POLICY_DOCUMENTS)
                self.documents["artifact_migration_registry.json"]["migrations"][0][
                    field
                ] = "missing-artifact" if field.endswith("type") else "missing@1"
                self.write_documents()
                with self.assertRaisesRegex(ValueError, "registered schema"):
                    self.load()

    def test_registry_rejects_duplicate_dispatch_keys(self):
        duplicate = copy.deepcopy(
            self.documents["artifact_schema_registry.json"]["schemas"][0]
        )
        self.documents["artifact_schema_registry.json"]["schemas"].append(duplicate)
        self.write_documents()
        with self.assertRaisesRegex(ValueError, "duplicate dispatch"):
            self.load()

    def test_same_policy_version_with_changed_bytes_is_rejected(self):
        self.documents["artifact_schema_registry.json"]["schemas"][0][
            "reader_contract"
        ] = "legacy-exact-shape"
        self.write_documents()
        with self.assertRaisesRegex(ValueError, "approved policy bytes"):
            self.load()

    def test_constructor_inputs_and_property_results_are_defensive_copies(self):
        loaded = self.load()
        schema = loaded.schema_registry
        migration = loaded.migration_registry
        health = loaded.health_event_schema
        identities = loaded.identities()
        policies = ArtifactPolicySet(
            schema_registry=schema,
            migration_registry=migration,
            health_event_schema=health,
            identities=identities,
        )

        schema["schemas"][0]["artifact_type"] = "tampered-input"
        identities["artifact_schema_registry"]["version"] = "tampered-input"
        returned = policies.schema_registry
        returned["schemas"][0]["artifact_type"] = "tampered-property"
        returned_identities = policies.identities()
        returned_identities["artifact_schema_registry"]["version"] = (
            "tampered-property"
        )

        self.assertEqual(
            "artifact-migration-registry",
            policies.schema_registry["schemas"][0]["artifact_type"],
        )
        self.assertEqual(EXPECTED_IDENTITIES, policies.identities())
        with self.assertRaises(TypeError):
            policies._schema_registry["schemas"] = ()

    def test_constructor_accepts_read_only_mapping_inputs(self):
        loaded = self.load()
        identities = {
            name: MappingProxyType(row)
            for name, row in loaded.identities().items()
        }
        policies = ArtifactPolicySet(
            schema_registry=MappingProxyType(loaded.schema_registry),
            migration_registry=MappingProxyType(loaded.migration_registry),
            health_event_schema=MappingProxyType(loaded.health_event_schema),
            identities=MappingProxyType(identities),
        )
        self.assertEqual(EXPECTED_IDENTITIES, policies.identities())

    def test_constructor_rejects_non_json_identity_equality_spoofs(self):
        loaded = self.load()
        identities = loaded.identities()
        identities["artifact_schema_registry"]["sha256"] = EqualitySpoof()
        with self.assertRaisesRegex(ValueError, "identities.*I-JSON"):
            ArtifactPolicySet(
                schema_registry=loaded.schema_registry,
                migration_registry=loaded.migration_registry,
                health_event_schema=loaded.health_event_schema,
                identities=identities,
            )

    def test_loader_rejects_registry_symlinks_and_unsafe_root_spelling(self):
        outside = self.root / "outside.json"
        outside.write_text("{}\n", encoding="utf-8")
        registry = self.policy_root / "artifact_schema_registry.json"
        registry.unlink()
        registry.symlink_to(outside)
        with self.assertRaises(ValueError):
            self.load()

        registry.unlink()
        self.documents = copy.deepcopy(POLICY_DOCUMENTS)
        self.write_documents()
        (self.root / "child").mkdir()
        unsafe = self.root / "child" / ".." / "policies"
        with self.assertRaisesRegex(ValueError, "unsafe"):
            load_artifact_policy_set(unsafe)

    def test_all_health_event_evidence_contracts_validate(self):
        policies = self.load()
        for row in HEALTH_EVENT_ROWS:
            properties = row["evidence"]["properties"]
            evidence = {}
            for name, contract in properties.items():
                kind = contract["type"]
                if kind == "enum":
                    evidence[name] = contract["values"][0]
                elif kind == "bounded-identifier":
                    evidence[name] = "artifact@1"
                elif kind in {"positive-integer", "nonnegative-integer"}:
                    evidence[name] = 1
                elif kind == "boolean":
                    evidence[name] = True
            value = self.event(event_type=row["event_type"], evidence=evidence)
            with self.subTest(event_type=row["event_type"]):
                self.assertEqual(
                    value,
                    validate_health_event_document(
                        value, policies=policies, require_digest=True
                    ),
                )

    def test_health_event_rejects_unknown_enums(self):
        policies = self.load()
        for field, value in (
            ("event_type", "future-event"),
            ("error_class", "future-error"),
            ("resource_kind", "future-resource"),
        ):
            with self.subTest(field=field):
                event = self.event()
                event[field] = value
                with self.assertRaises(ValueError):
                    validate_health_event_document(
                        event, policies=policies, require_digest=True
                    )

    def test_health_event_rejects_missing_extra_and_wrong_typed_evidence(self):
        policies = self.load()
        for evidence in ({}, {"attempt": 1, "future": True}, {"attempt": "1"}):
            with self.subTest(evidence=evidence):
                event = self.event(evidence=evidence)
                with self.assertRaisesRegex(ValueError, "evidence"):
                    validate_health_event_document(
                        event, policies=policies, require_digest=True
                    )

    def test_health_event_rejects_unrestricted_sensitive_fields(self):
        policies = self.load()
        for field, value in (
            ("error", "full exception text"),
            ("path", "/Users/alice/private/repo"),
            ("hostname", "workstation.local"),
            ("pid", 1234),
        ):
            with self.subTest(field=field):
                event = self.event()
                event[field] = value
                with self.assertRaisesRegex(ValueError, "exact allowed keys"):
                    validate_health_event_document(
                        event, policies=policies, require_digest=True
                    )

    def test_health_event_rejects_stale_and_self_inconsistent_hashes(self):
        policies = self.load()
        stale = self.event()
        stale["resource_key"] = "sha256-changed"
        self_referential = self.event()
        self_referential["event_sha256"] = hash_canonical(
            b"workflow-observatory:health-event:v1\0", self_referential
        )
        for value in (stale, self_referential):
            with self.assertRaisesRegex(ValueError, "event_sha256"):
                validate_health_event_document(
                    value, policies=policies, require_digest=True
                )

    def test_health_event_enforces_timestamp_identifier_and_integer_bounds(self):
        policies = self.load()
        cases = (
            ("occurred_at", "2026-08-11T04:00:00.000Z", "occurred_at"),
            ("operation_id", "é", "identifier"),
            ("resource_key", "x" * 129, "identifier"),
        )
        for field, replacement, message in cases:
            with self.subTest(field=field):
                event = self.event()
                event[field] = replacement
                with self.assertRaisesRegex(ValueError, message):
                    validate_health_event_document(
                        event, policies=policies, require_digest=True
                    )

        event = self.event()
        event["evidence"]["attempt"] = 9007199254740992
        with self.assertRaisesRegex(ValueError, "evidence"):
            validate_health_event_document(
                event, policies=policies, require_digest=True
            )

    def test_health_event_preimage_mode_is_exact_and_does_not_generate_fields(self):
        policies = self.load()
        preimage = self.event()
        preimage.pop("event_sha256")
        before = copy.deepcopy(preimage)
        self.assertEqual(
            preimage,
            validate_health_event_document(
                preimage, policies=policies, require_digest=False
            ),
        )
        self.assertEqual(before, preimage)
        with self.assertRaisesRegex(ValueError, "exact allowed keys"):
            validate_health_event_document(
                self.event(), policies=policies, require_digest=False
            )

    def test_health_event_accepts_a_read_only_mapping_input(self):
        policies = self.load()
        event = self.event()
        self.assertEqual(
            event,
            validate_health_event_document(
                MappingProxyType(event),
                policies=policies,
                require_digest=True,
            ),
        )


if __name__ == "__main__":
    unittest.main()
