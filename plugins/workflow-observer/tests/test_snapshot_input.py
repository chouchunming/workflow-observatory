from __future__ import annotations

from dataclasses import fields
from datetime import date
import hashlib
from pathlib import Path
import re
import sys
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
for module_root in (PLUGIN_ROOT / "scripts", PLUGIN_ROOT / "tests"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import canonicalize, hash_canonical
from policy_artifacts import PolicySet, load_policy_set
from snapshot_input import (
    SNAPSHOT_ANALYZER_FILES,
    SnapshotInputError,
    SnapshotQuery,
    acquire_snapshot_input,
    canonical_interval,
    canonical_reference_manifest,
    derive_store_identity,
)
from store_config import LLMWIKI_SEMANTICS, PORTABLE_SEMANTICS
from wiki_observations import (
    InvalidationEvidence,
    ObservationCollection,
    ObservationPaths,
    ReferenceEvidence,
    collect_record_documents,
)
from workflow_evolution_fixtures import (
    FakeObservationStore,
    PRIVACY_SENTINEL,
    V1_BODY,
    temporary_timezone,
    v1_body_with_privacy_sentinel,
)
import workflow_observer_cli


class SnapshotInputTests(unittest.TestCase):
    def setUp(self):
        self.stores = {
            "portable": FakeObservationStore("portable"),
            "llmwiki": FakeObservationStore("llmwiki"),
        }
        self.addCleanup(self.stores["llmwiki"].close)
        self.addCleanup(self.stores["portable"].close)
        for store in self.stores.values():
            store.v2_path.unlink()
        self.store = self.stores["portable"]
        self.policy_set = load_policy_set(
            PLUGIN_ROOT / "policies",
            analyzer_files=SNAPSHOT_ANALYZER_FILES,
            canonicalizer_files=("scripts/canonical_json.py",),
        )
        interval = {
            "basis": "started_at",
            "since_inclusive": "2026-08-02T00:00:00Z",
            "until_exclusive": "2026-08-03T00:00:00Z",
            "requested_timezone": "UTC",
            "requested_dates": {
                "since": "2026-08-02",
                "until_inclusive": "2026-08-02",
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

    def acquire(self, adapter="portable"):
        store = self.stores[adapter]
        semantics = {
            "portable": PORTABLE_SEMANTICS,
            "llmwiki": LLMWIKI_SEMANTICS,
        }[adapter]
        return acquire_snapshot_input(
            ObservationPaths.from_root(store.store_root),
            semantics,
            self.query,
            self.policy_set,
        )

    def add_reviewed_generation_mapping(
        self, generation="implementation-with-review@1"
    ):
        documents = self.policy_set.documents
        mapping = documents["workflow_generation_mapping"]
        mapping["mapping"] = {
            "obs-20260802-000000-abcdef": generation,
        }
        identities = self.policy_set.identities
        identities["workflow_generation_mapping"] = {
            "version": mapping["version"],
            "sha256": "sha256:" + hashlib.sha256(
                canonicalize(mapping)
            ).hexdigest(),
        }
        self.policy_set = PolicySet(documents, identities)

    def _collection(self, adapter="portable"):
        store = self.stores[adapter]
        semantics = {
            "portable": PORTABLE_SEMANTICS,
            "llmwiki": LLMWIKI_SEMANTICS,
        }[adapter]
        return collect_record_documents(
            ObservationPaths.from_root(store.store_root), semantics
        )

    def _write_valid_record_with_privacy_sentinel(self):
        content = self.store.v1_path.read_bytes().replace(
            V1_BODY.encode("utf-8"),
            v1_body_with_privacy_sentinel().encode("utf-8"),
        )
        self.store.v1_path.write_bytes(content)
        self.store.v1_path.chmod(0o600)

    def test_snapshot_query_has_exact_public_fields(self):
        self.assertEqual(
            (
                "interval",
                "lifecycle_as_of",
                "project",
                "workspace",
                "workspace_id",
                "task_type",
            ),
            tuple(field.name for field in fields(SnapshotQuery)),
        )

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
        }, canonical_interval(
            date(2026, 7, 15), date(2026, 8, 2), "Asia/Taipei"
        ))

    def test_interval_rejects_reverse_unknown_and_skipped_midnight(self):
        invalid = (
            (date(2026, 8, 2), date(2026, 8, 1), "UTC"),
            (date(2026, 8, 2), date(2026, 8, 2), "Not/A_Zone"),
            (date(2011, 12, 30), date(2011, 12, 30), "Pacific/Apia"),
        )
        for since, until, zone in invalid:
            with self.subTest(zone=zone), self.assertRaises(SnapshotInputError):
                canonical_interval(since, until, zone)

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
        self.assertEqual(
            {
                "availability": "observed",
                "value": "implementation-with-review@1",
            },
            acquired.semantic_bundle["episodes"][0]["workflow_generation"],
        )

    def test_reviewed_mapping_must_equal_an_observed_generation(self):
        store = self.store
        store.v1_path.unlink()
        store._write_private(store.v2_path, store.expected_raw_bytes["v2"])
        self.add_reviewed_generation_mapping("conflicting-generation@9")
        documents = self.policy_set.documents
        mapping = documents["workflow_generation_mapping"]
        mapping["mapping"] = {
            "obs-20260802-000001-fedcba": "conflicting-generation@9"
        }
        identities = self.policy_set.identities
        identities["workflow_generation_mapping"]["sha256"] = (
            "sha256:" + hashlib.sha256(canonicalize(mapping)).hexdigest()
        )
        self.policy_set = PolicySet(documents, identities)

        with self.assertRaisesRegex(SnapshotInputError, "conflicts with observed"):
            self.acquire()

    def test_duplicate_physical_sources_for_run_id_fail_gate(self):
        collection = self._collection()
        duplicate = ObservationCollection(
            records=(collection.records[0], collection.records[0]),
            invalidations=(),
        )
        with mock.patch(
            "snapshot_input.collect_record_documents", return_value=duplicate
        ), self.assertRaisesRegex(
            SnapshotInputError, "duplicate physical Episode"
        ):
            self.acquire()

    def test_unexpected_layout_entry_fails_integrity_gate(self):
        (self.store.observations / "record.backup").write_text(
            "unexpected", encoding="utf-8"
        )

        with self.assertRaisesRegex(SnapshotInputError, "unexpected observation"):
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
        self.assertEqual(
            {
                "name",
                "implementation_version",
                "implementation_sha256",
            },
            set(acquired.adapter),
        )
        self.assertNotIn("adapter", acquired.semantic_bundle)

    def test_cli_and_direct_acquisition_close_the_same_analyzer_dependencies(self):
        self.assertEqual(
            (
                "scripts/episode_schema.py",
                "scripts/policy_artifacts.py",
                "scripts/snapshot_input.py",
                "scripts/store_config.py",
                "scripts/wiki_observations.py",
            ),
            SNAPSHOT_ANALYZER_FILES,
        )
        self.assertEqual(
            tuple(sorted(SNAPSHOT_ANALYZER_FILES, key=lambda path: path.encode("utf-8"))),
            SNAPSHOT_ANALYZER_FILES,
        )
        direct = self.policy_set.core_identity()["analyzer_artifact"]
        cli = workflow_observer_cli._snapshot_policy_set().core_identity() \
            ["analyzer_artifact"]
        without_store_config = load_policy_set(
            PLUGIN_ROOT / "policies",
            analyzer_files=tuple(
                path for path in SNAPSHOT_ANALYZER_FILES
                if path != "scripts/store_config.py"
            ),
            canonicalizer_files=("scripts/canonical_json.py",),
        ).core_identity()["analyzer_artifact"]

        self.assertEqual(direct, cli)
        self.assertNotEqual(direct, without_store_config)

    def test_snapshot_input_excludes_human_text_and_reference_bodies(self):
        self._write_valid_record_with_privacy_sentinel()
        acquired = self.acquire()
        self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), acquired.manifest_bytes)

    def test_reference_manifest_collapses_identical_rows_and_rejects_conflicts(self):
        same = ReferenceEvidence("task", "example", "a" * 64)
        self.assertEqual(
            [{"kind": "task", "identity": "example", "sha256": "a" * 64}],
            canonical_reference_manifest([same, same]),
        )
        with self.assertRaisesRegex(
            SnapshotInputError, "conflicting reference identity"
        ):
            canonical_reference_manifest([
                same,
                ReferenceEvidence("task", "example", "b" * 64),
            ])

    def test_reference_manifest_rejects_absolute_identity(self):
        with self.assertRaisesRegex(SnapshotInputError, "relative identity"):
            canonical_reference_manifest([
                ReferenceEvidence("source", "/private/source.md", "a" * 64)
            ])

    def test_reference_manifest_rejects_non_normalized_relative_identity(self):
        for identity in (
            "raw\\source.md",
            "raw//source.md",
            "raw/./source.md",
            "raw/../source.md",
            "\ud800",
        ):
            with self.subTest(identity=identity), self.assertRaisesRegex(
                SnapshotInputError, "relative identity"
            ):
                canonical_reference_manifest([
                    ReferenceEvidence("source", identity, "a" * 64)
                ])

    def test_store_identity_uses_adapter_device_and_inode_without_path(self):
        paths = ObservationPaths.from_root(self.store.store_root)
        metadata = paths.root.stat()
        expected = hashlib.sha256(
            b"workflow-observatory:store-identity:v1\0"
            + b"portable\0"
            + str(metadata.st_dev).encode()
            + b"\0"
            + str(metadata.st_ino).encode()
        ).hexdigest()
        actual = derive_store_identity(paths, PORTABLE_SEMANTICS)
        self.assertEqual(expected, actual)
        self.assertNotIn(str(paths.root), actual)

    def test_selection_uses_started_at_and_exact_filters(self):
        self.query = SnapshotQuery(
            interval=self.query.interval,
            lifecycle_as_of=self.query.lifecycle_as_of,
            project="different-project",
            workspace=None,
            workspace_id=None,
            task_type=None,
        )
        acquired = self.acquire()
        self.assertEqual([], acquired.semantic_bundle["episodes"])
        self.assertEqual(0, acquired.semantic_bundle["record_counts"]
                         ["selected_episode_n"])

    def test_filters_reject_sensitive_path_or_credential_text(self):
        for field, value in (
            ("project", "/Users/alice/private-project"),
            ("workspace", "password=top-secret"),
        ):
            values = {
                "interval": self.query.interval,
                "lifecycle_as_of": self.query.lifecycle_as_of,
                "project": None,
                "workspace": None,
                "workspace_id": None,
                "task_type": None,
            }
            values[field] = value
            self.query = SnapshotQuery(**values)
            with self.subTest(field=field), self.assertRaisesRegex(
                SnapshotInputError, "sensitive path or credential"
            ):
                self.acquire()

    def test_policy_identity_rows_have_exact_path_free_fields(self):
        identities = self.policy_set.identities
        identities["quantile_policy"]["local_path"] = "/private/policy.json"
        self.policy_set = PolicySet(self.policy_set.documents, identities)

        with self.assertRaisesRegex(SnapshotInputError, "identity.*exact fields"):
            self.acquire()

    def test_selected_invalidation_is_manifested_regardless_of_its_timestamp(self):
        collection = self._collection()
        invalidation = InvalidationEvidence(
            collection.records[0].run_id,
            "2035-01-01T00:00:00Z",
            "c" * 64,
        )
        selected = ObservationCollection(
            records=collection.records,
            invalidations=(invalidation,),
        )
        with mock.patch(
            "snapshot_input.collect_record_documents", return_value=selected
        ):
            acquired = self.acquire()
        self.assertEqual(
            [{
                "run_id": collection.records[0].run_id,
                "source_sha256": "c" * 64,
                "timestamp": "2035-01-01T00:00:00Z",
            }],
            acquired.semantic_bundle["invalidations"],
        )
        self.assertEqual(
            1,
            acquired.semantic_bundle["record_counts"]
            ["selected_invalidation_n"],
        )

    def test_manifest_digest_closes_semantic_bundle_without_self_reference(self):
        acquired = self.acquire()
        bundle = dict(acquired.semantic_bundle)
        actual = bundle.pop("input_manifest_sha256")
        self.assertRegex(actual, r"^[0-9a-f]{64}$")
        self.assertEqual(
            hash_canonical(
                b"workflow-observatory:snapshot-input-manifest:v1\0",
                bundle,
            ),
            actual,
        )
        self.assertEqual(
            {"adapter", "store_identity", "semantic_bundle"},
            set(acquired.canonical_representation),
        )
        self.assertEqual(
            canonicalize(acquired.canonical_representation),
            acquired.manifest_bytes,
        )

    def test_lifecycle_as_of_must_be_utc_second_precision_and_not_before_window(self):
        invalid_values = (
            "2026-08-02T23:59:59Z",
            "2026-08-03T00:00:00.000Z",
            "2026-08-03T08:00:00+08:00",
        )
        for value in invalid_values:
            with self.subTest(value=value):
                self.query = SnapshotQuery(
                    interval=self.query.interval,
                    lifecycle_as_of=value,
                    project=None,
                    workspace=None,
                    workspace_id=None,
                    task_type=None,
                )
                with self.assertRaises(SnapshotInputError):
                    self.acquire()


if __name__ == "__main__":
    unittest.main()
