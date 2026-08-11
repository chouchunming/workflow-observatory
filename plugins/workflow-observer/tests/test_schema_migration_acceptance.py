from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import threading
import unittest
import zipfile


sys.dont_write_bytecode = True
PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE_ROOT = PLUGIN_ROOT.parents[1]
REPOSITORY_ROOT = MARKETPLACE_ROOT / "evidence"
for module_root in (
    PLUGIN_ROOT / "scripts",
    PLUGIN_ROOT / "tests",
    REPOSITORY_ROOT / "scripts",
):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from artifact_migration import migrate_artifact
from artifact_schema import (
    ArtifactSchemaError,
    load_artifact_policy_set,
    parse_markdown_envelope,
    validate_health_event_document,
)
from canonical_json import canonicalize, hash_canonical, strict_json_loads
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
from store_config import LLMWIKI_SEMANTICS, PORTABLE_SEMANTICS
from wiki_observations import (
    ObservationError,
    ObservationPaths,
    collect_record_documents,
    invalidate_observation,
)
from workflow_evolution_fixtures import (
    APPROVED_PHASE1_ARCHIVE_INVENTORY,
    AcceptanceFixtureMatrix,
    PRIVACY_SENTINEL,
    load_artifact_migration_vectors,
    select_phase1_archive_inventory,
)
from package_workflow_observatory import (
    _stage_live_marketplace,
    build_archive,
    default_evidence,
)


EXPECTED_CASE_IDS = tuple(f"test_{number:02d}" for number in range(1, 13))
_FIXED_NOW = "2026-08-03T00:01:00Z"
_SEMANTICS = {
    "portable": PORTABLE_SEMANTICS,
    "llmwiki": LLMWIKI_SEMANTICS,
}


def _declared_case_ids() -> tuple[str, ...]:
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=__file__)
    classes = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "WorkflowEvolutionAcceptanceTests"
    ]
    if len(classes) != 1:
        raise RuntimeError("acceptance matrix must define exactly one formal class")
    return tuple(
        node.name
        for node in classes[0].body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    )


if _declared_case_ids() != EXPECTED_CASE_IDS:
    raise RuntimeError(
        "acceptance case IDs are missing, duplicated, reordered, or appended"
    )


class WorkflowEvolutionAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._temporary = tempfile.TemporaryDirectory(
            prefix="workflow-observatory-phase1-acceptance-"
        )
        try:
            cls.root = Path(cls._temporary.name).resolve(strict=True)
            cls.root.chmod(0o700)
            cls.fixtures = AcceptanceFixtureMatrix(cls.root, EXPECTED_CASE_IDS)
            cls.migration_vectors = load_artifact_migration_vectors()
            cls.artifact_policies = load_artifact_policy_set(
                PLUGIN_ROOT / "policies"
            )
            cls.learning_policies = load_policy_set(
                PLUGIN_ROOT / "policies",
                analyzer_files=SNAPSHOT_ANALYZER_FILES,
                canonicalizer_files=("scripts/canonical_json.py",),
            )
            cls.query = SnapshotQuery(
                interval={
                    "basis": "started_at",
                    "since_inclusive": "2026-08-01T16:00:00Z",
                    "until_exclusive": "2026-08-03T16:00:00Z",
                    "requested_timezone": "Asia/Taipei",
                    "requested_dates": {
                        "since": "2026-08-02",
                        "until_inclusive": "2026-08-03",
                    },
                },
                lifecycle_as_of="2026-08-03T16:00:00Z",
                project=None,
                workspace=None,
                workspace_id=None,
                task_type=None,
            )
            cls._assert_isolation_preflight()
        except BaseException:
            cls._temporary.cleanup()
            raise

    @classmethod
    def tearDownClass(cls):
        failure = None
        try:
            cls.fixtures.assert_private_modes()
            cls.fixtures.assert_no_publication_residue()
        except BaseException as error:
            failure = error
        finally:
            root = cls.root
            cls._temporary.cleanup()
        if root.exists() and failure is None:
            failure = AssertionError("acceptance TemporaryDirectory cleanup failed")
        if failure is not None:
            raise failure

    @classmethod
    def _assert_isolation_preflight(cls) -> None:
        if stat.S_IMODE(cls.root.stat().st_mode) != 0o700:
            raise AssertionError("acceptance root must be mode-0700")
        cls.fixtures.assert_isolated()
        cls.fixtures.assert_private_modes()
        cls.fixtures.assert_fixture_provenance()

        source = Path(__file__).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=__file__)
        forbidden_imports = {"socket", "subprocess", "urllib", "requests", "httpx"}
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.partition(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.partition(".")[0])
        forbidden = sorted(forbidden_imports.intersection(imported))
        if forbidden:
            raise AssertionError(
                "formal acceptance must not use network or external commands: "
                + ", ".join(forbidden)
            )

    @classmethod
    def _stores(cls, case_id: str):
        return cls.fixtures.stores(case_id)

    @classmethod
    def _acquire(cls, case_id: str, adapter: str):
        store = cls._stores(case_id)[adapter]
        return acquire_snapshot_input(
            ObservationPaths.from_root(store.store_root),
            _SEMANTICS[adapter],
            cls.query,
            cls.learning_policies,
            cls.artifact_policies,
        )

    @classmethod
    def _publish(cls, case_id: str, adapter: str = "portable"):
        store = cls._stores(case_id)[adapter]
        return create_learning_snapshot(
            acquire=lambda: cls._acquire(case_id, adapter),
            query=cls.query,
            policy_set=cls.learning_policies,
            home=store.store_root.parent,
            generated_at=_FIXED_NOW,
        )

    @classmethod
    def _historical_v1(cls):
        vector = cls.migration_vectors["learning-snapshot-v1"]
        artifact = strict_json_loads(vector.source_bytes)
        policies = PolicySet(
            cls.learning_policies.documents,
            artifact["core"]["analysis_policy_set"],
        )
        return artifact, vector.source_bytes, policies

    @classmethod
    def _health_event(cls, row: dict[str, object]) -> dict[str, object]:
        evidence = {}
        for name, contract in row["evidence"]["properties"].items():
            kind = contract["type"]
            if kind == "enum":
                evidence[name] = contract["values"][0]
            elif kind == "bounded-identifier":
                evidence[name] = "artifact@1"
            elif kind in {"positive-integer", "nonnegative-integer"}:
                evidence[name] = 1
            elif kind == "boolean":
                evidence[name] = True
        event = {
            "artifact_type": "health-event",
            "schema_version": 1,
            "event_id": "health-20260811-040000-abcdef",
            "occurred_at": "2026-08-11T04:00:00Z",
            "event_type": row["event_type"],
            "operation_id": "op-abcdef",
            "run_id": None,
            "resource_kind": "observation",
            "resource_key": "sha256-abcdef",
            "evidence": evidence,
            "error_class": "state",
            "policy_identity": "writer-safety@1",
        }
        event["event_sha256"] = hash_canonical(
            b"workflow-observatory:health-event:v1\0", event
        )
        return event

    @staticmethod
    def _write_private(path: Path, content: bytes) -> None:
        path.write_bytes(content)
        path.chmod(0o600)

    def test_01(self):
        """Observation v1 exact legacy shape migrates deterministically."""
        vector = self.migration_vectors["workflow-observation-v1"]
        for adapter, store in self._stores("test_01").items():
            with self.subTest(adapter=adapter):
                source = store.v1_path.read_bytes()
                before = bytes(source)
                envelope = parse_markdown_envelope(
                    source.decode("utf-8"),
                    expected_human_type="observation",
                    policies=self.artifact_policies,
                )
                self.assertEqual(1, envelope.artifact.schema_version)
                first = migrate_artifact(
                    source_bytes=source,
                    expected_artifact_type="workflow-observation",
                    policies=self.artifact_policies,
                    observation_projection_policy=self.learning_policies.documents,
                )
                second = migrate_artifact(
                    source_bytes=source,
                    expected_artifact_type="workflow-observation",
                    policies=self.artifact_policies,
                    observation_projection_policy=self.learning_policies.documents,
                )
                self.assertEqual(vector.derived_bytes, first.canonical_bytes)
                self.assertEqual(first.canonical_bytes, second.canonical_bytes)
                self.assertEqual(before, source)
                self.assertEqual(before, store.v1_path.read_bytes())

    def test_02(self):
        """Observation v2 exact Episode shape yields one projection sample."""
        vector = self.migration_vectors["workflow-observation-v2"]
        for adapter, store in self._stores("test_02").items():
            with self.subTest(adapter=adapter):
                source = store.v2_path.read_bytes()
                envelope = parse_markdown_envelope(
                    source.decode("utf-8"),
                    expected_human_type="observation",
                    policies=self.artifact_policies,
                )
                self.assertEqual(2, envelope.artifact.schema_version)
                derived = migrate_artifact(
                    source_bytes=source,
                    expected_artifact_type="workflow-observation",
                    policies=self.artifact_policies,
                    observation_projection_policy=self.learning_policies.documents,
                )
                self.assertEqual(vector.derived_bytes, derived.canonical_bytes)
                self.assertEqual("episode-projection@2", derived.canonical_document[
                    "target"
                ]["contract"])
                self.assertEqual(2, derived.canonical_document["target"]["value"][
                    "episode_schema_version"
                ])
                self.assertEqual(1, len([derived.canonical_document["target"]["value"]]))

    def test_03(self):
        """Ambiguous absent-version observation fails the Data Trust Gate."""
        source = self._stores("test_03")["portable"].v1_path.read_bytes()
        ambiguous = source.replace(
            b'type: "observation"\n',
            b'type: "observation"\nartifact_type: "workflow-observation"\n',
            1,
        )
        with self.assertRaises(ArtifactSchemaError):
            migrate_artifact(
                source_bytes=ambiguous,
                expected_artifact_type="workflow-observation",
                policies=self.artifact_policies,
                observation_projection_policy=self.learning_policies.documents,
            )

    def test_04(self):
        """All 14 health-event schemas validate; mismatches fail closed."""
        rows = self.artifact_policies.health_event_schema["event_types"]
        self.assertEqual(14, len(rows))
        for row in rows:
            with self.subTest(event_type=row["event_type"]):
                event = self._health_event(row)
                self.assertEqual(
                    event,
                    validate_health_event_document(
                        event,
                        policies=self.artifact_policies,
                        require_digest=True,
                    ),
                )

        unknown_event = self._health_event(rows[0])
        unknown_event["event_type"] = "unknown-event"
        unknown_event["event_sha256"] = hash_canonical(
            b"workflow-observatory:health-event:v1\0",
            {key: value for key, value in unknown_event.items()
             if key != "event_sha256"},
        )
        unknown_schema = self._health_event(rows[0])
        unknown_schema["schema_version"] = 2
        unknown_schema["event_sha256"] = hash_canonical(
            b"workflow-observatory:health-event:v1\0",
            {key: value for key, value in unknown_schema.items()
             if key != "event_sha256"},
        )
        for label, event in (("event", unknown_event), ("schema", unknown_schema)):
            with self.subTest(label=label), self.assertRaises(ArtifactSchemaError):
                validate_health_event_document(
                    event,
                    policies=self.artifact_policies,
                    require_digest=True,
                )

        source = self._stores("test_04")["portable"].v2_path.read_bytes()
        disagreement = source.replace(
            b'```json\n{"decisions"',
            b'```json\n{"schema_version":1,"decisions"',
            1,
        ).replace(b',"schema_version":2}', b'}', 1)
        with self.assertRaises(ArtifactSchemaError):
            parse_markdown_envelope(
                disagreement.decode("utf-8"),
                expected_human_type="observation",
                policies=self.artifact_policies,
            )

    def test_05(self):
        """Invalidation v1 and v2 normalize equivalently with distinct hashes."""
        stores = self._stores("test_05")
        run_id = "obs-20260802-000000-abcdef"
        legacy = (
            "---\n"
            "type: observation-invalidation\n"
            f"title: Invalidate {run_id}\n"
            'tags: ["observation","invalidation"]\n'
            "timestamp: 2026-08-02T23:17:45+08:00\n"
            f"target_run_id: {run_id}\n"
            "reason: bounded legacy evidence\n"
            "sources: []\n"
            "---\n"
        ).encode("utf-8")
        explicit = (
            "---\n"
            "type: observation-invalidation\n"
            "artifact_type: observation-invalidation\n"
            "schema_version: 2\n"
            f"run_id: {run_id}\n"
            "timestamp: 2026-08-02T15:17:45Z\n"
            "---\n"
        ).encode("utf-8")
        self._write_private(stores["portable"].invalidations / f"{run_id}.md", legacy)
        self._write_private(stores["llmwiki"].invalidations / f"{run_id}.md", explicit)
        portable = collect_record_documents(
            ObservationPaths.from_root(stores["portable"].store_root),
            PORTABLE_SEMANTICS,
        ).invalidations[0]
        llmwiki = collect_record_documents(
            ObservationPaths.from_root(stores["llmwiki"].store_root),
            LLMWIKI_SEMANTICS,
        ).invalidations[0]
        self.assertEqual(
            (portable.run_id, portable.timestamp),
            (llmwiki.run_id, llmwiki.timestamp),
        )
        self.assertNotEqual(portable.source_sha256, llmwiki.source_sha256)
        self.assertEqual(hashlib.sha256(legacy).hexdigest(), portable.source_sha256)
        self.assertEqual(hashlib.sha256(explicit).hexdigest(), llmwiki.source_sha256)

    def test_06(self):
        """New invalidation writes v2 while a legacy tombstone stays immutable."""
        for adapter, store in self._stores("test_06").items():
            with self.subTest(adapter=adapter):
                legacy_run_id = "obs-20260802-000000-abcdef"
                new_run_id = "obs-20260802-000001-fedcba"
                legacy_path = store.invalidations / f"{legacy_run_id}.md"
                legacy = (
                    "---\n"
                    "type: observation-invalidation\n"
                    f"title: Invalidate {legacy_run_id}\n"
                    'tags: ["observation","invalidation"]\n'
                    "timestamp: 2026-08-02T23:17:45+08:00\n"
                    f"target_run_id: {legacy_run_id}\n"
                    "reason: immutable legacy evidence\n"
                    "sources: []\n"
                    "---\n"
                ).encode("utf-8")
                self._write_private(legacy_path, legacy)
                invalidate_observation(
                    ObservationPaths.from_root(store.store_root),
                    new_run_id,
                    "new bounded evidence",
                    now=datetime(2026, 8, 11, 1, 2, 3, tzinfo=timezone.utc),
                )
                new_path = store.invalidations / f"{new_run_id}.md"
                new_bytes = new_path.read_bytes()
                self.assertEqual(legacy, legacy_path.read_bytes())
                self.assertIn(b"artifact_type: observation-invalidation", new_bytes)
                self.assertIn(b"schema_version: 2", new_bytes)
                new_path.chmod(0o600)
                with self.assertRaisesRegex(ObservationError, "already invalidated"):
                    invalidate_observation(
                        ObservationPaths.from_root(store.store_root),
                        legacy_run_id,
                        "must not replace legacy",
                    )
                self.assertEqual(legacy, legacy_path.read_bytes())

    def test_07(self):
        """Snapshot Input v2 binds policies and a sorted private manifest."""
        identities = self.artifact_policies.identities()
        expected_policies = {
            key: identities[key]
            for key in (
                "artifact_schema_registry",
                "artifact_migration_registry",
            )
        }
        semantic_bytes = []
        for adapter in _SEMANTICS:
            acquired = self._acquire("test_07", adapter)
            bundle = acquired.semantic_bundle
            semantic_bytes.append(canonicalize(bundle))
            self.assertEqual(expected_policies, bundle["artifact_policy_set"])
            self.assertEqual(
                sorted(bundle["migration_manifest"], key=canonicalize),
                bundle["migration_manifest"],
            )
            self.assertEqual(1, len(bundle["migration_manifest"]))
            self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), semantic_bytes[-1])
        self.assertEqual(semantic_bytes[0], semantic_bytes[1])

    def test_08(self):
        """Learning Snapshot v1 derives the fixed v2 core without rewriting."""
        vector = self.migration_vectors["learning-snapshot-v1"]
        source = vector.source_bytes
        artifact = strict_json_loads(source)
        policies = PolicySet(
            self.learning_policies.documents,
            artifact["core"]["analysis_policy_set"],
        )
        root = self.fixtures.case_root("test_08") / "historical"
        root.mkdir(mode=0o700)
        path = root / f"{artifact['snapshot_id']}.json"
        self._write_private(path, source)
        before_inventory = tuple(sorted(item.name for item in root.iterdir()))
        derived = migrate_artifact(
            source_bytes=source,
            expected_artifact_type="learning-snapshot",
            policies=self.artifact_policies,
            learning_policy_set=policies,
        )
        self.assertEqual(vector.derived_bytes, derived.canonical_bytes)
        self.assertEqual("learning-snapshot-core@2", derived.canonical_document[
            "target"
        ]["contract"])
        self.assertEqual(2, derived.canonical_document["target"]["value"][
            "schema_version"
        ])
        self.assertEqual(source, path.read_bytes())
        self.assertEqual(before_inventory, tuple(sorted(item.name for item in root.iterdir())))

    def test_09(self):
        """New Snapshot v2 records exhaustive zero sampling semantics."""
        published = self._publish("test_09")
        artifact = strict_json_loads(published.path.read_bytes())
        core = artifact["core"]
        selected = len(core["input_manifest"]["episodes"])
        invalidated = len(core["input_manifest"]["invalidations"])
        self.assertEqual(2, artifact["schema_version"])
        self.assertEqual(2, core["schema_version"])
        self.assertEqual(0, core["sampled_by_policy_n"])
        self.assertEqual(
            {
                "full_retained_episode_n": selected - invalidated,
                "sampled_minimal_episode_n": 0,
                "sampling_policy_identities": [],
            },
            core["sampling_summary"],
        )
        for cohort in core["cohorts"]:
            for metric in cohort["metrics"]:
                missingness = metric["missingness"]
                self.assertEqual(0, missingness["sampled_by_policy_n"])
                self.assertEqual(
                    missingness["eligible_episode_n"],
                    sum(
                        missingness[name]
                        for name in (
                            "observed_n",
                            "not_recorded_n",
                            "unsupported_by_schema_n",
                            "not_applicable_n",
                            "sampled_by_policy_n",
                        )
                    ),
                )
        for candidate in core["candidates"]:
            self.assertEqual(
                {
                    "eligible_episode_n",
                    "outcome_episode_n",
                    "supporting_episode_n",
                },
                set(candidate["denominators"]),
            )

    def test_10(self):
        """Secure readback accepts siblings and rejects four mismatch classes."""
        store = self._stores("test_10")["portable"]
        historical, historical_bytes, historical_policies = self._historical_v1()
        learning_dir = store.store_root.parent / "learning"
        learning_dir.mkdir(mode=0o700)
        learning_dir.chmod(0o700)
        snapshot_dir = learning_dir / "snapshots"
        snapshot_dir.mkdir(mode=0o700)
        snapshot_dir.chmod(0o700)
        v1_path = snapshot_dir / f"{historical['snapshot_id']}.json"
        self._write_private(v1_path, historical_bytes)
        v2 = self._publish("test_10")
        self.assertEqual(1, read_learning_artifact(
            snapshot_dir, historical["snapshot_id"], historical_policies
        )["core"]["schema_version"])
        v2_artifact = read_learning_artifact(
            snapshot_dir, v2.snapshot_id, self.learning_policies
        )
        self.assertEqual(2, v2_artifact["schema_version"])
        self.assertEqual(historical_bytes, v1_path.read_bytes())

        raw = v2.path.read_bytes()
        value = strict_json_loads(raw)
        wrong_schema = deepcopy(value)
        wrong_schema["schema_version"] = 3
        wrong_schema.pop("artifact_sha256")
        wrong_schema["artifact_sha256"] = hashlib.sha256(
            canonicalize(wrong_schema)
        ).hexdigest()
        wrong_digest = deepcopy(value)
        wrong_digest["artifact_sha256"] = "0" * 64
        failures = (
            (canonicalize(wrong_schema), None),
            (raw, "0" * 64),
            (canonicalize(wrong_digest), None),
            (b" " + raw, None),
        )
        for index, (candidate, expected_id) in enumerate(failures):
            with self.subTest(mismatch=index), self.assertRaises(
                SnapshotPublicationError
            ):
                validate_learning_artifact_bytes(
                    candidate,
                    self.learning_policies,
                    expected_snapshot_id=expected_id,
                )

    def test_11(self):
        """Concurrent v2 publishers create exactly once without residue."""
        for adapter in ("portable", "llmwiki"):
            with self.subTest(adapter=adapter):
                barrier = threading.Barrier(2, timeout=15)

                def publish():
                    def acquire():
                        barrier.wait()
                        return self._acquire("test_11", adapter)

                    store = self._stores("test_11")[adapter]
                    return create_learning_snapshot(
                        acquire=acquire,
                        query=self.query,
                        policy_set=self.learning_policies,
                        home=store.store_root.parent,
                        generated_at=_FIXED_NOW,
                    )

                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = [executor.submit(publish) for _ in range(2)]
                    results = [future.result(timeout=30) for future in futures]
                self.assertEqual(
                    [False, True], sorted(result.created for result in results)
                )
                self.assertEqual(1, len({result.snapshot_id for result in results}))
                final = results[0].path
                read_learning_artifact(
                    final.parent, results[0].snapshot_id, self.learning_policies
                )
                self.assertEqual([], list(final.parent.glob(".snapshot-*.tmp")))

    def test_12(self):
        """Adapter semantic bytes and approved archive inventory are exact."""
        semantic_bytes = [
            canonicalize(self._acquire("test_12", adapter).semantic_bundle)
            for adapter in ("portable", "llmwiki")
        ]
        self.assertEqual(semantic_bytes[0], semantic_bytes[1])

        archive = self.fixtures.case_root("test_12") / "phase1.zip"
        staging_parent = self.fixtures.case_root("test_12") / "staging"
        staging_parent.mkdir(mode=0o700)
        try:
            source = _stage_live_marketplace(MARKETPLACE_ROOT, staging_parent)
            build_archive(
                source,
                archive,
                default_evidence(REPOSITORY_ROOT),
            )
        finally:
            shutil.rmtree(staging_parent)
        archive.chmod(0o600)
        with zipfile.ZipFile(archive) as bundle:
            inventory = json.loads(
                bundle.read("workflow-observatory/SHA256SUMS.json")
            )["marketplace_files"]
        self.assertEqual(
            APPROVED_PHASE1_ARCHIVE_INVENTORY,
            select_phase1_archive_inventory(inventory),
        )


if __name__ == "__main__":
    unittest.main()
