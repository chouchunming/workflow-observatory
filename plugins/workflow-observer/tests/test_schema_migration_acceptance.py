from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import multiprocessing.process
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
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
    _MANIFEST_DOMAIN,
    SNAPSHOT_ANALYZER_FILES,
    SnapshotInput,
    SnapshotQuery,
    acquire_snapshot_input,
)
from learning_snapshot import build_snapshot_core
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


class _FormalIsolationViolation(RuntimeError):
    pass


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
        cls._guard_stack = cls._install_external_effect_guards()
        cls._temporary = None
        try:
            cls._temporary = tempfile.TemporaryDirectory(
                prefix="workflow-observatory-phase1-acceptance-"
            )
            cls.root = Path(cls._temporary.name).resolve(strict=True)
            if stat.S_IMODE(cls.root.stat().st_mode) != 0o700:
                raise AssertionError("acceptance root must be mode-0700 at creation")
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
            if cls._temporary is not None:
                cls._temporary.cleanup()
            cls._guard_stack.close()
            raise

    @classmethod
    def tearDownClass(cls):
        failure = None
        try:
            cls.fixtures.assert_private_modes()
            cls.fixtures.assert_no_publication_residue()
            if cls._external_effect_violation is not None:
                raise AssertionError(cls._external_effect_violation)
        except BaseException as error:
            failure = error
        finally:
            root = cls.root
            cls._temporary.cleanup()
            cls._guard_stack.close()
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
        if cls._guard_probe_results != ("process", "network"):
            raise AssertionError("formal external-effect guard probes did not pass")

    @classmethod
    def _install_external_effect_guards(cls) -> ExitStack:
        cls._external_effect_violation = None
        cls._guard_probe_active = True
        stack = ExitStack()

        def blocker(label: str):
            def blocked(*_args, **_kwargs):
                message = f"formal acceptance blocked external effect: {label}"
                if cls._guard_probe_active:
                    raise _FormalIsolationViolation(message)
                cls._external_effect_violation = (
                    cls._external_effect_violation or message
                )
                result = getattr(cls, "_active_result", None)
                if result is None:
                    raise _FormalIsolationViolation(message)
                result.stop()
                raise unittest.case._ShouldStop

            return blocked

        process_targets = [(subprocess, "Popen")]
        process_targets.extend(
            (os, name)
            for name in (
                "execl",
                "execle",
                "execlp",
                "execlpe",
                "execv",
                "execve",
                "execvp",
                "execvpe",
                "fork",
                "forkpty",
                "popen",
                "posix_spawn",
                "posix_spawnp",
                "spawnl",
                "spawnle",
                "spawnlp",
                "spawnlpe",
                "spawnv",
                "spawnve",
                "spawnvp",
                "spawnvpe",
                "system",
            )
            if hasattr(os, name)
        )
        process_targets.append((multiprocessing.process.BaseProcess, "start"))
        network_targets = [
            (socket, name)
            for name in (
                "create_connection",
                "fromfd",
                "socket",
                "socketpair",
            )
            if hasattr(socket, name)
        ]
        try:
            for owner, name in (*process_targets, *network_targets):
                stack.enter_context(
                    mock.patch.object(
                        owner,
                        name,
                        new=blocker(f"{owner.__name__}.{name}"),
                    )
                )

            probes = []
            for label, attempt in (
                ("process", lambda: subprocess.Popen(("formal-guard-probe",))),
                ("network", lambda: socket.socket()),
            ):
                try:
                    attempt()
                except _FormalIsolationViolation:
                    probes.append(label)
                else:
                    raise AssertionError(
                        f"formal {label} guard probe was not blocked"
                    )
            cls._guard_probe_results = tuple(probes)
            cls._guard_probe_active = False
            return stack
        except BaseException:
            stack.close()
            raise

    def setUp(self):
        if self._external_effect_violation is not None:
            raise AssertionError(self._external_effect_violation)

    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
        self.__class__._active_result = result
        try:
            return super().run(result)
        finally:
            self.__class__._active_result = None

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
        published = create_learning_snapshot(
            acquire=lambda: cls._acquire(case_id, adapter),
            query=cls.query,
            policy_set=cls.learning_policies,
            home=store.store_root.parent,
            generated_at=_FIXED_NOW,
        )
        if stat.S_IMODE(published.path.stat().st_mode) != 0o600:
            raise AssertionError(
                f"{adapter} snapshot was not mode-0600 at publication"
            )
        return published

    @classmethod
    def _legacy_v02_core(cls, acquired: SnapshotInput) -> dict:
        bundle = acquired.semantic_bundle
        bundle["schema_version"] = 1
        bundle.pop("artifact_policy_set")
        bundle.pop("migration_manifest")
        bundle.pop("input_manifest_sha256")
        bundle["input_manifest_sha256"] = hash_canonical(
            _MANIFEST_DOMAIN, bundle
        )
        legacy = SnapshotInput(
            adapter=acquired.adapter,
            store_identity=acquired.store_identity,
            semantic_bundle=bundle,
            reviewed_generation_mapping=(
                cls.learning_policies.documents["workflow_generation_mapping"]
            ),
        )
        return build_snapshot_core(legacy, cls.learning_policies)

    @staticmethod
    def _candidate_denominators(core: dict) -> tuple[tuple[bytes, bytes], ...]:
        rows = tuple(sorted(
            (
                canonicalize([
                    candidate["candidate_type"],
                    candidate["class"],
                    candidate["cohort"],
                    candidate["source"],
                ]),
                canonicalize(candidate["denominators"]),
            )
            for candidate in core["candidates"]
        ))
        if len({key for key, _value in rows}) != len(rows):
            raise AssertionError("candidate denominator control keys are duplicated")
        return rows

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
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise AssertionError(f"private fixture output is not mode-0600: {path}")

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
                self.assertEqual(
                    (store.v2_path,),
                    self.fixtures.explicit_observation_sources(
                        "test_02", adapter, schema_version=2
                    ),
                )

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
                self.assertEqual(
                    0o600,
                    stat.S_IMODE(new_path.stat().st_mode),
                    "new invalidation must be mode-0600 at publication",
                )
                new_bytes = new_path.read_bytes()
                self.assertEqual(legacy, legacy_path.read_bytes())
                self.assertIn(b"artifact_type: observation-invalidation", new_bytes)
                self.assertIn(b"schema_version: 2", new_bytes)
                with self.assertRaisesRegex(ObservationError, "already invalidated"):
                    invalidate_observation(
                        ObservationPaths.from_root(store.store_root),
                        legacy_run_id,
                        "must not replace legacy",
                    )
                self.assertEqual(legacy, legacy_path.read_bytes())

    def test_07(self):
        """Snapshot Input v2 binds policies and a sorted private manifest."""
        self.fixtures.inject_privacy_sentinel_observations("test_07")
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
            source = self._stores("test_07")[adapter].v1_path.read_bytes()
            decoded_source = source.decode("utf-8")
            self.assertIn(
                f'title: "Workflow evolution fixture {PRIVACY_SENTINEL}"',
                decoded_source,
            )
            self.assertIn(
                f"- Goal: Exercise canonical Episode projection {PRIVACY_SENTINEL}",
                decoded_source,
            )
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
        self.assertEqual(
            before_inventory,
            tuple(sorted(item.name for item in root.iterdir())),
        )

    def test_09(self):
        """New Snapshot v2 records exhaustive zero sampling semantics."""
        cores = []
        for adapter in ("portable", "llmwiki"):
            acquired = self._acquire("test_09", adapter)
            legacy_core = self._legacy_v02_core(acquired)
            published = self._publish("test_09", adapter)
            self.assertEqual(
                0o600,
                stat.S_IMODE(published.path.stat().st_mode),
                f"{adapter} snapshot must be mode-0600 at publication",
            )
            artifact = strict_json_loads(published.path.read_bytes())
            core = artifact["core"]
            cores.append(canonicalize(core))
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
            fixed_denominators = {
                "eligible_episode_n": 1,
                "outcome_episode_n": 1,
                "supporting_episode_n": None,
            }
            for candidate in core["candidates"]:
                self.assertEqual(fixed_denominators, candidate["denominators"])
                missingness = candidate["evidence"]["missingness"]
                if candidate["source"]["kind"] != "metric" or missingness is None:
                    continue
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
            self.assertEqual(
                self._candidate_denominators(legacy_core),
                self._candidate_denominators(core),
            )
        self.assertEqual(cores[0], cores[1])

    def test_10(self):
        """Secure readback accepts siblings and rejects four mismatch classes."""
        store = self._stores("test_10")["portable"]
        historical, historical_bytes, historical_policies = self._historical_v1()
        learning_dir = store.store_root.parent / "learning"
        learning_dir.mkdir(mode=0o700)
        self.assertEqual(0o700, stat.S_IMODE(learning_dir.stat().st_mode))
        snapshot_dir = learning_dir / "snapshots"
        snapshot_dir.mkdir(mode=0o700)
        self.assertEqual(0o700, stat.S_IMODE(snapshot_dir.stat().st_mode))
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
                self.assertEqual(
                    0o600,
                    stat.S_IMODE(final.stat().st_mode),
                    f"{adapter} concurrent snapshot must publish mode-0600",
                )
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
            # Descendants are non-authoritative repo copies with ordinary
            # 0644/0755 modes; their mode-0700 ancestor is the privacy boundary.
            self.fixtures.assert_private_staging_boundary(staging_parent, source)
            build_archive(
                source,
                archive,
                default_evidence(REPOSITORY_ROOT),
            )
            self.assertEqual(
                0o600,
                stat.S_IMODE(archive.stat().st_mode),
                "archive must be mode-0600 at publication",
            )
        finally:
            shutil.rmtree(staging_parent)
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
