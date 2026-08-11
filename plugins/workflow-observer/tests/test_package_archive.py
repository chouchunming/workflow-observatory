import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import warnings
import zipfile


_TEST_PATH = Path(__file__).resolve()
_LAYOUT_ROOT = _TEST_PATH.parents[3]
if (_LAYOUT_ROOT / ".agents/plugins/marketplace.json").is_file():
    MARKETPLACE_ROOT = _LAYOUT_ROOT
    REPOSITORY_ROOT = _LAYOUT_ROOT / "evidence"
else:
    REPOSITORY_ROOT = _LAYOUT_ROOT / "evidence"
    MARKETPLACE_ROOT = REPOSITORY_ROOT / "marketplace/workflow-observatory"
for module_root in (
    REPOSITORY_ROOT / "scripts",
    MARKETPLACE_ROOT / "plugins/workflow-observer/scripts",
    MARKETPLACE_ROOT / "plugins/workflow-observer/tests",
):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import package_workflow_observatory as packager
from package_workflow_observatory import (
    PackageError,
    build_archive,
    default_evidence,
    main,
    verify_archive,
)
from workflow_evolution_fixtures import (
    APPROVED_PHASE1_ARCHIVE_INVENTORY,
    select_phase1_archive_inventory,
)


PHASE1_ACCEPTANCE_DOCUMENT_INVENTORY = frozenset(
    {
        "README.md",
        "ROADMAP.md",
        "TODO.md",
        "plugins/workflow-observer/README.md",
    }
)
PHASE1_ACCEPTANCE_TEST_INVENTORY = frozenset(
    {
        "plugins/workflow-observer/tests/test_artifact_migration.py",
        "plugins/workflow-observer/tests/test_artifact_schema.py",
        "plugins/workflow-observer/tests/test_schema_migration_acceptance.py",
    }
)
PHASE1_ACCEPTANCE_PACKAGE_INVENTORY = frozenset(
    {
        *APPROVED_PHASE1_ARCHIVE_INVENTORY,
        *PHASE1_ACCEPTANCE_DOCUMENT_INVENTORY,
        *PHASE1_ACCEPTANCE_TEST_INVENTORY,
    }
)
PHASE1_IMPLEMENTATION_BASELINE = "53d45af5344dc5fc231723802dad70fa5a0b564a"
PHASE1_CHECKPOINT_STATUS = (
    "Phase 1 is an unreleased implementation checkpoint, not a v0.3 release "
    "or publication."
)
V02_COMPLETION_STATUS = (
    "Workflow Evolution Foundation v0.2, including Task 11's 15-case matrix "
    "and bounded fake-store historical dry run, is complete."
)
LEGACY_BYTE_PRESERVATION = (
    "Phase 1 preserves existing observation v1, invalidation v1, and Learning "
    "Snapshot v1/v2 artifact bytes byte-for-byte. Readback and pure derived "
    "migrations never rewrite those artifacts in place."
)
ZERO_SAMPLING_BOUNDARY = (
    "Zero-sampling fields are schema semantics only: they state that no "
    "record/sample selection policy ran; Phase 1 does not select, retain, "
    "drop, or sample records."
)
BACKLOG_AUTHORITY_BOUNDARY = (
    "Roadmap and backlog items describe future design work; they are not "
    "implementation authority."
)


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "workflow-observatory"
        shutil.copytree(
            MARKETPLACE_ROOT,
            self.source,
            ignore=shutil.ignore_patterns(
                ".git", ".codegraph", ".superpowers", "__pycache__", "*.pyc"
            ),
        )
        self.archive = self.root / "workflow-observatory-0.1.0.zip"
        self.evidence = (
            REPOSITORY_ROOT
            / "wiki/concept/Workflow_Observation_and_Process_Knowledge.md",
            REPOSITORY_ROOT
            / "docs/superpowers/specs/2026-07-12-observation-records-design.md",
            REPOSITORY_ROOT
            / "docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md",
            REPOSITORY_ROOT
            / "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
            REPOSITORY_ROOT
            / "docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md",
            REPOSITORY_ROOT / "scripts/run_observing_workflows_task9_eval.py",
            REPOSITORY_ROOT / "tests/observing_workflows_eval_harness.py",
            REPOSITORY_ROOT / "tests/test_observation_lifecycle.py",
        )

    def _inventory(self, archive=None):
        archive = archive or self.archive
        with zipfile.ZipFile(archive) as bundle:
            return json.loads(
                bundle.read("workflow-observatory/SHA256SUMS.json")
            )

    def _public_documents(self):
        return {
            path: (self.source / path).read_text(encoding="utf-8")
            for path in PHASE1_ACCEPTANCE_DOCUMENT_INVENTORY
        }

    def _assert_live_v02_inventory(self, archive):
        inventory = self._inventory(archive)
        marketplace = inventory["marketplace_files"]
        repository_evidence = inventory["repository_evidence"]
        expected_policies = {
            path.relative_to(MARKETPLACE_ROOT).as_posix()
            for path in (
                MARKETPLACE_ROOT / "plugins/workflow-observer/policies"
            ).glob("*.json")
        }
        expected = {
            *expected_policies,
            "plugins/workflow-observer/tests/fixtures/"
            "jcs_conformance_vectors.json",
            "docs/superpowers/specs/"
            "2026-08-02-workflow-evolution-foundation-v0.2-design.md",
            "docs/superpowers/plans/"
            "2026-08-02-workflow-evolution-foundation-v0.2.md",
        }
        self.assertTrue(expected_policies)
        self.assertLessEqual(expected, set(marketplace))
        self.assertTrue(expected.isdisjoint(repository_evidence))

    def test_archive_contains_marketplace_and_repository_evidence(self):
        digest = build_archive(self.source, self.archive, self.evidence)
        self.assertEqual(digest, verify_archive(self.archive))
        with zipfile.ZipFile(self.archive) as bundle:
            names = set(bundle.namelist())
        for suffix in (
            ".agents/plugins/marketplace.json",
            ".codex-plugin/plugin.json",
            "TODO.md",
            "ROADMAP.md",
            "docs/parallel-evaluation-plan.md",
            "docs/release-acceptance.md",
            "Workflow_Observation_and_Process_Knowledge.md",
            "2026-07-12-observation-records-design.md",
            "2026-07-15-workflow-observatory-marketplace-design.md",
            "2026-07-13-observation-records-v2.md",
            "2026-07-15-workflow-observatory-marketplace.md",
            "run_observing_workflows_task9_eval.py",
            "observing_workflows_eval_harness.py",
            "test_parallel_eval_runner.py",
            "SHA256SUMS.json",
        ):
            self.assertTrue(any(name.endswith(suffix) for name in names), suffix)

        inventory = self._inventory()
        self.assertEqual(
            {path.relative_to(REPOSITORY_ROOT).as_posix() for path in self.evidence},
            set(inventory["repository_evidence"]),
        )
        for entry in inventory["repository_evidence"].values():
            self.assertRegex(entry["source_sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(entry["packaged_sha256"], r"^[0-9a-f]{64}$")
            self.assertIn(entry["member"], inventory["members"])

    def test_archive_is_reproducible_and_normalizes_personal_paths(self):
        second = self.root / "second.zip"
        first_digest = build_archive(self.source, self.archive, self.evidence)
        second_digest = build_archive(self.source, second, self.evidence)
        self.assertEqual(first_digest, second_digest)
        self.assertEqual(self.archive.read_bytes(), second.read_bytes())

        inventory = self._inventory()
        runner = inventory["repository_evidence"][
            "scripts/run_observing_workflows_task9_eval.py"
        ]
        runner_source = (
            REPOSITORY_ROOT / "scripts/run_observing_workflows_task9_eval.py"
        ).read_bytes()
        if b"${LLMWIKI_ROOT}" in runner_source:
            self.assertFalse(runner["normalized"])
            self.assertEqual(runner["source_sha256"], runner["packaged_sha256"])
        else:
            self.assertTrue(runner["normalized"])
            self.assertNotEqual(
                runner["source_sha256"], runner["packaged_sha256"]
            )
        with zipfile.ZipFile(self.archive) as bundle:
            all_bytes = b"\n".join(bundle.read(name) for name in bundle.namelist())
        self.assertNotIn(b"/" + b"Users/" + b"vincent", all_bytes)
        self.assertNotIn(b"/private/var/" + b"folders/", all_bytes)
        self.assertIn(b"/" + b"Users/" + b"alice/private/repo", all_bytes)

    def test_archive_packages_portable_mvp_implementation_plan(self):
        build_archive(self.source, self.archive, self.evidence)
        source_plan = (
            self.source / "docs/parallel-evaluation-mvp-implementation-plan.md"
        ).read_bytes()
        member = (
            "workflow-observatory/docs/"
            "parallel-evaluation-mvp-implementation-plan.md"
        )
        with zipfile.ZipFile(self.archive) as bundle:
            self.assertIn(member, bundle.namelist())
            plan = bundle.read(member)

        self.assertIn(b"${HOME}", plan)
        self.assertIn(b"${CODEX_HOME}", plan)
        self.assertNotIn(b"/" + b"Users/vincent", plan)
        row = self._inventory()["marketplace_files"][
            "docs/parallel-evaluation-mvp-implementation-plan.md"
        ]
        self.assertEqual(member, row["member"])
        expected_normalizations = [
            label
            for label, placeholder in (
                ("codex-home", b"${CODEX_HOME}"),
                ("user-home", b"${HOME}"),
            )
            if plan.count(placeholder) > source_plan.count(placeholder)
        ]
        self.assertEqual(
            expected_normalizations,
            row["normalizations"],
        )

    def test_archive_normalizes_declared_macos_home_off_host(self):
        plan = self.source / "docs/parallel-evaluation-mvp-implementation-plan.md"
        plan.write_text(
            plan.read_text(encoding="utf-8")
            + "\noff-host-home=/"
            + "Users/foreign/private.txt\n"
            + "off-host-codex=/"
            + "Users/foreign/.codex/skills/example\n",
            encoding="utf-8",
        )

        build_archive(self.source, self.archive, self.evidence)
        member = (
            "workflow-observatory/docs/"
            "parallel-evaluation-mvp-implementation-plan.md"
        )
        with zipfile.ZipFile(self.archive) as bundle:
            packaged = bundle.read(member)

        self.assertIn(b"off-host-home=${HOME}/private.txt", packaged)
        self.assertIn(b"off-host-codex=${CODEX_HOME}/skills/example", packaged)
        self.assertNotIn(b"/" + b"Users/foreign", packaged)

    def test_source_archive_inventory_closes_v02_policies_and_approved_docs(self):
        build_archive(self.source, self.archive, self.evidence)
        self._assert_live_v02_inventory(self.archive)

    def test_archive_contains_approved_v03_phase1_documents(self):
        build_archive(self.source, self.archive, self.evidence)
        marketplace = self._inventory()["marketplace_files"]
        self.assertIn(
            "docs/superpowers/specs/"
            "2026-08-11-workflow-observatory-concurrency-operability-v0.3-design.md",
            marketplace,
        )
        self.assertIn(
            "docs/superpowers/plans/"
            "2026-08-11-workflow-observatory-v0.3-phase-1-schema-migration.md",
            marketplace,
        )

    def test_archive_contains_exact_phase1_acceptance_inventory(self):
        build_archive(self.source, self.archive, self.evidence)
        marketplace = set(self._inventory()["marketplace_files"])
        selected = set(select_phase1_archive_inventory(marketplace))
        selected.update(
            path
            for path in marketplace
            if path in PHASE1_ACCEPTANCE_DOCUMENT_INVENTORY
            or path.startswith(
                "plugins/workflow-observer/tests/test_artifact_"
            )
        )
        self.assertEqual(PHASE1_ACCEPTANCE_PACKAGE_INVENTORY, selected)

    def test_public_documents_close_phase1_schema_and_authority_boundaries(self):
        for path, text in self._public_documents().items():
            with self.subTest(path=path):
                normalized = " ".join(text.split())
                self.assertIn(PHASE1_IMPLEMENTATION_BASELINE, normalized)
                self.assertIn(PHASE1_CHECKPOINT_STATUS, normalized)
                self.assertIn(V02_COMPLETION_STATUS, normalized)
                self.assertIn(LEGACY_BYTE_PRESERVATION, normalized)
                self.assertIn(ZERO_SAMPLING_BOUNDARY, normalized)
                self.assertIn(BACKLOG_AUTHORITY_BOUNDARY, normalized)
                self.assertIn("test_schema_migration_acceptance.py", normalized)

    def test_public_documents_remove_stale_v02_and_phase1_checklists(self):
        documents = self._public_documents()
        roadmap = documents["ROADMAP.md"]
        todo = documents["TODO.md"]
        for stale in (
            "`supported-interpreter execution` remains pending",
            "formal acceptance execution, including the Task 11 interpreter",
            "Execute Task 11's 15-case acceptance matrix",
        ):
            with self.subTest(stale=stale):
                self.assertNotIn(stale, roadmap + todo)
        phase1_todo = todo.split(
            "## Workflow Observatory v0.3 Phase 1 checkpoint", 1
        )[1].split("\n## ", 1)[0]
        self.assertNotIn("- [ ]", phase1_todo)
        self.assertIn("## Backlog and design gates — not implementation authority", todo)

    def test_root_readme_records_exact_platform_acceptance_contract(self):
        readme = self._public_documents()["README.md"]
        normalized = " ".join(readme.split())
        for command in (
            'for py in 3.11 3.12 3.13 3.14; do',
            'caffeinate -i -m uv run --no-project --python "$py"',
            "caffeinate -i -m python3 -m unittest discover",
            'PHASE1_CANDIDATE_COMMIT="${PHASE1_CANDIDATE_COMMIT:?set reviewed checkpoint SHA}"',
            'uv run --no-project --python "$py" python -m unittest',
            "python3 -m unittest discover",
            "python3 evidence/scripts/package_workflow_observatory.py",
            "cmp evidence/dist/phase1-verify-a/",
        ):
            with self.subTest(command=command):
                self.assertIn(command, readme)
        for evidence_field in (
            "candidate commit",
            "implementation baseline",
            "distribution",
            "kernel",
            "architecture",
            "Python versions",
            "test counts, skips, failures, and exit statuses",
            "archive filenames and SHA-256 hashes",
            "byte-comparison result",
        ):
            with self.subTest(evidence_field=evidence_field):
                self.assertIn(evidence_field, normalized)
        self.assertIn(
            "evidence/dist/phase1-acceptance/<platform>/<candidate-commit>/",
            readme,
        )
        self.assertIn(
            "Pass criterion: 12/12 on every requested interpreter with no "
            "skip, the complete plugin suite with no failure, and two "
            "exact-inventory archives with byte-identical ZIP bytes.",
            normalized,
        )

    def test_public_release_and_archive_hash_language_matches_repository_state(self):
        documents = self._public_documents()
        readme = documents["README.md"]
        normalized = " ".join(readme.split())
        todo = documents["TODO.md"]
        self.assertIn(
            "The current stable GitHub release is v0.1.0; v0.2.0-rc1 is a "
            "public prerelease.",
            normalized,
        )
        self.assertIn("- [x] Publish the public MIT repository.", todo)
        self.assertIn("- [x] Publish the stable v0.1.0 release archive.", todo)
        self.assertIn(
            "Manual filesystem deletion or copying is an operator action, not "
            "a product delete/export operation.",
            normalized,
        )
        self.assertIn(
            "A ZIP archive hash must be recorded outside the ZIP in local or "
            "independently published acceptance evidence; `SHA256SUMS.json` "
            "authenticates members but cannot authenticate its containing "
            "archive.",
            normalized,
        )

    def test_archive_rejects_unlisted_v03_phase1_document_sibling(self):
        sibling = self.source / "docs/superpowers/specs/unlisted-v0.3-sibling.md"
        sibling.write_text("# Unlisted sibling\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PackageError,
            "unexpected marketplace file: "
            "docs/superpowers/specs/unlisted-v0.3-sibling.md",
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_unlisted_plugin_document(self):
        sibling = self.source / "plugins/workflow-observer/docs/unlisted.md"
        sibling.parent.mkdir(parents=True, exist_ok=True)
        sibling.write_text("# Unlisted plugin document\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PackageError,
            "unexpected marketplace file: "
            "plugins/workflow-observer/docs/unlisted.md",
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_unlisted_plugin_script(self):
        sibling = self.source / "plugins/workflow-observer/scripts/unlisted.py"
        sibling.write_text("raise SystemExit('unlisted')\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PackageError,
            "unexpected marketplace file: "
            "plugins/workflow-observer/scripts/unlisted.py",
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_unlisted_plugin_test(self):
        sibling = self.source / "plugins/workflow-observer/tests/test_unlisted.py"
        sibling.write_text("raise AssertionError('unlisted')\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PackageError,
            "unexpected marketplace file: "
            "plugins/workflow-observer/tests/test_unlisted.py",
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_unlisted_migration_fixture_sibling(self):
        sibling = (
            self.source
            / "plugins/workflow-observer/tests/fixtures/"
            "unapproved_migration_vectors.json"
        )
        sibling.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            PackageError,
            "unexpected marketplace file: plugins/workflow-observer/tests/"
            "fixtures/unapproved_migration_vectors.json",
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_public_main_builds_from_live_v02_source_inventory(self):
        destination = self.root / "public-main.zip"
        real_build_archive = build_archive

        def redirect_destination(source_root, _destination, evidence):
            return real_build_archive(source_root, destination, evidence)

        with mock.patch(
            "package_workflow_observatory.build_archive",
            side_effect=redirect_destination,
        ), mock.patch("builtins.print") as output:
            self.assertEqual(0, main(["--version", "review-test"]))
        output.assert_called_once()

        self._assert_live_v02_inventory(destination)

    def test_live_staging_excludes_only_development_roots(self):
        excluded = {".git", ".codegraph", ".superpowers", "evidence", "dist"}
        for name in excluded:
            path = self.source / name
            path.mkdir(exist_ok=True)
            (path / "private-state").write_text("not packaged\n", encoding="utf-8")
        unexpected = self.source / "secret.env"
        unexpected.write_text("TOKEN=secret\n", encoding="utf-8")

        staging_parent = self.root / "staging-parent"
        staging_parent.mkdir()
        staged = packager._stage_live_marketplace(self.source, staging_parent)

        for name in excluded:
            with self.subTest(name=name):
                self.assertFalse((staged / name).exists())
        self.assertTrue((staged / "secret.env").is_file())
        with self.assertRaisesRegex(PackageError, "unexpected marketplace file"):
            build_archive(staged, self.archive, self.evidence)

    def test_live_staging_preserves_symlinks_for_packager_rejection(self):
        outside = self.root / "outside.txt"
        outside.write_text("outside\n", encoding="utf-8")
        (self.source / "linked").symlink_to(outside)
        staging_parent = self.root / "symlink-staging-parent"
        staging_parent.mkdir()

        staged = packager._stage_live_marketplace(self.source, staging_parent)

        self.assertTrue((staged / "linked").is_symlink())
        with self.assertRaisesRegex(PackageError, "symlink"):
            build_archive(staged, self.archive, self.evidence)

    def test_policy_allowlist_rejects_non_json_and_nested_files(self):
        policies = self.source / "plugins/workflow-observer/policies"
        for relative in ("notes.md", "nested/unapproved.json"):
            with self.subTest(relative=relative):
                unexpected = policies / relative
                unexpected.parent.mkdir(parents=True, exist_ok=True)
                unexpected.write_text("{}\n", encoding="utf-8")
                with self.assertRaisesRegex(
                    PackageError, "unexpected marketplace file"
                ):
                    build_archive(self.source, self.archive, self.evidence)
                unexpected.unlink()

    def test_archive_contains_exact_artifact_policy_inventory(self):
        build_archive(self.source, self.archive, self.evidence)
        marketplace = self._inventory()["marketplace_files"]
        expected = {
            "plugins/workflow-observer/policies/"
            "artifact_migration_registry.json",
            "plugins/workflow-observer/policies/artifact_schema_registry.json",
            "plugins/workflow-observer/policies/health_event_schema.json",
            "plugins/workflow-observer/scripts/artifact_migration.py",
            "plugins/workflow-observer/scripts/artifact_schema.py",
            "plugins/workflow-observer/tests/fixtures/"
            "artifact_migration_vectors.json",
            "plugins/workflow-observer/tests/test_artifact_migration.py",
            "plugins/workflow-observer/tests/test_artifact_schema.py",
        }
        self.assertLessEqual(expected, set(marketplace))

    def test_parallel_worker_sources_are_captured_reproducibly(self):
        second = self.root / "parallel-worker-second.zip"
        evidence = default_evidence(REPOSITORY_ROOT)
        required = {
            "scripts/check_parallel_eval_frozen_boundary.py",
            "scripts/workflow_eval_sharding.py",
            "scripts/run_observing_workflows_eval_worker.py",
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py",
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/test_parallel_eval_runner.py",
            "tests/run_parallel_eval_no_model_coordinator.py",
            "tests/run_parallel_eval_no_model_worker.py",
            "tests/test_parallel_eval_no_model_integration.py",
            "tests/test_parallel_eval_frozen_boundary.py",
            "tests/test_workflow_eval_sharding.py",
        }
        first_digest = build_archive(self.source, self.archive, evidence)
        second_digest = build_archive(self.source, second, evidence)
        self.assertEqual(first_digest, second_digest)
        self.assertEqual(self.archive.read_bytes(), second.read_bytes())
        with zipfile.ZipFile(self.archive) as bundle:
            names = set(bundle.namelist())
        for relative_path in required:
            self.assertIn(
                f"workflow-observatory/evidence/{relative_path}", names
            )

    def test_archive_rejects_symlinks_and_unexpected_files(self):
        outside = self.root / "outside.txt"
        outside.write_text("outside", encoding="utf-8")
        (self.source / "linked").symlink_to(outside)
        with self.assertRaisesRegex(PackageError, "symlink"):
            build_archive(self.source, self.archive, self.evidence)

        (self.source / "linked").unlink()
        (self.source / "secret.env").write_text("TOKEN=secret", encoding="utf-8")
        with self.assertRaisesRegex(PackageError, "unexpected marketplace file"):
            build_archive(self.source, self.archive, self.evidence)

    def test_verify_rejects_tampered_member(self):
        build_archive(self.source, self.archive, self.evidence)
        tampered = self.root / "tampered.zip"
        shutil.copy2(self.archive, tampered)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(tampered, "a") as bundle:
                bundle.writestr("workflow-observatory/README.md", b"tampered")
        with self.assertRaises(PackageError):
            verify_archive(tampered)

    def test_verify_rejects_incomplete_completeness_mapping(self):
        build_archive(self.source, self.archive, self.evidence)
        tampered = self.root / "incomplete-inventory.zip"
        with zipfile.ZipFile(self.archive) as source, zipfile.ZipFile(
            tampered, "w"
        ) as target:
            for info in source.infolist():
                data = source.read(info.filename)
                if info.filename == "workflow-observatory/SHA256SUMS.json":
                    inventory = json.loads(data)
                    inventory["marketplace_files"].pop(
                        ".agents/plugins/marketplace.json"
                    )
                    data = (
                        json.dumps(
                            inventory,
                            ensure_ascii=False,
                            indent=2,
                            sort_keys=True,
                        ).encode("utf-8")
                        + b"\n"
                    )
                target.writestr(info, data)
        with self.assertRaisesRegex(PackageError, "completeness mapping"):
            verify_archive(tampered)

    def test_verify_rejects_origin_member_mapping_swap(self):
        build_archive(self.source, self.archive, self.evidence)
        tampered = self.root / "swapped-origin-inventory.zip"
        with zipfile.ZipFile(self.archive) as source, zipfile.ZipFile(
            tampered, "w"
        ) as target:
            for info in source.infolist():
                data = source.read(info.filename)
                if info.filename == "workflow-observatory/SHA256SUMS.json":
                    inventory = json.loads(data)
                    rows = inventory["marketplace_files"]
                    left = ".agents/plugins/marketplace.json"
                    right = ".gitignore"
                    rows[left], rows[right] = rows[right], rows[left]
                    data = (
                        json.dumps(
                            inventory,
                            ensure_ascii=False,
                            indent=2,
                            sort_keys=True,
                        ).encode("utf-8")
                        + b"\n"
                    )
                target.writestr(info, data)
        with self.assertRaisesRegex(PackageError, "origin-member mapping"):
            verify_archive(tampered)

    def test_verify_rejects_invalid_completeness_metadata(self):
        build_archive(self.source, self.archive, self.evidence)
        mutations = {
            "source-digest": lambda row: row.__setitem__(
                "source_sha256", "not-a-digest"
            ),
            "normalized-type": lambda row: row.__setitem__(
                "normalized", 1
            ),
            "normalization-label": lambda row: (
                row.__setitem__("normalized", True),
                row.__setitem__("normalizations", ["unknown-normalization"]),
            ),
            "normalization-consistency": lambda row: row.__setitem__(
                "normalized", not bool(row["normalizations"])
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                tampered = self.root / f"invalid-{label}.zip"
                with zipfile.ZipFile(self.archive) as source, zipfile.ZipFile(
                    tampered, "w"
                ) as target:
                    for info in source.infolist():
                        data = source.read(info.filename)
                        if info.filename == "workflow-observatory/SHA256SUMS.json":
                            inventory = json.loads(data)
                            row = inventory["marketplace_files"]["README.md"]
                            mutate(row)
                            data = (
                                json.dumps(
                                    inventory,
                                    ensure_ascii=False,
                                    indent=2,
                                    sort_keys=True,
                                ).encode("utf-8")
                                + b"\n"
                            )
                        target.writestr(info, data)
                with self.assertRaisesRegex(
                    PackageError, "completeness metadata"
                ):
                    verify_archive(tampered)

    def test_packaged_packager_verifies_its_source_archive(self):
        build_archive(self.source, self.archive, default_evidence(REPOSITORY_ROOT))
        extracted = self.root / "extracted"
        with zipfile.ZipFile(self.archive) as bundle:
            bundle.extractall(extracted)
        packaged_script = (
            extracted
            / "workflow-observatory/evidence/scripts/"
            "package_workflow_observatory.py"
        )
        result = subprocess.run(
            [sys.executable, str(packaged_script), "--verify", str(self.archive)],
            cwd=extracted / "workflow-observatory",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_archive_rejects_unrecognized_user_home_paths(self):
        readme = self.source / "README.md"
        readme.write_text(
            readme.read_text(encoding="utf-8")
            + "\nprivate=" + "/" + "Users/bob/secret\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PackageError, "personal path"):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_synthetic_path_prefix_extension(self):
        readme = self.source / "README.md"
        readme.write_text(
            readme.read_text(encoding="utf-8")
            + "\nprivate=" + "/" + "Users/alice/private/repo-real-secret\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PackageError, "personal path"):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_normalization_outside_declared_paths(self):
        notice = self.source / "NOTICE.md"
        notice.write_text(
            notice.read_text(encoding="utf-8")
            + f"\nundeclared={Path.home() / 'private'}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PackageError, "undeclared path normalization"):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_rejects_user_home_prefix_collision(self):
        plan = self.source / "docs/parallel-evaluation-mvp-implementation-plan.md"
        plan.write_text(
            plan.read_text(encoding="utf-8")
            + f"\nprivate={Path.home()}-backup/private.txt\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PackageError, "personal path"):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_normalizes_codex_prefix_collision_via_home(self):
        plan = self.source / "docs/parallel-evaluation-mvp-implementation-plan.md"
        plan.write_text(
            plan.read_text(encoding="utf-8")
            + f"\n"
            + f"codex-backup={Path.home() / '.codex-backup' / 'private.txt'}\n"
            + f"codex-exact={Path.home() / '.codex' / 'skills'}\n"
            + f"home-exact={Path.home() / 'private.txt'}\n",
            encoding="utf-8",
        )
        build_archive(self.source, self.archive, self.evidence)
        member = (
            "workflow-observatory/docs/"
            "parallel-evaluation-mvp-implementation-plan.md"
        )
        with zipfile.ZipFile(self.archive) as bundle:
            packaged = bundle.read(member)
        self.assertIn(b"codex-backup=${HOME}/.codex-backup/private.txt", packaged)
        self.assertNotIn(b"${CODEX_HOME}-backup", packaged)
        self.assertIn(b"codex-exact=${CODEX_HOME}/skills", packaged)
        self.assertIn(b"home-exact=${HOME}/private.txt", packaged)

    def test_archive_fsyncs_regular_file_before_directory_replace(self):
        modes = []
        real_fsync = os.fsync

        def record_fsync(descriptor):
            modes.append(os.fstat(descriptor).st_mode)
            real_fsync(descriptor)

        with mock.patch("package_workflow_observatory.os.fsync", record_fsync):
            build_archive(self.source, self.archive, self.evidence)
        self.assertGreaterEqual(len(modes), 2)
        self.assertTrue(stat.S_ISREG(modes[-2]))
        self.assertTrue(stat.S_ISDIR(modes[-1]))

    def test_default_evidence_covers_required_artifact_classes(self):
        relative = {
            path.relative_to(REPOSITORY_ROOT).as_posix()
            for path in default_evidence(REPOSITORY_ROOT)
        }
        required = {
            "AGENTS.md",
            "wiki/concept/Workflow_Observation_and_Process_Knowledge.md",
            "docs/superpowers/specs/2026-07-12-observation-records-design.md",
            "docs/superpowers/specs/2026-07-15-workflow-observatory-marketplace-design.md",
            "docs/superpowers/plans/2026-07-12-observation-records.md",
            "docs/superpowers/plans/2026-07-13-observation-records-v2.md",
            "docs/superpowers/plans/2026-07-15-workflow-observatory-marketplace.md",
            "docs/superpowers/plans/2026-07-15-workflow-telemetry-best-practices-research.md",
            ".superpowers/sdd/workflow-observatory-task-6-report.md",
            "scripts/run_observing_workflows_task9_eval.py",
            "scripts/run_observing_workflows_eval_worker.py",
            "scripts/__init__.py",
            "scripts/package_workflow_observatory.py",
            "scripts/check_parallel_eval_frozen_boundary.py",
            "scripts/workflow_eval_sharding.py",
            "tests/run_parallel_eval_no_model_coordinator.py",
            "tests/run_parallel_eval_no_model_worker.py",
            "tests/test_parallel_eval_no_model_integration.py",
            "tests/test_parallel_eval_frozen_boundary.py",
            "tests/test_workflow_eval_sharding.py",
            "tests/observing_workflows_eval_harness.py",
            "tests/run_observing_workflows_eval.py",
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py",
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/test_parallel_eval_runner.py",
        }
        self.assertLessEqual(required, relative)
        self.assertTrue(any(name.startswith("tests/test_observation") for name in relative))
        self.assertTrue(
            any(name.startswith("tests/test_observing_workflows") for name in relative)
        )
        self.assertTrue(any(name.startswith("tests/skill_evals/") for name in relative))
        self.assertFalse(any(name.startswith("raw/") for name in relative))
        self.assertFalse(any(name.startswith("wiki/observations/") for name in relative))
        self.assertNotIn("tests/test_llmwiki_skill.py", relative)


if __name__ == "__main__":
    unittest.main()
