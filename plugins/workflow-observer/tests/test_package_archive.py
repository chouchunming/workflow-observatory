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
_SOURCE_REPOSITORY = _TEST_PATH.parents[5]
_SOURCE_MARKETPLACE = _SOURCE_REPOSITORY / "marketplace/workflow-observatory"
_CHECKOUT_REPOSITORY = _TEST_PATH.parents[3] / "evidence"
_CHECKOUT_MARKETPLACE = _CHECKOUT_REPOSITORY / "marketplace/workflow-observatory"
if _SOURCE_MARKETPLACE.is_dir():
    REPOSITORY_ROOT = _SOURCE_REPOSITORY
    MARKETPLACE_ROOT = _SOURCE_MARKETPLACE
elif _CHECKOUT_MARKETPLACE.is_dir():
    REPOSITORY_ROOT = _CHECKOUT_REPOSITORY
    MARKETPLACE_ROOT = _CHECKOUT_MARKETPLACE
else:
    MARKETPLACE_ROOT = _TEST_PATH.parents[3]
    REPOSITORY_ROOT = MARKETPLACE_ROOT / "evidence"
sys.path.insert(0, str(REPOSITORY_ROOT / "scripts"))

from package_workflow_observatory import (
    PackageError,
    build_archive,
    default_evidence,
    verify_archive,
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
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
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

    def test_repository_git_metadata_is_ignored(self):
        git_dir = self.source / ".git" / "refs" / "heads"
        git_dir.mkdir(parents=True, exist_ok=True)
        (self.source / ".git" / "HEAD").write_text(
            "ref: refs/heads/main\n", encoding="utf-8"
        )
        (git_dir / "main").write_text("deadbeef\n", encoding="utf-8")

        digest = build_archive(self.source, self.archive, self.evidence)

        self.assertEqual(digest, verify_archive(self.archive))

    def test_repository_git_file_is_ignored(self):
        (self.source / ".git").write_text(
            "gitdir: ../checkout.git/worktrees/release\n", encoding="utf-8"
        )

        digest = build_archive(self.source, self.archive, self.evidence)

        self.assertEqual(digest, verify_archive(self.archive))

    def test_repository_git_directory_symlink_is_rejected(self):
        outside = self.root / "outside-git"
        outside.mkdir()
        (self.source / ".git").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(
            PackageError, r"symlink is forbidden in marketplace: \.git$"
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_repository_git_entry_symlink_is_rejected(self):
        git_dir = self.source / ".git"
        git_dir.mkdir()
        outside = self.root / "outside-head"
        outside.write_text("ref: refs/heads/main\n", encoding="utf-8")
        (git_dir / "HEAD").symlink_to(outside)

        with self.assertRaisesRegex(
            PackageError, r"symlink is forbidden in marketplace: \.git/HEAD$"
        ):
            build_archive(self.source, self.archive, self.evidence)

    def test_archive_still_rejects_unrelated_dotfile(self):
        (self.source / ".unexpected").write_text(
            "must still fail\n", encoding="utf-8"
        )

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
            "scripts/__init__.py",
            "scripts/package_workflow_observatory.py",
            "tests/observing_workflows_eval_harness.py",
            "tests/run_observing_workflows_eval.py",
            "marketplace/workflow-observatory/plugins/workflow-observer/"
            "tests/run_marketplace_eval.py",
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
