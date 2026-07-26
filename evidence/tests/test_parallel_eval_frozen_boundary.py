import shutil
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from scripts import check_parallel_eval_frozen_boundary as boundary


EXPECTED_ALLOWED_IMPLEMENTATION_PATHS = frozenset(
    {
        "README.md",
        "ROADMAP.md",
        "TODO.md",
        "docs/parallel-evaluation-mvp-implementation-plan.md",
        "docs/parallel-evaluation-plan.md",
        "evidence/scripts/check_parallel_eval_frozen_boundary.py",
        "evidence/scripts/package_workflow_observatory.py",
        "evidence/scripts/run_observing_workflows_eval_worker.py",
        "evidence/scripts/run_observing_workflows_task9_eval.py",
        "evidence/scripts/workflow_eval_sharding.py",
        "evidence/tests/run_parallel_eval_no_model_coordinator.py",
        "evidence/tests/run_parallel_eval_no_model_worker.py",
        "evidence/tests/test_observing_workflows_task9_eval.py",
        "evidence/tests/test_parallel_eval_frozen_boundary.py",
        "evidence/tests/test_parallel_eval_no_model_integration.py",
        "evidence/tests/test_workflow_eval_sharding.py",
        "plugins/workflow-observer/tests/run_marketplace_eval.py",
        "plugins/workflow-observer/tests/test_eval_runner_hygiene.py",
        "plugins/workflow-observer/tests/test_package_archive.py",
        "plugins/workflow-observer/tests/test_parallel_eval_runner.py",
        "evidence/marketplace/workflow-observatory/README.md",
        "evidence/marketplace/workflow-observatory/ROADMAP.md",
        "evidence/marketplace/workflow-observatory/TODO.md",
        "evidence/marketplace/workflow-observatory/docs/parallel-evaluation-mvp-implementation-plan.md",
        "evidence/marketplace/workflow-observatory/docs/parallel-evaluation-plan.md",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/run_marketplace_eval.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_eval_runner_hygiene.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_package_archive.py",
        "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/test_parallel_eval_runner.py",
    }
)
EXPECTED_MANIFEST_PATHS = (
    "evidence/tests/skill_evals/observing_workflows_cases.json",
    "evidence/tests/skill_evals/observing_workflows_lifecycle_cases.json",
    "plugins/workflow-observer/tests/skill_evals/observing_workflows_cases.json",
    "plugins/workflow-observer/tests/skill_evals/observing_workflows_lifecycle_cases.json",
    "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals/observing_workflows_cases.json",
    "evidence/marketplace/workflow-observatory/plugins/workflow-observer/tests/skill_evals/observing_workflows_lifecycle_cases.json",
)
EXPECTED_FROZEN_BYTE_PATHS = (
    *EXPECTED_MANIFEST_PATHS,
    "SHA256SUMS.json",
    "evidence/tests/run_observing_workflows_eval.py",
)
EXPECTED_AST_BINDINGS = {
    "evidence/tests/run_observing_workflows_eval.py": (
        "DECISION_MANIFEST_FIELDS",
        "LIFECYCLE_MANIFEST_FIELDS",
        "RESULT_SCHEMAS",
    ),
    "evidence/scripts/run_observing_workflows_task9_eval.py": (
        "EXEC_TURN_TIMEOUT_SECONDS",
        "APP_SERVER_TURN_TIMEOUT_SECONDS",
        "GATE_TIMEOUT_SECONDS",
        "FROZEN_MANIFEST_HASHES",
        "FROZEN_MANIFEST_IDS",
    ),
}
FORBIDDEN_RESULT_BASENAMES = (
    "observing_workflows_forward.json",
    "observing_workflows_lifecycle_forward.json",
    "observing_workflows_results_commit.json",
)


EVALUATOR_SOURCE = b"""\
DECISION_MANIFEST_FIELDS = {"id", "turns"}
LIFECYCLE_MANIFEST_FIELDS = {"id", "mode"}
RESULT_SCHEMAS = {"forward": {"id", "decisions"}}
"""

TASK9_SOURCE = b"""\
EXEC_TURN_TIMEOUT_SECONDS = 20 * 60
APP_SERVER_TURN_TIMEOUT_SECONDS = 10 * 60
GATE_TIMEOUT_SECONDS = 5 * 60
FROZEN_MANIFEST_HASHES = {"forward": "forward-hash", "lifecycle": "lifecycle-hash"}
FROZEN_MANIFEST_IDS = {"forward": ("forward-case",), "lifecycle": ("lifecycle-case",)}
"""


class FrozenBoundaryTests(unittest.TestCase):
    def _write(self, root, relative_path, payload):
        target = root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)

    def _make_base_tree(self, root):
        for index, path in enumerate(EXPECTED_FROZEN_BYTE_PATHS):
            if path == "evidence/tests/run_observing_workflows_eval.py":
                payload = EVALUATOR_SOURCE
            elif path == "SHA256SUMS.json":
                payload = b'{"archive_root":"workflow-observatory"}\n'
            else:
                payload = f"manifest-{index}\n".encode("ascii")
            self._write(root, path, payload)
        self._write(
            root,
            "evidence/scripts/run_observing_workflows_task9_eval.py",
            TASK9_SOURCE,
        )
        self._write(root, "LICENSE", b"frozen base file\n")

    def _copy_base_tree(self, temporary_root, name):
        base_tree = temporary_root / "base"
        if not base_tree.exists():
            self._make_base_tree(base_tree)
        head_tree = temporary_root / name
        shutil.copytree(base_tree, head_tree)
        return base_tree, head_tree

    def test_allowlist_and_frozen_contract_are_disjoint(self):
        self.assertEqual(
            boundary.ALLOWED_IMPLEMENTATION_PATHS,
            EXPECTED_ALLOWED_IMPLEMENTATION_PATHS,
        )
        self.assertEqual(boundary.FROZEN_MANIFEST_PATHS, EXPECTED_MANIFEST_PATHS)
        self.assertEqual(boundary.FROZEN_BYTE_PATHS, EXPECTED_FROZEN_BYTE_PATHS)
        self.assertEqual(boundary.FROZEN_AST_BINDINGS, EXPECTED_AST_BINDINGS)
        self.assertTrue(
            boundary.ALLOWED_IMPLEMENTATION_PATHS.isdisjoint(
                boundary.FROZEN_BYTE_PATHS
            )
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            base_tree, allowed_head = self._copy_base_tree(
                temporary_root, "allowed-head"
            )
            allowed_path = (
                allowed_head
                / "evidence/scripts/run_observing_workflows_task9_eval.py"
            )
            allowed_path.write_bytes(allowed_path.read_bytes() + b"# allowed edit\n")
            self.assertEqual(boundary.compare_trees(base_tree, allowed_head), [])

            unallowlisted_head = temporary_root / "unallowlisted-head"
            shutil.copytree(base_tree, unallowlisted_head)
            self._write(unallowlisted_head, "LICENSE", b"changed\n")
            errors = boundary.compare_trees(base_tree, unallowlisted_head)
            self.assertIn("LICENSE", "\n".join(errors))

            for index, frozen_path in enumerate(EXPECTED_FROZEN_BYTE_PATHS):
                with self.subTest(frozen_path=frozen_path):
                    frozen_head = temporary_root / f"frozen-head-{index}"
                    shutil.copytree(base_tree, frozen_head)
                    target = frozen_head / frozen_path
                    target.write_bytes(target.read_bytes() + b"changed\n")
                    errors = boundary.compare_trees(base_tree, frozen_head)
                    self.assertIn(frozen_path, "\n".join(errors))

    def test_ordinary_add_delete_and_frozen_delete_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            base_tree, added_head = self._copy_base_tree(temporary_root, "added")
            self._write(added_head, "unauthorized.txt", b"added\n")
            errors = boundary.compare_trees(base_tree, added_head)
            self.assertIn("unauthorized changed path: unauthorized.txt", errors)

            _, deleted_head = self._copy_base_tree(temporary_root, "deleted")
            (deleted_head / "LICENSE").unlink()
            errors = boundary.compare_trees(base_tree, deleted_head)
            self.assertIn("unauthorized changed path: LICENSE", errors)

            _, frozen_deleted_head = self._copy_base_tree(
                temporary_root, "frozen-deleted"
            )
            frozen_path = EXPECTED_FROZEN_BYTE_PATHS[0]
            (frozen_deleted_head / frozen_path).unlink()
            errors = boundary.compare_trees(base_tree, frozen_deleted_head)
            self.assertIn(f"frozen byte path missing from head: {frozen_path}", errors)

    def test_ast_bindings_and_forbidden_result_paths_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            ast_mutations = (
                (
                    "evidence/tests/run_observing_workflows_eval.py",
                    "DECISION_MANIFEST_FIELDS",
                    b'{"id", "turns"}',
                    b'{"changed", "turns"}',
                ),
                (
                    "evidence/tests/run_observing_workflows_eval.py",
                    "LIFECYCLE_MANIFEST_FIELDS",
                    b'{"id", "mode"}',
                    b'{"changed", "mode"}',
                ),
                (
                    "evidence/tests/run_observing_workflows_eval.py",
                    "RESULT_SCHEMAS",
                    b'{"forward": {"id", "decisions"}}',
                    b'{"forward": {"changed", "decisions"}}',
                ),
                (
                    "evidence/scripts/run_observing_workflows_task9_eval.py",
                    "EXEC_TURN_TIMEOUT_SECONDS",
                    b"20 * 60",
                    b"19 * 60",
                ),
                (
                    "evidence/scripts/run_observing_workflows_task9_eval.py",
                    "APP_SERVER_TURN_TIMEOUT_SECONDS",
                    b"10 * 60",
                    b"9 * 60",
                ),
                (
                    "evidence/scripts/run_observing_workflows_task9_eval.py",
                    "GATE_TIMEOUT_SECONDS",
                    b"5 * 60",
                    b"4 * 60",
                ),
                (
                    "evidence/scripts/run_observing_workflows_task9_eval.py",
                    "FROZEN_MANIFEST_HASHES",
                    b'"forward-hash", "lifecycle": "lifecycle-hash"',
                    b'"changed-hash", "lifecycle": "lifecycle-hash"',
                ),
                (
                    "evidence/scripts/run_observing_workflows_task9_eval.py",
                    "FROZEN_MANIFEST_IDS",
                    b'("forward-case",), "lifecycle": ("lifecycle-case",)',
                    b'("changed-case",), "lifecycle": ("lifecycle-case",)',
                ),
            )
            for index, (path, binding, old, new) in enumerate(ast_mutations):
                with self.subTest(binding=binding):
                    base_tree, ast_head = self._copy_base_tree(
                        temporary_root, f"ast-head-{index}"
                    )
                    target = ast_head / path
                    source = target.read_bytes()
                    self.assertIn(old, source)
                    target.write_bytes(source.replace(old, new, 1))
                    errors = boundary.compare_trees(base_tree, ast_head)
                    self.assertIn(
                        f"frozen AST binding changed: {path}:{binding}", errors
                    )

            for index, basename in enumerate(FORBIDDEN_RESULT_BASENAMES):
                _, named_head = self._copy_base_tree(
                    temporary_root, f"named-head-{index}"
                )
                forbidden_name = f"sandbox/{basename}"
                self._write(named_head, forbidden_name, b"{}\n")
                errors = boundary.compare_trees(base_tree, named_head)
                self.assertIn(forbidden_name, "\n".join(errors))
                self.assertTrue(
                    any("forbidden result artifact" in error for error in errors)
                )

            _, generation_head = self._copy_base_tree(
                temporary_root, "generation-head"
            )
            forbidden_generation = (
                "sandbox/.observing_workflows_result_generations/result.json"
            )
            self._write(generation_head, forbidden_generation, b"{}\n")
            errors = boundary.compare_trees(base_tree, generation_head)
            self.assertIn(forbidden_generation, "\n".join(errors))
            self.assertTrue(
                any("forbidden result-generation" in error for error in errors)
            )

            _, seventh_head = self._copy_base_tree(temporary_root, "seventh-head")
            seventh_manifest = "extra/skill_evals/observing_workflows_cases.json"
            self._write(seventh_head, seventh_manifest, b"[]\n")
            errors = boundary.compare_trees(base_tree, seventh_head)
            self.assertIn(seventh_manifest, "\n".join(errors))
            self.assertTrue(
                any("seventh frozen manifest" in error for error in errors)
            )

    def test_two_tree_file_kinds_are_never_omitted_or_conflated(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            base_tree, fifo_head = self._copy_base_tree(temporary_root, "fifo")
            fifo_path = fifo_head / "unauthorized.fifo"
            os.mkfifo(fifo_path)
            errors = boundary.compare_trees(base_tree, fifo_head)
            self.assertIn("unauthorized.fifo", "\n".join(errors))
            self.assertTrue(any("special file" in error for error in errors))

            _, broken_head = self._copy_base_tree(temporary_root, "broken")
            os.symlink("missing-target", broken_head / "broken-link")
            errors = boundary.compare_trees(base_tree, broken_head)
            self.assertIn("broken-link", "\n".join(errors))

            _, directory_head = self._copy_base_tree(temporary_root, "directory")
            real_directory = directory_head / "real-directory"
            real_directory.mkdir()
            self._write(real_directory, "secret.txt", b"not traversed through link\n")
            os.symlink("real-directory", directory_head / "linked-directory")
            errors = boundary.compare_trees(base_tree, directory_head)
            joined_errors = "\n".join(errors)
            self.assertIn("linked-directory", joined_errors)
            self.assertNotIn("linked-directory/secret.txt", joined_errors)

            _, replacement_head = self._copy_base_tree(
                temporary_root, "replacement"
            )
            license_path = replacement_head / "LICENSE"
            license_path.write_bytes(b"target")
            (base_tree / "LICENSE").write_bytes(b"target")
            license_path.unlink()
            os.symlink("target", license_path)
            errors = boundary.compare_trees(base_tree, replacement_head)
            self.assertIn("unauthorized changed path: LICENSE", errors)

    def test_git_range_mode_compares_tracked_blobs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository = Path(temporary_directory) / "repository"
            repository.mkdir()
            self._make_base_tree(repository)

            def git(*arguments):
                return subprocess.run(
                    ["git", "-C", str(repository), *arguments],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                ).stdout.strip()

            git("init", "-q")
            git("config", "user.name", "Boundary Test")
            git("config", "user.email", "boundary@example.invalid")
            git("add", ".")
            git("commit", "-qm", "base")
            base_revision = git("rev-parse", "HEAD")

            task9_path = (
                repository
                / "evidence/scripts/run_observing_workflows_task9_eval.py"
            )
            task9_path.write_bytes(task9_path.read_bytes() + b"# allowed edit\n")
            git("add", str(task9_path.relative_to(repository)))
            git("commit", "-qm", "allowed")
            head_revision = git("rev-parse", "HEAD")

            self.assertEqual(
                boundary.compare_git_range(
                    repository, base_revision, head_revision
                ),
                [],
            )

            script = Path(boundary.__file__).resolve()
            direct_command = [
                sys.executable,
                str(script),
                "--repository",
                str(repository),
                "--base",
                base_revision,
                "--head",
                head_revision,
            ]
            direct = subprocess.run(
                direct_command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(direct.returncode, 0, direct.stderr)

            plan_gate = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--base",
                    base_revision,
                    "--head",
                    head_revision,
                ],
                cwd=repository / "evidence",
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(plan_gate.returncode, 0, plan_gate.stderr)

            explicit = subprocess.run(
                direct_command[:2] + ["git-range", *direct_command[2:]],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(explicit.returncode, 0, explicit.stderr)

            (repository / "LICENSE").write_text("changed\n", encoding="utf-8")
            git("add", "LICENSE")
            git("commit", "-qm", "unauthorized")
            unauthorized_revision = git("rev-parse", "HEAD")
            rejected = subprocess.run(
                direct_command[:-1] + [unauthorized_revision],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(rejected.returncode, 1)
            self.assertIn("LICENSE", rejected.stderr)

    def test_two_tree_cli_returns_success_and_failure(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = Path(temporary_directory)
            base_tree, head_tree = self._copy_base_tree(temporary_root, "head")
            script = Path(boundary.__file__).resolve()
            command = [
                sys.executable,
                str(script),
                "trees",
                "--base-tree",
                str(base_tree),
                "--head-tree",
                str(head_tree),
            ]
            accepted = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)

            (head_tree / "LICENSE").unlink()
            rejected = subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(rejected.returncode, 1)
            self.assertIn("LICENSE", rejected.stderr)


if __name__ == "__main__":
    unittest.main()
