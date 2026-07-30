from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


class MarketplaceEvalRunnerHygieneTests(unittest.TestCase):
    def test_runner_identifies_the_next_formal_run_as_revision_6(self):
        runner = Path(__file__).with_name("run_marketplace_eval.py")
        text = runner.read_text(encoding="utf-8")
        self.assertIn("Run revision 6 frozen evaluations", text)
        self.assertIn("Revision 6 frozen evaluations passed", text)
        self.assertNotIn("revision 5", text.lower())

    def test_bare_help_does_not_write_repository_runner_bytecode(self):
        marketplace_tests = Path(__file__).resolve().parent
        marketplace = marketplace_tests.parents[2]
        source_repository = marketplace.parents[1]
        repository = (
            source_repository
            if (source_repository / "scripts/run_observing_workflows_task9_eval.py").is_file()
            else marketplace / "evidence"
        )
        relative_sources = (
            Path("scripts/__init__.py"),
            Path("scripts/run_observing_workflows_task9_eval.py"),
            Path("scripts/workflow_eval_sharding.py"),
            Path("tests/observing_workflows_eval_harness.py"),
            Path("tests/run_observing_workflows_eval.py"),
        )
        runner_relative = Path(
                "marketplace/workflow-observatory/plugins/"
                "workflow-observer/tests/run_marketplace_eval.py"
        )
        with tempfile.TemporaryDirectory() as temporary:
            isolated_repository = Path(temporary) / "repository"
            for relative in relative_sources:
                destination = isolated_repository / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes((repository / relative).read_bytes())
            runner = isolated_repository / runner_relative
            runner.parent.mkdir(parents=True, exist_ok=True)
            runner.write_bytes(
                (marketplace_tests / "run_marketplace_eval.py").read_bytes()
            )

            environment = os.environ.copy()
            environment.pop("PYTHONDONTWRITEBYTECODE", None)
            environment.pop("PYTHONPYCACHEPREFIX", None)
            result = subprocess.run(
                [sys.executable, str(runner), "--help"],
                cwd=isolated_repository,
                env=environment,
                text=True,
                capture_output=True,
            )

            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("--preflight", result.stdout)
            repository_runner_caches = list(
                (isolated_repository / "scripts").glob(
                    "__pycache__/run_observing_workflows_task9_eval.*.pyc"
                )
            )
            self.assertEqual([], repository_runner_caches)

    def test_parallel_cli_exposes_no_test_driver_override(self):
        runner = Path(__file__).with_name("run_marketplace_eval.py")
        result = subprocess.run(
            [sys.executable, str(runner), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )
        text = runner.read_text(encoding="utf-8")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--parallel {diagnostic,discovery,formal}", result.stdout)
        self.assertIn("--resume-run-root", result.stdout)
        for forbidden in (
            "--test-driver",
            "--worker-command",
            "--coordinator-dependencies",
            "CoordinatorDependencies",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, result.stdout)
                self.assertNotIn(forbidden, text)

    def test_parallel_public_document_mirrors_preserve_evidence_boundary(self):
        marketplace_tests = Path(__file__).resolve().parent
        marketplace = marketplace_tests.parents[2]
        source_repository = marketplace.parents[1]
        evidence = (
            source_repository
            if (
                source_repository
                / "scripts/run_observing_workflows_task9_eval.py"
            ).is_file()
            else marketplace / "evidence"
        )
        repository = evidence.parent
        packaged = evidence / "marketplace/workflow-observatory"
        pairs = (
            ("README.md", "README.md"),
            ("ROADMAP.md", "ROADMAP.md"),
            ("TODO.md", "TODO.md"),
            (
                "docs/parallel-evaluation-plan.md",
                "docs/parallel-evaluation-plan.md",
            ),
            (
                "plugins/workflow-observer/README.md",
                "plugins/workflow-observer/README.md",
            ),
        )
        contents = {}
        for source_name, packaged_name in pairs:
            with self.subTest(source_name=source_name):
                source = repository / source_name
                mirror = packaged / packaged_name
                self.assertEqual(source.read_bytes(), mirror.read_bytes())
                contents[source_name] = source.read_text(encoding="utf-8")

        readme = " ".join(contents["README.md"].split())
        roadmap = " ".join(contents["ROADMAP.md"].split())
        todo = " ".join(contents["TODO.md"].split())
        plan = " ".join(
            contents["docs/parallel-evaluation-plan.md"].split()
        )
        self.assertIn(
            "deterministic 28-case no-model gate",
            readme,
        )
        self.assertIn(
            "not a real-model 28/28 result",
            readme,
        )
        self.assertIn(
            "protected real-model formal epoch remains pending",
            roadmap,
        )
        self.assertIn(
            "explicit approval before one protected real-model formal epoch",
            todo,
        )
        self.assertIn(
            "Discovery remains non-authoritative",
            plan,
        )


if __name__ == "__main__":
    unittest.main()
