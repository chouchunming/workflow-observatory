from pathlib import Path
import hashlib
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

    def test_parallel_public_documents_preserve_current_and_frozen_boundaries(self):
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
        documents = (
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
        contents = {
            source_name: (repository / source_name).read_text(encoding="utf-8")
            for source_name, _ in documents
        }
        frozen_sha256 = {
            "README.md": "0b2ed4d2a4b0e24ef06541c8ac04767802113de1b037e2bd95861c4d4ccda43f",
            "ROADMAP.md": "7eeb4457e26d134c6d50e442077860244d5d48766b2cb136bf9a8794efce813d",
            "TODO.md": "20dd328f2a1fb42682a706a251388bd3af2631409175491931eaf090b304defe",
            "docs/parallel-evaluation-plan.md": "90192cdd4b835a81de7c07fc7ead37102140a57424108069e925e10c55bd7901",
            "plugins/workflow-observer/README.md": "40e60592ff217924324a996471eb1f2d499b5b79cf5dd0d5a8e2ef0859ce200b",
        }
        for _, packaged_name in documents:
            with self.subTest(packaged_name=packaged_name):
                frozen = packaged / packaged_name
                self.assertEqual(
                    frozen_sha256[packaged_name],
                    hashlib.sha256(frozen.read_bytes()).hexdigest(),
                )

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
