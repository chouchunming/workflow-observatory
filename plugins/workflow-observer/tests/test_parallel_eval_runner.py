import io
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


def _load_runner():
    runner = Path(__file__).with_name("run_marketplace_eval.py")
    namespace = runpy.run_path(str(runner))
    return runner, namespace, namespace["main"].__globals__


class ParallelMarketplaceEvalRunnerTests(unittest.TestCase):
    def test_help_exposes_opt_in_parallel_modes_and_resume_root(self):
        runner = Path(__file__).with_name("run_marketplace_eval.py")
        result = subprocess.run(
            [sys.executable, str(runner), "--help"],
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("--parallel {diagnostic,discovery,formal}", result.stdout)
        self.assertIn("--resume-run-root", result.stdout)

    def test_resume_root_requires_an_explicit_parallel_mode(self):
        runner, namespace, runtime_globals = _load_runner()
        legacy_suite = mock.Mock(
            side_effect=AssertionError(
                "resume without --parallel reached the legacy suite"
            )
        )
        with mock.patch.object(
            sys,
            "argv",
            [str(runner), "--resume-run-root", "/private/tmp/parallel-run"],
        ), mock.patch.object(
            sys, "stderr", new_callable=io.StringIO
        ) as stderr, mock.patch.dict(
            runtime_globals,
            {
                "validate_marketplace_manifest_hashes": mock.Mock(),
                "run_suite": legacy_suite,
            },
        ), self.assertRaises(SystemExit) as raised:
            namespace["main"]()

        self.assertEqual(2, raised.exception.code)
        self.assertIn(
            "--resume-run-root requires --parallel",
            stderr.getvalue(),
        )
        legacy_suite.assert_not_called()

    def test_parallel_modes_route_to_production_coordinator(self):
        runner, namespace, runtime_globals = _load_runner()
        statuses = {
            "diagnostic": "diagnostic",
            "discovery": "validated",
            "formal": "committed",
        }
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        root = Path(temporary).resolve(strict=True)
        source_codex_home = root / "source-codex-home"
        source_codex_home.mkdir(mode=0o700)
        codex_executable = root / "codex"
        codex_executable.write_text("#!/bin/sh\n", encoding="ascii")
        codex_executable.chmod(0o700)
        exact_repository = root / "repository"
        exact_repository.mkdir()

        for mode, status in statuses.items():
            with self.subTest(mode=mode):
                run_root = root / f"{mode}-run"
                run_root.mkdir(mode=0o700)
                calls = []

                def fake_parallel(**kwargs):
                    calls.append(kwargs)
                    return mock.Mock(
                        run_kind=mode,
                        run_root=run_root,
                        status=status,
                        validated=None,
                    )

                with mock.patch.object(
                    sys, "argv", [str(runner), "--parallel", mode]
                ), mock.patch.object(
                    sys, "stdout", new_callable=io.StringIO
                ) as stdout, mock.patch.dict(
                    runtime_globals["os"].environ,
                    {"CODEX_HOME": str(source_codex_home)},
                ), mock.patch.object(
                    runtime_globals["shutil"],
                    "which",
                    return_value=str(codex_executable),
                ), mock.patch.object(
                    runtime_globals["tempfile"],
                    "mkdtemp",
                    return_value=str(run_root),
                ), mock.patch.dict(
                    runtime_globals,
                    {
                        "exact_git_repository_root": (
                            lambda _start: exact_repository
                        ),
                        "run_parallel_evaluation": fake_parallel,
                        "run_suite": mock.Mock(
                            side_effect=AssertionError(
                                "parallel mode reached the legacy suite"
                            )
                        ),
                    },
                ):
                    self.assertEqual(0, namespace["main"]())

                self.assertEqual(1, len(calls))
                arguments = calls[0]
                self.assertEqual(exact_repository, arguments["repository_root"])
                self.assertEqual(
                    {"forward", "lifecycle"},
                    set(arguments["manifests"]),
                )
                self.assertIs(
                    runtime_globals["RESULT_PATHS"]
                    if mode == "formal"
                    else None,
                    arguments["result_destinations"],
                )
                self.assertNotIn("dependencies", arguments)
                options = arguments["options"]
                self.assertIs(runtime_globals["ParallelOptions"], type(options))
                self.assertEqual(mode, options.run_kind)
                self.assertEqual(run_root, options.run_root)
                self.assertEqual(source_codex_home, options.source_codex_home)
                self.assertEqual(codex_executable, options.codex_executable)
                self.assertIsNone(options.resume_run_root)
                rendered = stdout.getvalue()
                self.assertIn(f'"run_kind": "{mode}"', rendered)
                self.assertIn(f'"status": "{status}"', rendered)
                self.assertIn(
                    '"authoritative": '
                    + ("true" if mode == "formal" else "false"),
                    rendered,
                )

    def test_parallel_resume_reuses_the_exact_run_root(self):
        runner, namespace, runtime_globals = _load_runner()
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        root = Path(temporary).resolve(strict=True)
        source_codex_home = root / "source-codex-home"
        source_codex_home.mkdir(mode=0o700)
        codex_executable = root / "codex"
        codex_executable.write_text("#!/bin/sh\n", encoding="ascii")
        codex_executable.chmod(0o700)
        run_root = root / "resume-run"
        run_root.mkdir(mode=0o700)
        exact_repository = root / "repository"
        exact_repository.mkdir()
        calls = []

        def fake_parallel(**kwargs):
            calls.append(kwargs)
            return mock.Mock(
                run_kind="discovery",
                run_root=run_root,
                status="validated",
                validated=mock.sentinel.validated,
            )

        with mock.patch.object(
            sys,
            "argv",
            [
                str(runner),
                "--parallel",
                "discovery",
                "--resume-run-root",
                str(run_root),
            ],
        ), mock.patch.object(
            sys, "stdout", new_callable=io.StringIO
        ), mock.patch.dict(
            runtime_globals["os"].environ,
            {"CODEX_HOME": str(source_codex_home)},
        ), mock.patch.object(
            runtime_globals["shutil"],
            "which",
            return_value=str(codex_executable),
        ), mock.patch.object(
            runtime_globals["tempfile"],
            "mkdtemp",
        ) as mkdtemp, mock.patch.dict(
            runtime_globals,
            {
                "exact_git_repository_root": (
                    lambda _start: exact_repository
                ),
                "run_parallel_evaluation": fake_parallel,
            },
        ):
            self.assertEqual(0, namespace["main"]())

        mkdtemp.assert_not_called()
        self.assertEqual(1, len(calls))
        options = calls[0]["options"]
        self.assertEqual(run_root, options.run_root)
        self.assertEqual(run_root, options.resume_run_root)
        self.assertIsNone(calls[0]["result_destinations"])

    def test_parallel_is_opt_in_and_legacy_default_keeps_serial_authority(self):
        runner, namespace, runtime_globals = _load_runner()
        exact_repository = Path("/private/tmp/exact-repository")
        suite_calls = []
        parallel = mock.Mock(
            side_effect=AssertionError(
                "legacy default reached parallel coordinator"
            )
        )

        def fake_suite(*args, **kwargs):
            suite_calls.append((args, kwargs))
            return [], []

        with mock.patch.object(
            sys, "argv", [str(runner)]
        ), mock.patch.object(
            sys, "stdout", new_callable=io.StringIO
        ), mock.patch.dict(
            runtime_globals,
            {
                "validate_marketplace_manifest_hashes": lambda: None,
                "exact_git_repository_root": (
                    lambda _start: exact_repository
                ),
                "MarketplaceRuntimeFactory": (
                    lambda: mock.sentinel.runtime_factory
                ),
                "run_suite": fake_suite,
                "run_parallel_evaluation": parallel,
            },
        ):
            self.assertEqual(0, namespace["main"]())

        self.assertEqual(1, len(suite_calls))
        _args, kwargs = suite_calls[0]
        self.assertEqual("serial-coordinator", kwargs["coordinator_role"])
        parallel.assert_not_called()

    def test_runner_delegates_parallel_authority_instead_of_reimplementing_it(self):
        runner, _namespace, runtime_globals = _load_runner()
        text = runner.read_text(encoding="utf-8")

        self.assertIn("run_parallel_evaluation", text)
        self.assertNotIn("claim_formal_commit", text)
        self.assertNotIn("persist_validated_epoch", text)
        self.assertNotIn("ResultWriterLease", text)
        self.assertNotIn("CoordinatorDependencies", text)
        self.assertIsNotNone(runtime_globals.get("run_parallel_evaluation"))

    def test_canonical_and_packaged_runner_and_test_mirrors_are_identical(self):
        _runner, _namespace, runtime_globals = _load_runner()
        repository = runtime_globals["REPOSITORY_ROOT"].parent
        canonical = (
            repository / "plugins/workflow-observer/tests"
        )
        packaged = repository / (
            "evidence/marketplace/workflow-observatory/"
            "plugins/workflow-observer/tests"
        )

        for name in ("run_marketplace_eval.py", "test_parallel_eval_runner.py"):
            with self.subTest(name=name):
                self.assertEqual(
                    (canonical / name).read_bytes(),
                    (packaged / name).read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
