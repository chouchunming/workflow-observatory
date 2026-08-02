import hashlib
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
    TRUSTED_ARCHIVE_SHA256 = "a" * 64

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
        self.assertIn("--archive", result.stdout)
        self.assertIn("--expected-archive-sha256", result.stdout)

    def test_every_parallel_mode_requires_external_archive_identity(self):
        runner, namespace, runtime_globals = _load_runner()
        for mode in ("diagnostic", "discovery", "formal"):
            with self.subTest(mode=mode), mock.patch.object(
                sys, "argv", [str(runner), "--parallel", mode]
            ), mock.patch.object(
                sys, "stderr", new_callable=io.StringIO
            ) as stderr, mock.patch.dict(
                runtime_globals,
                {"validate_marketplace_manifest_hashes": mock.Mock()},
            ), self.assertRaises(SystemExit) as raised:
                namespace["main"]()

            self.assertEqual(2, raised.exception.code)
            self.assertIn(
                "--expected-archive-sha256 is required with --parallel",
                stderr.getvalue(),
            )

    def test_every_parallel_mode_requires_explicit_external_archive_path(self):
        runner, namespace, runtime_globals = _load_runner()
        for mode in ("diagnostic", "discovery", "formal"):
            with self.subTest(mode=mode), mock.patch.object(
                sys,
                "argv",
                [
                    str(runner),
                    "--parallel",
                    mode,
                    "--expected-archive-sha256",
                    self.TRUSTED_ARCHIVE_SHA256,
                ],
            ), mock.patch.object(
                sys, "stderr", new_callable=io.StringIO
            ) as stderr, mock.patch.dict(
                runtime_globals,
                {"validate_marketplace_manifest_hashes": mock.Mock()},
            ), self.assertRaises(SystemExit) as raised:
                namespace["main"]()

            self.assertEqual(2, raised.exception.code)
            self.assertIn(
                "--archive is required with --parallel",
                stderr.getvalue(),
            )

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

    def test_parallel_rejects_a_symlink_archive_path(self):
        runner, namespace, runtime_globals = _load_runner()
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        root = Path(temporary).resolve(strict=True)
        target = root / "release.zip"
        target.write_bytes(b"release")
        archive = root / "release-link.zip"
        archive.symlink_to(target)
        with mock.patch.object(
            sys,
            "argv",
            [
                str(runner),
                "--parallel",
                "discovery",
                "--archive",
                str(archive),
                "--expected-archive-sha256",
                self.TRUSTED_ARCHIVE_SHA256,
            ],
        ), mock.patch.dict(
            runtime_globals,
            {"validate_marketplace_manifest_hashes": mock.Mock()},
        ), self.assertRaisesRegex(
            ValueError, "regular non-symlink file"
        ):
            namespace["main"]()

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
        archive = root / "externally-trusted.zip"
        archive.write_bytes(b"trusted archive fixture")

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
                    sys,
                    "argv",
                    [
                        str(runner),
                        "--parallel",
                        mode,
                        "--archive",
                        str(archive),
                        "--expected-archive-sha256",
                        self.TRUSTED_ARCHIVE_SHA256,
                    ],
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
                self.assertEqual(
                    {"forward": 20, "lifecycle": 8},
                    {
                        name: len(cases)
                        for name, cases in arguments["manifests"].items()
                    },
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
                self.assertEqual(
                    {
                        "run_kind",
                        "run_root",
                        "source_codex_home",
                        "codex_executable",
                        "archive_path",
                        "expected_archive_sha256",
                        "requested_model",
                        "requested_reasoning_effort",
                        "resume_run_root",
                        "max_total_tokens",
                    },
                    set(vars(options)),
                )
                self.assertEqual(mode, options.run_kind)
                self.assertEqual(run_root, options.run_root)
                self.assertEqual(source_codex_home, options.source_codex_home)
                self.assertEqual(codex_executable, options.codex_executable)
                self.assertEqual(archive, options.archive_path)
                self.assertEqual(
                    self.TRUSTED_ARCHIVE_SHA256,
                    options.expected_archive_sha256,
                )
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
        archive = root / "externally-trusted.zip"
        archive.write_bytes(b"trusted archive fixture")
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
                "--archive",
                str(archive),
                "--expected-archive-sha256",
                self.TRUSTED_ARCHIVE_SHA256,
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

    def test_fresh_run_root_is_disclosed_before_coordinator_failure(self):
        runner, namespace, runtime_globals = _load_runner()
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        root = Path(temporary).resolve(strict=True)
        source_codex_home = root / "source-codex-home"
        source_codex_home.mkdir(mode=0o700)
        codex_executable = root / "codex"
        codex_executable.write_text("#!/bin/sh\n", encoding="ascii")
        codex_executable.chmod(0o700)
        run_root = root / "failed-run"
        run_root.mkdir(mode=0o700)
        exact_repository = root / "repository"
        exact_repository.mkdir()
        archive = root / "externally-trusted.zip"
        archive.write_bytes(b"trusted archive fixture")

        with mock.patch.object(
            sys,
            "argv",
            [
                str(runner),
                "--parallel",
                "discovery",
                "--archive",
                str(archive),
                "--expected-archive-sha256",
                self.TRUSTED_ARCHIVE_SHA256,
            ],
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
                "run_parallel_evaluation": mock.Mock(
                    side_effect=RuntimeError("coordinator failed")
                ),
            },
        ), self.assertRaisesRegex(RuntimeError, "coordinator failed"):
            namespace["main"]()

        self.assertIn(
            f"Parallel discovery run root retained at {run_root}",
            stdout.getvalue(),
        )

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

    def test_parallel_diagnostic_docs_fix_target_without_model_success_claim(self):
        _runner, _namespace, runtime_globals = _load_runner()
        repository = runtime_globals["REPOSITORY_ROOT"].parent
        readmes = (
            repository / "README.md",
            repository
            / "evidence/marketplace/workflow-observatory/README.md",
        )
        fixed_target_contract = (
            "`--parallel diagnostic` runs only the fixed non-authoritative\n"
            "`forward/3 reviewed-refactor` case through the reviewed "
            "coordinator/worker\n"
            "path. It cannot select another case or persist results. "
            "Discovery and formal\n"
            "continue to use the complete frozen 20+8 inventory."
        )

        for readme in readmes:
            with self.subTest(readme=readme):
                text = readme.read_text(encoding="utf-8")
                self.assertIn(fixed_target_contract, text)
                self.assertIn(
                    "A real-model diagnostic, discovery sweep, and protected "
                    "formal\n"
                    "epoch still require their separate review and approval "
                    "gates.",
                    text,
                )
                self.assertIn(
                    "Every parallel mode requires an externally trusted full "
                    "archive SHA-256",
                    text,
                )
                self.assertIn(
                    "`--archive` must name the original absolute, regular, "
                    "non-symlink ZIP path",
                    text,
                )
                self.assertIn(
                    "before `lane-ready` or any model-capable work",
                    text,
                )
                self.assertIn(
                    "WORKFLOW_OBSERVATORY_EVAL_ARCHIVE_SHA256="
                    "'64-lowercase-hex-from-trusted-channel'",
                    text,
                )
                self.assertIn(
                    "python3 -m unittest discover -v -s tests -p 'test_*.py'",
                    text,
                )
                self.assertIn(
                    "does not establish the archive's authenticity",
                    text,
                )

    def test_frozen_runner_and_test_evidence_matches_reviewed_hashes(self):
        _runner, _namespace, runtime_globals = _load_runner()
        repository = runtime_globals["REPOSITORY_ROOT"].parent
        packaged = repository / (
            "evidence/marketplace/workflow-observatory/"
            "plugins/workflow-observer/tests"
        )

        frozen_sha256 = {
            "run_marketplace_eval.py": "eb8f2629fe4636d601a3d440c767432fb6633b3b0b8d725bc41cd927a5c1f643",
            "test_parallel_eval_runner.py": "e564b619919883de9bdd09a7a6ed2d9e6e5c27f0ae3b3d23c5e3090f032928e2",
            "test_eval_runner_hygiene.py": "7433345055680b0b3e2414b420e06834ef57c8c4d9d8e158116e9fcaeacc8a6f",
            "test_package_archive.py": "7b4f16c9b84b931457fc825bf8b0cb99666462f854ad85e6a17ffb38052a671a",
        }
        for name, expected in frozen_sha256.items():
            with self.subTest(name=name):
                self.assertEqual(
                    expected,
                    hashlib.sha256((packaged / name).read_bytes()).hexdigest(),
                )


if __name__ == "__main__":
    unittest.main()
