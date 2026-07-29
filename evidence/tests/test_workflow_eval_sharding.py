from contextlib import contextmanager
from dataclasses import asdict, fields, FrozenInstanceError, replace
import errno
import hashlib
import inspect
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
from typing import get_args, get_type_hints
import unittest
from unittest import mock
import weakref

from scripts import workflow_eval_sharding as sharding


FIXTURES = Path(__file__).parent / "skill_evals"


def load_cases(filename):
    return json.loads((FIXTURES / filename).read_text(encoding="utf-8"))


def input_fingerprints(run_kind):
    return sharding.InputFingerprints(
        schema_version=1,
        epoch_id="",
        run_kind=run_kind,
        archive_sha256="a" * 64,
        marketplace_sha256="b" * 64,
        evaluator_sha256="c" * 64,
        transport_config_sha256="d" * 64,
        forward_manifest_sha256=(
            "f3bd3b758e5fff43ed3bc50359d3799c111174a6bc8a225208b6c9989b7358a2"
        ),
        lifecycle_manifest_sha256=(
            "d3f91c1359b4087ed5d336fb079f020eed3c42e132360b5d5ca684518a411e8b"
        ),
    )


class PlannerTests(unittest.TestCase):
    def test_public_planner_types_match_the_frozen_contract(self):
        self.assertEqual(get_args(sharding.EvalMode), ("forward", "lifecycle"))
        self.assertEqual(get_args(sharding.LaneName), ("E1", "E2", "E3", "APP"))
        self.assertEqual(
            [field.name for field in fields(sharding.CaseKey)],
            ["mode", "ordinal", "case_id"],
        )
        self.assertEqual(
            [field.name for field in fields(sharding.CaseAssignment)],
            ["key", "lane", "route", "manifest_sha256"],
        )
        self.assertLess(
            sharding.CaseKey("forward", 1, "first"),
            sharding.CaseKey("forward", 2, "second"),
        )

    def test_frozen_plan_has_exact_8_8_8_4_coverage(self):
        forward_cases = load_cases("observing_workflows_cases.json")
        lifecycle_cases = load_cases("observing_workflows_lifecycle_cases.json")
        manifests = {
            "forward": forward_cases,
            "lifecycle": lifecycle_cases,
        }

        discovery = sharding.build_epoch_plan(
            run_kind="discovery",
            fingerprints=input_fingerprints("discovery"),
            manifests=manifests,
        )
        formal = sharding.build_epoch_plan(
            run_kind="formal",
            fingerprints=input_fingerprints("formal"),
            manifests=manifests,
        )

        self.assertEqual(len(discovery.assignments), 28)
        self.assertEqual(
            len({assignment.key for assignment in discovery.assignments}), 28
        )
        self.assertEqual(
            [assignment.route for assignment in discovery.assignments].count("exec"),
            24,
        )
        self.assertEqual(
            [assignment.route for assignment in discovery.assignments].count(
                "app-server"
            ),
            4,
        )

        expected_lanes = {
            "E1": (
                ("forward", 1, "multi-file-feature"),
                ("forward", 5, "wiki-compile"),
                ("forward", 11, "chat"),
                ("forward", 14, "plan-only"),
                ("forward", 16, "single-file-copy"),
                ("forward", 19, "worker-with-parent-marker"),
                ("lifecycle", 1, "planned-success"),
                ("lifecycle", 6, "central-cli-unavailable"),
            ),
            "E2": (
                ("forward", 2, "tested-bugfix"),
                ("forward", 4, "multi-file-docs"),
                ("forward", 6, "durable-query"),
                ("forward", 10, "parent-managed-subagent"),
                ("forward", 15, "single-file-typo"),
                ("forward", 17, "status-question"),
                ("lifecycle", 5, "task-failure"),
                ("lifecycle", 8, "incomplete-eval-override"),
            ),
            "E3": (
                ("forward", 3, "reviewed-refactor"),
                ("forward", 7, "inbox-processing"),
                ("forward", 12, "read-only-search"),
                ("forward", 13, "answer-only"),
                ("forward", 18, "review-only"),
                ("forward", 20, "ambiguous-default-no-trigger"),
                ("lifecycle", 4, "parent-managed-subagent"),
                ("lifecycle", 7, "complete-eval-override"),
            ),
            "APP": (
                ("forward", 8, "late-trigger"),
                ("forward", 9, "scope-supersession"),
                ("lifecycle", 2, "late-success"),
                ("lifecycle", 3, "scope-supersession"),
            ),
        }
        for lane, expected_keys in expected_lanes.items():
            lane_assignments = [
                assignment
                for assignment in discovery.assignments
                if assignment.lane == lane
            ]
            self.assertEqual(
                tuple(
                    (
                        assignment.key.mode,
                        assignment.key.ordinal,
                        assignment.key.case_id,
                    )
                    for assignment in lane_assignments
                ),
                expected_keys,
            )

        canonical_keys = tuple(
            sharding.CaseKey("forward", ordinal, case["id"])
            for ordinal, case in enumerate(forward_cases, start=1)
        ) + tuple(
            sharding.CaseKey("lifecycle", ordinal, case["id"])
            for ordinal, case in enumerate(lifecycle_cases, start=1)
        )
        self.assertEqual(
            tuple(assignment.key for assignment in discovery.assignments),
            canonical_keys,
        )
        for assignment in discovery.assignments:
            expected_hash = (
                discovery.fingerprints.forward_manifest_sha256
                if assignment.key.mode == "forward"
                else discovery.fingerprints.lifecycle_manifest_sha256
            )
            self.assertEqual(assignment.manifest_sha256, expected_hash)
        self.assertNotEqual(discovery.epoch_id, formal.epoch_id)
        self.assertEqual(discovery.epoch_id, discovery.fingerprints.epoch_id)
        self.assertEqual(discovery.run_kind, discovery.fingerprints.run_kind)

    def test_epoch_id_matches_the_exact_independent_canonical_payload(self):
        manifests = {
            "forward": load_cases("observing_workflows_cases.json"),
            "lifecycle": load_cases("observing_workflows_lifecycle_cases.json"),
        }
        plan = sharding.build_epoch_plan(
            run_kind="discovery",
            manifests=manifests,
            fingerprints=input_fingerprints("discovery"),
        )

        fingerprint_fields = asdict(plan.fingerprints)
        fingerprint_fields.pop("epoch_id")
        assignment_fields = [
            {
                "key": {
                    "mode": assignment.key.mode,
                    "ordinal": assignment.key.ordinal,
                    "case_id": assignment.key.case_id,
                },
                "lane": assignment.lane,
                "route": assignment.route,
                "manifest_sha256": assignment.manifest_sha256,
            }
            for assignment in plan.assignments
        ]
        payload = json.dumps(
            {
                "run_kind": "discovery",
                "fingerprints": fingerprint_fields,
                "assignments": assignment_fields,
            },
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        self.assertEqual(plan.epoch_id, hashlib.sha256(payload).hexdigest())


class FingerprintTests(unittest.TestCase):
    def test_canonical_config_bytes_are_sorted_compact_ascii_json(self):
        self.assertEqual(
            sharding.canonical_config_bytes({"z": "雪", "a": [2, 1]}),
            b'{"a":[2,1],"z":"\\u96ea"}',
        )

    def test_component_digest_uses_sorted_inventory_member_pairs(self):
        entries = [
            ("z/member.py", "a" * 64),
            ("a/member.py", "b" * 64),
        ]
        self.assertEqual(
            sharding.component_digest(entries),
            "08475606f78449a9db38e0634b32df59cc827ed786c0f63ebca30c32dc80c5d6",
        )
        self.assertEqual(
            sharding.component_digest(entries), sharding.component_digest(entries[::-1])
        )


class WriterLeaseTests(unittest.TestCase):
    class EqualitySpoof:
        def __eq__(self, _other):
            return True

    @staticmethod
    def _git_repository(root: Path, name: str = "repository") -> Path:
        repository = (root / name).resolve()
        subprocess.run(
            ["git", "init", "-q", str(repository)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return repository

    def test_run_lease_acquires_exact_lock_and_releases(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "formal-run"
            lease = sharding.RunCoordinatorLease.acquire(
                run_root=run_root,
                epoch_id="e" * 64,
                run_kind="formal",
            )
            try:
                self.assertTrue(lease.active)
                self.assertEqual(0o700, stat.S_IMODE(run_root.stat().st_mode))
                coordinator = run_root / "coordinator"
                self.assertEqual(0o700, stat.S_IMODE(coordinator.stat().st_mode))
                lock = coordinator / "coordinator.lock"
                self.assertTrue(lock.is_file())
                self.assertEqual(0o600, stat.S_IMODE(lock.stat().st_mode))
                with self.assertRaises((BlockingIOError, RuntimeError)):
                    sharding.RunCoordinatorLease.acquire(
                        run_root=run_root,
                        epoch_id="e" * 64,
                        run_kind="formal",
                    )
            finally:
                lease.close()

            self.assertFalse(lease.active)
            reacquired = sharding.RunCoordinatorLease.acquire(
                run_root=run_root,
                epoch_id="e" * 64,
                run_kind="formal",
            )
            self.assertTrue(reacquired.active)
            reacquired.close()

    def test_managed_lock_post_create_stat_failure_retires_opened_fd_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve(strict=True)
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            real_open = sharding.os.open
            real_close = sharding.os.close
            real_stat = sharding.os.stat
            real_slot = sharding._DescriptorSlot
            opened: list[int] = []
            slotted: list[int] = []
            closed: list[int] = []
            stat_calls = 0

            def injected_stat(path, *args, **kwargs):
                nonlocal stat_calls
                if path == "created.lock":
                    stat_calls += 1
                    if stat_calls == 1:
                        raise FileNotFoundError(path)
                    if stat_calls == 2:
                        raise RuntimeError("POST_CREATE_STAT_FAILURE")
                return real_stat(path, *args, **kwargs)

            def recording_open(path, flags, *args, **kwargs):
                descriptor = real_open(path, flags, *args, **kwargs)
                if path == "created.lock":
                    opened.append(descriptor)
                return descriptor

            def recording_slot(descriptor):
                slotted.append(descriptor)
                return real_slot(descriptor)

            def recording_close(descriptor):
                if descriptor in opened:
                    closed.append(descriptor)
                return real_close(descriptor)

            try:
                with mock.patch.object(
                    sharding.os, "stat", side_effect=injected_stat
                ), mock.patch.object(
                    sharding.os, "open", side_effect=recording_open
                ), mock.patch.object(
                    sharding.os, "close", side_effect=recording_close
                ), mock.patch.object(
                    sharding, "_DescriptorSlot", side_effect=recording_slot
                ):
                    with self.assertRaisesRegex(
                        RuntimeError, "POST_CREATE_STAT_FAILURE"
                    ):
                        sharding._open_managed_lock_at(
                            directory_fd,
                            "created.lock",
                            label="created lock",
                        )

                self.assertEqual(1, len(opened))
                self.assertEqual(opened, slotted)
                self.assertEqual(opened, closed)
                with self.assertRaises(OSError):
                    os.fstat(opened[0])
            finally:
                for descriptor in opened:
                    try:
                        os.fstat(descriptor)
                    except OSError:
                        continue
                    real_close(descriptor)
                real_close(directory_fd)

    def test_managed_lock_post_create_close_reuse_poison_is_one_shot(self):
        evidence_root = Path(__file__).parents[1]
        program = r'''
from pathlib import Path
import os
import sys

from scripts import workflow_eval_sharding as sharding

run_root = Path(sys.argv[1])
real_open = os.open
real_close = os.close
real_stat = os.stat
target = None
target_identity = None
target_parent_fd = None
target_close_calls = 0
close_calls = []
stat_calls = 0

def injected_stat(path, *args, **kwargs):
    global stat_calls
    if path == "coordinator.lock":
        stat_calls += 1
        if stat_calls == 1:
            raise FileNotFoundError(path)
        if stat_calls == 2:
            raise RuntimeError("POST_CREATE_STAT_FAILURE")
    return real_stat(path, *args, **kwargs)

def recording_open(path, flags, *args, **kwargs):
    global target, target_identity, target_parent_fd
    descriptor = real_open(path, flags, *args, **kwargs)
    if path == "coordinator.lock" and target is None:
        target = descriptor
        metadata = os.fstat(descriptor)
        target_identity = (metadata.st_dev, metadata.st_ino)
        target_parent_fd = kwargs.get("dir_fd")
    return descriptor

def close_then_reuse(descriptor):
    global target_close_calls
    close_calls.append(descriptor)
    real_close(descriptor)
    if descriptor == target:
        target_close_calls += 1
        replacement = real_open(
            "coordinator.lock",
            os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=target_parent_fd,
        )
        if replacement != descriptor:
            os.dup2(replacement, descriptor)
            real_close(replacement)
        metadata = os.fstat(descriptor)
        if (metadata.st_dev, metadata.st_ino) != target_identity:
            raise SystemExit(2)
        raise OSError("indeterminate managed-lock close")

sharding.os.stat = injected_stat
sharding.os.open = recording_open
sharding.os.close = close_then_reuse
try:
    try:
        sharding.RunCoordinatorLease.acquire(
            run_root=run_root,
            epoch_id="e" * 64,
            run_kind="formal",
        )
    except BaseException as error:
        if not sharding.is_indeterminate_descriptor_close(error):
            raise SystemExit(3)
    else:
        raise SystemExit(4)
    if target is None or target_close_calls != 1:
        raise SystemExit(5)
    os.fstat(target)
    first_close_calls = tuple(close_calls)
    try:
        sharding.RunCoordinatorLease.acquire(
            run_root=run_root,
            epoch_id="e" * 64,
            run_kind="formal",
        )
    except RuntimeError:
        pass
    else:
        raise SystemExit(6)
    if tuple(close_calls) != first_close_calls:
        raise SystemExit(7)
finally:
    sharding.os.stat = real_stat
    sharding.os.open = real_open
    sharding.os.close = real_close
    if target is not None:
        real_close(target)
'''
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    program,
                    str(Path(temporary, "formal-run").resolve()),
                ],
                cwd=evidence_root,
                env={**os.environ, "PYTHONPATH": str(evidence_root)},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=15,
            )
        self.assertEqual(0, completed.returncode, completed.stderr)

    def test_trusted_parent_policy_accepts_sticky_and_rejects_writable_nonsticky(self):
        def directory_metadata(mode, uid):
            values = [0] * 10
            values[0] = stat.S_IFDIR | mode
            values[4] = uid
            return os.stat_result(values)

        for uid in {0, os.geteuid()}:
            with self.subTest(uid=uid, mode="sticky"):
                sharding._validate_trusted_parent(
                    directory_metadata(0o1777, uid), label="trusted parent"
                )
            with self.subTest(uid=uid, mode="non-writable"):
                sharding._validate_trusted_parent(
                    directory_metadata(0o755, uid), label="trusted parent"
                )
            with self.subTest(uid=uid, mode="writable-nonsticky"):
                with self.assertRaises(PermissionError):
                    sharding._validate_trusted_parent(
                        directory_metadata(0o777, uid), label="trusted parent"
                    )

        with self.assertRaises(PermissionError):
            sharding._validate_trusted_parent(
                directory_metadata(0o755, os.geteuid() + 10000),
                label="trusted parent",
            )

    def test_lease_ownership_uses_effective_uid_but_lock_namespace_uses_real_uid(self):
        def metadata(mode, uid, *, directory=True):
            values = [0] * 10
            values[0] = (stat.S_IFDIR if directory else stat.S_IFREG) | mode
            values[4] = uid
            return os.stat_result(values)

        with mock.patch.object(sharding.os, "getuid", return_value=1001), \
             mock.patch.object(sharding.os, "geteuid", return_value=2002):
            sharding._validate_owned_entry(
                metadata(0o700, 2002),
                label="effective-owner directory",
                kind="directory",
                mode=0o700,
            )
            sharding._validate_trusted_parent(
                metadata(0o1777, 2002), label="effective-owner sticky parent"
            )
            with self.assertRaises(PermissionError):
                sharding._validate_owned_entry(
                    metadata(0o700, 1001),
                    label="real-owner directory",
                    kind="directory",
                    mode=0o700,
                )
            with mock.patch.object(
                sharding,
                "_canonical_git_repository_root",
                return_value=(Path("/repository"), (1, 2)),
            ):
                self.assertEqual(
                    Path(
                        "/var/tmp/workflow-observatory-result-locks-uid-1001/"
                        + hashlib.sha256(b"/repository").hexdigest()
                        + ".lock"
                    ),
                    sharding.result_writer_lock_path(Path("/repository")),
                )

    def test_result_writer_remains_live_under_root_owned_sticky_parent(self):
        repository = Path(
            tempfile.mkdtemp(
                prefix="workflow-result-repository-", dir="/private/tmp"
            )
        ).resolve(strict=True)
        try:
            if repository.parent.stat().st_uid == os.geteuid():
                self.skipTest("repository parent is not externally owned")
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            authority = lease.authority()
            self.assertFalse(authority.consumed)
            lease.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                lease._validate_live()
        finally:
            shutil.rmtree(repository, ignore_errors=True)

    def test_result_writer_binds_exact_git_root_and_issues_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            repository = Path(temporary, "repository").resolve()
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            try:
                expected_key = hashlib.sha256(os.fsencode(repository)).hexdigest()
                expected_path = Path(
                    f"/var/tmp/workflow-observatory-result-locks-uid-{os.getuid()}",
                    f"{expected_key}.lock",
                )
                self.assertEqual(expected_path, sharding.result_writer_lock_path(repository))
                self.assertEqual(0o700, stat.S_IMODE(expected_path.parent.stat().st_mode))
                self.assertEqual(0o600, stat.S_IMODE(expected_path.stat().st_mode))
                authority = lease.authority()
                self.assertEqual(expected_key, authority.repository_key)
                self.assertEqual("serial-coordinator", authority.role)
                self.assertEqual("formal", authority.run_kind)
                self.assertFalse(authority.consumed)
                with self.assertRaises(RuntimeError):
                    lease.authority()
            finally:
                lease.close()

            child = repository / "child"
            child.mkdir()
            with self.assertRaises(ValueError):
                sharding.ResultWriterLease.acquire(
                    child,
                    role="serial-coordinator",
                    run_kind="formal",
                    run_lease=None,
                )

    def test_run_lease_is_nominal_pid_bound_nonreentrant_and_writer_closes_first(self):
        with self.assertRaises(ValueError):
            sharding.RunCoordinatorLease.acquire(
                run_root=Path("/var/tmp/equality-spoof-must-not-exist"),
                epoch_id="d" * 64,
                run_kind=self.EqualitySpoof(),
            )
        root_owned_run = Path(
            tempfile.mkdtemp(prefix="workflow-run-lease-", dir="/var/tmp")
        ).resolve(strict=True)
        try:
            if root_owned_run.parent.stat().st_uid == os.getuid():
                self.skipTest("run-root parent is not externally owned on this system")
            external_parent_lease = sharding.RunCoordinatorLease.acquire(
                run_root=root_owned_run,
                epoch_id="d" * 64,
                run_kind="formal",
            )
            self.assertTrue(external_parent_lease.active)
            external_parent_lease.close()
        finally:
            shutil.rmtree(root_owned_run, ignore_errors=True)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = self._git_repository(root)
            run_root = root / "formal-run"
            run_lease = sharding.RunCoordinatorLease.acquire(
                run_root=run_root,
                epoch_id="e" * 64,
                run_kind="formal",
            )
            try:
                with self.assertRaises(RuntimeError):
                    sharding.RunCoordinatorLease.acquire(
                        run_root=root / "other-run",
                        epoch_id="f" * 64,
                        run_kind="formal",
                    )

                writer = sharding.ResultWriterLease.acquire(
                    repository,
                    role="parallel-coordinator",
                    run_kind="formal",
                    run_lease=run_lease,
                )
                try:
                    with self.assertRaisesRegex(RuntimeError, "writer.*close"):
                        run_lease.close()
                    with self.assertRaises(RuntimeError):
                        sharding.ResultWriterLease.acquire(
                            repository,
                            role="parallel-coordinator",
                            run_kind="formal",
                            run_lease=run_lease,
                        )
                finally:
                    writer.close()
            finally:
                run_lease.close()

            class FakeRunLease(sharding.RunCoordinatorLease):
                pass

            with self.assertRaises(TypeError):
                FakeRunLease.acquire(
                    run_root=root / "fake-run",
                    epoch_id="e" * 64,
                    run_kind="formal",
                )

            pid_program = r"""
from pathlib import Path
import os
import sys
from scripts.workflow_eval_sharding import RunCoordinatorLease

lease = RunCoordinatorLease.acquire(
    run_root=Path(sys.argv[1]), epoch_id="e" * 64, run_kind="formal"
)
child = os.fork()
if child == 0:
    try:
        lease.active
    except RuntimeError as error:
        os._exit(0 if "another process" in str(error) else 2)
    os._exit(3)
_, status = os.waitpid(child, 0)
if os.waitstatus_to_exitcode(status) != 0:
    raise SystemExit(4)
if not lease.active:
    raise SystemExit(5)
lease.close()
"""
            completed = subprocess.run(
                [sys.executable, "-c", pid_program, str(root / "pid-run")],
                cwd=Path(__file__).parents[1],
                env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1])},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)

            swap_program = r"""
from pathlib import Path
import sys
from scripts.workflow_eval_sharding import RunCoordinatorLease

parent = Path(sys.argv[1])
parent.mkdir(mode=0o700)
lease = RunCoordinatorLease.acquire(
    run_root=parent / "formal-run", epoch_id="e" * 64, run_kind="formal"
)
moved = parent.with_name("moved-anchor")
parent.rename(moved)
parent.mkdir(mode=0o700)
try:
    lease.active
except RuntimeError as error:
    raise SystemExit(0 if "parent name changed" in str(error) else 2)
raise SystemExit(3)
"""
            completed = subprocess.run(
                [sys.executable, "-c", swap_program, str(root / "anchor")],
                cwd=Path(__file__).parents[1],
                env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1])},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)

    def test_fixed_result_lock_root_rejects_unsafe_entries_and_repository_aliases(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = self._git_repository(root)
            child = repository / "child"
            child.mkdir()
            alias = root / "repository-alias"
            alias.symlink_to(repository, target_is_directory=True)
            for candidate in (child, alias):
                with self.subTest(candidate=candidate), self.assertRaises(ValueError):
                    sharding.ResultWriterLease.acquire(
                        candidate,
                        role="serial-coordinator",
                        run_kind="formal",
                        run_lease=None,
                    )

            lock_path = sharding.result_writer_lock_path(repository)
            if lock_path.exists() or lock_path.is_symlink():
                lock_path.unlink()
            unsafe_target = root / "unsafe-target"
            unsafe_target.write_text("do not lock\n", encoding="utf-8")
            lock_path.symlink_to(unsafe_target)
            try:
                with self.assertRaises((OSError, PermissionError, ValueError)):
                    sharding.ResultWriterLease.acquire(
                        repository,
                        role="serial-coordinator",
                        run_kind="formal",
                        run_lease=None,
                    )
                self.assertTrue(lock_path.is_symlink())
                self.assertEqual("do not lock\n", unsafe_target.read_text(encoding="utf-8"))
            finally:
                lock_path.unlink(missing_ok=True)

    def test_parallel_writer_requires_exact_live_run_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = self._git_repository(root)
            for role, run_kind in (
                (self.EqualitySpoof(), "formal"),
                ("serial-coordinator", self.EqualitySpoof()),
            ):
                with self.subTest(role=role, run_kind=run_kind), self.assertRaises(
                    ValueError
                ):
                    sharding.ResultWriterLease.acquire(
                        repository,
                        role=role,
                        run_kind=run_kind,
                        run_lease=None,
                    )
            for run_lease in (None, object(), True):
                with self.subTest(run_lease=run_lease), self.assertRaises(TypeError):
                    sharding.ResultWriterLease.acquire(
                        repository,
                        role="parallel-coordinator",
                        run_kind="formal",
                        run_lease=run_lease,
                    )

            run_lease = sharding.RunCoordinatorLease.acquire(
                run_root=root / "formal-run",
                epoch_id="e" * 64,
                run_kind="formal",
            )
            run_lease.close()
            with self.assertRaises(RuntimeError):
                sharding.ResultWriterLease.acquire(
                    repository,
                    role="parallel-coordinator",
                    run_kind="formal",
                    run_lease=run_lease,
                )

    def test_authority_issues_and_consumes_once_before_any_destination_open(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = self._git_repository(root)
            lease = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            try:
                authority = lease.authority()
                destinations = {
                    "forward": repository / "results" / "forward.json",
                    "lifecycle": repository / "results" / "lifecycle.json",
                }
                with mock.patch.object(
                    Path,
                    "resolve",
                    side_effect=AssertionError(
                        "destination consume must remain lexical-only"
                    ),
                ), mock.patch.object(
                    sharding.os,
                    "lstat",
                    side_effect=AssertionError(
                        "destination consume must not inspect the filesystem"
                    ),
                ), mock.patch.object(
                    sharding.os,
                    "stat",
                    side_effect=AssertionError(
                        "destination consume must not inspect the filesystem"
                    ),
                ), mock.patch.object(
                    sharding.os,
                    "fstat",
                    side_effect=AssertionError(
                        "destination consume must not inspect descriptors"
                    ),
                ), mock.patch.object(
                    sharding.os,
                    "open",
                    side_effect=AssertionError(
                        "destination consume must not open filesystem entries"
                    ),
                ):
                    authority._consume(destinations)
                self.assertTrue(authority.consumed)
                self.assertFalse((repository / "results").exists())
                with self.assertRaises(RuntimeError):
                    authority._consume(destinations)
            finally:
                lease.close()

    def test_lease_close_failures_are_one_shot_and_poison_process(self):
        evidence_root = Path(__file__).parents[1]
        program = r"""
from pathlib import Path
import os
import subprocess
import sys
from scripts import workflow_eval_sharding as sharding

root = Path(sys.argv[1])
kind = sys.argv[2]
role = sys.argv[3]
if kind == "run":
    lease = sharding.RunCoordinatorLease.acquire(
        run_root=root / "formal-run", epoch_id="e" * 64, run_kind="formal"
    )
else:
    repository = root / "repository"
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    lease = sharding.ResultWriterLease.acquire(
        repository, role="serial-coordinator", run_kind="formal", run_lease=None
    )

if kind == "run":
    slots = {
        "parent": (lease._parent_slot, root),
        "run": (lease._run_slot, root / "formal-run"),
        "coordinator": (lease._coordinator_slot, root / "formal-run" / "coordinator"),
        "lock": (lease._lock_slot, root / "formal-run" / "coordinator" / "coordinator.lock"),
    }
    expected_order = ["coordinator", "run", "parent", "lock"]
else:
    lock_path = sharding.result_writer_lock_path(repository)
    slots = {
        "repository-parent": (lease._repository_parent_slot, repository.parent),
        "repository": (lease._repository_slot, repository),
        "lock-parent": (lease._parent_slot, Path("/var/tmp").resolve(strict=True)),
        "lock-root": (lease._root_slot, lock_path.parent),
        "lock": (lease._lock_slot, lock_path),
    }
    expected_order = [
        "repository", "repository-parent", "lock-root", "lock-parent", "lock"
    ]
target_slot, target_path = slots[role]
target = target_slot.descriptor
slot_by_descriptor = {slot.descriptor: name for name, (slot, _path) in slots.items()}
real_close = sharding.os.close
real_open = sharding.os.open
calls = []
def close_then_fail(descriptor):
    if descriptor in slot_by_descriptor:
        calls.append(slot_by_descriptor[descriptor])
    real_close(descriptor)
    if descriptor == target:
        flags = os.O_RDWR if target_path.is_file() else os.O_RDONLY | os.O_DIRECTORY
        replacement = real_open(target_path, flags)
        if replacement != descriptor:
            os.dup2(replacement, descriptor)
            real_close(replacement)
        raise OSError("indeterminate lease close")
sharding.os.close = close_then_fail
try:
    try:
        lease.close()
    except OSError as error:
        if not sharding.is_indeterminate_descriptor_close(error):
            raise SystemExit(2)
    else:
        raise SystemExit(3)
    first_calls = tuple(calls)
    if first_calls != tuple(expected_order):
        raise SystemExit(4)
    os.fstat(target)
    for operation in (
        lambda: lease.close(),
        lambda: sharding.RunCoordinatorLease.acquire(
            run_root=root / "second-run", epoch_id="f" * 64, run_kind="formal"
        ),
    ):
        try:
            operation()
        except RuntimeError:
            pass
        else:
            raise SystemExit(5)
    if tuple(calls) != first_calls:
        raise SystemExit(6)
finally:
    sharding.os.close = real_close
    real_close(target)
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            roles = {
                "run": ("parent", "run", "coordinator", "lock"),
                "result": (
                    "repository-parent",
                    "repository",
                    "lock-parent",
                    "lock-root",
                    "lock",
                ),
            }
            for kind, kind_roles in roles.items():
                for role in kind_roles:
                    with self.subTest(kind=kind, role=role):
                        process_root = root / f"{kind}-{role}"
                        process_root.mkdir()
                        completed = subprocess.run(
                            [
                                sys.executable,
                                "-c",
                                program,
                                str(process_root),
                                kind,
                                role,
                            ],
                            cwd=evidence_root,
                            env={**os.environ, "PYTHONPATH": str(evidence_root)},
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            timeout=10,
                        )
                        self.assertEqual(0, completed.returncode, completed.stderr)

    def test_indeterminate_rollback_primary_poison_is_terminal(self):
        evidence_root = Path(__file__).parents[1]
        program = r'''
from pathlib import Path
import tempfile
from scripts import workflow_eval_sharding as sharding

error = OSError("nested rollback close became indeterminate")
setattr(error, sharding._INDETERMINATE_CLOSE_MARKER, True)
try:
    sharding._raise_task_failures(
        primary=error, close_errors=[], label="nested rollback"
    )
except OSError as caught:
    if caught is not error:
        raise SystemExit(2)
else:
    raise SystemExit(3)
with tempfile.TemporaryDirectory() as temporary:
    try:
        sharding.RunCoordinatorLease.acquire(
            run_root=Path(temporary).resolve(strict=True) / "run",
            epoch_id="e" * 64,
            run_kind="formal",
        )
    except RuntimeError as caught:
        if "poisoned" not in str(caught):
            raise SystemExit(4)
    else:
        raise SystemExit(5)
'''
        completed = subprocess.run(
            [sys.executable, "-c", program],
            cwd=evidence_root,
            env={**os.environ, "PYTHONPATH": str(evidence_root)},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)

    def test_validation_failure_close_retires_all_and_allows_reacquire(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_parent = root / "run-parent"
            run_parent.mkdir(mode=0o700)
            run_root = run_parent / "formal-run"
            lease = sharding.RunCoordinatorLease.acquire(
                run_root=run_root,
                epoch_id="e" * 64,
                run_kind="formal",
            )
            moved_parent = root / "moved-run-parent"
            run_parent.rename(moved_parent)
            run_parent.mkdir(mode=0o700)
            with self.assertRaisesRegex(RuntimeError, "parent name changed"):
                lease.close()
            self.assertTrue(
                all(
                    slot.descriptor_close_state == "closed"
                    for slot in (
                        lease._parent_slot,
                        lease._run_slot,
                        lease._coordinator_slot,
                        lease._lock_slot,
                    )
                )
            )
            shutil.rmtree(run_parent)
            moved_parent.rename(run_parent)
            reacquired = sharding.RunCoordinatorLease.acquire(
                run_root=run_root,
                epoch_id="e" * 64,
                run_kind="formal",
            )
            reacquired.close()

            repository = self._git_repository(root)
            writer = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            moved_repository = root / "moved-repository"
            repository.rename(moved_repository)
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            with self.assertRaisesRegex(RuntimeError, "repository root name changed"):
                writer.close()
            self.assertTrue(
                all(
                    slot.descriptor_close_state == "closed"
                    for slot in (
                        writer._repository_slot,
                        writer._repository_parent_slot,
                        writer._root_slot,
                        writer._parent_slot,
                        writer._lock_slot,
                    )
                )
            )
            shutil.rmtree(repository)
            moved_repository.rename(repository)
            reacquired_writer = sharding.ResultWriterLease.acquire(
                repository,
                role="serial-coordinator",
                run_kind="formal",
                run_lease=None,
            )
            reacquired_writer.close()

    def test_serial_and_parallel_writers_contend_across_processes(self):
        evidence_root = Path(__file__).parents[1]
        holder_program = r"""
from pathlib import Path
import sys
import time

from scripts import run_observing_workflows_task9_eval as evaluator
from scripts.workflow_eval_sharding import RunCoordinatorLease, ResultWriterLease

repository = Path(sys.argv[1])
run_root = Path(sys.argv[2])
ready = Path(sys.argv[3])
release = Path(sys.argv[4])
readback = Path(sys.argv[5])
results_root = Path(sys.argv[6])
run_lease = RunCoordinatorLease.acquire(
    run_root=run_root,
    epoch_id="e" * 64,
    run_kind="formal",
)
writer_lease = ResultWriterLease.acquire(
    repository,
    role="parallel-coordinator",
    run_kind="formal",
    run_lease=run_lease,
)
manifests = {"forward": [], "lifecycle": []}
results = {"forward": [], "lifecycle": []}
original_readback = evaluator._readback_result_pair_at

def readback_after_barrier(*args, **kwargs):
    ready.write_text("pointer-replaced-before-readback\n", encoding="utf-8")
    deadline = time.monotonic() + 10
    while not release.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("test release marker was not created")
        time.sleep(0.01)
    resolved = original_readback(*args, **kwargs)
    readback.write_text("readback-complete\n", encoding="utf-8")
    return resolved

evaluator._readback_result_pair_at = readback_after_barrier
try:
    evaluator.persist_result_pair(
        {
            "forward": results_root / "forward.json",
            "lifecycle": results_root / "lifecycle.json",
        },
        results,
        manifests,
        authority=writer_lease.authority(),
    )
finally:
    writer_lease.close()
    run_lease.close()
"""
        serial_program = r"""
from pathlib import Path
import sys
import time

from scripts import run_observing_workflows_task9_eval as evaluator

evaluator_root = Path(sys.argv[1])
repository = Path(sys.argv[2])
ready = Path(sys.argv[3])
persistence_entered = Path(sys.argv[4])
results_root = Path(sys.argv[5])
forward_manifest = Path(sys.argv[6])
lifecycle_manifest = Path(sys.argv[7])
deadline = time.monotonic() + 3
while not ready.exists():
    if time.monotonic() >= deadline:
        raise TimeoutError("parallel writer never became ready")
    time.sleep(0.01)

evaluator.validate_frozen_manifests = lambda *args, **kwargs: None
evaluator.snapshot_production = lambda *args, **kwargs: object()
evaluator.assert_production_unchanged = lambda *args, **kwargs: None
original_persist = evaluator._persist_result_pair_retained

def persist_with_entry_marker(*args, **kwargs):
    persistence_entered.write_text("entered\n", encoding="utf-8")
    return original_persist(*args, **kwargs)

evaluator._persist_result_pair_retained = persist_with_entry_marker
try:
    evaluator.run_suite(
        evaluator_root,
        repository_root=repository,
        manifest_paths={
            "forward": forward_manifest,
            "lifecycle": lifecycle_manifest,
        },
        result_destinations={
            "forward": results_root / "forward.json",
            "lifecycle": results_root / "lifecycle.json",
        },
        coordinator_role="serial-coordinator",
    )
except BaseException as error:
    print(f"lease-failed:{type(error).__name__}", file=sys.stderr)
    raise SystemExit(23)
print("serial-success")
"""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            repository = root / "repository"
            subprocess.run(
                ["git", "init", "-q", str(repository)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            run_root = root / "parallel-run"
            ready = root / "parallel-ready"
            release = root / "parallel-release"
            holder_readback = root / "parallel-readback"
            holder_results = repository / "holder-results"
            holder_pointer = (
                holder_results / "observing_workflows_results_commit.json"
            )
            manifest_root = repository / "serial-inputs"
            manifest_root.mkdir()
            forward_manifest = manifest_root / "forward.json"
            lifecycle_manifest = manifest_root / "lifecycle.json"
            forward_manifest.write_text("[]\n", encoding="utf-8")
            lifecycle_manifest.write_text("[]\n", encoding="utf-8")
            first_persistence_entered = root / "first-serial-persistence-entered"
            first_results = repository / "first-serial-results"
            second_persistence_entered = root / "second-serial-persistence-entered"
            second_results = repository / "second-serial-results"
            environments = []
            for process_name in ("parallel", "serial"):
                process_root = root / process_name
                tmpdir = process_root / "tmp"
                home = process_root / "home"
                codex_home = process_root / "codex-home"
                tmpdir.mkdir(parents=True)
                home.mkdir()
                codex_home.mkdir()
                environment = os.environ.copy()
                environment.update(
                    {
                        "TMPDIR": str(tmpdir),
                        "HOME": str(home),
                        "CODEX_HOME": str(codex_home),
                        "PYTHONPATH": str(evidence_root),
                    }
                )
                environments.append(environment)

            holder = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    holder_program,
                    str(repository),
                    str(run_root),
                    str(ready),
                    str(release),
                    str(holder_readback),
                    str(holder_results),
                ],
                cwd=evidence_root,
                env=environments[0],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            serial = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    serial_program,
                    str(evidence_root),
                    str(repository),
                    str(ready),
                    str(first_persistence_entered),
                    str(first_results),
                    str(forward_manifest),
                    str(lifecycle_manifest),
                ],
                cwd=evidence_root,
                env=environments[1],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            holder_stdout = ""
            holder_stderr = ""
            holder_output_collected = False
            try:
                serial_stdout, serial_stderr = serial.communicate(timeout=10)
                holder_was_live = holder.poll() is None
                if not holder_was_live:
                    holder_stdout, holder_stderr = holder.communicate(timeout=1)
                    holder_output_collected = True
                self.assertTrue(
                    ready.exists(),
                    "parallel holder never reached readback barrier:\n"
                    f"stdout={holder_stdout}\nstderr={holder_stderr}",
                )
                self.assertTrue(holder_pointer.is_file())
                self.assertFalse(holder_readback.exists())
                self.assertTrue(
                    holder_was_live,
                    "parallel lease was not held between pointer replace and readback",
                )
                self.assertEqual("", serial_stdout)
                self.assertEqual(23, serial.returncode, serial_stderr)
                self.assertIn("lease-failed:", serial_stderr)
                self.assertFalse(first_persistence_entered.exists())
                self.assertFalse(first_results.exists())
            finally:
                release.touch(exist_ok=True)
                if not holder_output_collected:
                    try:
                        holder_stdout, holder_stderr = holder.communicate(timeout=10)
                    except subprocess.TimeoutExpired:
                        holder.kill()
                        holder_stdout, holder_stderr = holder.communicate(timeout=5)
                if serial.poll() is None:
                    serial.kill()
                    serial_stdout, serial_stderr = serial.communicate(timeout=5)

            self.assertEqual(
                0,
                holder.returncode,
                "parallel holder failed after release:\n"
                f"stdout={holder_stdout}\nstderr={holder_stderr}",
            )
            self.assertTrue(holder_readback.is_file())

            reacquired = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    serial_program,
                    str(evidence_root),
                    str(repository),
                    str(ready),
                    str(second_persistence_entered),
                    str(second_results),
                    str(forward_manifest),
                    str(lifecycle_manifest),
                ],
                cwd=evidence_root,
                env=environments[1],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            reacquired_stdout, reacquired_stderr = reacquired.communicate(timeout=10)
            self.assertEqual(0, reacquired.returncode, reacquired_stderr)
            self.assertEqual("serial-success\n", reacquired_stdout)
            self.assertTrue(second_persistence_entered.is_file())
            self.assertTrue(
                (
                    second_results
                    / "observing_workflows_results_commit.json"
                ).is_file()
            )


class ResolvedTransportConfigTests(unittest.TestCase):
    @staticmethod
    def _write_fake_codex(path: Path, version: str = "codex-cli 9.9.9") -> None:
        path.write_text(
            "#!/bin/sh\nprintf '%s\\n' " + json.dumps(version) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o700)

    @staticmethod
    def _assignment_and_plan(case_id="auth-case"):
        assignment = sharding.CaseAssignment(
            key=sharding.CaseKey("forward", 1, case_id),
            lane="E1",
            route="exec",
            manifest_sha256="a" * 64,
        )
        epoch_id = "e" * 64
        plan = sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind="diagnostic",
            fingerprints=replace(
                input_fingerprints("diagnostic"), epoch_id=epoch_id
            ),
            assignments=(assignment,),
        )
        return assignment, plan

    def test_public_transport_config_matches_exact_contract_and_canonical_bytes(self):
        expected_fields = [
            "schema_version",
            "codex_version",
            "codex_executable_path",
            "codex_executable_sha256",
            "codex_executable_device",
            "codex_executable_inode",
            "codex_executable_size",
            "model",
            "model_reasoning_effort",
            "approval_policy",
            "sandbox_mode",
            "network_access",
            "web_search",
            "multi_agent",
            "exec_timeout_seconds",
            "app_server_timeout_seconds",
            "gate_timeout_seconds",
        ]
        self.assertEqual(
            expected_fields,
            [field.name for field in fields(sharding.ResolvedTransportConfig)],
        )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            (source_home / "config.toml").write_text(
                'model = "config-model"\nmodel_reasoning_effort = "high"\n',
                encoding="utf-8",
            )
            executable = root / "fake-codex"
            self._write_fake_codex(executable)

            config = sharding.resolve_transport_config(
                codex_executable=executable,
                source_codex_home=source_home,
                requested_model=None,
                requested_reasoning_effort=None,
            )

        serialized = sharding.transport_config_bytes(config)
        self.assertEqual(
            serialized,
            json.dumps(
                asdict(config),
                sort_keys=True,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("ascii"),
        )
        self.assertEqual("config-model", config.model)
        self.assertEqual("high", config.model_reasoning_effort)
        self.assertEqual("never", config.approval_policy)
        self.assertEqual("workspace-write", config.sandbox_mode)
        self.assertIs(False, config.network_access)
        self.assertEqual("disabled", config.web_search)
        self.assertIs(True, config.multi_agent)
        self.assertEqual(1200, config.exec_timeout_seconds)
        self.assertEqual(600, config.app_server_timeout_seconds)
        self.assertEqual(300, config.gate_timeout_seconds)

    def test_resolved_executable_identity_rejects_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            executable = root / "fake-codex"
            self._write_fake_codex(executable)
            config = sharding.resolve_transport_config(
                codex_executable=executable,
                source_codex_home=source_home,
                requested_model="requested-model",
                requested_reasoning_effort="medium",
            )
            self.assertEqual(
                executable.resolve(), sharding.verify_codex_executable(config)
            )

            executable.unlink()
            self._write_fake_codex(executable, "codex-cli replacement")
            with self.assertRaisesRegex(RuntimeError, "executable identity changed"):
                sharding.verify_codex_executable(config)

    def test_auth_bootstrap_and_case_install_are_private_auth_only_copies(self):
        secret = b'{"token":"AUTH_SECRET_SENTINEL"}\n'
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_bytes(secret)
            auth.chmod(0o600)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            assignment, plan = self._assignment_and_plan()

            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            paths = sharding.paths_for_case(run_root, assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            installed = sharding.install_case_auth(
                bootstrap=bootstrap.path,
                plan=plan,
                assignment=assignment,
                paths=paths,
            )

            self.assertEqual({"auth.json"}, {path.name for path in bootstrap.path.iterdir()})
            self.assertEqual({"auth.json"}, {path.name for path in paths.codex_home.iterdir()})
            self.assertEqual(secret, (bootstrap.path / "auth.json").read_bytes())
            self.assertEqual(secret, (paths.codex_home / "auth.json").read_bytes())
            self.assertEqual(0o700, stat.S_IMODE(bootstrap.path.stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(paths.codex_home.stat().st_mode))
            self.assertEqual(
                0o600, stat.S_IMODE((bootstrap.path / "auth.json").stat().st_mode)
            )
            self.assertEqual(
                0o600, stat.S_IMODE((paths.codex_home / "auth.json").stat().st_mode)
            )
            os.close(installed.descriptor)
            os.close(bootstrap.descriptor)

    def test_auth_bootstrap_rejects_missing_symlinked_and_unsafe_auth(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            coordinator = root / "coordinator"
            coordinator.mkdir(mode=0o700)
            _, plan = self._assignment_and_plan()

            missing_home = root / "missing-home"
            missing_home.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "safe auth.json"):
                sharding.prepare_auth_bootstrap(
                    source_codex_home=missing_home,
                    coordinator_root=coordinator,
                    plan=plan,
                )

            target = root / "target-auth.json"
            target.write_text("SYMLINK_AUTH_SECRET", encoding="utf-8")
            target.chmod(0o600)
            symlink_home = root / "symlink-home"
            symlink_home.mkdir(mode=0o700)
            (symlink_home / "auth.json").symlink_to(target)
            with self.assertRaisesRegex(ValueError, "safe auth.json"):
                sharding.prepare_auth_bootstrap(
                    source_codex_home=symlink_home,
                    coordinator_root=coordinator,
                    plan=plan,
                )

            unsafe_home = root / "unsafe-home"
            unsafe_home.mkdir(mode=0o700)
            unsafe_auth = unsafe_home / "auth.json"
            unsafe_auth.write_text("UNSAFE_AUTH_SECRET", encoding="utf-8")
            unsafe_auth.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "safe auth.json") as caught:
                sharding.prepare_auth_bootstrap(
                    source_codex_home=unsafe_home,
                    coordinator_root=coordinator,
                    plan=plan,
                )
            self.assertNotIn("UNSAFE_AUTH_SECRET", str(caught.exception))

    def test_case_auth_install_rejects_symlinked_or_unsafe_bootstrap_auth(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            coordinator = run_root / "coordinator"
            coordinator.mkdir(parents=True, mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            source_auth = source_home / "auth.json"
            source_auth.write_text("BOOTSTRAP_AUTH_SECRET", encoding="utf-8")
            source_auth.chmod(0o600)
            assignment, plan = self._assignment_and_plan()
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            paths = sharding.paths_for_case(run_root, assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            target = root / "target-auth.json"
            target.write_text("BOOTSTRAP_AUTH_SECRET", encoding="utf-8")
            target.chmod(0o600)
            (bootstrap.path / "auth.json").unlink()
            (bootstrap.path / "auth.json").symlink_to(target)
            with self.assertRaisesRegex(ValueError, "safe bootstrap auth.json"):
                sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )

            (bootstrap.path / "auth.json").unlink()
            unsafe_auth = bootstrap.path / "auth.json"
            unsafe_auth.write_text("UNSAFE_BOOTSTRAP_SECRET", encoding="utf-8")
            unsafe_auth.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "safe bootstrap auth.json"):
                sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )
            self.assertFalse(paths.codex_home.exists())
            os.close(bootstrap.descriptor)

    def test_source_auth_fifo_is_rejected_without_blocking(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            os_fifo = source_home / "auth.json"
            os.mkfifo(os_fifo, mode=0o600)
            coordinator = root / "coordinator"
            coordinator.mkdir(mode=0o700)
            code = "\n".join(
                (
                    "import sys",
                    "from pathlib import Path",
                    "from scripts import workflow_eval_sharding as sharding",
                    "root = Path(sys.argv[1])",
                    "try:",
                    "    sharding.prepare_legacy_auth_bootstrap(source_codex_home=root / 'source-home', coordinator_root=root / 'coordinator')",
                    "except ValueError:",
                    "    print('rejected')",
                    "else:",
                    "    raise SystemExit(3)",
                )
            )
            try:
                completed = subprocess.run(
                    [sys.executable, "-c", code, str(root)],
                    cwd=Path(__file__).resolve().parents[1],
                    text=True,
                    capture_output=True,
                    timeout=1,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                self.fail("mode-0600 source auth FIFO blocked preflight")
            self.assertEqual(0, completed.returncode, completed.stderr)
            self.assertEqual("rejected", completed.stdout.strip())

    def test_bootstrap_auth_fifo_is_rejected_without_blocking(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bootstrap = root / "bootstrap"
            bootstrap.mkdir(mode=0o700)
            os.mkfifo(bootstrap / "auth.json", mode=0o600)
            code = "\n".join(
                (
                    "import sys",
                    "from pathlib import Path",
                    "from scripts import workflow_eval_sharding as sharding",
                    "root = Path(sys.argv[1])",
                    "try:",
                    "    sharding.install_legacy_case_auth(bootstrap=root / 'bootstrap', case_codex_home=root / 'case-home')",
                    "except ValueError:",
                    "    print('rejected')",
                    "else:",
                    "    raise SystemExit(3)",
                )
            )
            try:
                completed = subprocess.run(
                    [sys.executable, "-c", code, str(root)],
                    cwd=Path(__file__).resolve().parents[1],
                    text=True,
                    capture_output=True,
                    timeout=1,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                self.fail("mode-0600 bootstrap auth FIFO blocked preflight")
            self.assertEqual(0, completed.returncode, completed.stderr)
            self.assertEqual("rejected", completed.stdout.strip())

    def test_transport_config_explicit_values_and_partial_fallback_precedence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            (source_home / "config.toml").write_text(
                'model = "fallback-model"\nmodel_reasoning_effort = "fallback-reasoning"\n',
                encoding="utf-8",
            )
            executable = root / "fake-codex"
            self._write_fake_codex(executable)
            cases = (
                ("explicit-model", "explicit-reasoning", "explicit-model", "explicit-reasoning"),
                ("explicit-model", None, "explicit-model", "fallback-reasoning"),
                (None, "explicit-reasoning", "fallback-model", "explicit-reasoning"),
                (None, None, "fallback-model", "fallback-reasoning"),
            )
            for requested_model, requested_reasoning, expected_model, expected_reasoning in cases:
                with self.subTest(
                    model=requested_model, reasoning=requested_reasoning
                ):
                    config = sharding.resolve_transport_config(
                        codex_executable=executable,
                        source_codex_home=source_home,
                        requested_model=requested_model,
                        requested_reasoning_effort=requested_reasoning,
                    )
                    self.assertEqual(expected_model, config.model)
                    self.assertEqual(
                        expected_reasoning, config.model_reasoning_effort
                    )


class RuntimeIsolationTests(unittest.TestCase):
    @staticmethod
    def _sha256_tree(root: Path) -> dict[str, tuple[int, str]]:
        inventory = {}
        for path in sorted(root.rglob("*")):
            relative = path.relative_to(root).as_posix()
            details = path.lstat()
            digest = ""
            if path.is_file():
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
            inventory[relative] = (stat.S_IMODE(details.st_mode), digest)
        return inventory

    @staticmethod
    def _write_read_only_marketplace(root: Path) -> None:
        files = {
            ".agents/plugins/marketplace.json": b'{"name":"test"}\n',
            "plugins/workflow-observer/.codex-plugin/plugin.json": b"{}\n",
            "plugins/workflow-observer/scripts/workflow_observer_cli.py": (
                b"#!/usr/bin/env python3\nraise SystemExit(0)\n"
            ),
            "plugins/workflow-observer/scripts/store_config.py": b"VALUE = 1\n",
            "plugins/workflow-observer/scripts/core_source.json": b"{}\n",
            "plugins/workflow-observer/skills/workflow-observer/SKILL.md": (
                b"---\nname: workflow-observer\n---\n"
            ),
            "README.md": b"captured marketplace\n",
        }
        root.mkdir(parents=True, mode=0o700)
        for relative, content in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        for path in sorted(root.rglob("*"), reverse=True):
            path.chmod(0o555 if path.is_dir() else 0o444)
        root.chmod(0o555)

    @staticmethod
    def _assignment(mode: str, ordinal: int, case_id: str):
        return sharding.CaseAssignment(
            key=sharding.CaseKey(mode, ordinal, case_id),
            lane="E1",
            route="exec",
            manifest_sha256="f" * 64,
        )

    @staticmethod
    def _transport_config(executable: Path):
        return sharding.ResolvedTransportConfig(
            schema_version=1,
            codex_version="codex-cli test",
            codex_executable_path=str(executable),
            codex_executable_sha256="0" * 64,
            codex_executable_device=1,
            codex_executable_inode=2,
            codex_executable_size=3,
            model="test-model",
            model_reasoning_effort="high",
            approval_policy="never",
            sandbox_mode="workspace-write",
            network_access=False,
            web_search="disabled",
            multi_agent=True,
            exec_timeout_seconds=1200,
            app_server_timeout_seconds=600,
            gate_timeout_seconds=300,
        )

    @staticmethod
    def _plan(assignment, run_kind="diagnostic"):
        epoch_id = "e" * 64
        fingerprints = replace(
            input_fingerprints(run_kind), epoch_id=epoch_id
        )
        return sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind=run_kind,
            fingerprints=fingerprints,
            assignments=(assignment,),
        )

    @staticmethod
    def _fd_count():
        for candidate in (Path("/dev/fd"), Path("/proc/self/fd")):
            if candidate.is_dir():
                return len(list(candidate.iterdir()))
        raise unittest.SkipTest("open-descriptor inventory is unavailable")

    @staticmethod
    def _exception_leaves(error):
        if isinstance(error, BaseExceptionGroup):
            return tuple(
                leaf
                for nested in error.exceptions
                for leaf in RuntimeIsolationTests._exception_leaves(nested)
            )
        return (error,)

    def _prepare_bootstrap(self, root, plan, secret=b'{"token":"SECRET"}\n'):
        source_home = root / "source-home"
        source_home.mkdir(mode=0o700)
        auth = source_home / "auth.json"
        auth.write_bytes(secret)
        auth.chmod(0o600)
        coordinator = root / "run/coordinator"
        coordinator.mkdir(parents=True, mode=0o700)
        return sharding.prepare_auth_bootstrap(
            source_codex_home=source_home,
            coordinator_root=coordinator,
            plan=plan,
        )

    def _install_case(self, root, plan, assignment):
        bootstrap = self._prepare_bootstrap(root, plan)
        paths = sharding.paths_for_case(root / "run", assignment)
        paths.root.mkdir(parents=True, mode=0o700)
        paths.cleanup.mkdir(mode=0o700)
        installed = sharding.install_case_auth(
            bootstrap=bootstrap.path,
            plan=plan,
            assignment=assignment,
            paths=paths,
        )
        return bootstrap, paths, installed

    def test_cleanup_public_contract_matches_amended_plan(self):
        self.assertEqual(
            ("active", "scrubbing", "tombstoned"),
            get_args(sharding.CleanupState),
        )
        self.assertEqual(
            ("owned", "closing", "closed", "indeterminate"),
            get_args(sharding.DescriptorCloseState),
        )

    def test_worker_exit_required_is_exact_recursive_terminal_signal(self):
        from scripts import run_observing_workflows_eval_worker as worker

        signature = inspect.signature(worker.worker_exit_required)
        self.assertEqual(("error", "factory"), tuple(signature.parameters))
        hints = get_type_hints(worker.worker_exit_required)
        self.assertIs(BaseException, hints["error"])
        self.assertIs(bool, hints["return"])

        ordinary = RuntimeError("ordinary")
        factory = mock.Mock(poisoned=False)
        self.assertFalse(worker.worker_exit_required(ordinary, factory))
        factory.poisoned = True
        self.assertTrue(worker.worker_exit_required(ordinary, factory))

        marked = RuntimeError("indeterminate")
        setattr(
            marked,
            sharding._INDETERMINATE_CLOSE_MARKER,
            True,
        )
        nested = ExceptionGroup("nested", [ordinary, marked])
        factory.poisoned = False
        self.assertTrue(worker.worker_exit_required(nested, factory))
        with self.assertRaises(TypeError):
            worker.worker_exit_required("not-an-error", factory)
        expected = {
            sharding.BootstrapOwnership: (
                "schema_version", "epoch_id", "run_kind",
                "bootstrap_device", "bootstrap_inode",
            ),
            sharding.InstalledAuthBootstrap: (
                "path", "ownership", "descriptor", "state",
                "descriptor_close_state", "descriptor_close_error",
            ),
            sharding.CaseAuthOwnership: (
                "schema_version", "epoch_id", "run_kind", "case",
                "case_root_device", "case_root_inode",
                "codex_home_device", "codex_home_inode",
            ),
            sharding.InstalledCaseAuth: (
                "ownership", "descriptor", "state",
                "descriptor_close_state", "descriptor_close_error",
            ),
            sharding.TombstoneReceipt: (
                "schema_version", "epoch_id", "run_kind", "case",
                "ownership_sha256", "case_root_device", "case_root_inode",
                "codex_home_device", "codex_home_inode", "scrubbed", "empty",
                "canonical_binding", "producer",
            ),
        }
        for record, names in expected.items():
            with self.subTest(record=record.__name__):
                self.assertEqual(names, tuple(field.name for field in fields(record)))
        self.assertEqual(
            ("source_codex_home", "coordinator_root", "plan"),
            tuple(inspect.signature(sharding.prepare_auth_bootstrap).parameters),
        )
        self.assertEqual(
            ("bootstrap", "plan", "assignment", "paths"),
            tuple(inspect.signature(sharding.install_case_auth).parameters),
        )

    def test_indeterminate_close_predicate_marks_exact_leaves_and_groups(self):
        failures = (
            RuntimeError("runtime close failure"),
            KeyboardInterrupt("close interrupted"),
            OSError(errno.EBADF, "bad descriptor"),
            OSError(errno.EIO, "I/O close failure"),
        )
        real_close = os.close
        for failure in failures:
            with self.subTest(failure=repr(failure)):
                descriptor = os.open(os.devnull, os.O_RDONLY)
                slot = sharding._DescriptorSlot(descriptor)

                def close_then_raise(candidate):
                    real_close(candidate)
                    raise failure

                with mock.patch.object(os, "close", side_effect=close_then_raise):
                    returned = sharding._retire_descriptor_capability(slot)

                self.assertIs(failure, returned)
                self.assertTrue(
                    sharding.is_indeterminate_descriptor_close(failure)
                )
                ordinary = RuntimeError("ordinary")
                group_type = (
                    ExceptionGroup
                    if isinstance(failure, Exception)
                    else BaseExceptionGroup
                )
                group = group_type("nested", [ordinary, failure])
                nested = BaseExceptionGroup("outer", [group])
                self.assertTrue(
                    sharding.is_indeterminate_descriptor_close(nested)
                )
                self.assertFalse(
                    sharding.is_indeterminate_descriptor_close(ordinary)
                )

    def test_atomic_record_temp_close_reuse_is_retired_without_publish_or_unlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            root.chmod(0o700)
            destination = root / "record.json"
            failure = RuntimeError("temporary descriptor close indeterminate")
            real_open = os.open
            real_close = os.close
            captured = {}
            replacement = None
            close_calls = 0
            parent_close_calls = 0
            mutations_after_failure = []
            close_failed = False

            def tracked_open(path, flags, mode=0o777, *, dir_fd=None):
                descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
                if dir_fd is None and Path(path) == root:
                    captured["parent"] = descriptor
                elif (
                    dir_fd == captured.get("parent")
                    and isinstance(path, str)
                    and path.startswith(".record.json.tmp-")
                ):
                    captured["temporary"] = descriptor
                    captured["temporary_name"] = path
                return descriptor

            def close_then_reuse(descriptor):
                nonlocal replacement, close_calls, close_failed, parent_close_calls
                if descriptor == captured.get("temporary"):
                    close_calls += 1
                    if close_calls == 1:
                        before = os.fstat(descriptor)
                        real_close(descriptor)
                        opened = real_open(
                            captured["temporary_name"],
                            os.O_RDWR,
                            dir_fd=captured["parent"],
                        )
                        if opened != descriptor:
                            os.dup2(opened, descriptor)
                            real_close(opened)
                        replacement = descriptor
                        after = os.fstat(replacement)
                        self.assertEqual(
                            (before.st_dev, before.st_ino),
                            (after.st_dev, after.st_ino),
                        )
                        close_failed = True
                        raise failure
                if descriptor == captured.get("parent"):
                    parent_close_calls += 1
                return real_close(descriptor)

            real_replace = os.replace
            real_unlink = os.unlink
            real_fsync = os.fsync

            def tracked_replace(*args, **kwargs):
                if close_failed:
                    mutations_after_failure.append("replace")
                return real_replace(*args, **kwargs)

            def tracked_unlink(*args, **kwargs):
                if close_failed:
                    mutations_after_failure.append("unlink")
                return real_unlink(*args, **kwargs)

            def tracked_fsync(*args, **kwargs):
                if close_failed:
                    mutations_after_failure.append("fsync")
                return real_fsync(*args, **kwargs)

            try:
                with mock.patch.object(os, "open", side_effect=tracked_open), \
                        mock.patch.object(os, "close", side_effect=close_then_reuse), \
                        mock.patch.object(os, "replace", side_effect=tracked_replace), \
                        mock.patch.object(os, "unlink", side_effect=tracked_unlink), \
                        mock.patch.object(os, "fsync", side_effect=tracked_fsync):
                    with self.assertRaises(RuntimeError) as caught:
                        sharding._atomic_write_record(
                            destination, {"schema_version": 1}
                        )
                self.assertIs(failure, caught.exception)
                self.assertTrue(
                    sharding.is_indeterminate_descriptor_close(caught.exception)
                )
                self.assertEqual(1, close_calls)
                self.assertEqual(1, parent_close_calls)
                self.assertEqual([], mutations_after_failure)
                self.assertFalse(destination.exists())
                self.assertTrue(
                    (root / captured["temporary_name"]).is_file()
                )
                os.fstat(replacement)
            finally:
                if replacement is not None:
                    try:
                        real_close(replacement)
                    except OSError:
                        pass
                temporary_path = root / captured.get("temporary_name", "missing")
                temporary_path.unlink(missing_ok=True)

    def test_atomic_record_parent_close_reuse_is_marked_and_replacement_survives(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            root.chmod(0o700)
            destination = root / "record.json"
            failure = KeyboardInterrupt("parent descriptor close indeterminate")
            real_open = os.open
            real_close = os.close
            captured = {}
            replacement = None
            close_calls = 0

            def tracked_open(path, flags, mode=0o777, *, dir_fd=None):
                descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
                if dir_fd is None and Path(path) == root:
                    captured["parent"] = descriptor
                return descriptor

            def close_then_reuse(descriptor):
                nonlocal replacement, close_calls
                if descriptor == captured.get("parent"):
                    close_calls += 1
                    if close_calls == 1:
                        before = os.fstat(descriptor)
                        real_close(descriptor)
                        opened = real_open(
                            root,
                            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                        )
                        if opened != descriptor:
                            os.dup2(opened, descriptor)
                            real_close(opened)
                        replacement = descriptor
                        after = os.fstat(replacement)
                        self.assertEqual(
                            (before.st_dev, before.st_ino),
                            (after.st_dev, after.st_ino),
                        )
                        raise failure
                return real_close(descriptor)

            try:
                with mock.patch.object(os, "open", side_effect=tracked_open), \
                        mock.patch.object(os, "close", side_effect=close_then_reuse):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        sharding._atomic_write_record(
                            destination, {"schema_version": 1}
                        )
                self.assertIs(failure, caught.exception)
                self.assertTrue(
                    sharding.is_indeterminate_descriptor_close(caught.exception)
                )
                self.assertEqual(1, close_calls)
                self.assertEqual(
                    b'{"schema_version":1}', destination.read_bytes()
                )
                os.fstat(replacement)
            finally:
                if replacement is not None:
                    try:
                        real_close(replacement)
                    except OSError:
                        pass

    def test_read_only_capture_stages_disjoint_writable_cases(self):
        self.assertEqual(
            ["root", "start", "terminal"],
            [field.name for field in fields(sharding.AttemptPaths)],
        )
        self.assertEqual(
            [
                "root", "cleanup", "attempts", "staging", "workspace", "store",
                "audit", "payload", "output", "home", "codex_home", "tmp",
                "config", "cache", "sealed",
            ],
            [field.name for field in fields(sharding.CasePaths)],
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            before = self._sha256_tree(snapshot)

            forward = self._assignment("forward", 9, "scope-supersession")
            lifecycle = self._assignment("lifecycle", 3, "scope-supersession")
            forward_paths = sharding.paths_for_case(root / "run", forward)
            lifecycle_paths = sharding.paths_for_case(root / "run", lifecycle)
            self.assertNotEqual(forward_paths.root, lifecycle_paths.root)
            self.assertEqual(
                "forward-09-scope-supersession", forward_paths.root.name
            )
            self.assertEqual(
                "lifecycle-03-scope-supersession", lifecycle_paths.root.name
            )
            self.assertEqual(
                forward_paths.root / "workspace" / "scope-supersession",
                forward_paths.workspace,
            )
            self.assertEqual(
                lifecycle_paths.root / "workspace" / "scope-supersession",
                lifecycle_paths.workspace,
            )

            forward_stage = sharding.stage_marketplace_for_case(
                read_only_snapshot=snapshot,
                destination=forward_paths.staging / "workflow-observatory",
            )
            lifecycle_stage = sharding.stage_marketplace_for_case(
                read_only_snapshot=snapshot,
                destination=lifecycle_paths.staging / "workflow-observatory",
            )
            for staged in (forward_stage, lifecycle_stage):
                cli = staged / (
                    "plugins/workflow-observer/scripts/workflow_observer_cli.py"
                )
                library = staged / (
                    "plugins/workflow-observer/scripts/store_config.py"
                )
                self.assertEqual(0o700, stat.S_IMODE(cli.stat().st_mode))
                self.assertEqual(0o600, stat.S_IMODE(library.stat().st_mode))
                self.assertTrue(all(
                    stat.S_IMODE(path.stat().st_mode) == 0o700
                    for path in (staged, cli.parent, library.parent)
                ))
                self.assertEqual(
                    (snapshot / cli.relative_to(staged)).read_bytes(),
                    cli.read_bytes(),
                )
            self.assertEqual(before, self._sha256_tree(snapshot))

            attempt_1 = sharding.paths_for_attempt(forward_paths, 1)
            attempt_2 = sharding.paths_for_attempt(forward_paths, 2)
            self.assertEqual(forward_paths.attempts / "01", attempt_1.root)
            self.assertEqual(attempt_1.root / "start.json", attempt_1.start)
            self.assertEqual(attempt_1.root / "terminal.json", attempt_1.terminal)
            self.assertEqual(forward_paths.attempts / "02", attempt_2.root)
            self.assertEqual(attempt_2.root / "start.json", attempt_2.start)
            self.assertEqual(attempt_2.root / "terminal.json", attempt_2.terminal)

    def test_production_runtime_factory_uses_writable_stage(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from scripts import run_observing_workflows_task9_eval as task9_eval

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            before = self._sha256_tree(snapshot)

            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            source_home = root / "source-codex-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_bytes(b'{"token":"TEST_ONLY_SECRET"}\n')
            auth.chmod(0o600)
            assignment = self._assignment("forward", 1, "shared-id")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )

            paths = sharding.paths_for_case(run_root, assignment)
            paths.workspace.mkdir(parents=True, mode=0o700)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            runtime = factory(
                assignment=assignment,
                manifest_case={"id": "shared-id", "setup": {"cli": "available"}},
                paths=paths,
                transport_config=config,
            )

            staged_cli = paths.staging / (
                "workflow-observatory/plugins/workflow-observer/scripts/"
                "workflow_observer_cli.py"
            )
            captured_cli = snapshot / (
                "plugins/workflow-observer/scripts/workflow_observer_cli.py"
            )
            self.assertEqual(staged_cli, runtime.audited_wrapper_path)
            self.assertNotEqual(captured_cli, runtime.audited_wrapper_path)
            self.assertEqual(0o700, stat.S_IMODE(staged_cli.stat().st_mode))
            self.assertEqual(config, runtime.transport_config)
            self.assertEqual(
                tuple[Path, ...],
                get_type_hints(task9_eval.CaseRuntime)["writable_roots"],
            )
            self.assertIsInstance(runtime.writable_roots, tuple)
            self.assertEqual(str(paths.home), runtime.environment["HOME"])
            self.assertEqual(
                str(paths.codex_home), runtime.environment["CODEX_HOME"]
            )
            self.assertEqual(str(paths.tmp), runtime.environment["TMPDIR"])
            self.assertEqual(
                str(paths.config), runtime.environment["XDG_CONFIG_HOME"]
            )
            self.assertEqual(
                str(paths.cache), runtime.environment["XDG_CACHE_HOME"]
            )
            self.assertEqual(str(paths.store), str(runtime.store_root))
            self.assertEqual(
                b'{"token":"TEST_ONLY_SECRET"}\n',
                (paths.codex_home / "auth.json").read_bytes(),
            )
            self.assertEqual(
                0o600,
                stat.S_IMODE((paths.codex_home / "auth.json").stat().st_mode),
            )
            self.assertFalse((paths.codex_home / "config.toml").exists())
            self.assertNotIn(paths.codex_home, runtime.writable_roots)
            for isolated in (
                paths.root,
                paths.workspace,
                paths.store,
                paths.audit,
                paths.payload,
                paths.output,
                paths.home,
                paths.codex_home,
                paths.tmp,
                paths.config,
                paths.cache,
            ):
                self.assertEqual(0o700, stat.S_IMODE(isolated.stat().st_mode))
            self.assertEqual(before, self._sha256_tree(snapshot))

            unrelated = root / "unrelated-codex-home"
            unrelated.mkdir(mode=0o700)
            keep = unrelated / "keep.txt"
            keep.write_text("keep", encoding="utf-8")
            keep.chmod(0o600)
            forged = replace(paths, codex_home=unrelated)
            with self.assertRaisesRegex(ValueError, "ownership|canonical"):
                factory.cleanup_case(forged)
            self.assertEqual("keep", keep.read_text(encoding="utf-8"))

            first = factory.cleanup_case(paths)
            second = factory.cleanup_case(paths)
            self.assertEqual(first, second)
            self.assertTrue(paths.codex_home.is_dir())
            self.assertEqual([], list(paths.codex_home.iterdir()))
            self.assertTrue((bootstrap.path / "auth.json").is_file())
            factory.close()
            os.close(bootstrap.descriptor)

    def test_production_driver_uses_captured_evaluator_with_transport_stub(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from scripts import run_observing_workflows_task9_eval as task9_eval

        self.assertTrue(
            hasattr(worker, "build_production_case_driver"),
            "production case driver is missing",
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            assignment = self._assignment("forward", 1, "shared-id")
            plan = self._plan(assignment)
            bootstrap = self._prepare_bootstrap(root, plan)
            paths = sharding.paths_for_case(run_root, assignment)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            captured_calls = []
            transport_calls = []
            expected_execution = task9_eval.CaseExecution(
                "completed", "done", (), (), task9_eval.ZERO_TOKEN_USAGE
            )

            class CapturedEvaluator:
                @staticmethod
                def _run_case(
                    case,
                    destination,
                    lifecycle,
                    runtime_factory=None,
                    *,
                    workspace_parent,
                    transport_runner,
                    event_sink,
                    execution_sink,
                ):
                    captured_calls.append(
                        (case["id"], destination, workspace_parent, lifecycle)
                    )
                    workspace = workspace_parent / case["id"]
                    workspace.mkdir()
                    runtime = runtime_factory(
                        case, destination, workspace, lifecycle
                    )
                    execution = transport_runner(
                        case,
                        workspace,
                        runtime,
                        runtime.store_root,
                        None,
                        event_sink=event_sink,
                    )
                    execution_sink(execution)
                    return {"id": case["id"], "captured": True}

            def transport_stub(*args, **kwargs):
                transport_calls.append((args, kwargs))
                return expected_execution

            with mock.patch.object(
                worker,
                "_load_captured_evaluator",
                return_value=CapturedEvaluator,
            ):
                driver = worker.build_production_case_driver(
                    snapshot_root=snapshot,
                    transport_config=config,
                    transport_runner=transport_stub,
                )
            driven = driver(
                assignment=assignment,
                manifest_case={
                    "id": "shared-id",
                    "fixture": "empty",
                    "turns": [{"prompt": "one"}],
                },
                paths=paths,
                runtime_factory=factory,
                event_sink=lambda *_: None,
            )

            self.assertEqual(
                [
                    (
                        "shared-id",
                        paths.root,
                        paths.workspace.parent,
                        False,
                    )
                ],
                captured_calls,
            )
            self.assertEqual(1, len(transport_calls))
            self.assertIs(expected_execution, driven.execution)
            self.assertEqual(
                {"id": "shared-id", "captured": True}, driven.result
            )
            self.assertTrue(
                paths.staging.joinpath(
                    "workflow-observatory/plugins/workflow-observer/scripts/"
                    "workflow_observer_cli.py"
                ).is_file()
            )
            self.assertEqual([], list(paths.codex_home.iterdir()))
            factory.close()
            os.close(bootstrap.descriptor)

    def test_production_driver_loads_actual_captured_run_case(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from scripts import run_observing_workflows_task9_eval as task9_eval
        from tests import observing_workflows_eval_harness as harness

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            captured = root / "captured-input"
            marketplace = captured / "workflow-observatory"
            self._write_read_only_marketplace(marketplace)
            evaluator_path = (
                captured
                / "evidence/scripts/run_observing_workflows_task9_eval.py"
            )
            evaluator_path.parent.mkdir(parents=True)
            evaluator_path.write_bytes(Path(task9_eval.__file__).read_bytes())
            evaluator_path.chmod(0o444)
            for directory in (
                evaluator_path.parent,
                evaluator_path.parent.parent,
                captured,
            ):
                directory.chmod(0o555)

            assignment = self._assignment("forward", 1, "shared-id")
            plan = self._plan(assignment)
            bootstrap = self._prepare_bootstrap(root, plan)
            paths = sharding.paths_for_case(run_root, assignment)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            factory = worker.build_production_runtime_factory(
                snapshot_root=captured,
                transport_config=config,
                plan=plan,
            )
            calls = []

            def transport_stub(
                case,
                workspace,
                runtime,
                wiki_root,
                after_first_turn=None,
                event_sink=None,
            ):
                calls.append((case["id"], workspace, runtime, wiki_root))
                self.assertEqual(paths.workspace, workspace)
                self.assertTrue((workspace / ".git").is_dir())
                self.assertTrue(
                    workspace.joinpath(
                        ".agents/skills/workflow-observer/SKILL.md"
                    ).is_file()
                )
                self.assertEqual(paths.store, runtime.store_root)
                self.assertEqual(paths.store, wiki_root)
                self.assertEqual(workspace, harness._GATE_ROOTS[case["id"]])
                driver._evaluator.release_gate(case["id"])
                return task9_eval.CaseExecution(
                    "completed",
                    "done",
                    (),
                    (),
                    task9_eval.ZERO_TOKEN_USAGE,
                )

            driver = worker.build_production_case_driver(
                snapshot_root=captured,
                transport_config=config,
                transport_runner=transport_stub,
            )
            self.assertEqual(
                evaluator_path.resolve(strict=True),
                Path(driver._evaluator.__file__).resolve(strict=True),
            )
            driver._evaluator.run_configured_integrity = lambda *a, **k: None
            driven = driver(
                assignment=assignment,
                manifest_case={
                    "id": "shared-id",
                    "fixture": "empty",
                    "turns": [{"prompt": "run scripts/gate.py"}],
                    "expected_run_count": 0,
                    "expected_draft_count": 0,
                    "expected_final_statuses": [],
                    "expected_decisions": [
                        {"after_turn": 1, "triggered": False}
                    ],
                },
                paths=paths,
                runtime_factory=factory,
                event_sink=lambda *_: None,
            )

            self.assertEqual("shared-id", driven.result["id"])
            self.assertEqual(1, len(calls))
            self.assertEqual(paths.workspace, calls[0][1])
            self.assertNotIn("shared-id", harness._GATE_ROOTS)
            self.assertEqual([], list(paths.codex_home.iterdir()))
            factory.close()
            os.close(bootstrap.descriptor)

    def test_captured_evaluator_loader_never_reuses_another_snapshot_module(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from scripts import run_observing_workflows_task9_eval as task9_eval

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            sources = []
            for name in ("capture-a", "capture-b"):
                source = (
                    root
                    / name
                    / "evidence/scripts/run_observing_workflows_task9_eval.py"
                )
                source.parent.mkdir(parents=True, mode=0o700)
                source.write_bytes(Path(task9_eval.__file__).read_bytes())
                source.chmod(0o444)
                sources.append(source)

            first = worker._load_captured_evaluator(sources[0].parents[2])
            second = worker._load_captured_evaluator(sources[1].parents[2])

        self.assertIsNot(first, second)
        self.assertEqual(sources[0], Path(first.__file__))
        self.assertEqual(sources[1], Path(second.__file__))

    def test_production_driver_honors_captured_process_survival_type(self):
        from scripts import run_observing_workflows_eval_worker as worker

        class ForeignProcessSurvival(RuntimeError):
            pass

        class CapturedEvaluator:
            @staticmethod
            def _contains_process_survival_failure(error):
                return isinstance(error, ForeignProcessSurvival)

            @staticmethod
            def _run_case(
                case,
                destination,
                lifecycle,
                runtime_factory=None,
                *,
                workspace_parent,
                transport_runner,
                event_sink,
                execution_sink,
            ):
                workspace = workspace_parent / case["id"]
                workspace.mkdir()
                runtime_factory(case, destination, workspace, lifecycle)
                raise ForeignProcessSurvival("foreign captured survival")

        class Factory:
            poisoned = False

            def __init__(self):
                self.cleaned = []

            def __call__(self, **kwargs):
                return mock.sentinel.runtime

            def cleanup_case(self, paths):
                self.cleaned.append(paths)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            assignment = self._assignment("forward", 1, "shared-id")
            paths = sharding.paths_for_case(run_root, assignment)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            with mock.patch.object(
                worker,
                "_load_captured_evaluator",
                return_value=CapturedEvaluator,
            ):
                driver = worker.build_production_case_driver(
                    snapshot_root=root,
                    transport_config=config,
                    transport_runner=mock.Mock(),
                )
            factory = Factory()
            with self.assertRaises(
                worker._CapturedEvaluatorFailure
            ) as raised:
                driver(
                    assignment=assignment,
                    manifest_case={"id": "shared-id"},
                    paths=paths,
                    runtime_factory=factory,
                    event_sink=lambda *_: None,
                )
            self.assertEqual(
                "surviving-process",
                worker._classify_worker_failure(
                    raised.exception, model_started=False
                ),
            )
            self.assertTrue(
                worker.worker_exit_required(raised.exception, factory)
            )

        self.assertEqual([], factory.cleaned)

    def test_staging_and_attempt_scans_reject_unsafe_or_partial_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            mutable_snapshot = root / "mutable-snapshot"
            self._write_read_only_marketplace(mutable_snapshot)
            mutable_file = mutable_snapshot / "README.md"
            mutable_file.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "0444"):
                sharding.stage_marketplace_for_case(
                    read_only_snapshot=mutable_snapshot,
                    destination=root / "mutable-stage",
                )

            snapshot = root / "snapshot"
            self._write_read_only_marketplace(snapshot)
            outside = root / "outside"
            outside.write_text("outside", encoding="utf-8")
            snapshot.chmod(0o755)
            (snapshot / "unsafe-link").symlink_to(outside)
            snapshot.chmod(0o555)
            with self.assertRaisesRegex(ValueError, "symlink|special"):
                sharding.stage_marketplace_for_case(
                    read_only_snapshot=snapshot,
                    destination=root / "stage",
                )

            special_snapshot = root / "special-snapshot"
            self._write_read_only_marketplace(special_snapshot)
            special_snapshot.chmod(0o755)
            os.mkfifo(special_snapshot / "unsafe-fifo", mode=0o444)
            special_snapshot.chmod(0o555)
            with self.assertRaisesRegex(ValueError, "special"):
                sharding.stage_marketplace_for_case(
                    read_only_snapshot=special_snapshot,
                    destination=root / "special-stage",
                )

            manifests = {
                "forward": load_cases("observing_workflows_cases.json"),
                "lifecycle": load_cases(
                    "observing_workflows_lifecycle_cases.json"
                ),
            }
            plan = sharding.build_epoch_plan(
                run_kind="diagnostic",
                manifests=manifests,
                fingerprints=input_fingerprints("diagnostic"),
            )
            assignment = plan.assignments[0]
            manifest_case = manifests["forward"][0]
            run_root = root / "run"
            (run_root / "cases").mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.root.mkdir(mode=0o700)
            sharding.write_attempt_start(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
            )
            attempt = sharding.paths_for_attempt(paths, 1)
            with self.assertRaisesRegex(ValueError, "partial|incomplete"):
                sharding.scan_attempts(
                    paths,
                    plan=plan,
                    manifest_case=manifest_case,
                )
            start = json.loads(attempt.start.read_text(encoding="ascii"))
            failure_text = "pre-model failure"
            sharding._atomic_write_record(
                attempt.terminal,
                {
                    **start,
                    "start_sha256": hashlib.sha256(
                        attempt.start.read_bytes()
                    ).hexdigest(),
                    "status": "failed",
                    "classification": "pre-model-infrastructure",
                    "model_started": False,
                    "cleanup_passed": False,
                    "usage": None,
                    "failure": {
                        "classification": "pre-model-infrastructure",
                        "type": "RuntimeError",
                        "chars": len(failure_text),
                        "sha256": hashlib.sha256(
                            failure_text.encode("utf-8")
                        ).hexdigest(),
                    },
                    "tombstone_receipt_sha256": None,
                },
            )
            self.assertEqual(
                1,
                len(
                    sharding.scan_attempts(
                        paths,
                        plan=plan,
                        manifest_case=manifest_case,
                    )
                ),
            )
            gap = paths.attempts / "02"
            attempt.root.rename(gap)
            with self.assertRaisesRegex(ValueError, "gap"):
                sharding.scan_attempts(
                    paths,
                    plan=plan,
                    manifest_case=manifest_case,
                )
            gap.rename(attempt.root)
            extra = paths.attempts / "03"
            extra.mkdir(mode=0o700)
            with self.assertRaisesRegex(ValueError, "attempt"):
                sharding.scan_attempts(
                    paths,
                    plan=plan,
                    manifest_case=manifest_case,
                )

    def test_runtime_factory_rejects_symlinked_case_ancestor(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            outside = root / "outside"
            outside.mkdir(mode=0o700)
            (run_root / "cases").symlink_to(outside, target_is_directory=True)

            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            paths = sharding.paths_for_case(run_root, assignment)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )

            with self.assertRaisesRegex(ValueError, "symlink"):
                factory(
                    assignment=assignment,
                    manifest_case={"id": "case"},
                    paths=paths,
                    transport_config=config,
                )
            self.assertEqual([], list(outside.iterdir()))
            factory.close()
            os.close(bootstrap.descriptor)

    def test_runtime_factory_rejects_symlink_immediately_above_run_root(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            target = root / "symlink-target"
            target.mkdir(mode=0o700)
            alias = root / "run-alias"
            alias.symlink_to(target, target_is_directory=True)
            run_root = alias / "run"
            run_root.mkdir(mode=0o700)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            with self.assertRaisesRegex(ValueError, "canonical|symlink"):
                sharding.paths_for_case(run_root, assignment)
            canonical_paths = sharding.paths_for_case(target / "run", assignment)
            paths = sharding.CasePaths(**{
                field.name: alias
                / getattr(canonical_paths, field.name).relative_to(target)
                for field in fields(sharding.CasePaths)
            })
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )

            with self.assertRaisesRegex(ValueError, "canonical|symlink"):
                factory(
                    assignment=assignment,
                    manifest_case={"id": "case"},
                    paths=paths,
                    transport_config=config,
                )
            self.assertFalse(
                (target / "run/cases/forward-01-case").exists(),
                "factory wrote through the symlinked run-root ancestor",
            )

    def test_scan_attempts_rejects_every_forged_case_path_field(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            manifests = {
                "forward": load_cases("observing_workflows_cases.json"),
                "lifecycle": load_cases(
                    "observing_workflows_lifecycle_cases.json"
                ),
            }
            plan = sharding.build_epoch_plan(
                run_kind="diagnostic",
                manifests=manifests,
                fingerprints=input_fingerprints("diagnostic"),
            )
            assignment = plan.assignments[0]
            manifest_case = manifests["forward"][0]
            (run_root / "cases").mkdir(mode=0o700)
            run_root.chmod(0o700)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.root.mkdir(mode=0o700)
            sharding.write_attempt_start(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
            )
            attempt = sharding.paths_for_attempt(paths, 1)
            start = json.loads(attempt.start.read_text(encoding="ascii"))
            failure_text = "pre-model failure"
            sharding._atomic_write_record(
                attempt.terminal,
                {
                    **start,
                    "start_sha256": hashlib.sha256(
                        attempt.start.read_bytes()
                    ).hexdigest(),
                    "status": "failed",
                    "classification": "pre-model-infrastructure",
                    "model_started": False,
                    "cleanup_passed": False,
                    "usage": None,
                    "failure": {
                        "classification": "pre-model-infrastructure",
                        "type": "RuntimeError",
                        "chars": len(failure_text),
                        "sha256": hashlib.sha256(
                            failure_text.encode("utf-8")
                        ).hexdigest(),
                    },
                    "tombstone_receipt_sha256": None,
                },
            )
            self.assertEqual(
                1,
                len(
                    sharding.scan_attempts(
                        paths,
                        plan=plan,
                        manifest_case=manifest_case,
                    )
                ),
            )

            unrelated = root / "unrelated"
            unrelated.mkdir(mode=0o700)
            external_attempt = unrelated / "attempts/01"
            external_attempt.mkdir(parents=True, mode=0o700)
            for name in ("start.json", "terminal.json"):
                artifact = external_attempt / name
                artifact.write_text("{}", encoding="utf-8")
                artifact.chmod(0o600)

            for field in fields(sharding.CasePaths):
                redirected = unrelated / field.name
                if field.name == "attempts":
                    redirected = unrelated / "attempts"
                forged = replace(paths, **{field.name: redirected})
                with self.subTest(field=field.name):
                    with self.assertRaises(ValueError):
                        sharding.scan_attempts(
                            forged,
                            plan=plan,
                            manifest_case=manifest_case,
                        )

            dotdot = replace(
                paths,
                attempts=paths.root / "nested" / ".." / "attempts",
            )
            with self.assertRaisesRegex(ValueError, "canonical"):
                sharding.scan_attempts(
                    dotdot,
                    plan=plan,
                    manifest_case=manifest_case,
                )

            alias = root / "attempt-alias"
            alias.symlink_to(paths.attempts, target_is_directory=True)
            symlinked = replace(paths, attempts=alias)
            with self.assertRaisesRegex(ValueError, "canonical"):
                sharding.scan_attempts(
                    symlinked,
                    plan=plan,
                    manifest_case=manifest_case,
                )

    def test_runtime_factory_preserves_setup_and_auth_cleanup_failures(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.workspace.mkdir(parents=True, mode=0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            setup_error = ValueError("post-auth setup failed")
            cleanup_error = RuntimeError("auth cleanup failed")

            with mock.patch.object(
                worker,
                "inventory_external_skill_paths",
                side_effect=setup_error,
            ), mock.patch.object(
                worker,
                "cleanup_case_auth",
                side_effect=cleanup_error,
            ):
                with self.assertRaises(ExceptionGroup) as caught:
                    factory(
                        assignment=assignment,
                        manifest_case={"id": "case"},
                        paths=paths,
                        transport_config=config,
                    )
            self.assertEqual(
                (setup_error, cleanup_error), caught.exception.exceptions
            )
            factory.close()
            os.close(bootstrap.descriptor)

    def test_runtime_cleanup_rejects_forged_codex_home(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.workspace.mkdir(parents=True, mode=0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            factory(
                assignment=assignment,
                manifest_case={"id": "case"},
                paths=paths,
                transport_config=config,
            )
            unrelated = root / "unrelated-codex-home"
            unrelated.mkdir(mode=0o700)
            keep = unrelated / "keep.txt"
            keep.write_text("keep", encoding="utf-8")
            keep.chmod(0o600)

            forged = replace(paths, codex_home=unrelated)
            with self.assertRaisesRegex(ValueError, "ownership|canonical"):
                factory.cleanup_case(forged)
            self.assertEqual("keep", keep.read_text(encoding="utf-8"))
            factory.cleanup_case(paths)
            self.assertTrue(paths.codex_home.is_dir())
            self.assertEqual([], list(paths.codex_home.iterdir()))
            factory.close()
            os.close(bootstrap.descriptor)

    def test_runtime_cleanup_scrubs_owned_descriptor_and_records_replacement(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.workspace.mkdir(parents=True, mode=0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            factory(
                assignment=assignment,
                manifest_case={"id": "case"},
                paths=paths,
                transport_config=config,
            )

            owned_home = root / "displaced-owned-codex-home"
            paths.codex_home.rename(owned_home)
            paths.codex_home.mkdir(mode=0o700)
            keep = paths.codex_home / "keep.txt"
            keep.write_text("keep", encoding="utf-8")
            keep.chmod(0o600)

            receipt = factory.cleanup_case(paths)
            self.assertEqual("replaced", receipt.canonical_binding)
            self.assertEqual("keep", keep.read_text(encoding="utf-8"))
            self.assertEqual([], list(owned_home.iterdir()))
            factory.close()
            os.close(bootstrap.descriptor)

    def test_runtime_factory_retains_one_descriptor_and_close_releases_it(self):
        from scripts import run_observing_workflows_eval_worker as worker

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            snapshot = root / "captured-input" / "workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            run_root = root / "run"
            run_root.mkdir(mode=0o700)
            coordinator = run_root / "coordinator"
            coordinator.mkdir(mode=0o700)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            assignment = self._assignment("forward", 1, "case")
            plan = self._plan(assignment)
            bootstrap = sharding.prepare_auth_bootstrap(
                source_codex_home=source_home,
                coordinator_root=coordinator,
                plan=plan,
            )
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            config = self._transport_config(executable)
            paths = sharding.paths_for_case(run_root, assignment)
            paths.workspace.mkdir(parents=True, mode=0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=config,
                plan=plan,
            )
            baseline = self._fd_count()
            factory(
                assignment=assignment,
                manifest_case={"id": "case"},
                paths=paths,
                transport_config=config,
            )
            self.assertEqual(baseline + 1, self._fd_count())
            with self.assertRaisesRegex(ValueError, "already exists"):
                factory(
                    assignment=assignment,
                    manifest_case={"id": "case"},
                    paths=paths,
                    transport_config=config,
                )
            self.assertEqual(baseline + 1, self._fd_count())
            factory.close()
            self.assertEqual(baseline, self._fd_count())
            self.assertTrue((paths.codex_home / "auth.json").is_file())
            factory.close()
            self.assertEqual(baseline, self._fd_count())
            os.close(bootstrap.descriptor)

    def test_bootstrap_ownership_precedes_secret_and_interruption_closes(self):
        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("BOOTSTRAP_SECRET", encoding="utf-8")
            auth.chmod(0o600)
            coordinator = root / "run/coordinator"
            coordinator.mkdir(parents=True, mode=0o700)
            baseline = self._fd_count()

            def interrupt_after_ownership(source_descriptor, directory_descriptor, name):
                ownership_path = coordinator / "cleanup/bootstrap-ownership.json"
                self.assertTrue(ownership_path.is_file())
                ownership = sharding.read_bootstrap_ownership(
                    coordinator_root=coordinator,
                    plan=plan,
                )
                self.assertEqual(plan.epoch_id, ownership.epoch_id)
                self.assertFalse((coordinator / "auth-bootstrap/auth.json").exists())
                raise OSError("interrupted before auth copy")

            with mock.patch.object(
                sharding,
                "_copy_auth_descriptor_at",
                side_effect=interrupt_after_ownership,
            ):
                with self.assertRaisesRegex(ValueError, "bootstrap"):
                    sharding.prepare_auth_bootstrap(
                        source_codex_home=source_home,
                        coordinator_root=coordinator,
                        plan=plan,
                    )

            self.assertEqual(baseline, self._fd_count())
            ownership = sharding.read_bootstrap_ownership(
                coordinator_root=coordinator,
                plan=plan,
            )
            self.assertEqual(plan.run_kind, ownership.run_kind)
            self.assertTrue((coordinator / "auth-bootstrap").is_dir())
            self.assertFalse((coordinator / "auth-bootstrap/auth.json").exists())

    def test_case_ownership_precedes_secret_and_interruption_closes(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            paths = sharding.paths_for_case(root / "run", assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            baseline = self._fd_count()

            def interrupt_after_ownership(source_descriptor, directory_descriptor, name):
                ownership = worker.read_case_auth_ownership(
                    plan=plan, assignment=assignment, paths=paths
                )
                self.assertEqual(assignment.key, ownership.case)
                self.assertFalse((paths.codex_home / "auth.json").exists())
                raise KeyboardInterrupt("interrupted before case auth copy")

            with mock.patch.object(
                sharding,
                "_copy_auth_descriptor_at",
                side_effect=interrupt_after_ownership,
            ):
                with self.assertRaisesRegex(
                    KeyboardInterrupt, "interrupted before case auth copy"
                ):
                    sharding.install_case_auth(
                        bootstrap=bootstrap.path,
                        plan=plan,
                        assignment=assignment,
                        paths=paths,
                    )
            self.assertEqual(baseline, self._fd_count())
            self.assertTrue((paths.cleanup / "ownership.json").is_file())
            self.assertTrue(paths.codex_home.is_dir())
            self.assertFalse((paths.codex_home / "auth.json").exists())
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_bootstrap_namespace_fsync_barriers_precede_secret_copy(self):
        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("BOOTSTRAP_SECRET", encoding="utf-8")
            auth.chmod(0o600)
            coordinator = root / "run/coordinator"
            coordinator.mkdir(parents=True, mode=0o700)
            coordinator_identity = (
                coordinator.stat().st_dev,
                coordinator.stat().st_ino,
            )
            events = []
            bootstrap_identities = set()
            real_fsync = os.fsync
            real_record = sharding._atomic_write_record
            real_copy = sharding._copy_auth_descriptor_at

            def traced_fsync(descriptor):
                identity = (os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino)
                if identity == coordinator_identity:
                    events.append("coordinator-fsync")
                elif identity in bootstrap_identities:
                    events.append("bootstrap-fsync")
                return real_fsync(descriptor)

            def traced_record(path, payload):
                if path.name == "bootstrap-ownership.json":
                    events.append("ownership")
                return real_record(path, payload)

            def traced_copy(source_descriptor, directory_descriptor, name):
                metadata = os.fstat(directory_descriptor)
                bootstrap_identities.add((metadata.st_dev, metadata.st_ino))
                events.append("secret-copy")
                return real_copy(source_descriptor, directory_descriptor, name)

            with mock.patch.object(os, "fsync", side_effect=traced_fsync), \
                    mock.patch.object(
                        sharding, "_atomic_write_record", side_effect=traced_record
                    ), mock.patch.object(
                        sharding, "_copy_auth_descriptor_at", side_effect=traced_copy
                    ):
                bootstrap = sharding.prepare_auth_bootstrap(
                    source_codex_home=source_home,
                    coordinator_root=coordinator,
                    plan=plan,
                )

            parent_barriers = [
                index for index, event in enumerate(events)
                if event == "coordinator-fsync"
            ]
            self.assertGreaterEqual(len(parent_barriers), 2, events)
            ownership_index = events.index("ownership")
            copy_index = events.index("secret-copy")
            self.assertTrue(all(index < ownership_index for index in parent_barriers[:2]))
            self.assertLess(ownership_index, copy_index)
            self.assertGreater(events.index("bootstrap-fsync"), copy_index)
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_case_namespace_fsync_barriers_precede_secret_copy(self):
        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            paths = sharding.paths_for_case(root / "run", assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            case_root_identity = (
                paths.root.stat().st_dev,
                paths.root.stat().st_ino,
            )
            events = []
            codex_identities = set()
            real_fsync = os.fsync
            real_record = sharding._atomic_write_record
            real_copy = sharding._copy_auth_descriptor_at

            def traced_fsync(descriptor):
                metadata = os.fstat(descriptor)
                identity = (metadata.st_dev, metadata.st_ino)
                if identity == case_root_identity:
                    events.append("case-root-fsync")
                elif identity in codex_identities:
                    events.append("codex-home-fsync")
                return real_fsync(descriptor)

            def traced_record(path, payload):
                if path.name == "ownership.json":
                    events.append("ownership")
                return real_record(path, payload)

            def traced_copy(source_descriptor, directory_descriptor, name):
                metadata = os.fstat(directory_descriptor)
                codex_identities.add((metadata.st_dev, metadata.st_ino))
                events.append("secret-copy")
                return real_copy(source_descriptor, directory_descriptor, name)

            with mock.patch.object(os, "fsync", side_effect=traced_fsync), \
                    mock.patch.object(
                        sharding, "_atomic_write_record", side_effect=traced_record
                    ), mock.patch.object(
                        sharding, "_copy_auth_descriptor_at", side_effect=traced_copy
                    ):
                installed = sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )

            parent_barriers = [
                index for index, event in enumerate(events)
                if event == "case-root-fsync"
            ]
            self.assertGreaterEqual(len(parent_barriers), 2, events)
            ownership_index = events.index("ownership")
            copy_index = events.index("secret-copy")
            self.assertTrue(all(index < ownership_index for index in parent_barriers[:2]))
            self.assertLess(ownership_index, copy_index)
            self.assertGreater(events.index("codex-home-fsync"), copy_index)
            os.close(installed.descriptor)
            installed.descriptor = -1
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_worker_fsyncs_empty_owned_home_before_tombstone_publish(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap, paths, installed = self._install_case(
                root, plan, assignment
            )
            events = []
            removed = False
            real_remove = worker._remove_tree_entry
            real_fstat = os.fstat
            real_scandir = os.scandir
            real_fsync = os.fsync
            real_record = worker._atomic_write_record

            def traced_remove(*args, **kwargs):
                nonlocal removed
                result = real_remove(*args, **kwargs)
                removed = True
                events.append("delete")
                return result

            def traced_fstat(descriptor):
                metadata = real_fstat(descriptor)
                if descriptor == installed.descriptor and removed:
                    events.append("identity")
                return metadata

            def traced_scandir(path):
                if path == installed.descriptor and removed:
                    events.append("empty")
                return real_scandir(path)

            def traced_fsync(descriptor):
                if descriptor == installed.descriptor:
                    events.append("codex-home-fsync")
                return real_fsync(descriptor)

            def traced_record(path, payload):
                if path.name == "tombstone.json":
                    events.append("tombstone-publish")
                return real_record(path, payload)

            with mock.patch.object(
                worker, "_remove_tree_entry", side_effect=traced_remove
            ), mock.patch.object(os, "fstat", side_effect=traced_fstat), \
                    mock.patch.object(os, "scandir", side_effect=traced_scandir), \
                    mock.patch.object(os, "fsync", side_effect=traced_fsync), \
                    mock.patch.object(
                        worker, "_atomic_write_record", side_effect=traced_record
                    ):
                worker.cleanup_case_auth(installed=installed, paths=paths)

            self.assertLess(events.index("delete"), events.index("identity"))
            self.assertLess(events.index("identity"), events.index("empty"))
            self.assertLess(events.index("empty"), events.index("codex-home-fsync"))
            self.assertLess(
                events.index("codex-home-fsync"),
                events.index("tombstone-publish"),
            )
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_worker_base_exceptions_restore_active_descriptor_for_retry(self):
        from scripts import run_observing_workflows_eval_worker as worker

        for failure in (
            RuntimeError("runtime partial scrub"),
            KeyboardInterrupt("keyboard partial scrub"),
        ):
            with self.subTest(failure=type(failure).__name__), \
                    tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(strict=True)
                assignment = self._assignment(
                    "forward", 1, f"case-{type(failure).__name__.lower()}"
                )
                plan = self._plan(assignment)
                bootstrap, paths, installed = self._install_case(
                    root, plan, assignment
                )
                for name in ("one", "two"):
                    child = paths.codex_home / name
                    child.write_text(name, encoding="utf-8")
                    child.chmod(0o600)
                real_remove = worker._remove_tree_entry
                injected = False

                def remove_then_fail(*args, **kwargs):
                    nonlocal injected
                    result = real_remove(*args, **kwargs)
                    if not injected:
                        injected = True
                        raise failure
                    return result

                with mock.patch.object(
                    worker, "_remove_tree_entry", side_effect=remove_then_fail
                ):
                    with self.assertRaises(type(failure)) as caught:
                        worker.cleanup_case_auth(installed=installed, paths=paths)
                self.assertIs(failure, caught.exception)
                self.assertEqual("active", installed.state)
                self.assertEqual("owned", installed.descriptor_close_state)
                self.assertIsNone(installed.descriptor_close_error)
                os.fstat(installed.descriptor)
                self.assertFalse((paths.codex_home / "auth.json").exists())
                self.assertEqual(2, len(list(paths.codex_home.iterdir())))

                receipt = worker.cleanup_case_auth(
                    installed=installed, paths=paths
                )
                self.assertTrue(receipt.scrubbed)
                self.assertEqual("tombstoned", installed.state)
                os.close(bootstrap.descriptor)
                bootstrap.descriptor = -1

    def test_worker_close_failure_retires_capability_without_closing_same_identity_reuse(self):
        from scripts import run_observing_workflows_eval_worker as worker

        for failure in (
            RuntimeError("close failed after same-identity reuse"),
            KeyboardInterrupt("close interrupted after same-identity reuse"),
        ):
            with self.subTest(failure=type(failure).__name__), \
                    tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(strict=True)
                assignment = self._assignment(
                    "forward", 1, f"case-{type(failure).__name__.lower()}"
                )
                plan = self._plan(assignment)
                bootstrap, paths, installed = self._install_case(
                    root, plan, assignment
                )
                target = installed.descriptor
                original_identity = os.fstat(target).st_dev, os.fstat(target).st_ino
                real_close = os.close
                replacement = None
                close_calls = 0

                def close_then_reuse(descriptor):
                    nonlocal replacement, close_calls
                    if descriptor == target:
                        close_calls += 1
                        if close_calls == 1:
                            real_close(descriptor)
                            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                            opened = os.open(paths.codex_home, flags)
                            if opened != target:
                                os.dup2(opened, target)
                                real_close(opened)
                            replacement = target
                            self.assertEqual(target, replacement)
                            identity = (
                                os.fstat(replacement).st_dev,
                                os.fstat(replacement).st_ino,
                            )
                            self.assertEqual(original_identity, identity)
                            raise failure
                    return real_close(descriptor)

                try:
                    with mock.patch.object(os, "close", side_effect=close_then_reuse):
                        with self.assertRaises(type(failure)) as caught:
                            worker.cleanup_case_auth(installed=installed, paths=paths)
                    self.assertIs(failure, caught.exception)
                    self.assertEqual(1, close_calls)
                    self.assertEqual(-1, installed.descriptor)
                    self.assertEqual("tombstoned", installed.state)
                    self.assertEqual("indeterminate", installed.descriptor_close_state)
                    self.assertIs(failure, installed.descriptor_close_error)
                    os.fstat(replacement)

                    first_bytes = (paths.cleanup / "tombstone.json").read_bytes()
                    def reject_retired_close(descriptor):
                        if descriptor == target:
                            raise AssertionError("retired descriptor was retried")
                        return real_close(descriptor)

                    with mock.patch.object(
                        os,
                        "close",
                        side_effect=reject_retired_close,
                    ), mock.patch.object(
                        worker,
                        "_remove_tree_entry",
                        side_effect=AssertionError("tombstoned retry re-scrubbed"),
                    ):
                        with self.assertRaises(type(failure)) as repeated:
                            worker.cleanup_case_auth(
                                installed=installed, paths=paths
                            )
                    self.assertIs(failure, repeated.exception)
                    self.assertEqual(
                        first_bytes,
                        (paths.cleanup / "tombstone.json").read_bytes(),
                    )
                    os.fstat(replacement)
                finally:
                    if replacement is not None:
                        try:
                            real_close(replacement)
                        except OSError:
                            pass
                    if installed.descriptor >= 0:
                        try:
                            real_close(installed.descriptor)
                        except OSError:
                            pass
                        installed.descriptor = -1
                    real_close(bootstrap.descriptor)
                    bootstrap.descriptor = -1

    def test_worker_close_failures_are_one_shot_even_when_original_may_remain_open(self):
        from scripts import run_observing_workflows_eval_worker as worker

        outcomes = (
            (RuntimeError("pre-close runtime failure"), False),
            (KeyboardInterrupt("pre-close interrupt"), False),
            (OSError(errno.EBADF, "pre-close bad descriptor"), False),
            (OSError(errno.EIO, "post-close I/O failure"), True),
        )
        for failure, close_before_raise in outcomes:
            with self.subTest(failure=repr(failure)), \
                    tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(strict=True)
                assignment = self._assignment(
                    "forward", 1, f"case-{failure.__class__.__name__.lower()}-{failure.errno if isinstance(failure, OSError) else 'base'}"
                )
                plan = self._plan(assignment)
                bootstrap, paths, installed = self._install_case(
                    root, plan, assignment
                )
                target = installed.descriptor
                real_close = os.close
                close_calls = 0
                guard_descriptor = None

                def fail_once(descriptor):
                    nonlocal close_calls
                    if descriptor == target:
                        close_calls += 1
                        if close_calls == 1:
                            if close_before_raise:
                                real_close(descriptor)
                            raise failure
                    return real_close(descriptor)

                try:
                    with mock.patch.object(os, "close", side_effect=fail_once):
                        with self.assertRaises(type(failure)) as caught:
                            worker.cleanup_case_auth(
                                installed=installed, paths=paths
                            )
                    self.assertIs(failure, caught.exception)
                    self.assertEqual(1, close_calls)
                    self.assertEqual(-1, installed.descriptor)
                    self.assertEqual("indeterminate", installed.descriptor_close_state)
                    self.assertIs(failure, installed.descriptor_close_error)

                    if close_before_raise:
                        opened = os.open(os.devnull, os.O_RDONLY)
                        if opened != target:
                            os.dup2(opened, target)
                            real_close(opened)
                        guard_descriptor = target

                    def reject_retired_close(descriptor):
                        if descriptor == target:
                            raise AssertionError("retired descriptor was retried")
                        return real_close(descriptor)

                    with mock.patch.object(
                        os,
                        "close",
                        side_effect=reject_retired_close,
                    ):
                        with self.assertRaises(type(failure)) as repeated:
                            worker.cleanup_case_auth(
                                installed=installed, paths=paths
                            )
                    self.assertIs(failure, repeated.exception)
                finally:
                    if guard_descriptor is not None:
                        try:
                            real_close(guard_descriptor)
                        except OSError:
                            pass
                    elif not close_before_raise:
                        try:
                            real_close(target)
                        except OSError:
                            pass
                    real_close(bootstrap.descriptor)
                    bootstrap.descriptor = -1

    def test_worker_successful_retirement_is_closed_and_idempotent(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap, paths, installed = self._install_case(
                root, plan, assignment
            )
            first = worker.cleanup_case_auth(installed=installed, paths=paths)
            second = worker.cleanup_case_auth(installed=installed, paths=paths)
            self.assertEqual(first, second)
            self.assertEqual(-1, installed.descriptor)
            self.assertEqual("closed", installed.descriptor_close_state)
            self.assertIsNone(installed.descriptor_close_error)
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_factory_close_is_terminal_and_rereports_stored_error_without_retry(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap, paths, installed = self._install_case(
                root, plan, assignment
            )
            target = installed.descriptor
            real_close = os.close
            failure = RuntimeError("one-shot final close failure")
            close_calls = 0

            def fail_once_while_open(descriptor):
                nonlocal close_calls
                if descriptor == target:
                    close_calls += 1
                    if close_calls == 1:
                        raise failure
                return real_close(descriptor)

            with mock.patch.object(os, "close", side_effect=fail_once_while_open):
                with self.assertRaises(RuntimeError) as caught:
                    worker.cleanup_case_auth(installed=installed, paths=paths)
            self.assertIs(failure, caught.exception)
            self.assertEqual("tombstoned", installed.state)
            self.assertEqual(-1, installed.descriptor)
            self.assertEqual("indeterminate", installed.descriptor_close_state)
            os.fstat(target)

            snapshot = root / "captured-input/workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=self._transport_config(executable),
                plan=plan,
            )
            factory._owned_cases[paths.root] = (paths, installed)

            for _ in range(2):
                with mock.patch.object(
                    os,
                    "close",
                    side_effect=AssertionError("factory retried retired descriptor"),
                ):
                    with self.assertRaises(RuntimeError) as repeated:
                        factory.close()
                self.assertIs(failure, repeated.exception)
            self.assertEqual(1, close_calls)
            real_close(target)
            real_close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_factory_close_orders_multiple_exact_errors_and_never_retries(self):
        from scripts import run_observing_workflows_eval_worker as worker

        first_assignment = self._assignment("forward", 1, "a-case")
        second_assignment = self._assignment("forward", 2, "b-case")
        epoch_id = "e" * 64
        plan = sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind="diagnostic",
            fingerprints=replace(
                input_fingerprints("diagnostic"), epoch_id=epoch_id
            ),
            assignments=(first_assignment, second_assignment),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            installed_by_assignment = {}
            for assignment in (first_assignment, second_assignment):
                paths = sharding.paths_for_case(root / "run", assignment)
                paths.root.mkdir(parents=True, mode=0o700)
                paths.cleanup.mkdir(mode=0o700)
                installed = sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )
                installed_by_assignment[assignment] = (paths, installed)

            snapshot = root / "captured-input/workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=self._transport_config(executable),
                plan=plan,
            )
            for assignment in (second_assignment, first_assignment):
                paths, installed = installed_by_assignment[assignment]
                factory._owned_cases[paths.root] = (paths, installed)

            first_installed = installed_by_assignment[first_assignment][1]
            second_installed = installed_by_assignment[second_assignment][1]
            first_descriptor = first_installed.descriptor
            second_descriptor = second_installed.descriptor
            first_failure = RuntimeError("a close failed")
            second_failure = KeyboardInterrupt("b close failed")
            failures = {
                first_descriptor: first_failure,
                second_descriptor: second_failure,
            }
            retired_descriptors = tuple(failures)
            close_calls = []
            real_close = os.close

            def fail_owned_close(descriptor):
                if descriptor in failures:
                    close_calls.append(descriptor)
                    raise failures[descriptor]
                return real_close(descriptor)

            try:
                with mock.patch.object(os, "close", side_effect=fail_owned_close):
                    with self.assertRaises(BaseExceptionGroup) as caught:
                        factory.close()
                expected = (first_failure, second_failure)
                self.assertEqual(expected, self._exception_leaves(caught.exception))
                self.assertEqual(list(retired_descriptors), close_calls)
                self.assertEqual(-1, first_installed.descriptor)
                self.assertEqual(-1, second_installed.descriptor)

                with mock.patch.object(
                    os,
                    "close",
                    side_effect=AssertionError("terminal factory retried close"),
                ):
                    with self.assertRaises(BaseExceptionGroup) as repeated:
                        factory.close()
                self.assertEqual(
                    expected, self._exception_leaves(repeated.exception)
                )
            finally:
                for descriptor in retired_descriptors:
                    try:
                        real_close(descriptor)
                    except OSError:
                        pass
                real_close(bootstrap.descriptor)
                bootstrap.descriptor = -1

    def test_factory_setup_indeterminate_close_poison_is_terminal_before_later_work(self):
        from scripts import run_observing_workflows_eval_worker as worker

        first_assignment = self._assignment("forward", 1, "owned-case")
        second_assignment = self._assignment("forward", 2, "failing-case")
        epoch_id = "e" * 64
        plan = sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind="diagnostic",
            fingerprints=replace(
                input_fingerprints("diagnostic"), epoch_id=epoch_id
            ),
            assignments=(first_assignment, second_assignment),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            first_paths = sharding.paths_for_case(root / "run", first_assignment)
            first_paths.root.mkdir(parents=True, mode=0o700)
            first_paths.cleanup.mkdir(mode=0o700)
            first_installed = sharding.install_case_auth(
                bootstrap=bootstrap.path,
                plan=plan,
                assignment=first_assignment,
                paths=first_paths,
            )
            owned_descriptor = first_installed.descriptor
            second_paths = sharding.paths_for_case(root / "run", second_assignment)
            snapshot = root / "captured-input/workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=self._transport_config(executable),
                plan=plan,
            )
            factory._owned_cases[first_paths.root] = (
                first_paths,
                first_installed,
            )

            failure = RuntimeError("marked setup close failure")
            marker_slot = sharding._DescriptorSlot(
                os.open(os.devnull, os.O_RDONLY)
            )
            real_close = os.close

            def close_marker_then_raise(descriptor):
                real_close(descriptor)
                raise failure

            with mock.patch.object(
                os, "close", side_effect=close_marker_then_raise
            ):
                self.assertIs(
                    failure,
                    sharding._retire_descriptor_capability(marker_slot),
                )
            self.assertTrue(
                sharding.is_indeterminate_descriptor_close(failure)
            )

            owned_close_calls = 0

            def trace_owned_close(descriptor):
                nonlocal owned_close_calls
                if descriptor == owned_descriptor:
                    owned_close_calls += 1
                return real_close(descriptor)

            try:
                with mock.patch.object(
                    worker, "_prepare_case_directories", side_effect=failure
                ), mock.patch.object(
                    os, "close", side_effect=trace_owned_close
                ):
                    with self.assertRaises(RuntimeError) as caught:
                        factory(
                            assignment=second_assignment,
                            manifest_case={"id": second_assignment.key.case_id},
                            paths=second_paths,
                            transport_config=self._transport_config(executable),
                        )
                self.assertIs(failure, caught.exception)
                self.assertTrue(factory.poisoned)
                self.assertEqual(1, owned_close_calls)
                self.assertEqual(-1, first_installed.descriptor)

                with mock.patch.object(
                    worker,
                    "_prepare_case_directories",
                    side_effect=AssertionError("poisoned factory did work"),
                ):
                    with self.assertRaisesRegex(ValueError, "poisoned"):
                        factory(
                            assignment=second_assignment,
                            manifest_case={"id": second_assignment.key.case_id},
                            paths=second_paths,
                            transport_config=self._transport_config(executable),
                        )
                with mock.patch.object(
                    os,
                    "close",
                    side_effect=AssertionError("poisoned close retried"),
                ):
                    with self.assertRaises(RuntimeError) as repeated:
                        factory.close()
                self.assertIs(failure, repeated.exception)
            finally:
                if first_installed.descriptor >= 0:
                    try:
                        real_close(first_installed.descriptor)
                    except OSError:
                        pass
                    first_installed.descriptor = -1
                real_close(bootstrap.descriptor)
                bootstrap.descriptor = -1

    def test_factory_cleanup_indeterminate_close_poison_retires_other_owner_once(self):
        from scripts import run_observing_workflows_eval_worker as worker

        first_assignment = self._assignment("forward", 1, "cleanup-case")
        second_assignment = self._assignment("forward", 2, "other-case")
        epoch_id = "e" * 64
        plan = sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind="diagnostic",
            fingerprints=replace(
                input_fingerprints("diagnostic"), epoch_id=epoch_id
            ),
            assignments=(first_assignment, second_assignment),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            owned = []
            for assignment in (first_assignment, second_assignment):
                paths = sharding.paths_for_case(root / "run", assignment)
                paths.root.mkdir(parents=True, mode=0o700)
                paths.cleanup.mkdir(mode=0o700)
                installed = sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )
                owned.append((paths, installed))
            target_paths, target = owned[0]
            _, other = owned[1]
            target_descriptor = target.descriptor
            other_descriptor = other.descriptor

            snapshot = root / "captured-input/workflow-observatory"
            self._write_read_only_marketplace(snapshot)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            factory = worker.build_production_runtime_factory(
                snapshot_root=snapshot,
                transport_config=self._transport_config(executable),
                plan=plan,
            )
            for paths, installed in reversed(owned):
                factory._owned_cases[paths.root] = (paths, installed)

            failure = KeyboardInterrupt("marked cleanup close failure")
            close_calls = {target_descriptor: 0, other_descriptor: 0}
            real_close = os.close

            def fail_target_and_trace_other(descriptor):
                if descriptor in close_calls:
                    close_calls[descriptor] += 1
                if descriptor == target_descriptor:
                    real_close(descriptor)
                    raise failure
                return real_close(descriptor)

            try:
                with mock.patch.object(
                    os, "close", side_effect=fail_target_and_trace_other
                ):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        factory.cleanup_case(target_paths)
                self.assertIs(failure, caught.exception)
                self.assertTrue(
                    sharding.is_indeterminate_descriptor_close(caught.exception)
                )
                self.assertTrue(factory.poisoned)
                self.assertEqual(
                    {target_descriptor: 1, other_descriptor: 1}, close_calls
                )
                self.assertEqual(-1, target.descriptor)
                self.assertEqual(-1, other.descriptor)

                with mock.patch.object(
                    worker,
                    "cleanup_case_auth",
                    side_effect=AssertionError("poisoned factory cleaned"),
                ):
                    with self.assertRaisesRegex(ValueError, "poisoned"):
                        factory.cleanup_case(owned[1][0])
                with mock.patch.object(
                    worker,
                    "_prepare_case_directories",
                    side_effect=AssertionError("poisoned factory did work"),
                ):
                    with self.assertRaisesRegex(ValueError, "poisoned"):
                        factory(
                            assignment=second_assignment,
                            manifest_case={"id": second_assignment.key.case_id},
                            paths=owned[1][0],
                            transport_config=self._transport_config(executable),
                        )
                with mock.patch.object(
                    os,
                    "close",
                    side_effect=AssertionError("poisoned close retried"),
                ):
                    with self.assertRaises(KeyboardInterrupt) as repeated:
                        factory.close()
                self.assertIs(failure, repeated.exception)
            finally:
                for descriptor in (target_descriptor, other_descriptor):
                    try:
                        real_close(descriptor)
                    except OSError:
                        pass
                real_close(bootstrap.descriptor)
                bootstrap.descriptor = -1

    def test_bootstrap_close_finalization_attempts_every_owned_descriptor(self):
        roles = ("cleanup", "coordinator", "source", "bootstrap")
        for exception_type in (RuntimeError, KeyboardInterrupt):
            for target_role in roles:
                with self.subTest(
                    exception=exception_type.__name__, target=target_role
                ), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary).resolve(strict=True)
                    assignment = self._assignment("forward", 1, "case")
                    plan = self._plan(assignment)
                    source_home = root / "source-home"
                    source_home.mkdir(mode=0o700)
                    auth = source_home / "auth.json"
                    auth.write_text("BOOTSTRAP_SECRET", encoding="utf-8")
                    auth.chmod(0o600)
                    coordinator = root / "run/coordinator"
                    coordinator.mkdir(parents=True, mode=0o700)
                    baseline = self._fd_count()
                    captured = {}
                    close_events = []
                    injected = False
                    primary = RuntimeError("copy setup failed")
                    failure = exception_type(f"{target_role} close failed")
                    real_close = os.close
                    real_open_directory = sharding._open_private_directory
                    real_validate = sharding._validate_private_auth
                    real_copy = sharding._copy_auth_descriptor_at
                    replacement = None
                    role_paths = {
                        "cleanup": coordinator / "cleanup",
                        "coordinator": coordinator,
                        "source": auth,
                        "bootstrap": coordinator / "auth-bootstrap",
                    }

                    def track_validate(path, label):
                        descriptor = real_validate(path, label)
                        captured["source"] = descriptor
                        return descriptor

                    def track_directory(path, label):
                        descriptor, metadata = real_open_directory(path, label)
                        role = {
                            "coordinator root": "coordinator",
                            "coordinator cleanup directory": "cleanup",
                            "auth bootstrap": "bootstrap",
                        }.get(label)
                        if role is not None and role not in captured:
                            captured[role] = descriptor
                        return descriptor, metadata

                    def close_with_one_failure(descriptor):
                        nonlocal injected, replacement
                        role = next(
                            (
                                name for name in roles
                                if captured.get(name) == descriptor
                            ),
                            None,
                        )
                        if role is not None:
                            close_events.append(role)
                        if role == target_role and not injected:
                            injected = True
                            before = os.fstat(descriptor)
                            real_close(descriptor)
                            flags = os.O_RDONLY
                            if target_role != "source":
                                flags |= getattr(os, "O_DIRECTORY", 0)
                            opened = os.open(role_paths[target_role], flags)
                            if opened != descriptor:
                                os.dup2(opened, descriptor)
                                real_close(opened)
                            replacement = descriptor
                            after = os.fstat(replacement)
                            self.assertEqual(
                                (before.st_dev, before.st_ino),
                                (after.st_dev, after.st_ino),
                            )
                            raise failure
                        return real_close(descriptor)

                    def copy_or_fail(*args, **kwargs):
                        if target_role == "bootstrap":
                            raise primary
                        return real_copy(*args, **kwargs)

                    observed = None
                    with mock.patch.object(
                        sharding,
                        "_validate_private_auth",
                        side_effect=track_validate,
                    ), mock.patch.object(
                        sharding,
                        "_open_private_directory",
                        side_effect=track_directory,
                    ), mock.patch.object(
                        sharding,
                        "_copy_auth_descriptor_at",
                        side_effect=copy_or_fail,
                    ), mock.patch.object(
                        os, "close", side_effect=close_with_one_failure
                    ):
                        try:
                            sharding.prepare_auth_bootstrap(
                                source_codex_home=source_home,
                                coordinator_root=coordinator,
                                plan=plan,
                            )
                        except BaseException as caught:
                            observed = caught
                        else:
                            self.fail("close failure did not escape bootstrap setup")

                    self.assertIsNotNone(observed)
                    leaves = self._exception_leaves(observed)
                    expected = (
                        (primary, failure)
                        if target_role == "bootstrap"
                        else (failure,)
                    )
                    self.assertEqual(expected, leaves)
                    self.assertEqual(
                        roles,
                        tuple(dict.fromkeys(close_events)),
                        close_events,
                    )
                    self.assertTrue(injected)
                    self.assertEqual(baseline + 1, self._fd_count())
                    os.fstat(replacement)
                    real_close(replacement)
                    self.assertEqual(baseline, self._fd_count())
                    self.assertNotIn("BOOTSTRAP_SECRET", str(observed))

    def test_case_close_finalization_attempts_every_owned_descriptor(self):
        roles = ("cleanup", "case-root", "source", "bootstrap", "case")
        for exception_type in (RuntimeError, KeyboardInterrupt):
            for target_role in roles:
                with self.subTest(
                    exception=exception_type.__name__, target=target_role
                ), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary).resolve(strict=True)
                    assignment = self._assignment("forward", 1, "case")
                    plan = self._plan(assignment)
                    bootstrap = self._prepare_bootstrap(root, plan)
                    paths = sharding.paths_for_case(root / "run", assignment)
                    paths.root.mkdir(parents=True, mode=0o700)
                    paths.cleanup.mkdir(mode=0o700)
                    baseline = self._fd_count()
                    captured = {}
                    close_events = []
                    injected = False
                    primary = RuntimeError("case copy setup failed")
                    failure = exception_type(f"{target_role} close failed")
                    real_close = os.close
                    real_open = os.open
                    real_open_directory = sharding._open_private_directory
                    real_copy = sharding._copy_auth_descriptor_at
                    replacement = None
                    role_paths = {
                        "cleanup": paths.cleanup,
                        "case-root": paths.root,
                        "source": bootstrap.path / "auth.json",
                        "bootstrap": bootstrap.path,
                        "case": paths.codex_home,
                    }

                    def track_directory(path, label):
                        descriptor, metadata = real_open_directory(path, label)
                        role = {
                            "auth bootstrap": "bootstrap",
                            "case root": "case-root",
                            "case cleanup directory": "cleanup",
                            "case Codex home": "case",
                        }.get(label)
                        if role is not None and role not in captured:
                            captured[role] = descriptor
                        return descriptor, metadata

                    def track_open(path, flags, mode=0o777, *, dir_fd=None):
                        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
                        if path == "auth.json" and dir_fd == captured.get("bootstrap"):
                            captured["source"] = descriptor
                        return descriptor

                    def close_with_one_failure(descriptor):
                        nonlocal injected, replacement
                        role = next(
                            (
                                name for name in roles
                                if captured.get(name) == descriptor
                            ),
                            None,
                        )
                        if role is not None:
                            close_events.append(role)
                        if role == target_role and not injected:
                            injected = True
                            before = os.fstat(descriptor)
                            real_close(descriptor)
                            flags = os.O_RDONLY
                            if target_role != "source":
                                flags |= getattr(os, "O_DIRECTORY", 0)
                            opened = real_open(role_paths[target_role], flags)
                            if opened != descriptor:
                                os.dup2(opened, descriptor)
                                real_close(opened)
                            replacement = descriptor
                            after = os.fstat(replacement)
                            self.assertEqual(
                                (before.st_dev, before.st_ino),
                                (after.st_dev, after.st_ino),
                            )
                            raise failure
                        return real_close(descriptor)

                    def copy_or_fail(*args, **kwargs):
                        if target_role == "case":
                            raise primary
                        return real_copy(*args, **kwargs)

                    observed = None
                    with mock.patch.object(
                        sharding,
                        "_open_private_directory",
                        side_effect=track_directory,
                    ), mock.patch.object(
                        os, "open", side_effect=track_open
                    ), mock.patch.object(
                        sharding,
                        "_copy_auth_descriptor_at",
                        side_effect=copy_or_fail,
                    ), mock.patch.object(
                        os, "close", side_effect=close_with_one_failure
                    ):
                        try:
                            sharding.install_case_auth(
                                bootstrap=bootstrap.path,
                                plan=plan,
                                assignment=assignment,
                                paths=paths,
                            )
                        except BaseException as caught:
                            observed = caught
                        else:
                            self.fail("close failure did not escape case auth setup")

                    self.assertIsNotNone(observed)
                    leaves = self._exception_leaves(observed)
                    expected = (
                        (primary, failure)
                        if target_role == "case"
                        else (failure,)
                    )
                    self.assertEqual(expected, leaves)
                    self.assertEqual(
                        roles,
                        tuple(dict.fromkeys(close_events)),
                        close_events,
                    )
                    self.assertTrue(injected)
                    self.assertEqual(baseline + 1, self._fd_count())
                    os.fstat(replacement)
                    real_close(replacement)
                    self.assertEqual(baseline, self._fd_count())
                    self.assertNotIn("BOOTSTRAP_SECRET", str(observed))
                    real_close(bootstrap.descriptor)
                    bootstrap.descriptor = -1

    def test_auth_close_finalization_groups_all_errors_in_descriptor_order(self):
        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with self.subTest(owner="bootstrap"), \
                tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            source_home = root / "source-home"
            source_home.mkdir(mode=0o700)
            auth = source_home / "auth.json"
            auth.write_text("{}\n", encoding="utf-8")
            auth.chmod(0o600)
            coordinator = root / "run/coordinator"
            coordinator.mkdir(parents=True, mode=0o700)
            roles = ("cleanup", "coordinator", "source", "bootstrap")
            failures = {
                role: (
                    KeyboardInterrupt(f"{role} close failed")
                    if role == "bootstrap"
                    else RuntimeError(f"{role} close failed")
                )
                for role in roles
            }
            primary = RuntimeError("bootstrap primary")
            captured = {}
            injected = set()
            real_close = os.close
            real_open_directory = sharding._open_private_directory
            real_validate = sharding._validate_private_auth

            def track_validate(path, label):
                descriptor = real_validate(path, label)
                captured["source"] = descriptor
                return descriptor

            def track_directory(path, label):
                descriptor, metadata = real_open_directory(path, label)
                role = {
                    "coordinator root": "coordinator",
                    "coordinator cleanup directory": "cleanup",
                    "auth bootstrap": "bootstrap",
                }.get(label)
                if role is not None and role not in captured:
                    captured[role] = descriptor
                return descriptor, metadata

            def close_with_failures(descriptor):
                role = next(
                    (name for name in roles if captured.get(name) == descriptor),
                    None,
                )
                if role is not None and role not in injected:
                    injected.add(role)
                    real_close(descriptor)
                    raise failures[role]
                return real_close(descriptor)

            baseline = self._fd_count()
            observed = None
            with mock.patch.object(
                sharding, "_validate_private_auth", side_effect=track_validate
            ), mock.patch.object(
                sharding, "_open_private_directory", side_effect=track_directory
            ), mock.patch.object(
                sharding, "_copy_auth_descriptor_at", side_effect=primary
            ), mock.patch.object(os, "close", side_effect=close_with_failures):
                try:
                    sharding.prepare_auth_bootstrap(
                        source_codex_home=source_home,
                        coordinator_root=coordinator,
                        plan=plan,
                    )
                except BaseException as caught:
                    observed = caught
                else:
                    self.fail("bootstrap primary and close failures did not escape")

            self.assertIsInstance(observed, BaseExceptionGroup)
            self.assertEqual(
                (primary, *(failures[role] for role in roles)),
                self._exception_leaves(observed),
            )
            self.assertEqual(set(roles), injected)
            self.assertEqual(baseline, self._fd_count())

        with self.subTest(owner="case"), \
                tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            paths = sharding.paths_for_case(root / "run", assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            roles = ("cleanup", "case-root", "source", "bootstrap", "case")
            failures = {
                role: (
                    KeyboardInterrupt(f"{role} close failed")
                    if role == "case"
                    else RuntimeError(f"{role} close failed")
                )
                for role in roles
            }
            primary = RuntimeError("case primary")
            captured = {}
            injected = set()
            real_close = os.close
            real_open = os.open
            real_open_directory = sharding._open_private_directory

            def track_directory(path, label):
                descriptor, metadata = real_open_directory(path, label)
                role = {
                    "auth bootstrap": "bootstrap",
                    "case root": "case-root",
                    "case cleanup directory": "cleanup",
                    "case Codex home": "case",
                }.get(label)
                if role is not None and role not in captured:
                    captured[role] = descriptor
                return descriptor, metadata

            def track_open(path, flags, mode=0o777, *, dir_fd=None):
                descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
                if path == "auth.json" and dir_fd == captured.get("bootstrap"):
                    captured["source"] = descriptor
                return descriptor

            def close_with_failures(descriptor):
                role = next(
                    (name for name in roles if captured.get(name) == descriptor),
                    None,
                )
                if role is not None and role not in injected:
                    injected.add(role)
                    real_close(descriptor)
                    raise failures[role]
                return real_close(descriptor)

            baseline = self._fd_count()
            observed = None
            with mock.patch.object(
                sharding, "_open_private_directory", side_effect=track_directory
            ), mock.patch.object(
                os, "open", side_effect=track_open
            ), mock.patch.object(
                sharding, "_copy_auth_descriptor_at", side_effect=primary
            ), mock.patch.object(os, "close", side_effect=close_with_failures):
                try:
                    sharding.install_case_auth(
                        bootstrap=bootstrap.path,
                        plan=plan,
                        assignment=assignment,
                        paths=paths,
                    )
                except BaseException as caught:
                    observed = caught
                else:
                    self.fail("case primary and close failures did not escape")

            self.assertIsInstance(observed, BaseExceptionGroup)
            self.assertEqual(
                (primary, *(failures[role] for role in roles)),
                self._exception_leaves(observed),
            )
            self.assertEqual(set(roles), injected)
            self.assertEqual(baseline, self._fd_count())
            real_close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_successful_auth_install_returns_live_owner_descriptors(self):
        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            before_bootstrap = self._fd_count()
            bootstrap = self._prepare_bootstrap(root, plan)
            self.assertEqual(before_bootstrap + 1, self._fd_count())
            os.fstat(bootstrap.descriptor)
            self.assertEqual("owned", bootstrap.descriptor_close_state)
            self.assertIsNone(bootstrap.descriptor_close_error)

            paths = sharding.paths_for_case(root / "run", assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            before_case = self._fd_count()
            installed = sharding.install_case_auth(
                bootstrap=bootstrap.path,
                plan=plan,
                assignment=assignment,
                paths=paths,
            )
            self.assertEqual(before_case + 1, self._fd_count())
            os.fstat(installed.descriptor)
            self.assertEqual("owned", installed.descriptor_close_state)
            self.assertIsNone(installed.descriptor_close_error)

            os.close(installed.descriptor)
            installed.descriptor = -1
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1
            self.assertEqual(before_bootstrap, self._fd_count())

    def test_worker_scrub_retains_tombstone_and_idempotent_receipt(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap = self._prepare_bootstrap(root, plan)
            self.addCleanup(
                lambda: os.close(bootstrap.descriptor)
                if bootstrap.descriptor >= 0
                else None
            )
            paths = sharding.paths_for_case(root / "run", assignment)
            paths.root.mkdir(parents=True, mode=0o700)
            paths.cleanup.mkdir(mode=0o700)
            real_copy = sharding._copy_auth_descriptor_at

            def copy_after_case_ownership(source_descriptor, directory_descriptor, name):
                ownership = worker.read_case_auth_ownership(
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )
                self.assertEqual(assignment.key, ownership.case)
                self.assertFalse((paths.codex_home / "auth.json").exists())
                return real_copy(source_descriptor, directory_descriptor, name)

            with mock.patch.object(
                sharding,
                "_copy_auth_descriptor_at",
                side_effect=copy_after_case_ownership,
            ):
                installed = sharding.install_case_auth(
                    bootstrap=bootstrap.path,
                    plan=plan,
                    assignment=assignment,
                    paths=paths,
                )

            ownership_bytes = (paths.cleanup / "ownership.json").read_bytes()
            self.assertNotIn(b"SECRET", ownership_bytes)
            before_names = {path.name for path in paths.root.iterdir()}
            first = worker.cleanup_case_auth(installed=installed, paths=paths)
            first_bytes = (paths.cleanup / "tombstone.json").read_bytes()
            second = worker.cleanup_case_auth(installed=installed, paths=paths)

            self.assertEqual(first, second)
            self.assertEqual(first_bytes, (paths.cleanup / "tombstone.json").read_bytes())
            self.assertEqual("expected", first.canonical_binding)
            self.assertEqual("tombstoned", installed.state)
            self.assertEqual(-1, installed.descriptor)
            self.assertTrue(paths.codex_home.is_dir())
            self.assertEqual([], list(paths.codex_home.iterdir()))
            self.assertEqual(0o700, stat.S_IMODE(paths.codex_home.stat().st_mode))
            self.assertEqual(before_names, {path.name for path in paths.root.iterdir()})
            self.assertFalse(any(
                path.name.startswith(".codex-home-cleanup-")
                for path in paths.root.iterdir()
            ))
            self.assertEqual(
                0o600,
                stat.S_IMODE((paths.cleanup / "ownership.json").stat().st_mode),
            )
            self.assertEqual(
                0o600,
                stat.S_IMODE((paths.cleanup / "tombstone.json").stat().st_mode),
            )

    def test_worker_scrubs_moved_owned_inode_and_preserves_replacement(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap, paths, installed = self._install_case(
                root, plan, assignment
            )
            displaced = root / "displaced-owned-codex-home"
            paths.codex_home.rename(displaced)
            paths.codex_home.mkdir(mode=0o700)
            keep = paths.codex_home / "unrelated.txt"
            keep.write_text("unrelated", encoding="utf-8")
            keep.chmod(0o600)

            receipt = worker.cleanup_case_auth(installed=installed, paths=paths)

            self.assertEqual("replaced", receipt.canonical_binding)
            self.assertEqual("unrelated", keep.read_text(encoding="utf-8"))
            self.assertTrue(displaced.is_dir())
            self.assertEqual([], list(displaced.iterdir()))
            self.assertEqual("tombstoned", installed.state)
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_worker_scrub_failures_retain_descriptor_and_retry(self):
        from scripts import run_observing_workflows_eval_worker as worker

        for failure in ("child", "depth", "entry"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(strict=True)
                assignment = self._assignment("forward", 1, f"case-{failure}")
                plan = self._plan(assignment)
                bootstrap, paths, installed = self._install_case(
                    root, plan, assignment
                )
                nested = paths.codex_home / "nested"
                nested.mkdir(mode=0o700)
                (nested / "child").mkdir(mode=0o700)
                for name in ("one", "two"):
                    child = paths.codex_home / name
                    child.write_text(name, encoding="utf-8")
                    child.chmod(0o600)

                if failure == "child":
                    patcher = mock.patch.object(
                        worker,
                        "_remove_tree_entry",
                        side_effect=OSError("child scrub failed"),
                    )
                elif failure == "depth":
                    patcher = mock.patch.object(worker, "AUTH_CLEANUP_MAX_DEPTH", 0)
                else:
                    patcher = mock.patch.object(worker, "AUTH_CLEANUP_MAX_ENTRIES", 1)

                with patcher:
                    with self.assertRaisesRegex(OSError, "scrub|cleanup"):
                        worker.cleanup_case_auth(installed=installed, paths=paths)
                self.assertEqual("active", installed.state)
                self.assertEqual("owned", installed.descriptor_close_state)
                self.assertIsNone(installed.descriptor_close_error)
                os.fstat(installed.descriptor)

                receipt = worker.cleanup_case_auth(installed=installed, paths=paths)
                self.assertTrue(receipt.scrubbed)
                self.assertEqual([], list(paths.codex_home.iterdir()))
                os.close(bootstrap.descriptor)
                bootstrap.descriptor = -1

    def test_tombstone_reader_rejects_stale_unsafe_or_hash_mismatched_records(self):
        from scripts import run_observing_workflows_eval_worker as worker

        assignment = self._assignment("forward", 1, "case")
        plan = self._plan(assignment)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            bootstrap, paths, installed = self._install_case(
                root, plan, assignment
            )
            worker.cleanup_case_auth(installed=installed, paths=paths)
            tombstone = paths.cleanup / "tombstone.json"
            ownership = paths.cleanup / "ownership.json"
            good_tombstone = tombstone.read_bytes()
            good_ownership = ownership.read_bytes()
            decoded = json.loads(good_tombstone.decode("ascii"))

            mutations = {
                "stale epoch": {**decoded, "epoch_id": "f" * 64},
                "wrong run": {**decoded, "run_kind": "formal"},
                "wrong case": {
                    **decoded,
                    "case": {**decoded["case"], "case_id": "other"},
                },
                "unknown field": {**decoded, "extra": True},
                "missing field": {
                    key: value for key, value in decoded.items() if key != "empty"
                },
            }
            for label, payload in mutations.items():
                with self.subTest(label=label):
                    tombstone.write_bytes(sharding.canonical_config_bytes(payload))
                    tombstone.chmod(0o600)
                    with self.assertRaises(ValueError):
                        worker.read_tombstone_receipt(
                            plan=plan,
                            assignment=assignment,
                            paths=paths,
                        )
                    tombstone.write_bytes(good_tombstone)
                    tombstone.chmod(0o600)

            tombstone.write_bytes(good_tombstone[: max(1, len(good_tombstone) // 2)])
            with self.assertRaises(ValueError):
                worker.read_tombstone_receipt(
                    plan=plan, assignment=assignment, paths=paths
                )
            tombstone.write_bytes(good_tombstone)
            tombstone.chmod(0o644)
            with self.assertRaises(ValueError):
                worker.read_tombstone_receipt(
                    plan=plan, assignment=assignment, paths=paths
                )
            tombstone.unlink()
            external = root / "external-tombstone.json"
            external.write_bytes(good_tombstone)
            external.chmod(0o600)
            tombstone.symlink_to(external)
            with self.assertRaises(ValueError):
                worker.read_tombstone_receipt(
                    plan=plan, assignment=assignment, paths=paths
                )
            tombstone.unlink()
            tombstone.write_bytes(good_tombstone)
            tombstone.chmod(0o600)

            ownership_payload = json.loads(good_ownership.decode("ascii"))
            ownership_payload["codex_home_inode"] += 1
            ownership.write_bytes(sharding.canonical_config_bytes(ownership_payload))
            ownership.chmod(0o600)
            with self.assertRaises(ValueError):
                worker.read_tombstone_receipt(
                    plan=plan, assignment=assignment, paths=paths
                )
            os.close(bootstrap.descriptor)
            bootstrap.descriptor = -1

    def test_case_runtime_writable_roots_are_immutable_tuple(self):
        from scripts import run_observing_workflows_task9_eval as task9_eval

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve(strict=True)
            executable = root / "fake-codex"
            executable.write_text("#!/bin/sh\n", encoding="utf-8")
            executable.chmod(0o700)
            runtime = task9_eval.CaseRuntime(
                store_root=root / "store",
                audit=mock.sentinel.audit,
                environment={},
                writable_roots=(root / "store",),
                transport_config=self._transport_config(executable),
            )
            self.assertEqual(
                tuple[Path, ...],
                get_type_hints(task9_eval.CaseRuntime)["writable_roots"],
            )
            self.assertIsInstance(runtime.writable_roots, tuple)
            with self.assertRaises(AttributeError):
                runtime.writable_roots.append(root / "forged")


class RetryResumeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.root = Path(self.temporary.name).resolve(strict=True)
        self.manifests = {
            "forward": load_cases("observing_workflows_cases.json"),
            "lifecycle": load_cases(
                "observing_workflows_lifecycle_cases.json"
            ),
        }
        self.plan = sharding.build_epoch_plan(
            run_kind="formal",
            manifests=self.manifests,
            fingerprints=input_fingerprints("formal"),
        )
        self.assignment = self.plan.assignments[0]
        self.manifest_case = self.manifests["forward"][0]
        self.run_root = self.root / "run"
        (self.run_root / "cases").mkdir(parents=True, mode=0o700)
        self.run_root.chmod(0o700)
        self.paths = sharding.paths_for_case(
            self.run_root, self.assignment
        )
        self.paths.root.mkdir(mode=0o700)
        self.paths.cleanup.mkdir(mode=0o700)
        self.paths.codex_home.mkdir(mode=0o700)
        self.usage = {
            "input_tokens": 10,
            "cached_input_tokens": 2,
            "output_tokens": 5,
            "reasoning_output_tokens": 1,
            "total_tokens": 15,
        }
        self.result = {
            "id": self.assignment.key.case_id,
            "decisions": [
                {
                    "after_turn": 1,
                    "triggered": True,
                    "task_type": "feature",
                    "workflow_variant": "implementation-basic",
                }
            ],
            "record_checkpoints": [
                {
                    "after_turn": 1,
                    "records": [
                        {
                            "role": "run-1",
                            "status": "success",
                            "start_mode": "planned",
                            "superseded_by_role": None,
                        }
                    ],
                }
            ],
            "run_count": 1,
            "draft_count": 0,
            "final_statuses": ["success"],
        }
        self.evidence = {
            "status": "success",
            "classification": "success",
            "model_started": True,
            "elapsed_milliseconds": 25,
            "usage": self.usage,
            "failure": None,
            "store_record_count": 1,
            "store_invalidated_count": 0,
            "audit_event_count": 3,
            "payload_file_count": 0,
            "output_file_count": 0,
            "process_cleanup_passed": True,
            "credential_cleanup_passed": True,
        }
        self._write_expected_tombstone()

    def tearDown(self):
        self.temporary.cleanup()

    def _write_expected_tombstone(self):
        root_stat = self.paths.root.stat()
        home_stat = self.paths.codex_home.stat()
        ownership = sharding.CaseAuthOwnership(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            case=self.assignment.key,
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
        )
        ownership_bytes = sharding._atomic_write_record(
            self.paths.cleanup / "ownership.json", asdict(ownership)
        )
        receipt = sharding.TombstoneReceipt(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            case=self.assignment.key,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="worker",
        )
        sharding._atomic_write_record(
            self.paths.cleanup / "tombstone.json", asdict(receipt)
        )

    def _replace_tombstone_binding(self, binding):
        receipt_path = self.paths.cleanup / "tombstone.json"
        receipt = json.loads(receipt_path.read_text(encoding="ascii"))
        receipt["canonical_binding"] = binding
        receipt_path.write_bytes(sharding.canonical_config_bytes(receipt))

    def _copy_attempt(self, source, destination):
        source_paths = sharding.paths_for_attempt(self.paths, source)
        destination_paths = sharding.paths_for_attempt(
            self.paths, destination
        )
        destination_paths.root.mkdir(mode=0o700)
        start = json.loads(source_paths.start.read_text(encoding="ascii"))
        terminal = json.loads(
            source_paths.terminal.read_text(encoding="ascii")
        )
        start["attempt"] = destination
        start_bytes = sharding._atomic_write_record(
            destination_paths.start, start
        )
        terminal["attempt"] = destination
        terminal["start_sha256"] = hashlib.sha256(start_bytes).hexdigest()
        sharding._atomic_write_record(destination_paths.terminal, terminal)

    @staticmethod
    def _failure(classification):
        text = f"{classification} failure"
        return {
            "classification": classification,
            "type": "RuntimeError",
            "chars": len(text),
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }

    def _write_attempt(
        self,
        attempt,
        *,
        status,
        classification,
        model_started,
        cleanup_passed=True,
        manifest_case=None,
    ):
        bound_manifest_case = (
            self.manifest_case
            if manifest_case is None
            else manifest_case
        )
        sharding.write_attempt_start(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=attempt,
            manifest_case=bound_manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=attempt,
            manifest_case=bound_manifest_case,
            status=status,
            classification=classification,
            model_started=model_started,
            cleanup_passed=cleanup_passed,
            usage=self.usage if status == "success" else None,
            failure=(
                None
                if status == "success"
                else self._failure(classification)
            ),
        )

    def _seal_success(self, attempt, *, manifest_case=None):
        sharding.seal_case(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=attempt,
            result=self.result,
            evidence=self.evidence,
            manifest_case=(
                self.manifest_case
                if manifest_case is None
                else manifest_case
            ),
        )

    def _resume(self):
        return sharding.plan_resume(
            plan=self.plan,
            run_root=self.run_root,
            current_fingerprints=self.plan.fingerprints,
            manifests=self.manifests,
        )

    def test_two_attempt_layout_requires_proved_pre_model_first_attempt(self):
        from scripts import run_observing_workflows_eval_worker as worker

        self.assertIs(worker.RetryDecision, sharding.RetryDecision)
        self.assertIs(worker.decide_retry, sharding.decide_retry)
        decision = sharding.decide_retry(
            classification="pre-model-infrastructure",
            attempt=1,
            model_started=False,
            cleanup_passed=True,
            fingerprints_unchanged=True,
        )
        self.assertIsInstance(decision, sharding.RetryDecision)
        self.assertTrue(decision.retry)
        self.assertEqual(2, decision.next_attempt)
        self.assertEqual("reuse", decision.action)
        self.assertTrue(decision.reason)

        self._write_attempt(
            1,
            status="failed",
            classification="pre-model-infrastructure",
            model_started=False,
        )
        self._write_attempt(
            2,
            status="success",
            classification="success",
            model_started=True,
        )
        self._seal_success(2)

        attempts = sharding.scan_attempts(
            self.paths,
            plan=self.plan,
            manifest_case=self.manifest_case,
        )
        self.assertEqual((1, 2), tuple(
            int(attempt.root.name) for attempt in attempts
        ))
        resume = self._resume()
        self.assertEqual((self.assignment.key,), resume.reusable)
        self.assertEqual(
            tuple(assignment.key for assignment in self.plan.assignments[1:]),
            resume.pending,
        )
        self.assertEqual((), resume.invalid)

        sharding.paths_for_attempt(self.paths, 1).terminal.unlink()
        resume = self._resume()
        self.assertEqual((), resume.reusable)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_rejects_forged_case_digest_with_unchanged_manifest_identity(self):
        self._write_attempt(
            1,
            status="success",
            classification="success",
            model_started=True,
        )
        self._seal_success(1)
        self.assertEqual(
            (self.assignment.key,), self._resume().reusable
        )

        epoch_id = self.plan.epoch_id
        forward_manifest_sha256 = (
            self.plan.fingerprints.forward_manifest_sha256
        )
        forged_digest = hashlib.sha256(
            sharding.canonical_config_bytes(self.manifests["forward"][1])
        ).hexdigest()
        attempt = sharding.paths_for_attempt(self.paths, 1)
        start = json.loads(attempt.start.read_text(encoding="ascii"))
        terminal = json.loads(attempt.terminal.read_text(encoding="ascii"))
        start["manifest_case_sha256"] = forged_digest
        start_bytes = sharding.canonical_config_bytes(start)
        terminal["manifest_case_sha256"] = forged_digest
        terminal["start_sha256"] = hashlib.sha256(start_bytes).hexdigest()
        attempt.start.write_bytes(start_bytes)
        attempt.terminal.write_bytes(
            sharding.canonical_config_bytes(terminal)
        )

        resume = self._resume()
        self.assertEqual(epoch_id, self.plan.epoch_id)
        self.assertEqual(
            forward_manifest_sha256,
            self.plan.fingerprints.forward_manifest_sha256,
        )
        self.assertEqual((), resume.reusable)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_freezes_manifest_before_rebuild_and_ordinal_lookup(self):
        forged_manifest_case = json.loads(
            json.dumps(self.manifest_case)
        )
        forged_manifest_case["turns"][0]["prompt"] += " Forged substitution."
        self._write_attempt(
            1,
            status="success",
            classification="success",
            model_started=True,
            manifest_case=forged_manifest_case,
        )
        self._seal_success(1, manifest_case=forged_manifest_case)
        real_build_epoch_plan = sharding.build_epoch_plan
        substituted = False

        def substitute_after_rebuild(**kwargs):
            nonlocal substituted
            rebuilt = real_build_epoch_plan(**kwargs)
            self.manifests["forward"][0] = forged_manifest_case
            substituted = True
            return rebuilt

        with mock.patch.object(
            sharding,
            "build_epoch_plan",
            side_effect=substitute_after_rebuild,
        ):
            resume = self._resume()

        self.assertTrue(substituted)
        self.assertEqual((), resume.reusable)
        self.assertNotIn(self.assignment.key, resume.pending)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_snapshot_rejects_custom_mutable_manifest_shapes(self):
        class CustomList(list):
            pass

        class CustomDict(dict):
            pass

        mutations = (
            (
                "manifest list subclass",
                lambda value: value.__setitem__(
                    "forward", CustomList(value["forward"])
                ),
            ),
            (
                "manifest row subclass",
                lambda value: value["forward"].__setitem__(
                    0, CustomDict(value["forward"][0])
                ),
            ),
            (
                "nested list subclass",
                lambda value: value["forward"][0].__setitem__(
                    "turns", CustomList(value["forward"][0]["turns"])
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label):
                candidate = json.loads(json.dumps(self.manifests))
                mutate(candidate)

                resume = sharding.plan_resume(
                    plan=self.plan,
                    run_root=self.run_root,
                    current_fingerprints=self.plan.fingerprints,
                    manifests=candidate,
                )

                self.assertEqual((), resume.reusable)
                self.assertEqual((), resume.pending)
                self.assertEqual(
                    tuple(
                        assignment.key
                        for assignment in self.plan.assignments
                    ),
                    resume.invalid,
                )

    def test_retry_decision_contract_is_table_driven(self):
        cases = (
            (
                "eligible first attempt",
                "pre-model-infrastructure",
                1,
                False,
                True,
                True,
                (True, 2, "reuse"),
            ),
            (
                "successful reuse",
                "success",
                1,
                True,
                True,
                True,
                (False, None, "reuse"),
            ),
            (
                "semantic invalidation",
                "semantic",
                1,
                True,
                True,
                True,
                (False, None, "invalidate"),
            ),
            (
                "model invalidation",
                "model",
                1,
                True,
                True,
                True,
                (False, None, "invalidate"),
            ),
            *(
                (
                    f"{classification} abort",
                    classification,
                    1,
                    False,
                    True,
                    True,
                    (False, None, "abort"),
                )
                for classification in (
                    "cleanup",
                    "production-mutation",
                    "manifest-mutation",
                    "timeout",
                    "protocol",
                    "post-start-transport",
                    "surviving-process",
                    "coordinator-crash",
                )
            ),
            (
                "second attempt denied",
                "pre-model-infrastructure",
                2,
                False,
                True,
                True,
                (False, None, "abort"),
            ),
            (
                "model already started",
                "pre-model-infrastructure",
                1,
                True,
                True,
                True,
                (False, None, "abort"),
            ),
            (
                "cleanup not proved",
                "pre-model-infrastructure",
                1,
                False,
                False,
                True,
                (False, None, "abort"),
            ),
            (
                "fingerprint mismatch",
                "pre-model-infrastructure",
                1,
                False,
                True,
                False,
                (False, None, "abort"),
            ),
        )
        for (
            label,
            classification,
            attempt,
            model_started,
            cleanup_passed,
            fingerprints_unchanged,
            expected,
        ) in cases:
            with self.subTest(label):
                decision = sharding.decide_retry(
                    classification=classification,
                    attempt=attempt,
                    model_started=model_started,
                    cleanup_passed=cleanup_passed,
                    fingerprints_unchanged=fingerprints_unchanged,
                )
                self.assertEqual(
                    expected,
                    (
                        decision.retry,
                        decision.next_attempt,
                        decision.action,
                    ),
                )

    def test_resume_invalidates_case_disappearance_after_initial_inventory(self):
        sharding.write_attempt_start(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        real_inventory = sharding._RecordDirectoryCapability.inventory
        disappeared = False

        def disappear_after_inventory(directory):
            nonlocal disappeared
            inventory = real_inventory(directory)
            if (
                directory.label == "resume case directory"
                and not disappeared
            ):
                disappeared = True
                shutil.rmtree(self.paths.root)
            return inventory

        with mock.patch.object(
            sharding._RecordDirectoryCapability,
            "inventory",
            autospec=True,
            side_effect=disappear_after_inventory,
        ):
            resume = self._resume()

        self.assertTrue(disappeared)
        self.assertEqual((), resume.reusable)
        self.assertNotIn(self.assignment.key, resume.pending)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_invalidates_attempt_two_added_after_scan(self):
        self._write_attempt(
            1,
            status="success",
            classification="success",
            model_started=True,
        )
        self._seal_success(1)
        real_read_attempt_seal = sharding.read_attempt_seal
        added = False

        def add_attempt_after_read(**kwargs):
            nonlocal added
            seal = real_read_attempt_seal(**kwargs)
            if not added:
                added = True
                self._copy_attempt(1, 2)
            return seal

        with mock.patch.object(
            sharding,
            "read_attempt_seal",
            side_effect=add_attempt_after_read,
        ):
            resume = self._resume()

        self.assertTrue(added)
        self.assertEqual((), resume.reusable)
        self.assertNotIn(self.assignment.key, resume.pending)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_invalidates_attempt_replacement_after_scan(self):
        self._write_attempt(
            1,
            status="success",
            classification="success",
            model_started=True,
        )
        self._seal_success(1)
        real_read_attempt_seal = sharding.read_attempt_seal
        replaced = False

        def replace_attempt_after_read(**kwargs):
            nonlocal replaced
            seal = real_read_attempt_seal(**kwargs)
            if not replaced:
                replaced = True
                retired = self.root / "retired-attempt-01"
                self.paths.attempts.joinpath("01").rename(retired)
                shutil.copytree(retired, self.paths.attempts / "01")
            return seal

        with mock.patch.object(
            sharding,
            "read_attempt_seal",
            side_effect=replace_attempt_after_read,
        ):
            resume = self._resume()

        self.assertTrue(replaced)
        self.assertEqual((), resume.reusable)
        self.assertNotIn(self.assignment.key, resume.pending)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_resume_revalidates_inventory_before_final_disposition(self):
        self._write_attempt(
            1,
            status="success",
            classification="success",
            model_started=True,
        )
        self._seal_success(1)
        real_read_case_seal = sharding._read_case_seal_retained
        mutated = False

        def mutate_after_case_seal(**kwargs):
            nonlocal mutated
            seal = real_read_case_seal(**kwargs)
            if not mutated:
                mutated = True
                (self.paths.attempts / "unexpected").mkdir(mode=0o700)
            return seal

        with mock.patch.object(
            sharding,
            "_read_case_seal_retained",
            side_effect=mutate_after_case_seal,
        ):
            resume = self._resume()

        self.assertTrue(mutated)
        self.assertEqual((), resume.reusable)
        self.assertNotIn(self.assignment.key, resume.pending)
        self.assertEqual((self.assignment.key,), resume.invalid)

    def test_retry_requires_expected_verified_tombstone_binding(self):
        for binding in ("missing", "replaced"):
            with self.subTest(binding):
                if self.paths.attempts.exists():
                    shutil.rmtree(self.paths.attempts)
                self._replace_tombstone_binding(binding)
                self._write_attempt(
                    1,
                    status="failed",
                    classification="pre-model-infrastructure",
                    model_started=False,
                )

                resume = self._resume()

                self.assertEqual((), resume.reusable)
                self.assertNotIn(self.assignment.key, resume.pending)
                self.assertEqual((self.assignment.key,), resume.invalid)


class SealTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.root = Path(self.temporary.name).resolve(strict=True)
        self.manifests = {
            "forward": load_cases("observing_workflows_cases.json"),
            "lifecycle": load_cases(
                "observing_workflows_lifecycle_cases.json"
            ),
        }
        self.plan = sharding.build_epoch_plan(
            run_kind="diagnostic",
            manifests=self.manifests,
            fingerprints=input_fingerprints("diagnostic"),
        )
        self.assignment = self.plan.assignments[0]
        self.manifest_case = self.manifests["forward"][0]
        self.run_root = self.root / "run"
        self.run_root.mkdir(mode=0o700)
        (self.run_root / "cases").mkdir(mode=0o700)
        self.paths = sharding.paths_for_case(self.run_root, self.assignment)
        self.paths.root.mkdir(mode=0o700)
        self.paths.cleanup.mkdir(mode=0o700)
        self.paths.codex_home.mkdir(mode=0o700)
        self.usage = {
            "input_tokens": 10,
            "cached_input_tokens": 2,
            "output_tokens": 5,
            "reasoning_output_tokens": 1,
            "total_tokens": 15,
        }
        self.result = {
            "id": self.assignment.key.case_id,
            "decisions": [
                {
                    "after_turn": 1,
                    "triggered": True,
                    "task_type": "feature",
                    "workflow_variant": "implementation-basic",
                }
            ],
            "record_checkpoints": [
                {
                    "after_turn": 1,
                    "records": [
                        {
                            "role": "run-1",
                            "status": "success",
                            "start_mode": "planned",
                            "superseded_by_role": None,
                        }
                    ],
                }
            ],
            "run_count": 1,
            "draft_count": 0,
            "final_statuses": ["success"],
        }
        self.evidence = {
            "status": "success",
            "classification": "success",
            "model_started": True,
            "elapsed_milliseconds": 25,
            "usage": self.usage,
            "failure": None,
            "store_record_count": 1,
            "store_invalidated_count": 0,
            "audit_event_count": 3,
            "payload_file_count": 0,
            "output_file_count": 0,
            "process_cleanup_passed": True,
            "credential_cleanup_passed": True,
        }
        self._write_expected_tombstone()
        self._write_attempt_one_success()

    def tearDown(self):
        self.temporary.cleanup()

    def _write_expected_tombstone(self):
        root_stat = self.paths.root.stat()
        home_stat = self.paths.codex_home.stat()
        ownership = sharding.CaseAuthOwnership(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            case=self.assignment.key,
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
        )
        ownership_bytes = sharding._atomic_write_record(
            self.paths.cleanup / "ownership.json", asdict(ownership)
        )
        receipt = sharding.TombstoneReceipt(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            case=self.assignment.key,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="worker",
        )
        receipt_bytes = sharding._atomic_write_record(
            self.paths.cleanup / "tombstone.json", asdict(receipt)
        )
        self.tombstone_sha256 = hashlib.sha256(receipt_bytes).hexdigest()

    def _write_attempt_one_success(self):
        attempt = sharding.paths_for_attempt(self.paths, 1)
        self.paths.attempts.mkdir(mode=0o700)
        attempt.root.mkdir(mode=0o700)
        manifest_case_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(self.manifest_case)
        ).hexdigest()
        start = {
            "schema_version": 1,
            "epoch_id": self.plan.epoch_id,
            "run_kind": self.plan.run_kind,
            "case": asdict(self.assignment.key),
            "lane": self.assignment.lane,
            "route": self.assignment.route,
            "attempt": 1,
            "manifest_sha256": self.assignment.manifest_sha256,
            "manifest_case_sha256": manifest_case_sha256,
        }
        start_bytes = sharding._atomic_write_record(attempt.start, start)
        terminal = {
            **start,
            "start_sha256": hashlib.sha256(start_bytes).hexdigest(),
            "status": "success",
            "classification": "success",
            "model_started": True,
            "cleanup_passed": True,
            "usage": self.usage,
            "failure": None,
            "tombstone_receipt_sha256": self.tombstone_sha256,
        }
        sharding._atomic_write_record(attempt.terminal, terminal)

    def test_verified_tombstone_retains_one_cleanup_descriptor(self):
        from scripts import run_observing_workflows_eval_worker as worker

        self.assertIs(
            worker.read_tombstone_receipt,
            sharding.read_tombstone_receipt,
        )
        self.assertIs(
            worker._receipt_from_payload,
            sharding._tombstone_receipt_from_payload,
        )
        real_open_private_directory = sharding._open_private_directory
        cleanup_opens = []

        def track_cleanup_open(path, label):
            if Path(path) == self.paths.cleanup:
                cleanup_opens.append(label)
            return real_open_private_directory(path, label)

        with mock.patch.object(
            sharding,
            "_open_private_directory",
            side_effect=track_cleanup_open,
        ):
            verified = sharding.read_verified_tombstone_receipt(
                plan=self.plan,
                assignment=self.assignment,
                paths=self.paths,
            )

        self.assertEqual(self.tombstone_sha256, verified.sha256)
        self.assertEqual(
            ["case auth cleanup directory"],
            cleanup_opens,
        )

    def _new_seal_scenario(self, name, *, canonical_binding=None):
        run_root = self.root / name / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        paths = sharding.paths_for_case(run_root, self.assignment)
        paths.root.mkdir(mode=0o700)
        paths.cleanup.mkdir(mode=0o700)
        paths.codex_home.mkdir(mode=0o700)
        tombstone_sha256 = None
        if canonical_binding is not None:
            root_stat = paths.root.stat()
            home_stat = paths.codex_home.stat()
            ownership = sharding.CaseAuthOwnership(
                schema_version=1,
                epoch_id=self.plan.epoch_id,
                run_kind=self.plan.run_kind,
                case=self.assignment.key,
                case_root_device=root_stat.st_dev,
                case_root_inode=root_stat.st_ino,
                codex_home_device=home_stat.st_dev,
                codex_home_inode=home_stat.st_ino,
            )
            ownership_bytes = sharding._atomic_write_record(
                paths.cleanup / "ownership.json", asdict(ownership)
            )
            receipt = sharding.TombstoneReceipt(
                schema_version=1,
                epoch_id=self.plan.epoch_id,
                run_kind=self.plan.run_kind,
                case=self.assignment.key,
                ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
                case_root_device=root_stat.st_dev,
                case_root_inode=root_stat.st_ino,
                codex_home_device=home_stat.st_dev,
                codex_home_inode=home_stat.st_ino,
                scrubbed=True,
                empty=True,
                canonical_binding=canonical_binding,
                producer="worker",
            )
            receipt_bytes = sharding._atomic_write_record(
                paths.cleanup / "tombstone.json", asdict(receipt)
            )
            tombstone_sha256 = hashlib.sha256(receipt_bytes).hexdigest()
        return paths, tombstone_sha256

    def _write_scenario_attempt(
        self,
        paths,
        *,
        status,
        classification,
        cleanup_passed,
        failure,
    ):
        sharding.write_attempt_start(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
            status=status,
            classification=classification,
            model_started=True,
            cleanup_passed=cleanup_passed,
            usage=self.usage,
            failure=failure,
        )

    def _manifest_for_assignment(self, assignment):
        return self.manifests[assignment.key.mode][assignment.key.ordinal - 1]

    def _result_for_assignment(self, assignment):
        if assignment.key.mode == "forward":
            return {**self.result, "id": assignment.key.case_id}
        return {
            "id": assignment.key.case_id,
            "record_checkpoints": [],
            "run_count": 0,
            "draft_count": 0,
            "final_statuses": [],
            "failure_disclosed": False,
            "selected_command": None,
        }

    def _prepare_lane_terminal(
        self,
        run_root,
        assignment,
        *,
        status="success",
        cleanup_passed=True,
        canonical_binding="expected",
        seal_case_record=True,
    ):
        paths = sharding.paths_for_case(run_root, assignment)
        paths.root.mkdir(mode=0o700)
        paths.cleanup.mkdir(mode=0o700)
        paths.codex_home.mkdir(mode=0o700)
        if canonical_binding is not None:
            root_stat = paths.root.stat()
            home_stat = paths.codex_home.stat()
            ownership = sharding.CaseAuthOwnership(
                schema_version=1,
                epoch_id=self.plan.epoch_id,
                run_kind=self.plan.run_kind,
                case=assignment.key,
                case_root_device=root_stat.st_dev,
                case_root_inode=root_stat.st_ino,
                codex_home_device=home_stat.st_dev,
                codex_home_inode=home_stat.st_ino,
            )
            ownership_bytes = sharding._atomic_write_record(
                paths.cleanup / "ownership.json", asdict(ownership)
            )
            receipt = sharding.TombstoneReceipt(
                schema_version=1,
                epoch_id=self.plan.epoch_id,
                run_kind=self.plan.run_kind,
                case=assignment.key,
                ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
                case_root_device=root_stat.st_dev,
                case_root_inode=root_stat.st_ino,
                codex_home_device=home_stat.st_dev,
                codex_home_inode=home_stat.st_ino,
                scrubbed=True,
                empty=True,
                canonical_binding=canonical_binding,
                producer="worker",
            )
            sharding._atomic_write_record(
                paths.cleanup / "tombstone.json", asdict(receipt)
            )
        manifest_case = self._manifest_for_assignment(assignment)
        failure = None
        classification = "success"
        if status == "failed":
            failure_text = f"failed {assignment.key.case_id}"
            classification = "semantic"
            failure = {
                "classification": classification,
                "type": "SemanticFailure",
                "chars": len(failure_text),
                "sha256": hashlib.sha256(failure_text.encode()).hexdigest(),
            }
        sharding.write_attempt_start(
            plan=self.plan,
            paths=paths,
            assignment=assignment,
            attempt=1,
            manifest_case=manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=paths,
            assignment=assignment,
            attempt=1,
            manifest_case=manifest_case,
            status=status,
            classification=classification,
            model_started=True,
            cleanup_passed=cleanup_passed,
            usage=self.usage,
            failure=failure,
        )
        attempt_seal = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=assignment,
            attempt=1,
            manifest_case=manifest_case,
        )
        case_seal = None
        if seal_case_record:
            evidence = {
                **self.evidence,
                "status": status,
                "classification": classification,
                "failure": failure,
                "process_cleanup_passed": cleanup_passed,
                "credential_cleanup_passed": cleanup_passed,
            }
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                result=self._result_for_assignment(assignment),
                evidence=evidence,
                manifest_case=manifest_case,
            )
            case_seal = sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
            )
        return paths, sharding.ShardTerminal(
            key=assignment.key,
            run_kind=self.plan.run_kind,
            status=status,
            classification=classification,
            attempt_terminal_sha256=attempt_seal.terminal_sha256,
            case_commit_sha256=(case_seal.commit_sha256 if case_seal else None),
            tombstone_receipt_sha256=(
                case_seal.tombstone_receipt_sha256 if case_seal else None
            ),
            failure=(sharding.FailureSummary(**failure) if failure else None),
        )

    def test_fault_after_evidence_never_exposes_case_commit(self):
        arguments = {
            "plan": self.plan,
            "paths": self.paths,
            "assignment": self.assignment,
            "attempt": 1,
            "result": self.result,
            "evidence": self.evidence,
            "manifest_case": self.manifest_case,
        }
        with self.assertRaises(TypeError):
            sharding.seal_case(**arguments, crash_at="after-evidence-replace")

        observed = []

        def inject(point):
            observed.append(point)
            if point == "after-evidence-replace":
                raise RuntimeError("AFTER_EVIDENCE")

        with self.assertRaisesRegex(RuntimeError, "AFTER_EVIDENCE"):
            sharding.seal_case(**arguments, fault_injector=inject)

        self.assertEqual(
            ["after-result-replace", "after-evidence-replace"], observed
        )
        self.assertTrue((self.paths.sealed / "case-result.json").is_file())
        self.assertTrue((self.paths.sealed / "case-evidence.json").is_file())
        self.assertFalse((self.paths.sealed / "case-commit.json").exists())
        with self.assertRaises(ValueError):
            sharding.read_case_seal(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )

    def test_attempt_records_have_exact_canonical_schema_and_caps(self):
        shutil.rmtree(self.paths.attempts)
        expected_start_fields = {
            "schema_version",
            "epoch_id",
            "run_kind",
            "case",
            "lane",
            "route",
            "attempt",
            "manifest_sha256",
            "manifest_case_sha256",
        }
        expected_terminal_fields = expected_start_fields | {
            "start_sha256",
            "status",
            "classification",
            "model_started",
            "cleanup_passed",
            "usage",
            "failure",
            "tombstone_receipt_sha256",
        }
        self.assertEqual(4 * 1024, sharding.MAX_ATTEMPT_START_BYTES)
        self.assertEqual(8 * 1024, sharding.MAX_ATTEMPT_TERMINAL_BYTES)

        start_path = sharding.write_attempt_start(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        start_bytes = start_path.read_bytes()
        self.assertEqual(0o600, stat.S_IMODE(start_path.stat().st_mode))
        self.assertFalse(start_bytes.endswith(b"\n"))
        self.assertEqual(start_bytes.decode("ascii").encode("ascii"), start_bytes)
        self.assertEqual(expected_start_fields, set(json.loads(start_bytes)))
        self.assertEqual(
            start_path,
            sharding.write_attempt_start(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                attempt=1,
                manifest_case=self.manifest_case,
            ),
        )

        terminal_path = sharding.write_attempt_terminal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
            status="success",
            classification="success",
            model_started=True,
            cleanup_passed=True,
            usage=self.usage,
            failure=None,
        )
        terminal_bytes = terminal_path.read_bytes()
        self.assertEqual(0o600, stat.S_IMODE(terminal_path.stat().st_mode))
        self.assertFalse(terminal_bytes.endswith(b"\n"))
        self.assertEqual(expected_terminal_fields, set(json.loads(terminal_bytes)))
        seal = sharding.read_attempt_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        self.assertEqual(
            hashlib.sha256(start_bytes).hexdigest(), seal.start_sha256
        )
        self.assertEqual(
            hashlib.sha256(terminal_bytes).hexdigest(), seal.terminal_sha256
        )
        self.assertEqual(
            terminal_path,
            sharding.write_attempt_terminal(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                attempt=1,
                manifest_case=self.manifest_case,
                status="success",
                classification="success",
                model_started=True,
                cleanup_passed=True,
                usage=self.usage,
                failure=None,
            ),
        )

        sharding.write_attempt_start(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=2,
            manifest_case=self.manifest_case,
        )
        with self.assertRaises(ValueError):
            sharding.write_attempt_terminal(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                attempt=2,
                manifest_case=self.manifest_case,
                status="success",
                classification="success",
                model_started=True,
                cleanup_passed=True,
                usage=None,
                failure=None,
            )
        failure_text = "transport unavailable"
        failure = {
            "classification": "pre-model-infrastructure",
            "type": "TransportUnavailable",
            "chars": len(failure_text),
            "sha256": hashlib.sha256(failure_text.encode()).hexdigest(),
        }
        (self.paths.cleanup / "tombstone.json").unlink()
        failed_terminal = sharding.write_attempt_terminal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=2,
            manifest_case=self.manifest_case,
            status="failed",
            classification="pre-model-infrastructure",
            model_started=False,
            cleanup_passed=False,
            usage=None,
            failure=failure,
        )
        self.assertIsNone(json.loads(failed_terminal.read_bytes())["usage"])
        self.assertIsNone(
            json.loads(failed_terminal.read_bytes())["tombstone_receipt_sha256"]
        )

        start_path.write_bytes(b"{" + b'"x":"' + b"x" * 4096 + b'"}')
        start_path.chmod(0o600)
        with self.assertRaises(ValueError):
            sharding.read_attempt_start(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                attempt=1,
                manifest_case=self.manifest_case,
            )

    def test_seal_schema_constants_match_frozen_result_contract(self):
        from tests import run_observing_workflows_eval as frozen

        self.assertEqual(
            frozen.DECISION_MANIFEST_FIELDS,
            set(sharding.DECISION_MANIFEST_FIELDS),
        )
        self.assertEqual(
            frozen.LIFECYCLE_MANIFEST_FIELDS,
            set(sharding.LIFECYCLE_MANIFEST_FIELDS),
        )
        self.assertEqual(
            frozen.RESULT_SCHEMAS["forward"],
            set(sharding.RESULT_SCHEMAS["forward"]),
        )
        self.assertEqual(
            frozen.RESULT_SCHEMAS["lifecycle"],
            set(sharding.RESULT_SCHEMAS["lifecycle"]),
        )
        self.assertEqual(
            frozen.OBSERVED_DECISION_FIELDS,
            set(sharding.OBSERVED_DECISION_FIELDS),
        )
        self.assertEqual(
            frozen.CHECKPOINT_FIELDS,
            set(sharding.CHECKPOINT_FIELDS),
        )
        self.assertEqual(
            frozen.NORMALIZED_RECORD_FIELDS,
            set(sharding.NORMALIZED_RECORD_FIELDS),
        )

    def test_shard_terminal_schema_constant_and_wire_mapping_are_exact(self):
        expected = {
            "case",
            "status",
            "classification",
            "attempt_terminal_sha256",
            "case_commit_sha256",
            "tombstone_receipt_sha256",
            "failure",
        }
        self.assertEqual(expected, set(sharding.SHARD_TERMINAL_FIELDS))
        terminal = sharding.ShardTerminal(
            key=self.assignment.key,
            run_kind=self.plan.run_kind,
            status="success",
            classification="success",
            attempt_terminal_sha256="a" * 64,
            case_commit_sha256="b" * 64,
            tombstone_receipt_sha256="c" * 64,
            failure=None,
        )
        encoded = sharding._encode_shard_terminal(terminal)
        self.assertEqual(expected, set(encoded))
        self.assertNotIn("key", encoded)
        self.assertNotIn("run_kind", encoded)
        self.assertEqual(
            {
                "mode": self.assignment.key.mode,
                "ordinal": self.assignment.key.ordinal,
                "case_id": self.assignment.key.case_id,
            },
            encoded["case"],
        )
        self.assertEqual(
            terminal,
            sharding._decode_shard_terminal(
                encoded, run_kind=self.plan.run_kind
            ),
        )
        for mutation in (
            {**encoded, "key": encoded["case"]},
            {key: value for key, value in encoded.items() if key != "case"},
            {**encoded, "status": "failed"},
        ):
            with self.subTest(mutation=set(mutation)):
                with self.assertRaises(ValueError):
                    sharding._decode_shard_terminal(
                        mutation, run_kind=self.plan.run_kind
                    )

    def test_seal_evidence_has_closed_privacy_schema(self):
        attempt_seal = sharding.read_attempt_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        result_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(self.result)
        ).hexdigest()
        for mutation in (
            {**self.evidence, "prompt": "secret prompt"},
            {
                **self.evidence,
                "usage": {**self.usage, "raw_output": "secret output"},
            },
            {
                **self.evidence,
                "status": type("StringSubclass", (str,), {})("success"),
            },
        ):
            with self.subTest(fields=set(mutation)):
                with self.assertRaises((TypeError, ValueError)):
                    sharding._validate_evidence_input(
                        mutation,
                        attempt_seal=attempt_seal,
                        result_sha256=result_sha256,
                    )
        failure = {
            "classification": "model",
            "type": "ModelFailure",
            "chars": 12,
            "sha256": "f" * 64,
            "message": "/private/secret",
        }
        with self.assertRaises(ValueError):
            sharding._validate_failure(
                failure, classification="model", nullable=False
            )

    def test_forward_and_executable_lifecycle_require_checkpoint_lists(self):
        forward_result = {**self.result, "record_checkpoints": None}
        with self.assertRaises(ValueError):
            sharding._validate_result_record(
                forward_result,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )
        with self.assertRaises(ValueError):
            sharding.seal_case(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                attempt=1,
                result=forward_result,
                evidence=self.evidence,
                manifest_case=self.manifest_case,
            )

        lifecycle_assignment = next(
            assignment
            for assignment in self.plan.assignments
            if assignment.key.mode == "lifecycle"
            and self._manifest_for_assignment(assignment)["mode"] == "executable"
        )
        lifecycle_result = {
            **self._result_for_assignment(lifecycle_assignment),
            "record_checkpoints": None,
        }
        with self.assertRaises(ValueError):
            sharding._validate_result_record(
                lifecycle_result,
                assignment=lifecycle_assignment,
                manifest_case=self._manifest_for_assignment(
                    lifecycle_assignment
                ),
            )
        command_selection_bypass = {
            "id": lifecycle_assignment.key.case_id,
            "record_checkpoints": None,
            "run_count": None,
            "draft_count": None,
            "final_statuses": None,
            "failure_disclosed": None,
            "selected_command": "python3 wiki_cli.py observe",
        }
        with self.assertRaises(ValueError):
            sharding._validate_result_record(
                command_selection_bypass,
                assignment=lifecycle_assignment,
                manifest_case=self._manifest_for_assignment(
                    lifecycle_assignment
                ),
            )
        mutated_manifest = {
            **self._manifest_for_assignment(lifecycle_assignment),
            "mode": "command-selection-only",
            "expected_record_checkpoints": None,
            "expected_run_count": None,
            "expected_draft_count": None,
            "expected_final_statuses": None,
            "expect_failure_disclosure": None,
            "expected_selected_command": "python3 wiki_cli.py observe",
        }
        with self.assertRaises(ValueError):
            sharding._validate_result_record(
                command_selection_bypass,
                assignment=lifecycle_assignment,
                manifest_case=mutated_manifest,
            )

        run_root = self.root / "lifecycle-checkpoints" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        lifecycle_paths, _ = self._prepare_lane_terminal(
            run_root, lifecycle_assignment, seal_case_record=False
        )
        with self.assertRaises(ValueError):
            sharding.seal_case(
                plan=self.plan,
                paths=lifecycle_paths,
                assignment=lifecycle_assignment,
                attempt=1,
                result=lifecycle_result,
                evidence=self.evidence,
                manifest_case=self._manifest_for_assignment(
                    lifecycle_assignment
                ),
            )
        mutated_root = self.root / "lifecycle-subtype-mutation" / "run"
        mutated_root.mkdir(parents=True, mode=0o700)
        mutated_root.chmod(0o700)
        (mutated_root / "cases").mkdir(mode=0o700)
        mutated_paths = sharding.paths_for_case(
            mutated_root, lifecycle_assignment
        )
        mutated_paths.root.mkdir(mode=0o700)
        with self.assertRaises(ValueError):
            sharding.write_attempt_start(
                plan=self.plan,
                paths=mutated_paths,
                assignment=lifecycle_assignment,
                attempt=1,
                manifest_case=mutated_manifest,
            )
        with self.assertRaises(ValueError):
            sharding.seal_case(
                plan=self.plan,
                paths=lifecycle_paths,
                assignment=lifecycle_assignment,
                attempt=1,
                result=command_selection_bypass,
                evidence=self.evidence,
                manifest_case=self._manifest_for_assignment(
                    lifecycle_assignment
                ),
            )

    def test_attempt_writer_rejects_replaced_case_root_before_any_write(self):
        paths, _ = self._new_seal_scenario("replaced-case-root")
        moved_root = paths.root.with_name(f"{paths.root.name}-moved")
        paths.root.rename(moved_root)
        paths.root.symlink_to(moved_root, target_is_directory=True)

        with self.assertRaises(ValueError):
            sharding.write_attempt_start(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                manifest_case=self.manifest_case,
            )
        self.assertFalse(
            (moved_root / "attempts" / "01" / "start.json").exists()
        )

    def test_attempt_lookup_rejects_inventory_added_during_uniqueness_scan(self):
        paths, _ = self._new_seal_scenario(
            "attempt-inventory-race", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )
        sharding.write_attempt_start(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=2,
            manifest_case=self.manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=2,
            manifest_case=self.manifest_case,
            status="success",
            classification="success",
            model_started=True,
            cleanup_passed=True,
            usage=self.usage,
            failure=None,
        )
        attempt_one = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        attempt_two = sharding.paths_for_attempt(paths, 2)
        held_attempt_two = paths.root / "held-attempt-02"
        attempt_two.root.rename(held_attempt_two)
        real_read_attempt = sharding._read_attempt_seal_retained
        installed = False

        def read_and_install_second_attempt(**kwargs):
            nonlocal installed
            seal = real_read_attempt(**kwargs)
            if kwargs["attempt"] == 1 and not installed:
                installed = True
                held_attempt_two.rename(attempt_two.root)
            return seal

        with mock.patch.object(
            sharding,
            "_read_attempt_seal_retained",
            side_effect=read_and_install_second_attempt,
        ):
            with self.assertRaises((RuntimeError, ValueError)):
                sharding._find_attempt_for_shard_terminal(
                    plan=self.plan,
                    assignment=self.assignment,
                    paths=paths,
                    manifest_case=self.manifest_case,
                    expected_sha256=attempt_one.terminal_sha256,
                )

    def test_attempt_lookup_rejects_transient_child_aba_swap(self):
        paths, _ = self._new_seal_scenario(
            "attempt-child-aba", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )
        expected = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        attempt = sharding.paths_for_attempt(paths, 1)
        held_expected = paths.root / "held-expected-attempt"
        attempt.root.rename(held_expected)
        sharding.write_attempt_start(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
            status="failed",
            classification="semantic",
            model_started=True,
            cleanup_passed=False,
            usage=self.usage,
            failure={
                "classification": "semantic",
                "type": "SemanticFailure",
                "chars": 1,
                "sha256": "e" * 64,
            },
        )
        held_current = paths.root / "held-current-attempt"
        real_read_attempt = sharding.read_attempt_seal

        def read_transient_expected(**kwargs):
            attempt.root.rename(held_current)
            held_expected.rename(attempt.root)
            try:
                return real_read_attempt(**kwargs)
            finally:
                attempt.root.rename(held_expected)
                held_current.rename(attempt.root)

        with mock.patch.object(
            sharding,
            "read_attempt_seal",
            side_effect=read_transient_expected,
        ):
            with self.assertRaises(ValueError):
                sharding._find_attempt_for_shard_terminal(
                    plan=self.plan,
                    assignment=self.assignment,
                    paths=paths,
                    manifest_case=self.manifest_case,
                    expected_sha256=expected.terminal_sha256,
                )

    def test_record_capability_enter_failure_retires_all_descriptors(self):
        paths, _ = self._new_seal_scenario("record-enter-failure")
        directory = sharding._open_case_record_directory(
            paths=paths,
            components=("sealed",),
            create=True,
            label="case seal directory",
        )
        descriptors = [
            directory._anchor_slot.descriptor,
            *(entry.slot.descriptor for entry in directory._retained),
        ]
        moved_sealed = paths.sealed.with_name("sealed-moved")
        paths.sealed.rename(moved_sealed)
        try:
            with self.assertRaises((OSError, RuntimeError, ValueError)):
                with directory:
                    self.fail("replaced record directory became authoritative")
            for descriptor in descriptors:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)
        finally:
            for descriptor in descriptors:
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def test_attempt_inventory_failure_closes_capability_before_return(self):
        paths, _ = self._new_seal_scenario("attempt-inventory-failure")
        real_close = sharding._RecordDirectoryCapability.close
        close_calls = []

        def track_close(directory, primary=None):
            close_calls.append(primary)
            return real_close(directory, primary)

        with (
            mock.patch.object(
                sharding._RecordDirectoryCapability,
                "inventory",
                side_effect=RuntimeError("inventory failed"),
            ),
            mock.patch.object(
                sharding._RecordDirectoryCapability,
                "close",
                new=track_close,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "inventory failed"):
                sharding.write_attempt_start(
                    plan=self.plan,
                    paths=paths,
                    assignment=self.assignment,
                    attempt=1,
                    manifest_case=self.manifest_case,
                )
        self.assertEqual(1, len(close_calls))

    def test_anchor_indeterminate_failure_poisons_before_later_open(self):
        previous_poison = sharding._LEASE_PROCESS_POISON
        failure = OSError("indeterminate anchor close")
        setattr(failure, sharding._INDETERMINATE_CLOSE_MARKER, True)
        sharding._LEASE_PROCESS_POISON = None
        try:
            with mock.patch.object(
                sharding,
                "_open_absolute_directory_anchor",
                side_effect=failure,
            ):
                with self.assertRaises(OSError):
                    sharding._open_anchored_record_directory(
                        anchor_path=self.run_root,
                        base_components=("cases", self.paths.root.name),
                        record_components=("sealed",),
                        create=True,
                        label="case seal directory",
                    )
            self.assertIs(failure, sharding._LEASE_PROCESS_POISON)
        finally:
            sharding._LEASE_PROCESS_POISON = previous_poison

    def test_attempt_writer_rejects_raced_symlinked_run_root_ancestor(self):
        canonical_parent = self.root / "canonical-parent"
        run_root = canonical_parent / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        paths = sharding.paths_for_case(run_root, self.assignment)
        paths.root.mkdir(mode=0o700)

        alternate_parent = self.root / "alternate-parent"
        alternate_run = alternate_parent / "run"
        alternate_run.mkdir(parents=True, mode=0o700)
        alternate_run.chmod(0o700)
        (alternate_run / "cases").mkdir(mode=0o700)
        alternate_paths = sharding.paths_for_case(
            alternate_run, self.assignment
        )
        alternate_paths.root.mkdir(mode=0o700)

        moved_parent = self.root / "canonical-parent-moved"
        real_open_anchor = sharding._open_absolute_directory_anchor
        swapped = False

        def swap_ancestor_then_open(path, label, **kwargs):
            nonlocal swapped
            if Path(path) == Path("/") and not swapped:
                swapped = True
                canonical_parent.rename(moved_parent)
                canonical_parent.symlink_to(
                    alternate_parent, target_is_directory=True
                )
            return real_open_anchor(path, label, **kwargs)

        with mock.patch.object(
            sharding,
            "_open_absolute_directory_anchor",
            side_effect=swap_ancestor_then_open,
        ):
            with self.assertRaises((RuntimeError, ValueError)):
                sharding.write_attempt_start(
                    plan=self.plan,
                    paths=paths,
                    assignment=self.assignment,
                    attempt=1,
                    manifest_case=self.manifest_case,
                )
        self.assertFalse(
            (alternate_paths.root / "attempts" / "01" / "start.json").exists()
        )

    def test_case_seal_stops_after_retained_case_root_is_replaced(self):
        paths, _ = self._new_seal_scenario(
            "replace-case-during-seal", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )
        moved_root = paths.root.with_name(f"{paths.root.name}-moved")

        def replace_after_result(point):
            if point == "after-result-replace":
                paths.root.rename(moved_root)
                paths.root.symlink_to(moved_root, target_is_directory=True)

        with self.assertRaises((RuntimeError, ValueError)):
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result={**self.result},
                evidence={**self.evidence},
                manifest_case=self.manifest_case,
                fault_injector=replace_after_result,
            )
        self.assertTrue((moved_root / "sealed" / "case-result.json").is_file())
        self.assertFalse((moved_root / "sealed" / "case-evidence.json").exists())
        self.assertFalse((moved_root / "sealed" / "case-commit.json").exists())

    def test_case_reader_rejects_seal_directory_replacement_mid_read(self):
        paths, _ = self._new_seal_scenario(
            "replace-case-sealed-during-read", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )
        sharding.seal_case(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            result={**self.result},
            evidence={**self.evidence},
            manifest_case=self.manifest_case,
        )
        moved_sealed = paths.sealed.with_name("sealed-moved")
        real_read = sharding._read_canonical_record_at
        replaced = False

        def replace_after_commit(**kwargs):
            nonlocal replaced
            result = real_read(**kwargs)
            if kwargs["label"] == "case commit" and not replaced:
                replaced = True
                paths.sealed.rename(moved_sealed)
                paths.sealed.mkdir(mode=0o700)
                for source in moved_sealed.iterdir():
                    destination = paths.sealed / source.name
                    destination.write_bytes(source.read_bytes())
                    destination.chmod(0o600)
            return result

        with mock.patch.object(
            sharding,
            "_read_canonical_record_at",
            side_effect=replace_after_commit,
        ):
            with self.assertRaises((RuntimeError, ValueError)):
                sharding.read_case_seal(
                    plan=self.plan,
                    paths=paths,
                    assignment=self.assignment,
                    manifest_case=self.manifest_case,
                )

    def test_shard_seal_stops_after_retained_worker_root_is_replaced(self):
        lane = "APP"
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == lane
        )
        run_root = self.root / "replace-worker-during-seal" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        worker_root = run_root / "app-server"
        worker_root.mkdir(mode=0o700)
        terminals = []
        case_paths = {}
        for assignment in assignments:
            case_path, terminal = self._prepare_lane_terminal(
                run_root, assignment
            )
            case_paths[assignment.key] = case_path
            terminals.append(terminal)
        moved_worker = worker_root.with_name("app-server-moved")

        def replace_before_commit(point):
            if point == "before-shard-commit":
                worker_root.rename(moved_worker)
                worker_root.symlink_to(moved_worker, target_is_directory=True)

        with self.assertRaises((RuntimeError, ValueError)):
            sharding.seal_shard(
                worker_root=worker_root,
                plan=self.plan,
                lane=lane,
                terminals=terminals,
                manifests=self.manifests,
                case_paths=case_paths,
                fault_injector=replace_before_commit,
            )
        self.assertFalse(
            (moved_worker / "sealed" / "shard-commit.json").exists()
        )

    def test_case_seal_binds_fingerprints_attempt_result_and_expected_tombstone(self):
        invalid_manifest = json.loads(
            sharding.canonical_config_bytes(self.manifest_case)
        )
        invalid_manifest["expected_decisions"][0]["after_turn"] = True
        with self.assertRaises(ValueError):
            sharding._validate_seal_context(
                plan=self.plan,
                paths=self.paths,
                assignment=self.assignment,
                manifest_case=invalid_manifest,
            )

        commit_path = sharding.seal_case(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            result=self.result,
            evidence=self.evidence,
            manifest_case=self.manifest_case,
        )
        self.assertEqual(self.paths.sealed / "case-commit.json", commit_path)
        self.assertEqual(
            ["case-commit.json", "case-evidence.json", "case-result.json"],
            sorted(path.name for path in self.paths.sealed.iterdir()),
        )
        for path in self.paths.sealed.iterdir():
            content = path.read_bytes()
            self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
            self.assertFalse(content.endswith(b"\n"))
            self.assertEqual(content.decode("ascii").encode("ascii"), content)

        seal = sharding.read_case_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            manifest_case=self.manifest_case,
        )
        attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        expected_manifest_case_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(self.manifest_case)
        ).hexdigest()
        expected_result_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(self.result)
        ).hexdigest()
        self.assertEqual(self.assignment.manifest_sha256, seal.commit["manifest_sha256"])
        self.assertEqual(
            expected_manifest_case_sha256, seal.commit["manifest_case_sha256"]
        )
        self.assertEqual(expected_result_sha256, seal.result_sha256)
        self.assertEqual(attempt.start_sha256, seal.commit["attempt_start_sha256"])
        self.assertEqual(
            attempt.terminal_sha256, seal.commit["attempt_terminal_sha256"]
        )
        self.assertEqual(self.tombstone_sha256, seal.tombstone_receipt_sha256)
        self.assertEqual(
            self.plan.fingerprints.archive_sha256,
            seal.evidence["archive_sha256"],
        )
        self.assertEqual(
            self.plan.fingerprints.marketplace_sha256,
            seal.evidence["marketplace_sha256"],
        )
        self.assertEqual(
            self.plan.fingerprints.evaluator_sha256,
            seal.evidence["evaluator_sha256"],
        )
        self.assertEqual(
            self.plan.fingerprints.transport_config_sha256,
            seal.evidence["transport_config_sha256"],
        )

        attempt_seal = sharding.read_attempt_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        for field, invalid in (
            ("elapsed_milliseconds", sharding.MAX_SEAL_ELAPSED_MILLISECONDS + 1),
            ("audit_event_count", sharding.MAX_SEAL_COUNTER + 1),
        ):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    sharding._validate_evidence_input(
                        {**self.evidence, field: invalid},
                        attempt_seal=attempt_seal,
                        result_sha256=expected_result_sha256,
                    )

    def test_failed_case_nullable_artifacts_require_failed_cleanup(self):
        failure_text = "semantic evaluation mismatch"
        failure = {
            "classification": "semantic",
            "type": "SemanticFailure",
            "chars": len(failure_text),
            "sha256": hashlib.sha256(failure_text.encode()).hexdigest(),
        }
        failed_evidence = {
            **self.evidence,
            "status": "failed",
            "classification": "semantic",
            "failure": failure,
            "process_cleanup_passed": False,
            "credential_cleanup_passed": False,
        }

        paths, _ = self._new_seal_scenario("failed-no-artifacts")
        self._write_scenario_attempt(
            paths,
            status="failed",
            classification="semantic",
            cleanup_passed=False,
            failure=failure,
        )
        sharding.seal_case(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            result=None,
            evidence=failed_evidence,
            manifest_case=self.manifest_case,
        )
        seal = sharding.read_case_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            manifest_case=self.manifest_case,
        )
        self.assertIsNone(seal.result)
        self.assertIsNone(seal.tombstone_receipt_sha256)

        paths, _ = self._new_seal_scenario(
            "failed-clean-result", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="failed",
            classification="semantic",
            cleanup_passed=True,
            failure=failure,
        )
        clean_failed_evidence = {
            **failed_evidence,
            "process_cleanup_passed": True,
            "credential_cleanup_passed": True,
        }
        with self.assertRaises(ValueError):
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result=None,
                evidence=clean_failed_evidence,
                manifest_case=self.manifest_case,
            )

        paths, receipt_sha256 = self._new_seal_scenario(
            "failed-nonexpected-binding", canonical_binding="missing"
        )
        self._write_scenario_attempt(
            paths,
            status="failed",
            classification="semantic",
            cleanup_passed=True,
            failure=failure,
        )
        sharding.seal_case(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            result={**self.result},
            evidence=clean_failed_evidence,
            manifest_case=self.manifest_case,
        )
        self.assertEqual(
            receipt_sha256,
            sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            ).tombstone_receipt_sha256,
        )

        for binding in ("missing", "replaced"):
            paths, _ = self._new_seal_scenario(
                f"success-{binding}-binding", canonical_binding=binding
            )
            self._write_scenario_attempt(
                paths,
                status="success",
                classification="success",
                cleanup_passed=True,
                failure=None,
            )
            with self.subTest(binding=binding):
                with self.assertRaises(ValueError):
                    sharding.seal_case(
                        plan=self.plan,
                        paths=paths,
                        assignment=self.assignment,
                        attempt=1,
                        result={**self.result},
                        evidence={**self.evidence},
                        manifest_case=self.manifest_case,
                    )

    def test_shard_seal_accepts_only_full_success_or_unique_failed_prefix(self):
        lane = "APP"
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == lane
        )

        success_root = self.root / "full-success" / "run"
        success_root.mkdir(parents=True, mode=0o700)
        success_root.chmod(0o700)
        (success_root / "cases").mkdir(mode=0o700)
        success_worker = success_root / "app-server"
        success_worker.mkdir(mode=0o700)
        success_terminals = []
        success_paths = {}
        for assignment in assignments:
            paths, terminal = self._prepare_lane_terminal(
                success_root, assignment
            )
            success_paths[assignment.key] = paths
            success_terminals.append(terminal)

        shard_path = sharding.seal_shard(
            worker_root=success_worker,
            plan=self.plan,
            lane=lane,
            terminals=success_terminals,
            manifests=self.manifests,
            case_paths=success_paths,
        )
        self.assertEqual(
            success_worker / "sealed" / "shard-commit.json", shard_path
        )
        success_seal = sharding.read_shard_seal(
            worker_root=success_worker,
            plan=self.plan,
            lane=lane,
            manifests=self.manifests,
            case_paths=success_paths,
        )
        self.assertEqual("success", success_seal.status)
        self.assertEqual(tuple(success_terminals), success_seal.terminals)
        self.assertTrue(all(
            terminal.case_commit_sha256 is not None
            and terminal.tombstone_receipt_sha256 is not None
            for terminal in success_seal.terminals
        ))

        for invalid in (
            (),
            tuple(success_terminals[:-1]),
            tuple(reversed(success_terminals)),
            tuple(success_terminals[:1] + success_terminals),
            (
                sharding.ShardTerminal(
                    **{
                        **asdict(success_terminals[0]),
                        "attempt_terminal_sha256": "d" * 64,
                    }
                ),
                *success_terminals[1:],
            ),
        ):
            with self.subTest(invalid_length=len(invalid)):
                with self.assertRaises((TypeError, ValueError)):
                    sharding.seal_shard(
                        worker_root=success_worker,
                        plan=self.plan,
                        lane=lane,
                        terminals=invalid,
                        manifests=self.manifests,
                        case_paths=success_paths,
                    )

        failed_root = self.root / "failed-prefix" / "run"
        failed_root.mkdir(parents=True, mode=0o700)
        failed_root.chmod(0o700)
        (failed_root / "cases").mkdir(mode=0o700)
        failed_worker = failed_root / "app-server"
        failed_worker.mkdir(mode=0o700)
        failed_paths = {
            assignment.key: sharding.paths_for_case(failed_root, assignment)
            for assignment in assignments
        }
        first_paths, first_terminal = self._prepare_lane_terminal(
            failed_root, assignments[0]
        )
        second_paths, failed_terminal = self._prepare_lane_terminal(
            failed_root,
            assignments[1],
            status="failed",
            cleanup_passed=False,
            canonical_binding=None,
            seal_case_record=False,
        )
        failed_paths[assignments[0].key] = first_paths
        failed_paths[assignments[1].key] = second_paths
        failed_prefix = (first_terminal, failed_terminal)
        sharding.seal_shard(
            worker_root=failed_worker,
            plan=self.plan,
            lane=lane,
            terminals=failed_prefix,
            manifests=self.manifests,
            case_paths=failed_paths,
        )
        failed_seal = sharding.read_shard_seal(
            worker_root=failed_worker,
            plan=self.plan,
            lane=lane,
            manifests=self.manifests,
            case_paths=failed_paths,
        )
        self.assertEqual("failed", failed_seal.status)
        self.assertEqual(failed_prefix, failed_seal.terminals)
        self.assertIsNone(failed_seal.terminals[-1].case_commit_sha256)
        self.assertIsNone(failed_seal.terminals[-1].tombstone_receipt_sha256)
        with self.assertRaises(ValueError):
            sharding.seal_shard(
                worker_root=failed_worker,
                plan=self.plan,
                lane=lane,
                terminals=(*failed_prefix, success_terminals[2]),
                manifests=self.manifests,
                case_paths=failed_paths,
            )

    def test_seal_readers_reject_partial_extra_stale_and_unsafe_inputs(self):
        counter = 0

        def build_valid_case():
            nonlocal counter
            counter += 1
            paths, _ = self._new_seal_scenario(
                f"unsafe-case-{counter}", canonical_binding="expected"
            )
            self._write_scenario_attempt(
                paths,
                status="success",
                classification="success",
                cleanup_passed=True,
                failure=None,
            )
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result={**self.result},
                evidence={**self.evidence},
                manifest_case=self.manifest_case,
            )
            return paths

        short_run = self.root / "t"
        short_run.mkdir(mode=0o700)
        (short_run / "cases").mkdir(mode=0o700)
        paths, _ = self._prepare_lane_terminal(short_run, self.assignment)
        evidence_path = paths.sealed / "case-evidence.json"
        commit_path = paths.sealed / "case-commit.json"
        evidence = json.loads(evidence_path.read_bytes())
        evidence["archive_sha256"] = "e" * 64
        evidence_bytes = sharding._atomic_write_record(evidence_path, evidence)
        commit = json.loads(commit_path.read_bytes())
        commit["evidence_sha256"] = hashlib.sha256(evidence_bytes).hexdigest()
        sharding._atomic_write_record(commit_path, commit)
        with self.assertRaises(ValueError):
            sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )

        unsafe_mutations = []

        paths = build_valid_case()
        (paths.sealed / "case-commit.json").unlink()
        unsafe_mutations.append(("partial", paths))

        paths = build_valid_case()
        (paths.sealed / "extra.json").write_bytes(b"{}")
        (paths.sealed / "extra.json").chmod(0o600)
        unsafe_mutations.append(("extra", paths))

        paths = build_valid_case()
        (paths.sealed / "case-commit.json").chmod(0o644)
        unsafe_mutations.append(("wrong-mode", paths))

        paths = build_valid_case()
        commit_path = paths.sealed / "case-commit.json"
        commit_path.write_text("{\n}\n", encoding="ascii")
        commit_path.chmod(0o600)
        unsafe_mutations.append(("noncanonical", paths))

        paths = build_valid_case()
        commit_path = paths.sealed / "case-commit.json"
        commit_path.write_bytes(commit_path.read_bytes()[:17])
        commit_path.chmod(0o600)
        unsafe_mutations.append(("truncated", paths))

        paths = build_valid_case()
        commit_path = paths.sealed / "case-commit.json"
        commit_path.write_bytes(b" " * (sharding.MAX_CASE_COMMIT_BYTES + 1))
        commit_path.chmod(0o600)
        unsafe_mutations.append(("oversize", paths))

        for label, unsafe_paths in unsafe_mutations:
            with self.subTest(mutation=label):
                with self.assertRaises(ValueError):
                    sharding.read_case_seal(
                        plan=self.plan,
                        paths=unsafe_paths,
                        assignment=self.assignment,
                        manifest_case=self.manifest_case,
                    )

        stale_values = {
            "epoch_id": "0" * 64,
            "run_kind": "formal",
            "case": {
                "mode": "forward",
                "ordinal": 1,
                "case_id": "wrong-case",
            },
            "lane": "E2",
            "route": "app-server",
            "manifest_sha256": "0" * 64,
            "manifest_case_sha256": "1" * 64,
        }
        for field, value in stale_values.items():
            paths = build_valid_case()
            commit_path = paths.sealed / "case-commit.json"
            commit = json.loads(commit_path.read_bytes())
            commit[field] = value
            sharding._atomic_write_record(commit_path, commit)
            with self.subTest(stale_field=field):
                with self.assertRaises(ValueError):
                    sharding.read_case_seal(
                        plan=self.plan,
                        paths=paths,
                        assignment=self.assignment,
                        manifest_case=self.manifest_case,
                    )

        paths = build_valid_case()
        result_path = paths.sealed / "case-result.json"
        replacement = paths.sealed / "replacement.json"
        replacement.write_bytes(result_path.read_bytes())
        replacement.chmod(0o600)
        result_path.unlink()
        result_path.symlink_to(replacement.name)
        with self.assertRaises(ValueError):
            sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )

        short_run = self.root / "s"
        short_run.mkdir(mode=0o700)
        (short_run / "cases").mkdir(mode=0o700)
        paths, _ = self._prepare_lane_terminal(short_run, self.assignment)
        evidence_path = paths.sealed / "case-evidence.json"
        evidence_path.unlink()
        os.mkfifo(evidence_path, 0o600)
        with self.assertRaises(ValueError):
            sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )

        socket_run = self.root / "u"
        socket_run.mkdir(mode=0o700)
        (socket_run / "cases").mkdir(mode=0o700)
        paths, _ = self._prepare_lane_terminal(socket_run, self.assignment)
        evidence_path = paths.sealed / "case-evidence.json"
        evidence_path.unlink()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(evidence_path))
            with self.assertRaises(ValueError):
                sharding.read_case_seal(
                    plan=self.plan,
                    paths=paths,
                    assignment=self.assignment,
                    manifest_case=self.manifest_case,
                )
        finally:
            listener.close()

        paths = build_valid_case()
        attempt_terminal = sharding.paths_for_attempt(paths, 1).terminal
        terminal = json.loads(attempt_terminal.read_bytes())
        terminal["classification"] = "model"
        sharding._atomic_write_record(attempt_terminal, terminal)
        with self.assertRaises(ValueError):
            sharding.read_case_seal(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                manifest_case=self.manifest_case,
            )

        command_assignment = next(
            assignment
            for assignment in self.plan.assignments
            if assignment.key.mode == "lifecycle"
            and self._manifest_for_assignment(assignment)["mode"]
            == "command-selection-only"
        )
        command_result = {
            "id": command_assignment.key.case_id,
            "record_checkpoints": None,
            "run_count": None,
            "draft_count": None,
            "final_statuses": None,
            "failure_disclosed": None,
            "selected_command": "python3 wiki_cli.py observe",
        }
        self.assertEqual(
            command_result,
            sharding._validate_result_record(
                command_result,
                assignment=command_assignment,
                manifest_case=self._manifest_for_assignment(
                    command_assignment
                ),
            ),
        )

    def test_immutable_seal_publication_never_overwrites_collision(self):
        arguments = {
            "plan": self.plan,
            "paths": self.paths,
            "assignment": self.assignment,
            "attempt": 1,
            "result": self.result,
            "evidence": self.evidence,
            "manifest_case": self.manifest_case,
        }
        commit_path = sharding.seal_case(**arguments)
        before = {
            path.name: path.read_bytes() for path in self.paths.sealed.iterdir()
        }
        observed = []
        self.assertEqual(
            commit_path,
            sharding.seal_case(
                **arguments, fault_injector=lambda point: observed.append(point)
            ),
        )
        self.assertEqual([], observed)
        self.assertEqual(
            before,
            {path.name: path.read_bytes() for path in self.paths.sealed.iterdir()},
        )

        with self.assertRaises(ValueError):
            sharding.seal_case(
                **{
                    **arguments,
                    "evidence": {**self.evidence, "elapsed_milliseconds": 26},
                }
            )
        self.assertEqual(
            before,
            {path.name: path.read_bytes() for path in self.paths.sealed.iterdir()},
        )

        paths, _ = self._new_seal_scenario(
            "partial-no-heal", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )

        def stop_after_evidence(point):
            if point == "after-evidence-replace":
                raise RuntimeError("STOP")

        partial_arguments = {
            **arguments,
            "paths": paths,
        }
        with self.assertRaisesRegex(RuntimeError, "STOP"):
            sharding.seal_case(
                **partial_arguments, fault_injector=stop_after_evidence
            )
        partial_before = {
            path.name: path.read_bytes() for path in paths.sealed.iterdir()
        }
        with self.assertRaises(ValueError):
            sharding.seal_case(**partial_arguments)
        self.assertEqual(
            partial_before,
            {path.name: path.read_bytes() for path in paths.sealed.iterdir()},
        )

        paths, _ = self._new_seal_scenario(
            "attempt-collision", canonical_binding="expected"
        )
        real_open = os.open
        collision_bytes = b"collision-survives"

        def collide(_source, destination, *, src_dir_fd, dst_dir_fd, **_kwargs):
            descriptor = real_open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dst_dir_fd,
            )
            try:
                os.write(descriptor, collision_bytes)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            raise FileExistsError(destination)

        with mock.patch.object(sharding.os, "link", side_effect=collide):
            with self.assertRaises(ValueError):
                sharding.write_attempt_start(
                    plan=self.plan,
                    paths=paths,
                    assignment=self.assignment,
                    attempt=1,
                    manifest_case=self.manifest_case,
                )
        collision_path = sharding.paths_for_attempt(paths, 1).start
        self.assertEqual(collision_bytes, collision_path.read_bytes())
        self.assertEqual(0o600, stat.S_IMODE(collision_path.stat().st_mode))

        collision_path.chmod(0o644)
        with self.assertRaises(ValueError):
            sharding.write_attempt_start(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                manifest_case=self.manifest_case,
            )
        self.assertEqual(0o644, stat.S_IMODE(collision_path.stat().st_mode))

    def test_all_seal_fault_points_have_exact_durable_visibility(self):
        for target in (
            "after-result-replace",
            "after-evidence-replace",
            "before-case-commit",
            "after-case-commit",
        ):
            paths, _ = self._new_seal_scenario(
                f"fault-{target}", canonical_binding="expected"
            )
            self._write_scenario_attempt(
                paths,
                status="success",
                classification="success",
                cleanup_passed=True,
                failure=None,
            )
            observed = []

            def inject(point, *, expected=target):
                observed.append(point)
                if point == expected:
                    raise RuntimeError(expected)

            with self.subTest(point=target):
                with self.assertRaisesRegex(RuntimeError, target):
                    sharding.seal_case(
                        plan=self.plan,
                        paths=paths,
                        assignment=self.assignment,
                        attempt=1,
                        result={**self.result},
                        evidence={**self.evidence},
                        manifest_case=self.manifest_case,
                        fault_injector=inject,
                    )
                commit_exists = (paths.sealed / "case-commit.json").exists()
                self.assertEqual(target == "after-case-commit", commit_exists)
                if target == "after-case-commit":
                    sharding.read_case_seal(
                        plan=self.plan,
                        paths=paths,
                        assignment=self.assignment,
                        manifest_case=self.manifest_case,
                    )
                else:
                    with self.assertRaises(ValueError):
                        sharding.read_case_seal(
                            plan=self.plan,
                            paths=paths,
                            assignment=self.assignment,
                            manifest_case=self.manifest_case,
                        )

        paths, _ = self._new_seal_scenario(
            "fault-rehash", canonical_binding="expected"
        )
        self._write_scenario_attempt(
            paths,
            status="success",
            classification="success",
            cleanup_passed=True,
            failure=None,
        )

        def mutate_after_evidence(point):
            if point == "after-evidence-replace":
                result_path = paths.sealed / "case-result.json"
                result_path.write_bytes(result_path.read_bytes() + b" ")
                result_path.chmod(0o600)

        with self.assertRaises(ValueError):
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result={**self.result},
                evidence={**self.evidence},
                manifest_case=self.manifest_case,
                fault_injector=mutate_after_evidence,
            )
        self.assertFalse((paths.sealed / "case-commit.json").exists())

        failure_text = "cleanup failed"
        failure = {
            "classification": "cleanup",
            "type": "CleanupFailure",
            "chars": len(failure_text),
            "sha256": hashlib.sha256(failure_text.encode()).hexdigest(),
        }
        paths, _ = self._new_seal_scenario("fault-null-result")
        self._write_scenario_attempt(
            paths,
            status="failed",
            classification="cleanup",
            cleanup_passed=False,
            failure=failure,
        )
        observed = []

        def null_result_fault(point):
            observed.append(point)
            if point == "after-evidence-replace":
                raise RuntimeError("NULL_RESULT")

        with self.assertRaisesRegex(RuntimeError, "NULL_RESULT"):
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result=None,
                evidence={
                    **self.evidence,
                    "status": "failed",
                    "classification": "cleanup",
                    "failure": failure,
                    "process_cleanup_passed": False,
                    "credential_cleanup_passed": False,
                },
                manifest_case=self.manifest_case,
                fault_injector=null_result_fault,
            )
        self.assertNotIn("after-result-replace", observed)

        lane = "APP"
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == lane
        )
        for target in ("before-shard-commit", "after-shard-commit"):
            run_root = self.root / f"fault-{target}" / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            (run_root / "cases").mkdir(mode=0o700)
            worker_root = run_root / "app-server"
            worker_root.mkdir(mode=0o700)
            terminals = []
            case_paths = {}
            for assignment in assignments:
                case_path, terminal = self._prepare_lane_terminal(
                    run_root, assignment
                )
                case_paths[assignment.key] = case_path
                terminals.append(terminal)

            def shard_fault(point, *, expected=target):
                if point == expected:
                    raise RuntimeError(expected)

            with self.subTest(point=target):
                with self.assertRaisesRegex(RuntimeError, target):
                    sharding.seal_shard(
                        worker_root=worker_root,
                        plan=self.plan,
                        lane=lane,
                        terminals=terminals,
                        manifests=self.manifests,
                        case_paths=case_paths,
                        fault_injector=shard_fault,
                    )
                if target == "after-shard-commit":
                    sharding.read_shard_seal(
                        worker_root=worker_root,
                        plan=self.plan,
                        lane=lane,
                        manifests=self.manifests,
                        case_paths=case_paths,
                    )
                else:
                    with self.assertRaises(ValueError):
                        sharding.read_shard_seal(
                            worker_root=worker_root,
                            plan=self.plan,
                            lane=lane,
                            manifests=self.manifests,
                            case_paths=case_paths,
                        )

    def test_seal_descriptor_failures_retire_once_and_poison_process(self):
        program = r'''
from pathlib import Path
import os
import sys

from scripts import workflow_eval_sharding as sharding

root = Path(sys.argv[1]).resolve(strict=True)
role = sys.argv[2]
records = root / "records"
records.mkdir(mode=0o700)
source = records / "source.json"
source.write_bytes(b"{}")
source.chmod(0o600)

real_open = sharding.os.open
real_close = sharding.os.close
real_unlink = sharding.os.unlink
real_link = sharding.os.link
real_slot = sharding._DescriptorSlot
target = None
target_path = None
target_is_directory = False
target_close_calls = 0
slotted = []
close_calls = []

def tracking_slot(descriptor, *args, **kwargs):
    slotted.append(descriptor)
    return real_slot(descriptor, *args, **kwargs)

def tracking_open(path, flags, *args, **kwargs):
    global target, target_path, target_is_directory
    descriptor = real_open(path, flags, *args, **kwargs)
    dir_fd = kwargs.get("dir_fd")
    if target is None:
        if role == "read-file" and (
            path == source or (path == source.name and dir_fd is not None)
        ):
            target, target_path = descriptor, source
        elif role == "read-parent" and path == records:
            target, target_path, target_is_directory = descriptor, records, True
        elif role == "publish-temp" and isinstance(path, str) and path.startswith(
            ".published.json.tmp-"
        ):
            target, target_path = descriptor, records / path
        elif role == "publish-parent" and path == records:
            target, target_path, target_is_directory = descriptor, records, True
    return descriptor

def close_then_reuse(descriptor):
    global target_close_calls
    close_calls.append(descriptor)
    if descriptor != target:
        return real_close(descriptor)
    if descriptor not in slotted:
        raise SystemExit(10)
    target_close_calls += 1
    real_close(descriptor)
    flags = os.O_RDONLY
    if target_is_directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    replacement = real_open(target_path, flags)
    if replacement != descriptor:
        os.dup2(replacement, descriptor)
        real_close(replacement)
    raise OSError(f"indeterminate {role} close")

sharding._DescriptorSlot = tracking_slot
sharding.os.open = tracking_open
sharding.os.close = close_then_reuse
try:
    try:
        if role.startswith("read-"):
            sharding._read_canonical_record(
                source, "source", byte_cap=sharding.MAX_CASE_COMMIT_BYTES
            )
        else:
            sharding._publish_immutable_json(
                records / "published.json",
                {"schema_version": 1},
                byte_cap=sharding.MAX_CASE_COMMIT_BYTES,
            )
    except BaseException as error:
        if not sharding.is_indeterminate_descriptor_close(error):
            raise SystemExit(2)
    else:
        raise SystemExit(3)
    if target is None or target_close_calls != 1:
        raise SystemExit(4)
    os.fstat(target)
    before = tuple(close_calls)
    sharding.os.open = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("poisoned seal opened a file")
    )
    sharding.os.unlink = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("poisoned seal unlinked a file")
    )
    sharding.os.link = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("poisoned seal published a file")
    )
    try:
        sharding._publish_immutable_json(
            records / "later.json", {"schema_version": 1}, byte_cap=128
        )
    except RuntimeError as error:
        if "poisoned" not in str(error):
            raise SystemExit(5)
    else:
        raise SystemExit(6)
    if tuple(close_calls) != before:
        raise SystemExit(7)
finally:
    sharding._DescriptorSlot = real_slot
    sharding.os.open = real_open
    sharding.os.close = real_close
    sharding.os.unlink = real_unlink
    sharding.os.link = real_link
    if target is not None:
        real_close(target)
'''
        for role in (
            "read-file",
            "read-parent",
            "publish-temp",
            "publish-parent",
        ):
            with self.subTest(role=role), tempfile.TemporaryDirectory(
                dir="/private/tmp"
            ) as temporary:
                completed = subprocess.run(
                    [sys.executable, "-c", program, temporary, role],
                    cwd=Path(__file__).parents[1],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(
                    0,
                    completed.returncode,
                    f"{role}: stdout={completed.stdout!r} stderr={completed.stderr!r}",
                )


class AggregationGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.root = Path(self.temporary.name).resolve(strict=True)
        self.manifests = {
            "forward": load_cases("observing_workflows_cases.json"),
            "lifecycle": load_cases(
                "observing_workflows_lifecycle_cases.json"
            ),
        }
        self.repository = self._new_repository("repository")

        self.usage = {
            "input_tokens": 10,
            "cached_input_tokens": 2,
            "output_tokens": 5,
            "reasoning_output_tokens": 1,
            "total_tokens": 15,
        }

    def _new_repository(self, name):
        repository = (self.root / name).resolve()
        subprocess.run(
            ["git", "init", "-q", str(repository)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        (repository / "baseline.txt").write_text(
            "baseline\n", encoding="utf-8"
        )
        return repository

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def _worker_root(run_root, lane):
        if lane == "APP":
            return run_root / "app-server"
        return run_root / "workers" / lane

    @staticmethod
    def _result_for_manifest(mode, manifest_case):
        if mode == "forward":
            decisions = [
                {
                    **decision,
                    "task_type": (
                        manifest_case["task_type"]
                        if decision["triggered"]
                        else None
                    ),
                    "workflow_variant": (
                        manifest_case["workflow_variant"]
                        if decision["triggered"]
                        else None
                    ),
                }
                for decision in manifest_case["expected_decisions"]
            ]
            return {
                "id": manifest_case["id"],
                "decisions": decisions,
                "record_checkpoints": manifest_case[
                    "expected_record_checkpoints"
                ],
                "run_count": manifest_case["expected_run_count"],
                "draft_count": 0,
                "final_statuses": manifest_case["expected_final_statuses"],
            }
        return {
            "id": manifest_case["id"],
            "record_checkpoints": manifest_case[
                "expected_record_checkpoints"
            ],
            "run_count": manifest_case["expected_run_count"],
            "draft_count": manifest_case["expected_draft_count"],
            "final_statuses": manifest_case["expected_final_statuses"],
            "failure_disclosed": manifest_case[
                "expect_failure_disclosure"
            ],
            "selected_command": manifest_case[
                "expected_selected_command"
            ],
        }

    def _write_case_tombstone(self, plan, assignment, paths):
        root_stat = paths.root.stat()
        home_stat = paths.codex_home.stat()
        ownership = sharding.CaseAuthOwnership(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
        )
        ownership_bytes = sharding._atomic_write_record(
            paths.cleanup / "ownership.json", asdict(ownership)
        )
        receipt = sharding.TombstoneReceipt(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="worker",
        )
        content = sharding._atomic_write_record(
            paths.cleanup / "tombstone.json", asdict(receipt)
        )
        return hashlib.sha256(content).hexdigest()

    def _build_valid_epoch(
        self,
        run_kind,
        *,
        fixture_name=None,
        repository=None,
        fingerprints=None,
    ):
        fixture_name = run_kind if fixture_name is None else fixture_name
        repository = self.repository if repository is None else repository
        fingerprints = (
            input_fingerprints(run_kind)
            if fingerprints is None
            else fingerprints
        )
        plan = sharding.build_epoch_plan(
            run_kind=run_kind,
            manifests=self.manifests,
            fingerprints=fingerprints,
        )
        run_root = self.root / f"{fixture_name}-run"
        run_root.mkdir(mode=0o700)
        (run_root / "cases").mkdir(mode=0o700)
        (run_root / "workers").mkdir(mode=0o700)
        for lane in ("E1", "E2", "E3"):
            (run_root / "workers" / lane).mkdir(mode=0o700)
        (run_root / "app-server").mkdir(mode=0o700)
        coordinator = run_root / "coordinator"
        coordinator.mkdir(mode=0o700)
        coordinator_cleanup = coordinator / "cleanup"
        coordinator_cleanup.mkdir(mode=0o700)

        snapshot_root = self.root / f"{fixture_name}-snapshot"
        snapshot_root.mkdir(mode=0o555)
        snapshot_root.chmod(0o555)

        case_paths = {}
        terminals = {lane: [] for lane in ("E1", "E2", "E3", "APP")}
        integrity_counts = {}
        tombstone_hashes = []
        for assignment in plan.assignments:
            paths = sharding.paths_for_case(run_root, assignment)
            paths.root.mkdir(mode=0o700)
            for directory in (
                paths.cleanup,
                paths.codex_home,
                paths.store,
                paths.audit,
                paths.payload,
                paths.output,
                paths.staging,
                paths.home,
                paths.tmp,
                paths.config,
                paths.cache,
            ):
                directory.mkdir(mode=0o700)
            cli = (
                paths.staging
                / "marketplace/plugins/workflow-observer/scripts/"
                "workflow_observer_cli.py"
            )
            cli.parent.mkdir(parents=True, mode=0o700)
            cli.write_text("#!/usr/bin/env python3\n", encoding="utf-8")
            cli.chmod(0o700)

            tombstone_sha256 = self._write_case_tombstone(
                plan, assignment, paths
            )
            manifest_case = self.manifests[assignment.key.mode][
                assignment.key.ordinal - 1
            ]
            result = self._result_for_manifest(
                assignment.key.mode, manifest_case
            )
            store_count = (
                result["run_count"]
                if type(result["run_count"]) is int
                else 0
            )
            integrity_counts[paths.home] = {
                "records": store_count,
                "invalidated": 0,
            }
            sharding.write_attempt_start(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
            )
            sharding.write_attempt_terminal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
                status="success",
                classification="success",
                model_started=True,
                cleanup_passed=True,
                usage=self.usage,
                failure=None,
            )
            sharding.seal_case(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                result=result,
                evidence={
                    "status": "success",
                    "classification": "success",
                    "model_started": True,
                    "elapsed_milliseconds": 25,
                    "usage": self.usage,
                    "failure": None,
                    "store_record_count": store_count,
                    "store_invalidated_count": 0,
                    "audit_event_count": 0,
                    "payload_file_count": 0,
                    "output_file_count": 0,
                    "process_cleanup_passed": True,
                    "credential_cleanup_passed": True,
                },
                manifest_case=manifest_case,
            )
            attempt_seal = sharding.read_attempt_seal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
            )
            case_seal = sharding.read_case_seal(
                plan=plan,
                paths=paths,
                assignment=assignment,
                manifest_case=manifest_case,
            )
            terminals[assignment.lane].append(
                sharding.ShardTerminal(
                    key=assignment.key,
                    run_kind=plan.run_kind,
                    status="success",
                    classification="success",
                    attempt_terminal_sha256=attempt_seal.terminal_sha256,
                    case_commit_sha256=case_seal.commit_sha256,
                    tombstone_receipt_sha256=tombstone_sha256,
                    failure=None,
                )
            )
            paths.codex_home.rmdir()
            case_paths[assignment.key] = paths
            tombstone_hashes.append((assignment.key, tombstone_sha256))

        shard_paths = {}
        for lane in ("E1", "E2", "E3", "APP"):
            lane_paths = {
                assignment.key: case_paths[assignment.key]
                for assignment in plan.assignments
                if assignment.lane == lane
            }
            shard_paths[lane] = sharding.seal_shard(
                worker_root=self._worker_root(run_root, lane),
                plan=plan,
                lane=lane,
                terminals=terminals[lane],
                manifests=self.manifests,
                case_paths=lane_paths,
            )

        bootstrap = coordinator / "auth-bootstrap"
        bootstrap.mkdir(mode=0o700)
        bootstrap_stat = bootstrap.stat()
        bootstrap_ownership = sharding.BootstrapOwnership(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            bootstrap_device=bootstrap_stat.st_dev,
            bootstrap_inode=bootstrap_stat.st_ino,
        )
        bootstrap_ownership_bytes = sharding._atomic_write_record(
            coordinator_cleanup / "bootstrap-ownership.json",
            asdict(bootstrap_ownership),
        )
        bootstrap_receipt = sharding.BootstrapTombstoneReceipt(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            ownership_sha256=hashlib.sha256(
                bootstrap_ownership_bytes
            ).hexdigest(),
            bootstrap_device=bootstrap_stat.st_dev,
            bootstrap_inode=bootstrap_stat.st_ino,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="coordinator",
        )
        bootstrap_receipt_bytes = sharding._atomic_write_record(
            coordinator_cleanup / "bootstrap-tombstone.json",
            asdict(bootstrap_receipt),
        )
        bootstrap.rmdir()
        teardown_path = coordinator / "teardown.json"
        sharding._atomic_write_record(
            teardown_path,
            asdict(
                sharding.TeardownReceipt(
                    schema_version=1,
                    epoch_id=plan.epoch_id,
                    run_kind=plan.run_kind,
                    tombstone_receipts=tuple(tombstone_hashes),
                    bootstrap_tombstone_receipt_sha256=hashlib.sha256(
                        bootstrap_receipt_bytes
                    ).hexdigest(),
                    codex_homes_absent=True,
                    bootstrap_absent=True,
                )
            ),
        )

        integrity_calls = []

        def integrity_runner(command, environment, *, expected_records):
            home = Path(environment["WORKFLOW_OBSERVATORY_HOME"])
            expected = integrity_counts[home]
            self.assertEqual(expected["records"], expected_records)
            self.assertEqual("integrity", command[-1])
            integrity_calls.append(home)
            return expected.copy()

        guard = sharding.CoordinatorGuard.capture(repository)
        return {
            "plan": plan,
            "run_root": run_root,
            "snapshot_root": snapshot_root,
            "manifests": self.manifests,
            "shard_paths": shard_paths,
            "case_paths": case_paths,
            "integrity_runner": integrity_runner,
            "integrity_calls": integrity_calls,
            "guard": guard,
            "current_fingerprints": plan.fingerprints,
            "teardown_receipt": teardown_path,
        }

    def test_unvalidated_rows_cannot_aggregate_or_persist(self):
        self.assertEqual(
            ["run_kind", "forward_rows", "lifecycle_rows", "evidence_sha256"],
            [field.name for field in fields(sharding.Aggregate)],
        )
        self.assertEqual(
            ["fingerprint", "entries"],
            [field.name for field in fields(sharding.ProductionSnapshot)],
        )
        with self.assertRaises(TypeError):
            sharding.aggregate_committed_cases({"forward": [], "lifecycle": []})
        with self.assertRaises(TypeError):
            sharding.persist_validated_epoch(
                {},
                authority=object(),
                destinations={},
                guard=object(),
            )

        arguments = self._build_valid_epoch("formal")
        arguments.pop("integrity_calls")
        missing = dict(arguments["case_paths"])
        missing.pop(arguments["plan"].assignments[0].key)
        arguments["case_paths"] = missing
        with self.assertRaises((TypeError, ValueError)):
            sharding.validate_epoch_for_aggregation(**arguments)

    def test_discovery_and_reused_formal_capability_cannot_persist(self):
        discovery_arguments = self._build_valid_epoch("discovery")
        discovery_integrity_calls = discovery_arguments.pop("integrity_calls")
        discovery = sharding.validate_epoch_for_aggregation(
            **discovery_arguments
        )
        discovery_aggregate = sharding.aggregate_committed_cases(discovery)
        self.assertEqual("discovery", discovery_aggregate.run_kind)
        self.assertEqual(
            [case["id"] for case in self.manifests["forward"]],
            [row["id"] for row in discovery_aggregate.forward_rows],
        )
        self.assertEqual(
            [case["id"] for case in self.manifests["lifecycle"]],
            [row["id"] for row in discovery_aggregate.lifecycle_rows],
        )
        self.assertEqual(28, len(discovery_integrity_calls))
        with self.assertRaises((TypeError, ValueError, RuntimeError)):
            discovery.claim_formal_commit()
        with self.assertRaises(TypeError):
            sharding.persist_validated_epoch(
                discovery,
                authority=object(),
                destinations={},
                guard=discovery_arguments["guard"],
            )

        formal_arguments = self._build_valid_epoch("formal")
        formal_arguments.pop("integrity_calls")
        formal = sharding.validate_epoch_for_aggregation(**formal_arguments)
        commit = formal.claim_formal_commit()
        self.assertEqual(formal.epoch_id, commit.epoch_id)
        self.assertEqual("formal", commit.run_kind)
        self.assertFalse(commit.consumed)
        with self.assertRaises(RuntimeError):
            formal.claim_formal_commit()
        destinations = {
            "forward": self.repository / "results/forward.json",
            "lifecycle": self.repository / "results/lifecycle.json",
        }
        lease = sharding.ResultWriterLease.acquire(
            self.repository,
            role="serial-coordinator",
            run_kind="formal",
        )
        try:
            authority = lease.authority()
            pointer = sharding.persist_validated_epoch(
                commit,
                authority=authority,
                destinations=destinations,
                guard=formal_arguments["guard"],
            )
            self.assertTrue(pointer.is_file())
            self.assertTrue(commit.consumed)

            from scripts import run_observing_workflows_task9_eval as evaluator

            with mock.patch.object(
                evaluator,
                "persist_result_pair",
                side_effect=AssertionError("writer must not be entered"),
            ) as persist_spy:
                with self.assertRaises(RuntimeError):
                    sharding.persist_validated_epoch(
                        commit,
                        authority=authority,
                        destinations=destinations,
                        guard=formal_arguments["guard"],
                    )
            persist_spy.assert_not_called()
        finally:
            lease.close()

    def test_capabilities_require_nominal_provenance_and_bound_teardown(self):
        arguments = self._build_valid_epoch("formal")
        arguments.pop("integrity_calls")
        validated = sharding.validate_epoch_for_aggregation(**arguments)

        fabricated = object.__new__(sharding.ValidatedEpoch)
        fabricated.__dict__.update(validated.__dict__)
        with self.assertRaises((TypeError, RuntimeError)):
            sharding.aggregate_committed_cases(fabricated)

        original_forward = validated._forward_bytes
        validated._forward_bytes = b"[]"
        with self.assertRaises(RuntimeError):
            sharding.aggregate_committed_cases(validated)
        validated._forward_bytes = original_forward

        class ValidatedSubclass(sharding.ValidatedEpoch):
            pass

        with self.assertRaises(TypeError):
            sharding.aggregate_committed_cases(
                object.__new__(ValidatedSubclass)
            )

        teardown_path = arguments["teardown_receipt"]
        teardown = json.loads(teardown_path.read_text(encoding="ascii"))
        teardown["tombstone_receipts"][0][1] = "f" * 64
        sharding._atomic_write_record(teardown_path, teardown)
        with self.assertRaises(ValueError):
            sharding.validate_epoch_for_aggregation(**arguments)

        teardown_path.unlink()
        with self.assertRaises(ValueError):
            sharding.validate_epoch_for_aggregation(**arguments)

    def test_coordinator_guard_requires_zero_or_exact_result_delta(self):
        guard = sharding.CoordinatorGuard.capture(self.repository)
        self.assertEqual(guard.baseline, guard.checkpoint("unchanged"))

        baseline = self.repository / "baseline.txt"
        baseline.write_text("mutated\n", encoding="utf-8")
        with self.assertRaises(AssertionError):
            guard.checkpoint("unexpected mutation")
        baseline.write_text("baseline\n", encoding="utf-8")
        self.assertEqual(
            guard.baseline,
            guard.verify_exact_result_delta({}, "restored baseline"),
        )

        result = self.repository / "results" / "committed.json"
        result.parent.mkdir(mode=0o700)
        content = b"committed\n"
        result.write_bytes(content)
        result.chmod(0o600)
        expected = {
            "results/committed.json": hashlib.sha256(content).hexdigest()
        }
        snapshot = guard.verify_exact_result_delta(
            expected, "allowed result"
        )
        self.assertNotEqual(guard.baseline.fingerprint, snapshot.fingerprint)

        unexpected = self.repository / "unexpected.txt"
        unexpected.write_text("unexpected\n", encoding="utf-8")
        unexpected.chmod(0o600)
        with self.assertRaises(AssertionError):
            guard.verify_exact_result_delta(expected, "unexpected result")
        with self.assertRaises(ValueError):
            guard.verify_exact_result_delta(
                {"../escape": "a" * 64}, "invalid expected path"
            )

    def test_coherent_guard_and_validated_retargeting_is_rejected(self):
        other_repository = self._new_repository("other-repository")
        other_fingerprints = replace(
            input_fingerprints("formal"), archive_sha256="e" * 64
        )
        first_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="retarget-first",
            repository=self.repository,
        )
        first_arguments.pop("integrity_calls")
        second_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="retarget-second",
            repository=other_repository,
            fingerprints=other_fingerprints,
        )
        second_arguments.pop("integrity_calls")
        first = sharding.validate_epoch_for_aggregation(**first_arguments)
        second = sharding.validate_epoch_for_aggregation(**second_arguments)
        self.assertNotEqual(first.epoch_id, second.epoch_id)

        guard_fields = (
            "_repository_root",
            "_repository_key",
            "_baseline",
            "_baseline_rows",
            "_owner_pid",
        )
        original_guard_fields = {
            field: getattr(first_arguments["guard"], field)
            for field in guard_fields
        }
        for field in guard_fields:
            setattr(
                first_arguments["guard"],
                field,
                getattr(second_arguments["guard"], field),
            )
        with self.subTest(surface="guard-checkpoint"):
            with self.assertRaises(RuntimeError):
                first_arguments["guard"].checkpoint(
                    "coherently retargeted guard"
                )
        for field, value in original_guard_fields.items():
            setattr(first_arguments["guard"], field, value)

        validated_fields = (
            "_plan",
            "_forward_bytes",
            "_lifecycle_bytes",
            "_manifest_bytes",
            "_forward_sha256",
            "_lifecycle_sha256",
            "_manifest_sha256",
            "_evidence_sha256",
            "_teardown_receipt_sha256",
            "_guard",
            "_owner_pid",
        )
        for field in validated_fields:
            setattr(first, field, getattr(second, field))
        for surface, operation in (
            ("property", lambda: first.epoch_id),
            (
                "aggregation",
                lambda: sharding.aggregate_committed_cases(first),
            ),
            ("claim", first.claim_formal_commit),
        ):
            with self.subTest(surface=surface):
                with self.assertRaises(RuntimeError):
                    operation()

    def test_coherent_already_issued_commit_retargeting_is_rejected(self):
        other_repository = self._new_repository("issued-other-repository")
        other_fingerprints = replace(
            input_fingerprints("formal"), archive_sha256="e" * 64
        )
        first_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="issued-first",
            repository=self.repository,
        )
        first_arguments.pop("integrity_calls")
        second_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="issued-second",
            repository=other_repository,
            fingerprints=other_fingerprints,
        )
        second_arguments.pop("integrity_calls")
        first = sharding.validate_epoch_for_aggregation(**first_arguments)
        second = sharding.validate_epoch_for_aggregation(**second_arguments)
        first_commit = first.claim_formal_commit()
        self.assertNotEqual(first_commit.epoch_id, second.epoch_id)
        for field in (
            "_plan",
            "_forward_bytes",
            "_lifecycle_bytes",
            "_manifest_bytes",
            "_forward_sha256",
            "_lifecycle_sha256",
            "_manifest_sha256",
            "_evidence_sha256",
            "_teardown_receipt_sha256",
            "_guard",
            "_owner_pid",
        ):
            setattr(first, field, getattr(second, field))
        for surface, operation in (
            ("property", lambda: first_commit.epoch_id),
            ("consumed", lambda: first_commit.consumed),
            ("consume", first_commit._consume),
        ):
            with self.subTest(surface=surface):
                with self.assertRaises(RuntimeError):
                    operation()

    def test_persist_preflight_retryability_ends_at_writer_entry(self):
        other_repository = self._new_repository("preflight-other-repository")
        other_guard = sharding.CoordinatorGuard.capture(other_repository)
        destinations = {
            "forward": self.repository / "results/forward.json",
            "lifecycle": self.repository / "results/lifecycle.json",
        }
        lease = sharding.ResultWriterLease.acquire(
            self.repository,
            role="serial-coordinator",
            run_kind="formal",
        )
        try:
            authority = lease.authority()

            def fresh_commit(label):
                arguments = self._build_valid_epoch(
                    "formal", fixture_name=label
                )
                arguments.pop("integrity_calls")
                validated = sharding.validate_epoch_for_aggregation(
                    **arguments
                )
                return arguments, validated.claim_formal_commit()

            for preflight in (
                "bad-authority",
                "mismatched-guard",
                "bad-destinations",
                "bad-checkpoint",
            ):
                with self.subTest(preflight=preflight):
                    arguments, commit = fresh_commit(preflight)
                    if preflight == "bad-authority":
                        invalid_authority = object()
                        invalid_guard = arguments["guard"]
                        invalid_destinations = destinations
                        expected_error = TypeError
                    elif preflight == "mismatched-guard":
                        invalid_authority = authority
                        invalid_guard = other_guard
                        invalid_destinations = destinations
                        expected_error = TypeError
                    elif preflight == "bad-destinations":
                        invalid_authority = authority
                        invalid_guard = arguments["guard"]
                        invalid_destinations = {
                            "forward": self.root / "outside-forward.json",
                            "lifecycle": self.root / "outside-lifecycle.json",
                        }
                        expected_error = ValueError
                    else:
                        invalid_authority = authority
                        invalid_guard = arguments["guard"]
                        invalid_destinations = destinations
                        expected_error = AssertionError
                    baseline = self.repository / "baseline.txt"
                    if preflight == "bad-checkpoint":
                        baseline.write_text("mutated\n", encoding="utf-8")
                    try:
                        with self.assertRaises(expected_error):
                            sharding.persist_validated_epoch(
                                commit,
                                authority=invalid_authority,
                                destinations=invalid_destinations,
                                guard=invalid_guard,
                            )
                    finally:
                        if preflight == "bad-checkpoint":
                            baseline.write_text(
                                "baseline\n", encoding="utf-8"
                            )
                    self.assertFalse(commit.consumed)

            arguments, commit = fresh_commit("writer-failure")
            from scripts import run_observing_workflows_task9_eval as evaluator

            with mock.patch.object(
                evaluator,
                "persist_result_pair",
                side_effect=OSError("writer entered"),
            ) as persist_spy:
                with self.assertRaisesRegex(OSError, "writer entered"):
                    sharding.persist_validated_epoch(
                        commit,
                        authority=authority,
                        destinations=destinations,
                        guard=arguments["guard"],
                    )
            persist_spy.assert_called_once()
            self.assertTrue(commit.consumed)

            with mock.patch.object(
                evaluator,
                "persist_result_pair",
                side_effect=AssertionError("writer must not be re-entered"),
            ) as repeat_spy:
                with self.assertRaises(RuntimeError):
                    sharding.persist_validated_epoch(
                        commit,
                        authority=authority,
                        destinations=destinations,
                        guard=arguments["guard"],
                    )
            repeat_spy.assert_not_called()
        finally:
            lease.close()

    def test_live_issuance_binding_cannot_retarget_validated_epoch(self):
        other_repository = self._new_repository(
            "issuance-other-repository"
        )
        other_fingerprints = replace(
            input_fingerprints("formal"), archive_sha256="e" * 64
        )
        first_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="issuance-first",
            repository=self.repository,
        )
        first_arguments.pop("integrity_calls")
        second_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="issuance-second",
            repository=other_repository,
            fingerprints=other_fingerprints,
        )
        second_arguments.pop("integrity_calls")
        first = sharding.validate_epoch_for_aggregation(**first_arguments)
        second = sharding.validate_epoch_for_aggregation(**second_arguments)
        self.assertNotEqual(first.epoch_id, second.epoch_id)

        first_exposed = first._issuance()
        second_exposed = second._issuance()
        target_binding = getattr(
            second_exposed, "binding", second_exposed
        )
        with self.subTest(surface="immutable issuance"):
            with self.assertRaises(
                (AttributeError, FrozenInstanceError, TypeError)
            ):
                if hasattr(first_exposed, "binding"):
                    first_exposed.binding = target_binding
                else:
                    first_exposed.plan = target_binding.plan
        with self.subTest(surface="state not exposed"):
            for attribute in (
                "lock",
                "claim_state",
                "issued_capability_ref",
                "consumed",
            ):
                self.assertFalse(hasattr(first_exposed, attribute))

        for field in (
            "_plan",
            "_forward_bytes",
            "_lifecycle_bytes",
            "_manifest_bytes",
            "_forward_sha256",
            "_lifecycle_sha256",
            "_manifest_sha256",
            "_evidence_sha256",
            "_teardown_receipt_sha256",
            "_guard",
            "_owner_pid",
        ):
            setattr(first, field, getattr(second, field))
        for surface, operation in (
            ("issuance", first._issuance),
            ("nominal", first._validate_nominal),
            ("property", lambda: first.epoch_id),
            (
                "aggregation",
                lambda: sharding.aggregate_committed_cases(first),
            ),
            ("claim", first.claim_formal_commit),
        ):
            with self.subTest(surface=surface):
                with self.assertRaises(RuntimeError):
                    operation()

    def test_live_issuance_binding_cannot_retarget_issued_commit(self):
        other_repository = self._new_repository(
            "commit-issuance-other-repository"
        )
        other_fingerprints = replace(
            input_fingerprints("formal"), archive_sha256="e" * 64
        )
        first_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="commit-issuance-first",
            repository=self.repository,
        )
        first_arguments.pop("integrity_calls")
        second_arguments = self._build_valid_epoch(
            "formal",
            fixture_name="commit-issuance-second",
            repository=other_repository,
            fingerprints=other_fingerprints,
        )
        second_arguments.pop("integrity_calls")
        first = sharding.validate_epoch_for_aggregation(**first_arguments)
        second = sharding.validate_epoch_for_aggregation(**second_arguments)
        first_commit = first.claim_formal_commit()
        second_commit = second.claim_formal_commit()
        self.assertNotEqual(first_commit.epoch_id, second_commit.epoch_id)

        first_exposed = first_commit._issuance()
        second_exposed = second_commit._issuance()
        target_binding = getattr(
            second_exposed, "binding", second_exposed
        )
        second_validated_exposed = second._issuance()
        with self.subTest(surface="immutable formal issuance"):
            with self.assertRaises(
                (AttributeError, FrozenInstanceError, TypeError)
            ):
                if hasattr(first_exposed, "binding"):
                    first_exposed.binding = target_binding
                else:
                    first_exposed.plan = target_binding.plan
        with self.subTest(surface="immutable claim state"):
            with self.assertRaises(
                (AttributeError, FrozenInstanceError, TypeError)
            ):
                second_validated_exposed.issued_capability_ref = weakref.ref(
                    first_commit
                )

        first_commit._validated = second
        first_commit._owner_pid = second_commit._owner_pid
        second._issued_capability = first_commit
        for surface, operation in (
            ("issuance", first_commit._issuance),
            ("nominal", first_commit._validate_nominal),
            ("property", lambda: first_commit.epoch_id),
            ("consumed", lambda: first_commit.consumed),
            ("preflight", first_commit._preflight),
            ("consume", first_commit._consume),
        ):
            with self.subTest(surface=surface):
                with self.assertRaises(RuntimeError):
                    operation()

    def test_consumed_commit_state_cannot_be_reset_or_reused(self):
        arguments = self._build_valid_epoch(
            "formal", fixture_name="consume-reset"
        )
        arguments.pop("integrity_calls")
        validated = sharding.validate_epoch_for_aggregation(**arguments)
        commit = validated.claim_formal_commit()
        binding = commit._preflight()
        commit._consume(binding)
        self.assertTrue(commit.consumed)

        exposed = commit._issuance()
        with self.subTest(surface="immutable consume state"):
            with self.assertRaises(
                (AttributeError, FrozenInstanceError, TypeError)
            ):
                exposed.consumed = False
        commit._consumed = False
        with self.subTest(surface="consumed property"):
            self.assertTrue(commit.consumed)
        for surface, operation in (
            ("preflight", commit._preflight),
            ("consume", commit._consume),
        ):
            with self.subTest(surface=surface):
                with self.assertRaises(RuntimeError):
                    operation()

    def test_two_thread_persist_invokes_writer_exactly_once(self):
        arguments = self._build_valid_epoch(
            "formal", fixture_name="two-thread-persist"
        )
        arguments.pop("integrity_calls")
        validated = sharding.validate_epoch_for_aggregation(**arguments)
        commit = validated.claim_formal_commit()
        destinations = {
            "forward": self.repository / "results/forward.json",
            "lifecycle": self.repository / "results/lifecycle.json",
        }
        lease = sharding.ResultWriterLease.acquire(
            self.repository,
            role="serial-coordinator",
            run_kind="formal",
        )
        try:
            authority = lease.authority()
            real_preflight = commit._preflight
            preflight_barrier = threading.Barrier(2)
            writer_calls = []
            writer_lock = threading.Lock()
            errors = []
            errors_lock = threading.Lock()

            def synchronized_preflight():
                binding = real_preflight()
                preflight_barrier.wait(timeout=5)
                return binding

            def writer(*_args, **_kwargs):
                with writer_lock:
                    writer_calls.append(threading.get_ident())
                raise OSError("writer entered")

            def persist():
                try:
                    sharding.persist_validated_epoch(
                        commit,
                        authority=authority,
                        destinations=destinations,
                        guard=arguments["guard"],
                    )
                except BaseException as error:
                    with errors_lock:
                        errors.append(error)

            from scripts import run_observing_workflows_task9_eval as evaluator

            with mock.patch.object(
                commit,
                "_preflight",
                side_effect=synchronized_preflight,
            ), mock.patch.object(
                evaluator, "persist_result_pair", side_effect=writer
            ) as persist_spy:
                threads = [
                    threading.Thread(target=persist)
                    for _index in range(2)
                ]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=10)
                self.assertFalse(any(thread.is_alive() for thread in threads))

            self.assertEqual(1, len(writer_calls))
            self.assertEqual(1, persist_spy.call_count)
            self.assertEqual(2, len(errors))
            self.assertEqual(
                [OSError, RuntimeError],
                sorted((type(error) for error in errors), key=lambda item: item.__name__),
            )
            self.assertTrue(commit.consumed)
        finally:
            lease.close()


class ProgressProtocolTests(unittest.TestCase):
    setUp = SealTests.setUp
    tearDown = SealTests.tearDown
    _write_expected_tombstone = SealTests._write_expected_tombstone
    _write_attempt_one_success = SealTests._write_attempt_one_success
    _new_seal_scenario = SealTests._new_seal_scenario
    _write_scenario_attempt = SealTests._write_scenario_attempt
    _manifest_for_assignment = SealTests._manifest_for_assignment
    _prepare_lane_terminal = SealTests._prepare_lane_terminal
    _result_for_assignment = SealTests._result_for_assignment

    def _worker_root(self, run_root=None, lane="E1"):
        run_root = self.run_root if run_root is None else run_root
        parent = run_root / ("app-server" if lane == "APP" else "workers")
        parent.mkdir(mode=0o700, exist_ok=True)
        worker_root = parent if lane == "APP" else parent / lane
        worker_root.mkdir(mode=0o700, exist_ok=True)
        return worker_root

    def _success_terminal_message(self, *, seq=1):
        sharding.seal_case(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            result={**self.result},
            evidence={**self.evidence},
            manifest_case=self.manifest_case,
        )
        attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        case = sharding.read_case_seal(
            plan=self.plan,
            paths=self.paths,
            assignment=self.assignment,
            manifest_case=self.manifest_case,
        )
        return sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane=self.assignment.lane,
            seq=seq,
            type="case-terminal",
            case=self.assignment.key,
            attempt=1,
            status="success",
            classification="success",
            model_started=True,
            usage=sharding.TokenUsage(**self.usage),
            attempt_terminal_sha256=attempt.terminal_sha256,
            case_commit_sha256=case.commit_sha256,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=self.tombstone_sha256,
        )

    def test_lost_wakeup_recovers_and_ack_blocks_next_launch(self):
        self.assertTrue(
            all(
                hasattr(sharding, name)
                for name in (
                    "ProgressMessage",
                    "TokenUsage",
                    "wait_for_progress",
                    "write_ack",
                )
            ),
            "durable progress/ACK protocol is absent",
        )
        from scripts import run_observing_workflows_eval_worker as worker

        self.assertTrue(
            hasattr(worker, "publish_progress_and_wait_for_ack"),
            "worker ACK launch barrier is absent",
        )
        worker_root = self._worker_root()
        message = self._success_terminal_message()
        wakeups = []
        next_case_started = threading.Event()
        failures = []

        def publish_terminal_then_launch():
            try:
                ack = worker.publish_progress_and_wait_for_ack(
                    worker_root=worker_root,
                    message=message,
                    timeout=2.0,
                    wakeup_sink=wakeups.append,
                )
                if ack.decision == "continue":
                    next_case_started.set()
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=publish_terminal_then_launch)
        thread.start()
        observed = sharding.wait_for_progress(
            worker_root=worker_root,
            expected_lane="E1",
            expected_seq=1,
            timeout=2.0,
        )
        self.assertEqual(message, observed)
        self.assertEqual(1, len(wakeups))
        self.assertEqual(
            {"lane", "seq", "sha256"},
            set(wakeups[0]),
        )
        self.assertFalse(
            next_case_started.wait(0.05),
            "worker launched the next case before terminal ACK",
        )

        sharding.write_ack(worker_root, observed, "continue")
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertTrue(next_case_started.is_set())

    def test_progress_types_have_exact_seal_hash_truth(self):
        self.assertTrue(
            all(
                hasattr(sharding, name)
                for name in (
                    "ProgressMessage",
                    "TokenUsage",
                    "PROGRESS_FIELDS",
                    "write_progress",
                    "read_progress",
                )
            ),
            "exact progress schema is absent",
        )
        run_root = self.root / "progress-types" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        worker_root = self._worker_root(run_root)
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E1"
        )
        terminals = []
        case_paths = {}
        for assignment in assignments:
            paths, terminal = self._prepare_lane_terminal(
                run_root, assignment
            )
            terminals.append(terminal)
            case_paths[assignment.key] = paths
        sharding.seal_shard(
            worker_root=worker_root,
            plan=self.plan,
            lane="E1",
            terminals=terminals,
            manifests=self.manifests,
            case_paths=case_paths,
        )
        shard = sharding.read_shard_seal(
            worker_root=worker_root,
            plan=self.plan,
            lane="E1",
            manifests=self.manifests,
            case_paths=case_paths,
        )
        first = terminals[0]
        first_attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=case_paths[first.key],
            assignment=assignments[0],
            attempt=1,
            manifest_case=self._manifest_for_assignment(assignments[0]),
        )
        usage = sharding.TokenUsage(**self.usage)
        common = {
            "schema_version": 1,
            "epoch_id": self.plan.epoch_id,
            "run_kind": self.plan.run_kind,
            "lane": "E1",
        }
        messages = (
            sharding.ProgressMessage(
                **common,
                seq=1,
                type="lane-ready",
                case=None,
                attempt=None,
                status=None,
                classification=None,
                model_started=None,
                usage=None,
                attempt_terminal_sha256=None,
                case_commit_sha256=None,
                shard_commit_sha256=None,
                tombstone_receipt_sha256=None,
            ),
            sharding.ProgressMessage(
                **common,
                seq=2,
                type="case-started",
                case=first.key,
                attempt=1,
                status=None,
                classification=None,
                model_started=None,
                usage=None,
                attempt_terminal_sha256=None,
                case_commit_sha256=None,
                shard_commit_sha256=None,
                tombstone_receipt_sha256=None,
            ),
            sharding.ProgressMessage(
                **common,
                seq=3,
                type="case-terminal",
                case=first.key,
                attempt=1,
                status="success",
                classification="success",
                model_started=True,
                usage=usage,
                attempt_terminal_sha256=first_attempt.terminal_sha256,
                case_commit_sha256=first.case_commit_sha256,
                shard_commit_sha256=None,
                tombstone_receipt_sha256=first.tombstone_receipt_sha256,
            ),
            sharding.ProgressMessage(
                **common,
                seq=4,
                type="shard-terminal",
                case=None,
                attempt=None,
                status="success",
                classification=None,
                model_started=None,
                usage=None,
                attempt_terminal_sha256=None,
                case_commit_sha256=None,
                shard_commit_sha256=shard.commit_sha256,
                tombstone_receipt_sha256=None,
            ),
            sharding.ProgressMessage(
                **common,
                seq=5,
                type="worker-stopped",
                case=None,
                attempt=None,
                status=None,
                classification=None,
                model_started=None,
                usage=None,
                attempt_terminal_sha256=None,
                case_commit_sha256=None,
                shard_commit_sha256=None,
                tombstone_receipt_sha256=None,
            ),
        )
        expected_fields = {
            "schema_version",
            "epoch_id",
            "run_kind",
            "lane",
            "seq",
            "type",
            "case",
            "attempt",
            "status",
            "classification",
            "model_started",
            "usage",
            "attempt_terminal_sha256",
            "case_commit_sha256",
            "shard_commit_sha256",
            "tombstone_receipt_sha256",
        }
        self.assertEqual(expected_fields, sharding.PROGRESS_FIELDS)
        self.assertEqual(
            (
                "lane-ready",
                "case-started",
                "case-terminal",
                "shard-terminal",
                "worker-stopped",
            ),
            get_args(sharding.ProgressType),
        )
        self.assertEqual(
            ("continue", "retry", "stop-launches", "abort"),
            get_args(sharding.AckDecision),
        )
        self.assertEqual(4096, sharding.MAX_PROGRESS_BYTES)
        self.assertEqual(256, sharding.MAX_PROGRESS_STRING_CHARS)
        self.assertEqual(2**63 - 1, sharding.MAX_TOKEN_COUNT)
        self.assertEqual(
            [
                "schema_version",
                "epoch_id",
                "run_kind",
                "lane",
                "seq",
                "type",
                "case",
                "attempt",
                "status",
                "classification",
                "model_started",
                "usage",
                "attempt_terminal_sha256",
                "case_commit_sha256",
                "shard_commit_sha256",
                "tombstone_receipt_sha256",
            ],
            [field.name for field in fields(sharding.ProgressMessage)],
        )
        self.assertEqual(
            expected_fields,
            {field.name for field in fields(sharding.ProgressMessage)},
        )

        for message in messages:
            with self.subTest(progress_type=message.type):
                path = sharding.write_progress(worker_root, message)
                payload = json.loads(path.read_text(encoding="ascii"))
                self.assertEqual(expected_fields, set(payload))
                self.assertNotIn("prompt", path.read_text(encoding="ascii"))
                self.assertEqual(
                    message,
                    sharding.read_progress(path, "E1", message.seq),
                )
                for field_name in (
                    "attempt_terminal_sha256",
                    "case_commit_sha256",
                    "shard_commit_sha256",
                    "tombstone_receipt_sha256",
                ):
                    expected = getattr(message, field_name)
                    self.assertEqual(expected, payload[field_name])

    def _lane_message(self, *, seq, progress_type, lane="E1"):
        return sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane=lane,
            seq=seq,
            type=progress_type,
            case=None,
            attempt=None,
            status=None,
            classification=None,
            model_started=None,
            usage=None,
            attempt_terminal_sha256=None,
            case_commit_sha256=None,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=None,
        )

    def _case_started_message(self, *, seq, attempt=1):
        return sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=seq,
            type="case-started",
            case=self.assignment.key,
            attempt=attempt,
            status=None,
            classification=None,
            model_started=None,
            usage=None,
            attempt_terminal_sha256=None,
            case_commit_sha256=None,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=None,
        )

    def _write_protocol_prefix(self, worker_root, messages, decisions):
        self.assertEqual(len(messages), len(decisions))
        for message, decision in zip(messages, decisions, strict=True):
            sharding.write_progress(worker_root, message)
            sharding.write_ack(worker_root, message, decision)

    def test_resume_replays_durable_stop_prefix_into_exact_ledger(self):
        worker_root = self._worker_root()
        terminal = self._success_terminal_message(seq=3)
        messages = (
            self._lane_message(seq=1, progress_type="lane-ready"),
            self._case_started_message(seq=2),
            terminal,
            self._lane_message(seq=4, progress_type="worker-stopped"),
        )
        self._write_protocol_prefix(
            worker_root,
            messages,
            ("continue", "continue", "stop-launches", "stop-launches"),
        )

        ledger = sharding._resume_protocol_bindings(
            plan=self.plan,
            run_root=self.run_root,
            manifests=self.manifests,
            max_total_tokens=terminal.usage.total_tokens,
        )

        self.assertIs(type(ledger), sharding.ProgressAckLedger)
        self.assertEqual(terminal.usage.total_tokens, ledger.total_tokens)
        self.assertTrue(ledger.stop_launches)
        self.assertFalse(ledger.aborted)
        self.assertEqual(4, ledger._state.last_sequence["E1"])
        self.assertIn(self.assignment.key, {
            key for key, _attempt in ledger._state.completed_attempts
        })
        self.assertIn("E1", ledger._state.exited)
        resume = sharding.ResumePlan(
            run_kind=self.plan.run_kind,
            reusable=(),
            pending=(self.assignment.key,),
            invalid=(),
        )
        self.assertTrue(
            sharding._resume_replay_requires_cleanup(
                ledger=ledger,
                resume=resume,
                plan=self.plan,
            )
        )

    def test_resume_rejects_durable_ack_that_does_not_match_replay(self):
        worker_root = self._worker_root()
        terminal = self._success_terminal_message(seq=3)
        messages = (
            self._lane_message(seq=1, progress_type="lane-ready"),
            self._case_started_message(seq=2),
            terminal,
        )
        self._write_protocol_prefix(
            worker_root,
            messages,
            ("continue", "continue", "continue"),
        )

        with self.assertRaisesRegex(ValueError, "durable ACK"):
            sharding._resume_protocol_bindings(
                plan=self.plan,
                run_root=self.run_root,
                manifests=self.manifests,
                max_total_tokens=terminal.usage.total_tokens,
            )

    def _failed_terminal_scenario(
        self,
        name,
        *,
        cleanup_passed,
        canonical_binding,
    ):
        paths, receipt_sha256 = self._new_seal_scenario(
            name, canonical_binding=canonical_binding
        )
        failure_text = f"{name} failed"
        failure = {
            "classification": "semantic",
            "type": "SemanticFailure",
            "chars": len(failure_text),
            "sha256": hashlib.sha256(failure_text.encode()).hexdigest(),
        }
        self._write_scenario_attempt(
            paths,
            status="failed",
            classification="semantic",
            cleanup_passed=cleanup_passed,
            failure=failure,
        )
        attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        run_root = paths.root.parent.parent
        worker_root = self._worker_root(run_root)
        message = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=1,
            type="case-terminal",
            case=self.assignment.key,
            attempt=1,
            status="failed",
            classification="semantic",
            model_started=True,
            usage=sharding.TokenUsage(**self.usage),
            attempt_terminal_sha256=attempt.terminal_sha256,
            case_commit_sha256=None,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=receipt_sha256,
        )
        return worker_root, message

    def test_resume_replays_durable_abort_prefix(self):
        worker_root, terminal = self._failed_terminal_scenario(
            "resume-abort-prefix",
            cleanup_passed=True,
            canonical_binding="expected",
        )
        terminal = replace(terminal, seq=3)
        messages = (
            self._lane_message(seq=1, progress_type="lane-ready"),
            self._case_started_message(seq=2),
            terminal,
            self._lane_message(seq=4, progress_type="worker-stopped"),
        )
        self._write_protocol_prefix(
            worker_root,
            messages,
            ("continue", "continue", "abort", "abort"),
        )

        ledger = sharding._resume_protocol_bindings(
            plan=self.plan,
            run_root=worker_root.parent.parent,
            manifests=self.manifests,
            max_total_tokens=None,
        )

        self.assertTrue(ledger.aborted)
        self.assertFalse(ledger.stop_launches)
        self.assertIn("E1", ledger._state.exited)
        self.assertEqual(4, ledger._state.last_sequence["E1"])

    def test_resume_preserves_pending_retry_ack_authority(self):
        paths, receipt_sha256 = self._new_seal_scenario(
            "resume-retry-prefix",
            canonical_binding="expected",
        )
        failure_text = "retryable pre-model infrastructure failure"
        sharding.write_attempt_start(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        sharding.write_attempt_terminal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
            status="failed",
            classification="pre-model-infrastructure",
            model_started=False,
            cleanup_passed=True,
            usage=None,
            failure={
                "classification": "pre-model-infrastructure",
                "type": "RuntimeError",
                "chars": len(failure_text),
                "sha256": hashlib.sha256(
                    failure_text.encode("utf-8")
                ).hexdigest(),
            },
        )
        attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        worker_root = self._worker_root(paths.root.parent.parent)
        terminal = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=3,
            type="case-terminal",
            case=self.assignment.key,
            attempt=1,
            status="failed",
            classification="pre-model-infrastructure",
            model_started=False,
            usage=None,
            attempt_terminal_sha256=attempt.terminal_sha256,
            case_commit_sha256=None,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=receipt_sha256,
        )
        self._write_protocol_prefix(
            worker_root,
            (
                self._lane_message(seq=1, progress_type="lane-ready"),
                self._case_started_message(seq=2),
                terminal,
            ),
            ("continue", "continue", "retry"),
        )

        ledger = sharding._resume_protocol_bindings(
            plan=self.plan,
            run_root=worker_root.parent.parent,
            manifests=self.manifests,
            max_total_tokens=None,
        )

        self.assertEqual(
            (self.assignment.key, 2),
            ledger.pending_retries["E1"],
        )
        self.assertFalse(ledger.aborted)
        self.assertFalse(ledger.stop_launches)
        resume = sharding.ResumePlan(
            run_kind=self.plan.run_kind,
            reusable=(),
            pending=(self.assignment.key,),
            invalid=(),
        )
        self.assertFalse(
            sharding._resume_replay_requires_cleanup(
                ledger=ledger,
                resume=resume,
                plan=self.plan,
            )
        )

    def _failed_precommit_scenario(
        self,
        name,
        *,
        fault_point,
        cleanup_passed=True,
        canonical_binding="expected",
        include_result=True,
    ):
        worker_root, message = self._failed_terminal_scenario(
            name,
            cleanup_passed=cleanup_passed,
            canonical_binding=canonical_binding,
        )
        paths = sharding.paths_for_case(
            worker_root.parent.parent,
            self.assignment,
        )
        terminal = json.loads(
            sharding.paths_for_attempt(paths, 1).terminal.read_text(
                encoding="ascii"
            )
        )
        evidence = {
            **self.evidence,
            "status": "failed",
            "classification": "semantic",
            "failure": terminal["failure"],
            "process_cleanup_passed": cleanup_passed,
            "credential_cleanup_passed": cleanup_passed,
        }

        def interrupt(point):
            if point == fault_point:
                raise RuntimeError(f"fault at {point}")

        with self.assertRaisesRegex(RuntimeError, f"fault at {fault_point}"):
            sharding.seal_case(
                plan=self.plan,
                paths=paths,
                assignment=self.assignment,
                attempt=1,
                result={**self.result} if include_result else None,
                evidence=evidence,
                manifest_case=self.manifest_case,
                fault_injector=interrupt,
            )
        return worker_root, paths, message

    def _success_terminal_scenario(self, name):
        run_root = self.root / name / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        paths, terminal = self._prepare_lane_terminal(
            run_root, self.assignment
        )
        attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        message = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane=self.assignment.lane,
            seq=1,
            type="case-terminal",
            case=self.assignment.key,
            attempt=1,
            status="success",
            classification="success",
            model_started=True,
            usage=sharding.TokenUsage(**self.usage),
            attempt_terminal_sha256=attempt.terminal_sha256,
            case_commit_sha256=terminal.case_commit_sha256,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=terminal.tombstone_receipt_sha256,
        )
        return self._worker_root(run_root), paths, message

    def _rewrite_success_chain(
        self,
        paths,
        message,
        *,
        forged_identity=False,
        result_case_id=None,
    ):
        attempt_paths = sharding.paths_for_attempt(paths, 1)
        start = json.loads(attempt_paths.start.read_text(encoding="ascii"))
        terminal = json.loads(
            attempt_paths.terminal.read_text(encoding="ascii")
        )
        result_path = paths.sealed / "case-result.json"
        evidence_path = paths.sealed / "case-evidence.json"
        commit_path = paths.sealed / "case-commit.json"
        result = json.loads(result_path.read_text(encoding="ascii"))
        evidence = json.loads(evidence_path.read_text(encoding="ascii"))
        commit = json.loads(commit_path.read_text(encoding="ascii"))
        if forged_identity:
            start["manifest_sha256"] = "a" * 64
            start["manifest_case_sha256"] = "b" * 64
            terminal["manifest_sha256"] = "a" * 64
            terminal["manifest_case_sha256"] = "b" * 64
            evidence["manifest_sha256"] = "a" * 64
            evidence["manifest_case_sha256"] = "b" * 64
            evidence["archive_sha256"] = "c" * 64
            evidence["marketplace_sha256"] = "d" * 64
            evidence["evaluator_sha256"] = "e" * 64
            evidence["transport_config_sha256"] = "f" * 64
            commit["manifest_sha256"] = "a" * 64
            commit["manifest_case_sha256"] = "b" * 64
        start_content = sharding._atomic_write_record(
            attempt_paths.start, start
        )
        start_sha256 = hashlib.sha256(start_content).hexdigest()
        terminal["start_sha256"] = start_sha256
        terminal_content = sharding._atomic_write_record(
            attempt_paths.terminal, terminal
        )
        terminal_sha256 = hashlib.sha256(terminal_content).hexdigest()
        if result_case_id is not None:
            result["id"] = result_case_id
        result_content = sharding._atomic_write_record(result_path, result)
        result_sha256 = hashlib.sha256(result_content).hexdigest()
        evidence["attempt_start_sha256"] = start_sha256
        evidence["attempt_terminal_sha256"] = terminal_sha256
        evidence["result_sha256"] = result_sha256
        evidence_content = sharding._atomic_write_record(
            evidence_path, evidence
        )
        commit["attempt_start_sha256"] = start_sha256
        commit["attempt_terminal_sha256"] = terminal_sha256
        commit["result_sha256"] = result_sha256
        commit["evidence_sha256"] = hashlib.sha256(
            evidence_content
        ).hexdigest()
        commit_content = sharding._atomic_write_record(commit_path, commit)
        return replace(
            message,
            attempt_terminal_sha256=terminal_sha256,
            case_commit_sha256=hashlib.sha256(commit_content).hexdigest(),
        )

    def _case_started_for_terminal(self, terminal, *, seq):
        return replace(
            terminal,
            seq=seq,
            type="case-started",
            status=None,
            classification=None,
            model_started=None,
            usage=None,
            attempt_terminal_sha256=None,
            case_commit_sha256=None,
            tombstone_receipt_sha256=None,
        )

    def _relabel_success_attempt_as_two(self, paths, message):
        attempt_one = sharding.paths_for_attempt(paths, 1)
        attempt_two = sharding.paths_for_attempt(paths, 2)
        attempt_one.root.rename(attempt_two.root)
        start = json.loads(attempt_two.start.read_text(encoding="ascii"))
        terminal = json.loads(
            attempt_two.terminal.read_text(encoding="ascii")
        )
        start["attempt"] = 2
        start_content = sharding._atomic_write_record(
            attempt_two.start, start
        )
        start_sha256 = hashlib.sha256(start_content).hexdigest()
        terminal["attempt"] = 2
        terminal["start_sha256"] = start_sha256
        terminal_content = sharding._atomic_write_record(
            attempt_two.terminal, terminal
        )
        terminal_sha256 = hashlib.sha256(terminal_content).hexdigest()
        evidence_path = paths.sealed / "case-evidence.json"
        commit_path = paths.sealed / "case-commit.json"
        evidence = json.loads(evidence_path.read_text(encoding="ascii"))
        commit = json.loads(commit_path.read_text(encoding="ascii"))
        evidence["attempt"] = 2
        evidence["attempt_start_sha256"] = start_sha256
        evidence["attempt_terminal_sha256"] = terminal_sha256
        evidence_content = sharding._atomic_write_record(
            evidence_path, evidence
        )
        commit["attempt"] = 2
        commit["attempt_start_sha256"] = start_sha256
        commit["attempt_terminal_sha256"] = terminal_sha256
        commit["evidence_sha256"] = hashlib.sha256(
            evidence_content
        ).hexdigest()
        commit_content = sharding._atomic_write_record(commit_path, commit)
        return replace(
            message,
            attempt=2,
            attempt_terminal_sha256=terminal_sha256,
            case_commit_sha256=hashlib.sha256(commit_content).hexdigest(),
        )

    def test_progress_rejects_rehashed_identity_and_cross_case_result(self):
        for name, mutation in (
            ("forged-authoritative-identity", {"forged_identity": True}),
            (
                "cross-case-result",
                {
                    "result_case_id": dict(
                        sharding.FROZEN_LANE_CASES
                    )["E1"][1].case_id
                },
            ),
        ):
            with self.subTest(attack=name):
                worker_root, paths, message = (
                    self._success_terminal_scenario(name)
                )
                forged = self._rewrite_success_chain(
                    paths, message, **mutation
                )
                with self.assertRaises(ValueError):
                    sharding.write_progress(worker_root, forged)

        worker_root, paths, message = self._success_terminal_scenario(
            "forged-authoritative-read"
        )
        path = sharding.write_progress(worker_root, message)
        forged = self._rewrite_success_chain(
            paths,
            message,
            result_case_id=dict(sharding.FROZEN_LANE_CASES)["E1"][1].case_id,
        )
        sharding._atomic_write_record(path, asdict(forged))
        with self.assertRaises(ValueError):
            sharding.read_progress(path, "E1", 1)

    def test_shard_progress_rejects_rehashed_case_identity(self):
        run_root = self.root / "forged-shard-case" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        worker_root = self._worker_root(run_root)
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E1"
        )
        terminals = []
        case_paths = {}
        for assignment in assignments:
            paths, terminal = self._prepare_lane_terminal(
                run_root, assignment
            )
            terminals.append(terminal)
            case_paths[assignment.key] = paths
        shard_path = sharding.seal_shard(
            worker_root=worker_root,
            plan=self.plan,
            lane="E1",
            terminals=terminals,
            manifests=self.manifests,
            case_paths=case_paths,
        )
        first_assignment = assignments[0]
        first_terminal = terminals[0]
        first_paths = case_paths[first_assignment.key]
        first_attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=first_paths,
            assignment=first_assignment,
            attempt=1,
            manifest_case=self._manifest_for_assignment(first_assignment),
        )
        first_message = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=1,
            type="case-terminal",
            case=first_assignment.key,
            attempt=1,
            status="success",
            classification="success",
            model_started=True,
            usage=sharding.TokenUsage(**self.usage),
            attempt_terminal_sha256=first_attempt.terminal_sha256,
            case_commit_sha256=first_terminal.case_commit_sha256,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=(
                first_terminal.tombstone_receipt_sha256
            ),
        )
        forged = self._rewrite_success_chain(
            first_paths, first_message, forged_identity=True
        )
        shard_payload = json.loads(shard_path.read_text(encoding="ascii"))
        shard_payload["terminals"][0]["attempt_terminal_sha256"] = (
            forged.attempt_terminal_sha256
        )
        shard_payload["terminals"][0]["case_commit_sha256"] = (
            forged.case_commit_sha256
        )
        forged_shard_content = sharding._atomic_write_record(
            shard_path, shard_payload
        )
        shard_message = sharding.ProgressMessage(
            **{
                **asdict(self._lane_message(
                    seq=1, progress_type="shard-terminal"
                )),
                "status": "success",
                "shard_commit_sha256": hashlib.sha256(
                    forged_shard_content
                ).hexdigest(),
            }
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(worker_root, shard_message)

    def test_progress_rejects_invalid_attempt_directory_inventory(self):
        worker_root, paths, message = self._success_terminal_scenario(
            "attempt-extra"
        )
        attempt_paths = sharding.paths_for_attempt(paths, 1)
        sharding._atomic_write_record(
            attempt_paths.root / "unexpected.json",
            {"unexpected": True},
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(worker_root, message)

        worker_root, paths, message = self._success_terminal_scenario(
            "attempt-missing"
        )
        sharding.paths_for_attempt(paths, 1).start.unlink()
        with self.assertRaises(ValueError):
            sharding.write_progress(worker_root, message)

        worker_root, paths, message = self._success_terminal_scenario(
            "attempt-replaced"
        )
        attempt_paths = sharding.paths_for_attempt(paths, 1)
        terminal_content = attempt_paths.terminal.read_bytes()
        attempt_paths.terminal.unlink()
        replacement = attempt_paths.root / "replacement.json"
        replacement.write_bytes(terminal_content)
        replacement.chmod(0o600)
        with self.assertRaises(ValueError):
            sharding.write_progress(worker_root, message)

    def test_progress_rejects_invalid_attempt_root_inventory(self):
        for name, prepare in (
            (
                "attempt-root-03",
                lambda attempts: (attempts / "03").mkdir(mode=0o700),
            ),
            (
                "attempt-root-partial-02",
                lambda attempts: (attempts / "02").mkdir(mode=0o700),
            ),
            (
                "attempt-root-file-02",
                lambda attempts: sharding._atomic_write_record(
                    attempts / "02", {"invalid": True}
                ),
            ),
        ):
            with self.subTest(layout=name):
                worker_root, paths, message = (
                    self._success_terminal_scenario(name)
                )
                prepare(paths.attempts)
                with self.assertRaises(ValueError):
                    sharding.write_progress(worker_root, message)

        worker_root, paths, message = self._success_terminal_scenario(
            "attempt-root-only-02"
        )
        message = self._relabel_success_attempt_as_two(paths, message)
        with self.assertRaises(ValueError):
            sharding.write_progress(worker_root, message)

    def test_terminal_requires_continue_ack_launch_authority(self):
        terminal = self._success_terminal_message()
        unauthorized = sharding.ProgressAckLedger(max_total_tokens=None)
        with self.assertRaises(ValueError):
            unauthorized.accept_progress(terminal)
        self.assertEqual(0, unauthorized.total_tokens)

        stopped = sharding.ProgressAckLedger(max_total_tokens=0)
        started = self._case_started_for_terminal(terminal, seq=1)
        self.assertEqual("stop-launches", stopped.accept_progress(started))
        with self.assertRaises(ValueError):
            stopped.accept_progress(replace(terminal, seq=2))
        self.assertEqual(0, stopped.total_tokens)

        authorized = sharding.ProgressAckLedger(max_total_tokens=None)
        self.assertEqual("continue", authorized.accept_progress(started))
        self.assertEqual(
            "continue",
            authorized.accept_progress(replace(terminal, seq=2)),
        )
        self.assertEqual(self.usage["total_tokens"], authorized.total_tokens)

    def test_terminal_must_match_authorized_attempt(self):
        terminal = self._success_terminal_message()
        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        started = self._case_started_for_terminal(terminal, seq=1)
        self.assertEqual("continue", ledger.accept_progress(started))
        with self.assertRaises(ValueError):
            ledger.accept_progress(
                replace(terminal, seq=2, attempt=2)
            )
        self.assertEqual(0, ledger.total_tokens)

    def test_completed_attempt_cannot_be_reauthorized_or_recounted(self):
        terminal = self._success_terminal_message()
        started = self._case_started_for_terminal(terminal, seq=1)
        terminal = replace(terminal, seq=2)
        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        self.assertEqual("continue", ledger.accept_progress(started))
        self.assertEqual("continue", ledger.accept_progress(terminal))
        self.assertEqual(self.usage["total_tokens"], ledger.total_tokens)
        self.assertEqual("continue", ledger.accept_progress(terminal))
        self.assertEqual(self.usage["total_tokens"], ledger.total_tokens)

        replayed_start = replace(started, seq=3)
        with self.assertRaises(ValueError):
            ledger.accept_progress(replayed_start)
        self.assertEqual(self.usage["total_tokens"], ledger.total_tokens)

        next_key = dict(sharding.FROZEN_LANE_CASES)["E1"][1]
        next_start = replace(started, seq=3, case=next_key)
        self.assertEqual("continue", ledger.accept_progress(next_start))
        with self.assertRaises(ValueError):
            ledger.accept_progress(replace(terminal, seq=4))
        self.assertEqual(self.usage["total_tokens"], ledger.total_tokens)

    def test_duplicate_active_start_does_not_consume_sequence(self):
        terminal = self._success_terminal_message()
        started = self._case_started_for_terminal(terminal, seq=1)
        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        self.assertEqual("continue", ledger.accept_progress(started))
        with self.assertRaises(ValueError):
            ledger.accept_progress(replace(started, seq=2))
        self.assertEqual(
            "continue",
            ledger.accept_progress(replace(terminal, seq=2)),
        )
        self.assertEqual(self.usage["total_tokens"], ledger.total_tokens)

    def test_case_terminal_truth_table_drives_exact_ack_decisions(self):
        self.assertTrue(
            hasattr(sharding, "ProgressAckLedger"),
            "stateful progress decisions are absent",
        )
        success_root = self._worker_root()
        success = self._success_terminal_message()
        evaluated_root, evaluated = self._failed_terminal_scenario(
            "evaluated-failure",
            cleanup_passed=True,
            canonical_binding="expected",
        )
        no_receipt_root, no_receipt = self._failed_terminal_scenario(
            "cleanup-failure-no-receipt",
            cleanup_passed=False,
            canonical_binding=None,
        )
        receipt_root, with_receipt = self._failed_terminal_scenario(
            "cleanup-failure-with-receipt",
            cleanup_passed=False,
            canonical_binding="expected",
        )
        replaced_root, replaced = self._failed_terminal_scenario(
            "cleanup-failure-replaced-receipt",
            cleanup_passed=False,
            canonical_binding="replaced",
        )
        missing_root, missing = self._failed_terminal_scenario(
            "cleanup-failure-missing-receipt",
            cleanup_passed=False,
            canonical_binding="missing",
        )
        committed_run_root = self.root / "committed-failure" / "run"
        committed_run_root.mkdir(parents=True, mode=0o700)
        committed_run_root.chmod(0o700)
        (committed_run_root / "cases").mkdir(mode=0o700)
        committed_paths, committed_terminal = self._prepare_lane_terminal(
            committed_run_root,
            self.assignment,
            status="failed",
            cleanup_passed=True,
            canonical_binding="expected",
            seal_case_record=True,
        )
        committed_attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=committed_paths,
            assignment=self.assignment,
            attempt=1,
            manifest_case=self.manifest_case,
        )
        committed = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=1,
            type="case-terminal",
            case=self.assignment.key,
            attempt=1,
            status="failed",
            classification="semantic",
            model_started=True,
            usage=sharding.TokenUsage(**self.usage),
            attempt_terminal_sha256=committed_attempt.terminal_sha256,
            case_commit_sha256=committed_terminal.case_commit_sha256,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=(
                committed_terminal.tombstone_receipt_sha256
            ),
        )
        committed_root = self._worker_root(committed_run_root)
        scenarios = (
            (success_root, success, "continue"),
            (evaluated_root, evaluated, "abort"),
            (committed_root, committed, "abort"),
            (no_receipt_root, no_receipt, "abort"),
            (receipt_root, with_receipt, "abort"),
            (replaced_root, replaced, "abort"),
            (missing_root, missing, "abort"),
        )
        for worker_root, message, expected in scenarios:
            with self.subTest(condition=worker_root.parent.parent.name):
                path = sharding.write_progress(worker_root, message)
                observed = sharding.read_progress(path, "E1", 1)
                ledger = sharding.ProgressAckLedger(max_total_tokens=None)
                started = self._case_started_for_terminal(observed, seq=1)
                self.assertEqual("continue", ledger.accept_progress(started))
                self.assertEqual(
                    expected,
                    ledger.accept_progress(replace(observed, seq=2)),
                )

        invalid = (
            (evaluated_root, replace(evaluated, tombstone_receipt_sha256=None)),
            (receipt_root, replace(with_receipt, tombstone_receipt_sha256=None)),
            (
                no_receipt_root,
                replace(no_receipt, tombstone_receipt_sha256="f" * 64),
            ),
            (
                success_root,
                replace(success, case_commit_sha256=None),
            ),
            (
                success_root,
                replace(success, tombstone_receipt_sha256=None),
            ),
        )
        for worker_root, message in invalid:
            with self.subTest(invalid=message):
                with self.assertRaises((TypeError, ValueError)):
                    sharding.write_progress(worker_root, message)

    def test_durable_nonretryable_failure_ack_remains_abort(self):
        worker_root, terminal = self._failed_terminal_scenario(
            "durable-nonretryable-failure",
            cleanup_passed=True,
            canonical_binding="expected",
        )
        terminal = replace(terminal, seq=2)
        started = self._case_started_for_terminal(terminal, seq=1)
        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        self.assertEqual("continue", ledger.accept_progress(started))
        self.assertEqual(
            "abort",
            ledger.accept_durable_progress(
                worker_root=worker_root,
                message=terminal,
            ),
        )
        self.assertTrue(ledger.aborted)

    def test_failed_terminal_accepts_only_authoritative_precommit_case_artifacts(self):
        for fault_point, expected_inventory in (
            ("after-result-replace", ("case-result.json",)),
            (
                "before-case-commit",
                ("case-evidence.json", "case-result.json"),
            ),
        ):
            with self.subTest(fault_point=fault_point):
                worker_root, paths, message = self._failed_precommit_scenario(
                    f"valid-{fault_point}",
                    fault_point=fault_point,
                )
                self.assertEqual(
                    expected_inventory,
                    tuple(sorted(path.name for path in paths.sealed.iterdir())),
                )
                progress_path = sharding.write_progress(worker_root, message)
                observed = sharding.read_progress(progress_path, "E1", 1)
                ledger = sharding.ProgressAckLedger(max_total_tokens=None)
                self.assertEqual(
                    "continue",
                    ledger.accept_progress(
                        self._case_started_for_terminal(observed, seq=1)
                    ),
                )
                self.assertEqual(
                    "abort",
                    ledger.accept_progress(replace(observed, seq=2)),
                )
                self.assertFalse((paths.sealed / "case-commit.json").exists())

        worker_root, paths, message = self._failed_precommit_scenario(
            "valid-evidence-only",
            fault_point="before-case-commit",
            cleanup_passed=False,
            canonical_binding=None,
            include_result=False,
        )
        self.assertEqual(
            ("case-evidence.json",),
            tuple(path.name for path in paths.sealed.iterdir()),
        )
        sharding.write_progress(worker_root, message)

        empty_root, empty_message = self._failed_terminal_scenario(
            "valid-empty-seal",
            cleanup_passed=False,
            canonical_binding=None,
        )
        empty_paths = sharding.paths_for_case(
            empty_root.parent.parent,
            self.assignment,
        )
        empty_paths.sealed.mkdir(mode=0o700)
        sharding.write_progress(empty_root, empty_message)

        for mutation in ("orphan-evidence", "cross-case-result", "extra-record"):
            with self.subTest(mutation=mutation):
                worker_root, paths, message = self._failed_precommit_scenario(
                    f"invalid-{mutation}",
                    fault_point="before-case-commit",
                )
                if mutation == "orphan-evidence":
                    (paths.sealed / "case-result.json").unlink()
                elif mutation == "cross-case-result":
                    result_path = paths.sealed / "case-result.json"
                    result = json.loads(result_path.read_text(encoding="ascii"))
                    result["id"] = dict(sharding.FROZEN_LANE_CASES)["E1"][1].case_id
                    sharding._atomic_write_record(result_path, result)
                else:
                    sharding._atomic_write_record(
                        paths.sealed / "unexpected.json",
                        {"unexpected": True},
                    )
                with self.assertRaises(ValueError):
                    sharding.write_progress(worker_root, message)

    def test_worker_protocol_and_ledger_reject_mixed_epoch_and_run_kind(self):
        formal = sharding.build_epoch_plan(
            run_kind="formal",
            manifests=self.manifests,
            fingerprints=input_fingerprints("formal"),
        )
        diagnostic_first = self._lane_message(
            seq=1,
            progress_type="lane-ready",
        )
        formal_second = replace(
            diagnostic_first,
            epoch_id=formal.epoch_id,
            run_kind=formal.run_kind,
            seq=2,
        )

        progress_run_root = self.root / "mixed-progress" / "run"
        progress_run_root.mkdir(parents=True, mode=0o700)
        progress_run_root.chmod(0o700)
        progress_root = self._worker_root(progress_run_root)
        sharding.write_progress(progress_root, diagnostic_first)
        progress_path = progress_root / "progress" / "000002.json"
        sharding._atomic_write_record(progress_path, asdict(formal_second))
        with self.assertRaises(ValueError):
            sharding.wait_for_progress(
                worker_root=progress_root,
                expected_lane="E1",
                expected_seq=2,
                timeout=0.1,
            )

        ack_run_root = self.root / "mixed-ack" / "run"
        ack_run_root.mkdir(parents=True, mode=0o700)
        ack_run_root.chmod(0o700)
        ack_root = self._worker_root(ack_run_root)
        sharding.write_progress(ack_root, diagnostic_first)
        sharding.write_ack(ack_root, diagnostic_first, "continue")
        formal_content = sharding._atomic_write_record(
            ack_root / "progress" / "000002.json",
            asdict(formal_second),
        )
        sharding._atomic_write_record(
            ack_root / "acks" / "000002.json",
            {
                "schema_version": 1,
                "epoch_id": formal_second.epoch_id,
                "run_kind": formal_second.run_kind,
                "lane": formal_second.lane,
                "seq": formal_second.seq,
                "message_sha256": hashlib.sha256(formal_content).hexdigest(),
                "decision": "continue",
            },
        )
        with self.assertRaises(ValueError):
            sharding.wait_for_ack(ack_root, formal_second, 0.1)

        for mixed in (
            formal_second,
            replace(
                diagnostic_first,
                seq=2,
                run_kind="formal",
            ),
        ):
            with self.subTest(ledger=mixed):
                ledger = sharding.ProgressAckLedger(max_total_tokens=None)
                self.assertEqual(
                    "continue",
                    ledger.accept_progress(diagnostic_first),
                )
                with self.assertRaises(ValueError):
                    ledger.accept_progress(mixed)
                self.assertEqual(
                    "continue",
                    ledger.accept_progress(
                        replace(diagnostic_first, seq=2)
                    ),
                )
                self.assertEqual(0, ledger.total_tokens)

    def test_concurrent_different_progress_and_ack_publishers_never_clobber(self):
        def exercise(*, target, first_call, second_call, first_value, second_value):
            real_optional = sharding._read_optional_protocol_record
            real_atomic = sharding._atomic_write_record
            real_read = sharding._read_canonical_record
            stale_absence = threading.Barrier(2)
            first_readback = threading.Event()
            successes = []
            failures = []

            def synchronized_optional(path, label, *, byte_cap):
                if path == target and not path.exists():
                    stale_absence.wait(2.0)
                    return None
                return real_optional(path, label, byte_cap=byte_cap)

            def ordered_atomic(path, payload):
                if path == target and (
                    payload.get("type") == second_value
                    or payload.get("decision") == second_value
                ):
                    if not first_readback.wait(2.0):
                        raise AssertionError("first publisher never read back")
                return real_atomic(path, payload)

            def signal_first_readback(path, label, *, byte_cap):
                value = real_read(path, label, byte_cap=byte_cap)
                if (
                    path == target
                    and threading.current_thread().name == "protocol-first"
                ):
                    first_readback.set()
                return value

            def publish(call, value):
                try:
                    call()
                    successes.append(value)
                except BaseException as error:
                    failures.append(error)

            with mock.patch.object(
                sharding,
                "_read_optional_protocol_record",
                side_effect=synchronized_optional,
            ), mock.patch.object(
                sharding,
                "_atomic_write_record",
                side_effect=ordered_atomic,
            ), mock.patch.object(
                sharding,
                "_read_canonical_record",
                side_effect=signal_first_readback,
            ):
                first = threading.Thread(
                    target=publish,
                    args=(first_call, first_value),
                    name="protocol-first",
                )
                second = threading.Thread(
                    target=publish,
                    args=(second_call, second_value),
                    name="protocol-second",
                )
                first.start()
                second.start()
                first.join(3.0)
                second.join(3.0)
                self.assertFalse(first.is_alive())
                self.assertFalse(second.is_alive())

            self.assertEqual(1, len(successes))
            self.assertEqual(1, len(failures))
            payload = json.loads(target.read_text(encoding="ascii"))
            durable_value = payload.get("type", payload.get("decision"))
            self.assertEqual(successes[0], durable_value)
            self.assertEqual(
                [],
                [
                    path.name
                    for path in target.parent.iterdir()
                    if ".tmp-" in path.name
                ],
            )

        progress_run = self.root / "progress-race" / "run"
        progress_run.mkdir(parents=True, mode=0o700)
        progress_run.chmod(0o700)
        progress_worker = self._worker_root(progress_run)
        ready = self._lane_message(seq=1, progress_type="lane-ready")
        stopped = self._lane_message(seq=1, progress_type="worker-stopped")
        exercise(
            target=progress_worker / "progress" / "000001.json",
            first_call=lambda: sharding.write_progress(progress_worker, ready),
            second_call=lambda: sharding.write_progress(progress_worker, stopped),
            first_value="lane-ready",
            second_value="worker-stopped",
        )

        ack_run = self.root / "ack-race" / "run"
        ack_run.mkdir(parents=True, mode=0o700)
        ack_run.chmod(0o700)
        ack_worker = self._worker_root(ack_run)
        sharding.write_progress(ack_worker, ready)
        exercise(
            target=ack_worker / "acks" / "000001.json",
            first_call=lambda: sharding.write_ack(
                ack_worker,
                ready,
                "continue",
            ),
            second_call=lambda: sharding.write_ack(
                ack_worker,
                ready,
                "abort",
            ),
            first_value="continue",
            second_value="abort",
        )

    def test_protocol_inventory_caps_crash_temps_and_checks_deadline_while_scanning(self):
        self.assertEqual(35, sharding.MAX_PROTOCOL_RECORDS)
        self.assertEqual(19, sharding.MAX_PROTOCOL_CRASH_TEMPS)
        run_root = self.root / "bounded-inventory" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        progress_root = worker_root / "progress"
        progress_root.mkdir(mode=0o700)
        for index in range(sharding.MAX_PROTOCOL_CRASH_TEMPS + 1):
            path = progress_root / (
                f".000001.json.tmp-{index + 1}-{index:032x}"
            )
            path.write_bytes(b"")
            path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "crash temporary"):
            sharding._protocol_sequence_inventory(
                progress_root,
                label="progress",
                deadline=None,
            )
        with self.assertRaisesRegex(ValueError, "crash temporary"):
            sharding.write_progress(
                worker_root,
                self._lane_message(seq=1, progress_type="lane-ready"),
            )
        self.assertFalse((progress_root / "000001.json").exists())

        for path in progress_root.iterdir():
            path.unlink()
        unknown = progress_root / ".000001.json.tmp-1-not-hex"
        unknown.write_bytes(b"")
        unknown.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "crash temporary"):
            sharding._protocol_sequence_inventory(
                progress_root,
                label="progress",
                deadline=None,
            )

        unknown.unlink()
        first = progress_root / f".000001.json.tmp-1-{'0' * 32}"
        first.write_bytes(b"")
        first.chmod(0o600)
        with mock.patch.object(
            sharding.time,
            "monotonic",
            side_effect=(0.0, 2.0),
        ):
            with self.assertRaises(TimeoutError):
                sharding._protocol_sequence_inventory(
                    progress_root,
                    label="progress",
                    deadline=1.0,
                )

    def test_progress_and_ack_directory_fsync_failures_remain_pending(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        for protocol in ("progress", "ACK"):
            for failed_boundary in range(1, 5):
                with self.subTest(
                    protocol=protocol,
                    failed_directory_fsync=failed_boundary,
                ):
                    run_root = (
                        self.root
                        / f"pending-{protocol.lower()}-{failed_boundary}"
                        / "run"
                    )
                    run_root.mkdir(parents=True, mode=0o700)
                    run_root.chmod(0o700)
                    worker_root = self._worker_root(run_root)
                    if protocol == "progress":
                        directory = worker_root / "progress"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_progress(
                            worker_root,
                            ready,
                        )
                        readers = (
                            lambda: sharding.read_progress(
                                directory / "000001.json",
                                "E1",
                                1,
                            ),
                            lambda: sharding.wait_for_progress(
                                worker_root=worker_root,
                                expected_lane="E1",
                                expected_seq=1,
                                timeout=0.01,
                            ),
                        )
                    else:
                        sharding.write_progress(worker_root, ready)
                        directory = worker_root / "acks"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_ack(
                            worker_root,
                            ready,
                            "continue",
                        )
                        readers = (
                            lambda: sharding.wait_for_ack(
                                worker_root,
                                ready,
                                0.01,
                            ),
                        )

                    directory_identity = (
                        directory.stat().st_dev,
                        directory.stat().st_ino,
                    )
                    real_fsync = sharding.os.fsync
                    directory_fsyncs = 0

                    def fail_selected_directory_fsync(descriptor):
                        nonlocal directory_fsyncs
                        metadata = os.fstat(descriptor)
                        if (
                            stat.S_ISDIR(metadata.st_mode)
                            and (metadata.st_dev, metadata.st_ino)
                            == directory_identity
                        ):
                            directory_fsyncs += 1
                            if directory_fsyncs == failed_boundary:
                                raise OSError(
                                    "injected protocol directory fsync failure"
                                )
                        return real_fsync(descriptor)

                    with mock.patch.object(
                        sharding.os,
                        "fsync",
                        side_effect=fail_selected_directory_fsync,
                    ):
                        with self.assertRaisesRegex(
                            OSError,
                            "protocol directory fsync failure",
                        ):
                            publish()

                    self.assertGreaterEqual(
                        directory_fsyncs,
                        failed_boundary,
                    )
                    pending = directory / ".000001.json.pending"
                    self.assertTrue(
                        pending.is_file(),
                        "failed publication did not leave a fail-closed marker",
                    )
                    for reader in readers:
                        with self.assertRaises(
                            (FileNotFoundError, TimeoutError, ValueError)
                        ):
                            reader()

                    final = directory / "000001.json"
                    if final.exists():
                        before = final.read_bytes()
                        with self.assertRaisesRegex(ValueError, "pending"):
                            publish()
                        self.assertEqual(before, final.read_bytes())

    def test_double_restoration_fsync_failures_require_durable_poison_or_committed_success(
        self,
    ):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        for protocol in ("progress", "ACK"):
            for failed_boundaries, reports_committed in (
                (frozenset((4, 5)), False),
                (frozenset((4, 5, 6)), True),
            ):
                with self.subTest(
                    protocol=protocol,
                    failed_boundaries=tuple(sorted(failed_boundaries)),
                ):
                    run_root = (
                        self.root
                        / (
                            "double-restoration-fsync-"
                            f"{protocol.lower()}-"
                            f"{len(failed_boundaries)}"
                        )
                        / "run"
                    )
                    run_root.mkdir(parents=True, mode=0o700)
                    run_root.chmod(0o700)
                    worker_root = self._worker_root(run_root)
                    if protocol == "progress":
                        directory = worker_root / "progress"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_progress(
                            worker_root,
                            ready,
                        )
                        read = lambda: sharding.read_progress(
                            directory / "000001.json",
                            "E1",
                            1,
                        )
                        process_script = """
import sys
from pathlib import Path
from scripts import workflow_eval_sharding as sharding
worker_root = Path(sys.argv[1])
sharding.read_progress(
    worker_root / "progress" / "000001.json", "E1", 1
)
"""
                    else:
                        sharding.write_progress(worker_root, ready)
                        directory = worker_root / "acks"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_ack(
                            worker_root,
                            ready,
                            "continue",
                        )
                        read = lambda: sharding.wait_for_ack(
                            worker_root,
                            ready,
                            0.01,
                        )
                        process_script = """
import sys
from pathlib import Path
from scripts import workflow_eval_sharding as sharding
worker_root = Path(sys.argv[1])
message = sharding.read_progress(
    worker_root / "progress" / "000001.json", "E1", 1
)
sharding.wait_for_ack(worker_root, message, 0.01)
"""

                    directory_identity = (
                        directory.stat().st_dev,
                        directory.stat().st_ino,
                    )
                    real_fsync = sharding.os.fsync
                    directory_fsyncs = 0

                    def fail_selected_directory_fsync(descriptor):
                        nonlocal directory_fsyncs
                        metadata = os.fstat(descriptor)
                        if (
                            stat.S_ISDIR(metadata.st_mode)
                            and (metadata.st_dev, metadata.st_ino)
                            == directory_identity
                        ):
                            directory_fsyncs += 1
                            if directory_fsyncs in failed_boundaries:
                                raise OSError(
                                    "injected restoration directory fsync failure"
                                )
                        return real_fsync(descriptor)

                    published = None
                    writer_error = None
                    with mock.patch.object(
                        sharding.os,
                        "fsync",
                        side_effect=fail_selected_directory_fsync,
                    ):
                        try:
                            published = publish()
                        except BaseException as error:
                            writer_error = error

                    self.assertEqual(6, directory_fsyncs)
                    final = directory / "000001.json"
                    pending = directory / ".000001.json.pending"
                    self.assertTrue(final.is_file())
                    self.assertTrue(pending.is_file())
                    if reports_committed:
                        self.assertEqual(final, published)
                        self.assertIsNone(writer_error)
                    else:
                        self.assertIsNone(published)
                        self.assertIsNotNone(writer_error)

                    with self.assertRaises((TimeoutError, ValueError)):
                        read()
                    process = subprocess.run(
                        [
                            sys.executable,
                            "-c",
                            process_script,
                            str(worker_root),
                        ],
                        cwd=Path(__file__).resolve().parents[1],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        check=False,
                    )
                    self.assertNotEqual(0, process.returncode)
                    self.assertRegex(
                        process.stderr,
                        "pending|timed out|TimeoutError|ValueError",
                    )
                    with self.assertRaisesRegex(ValueError, "pending"):
                        publish()

    def test_progress_and_ack_cleanup_failures_remain_pending(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        for protocol in ("progress", "ACK"):
            for cleanup_target in ("temporary", "pending"):
                with self.subTest(
                    protocol=protocol,
                    cleanup_target=cleanup_target,
                ):
                    run_root = (
                        self.root
                        / f"cleanup-{protocol.lower()}-{cleanup_target}"
                        / "run"
                    )
                    run_root.mkdir(parents=True, mode=0o700)
                    run_root.chmod(0o700)
                    worker_root = self._worker_root(run_root)
                    if protocol == "progress":
                        directory = worker_root / "progress"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_progress(
                            worker_root,
                            ready,
                        )
                        reader = lambda: sharding.read_progress(
                            directory / "000001.json",
                            "E1",
                            1,
                        )
                    else:
                        sharding.write_progress(worker_root, ready)
                        directory = worker_root / "acks"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_ack(
                            worker_root,
                            ready,
                            "continue",
                        )
                        reader = lambda: sharding.wait_for_ack(
                            worker_root,
                            ready,
                            0.01,
                        )

                    directory_identity = (
                        directory.stat().st_dev,
                        directory.stat().st_ino,
                    )
                    real_unlink = sharding.os.unlink

                    def fail_selected_cleanup(name, *args, **kwargs):
                        dir_fd = kwargs.get("dir_fd")
                        matches_directory = (
                            dir_fd is not None
                            and (
                                os.fstat(dir_fd).st_dev,
                                os.fstat(dir_fd).st_ino,
                            )
                            == directory_identity
                        )
                        matches_target = (
                            cleanup_target == "temporary"
                            and isinstance(name, str)
                            and ".tmp-" in name
                        ) or (
                            cleanup_target == "pending"
                            and name == ".000001.json.pending"
                        )
                        if matches_directory and matches_target:
                            raise OSError(
                                "injected protocol cleanup failure"
                            )
                        return real_unlink(name, *args, **kwargs)

                    with mock.patch.object(
                        sharding.os,
                        "unlink",
                        side_effect=fail_selected_cleanup,
                    ):
                        with self.assertRaises(BaseException):
                            publish()

                    self.assertTrue(
                        (directory / ".000001.json.pending").is_file()
                    )
                    self.assertTrue((directory / "000001.json").is_file())
                    with self.assertRaises(
                        (FileNotFoundError, TimeoutError, ValueError)
                    ):
                        reader()

    def test_pending_cleanup_window_is_serialized_from_readers(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        for protocol in ("progress", "ACK"):
            with self.subTest(protocol=protocol):
                run_root = (
                    self.root
                    / f"serialized-pending-cleanup-{protocol.lower()}"
                    / "run"
                )
                run_root.mkdir(parents=True, mode=0o700)
                run_root.chmod(0o700)
                worker_root = self._worker_root(run_root)
                if protocol == "progress":
                    directory = worker_root / "progress"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_progress(
                        worker_root,
                        ready,
                    )
                    read = lambda: sharding.read_progress(
                        directory / "000001.json",
                        "E1",
                        1,
                    )
                else:
                    sharding.write_progress(worker_root, ready)
                    directory = worker_root / "acks"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_ack(
                        worker_root,
                        ready,
                        "continue",
                    )
                    read = lambda: sharding.wait_for_ack(
                        worker_root,
                        ready,
                        1.0,
                    )

                directory_identity = (
                    directory.stat().st_dev,
                    directory.stat().st_ino,
                )
                real_fsync = sharding.os.fsync
                cleanup_fsync_entered = threading.Event()
                release_cleanup_fsync = threading.Event()
                directory_fsyncs = 0
                writer_errors = []
                reader_results = []
                reader_errors = []
                reader_finished = threading.Event()
                process_reader = None
                reader = None
                process_stdout = ""
                process_stderr = ""

                def pause_then_fail_cleanup_fsync(descriptor):
                    nonlocal directory_fsyncs
                    metadata = os.fstat(descriptor)
                    if (
                        stat.S_ISDIR(metadata.st_mode)
                        and (metadata.st_dev, metadata.st_ino)
                        == directory_identity
                    ):
                        directory_fsyncs += 1
                        if directory_fsyncs == 4:
                            cleanup_fsync_entered.set()
                            if not release_cleanup_fsync.wait(2.0):
                                raise AssertionError(
                                    "pending cleanup fsync was not released"
                                )
                            raise OSError(
                                "injected pending cleanup fsync failure"
                            )
                    return real_fsync(descriptor)

                def invoke_writer():
                    try:
                        publish()
                    except BaseException as error:
                        writer_errors.append(error)

                def invoke_reader():
                    try:
                        reader_results.append(read())
                    except BaseException as error:
                        reader_errors.append(error)
                    finally:
                        reader_finished.set()

                with mock.patch.object(
                    sharding.os,
                    "fsync",
                    side_effect=pause_then_fail_cleanup_fsync,
                ):
                    writer = threading.Thread(target=invoke_writer)
                    writer.start()
                    try:
                        self.assertTrue(cleanup_fsync_entered.wait(2.0))
                        self.assertTrue((directory / "000001.json").is_file())
                        self.assertFalse(
                            (directory / ".000001.json.pending").exists()
                        )
                        reader = threading.Thread(target=invoke_reader)
                        reader.start()
                        if protocol == "progress":
                            process_script = """
import sys
from pathlib import Path
from scripts import workflow_eval_sharding as sharding
sharding.read_progress(Path(sys.argv[1]), "E1", 1)
"""
                            process_argument = str(directory / "000001.json")
                        else:
                            process_script = """
import sys
from pathlib import Path
from scripts import workflow_eval_sharding as sharding
worker_root = Path(sys.argv[1])
message = sharding.read_progress(
    worker_root / "progress" / "000001.json", "E1", 1
)
sharding.wait_for_ack(worker_root, message, 0.05)
"""
                            process_argument = str(worker_root)
                        process_reader = subprocess.Popen(
                            [
                                sys.executable,
                                "-c",
                                process_script,
                                process_argument,
                            ],
                            cwd=Path(__file__).resolve().parents[1],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        )
                        self.assertFalse(
                            reader_finished.wait(0.05),
                            "reader bypassed the in-progress writer lock",
                        )
                        self.assertIsNone(
                            process_reader.poll(),
                            "process reader bypassed the in-progress writer lock",
                        )
                    finally:
                        release_cleanup_fsync.set()
                        writer.join(2.0)
                        if reader is not None:
                            reader.join(2.0)
                        if process_reader is not None:
                            try:
                                process_stdout, process_stderr = (
                                    process_reader.communicate(timeout=2.0)
                                )
                            except subprocess.TimeoutExpired:
                                process_reader.kill()
                                process_stdout, process_stderr = (
                                    process_reader.communicate(timeout=2.0)
                                )

                self.assertFalse(writer.is_alive())
                self.assertIsNotNone(reader)
                self.assertFalse(reader.is_alive())
                self.assertIsNotNone(process_reader)
                self.assertEqual(1, len(writer_errors))
                self.assertEqual([], reader_results)
                self.assertEqual(1, len(reader_errors))
                self.assertIsInstance(
                    reader_errors[0],
                    (TimeoutError, ValueError),
                )
                self.assertTrue(
                    (directory / ".000001.json.pending").is_file()
                )
                self.assertNotEqual(
                    0,
                    process_reader.returncode,
                    process_stdout,
                )
                self.assertRegex(
                    process_stderr,
                    "pending|timed out|ValueError",
                )

    def test_valid_ack_publication_window_is_serialized_from_waiter(self):
        run_root = self.root / "serialized-valid-ack-publication" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        ready = self._lane_message(seq=1, progress_type="lane-ready")
        sharding.write_progress(worker_root, ready)
        ack_directory = worker_root / "acks"
        ack_directory.mkdir(mode=0o700)
        pending = ack_directory / ".000001.json.pending"
        final = ack_directory / "000001.json"

        real_open_directory = sharding._open_protocol_record_directory
        real_write_record = sharding._write_protocol_record_retained
        real_lock_shared = sharding._lock_protocol_worker_shared
        waiter_at_ack_inventory = threading.Event()
        writer_in_publication_window = threading.Event()
        waiter_attempted_shared_lock = threading.Event()
        release_waiter = threading.Event()
        release_writer = threading.Event()
        waiter_finished = threading.Event()
        writer_results = []
        writer_errors = []
        waiter_results = []
        waiter_errors = []

        def pause_waiter_before_ack_inventory(
            worker,
            directory_name,
            **kwargs,
        ):
            capability = real_open_directory(
                worker,
                directory_name,
                **kwargs,
            )
            if (
                directory_name == "acks"
                and threading.current_thread().name == "ack-waiter"
            ):
                waiter_at_ack_inventory.set()
                if not release_waiter.wait(2.0):
                    raise AssertionError("ACK waiter gate was not released")
            return capability

        def pause_writer_with_pending_marker(
            directory,
            name,
            payload,
            *,
            byte_cap,
        ):
            if (
                directory.path == ack_directory
                and threading.current_thread().name == "ack-writer"
            ):
                self.assertTrue(pending.is_file())
                self.assertFalse(final.exists())
                writer_in_publication_window.set()
                if not release_writer.wait(2.0):
                    raise AssertionError("ACK writer gate was not released")
            return real_write_record(
                directory,
                name,
                payload,
                byte_cap=byte_cap,
            )

        def signal_waiter_shared_lock_attempt(worker, *, deadline):
            if (
                threading.current_thread().name == "ack-waiter"
                and writer_in_publication_window.is_set()
            ):
                waiter_attempted_shared_lock.set()
            return real_lock_shared(worker, deadline=deadline)

        def invoke_writer():
            try:
                writer_results.append(
                    sharding.write_ack(worker_root, ready, "continue")
                )
            except BaseException as error:
                writer_errors.append(error)

        def invoke_waiter():
            try:
                waiter_results.append(
                    sharding.wait_for_ack(worker_root, ready, 1.0)
                )
            except BaseException as error:
                waiter_errors.append(error)
            finally:
                waiter_finished.set()

        waiter_finished_during_publication = None
        with mock.patch.object(
            sharding,
            "_open_protocol_record_directory",
            side_effect=pause_waiter_before_ack_inventory,
        ), mock.patch.object(
            sharding,
            "_write_protocol_record_retained",
            side_effect=pause_writer_with_pending_marker,
        ), mock.patch.object(
            sharding,
            "_lock_protocol_worker_shared",
            side_effect=signal_waiter_shared_lock_attempt,
        ):
            waiter = threading.Thread(
                target=invoke_waiter,
                name="ack-waiter",
            )
            writer = threading.Thread(
                target=invoke_writer,
                name="ack-writer",
            )
            waiter.start()
            try:
                self.assertTrue(waiter_at_ack_inventory.wait(2.0))
                writer.start()
                self.assertTrue(writer_in_publication_window.wait(2.0))
                release_waiter.set()
                self.assertTrue(
                    waiter_attempted_shared_lock.wait(2.0),
                    "ACK waiter never attempted the shared worker lock",
                )
                waiter_finished_during_publication = waiter_finished.is_set()
            finally:
                release_waiter.set()
                release_writer.set()
                if writer.ident is not None:
                    writer.join(2.0)
                waiter.join(2.0)

        self.assertFalse(writer.is_alive())
        self.assertFalse(waiter.is_alive())
        self.assertFalse(
            waiter_finished_during_publication,
            "ACK waiter exposed a cooperating publisher's pending window",
        )
        self.assertEqual([], writer_errors)
        self.assertEqual([final], writer_results)
        self.assertEqual([], waiter_errors)
        self.assertEqual(1, len(waiter_results))
        self.assertEqual("continue", waiter_results[0].decision)

    def test_valid_progress_publication_window_is_serialized_from_waiter(self):
        run_root = self.root / "serialized-valid-progress-publication" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        ready = self._lane_message(seq=1, progress_type="lane-ready")
        progress_directory = worker_root / "progress"
        progress_directory.mkdir(mode=0o700)
        pending = progress_directory / ".000001.json.pending"
        final = progress_directory / "000001.json"

        real_write_record = sharding._write_protocol_record_retained
        real_lock_shared = sharding._lock_protocol_worker_shared
        writer_in_publication_window = threading.Event()
        waiter_attempted_shared_lock = threading.Event()
        release_writer = threading.Event()
        waiter_finished = threading.Event()
        writer_results = []
        writer_errors = []
        waiter_results = []
        waiter_errors = []

        def pause_writer_with_pending_marker(
            directory,
            name,
            payload,
            *,
            byte_cap,
        ):
            if (
                directory.path == progress_directory
                and threading.current_thread().name == "progress-writer"
            ):
                self.assertTrue(pending.is_file())
                self.assertFalse(final.exists())
                writer_in_publication_window.set()
                if not release_writer.wait(2.0):
                    raise AssertionError(
                        "progress writer gate was not released"
                    )
            return real_write_record(
                directory,
                name,
                payload,
                byte_cap=byte_cap,
            )

        def signal_waiter_shared_lock_attempt(worker, *, deadline):
            if threading.current_thread().name == "progress-waiter":
                waiter_attempted_shared_lock.set()
            return real_lock_shared(worker, deadline=deadline)

        def invoke_writer():
            try:
                writer_results.append(
                    sharding.write_progress(worker_root, ready)
                )
            except BaseException as error:
                writer_errors.append(error)

        def invoke_waiter():
            try:
                waiter_results.append(
                    sharding.wait_for_progress(
                        worker_root=worker_root,
                        expected_lane="E1",
                        expected_seq=1,
                        timeout=1.0,
                    )
                )
            except BaseException as error:
                waiter_errors.append(error)
            finally:
                waiter_finished.set()

        with mock.patch.object(
            sharding,
            "_write_protocol_record_retained",
            side_effect=pause_writer_with_pending_marker,
        ), mock.patch.object(
            sharding,
            "_lock_protocol_worker_shared",
            side_effect=signal_waiter_shared_lock_attempt,
        ):
            writer = threading.Thread(
                target=invoke_writer,
                name="progress-writer",
            )
            waiter = threading.Thread(
                target=invoke_waiter,
                name="progress-waiter",
            )
            writer.start()
            try:
                self.assertTrue(writer_in_publication_window.wait(2.0))
                waiter.start()
                self.assertTrue(
                    waiter_attempted_shared_lock.wait(2.0),
                    "progress waiter never attempted the shared worker lock",
                )
                self.assertFalse(
                    waiter_finished.is_set(),
                    "progress waiter exposed a cooperating publisher's "
                    "pending window",
                )
            finally:
                release_writer.set()
                if waiter.ident is not None:
                    waiter.join(2.0)
                writer.join(2.0)

        self.assertFalse(writer.is_alive())
        self.assertFalse(waiter.is_alive())
        self.assertEqual([], writer_errors)
        self.assertEqual([final], writer_results)
        self.assertEqual([], waiter_errors)
        self.assertEqual([ready], waiter_results)

    def test_ack_prefix_validation_does_not_reenter_shared_worker_lock(self):
        run_root = self.root / "single-depth-ack-prefix" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        first = self._lane_message(seq=1, progress_type="lane-ready")
        second = self._lane_message(seq=2, progress_type="worker-stopped")
        sharding.write_progress(worker_root, first)
        sharding.write_ack(worker_root, first, "continue")
        sharding.write_progress(worker_root, second)
        sharding.write_ack(worker_root, second, "abort")
        worker_identity = (
            worker_root.stat().st_dev,
            worker_root.stat().st_ino,
        )

        real_shared_lock = sharding._shared_protocol_worker_lock
        real_read_ack = (
            sharding._read_ack_for_progress_with_worker_lock_retained
        )
        lock_depths = {}
        lock_identities = []
        validated_ack_sequences = []
        maximum_lock_depth = 0

        @contextmanager
        def track_shared_lock(worker, *, operation_deadline):
            nonlocal maximum_lock_depth
            descriptor = worker._retained[-1].slot.descriptor
            metadata = os.fstat(descriptor)
            identity = (metadata.st_dev, metadata.st_ino)
            lock_identities.append(identity)
            with real_shared_lock(
                worker,
                operation_deadline=operation_deadline,
            ):
                depth = lock_depths.get(identity, 0) + 1
                lock_depths[identity] = depth
                maximum_lock_depth = max(maximum_lock_depth, depth)
                try:
                    yield
                finally:
                    lock_depths[identity] -= 1

        def track_ack_prefix(worker, acks, message):
            validated_ack_sequences.append(message.seq)
            return real_read_ack(worker, acks, message)

        with mock.patch.object(
            sharding,
            "_shared_protocol_worker_lock",
            new=track_shared_lock,
        ), mock.patch.object(
            sharding,
            "_read_ack_for_progress_with_worker_lock_retained",
            side_effect=track_ack_prefix,
        ):
            ack = sharding.wait_for_ack(worker_root, second, 1.0)

        self.assertEqual("abort", ack.decision)
        self.assertEqual([1, 2], validated_ack_sequences)
        self.assertEqual({worker_identity}, set(lock_identities))
        self.assertEqual(
            {},
            {
                key: value
                for key, value in lock_depths.items()
                if value
            },
        )
        self.assertEqual(1, maximum_lock_depth)

    def test_noncooperating_ack_inventory_mutation_remains_fail_closed(self):
        run_root = self.root / "noncooperating-ack-mutation" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        ready = self._lane_message(seq=1, progress_type="lane-ready")
        sharding.write_progress(worker_root, ready)
        sharding.write_ack(worker_root, ready, "continue")
        ack_directory = worker_root / "acks"
        ack_identity = (
            ack_directory.stat().st_dev,
            ack_directory.stat().st_ino,
        )

        real_scandir = sharding.os.scandir
        inventory_exhausted = threading.Event()
        release_inventory = threading.Event()
        reader_errors = []

        class PausedScandir:
            def __init__(self, entries):
                self.entries = entries

            def __enter__(self):
                self.entries.__enter__()
                return self

            def __exit__(self, *args):
                return self.entries.__exit__(*args)

            def __iter__(self):
                return self

            def __next__(self):
                try:
                    return next(self.entries)
                except StopIteration:
                    inventory_exhausted.set()
                    if not release_inventory.wait(2.0):
                        raise AssertionError(
                            "ACK inventory gate was not released"
                        )
                    raise

        def pause_ack_inventory(path):
            entries = real_scandir(path)
            if isinstance(path, int):
                metadata = os.fstat(path)
                if (metadata.st_dev, metadata.st_ino) == ack_identity:
                    return PausedScandir(entries)
            return entries

        def invoke_reader():
            try:
                sharding.wait_for_ack(worker_root, ready, 1.0)
            except BaseException as error:
                reader_errors.append(error)

        with mock.patch.object(
            sharding.os,
            "scandir",
            side_effect=pause_ack_inventory,
        ):
            reader = threading.Thread(target=invoke_reader)
            reader.start()
            try:
                self.assertTrue(inventory_exhausted.wait(2.0))
                foreign = ack_directory / "noncooperating-entry"
                foreign.write_bytes(b"")
                foreign.chmod(0o600)
            finally:
                release_inventory.set()
                reader.join(2.0)

        self.assertFalse(reader.is_alive())
        self.assertEqual(1, len(reader_errors))
        self.assertIsInstance(reader_errors[0], RuntimeError)
        self.assertEqual(
            "ACK inventory changed while scanning",
            str(reader_errors[0]),
        )

    def test_post_commit_failures_restore_pending_before_writer_raises(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        class InjectedPostCommitBase(BaseException):
            pass

        for protocol in ("progress", "ACK"):
            for failure_point in ("validation", "retirement"):
                with self.subTest(
                    protocol=protocol,
                    failure_point=failure_point,
                ):
                    run_root = (
                        self.root
                        / f"post-commit-{protocol.lower()}-{failure_point}"
                        / "run"
                    )
                    run_root.mkdir(parents=True, mode=0o700)
                    run_root.chmod(0o700)
                    worker_root = self._worker_root(run_root)
                    if protocol == "progress":
                        directory = worker_root / "progress"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_progress(
                            worker_root,
                            ready,
                        )
                        read = lambda: sharding.read_progress(
                            directory / "000001.json",
                            "E1",
                            1,
                        )
                    else:
                        sharding.write_progress(worker_root, ready)
                        directory = worker_root / "acks"
                        directory.mkdir(mode=0o700)
                        publish = lambda: sharding.write_ack(
                            worker_root,
                            ready,
                            "continue",
                        )
                        read = lambda: sharding.wait_for_ack(
                            worker_root,
                            ready,
                            0.01,
                        )

                    final = directory / "000001.json"
                    pending = directory / ".000001.json.pending"
                    if failure_point == "validation":
                        real_validate = (
                            sharding._RecordChildDirectoryCapability._validate_live
                        )
                        clear_validations = 0

                        def fail_second_clear_validation(capability):
                            nonlocal clear_validations
                            result = real_validate(capability)
                            if (
                                capability.path == directory
                                and final.exists()
                                and not pending.exists()
                            ):
                                clear_validations += 1
                                if clear_validations == 2:
                                    raise InjectedPostCommitBase(
                                        "injected post-commit validation failure"
                                    )
                            return result

                        patcher = mock.patch.object(
                            sharding._RecordChildDirectoryCapability,
                            "_validate_live",
                            new=fail_second_clear_validation,
                        )
                        expected_error = InjectedPostCommitBase
                    else:
                        real_close = (
                            sharding._RecordChildDirectoryCapability.close
                        )
                        injected = False

                        def fail_committed_retirement(
                            capability,
                            primary=None,
                        ):
                            nonlocal injected
                            if (
                                not injected
                                and capability.path == directory
                                and final.exists()
                                and not pending.exists()
                                and primary is None
                            ):
                                injected = True
                                real_close(capability, primary)
                                raise OSError(
                                    "injected post-commit retirement failure"
                                )
                            return real_close(capability, primary)

                        patcher = mock.patch.object(
                            sharding._RecordChildDirectoryCapability,
                            "close",
                            new=fail_committed_retirement,
                        )
                        expected_error = OSError

                    with patcher:
                        with self.assertRaises(expected_error):
                            publish()
                    self.assertTrue(final.is_file())
                    self.assertTrue(pending.is_file())
                    with self.assertRaises(
                        (FileNotFoundError, TimeoutError, ValueError)
                    ):
                        read()

    def test_final_worker_retirement_failure_reports_committed_success(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        for protocol in ("progress", "ACK"):
            with self.subTest(protocol=protocol):
                run_root = (
                    self.root
                    / f"committed-worker-retirement-{protocol.lower()}"
                    / "run"
                )
                run_root.mkdir(parents=True, mode=0o700)
                run_root.chmod(0o700)
                worker_root = self._worker_root(run_root)
                if protocol == "progress":
                    directory = worker_root / "progress"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_progress(
                        worker_root,
                        ready,
                    )
                    read = lambda path: sharding.read_progress(
                        path,
                        "E1",
                        1,
                    )
                else:
                    sharding.write_progress(worker_root, ready)
                    directory = worker_root / "acks"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_ack(
                        worker_root,
                        ready,
                        "continue",
                    )
                    read = lambda _path: sharding.wait_for_ack(
                        worker_root,
                        ready,
                        0.1,
                    )

                final = directory / "000001.json"
                pending = directory / ".000001.json.pending"
                real_close = sharding._RecordDirectoryCapability.close
                injected = False

                def fail_final_worker_retirement(capability, primary=None):
                    nonlocal injected
                    if (
                        not injected
                        and capability.path == worker_root
                        and final.exists()
                        and not pending.exists()
                        and primary is None
                    ):
                        injected = True
                        real_close(capability, primary)
                        raise OSError(
                            "injected final worker retirement failure"
                        )
                    return real_close(capability, primary)

                with mock.patch.object(
                    sharding._RecordDirectoryCapability,
                    "close",
                    new=fail_final_worker_retirement,
                ):
                    published = publish()

                self.assertTrue(injected)
                self.assertEqual(final, published)
                self.assertFalse(pending.exists())
                self.assertIsNotNone(read(published))

    def test_failed_pending_restoration_reports_committed_success(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        class InjectedPostCommitBase(BaseException):
            pass

        for protocol in ("progress", "ACK"):
            with self.subTest(protocol=protocol):
                run_root = (
                    self.root
                    / f"failed-pending-restoration-{protocol.lower()}"
                    / "run"
                )
                run_root.mkdir(parents=True, mode=0o700)
                run_root.chmod(0o700)
                worker_root = self._worker_root(run_root)
                if protocol == "progress":
                    directory = worker_root / "progress"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_progress(
                        worker_root,
                        ready,
                    )
                    read = lambda path: sharding.read_progress(
                        path,
                        "E1",
                        1,
                    )
                else:
                    sharding.write_progress(worker_root, ready)
                    directory = worker_root / "acks"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_ack(
                        worker_root,
                        ready,
                        "continue",
                    )
                    read = lambda _path: sharding.wait_for_ack(
                        worker_root,
                        ready,
                        0.1,
                    )

                final = directory / "000001.json"
                pending = directory / ".000001.json.pending"
                real_validate = (
                    sharding._RecordChildDirectoryCapability._validate_live
                )
                real_open_directory = sharding._open_protocol_record_directory
                clear_validations = 0
                post_commit_failure = False

                def fail_second_clear_validation(capability):
                    nonlocal clear_validations, post_commit_failure
                    result = real_validate(capability)
                    if (
                        capability.path == directory
                        and final.exists()
                        and not pending.exists()
                    ):
                        clear_validations += 1
                        if clear_validations == 2:
                            post_commit_failure = True
                            raise InjectedPostCommitBase(
                                "injected post-commit validation failure"
                            )
                    return result

                def fail_restoration_open(*args, **kwargs):
                    if post_commit_failure:
                        raise OSError(
                            "injected pending restoration open failure"
                        )
                    return real_open_directory(*args, **kwargs)

                with mock.patch.object(
                    sharding._RecordChildDirectoryCapability,
                    "_validate_live",
                    new=fail_second_clear_validation,
                ), mock.patch.object(
                    sharding,
                    "_open_protocol_record_directory",
                    side_effect=fail_restoration_open,
                ):
                    published = publish()

                self.assertTrue(post_commit_failure)
                self.assertEqual(final, published)
                self.assertFalse(pending.exists())
                self.assertIsNotNone(read(published))

    def test_async_during_pending_restoration_cannot_expose_writer_failure(self):
        ready = self._lane_message(seq=1, progress_type="lane-ready")

        class InjectedPostCommitBase(BaseException):
            pass

        for protocol in ("progress", "ACK"):
            with self.subTest(protocol=protocol):
                run_root = (
                    self.root
                    / f"async-pending-restoration-{protocol.lower()}"
                    / "run"
                )
                run_root.mkdir(parents=True, mode=0o700)
                run_root.chmod(0o700)
                worker_root = self._worker_root(run_root)
                if protocol == "progress":
                    directory = worker_root / "progress"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_progress(
                        worker_root,
                        ready,
                    )
                    read = lambda: sharding.read_progress(
                        directory / "000001.json",
                        "E1",
                        1,
                    )
                else:
                    sharding.write_progress(worker_root, ready)
                    directory = worker_root / "acks"
                    directory.mkdir(mode=0o700)
                    publish = lambda: sharding.write_ack(
                        worker_root,
                        ready,
                        "continue",
                    )
                    read = lambda: sharding.wait_for_ack(
                        worker_root,
                        ready,
                        0.01,
                    )

                final = directory / "000001.json"
                pending = directory / ".000001.json.pending"
                real_validate = (
                    sharding._RecordChildDirectoryCapability._validate_live
                )
                real_restore = (
                    sharding._restore_protocol_pending_after_failure_retained
                )
                clear_validations = 0

                def fail_second_clear_validation(capability):
                    nonlocal clear_validations
                    result = real_validate(capability)
                    if (
                        capability.path == directory
                        and final.exists()
                        and not pending.exists()
                    ):
                        clear_validations += 1
                        if clear_validations == 2:
                            raise InjectedPostCommitBase(
                                "injected post-commit validation failure"
                            )
                    return result

                def interrupt_after_restoration(*args, **kwargs):
                    self.assertEqual((), real_restore(*args, **kwargs))
                    raise InjectedPostCommitBase(
                        "interrupted after pending restoration"
                    )

                with mock.patch.object(
                    sharding._RecordChildDirectoryCapability,
                    "_validate_live",
                    new=fail_second_clear_validation,
                ), mock.patch.object(
                    sharding,
                    "_restore_protocol_pending_after_failure_retained",
                    side_effect=interrupt_after_restoration,
                ):
                    published = publish()

                self.assertEqual(final, published)
                self.assertTrue(pending.is_file())
                with self.assertRaises(
                    (FileNotFoundError, TimeoutError, ValueError)
                ):
                    read()

    def test_protocol_pending_marker_inventory_is_bounded(self):
        run_root = self.root / "pending-inventory" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        progress_root = self._worker_root(run_root) / "progress"
        progress_root.mkdir(mode=0o700)
        for sequence in range(1, sharding.MAX_PROTOCOL_RECORDS + 2):
            marker = progress_root / f".{sequence:06d}.json.pending"
            marker.write_bytes(b"")
            marker.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "pending.*cap"):
            sharding._protocol_sequence_inventory(
                progress_root,
                label="progress",
                deadline=None,
            )

    def _concurrent_publication_temp_peaks(self, directory, pattern, calls):
        real_open = sharding.os.open
        real_fsync = sharding.os.fsync
        directory_identity = (
            directory.stat().st_dev,
            directory.stat().st_ino,
        )
        tracked = set()
        tracked_lock = threading.Lock()
        boundary = threading.Barrier(2)
        peaks = []
        results = []
        failures = []

        def tracking_open(path, flags, *args, **kwargs):
            descriptor = real_open(path, flags, *args, **kwargs)
            dir_fd = kwargs.get("dir_fd")
            if (
                isinstance(path, str)
                and pattern.fullmatch(path) is not None
                and dir_fd is not None
                and (
                    os.fstat(dir_fd).st_dev,
                    os.fstat(dir_fd).st_ino,
                )
                == directory_identity
            ):
                with tracked_lock:
                    tracked.add(descriptor)
            return descriptor

        def observing_fsync(descriptor):
            with tracked_lock:
                is_temporary = descriptor in tracked
                if is_temporary:
                    tracked.remove(descriptor)
            if is_temporary:
                try:
                    boundary.wait(0.1)
                except threading.BrokenBarrierError:
                    pass
                peaks.append(
                    sum(
                        pattern.fullmatch(path.name) is not None
                        for path in directory.iterdir()
                    )
                )
            return real_fsync(descriptor)

        def publish(call):
            try:
                call()
                results.append("ok")
            except BaseException as error:
                failures.append(error)

        with mock.patch.object(
            sharding.os,
            "open",
            side_effect=tracking_open,
        ), mock.patch.object(
            sharding.os,
            "fsync",
            side_effect=observing_fsync,
        ):
            threads = [
                threading.Thread(target=publish, args=(call,))
                for call in calls
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(3.0)
                self.assertFalse(thread.is_alive())
        return results, failures, peaks

    def test_progress_ack_ledger_serializes_identity_and_token_state(self):
        formal = sharding.build_epoch_plan(
            run_kind="formal",
            manifests=self.manifests,
            fingerprints=input_fingerprints("formal"),
        )
        diagnostic = self._lane_message(
            seq=1,
            progress_type="lane-ready",
            lane="E1",
        )
        mixed = replace(
            diagnostic,
            epoch_id=formal.epoch_id,
            run_kind=formal.run_kind,
            lane="E2",
        )
        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        identity_boundary = threading.Barrier(2)
        real_canonical = sharding.canonical_config_bytes
        results = []
        failures = []

        def pause_after_identity_check(config):
            if (
                isinstance(config, dict)
                and set(config) == sharding.PROGRESS_FIELDS
                and config.get("seq") == 1
            ):
                try:
                    identity_boundary.wait(0.1)
                except threading.BrokenBarrierError:
                    pass
            return real_canonical(config)

        def accept(message):
            try:
                results.append((message, ledger.accept_progress(message)))
            except BaseException as error:
                failures.append(error)

        with mock.patch.object(
            sharding,
            "canonical_config_bytes",
            side_effect=pause_after_identity_check,
        ):
            first = threading.Thread(target=accept, args=(diagnostic,))
            second = threading.Thread(target=accept, args=(mixed,))
            first.start()
            second.start()
            first.join(2.0)
            second.join(2.0)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
        self.assertEqual(1, len(results))
        self.assertEqual(1, len(failures))
        self.assertIsInstance(failures[0], ValueError)

        winning = results[0][0]
        losing = mixed if winning == diagnostic else diagnostic
        self.assertEqual(
            "continue",
            ledger.accept_progress(
                replace(
                    losing,
                    epoch_id=winning.epoch_id,
                    run_kind=winning.run_kind,
                )
            ),
        )

        terminal_e1 = self._success_terminal_message(seq=2)
        terminal_e2 = replace(
            terminal_e1,
            lane="E2",
            case=dict(sharding.FROZEN_LANE_CASES)["E2"][0],
        )
        token_ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        token_ledger.accept_progress(
            self._case_started_for_terminal(terminal_e1, seq=1)
        )
        token_ledger.accept_progress(
            self._case_started_for_terminal(terminal_e2, seq=1)
        )
        token_boundary = threading.Barrier(2)

        class RacingTotal(int):
            def __add__(self, other):
                try:
                    token_boundary.wait(0.1)
                except threading.BrokenBarrierError:
                    pass
                return int(self) + other

        token_ledger._state = replace(
            token_ledger._state,
            total_tokens=RacingTotal(0),
        )
        token_results = []
        token_failures = []

        def accept_terminal(message):
            try:
                token_results.append(token_ledger.accept_progress(message))
            except BaseException as error:
                token_failures.append(error)

        first = threading.Thread(target=accept_terminal, args=(terminal_e1,))
        second = threading.Thread(target=accept_terminal, args=(terminal_e2,))
        first.start()
        second.start()
        first.join(2.0)
        second.join(2.0)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual([], token_failures)
        self.assertEqual(["continue", "continue"], sorted(token_results))
        self.assertEqual(2 * self.usage["total_tokens"], token_ledger.total_tokens)

    def test_concurrent_progress_and_ack_publishers_reserve_temp_capacity(self):
        sequence_pattern = re.compile(
            r"^\.[0-9]{6}\.json\.tmp-[0-9]+-[0-9a-f]{32}$"
        )

        progress_run = self.root / "reserved-progress" / "run"
        progress_run.mkdir(parents=True, mode=0o700)
        progress_run.chmod(0o700)
        progress_worker = self._worker_root(progress_run)
        sharding.write_progress(
            progress_worker,
            self._lane_message(seq=1, progress_type="lane-ready"),
        )
        progress_dir = progress_worker / "progress"
        for index in range(18):
            temporary = progress_dir / (
                f".000001.json.tmp-{index + 1}-{index:032x}"
            )
            temporary.write_bytes(b"")
            temporary.chmod(0o600)
        results, failures, peaks = self._concurrent_publication_temp_peaks(
            progress_dir,
            sequence_pattern,
            (
                lambda: sharding.write_progress(
                    progress_worker,
                    self._lane_message(seq=2, progress_type="lane-ready"),
                ),
                lambda: sharding.write_progress(
                    progress_worker,
                    self._lane_message(seq=3, progress_type="lane-ready"),
                ),
            ),
        )
        self.assertEqual(["ok", "ok"], sorted(results))
        self.assertEqual([], failures)
        self.assertTrue(peaks)
        self.assertLessEqual(max(peaks), sharding.MAX_PROTOCOL_CRASH_TEMPS)

        ack_run = self.root / "reserved-ack" / "run"
        ack_run.mkdir(parents=True, mode=0o700)
        ack_run.chmod(0o700)
        ack_worker = self._worker_root(ack_run)
        messages = tuple(
            self._lane_message(seq=seq, progress_type="lane-ready")
            for seq in (1, 2, 3)
        )
        for message in messages:
            sharding.write_progress(ack_worker, message)
        ack_dir = ack_worker / "acks"
        ack_dir.mkdir(mode=0o700)
        for index in range(18):
            temporary = ack_dir / (
                f".000001.json.tmp-{index + 1}-{index:032x}"
            )
            temporary.write_bytes(b"")
            temporary.chmod(0o600)
        results, failures, peaks = self._concurrent_publication_temp_peaks(
            ack_dir,
            sequence_pattern,
            (
                lambda: sharding.write_ack(
                    ack_worker,
                    messages[1],
                    "continue",
                ),
                lambda: sharding.write_ack(
                    ack_worker,
                    messages[2],
                    "continue",
                ),
            ),
        )
        self.assertEqual(["ok", "ok"], sorted(results))
        self.assertEqual([], failures)
        self.assertTrue(peaks)
        self.assertLessEqual(max(peaks), sharding.MAX_PROTOCOL_CRASH_TEMPS)

    def test_protocol_identity_temps_are_bounded_validated_and_idempotent(self):
        identity_pattern = re.compile(
            r"^\.protocol-identity\.json\.tmp-[0-9]+-[0-9a-f]{32}$"
        )
        self.assertEqual(19, sharding.MAX_PROTOCOL_IDENTITY_CRASH_TEMPS)

        overflow_run = self.root / "identity-overflow" / "run"
        overflow_run.mkdir(parents=True, mode=0o700)
        overflow_run.chmod(0o700)
        overflow_worker = self._worker_root(overflow_run)
        for index in range(20):
            temporary = overflow_worker / (
                f".protocol-identity.json.tmp-{index + 1}-{index:032x}"
            )
            temporary.write_bytes(b"")
            temporary.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "identity crash temporary"):
            sharding.write_progress(
                overflow_worker,
                self._lane_message(seq=1, progress_type="lane-ready"),
            )
        self.assertFalse(
            (overflow_worker / "protocol-identity.json").exists()
        )

        unknown_run = self.root / "identity-unknown" / "run"
        unknown_run.mkdir(parents=True, mode=0o700)
        unknown_run.chmod(0o700)
        unknown_worker = self._worker_root(unknown_run)
        unknown = unknown_worker / ".protocol-identity.json.tmp-1-not-hex"
        unknown.write_bytes(b"")
        unknown.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "identity crash temporary"):
            sharding.write_progress(
                unknown_worker,
                self._lane_message(seq=1, progress_type="lane-ready"),
            )

        idempotent_run = self.root / "identity-idempotent" / "run"
        idempotent_run.mkdir(parents=True, mode=0o700)
        idempotent_run.chmod(0o700)
        idempotent_worker = self._worker_root(idempotent_run)
        sharding.write_progress(
            idempotent_worker,
            self._lane_message(seq=1, progress_type="lane-ready"),
        )
        real_open = sharding.os.open

        def reject_identity_temp(path, flags, *args, **kwargs):
            if isinstance(path, str) and path.startswith(
                ".protocol-identity.json.tmp-"
            ):
                raise AssertionError("idempotent identity allocated a temp")
            return real_open(path, flags, *args, **kwargs)

        with mock.patch.object(
            sharding.os,
            "open",
            side_effect=reject_identity_temp,
        ):
            sharding.write_progress(
                idempotent_worker,
                self._lane_message(seq=2, progress_type="lane-ready"),
            )

        race_run = self.root / "identity-race" / "run"
        race_run.mkdir(parents=True, mode=0o700)
        race_run.chmod(0o700)
        race_worker = self._worker_root(race_run)
        for index in range(18):
            temporary = race_worker / (
                f".protocol-identity.json.tmp-{index + 1}-{index:032x}"
            )
            temporary.write_bytes(b"")
            temporary.chmod(0o600)
        results, failures, peaks = self._concurrent_publication_temp_peaks(
            race_worker,
            identity_pattern,
            (
                lambda: sharding.write_progress(
                    race_worker,
                    self._lane_message(seq=1, progress_type="lane-ready"),
                ),
                lambda: sharding.write_progress(
                    race_worker,
                    self._lane_message(seq=2, progress_type="lane-ready"),
                ),
            ),
        )
        self.assertEqual(["ok", "ok"], sorted(results))
        self.assertEqual([], failures)
        self.assertTrue(peaks)
        self.assertLessEqual(
            max(peaks),
            sharding.MAX_PROTOCOL_IDENTITY_CRASH_TEMPS,
        )

    def test_progress_retains_and_reconciles_the_complete_worker_hierarchy(self):
        mode_run = self.root / "workers-mode" / "run"
        mode_run.mkdir(parents=True, mode=0o700)
        mode_run.chmod(0o700)
        mode_worker = self._worker_root(mode_run)
        (mode_run / "workers").chmod(0o777)
        with self.assertRaises((PermissionError, ValueError)):
            sharding.write_progress(
                mode_worker,
                self._lane_message(seq=1, progress_type="lane-ready"),
            )

        symlink_run = self.root / "workers-symlink" / "run"
        symlink_run.mkdir(parents=True, mode=0o700)
        symlink_run.chmod(0o700)
        symlink_worker = self._worker_root(symlink_run)
        original_workers = symlink_run / "workers"
        moved_workers = symlink_run / "moved-workers"
        original_workers.rename(moved_workers)
        original_workers.symlink_to(moved_workers, target_is_directory=True)
        with self.assertRaises(ValueError):
            sharding.write_progress(
                symlink_worker,
                self._lane_message(seq=1, progress_type="lane-ready"),
            )

        replaced_run = self.root / "workers-replaced" / "run"
        replaced_run.mkdir(parents=True, mode=0o700)
        replaced_run.chmod(0o700)
        replaced_worker = self._worker_root(replaced_run)
        real_validate = sharding._validate_progress_durable
        replaced = False

        def replace_hierarchy(worker_root, message):
            nonlocal replaced
            real_validate(worker_root, message)
            if not replaced:
                replaced = True
                workers = replaced_run / "workers"
                workers.rename(replaced_run / "workers-old")
                workers.mkdir(mode=0o700)
                (workers / "E1").mkdir(mode=0o700)

        with mock.patch.object(
            sharding,
            "_validate_progress_durable",
            side_effect=replace_hierarchy,
        ):
            with self.assertRaises((RuntimeError, ValueError)):
                sharding.write_progress(
                    replaced_worker,
                    self._lane_message(seq=1, progress_type="lane-ready"),
                )
        self.assertFalse(
            (replaced_run / "workers" / "E1" / "progress" / "000001.json").exists()
        )
        self.assertFalse(
            (
                replaced_run
                / "workers-old"
                / "E1"
                / "progress"
                / "000001.json"
            ).exists()
        )

    def test_progress_ack_ledger_rejects_same_thread_transition_reentry(self):
        formal = sharding.build_epoch_plan(
            run_kind="formal",
            manifests=self.manifests,
            fingerprints=input_fingerprints("formal"),
        )
        outer = self._lane_message(
            seq=1,
            progress_type="lane-ready",
            lane="E1",
        )
        nested = replace(
            outer,
            epoch_id=formal.epoch_id,
            run_kind=formal.run_kind,
            lane="E2",
        )

        for boundary in ("encode", "canonical"):
            with self.subTest(boundary=boundary):
                ledger = sharding.ProgressAckLedger(max_total_tokens=None)
                nested_results = []
                nested_errors = []
                outer_results = []
                outer_errors = []
                invoked = False

                def reenter():
                    nonlocal invoked
                    if invoked:
                        return
                    invoked = True
                    try:
                        nested_results.append(ledger.accept_progress(nested))
                    except BaseException as error:
                        nested_errors.append(error)

                if boundary == "encode":
                    real_encode = sharding._encode_progress_message

                    def callback(message):
                        if message == outer:
                            reenter()
                        return real_encode(message)

                    patcher = mock.patch.object(
                        sharding,
                        "_encode_progress_message",
                        side_effect=callback,
                    )
                else:
                    real_canonical = sharding.canonical_config_bytes

                    def callback(config):
                        if (
                            isinstance(config, dict)
                            and config.get("lane") == outer.lane
                            and config.get("seq") == outer.seq
                            and set(config) == sharding.PROGRESS_FIELDS
                        ):
                            reenter()
                        return real_canonical(config)

                    patcher = mock.patch.object(
                        sharding,
                        "canonical_config_bytes",
                        side_effect=callback,
                    )

                with patcher:
                    try:
                        outer_results.append(ledger.accept_progress(outer))
                    except BaseException as error:
                        outer_errors.append(error)

                self.assertEqual(["continue"], outer_results)
                self.assertEqual([], outer_errors)
                self.assertEqual([], nested_results)
                self.assertEqual(1, len(nested_errors))
                self.assertIsInstance(nested_errors[0], RuntimeError)
                self.assertRegex(str(nested_errors[0]), "transition.*active")
                with self.assertRaisesRegex(
                    ValueError,
                    "protocol identity",
                ):
                    ledger.accept_progress(nested)
                self.assertEqual(
                    "continue",
                    ledger.accept_progress(
                        replace(
                            nested,
                            epoch_id=outer.epoch_id,
                            run_kind=outer.run_kind,
                        )
                    ),
                )
                self.assertEqual(0, ledger.total_tokens)

        exit_ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        exit_errors = []
        real_encode = sharding._encode_progress_message

        def reenter_worker_exit(message):
            if message == outer and not exit_errors:
                try:
                    exit_ledger.worker_exited("E2")
                except BaseException as error:
                    exit_errors.append(error)
            return real_encode(message)

        with mock.patch.object(
            sharding,
            "_encode_progress_message",
            side_effect=reenter_worker_exit,
        ):
            self.assertEqual("continue", exit_ledger.accept_progress(outer))
        self.assertEqual(1, len(exit_errors))
        self.assertIsInstance(exit_errors[0], RuntimeError)
        self.assertFalse(exit_ledger.aborted)
        self.assertEqual(
            "continue",
            exit_ledger.accept_progress(
                replace(outer, lane="E2")
            ),
        )

        cleanup_ledger = sharding.ProgressAckLedger(max_total_tokens=None)

        class InjectedTransitionBase(BaseException):
            pass

        self.assertFalse(hasattr(cleanup_ledger, "_transition_owner"))
        self.assertFalse(hasattr(cleanup_ledger, "_begin_transition_locked"))
        self.assertFalse(hasattr(cleanup_ledger, "_end_transition_locked"))
        with mock.patch.object(
            sharding,
            "canonical_config_bytes",
            side_effect=InjectedTransitionBase(
                "serialization callback failed"
            ),
        ):
            with self.assertRaisesRegex(
                InjectedTransitionBase,
                "serialization callback",
            ):
                cleanup_ledger.accept_progress(outer)
        self.assertEqual("continue", cleanup_ledger.accept_progress(outer))

        committed_ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        real_accept = committed_ledger._accept_progress_locked

        def interrupt_after_commit(message, payload):
            real_accept(message, payload)
            raise InjectedTransitionBase("interrupted after ledger commit")

        with mock.patch.object(
            committed_ledger,
            "_accept_progress_locked",
            side_effect=interrupt_after_commit,
        ):
            with self.assertRaisesRegex(
                InjectedTransitionBase,
                "after ledger commit",
            ):
                committed_ledger.accept_progress(outer)
        self.assertEqual("continue", committed_ledger.accept_progress(outer))

    def test_protocol_writer_lock_wait_is_bounded_and_retires_capabilities(self):
        from scripts import run_observing_workflows_eval_worker as worker

        ready = self._lane_message(seq=1, progress_type="lane-ready")
        scenarios = []

        progress_run = self.root / "lock-timeout-progress" / "run"
        progress_run.mkdir(parents=True, mode=0o700)
        progress_run.chmod(0o700)
        progress_worker = self._worker_root(progress_run)
        scenarios.append(
            (
                "progress",
                progress_worker,
                lambda: sharding.write_progress(progress_worker, ready),
                progress_worker / "progress" / "000001.json",
                0.4,
            )
        )

        ack_run = self.root / "lock-timeout-ack" / "run"
        ack_run.mkdir(parents=True, mode=0o700)
        ack_run.chmod(0o700)
        ack_worker = self._worker_root(ack_run)
        sharding.write_progress(ack_worker, ready)
        scenarios.append(
            (
                "ACK",
                ack_worker,
                lambda: sharding.write_ack(ack_worker, ready, "continue"),
                ack_worker / "acks" / "000001.json",
                0.4,
            )
        )

        helper_run = self.root / "lock-timeout-helper" / "run"
        helper_run.mkdir(parents=True, mode=0o700)
        helper_run.chmod(0o700)
        helper_worker = self._worker_root(helper_run)
        scenarios.append(
            (
                "worker helper",
                helper_worker,
                lambda: worker.publish_progress_and_wait_for_ack(
                    worker_root=helper_worker,
                    message=ready,
                    timeout=0.05,
                    wakeup_sink=lambda _wakeup: None,
                ),
                helper_worker / "progress" / "000001.json",
                0.15,
            )
        )

        observed = {}
        for label, worker_root, call, final_path, wait_seconds in scenarios:
            holder = os.open(
                worker_root,
                os.O_RDONLY
                | sharding._required_os_flag("O_DIRECTORY")
                | getattr(os, "O_CLOEXEC", 0),
            )
            sharding.fcntl.flock(
                holder,
                sharding.fcntl.LOCK_EX | sharding.fcntl.LOCK_NB,
            )
            captured = []
            results = []
            errors = []
            finished = threading.Event()
            real_open_worker = sharding._open_protocol_worker_directory

            def capture_worker(*args, **kwargs):
                capability = real_open_worker(*args, **kwargs)
                captured.append(capability)
                return capability

            def invoke():
                try:
                    results.append(call())
                except BaseException as error:
                    errors.append(error)
                finally:
                    finished.set()

            try:
                with mock.patch.object(
                    sharding,
                    "_open_protocol_worker_directory",
                    side_effect=capture_worker,
                ):
                    thread = threading.Thread(target=invoke)
                    thread.start()
                    completed_in_bound = finished.wait(wait_seconds)
                    sharding.fcntl.flock(holder, sharding.fcntl.LOCK_UN)
                    thread.join(2.0)
                    self.assertFalse(thread.is_alive())
            finally:
                os.close(holder)
            observed[label] = (
                completed_in_bound,
                results,
                errors,
                captured,
                final_path,
            )

        for label, (
            completed_in_bound,
            results,
            errors,
            captured,
            final_path,
        ) in observed.items():
            with self.subTest(label=label):
                self.assertTrue(
                    completed_in_bound,
                    f"{label} remained blocked past its lock deadline",
                )
                self.assertEqual([], results)
                self.assertEqual(1, len(errors))
                self.assertIsInstance(errors[0], TimeoutError)
                self.assertEqual(
                    "timed out acquiring protocol worker lock",
                    str(errors[0]),
                )
                self.assertFalse(final_path.exists())
                self.assertTrue(captured)
                for capability in captured:
                    self.assertTrue(capability._closed)
                    slots = (
                        capability._anchor_slot,
                        *(
                            entry.slot
                            for entry in capability._retained
                        ),
                    )
                    self.assertTrue(
                        all(
                            slot.descriptor_close_state == "closed"
                            for slot in slots
                        )
                    )

    def test_worker_wakeup_digest_survives_hierarchy_replacement_after_publish(self):
        from scripts import run_observing_workflows_eval_worker as worker

        run_root = self.root / "wakeup-replacement" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        message = self._lane_message(seq=1, progress_type="lane-ready")
        forged = self._lane_message(seq=1, progress_type="worker-stopped")
        expected_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(asdict(message))
        ).hexdigest()
        forged_sha256 = hashlib.sha256(
            sharding.canonical_config_bytes(asdict(forged))
        ).hexdigest()
        self.assertNotEqual(expected_sha256, forged_sha256)

        real_close = sharding._RecordDirectoryCapability.close
        replaced = False

        def close_then_replace(capability, primary=None):
            nonlocal replaced
            result = real_close(capability, primary)
            if not replaced and capability.path == worker_root:
                replaced = True
                workers = run_root / "workers"
                workers.rename(run_root / "workers-old")
                progress = workers / "E1" / "progress"
                progress.mkdir(parents=True, mode=0o700)
                workers.chmod(0o700)
                (workers / "E1").chmod(0o700)
                progress.chmod(0o700)
                sharding._atomic_write_record(
                    progress / "000001.json",
                    asdict(forged),
                )
            return result

        class WakeupCaptured(RuntimeError):
            pass

        wakeups = []

        def capture_wakeup(wakeup):
            wakeups.append(wakeup)
            raise WakeupCaptured("stop after wake-up")

        with mock.patch.object(
            sharding._RecordDirectoryCapability,
            "close",
            new=close_then_replace,
        ):
            with self.assertRaisesRegex(WakeupCaptured, "stop after wake-up"):
                worker.publish_progress_and_wait_for_ack(
                    worker_root=worker_root,
                    message=message,
                    timeout=1.0,
                    wakeup_sink=capture_wakeup,
                )

        self.assertTrue(replaced)
        self.assertEqual(
            [
                {
                    "lane": message.lane,
                    "seq": message.seq,
                    "sha256": expected_sha256,
                }
            ],
            wakeups,
        )
        self.assertEqual(
            forged_sha256,
            hashlib.sha256(
                (worker_root / "progress" / "000001.json").read_bytes()
            ).hexdigest(),
        )

    def test_acked_usage_is_bounded_idempotent_and_stops_future_launches(self):
        self.assertTrue(
            hasattr(sharding, "ProgressAckLedger"),
            "cumulative launch-ceiling accounting is absent",
        )
        first = self._success_terminal_message()
        second_key = dict(sharding.FROZEN_LANE_CASES)["E1"][1]
        second = replace(
            first,
            seq=4,
            case=second_key,
            usage=sharding.TokenUsage(
                input_tokens=4,
                cached_input_tokens=1,
                output_tokens=2,
                reasoning_output_tokens=1,
                total_tokens=6,
            ),
        )
        ledger = sharding.ProgressAckLedger(max_total_tokens=20)
        first_started = self._case_started_for_terminal(first, seq=1)
        first = replace(first, seq=2)
        second_started = self._case_started_for_terminal(second, seq=3)
        self.assertEqual("continue", ledger.accept_progress(first_started))
        self.assertEqual("continue", ledger.accept_progress(first))
        self.assertEqual(15, ledger.total_tokens)
        self.assertEqual("continue", ledger.accept_progress(first))
        self.assertEqual(15, ledger.total_tokens)
        with self.assertRaises(ValueError):
            ledger.accept_progress(
                replace(
                    first,
                    usage=sharding.TokenUsage(11, 2, 5, 1, 16),
                )
            )
        self.assertEqual("continue", ledger.accept_progress(second_started))
        self.assertEqual("stop-launches", ledger.accept_progress(second))
        self.assertEqual(21, ledger.total_tokens)

        e2_started = sharding.ProgressMessage(
            **{
                **asdict(self._lane_message(
                    seq=1, progress_type="case-started", lane="E2"
                )),
                "case": dict(sharding.FROZEN_LANE_CASES)["E2"][0],
                "attempt": 1,
            }
        )
        self.assertEqual("stop-launches", ledger.accept_progress(e2_started))
        self.assertEqual(21, ledger.total_tokens)
        self.assertFalse(hasattr(ledger, "monetary_cost"))

        for usage in (
            sharding.TokenUsage(-1, 0, 1, 0, 0),
            sharding.TokenUsage(sharding.MAX_TOKEN_COUNT + 1, 0, 0, 0, 0),
            sharding.TokenUsage(1, 2, 1, 0, 2),
            sharding.TokenUsage(1, 0, 1, 0, 3),
        ):
            with self.subTest(usage=usage):
                invalid_ledger = sharding.ProgressAckLedger(
                    max_total_tokens=None
                )
                invalid_ledger.accept_progress(first_started)
                with self.assertRaises((TypeError, ValueError)):
                    invalid_ledger.accept_progress(
                        replace(first, usage=usage)
                    )
        with self.assertRaises(ValueError):
            sharding.ProgressAckLedger(max_total_tokens=-1)
        large_ledger = sharding.ProgressAckLedger(
            max_total_tokens=sharding.MAX_TOKEN_COUNT + 1
        )
        large_usage = sharding.TokenUsage(
            sharding.MAX_TOKEN_COUNT,
            0,
            0,
            0,
            sharding.MAX_TOKEN_COUNT,
        )
        self.assertEqual(
            "continue",
            large_ledger.accept_progress(first_started),
        )
        self.assertEqual(
            "continue",
            large_ledger.accept_progress(replace(first, usage=large_usage)),
        )
        self.assertEqual(
            "continue",
            large_ledger.accept_progress(second_started),
        )
        self.assertEqual(
            "stop-launches",
            large_ledger.accept_progress(
                replace(
                    second,
                    case=second_key,
                    usage=large_usage,
                )
            ),
        )
        self.assertEqual(
            2 * sharding.MAX_TOKEN_COUNT,
            large_ledger.total_tokens,
        )

    def test_progress_polling_rejects_missing_or_invalid_prefix(self):
        def polling_root(name):
            run_root = self.root / name / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            return self._worker_root(run_root)

        worker_root = polling_root("missing-progress-prefix")
        second = self._lane_message(seq=2, progress_type="lane-ready")
        sharding.write_progress(worker_root, second)
        with self.assertRaises(ValueError):
            sharding.wait_for_progress(
                worker_root=worker_root,
                expected_lane="E1",
                expected_seq=2,
                timeout=0.1,
            )

        worker_root = polling_root("invalid-progress-prefix")
        first = self._lane_message(seq=1, progress_type="lane-ready")
        second = self._lane_message(seq=2, progress_type="lane-ready")
        first_path = sharding.write_progress(worker_root, first)
        sharding.write_progress(worker_root, second)
        first_payload = json.loads(first_path.read_text(encoding="ascii"))
        first_payload["prompt"] = "unsafe prefix"
        sharding._atomic_write_record(first_path, first_payload)
        with self.assertRaises(ValueError):
            sharding.wait_for_progress(
                worker_root=worker_root,
                expected_lane="E1",
                expected_seq=2,
                timeout=0.1,
            )

    def test_ack_polling_rejects_missing_or_invalid_prefix(self):
        def polling_root(name):
            run_root = self.root / name / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            return self._worker_root(run_root)

        worker_root = polling_root("missing-ack-prefix")
        first = self._lane_message(seq=1, progress_type="lane-ready")
        second = self._lane_message(seq=2, progress_type="lane-ready")
        sharding.write_progress(worker_root, first)
        sharding.write_progress(worker_root, second)
        sharding.write_ack(worker_root, second, "continue")
        with self.assertRaises(ValueError):
            sharding.wait_for_ack(worker_root, second, 0.1)

        worker_root = polling_root("invalid-ack-prefix")
        first_path = sharding.write_progress(worker_root, first)
        sharding.write_progress(worker_root, second)
        first_ack = sharding.write_ack(worker_root, first, "continue")
        sharding.write_ack(worker_root, second, "continue")
        first_ack_payload = json.loads(first_ack.read_text(encoding="ascii"))
        first_ack_payload["message_sha256"] = "f" * 64
        sharding._atomic_write_record(first_ack, first_ack_payload)
        with self.assertRaises(ValueError):
            sharding.wait_for_ack(worker_root, second, 0.1)
        self.assertTrue(first_path.exists())

    def test_exact_progress_read_rejects_lane_gap_and_unknown_future_final(
        self,
    ):
        def protocol_root(name):
            run_root = self.root / name / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            return self._worker_root(run_root)

        missing_prefix_root = protocol_root("exact-read-missing-prefix")
        missing_prefix = sharding.write_progress(
            missing_prefix_root,
            self._lane_message(seq=2, progress_type="lane-ready"),
        )
        with self.assertRaisesRegex(ValueError, "gapped|reordered"):
            sharding.read_progress(missing_prefix, "E1", 2)

        future_gap_root = protocol_root("exact-read-future-gap")
        first = self._lane_message(seq=1, progress_type="lane-ready")
        first_path = sharding.write_progress(future_gap_root, first)
        sharding.write_progress(
            future_gap_root,
            self._lane_message(seq=3, progress_type="worker-stopped"),
        )
        with self.assertRaisesRegex(ValueError, "gapped|reordered"):
            sharding.read_progress(first_path, "E1", 1)

        unknown_root = protocol_root("exact-read-unknown-future-final")
        first_path = sharding.write_progress(unknown_root, first)
        unknown = unknown_root / "progress" / "unexpected.json"
        unknown.write_bytes(b"{}")
        unknown.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "unsafe record"):
            sharding.read_progress(first_path, "E1", 1)

        for authority in ("read_progress", "write_ack"):
            with self.subTest(
                invalid_prior_record=True,
                authority=authority,
            ):
                invalid_prefix_root = protocol_root(
                    f"exact-read-invalid-prefix-{authority}"
                )
                first_path = sharding.write_progress(
                    invalid_prefix_root,
                    first,
                )
                sharding.write_ack(
                    invalid_prefix_root,
                    first,
                    "continue",
                )
                second = sharding.ProgressMessage(
                    **{
                        **asdict(
                            self._lane_message(
                                seq=2,
                                progress_type="case-started",
                            )
                        ),
                        "case": self.assignment.key,
                        "attempt": 1,
                    }
                )
                second_path = sharding.write_progress(
                    invalid_prefix_root,
                    second,
                )
                first_payload = json.loads(
                    first_path.read_text(encoding="ascii")
                )
                first_payload["prompt"] = "invalid durable prefix"
                sharding._atomic_write_record(first_path, first_payload)
                operations = {
                    "read_progress": lambda: sharding.read_progress(
                        second_path,
                        "E1",
                        2,
                    ),
                    "write_ack": lambda: sharding.write_ack(
                        invalid_prefix_root,
                        second,
                        "continue",
                    ),
                }
                with self.assertRaises(ValueError):
                    operations[authority]()

    def test_future_pending_marker_poisons_progress_and_ack_lane(self):
        def started():
            return sharding.ProgressMessage(
                **{
                    **asdict(
                        self._lane_message(
                            seq=1,
                            progress_type="case-started",
                        )
                    ),
                    "case": self.assignment.key,
                    "attempt": 1,
                }
            )

        def protocol_root(name):
            run_root = self.root / name / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            return self._worker_root(run_root)

        for authority in ("read_progress", "wait_for_progress", "write_ack"):
            with self.subTest(protocol="progress", authority=authority):
                worker_root = protocol_root(
                    f"future-progress-pending-{authority}"
                )
                first = started()
                first_path = sharding.write_progress(worker_root, first)
                sharding.write_progress(
                    worker_root,
                    self._lane_message(seq=2, progress_type="lane-ready"),
                )
                pending = (
                    worker_root / "progress" / ".000002.json.pending"
                )
                pending.write_bytes(b"")
                pending.chmod(0o600)
                operations = {
                    "read_progress": lambda: sharding.read_progress(
                        first_path,
                        "E1",
                        1,
                    ),
                    "wait_for_progress": lambda: sharding.wait_for_progress(
                        worker_root=worker_root,
                        expected_lane="E1",
                        expected_seq=1,
                        timeout=0.1,
                    ),
                    "write_ack": lambda: sharding.write_ack(
                        worker_root,
                        first,
                        "continue",
                    ),
                }
                with self.assertRaisesRegex(ValueError, "pending"):
                    operations[authority]()

        for authority in ("wait_for_ack", "write_ack"):
            with self.subTest(protocol="ACK", authority=authority):
                worker_root = protocol_root(f"future-ack-pending-{authority}")
                first = started()
                second = self._lane_message(
                    seq=2,
                    progress_type="lane-ready",
                )
                sharding.write_progress(worker_root, first)
                sharding.write_ack(worker_root, first, "continue")
                sharding.write_progress(worker_root, second)
                sharding.write_ack(worker_root, second, "continue")
                pending = worker_root / "acks" / ".000002.json.pending"
                pending.write_bytes(b"")
                pending.chmod(0o600)
                operations = {
                    "wait_for_ack": lambda: sharding.wait_for_ack(
                        worker_root,
                        first,
                        0.1,
                    ),
                    "write_ack": lambda: sharding.write_ack(
                        worker_root,
                        first,
                        "continue",
                    ),
                }
                with self.assertRaisesRegex(ValueError, "pending"):
                    operations[authority]()

    def test_protocol_lane_accepts_legitimate_prefix_and_same_seq_retry(self):
        run_root = self.root / "legitimate-protocol-prefix" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        worker_root = self._worker_root(run_root)
        first = self._lane_message(seq=1, progress_type="lane-ready")
        second = self._lane_message(seq=2, progress_type="worker-stopped")

        first_path = sharding.write_progress(worker_root, first)
        first_ack = sharding.write_ack(worker_root, first, "continue")
        second_path = sharding.write_progress(worker_root, second)
        second_ack = sharding.write_ack(worker_root, second, "abort")
        second_progress_bytes = second_path.read_bytes()
        second_ack_bytes = second_ack.read_bytes()

        self.assertEqual(first, sharding.read_progress(first_path, "E1", 1))
        self.assertEqual(second, sharding.read_progress(second_path, "E1", 2))
        self.assertEqual(
            second,
            sharding.wait_for_progress(
                worker_root=worker_root,
                expected_lane="E1",
                expected_seq=2,
                timeout=0.1,
            ),
        )
        self.assertEqual(
            "abort",
            sharding.wait_for_ack(worker_root, second, 0.1).decision,
        )
        self.assertEqual(
            second_path,
            sharding.write_progress(worker_root, second),
        )
        self.assertEqual(
            second_ack,
            sharding.write_ack(worker_root, second, "abort"),
        )
        self.assertEqual(second_progress_bytes, second_path.read_bytes())
        self.assertEqual(second_ack_bytes, second_ack.read_bytes())
        self.assertTrue(first_ack.is_file())

    def test_duplicate_gap_reorder_truncation_oversize_and_prompt_are_rejected(self):
        worker_root = self._worker_root()
        ready = self._lane_message(seq=1, progress_type="lane-ready")
        path = sharding.write_progress(worker_root, ready)
        original = path.read_bytes()
        self.assertEqual(path, sharding.write_progress(worker_root, ready))
        self.assertEqual(original, path.read_bytes())
        with self.assertRaises(ValueError):
            sharding.write_progress(
                worker_root,
                self._lane_message(seq=1, progress_type="worker-stopped"),
            )

        ack_path = sharding.write_ack(worker_root, ready, "continue")
        ack_bytes = ack_path.read_bytes()
        ack_payload = json.loads(ack_bytes.decode("ascii"))
        self.assertEqual(sharding.ACK_FIELDS, set(ack_payload))
        self.assertEqual(
            sharding.ACK_FIELDS,
            {field.name for field in fields(sharding.Ack)},
        )
        self.assertEqual(
            hashlib.sha256(original).hexdigest(),
            ack_payload["message_sha256"],
        )
        self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
        self.assertEqual(0o600, stat.S_IMODE(ack_path.stat().st_mode))
        self.assertEqual(0o700, stat.S_IMODE(path.parent.stat().st_mode))
        self.assertEqual(0o700, stat.S_IMODE(ack_path.parent.stat().st_mode))
        self.assertLessEqual(len(original), sharding.MAX_PROGRESS_BYTES)
        self.assertLessEqual(len(ack_bytes), sharding.MAX_PROGRESS_BYTES)
        self.assertEqual(
            ack_path, sharding.write_ack(worker_root, ready, "continue")
        )
        self.assertEqual(ack_bytes, ack_path.read_bytes())
        self.assertEqual(
            "continue",
            sharding.wait_for_ack(worker_root, ready, 0.1).decision,
        )
        with self.assertRaises(ValueError):
            sharding.write_ack(worker_root, ready, "abort")

        def protocol_root(name):
            run_root = self.root / name / "run"
            run_root.mkdir(parents=True, mode=0o700)
            run_root.chmod(0o700)
            return self._worker_root(run_root)

        truncated_root = protocol_root("truncated")
        truncated = truncated_root / "progress"
        truncated.mkdir(mode=0o700)
        truncated_path = truncated / "000001.json"
        truncated_path.write_bytes(b'{"schema_version":1')
        truncated_path.chmod(0o600)
        with self.assertRaises(ValueError):
            sharding.read_progress(truncated_path, "E1", 1)

        oversized_root = protocol_root("oversized")
        oversized = oversized_root / "progress"
        oversized.mkdir(mode=0o700)
        oversized_path = oversized / "000001.json"
        oversized_path.write_bytes(b"x" * (sharding.MAX_PROGRESS_BYTES + 1))
        oversized_path.chmod(0o600)
        with self.assertRaises(ValueError):
            sharding.read_progress(oversized_path, "E1", 1)

        prompt_root = protocol_root("prompt")
        prompt_message = self._lane_message(seq=1, progress_type="lane-ready")
        prompt_path = sharding.write_progress(prompt_root, prompt_message)
        prompt_payload = json.loads(prompt_path.read_text(encoding="ascii"))
        prompt_payload["prompt"] = "secret model prompt"
        sharding._atomic_write_record(prompt_path, prompt_payload)
        with self.assertRaises(ValueError):
            sharding.read_progress(prompt_path, "E1", 1)

        unsafe_root = protocol_root("unsafe-string")
        unsafe_message = self._lane_message(seq=1, progress_type="lane-ready")
        unsafe_payload = asdict(unsafe_message)
        unsafe_payload.update(
            {
                "type": "case-started",
                "case": {
                    "mode": "forward",
                    "ordinal": 1,
                    "case_id": "p" * (sharding.MAX_PROGRESS_STRING_CHARS + 1),
                },
                "attempt": 1,
            }
        )
        unsafe_progress_root = unsafe_root / "progress"
        unsafe_progress_root.mkdir(mode=0o700)
        unsafe_path = unsafe_progress_root / "000001.json"
        sharding._atomic_write_record(unsafe_path, unsafe_payload)
        with self.assertRaises(ValueError):
            sharding.read_progress(unsafe_path, "E1", 1)

        forged_ack_root = protocol_root("forged-ack")
        forged_ready = self._lane_message(seq=1, progress_type="lane-ready")
        sharding.write_progress(forged_ack_root, forged_ready)
        forged_ack_dir = forged_ack_root / "acks"
        forged_ack_dir.mkdir(mode=0o700)
        forged_ack_path = forged_ack_dir / "000001.json"
        forged_ack_payload = {
            "schema_version": 1,
            "epoch_id": self.plan.epoch_id,
            "run_kind": self.plan.run_kind,
            "lane": "E1",
            "seq": 1,
            "message_sha256": "f" * 64,
            "decision": "continue",
        }
        sharding._atomic_write_record(forged_ack_path, forged_ack_payload)
        with self.assertRaises(ValueError):
            sharding.wait_for_ack(forged_ack_root, forged_ready, 0.1)

        gap_root = protocol_root("gap")
        sharding.write_progress(
            gap_root,
            self._lane_message(seq=2, progress_type="lane-ready"),
        )
        with self.assertRaises(ValueError):
            sharding.wait_for_progress(
                worker_root=gap_root,
                expected_lane="E1",
                expected_seq=1,
                timeout=0.1,
            )
        with self.assertRaises(ValueError):
            sharding.read_progress(
                gap_root / "progress" / "000002.json",
                "E1",
                1,
            )

    def test_forged_and_cross_case_seal_hashes_are_rejected(self):
        worker_root = self._worker_root()
        first = self._success_terminal_message()
        second_assignment = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E1"
        )[1]
        second_paths, second_terminal = self._prepare_lane_terminal(
            self.run_root, second_assignment
        )
        second_attempt = sharding.read_attempt_seal(
            plan=self.plan,
            paths=second_paths,
            assignment=second_assignment,
            attempt=1,
            manifest_case=self._manifest_for_assignment(second_assignment),
        )
        cross_case = (
            replace(
                first,
                attempt_terminal_sha256=second_attempt.terminal_sha256,
            ),
            replace(
                first,
                case_commit_sha256=second_terminal.case_commit_sha256,
            ),
            replace(
                first,
                tombstone_receipt_sha256=(
                    second_terminal.tombstone_receipt_sha256
                ),
            ),
        )
        forged = tuple(
            replace(first, **{field_name: "f" * 64})
            for field_name in (
                "attempt_terminal_sha256",
                "case_commit_sha256",
                "tombstone_receipt_sha256",
            )
        )
        for message in (*cross_case, *forged):
            with self.subTest(message=message):
                with self.assertRaises(ValueError):
                    sharding.write_progress(worker_root, message)
        sharding.write_progress(worker_root, first)

        run_root = self.root / "forged-shard" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        shard_root = self._worker_root(run_root)
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E1"
        )
        terminals = []
        case_paths = {}
        for assignment in assignments:
            paths, terminal = self._prepare_lane_terminal(
                run_root, assignment
            )
            terminals.append(terminal)
            case_paths[assignment.key] = paths
        sharding.seal_shard(
            worker_root=shard_root,
            plan=self.plan,
            lane="E1",
            terminals=terminals,
            manifests=self.manifests,
            case_paths=case_paths,
        )
        shard = sharding.read_shard_seal(
            worker_root=shard_root,
            plan=self.plan,
            lane="E1",
            manifests=self.manifests,
            case_paths=case_paths,
        )
        shard_message = sharding.ProgressMessage(
            **{
                **asdict(self._lane_message(
                    seq=1, progress_type="shard-terminal"
                )),
                "status": "success",
                "shard_commit_sha256": shard.commit_sha256,
            }
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(
                shard_root,
                replace(shard_message, shard_commit_sha256="f" * 64),
            )
        cross_run_root = self.root / "cross-shard" / "run"
        cross_run_root.mkdir(parents=True, mode=0o700)
        cross_run_root.chmod(0o700)
        (cross_run_root / "cases").mkdir(mode=0o700)
        cross_shard_root = self._worker_root(cross_run_root, lane="E3")
        cross_assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E3"
        )
        cross_terminals = []
        cross_paths = {}
        for assignment in cross_assignments:
            paths, terminal = self._prepare_lane_terminal(
                cross_run_root, assignment
            )
            cross_terminals.append(terminal)
            cross_paths[assignment.key] = paths
        sharding.seal_shard(
            worker_root=cross_shard_root,
            plan=self.plan,
            lane="E3",
            terminals=cross_terminals,
            manifests=self.manifests,
            case_paths=cross_paths,
        )
        cross_shard = sharding.read_shard_seal(
            worker_root=cross_shard_root,
            plan=self.plan,
            lane="E3",
            manifests=self.manifests,
            case_paths=cross_paths,
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(
                shard_root,
                replace(
                    shard_message,
                    shard_commit_sha256=cross_shard.commit_sha256,
                ),
            )
        sharding.write_progress(shard_root, shard_message)

    def test_forged_durable_case_and_shard_commits_are_rejected(self):
        worker_root = self._worker_root()
        message = self._success_terminal_message()
        case_commit_path = self.paths.sealed / "case-commit.json"
        case_commit = json.loads(case_commit_path.read_text(encoding="ascii"))
        case_commit["attempt_start_sha256"] = "f" * 64
        forged_case_content = sharding._atomic_write_record(
            case_commit_path, case_commit
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(
                worker_root,
                replace(
                    message,
                    case_commit_sha256=hashlib.sha256(
                        forged_case_content
                    ).hexdigest(),
                ),
            )
        case_commit["attempt_start_sha256"] = json.loads(
            self.paths.attempts.joinpath("01", "terminal.json").read_text(
                encoding="ascii"
            )
        )["start_sha256"]
        case_commit["evidence_sha256"] = "f" * 64
        forged_evidence_binding = sharding._atomic_write_record(
            case_commit_path, case_commit
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(
                worker_root,
                replace(
                    message,
                    case_commit_sha256=hashlib.sha256(
                        forged_evidence_binding
                    ).hexdigest(),
                ),
            )

        run_root = self.root / "forged-durable-shard" / "run"
        run_root.mkdir(parents=True, mode=0o700)
        run_root.chmod(0o700)
        (run_root / "cases").mkdir(mode=0o700)
        shard_root = self._worker_root(run_root)
        assignments = tuple(
            assignment
            for assignment in self.plan.assignments
            if assignment.lane == "E1"
        )
        terminals = []
        case_paths = {}
        for assignment in assignments:
            paths, terminal = self._prepare_lane_terminal(
                run_root, assignment
            )
            terminals.append(terminal)
            case_paths[assignment.key] = paths
        shard_path = sharding.seal_shard(
            worker_root=shard_root,
            plan=self.plan,
            lane="E1",
            terminals=terminals,
            manifests=self.manifests,
            case_paths=case_paths,
        )
        shard_payload = json.loads(shard_path.read_text(encoding="ascii"))
        shard_payload["terminals"][0]["attempt_terminal_sha256"] = "f" * 64
        forged_shard_content = sharding._atomic_write_record(
            shard_path, shard_payload
        )
        shard_message = sharding.ProgressMessage(
            **{
                **asdict(self._lane_message(
                    seq=1, progress_type="shard-terminal"
                )),
                "status": "success",
                "shard_commit_sha256": hashlib.sha256(
                    forged_shard_content
                ).hexdigest(),
            }
        )
        with self.assertRaises(ValueError):
            sharding.write_progress(shard_root, shard_message)

    def test_case_started_ack_barrier_and_worker_exit_before_terminal_abort(self):
        self.assertTrue(
            hasattr(sharding, "ProgressAckLedger"),
            "worker-exit protocol decision is absent",
        )
        from scripts import run_observing_workflows_eval_worker as worker

        worker_root = self._worker_root()
        started = sharding.ProgressMessage(
            **{
                **asdict(self._lane_message(
                    seq=1, progress_type="case-started"
                )),
                "case": self.assignment.key,
                "attempt": 1,
            }
        )
        model_started = threading.Event()
        failures = []

        def await_launch_permission():
            try:
                ack = worker.publish_progress_and_wait_for_ack(
                    worker_root=worker_root,
                    message=started,
                    timeout=2.0,
                    wakeup_sink=lambda _wakeup: None,
                )
                if ack.decision == "continue":
                    model_started.set()
            except BaseException as error:
                failures.append(error)

        thread = threading.Thread(target=await_launch_permission)
        thread.start()
        observed = sharding.wait_for_progress(
            worker_root=worker_root,
            expected_lane="E1",
            expected_seq=1,
            timeout=2.0,
        )
        self.assertFalse(model_started.wait(0.05))
        sharding.write_ack(worker_root, observed, "abort")
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], failures)
        self.assertFalse(model_started.is_set())

        ledger = sharding.ProgressAckLedger(max_total_tokens=None)
        self.assertEqual("continue", ledger.accept_progress(started))
        self.assertEqual("abort", ledger.worker_exited("E1"))
        self.assertEqual(0, ledger.total_tokens)

    def test_transport_and_progress_share_one_token_usage_type(self):
        from scripts import run_observing_workflows_task9_eval as task9_eval

        self.assertIs(
            sharding.TokenUsage,
            task9_eval.TokenUsage,
            "transport and progress must use one bounded TokenUsage type",
        )


class CoordinatorStateTests(unittest.TestCase):
    class FakeReader:
        def __init__(self, events, label):
            self.events = events
            self.label = label
            self.joined = False

        def join(self, timeout=None):
            self.events.append(("reader-joined", self.label))
            self.joined = True

        def is_alive(self):
            return not self.joined

    class FakeProcess:
        def __init__(self, lane, root, events, pid):
            self.lane = lane
            self.pid = pid
            self.pgid = pid + 1000
            self.returncode = None
            self.events = events
            self.worker_root = root
            self._coordinator_readers = (
                CoordinatorStateTests.FakeReader(events, lane),
            )

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.events.append(("process-waited", self.lane))
            self.returncode = -signal.SIGTERM
            return self.returncode

        def terminate(self):
            raise AssertionError("coordinator must cancel the process group")

        def kill(self):
            raise AssertionError("coordinator must kill the process group")

    class FakeLedger:
        def __init__(self, malformed_lane=None, decisions=None):
            self.malformed_lane = malformed_lane
            self.decisions = {} if decisions is None else dict(decisions)

        def accept_progress(self, message):
            if message.lane == self.malformed_lane:
                raise ValueError("malformed lane progress")
            return self.decisions.get(message.lane, "continue")

        def worker_exited(self, _lane):
            return "abort"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.root = Path(self.temporary.name).resolve(strict=True)
        self.repository = (self.root / "repository").resolve()
        subprocess.run(
            ["git", "init", "-q", str(self.repository)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.manifests = {
            "forward": load_cases("observing_workflows_cases.json"),
            "lifecycle": load_cases(
                "observing_workflows_lifecycle_cases.json"
            ),
        }
        self.plan = sharding.build_epoch_plan(
            run_kind="discovery",
            manifests=self.manifests,
            fingerprints=input_fingerprints("discovery"),
        )
        self.run_root = self.root / "run"
        self.source_codex_home = self.root / "source-codex-home"
        self.source_codex_home.mkdir(mode=0o700)
        (self.source_codex_home / "auth.json").write_bytes(b'{"token":"secret"}')
        (self.source_codex_home / "auth.json").chmod(0o600)
        self.codex_executable = self.root / "codex"
        self.codex_executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        self.codex_executable.chmod(0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _require_state_machine(self):
        self.assertTrue(
            all(
                hasattr(sharding, name)
                for name in (
                    "CoordinatorPhase",
                    "QuiescentRunAuthority",
                    "CoordinatorStateMachine",
                    "ParallelOptions",
                    "WorkerDependencies",
                    "CoordinatorDependencies",
                    "ParallelRunResult",
                    "build_production_case_driver",
                    "build_production_runtime_factory",
                    "production_worker_dependencies",
                    "production_coordinator_dependencies",
                    "run_worker",
                    "worker_main",
                    "run_parallel_evaluation",
                )
            ),
            "coordinator state machine is absent",
        )

    def _options(self, *, run_kind="discovery", max_total_tokens=None):
        self._require_state_machine()
        return sharding.ParallelOptions(
            run_kind=run_kind,
            run_root=self.run_root,
            source_codex_home=self.source_codex_home,
            codex_executable=self.codex_executable,
            max_total_tokens=max_total_tokens,
        )

    def _new_machine(self, *, plan=None, options=None):
        self._require_state_machine()
        plan = self.plan if plan is None else plan
        options = self._options() if options is None else options
        guard = sharding.CoordinatorGuard.capture(self.repository)
        return sharding.CoordinatorStateMachine.create(
            plan, options, guard
        ), guard

    def _lane_message(self, lane, *, seq=1, progress_type="shard-terminal"):
        status = "success" if progress_type == "shard-terminal" else None
        return sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane=lane,
            seq=seq,
            type=progress_type,
            case=None,
            attempt=None,
            status=status,
            classification=None,
            model_started=None,
            usage=None,
            attempt_terminal_sha256=None,
            case_commit_sha256=None,
            shard_commit_sha256=(
                hashlib.sha256(lane.encode("ascii")).hexdigest()
                if progress_type == "shard-terminal"
                else None
            ),
            tombstone_receipt_sha256=None,
        )

    def _register_fake_lanes(self, machine, events):
        processes = {}
        for index, lane in enumerate(("E1", "E2", "E3", "APP"), start=1):
            worker_root = (
                self.run_root / "app-server"
                if lane == "APP"
                else self.run_root / "workers" / lane
            )
            process = self.FakeProcess(
                lane, worker_root, events, pid=4100 + index
            )
            processes[lane] = process
            machine.register_worker(lane, process)
        return processes

    def _prepare_case_and_bootstrap(self):
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        bootstrap = sharding.prepare_auth_bootstrap(
            source_codex_home=self.source_codex_home,
            coordinator_root=self.run_root / "coordinator",
            plan=self.plan,
        )
        assignment = self.plan.assignments[0]
        paths = sharding.paths_for_case(self.run_root, assignment)
        for path in (
            paths.root,
            paths.cleanup,
            paths.attempts,
            paths.staging,
            paths.workspace.parent,
            paths.store,
            paths.audit,
            paths.payload,
            paths.output,
            paths.home,
            paths.tmp,
            paths.config,
            paths.cache,
            paths.sealed,
        ):
            path.mkdir(parents=True, mode=0o700, exist_ok=True)
            path.chmod(0o700)
        installed = sharding.install_case_auth(
            bootstrap=bootstrap.path,
            plan=self.plan,
            assignment=assignment,
            paths=paths,
        )
        evidence = paths.sealed / "retained-evidence.txt"
        evidence.write_text("retain me", encoding="utf-8")
        evidence.chmod(0o600)
        return lease, bootstrap, assignment, paths, installed, evidence

    def test_quiescent_authority_requires_live_bound_run_lease(self):
        machine, _guard = self._new_machine()
        with self.assertRaisesRegex(RuntimeError, "live run lease"):
            machine.workers_stopped()

        stale_lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        machine._bind_run_lease(stale_lease)
        stale_lease.close()
        with self.assertRaises(RuntimeError):
            machine.workers_stopped()

        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            self.assertIs(authority._lease, lease)
            lease.close()
            with self.assertRaises(RuntimeError):
                machine.begin_teardown()
        finally:
            if lease.active:
                lease.close()

    def test_seed_resume_protocol_preserves_exact_replayed_ledger(self):
        ledger = sharding.ProgressAckLedger(max_total_tokens=0)
        message = self._lane_message("E1", progress_type="lane-ready")
        self.assertEqual("stop-launches", ledger.accept_progress(message))
        stopped = self._lane_message(
            "E1", seq=2, progress_type="worker-stopped"
        )
        self.assertEqual("stop-launches", ledger.accept_progress(stopped))
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            machine, _guard = self._new_machine(
                options=self._options(max_total_tokens=0)
            )
            machine._bind_run_lease(lease)
            machine.workers_stopped()
            machine._seed_resume_protocol(ledger=ledger)
            self.assertIs(machine._ledger, ledger)
            self.assertTrue(machine.stop_launches)
            self.assertIn("E1", machine._ledger.exited)
        finally:
            if lease.active:
                lease.close()

    def test_retained_proof_retirement_never_deletes_replacement(self):
        relative_proofs = (
            PurePosixPath("coordinator/teardown.json"),
            PurePosixPath("coordinator/stop-launches.json"),
            PurePosixPath(
                "coordinator/cleanup/bootstrap-ownership.json"
            ),
            PurePosixPath(
                "coordinator/cleanup/bootstrap-tombstone.json"
            ),
        )
        replacement = b'{"proof":"replacement"}'
        for index, relative in enumerate(relative_proofs):
            with self.subTest(name=relative.name):
                self._prepare_retirement_crash_scenario(
                    f"retirement-replacement-{index}"
                )
                path = self.run_root / relative
                lease, authority = self._fresh_retirement_authority()
                real_rename = sharding._rename_exclusive_at
                injected = False

                def replace_before_rename(
                    *,
                    source_slot,
                    source_name,
                    destination_slot,
                    destination_name,
                ):
                    nonlocal injected
                    if not injected and source_name == relative.name:
                        named = os.stat(
                            source_name,
                            dir_fd=source_slot.descriptor,
                            follow_symlinks=False,
                        )
                        if (
                            named.st_dev,
                            named.st_ino,
                        ) == (
                            path.stat().st_dev,
                            path.stat().st_ino,
                        ):
                            injected = True
                            path.unlink()
                            path.write_bytes(replacement)
                            path.chmod(0o600)
                    return real_rename(
                        source_slot=source_slot,
                        source_name=source_name,
                        destination_slot=destination_slot,
                        destination_name=destination_name,
                    )

                try:
                    with mock.patch.object(
                        sharding,
                        "_rename_exclusive_at",
                        side_effect=replace_before_rename,
                    ):
                        with self.assertRaisesRegex(
                            ValueError, "changed during move"
                        ):
                            sharding._rearm_bootstrap_before_resume(
                                plan=self.plan,
                                coordinator_root=(
                                    self.run_root / "coordinator"
                                ),
                                source_codex_home=self.source_codex_home,
                                lease=lease,
                                authority=authority,
                            )
                    self.assertEqual(replacement, path.read_bytes())
                finally:
                    if lease.active:
                        lease.close()

    def test_retained_proof_retirement_has_no_final_unlink_window(self):
        expected = self._prepare_retirement_crash_scenario(
            "retirement-no-final-unlink"
        )
        lease, authority = self._fresh_retirement_authority()
        bootstrap = None
        try:
            with mock.patch.object(
                sharding.os,
                "unlink",
                side_effect=AssertionError(
                    "retirement must not have a final unlink race"
                ),
            ):
                bootstrap = sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=lease,
                    authority=authority,
                )
            archive = self.run_root / "coordinator/retired-proofs"
            members = tuple(
                path.read_bytes()
                for path in archive.iterdir()
                if re.fullmatch(
                    r"[0-9a-f]{64}-[0-9]{2}-.+",
                    path.name,
                )
            )
            self.assertEqual(
                sorted(expected.values()),
                sorted(members),
            )
        finally:
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()

    def test_rearm_retires_only_retained_verified_proofs(self):
        from scripts import run_observing_workflows_eval_worker as worker

        (
            lease,
            bootstrap,
            assignment,
            paths,
            installed,
            _evidence,
        ) = self._prepare_case_and_bootstrap()
        try:
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            machine.begin_teardown()
            case_receipt = worker.cleanup_case_auth(
                installed=installed,
                paths=paths,
            )
            sharding.teardown_case_auth(
                paths=paths,
                receipt=case_receipt,
                lease=lease,
                authority=authority,
            )
            bootstrap_receipt = sharding.cleanup_auth_bootstrap(
                installed=bootstrap,
                lease=lease,
                authority=authority,
            )
            sharding.teardown_auth_bootstrap(
                coordinator_root=self.run_root / "coordinator",
                receipt=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
            sharding.write_teardown_receipt(
                plan=self.plan,
                run_root=self.run_root,
                tombstones=((assignment.key, case_receipt),),
                bootstrap=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
        finally:
            if lease.active:
                lease.close()

        sharding._atomic_write_record(
            self.run_root / "coordinator/stop-launches.json",
            {
                "schema_version": 1,
                "epoch_id": self.plan.epoch_id,
                "run_kind": self.plan.run_kind,
                "reason_sha256": "a" * 64,
            },
        )
        resumed_lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        resumed_bootstrap = None
        try:
            machine, _guard = self._new_machine()
            machine._bind_run_lease(resumed_lease)
            authority = machine.workers_stopped()
            resumed_bootstrap = sharding._rearm_bootstrap_before_resume(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                source_codex_home=self.source_codex_home,
                lease=resumed_lease,
                authority=authority,
            )
            self.assertTrue(resumed_bootstrap.path.is_dir())
            self.assertFalse(
                (self.run_root / "coordinator/teardown.json").exists()
            )
            self.assertFalse(
                (self.run_root / "coordinator/stop-launches.json").exists()
            )
            retired = tuple(
                entry
                for entry in sorted(
                    os.listdir(self.run_root / "coordinator/retired-proofs")
                )
                if re.fullmatch(
                    r"[0-9a-f]{64}-[0-9]{2}-.+",
                    entry,
                )
            )
            self.assertEqual(4, len(retired))
            self.assertEqual(
                ("bootstrap-ownership.json",),
                tuple(sorted(
                    entry.name
                    for entry in (
                        self.run_root / "coordinator/cleanup"
                    ).iterdir()
                )),
            )
        finally:
            if (
                resumed_bootstrap is not None
                and resumed_bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(
                    resumed_bootstrap
                )
            if resumed_lease.active:
                resumed_lease.close()

    def _prepare_retirement_crash_scenario(
        self,
        label,
        *,
        include_stop=True,
    ):
        from scripts import run_observing_workflows_eval_worker as worker

        self.run_root = self.root / label / "run"
        self.source_codex_home = self.root / label / "source-codex-home"
        self.source_codex_home.mkdir(parents=True, mode=0o700)
        auth = self.source_codex_home / "auth.json"
        auth.write_bytes(b'{"token":"secret"}')
        auth.chmod(0o600)
        (
            lease,
            bootstrap,
            assignment,
            paths,
            installed,
            _evidence,
        ) = self._prepare_case_and_bootstrap()
        try:
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            machine.begin_teardown()
            case_receipt = worker.cleanup_case_auth(
                installed=installed,
                paths=paths,
            )
            sharding.teardown_case_auth(
                paths=paths,
                receipt=case_receipt,
                lease=lease,
                authority=authority,
            )
            bootstrap_receipt = sharding.cleanup_auth_bootstrap(
                installed=bootstrap,
                lease=lease,
                authority=authority,
            )
            sharding.teardown_auth_bootstrap(
                coordinator_root=self.run_root / "coordinator",
                receipt=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
            sharding.write_teardown_receipt(
                plan=self.plan,
                run_root=self.run_root,
                tombstones=((assignment.key, case_receipt),),
                bootstrap=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
        finally:
            if lease.active:
                lease.close()
        if include_stop:
            sharding._atomic_write_record(
                self.run_root / "coordinator/stop-launches.json",
                {
                    "schema_version": 1,
                    "epoch_id": self.plan.epoch_id,
                    "run_kind": self.plan.run_kind,
                    "reason_sha256": "a" * 64,
                },
            )
        proof_paths = [
            self.run_root / "coordinator/teardown.json",
            self.run_root
            / "coordinator/cleanup/bootstrap-ownership.json",
            self.run_root
            / "coordinator/cleanup/bootstrap-tombstone.json",
        ]
        if include_stop:
            proof_paths.insert(
                1,
                self.run_root / "coordinator/stop-launches.json",
            )
        return {
            path.name: path.read_bytes()
            for path in proof_paths
        }

    def _fresh_retirement_authority(self):
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        machine, _guard = self._new_machine()
        machine._bind_run_lease(lease)
        return lease, machine.workers_stopped()

    def _crash_retirement_at(self, cut):
        pid = os.fork()
        if pid == 0:
            child_lease = None
            try:
                child_lease, authority = self._fresh_retirement_authority()

                def terminate(point):
                    if point == cut:
                        os._exit(77)

                sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=child_lease,
                    authority=authority,
                    retirement_fault_injector=terminate,
                )
            except BaseException:
                os._exit(99)
            finally:
                if child_lease is not None and child_lease.active:
                    child_lease.close()
            os._exit(0)
        _child, status = os.waitpid(pid, 0)
        self.assertEqual(77, os.waitstatus_to_exitcode(status))

    def test_retirement_recovers_every_durable_crash_cut(self):
        cuts = (
            "prepared-file-fsync",
            "prepared-rename",
            "prepared-directory-fsync",
            "member-0-rename",
            "member-0-source-fsync",
            "member-0-archive-fsync",
            "member-1-rename",
            "member-1-source-fsync",
            "member-1-archive-fsync",
            "member-2-rename",
            "member-2-source-fsync",
            "member-2-archive-fsync",
            "member-3-rename",
            "member-3-source-fsync",
            "member-3-archive-fsync",
            "complete-file-fsync",
            "complete-rename",
            "complete-directory-fsync",
        )
        for index, cut in enumerate(cuts):
            with self.subTest(cut=cut):
                expected = self._prepare_retirement_crash_scenario(
                    f"retirement-crash-{index}"
                )
                self._crash_retirement_at(cut)
                lease, authority = self._fresh_retirement_authority()
                bootstrap = None
                try:
                    bootstrap = sharding._rearm_bootstrap_before_resume(
                        plan=self.plan,
                        coordinator_root=self.run_root / "coordinator",
                        source_codex_home=self.source_codex_home,
                        lease=lease,
                        authority=authority,
                    )
                    archive = (
                        self.run_root / "coordinator/retired-proofs"
                    )
                    names = tuple(sorted(os.listdir(archive)))
                    prepared = tuple(
                        name
                        for name in names
                        if name.endswith("-prepared.json")
                    )
                    complete = tuple(
                        name
                        for name in names
                        if name.endswith("-complete.json")
                    )
                    members = tuple(
                        name
                        for name in names
                        if re.fullmatch(
                            r"[0-9a-f]{64}-[0-9]{2}-.+",
                            name,
                        )
                    )
                    self.assertGreaterEqual(len(prepared), 1)
                    self.assertEqual(len(prepared), len(complete))
                    self.assertEqual(4, len(members))
                    self.assertEqual(
                        sorted(expected.values()),
                        sorted((archive / name).read_bytes() for name in members),
                    )
                    self.assertTrue(bootstrap.path.is_dir())
                    self.assertTrue(
                        (
                            self.run_root
                            / "coordinator/cleanup/bootstrap-ownership.json"
                        ).is_file()
                    )
                finally:
                    if (
                        bootstrap is not None
                        and bootstrap.descriptor_close_state == "owned"
                    ):
                        sharding._retire_descriptor_capability(bootstrap)
                    if lease.active:
                        lease.close()

    def test_retirement_transaction_rejects_mismatched_bindings(self):
        self._prepare_retirement_crash_scenario(
            "retirement-mismatched-binding"
        )
        self._crash_retirement_at("prepared-directory-fsync")
        archive = self.run_root / "coordinator/retired-proofs"
        prepared = next(
            path
            for path in archive.iterdir()
            if path.name.endswith("-prepared.json")
        )
        payload = json.loads(prepared.read_text(encoding="ascii"))
        payload["members"][0]["content_sha256"] = "0" * 64
        prepared.write_bytes(sharding.canonical_config_bytes(payload))
        prepared.chmod(0o600)
        active_before = {
            path.relative_to(self.run_root): path.read_bytes()
            for path in (
                self.run_root / "coordinator/teardown.json",
                self.run_root / "coordinator/stop-launches.json",
                self.run_root
                / "coordinator/cleanup/bootstrap-ownership.json",
                self.run_root
                / "coordinator/cleanup/bootstrap-tombstone.json",
            )
        }
        lease, authority = self._fresh_retirement_authority()
        try:
            with self.assertRaisesRegex(ValueError, "transaction"):
                sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=lease,
                    authority=authority,
                )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(
            active_before,
            {
                path.relative_to(self.run_root): path.read_bytes()
                for path in (
                    self.run_root / "coordinator/teardown.json",
                    self.run_root / "coordinator/stop-launches.json",
                    self.run_root
                    / "coordinator/cleanup/bootstrap-ownership.json",
                    self.run_root
                    / "coordinator/cleanup/bootstrap-tombstone.json",
                )
            },
        )

    def test_retirement_transaction_allows_absent_stop_marker(self):
        expected = self._prepare_retirement_crash_scenario(
            "retirement-without-stop",
            include_stop=False,
        )
        lease, authority = self._fresh_retirement_authority()
        bootstrap = None
        try:
            bootstrap = sharding._rearm_bootstrap_before_resume(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                source_codex_home=self.source_codex_home,
                lease=lease,
                authority=authority,
            )
            archive = self.run_root / "coordinator/retired-proofs"
            members = tuple(
                path.read_bytes()
                for path in archive.iterdir()
                if re.fullmatch(
                    r"[0-9a-f]{64}-[0-9]{2}-.+",
                    path.name,
                )
            )
            self.assertEqual(
                sorted(expected.values()),
                sorted(members),
            )
            self.assertEqual(3, len(members))
        finally:
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()

    def test_retirement_transaction_reserves_the_bounded_archive(self):
        self._prepare_retirement_crash_scenario(
            "retirement-archive-cap"
        )
        archive = self.run_root / "coordinator/retired-proofs"
        archive.mkdir(mode=0o700)
        archive.chmod(0o700)
        for index in range(sharding.MAX_RETIRED_PROOF_RECORDS - 5):
            record = archive / f"{index:032x}-legacy.json"
            record.write_bytes(b"{}")
            record.chmod(0o600)
        active = self.run_root / "coordinator/teardown.json"
        expected = active.read_bytes()
        lease, authority = self._fresh_retirement_authority()
        try:
            with self.assertRaisesRegex(ValueError, "record cap"):
                sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=lease,
                    authority=authority,
                )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(expected, active.read_bytes())
        self.assertFalse(any(
            path.name.endswith("-prepared.json")
            for path in archive.iterdir()
        ))

    def test_retirement_promotes_crash_temps_at_archive_capacity(self):
        for index, cut in enumerate(
            ("prepared-file-fsync", "complete-file-fsync")
        ):
            with self.subTest(cut=cut):
                expected = self._prepare_retirement_crash_scenario(
                    f"retirement-crash-temp-cap-{index}"
                )
                archive = self.run_root / "coordinator/retired-proofs"
                archive.mkdir(mode=0o700)
                archive.chmod(0o700)
                for legacy_index in range(
                    sharding.MAX_RETIRED_PROOF_RECORDS - 6
                ):
                    record = (
                        archive / f"{legacy_index:032x}-legacy.json"
                    )
                    record.write_bytes(b"{}")
                    record.chmod(0o600)
                self._crash_retirement_at(cut)
                lease, authority = self._fresh_retirement_authority()
                bootstrap = None
                try:
                    bootstrap = sharding._rearm_bootstrap_before_resume(
                        plan=self.plan,
                        coordinator_root=self.run_root / "coordinator",
                        source_codex_home=self.source_codex_home,
                        lease=lease,
                        authority=authority,
                    )
                    names = tuple(sorted(os.listdir(archive)))
                    self.assertEqual(
                        sharding.MAX_RETIRED_PROOF_RECORDS,
                        len(names),
                    )
                    self.assertFalse(any(".tmp-" in name for name in names))
                    self.assertEqual(
                        1,
                        sum(
                            name.endswith("-prepared.json")
                            for name in names
                        ),
                    )
                    self.assertEqual(
                        1,
                        sum(
                            name.endswith("-complete.json")
                            for name in names
                        ),
                    )
                    members = tuple(
                        name
                        for name in names
                        if re.fullmatch(
                            r"[0-9a-f]{64}-[0-9]{2}-.+",
                            name,
                        )
                    )
                    self.assertEqual(4, len(members))
                    self.assertEqual(
                        sorted(expected.values()),
                        sorted(
                            (archive / name).read_bytes()
                            for name in members
                        ),
                    )
                    self.assertTrue(bootstrap.path.is_dir())
                finally:
                    if (
                        bootstrap is not None
                        and bootstrap.descriptor_close_state == "owned"
                    ):
                        sharding._retire_descriptor_capability(
                            bootstrap
                        )
                    if lease.active:
                        lease.close()

    def test_retirement_rejects_replaced_crash_temp_without_deleting_it(
        self,
    ):
        self._prepare_retirement_crash_scenario(
            "retirement-replaced-crash-temp"
        )
        self._crash_retirement_at("prepared-file-fsync")
        archive = self.run_root / "coordinator/retired-proofs"
        temporary = next(
            path for path in archive.iterdir() if ".tmp-" in path.name
        )
        replacement = b"replacement crash temp"
        temporary.write_bytes(replacement)
        temporary.chmod(0o600)
        lease, authority = self._fresh_retirement_authority()
        try:
            with self.assertRaisesRegex(
                ValueError,
                "transaction is not canonical",
            ):
                sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=lease,
                    authority=authority,
                )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(replacement, temporary.read_bytes())

    def test_retirement_rechecks_prepared_record_before_first_move(self):
        self._prepare_retirement_crash_scenario(
            "retirement-prepared-record-replacement"
        )
        active_paths = (
            self.run_root / "coordinator/teardown.json",
            self.run_root / "coordinator/stop-launches.json",
            self.run_root
            / "coordinator/cleanup/bootstrap-ownership.json",
            self.run_root
            / "coordinator/cleanup/bootstrap-tombstone.json",
        )
        active_before = {
            path.relative_to(self.run_root): path.read_bytes()
            for path in active_paths
        }
        replacement = b'{"replacement":true}'
        lease, authority = self._fresh_retirement_authority()

        def replace_prepared(point):
            if point != "prepared-directory-fsync":
                return
            archive = self.run_root / "coordinator/retired-proofs"
            prepared = next(
                path
                for path in archive.iterdir()
                if path.name.endswith("-prepared.json")
            )
            prepared.write_bytes(replacement)
            prepared.chmod(0o600)

        try:
            with self.assertRaisesRegex(
                ValueError,
                "transaction.*(changed|invalid)|canonical ASCII JSON",
            ):
                sharding._rearm_bootstrap_before_resume(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    source_codex_home=self.source_codex_home,
                    lease=lease,
                    authority=authority,
                    retirement_fault_injector=replace_prepared,
                )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(
            active_before,
            {
                path.relative_to(self.run_root): path.read_bytes()
                for path in active_paths
            },
        )
        prepared = next(
            path
            for path in (
                self.run_root / "coordinator/retired-proofs"
            ).iterdir()
            if path.name.endswith("-prepared.json")
        )
        self.assertEqual(replacement, prepared.read_bytes())

    def test_completed_retirement_preserves_reused_active_names(self):
        self._prepare_retirement_crash_scenario(
            "retirement-active-name-reuse"
        )
        lease, authority = self._fresh_retirement_authority()
        bootstrap = None
        try:
            bootstrap = sharding._rearm_bootstrap_before_resume(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                source_codex_home=self.source_codex_home,
                lease=lease,
                authority=authority,
            )
        finally:
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()
        ownership = (
            self.run_root / "coordinator/cleanup/bootstrap-ownership.json"
        )
        expected = ownership.read_bytes()
        lease, authority = self._fresh_retirement_authority()
        try:
            sharding._reconcile_retired_proof_archive_before_resume(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                lease=lease,
                authority=authority,
            )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(expected, ownership.read_bytes())

    def test_reconciliation_rechecks_complete_record_before_bootstrap(self):
        self._prepare_retirement_crash_scenario(
            "retirement-complete-record-replacement"
        )
        lease, authority = self._fresh_retirement_authority()
        bootstrap = None
        try:
            bootstrap = sharding._rearm_bootstrap_before_resume(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                source_codex_home=self.source_codex_home,
                lease=lease,
                authority=authority,
            )
        finally:
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()
        ownership = (
            self.run_root / "coordinator/cleanup/bootstrap-ownership.json"
        )
        expected_ownership = ownership.read_bytes()
        archive = self.run_root / "coordinator/retired-proofs"
        complete = next(
            path
            for path in archive.iterdir()
            if path.name.endswith("-complete.json")
        )
        replacement = b'{"replacement":true}'
        real_reconcile = sharding._reconcile_retirement_member
        calls = 0

        def replace_after_last_member(**kwargs):
            nonlocal calls
            result = real_reconcile(**kwargs)
            calls += 1
            if calls == 4:
                complete.write_bytes(replacement)
                complete.chmod(0o600)
            return result

        lease, authority = self._fresh_retirement_authority()
        try:
            with mock.patch.object(
                sharding,
                "_reconcile_retirement_member",
                side_effect=replace_after_last_member,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "transaction record changed",
                ):
                    sharding._reconcile_retired_proof_archive_before_resume(
                        plan=self.plan,
                        coordinator_root=self.run_root / "coordinator",
                        lease=lease,
                        authority=authority,
                    )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(replacement, complete.read_bytes())
        self.assertEqual(expected_ownership, ownership.read_bytes())

    def test_reconciliation_rechecks_archived_member_before_completion(self):
        self._prepare_retirement_crash_scenario(
            "retirement-archived-member-replacement"
        )
        self._crash_retirement_at("member-0-archive-fsync")
        archive = self.run_root / "coordinator/retired-proofs"
        archived = next(
            path
            for path in archive.iterdir()
            if re.fullmatch(
                r"[0-9a-f]{64}-00-.+",
                path.name,
            )
        )
        replacement = b'{"replacement":true}'
        real_read = sharding._read_retained_unlink_content
        injected = False

        def replace_after_descriptor_read(
            proof,
            *,
            byte_cap,
            require_original_ctime,
        ):
            nonlocal injected
            content = real_read(
                proof,
                byte_cap=byte_cap,
                require_original_ctime=require_original_ctime,
            )
            if not injected and proof.name == archived.name:
                injected = True
                archived.unlink()
                archived.write_bytes(replacement)
                archived.chmod(0o600)
            return content

        lease, authority = self._fresh_retirement_authority()
        try:
            with mock.patch.object(
                sharding,
                "_read_retained_unlink_content",
                side_effect=replace_after_descriptor_read,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "member.*changed",
                ):
                    sharding._rearm_bootstrap_before_resume(
                        plan=self.plan,
                        coordinator_root=self.run_root / "coordinator",
                        source_codex_home=self.source_codex_home,
                        lease=lease,
                        authority=authority,
                    )
        finally:
            if lease.active:
                lease.close()
        self.assertEqual(replacement, archived.read_bytes())
        self.assertFalse(any(
            path.name.endswith("-complete.json")
            for path in archive.iterdir()
        ))

    def test_retirement_retains_every_member_through_completion(self):
        self._prepare_retirement_crash_scenario(
            "retirement-member-set-retention"
        )
        archive = self.run_root / "coordinator/retired-proofs"
        replacement = b'{"replacement":true}'
        real_reconcile = sharding._reconcile_retirement_member
        calls = 0

        def replace_after_early_final_pass(**kwargs):
            nonlocal calls
            result = real_reconcile(**kwargs)
            calls += 1
            if calls == 5:
                archived = next(
                    path
                    for path in archive.iterdir()
                    if re.fullmatch(
                        r"[0-9a-f]{64}-00-.+",
                        path.name,
                    )
                )
                archived.unlink()
                archived.write_bytes(replacement)
                archived.chmod(0o600)
            return result

        lease, authority = self._fresh_retirement_authority()
        bootstrap = None
        try:
            with mock.patch.object(
                sharding,
                "_reconcile_retirement_member",
                side_effect=replace_after_early_final_pass,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "member.*changed",
                ):
                    bootstrap = sharding._rearm_bootstrap_before_resume(
                        plan=self.plan,
                        coordinator_root=self.run_root / "coordinator",
                        source_codex_home=self.source_codex_home,
                        lease=lease,
                        authority=authority,
                    )
        finally:
            if (
                bootstrap is not None
                and bootstrap.descriptor_close_state == "owned"
            ):
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()
        archived = next(
            path
            for path in archive.iterdir()
            if re.fullmatch(
                r"[0-9a-f]{64}-00-.+",
                path.name,
            )
        )
        self.assertEqual(replacement, archived.read_bytes())
        self.assertFalse(any(
            path.name.endswith("-complete.json")
            for path in archive.iterdir()
        ))

    def test_terminal_checkpoint_precedes_ack_and_failure_cancels_all(self):
        machine, _guard = self._new_machine()
        events = []
        processes = self._register_fake_lanes(machine, events)
        machine._ledger = self.FakeLedger(malformed_lane="E2")

        real_checkpoint = sharding.CoordinatorGuard.checkpoint

        def checkpoint(guard, reason):
            events.append(("checkpoint", reason))
            return real_checkpoint(guard, reason)

        def ack(_worker_root, message, decision):
            events.append(("ack", message.lane, decision))
            return self.run_root / f"{message.lane}.ack"

        def killpg(pgid, sig):
            events.append(("killpg", pgid, sig))

        with mock.patch.object(
            sharding.CoordinatorGuard, "checkpoint", new=checkpoint
        ), mock.patch.object(
            sharding, "write_ack", side_effect=ack
        ), mock.patch.object(
            sharding.os, "getpgid", side_effect=lambda pid: pid + 1000
        ), mock.patch.object(
            sharding.os, "killpg", side_effect=killpg
        ):
            for lane in ("APP", "E3", "E1"):
                self.assertEqual(
                    "continue",
                    machine.accept_progress(self._lane_message(lane)),
                )
            with self.assertRaisesRegex(ValueError, "malformed lane"):
                machine.accept_progress(self._lane_message("E2"))

        for lane in ("APP", "E3", "E1"):
            checkpoint_index = next(
                index
                for index, event in enumerate(events)
                if event[0] == "checkpoint" and lane in event[1]
            )
            ack_index = events.index(("ack", lane, "continue"))
            self.assertLess(checkpoint_index, ack_index)
        self.assertEqual(
            {process.pid + 1000 for process in processes.values()},
            {event[1] for event in events if event[0] == "killpg"},
        )
        self.assertTrue(
            all(
                reader.joined
                for process in processes.values()
                for reader in process._coordinator_readers
            )
        )
        self.assertEqual("cancelling", machine.phase)

    def test_cancel_recovers_active_case_before_failed(self):
        self._require_state_machine()
        (
            lease,
            bootstrap,
            assignment,
            paths,
            installed,
            evidence,
        ) = self._prepare_case_and_bootstrap()
        try:
            close_error = sharding._retire_descriptor_capability(installed)
            self.assertIsNone(close_error)
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            machine.cancel("worker exited before tombstone")
            authority = machine.workers_stopped()
            machine.begin_teardown()

            recovered = sharding.recover_case_auth_cleanup(
                plan=self.plan,
                assignment=assignment,
                paths=paths,
                lease=lease,
                authority=authority,
            )
            self.assertEqual("coordinator-recovery", recovered.producer)
            sharding.teardown_case_auth(
                paths=paths,
                receipt=recovered,
                lease=lease,
                authority=authority,
            )
            bootstrap_receipt = sharding.cleanup_auth_bootstrap(
                installed=bootstrap,
                lease=lease,
                authority=authority,
            )
            sharding.teardown_auth_bootstrap(
                coordinator_root=self.run_root / "coordinator",
                receipt=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
            receipt = sharding.write_teardown_receipt(
                plan=self.plan,
                run_root=self.run_root,
                tombstones=((assignment.key, recovered),),
                bootstrap=bootstrap_receipt,
                lease=lease,
                authority=authority,
            )
            self.assertTrue(
                (self.run_root / "coordinator" / "teardown.json").is_file()
            )
            machine.mark_torn_down(receipt)
            self.assertEqual("failed", machine.phase)
            self.assertFalse(paths.codex_home.exists())
            self.assertTrue(paths.root.is_dir())
            self.assertEqual("retain me", evidence.read_text(encoding="utf-8"))
        finally:
            if lease.active:
                lease.close()

    def test_teardown_requires_ordered_leases_and_quiescent_authority(self):
        self._require_state_machine()
        lease, _bootstrap, assignment, paths, installed, _evidence = (
            self._prepare_case_and_bootstrap()
        )
        try:
            with self.assertRaises(RuntimeError):
                sharding.RunCoordinatorLease.acquire(
                    run_root=self.run_root,
                    epoch_id=self.plan.epoch_id,
                    run_kind=self.plan.run_kind,
                )
            with self.assertRaises((TypeError, RuntimeError)):
                sharding.teardown_case_auth(
                    paths=paths,
                    receipt=mock.sentinel.receipt,
                    lease=lease,
                    authority=mock.sentinel.authority,
                )
            with self.assertRaises(RuntimeError):
                sharding.ResultWriterLease.acquire(
                    self.repository,
                    "serial-coordinator",
                    "formal",
                )

            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            with self.assertRaises(RuntimeError):
                machine.begin_teardown()
            authority = machine.workers_stopped()
            machine.begin_teardown()
            close_error = sharding._retire_descriptor_capability(installed)
            self.assertIsNone(close_error)
            receipt = sharding.recover_case_auth_cleanup(
                plan=self.plan,
                assignment=assignment,
                paths=paths,
                lease=lease,
                authority=authority,
            )
            sharding.teardown_case_auth(
                paths=paths,
                receipt=receipt,
                lease=lease,
                authority=authority,
            )
            self.assertFalse(authority.consumed)
        finally:
            if lease.active:
                lease.close()

        writer = sharding.ResultWriterLease.acquire(
            self.repository,
            "serial-coordinator",
            "formal",
        )
        try:
            with self.assertRaises(RuntimeError):
                sharding.RunCoordinatorLease.acquire(
                    run_root=self.root / "reverse-run",
                    epoch_id="f" * 64,
                    run_kind="formal",
                )
        finally:
            writer.close()

    def test_interrupted_bootstrap_cleanup_recovers_before_teardown(self):
        self._require_state_machine()
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            installed = sharding.prepare_auth_bootstrap(
                source_codex_home=self.source_codex_home,
                coordinator_root=self.run_root / "coordinator",
                plan=self.plan,
            )
            close_error = sharding._retire_descriptor_capability(installed)
            self.assertIsNone(close_error)
            self.assertFalse(
                (
                    self.run_root
                    / "coordinator/cleanup/bootstrap-tombstone.json"
                ).exists()
            )
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            machine.begin_teardown()
            recovered = sharding.recover_auth_bootstrap_cleanup(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                lease=lease,
                authority=authority,
            )
            self.assertEqual("coordinator-recovery", recovered.producer)
            sharding.teardown_auth_bootstrap(
                coordinator_root=self.run_root / "coordinator",
                receipt=recovered,
                lease=lease,
                authority=authority,
            )
            teardown = sharding.write_teardown_receipt(
                plan=self.plan,
                run_root=self.run_root,
                tombstones=(),
                bootstrap=recovered,
                lease=lease,
                authority=authority,
            )
            self.assertTrue(authority.consumed)
            self.assertFalse(installed.path.exists())
            self.assertTrue(
                (self.run_root / "coordinator/teardown.json").is_file()
            )
            machine.mark_torn_down(teardown)
            self.assertEqual("tearing-down", machine.phase)
        finally:
            if lease.active:
                lease.close()

    def test_indeterminate_close_requires_fresh_cleanup_only_coordinator(self):
        self._require_state_machine()
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        installed = sharding.prepare_auth_bootstrap(
            source_codex_home=self.source_codex_home,
            coordinator_root=self.run_root / "coordinator",
            plan=self.plan,
        )
        machine, _guard = self._new_machine()
        machine._bind_run_lease(lease)
        authority = machine.workers_stopped()
        machine.begin_teardown()
        real_close = sharding.os.close
        target = installed.descriptor

        def close_then_raise(descriptor):
            real_close(descriptor)
            if descriptor == target:
                raise OSError("indeterminate bootstrap close")

        with mock.patch.object(
            sharding.os, "close", side_effect=close_then_raise
        ):
            with self.assertRaisesRegex(
                OSError, "indeterminate bootstrap close"
            ) as caught:
                sharding.cleanup_auth_bootstrap(
                    installed=installed,
                    lease=lease,
                    authority=authority,
                )
        self.assertTrue(
            sharding.is_indeterminate_descriptor_close(caught.exception)
        )
        self.assertTrue(
            (
                self.run_root / "coordinator/cleanup/bootstrap-tombstone.json"
            ).is_file()
        )
        self.assertTrue(installed.path.is_dir())

        with mock.patch.object(
            sharding, "_read_canonical_record"
        ) as read_spy, mock.patch.object(
            sharding.os, "rmdir"
        ) as remove_spy, mock.patch.object(
            sharding, "_atomic_write_record"
        ) as write_spy:
            with self.assertRaisesRegex(RuntimeError, "poisoned"):
                sharding.recover_auth_bootstrap_cleanup(
                    plan=self.plan,
                    coordinator_root=self.run_root / "coordinator",
                    lease=lease,
                    authority=authority,
                )
            read_spy.assert_not_called()
            remove_spy.assert_not_called()
            write_spy.assert_not_called()
        lease.close()
        with self.assertRaisesRegex(RuntimeError, "lease process is poisoned"):
            sharding.RunCoordinatorLease.acquire(
                run_root=self.run_root,
                epoch_id=self.plan.epoch_id,
                run_kind=self.plan.run_kind,
            )
        # The remaining assertions model a new interpreter after process exit.
        sharding._LEASE_PROCESS_POISON = None
        self.addCleanup(
            setattr, sharding, "_LEASE_PROCESS_POISON", None
        )

        fresh_lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        recovered = None
        teardown = None
        try:
            fresh_machine, _guard = self._new_machine()
            fresh_machine._bind_run_lease(fresh_lease)
            fresh_authority = fresh_machine.workers_stopped()
            fresh_machine.begin_teardown()
            recovered = sharding.recover_auth_bootstrap_cleanup(
                plan=self.plan,
                coordinator_root=self.run_root / "coordinator",
                lease=fresh_lease,
                authority=fresh_authority,
            )
            self.assertEqual("coordinator", recovered.producer)
            sharding.teardown_auth_bootstrap(
                coordinator_root=self.run_root / "coordinator",
                receipt=recovered,
                lease=fresh_lease,
                authority=fresh_authority,
            )
            teardown = sharding.write_teardown_receipt(
                plan=self.plan,
                run_root=self.run_root,
                tombstones=(),
                bootstrap=recovered,
                lease=fresh_lease,
                authority=fresh_authority,
            )
            fresh_machine.mark_torn_down(teardown)
            self.assertFalse(installed.path.exists())
            self.assertTrue(fresh_authority.consumed)
            self.assertEqual({}, fresh_machine._workers)
        finally:
            if fresh_lease.active:
                fresh_lease.close()

        verifying_lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            verifying_machine, _guard = self._new_machine()
            verifying_machine._bind_run_lease(verifying_lease)
            verifying_authority = verifying_machine.workers_stopped()
            verifying_machine.begin_teardown()
            self.assertEqual(
                teardown,
                sharding.write_teardown_receipt(
                    plan=self.plan,
                    run_root=self.run_root,
                    tombstones=(),
                    bootstrap=recovered,
                    lease=verifying_lease,
                    authority=verifying_authority,
                ),
            )
            self.assertTrue(verifying_authority.consumed)
            self.assertEqual({}, verifying_machine._workers)
        finally:
            if verifying_lease.active:
                verifying_lease.close()

    def test_phase_transition_matrix_and_single_use_authority(self):
        machine, _guard = self._new_machine()
        self.assertEqual(
            (
                "preflight",
                "running",
                "cancelling",
                "tearing-down",
                "validating",
                "validated",
                "commit-ready",
                "committed",
                "failed",
            ),
            get_args(sharding.CoordinatorPhase),
        )
        self.assertEqual(
            [
                "run_kind",
                "run_root",
                "source_codex_home",
                "codex_executable",
                "requested_model",
                "requested_reasoning_effort",
                "resume_run_root",
                "max_total_tokens",
            ],
            [field.name for field in fields(sharding.ParallelOptions)],
        )
        self.assertEqual(
            ["runtime_factory", "case_driver"],
            [field.name for field in fields(sharding.WorkerDependencies)],
        )
        self.assertEqual(
            ["worker_command_factory", "integrity_runner"],
            [field.name for field in fields(sharding.CoordinatorDependencies)],
        )
        self.assertEqual(
            ["run_kind", "run_root", "status", "validated"],
            [field.name for field in fields(sharding.ParallelRunResult)],
        )
        expected_signatures = {
            "build_production_case_driver": (
                "snapshot_root",
                "transport_config",
                "transport_runner",
            ),
            "build_production_runtime_factory": (
                "snapshot_root",
                "transport_config",
                "plan",
            ),
            "production_worker_dependencies": (
                "snapshot_root",
                "transport_config",
                "plan",
            ),
            "production_coordinator_dependencies": ("snapshot_root",),
            "run_worker": (
                "lane",
                "plan",
                "run_root",
                "snapshot_root",
                "dependencies",
            ),
            "worker_main": ("argv",),
            "run_parallel_evaluation": (
                "repository_root",
                "manifests",
                "result_destinations",
                "options",
                "dependencies",
            ),
        }
        for name, expected in expected_signatures.items():
            with self.subTest(interface=name):
                self.assertEqual(
                    expected,
                    tuple(
                        inspect.signature(
                            getattr(sharding, name)
                        ).parameters
                    ),
                )
        with self.assertRaises(RuntimeError):
            machine.begin_validation()
        with self.assertRaises(RuntimeError):
            machine.mark_committed()
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            with self.assertRaises(RuntimeError):
                machine.workers_stopped()
            machine.begin_teardown()
            self.assertFalse(authority.consumed)
            with self.assertRaises((TypeError, RuntimeError)):
                sharding.QuiescentRunAuthority()
        finally:
            if lease.active:
                lease.close()

    def test_option_a_resume_classification_holds_launch_authority(self):
        machine, _guard = self._new_machine()
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            observed = []

            def classify(**kwargs):
                observed.append(
                    (
                        authority.consumed,
                        lease.active,
                        machine.phase,
                    )
                )
                return sharding.ResumePlan(
                    run_kind=self.plan.run_kind,
                    reusable=(),
                    pending=tuple(
                        assignment.key
                        for assignment in self.plan.assignments
                    ),
                    invalid=(),
                )

            with mock.patch.object(
                sharding, "plan_resume", side_effect=classify
            ):
                resume = sharding._classify_resume_under_quiescence(
                    plan=self.plan,
                    run_root=self.run_root,
                    current_fingerprints=self.plan.fingerprints,
                    manifests=self.manifests,
                    lease=lease,
                    authority=authority,
                )
            self.assertEqual(
                [(False, True, "tearing-down")],
                observed,
            )
            self.assertEqual(self.plan.run_kind, resume.run_kind)
            machine._resume_to_launch(authority)
            self.assertTrue(authority.consumed)
            with self.assertRaises(RuntimeError):
                machine._resume_to_launch(authority)
        finally:
            if lease.active:
                lease.close()

    def test_worker_command_has_no_persistence_surface(self):
        self._require_state_machine()
        dependencies = sharding.production_coordinator_dependencies(
            snapshot_root=self.root / "snapshot"
        )
        command = tuple(
            dependencies.worker_command_factory(
                "E1", self.plan, self._options(), self.root / "snapshot"
            )
        )
        rendered = "\0".join(command).lower()
        for forbidden in (
            "result_destination",
            "result-destination",
            "persist",
            "commit-capability",
            "writer-authority",
        ):
            self.assertNotIn(forbidden, rendered)
        signature = inspect.signature(sharding.worker_main)
        self.assertEqual(["argv"], list(signature.parameters))
        self.assertIsNone(signature.parameters["argv"].default)

    def test_default_worker_launcher_reaches_durable_lane_ready(self):
        repository = Path(__file__).resolve().parents[2]
        coordinator_root = self.run_root / "coordinator"
        coordinator_root.mkdir(parents=True, mode=0o700)
        self.run_root.chmod(0o700)
        snapshot_root = coordinator_root / "captured-snapshot"
        _archive, capture_digests, _inventory, _inventory_bytes = (
            sharding._verified_parallel_archive_inputs(repository)
        )
        sharding._materialize_parallel_snapshot(
            repository_root=repository,
            snapshot_root=snapshot_root,
            expected_digests=capture_digests,
        )
        self.codex_executable.write_text(
            "#!/bin/sh\nprintf '%s\\n' 'codex-cli 9.9.9'\n",
            encoding="utf-8",
        )
        self.codex_executable.chmod(0o700)
        transport_config = sharding.resolve_transport_config(
            codex_executable=self.codex_executable,
            source_codex_home=self.source_codex_home,
            requested_model="test-model",
            requested_reasoning_effort="medium",
        )
        plan = sharding.build_epoch_plan(
            run_kind="discovery",
            manifests=self.manifests,
            fingerprints=replace(
                input_fingerprints("discovery"),
                transport_config_sha256=hashlib.sha256(
                    sharding.transport_config_bytes(transport_config)
                ).hexdigest(),
            ),
        )
        sharding._atomic_write_record(
            coordinator_root / "epoch-plan.json",
            sharding._encode_epoch_plan_record(plan),
        )
        sharding._atomic_write_record(
            coordinator_root / "transport-config.json",
            asdict(transport_config),
        )
        for worker_root in (
            self.run_root / "workers/E1",
            self.run_root / "workers/E2",
            self.run_root / "workers/E3",
            self.run_root / "app-server",
        ):
            worker_root.mkdir(parents=True, mode=0o700)
            worker_root.parent.chmod(0o700)
        machine, _guard = self._new_machine(plan=plan)
        dependencies = sharding.production_coordinator_dependencies(
            snapshot_root=snapshot_root
        )
        resume = sharding.ResumePlan(
            run_kind=self.plan.run_kind,
            reusable=(),
            pending=tuple(
                assignment.key for assignment in plan.assignments
            ),
            invalid=(),
        )
        try:
            sharding._launch_parallel_workers(
                machine=machine,
                plan=plan,
                options=self._options(),
                snapshot_root=snapshot_root,
                dependencies=dependencies,
                resume=resume,
            )
            ready = sharding.wait_for_progress(
                worker_root=self.run_root / "workers/E1",
                expected_lane="E1",
                expected_seq=1,
                timeout=5.0,
            )
            self.assertEqual("lane-ready", ready.type)
            command = tuple(
                dependencies.worker_command_factory(
                    "E1",
                    plan,
                    self._options(),
                    snapshot_root,
                )
            )
            self.assertEqual(
                (
                    sys.executable,
                    "-m",
                    "scripts.run_observing_workflows_eval_worker",
                ),
                command[:3],
            )
        finally:
            if machine.phase in ("preflight", "running", "cancelling"):
                machine.cancel("production launcher boundary test complete")
            for process in machine._workers.values():
                for stream in (process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()

    def test_production_integrity_runner_parses_exact_healthy_output(self):
        completed = subprocess.CompletedProcess(
            args=("integrity",),
            returncode=0,
            stdout="healthy records=7 invalidated=2\n",
            stderr="",
        )
        with mock.patch.object(
            sharding.subprocess, "run", return_value=completed
        ):
            self.assertEqual(
                {"records": 7, "invalidated": 2},
                sharding._production_integrity_runner(
                    ("integrity",), {}, expected_records=7
                ),
            )

    def test_production_integrity_runner_rejects_bad_process_or_output(self):
        cases = (
            (1, "", "failed\n", 7),
            (0, "healthy records=7 invalidated=2\n", "warning\n", 7),
            (0, '{"records":7,"invalidated":2}\n', "", 7),
            (0, "healthy records=6 invalidated=2\n", "", 7),
            (0, "healthy records=7 invalidated=-1\n", "", 7),
            (0, "healthy records=7 invalidated=2\nextra\n", "", 7),
        )
        for returncode, stdout, stderr, expected_records in cases:
            with self.subTest(
                returncode=returncode,
                stdout=stdout,
                stderr=stderr,
            ), mock.patch.object(
                sharding.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    args=("integrity",),
                    returncode=returncode,
                    stdout=stdout,
                    stderr=stderr,
                ),
            ), self.assertRaises((RuntimeError, ValueError)):
                sharding._production_integrity_runner(
                    ("integrity",),
                    {},
                    expected_records=expected_records,
                )

    def test_captured_failure_facts_ignore_foreign_class_identity(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from types import ModuleType

        evaluator = ModuleType("_task12_foreign_evaluator")
        exec(
            """
class CaseCleanupFailure(RuntimeError):
    pass

class ProcessSurvivalCleanupFailure(CaseCleanupFailure):
    pass

class CaseInfrastructureFailure(RuntimeError):
    pass

class CaseTransportFailure(RuntimeError):
    def __init__(self, message, *, classification, retryable):
        super().__init__(message)
        self.classification = classification
        self.retryable = retryable

def _contains_process_survival_failure(error):
    if isinstance(error, ProcessSurvivalCleanupFailure):
        return True
    return any(
        _contains_process_survival_failure(child)
        for child in getattr(error, "exceptions", ())
    )
""",
            evaluator.__dict__,
        )
        assignment = self.plan.assignments[0]
        manifest_case = self.manifests["forward"][0]
        self.run_root.mkdir(mode=0o700)
        paths = sharding.paths_for_case(self.run_root, assignment)

        class Factory:
            poisoned = False

            def cleanup_case(self, _paths):
                raise AssertionError("foreign pre-runtime failure was cleaned")

        factory = Factory()
        cases = (
            (
                ExceptionGroup(
                    "nested captured model failure",
                    [
                        RuntimeError("outer context"),
                        evaluator.CaseTransportFailure(
                            "model rejected request",
                            classification="model",
                            retryable=False,
                        ),
                    ],
                ),
                "model",
                False,
            ),
            (
                evaluator.CaseInfrastructureFailure(
                    "transport could not start"
                ),
                "pre-model-infrastructure",
                False,
            ),
            (
                evaluator.CaseCleanupFailure("cleanup failed"),
                "cleanup",
                False,
            ),
            (
                ExceptionGroup(
                    "nested process survival",
                    [
                        RuntimeError("cleanup context"),
                        evaluator.ProcessSurvivalCleanupFailure(
                            "process still exists"
                        ),
                    ],
                ),
                "surviving-process",
                True,
            ),
        )
        for foreign_error, expected, exit_required in cases:
            with self.subTest(classification=expected):
                def fail_case(**_kwargs):
                    raise foreign_error

                evaluator._run_case = fail_case
                driver = worker._ProductionCaseDriver(
                    evaluator=evaluator,
                    transport_config=mock.sentinel.transport_config,
                    transport_runner=mock.sentinel.transport_runner,
                )
                with self.assertRaises(BaseException) as raised:
                    driver(
                        assignment=assignment,
                        manifest_case=manifest_case,
                        paths=paths,
                        runtime_factory=factory,
                        event_sink=mock.sentinel.event_sink,
                    )
                normalized = raised.exception
                self.assertEqual(
                    expected,
                    worker._classify_worker_failure(
                        normalized, model_started=False
                    ),
                )
                self.assertIs(
                    exit_required,
                    worker.worker_exit_required(normalized, factory),
                )

    @staticmethod
    def _write_retry_test_tombstone(
        *,
        plan,
        assignment,
        paths,
    ):
        paths.codex_home.mkdir(mode=0o700)
        root_stat = paths.root.stat()
        home_stat = paths.codex_home.stat()
        ownership = sharding.CaseAuthOwnership(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
        )
        ownership_bytes = sharding._atomic_write_record(
            paths.cleanup / "ownership.json", asdict(ownership)
        )
        receipt = sharding.TombstoneReceipt(
            schema_version=1,
            epoch_id=plan.epoch_id,
            run_kind=plan.run_kind,
            case=assignment.key,
            ownership_sha256=hashlib.sha256(ownership_bytes).hexdigest(),
            case_root_device=root_stat.st_dev,
            case_root_inode=root_stat.st_ino,
            codex_home_device=home_stat.st_dev,
            codex_home_inode=home_stat.st_ino,
            scrubbed=True,
            empty=True,
            canonical_binding="expected",
            producer="worker",
        )
        sharding._atomic_write_record(
            paths.cleanup / "tombstone.json", asdict(receipt)
        )
        paths.codex_home.rmdir()

    def test_one_lane_retries_once_through_durable_coordinator_ack(self):
        from scripts import run_observing_workflows_eval_worker as worker
        from scripts import run_observing_workflows_task9_eval as task9_eval

        self.run_root.mkdir(mode=0o700)
        coordinator_root = self.run_root / "coordinator"
        coordinator_root.mkdir(mode=0o700)
        snapshot_root = coordinator_root / "captured-snapshot"
        fixture_root = snapshot_root / "evidence/tests/skill_evals"
        fixture_root.mkdir(parents=True, mode=0o700)
        for name in (
            "observing_workflows_cases.json",
            "observing_workflows_lifecycle_cases.json",
        ):
            shutil.copy2(FIXTURES / name, fixture_root / name)

        self.codex_executable.write_text(
            "#!/bin/sh\nprintf '%s\\n' 'codex-cli 9.9.9'\n",
            encoding="utf-8",
        )
        self.codex_executable.chmod(0o700)
        transport_config = sharding.resolve_transport_config(
            codex_executable=self.codex_executable,
            source_codex_home=self.source_codex_home,
            requested_model="test-model",
            requested_reasoning_effort="medium",
        )
        epoch_id = hashlib.sha256(
            (
                "task-12-one-lane-retry\0"
                + str(self.root)
            ).encode("utf-8")
        ).hexdigest()
        assignment = self.plan.assignments[0]
        plan = sharding.EpochPlan(
            schema_version=1,
            epoch_id=epoch_id,
            run_kind=self.plan.run_kind,
            fingerprints=replace(
                self.plan.fingerprints,
                epoch_id=epoch_id,
                transport_config_sha256=hashlib.sha256(
                    sharding.transport_config_bytes(transport_config)
                ).hexdigest(),
            ),
            assignments=(assignment,),
        )
        sharding._register_progress_epoch_context(
            plan=plan, manifests=self.manifests
        )
        sharding._atomic_write_record(
            coordinator_root / "epoch-plan.json",
            sharding._encode_epoch_plan_record(plan),
        )
        sharding._atomic_write_record(
            coordinator_root / "transport-config.json",
            asdict(transport_config),
        )

        class Factory:
            poisoned = False

            def __init__(self):
                self.closed = False

            def __call__(self, **_kwargs):
                raise AssertionError("test case driver owns the fake runtime")

            def close(self):
                self.closed = True

        runtime_factory = Factory()
        attempts = []
        usage = sharding.TokenUsage(
            input_tokens=10,
            cached_input_tokens=2,
            output_tokens=5,
            reasoning_output_tokens=1,
            total_tokens=15,
        )
        result = {
            "id": assignment.key.case_id,
            "decisions": [
                {
                    "after_turn": 1,
                    "triggered": True,
                    "task_type": "feature",
                    "workflow_variant": "implementation-basic",
                }
            ],
            "record_checkpoints": [
                {
                    "after_turn": 1,
                    "records": [
                        {
                            "role": "run-1",
                            "status": "success",
                            "start_mode": "planned",
                            "superseded_by_role": None,
                        }
                    ],
                }
            ],
            "run_count": 1,
            "draft_count": 0,
            "final_statuses": ["success"],
        }

        def case_driver(
            *,
            assignment,
            manifest_case,
            paths,
            runtime_factory,
            event_sink,
        ):
            attempts.append(len(attempts) + 1)
            for directory in (
                paths.store,
                paths.audit,
                paths.payload,
                paths.output,
            ):
                directory.mkdir(mode=0o700)
            self._write_retry_test_tombstone(
                plan=plan,
                assignment=assignment,
                paths=paths,
            )
            if len(attempts) == 1:
                raise task9_eval.CaseInfrastructureFailure(
                    "injected pre-model infrastructure failure"
                )
            event_sink("model-started", 4100, 4100)
            return worker.DrivenCase(
                result=result,
                execution=task9_eval.CaseExecution(
                    terminal_status="completed",
                    final_text="done",
                    command_executions=(),
                    observation_command_diagnostics=(),
                    usage=usage,
                ),
            )

        dependencies = sharding.WorkerDependencies(
            runtime_factory=runtime_factory,
            case_driver=case_driver,
        )
        machine, _guard = self._new_machine(plan=plan)
        worker_root = self.run_root / "workers/E1"
        worker_root.mkdir(parents=True, mode=0o700)
        worker_root.parent.chmod(0o700)

        class Process:
            pid = os.getpid()
            pgid = os.getpgrp()
            returncode = 0
            _coordinator_readers = ()

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                return self.returncode

        process = Process()
        process.worker_root = worker_root
        machine.register_worker("E1", process)
        resume = sharding.ResumePlan(
            run_kind=plan.run_kind,
            reusable=(),
            pending=(assignment.key,),
            invalid=(),
        )
        worker_result = []
        worker_errors = []

        def run_lane():
            try:
                worker_result.append(
                    worker._run_worker_impl(
                        lane="E1",
                        plan=plan,
                        run_root=self.run_root,
                        snapshot_root=snapshot_root,
                        resume=resume,
                        dependencies=dependencies,
                    )
                )
            except BaseException as error:
                worker_errors.append(error)

        thread = threading.Thread(target=run_lane)
        thread.start()
        decisions = []
        sequence = 1
        try:
            while True:
                try:
                    message = sharding.wait_for_progress(
                        worker_root=worker_root,
                        expected_lane="E1",
                        expected_seq=sequence,
                        timeout=5.0,
                    )
                except RuntimeError as error:
                    if "inventory changed" in str(error):
                        continue
                    raise
                except ValueError as error:
                    if str(error) == "progress publication is pending":
                        continue
                    raise
                except TimeoutError:
                    if worker_errors:
                        raise worker_errors[0]
                    raise
                decision = machine.accept_progress(message)
                decisions.append((message.type, message.attempt, decision))
                sequence += 1
                if decision == "abort":
                    while message.type != "worker-stopped":
                        try:
                            message = sharding.wait_for_progress(
                                worker_root=worker_root,
                                expected_lane="E1",
                                expected_seq=sequence,
                                timeout=5.0,
                            )
                        except RuntimeError as error:
                            if "inventory changed" in str(error):
                                continue
                            raise
                        except ValueError as error:
                            if str(error) == "progress publication is pending":
                                continue
                            raise
                        sharding.write_ack(worker_root, message, "abort")
                        sequence += 1
                    break
                if message.type == "worker-stopped":
                    break
        finally:
            thread.join(5.0)
        self.assertFalse(thread.is_alive())
        self.assertEqual([], worker_errors)
        self.assertEqual([1, 2], attempts)
        self.assertIn(("case-terminal", 1, "retry"), decisions)
        self.assertNotIn("abort", [decision for _, _, decision in decisions])
        self.assertEqual(1, len(worker_result))
        shard = sharding.read_shard_seal(
            worker_root=worker_root,
            plan=plan,
            lane="E1",
            manifests=self.manifests,
            case_paths={
                assignment.key: sharding.paths_for_case(
                    self.run_root, assignment
                )
            },
        )
        self.assertEqual("success", shard.status)
        self.assertEqual((assignment.key,), tuple(
            terminal.key for terminal in shard.terminals
        ))
        self.assertEqual(
            (1, 2),
            tuple(
                int(attempt.root.name)
                for attempt in sharding.scan_attempts(
                    sharding.paths_for_case(self.run_root, assignment),
                    plan=plan,
                    manifest_case=self.manifests["forward"][0],
                )
            ),
        )
        self.assertTrue(runtime_factory.closed)
        self.assertFalse(
            (coordinator_root / "stop-launches.json").exists()
        )

    def test_resume_protocol_and_stop_marker_boundaries_are_durable(self):
        from scripts import run_observing_workflows_eval_worker as worker

        worker_root = self.run_root / "workers/E1"
        self.run_root.mkdir(mode=0o700)
        worker_root.parent.mkdir(mode=0o700)
        worker_root.mkdir(mode=0o700)
        message = self._lane_message(
            "E1", seq=1, progress_type="lane-ready"
        )
        sharding.write_progress(worker_root, message)
        sharding.write_ack(worker_root, message, "continue")
        self.assertEqual(
            2, worker._next_worker_sequence(worker_root, "E1")
        )
        self.assertTrue(
            sharding._parallel_recovery_is_cleanup_only(
                has_ownership=True,
                has_tombstone=False,
                has_teardown=False,
                has_stop_launches=True,
                resume_requested=True,
            )
        )
        self.assertFalse(
            sharding._parallel_recovery_is_cleanup_only(
                has_ownership=True,
                has_tombstone=True,
                has_teardown=True,
                has_stop_launches=True,
                resume_requested=True,
            )
        )

    def test_production_capture_fingerprints_complete_evaluator_boundary(self):
        self.assertIn(
            "wiki_cli.py", sharding._PARALLEL_EVALUATOR_ORIGINS
        )
        self.assertIn(
            "wiki_observations.py", sharding._PARALLEL_EVALUATOR_ORIGINS
        )
        self.assertNotIn(
            "tests/skill_evals/observing_workflows_cases.json",
            sharding._PARALLEL_EVALUATOR_ORIGINS,
        )
        repository = Path(__file__).resolve().parents[2]
        _archive, digests, inventory, _inventory_bytes = (
            sharding._verified_parallel_archive_inputs(repository)
        )
        evaluator_rows = tuple(
            (
                inventory["repository_evidence"][origin]["member"],
                inventory["repository_evidence"][origin][
                    "packaged_sha256"
                ],
            )
            for origin in sharding._PARALLEL_EVALUATOR_ORIGINS
        )
        self.assertEqual(
            sharding.component_digest(evaluator_rows), digests[2]
        )

    def test_top_level_resume_honors_uncommitted_stop_marker(self):
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            sharding.prepare_auth_bootstrap(
                source_codex_home=self.source_codex_home,
                coordinator_root=self.run_root / "coordinator",
                plan=self.plan,
            )
            sharding._atomic_write_record(
                self.run_root / "coordinator/stop-launches.json",
                {
                    "schema_version": 1,
                    "epoch_id": self.plan.epoch_id,
                    "run_kind": self.plan.run_kind,
                    "reason_sha256": "a" * 64,
                },
            )
        finally:
            lease.close()
        transport_config = RuntimeIsolationTests._transport_config(
            self.codex_executable
        )
        snapshot_root = self.run_root / "coordinator/captured-snapshot"
        options = replace(
            self._options(), resume_run_root=self.run_root
        )
        dependencies = sharding.CoordinatorDependencies(
            worker_command_factory=mock.Mock(
                side_effect=AssertionError("worker launch was attempted")
            ),
            integrity_runner=mock.Mock(),
        )
        with mock.patch.object(
            sharding,
            "_parallel_plan_inputs",
            return_value=(
                self.plan,
                transport_config,
                snapshot_root,
                self.manifests,
                ("a" * 64, "b" * 64, "c" * 64),
            ),
        ), mock.patch.object(
            sharding, "_materialize_parallel_snapshot"
        ), mock.patch.object(
            sharding, "_launch_parallel_workers"
        ) as launch:
            result = sharding.run_parallel_evaluation(
                repository_root=self.repository,
                manifests=self.manifests,
                result_destinations=None,
                options=options,
                dependencies=dependencies,
            )
        launch.assert_not_called()
        self.assertEqual("failed", result.status)
        self.assertTrue(
            (self.run_root / "coordinator/teardown.json").is_file()
        )

    def test_prebootstrap_cancel_does_not_commit_stop_marker(self):
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        try:
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            machine.cancel("pre-bootstrap failure")
            self.assertFalse(
                (
                    self.run_root / "coordinator/stop-launches.json"
                ).exists()
            )
        finally:
            lease.close()

    def test_fresh_quiescence_cancels_durable_worker_group(self):
        lease = sharding.RunCoordinatorLease.acquire(
            run_root=self.run_root,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
        )
        lifetime_slot, record_path = (
            sharding._acquire_worker_lifetime_lock(
                self.run_root, "E1"
            )
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ],
            start_new_session=True,
            pass_fds=(lifetime_slot.descriptor,),
        )
        waiter = threading.Thread(target=process.wait)
        waiter.start()
        try:
            pgid = os.getpgid(process.pid)
            sharding._atomic_write_record(
                record_path,
                {
                    "schema_version": 1,
                    "epoch_id": self.plan.epoch_id,
                    "run_kind": self.plan.run_kind,
                    "lane": "E1",
                    "pid": process.pid,
                    "pgid": pgid,
                },
            )
            sharding._retire_task_descriptors(
                [lifetime_slot],
                primary=None,
                label="test worker lifetime close failed",
            )
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            self.assertFalse(authority.consumed)
            waiter.join(5.0)
            self.assertFalse(waiter.is_alive())
            self.assertIsNotNone(process.returncode)
        finally:
            if lifetime_slot.descriptor >= 0:
                sharding._retire_task_descriptors(
                    [lifetime_slot],
                    primary=None,
                    label="test worker lifetime cleanup failed",
                )
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            if lease.active:
                lease.close()

    def test_interrupted_retry_reset_restores_prior_cleanup_proof(self):
        from scripts import run_observing_workflows_eval_worker as worker

        (
            lease,
            bootstrap,
            assignment,
            paths,
            installed,
            _evidence,
        ) = self._prepare_case_and_bootstrap()
        manifest_case = self.manifests[assignment.key.mode][
            assignment.key.ordinal - 1
        ]
        paths.root.parent.chmod(0o700)
        try:
            sharding.write_attempt_start(
                plan=self.plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
            )
            machine, _guard = self._new_machine()
            machine._bind_run_lease(lease)
            authority = machine.workers_stopped()
            machine.begin_teardown()
            receipt = worker.cleanup_case_auth(
                installed=installed,
                paths=paths,
            )
            sharding.teardown_case_auth(
                paths=paths,
                receipt=receipt,
                lease=lease,
                authority=authority,
            )
            failure_text = "pre-model infrastructure failure"
            sharding.write_attempt_terminal(
                plan=self.plan,
                paths=paths,
                assignment=assignment,
                attempt=1,
                manifest_case=manifest_case,
                status="failed",
                classification="pre-model-infrastructure",
                model_started=False,
                cleanup_passed=True,
                usage=None,
                failure={
                    "classification": "pre-model-infrastructure",
                    "type": "RuntimeError",
                    "chars": len(failure_text),
                    "sha256": hashlib.sha256(
                        failure_text.encode("utf-8")
                    ).hexdigest(),
                },
            )
            worker._reset_case_for_retry(
                plan=self.plan,
                assignment=assignment,
                manifest_case=manifest_case,
                paths=paths,
            )
            self.assertEqual((), tuple(paths.cleanup.iterdir()))
            self.assertTrue(
                (paths.root / "cleanup-attempt-1").is_dir()
            )
            sharding._reconcile_retry_cleanup_backup(paths)
            self.assertFalse(
                (paths.root / "cleanup-attempt-1").exists()
            )
            self.assertEqual(
                receipt,
                sharding.read_tombstone_receipt(
                    plan=self.plan,
                    assignment=assignment,
                    paths=paths,
                ),
            )
        finally:
            if bootstrap.descriptor_close_state == "owned":
                sharding._retire_descriptor_capability(bootstrap)
            if lease.active:
                lease.close()

    def test_token_ceiling_is_a_launch_ceiling(self):
        machine, _guard = self._new_machine(
            options=self._options(max_total_tokens=10)
        )
        events = []
        process = self.FakeProcess(
            "E1",
            self.run_root / "workers/E1",
            events,
            pid=4301,
        )
        machine.register_worker("E1", process)
        started = sharding.ProgressMessage(
            schema_version=1,
            epoch_id=self.plan.epoch_id,
            run_kind=self.plan.run_kind,
            lane="E1",
            seq=1,
            type="case-started",
            case=self.plan.assignments[0].key,
            attempt=1,
            status=None,
            classification=None,
            model_started=None,
            usage=None,
            attempt_terminal_sha256=None,
            case_commit_sha256=None,
            shard_commit_sha256=None,
            tombstone_receipt_sha256=None,
        )
        terminal = replace(
            started,
            seq=2,
            type="case-terminal",
            status="success",
            classification="success",
            model_started=True,
            usage=sharding.TokenUsage(10, 0, 5, 0, 15),
            attempt_terminal_sha256="a" * 64,
            case_commit_sha256="b" * 64,
            tombstone_receipt_sha256="c" * 64,
        )
        with mock.patch.object(sharding, "write_ack"):
            self.assertEqual("continue", machine.accept_progress(started))
            self.assertEqual(
                "stop-launches", machine.accept_progress(terminal)
            )
        self.assertTrue(machine.stop_launches)
        self.assertEqual(15, machine.total_tokens)


if __name__ == "__main__":
    unittest.main()
