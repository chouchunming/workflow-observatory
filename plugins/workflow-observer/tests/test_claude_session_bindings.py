import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PLUGIN_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import claude_session_bindings as bindings


RUN_ONE = "obs-20260719-120000-abcdef"
RUN_TWO = "obs-20260719-120001-123abc"


class ClaudeSessionBindingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.plugin_data = self.base / "plugin-data"
        self.plugin_data.mkdir(mode=0o700)
        self.session_id = "opaque-session-SENTINEL-raw-value"
        self.digest = hashlib.sha256(self.session_id.encode("utf-8")).hexdigest()
        self.binding = self.plugin_data / "session-bindings" / f"{self.digest}.json"
        self.lock = self.plugin_data / "session-bindings/.locks" / f"{self.digest}.lock"

    def tearDown(self):
        self.temporary.cleanup()

    def assert_bounded(self, error: BaseException) -> None:
        message = str(error)
        self.assertLessEqual(len(message), 200)
        self.assertNotIn(self.session_id, message)
        self.assertNotIn(str(self.base), message)

    def test_bind_lookup_unbind_use_only_private_hashed_state(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)

        self.assertTrue(self.binding.is_file())
        self.assertTrue(self.lock.is_file())
        self.assertEqual(RUN_ONE, bindings.lookup_session(self.plugin_data, self.session_id))
        self.assertEqual(
            {"schema_version": 1, "run_id": RUN_ONE, "state": "active"},
            json.loads(self.binding.read_text(encoding="utf-8")),
        )
        for path in self.plugin_data.rglob("*"):
            self.assertNotIn(self.session_id, path.name)
            if path.is_file():
                self.assertNotIn(self.session_id, path.read_text(encoding="utf-8"))
                self.assertEqual(0o600, stat.S_IMODE(path.stat().st_mode))
            elif path.is_dir():
                self.assertEqual(0o700, stat.S_IMODE(path.stat().st_mode))

        self.assertTrue(
            bindings.unbind_session(
                self.plugin_data, self.session_id, expected_run_id=RUN_ONE
            )
        )
        self.assertFalse(self.binding.exists())
        self.assertIsNone(bindings.lookup_session(self.plugin_data, self.session_id))

    def test_bind_rejects_invalid_identity_and_schema_values_without_writes(self):
        invalid_values = (
            ("", RUN_ONE),
            (self.session_id, "not-a-run"),
            (self.session_id, RUN_ONE, "finished"),
        )
        for values in invalid_values:
            with self.subTest(values=values):
                with self.assertRaises(bindings.BindingError) as raised:
                    bindings.bind_session(self.plugin_data, *values)
                self.assert_bounded(raised.exception)
        self.assertFalse((self.plugin_data / "session-bindings").exists())

    def test_binding_root_must_be_an_absolute_nonroot_directory(self):
        for root in (Path("relative"), Path("/")):
            with self.subTest(root=root):
                with self.assertRaises(bindings.BindingError) as raised:
                    bindings._validate_inputs(root, self.session_id)
                self.assert_bounded(raised.exception)

    def test_lookup_rejects_symlink_nonregular_permissive_and_unknown_state(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        original = self.binding.read_bytes()

        self.binding.unlink()
        target = self.base / "target"
        target.write_bytes(original)
        target.chmod(0o600)
        self.binding.symlink_to(target)
        with self.assertRaises(bindings.BindingError) as symlinked:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(symlinked.exception)

        self.binding.unlink()
        self.binding.mkdir(mode=0o700)
        with self.assertRaises(bindings.BindingError) as nonregular:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(nonregular.exception)

        self.binding.rmdir()
        self.binding.write_bytes(original)
        self.binding.chmod(0o644)
        with self.assertRaises(bindings.BindingError) as permissive:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(permissive.exception)

        self.binding.chmod(0o600)
        self.binding.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": RUN_ONE,
                    "state": "active",
                    "unexpected": True,
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaises(bindings.BindingError) as schema:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(schema.exception)

    def test_directory_and_lock_integrity_fail_closed(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        binding_dir = self.binding.parent
        locks_dir = self.lock.parent

        binding_dir.chmod(0o755)
        with self.assertRaises(bindings.BindingError) as permissive_dir:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(permissive_dir.exception)
        binding_dir.chmod(0o700)

        self.lock.chmod(0o644)
        with self.assertRaises(bindings.BindingError) as permissive_lock:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(permissive_lock.exception)
        self.lock.chmod(0o600)

        moved = self.base / "moved-bindings"
        binding_dir.rename(moved)
        binding_dir.symlink_to(moved, target_is_directory=True)
        with self.assertRaises(bindings.BindingError) as symlinked_dir:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(symlinked_dir.exception)

        binding_dir.unlink()
        moved.rename(binding_dir)
        moved_locks = self.base / "moved-locks"
        locks_dir.rename(moved_locks)
        locks_dir.symlink_to(moved_locks, target_is_directory=True)
        with self.assertRaises(bindings.BindingError) as symlinked_locks:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(symlinked_locks.exception)

    def test_lookup_fails_closed_when_file_identity_changes(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        with mock.patch.object(bindings, "_same_identity", return_value=False):
            with self.assertRaises(bindings.BindingError) as changed:
                bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(changed.exception)

    def test_lookup_fails_closed_when_binding_mode_changes_during_read(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        real_stat = bindings.os.stat
        binding_stats = 0

        def race_mode(path, *args, **kwargs):
            nonlocal binding_stats
            result = real_stat(path, *args, **kwargs)
            if os.fspath(path) == self.binding.name:
                binding_stats += 1
                if binding_stats >= 4:
                    fields = list(result)
                    fields[0] = result.st_mode | 0o044
                    return os.stat_result(fields)
            return result

        with mock.patch.object(bindings.os, "stat", side_effect=race_mode):
            with self.assertRaises(bindings.BindingError) as changed:
                bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(changed.exception)

    def test_lookup_fails_closed_when_lock_mode_changes_after_locking(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        real_stat = bindings.os.stat
        lock_stats = 0

        def race_mode(path, *args, **kwargs):
            nonlocal lock_stats
            result = real_stat(path, *args, **kwargs)
            if os.fspath(path) == self.lock.name:
                lock_stats += 1
                if lock_stats >= 3:
                    fields = list(result)
                    fields[0] = result.st_mode | 0o044
                    return os.stat_result(fields)
            return result

        with mock.patch.object(bindings.os, "stat", side_effect=race_mode):
            with self.assertRaises(bindings.BindingError) as changed:
                bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(changed.exception)

    def test_bind_uses_exclusive_unique_temps_and_syncs_file_and_directory(self):
        opened_exclusive: list[str] = []
        real_open = bindings.os.open

        def track_open(path, flags, *args, **kwargs):
            if flags & os.O_EXCL and ".tmp-" in os.fspath(path):
                opened_exclusive.append(os.fspath(path))
            return real_open(path, flags, *args, **kwargs)

        with mock.patch.object(bindings.os, "open", side_effect=track_open), mock.patch.object(
            bindings.os, "fsync", wraps=os.fsync
        ) as synced:
            bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
            bindings.bind_session(self.plugin_data, self.session_id, RUN_TWO)

        self.assertEqual(2, len(opened_exclusive))
        self.assertEqual(2, len(set(opened_exclusive)))
        self.assertTrue(all(".tmp-" in name for name in opened_exclusive))
        self.assertGreaterEqual(synced.call_count, 4)
        self.assertEqual(RUN_TWO, bindings.lookup_session(self.plugin_data, self.session_id))
        self.assertEqual([], list(self.binding.parent.glob(".tmp-*")))

    def test_failed_atomic_replacement_preserves_previous_binding(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)

        with mock.patch.object(bindings.os, "replace", side_effect=OSError("replace failed")):
            with self.assertRaises(bindings.BindingError) as failed:
                bindings.bind_session(self.plugin_data, self.session_id, RUN_TWO)

        self.assert_bounded(failed.exception)
        self.assertEqual(RUN_ONE, bindings.lookup_session(self.plugin_data, self.session_id))
        self.assertEqual([], list(self.binding.parent.glob(".tmp-*")))

    def test_temp_name_collision_retries_without_removing_foreign_file(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        collision_token = "1" * 16
        collision = self.binding.parent / f".tmp-{self.digest}-{collision_token}"
        collision.write_text("foreign", encoding="utf-8")
        collision.chmod(0o600)

        with mock.patch.object(
            bindings.secrets,
            "token_hex",
            side_effect=(collision_token, "2" * 16),
        ):
            bindings.bind_session(self.plugin_data, self.session_id, RUN_TWO)

        self.assertEqual("foreign", collision.read_text(encoding="utf-8"))
        self.assertEqual(RUN_TWO, bindings.lookup_session(self.plugin_data, self.session_id))

    def test_symlinked_and_nonregular_lock_files_fail_closed(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_ONE)
        self.lock.unlink()
        target = self.base / "lock-target"
        target.touch(mode=0o600)
        self.lock.symlink_to(target)
        with self.assertRaises(bindings.BindingError) as symlinked:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(symlinked.exception)

        self.lock.unlink()
        self.lock.mkdir(mode=0o700)
        with self.assertRaises(bindings.BindingError) as nonregular:
            bindings.lookup_session(self.plugin_data, self.session_id)
        self.assert_bounded(nonregular.exception)

    def test_conditional_unbind_preserves_replacement(self):
        bindings.bind_session(self.plugin_data, self.session_id, RUN_TWO)

        self.assertFalse(
            bindings.unbind_session(
                self.plugin_data, self.session_id, expected_run_id=RUN_ONE
            )
        )
        self.assertEqual(RUN_TWO, bindings.lookup_session(self.plugin_data, self.session_id))

    def test_concurrent_binds_leave_one_complete_valid_binding(self):
        program = """
import pathlib
import sys
sys.path.insert(0, sys.argv[1])
from claude_session_bindings import bind_session
bind_session(pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4])
"""
        commands = [
            [
                sys.executable,
                "-c",
                program,
                str(SCRIPTS),
                str(self.plugin_data),
                self.session_id,
                run_id,
            ]
            for run_id in (RUN_ONE, RUN_TWO)
        ]
        processes = [
            subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for command in commands
        ]
        completed = [process.communicate(timeout=10) for process in processes]

        self.assertEqual([0, 0], [process.returncode for process in processes], completed)
        self.assertIn(
            bindings.lookup_session(self.plugin_data, self.session_id),
            {RUN_ONE, RUN_TWO},
        )
        self.assertEqual([], list(self.binding.parent.glob(".tmp-*")))


if __name__ == "__main__":
    unittest.main()
