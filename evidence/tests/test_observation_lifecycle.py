from dataclasses import replace
from datetime import datetime, timedelta, timezone
import multiprocessing
import os
from pathlib import Path
import queue
import stat
import tempfile
import unittest
from unittest import mock

from wiki_observations import (
    ObservationError,
    ObservationPaths,
    ScopePayload,
    StartRequest,
    finish_observation,
    invalidate_observation,
    parse_completion_payload,
    read_record,
    start_observation,
    validate_record,
)


COMPLETION_TEXT = """## Execution evidence

- Verification: `python3 -m unittest tests.test_observation_lifecycle -v` — pass
- Artifacts: `wiki_observations.py`, `tests/test_observation_lifecycle.py`

## Outcome and observation

- Outcome: Added atomic observation lifecycle transitions.
- Observation: Independent processes serialize finalization through a stable lock.

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 1
defects_found: 0
rework_count: 0
rework_reason: none
```
"""


def _finish_worker(root, run_id, payload_text, finished_at, result_queue):
    paths = ObservationPaths.from_root(Path(root))
    payload = parse_completion_payload(payload_text)
    try:
        finish_observation(
            paths,
            run_id,
            "success",
            payload,
            now=datetime.fromisoformat(finished_at),
        )
    except ObservationError as error:
        result_queue.put("state-error" if error.kind == "state" else error.kind)
    else:
        result_queue.put("finished")


class _WriteFailureTemporary:
    """Real allocated file whose first write fails for cleanup coverage."""

    def __init__(self, directory: Path):
        descriptor, name = tempfile.mkstemp(dir=directory)
        self.name = name
        self._stream = os.fdopen(descriptor, "w", encoding="utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stream.close()

    def fileno(self):
        return self._stream.fileno()

    def write(self, _content):
        raise OSError("write failure")

    def flush(self):
        self._stream.flush()


class _CloseFailureLock:
    def __init__(self, held_lock):
        self._held_lock = held_lock

    def verify(self):
        self._held_lock.verify()

    def close(self):
        self._held_lock.close()
        raise OSError("lock close failure")


class StartLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "wiki" / "tasks").mkdir(parents=True)
        (self.root / "raw").mkdir()
        self.paths = ObservationPaths.from_root(self.root)
        self.started = datetime(
            2026, 7, 13, 10, 0, tzinfo=timezone(timedelta(hours=8))
        )
        self.request = StartRequest(
            title="Implement observation start",
            project="example-project",
            workspace="example-project",
            workspace_id="7f4a1c29e083",
            revision="7316e5b",
            working_tree="dirty",
            agent_surface="codex",
            start_mode="planned",
            task_type="feature",
            workflow_variant="implementation-with-review",
            task_ref=None,
            sources=(),
        )
        self.scope = ScopePayload(
            goal="Implement a secure observation start.",
            included="Draft rendering and atomic exclusive creation.",
            excluded="Finish and invalidation transitions.",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def assert_observation_names(self, expected):
        if not self.paths.observations.exists():
            actual = []
        else:
            actual = sorted(path.name for path in self.paths.observations.iterdir())
        self.assertEqual(sorted(expected), actual)

    def test_same_second_starts_keep_true_timestamp_and_unique_ids(self):
        first = start_observation(self.paths, self.request, self.scope, now=self.started)
        second = start_observation(self.paths, self.request, self.scope, now=self.started)

        self.assertNotEqual(first, second)
        self.assertRegex(first, r"^obs-20260713-100000-[0-9a-f]{6}$")
        self.assertRegex(second, r"^obs-20260713-100000-[0-9a-f]{6}$")
        self.assertEqual(self.started.isoformat(), read_record(self.paths, first)[0]["timestamp"])
        self.assertEqual(self.started.isoformat(), read_record(self.paths, second)[0]["timestamp"])
        self.assert_observation_names([f"{first}.md", f"{second}.md"])

    def test_start_renders_a_valid_immutable_scope_and_safe_frontmatter(self):
        request = replace(
            self.request,
            title='Handle "quoted" path \\ safely',
            task_ref=None,
            sources=(),
        )
        run_id = start_observation(self.paths, request, self.scope, now=self.started)

        metadata, body = read_record(self.paths, run_id)
        self.assertEqual('Handle "quoted" path \\ safely', metadata["title"])
        self.assertEqual("draft", metadata["status"])
        self.assertEqual("planned", metadata["start_mode"])
        self.assertNotIn("subject_root", metadata)
        self.assertNotIn(str(self.root), self.paths.record(run_id).read_text(encoding="utf-8"))
        self.assertEqual([], validate_record(metadata, body, self.paths))
        self.assertEqual(stat.S_IMODE(self.paths.record(run_id).stat().st_mode), 0o644)

    def test_late_start_is_persisted_without_backdating(self):
        late = replace(self.request, start_mode="late")
        exact = self.started.replace(microsecond=987654)

        run_id = start_observation(self.paths, late, self.scope, now=exact)
        metadata, _ = read_record(self.paths, run_id)

        self.assertEqual("late", metadata["start_mode"])
        self.assertEqual(exact.replace(microsecond=0).isoformat(), metadata["timestamp"])
        self.assertRegex(run_id, r"^obs-20260713-100000-[0-9a-f]{6}$")

    def test_start_rejects_naive_now_without_creating_directory(self):
        with self.assertRaisesRegex(ObservationError, "aware") as raised:
            start_observation(
                self.paths,
                self.request,
                self.scope,
                now=datetime(2026, 7, 13, 10, 0),
            )
        self.assertEqual("validation", raised.exception.kind)
        self.assertFalse(self.paths.observations.exists())

    def test_invalid_request_or_scope_is_rejected_before_directory_creation(self):
        invalid_request = replace(self.request, workflow_variant="compile-basic")
        with self.assertRaisesRegex(ObservationError, "invalid taxonomy combination"):
            start_observation(self.paths, invalid_request, self.scope, now=self.started)
        with self.assertRaisesRegex(ObservationError, "Scope"):
            start_observation(
                self.paths,
                self.request,
                ScopePayload("", self.scope.included, self.scope.excluded),
                now=self.started,
            )
        self.assertFalse(self.paths.observations.exists())

    def test_sensitive_persisted_text_is_rejected_before_directory_creation(self):
        cases = (
            (replace(self.request, title="Inspect /Users/alice/private/repo"), self.scope),
            (replace(self.request, title="Inspect Path:/Users/alice/private/repo"), self.scope),
            (replace(self.request, title=r"Inspect \\server\private\repo"), self.scope),
            (replace(self.request, project="api_key=secret-value"), self.scope),
            (replace(self.request, project="token=secret-value"), self.scope),
            (replace(self.request, project="auth_token=secret-value"), self.scope),
            (replace(self.request, project="client_secret=secret-value"), self.scope),
            (replace(self.request, project="aws_access_key_id=AKIAEXAMPLE"), self.scope),
            (replace(self.request, project="aws_secret_access_key=secret-value"), self.scope),
            (
                self.request,
                replace(self.scope, included="Read C:\\Users\\alice\\private.txt"),
            ),
            (
                self.request,
                replace(self.scope, included="Read file:///Users/alice/private.txt"),
            ),
            (
                self.request,
                replace(self.scope, goal="Use https://alice:secret@example.invalid/repo"),
            ),
            (
                self.request,
                replace(self.scope, goal="Use https://secret-token@example.invalid/repo"),
            ),
        )
        for request, scope in cases:
            with self.subTest(title=request.title, scope=scope):
                with self.assertRaisesRegex(ObservationError, "sensitive") as raised:
                    start_observation(self.paths, request, scope, now=self.started)
                self.assertEqual("validation", raised.exception.kind)
        self.assertFalse(self.paths.observations.exists())

    def test_missing_source_or_task_reference_is_rejected_without_a_record(self):
        missing_source = replace(self.request, sources=("raw/missing.md",))
        with self.assertRaisesRegex(ObservationError, "source does not exist") as source_error:
            start_observation(self.paths, missing_source, self.scope, now=self.started)
        self.assertEqual("validation", source_error.exception.kind)

        missing_task = replace(self.request, task_ref="[[missing-task]]")
        with self.assertRaisesRegex(ObservationError, "task_ref points to no task record"):
            start_observation(self.paths, missing_task, self.scope, now=self.started)
        self.assertFalse(self.paths.observations.exists())

    def test_start_temporary_creation_failure_leaves_no_record(self):
        with mock.patch(
            "wiki_observations._create_temporary_file",
            side_effect=OSError("temporary creation failure"),
        ):
            with self.assertRaisesRegex(ObservationError, "temporary creation failure") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    def test_directory_open_cleanup_preserves_original_validation_error(self):
        original = ObservationError("validation", "identity validation failed")
        with mock.patch(
            "wiki_observations._assert_directory_identity", side_effect=original
        ):
            with mock.patch(
                "wiki_observations.os.close", side_effect=OSError("close failure")
            ):
                with self.assertRaisesRegex(
                    ObservationError, "identity validation failed"
                ) as raised:
                    start_observation(
                        self.paths, self.request, self.scope, now=self.started
                    )

        self.assertEqual("validation", raised.exception.kind)
        self.assert_observation_names([])

    def test_temporary_stream_setup_cleanup_preserves_original_error(self):
        with mock.patch(
            "wiki_observations.os.fdopen", side_effect=OSError("fdopen failure")
        ):
            with mock.patch(
                "wiki_observations.os.close", side_effect=OSError("close failure")
            ):
                with self.assertRaisesRegex(
                    ObservationError, "fdopen failure"
                ) as raised:
                    start_observation(
                        self.paths, self.request, self.scope, now=self.started
                    )

        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    def test_start_write_failure_cleans_allocated_temporary(self):
        self.paths.observations.mkdir(parents=True)

        def failing_temporary(_directory_fd):
            temporary = _WriteFailureTemporary(self.paths.observations)
            return Path(temporary.name).name, temporary

        with mock.patch(
            "wiki_observations._create_temporary_file",
            side_effect=failing_temporary,
        ):
            with self.assertRaisesRegex(ObservationError, "write failure") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    def test_start_fsync_failure_leaves_no_record_or_temporary(self):
        with mock.patch("wiki_observations.os.fsync", side_effect=OSError("disk failure")):
            with self.assertRaisesRegex(ObservationError, "disk failure") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    def test_directory_fsync_failure_rolls_back_claim_and_temporary(self):
        fsync_calls = 0

        def fail_directory_fsync(_descriptor):
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("directory fsync failure")

        with mock.patch(
            "wiki_observations.os.fsync", side_effect=fail_directory_fsync
        ):
            with self.assertRaisesRegex(
                ObservationError, "directory fsync failure"
            ) as raised:
                start_observation(
                    self.paths, self.request, self.scope, now=self.started
                )

        self.assertEqual("io", raised.exception.kind)
        self.assertEqual(3, fsync_calls)
        self.assert_observation_names([])

    def test_start_link_collision_retries_and_cleans_each_temporary(self):
        self.paths.observations.mkdir(parents=True)
        occupied = self.paths.observations / "obs-20260713-100000-a1b2c3.md"
        occupied.write_text("occupied", encoding="utf-8")
        with mock.patch(
            "wiki_observations.secrets.token_hex", side_effect=["a1b2c3", "d4e5f6"]
        ):
            run_id = start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("obs-20260713-100000-d4e5f6", run_id)
        self.assertEqual("occupied", occupied.read_text(encoding="utf-8"))
        self.assert_observation_names([occupied.name, f"{run_id}.md"])

    def test_start_exhausted_collisions_is_io_and_leaves_no_temporary(self):
        self.paths.observations.mkdir(parents=True)
        occupied = self.paths.observations / "obs-20260713-100000-a1b2c3.md"
        occupied.write_text("occupied", encoding="utf-8")
        with mock.patch("wiki_observations.secrets.token_hex", return_value="a1b2c3"):
            with self.assertRaisesRegex(ObservationError, "unique run id") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([occupied.name])

    def test_start_noncollision_link_error_is_io_and_cleans_temporary(self):
        with mock.patch("wiki_observations.os.link", side_effect=PermissionError("denied")):
            with self.assertRaisesRegex(ObservationError, "denied") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    def test_preclaim_cleanup_fallback_preserves_original_io_error(self):
        real_unlink = os.unlink
        cleanup_calls = 0

        def fail_first_unlink(path, *args, **kwargs):
            nonlocal cleanup_calls
            cleanup_calls += 1
            if cleanup_calls == 1:
                raise PermissionError("cleanup denied")
            return real_unlink(path, *args, **kwargs)

        with mock.patch("wiki_observations.os.link", side_effect=PermissionError("link denied")):
            with mock.patch("wiki_observations.os.unlink", side_effect=fail_first_unlink):
                with self.assertRaisesRegex(ObservationError, "link denied"):
                    start_observation(self.paths, self.request, self.scope, now=self.started)
        self.assert_observation_names([])

    def test_postclaim_cleanup_fallback_does_not_turn_success_into_failure(self):
        real_unlink = os.unlink
        cleanup_calls = 0

        def fail_first_unlink(path, *args, **kwargs):
            nonlocal cleanup_calls
            cleanup_calls += 1
            if cleanup_calls == 1:
                raise PermissionError("cleanup denied")
            return real_unlink(path, *args, **kwargs)

        with mock.patch("wiki_observations.os.unlink", side_effect=fail_first_unlink):
            run_id = start_observation(self.paths, self.request, self.scope, now=self.started)

        self.assertEqual("draft", read_record(self.paths, run_id)[0]["status"])
        self.assert_observation_names([f"{run_id}.md"])

    def test_descriptor_close_failure_does_not_turn_commit_into_failure(self):
        with mock.patch(
            "wiki_observations.os.close", side_effect=OSError("close failure")
        ):
            run_id = start_observation(
                self.paths, self.request, self.scope, now=self.started
            )

        self.assertEqual("draft", read_record(self.paths, run_id)[0]["status"])
        self.assert_observation_names([f"{run_id}.md"])

    def test_descriptor_close_failure_does_not_mask_precommit_error(self):
        with mock.patch(
            "wiki_observations.os.link", side_effect=PermissionError("link denied")
        ):
            with mock.patch(
                "wiki_observations.os.close", side_effect=OSError("close failure")
            ):
                with self.assertRaisesRegex(ObservationError, "link denied") as raised:
                    start_observation(
                        self.paths, self.request, self.scope, now=self.started
                    )

        self.assertEqual("io", raised.exception.kind)
        self.assert_observation_names([])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_start_rejects_observation_directory_symlink_created_after_path_setup(self):
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)
            os.symlink(outside, self.paths.observations)
            with self.assertRaisesRegex(ObservationError, "symlink") as raised:
                start_observation(self.paths, self.request, self.scope, now=self.started)
            self.assertEqual("validation", raised.exception.kind)
            self.assertEqual([], list(outside.iterdir()))

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_start_rejects_directory_swap_between_validation_and_temporary_write(self):
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)
            original = self.paths.observations.with_name("observations-original")

            def swap_directory(directory_fd):
                self.paths.observations.rename(original)
                os.symlink(outside, self.paths.observations)
                return wiki_create_temporary(directory_fd)

            import wiki_observations

            wiki_create_temporary = wiki_observations._create_temporary_file

            with mock.patch(
                "wiki_observations._create_temporary_file",
                side_effect=swap_directory,
            ):
                with self.assertRaisesRegex(ObservationError, "directory changed"):
                    start_observation(self.paths, self.request, self.scope, now=self.started)

            self.assertEqual([], list(outside.iterdir()))
            self.assertEqual([], list(original.iterdir()))

    def test_prelink_directory_swap_cleans_held_directory_by_descriptor(self):
        import wiki_observations

        real_assert_identity = wiki_observations._assert_directory_identity
        original = self.paths.observations.with_name("observations-original")
        identity_calls = 0

        def swap_after_temporary_write(directory_fd, path):
            nonlocal identity_calls
            identity_calls += 1
            if identity_calls == 4:
                path.rename(original)
                path.mkdir()
            return real_assert_identity(directory_fd, path)

        with mock.patch(
            "wiki_observations._assert_directory_identity",
            side_effect=swap_after_temporary_write,
        ):
            with self.assertRaisesRegex(ObservationError, "directory changed"):
                start_observation(
                    self.paths, self.request, self.scope, now=self.started
                )

        self.assertEqual([], list(self.paths.observations.iterdir()))
        self.assertEqual([], list(original.iterdir()))


class FinishLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "wiki" / "tasks").mkdir(parents=True)
        (self.root / "raw").mkdir()
        self.paths = ObservationPaths.from_root(self.root)
        self.started = datetime(
            2026, 7, 13, 10, 0, tzinfo=timezone(timedelta(hours=8))
        )
        self.finished = self.started + timedelta(seconds=90)
        self.request = StartRequest(
            title="Implement atomic observation finish",
            project="example-project",
            workspace="example-project",
            workspace_id="7f4a1c29e083",
            revision="7316e5b",
            working_tree="dirty",
            agent_surface="codex",
            start_mode="planned",
            task_type="feature",
            workflow_variant="implementation-with-review",
            task_ref=None,
            sources=(),
        )
        self.scope = ScopePayload(
            goal="Implement atomic final transitions.",
            included="Finish, supersession, and invalidation.",
            excluded="CLI and reporting integration.",
        )
        self.payload = parse_completion_payload(COMPLETION_TEXT)

    def tearDown(self):
        self.temporary.cleanup()

    def create_draft(self):
        return start_observation(
            self.paths, self.request, self.scope, now=self.started
        )

    def create_finished(self, status="success", payload=None, superseded_by=None):
        run_id = self.create_draft()
        finish_observation(
            self.paths,
            run_id,
            status,
            payload or self.payload,
            superseded_by=superseded_by,
            now=self.finished,
        )
        return run_id

    def assert_no_lifecycle_temporaries(self):
        for directory in (self.paths.observations, self.paths.invalidations):
            if directory.exists():
                self.assertEqual([], list(directory.glob(".observation-*.tmp")))

    def test_success_finish_preserves_scope_and_adds_derived_metrics(self):
        run_id = self.create_draft()

        finish_observation(
            self.paths, run_id, "success", self.payload, now=self.finished
        )

        metadata, body = read_record(self.paths, run_id)
        self.assertEqual("success", metadata["status"])
        self.assertNotIn("superseded_by", metadata)
        self.assertIn("- Goal: Implement atomic final transitions.", body)
        self.assertIn('finished_at: "2026-07-13T10:01:30+08:00"', body)
        self.assertIn("elapsed_seconds: 90", body)
        self.assertEqual([], validate_record(metadata, body, self.paths))
        self.assertEqual(0o644, stat.S_IMODE(self.paths.record(run_id).stat().st_mode))
        self.assert_no_lifecycle_temporaries()

    def test_final_status_rules_and_supersession_are_enforced(self):
        partial = replace(
            self.payload,
            outcome=(
                "Added a usable subset. Incomplete Included items: invalidation."
            ),
            follow_up="Implement the remaining invalidation path.",
        )
        failed = replace(
            self.payload,
            outcome="No deliverable met Scope.",
            verification="fail",
        )
        for status, payload in (
            ("partial", partial),
            ("failed", failed),
            ("rolled-back", failed),
        ):
            with self.subTest(status=status):
                run_id = self.create_finished(status=status, payload=payload)
                metadata, body = read_record(self.paths, run_id)
                self.assertEqual(status, metadata["status"])
                self.assertEqual([], validate_record(metadata, body, self.paths))

        replacement = self.create_draft()
        superseded = self.create_finished(
            status="superseded", superseded_by=replacement
        )
        metadata, body = read_record(self.paths, superseded)
        self.assertEqual(replacement, metadata["superseded_by"])
        self.assertEqual([], validate_record(metadata, body, self.paths))

        with self.assertRaisesRegex(ObservationError, "requires superseded_by"):
            finish_observation(
                self.paths,
                self.create_draft(),
                "superseded",
                self.payload,
                now=self.finished,
            )
        with self.assertRaisesRegex(ObservationError, "only valid"):
            finish_observation(
                self.paths,
                self.create_draft(),
                "success",
                self.payload,
                superseded_by=replacement,
                now=self.finished,
            )

    def test_final_record_cannot_be_finished_twice(self):
        run_id = self.create_finished()
        before = self.paths.record(run_id).read_bytes()

        with self.assertRaisesRegex(ObservationError, "already final") as raised:
            finish_observation(
                self.paths, run_id, "failed", self.payload, now=self.finished
            )

        self.assertEqual("state", raised.exception.kind)
        self.assertEqual(before, self.paths.record(run_id).read_bytes())

    def test_only_one_competing_finish_succeeds(self):
        run_id = self.create_draft()
        context = multiprocessing.get_context("spawn")
        result_queue = context.Queue()
        processes = [
            context.Process(
                target=_finish_worker,
                args=(
                    str(self.root),
                    run_id,
                    COMPLETION_TEXT,
                    self.finished.isoformat(),
                    result_queue,
                ),
            )
            for _ in range(2)
        ]
        try:
            for process in processes:
                process.start()
            for process in processes:
                process.join(10)
                if process.is_alive():
                    process.terminate()
                    process.join(2)
                    self.fail("competing finish process timed out")
                self.assertEqual(0, process.exitcode)
            outcomes = [result_queue.get(timeout=2) for _ in processes]
        except queue.Empty:
            self.fail("competing finish process returned no outcome")
        finally:
            result_queue.close()
            result_queue.join_thread()

        self.assertEqual(1, outcomes.count("finished"), outcomes)
        self.assertEqual(1, outcomes.count("state-error"), outcomes)
        self.assertEqual("success", read_record(self.paths, run_id)[0]["status"])

    def test_stale_draft_can_still_be_finished_without_backdating(self):
        run_id = self.create_draft()
        much_later = self.started + timedelta(hours=25, seconds=5)

        finish_observation(
            self.paths, run_id, "success", self.payload, now=much_later
        )

        metadata, body = read_record(self.paths, run_id)
        self.assertEqual("success", metadata["status"])
        self.assertIn("elapsed_seconds: 90005", body)

    def test_invalid_finish_time_or_status_leaves_draft_unchanged(self):
        for status, now, message in (
            ("unknown", self.finished, "status"),
            ("success", self.started - timedelta(seconds=1), "earlier"),
            ("success", datetime(2026, 7, 13, 10, 1), "aware"),
        ):
            with self.subTest(status=status, now=now):
                run_id = self.create_draft()
                before = self.paths.record(run_id).read_bytes()
                with self.assertRaisesRegex(ObservationError, message):
                    finish_observation(
                        self.paths, run_id, status, self.payload, now=now
                    )
                self.assertEqual(before, self.paths.record(run_id).read_bytes())

    def test_finish_write_or_replace_failure_leaves_valid_draft(self):
        run_id = self.create_draft()
        before = self.paths.record(run_id).read_bytes()
        with mock.patch(
            "wiki_observations.os.fsync", side_effect=OSError("finish fsync failure")
        ):
            with self.assertRaisesRegex(ObservationError, "finish fsync failure") as raised:
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )
        self.assertEqual("io", raised.exception.kind)
        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        self.assert_no_lifecycle_temporaries()

        with mock.patch(
            "wiki_observations.os.replace", side_effect=OSError("replace failure")
        ):
            with self.assertRaisesRegex(ObservationError, "replace failure"):
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )
        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        self.assert_no_lifecycle_temporaries()

    def test_finish_directory_fsync_failure_restores_original_draft(self):
        run_id = self.create_draft()
        before = self.paths.record(run_id).read_bytes()
        fsync_calls = 0

        def fail_first_directory_fsync(_descriptor):
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("finish directory fsync failure")

        with mock.patch(
            "wiki_observations.os.fsync", side_effect=fail_first_directory_fsync
        ):
            with self.assertRaisesRegex(
                ObservationError, "finish directory fsync failure"
            ) as raised:
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )

        self.assertEqual("io", raised.exception.kind)
        self.assertGreaterEqual(fsync_calls, 3)
        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        self.assert_no_lifecycle_temporaries()

    def test_finish_reports_uncertain_final_state_if_rollback_itself_fails(self):
        import wiki_observations

        run_id = self.create_draft()
        real_replace = wiki_observations.os.replace
        fsync_calls = 0
        replace_calls = 0

        def fail_directory_fsync(_descriptor):
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("finish directory fsync failure")

        def fail_rollback_replace(*args, **kwargs):
            nonlocal replace_calls
            replace_calls += 1
            if replace_calls == 2:
                raise OSError("rollback replace failure")
            return real_replace(*args, **kwargs)

        with mock.patch(
            "wiki_observations.os.fsync", side_effect=fail_directory_fsync
        ):
            with mock.patch(
                "wiki_observations.os.replace", side_effect=fail_rollback_replace
            ):
                with self.assertRaisesRegex(
                    ObservationError, "could not roll back failed finish"
                ) as raised:
                    finish_observation(
                        self.paths,
                        run_id,
                        "success",
                        self.payload,
                        now=self.finished,
                    )

        self.assertEqual("io", raised.exception.kind)
        metadata, body = read_record(self.paths, run_id)
        self.assertEqual("success", metadata["status"])
        self.assertEqual([], validate_record(metadata, body, self.paths))
        self.assert_no_lifecycle_temporaries()

    def test_direct_malformed_payload_is_typed_validation_and_keeps_draft(self):
        run_id = self.create_draft()
        before = self.paths.record(run_id).read_bytes()
        for malformed in (
            replace(self.payload, review_rounds=None),
            replace(self.payload, outcome=None),
            replace(self.payload, verification=[]),
        ):
            with self.subTest(payload=malformed):
                with self.assertRaises(ObservationError) as raised:
                    finish_observation(
                        self.paths, run_id, "success", malformed, now=self.finished
                    )
                self.assertEqual("validation", raised.exception.kind)
                self.assertEqual(before, self.paths.record(run_id).read_bytes())

    def test_record_fdopen_failure_is_typed_and_closes_descriptor(self):
        import wiki_observations

        run_id = self.create_draft()
        directory_fd = wiki_observations._open_observation_directory(
            self.paths.observations
        )
        real_close = os.close
        try:
            with mock.patch(
                "wiki_observations.os.fdopen",
                side_effect=OSError("record fdopen failure"),
            ):
                with mock.patch(
                    "wiki_observations.os.close", wraps=real_close
                ) as close_mock:
                    with self.assertRaisesRegex(
                        ObservationError, "record fdopen failure"
                    ) as raised:
                        wiki_observations._read_record_from_directory(
                            self.paths, run_id, directory_fd
                        )
            self.assertEqual("io", raised.exception.kind)
            self.assertGreaterEqual(close_mock.call_count, 1)
        finally:
            real_close(directory_fd)

    def test_lock_setup_and_close_failures_keep_typed_commit_semantics(self):
        run_id = self.create_draft()
        with mock.patch(
            "wiki_observations.os.fdopen", side_effect=OSError("lock fdopen failure")
        ):
            with self.assertRaisesRegex(
                ObservationError, "lock fdopen failure"
            ) as raised:
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )
        self.assertEqual("io", raised.exception.kind)
        self.assertEqual("draft", read_record(self.paths, run_id)[0]["status"])

        import wiki_observations

        real_open_run_lock = wiki_observations._open_run_lock

        def open_with_close_failure(*args, **kwargs):
            secure_paths, held_lock = real_open_run_lock(*args, **kwargs)
            return secure_paths, _CloseFailureLock(held_lock)

        with mock.patch(
            "wiki_observations._open_run_lock",
            side_effect=open_with_close_failure,
        ):
            finish_observation(
                self.paths, run_id, "success", self.payload, now=self.finished
            )
        self.assertEqual("success", read_record(self.paths, run_id)[0]["status"])

        with mock.patch(
            "wiki_observations._open_run_lock",
            side_effect=open_with_close_failure,
        ):
            with self.assertRaisesRegex(ObservationError, "already final") as state_error:
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )
        self.assertEqual("state", state_error.exception.kind)

        invalidated_run = self.create_finished()
        with mock.patch(
            "wiki_observations._open_run_lock",
            side_effect=open_with_close_failure,
        ):
            invalidate_observation(
                self.paths,
                invalidated_run,
                "invalid fixture",
                now=self.finished,
            )
        self.assertTrue(self.paths.invalidation(invalidated_run).exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_finish_never_creates_locks_through_swapped_observations_path(self):
        run_id = self.create_draft()
        original = self.paths.observations.with_name("observations-original")
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)
            self.paths.observations.rename(original)
            os.symlink(outside, self.paths.observations)

            with self.assertRaisesRegex(ObservationError, "symlink"):
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )

            self.assertEqual([], list(outside.iterdir()))

    def test_lock_directory_swap_after_flock_is_rejected_before_finish(self):
        import wiki_observations

        run_id = self.create_draft()
        before = self.paths.record(run_id).read_bytes()
        real_flock = wiki_observations.fcntl.flock
        original_locks = self.paths.locks.with_name("locks-original")

        def flock_then_swap(*args, **kwargs):
            result = real_flock(*args, **kwargs)
            self.paths.locks.rename(original_locks)
            self.paths.locks.mkdir()
            return result

        with mock.patch(
            "wiki_observations.fcntl.flock", side_effect=flock_then_swap
        ):
            with self.assertRaisesRegex(ObservationError, "directory changed"):
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )

        self.assertEqual(before, self.paths.record(run_id).read_bytes())

    def test_lock_entry_swap_after_flock_is_rejected_before_finish(self):
        import wiki_observations

        run_id = self.create_draft()
        before = self.paths.record(run_id).read_bytes()
        real_flock = wiki_observations.fcntl.flock

        def flock_then_replace_entry(*args, **kwargs):
            result = real_flock(*args, **kwargs)
            lock_path = self.paths.locks / f"{run_id}.lock"
            lock_path.unlink()
            lock_path.write_text("replacement", encoding="utf-8")
            return result

        with mock.patch(
            "wiki_observations.fcntl.flock", side_effect=flock_then_replace_entry
        ):
            with self.assertRaisesRegex(ObservationError, "lock changed"):
                finish_observation(
                    self.paths, run_id, "success", self.payload, now=self.finished
                )

        self.assertEqual(before, self.paths.record(run_id).read_bytes())

    def test_draft_cannot_be_invalidated(self):
        run_id = self.create_draft()
        with self.assertRaisesRegex(ObservationError, "still draft") as raised:
            invalidate_observation(
                self.paths, run_id, "invalid fixture", now=self.finished
            )
        self.assertEqual("state", raised.exception.kind)
        self.assertFalse(self.paths.invalidations.exists())

    def test_invalidation_preserves_original_and_has_fixed_schema(self):
        import wiki_observations

        run_id = self.create_finished()
        before = self.paths.record(run_id).read_bytes()

        invalidate_observation(
            self.paths, run_id, "invalid fixture", now=self.finished + timedelta(minutes=1)
        )

        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        tombstone = self.paths.invalidation(run_id)
        metadata, body = wiki_observations._parse_frontmatter(
            tombstone.read_text(encoding="utf-8")
        )
        self.assertEqual("observation-invalidation", metadata["type"])
        self.assertEqual(run_id, metadata["target_run_id"])
        self.assertEqual("invalid fixture", metadata["reason"])
        self.assertEqual([], metadata["sources"])
        self.assertEqual("", body)
        self.assertEqual(0o644, stat.S_IMODE(tombstone.stat().st_mode))
        self.assert_no_lifecycle_temporaries()

    def test_invalidation_is_exclusive_and_validates_inputs(self):
        run_id = self.create_finished()
        invalidate_observation(
            self.paths, run_id, "invalid fixture", now=self.finished
        )
        before = self.paths.invalidation(run_id).read_bytes()
        with self.assertRaisesRegex(ObservationError, "already invalidated") as raised:
            invalidate_observation(
                self.paths, run_id, "second reason", now=self.finished
            )
        self.assertEqual("state", raised.exception.kind)
        self.assertEqual(before, self.paths.invalidation(run_id).read_bytes())

        for reason, now, message in (
            ("token=secret-value", self.finished, "sensitive"),
            ("valid reason", datetime(2026, 7, 13, 10, 2), "aware"),
        ):
            other = self.create_finished()
            with self.subTest(reason=reason, now=now):
                with self.assertRaisesRegex(ObservationError, message):
                    invalidate_observation(self.paths, other, reason, now=now)
                self.assertFalse(self.paths.invalidation(other).exists())

    def test_invalidation_claim_failure_leaves_original_and_no_tombstone(self):
        run_id = self.create_finished()
        before = self.paths.record(run_id).read_bytes()
        with mock.patch(
            "wiki_observations.os.link", side_effect=PermissionError("claim denied")
        ):
            with self.assertRaisesRegex(ObservationError, "claim denied") as raised:
                invalidate_observation(
                    self.paths, run_id, "invalid fixture", now=self.finished
                )

        self.assertEqual("io", raised.exception.kind)
        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        self.assertFalse(self.paths.invalidation(run_id).exists())
        self.assert_no_lifecycle_temporaries()

    def test_invalidation_directory_fsync_failure_rolls_back_tombstone(self):
        run_id = self.create_finished()
        before = self.paths.record(run_id).read_bytes()
        fsync_calls = 0

        def fail_directory_fsync(_descriptor):
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("invalidation directory fsync failure")

        with mock.patch(
            "wiki_observations.os.fsync", side_effect=fail_directory_fsync
        ):
            with self.assertRaisesRegex(
                ObservationError, "invalidation directory fsync failure"
            ) as raised:
                invalidate_observation(
                    self.paths, run_id, "invalid fixture", now=self.finished
                )

        self.assertEqual("io", raised.exception.kind)
        self.assertGreaterEqual(fsync_calls, 3)
        self.assertEqual(before, self.paths.record(run_id).read_bytes())
        self.assertFalse(self.paths.invalidation(run_id).exists())
        self.assert_no_lifecycle_temporaries()

    def test_invalidation_rollback_failure_has_operation_specific_error(self):
        run_id = self.create_finished()
        fsync_calls = 0

        def fail_claim_and_rollback_fsync(_descriptor):
            nonlocal fsync_calls
            fsync_calls += 1
            if fsync_calls == 2:
                raise OSError("invalidation directory fsync failure")
            if fsync_calls == 3:
                raise OSError("invalidation rollback fsync failure")

        with mock.patch(
            "wiki_observations.os.fsync",
            side_effect=fail_claim_and_rollback_fsync,
        ):
            with self.assertRaisesRegex(
                ObservationError, "could not roll back failed invalidation"
            ) as raised:
                invalidate_observation(
                    self.paths, run_id, "invalid fixture", now=self.finished
                )

        self.assertEqual("io", raised.exception.kind)
        self.assertFalse(self.paths.invalidation(run_id).exists())
        self.assert_no_lifecycle_temporaries()

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_invalidation_never_creates_directory_through_postread_swap(self):
        import wiki_observations

        run_id = self.create_finished()
        original = self.paths.observations.with_name("observations-original")
        real_read = wiki_observations._read_record_from_directory
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)

            def read_then_swap(*args, **kwargs):
                result = real_read(*args, **kwargs)
                self.paths.observations.rename(original)
                os.symlink(outside, self.paths.observations)
                return result

            with mock.patch(
                "wiki_observations._read_record_from_directory",
                side_effect=read_then_swap,
            ):
                with self.assertRaisesRegex(ObservationError, "directory changed"):
                    invalidate_observation(
                        self.paths, run_id, "invalid fixture", now=self.finished
                    )

            self.assertEqual([], list(outside.iterdir()))


if __name__ == "__main__":
    unittest.main()
