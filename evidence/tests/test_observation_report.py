from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from wiki_observations import (
    CompletionPayload,
    ObservationError,
    ObservationPaths,
    ReportFilters,
    ScopePayload,
    StartRequest,
    collect_records,
    finish_observation,
    invalidate_observation,
    render_observation_report,
    start_observation,
)


def record(
    run_id,
    status,
    *,
    project="project-a",
    workspace="workspace-a",
    workspace_id="111111111111",
    task_type="feature",
    workflow_variant="implementation-with-review",
    timestamp="2026-07-14T08:00:00+08:00",
    elapsed="60",
    defects="0",
    rework="0",
    review_rounds="1",
):
    metrics = {}
    if status != "draft":
        metrics = {
            "elapsed_seconds": elapsed,
            "defects_found": defects,
            "rework_count": rework,
            "review_rounds": review_rounds,
        }
    return {
        "run_id": run_id,
        "status": status,
        "project": project,
        "workspace": workspace,
        "workspace_id": workspace_id,
        "task_type": task_type,
        "workflow_variant": workflow_variant,
        "timestamp": timestamp,
        "metrics": metrics,
    }


class ObservationReportTests(unittest.TestCase):
    def setUp(self):
        self.local_timezone = timezone(timedelta(hours=8))
        self.now = datetime(2026, 7, 14, 12, 0, tzinfo=self.local_timezone)

    def test_report_excludes_unknown_superseded_and_invalidated_from_rate(self):
        records = [
            record("ok", "success", elapsed="60"),
            record("partial", "partial", elapsed="unknown"),
            record("old", "superseded", elapsed="5"),
            record("bad", "success", elapsed="10"),
        ]

        report = render_observation_report(
            records, {"bad"}, ReportFilters(), now=self.now
        )

        self.assertIn("Success rate: 1/2 (50.0%)", report)
        self.assertIn("Average elapsed seconds: 60", report)
        self.assertIn("Missing elapsed seconds: 1", report)
        self.assertIn("Invalidated: 1", report)
        self.assertIn("small sample (n=3)", report)

    def test_invalidated_records_only_contribute_to_invalidation_count(self):
        report = render_observation_report(
            [record("invalid", "success")],
            {"invalid"},
            ReportFilters(),
            now=self.now,
        )

        self.assertIn("Samples: 0", report)
        self.assertIn("Invalidated: 1", report)
        self.assertIn("Status counts: none", report)
        self.assertIn("Success rate: 0/0", report)
        self.assertNotIn("success=1", report)

    def test_numeric_aggregates_preserve_unknown_as_missing(self):
        records = [
            record("one", "success", defects="2", rework="3", review_rounds="2"),
            record(
                "two",
                "failed",
                elapsed="unknown",
                defects="unknown",
                rework="unknown",
                review_rounds="unknown",
            ),
        ]

        report = render_observation_report(
            records, set(), ReportFilters(), now=self.now
        )

        self.assertIn("Total defects found: 2", report)
        self.assertIn("Missing defects found: 1", report)
        self.assertIn("Total rework count: 3", report)
        self.assertIn("Average rework count: 3", report)
        self.assertIn("Missing rework count: 1", report)
        self.assertIn("Average review rounds: 2", report)
        self.assertIn("Missing metric values: 4/8 (50.0%)", report)

        unknown_report = render_observation_report(
            [
                record(
                    "unknown",
                    "failed",
                    elapsed="unknown",
                    defects="unknown",
                    rework="unknown",
                    review_rounds="unknown",
                )
            ],
            set(),
            ReportFilters(),
            now=self.now,
        )
        self.assertIn("Total defects found: unknown", unknown_report)
        self.assertIn("Total rework count: unknown", unknown_report)

    def test_filters_and_grouping_are_exact_and_deterministic(self):
        records = [
            record("z", "success", project="zeta", workspace_id="222222222222"),
            record("a", "failed", project="alpha", workspace_id="111111111111"),
            record(
                "a-query",
                "success",
                project="alpha",
                workspace_id="111111111111",
                task_type="query",
                workflow_variant="research-basic",
            ),
        ]

        report = render_observation_report(
            records,
            set(),
            ReportFilters(project="alpha", task_type="query", status="success"),
            now=self.now,
        )

        self.assertIn("Project: alpha", report)
        self.assertIn("Task type: query", report)
        self.assertNotIn("Project: zeta", report)
        self.assertNotIn("Task type: feature", report)
        self.assertNotIn("best", report.lower())
        self.assertNotIn("recommend", report.lower())

        unfiltered = render_observation_report(
            records, set(), ReportFilters(), now=self.now
        )
        self.assertLess(unfiltered.index("Project: alpha"), unfiltered.index("Project: zeta"))

    def test_case_collisions_have_input_order_independent_output(self):
        records = [
            record("upper", "success", project="Alpha", workspace="Workspace"),
            record("lower", "success", project="alpha", workspace="workspace"),
        ]

        forward = render_observation_report(
            records, set(), ReportFilters(), now=self.now
        )
        reverse = render_observation_report(
            list(reversed(records)), set(), ReportFilters(), now=self.now
        )

        self.assertEqual(forward, reverse)

    def test_unhashable_taxonomy_filters_are_typed_validation_errors(self):
        for filters in (ReportFilters(task_type=[]), ReportFilters(status=[])):
            with self.subTest(filters=filters):
                with self.assertRaises(ObservationError) as raised:
                    render_observation_report([], set(), filters, now=self.now)
                self.assertEqual("validation", raised.exception.kind)

    def test_local_date_filters_are_inclusive_at_both_boundaries(self):
        records = [
            record("before", "success", timestamp="2026-07-01T15:59:59+00:00"),
            record("since", "success", timestamp="2026-07-01T16:00:00+00:00"),
            record("until", "success", timestamp="2026-07-02T15:59:59+00:00"),
            record("after", "success", timestamp="2026-07-02T16:00:00+00:00"),
        ]

        report = render_observation_report(
            records,
            set(),
            ReportFilters(since=date(2026, 7, 2), until=date(2026, 7, 2)),
            now=self.now,
        )

        self.assertIn("Samples: 2", report)
        self.assertIn("Success rate: 2/2 (100.0%)", report)

    def test_draft_ages_and_stale_threshold_are_reported(self):
        records = [
            record("stale", "draft", timestamp="2026-07-13T11:59:59+08:00"),
            record("fresh", "draft", timestamp="2026-07-13T12:00:01+08:00"),
        ]

        report = render_observation_report(
            records, set(), ReportFilters(), now=self.now
        )

        self.assertIn("Drafts: 2", report)
        self.assertIn("Stale drafts (>24h): 1", report)
        self.assertIn("stale=24.0h (stale)", report)
        self.assertIn("fresh=24.0h", report)
        self.assertNotIn("fresh=24.0h (stale)", report)
        self.assertIn("small sample (n=0)", report)

    def test_empty_filter_result_is_explicit(self):
        report = render_observation_report(
            [record("one", "success")],
            set(),
            ReportFilters(project="missing"),
            now=self.now,
        )

        self.assertIn("No observation records matched the filters.", report)
        self.assertIn("Invalidated: 0", report)


class CollectRecordsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "wiki" / "tasks").mkdir(parents=True)
        (self.root / "raw").mkdir()
        self.paths = ObservationPaths.from_root(self.root)
        self.started = datetime(
            2026, 7, 14, 8, 0, tzinfo=timezone(timedelta(hours=8))
        )
        self.request = StartRequest(
            title="Report observations",
            project="project-a",
            workspace="workspace-a",
            workspace_id="111111111111",
            revision="7316e5b",
            working_tree="clean",
            agent_surface="codex",
            start_mode="planned",
            task_type="feature",
            workflow_variant="implementation-with-review",
            task_ref=None,
            sources=(),
        )
        self.scope = ScopePayload("Report records.", "Read-only aggregation.", "None.")
        self.completion = CompletionPayload(
            "tests passed",
            "wiki_observations.py",
            "Reporting completed.",
            "Missing values remain explicit.",
            "None — no further action",
            "pass",
            1,
            0,
            0,
            "none",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def test_collects_direct_valid_records_metrics_and_tombstones(self):
        final_run = start_observation(
            self.paths, self.request, self.scope, now=self.started
        )
        finish_observation(
            self.paths,
            final_run,
            "success",
            self.completion,
            now=self.started + timedelta(seconds=90),
        )
        invalidate_observation(
            self.paths,
            final_run,
            "invalid fixture",
            now=self.started + timedelta(minutes=2),
        )
        draft_run = start_observation(
            self.paths,
            self.request,
            self.scope,
            now=self.started + timedelta(minutes=3),
        )
        (self.paths.locks / "shadow.md").write_text("ignored", encoding="utf-8")

        records, invalidated = collect_records(self.paths)

        self.assertEqual({final_run, draft_run}, {row["run_id"] for row in records})
        self.assertEqual({final_run}, invalidated)
        final = next(row for row in records if row["run_id"] == final_run)
        self.assertEqual(90, final["metrics"]["elapsed_seconds"])
        draft = next(row for row in records if row["run_id"] == draft_run)
        self.assertEqual({}, draft["metrics"])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_collect_rejects_symlink_records_without_following_them(self):
        self.paths.observations.mkdir(parents=True)
        outside = self.root / "outside.md"
        outside.write_text("outside", encoding="utf-8")
        os.symlink(outside, self.paths.observations / "obs-20260714-080000-aaaaaa.md")

        with self.assertRaisesRegex(Exception, "symlink"):
            collect_records(self.paths)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "FIFO fixtures unavailable")
    def test_collect_rejects_special_record_and_tombstone_files(self):
        self.paths.observations.mkdir(parents=True)
        record_fifo = self.paths.record("obs-20260714-080000-aaaaaa")
        os.mkfifo(record_fifo)
        with self.assertRaisesRegex(ObservationError, "regular file"):
            collect_records(self.paths)
        record_fifo.unlink()

        final_run = start_observation(
            self.paths, self.request, self.scope, now=self.started
        )
        finish_observation(
            self.paths,
            final_run,
            "success",
            self.completion,
            now=self.started + timedelta(seconds=90),
        )
        self.paths.invalidations.mkdir()
        tombstone_fifo = self.paths.invalidation(final_run)
        os.mkfifo(tombstone_fifo)
        with self.assertRaisesRegex(ObservationError, "regular file"):
            collect_records(self.paths)

    def test_record_swap_to_symlink_is_rejected_at_descriptor_open(self):
        run_id = start_observation(
            self.paths, self.request, self.scope, now=self.started
        )
        record_path = self.paths.record(run_id)
        backup = self.paths.observations / f"{run_id}.backup"
        outside = self.root / "outside-record.md"
        outside.write_text(record_path.read_text(encoding="utf-8"), encoding="utf-8")
        real_open = os.open
        swapped = False

        def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal swapped
            if path == record_path.name and dir_fd is not None and not swapped:
                swapped = True
                record_path.rename(backup)
                os.symlink(outside, record_path)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with mock.patch("wiki_observations.os.open", side_effect=swap_before_open):
            with self.assertRaisesRegex(ObservationError, "changed"):
                collect_records(self.paths)
        self.assertTrue(swapped)

    def test_tombstone_swap_to_symlink_is_rejected_at_descriptor_open(self):
        run_id = start_observation(
            self.paths, self.request, self.scope, now=self.started
        )
        finish_observation(
            self.paths,
            run_id,
            "success",
            self.completion,
            now=self.started + timedelta(seconds=90),
        )
        invalidate_observation(
            self.paths,
            run_id,
            "invalid fixture",
            now=self.started + timedelta(minutes=2),
        )
        tombstone = self.paths.invalidation(run_id)
        backup = self.paths.invalidations / f"{run_id}.backup"
        outside = self.root / "outside-tombstone.md"
        outside.write_text(tombstone.read_text(encoding="utf-8"), encoding="utf-8")
        invalidations_identity = os.stat(self.paths.invalidations)
        real_open = os.open
        swapped = False

        def swap_before_open(path, flags, mode=0o777, *, dir_fd=None):
            nonlocal swapped
            if path == tombstone.name and dir_fd is not None and not swapped:
                held = os.fstat(dir_fd)
                if (held.st_dev, held.st_ino) == (
                    invalidations_identity.st_dev,
                    invalidations_identity.st_ino,
                ):
                    swapped = True
                    tombstone.rename(backup)
                    os.symlink(outside, tombstone)
            return real_open(path, flags, mode, dir_fd=dir_fd)

        with mock.patch("wiki_observations.os.open", side_effect=swap_before_open):
            with self.assertRaisesRegex(ObservationError, "changed"):
                collect_records(self.paths)
        self.assertTrue(swapped)

    def test_post_open_child_directory_disappearance_is_typed(self):
        self.paths.observations.mkdir(parents=True)
        real_stat = os.stat
        matching_calls = 0

        def disappear_after_open(path, *args, **kwargs):
            nonlocal matching_calls
            if (
                path == "observations"
                and kwargs.get("dir_fd") is not None
                and kwargs.get("follow_symlinks") is False
            ):
                matching_calls += 1
                if matching_calls == 2:
                    raise FileNotFoundError("swapped away")
            return real_stat(path, *args, **kwargs)

        with mock.patch("wiki_observations.os.stat", side_effect=disappear_after_open):
            with self.assertRaises(ObservationError) as raised:
                collect_records(self.paths)

        self.assertEqual("validation", raised.exception.kind)
        self.assertIn("changed during report discovery", str(raised.exception))

    def test_post_open_record_disappearance_is_typed(self):
        run_id = start_observation(
            self.paths, self.request, self.scope, now=self.started
        )
        filename = self.paths.record(run_id).name
        real_stat = os.stat
        matching_calls = 0

        def disappear_after_open(path, *args, **kwargs):
            nonlocal matching_calls
            if (
                path == filename
                and kwargs.get("dir_fd") is not None
                and kwargs.get("follow_symlinks") is False
            ):
                matching_calls += 1
                if matching_calls == 2:
                    raise FileNotFoundError("swapped away")
            return real_stat(path, *args, **kwargs)

        with mock.patch("wiki_observations.os.stat", side_effect=disappear_after_open):
            with self.assertRaises(ObservationError) as raised:
                collect_records(self.paths)

        self.assertEqual("validation", raised.exception.kind)
        self.assertIn("changed during report discovery", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
