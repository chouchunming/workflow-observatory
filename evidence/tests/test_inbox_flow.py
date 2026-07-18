import os
import tempfile
import unittest
from datetime import datetime
from unittest import mock

import wiki_cli


class TestInboxFlow(unittest.TestCase):
    def setUp(self):
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.TemporaryDirectory()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_external_ref_prevents_duplicate_capture(self):
        self.assertTrue(wiki_cli.add_inbox_capture("test capture", "reminders", "reminder-123"))
        self.assertFalse(wiki_cli.add_inbox_capture("duplicate", "reminders", "reminder-123"))
        self.assertTrue(wiki_cli.write_inbox_dashboard(check=True))
        with open("wiki/_todo_list.md", encoding="utf-8") as file_obj:
            self.assertIn("待編譯 Inbox", file_obj.read())

    def test_dashboard_describes_reminders_as_capture_only(self):
        dashboard = wiki_cli.render_inbox_dashboard()
        self.assertIn("Reminders 同步只建立或關閉尚未編譯的 capture", dashboard)
        self.assertIn("不會自動建立／完成正式 Todo", dashboard)
        self.assertNotIn("Siri／Reminders 尚未接入", dashboard)

    def test_dashboard_write_fails_before_writing_when_todo_render_is_invalid(self):
        with mock.patch.object(
            wiki_cli, "render_todo_dashboard", side_effect=ValueError("private detail")
        ):
            self.assertFalse(wiki_cli.write_inbox_dashboard())
        self.assertFalse(os.path.exists("wiki/_inbox.md"))
        self.assertFalse(os.path.exists("wiki/_todo_list.md"))

    def test_capture_can_defer_dashboard_refresh(self):
        with mock.patch.object(wiki_cli, "write_inbox_dashboard") as refresh:
            self.assertTrue(
                wiki_cli.add_inbox_capture(
                    "deferred", "reminders", "reminder-deferred", refresh=False
                )
            )
        refresh.assert_not_called()
        records = wiki_cli.collect_inbox_records()
        self.assertEqual("reminder-deferred", records[0]["external_ref"])

    def test_rapid_captures_are_unique_and_include_required_title(self):
        class FixedDatetime:
            @classmethod
            def now(cls):
                return datetime(2026, 7, 14, 12, 34, 56)

        with (
            mock.patch.object(wiki_cli, "datetime", FixedDatetime),
            mock.patch.object(
                wiki_cli.secrets, "token_hex", side_effect=("aaaaaaaa", "aaaaaaaa", "bbbbbbbb")
            ),
        ):
            self.assertTrue(wiki_cli.add_inbox_capture("first", "manual"))
            self.assertTrue(wiki_cli.add_inbox_capture("second", "manual"))

        capture_paths = sorted(
            os.path.join("wiki/inbox", name)
            for name in os.listdir("wiki/inbox")
            if name.endswith(".md")
        )
        self.assertEqual(2, len(capture_paths))
        self.assertNotEqual(capture_paths[0], capture_paths[1])
        bodies = []
        for path in capture_paths:
            with open(path, encoding="utf-8") as file_obj:
                metadata, body = wiki_cli.parse_frontmatter(file_obj.read())
            self.assertEqual(metadata["capture_id"], metadata["title"])
            bodies.append(body)
        self.assertIn("first", "\n".join(bodies))
        self.assertIn("second", "\n".join(bodies))

    def test_source_completion_closes_only_new_reminders_capture(self):
        self.assertTrue(
            wiki_cli.add_inbox_capture(
                "new reminder", "reminders", "reminder-new", refresh=False
            )
        )
        self.assertTrue(
            wiki_cli.add_inbox_capture(
                "manual capture", "manual", "manual-new", refresh=False
            )
        )
        changed = wiki_cli.mark_inbox_source_completed(
            {"reminder-new", "manual-new"},
            synced_at="2026-07-15T12:00:00+08:00",
            refresh=False,
        )
        self.assertEqual(1, changed)

        records = {
            record["external_ref"]: record for record in wiki_cli.collect_inbox_records()
        }
        self.assertEqual("ignored", records["reminder-new"]["status"])
        self.assertEqual(
            "completed-at-source-before-compilation",
            records["reminder-new"]["reason"],
        )
        self.assertEqual(
            "2026-07-15T12:00:00+08:00",
            records["reminder-new"]["source_completed_at"],
        )
        self.assertEqual("new", records["manual-new"]["status"])

    def test_source_completion_preserves_compiled_and_ignored_captures(self):
        for status in ("compiled", "ignored"):
            external_ref = f"reminder-{status}"
            self.assertTrue(
                wiki_cli.add_inbox_capture(
                    status, "reminders", external_ref, refresh=False
                )
            )
            record = next(
                item
                for item in wiki_cli.collect_inbox_records()
                if item["external_ref"] == external_ref
            )
            with open(record["_path"], encoding="utf-8") as file_obj:
                content = file_obj.read()
            with open(record["_path"], "w", encoding="utf-8") as file_obj:
                file_obj.write(content.replace("status: new", f"status: {status}"))

        changed = wiki_cli.mark_inbox_source_completed(
            {"reminder-compiled", "reminder-ignored"}, refresh=False
        )
        self.assertEqual(0, changed)


if __name__ == "__main__":
    unittest.main()
