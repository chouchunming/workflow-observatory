import os
import tempfile
import unittest

import wiki_cli


class TestTaskRecords(unittest.TestCase):
    def setUp(self):
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.TemporaryDirectory()
        os.chdir(self.temp_dir.name)
        os.makedirs("wiki/tasks", exist_ok=True)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def write_task(self, filename, task_id, status, extra=""):
        with open(f"wiki/tasks/{filename}", "w", encoding="utf-8") as file_obj:
            file_obj.write(
                "---\n"
                "type: task\n"
                f"id: {task_id}\n"
                f"title: {task_id}\n"
                "domain: home\n"
                "project: test-project\n"
                f"status: {status}\n"
                "priority: P1\n"
                "next_action: take the next physical action\n"
                "context: [Home_Digital_Twin]\n"
                "tags: [test]\n"
                "timestamp: 2026-07-12\n"
                "created_at: 2026-07-12\n"
                "review_after: 2026-07-19\n"
                "sources: [\"raw/source.md\"]\n"
                f"{extra}"
                "---\n"
            )

    def test_dashboard_is_generated_from_task_records(self):
        self.write_task("pending.md", "test-pending", "pending")
        self.write_task("waiting.md", "test-waiting", "waiting", "waiting_on: someone else\n")

        self.assertTrue(wiki_cli.write_todo_dashboard())
        self.assertTrue(wiki_cli.todo_dashboard_is_current())
        with open("wiki/_todo_list.md", encoding="utf-8") as file_obj:
            dashboard = file_obj.read()
        self.assertIn("test-pending", dashboard)
        self.assertIn("test-waiting", dashboard)
        self.assertIn("Waiting on: someone else", dashboard)

    def test_dashboard_check_detects_stale_output(self):
        self.write_task("pending.md", "test-pending", "pending")
        wiki_cli.write_todo_dashboard()
        with open("wiki/_todo_list.md", "a", encoding="utf-8") as file_obj:
            file_obj.write("stale\n")
        self.assertFalse(wiki_cli.write_todo_dashboard(check=True))

    def test_invalid_task_status_fails_generation_and_check(self):
        self.write_task("invalid.md", "test-invalid", "pendng")

        self.assertFalse(wiki_cli.write_todo_dashboard())
        self.assertFalse(os.path.exists("wiki/_todo_list.md"))
        self.assertFalse(wiki_cli.write_todo_dashboard(check=True))
        errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        self.assertIn(
            "Invalid task status `pendng`",
            "\n".join(message for _, message in errors),
        )


if __name__ == "__main__":
    unittest.main()
