import os
import tempfile
import unittest

import wiki_cli


class TestKnowledgeLoop(unittest.TestCase):
    def setUp(self):
        self.original_cwd = os.getcwd()
        self.temp_dir = tempfile.TemporaryDirectory()
        os.chdir(self.temp_dir.name)
        os.makedirs("raw", exist_ok=True)
        os.makedirs("wiki/concept", exist_ok=True)
        os.makedirs("wiki/summary", exist_ok=True)
        with open("wiki/z_log.md", "w", encoding="utf-8") as file_obj:
            file_obj.write("# Log\n")
        with open("wiki/_queries.md", "w", encoding="utf-8") as file_obj:
            file_obj.write("# Queries\n")

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_source_catalog_separates_coverage_from_triage(self):
        with open("raw/cited.md", "w", encoding="utf-8") as file_obj:
            file_obj.write("source")
        with open("raw/pending.pdf", "wb") as file_obj:
            file_obj.write(b"%PDF")
        with open("wiki/concept/Compiled.md", "w", encoding="utf-8") as file_obj:
            file_obj.write(
                "---\n"
                "type: concept\n"
                "title: Compiled\n"
                "tags: [test]\n"
                "timestamp: 2026-07-11\n"
                "sources: [\"raw/cited.md\"]\n"
                "---\n"
            )

        self.assertTrue(wiki_cli.write_source_catalog())

        with open("wiki/_sources.md", encoding="utf-8") as file_obj:
            catalog = file_obj.read()
        self.assertIn("**compiled** · triage: **untriaged** · `raw/cited.md`", catalog)
        self.assertIn("**uncovered** · triage: **untriaged** · `raw/pending.pdf`", catalog)
        self.assertTrue(wiki_cli.source_catalog_is_current())

        with open("wiki/_sources.md", "a", encoding="utf-8") as file_obj:
            file_obj.write("stale\n")
        self.assertFalse(wiki_cli.source_catalog_is_current())
        self.assertFalse(wiki_cli.write_source_catalog(check=True))

    def test_source_triage_marks_noise_as_not_pending(self):
        with open("raw/notice.txt", "w", encoding="utf-8") as file_obj:
            file_obj.write("login notice")
        with open("raw/research.md", "w", encoding="utf-8") as file_obj:
            file_obj.write("research")
        with open("wiki/_source_triage.md", "w", encoding="utf-8") as file_obj:
            file_obj.write(
                "# Source triage\n\n"
                "```yaml\n"
                "source: raw/notice.txt\n"
                "triage: noise-ignore\n"
                "```\n"
            )

        records, errors = wiki_cli.parse_source_triage()
        self.assertEqual(errors, [])
        self.assertEqual(wiki_cli.source_triage("raw/notice.txt", records), "noise-ignore")
        self.assertEqual(wiki_cli.source_triage("raw/research.md", records), "untriaged")

    def test_generic_placeholder_raw_sources_are_reported_by_lint(self):
        placeholders = ["未命名 7.md", "無標題 2.md", "Untitled 1.md"]
        for name in placeholders:
            with open(os.path.join("raw", name), "w", encoding="utf-8") as file_obj:
                file_obj.write("source")
        with open("raw/Untitled Research.md", "w", encoding="utf-8") as file_obj:
            file_obj.write("source")

        self.assertEqual(
            wiki_cli.find_unnamed_raw_sources(),
            [
                "raw/Untitled 1.md",
                "raw/未命名 7.md",
                "raw/無標題 2.md",
            ],
        )
        errors_schema, *_ = wiki_cli.perform_lint_checks()
        for name in placeholders:
            self.assertIn(
                (
                    f"raw/{name}",
                    "Raw source filename is unnamed; assign a descriptive filename before triage or compilation",
                ),
                errors_schema,
            )
        self.assertNotIn(
            (
                "raw/Untitled Research.md",
                "Raw source filename is unnamed; assign a descriptive filename before triage or compilation",
            ),
            errors_schema,
        )

    def test_explicit_fileback_writes_query_to_summary_and_registers_it(self):
        wiki_cli.fileback_content(
            "Useful Answer",
            "research, test",
            "query",
            "A durable answer with [[Compiled]] context.",
            ["raw/cited.md"],
        )

        path = "wiki/summary/Useful_Answer.md"
        self.assertTrue(os.path.exists(path))
        with open(path, encoding="utf-8") as file_obj:
            page = file_obj.read()
        self.assertIn("type: query", page)
        self.assertIn('sources: ["raw/cited.md"]', page)
        with open("wiki/_queries.md", encoding="utf-8") as file_obj:
            self.assertIn("[[Useful_Answer]]", file_obj.read())


if __name__ == "__main__":
    unittest.main()
