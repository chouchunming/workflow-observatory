from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import wiki_cli
from wiki_observations import (
    ObservationPaths,
    ScopePayload,
    StartRequest,
    finish_observation,
    invalidate_observation,
    parse_completion_payload,
    start_observation,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
COMPLETION = """## Execution evidence

- Verification: `python3 -m unittest tests.test_observation_integration -v` — pass
- Artifacts: `wiki_cli.py`, `tests/test_observation_integration.py`

## Outcome and observation

- Outcome: Integrated operational observation lint.
- Observation: Dedicated validation keeps telemetry outside the knowledge graph.

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


class ObservationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "wiki" / "tasks").mkdir(parents=True)
        (self.root / "raw").mkdir()
        (self.root / "wiki" / "_overview.md").write_text(
            "# Overview\n", encoding="utf-8"
        )
        (self.root / "raw" / "source.md").write_text(
            "source evidence\n", encoding="utf-8"
        )
        self.previous_cwd = Path.cwd()
        os.chdir(self.root)
        self.paths = ObservationPaths.from_root(self.root)
        self.started = datetime(
            2026, 7, 13, 10, 0, tzinfo=timezone(timedelta(hours=8))
        )
        self.request = StartRequest(
            title="Integrate observation lint",
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
            sources=("raw/source.md",),
        )
        self.scope = ScopePayload(
            goal="Integrate observation-specific lint.",
            included="Records, tombstones, and generic-scan exclusions.",
            excluded="CLI commands and reporting.",
        )

    def tearDown(self):
        os.chdir(self.previous_cwd)
        self.temporary.cleanup()

    def start(self):
        return start_observation(
            self.paths, self.request, self.scope, now=self.started
        )

    def finish(self):
        run_id = self.start()
        finish_observation(
            self.paths,
            run_id,
            "success",
            parse_completion_payload(COMPLETION),
            now=self.started + timedelta(seconds=90),
        )
        return run_id

    def test_observations_are_linted_but_not_graph_or_source_coverage_nodes(self):
        run_id = self.start()

        references = wiki_cli.collect_source_references()
        errors, broken, orphans, drift, outbound = wiki_cli.perform_lint_checks()

        self.assertNotIn("raw/source.md", references)
        self.assertEqual([], errors)
        record_path = f"wiki/observations/{run_id}.md"
        self.assertNotIn(record_path, orphans)
        self.assertNotIn(record_path, outbound)
        self.assertEqual([], broken)
        self.assertIsNone(drift)

    def test_final_record_and_invalidation_tombstone_have_dedicated_lint(self):
        run_id = self.finish()
        invalidate_observation(
            self.paths,
            run_id,
            "invalid fixture",
            now=self.started + timedelta(minutes=2),
        )
        (self.paths.locks / "ignored.md").write_text(
            "not a record\n", encoding="utf-8"
        )

        errors, broken, orphans, drift, outbound = wiki_cli.perform_lint_checks()

        self.assertEqual([], errors)
        self.assertEqual([], broken)
        self.assertEqual([], orphans)
        self.assertIsNone(drift)
        self.assertEqual([], outbound)

    def test_malformed_record_and_tombstone_are_reported(self):
        run_id = self.finish()
        invalidate_observation(
            self.paths,
            run_id,
            "invalid fixture",
            now=self.started + timedelta(minutes=2),
        )
        record_path = self.paths.record(run_id)
        content = record_path.read_text(encoding="utf-8")
        record_path.write_text(
            content.replace("type: \"observation\"\n", 'type: "observation"\nsubject_root: "/tmp/private"\n'),
            encoding="utf-8",
        )
        tombstone = self.paths.invalidation(run_id)
        tombstone.write_text(
            tombstone.read_text(encoding="utf-8") + "unexpected body\n",
            encoding="utf-8",
        )

        errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        messages = "\n".join(message for _, message in errors)

        self.assertIn("unexpected frontmatter field `subject_root`", messages)
        self.assertIn("must not contain a body", messages)

    def test_operating_contract_declares_observation_boundary(self):
        contract = (REPOSITORY_ROOT / "AGENTS.md").read_text(encoding="utf-8")
        self.assertIn("wiki/observations/", contract)
        self.assertIn("operational", contract.lower())
        self.assertIn("source coverage", contract.lower())
        self.assertIn("invalidations/", contract)
        self.assertIn(".locks/", contract)

    def test_all_generic_graph_render_and_heal_scans_exclude_operational_files(self):
        scope = ScopePayload(
            goal="Preserve [[OldConcept]] in immutable telemetry.",
            included="Operational scan exclusions.",
            excluded="None.",
        )
        run_id = start_observation(
            self.paths, self.request, scope, now=self.started
        )
        finish_observation(
            self.paths,
            run_id,
            "success",
            parse_completion_payload(COMPLETION),
            now=self.started + timedelta(seconds=90),
        )
        invalidate_observation(
            self.paths,
            run_id,
            "Invalid [[OldConcept]] reference",
            now=self.started + timedelta(minutes=2),
        )
        before_record = self.paths.record(run_id).read_bytes()
        before_tombstone = self.paths.invalidation(run_id).read_bytes()

        wiki_cli.generate_graph_web(open_browser=False)
        graph = Path("wiki_graph.html").read_text(encoding="utf-8")
        self.assertNotIn(run_id, graph)

        with mock.patch("subprocess.run") as run_command:
            wiki_cli.render_slides(open_browser=False)
        run_command.assert_not_called()

        wiki_cli.heal_links("OldConcept", "NewConcept")
        self.assertEqual(before_record, self.paths.record(run_id).read_bytes())
        self.assertEqual(
            before_tombstone, self.paths.invalidation(run_id).read_bytes()
        )

    def test_record_discovery_is_direct_only(self):
        run_id = self.start()
        content = self.paths.record(run_id).read_text(encoding="utf-8")
        self.paths.locks.mkdir(exist_ok=True)
        (self.paths.locks / "shadow.md").write_text(content, encoding="utf-8")

        records, invalidated = wiki_cli.wiki_observations.collect_records(self.paths)

        self.assertEqual([run_id], [record["run_id"] for record in records])
        self.assertEqual(set(), invalidated)

    def test_malformed_operational_root_and_nested_symlink_are_lint_errors(self):
        observation_root = self.root / "wiki" / "observations"
        observation_root.write_text("not a directory", encoding="utf-8")
        errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        self.assertIn("must be a directory", "\n".join(message for _, message in errors))

        observation_root.unlink()
        os.symlink(self.root / "missing-observations", observation_root)
        errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        self.assertIn("symlink", "\n".join(message for _, message in errors))

        observation_root.unlink()
        observation_root.mkdir()
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)
            (outside / "bad.md").write_text("bad", encoding="utf-8")
            os.symlink(outside, observation_root / "nested")
            errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        self.assertIn("unexpected symlink", "\n".join(message for _, message in errors))

    def test_observation_enumeration_error_is_contained_in_lint_tuple(self):
        self.paths.observations.mkdir(parents=True)
        with mock.patch(
            "wiki_cli._list_directory_entries",
            side_effect=PermissionError("enumeration denied"),
        ):
            results = wiki_cli.perform_lint_checks()

        self.assertEqual(5, len(results))
        messages = "\n".join(message for _, message in results[0])
        self.assertIn("enumeration denied", messages)

    def test_operational_subdirectories_reject_wrong_types_and_all_symlinks(self):
        self.paths.observations.mkdir(parents=True)
        self.paths.locks.write_text("not a directory", encoding="utf-8")

        errors, _, _, _, _ = wiki_cli.perform_lint_checks()
        self.assertIn(
            ".locks path must be a directory",
            "\n".join(message for _, message in errors),
        )

        self.paths.locks.unlink()
        self.paths.locks.mkdir()
        self.paths.invalidations.mkdir()
        with tempfile.TemporaryDirectory() as outside_temporary:
            outside = Path(outside_temporary)
            outside_file = outside / "outside-file"
            outside_file.write_text("outside", encoding="utf-8")
            outside_dir = outside / "outside-dir"
            outside_dir.mkdir()
            file_link = self.paths.invalidations / "file-link"
            directory_link = self.paths.invalidations / "directory-link"
            os.symlink(outside_file, file_link)
            os.symlink(outside_dir, directory_link)

            errors, _, _, _, _ = wiki_cli.perform_lint_checks()

        symlink_paths = {
            path
            for path, message in errors
            if "unexpected symlink in invalidations storage" in message
        }
        self.assertEqual(
            {
                "wiki/observations/invalidations/file-link",
                "wiki/observations/invalidations/directory-link",
            },
            symlink_paths,
        )


if __name__ == "__main__":
    unittest.main()
