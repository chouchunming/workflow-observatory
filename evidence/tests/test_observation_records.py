import dataclasses
from datetime import date
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from wiki_observations import (
    CompletionPayload,
    FINAL_STATUSES,
    ObservationError,
    ObservationPaths,
    Provenance,
    ReportFilters,
    ScopePayload,
    StartRequest,
    TAXONOMY,
    derive_provenance,
    parse_completion_payload,
    parse_scope_payload,
    read_record,
    validate_record,
    validate_start_request,
)


SCOPE_TEXT = """## Scope

- Goal: Implement validated observation reporting.
- Included: Observation report aggregation and filters.
- Excluded: Workflow recommendation and automatic correction.
"""

COMPLETION_TEXT = """## Execution evidence

- Verification: `python3 -m unittest tests.test_observation_records -v` — pass
- Artifacts: `wiki_observations.py`, `tests/test_observation_records.py`

## Outcome and observation

- Outcome: Added observation lifecycle validation.
- Observation: Review found input validation defects before approval.

## Follow-up

- None — no further action

## Metrics

```yaml
verification: pass
review_rounds: 2
defects_found: 2
rework_count: 2
rework_reason: input serialization and parser delimiters
```
"""


def make_start_request(**overrides):
    values = {
        "title": "Implement observation records",
        "project": "example-project",
        "workspace": "example-project",
        "workspace_id": "7f4a1c29e083",
        "revision": "7316e5b",
        "working_tree": "dirty",
        "agent_surface": "codex",
        "start_mode": "planned",
        "task_type": "feature",
        "workflow_variant": "implementation-with-review",
        "task_ref": None,
        "sources": (),
    }
    values.update(overrides)
    return StartRequest(**values)


def valid_metadata(**overrides):
    values = {
        "type": "observation",
        "title": "Implement observation records",
        "tags": ["observation", "workflow"],
        "run_id": "obs-20260713-100000-a1b2c3",
        "timestamp": "2026-07-13T10:00:00+08:00",
        "project": "example-project",
        "workspace": "example-project",
        "workspace_id": "7f4a1c29e083",
        "revision": "7316e5b",
        "working_tree": "dirty",
        "agent_surface": "codex",
        "task_type": "feature",
        "workflow_variant": "implementation-with-review",
        "status": "draft",
        "start_mode": "planned",
        "sources": [],
    }
    values.update(overrides)
    return values


FINAL_BODY = SCOPE_TEXT + "\n" + COMPLETION_TEXT.replace(
    "verification: pass\n",
    'finished_at: "2026-07-13T10:01:30+08:00"\n'
    "elapsed_seconds: 90\n"
    "verification: pass\n",
    1,
)


class DomainTypeTests(unittest.TestCase):
    def test_taxonomy_is_exact(self):
        self.assertEqual(
            {
                "feature": {"implementation-basic", "implementation-with-review"},
                "bugfix": {"implementation-basic", "implementation-with-review"},
                "refactor": {"implementation-basic", "implementation-with-review"},
                "documentation": {"implementation-basic", "implementation-with-review"},
                "maintenance": {"maintenance-basic", "implementation-with-review"},
                "compile": {"compile-basic", "compile-with-review"},
                "inbox-processing": {"compile-basic", "compile-with-review"},
                "query": {"research-basic"},
            },
            TAXONOMY,
        )
        self.assertEqual(
            {"success", "partial", "failed", "rolled-back", "superseded"},
            FINAL_STATUSES,
        )

    def test_public_payload_and_filter_dataclasses_are_frozen(self):
        instances = [
            Provenance("p", "w", "7f4a1c29e083", "unknown", "unknown"),
            make_start_request(),
            ScopePayload("goal", "included", "None."),
            CompletionPayload(
                "None.", "None.", "done", "observed", "None — no further action",
                "not-run", "unknown", "unknown", "unknown", "unknown",
            ),
            ReportFilters(),
        ]
        for instance in instances:
            with self.subTest(type=type(instance).__name__):
                self.assertTrue(dataclasses.is_dataclass(instance))
                with self.assertRaises(dataclasses.FrozenInstanceError):
                    setattr(instance, dataclasses.fields(instance)[0].name, "changed")

    def test_report_filters_match_cli_filter_contract(self):
        filters = ReportFilters(
            project="p",
            workspace="w",
            workspace_id="7f4a1c29e083",
            task_type="feature",
            status="success",
            since=date(2026, 7, 1),
            until=date(2026, 7, 31),
        )
        self.assertEqual("success", filters.status)
        self.assertEqual(date(2026, 7, 1), filters.since)

    def test_observation_error_accepts_only_contract_kinds(self):
        error = ObservationError("validation", "bad input")
        self.assertEqual("validation", error.kind)
        self.assertEqual("bad input", str(error))
        with self.assertRaises(ValueError):
            ObservationError("unexpected", "bad kind")


class ObservationPathTests(unittest.TestCase):
    def test_from_root_resolves_an_existing_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            paths = ObservationPaths.from_root(root)
            self.assertEqual(root.resolve(), paths.root)
            self.assertEqual(root.resolve() / "wiki" / "observations", paths.observations)
            self.assertEqual(paths.observations / ".locks", paths.locks)
            self.assertEqual(paths.observations / "invalidations", paths.invalidations)
            self.assertEqual(paths.observations / "example.md", paths.record("example"))

    def test_from_root_rejects_missing_or_non_directory_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ObservationError, "wiki root does not exist"):
                ObservationPaths.from_root(root / "missing")
            file_path = root / "file"
            file_path.write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(ObservationError, "wiki root must be a directory"):
                ObservationPaths.from_root(file_path)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_from_root_rejects_observation_symlink_escape(self):
        with tempfile.TemporaryDirectory() as temporary, tempfile.TemporaryDirectory() as outside:
            root = Path(temporary)
            (root / "wiki").mkdir()
            os.symlink(outside, root / "wiki" / "observations")
            with self.assertRaisesRegex(ObservationError, "escapes wiki root"):
                ObservationPaths.from_root(root)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_from_root_rejects_observation_symlink_even_inside_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wiki = root / "wiki"
            target = wiki / "real-observations"
            target.mkdir(parents=True)
            os.symlink(target, wiki / "observations")
            with self.assertRaisesRegex(ObservationError, "must not contain symlinks"):
                ObservationPaths.from_root(root)


class ProvenanceTests(unittest.TestCase):
    def run_git(self, root, *arguments):
        return subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()

    def test_provenance_is_derived_without_persisting_subject_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.subject_root = Path(temporary) / "example-project"
            self.subject_root.mkdir()
            provenance = derive_provenance(self.subject_root)
            self.assertEqual("example-project", provenance.project)
            self.assertEqual("example-project", provenance.workspace)
            self.assertRegex(provenance.workspace_id, r"^[0-9a-f]{12}$")
            self.assertEqual("unknown", provenance.revision)
            self.assertEqual("unknown", provenance.working_tree)
            self.assertNotIn(str(self.subject_root), repr(provenance))

    def test_git_nested_subject_uses_top_level_and_normalized_remote_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "Local Checkout"
            nested = root / "nested" / "subject"
            nested.mkdir(parents=True)
            self.run_git(root, "init", "-q")
            self.run_git(root, "remote", "add", "origin", "git@GitHub.COM:Owner/RepoName.git")

            first = derive_provenance(nested)
            self.run_git(root, "remote", "set-url", "origin", "ssh://git@github.com:22/Owner//RepoName.git")
            second = derive_provenance(nested)

            self.assertEqual("RepoName", first.project)
            self.assertEqual("Local Checkout", first.workspace)
            self.assertEqual(first.workspace_id, second.workspace_id)
            self.assertEqual("unknown", first.revision)
            self.assertEqual("clean", first.working_tree)

    def test_non_default_remote_port_remains_part_of_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            self.run_git(root, "init", "-q")
            self.run_git(root, "remote", "add", "origin", "ssh://git@example.com:2222/Org/Repo.git")
            non_default = derive_provenance(root)
            self.run_git(root, "remote", "set-url", "origin", "ssh://git@example.com/Org/Repo.git")
            default = derive_provenance(root)
            self.assertNotEqual(non_default.workspace_id, default.workspace_id)

    def test_remote_trailing_slash_normalizes_before_dot_git_removal(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            self.run_git(root, "init", "-q")
            self.run_git(root, "remote", "add", "origin", "git@github.com:Owner/Repo.git")
            canonical = derive_provenance(root)
            self.run_git(
                root,
                "remote",
                "set-url",
                "origin",
                "ssh://git@github.com/Owner/Repo.git/",
            )
            trailing_slash = derive_provenance(root)
            self.assertEqual(canonical.workspace_id, trailing_slash.workspace_id)
            self.assertEqual("Repo", trailing_slash.project)

    def test_git_revision_and_working_tree_state_are_derived(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            self.run_git(root, "init", "-q")
            self.run_git(root, "config", "user.email", "test@example.invalid")
            self.run_git(root, "config", "user.name", "Observation Test")
            (root / "tracked.txt").write_text("clean\n", encoding="utf-8")
            self.run_git(root, "add", "tracked.txt")
            self.run_git(root, "commit", "-qm", "fixture")
            clean = derive_provenance(root)
            self.assertRegex(clean.revision, r"^[0-9a-f]{40}$")
            self.assertEqual("clean", clean.working_tree)
            (root / "tracked.txt").write_text("dirty\n", encoding="utf-8")
            self.assertEqual("dirty", derive_provenance(root).working_tree)

    def test_project_override_is_explicit_and_strictly_validated(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            self.assertEqual("My Project", derive_provenance(root, "My Project").project)
            for invalid in ("", "bad\nproject", "---", "x" * 201):
                with self.subTest(invalid=invalid):
                    with self.assertRaises(ObservationError) as raised:
                        derive_provenance(root, invalid)
                    self.assertEqual("validation", raised.exception.kind)

    def test_subject_root_must_be_an_existing_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ObservationError, "subject root does not exist"):
                derive_provenance(root / "missing")
            file_path = root / "file"
            file_path.write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(ObservationError, "subject root must be a directory"):
                derive_provenance(file_path)


class StartRequestValidationTests(unittest.TestCase):
    def test_agent_surface_set_is_exact(self):
        for surface in ("codex", "claude"):
            with self.subTest(surface=surface):
                validate_start_request(make_start_request(agent_surface=surface))
        for surface in ("", "Claude", "codex-cli", "other", [], {}):
            with self.subTest(surface=surface):
                with self.assertRaisesRegex(
                    ObservationError,
                    "agent_surface must be codex or claude",
                ):
                    validate_start_request(make_start_request(agent_surface=surface))

    def test_taxonomy_and_provenance_are_validated(self):
        taxonomy_error = make_start_request(task_type="feature", workflow_variant="compile-basic")
        with self.assertRaisesRegex(ObservationError, "invalid taxonomy combination"):
            validate_start_request(taxonomy_error)
        workspace_error = make_start_request(workspace_id="BAD")
        with self.assertRaisesRegex(ObservationError, "workspace_id must be 12 lowercase hex"):
            validate_start_request(workspace_error)

    def test_controlled_enums_are_validated(self):
        cases = {
            "revision": "not-a-sha",
            "working_tree": "modified",
            "agent_surface": "other",
            "start_mode": "retroactive",
        }
        for field, value in cases.items():
            with self.subTest(field=field):
                with self.assertRaises(ObservationError) as raised:
                    validate_start_request(make_start_request(**{field: value}))
                self.assertEqual("validation", raised.exception.kind)

    def test_frontmatter_scalars_reject_empty_long_multiline_control_and_delimiter(self):
        for field in ("title", "project", "workspace"):
            for value in ("", "x" * 201, "line\nbreak", "control\x00byte", "---"):
                with self.subTest(field=field, value=repr(value)):
                    with self.assertRaises(ObservationError):
                        validate_start_request(make_start_request(**{field: value}))

    def test_sources_are_required_but_may_be_empty(self):
        validate_start_request(make_start_request(sources=()))
        with self.assertRaisesRegex(ObservationError, "sources must be a tuple"):
            validate_start_request(make_start_request(sources=None))
        with self.assertRaisesRegex(ObservationError, "only strings"):
            validate_start_request(make_start_request(sources=(["raw/source.md"],)))

    def test_sources_must_be_unique_normalized_relative_raw_paths(self):
        validate_start_request(make_start_request(sources=("raw/source.md", "raw/nested/file.txt")))
        invalid_sources = (
            ("source.md",),
            ("/tmp/raw/source.md",),
            ("raw/../wiki/page.md",),
            ("raw/nested/../source.md",),
            ("raw\\source.md",),
            ("raw/source.md", "raw/source.md"),
        )
        for sources in invalid_sources:
            with self.subTest(sources=sources):
                with self.assertRaisesRegex(ObservationError, "sources"):
                    validate_start_request(make_start_request(sources=sources))

    def test_sources_reject_control_characters_and_frontmatter_delimiters(self):
        for source in ("raw/a\nname.md", "raw/a\x00name.md", "raw/---/file.md"):
            with self.subTest(source=repr(source)):
                with self.assertRaisesRegex(ObservationError, "sources"):
                    validate_start_request(make_start_request(sources=(source,)))

    def test_task_reference_must_be_a_single_safe_wikilink(self):
        validate_start_request(make_start_request(task_ref="[[open-loop-record]]"))
        for task_ref in ("open-loop-record", "[[../escape]]", "[[nested/task]]", "[[a]][[b]]"):
            with self.subTest(task_ref=task_ref):
                with self.assertRaisesRegex(ObservationError, "task_ref"):
                    validate_start_request(make_start_request(task_ref=task_ref))


class PayloadParserTests(unittest.TestCase):
    def test_scope_parser_returns_frozen_fields(self):
        payload = parse_scope_payload(SCOPE_TEXT)
        self.assertEqual("Implement validated observation reporting.", payload.goal)
        self.assertEqual("Observation report aggregation and filters.", payload.included)
        self.assertEqual("Workflow recommendation and automatic correction.", payload.excluded)

    def test_scope_requires_exact_heading_and_fields(self):
        invalid = (
            SCOPE_TEXT.replace("## Scope", "## Other"),
            SCOPE_TEXT.replace("- Goal: Implement validated observation reporting.\n", ""),
            SCOPE_TEXT.replace("Implement validated observation reporting.", ""),
            SCOPE_TEXT + "\n## Extra\n\nNo.\n",
            SCOPE_TEXT + "\n- Goal: Duplicate.\n",
            SCOPE_TEXT + "\nfree text\n",
        )
        for text in invalid:
            with self.subTest(text=text):
                with self.assertRaises(ObservationError) as raised:
                    parse_scope_payload(text)
                self.assertEqual("validation", raised.exception.kind)

    def test_payload_limit_counts_utf8_bytes_and_scalar_limit_counts_code_points(self):
        oversized = SCOPE_TEXT + ("界" * 22_000)
        self.assertGreater(len(oversized.encode("utf-8")), 64 * 1024)
        with self.assertRaisesRegex(ObservationError, "64 KiB"):
            parse_scope_payload(oversized)
        long_scalar = SCOPE_TEXT.replace(
            "Implement validated observation reporting.", "界" * 201
        )
        with self.assertRaisesRegex(ObservationError, "200 Unicode code points"):
            parse_scope_payload(long_scalar)

    def test_payload_rejects_frontmatter_delimiter_and_control_characters(self):
        for suffix in ("\n---\n", "\n\x00"):
            with self.subTest(suffix=repr(suffix)):
                with self.assertRaises(ObservationError):
                    parse_scope_payload(SCOPE_TEXT + suffix)

    def test_completion_parser_returns_all_fixed_fields(self):
        payload = parse_completion_payload(COMPLETION_TEXT)
        self.assertIn("unittest", payload.execution_verification)
        self.assertIn("wiki_observations.py", payload.artifacts)
        self.assertEqual("Added observation lifecycle validation.", payload.outcome)
        self.assertEqual("pass", payload.verification)
        self.assertEqual(2, payload.review_rounds)
        self.assertEqual(2, payload.defects_found)
        self.assertEqual(2, payload.rework_count)

    def test_completion_requires_fixed_sections(self):
        with self.assertRaisesRegex(ObservationError, "missing Outcome and observation"):
            parse_completion_payload(
                "## Execution evidence\n\n- Verification: None.\n- Artifacts: None.\n"
            )

    def test_completion_rejects_duplicate_extra_or_out_of_order_headings(self):
        cases = (
            COMPLETION_TEXT + "\n## Follow-up\n\n- Later\n",
            COMPLETION_TEXT + "\n## Extra\n\n- Later\n",
            COMPLETION_TEXT.replace(
                "## Execution evidence", "## Outcome and observation", 1
            ),
            COMPLETION_TEXT.replace(
                "## Execution evidence\n", "## Follow-up\n", 1
            ),
        )
        for text in cases:
            with self.subTest(text=text[:80]):
                with self.assertRaises(ObservationError):
                    parse_completion_payload(text)

    def test_completion_rejects_missing_or_duplicate_required_labels(self):
        cases = (
            COMPLETION_TEXT.replace("- Artifacts: `wiki_observations.py`, `tests/test_observation_records.py`\n", ""),
            COMPLETION_TEXT.replace("- Outcome: Added observation lifecycle validation.", "- Outcome:"),
            COMPLETION_TEXT.replace(
                "- Observation: Review found input validation defects before approval.\n",
                "- Observation: First.\n- Observation: Second.\n",
            ),
            COMPLETION_TEXT.replace("- None — no further action", ""),
        )
        for text in cases:
            with self.subTest(text=text[:100]):
                with self.assertRaises(ObservationError):
                    parse_completion_payload(text)

    def test_completion_metrics_are_strict_and_preserve_unknown(self):
        unknown = COMPLETION_TEXT.replace("verification: pass", "verification: unknown")
        unknown = unknown.replace("review_rounds: 2", "review_rounds: unknown")
        unknown = unknown.replace("defects_found: 2", "defects_found: unknown")
        unknown = unknown.replace("rework_count: 2", "rework_count: unknown")
        unknown = unknown.replace(
            "rework_reason: input serialization and parser delimiters",
            "rework_reason: unknown",
        )
        parsed = parse_completion_payload(unknown)
        self.assertEqual("unknown", parsed.review_rounds)
        self.assertEqual("unknown", parsed.rework_count)

        invalid = (
            COMPLETION_TEXT.replace("verification: pass", "verification: maybe"),
            COMPLETION_TEXT.replace("review_rounds: 2", "review_rounds: -1"),
            COMPLETION_TEXT.replace("defects_found: 2", "defects_found: 1.5"),
            COMPLETION_TEXT.replace("rework_count: 2", "rework_count: true"),
            COMPLETION_TEXT.replace("rework_reason:", "extra: no\nrework_reason:"),
        )
        for text in invalid:
            with self.subTest(text=text[-180:]):
                with self.assertRaises(ObservationError):
                    parse_completion_payload(text)

    def test_completion_metrics_enforce_rework_count_reason_consistency(self):
        invalid = (
            COMPLETION_TEXT.replace("rework_count: 2", "rework_count: 0"),
            COMPLETION_TEXT.replace(
                "rework_reason: input serialization and parser delimiters",
                "rework_reason: none",
            ),
            COMPLETION_TEXT.replace(
                "rework_count: 2\nrework_reason: input serialization and parser delimiters",
                "rework_count: 0\nrework_reason: unknown",
            ),
        )
        for text in invalid:
            with self.subTest(metrics=text[-140:]):
                with self.assertRaisesRegex(ObservationError, "rework_count"):
                    parse_completion_payload(text)

        no_rework = COMPLETION_TEXT.replace("rework_count: 2", "rework_count: 0")
        no_rework = no_rework.replace(
            "rework_reason: input serialization and parser delimiters",
            "rework_reason: none",
        )
        self.assertEqual(0, parse_completion_payload(no_rework).rework_count)

        unknown_count = COMPLETION_TEXT.replace(
            "rework_count: 2", "rework_count: unknown"
        )
        self.assertEqual(
            "unknown", parse_completion_payload(unknown_count).rework_count
        )
        unknown_reason = COMPLETION_TEXT.replace(
            "rework_reason: input serialization and parser delimiters",
            "rework_reason: unknown",
        )
        self.assertEqual("unknown", parse_completion_payload(unknown_reason).rework_reason)

    def test_completion_rejects_huge_integer_metrics_as_validation_errors(self):
        huge = COMPLETION_TEXT.replace("review_rounds: 2", "review_rounds: " + "9" * 5000)
        with self.assertRaises(ObservationError) as raised:
            parse_completion_payload(huge)
        self.assertEqual("validation", raised.exception.kind)


class RecordValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "wiki" / "observations").mkdir(parents=True)
        (self.root / "wiki" / "tasks").mkdir(parents=True)
        (self.root / "raw").mkdir()
        self.paths = ObservationPaths.from_root(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_record_rejects_unhashable_agent_surfaces_without_crashing(self):
        for surface in ([], {}):
            with self.subTest(surface=surface):
                errors = validate_record(
                    valid_metadata(agent_surface=surface),
                    SCOPE_TEXT,
                    self.paths,
                )
                self.assertIn("agent_surface must be codex or claude", errors)

    def write_record(self, metadata, body=SCOPE_TEXT):
        lines = ["---"]
        for key, value in metadata.items():
            if isinstance(value, list):
                encoded = "[" + ", ".join(f'"{item}"' for item in value) + "]"
            elif value is None:
                continue
            else:
                encoded = f'"{value}"' if isinstance(value, str) else str(value)
            lines.append(f"{key}: {encoded}")
        lines.extend(["---", "", body])
        path = self.paths.record(metadata["run_id"])
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def test_valid_draft_and_final_records_pass(self):
        self.assertEqual([], validate_record(valid_metadata(), SCOPE_TEXT, self.paths))
        metadata = valid_metadata(status="success")
        self.assertEqual([], validate_record(metadata, FINAL_BODY, self.paths))

    def test_record_schema_requires_all_fields_and_aware_timestamp(self):
        metadata = valid_metadata()
        del metadata["sources"]
        errors = validate_record(metadata, SCOPE_TEXT, self.paths)
        self.assertIn("missing required field `sources`", errors)
        errors = validate_record(
            valid_metadata(timestamp="2026-07-13T10:00:00"), SCOPE_TEXT, self.paths
        )
        self.assertIn("timestamp must be an aware ISO-8601 datetime", errors)

    def test_record_rejects_unknown_or_nonpersistent_frontmatter_fields(self):
        for field, value in (
            ("subject_root", "/secret/private/project"),
            ("finished_at", "2026-07-13T10:01:30+08:00"),
            ("elapsed_seconds", 90),
            ("arbitrary", "value"),
        ):
            with self.subTest(field=field):
                errors = validate_record(valid_metadata(**{field: value}), SCOPE_TEXT, self.paths)
                self.assertIn(f"unexpected frontmatter field `{field}`", errors)

    def test_record_rejects_invalid_taxonomy_scalars_and_status(self):
        metadata = valid_metadata(
            title="bad\nvalue",
            workflow_variant="compile-basic",
            status="done",
        )
        joined = "\n".join(validate_record(metadata, SCOPE_TEXT, self.paths))
        self.assertIn("title", joined)
        self.assertIn("invalid taxonomy combination", joined)
        self.assertIn("invalid status `done`", joined)

    def test_record_wrong_json_value_types_return_errors_instead_of_crashing(self):
        metadata = valid_metadata(
            tags=[{"not": "a string"}],
            task_type=[],
            workflow_variant={},
            status=[],
            sources=[["raw/source.md"]],
        )

        joined = "\n".join(validate_record(metadata, SCOPE_TEXT, self.paths))

        self.assertIn("tags must include observation and workflow", joined)
        self.assertIn("invalid taxonomy combination", joined)
        self.assertIn("sources must contain only strings", joined)
        self.assertIn("invalid status", joined)

    def test_record_checks_source_existence_and_task_reference(self):
        metadata = valid_metadata(
            sources=["raw/missing.md"], task_ref="[[missing-task]]"
        )
        joined = "\n".join(validate_record(metadata, SCOPE_TEXT, self.paths))
        self.assertIn("source does not exist", joined)
        self.assertIn("task_ref points to no task record", joined)
        (self.root / "raw" / "source.md").write_text("source", encoding="utf-8")
        (self.root / "wiki" / "tasks" / "open-loop.md").write_text(
            "---\ntype: task\nid: open-loop\ntitle: Open loop\nstatus: pending\n"
            "tags: [task, test]\ntimestamp: 2026-07-14\nsources: []\n---\n\n# Open loop\n",
            encoding="utf-8",
        )
        metadata["sources"] = ["raw/source.md"]
        metadata["task_ref"] = "[[open-loop]]"
        self.assertEqual([], validate_record(metadata, SCOPE_TEXT, self.paths))

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_record_references_reject_symlinks_and_invalid_task_schema(self):
        with tempfile.TemporaryDirectory() as outside:
            outside_source = Path(outside) / "source.md"
            outside_source.write_text("outside", encoding="utf-8")
            os.symlink(outside_source, self.root / "raw" / "linked.md")
            errors = validate_record(
                valid_metadata(sources=["raw/linked.md"]), SCOPE_TEXT, self.paths
            )
            self.assertIn("source must not be a symlink or escape raw", "\n".join(errors))

        task = self.root / "wiki" / "tasks" / "open-loop.md"
        task.write_text("plain text is not a task record", encoding="utf-8")
        errors = validate_record(
            valid_metadata(task_ref="[[open-loop]]"), SCOPE_TEXT, self.paths
        )
        self.assertIn("task_ref points to an invalid task record", errors)

        task.write_text(
            "---\ntype: task\nid: open-loop\ntitle: Open loop\nstatus: pending\n"
            "tags: [task]\ntimestamp: 2026-07-14\nsources: []\n---evil\n",
            encoding="utf-8",
        )
        errors = validate_record(
            valid_metadata(task_ref="[[open-loop]]"), SCOPE_TEXT, self.paths
        )
        self.assertIn("task_ref points to an invalid task record", errors)

    def test_draft_forbids_completion_sections_and_superseded_by(self):
        joined = "\n".join(
            validate_record(
                valid_metadata(superseded_by="obs-20260713-110000-b2c3d4"),
                FINAL_BODY,
                self.paths,
            )
        )
        self.assertIn("draft record must contain only Scope", joined)
        self.assertIn("draft record must not contain superseded_by", joined)

    def test_final_status_and_verification_invariants_are_enforced(self):
        success_fail = FINAL_BODY.replace("verification: pass", "verification: fail")
        joined = "\n".join(
            validate_record(valid_metadata(status="success"), success_fail, self.paths)
        )
        self.assertIn("success status cannot have fail verification", joined)

        no_follow_up = FINAL_BODY
        joined = "\n".join(
            validate_record(valid_metadata(status="partial"), no_follow_up, self.paths)
        )
        self.assertIn("partial status requires a follow-up action", joined)
        self.assertIn("partial outcome must identify incomplete Included items", joined)

    def test_partial_outcome_uses_anchored_incomplete_items_statement(self):
        valid_partial = FINAL_BODY.replace(
            "Added observation lifecycle validation.",
            "Completed validation. Incomplete Included items: CLI adapter.",
        ).replace("None — no further action", "Implement the CLI adapter next.")
        self.assertEqual(
            [], validate_record(valid_metadata(status="partial"), valid_partial, self.paths)
        )

        for outcome in (
            "All Included work is complete; nothing remaining.",
            "CLI adapter deferred.",
            "Incomplete Included items: None.",
        ):
            with self.subTest(outcome=outcome):
                body = FINAL_BODY.replace(
                    "Added observation lifecycle validation.", outcome
                ).replace("None — no further action", "Review the result next.")
                errors = validate_record(
                    valid_metadata(status="partial"), body, self.paths
                )
                self.assertIn(
                    "partial outcome must identify incomplete Included items", errors
                )

    def test_superseded_requires_existing_target_and_other_statuses_forbid_it(self):
        target = "obs-20260713-110000-b2c3d4"
        joined = "\n".join(
            validate_record(valid_metadata(status="superseded"), FINAL_BODY, self.paths)
        )
        self.assertIn("superseded status requires superseded_by", joined)

        metadata = valid_metadata(status="superseded", superseded_by=target)
        joined = "\n".join(validate_record(metadata, FINAL_BODY, self.paths))
        self.assertIn("superseded_by points to no observation record", joined)

        self.write_record(valid_metadata(run_id=target))
        self.assertEqual([], validate_record(metadata, FINAL_BODY, self.paths))

        joined = "\n".join(
            validate_record(
                valid_metadata(
                    status="superseded",
                    superseded_by="obs-20260713-100000-a1b2c3",
                ),
                FINAL_BODY,
                self.paths,
            )
        )
        self.assertIn("must not reference itself", joined)

        joined = "\n".join(
            validate_record(
                valid_metadata(status="failed", superseded_by=target), FINAL_BODY, self.paths
            )
        )
        self.assertIn("superseded_by is only valid", joined)

    def test_finished_at_order_and_elapsed_seconds_must_recompute(self):
        earlier = FINAL_BODY.replace(
            "2026-07-13T10:01:30+08:00", "2026-07-13T09:59:59+08:00"
        )
        joined = "\n".join(
            validate_record(valid_metadata(status="failed"), earlier, self.paths)
        )
        self.assertIn("finished_at must not be earlier than timestamp", joined)

        wrong_elapsed = FINAL_BODY.replace("elapsed_seconds: 90", "elapsed_seconds: 89")
        joined = "\n".join(
            validate_record(valid_metadata(status="success"), wrong_elapsed, self.paths)
        )
        self.assertIn("elapsed_seconds must equal finished_at minus timestamp", joined)

    def test_record_rejects_huge_elapsed_metric_as_a_validation_error(self):
        huge_elapsed = FINAL_BODY.replace(
            "elapsed_seconds: 90", "elapsed_seconds: " + "9" * 5000
        )
        errors = validate_record(
            valid_metadata(status="success"), huge_elapsed, self.paths
        )
        self.assertIn("elapsed_seconds must be a bounded nonnegative integer", errors)

    def test_read_record_parses_safe_frontmatter_and_rejects_unsafe_ids(self):
        metadata = valid_metadata()
        self.write_record(metadata)
        parsed, body = read_record(self.paths, metadata["run_id"])
        self.assertEqual(metadata["run_id"], parsed["run_id"])
        self.assertEqual([], parsed["sources"])
        self.assertIn("## Scope", body)
        for run_id in ("../escape", "/absolute", "obs-bad"):
            with self.subTest(run_id=run_id):
                with self.assertRaises(ObservationError) as raised:
                    read_record(self.paths, run_id)
                self.assertEqual("validation", raised.exception.kind)

    def test_read_record_maps_missing_and_malformed_records(self):
        with self.assertRaises(ObservationError) as missing:
            read_record(self.paths, "obs-20260713-100000-a1b2c3")
        self.assertEqual("state", missing.exception.kind)

        path = self.paths.record("obs-20260713-100000-a1b2c3")
        path.write_text("not frontmatter", encoding="utf-8")
        with self.assertRaises(ObservationError) as malformed:
            read_record(self.paths, "obs-20260713-100000-a1b2c3")
        self.assertEqual("validation", malformed.exception.kind)

    def test_read_record_requires_frontmatter_id_to_match_filename(self):
        requested = "obs-20260713-100000-a1b2c3"
        other = "obs-20260713-110000-b2c3d4"
        self.write_record(valid_metadata(run_id=other))
        self.paths.record(other).replace(self.paths.record(requested))

        with self.assertRaisesRegex(ObservationError, "does not match filename") as raised:
            read_record(self.paths, requested)
        self.assertEqual("validation", raised.exception.kind)


if __name__ == "__main__":
    unittest.main()
