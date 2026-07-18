from pathlib import Path
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE_ROOT = PLUGIN_ROOT.parents[1]
_SOURCE_REPOSITORY = MARKETPLACE_ROOT.parents[1]
REPOSITORY_ROOT = (
    _SOURCE_REPOSITORY
    if (_SOURCE_REPOSITORY / "skills/observing-workflows/README.md").is_file()
    else MARKETPLACE_ROOT / "evidence"
)
OBSERVER = PLUGIN_ROOT / "skills/workflow-observer/SKILL.md"
TELEMETRY = PLUGIN_ROOT / "skills/workflow-telemetry/SKILL.md"
LEGACY_README = REPOSITORY_ROOT / "skills/observing-workflows/README.md"


def skill_text(path: Path) -> str:
    if not path.is_file():
        raise AssertionError(f"missing skill: {path.relative_to(REPOSITORY_ROOT)}")
    return path.read_text(encoding="utf-8")


def observer_skill() -> str:
    return skill_text(OBSERVER)


def telemetry_skill() -> str:
    return skill_text(TELEMETRY)


def frontmatter_description(text: str) -> str:
    sections = text.split("---", 2)
    if len(sections) != 3:
        raise AssertionError("skill must have YAML frontmatter")
    fields = {}
    for line in sections[1].strip().splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key.strip()] = value.strip()
    if "description" not in fields:
        raise AssertionError("skill frontmatter must have a description")
    return fields["description"]


class SkillContractTests(unittest.TestCase):
    def test_observer_trigger_covers_substantial_work_and_wiki_workflows(self):
        description = frontmatter_description(observer_skill())
        self.assertEqual(
            "Use when Codex is about to perform substantial top-level "
            "implementation work involving multiple files, tests or lint, or at "
            "least two implementation steps; also use for a compile or inbox "
            "workflow that updates Wiki pages or generated catalogs.",
            description,
        )

    def test_observer_routes_once_to_telemetry(self):
        text = observer_skill()
        normalized = " ".join(text.split())
        self.assertIn(
            "Read `../workflow-telemetry/SKILL.md` before the first real start",
            text,
        )
        self.assertIn("one start for each stable top-level scope", normalized)
        self.assertIn("Do not inspect the draft or run help after start", text)
        self.assertIn("Decide eligibility once", text)

    def test_observer_explicitly_routes_compile_and_inbox_workflows(self):
        text = " ".join(observer_skill().split())
        self.assertIn(
            "Treat a compile or inbox workflow that updates Wiki pages or generated "
            "catalogs as eligible even though it is knowledge-base maintenance rather "
            "than software implementation.",
            text,
        )

    def test_observer_defaults_open_ended_optional_improvement_to_no_record(self):
        text = " ".join(observer_skill().split())
        self.assertIn(
            "Treat an open-ended request to improve something if useful that names no "
            "specific change or validation requirement as uncertain.",
            text,
        )
        self.assertIn(
            "Default to no observation, and do not manufacture eligibility by "
            "voluntarily expanding it into multiple files or tests.",
            text,
        )

    def test_observer_starts_before_a_controlled_gate(self):
        text = " ".join(observer_skill().split())
        self.assertIn(
            "In controlled evaluation, complete the observation start before entering "
            "a required fixture gate, while still entering that gate before the first "
            "task mutation.",
            text,
        )

    def test_observer_allows_one_replacement_lifecycle(self):
        text = " ".join(observer_skill().split())
        self.assertIn(
            "A material scope replacement is the only exception: start one replacement "
            "run first, then finish the prior run as `superseded` with "
            "`--superseded-by`.",
            text,
        )
        self.assertIn(
            "The replacement run uses the taxonomy and review requirement of the new "
            "authorized scope.",
            text,
        )

    def test_observer_propagates_parent_and_freezes_payload_calls(self):
        text = observer_skill()
        self.assertIn(
            "Observation managed by parent run <run-id>; do not start a child observation.",
            text,
        )
        self.assertIn("Do not start a child observation", text)
        self.assertIn("Do not automatically retry a rejected payload-bearing call", text)
        self.assertIn("retain only the run ID", text)

    def test_telemetry_owns_limits_enums_and_taxonomy(self):
        text = telemetry_skill()
        for token in (
            "65536 bytes",
            "200 Unicode code points",
            "implementation-basic",
            "implementation-with-review",
            "maintenance-basic",
            "compile-basic",
            "compile-with-review",
            "research-basic",
            "success",
            "partial",
            "failed",
            "rolled-back",
            "superseded",
        ):
            with self.subTest(token=token):
                self.assertIn(token, text)

    def test_telemetry_classifies_durable_research_by_task_intent(self):
        text = " ".join(telemetry_skill().split())
        self.assertIn(
            "A research or query task remains `query` with `research-basic` when "
            "its durable output is a comparison, answer, or Markdown summary.",
            text,
        )
        self.assertIn(
            "Use `documentation` only when the authorized task itself is to create "
            "or maintain documentation.",
            text,
        )

    def test_telemetry_defines_deterministic_review_variant_selection(self):
        text = " ".join(telemetry_skill().split())
        self.assertIn(
            "Select a review variant only when the authorized task instructions or "
            "an already-applicable workflow explicitly require a distinct reviewer, "
            "review gate, or delegated independent review.",
            text,
        )
        self.assertIn(
            "Multiple files, tests, lint, link checks, or ordinary self-verification "
            "do not by themselves imply a review variant.",
            text,
        )
        self.assertIn(
            "Otherwise choose the legal basic variant for the task type.",
            text,
        )

    def test_telemetry_owns_templates_adapter_resolution_and_cleanup(self):
        text = telemetry_skill()
        for token in (
            "## Scope",
            "## Execution evidence",
            "## Outcome and observation",
            "## Follow-up",
            "## Metrics",
            "WORKFLOW_OBSERVATORY_HOME",
            "workflow_observer_cli.py",
            "mode 0600",
            "unique",
            "finally",
            "atomic record writes",
            "exclusive lifecycle transitions",
            "shared adapter conformance suite",
            "start, finish, validate, report, and integrity",
            "--superseded-by",
            "None — no further action",
        ):
            with self.subTest(token=token):
                self.assertIn(token, text)

    def test_telemetry_defines_scope_replacement_lifecycle(self):
        text = " ".join(telemetry_skill().split())
        self.assertIn("Start once for each stable authorized scope", text)
        self.assertIn(
            "For a material scope replacement, start the replacement run before "
            "finishing the prior run as `superseded`.",
            text,
        )
        self.assertNotIn(
            "The automatic observer does not create an extra replacement lifecycle",
            text,
        )

    def test_telemetry_scopes_optional_referents_to_the_llmwiki_adapter(self):
        text = " ".join(telemetry_skill().split())
        self.assertIn(
            "Add `--task` or `--source` only when the selected adapter is `llmwiki`",
            text,
        )
        self.assertIn(
            "the exact canonical referent exists under that adapter's configured Wiki "
            "root",
            text,
        )
        self.assertIn(
            "Omit both options for the portable adapter even when the subject workspace "
            "contains similarly named task or raw files.",
            text,
        )

    def test_telemetry_forbids_zsh_readonly_status_assignment(self):
        text = " ".join(telemetry_skill().split())
        self.assertIn(
            "When a shell wrapper records an exit code, use `exit_code`; never assign "
            "to zsh's read-only special parameter `status`.",
            text,
        )
        self.assertIn(
            "Keep the cleanup trap active across every command after payload creation",
            text,
        )

    def test_telemetry_resolves_a_portable_interpreter_before_payloads(self):
        text = telemetry_skill()
        for token in (
            "On Codex for Unix, macOS, and Linux, `<command>` is exactly",
            "`python3 <resolved-cli-path>`",
            "Never use unqualified `python`",
            "On Windows only, `<command>` may be `py -3 <resolved-cli-path>`",
            "before creating a payload",
        ):
            with self.subTest(token=token):
                self.assertIn(token, text)
        self.assertNotIn("the current Python interpreter", text)

    def test_telemetry_forbids_sensitive_content_and_unsafe_probes(self):
        text = telemetry_skill().lower()
        for token in (
            "full prompts",
            "transcripts",
            "credentials",
            "secrets",
            "subject absolute paths",
            "payload reuse",
            "combined probes",
            "draft inspection",
            "help after start",
            "silently fall back",
        ):
            with self.subTest(token=token):
                self.assertIn(token, text)

    def test_legacy_readme_documents_double_trigger_safe_migration(self):
        text = LEGACY_README.read_text(encoding="utf-8")
        self.assertIn(
            "remains active until marketplace installation checks pass", text
        )
        self.assertIn(
            "old and new automatic descriptions must never be active simultaneously",
            text,
        )


if __name__ == "__main__":
    unittest.main()
