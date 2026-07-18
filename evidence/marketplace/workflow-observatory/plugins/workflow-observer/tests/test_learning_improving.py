from pathlib import Path
import re
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
LEARNING = PLUGIN_ROOT / "skills/workflow-learning/SKILL.md"
IMPROVING = PLUGIN_ROOT / "skills/workflow-improving/SKILL.md"


def skill_text(path: Path) -> str:
    if not path.is_file():
        raise AssertionError(f"missing skill: {path}")
    return path.read_text(encoding="utf-8")


def frontmatter(text: str) -> dict[str, str]:
    sections = text.split("---", 2)
    if len(sections) != 3:
        raise AssertionError("skill must have YAML frontmatter")
    fields: dict[str, str] = {}
    for line in sections[1].strip().splitlines():
        key, separator, value = line.partition(":")
        if separator:
            fields[key.strip()] = value.strip()
    return fields


def learning_skill() -> str:
    return skill_text(LEARNING)


def improving_skill() -> str:
    return skill_text(IMPROVING)


LEARNING_CONTRADICTIONS = (
    r"(?im)^\s*(?:run|invoke)\s+(?:this skill|workflow-learning|learning)\s+"
    r"(?:automatically|for every task|per task|on a schedule|in the background)\b",
    r"(?im)^\s*(?:include|use|count)\s+(?:drafts?|non-final records?)\b",
    r"(?im)^\s*(?:claim|infer)\s+causality\b",
    r"(?im)^\s*(?:name|declare)\s+(?:a\s+)?winning workflow\b",
    r"(?im)^\s*recommend\s+(?:a\s+)?workflow change\b",
    r"(?im)^\s*apply\s+(?:(?:a|the)\s+)?(?:change|proposal)\b",
)

IMPROVING_CONTRADICTIONS = (
    r"(?im)^\s*(?:run|invoke)\s+(?:this skill|workflow-improving|improving)\s+"
    r"(?:automatically|for every task|per task|on a schedule|in the background)\b",
    r"(?im)^\s*(?:claim|infer)\s+causality\b",
    r"(?im)^\s*(?:name|declare)\s+(?:a\s+)?winner\b",
    r"(?im)^\s*recommend\s+(?:a\s+)?winning workflow\b",
    r"(?im)^\s*apply\s+(?:(?:a|the)\s+)?(?:change|proposal)\b",
    r"(?im)^\s*(?:edit|run|start|publish)\b.*\bwithout fresh explicit user approval\b",
)


def assert_no_contradictions(
    testcase: unittest.TestCase, text: str, patterns: tuple[str, ...]
) -> None:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match is not None:
            raise AssertionError(
                f"contradictory clause matched {pattern!r}: {match.group(0)!r}"
            )


class LearningImprovingContractTests(unittest.TestCase):
    def test_frontmatter_triggers_are_on_demand_and_not_per_task(self):
        learning = frontmatter(learning_skill())
        improving = frontmatter(improving_skill())

        self.assertEqual("workflow-learning", learning.get("name"))
        self.assertEqual(
            "Use only when a user asks to analyze existing Workflow Observatory "
            "records or requests a workflow-learning review.",
            learning.get("description"),
        )
        self.assertEqual("workflow-improving", improving.get("name"))
        self.assertEqual(
            "Use only when a user explicitly asks for an improvement proposal "
            "based on cited Workflow Observatory learning output.",
            improving.get("description"),
        )

        for description in (learning["description"], improving["description"]):
            with self.subTest(description=description):
                self.assertNotIn("every task", description.lower())
                self.assertNotIn("about to perform", description.lower())
                self.assertNotIn("automatically", description.lower())

    def test_learning_is_read_only_validate_then_report(self):
        text = learning_skill()
        self.assertIn("on-demand, read-only", text)
        self.assertIn("Run `validate` first", text)
        self.assertIn("run only `report`", text)
        self.assertIn("Do not start, finish, invalidate, edit, or publish", text)

    def test_learning_uses_only_final_non_invalidated_records(self):
        text = learning_skill()
        self.assertIn("final, non-invalidated records", text)
        self.assertIn("Exclude drafts", text)
        self.assertIn("Exclude invalidated records", text)
        self.assertIn("final sample", text)
        self.assertIn(
            "The only permitted statuses are `success`, `partial`, `failed`, "
            "`rolled-back`, and `superseded`.",
            text,
        )
        self.assertIn(
            "Exclude drafts from every observed count, status count, sample, "
            "comparison, rate, trend, and inference.",
            text,
        )
        self.assertIn("Ignore every report draft count and draft summary", text)
        self.assertIn(
            "Exclude invalidated records from every observed count, status count, "
            "sample, comparison, rate, trend, and inference.",
            text,
        )

    def test_learning_groups_on_all_exact_comparability_keys(self):
        text = learning_skill()
        for key in (
            "project",
            "workspace",
            "workspace ID",
            "task type",
            "workflow variant",
        ):
            with self.subTest(key=key):
                self.assertIn(key, text)
        self.assertIn("exact five-part group key", text)
        self.assertIn("Do not merge", text)

    def test_learning_splits_report_workspace_labels_and_bounds_time(self):
        text = learning_skill()
        self.assertIn("more than one workspace label", text)
        self.assertIn("rerun `report --workspace", text)
        self.assertIn("bounded `--since` and `--until`", text)
        self.assertIn("reproducible time window", text)

    def test_learning_requires_five_records_and_labels_smaller_groups(self):
        text = learning_skill()
        self.assertIn("at least 5 comparable final records", text)
        self.assertIn("small sample", text)
        self.assertIn("Do not present a trend", text)

    def test_learning_separates_counts_from_inference_and_avoids_causality(self):
        text = learning_skill()
        self.assertIn("Observed counts", text)
        self.assertIn("Inference", text)
        self.assertIn("Final-status counts", text)
        self.assertIn("Final-record metric counts", text)
        self.assertIn("Do not claim causality", text)
        self.assertIn("Do not name a winning workflow", text)

    def test_whole_learning_document_rejects_reviewer_counterexamples(self):
        assert_no_contradictions(self, learning_skill(), LEARNING_CONTRADICTIONS)
        counterexamples = (
            "Run learning automatically for every task.",
            "Invoke workflow-learning on a schedule.",
            "Run this skill in the background.",
            "Include drafts in observed counts.",
            "Use non-final records for inference.",
            "Claim causality from the trend.",
            "Name a winning workflow from the rates.",
            "Recommend a workflow change.",
            "Apply the change after reporting.",
        )
        for clause in counterexamples:
            with self.subTest(clause=clause):
                with self.assertRaises(AssertionError):
                    assert_no_contradictions(
                        self,
                        learning_skill() + "\n" + clause + "\n",
                        LEARNING_CONTRADICTIONS,
                    )

    def test_improving_requires_cited_learning_evidence(self):
        text = improving_skill()
        self.assertIn("Consume cited workflow-learning output", text)
        self.assertIn("cite observation group keys", text)
        self.assertIn("record count", text)
        self.assertIn("time window", text)
        self.assertIn("uncertainty", text)

    def test_improving_proposes_exactly_one_bounded_experiment(self):
        text = improving_skill()
        self.assertIn("exactly one bounded change or experiment", text)
        self.assertIn("Measurement", text)
        self.assertIn("Rollback", text)
        self.assertIn("Do not claim causality", text)
        self.assertIn("Do not declare a winner", text)

    def test_improving_stops_before_any_mutation(self):
        text = improving_skill()
        self.assertIn("explicit user approval", text)
        self.assertIn("Stop after presenting the proposal", text)
        self.assertIn("edit, run, experiment, or publish", text)
        self.assertIn(
            "Before any edit, run, experiment, or publish action, obtain fresh "
            "explicit user approval for that specific action.",
            text,
        )
        self.assertNotIn("automatically apply", text.lower())

    def test_whole_improving_document_rejects_reviewer_counterexamples(self):
        assert_no_contradictions(self, improving_skill(), IMPROVING_CONTRADICTIONS)
        counterexamples = (
            "Run improving automatically for every task.",
            "Invoke workflow-improving on a schedule.",
            "Run this skill in the background.",
            "Claim causality from the observation.",
            "Declare a winner from these groups.",
            "Recommend a winning workflow.",
            "Apply the proposal now.",
            "Edit the skill without fresh explicit user approval.",
            "Run the experiment without fresh explicit user approval.",
            "Start the experiment without fresh explicit user approval.",
            "Publish the result without fresh explicit user approval.",
        )
        for clause in counterexamples:
            with self.subTest(clause=clause):
                with self.assertRaises(AssertionError):
                    assert_no_contradictions(
                        self,
                        improving_skill() + "\n" + clause + "\n",
                        IMPROVING_CONTRADICTIONS,
                    )


if __name__ == "__main__":
    unittest.main()
