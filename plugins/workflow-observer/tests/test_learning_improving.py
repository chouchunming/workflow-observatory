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
    r"(?im)^\s*(?:create|design|write)\s+(?:an?\s+)?Evolution Proposal\b",
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
            "Use only when a user explicitly asks to inspect one selected "
            "Workflow Observatory learning candidate.",
            improving.get("description"),
        )

        for description in (learning["description"], improving["description"]):
            with self.subTest(description=description):
                self.assertNotIn("every task", description.lower())
                self.assertNotIn("about to perform", description.lower())
                self.assertNotIn("automatically", description.lower())

    def test_learning_uses_the_canonical_snapshot_contract(self):
        text = learning_skill()
        self.assertIn("on-demand, read-only", text)
        self.assertIn("snapshot-input", text)
        self.assertIn("Learning Snapshot", text)
        self.assertNotIn("Run `validate` first", text)
        self.assertNotIn("run only `report`", text)
        self.assertIn("run only `snapshot`", text)
        self.assertIn("Do not start, finish, invalidate, edit, or publish", text)

    def test_learning_requires_an_explicit_bounded_interval_and_timezone(self):
        text = learning_skill()
        self.assertIn("explicit `--since`, `--until`, and `--timezone`", text)
        self.assertIn("ask the user", text)
        self.assertIn("bounded", text)

    def test_learning_reads_only_the_sanitized_snapshot_artifact(self):
        text = learning_skill()
        self.assertIn("sanitized artifact", text)
        self.assertIn("snapshot_id", text)
        self.assertIn("unranked", text)
        self.assertIn("observational", text)
        self.assertIn("Do not parse observation records", text)
        self.assertIn("Do not run or parse human `report`", text)
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

    def test_improving_requires_one_user_selected_snapshot_candidate_pair(self):
        text = improving_skill()
        self.assertIn("snapshot_id", text)
        self.assertIn("candidate_id", text)
        self.assertIn("both", text)
        self.assertIn("exactly 64 lowercase hexadecimal characters", text)
        self.assertIn("exists in that exact Learning Snapshot", text)

    def test_improving_uses_only_the_current_validated_learning_snapshot(self):
        text = improving_skill()
        self.assertIn(
            "Use only the already-validated sanitized Learning Snapshot returned "
            "by `workflow-learning` in the current context.",
            text,
        )
        self.assertIn(
            "Do not open or parse a local snapshot JSON file directly.", text
        )
        self.assertIn(
            "report that the exact snapshot evidence is unavailable and stop", text
        )
        self.assertIn("Do not rerun `workflow-learning`", text)
        self.assertIn("reconstruct the artifact", text)
        self.assertNotIn("or its immutable local artifact", text)
        self.assertNotRegex(
            text,
            r"(?i)\bor\s+(?:its|the|an?)\s+(?:immutable\s+)?local\s+"
            r"(?:snapshot\s+)?(?:artifact|file)\b",
        )

    def test_improving_stops_before_proposal_design_or_creation(self):
        text = improving_skill()
        self.assertIn("does not create an Evolution Proposal", text)
        self.assertIn("proposal design and creation remain deferred", text)
        self.assertIn("Do not claim causality", text)
        self.assertIn("Do not declare a winner", text)

    def test_improving_stops_before_any_mutation(self):
        text = improving_skill()
        self.assertIn("Stop after", text)
        self.assertIn("edit a workflow", text)
        self.assertIn("create a branch or pull request", text)
        self.assertIn("start an experiment", text)
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
            "Create an Evolution Proposal.",
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
