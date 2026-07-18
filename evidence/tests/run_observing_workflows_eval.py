import argparse
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


DECISION_MANIFEST_FIELDS = {
    "id", "turns", "fixture", "expected_decisions", "task_type",
    "workflow_variant", "expected_record_checkpoints", "expected_run_count",
    "expected_final_statuses",
}
LIFECYCLE_MANIFEST_FIELDS = {
    "id", "turns", "fixture", "mode", "setup", "expected_record_checkpoints",
    "expected_run_count", "expected_draft_count", "expected_final_statuses",
    "expect_failure_disclosure", "expected_selected_command",
}
RESULT_SCHEMAS = {
    "baseline": {"id", "decisions"},
    "forward": {
        "id", "decisions", "record_checkpoints", "run_count", "draft_count",
        "final_statuses",
    },
    "lifecycle": {
        "id", "record_checkpoints", "run_count", "draft_count", "final_statuses",
        "failure_disclosed", "selected_command",
    },
}
TURN_FIELDS = {"prompt", "dispatch_when"}
EXPECTED_DECISION_FIELDS = {"after_turn", "triggered"}
OBSERVED_DECISION_FIELDS = {
    "after_turn", "triggered", "task_type", "workflow_variant",
}
CHECKPOINT_FIELDS = {"after_turn", "records"}
NORMALIZED_RECORD_FIELDS = {
    "role", "status", "start_mode", "superseded_by_role",
}
SETUP_FIELDS = {"eval_override", "cli", "wiki_root"}
DISPATCH_VALUES = {
    "immediate", "after_single_file_mutation_without_run", "after_draft_run",
}
FIXTURE_VALUES = {"python-cli", "documentation", "wiki", "empty"}
LIFECYCLE_MODES = {"executable", "command-selection-only"}


def _is_nonempty_string(value):
    return isinstance(value, str) and bool(value.strip())


def _is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_fields(prefix, row, expected, errors):
    if not isinstance(row, dict):
        errors.append(f"{prefix}: expected object")
        return False
    missing = sorted(expected - set(row))
    extra = sorted(set(row) - expected)
    if missing:
        errors.append(f"{prefix}: missing fields: {', '.join(missing)}")
    if extra:
        errors.append(f"{prefix}: extra fields: {', '.join(extra)}")
    return not missing


def _validate_turns(prefix, turns, errors):
    if not isinstance(turns, list) or not turns:
        errors.append(f"{prefix}: turns must be a nonempty list")
        return
    for index, turn in enumerate(turns, 1):
        turn_prefix = f"{prefix} turn {index}"
        if not _validate_fields(turn_prefix, turn, TURN_FIELDS, errors):
            continue
        if not _is_nonempty_string(turn["prompt"]):
            errors.append(f"{turn_prefix}: prompt must be a nonempty string")
        dispatch_when = turn["dispatch_when"]
        if not isinstance(dispatch_when, str) or dispatch_when not in DISPATCH_VALUES:
            errors.append(f"{turn_prefix}: invalid dispatch_when")


def _validate_record(prefix, record, errors):
    if not _validate_fields(prefix, record, NORMALIZED_RECORD_FIELDS, errors):
        return
    role = record["role"]
    if not _is_nonempty_string(role) or not __import__("re").fullmatch(r"run-[1-9][0-9]*", role):
        errors.append(f"{prefix}: role must match run-N")
    if not _is_nonempty_string(record["status"]):
        errors.append(f"{prefix}: status must be a nonempty string")
    if not _is_nonempty_string(record["start_mode"]):
        errors.append(f"{prefix}: start_mode must be a nonempty string")
    superseded = record["superseded_by_role"]
    if superseded is not None and (
        not _is_nonempty_string(superseded)
        or not __import__("re").fullmatch(r"run-[1-9][0-9]*", superseded)
    ):
        errors.append(f"{prefix}: superseded_by_role must be null or match run-N")


def _validate_checkpoints(prefix, checkpoints, errors):
    if not isinstance(checkpoints, list):
        errors.append(f"{prefix}: record_checkpoints must be a list")
        return
    for checkpoint_index, checkpoint in enumerate(checkpoints, 1):
        checkpoint_prefix = f"{prefix} checkpoint {checkpoint_index}"
        if not _validate_fields(
            checkpoint_prefix, checkpoint, CHECKPOINT_FIELDS, errors
        ):
            continue
        if not _is_integer(checkpoint["after_turn"]):
            errors.append(f"{checkpoint_prefix}: after_turn must be an integer")
        records = checkpoint["records"]
        if not isinstance(records, list):
            errors.append(f"{checkpoint_prefix}: records must be a list")
            continue
        for record_index, record in enumerate(records, 1):
            _validate_record(
                f"{checkpoint_prefix} record {record_index}", record, errors
            )


def _validate_statuses(prefix, statuses, errors):
    if not isinstance(statuses, list):
        errors.append(f"{prefix}: final_statuses must be a list")
        return
    for status in statuses:
        if not _is_nonempty_string(status):
            errors.append(f"{prefix}: final_statuses must contain nonempty strings")
            return


def _validate_count(prefix, field, value, errors):
    if not _is_integer(value) or value < 0:
        errors.append(f"{prefix}: {field} must be a nonnegative integer")


def _validate_decision_manifest_row(prefix, row, errors):
    if not _validate_fields(prefix, row, DECISION_MANIFEST_FIELDS, errors):
        return
    if not _is_nonempty_string(row["id"]):
        errors.append(f"{prefix}: id must be a nonempty string")
    _validate_turns(prefix, row["turns"], errors)
    fixture = row["fixture"]
    if not isinstance(fixture, str) or fixture not in FIXTURE_VALUES:
        errors.append(f"{prefix}: invalid fixture")
    decisions = row["expected_decisions"]
    if not isinstance(decisions, list):
        errors.append(f"{prefix}: expected_decisions must be a list")
        decisions = []
    for index, decision in enumerate(decisions, 1):
        decision_prefix = f"{prefix} expected decision {index}"
        if not _validate_fields(
            decision_prefix, decision, EXPECTED_DECISION_FIELDS, errors
        ):
            continue
        if not _is_integer(decision["after_turn"]):
            errors.append(f"{decision_prefix}: after_turn must be an integer")
        if not isinstance(decision["triggered"], bool):
            errors.append(f"{decision_prefix}: triggered must be a boolean")
    triggered = any(
        isinstance(decision, dict) and decision.get("triggered") is True
        for decision in decisions
    )
    if triggered:
        for field in ("task_type", "workflow_variant"):
            if not _is_nonempty_string(row[field]):
                errors.append(f"{prefix}: triggered {field} must be a nonempty string")
    else:
        for field in ("task_type", "workflow_variant"):
            if row[field] is not None:
                errors.append(f"{prefix}: untriggered {field} must be null")
    _validate_checkpoints(prefix, row["expected_record_checkpoints"], errors)
    _validate_count(prefix, "expected_run_count", row["expected_run_count"], errors)
    _validate_statuses(prefix, row["expected_final_statuses"], errors)


def _validate_lifecycle_manifest_row(prefix, row, errors):
    if not _validate_fields(prefix, row, LIFECYCLE_MANIFEST_FIELDS, errors):
        return
    if not _is_nonempty_string(row["id"]):
        errors.append(f"{prefix}: id must be a nonempty string")
    _validate_turns(prefix, row["turns"], errors)
    fixture = row["fixture"]
    if not isinstance(fixture, str) or fixture not in FIXTURE_VALUES:
        errors.append(f"{prefix}: invalid fixture")
    mode = row["mode"]
    if not isinstance(mode, str) or mode not in LIFECYCLE_MODES:
        errors.append(f"{prefix}: invalid lifecycle mode")
    setup = row["setup"]
    if _validate_fields(f"{prefix} setup", setup, SETUP_FIELDS, errors):
        for field in sorted(SETUP_FIELDS):
            if not _is_nonempty_string(setup[field]):
                errors.append(
                    f"{prefix} setup: {field} must be a nonempty string"
                )
    store_fields = (
        "expected_record_checkpoints", "expected_run_count", "expected_draft_count",
        "expected_final_statuses", "expect_failure_disclosure",
    )
    if mode == "command-selection-only":
        for field in store_fields:
            if row[field] is not None:
                errors.append(
                    f"{prefix}: command-selection-only {field} must be null"
                )
        if not _is_nonempty_string(row["expected_selected_command"]):
            errors.append(
                f"{prefix}: command-selection-only expected_selected_command "
                "must be a nonempty string"
            )
    elif mode == "executable":
        _validate_checkpoints(prefix, row["expected_record_checkpoints"], errors)
        _validate_count(
            prefix, "expected_run_count", row["expected_run_count"], errors
        )
        _validate_count(
            prefix, "expected_draft_count", row["expected_draft_count"], errors
        )
        _validate_statuses(prefix, row["expected_final_statuses"], errors)
        if not isinstance(row["expect_failure_disclosure"], bool):
            errors.append(
                f"{prefix}: executable expect_failure_disclosure must be a boolean"
            )
        if row["expected_selected_command"] is not None:
            errors.append(
                f"{prefix}: executable expected_selected_command must be null"
            )


def validate_manifest_schema(mode, rows):
    """Validate the exact manifest schema for one explicit runner mode."""
    errors = []
    for index, row in enumerate(rows, 1):
        prefix = f"manifest row {index}"
        if mode in {"baseline", "forward"}:
            _validate_decision_manifest_row(prefix, row, errors)
        elif mode == "lifecycle":
            _validate_lifecycle_manifest_row(prefix, row, errors)
        else:
            raise ValueError(f"unknown mode: {mode}")
    return errors


def validate_id_set(manifest, results):
    """Require nonempty unique IDs and an exact manifest/result ID set."""
    errors = []
    valid_ids = {}
    for label, rows in (("manifest", manifest), ("result", results)):
        ids = []
        seen = set()
        for index, row in enumerate(rows, 1):
            row_id = row.get("id") if isinstance(row, dict) else None
            if not _is_nonempty_string(row_id):
                errors.append(f"{label} row {index}: id must be a nonempty string")
                continue
            ids.append(row_id)
            if row_id in seen:
                duplicate = f"duplicate {label} id: {row_id}"
                if duplicate not in errors:
                    errors.append(duplicate)
            seen.add(row_id)
        valid_ids[label] = ids
    manifest_ids = valid_ids["manifest"]
    result_ids = valid_ids["result"]
    result_set = set(result_ids)
    manifest_set = set(manifest_ids)
    for case_id in manifest_ids:
        if case_id not in result_set:
            errors.append(f"missing result id: {case_id}")
    seen_extra = set()
    for case_id in result_ids:
        if case_id not in manifest_set and case_id not in seen_extra:
            errors.append(f"extra result id: {case_id}")
            seen_extra.add(case_id)
    return errors


def score_results(manifest, results):
    by_id = {row["id"]: row for row in results if "id" in row}
    trigger_hits = taxonomy_hits = 0
    errors = []
    for case in manifest:
        actual = by_id.get(case["id"])
        expected_trigger_sequence = [
            (row["after_turn"], row["triggered"])
            for row in case["expected_decisions"]
        ]
        actual_trigger_sequence = [
            (row.get("after_turn"), row.get("triggered"))
            for row in actual.get("decisions", [])
        ] if actual else []
        if actual is None or actual_trigger_sequence != expected_trigger_sequence:
            errors.append(f"{case['id']}: trigger mismatch")
            continue
        trigger_hits += 1
        triggered_rows = [row for row in actual["decisions"] if row.get("triggered")]
        if not triggered_rows or all(
            row.get("task_type") == case.get("task_type")
            and row.get("workflow_variant") == case.get("workflow_variant")
            for row in triggered_rows
        ):
            taxonomy_hits += 1
        else:
            errors.append(f"{case['id']}: taxonomy mismatch")
    return trigger_hits, taxonomy_hits, errors


def _by_unique_id(rows):
    by_id = {}
    for row in rows:
        by_id.setdefault(row.get("id"), row)
    return by_id


def validate_decision_recording(manifest, results):
    errors = validate_id_set(manifest, results)
    if errors:
        return errors
    by_id = _by_unique_id(results)
    for case in manifest:
        case_id = case["id"]
        actual = by_id[case_id]
        if actual.get("record_checkpoints") != case.get("expected_record_checkpoints", []):
            errors.append(f"{case_id}: record checkpoints mismatch")
        if actual.get("run_count") != case.get("expected_run_count"):
            errors.append(f"{case_id}: run count mismatch")
        if actual.get("draft_count") != 0:
            errors.append(f"{case_id}: draft records remain")
        if sorted(actual.get("final_statuses", [])) != sorted(case.get("expected_final_statuses", [])):
            errors.append(f"{case_id}: final statuses mismatch")
    return errors


def validate_lifecycle_results(manifest, results):
    errors = validate_id_set(manifest, results)
    if errors:
        return errors
    by_id = _by_unique_id(results)
    for case in manifest:
        case_id = case["id"]
        actual = by_id[case_id]
        if actual.get("record_checkpoints") != case.get("expected_record_checkpoints"):
            errors.append(f"{case_id}: record checkpoints mismatch")
        if actual.get("run_count") != case.get("expected_run_count"):
            errors.append(f"{case_id}: run count mismatch")
        if actual.get("draft_count") != case.get("expected_draft_count"):
            errors.append(f"{case_id}: draft count mismatch")
        expected_statuses = case.get("expected_final_statuses")
        actual_statuses = actual.get("final_statuses")
        if (
            sorted(actual_statuses) if isinstance(actual_statuses, list) else actual_statuses
        ) != (
            sorted(expected_statuses) if isinstance(expected_statuses, list) else expected_statuses
        ):
            errors.append(f"{case_id}: final statuses mismatch")
        if actual.get("failure_disclosed") != case.get("expect_failure_disclosure"):
            errors.append(f"{case_id}: failure disclosure mismatch")
        if actual.get("selected_command") != case.get("expected_selected_command"):
            errors.append(f"{case_id}: selected command mismatch")
    return errors


def _validate_observed_decisions(prefix, decisions, errors):
    if not isinstance(decisions, list):
        errors.append(f"{prefix}: decisions must be a list")
        return
    for index, decision in enumerate(decisions, 1):
        decision_prefix = f"{prefix} decision {index}"
        if not _validate_fields(
            decision_prefix, decision, OBSERVED_DECISION_FIELDS, errors
        ):
            continue
        if not _is_integer(decision["after_turn"]):
            errors.append(f"{decision_prefix}: after_turn must be an integer")
        triggered = decision["triggered"]
        if not isinstance(triggered, bool):
            errors.append(f"{decision_prefix}: triggered must be a boolean")
            continue
        taxonomy = (decision["task_type"], decision["workflow_variant"])
        if triggered and not all(_is_nonempty_string(value) for value in taxonomy):
            errors.append(
                f"{decision_prefix}: triggered taxonomy must contain nonempty strings"
            )
        if not triggered and taxonomy != (None, None):
            errors.append(f"{decision_prefix}: untriggered taxonomy must be null")


def _validate_forward_store(prefix, row, errors):
    _validate_checkpoints(prefix, row["record_checkpoints"], errors)
    _validate_count(prefix, "run_count", row["run_count"], errors)
    _validate_count(prefix, "draft_count", row["draft_count"], errors)
    _validate_statuses(prefix, row["final_statuses"], errors)


def _validate_lifecycle_result(prefix, row, manifest_case, errors):
    mode = manifest_case.get("mode") if manifest_case else None
    if mode == "command-selection-only":
        for field in (
            "record_checkpoints", "run_count", "draft_count", "final_statuses",
            "failure_disclosed",
        ):
            if row[field] is not None:
                errors.append(
                    f"{prefix}: command-selection-only {field} must be null"
                )
        if not _is_nonempty_string(row["selected_command"]):
            errors.append(
                f"{prefix}: command-selection-only selected_command "
                "must be a nonempty string"
            )
    elif mode == "executable":
        _validate_forward_store(prefix, row, errors)
        if not isinstance(row["failure_disclosed"], bool):
            errors.append(
                f"{prefix}: executable failure_disclosed must be a boolean"
            )
        if row["selected_command"] is not None:
            errors.append(f"{prefix}: executable selected_command must be null")
    else:
        if row["record_checkpoints"] is not None:
            _validate_checkpoints(prefix, row["record_checkpoints"], errors)
        for field in ("run_count", "draft_count"):
            if row[field] is not None:
                _validate_count(prefix, field, row[field], errors)
        if row["final_statuses"] is not None:
            _validate_statuses(prefix, row["final_statuses"], errors)
        if row["failure_disclosed"] is not None and not isinstance(
            row["failure_disclosed"], bool
        ):
            errors.append(f"{prefix}: failure_disclosed must be null or a boolean")
        if row["selected_command"] is not None and not _is_nonempty_string(
            row["selected_command"]
        ):
            errors.append(f"{prefix}: selected_command must be null or nonempty")


def validate_result_schema(mode, rows, manifest=None):
    errors = []
    expected = RESULT_SCHEMAS[mode]
    manifest_by_id = {
        row["id"]: row for row in (manifest or [])
        if isinstance(row, dict) and _is_nonempty_string(row.get("id"))
    }
    for index, row in enumerate(rows, 1):
        prefix = f"result row {index}"
        if not _validate_fields(prefix, row, expected, errors):
            continue
        if not _is_nonempty_string(row["id"]):
            errors.append(f"{prefix}: id must be a nonempty string")
        if mode in {"baseline", "forward"}:
            _validate_observed_decisions(prefix, row["decisions"], errors)
        if mode == "forward":
            _validate_forward_store(prefix, row, errors)
        elif mode == "lifecycle":
            result_id = row["id"]
            manifest_row = (
                manifest_by_id.get(result_id)
                if _is_nonempty_string(result_id)
                else None
            )
            _validate_lifecycle_result(
                prefix, row, manifest_row, errors
            )
    return errors


def _load_rows(path):
    try:
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read {path}: {error}") from error
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected a JSON array")
    return rows


def _case_hits(manifest, errors):
    failed = {
        error.split(":", 1)[0]
        for error in errors
        if ":" in error and error.split(":", 1)[0] in {row["id"] for row in manifest}
    }
    return len(manifest) - len(failed)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Score observing-workflows evaluations")
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--baseline", nargs=2, metavar=("MANIFEST", "RESULTS"))
    modes.add_argument("--forward", nargs=2, metavar=("MANIFEST", "RESULTS"))
    modes.add_argument("--lifecycle", nargs=2, metavar=("MANIFEST", "RESULTS"))
    args = parser.parse_args(argv)
    mode = next(name for name in RESULT_SCHEMAS if getattr(args, name) is not None)
    manifest_path, results_path = getattr(args, mode)
    try:
        manifest = _load_rows(manifest_path)
        results = _load_rows(results_path)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1

    errors = validate_manifest_schema(mode, manifest)
    if not errors:
        errors.extend(validate_result_schema(mode, results, manifest))
    if not errors:
        errors.extend(validate_id_set(manifest, results))
    if not errors and mode in {"baseline", "forward"}:
        trigger_hits, taxonomy_hits, score_errors = score_results(manifest, results)
        print(f"Trigger accuracy: {trigger_hits}/{len(manifest)}")
        print(f"Taxonomy accuracy: {taxonomy_hits}/{len(manifest)}")
        if mode == "baseline":
            for error in score_errors:
                print(error, file=sys.stderr)
        else:
            errors.extend(score_errors)
            recording_errors = validate_decision_recording(manifest, results)
            errors.extend(recording_errors)
            print(f"Recording accuracy: {_case_hits(manifest, recording_errors)}/{len(manifest)}")
    elif not errors:
        lifecycle_errors = validate_lifecycle_results(manifest, results)
        errors.extend(lifecycle_errors)
        print(f"Lifecycle: {_case_hits(manifest, lifecycle_errors)}/{len(manifest)}")
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


class EvalScoreTests(unittest.TestCase):
    @staticmethod
    def _decision_manifest():
        return [{
            "id": "feature",
            "turns": [{"prompt": "Implement the feature.", "dispatch_when": "immediate"}],
            "fixture": "python-cli",
            "expected_decisions": [{"after_turn": 1, "triggered": True}],
            "task_type": "feature",
            "workflow_variant": "implementation-basic",
            "expected_record_checkpoints": [{
                "after_turn": 1,
                "records": [{
                    "role": "run-1",
                    "status": "success",
                    "start_mode": "planned",
                    "superseded_by_role": None,
                }],
            }],
            "expected_run_count": 1,
            "expected_final_statuses": ["success"],
        }]

    @staticmethod
    def _lifecycle_manifest(mode="executable"):
        command_selection = mode == "command-selection-only"
        return [{
            "id": "lifecycle-case",
            "turns": [{"prompt": "Implement the feature.", "dispatch_when": "immediate"}],
            "fixture": "python-cli",
            "mode": mode,
            "setup": {
                "eval_override": "complete",
                "cli": "temporary",
                "wiki_root": "temporary",
            },
            "expected_record_checkpoints": None if command_selection else [],
            "expected_run_count": None if command_selection else 0,
            "expected_draft_count": None if command_selection else 0,
            "expected_final_statuses": None if command_selection else [],
            "expect_failure_disclosure": None if command_selection else False,
            "expected_selected_command": "python3 wiki_cli.py observe" if command_selection else None,
        }]

    def test_frozen_manifests_match_exact_mode_specific_schemas(self):
        decision_manifest = _load_rows(
            Path(__file__).parent / "skill_evals" / "observing_workflows_cases.json"
        )
        lifecycle_manifest = _load_rows(
            Path(__file__).parent / "skill_evals" / "observing_workflows_lifecycle_cases.json"
        )

        self.assertEqual([], validate_manifest_schema("baseline", decision_manifest))
        self.assertEqual([], validate_manifest_schema("forward", decision_manifest))
        self.assertEqual([], validate_manifest_schema("lifecycle", lifecycle_manifest))

    def test_manifest_schema_rejects_extra_fields_and_malformed_nested_rows(self):
        manifest = self._decision_manifest()
        manifest[0]["unexpected"] = True
        manifest[0]["turns"][0]["dispatch_when"] = "later"
        manifest[0]["expected_decisions"][0]["after_turn"] = "1"
        manifest[0]["expected_record_checkpoints"][0]["records"][0]["status"] = 7

        self.assertEqual(
            [
                "manifest row 1: extra fields: unexpected",
                "manifest row 1 turn 1: invalid dispatch_when",
                "manifest row 1 expected decision 1: after_turn must be an integer",
                "manifest row 1 checkpoint 1 record 1: status must be a nonempty string",
            ],
            validate_manifest_schema("forward", manifest),
        )

    def test_lifecycle_manifest_schema_enforces_mode_specific_null_rules(self):
        executable = self._lifecycle_manifest()[0]
        executable["expected_selected_command"] = "must not be set"
        selection = self._lifecycle_manifest("command-selection-only")[0]
        selection["expected_run_count"] = 0

        self.assertEqual(
            [
                "manifest row 1: executable expected_selected_command must be null",
                "manifest row 2: command-selection-only expected_run_count must be null",
            ],
            validate_manifest_schema("lifecycle", [executable, selection]),
        )

    def test_result_schema_rejects_invalid_ids_and_malformed_nested_rows(self):
        manifest = self._decision_manifest()
        results = [{
            "id": "",
            "decisions": [{
                "after_turn": 1,
                "triggered": "yes",
                "task_type": None,
                "workflow_variant": None,
                "unexpected": True,
            }],
        }]

        self.assertEqual(
            [
                "result row 1: id must be a nonempty string",
                "result row 1 decision 1: extra fields: unexpected",
                "result row 1 decision 1: triggered must be a boolean",
            ],
            validate_result_schema("baseline", results, manifest),
        )

    def test_result_schema_enforces_lifecycle_mode_specific_null_rules(self):
        manifest = self._lifecycle_manifest("command-selection-only")
        results = [{
            "id": "lifecycle-case",
            "record_checkpoints": [],
            "run_count": None,
            "draft_count": None,
            "final_statuses": None,
            "failure_disclosed": None,
            "selected_command": "python3 wiki_cli.py observe",
        }]

        self.assertEqual(
            ["result row 1: command-selection-only record_checkpoints must be null"],
            validate_result_schema("lifecycle", results, manifest),
        )

    def test_validate_id_set_rejects_invalid_and_duplicate_manifest_ids(self):
        manifest = [{"id": ""}, {"id": "one"}, {"id": "one"}]
        results = [{"id": 7}, {"id": "one"}]

        self.assertEqual(
            [
                "manifest row 1: id must be a nonempty string",
                "duplicate manifest id: one",
                "result row 1: id must be a nonempty string",
            ],
            validate_id_set(manifest, results),
        )

    def test_every_runner_mode_validates_manifest_before_scoring(self):
        malformed_by_mode = {
            "baseline": [{"id": "case"}],
            "forward": [{"id": "case"}],
            "lifecycle": [{"id": "case"}],
        }
        results_by_mode = {
            "baseline": [{"id": "case", "decisions": []}],
            "forward": [{
                "id": "case", "decisions": [], "record_checkpoints": [],
                "run_count": 0, "draft_count": 0, "final_statuses": [],
            }],
            "lifecycle": [{
                "id": "case", "record_checkpoints": [], "run_count": 0,
                "draft_count": 0, "final_statuses": [],
                "failure_disclosed": False, "selected_command": None,
            }],
        }
        with tempfile.TemporaryDirectory() as temporary:
            for mode in ("baseline", "forward", "lifecycle"):
                with self.subTest(mode=mode):
                    manifest_path = Path(temporary) / f"{mode}-manifest.json"
                    result_path = Path(temporary) / f"{mode}-results.json"
                    manifest_path.write_text(
                        json.dumps(malformed_by_mode[mode]), encoding="utf-8"
                    )
                    result_path.write_text(
                        json.dumps(results_by_mode[mode]), encoding="utf-8"
                    )
                    stdout = io.StringIO()
                    stderr = io.StringIO()
                    with redirect_stdout(stdout), redirect_stderr(stderr):
                        exit_code = main([
                            f"--{mode}", str(manifest_path), str(result_path)
                        ])

                    self.assertEqual(1, exit_code)
                    self.assertIn("manifest row 1: missing fields:", stderr.getvalue())
                    self.assertNotIn("accuracy", stdout.getvalue().lower())
                    self.assertNotIn("Lifecycle:", stdout.getvalue())

    def test_cli_rejects_unhashable_schema_values_without_traceback(self):
        decision_manifest = self._decision_manifest()
        lifecycle_manifest = self._lifecycle_manifest()
        baseline_result = [{
            "id": "feature",
            "decisions": [{
                "after_turn": 1,
                "triggered": False,
                "task_type": None,
                "workflow_variant": None,
            }],
        }]
        forward_result = [{
            **baseline_result[0],
            "record_checkpoints": [],
            "run_count": 0,
            "draft_count": 0,
            "final_statuses": [],
        }]
        lifecycle_result = [{
            "id": "lifecycle-case",
            "record_checkpoints": [],
            "run_count": 0,
            "draft_count": 0,
            "final_statuses": [],
            "failure_disclosed": False,
            "selected_command": None,
        }]
        scenarios = []

        bad_dispatch = json.loads(json.dumps(decision_manifest))
        bad_dispatch[0]["turns"][0]["dispatch_when"] = []
        scenarios.append((
            "baseline", bad_dispatch, baseline_result,
            "manifest row 1 turn 1: invalid dispatch_when",
        ))

        bad_fixture = json.loads(json.dumps(decision_manifest))
        bad_fixture[0]["fixture"] = {}
        scenarios.append((
            "forward", bad_fixture, forward_result,
            "manifest row 1: invalid fixture",
        ))

        bad_mode = json.loads(json.dumps(lifecycle_manifest))
        bad_mode[0]["mode"] = []
        scenarios.append((
            "lifecycle", bad_mode, lifecycle_result,
            "manifest row 1: invalid lifecycle mode",
        ))

        bad_result_id = json.loads(json.dumps(lifecycle_result))
        bad_result_id[0]["id"] = []
        scenarios.append((
            "lifecycle", lifecycle_manifest, bad_result_id,
            "result row 1: id must be a nonempty string",
        ))

        with tempfile.TemporaryDirectory() as temporary:
            for index, (mode, manifest, results, expected_error) in enumerate(
                scenarios, 1
            ):
                with self.subTest(mode=mode, expected_error=expected_error):
                    manifest_path = Path(temporary) / f"manifest-{index}.json"
                    result_path = Path(temporary) / f"result-{index}.json"
                    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                    result_path.write_text(json.dumps(results), encoding="utf-8")

                    completed = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            f"--{mode}",
                            str(manifest_path),
                            str(result_path),
                        ],
                        text=True,
                        capture_output=True,
                    )

                    self.assertEqual(1, completed.returncode)
                    self.assertIn(expected_error, completed.stderr)
                    self.assertNotIn("Traceback", completed.stderr)

    def test_nonperfect_baseline_is_evidence_not_command_failure(self):
        manifest = self._decision_manifest()
        results = [{
            "id": "feature",
            "decisions": [{
                "after_turn": 1, "triggered": False,
                "task_type": None, "workflow_variant": None,
            }],
        }]
        with tempfile.TemporaryDirectory() as temporary:
            manifest_path = Path(temporary) / "manifest.json"
            result_path = Path(temporary) / "results.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            result_path.write_text(json.dumps(results), encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                exit_code = main(["--baseline", str(manifest_path), str(result_path)])

        self.assertEqual(0, exit_code)
        self.assertIn("Trigger accuracy: 0/1", stdout.getvalue())
        self.assertIn("feature: trigger mismatch", stderr.getvalue())

    def test_score_detects_trigger_and_taxonomy_mismatches(self):
        manifest = [{
            "id": "feature",
            "expected_decisions": [{"after_turn": 1, "triggered": True}],
            "task_type": "feature",
            "workflow_variant": "implementation-basic",
        }]
        results = [{
            "id": "feature",
            "decisions": [{"after_turn": 1, "triggered": False, "task_type": None, "workflow_variant": None}],
        }]
        trigger_hits, taxonomy_hits, errors = score_results(manifest, results)
        self.assertEqual((0, 0), (trigger_hits, taxonomy_hits))
        self.assertEqual(["feature: trigger mismatch"], errors)

    def test_lifecycle_validator_detects_store_and_disclosure_mismatches(self):
        manifest = [{
            "id": "failed-task",
            "expected_record_checkpoints": [],
            "expected_run_count": 1,
            "expected_draft_count": 0,
            "expected_final_statuses": ["failed"],
            "expect_failure_disclosure": False,
            "expected_selected_command": None,
        }]
        results = [{
            "id": "failed-task",
            "record_checkpoints": [],
            "run_count": 1,
            "draft_count": 1,
            "final_statuses": [],
            "failure_disclosed": False,
            "selected_command": None,
        }]
        self.assertEqual(
            ["failed-task: draft count mismatch", "failed-task: final statuses mismatch"],
            validate_lifecycle_results(manifest, results),
        )

    def test_decision_recording_validator_requires_final_records(self):
        manifest = [{
            "id": "feature",
            "expected_record_checkpoints": [{
                "after_turn": 1,
                "records": [{"role": "run-1", "status": "draft", "start_mode": "planned", "superseded_by_role": None}],
            }],
            "expected_run_count": 1,
            "expected_final_statuses": ["success"],
        }]
        results = [{
            "id": "feature",
            "record_checkpoints": [{
                "after_turn": 1,
                "records": [{"role": "run-1", "status": "draft", "start_mode": "planned", "superseded_by_role": None}],
            }],
            "run_count": 1,
            "draft_count": 1,
            "final_statuses": [],
        }]
        self.assertEqual(
            ["feature: draft records remain", "feature: final statuses mismatch"],
            validate_decision_recording(manifest, results),
        )

    def test_result_ids_must_match_manifest_exactly(self):
        manifest = [{"id": "one"}, {"id": "two"}]
        results = [{"id": "one"}, {"id": "one"}, {"id": "extra"}]
        self.assertEqual(
            ["duplicate result id: one", "missing result id: two", "extra result id: extra"],
            validate_id_set(manifest, results),
        )


if __name__ == "__main__":
    if any(argument in {"--baseline", "--forward", "--lifecycle"} for argument in sys.argv[1:]):
        sys.exit(main())
    unittest.main()
