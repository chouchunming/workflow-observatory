import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PLUGIN_ROOT / "scripts"
HOOK = SCRIPTS / "claude_hook.py"
HOOKS_JSON = PLUGIN_ROOT / "adapters/claude/hooks/hooks.json"
sys.path.insert(0, str(SCRIPTS))

from claude_session_bindings import bind_session


RUN_ID = "obs-20260719-120000-abcdef"
SESSION_ID = "opaque-session-id"
SENTINEL = "SENSITIVE-SENTINEL-PROMPT-TRANSCRIPT-TOOL-DATA"
MARKER = (
    f"Observation managed by parent run {RUN_ID}; "
    "do not start a child observation."
)


class ClaudeHookTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        self.plugin_data = self.base / "plugin-data"
        self.plugin_data.mkdir(mode=0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def run_hook(self, payload: object) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(HOOK), "--plugin-data", str(self.plugin_data)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )

    def assert_sentinel_absent(self, completed: subprocess.CompletedProcess[str]) -> None:
        self.assertNotIn(SENTINEL, completed.stdout)
        self.assertNotIn(SENTINEL, completed.stderr)
        for path in self.plugin_data.rglob("*"):
            self.assertNotIn(SENTINEL, path.name)
            if path.is_file():
                self.assertNotIn(SENTINEL, path.read_text(encoding="utf-8"))

    def test_subagent_start_emits_exact_additional_context_and_ignores_sensitive_fields(self):
        bind_session(self.plugin_data, SESSION_ID, RUN_ID)
        fixture = {
            "hook_event_name": "SubagentStart",
            "session_id": SESSION_ID,
            "prompt": SENTINEL,
            "cwd": f"/private/{SENTINEL}",
            "transcript_path": f"/private/{SENTINEL}.jsonl",
            "agent_transcript_path": f"/private/agent-{SENTINEL}.jsonl",
            "messages": [{"content": SENTINEL}],
            "tool_input": {"secret": SENTINEL},
        }

        completed = self.run_hook(fixture)

        expected = json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "SubagentStart",
                    "additionalContext": MARKER,
                }
            },
            separators=(",", ":"),
        ) + "\n"
        self.assertEqual(
            (0, expected, ""),
            (completed.returncode, completed.stdout, completed.stderr),
        )
        self.assert_sentinel_absent(completed)
        self.assertFalse((self.plugin_data / "store").exists())

    def test_non_subagent_and_invalid_inputs_are_nonblocking_and_silent(self):
        bind_session(self.plugin_data, SESSION_ID, RUN_ID)
        payloads = (
            {"hook_event_name": "Stop", "session_id": SESSION_ID, "prompt": SENTINEL},
            {"hook_event_name": "SubagentStart", "prompt": SENTINEL},
            {"hook_event_name": "SubagentStart", "session_id": 3, "prompt": SENTINEL},
            ["SubagentStart", SESSION_ID, SENTINEL],
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                completed = self.run_hook(payload)
                self.assertEqual(
                    (0, "", ""),
                    (completed.returncode, completed.stdout, completed.stderr),
                )
                self.assert_sentinel_absent(completed)

    def test_missing_invalid_symlinked_and_permissive_binding_emit_no_marker(self):
        missing = self.run_hook(
            {"hook_event_name": "SubagentStart", "session_id": SESSION_ID, "prompt": SENTINEL}
        )
        self.assertEqual((0, "", ""), (missing.returncode, missing.stdout, missing.stderr))

        bind_session(self.plugin_data, SESSION_ID, RUN_ID)
        binding = next((self.plugin_data / "session-bindings").glob("*.json"))
        binding.write_text("{}", encoding="utf-8")
        invalid = self.run_hook(
            {"hook_event_name": "SubagentStart", "session_id": SESSION_ID, "prompt": SENTINEL}
        )
        self.assertEqual((0, "", ""), (invalid.returncode, invalid.stdout, invalid.stderr))

        binding.unlink()
        target = self.base / "target.json"
        target.write_text(
            json.dumps({"schema_version": 1, "run_id": RUN_ID, "state": "active"}),
            encoding="utf-8",
        )
        target.chmod(0o600)
        binding.symlink_to(target)
        symlinked = self.run_hook(
            {"hook_event_name": "SubagentStart", "session_id": SESSION_ID, "prompt": SENTINEL}
        )
        self.assertEqual((0, "", ""), (symlinked.returncode, symlinked.stdout, symlinked.stderr))

        binding.unlink()
        binding.write_text(target.read_text(encoding="utf-8"), encoding="utf-8")
        binding.chmod(0o644)
        permissive = self.run_hook(
            {"hook_event_name": "SubagentStart", "session_id": SESSION_ID, "prompt": SENTINEL}
        )
        self.assertEqual((0, "", ""), (permissive.returncode, permissive.stdout, permissive.stderr))
        for completed in (missing, invalid, symlinked, permissive):
            self.assert_sentinel_absent(completed)

    def test_malformed_or_oversized_json_is_nonblocking_and_silent(self):
        for raw_input in ("not-json", "{", "x" * 1_048_577):
            with self.subTest(size=len(raw_input)):
                completed = subprocess.run(
                    [sys.executable, str(HOOK), "--plugin-data", str(self.plugin_data)],
                    input=raw_input,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(
                    (0, "", ""),
                    (completed.returncode, completed.stdout, completed.stderr),
                )

    def test_hook_manifest_declares_only_static_subagent_start_command(self):
        payload = json.loads(HOOKS_JSON.read_text(encoding="utf-8"))

        self.assertEqual({"hooks"}, set(payload))
        self.assertEqual({"SubagentStart"}, set(payload["hooks"]))
        declarations = payload["hooks"]["SubagentStart"]
        self.assertEqual(1, len(declarations))
        self.assertEqual({"hooks"}, set(declarations[0]))
        self.assertEqual(1, len(declarations[0]["hooks"]))
        command = declarations[0]["hooks"][0]
        self.assertEqual("command", command["type"])
        self.assertEqual(
            'python3 "${CLAUDE_PLUGIN_ROOT}/scripts/claude_hook.py" '
            '--plugin-data "${CLAUDE_PLUGIN_DATA}"',
            command["command"],
        )
        self.assertNotIn("UserPromptSubmit", HOOKS_JSON.read_text(encoding="utf-8"))
        self.assertNotIn("Stop", HOOKS_JSON.read_text(encoding="utf-8"))
        self.assertNotIn("SessionEnd", HOOKS_JSON.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
