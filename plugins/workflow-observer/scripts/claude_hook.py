#!/usr/bin/env python3
"""Best-effort Claude SubagentStart parent-marker injection."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Sequence

from claude_session_bindings import BindingError, lookup_session


_MAX_HOOK_INPUT_BYTES = 1_048_576


def _plugin_data(argv: Sequence[str], environ: dict[str, str]) -> Path | None:
    if len(argv) == 2 and argv[0] == "--plugin-data" and argv[1]:
        return Path(argv[1])
    value = environ.get("CLAUDE_PLUGIN_DATA")
    return Path(value) if value else None


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    environment = dict(os.environ if environ is None else environ)
    try:
        raw = sys.stdin.buffer.read(_MAX_HOOK_INPUT_BYTES + 1)
        if len(raw) > _MAX_HOOK_INPUT_BYTES:
            return 0
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            return 0
        if payload.get("hook_event_name") != "SubagentStart":
            return 0
        session_id = payload.get("session_id")
        if not isinstance(session_id, str) or not session_id:
            return 0
        plugin_data = _plugin_data(arguments, environment)
        if plugin_data is None:
            return 0
        run_id = lookup_session(plugin_data, session_id)
        if run_id is None:
            return 0
        marker = (
            f"Observation managed by parent run {run_id}; "
            "do not start a child observation."
        )
        response = {
            "hookSpecificOutput": {
                "hookEventName": "SubagentStart",
                "additionalContext": marker,
            }
        }
        sys.stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
        return 0
    except (BindingError, OSError, UnicodeError, json.JSONDecodeError, ValueError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
