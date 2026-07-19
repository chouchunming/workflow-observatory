#!/usr/bin/env python3
"""Claude Code entry point plus private parent-propagation lifecycle."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import os
from pathlib import Path
import re
import sys
from typing import Mapping, Sequence

from claude_session_bindings import BindingError, bind_session, unbind_session
from workflow_observer_cli import main as shared_main


_RUN_ID_RE = re.compile(r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
_DEGRADED_NOTICE = (
    "workflow observer notice: hook-assisted parent propagation unavailable\n"
)


def _binding_environment(environ: Mapping[str, str]) -> tuple[Path, str] | None:
    plugin_data = environ.get("CLAUDE_PLUGIN_DATA")
    session_id = environ.get("CLAUDE_SESSION_ID")
    if not plugin_data or not session_id:
        return None
    try:
        return Path(plugin_data), session_id
    except (TypeError, ValueError):
        return None


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    environment = os.environ if environ is None else environ
    captured_stdout = StringIO()
    captured_stderr = StringIO()
    with redirect_stdout(captured_stdout), redirect_stderr(captured_stderr):
        result = shared_main(
            arguments,
            agent_surface="claude",
            default_home=Path.home() / ".claude" / "workflow-observatory",
        )

    stdout = captured_stdout.getvalue()
    stderr = captured_stderr.getvalue()
    binding = _binding_environment(environment)
    binding_failed = False
    if result == 0 and arguments:
        try:
            if arguments[0] == "start":
                run_id = stdout.strip()
                if binding is None or _RUN_ID_RE.fullmatch(run_id) is None:
                    binding_failed = True
                else:
                    bind_session(binding[0], binding[1], run_id)
            elif arguments[0] == "finish" and len(arguments) >= 2:
                if binding is None:
                    binding_failed = True
                else:
                    run_id = arguments[1]
                    if _RUN_ID_RE.fullmatch(run_id) is not None:
                        unbind_session(binding[0], binding[1], expected_run_id=run_id)
        except (BindingError, OSError, ValueError):
            binding_failed = True

    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    if binding_failed:
        sys.stderr.write(_DEGRADED_NOTICE)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
