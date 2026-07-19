#!/usr/bin/env python3
"""Claude Code entry point for the shared Workflow Observatory CLI."""

from pathlib import Path

from workflow_observer_cli import main as shared_main


if __name__ == "__main__":
    raise SystemExit(
        shared_main(
            agent_surface="claude",
            default_home=Path.home() / ".claude" / "workflow-observatory",
        )
    )
