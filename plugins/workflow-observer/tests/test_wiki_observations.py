import sys
from pathlib import Path
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))

from wiki_observations import ObservationError, StartRequest, validate_start_request
from workflow_observer_cli import build_parser


def valid_request(*, agent_surface: object) -> StartRequest:
    return StartRequest(
        title="Surface contract",
        project="workflow-observatory",
        workspace="workflow-observatory",
        workspace_id="7f4a1c29e083",
        revision="unknown",
        working_tree="clean",
        agent_surface=agent_surface,
        start_mode="planned",
        task_type="feature",
        workflow_variant="implementation-with-review",
        task_ref=None,
        sources=(),
    )


class AgentSurfaceTests(unittest.TestCase):
    def test_exact_supported_surfaces_validate(self):
        for surface in ("codex", "claude"):
            with self.subTest(surface=surface):
                validate_start_request(valid_request(agent_surface=surface))

    def test_every_other_surface_is_rejected(self):
        for surface in ("", "Claude", "codex-cli", "other", [], {}):
            with self.subTest(surface=surface):
                with self.assertRaisesRegex(
                    ObservationError,
                    "agent_surface must be codex or claude",
                ):
                    validate_start_request(valid_request(agent_surface=surface))

    def test_parser_rejects_unhashable_caller_surfaces(self):
        for surface in ([], {}):
            with self.subTest(surface=surface):
                with self.assertRaisesRegex(
                    ObservationError,
                    "agent_surface must be codex or claude",
                ):
                    build_parser(agent_surface=surface)


if __name__ == "__main__":
    unittest.main()
