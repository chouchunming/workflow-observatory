import json
from pathlib import Path
import sys
import unittest

MARKET_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE = MARKET_ROOT / ".agents/plugins/marketplace.json"
PLUGIN_MANIFEST = PLUGIN_ROOT / ".codex-plugin/plugin.json"
if str(PLUGIN_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT / "scripts"))

from snapshot_input import SNAPSHOT_ANALYZER_FILES


class ManifestTests(unittest.TestCase):
    def test_snapshot_analyzer_source_manifest_is_exact_and_sorted(self):
        self.assertEqual(
            (
                "scripts/artifact_migration.py",
                "scripts/artifact_schema.py",
                "scripts/episode_schema.py",
                "scripts/learning_snapshot.py",
                "scripts/policy_artifacts.py",
                "scripts/snapshot_input.py",
                "scripts/store_config.py",
                "scripts/wiki_observations.py",
            ),
            SNAPSHOT_ANALYZER_FILES,
        )
        self.assertEqual(
            tuple(sorted(SNAPSHOT_ANALYZER_FILES, key=str.encode)),
            SNAPSHOT_ANALYZER_FILES,
        )

    def test_marketplace_and_plugin_identity(self):
        market = json.loads(MARKETPLACE.read_text())
        plugin = json.loads(PLUGIN_MANIFEST.read_text())
        self.assertEqual("workflow-observatory", market["name"])
        self.assertEqual("Workflow Observatory", market["interface"]["displayName"])
        self.assertEqual("workflow-observer", plugin["name"])
        self.assertEqual("./skills/", plugin["skills"])
        entry = market["plugins"][0]
        self.assertEqual("./plugins/workflow-observer", entry["source"]["path"])
        self.assertEqual("AVAILABLE", entry["policy"]["installation"])
        self.assertEqual("ON_INSTALL", entry["policy"]["authentication"])

    def test_artifact_policy_source_inventory_paths_are_exact(self):
        expected = {
            "policies/artifact_migration_registry.json",
            "policies/artifact_schema_registry.json",
            "policies/health_event_schema.json",
            "scripts/artifact_migration.py",
            "scripts/artifact_schema.py",
            "tests/fixtures/artifact_migration_vectors.json",
            "tests/test_artifact_migration.py",
            "tests/test_artifact_schema.py",
        }
        actual = {
            path.relative_to(PLUGIN_ROOT).as_posix()
            for path in PLUGIN_ROOT.rglob("*")
            if path.is_file()
            and (
                path.name in {
                    "artifact_migration_registry.json",
                    "artifact_schema_registry.json",
                    "health_event_schema.json",
                    "artifact_migration.py",
                    "artifact_schema.py",
                    "artifact_migration_vectors.json",
                    "test_artifact_migration.py",
                    "test_artifact_schema.py",
                }
            )
        }
        self.assertEqual(expected, actual)
