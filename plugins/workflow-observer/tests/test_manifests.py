import json
from pathlib import Path
import unittest

MARKET_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE = MARKET_ROOT / ".agents/plugins/marketplace.json"
PLUGIN_MANIFEST = PLUGIN_ROOT / ".codex-plugin/plugin.json"


class ManifestTests(unittest.TestCase):
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
