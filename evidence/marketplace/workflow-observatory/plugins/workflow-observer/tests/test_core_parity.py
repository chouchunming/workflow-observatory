import hashlib
import json
import os
from pathlib import Path
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
BUNDLED_CORE = PLUGIN_ROOT / "scripts/wiki_observations.py"
CORE_SOURCE = PLUGIN_ROOT / "scripts/core_source.json"


class CoreParityTests(unittest.TestCase):
    def test_bundled_core_matches_declared_hash(self):
        declared = json.loads(CORE_SOURCE.read_text(encoding="utf-8"))

        self.assertEqual(1, declared["schema_version"])
        self.assertEqual("llmwiki/wiki_observations.py", declared["source"])
        self.assertEqual(
            declared["sha256"],
            hashlib.sha256(BUNDLED_CORE.read_bytes()).hexdigest(),
        )

    def test_repository_copy_matches_when_source_is_configured(self):
        source_root = os.environ.get("LLMWIKI_SOURCE_ROOT")
        if source_root is None:
            self.skipTest("development source root not configured")

        self.assertEqual(
            (Path(source_root) / "wiki_observations.py").read_bytes(),
            BUNDLED_CORE.read_bytes(),
        )


if __name__ == "__main__":
    unittest.main()
