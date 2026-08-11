import hashlib
from pathlib import Path
import sys
import unittest
from unittest import mock


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PLUGIN_ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from canonical_json import CanonicalizationError, strict_json_loads


BUNDLED_CORE = PLUGIN_ROOT / "scripts/wiki_observations.py"
CORE_SOURCE = PLUGIN_ROOT / "scripts/core_source.json"
EXPECTED_SOURCE = (
    "workflow-observatory/plugins/workflow-observer/"
    "scripts/wiki_observations.py"
)


class CoreParityTests(unittest.TestCase):
    def test_bundled_core_matches_declared_hash(self):
        declared = strict_json_loads(CORE_SOURCE.read_bytes())

        self.assertEqual({"schema_version", "source", "sha256"}, set(declared))
        self.assertEqual(2, declared["schema_version"])
        self.assertEqual(EXPECTED_SOURCE, declared["source"])
        self.assertRegex(declared["sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            declared["sha256"],
            hashlib.sha256(BUNDLED_CORE.read_bytes()).hexdigest(),
        )

    def test_core_manifest_has_exact_generated_three_key_shape(self):
        digest = hashlib.sha256(BUNDLED_CORE.read_bytes()).hexdigest()
        expected = (
            "{\n"
            '  "schema_version": 2,\n'
            f'  "source": "{EXPECTED_SOURCE}",\n'
            f'  "sha256": "{digest}"\n'
            "}\n"
        ).encode("utf-8")

        self.assertEqual(expected, CORE_SOURCE.read_bytes())

    def test_duplicate_key_manifest_fails_before_hash_comparison(self):
        duplicate = (
            b'{"schema_version":2,"source":"' + EXPECTED_SOURCE.encode("ascii")
            + b'","sha256":"' + b"a" * 64
            + b'","sha256":"' + b"b" * 64 + b'"}'
        )
        with mock.patch(
            "hashlib.sha256",
            side_effect=AssertionError("hash comparison must not run"),
        ) as sha256:
            with self.assertRaises(CanonicalizationError):
                strict_json_loads(duplicate)
        sha256.assert_not_called()


if __name__ == "__main__":
    unittest.main()
