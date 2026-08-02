import json
import sys
from pathlib import Path
import unittest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
VECTOR = Path(__file__).resolve().parent / "fixtures/jcs_conformance_vectors.json"
sys.path.insert(0, str(SCRIPTS))

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)

_NONCHARACTER_CODE_POINTS = tuple(range(0xFDD0, 0xFDF0)) + tuple(
    (plane << 16) | suffix
    for plane in range(17)
    for suffix in (0xFFFE, 0xFFFF)
)


class CanonicalJsonTests(unittest.TestCase):
    def test_shared_unicode_and_escape_vector(self):
        vector = json.loads(VECTOR.read_text(encoding="utf-8"))
        value = {
            "😀": "emoji",
            "é": "原樣",
            "control": "\b\t\n\f\r\u000f",
            "quote": "\"\\",
            "nested": [{"z": None, "a": True}],
        }
        encoded = canonicalize(value)
        self.assertEqual(bytes.fromhex(vector["canonical_utf8_hex"]), encoded)
        self.assertEqual(
            vector["domain_hash"],
            hash_canonical(bytes.fromhex(vector["domain_utf8_hex"]), value),
        )

    def test_rejects_non_i_json_or_forbidden_numbers(self):
        for value in (
            1.5,
            {"x": float("nan")},
            {"x": 2**53},
            {"x": "\ud800"},
            {1: "non-string key"},
        ):
            with self.subTest(value=repr(value)):
                with self.assertRaises(CanonicalizationError):
                    canonicalize(value)

    def test_hash_is_domain_separated(self):
        value = {"a": 1}
        self.assertEqual(64, len(hash_canonical(b"a\0", value)))
        self.assertNotEqual(
            hash_canonical(b"a\0", value),
            hash_canonical(b"b\0", value),
        )

    def test_strict_json_rejects_ambiguous_or_non_i_json_input(self):
        invalid = (
            '{"x":1,"x":2}',
            '{"x":NaN}',
            '{"x":Infinity}',
            '{"x":-Infinity}',
            '{"x":"\\ud800"}',
            b'{"x":"\xff"}',
        )
        for payload in invalid:
            with self.subTest(payload=repr(payload)):
                with self.assertRaises(CanonicalizationError):
                    strict_json_loads(payload)

    def test_rejects_all_unicode_noncharacters_in_values_and_keys(self):
        self.assertEqual(66, len(_NONCHARACTER_CODE_POINTS))
        for code_point in _NONCHARACTER_CODE_POINTS:
            character = chr(code_point)
            with self.assertRaises(CanonicalizationError):
                canonicalize({"x": character})
            with self.assertRaises(CanonicalizationError):
                canonicalize({character: "x"})

    def test_strict_json_rejects_escaped_noncharacter(self):
        with self.assertRaises(CanonicalizationError):
            strict_json_loads('{"x":"\\ufdd0"}')

    def test_strict_json_rejects_literal_noncharacter(self):
        payload = '{"x":"' + chr(0xFDD0) + '"}'
        with self.assertRaises(CanonicalizationError):
            strict_json_loads(payload)

    def test_rejects_top_level_noncharacter(self):
        with self.assertRaises(CanonicalizationError):
            canonicalize(chr(0x10FFFF))
