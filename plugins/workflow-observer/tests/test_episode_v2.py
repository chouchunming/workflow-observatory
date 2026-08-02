from __future__ import annotations

import hashlib
import json
from pathlib import Path
import stat
import sys
import unittest


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PLUGIN_ROOT / "scripts"
TESTS = PLUGIN_ROOT / "tests"
EXPECTED_FIXTURE_SHA256 = {
    "v1": "5c798fb0e6b95e4f29868126d0d3f3d7dea986f9c46badc8543957a5ee2e8d9a",
    "v2": "7b909fe173fbd8425ea3e136c72f5a0892072c164484d6733903fcdc72a809e1",
}
for module_root in (SCRIPTS, TESTS):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

from canonical_json import canonicalize
from episode_schema import (
    EpisodeSchemaError,
    EpisodeV2Supplement,
    build_episode_v2,
    canonical_episode_projection,
    parse_episode_block,
    parse_v2_supplement,
    render_episode_block,
)
from workflow_evolution_fixtures import (
    DECISION,
    EXPECTED_CANONICAL_EPISODE_KEYS,
    FakeObservationStore,
    PRIVACY_SENTINEL,
    V1_BODY,
    V1_METADATA,
    V2_METADATA,
    V2_SUPPLEMENT,
    load_projection_policy,
    temporary_timezone,
    v1_body_with_privacy_sentinel,
)


class EpisodeV2Tests(unittest.TestCase):
    def setUp(self):
        self.projection = load_projection_policy()

    def _v2_final_body(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        return V1_BODY.rstrip() + "\n\n" + render_episode_block(episode)

    def test_v2_round_trip_is_canonical(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        block = render_episode_block(episode)
        human_body, parsed = parse_episode_block(
            "human\n\n" + block,
            self.projection,
        )
        self.assertEqual("human\n", human_body)
        self.assertEqual(episode, parsed)
        self.assertEqual(canonicalize(episode), canonicalize(parsed))

    def test_build_rejects_supplement_lifecycle_overrides(self):
        valid = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        forged = EpisodeV2Supplement(
            schema_version=2,
            execution=valid.execution,
            quality={
                "test_failures": 0,
                "timeout_count": 0,
                "elapsed_seconds": 999,
            },
            decisions=valid.decisions,
        )
        with self.assertRaises(EpisodeSchemaError):
            build_episode_v2(
                elapsed_seconds=120,
                completion_metrics={
                    "verification": "pass",
                    "review_rounds": 1,
                    "defects_found": 0,
                    "rework_count": 0,
                },
                supplement=forged,
            )

    def test_v1_projection_does_not_fabricate_v2_fields(self):
        self.assertNotIn("schema_version", V1_METADATA)
        projected = canonical_episode_projection(
            V1_METADATA,
            V1_BODY,
            self.projection,
        )
        self.assertEqual(EXPECTED_CANONICAL_EPISODE_KEYS, set(projected))
        self.assertEqual(1, projected["episode_schema_version"])
        self.assertEqual(
            "unsupported_by_schema",
            projected["metrics"]["test_failures"]["availability"],
        )
        self.assertEqual([], projected["decisions"])
        self.assertEqual(
            {"availability": "unavailable", "value": None},
            projected["workflow_generation"],
        )

    def test_v2_draft_schema_comes_from_frontmatter_without_episode_block(self):
        draft_metadata = {
            **V2_METADATA,
            "schema_version": 2,
            "status": "draft",
        }
        draft_body = V1_BODY.split("\n## Execution evidence", 1)[0].rstrip() + "\n"
        try:
            projected = canonical_episode_projection(
                draft_metadata,
                draft_body,
                self.projection,
            )
        except EpisodeSchemaError as error:
            self.fail(f"valid v2 draft was rejected: {error}")
        self.assertEqual(2, projected["episode_schema_version"])
        self.assertIsNone(projected["finished_at"])
        self.assertEqual(
            {
                "availability": "observed",
                "value": "implementation-with-review@2",
            },
            projected["workflow_generation"],
        )
        self.assertEqual([], projected["decisions"])
        self.assertTrue(all(
            metric == {
                "availability": "not_recorded",
                "value": None,
                "unit": None,
            }
            for metric in projected["metrics"].values()
        ))

    def test_final_v2_requires_matching_frontmatter_and_episode_block(self):
        body = self._v2_final_body()
        projected = canonical_episode_projection(
            {**V2_METADATA, "schema_version": 2},
            body,
            self.projection,
        )
        self.assertEqual(2, projected["episode_schema_version"])

        mismatches = (
            (V1_METADATA, body),
            ({**V1_METADATA, "schema_version": 2}, V1_BODY),
        )
        for metadata, mismatched_body in mismatches:
            with self.subTest(metadata=metadata), self.assertRaisesRegex(
                EpisodeSchemaError,
                "schema_version|Episode data",
            ):
                canonical_episode_projection(
                    metadata,
                    mismatched_body,
                    self.projection,
                )

    def test_explicit_schema_version_must_be_exact_integer_two(self):
        body = self._v2_final_body()
        for value in (True, 1, "2", 3, None):
            with self.subTest(value=value), self.assertRaisesRegex(
                EpisodeSchemaError,
                "schema_version",
            ):
                canonical_episode_projection(
                    {**V1_METADATA, "schema_version": value},
                    body,
                    self.projection,
                )

    def test_v2_draft_rejects_episode_block(self):
        with self.assertRaisesRegex(EpisodeSchemaError, "draft"):
            canonical_episode_projection(
                {
                    **V2_METADATA,
                    "schema_version": 2,
                    "status": "draft",
                },
                self._v2_final_body(),
                self.projection,
            )

    def test_v1_projection_rejects_additive_workflow_generation_metadata(self):
        metadata = {
            **V1_METADATA,
            "workflow_generation": "implementation-with-review@2",
        }
        with self.assertRaisesRegex(EpisodeSchemaError, "workflow_generation"):
            canonical_episode_projection(metadata, V1_BODY, self.projection)

    def test_projection_has_exact_privacy_safe_keys(self):
        metadata = {
            **V1_METADATA,
            "title": PRIVACY_SENTINEL,
            "task_ref": "[[private-task]]",
            "sources": ["raw/private-source.md"],
        }
        projected = canonical_episode_projection(
            metadata,
            v1_body_with_privacy_sentinel(),
            self.projection,
        )
        self.assertEqual(EXPECTED_CANONICAL_EPISODE_KEYS, set(projected))
        encoded = canonicalize(projected)
        self.assertNotIn(PRIVACY_SENTINEL.encode("utf-8"), encoded)
        self.assertNotIn(b"private-task", encoded)
        self.assertNotIn(b"private-source", encoded)

    def test_duplicate_key_rejected_in_episode_supplement(self):
        payload = V2_SUPPLEMENT.replace(
            '"schema_version": 2,',
            '"schema_version": 2, "schema_version": 2,',
            1,
        )
        with self.assertRaisesRegex(EpisodeSchemaError, "duplicate JSON key"):
            parse_v2_supplement(payload, self.projection)

    def test_duplicate_key_rejected_in_episode_block(self):
        body = (
            "human\n\n## Episode data\n\n```json\n"
            '{"schema_version":2,"schema_version":2}'
            "\n```\n"
        )
        with self.assertRaisesRegex(EpisodeSchemaError, "duplicate JSON key"):
            parse_episode_block(body, self.projection)

    def test_decisions_are_bounded_and_reject_sensitive_shapes(self):
        payload = json.loads(V2_SUPPLEMENT)
        payload["decisions"] = payload["decisions"] * 13
        with self.assertRaises(EpisodeSchemaError):
            parse_v2_supplement(json.dumps(payload), self.projection)
        for prohibited in (
            "/Users/alice/repo",
            "api_key=secret",
            "https://host/path",
        ):
            payload["decisions"] = [{**DECISION, "summary": prohibited}]
            with self.subTest(prohibited=prohibited), self.assertRaises(
                EpisodeSchemaError
            ):
                parse_v2_supplement(json.dumps(payload), self.projection)

    def test_supplement_contract_is_closed_and_measurements_are_exact(self):
        valid = json.loads(V2_SUPPLEMENT)
        invalid_payloads = []
        for parent in ((), ("execution",), ("quality",)):
            payload = json.loads(V2_SUPPLEMENT)
            target = payload
            for key in parent:
                target = target[key]
            target["extra"] = None
            invalid_payloads.append(payload)
        for amount in ("01", "1.0", "1e2", "1.", "-1"):
            payload = json.loads(V2_SUPPLEMENT)
            payload["execution"]["cost_amount"] = amount
            invalid_payloads.append(payload)
        payload = json.loads(V2_SUPPLEMENT)
        payload["execution"]["cost_currency"] = None
        invalid_payloads.append(payload)
        payload = json.loads(V2_SUPPLEMENT)
        payload["execution"].update(
            input_tokens=None,
            output_tokens=None,
            cache_read_tokens=None,
            cost_amount=None,
            cost_currency=None,
            measurement_source="agent-reported",
        )
        invalid_payloads.append(payload)
        payload = json.loads(V2_SUPPLEMENT)
        payload["execution"]["input_tokens"] = 2**53
        invalid_payloads.append(payload)
        payload = json.loads(V2_SUPPLEMENT)
        payload["quality"]["test_failures"] = True
        invalid_payloads.append(payload)
        payload = json.loads(V2_SUPPLEMENT)
        payload["decisions"][0]["phase"] = "secret-phase"
        invalid_payloads.append(payload)

        self.assertEqual(2, parse_v2_supplement(
            json.dumps(valid), self.projection
        ).schema_version)
        for payload in invalid_payloads:
            with self.subTest(payload=payload), self.assertRaises(
                EpisodeSchemaError
            ):
                parse_v2_supplement(json.dumps(payload), self.projection)

    def test_episode_block_must_be_single_final_and_canonical(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        block = render_episode_block(episode)
        noncanonical = block.replace(
            canonicalize(episode).decode("utf-8"),
            json.dumps(episode, ensure_ascii=False),
        )
        invalid_bodies = (
            "human\n\n" + block + "trailing\n",
            "human\n\n" + block + "\n" + block,
            block + "\nhuman\n",
            "human\n\n" + noncanonical,
        )
        for body in invalid_bodies:
            with self.subTest(body=body), self.assertRaises(EpisodeSchemaError):
                parse_episode_block(body, self.projection)

    def test_malformed_episode_heading_is_not_downgraded_to_v1(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        crlf_body = ("human\n\n" + render_episode_block(episode)).replace(
            "\n", "\r\n"
        )
        with self.assertRaisesRegex(EpisodeSchemaError, "exactly formatted"):
            parse_episode_block(crlf_body, self.projection)

    def test_v2_projection_has_complete_metric_availability_and_decisions(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        body = V1_BODY.rstrip() + "\n\n" + render_episode_block(episode)
        projected = canonical_episode_projection(
            V2_METADATA,
            body,
            self.projection,
        )
        metric_names = set(self.projection["metric_semantics"]["metrics"])
        self.assertEqual(EXPECTED_CANONICAL_EPISODE_KEYS, set(projected))
        self.assertEqual(metric_names, set(projected["metrics"]))
        self.assertTrue(all(
            set(metric) == {"availability", "value", "unit"}
            for metric in projected["metrics"].values()
        ))
        self.assertEqual(
            {"availability": "observed", "value": "1.25", "unit": "USD"},
            projected["metrics"]["cost_amount"],
        )
        self.assertEqual(
            {"availability": "not_recorded", "value": None, "unit": None},
            projected["metrics"]["cache_read_tokens"],
        )
        self.assertEqual(DECISION, projected["decisions"][0])
        self.assertEqual(
            {"availability": "observed", "value": "implementation-with-review@2"},
            projected["workflow_generation"],
        )
        self.assertIsNone(projected["runtime_provenance"])
        self.assertNotIn("measurement_source", canonicalize(projected).decode())

    def test_v2_missing_generation_is_unavailable_and_never_inferred(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        body = V1_BODY.rstrip() + "\n\n" + render_episode_block(episode)
        metadata = {
            **V1_METADATA,
            "schema_version": 2,
            "workflow_variant": "maintenance-basic",
            "revision": "fedcba9876543210",
            "timestamp": "2026-08-02T00:00:00Z",
        }
        projected = canonical_episode_projection(metadata, body, self.projection)
        self.assertEqual(
            {"availability": "unavailable", "value": None},
            projected["workflow_generation"],
        )

    def test_v2_generation_uses_only_valid_explicit_metadata(self):
        supplement = parse_v2_supplement(V2_SUPPLEMENT, self.projection)
        episode = build_episode_v2(
            elapsed_seconds=120,
            completion_metrics={
                "verification": "pass",
                "review_rounds": 1,
                "defects_found": 0,
                "rework_count": 0,
            },
            supplement=supplement,
        )
        body = V1_BODY.rstrip() + "\n\n" + render_episode_block(episode)
        explicit = "reviewed.gen+candidate@2"
        projected = canonical_episode_projection(
            {
                **V1_METADATA,
                "schema_version": 2,
                "workflow_generation": explicit,
            },
            body,
            self.projection,
        )
        self.assertEqual(
            {"availability": "observed", "value": explicit},
            projected["workflow_generation"],
        )

        invalid = (
            None,
            2,
            "",
            "unknown",
            "unavailable",
            "Uppercase@2",
            "-leading-hyphen",
            "contains space",
            "contains/slash",
            "x" * 201,
        )
        for value in invalid:
            with self.subTest(value=value), self.assertRaisesRegex(
                EpisodeSchemaError,
                "workflow_generation",
            ):
                canonical_episode_projection(
                    {
                        **V1_METADATA,
                        "schema_version": 2,
                        "workflow_generation": value,
                    },
                    body,
                    self.projection,
                )

    def test_projection_uses_utc_second_instants_independent_of_process_timezone(self):
        with temporary_timezone("Pacific/Honolulu"):
            projected = canonical_episode_projection(
                V1_METADATA,
                V1_BODY,
                self.projection,
            )
        self.assertIn("started_at", projected)
        self.assertEqual("2026-08-02T00:00:00Z", projected["started_at"])
        self.assertEqual("2026-08-02T00:02:00Z", projected["finished_at"])

    def test_v1_projection_rejects_lifecycle_integers_outside_safe_range(self):
        body = V1_BODY.replace(
            "review_rounds: 1",
            "review_rounds: 999999999999999999",
        )
        with self.assertRaisesRegex(EpisodeSchemaError, "safe non-negative"):
            canonical_episode_projection(V1_METADATA, body, self.projection)

    def test_fake_stores_are_private_temporary_and_hash_exact_raw_bytes(self):
        for adapter in ("portable", "llmwiki"):
            with self.subTest(adapter=adapter), FakeObservationStore(adapter) as store:
                store.store_root.resolve().relative_to(store.base)
                self.assertTrue(
                    hasattr(store, "expected_raw_bytes"),
                    "fixture must expose expected bytes assembled before writing",
                )
                self.assertEqual(
                    EXPECTED_FIXTURE_SHA256,
                    store.expected_raw_sha256,
                )
                expected_task_suffix = (
                    "wiki/tasks"
                    if adapter == "portable"
                    else "wiki/tasks/records"
                )
                self.assertTrue(store.tasks.as_posix().endswith(expected_task_suffix))
                for path, key in ((store.v1_path, "v1"), (store.v2_path, "v2")):
                    raw = path.read_bytes()
                    self.assertEqual(
                        stat.S_IMODE(path.stat().st_mode),
                        0o600,
                    )
                    self.assertEqual(store.expected_raw_bytes[key], raw)
                    self.assertEqual(
                        EXPECTED_FIXTURE_SHA256[key],
                        hashlib.sha256(raw).hexdigest(),
                    )
                self.assertEqual(0o600, stat.S_IMODE(store.task_path.stat().st_mode))


if __name__ == "__main__":
    unittest.main()
