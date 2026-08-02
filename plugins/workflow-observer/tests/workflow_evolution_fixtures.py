from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time

from canonical_json import canonicalize
from policy_artifacts import load_policy_set


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
POLICY_ROOT = PLUGIN_ROOT / "policies"
PRIVACY_SENTINEL = "PRIVATE-EVIDENCE-6f92d978b81f"
_EXPECTED_RAW_SHA256 = {
    "v1": "5c798fb0e6b95e4f29868126d0d3f3d7dea986f9c46badc8543957a5ee2e8d9a",
    "v2": "7ac2d72af20edee8bf0303b4612551132ac8bfd316178292d3eb4d0819dadf08",
}
EXPECTED_CANONICAL_EPISODE_KEYS = {
    "run_id",
    "episode_schema_version",
    "started_at",
    "finished_at",
    "project",
    "workspace",
    "workspace_id",
    "revision",
    "working_tree",
    "agent_surface",
    "task_type",
    "workflow_variant",
    "workflow_generation",
    "status",
    "metrics",
    "runtime_provenance",
    "decisions",
}
DECISION = {
    "sequence": 1,
    "phase": "implementation",
    "actor_role": "implementer",
    "decision_type": "reject",
    "reason_code": "integrity-risk",
    "result": "supported",
    "summary": "Rejected an unsafe mutation at the evidence boundary",
}
V2_SUPPLEMENT = json.dumps(
    {
        "schema_version": 2,
        "execution": {
            "input_tokens": 1200,
            "output_tokens": 300,
            "cache_read_tokens": None,
            "cost_amount": "1.25",
            "cost_currency": "USD",
            "measurement_source": "tool-derived",
        },
        "quality": {
            "test_failures": 0,
            "timeout_count": 0,
        },
        "decisions": [DECISION],
    },
    ensure_ascii=False,
    indent=2,
) + "\n"
V1_METADATA = {
    "type": "observation",
    "title": "Workflow evolution fixture",
    "tags": ["observation", "workflow"],
    "run_id": "obs-20260802-000000-abcdef",
    "timestamp": "2026-08-02T08:00:00+08:00",
    "project": "workflow-observatory",
    "workspace": "workflow-observatory",
    "workspace_id": "0123456789ab",
    "revision": "0123456789abcdef",
    "working_tree": "clean",
    "agent_surface": "codex",
    "task_type": "maintenance",
    "workflow_variant": "implementation-with-review",
    "status": "success",
    "start_mode": "planned",
    "sources": [],
}
V2_METADATA = {
    **V1_METADATA,
    "workflow_generation": "implementation-with-review@2",
}
V1_BODY = """## Scope

- Goal: Exercise canonical Episode projection
- Included: Version one fixture
- Excluded: Private evidence

## Execution evidence

- Verification: focused tests passed
- Artifacts: bounded fixture

## Outcome and observation

- Outcome: Projection fixture completed
- Observation: Version one remains readable

## Follow-up

- None — no further action

## Metrics

```yaml
finished_at: "2026-08-02T08:02:00+08:00"
elapsed_seconds: 120
verification: pass
review_rounds: 1
defects_found: 0
rework_count: 0
rework_reason: none
```
"""


def v1_body_with_privacy_sentinel() -> str:
    return f"""## Scope

- Goal: Exercise {PRIVACY_SENTINEL} exclusion
- Included: Version one {PRIVACY_SENTINEL} fixture
- Excluded: {PRIVACY_SENTINEL}

## Execution evidence

- Verification: focused tests passed
- Artifacts: bounded fixture

## Outcome and observation

- Outcome: {PRIVACY_SENTINEL}
- Observation: {PRIVACY_SENTINEL}

## Follow-up

- Review {PRIVACY_SENTINEL}

## Metrics

```yaml
finished_at: "2026-08-02T08:02:00+08:00"
elapsed_seconds: 120
verification: pass
review_rounds: 1
defects_found: 0
rework_count: 1
rework_reason: {PRIVACY_SENTINEL}
```
"""


def load_projection_policy() -> dict[str, dict[str, object]]:
    return load_policy_set(
        POLICY_ROOT,
        analyzer_files=(),
        canonicalizer_files=("scripts/canonical_json.py",),
    ).documents


@contextmanager
def temporary_timezone(name: str):
    previous = os.environ.get("TZ")
    os.environ["TZ"] = name
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        if hasattr(time, "tzset"):
            time.tzset()


def _render_frontmatter(metadata: dict[str, object]) -> str:
    lines = ["---"]
    for key, value in metadata.items():
        if isinstance(value, (list, str)):
            encoded = json.dumps(value, ensure_ascii=False)
        else:
            encoded = str(value).lower() if value is not None else "null"
        lines.append(f"{key}: {encoded}")
    return "\n".join(lines) + "\n---\n"


def _v2_body() -> str:
    supplement = json.loads(V2_SUPPLEMENT)
    episode = {
        "schema_version": 2,
        "execution": supplement["execution"],
        "quality": {
            "elapsed_seconds": 120,
            "verification": "pass",
            "review_rounds": 1,
            "defects_found": 0,
            "rework_count": 0,
            **supplement["quality"],
        },
        "decisions": supplement["decisions"],
    }
    block = (
        "## Episode data\n\n```json\n"
        + canonicalize(episode).decode("utf-8")
        + "\n```\n"
    )
    return V1_BODY.rstrip() + "\n\n" + block


class FakeObservationStore:
    """Private temporary portable or LLMWiki observation fixture layout."""

    def __init__(self, adapter: str):
        if adapter not in {"portable", "llmwiki"}:
            raise ValueError("adapter must be portable or llmwiki")
        self.adapter = adapter
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name).resolve()
        if adapter == "portable":
            self.store_root = self.base / "portable-home" / "store"
            self.tasks = self.store_root / "wiki" / "tasks"
        else:
            self.store_root = self.base / "llmwiki-root"
            self.tasks = self.store_root / "wiki" / "tasks" / "records"
        self.observations = self.store_root / "wiki" / "observations"
        self.invalidations = self.observations / "invalidations"
        self.locks = self.observations / ".locks"
        for directory in (
            self.tasks,
            self.invalidations,
            self.locks,
        ):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)

        self.task_path = self.tasks / "fixture-task.md"
        self._write_private(self.task_path, b"# Fixture task\n")
        self.v1_path = self.observations / "obs-20260802-000000-abcdef.md"
        v2_metadata = {
            **V2_METADATA,
            "run_id": "obs-20260802-000001-fedcba",
        }
        self.v2_path = self.observations / "obs-20260802-000001-fedcba.md"
        self.expected_raw_bytes = {
            "v1": (_render_frontmatter(V1_METADATA) + V1_BODY).encode("utf-8"),
            "v2": (_render_frontmatter(v2_metadata) + _v2_body()).encode("utf-8"),
        }
        self.expected_raw_sha256 = dict(_EXPECTED_RAW_SHA256)
        for key, expected in self.expected_raw_bytes.items():
            if hashlib.sha256(expected).hexdigest() != self.expected_raw_sha256[key]:
                raise AssertionError(f"reviewed {key} fixture digest is stale")
        self._write_private(self.v1_path, self.expected_raw_bytes["v1"])
        self._write_private(self.v2_path, self.expected_raw_bytes["v2"])
        self.v1_raw_sha256 = self.expected_raw_sha256["v1"]
        self.v2_raw_sha256 = self.expected_raw_sha256["v2"]

    def _write_private(self, path: Path, content: bytes) -> None:
        path.resolve(strict=False).relative_to(self.base)
        path.write_bytes(content)
        path.chmod(0o600)

    def close(self) -> None:
        self.temporary.cleanup()

    def __enter__(self) -> "FakeObservationStore":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()
