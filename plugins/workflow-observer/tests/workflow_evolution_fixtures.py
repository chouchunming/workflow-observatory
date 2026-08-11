from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import base64
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import time

from canonical_json import canonicalize
from policy_artifacts import load_policy_set


PLUGIN_ROOT = Path(__file__).resolve().parents[1]
POLICY_ROOT = PLUGIN_ROOT / "policies"
PRIVACY_SENTINEL = "PRIVATE-EVIDENCE-6f92d978b81f"
ARTIFACT_MIGRATION_FIXTURE = (
    PLUGIN_ROOT / "tests/fixtures/artifact_migration_vectors.json"
)
APPROVED_PHASE1_ARCHIVE_INVENTORY = frozenset({
    "docs/superpowers/plans/"
    "2026-08-11-workflow-observatory-v0.3-phase-1-schema-migration.md",
    "docs/superpowers/specs/"
    "2026-08-11-workflow-observatory-concurrency-operability-v0.3-design.md",
    *{
        "plugins/workflow-observer/policies/" + name
        for name in (
            "artifact_migration_registry.json",
            "artifact_schema_registry.json",
            "candidate_emission_policy.json",
            "decision_support_policy.json",
            "episode_projection.json",
            "health_event_schema.json",
            "lifecycle_health_policy.json",
            "metric_semantics.json",
            "producer_capabilities.json",
            "quantile_policy.json",
            "workflow_generation_mapping.json",
        )
    },
    *{
        "plugins/workflow-observer/scripts/" + name
        for name in (
            "artifact_migration.py",
            "artifact_schema.py",
            "canonical_json.py",
            "episode_schema.py",
            "learning_snapshot.py",
            "policy_artifacts.py",
            "snapshot_input.py",
            "snapshot_store.py",
            "store_config.py",
            "wiki_observations.py",
            "workflow_observer_cli.py",
        )
    },
    "plugins/workflow-observer/tests/fixtures/artifact_migration_vectors.json",
    "plugins/workflow-observer/tests/test_schema_migration_acceptance.py",
})


def select_phase1_archive_inventory(paths) -> frozenset[str]:
    return frozenset(
        path
        for path in paths
        if (
            path.startswith("plugins/workflow-observer/policies/")
            or (
                path.startswith("plugins/workflow-observer/scripts/")
                and path.endswith(".py")
            )
            or path in {
                "docs/superpowers/plans/"
                "2026-08-11-workflow-observatory-v0.3-phase-1-schema-migration.md",
                "docs/superpowers/specs/"
                "2026-08-11-workflow-observatory-concurrency-operability-v0.3-design.md",
                "plugins/workflow-observer/tests/fixtures/"
                "artifact_migration_vectors.json",
                "plugins/workflow-observer/tests/"
                "test_schema_migration_acceptance.py",
            }
        )
    )


_EXPECTED_RAW_SHA256 = {
    "v1": "5c798fb0e6b95e4f29868126d0d3f3d7dea986f9c46badc8543957a5ee2e8d9a",
    "v2": "62e0951e3ba1a08730d200c31a547d3fffaf42cd8a0090f955642eb9899c6b10",
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
    "schema_version": 2,
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
        "execution": {
            **supplement["execution"],
            "elapsed_seconds": 120,
        },
        "quality": {
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

    def __init__(self, adapter: str, *, base: Path | None = None):
        if adapter not in {"portable", "llmwiki"}:
            raise ValueError("adapter must be portable or llmwiki")
        self.adapter = adapter
        self.temporary = None
        if base is None:
            self.temporary = tempfile.TemporaryDirectory()
            self.base = Path(self.temporary.name).resolve(strict=True)
        else:
            self.base = Path(base).resolve(strict=False)
            self.base.mkdir(mode=0o700)
            self.base = self.base.resolve(strict=True)
        if stat.S_IMODE(self.base.stat().st_mode) != 0o700:
            raise AssertionError("fake observation store root must be mode-0700")
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
            self._mkdir_private(directory)

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

    def _mkdir_private(self, path: Path) -> None:
        path.resolve(strict=False).relative_to(self.base)
        relative = path.relative_to(self.base)
        cursor = self.base
        for part in relative.parts:
            cursor = cursor / part
            cursor.mkdir(exist_ok=True, mode=0o700)
            if stat.S_IMODE(cursor.stat().st_mode) != 0o700:
                raise AssertionError(f"fixture directory is not mode-0700: {cursor}")

    def _write_private(self, path: Path, content: bytes) -> None:
        path.resolve(strict=False).relative_to(self.base)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise AssertionError(f"fixture file is not mode-0600: {path}")

    def close(self) -> None:
        if self.temporary is not None:
            self.temporary.cleanup()

    def __enter__(self) -> "FakeObservationStore":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()


@dataclass(frozen=True)
class ArtifactMigrationVector:
    name: str
    source_bytes: bytes
    derived_bytes: bytes
    source_sha256: str
    derived_sha256: str


def load_artifact_migration_vectors() -> dict[str, ArtifactMigrationVector]:
    fixture_bytes = ARTIFACT_MIGRATION_FIXTURE.read_bytes()
    fixture = json.loads(fixture_bytes)
    vectors = {}
    for row in fixture["vectors"]:
        encoded = row["source_bytes_base64"]
        insertion = row.get("source_bytes_base64_tail_insertion", "")
        if insertion:
            encoded = encoded[:-4] + insertion + encoded[-4:]
        source = base64.b64decode(encoded, validate=True)
        derived = (
            bytes.fromhex(row["canonical_derived_utf8_hex"])
            if "canonical_derived_utf8_hex" in row
            else base64.b64decode(
                row["canonical_derived_bytes_base64"], validate=True
            )
        )
        if hashlib.sha256(source).hexdigest() != row["source_sha256"]:
            raise AssertionError(f"reviewed {row['name']} source digest is stale")
        if hashlib.sha256(derived).hexdigest() != row["derived_sha256"]:
            raise AssertionError(f"reviewed {row['name']} result digest is stale")
        vectors[row["name"]] = ArtifactMigrationVector(
            name=row["name"],
            source_bytes=source,
            derived_bytes=derived,
            source_sha256=row["source_sha256"],
            derived_sha256=row["derived_sha256"],
        )
    return vectors


class AcceptanceFixtureMatrix:
    """One private, isolated fake Portable/LLMWiki pair per frozen case."""

    _ADAPTERS = ("portable", "llmwiki")
    _LIVE_ROOT_ENVIRONMENT = (
        "WORKFLOW_OBSERVATORY_HOME",
        "OBSERVATION_WIKI_ROOT",
        "LLMWIKI_ROOT",
    )

    def __init__(self, root: Path, case_ids: tuple[str, ...]):
        self.root = Path(root).resolve(strict=True)
        if stat.S_IMODE(self.root.stat().st_mode) != 0o700:
            raise AssertionError("acceptance root must be mode-0700")
        self._case_ids = tuple(case_ids)
        self._case_roots = {}
        self._stores = {}
        for case_id in self._case_ids:
            case_root = self.root / case_id
            case_root.mkdir(mode=0o700)
            if stat.S_IMODE(case_root.stat().st_mode) != 0o700:
                raise AssertionError("acceptance case root must be mode-0700")
            self._case_roots[case_id] = case_root
            self._stores[case_id] = {
                adapter: FakeObservationStore(
                    adapter,
                    base=case_root / adapter,
                )
                for adapter in self._ADAPTERS
            }

    def case_root(self, case_id: str) -> Path:
        return self._case_roots[case_id]

    def stores(self, case_id: str) -> dict[str, FakeObservationStore]:
        return dict(self._stores[case_id])

    def explicit_observation_sources(
        self,
        case_id: str,
        adapter: str,
        *,
        schema_version: int,
    ) -> tuple[Path, ...]:
        store = self._stores[case_id][adapter]
        marker = f"schema_version: {schema_version}\n".encode("ascii")
        matches = []
        for path in sorted(store.observations.glob("*.md")):
            source = path.read_bytes()
            _opening, separator, remainder = source.partition(b"---\n")
            frontmatter, closing, _body = remainder.partition(b"---\n")
            if not separator or not closing:
                raise AssertionError(f"fixture observation lacks frontmatter: {path}")
            if marker in frontmatter:
                matches.append(path)
        return tuple(matches)

    def inject_privacy_sentinel_observations(self, case_id: str) -> None:
        metadata = {
            **V1_METADATA,
            "title": f"Workflow evolution fixture {PRIVACY_SENTINEL}",
        }
        body = V1_BODY.replace(
            "canonical Episode projection",
            f"canonical Episode projection {PRIVACY_SENTINEL}",
            1,
        )
        source = (_render_frontmatter(metadata) + body).encode("utf-8")
        if source.count(PRIVACY_SENTINEL.encode("utf-8")) < 2:
            raise AssertionError("privacy sentinel must occur in human title and body")
        for store in self._stores[case_id].values():
            store._write_private(store.v1_path, source)

    def assert_private_staging_boundary(
        self,
        staging_parent: Path,
        staged_source: Path,
    ) -> None:
        boundary = Path(staging_parent).resolve(strict=True)
        boundary.relative_to(self.root)
        details = boundary.lstat()
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise AssertionError("staging privacy boundary must be a real directory")
        if stat.S_IMODE(details.st_mode) != 0o700:
            raise AssertionError("staging privacy boundary must be mode-0700")

        source = Path(staged_source).resolve(strict=True)
        source.relative_to(boundary)
        directory_modes = set()
        file_modes = set()
        for path in (source, *source.rglob("*")):
            resolved = path.resolve(strict=True)
            resolved.relative_to(boundary)
            mode = path.lstat().st_mode
            if stat.S_ISLNK(mode):
                raise AssertionError("repo-copy staging must not contain symlinks")
            if stat.S_ISDIR(mode):
                directory_modes.add(stat.S_IMODE(mode))
            elif stat.S_ISREG(mode):
                file_modes.add(stat.S_IMODE(mode))
            else:
                raise AssertionError("repo-copy staging contains a special file")
        if directory_modes != {0o755}:
            raise AssertionError(
                f"repo-copy staging directory modes changed: {directory_modes}"
            )
        if file_modes != {0o644}:
            raise AssertionError(
                f"repo-copy staging file modes changed: {file_modes}"
            )

    def assert_isolated(self) -> None:
        live_roots = {
            (Path.home() / ".codex/workflow-observatory").resolve(strict=False)
        }
        for name in self._LIVE_ROOT_ENVIRONMENT:
            value = os.environ.get(name)
            if value:
                live_roots.add(Path(value).expanduser().resolve(strict=False))
        for case_id in self._case_ids:
            case_root = self.case_root(case_id).resolve(strict=True)
            case_root.relative_to(self.root)
            for store in self._stores[case_id].values():
                for path in (store.base, store.store_root):
                    resolved = path.resolve(strict=True)
                    resolved.relative_to(self.root)
                    for live_root in live_roots:
                        if resolved == live_root or live_root in resolved.parents:
                            raise AssertionError(
                                "acceptance fixture descends from a configured live root"
                            )

    def assert_fixture_provenance(self) -> None:
        fixture_bytes = [ARTIFACT_MIGRATION_FIXTURE.read_bytes()]
        for stores in self._stores.values():
            fixture_bytes.extend(
                content
                for store in stores.values()
                for content in store.expected_raw_bytes.values()
            )
        combined = b"\n".join(fixture_bytes).lower()
        forbidden = (
            b"/users/",
            b"c:\\users\\",
            b"password",
            b"api_key",
            b"api-key",
            b"credential",
            b"transcript",
            b"prompt:",
        )
        present = [marker.decode("ascii") for marker in forbidden if marker in combined]
        if present:
            raise AssertionError(
                "acceptance fixture contains forbidden provenance: "
                + ", ".join(present)
            )

    def assert_private_modes(self) -> None:
        for path in self.root.rglob("*"):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise AssertionError("acceptance fixture contains a symlink")
            expected = 0o700 if stat.S_ISDIR(details.st_mode) else 0o600
            if stat.S_IMODE(details.st_mode) != expected:
                raise AssertionError(
                    "acceptance fixture mode is not private: "
                    f"{path.relative_to(self.root)}="
                    f"{stat.S_IMODE(details.st_mode):04o}"
                )

    def assert_no_publication_residue(self) -> None:
        residue = sorted(
            path.relative_to(self.root).as_posix()
            for pattern in (".snapshot-*.tmp", "*.ready", "publish.release")
            for path in self.root.rglob(pattern)
        )
        if residue:
            raise AssertionError(
                "acceptance publication residue remains: " + ", ".join(residue)
            )
