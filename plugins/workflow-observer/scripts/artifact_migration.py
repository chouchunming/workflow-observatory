from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import re
from types import MappingProxyType

from artifact_schema import (
    ArtifactMigrationRef,
    ArtifactPolicySet,
    ArtifactSchemaError,
    ArtifactSchemaRef,
    MarkdownEnvelope,
    classify_json_artifact,
    parse_markdown_envelope,
    resolve_artifact_migration,
)
from canonical_json import CanonicalizationError, canonicalize, strict_json_loads
from episode_schema import canonical_episode_projection
from learning_snapshot import (
    migrate_learning_snapshot_v1_core,
    validate_learning_snapshot_artifact,
    validate_learning_snapshot_core,
)
from policy_artifacts import PolicySet


_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}")
_SOURCE_KEYS = {"artifact_type", "schema_version", "source_sha256"}
_LEARNING_SOURCE_KEYS = _SOURCE_KEYS | {"snapshot_id"}
_MIGRATION_KEYS = {"migration_identity", "migration_registry_sha256"}
_TARGET_KEYS = {"contract", "schema_version", "value"}
_DERIVED_KEYS = {
    "artifact_type",
    "schema_version",
    "source",
    "migration",
    "target",
}


class ArtifactMigrationError(ArtifactSchemaError):
    """A source artifact cannot be migrated under the bound policies."""


def _plain_json_copy(value: object) -> object:
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ArtifactMigrationError("derived artifact keys must be strings")
        return {key: _plain_json_copy(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_json_copy(item) for item in value]
    return value


def _deep_freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_thaw(item) for item in value]
    return value


def _exact_mapping(value: object, keys: set[str], label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ArtifactMigrationError(f"{label} does not have its exact keys")
    return value


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ArtifactMigrationError(f"{label} must be a positive integer")
    return value


def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ArtifactMigrationError(f"{label} must be a non-empty string")
    return value


def _validate_derived_document(value: object) -> dict[str, object]:
    document = _exact_mapping(value, _DERIVED_KEYS, "derived artifact")
    if document["artifact_type"] != "derived-artifact":
        raise ArtifactMigrationError("derived artifact type is invalid")
    if document["schema_version"] != 1:
        raise ArtifactMigrationError("derived artifact schema_version is invalid")

    raw_source = document["source"]
    source_keys = (
        _LEARNING_SOURCE_KEYS
        if isinstance(raw_source, Mapping)
        and raw_source.get("artifact_type") == "learning-snapshot"
        else _SOURCE_KEYS
    )
    source = _exact_mapping(raw_source, source_keys, "derived source")
    _nonempty_string(source["artifact_type"], "derived source artifact_type")
    _positive_integer(source["schema_version"], "derived source schema_version")
    source_sha256 = source["source_sha256"]
    if not isinstance(source_sha256, str) or _LOWER_SHA256.fullmatch(source_sha256) is None:
        raise ArtifactMigrationError("derived source_sha256 is invalid")
    if source["artifact_type"] == "learning-snapshot":
        snapshot_id = source["snapshot_id"]
        if (
            not isinstance(snapshot_id, str)
            or _LOWER_SHA256.fullmatch(snapshot_id) is None
        ):
            raise ArtifactMigrationError("derived source snapshot_id is invalid")

    migration = _exact_mapping(
        document["migration"], _MIGRATION_KEYS, "derived migration"
    )
    _nonempty_string(
        migration["migration_identity"], "derived migration identity"
    )
    registry_sha256 = migration["migration_registry_sha256"]
    if (
        not isinstance(registry_sha256, str)
        or not registry_sha256.startswith("sha256:")
        or _LOWER_SHA256.fullmatch(registry_sha256.removeprefix("sha256:")) is None
    ):
        raise ArtifactMigrationError("derived migration registry SHA-256 is invalid")

    target = _exact_mapping(document["target"], _TARGET_KEYS, "derived target")
    contract = _nonempty_string(target["contract"], "derived target contract")
    target_version = _positive_integer(
        target["schema_version"], "derived target schema_version"
    )
    contract_parts = contract.rsplit("@", 1)
    if len(contract_parts) != 2 or contract_parts[1] != str(target_version):
        raise ArtifactMigrationError("derived target contract/version disagree")
    return dict(document)


@dataclass(frozen=True, init=False)
class DerivedArtifact:
    _canonical_document: Mapping[str, object]
    _canonical_bytes: bytes

    def __init__(self, canonical_document: Mapping[str, object]) -> None:
        document = _plain_json_copy(canonical_document)
        validated = _validate_derived_document(document)
        try:
            encoded = canonicalize(validated)
        except CanonicalizationError as error:
            raise ArtifactMigrationError(
                f"derived artifact is not valid I-JSON: {error}"
            ) from error
        object.__setattr__(self, "_canonical_document", _deep_freeze(validated))
        object.__setattr__(self, "_canonical_bytes", bytes(encoded))

    @property
    def canonical_document(self) -> Mapping[str, object]:
        return _deep_thaw(self._canonical_document)

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes


def _workflow_observation_v1(
    envelope: MarkdownEnvelope,
    observation_projection_policy: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if observation_projection_policy is None:
        raise ArtifactMigrationError("observation projection policy is required")
    return canonical_episode_projection(
        envelope.metadata,
        envelope.body,
        observation_projection_policy,
        artifact=envelope.artifact,
    )


def _workflow_observation_v2(
    envelope: MarkdownEnvelope,
    observation_projection_policy: Mapping[str, object] | None,
) -> Mapping[str, object]:
    if observation_projection_policy is None:
        raise ArtifactMigrationError("observation projection policy is required")
    return canonical_episode_projection(
        envelope.metadata,
        envelope.body,
        observation_projection_policy,
        artifact=envelope.artifact,
    )


def _observation_invalidation_v1(
    envelope: MarkdownEnvelope,
    observation_projection_policy: Mapping[str, object] | None,
) -> Mapping[str, object]:
    del observation_projection_policy
    return {
        "artifact_type": "observation-invalidation",
        "schema_version": 2,
        "run_id": envelope.metadata["target_run_id"],
        "timestamp": envelope.metadata["timestamp"],
    }


def _learning_snapshot_v1(
    artifact: Mapping[str, object],
    learning_policy_set: object,
) -> Mapping[str, object]:
    if not isinstance(learning_policy_set, PolicySet):
        raise ArtifactMigrationError(
            "learning snapshot migration requires its exact PolicySet"
        )
    return migrate_learning_snapshot_v1_core(
        artifact["core"],
        adapter=artifact["adapter"],
        policy_set=learning_policy_set,
    )


_Handler = Callable[
    [object, object], Mapping[str, object]
]
_HANDLERS: Mapping[str, _Handler] = MappingProxyType({
    "learning-snapshot-v1": _learning_snapshot_v1,
    "observation-invalidation-v1": _observation_invalidation_v1,
    "workflow-observation-v1": _workflow_observation_v1,
    "workflow-observation-v2": _workflow_observation_v2,
})


def _markdown_source(
    source_bytes: bytes,
    *,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
) -> MarkdownEnvelope:
    human_types = {
        "workflow-observation": "observation",
        "observation-invalidation": "observation-invalidation",
    }
    expected_human_type = human_types.get(expected_artifact_type)
    if expected_human_type is None:
        raise ArtifactMigrationError("expected artifact type is unsupported")
    try:
        text = source_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ArtifactMigrationError("source bytes are not valid UTF-8") from error
    return parse_markdown_envelope(
        text,
        expected_human_type=expected_human_type,
        policies=policies,
    )


def _declared_json_source(
    source_bytes: bytes,
    *,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
    learning_policy_set: PolicySet | None,
) -> tuple[ArtifactSchemaRef, Mapping[str, object]]:
    try:
        value = strict_json_loads(source_bytes)
    except CanonicalizationError as error:
        raise ArtifactMigrationError(f"source JSON is invalid: {error}") from error
    try:
        if canonicalize(value) != source_bytes:
            raise ArtifactMigrationError(
                "legacy learning snapshot bytes are not canonical JCS"
            )
    except CanonicalizationError as error:
        raise ArtifactMigrationError(
            f"source JSON is not canonicalizable: {error}"
        ) from error
    if expected_artifact_type == "learning-snapshot":
        if not isinstance(value, Mapping) or "schema_version" in value:
            raise ArtifactMigrationError(
                "legacy learning snapshot must have its exact no-version envelope"
            )
        validated = validate_learning_snapshot_artifact(
            value,
            expected_schema_version=1,
            policy_set=learning_policy_set,
        )
        artifact = classify_json_artifact(
            {"artifact_type": "learning-snapshot", "schema_version": 1},
            expected_artifact_type="learning-snapshot",
            policies=policies,
        )
        return artifact, validated
    artifact = classify_json_artifact(
        value,
        expected_artifact_type=expected_artifact_type,
        policies=policies,
    )
    return artifact, value


def _registry_binding(
    artifact: ArtifactSchemaRef,
    *,
    policies: ArtifactPolicySet,
) -> ArtifactMigrationRef:
    return resolve_artifact_migration(artifact, policies=policies)


def migrate_artifact(
    *,
    source_bytes: bytes,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
    observation_projection_policy: Mapping[str, object] | None = None,
    learning_policy_set: PolicySet | None = None,
) -> DerivedArtifact:
    if type(source_bytes) is not bytes:
        raise ArtifactMigrationError("source_bytes must be exact immutable bytes")
    if not isinstance(expected_artifact_type, str) or not expected_artifact_type:
        raise ArtifactMigrationError("expected artifact type is invalid")
    if not isinstance(policies, ArtifactPolicySet):
        raise ArtifactMigrationError("artifact policies are required")

    if expected_artifact_type == "learning-snapshot":
        artifact, source_document = _declared_json_source(
            source_bytes,
            expected_artifact_type=expected_artifact_type,
            policies=policies,
            learning_policy_set=learning_policy_set,
        )
        migration = _registry_binding(artifact, policies=policies)
        handler = _HANDLERS.get(migration.handler)
        if handler is None:
            raise ArtifactMigrationError(
                f"unsupported migration handler: {migration.handler}"
            )
        value = handler(source_document, learning_policy_set)
        validate_learning_snapshot_core(
            value,
            expected_schema_version=2,
            adapter=source_document["adapter"],
            policy_set=learning_policy_set,
        )
        source_identity = {
            "artifact_type": artifact.artifact_type,
            "schema_version": artifact.schema_version,
            "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "snapshot_id": source_document["snapshot_id"],
        }
        source_run_id = None
    else:
        envelope = _markdown_source(
            source_bytes,
            expected_artifact_type=expected_artifact_type,
            policies=policies,
        )
        migration = _registry_binding(envelope.artifact, policies=policies)
        handler = _HANDLERS.get(migration.handler)
        if handler is None:
            raise ArtifactMigrationError(
                f"unsupported migration handler: {migration.handler}"
            )
        value = handler(envelope, observation_projection_policy)
        source_identity = {
            "artifact_type": envelope.artifact.artifact_type,
            "schema_version": envelope.artifact.schema_version,
            "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        }
        source_run_id = (
            envelope.metadata["run_id"]
            if envelope.artifact.artifact_type == "workflow-observation"
            else envelope.metadata["target_run_id"]
        )

    if migration.target_contract == "observation-invalidation@2":
        classify_json_artifact(
            value,
            expected_artifact_type="observation-invalidation",
            policies=policies,
        )
        if set(value) != {
            "artifact_type",
            "schema_version",
            "run_id",
            "timestamp",
        }:
            raise ArtifactMigrationError("invalidation projection keys are invalid")

    if source_run_id is not None and value.get("run_id") != source_run_id:
        raise ArtifactMigrationError("migration changed source run_id")

    identities = policies.identities()
    registry_identity = identities["artifact_migration_registry"]
    document = {
        "artifact_type": "derived-artifact",
        "schema_version": 1,
        "source": source_identity,
        "migration": {
            "migration_identity": migration.migration_identity,
            "migration_registry_sha256": registry_identity["sha256"],
        },
        "target": {
            "contract": migration.target_contract,
            "schema_version": migration.target_schema_version,
            "value": value,
        },
    }
    classify_json_artifact(
        document,
        expected_artifact_type="derived-artifact",
        policies=policies,
    )
    return DerivedArtifact(document)
