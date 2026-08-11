from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
from types import MappingProxyType

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)
from policy_artifacts import (
    PolicyError,
    read_regular_file_evidence,
    validate_relative_posix_artifact_path,
)


_POLICY_FILES = {
    "artifact_schema_registry": "artifact_schema_registry.json",
    "artifact_migration_registry": "artifact_migration_registry.json",
    "health_event_schema": "health_event_schema.json",
}
_APPROVED_IDENTITIES = {
    "artifact_schema_registry": {
        "version": "artifact-schema-registry@1",
        "sha256": "sha256:1bba0c5635ed2cedf4885861243947c89d3f9ba98e358b049ff3a61c0a40e7d6",
    },
    "artifact_migration_registry": {
        "version": "artifact-migration-registry@1",
        "sha256": "sha256:0c6bbdb88de176725c065f885a4393b73db19ee769f04f486ade013121e0fe90",
    },
    "health_event_schema": {
        "version": "health-event-schema@1",
        "sha256": "sha256:5abab8b18858e95535b31185eb65d273e8ec4758034e5cfe85492528fcaba516",
    },
}
_ASCII_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@+\-]*")
_UTC_INSTANT = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)
_DIGEST = re.compile(r"[0-9a-f]{64}")
_SCHEMA_ROOT_KEYS = {
    "artifact_type",
    "schema_version",
    "registry_version",
    "schemas",
}
_SCHEMA_ROW_KEYS = {
    "artifact_type",
    "schema_version",
    "schema_identity",
    "reader_contract",
    "writer_contract",
}
_MIGRATION_ROOT_KEYS = {
    "artifact_type",
    "schema_version",
    "registry_version",
    "migrations",
}
_MIGRATION_ROW_KEYS = {
    "source_artifact_type",
    "source_schema_version",
    "target_contract",
    "target_schema_version",
    "migration_identity",
    "handler",
}
_HEALTH_SCHEMA_ROOT_KEYS = {
    "artifact_type",
    "schema_version",
    "schema_identity",
    "event_schema_identity",
    "event_types",
    "error_classes",
    "resource_kinds",
    "limits",
    "hash_domain",
}
_HEALTH_EVENT_TYPES = (
    "validation-rejected",
    "schema-mismatch",
    "duplicate-finish",
    "record-dropped",
    "payload-cleanup-failed",
    "lock-contended",
    "lock-timeout",
    "stale-owner-recovered",
    "cas-conflict",
    "maintenance-lease-timeout",
    "maintenance-recovery-blocked",
    "sampling-decision-failed",
    "export-aborted",
    "purge-aborted",
)
_HEALTH_EVENT_KEYS = {
    "artifact_type",
    "schema_version",
    "event_id",
    "occurred_at",
    "event_type",
    "operation_id",
    "run_id",
    "resource_kind",
    "resource_key",
    "evidence",
    "error_class",
    "policy_identity",
}


class ArtifactSchemaError(PolicyError):
    """An artifact policy or schema envelope is invalid."""


def _error(message: str, cause: Exception | None = None) -> ArtifactSchemaError:
    result = ArtifactSchemaError(message)
    if cause is not None:
        result.__cause__ = cause
    return result


def _deep_freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_thaw(item) for item in value]
    return value


def _plain_json_copy(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _plain_json_copy(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain_json_copy(item) for item in value]
    return value


def _exact_mapping(
    value: object,
    keys: set[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ArtifactSchemaError(f"{label} does not have its exact allowed keys")
    return value


def _positive_integer(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ArtifactSchemaError(f"{label} must be a positive integer")
    return value


def _nonnegative_integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ArtifactSchemaError(f"{label} must be a non-negative integer")
    return value


def _ascii_identifier(
    value: object,
    label: str,
    *,
    maximum_bytes: int = 200,
) -> str:
    if (
        not isinstance(value, str)
        or _ASCII_IDENTIFIER.fullmatch(value) is None
        or len(value.encode("ascii")) > maximum_bytes
    ):
        raise ArtifactSchemaError(f"{label} must be a bounded ASCII identifier")
    return value


def _sorted_unique_strings(value: object, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
        or len(value) != len(set(value))
    ):
        raise ArtifactSchemaError(f"{label} must be a non-empty unique string list")
    if value != sorted(value, key=lambda item: item.encode("utf-8")):
        raise ArtifactSchemaError(f"{label} must be sorted by UTF-8 bytes")
    return value


@dataclass(frozen=True)
class ArtifactSchemaRef:
    artifact_type: str
    schema_version: int
    schema_identity: str
    reader_contract: str
    writer_contract: str


def _validate_schema_registry(
    value: object,
) -> tuple[Mapping[str, object], dict[tuple[str, int], ArtifactSchemaRef]]:
    document = _exact_mapping(value, _SCHEMA_ROOT_KEYS, "schema registry")
    if document["artifact_type"] != "artifact-schema-registry":
        raise ArtifactSchemaError("schema registry artifact_type is invalid")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ArtifactSchemaError("schema registry schema_version is invalid")
    if document["registry_version"] != "artifact-schema-registry@1":
        raise ArtifactSchemaError("schema registry version is invalid")
    rows = document["schemas"]
    if not isinstance(rows, list) or not rows:
        raise ArtifactSchemaError("schema registry schemas must be a non-empty list")

    index: dict[tuple[str, int], ArtifactSchemaRef] = {}
    dispatch_order = []
    for position, raw in enumerate(rows):
        row = _exact_mapping(raw, _SCHEMA_ROW_KEYS, f"schema row {position}")
        artifact_type = _ascii_identifier(
            row["artifact_type"], f"schema row {position} artifact_type"
        )
        schema_version = _positive_integer(
            row["schema_version"], f"schema row {position} schema_version"
        )
        schema_identity = _ascii_identifier(
            row["schema_identity"], f"schema row {position} schema identity"
        )
        if schema_identity != f"{artifact_type}@{schema_version}":
            raise ArtifactSchemaError("schema identity is inconsistent with dispatch key")
        reader_contract = _ascii_identifier(
            row["reader_contract"], f"schema row {position} reader_contract"
        )
        writer_contract = _ascii_identifier(
            row["writer_contract"], f"schema row {position} writer_contract"
        )
        key = (artifact_type, schema_version)
        if key in index:
            raise ArtifactSchemaError("schema registry has a duplicate dispatch key")
        index[key] = ArtifactSchemaRef(
            artifact_type=artifact_type,
            schema_version=schema_version,
            schema_identity=schema_identity,
            reader_contract=reader_contract,
            writer_contract=writer_contract,
        )
        dispatch_order.append(key)
    expected_order = sorted(
        dispatch_order, key=lambda item: (item[0].encode("utf-8"), item[1])
    )
    if dispatch_order != expected_order:
        raise ArtifactSchemaError("schema registry dispatch entries are not sorted")
    return document, index


def _validate_migration_registry(
    value: object,
    schema_index: Mapping[tuple[str, int], ArtifactSchemaRef],
) -> Mapping[str, object]:
    document = _exact_mapping(value, _MIGRATION_ROOT_KEYS, "migration registry")
    if document["artifact_type"] != "artifact-migration-registry":
        raise ArtifactSchemaError("migration registry artifact_type is invalid")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ArtifactSchemaError("migration registry schema_version is invalid")
    if document["registry_version"] != "artifact-migration-registry@1":
        raise ArtifactSchemaError("migration registry version is invalid")
    rows = document["migrations"]
    if not isinstance(rows, list) or not rows:
        raise ArtifactSchemaError("migration registry migrations must be non-empty")

    identity_index = {ref.schema_identity: ref for ref in schema_index.values()}
    dispatch_order = []
    migration_identities = set()
    handlers = set()
    for position, raw in enumerate(rows):
        row = _exact_mapping(
            raw, _MIGRATION_ROW_KEYS, f"migration row {position}"
        )
        source_type = _ascii_identifier(
            row["source_artifact_type"],
            f"migration row {position} source_artifact_type",
        )
        source_version = _positive_integer(
            row["source_schema_version"],
            f"migration row {position} source_schema_version",
        )
        source_key = (source_type, source_version)
        if source_key not in schema_index:
            raise ArtifactSchemaError("migration source is not a registered schema")
        target_contract = _ascii_identifier(
            row["target_contract"], f"migration row {position} target_contract"
        )
        target_version = _positive_integer(
            row["target_schema_version"],
            f"migration row {position} target_schema_version",
        )
        target = identity_index.get(target_contract)
        if target is None or target.schema_version != target_version:
            raise ArtifactSchemaError("migration target is not a registered schema")
        migration_identity = _ascii_identifier(
            row["migration_identity"],
            f"migration row {position} migration_identity",
        )
        handler = _ascii_identifier(
            row["handler"], f"migration row {position} handler"
        )
        if source_key in dispatch_order:
            raise ArtifactSchemaError("migration registry has a duplicate dispatch key")
        if migration_identity in migration_identities:
            raise ArtifactSchemaError("migration identity is duplicated")
        if handler in handlers:
            raise ArtifactSchemaError("migration handler is duplicated")
        dispatch_order.append(source_key)
        migration_identities.add(migration_identity)
        handlers.add(handler)
    expected_order = sorted(
        dispatch_order, key=lambda item: (item[0].encode("utf-8"), item[1])
    )
    if dispatch_order != expected_order:
        raise ArtifactSchemaError("migration dispatch entries are not sorted")
    return document


def _validate_evidence_property(value: object, label: str) -> None:
    if not isinstance(value, Mapping) or "type" not in value:
        raise ArtifactSchemaError(f"{label} is not an evidence mini-schema")
    property_type = value["type"]
    if property_type == "enum":
        row = _exact_mapping(value, {"type", "values"}, label)
        _sorted_unique_strings(row["values"], f"{label}.values")
    elif property_type in {
        "boolean",
        "bounded-identifier",
        "nonnegative-integer",
        "positive-integer",
    }:
        _exact_mapping(value, {"type"}, label)
    else:
        raise ArtifactSchemaError(f"{label} has an unsupported evidence type")


def _validate_health_schema(
    value: object,
    schema_index: Mapping[tuple[str, int], ArtifactSchemaRef],
) -> Mapping[str, object]:
    document = _exact_mapping(
        value, _HEALTH_SCHEMA_ROOT_KEYS, "health event schema"
    )
    if document["artifact_type"] != "health-event-schema":
        raise ArtifactSchemaError("health event schema artifact_type is invalid")
    if type(document["schema_version"]) is not int or document["schema_version"] != 1:
        raise ArtifactSchemaError("health event schema_version is invalid")
    if document["schema_identity"] != "health-event-schema@1":
        raise ArtifactSchemaError("health event schema identity is invalid")
    health_ref = schema_index.get(("health-event", 1))
    if (
        health_ref is None
        or document["event_schema_identity"] != health_ref.schema_identity
    ):
        raise ArtifactSchemaError("health event schema identity is not registered")

    rows = document["event_types"]
    if not isinstance(rows, list):
        raise ArtifactSchemaError("health event types must be a list")
    event_types = []
    for position, raw in enumerate(rows):
        row = _exact_mapping(raw, {"event_type", "evidence"}, f"event row {position}")
        event_type = _ascii_identifier(
            row["event_type"], f"event row {position} event_type"
        )
        evidence = _exact_mapping(
            row["evidence"], {"required", "properties"}, f"event row {position} evidence"
        )
        properties = evidence["properties"]
        if not isinstance(properties, Mapping) or not properties:
            raise ArtifactSchemaError("event evidence properties must be non-empty")
        required = evidence["required"]
        if not isinstance(required, list) or required != list(properties):
            raise ArtifactSchemaError("event evidence required keys are inconsistent")
        if len(required) != len(set(required)):
            raise ArtifactSchemaError("event evidence required keys are duplicated")
        for name, property_schema in properties.items():
            _ascii_identifier(name, "event evidence property")
            _validate_evidence_property(
                property_schema, f"event row {position} evidence.{name}"
            )
        event_types.append(event_type)
    if tuple(event_types) != _HEALTH_EVENT_TYPES:
        raise ArtifactSchemaError("health event types do not match approved order")

    _sorted_unique_strings(document["error_classes"], "health error classes")
    _sorted_unique_strings(document["resource_kinds"], "health resource kinds")
    limits = _exact_mapping(
        document["limits"],
        {"identifier_max_utf8_bytes", "integer_max"},
        "health event limits",
    )
    if limits["identifier_max_utf8_bytes"] != 128:
        raise ArtifactSchemaError("health identifier limit is invalid")
    if limits["integer_max"] != 9007199254740991:
        raise ArtifactSchemaError("health integer limit is invalid")
    if document["hash_domain"] != "workflow-observatory:health-event:v1":
        raise ArtifactSchemaError("health hash domain is invalid")
    return document


def _canonical_identity(version: str, document: object) -> dict[str, str]:
    return {
        "version": version,
        "sha256": "sha256:" + hashlib.sha256(canonicalize(document)).hexdigest(),
    }


def _validate_policy_documents(
    schema_registry: object,
    migration_registry: object,
    health_event_schema: object,
) -> tuple[dict[tuple[str, int], ArtifactSchemaRef], dict[str, dict[str, str]]]:
    try:
        canonicalize(
            {
                "schema_registry": schema_registry,
                "migration_registry": migration_registry,
                "health_event_schema": health_event_schema,
            }
        )
    except CanonicalizationError as error:
        raise _error("artifact policies are not valid I-JSON", error)
    schema, schema_index = _validate_schema_registry(schema_registry)
    migration = _validate_migration_registry(migration_registry, schema_index)
    health = _validate_health_schema(health_event_schema, schema_index)
    identities = {
        "artifact_schema_registry": _canonical_identity(
            str(schema["registry_version"]), schema
        ),
        "artifact_migration_registry": _canonical_identity(
            str(migration["registry_version"]), migration
        ),
        "health_event_schema": _canonical_identity(
            str(health["schema_identity"]), health
        ),
    }
    for name, identity in identities.items():
        if identity != _APPROVED_IDENTITIES[name]:
            raise ArtifactSchemaError(
                f"{name} version does not match its approved policy bytes"
            )
    return schema_index, identities


@dataclass(frozen=True, init=False)
class ArtifactPolicySet:
    _schema_registry: Mapping[str, object]
    _migration_registry: Mapping[str, object]
    _health_event_schema: Mapping[str, object]
    _identities: Mapping[str, Mapping[str, str]]
    _schema_index: Mapping[tuple[str, int], ArtifactSchemaRef]

    def __init__(
        self,
        *,
        schema_registry: Mapping[str, object],
        migration_registry: Mapping[str, object],
        health_event_schema: Mapping[str, object],
        identities: Mapping[str, Mapping[str, str]],
    ) -> None:
        schema_document = _plain_json_copy(schema_registry)
        migration_document = _plain_json_copy(migration_registry)
        health_document = _plain_json_copy(health_event_schema)
        identity_document = _plain_json_copy(identities)
        schema_index, expected_identities = _validate_policy_documents(
            schema_document, migration_document, health_document
        )
        try:
            canonicalize(identity_document)
        except CanonicalizationError as error:
            raise _error("artifact policy identities are not valid I-JSON", error)
        if identity_document != expected_identities:
            raise ArtifactSchemaError("artifact policy identities are inconsistent")
        object.__setattr__(self, "_schema_registry", _deep_freeze(schema_document))
        object.__setattr__(
            self, "_migration_registry", _deep_freeze(migration_document)
        )
        object.__setattr__(
            self, "_health_event_schema", _deep_freeze(health_document)
        )
        object.__setattr__(self, "_identities", _deep_freeze(expected_identities))
        object.__setattr__(
            self, "_schema_index", MappingProxyType(dict(schema_index))
        )

    @property
    def schema_registry(self) -> dict[str, object]:
        return _deep_thaw(self._schema_registry)

    @property
    def migration_registry(self) -> dict[str, object]:
        return _deep_thaw(self._migration_registry)

    @property
    def health_event_schema(self) -> dict[str, object]:
        return _deep_thaw(self._health_event_schema)

    def identities(self) -> dict[str, dict[str, str]]:
        return _deep_thaw(self._identities)


def load_artifact_policy_set(policy_root: Path) -> ArtifactPolicySet:
    try:
        root = Path(policy_root)
    except TypeError as error:
        raise _error("artifact policy root is invalid", error)
    if ".." in root.parts:
        raise ArtifactSchemaError("artifact policy root has unsafe path spelling")

    documents = {}
    for name, filename in _POLICY_FILES.items():
        validate_relative_posix_artifact_path(filename)
        evidence = read_regular_file_evidence(
            root,
            filename,
            max_bytes=1_048_576,
        )
        try:
            documents[name] = strict_json_loads(evidence.content)
        except CanonicalizationError as error:
            raise _error(f"{filename}: {error}", error)
    schema_index, identities = _validate_policy_documents(
        documents["artifact_schema_registry"],
        documents["artifact_migration_registry"],
        documents["health_event_schema"],
    )
    del schema_index
    return ArtifactPolicySet(
        schema_registry=documents["artifact_schema_registry"],
        migration_registry=documents["artifact_migration_registry"],
        health_event_schema=documents["health_event_schema"],
        identities=identities,
    )


def classify_json_artifact(
    value: object,
    *,
    expected_artifact_type: str,
    policies: ArtifactPolicySet,
) -> ArtifactSchemaRef:
    _ascii_identifier(expected_artifact_type, "expected artifact type")
    if not isinstance(policies, ArtifactPolicySet):
        raise ArtifactSchemaError("artifact policies are required")
    if not isinstance(value, Mapping):
        raise ArtifactSchemaError("JSON artifact must be an object")
    artifact_type = value.get("artifact_type")
    schema_version = value.get("schema_version")
    if artifact_type != expected_artifact_type:
        raise ArtifactSchemaError("artifact type does not match expectation")
    if type(schema_version) is not int or schema_version <= 0:
        raise ArtifactSchemaError("artifact schema version is invalid")
    ref = policies._schema_index.get((artifact_type, schema_version))
    if ref is None:
        raise ArtifactSchemaError("unknown artifact schema type/version pair")
    if "schema_identity" in value and value["schema_identity"] != ref.schema_identity:
        raise ArtifactSchemaError("artifact schema identity is inconsistent")
    return ref


def _canonical_utc_instant(value: object) -> str:
    if not isinstance(value, str) or _UTC_INSTANT.fullmatch(value) is None:
        raise ArtifactSchemaError("occurred_at must be a canonical UTC second instant")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise _error("occurred_at must be a canonical UTC second instant", error)
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ArtifactSchemaError("occurred_at must be a canonical UTC second instant")
    return value


def _validate_event_evidence(
    value: object,
    schema: Mapping[str, object],
    *,
    identifier_limit: int,
    integer_limit: int,
) -> None:
    required = schema["required"]
    properties = schema["properties"]
    if not isinstance(value, Mapping) or set(value) != set(required):
        raise ArtifactSchemaError("health event evidence does not have exact keys")
    for name, property_schema in properties.items():
        item = value[name]
        property_type = property_schema["type"]
        if property_type == "enum":
            if item not in property_schema["values"]:
                raise ArtifactSchemaError("health event evidence enum is invalid")
        elif property_type == "bounded-identifier":
            _ascii_identifier(
                item,
                "health event evidence identifier",
                maximum_bytes=identifier_limit,
            )
        elif property_type == "positive-integer":
            if type(item) is not int or not 0 < item <= integer_limit:
                raise ArtifactSchemaError("health event evidence integer is invalid")
        elif property_type == "nonnegative-integer":
            if type(item) is not int or not 0 <= item <= integer_limit:
                raise ArtifactSchemaError("health event evidence integer is invalid")
        elif property_type == "boolean":
            if type(item) is not bool:
                raise ArtifactSchemaError("health event evidence Boolean is invalid")


def validate_health_event_document(
    value: object,
    *,
    policies: ArtifactPolicySet,
    require_digest: bool,
) -> Mapping[str, object]:
    if type(require_digest) is not bool:
        raise ArtifactSchemaError("require_digest must be Boolean")
    expected_keys = set(_HEALTH_EVENT_KEYS)
    if require_digest:
        expected_keys.add("event_sha256")
    event = _exact_mapping(value, expected_keys, "health event")
    classify_json_artifact(
        event,
        expected_artifact_type="health-event",
        policies=policies,
    )
    schema = policies._health_event_schema
    limits = schema["limits"]
    identifier_limit = limits["identifier_max_utf8_bytes"]
    integer_limit = limits["integer_max"]
    for name in (
        "event_id",
        "operation_id",
        "resource_key",
        "policy_identity",
    ):
        _ascii_identifier(
            event[name],
            f"health event {name} identifier",
            maximum_bytes=identifier_limit,
        )
    if event["run_id"] is not None:
        _ascii_identifier(
            event["run_id"],
            "health event run_id identifier",
            maximum_bytes=identifier_limit,
        )
    _canonical_utc_instant(event["occurred_at"])
    if event["resource_kind"] not in schema["resource_kinds"]:
        raise ArtifactSchemaError("health event resource kind is unknown")
    if event["error_class"] not in schema["error_classes"]:
        raise ArtifactSchemaError("health event error class is unknown")
    rows = {row["event_type"]: row["evidence"] for row in schema["event_types"]}
    evidence_schema = rows.get(event["event_type"])
    if evidence_schema is None:
        raise ArtifactSchemaError("health event type is unknown")
    _validate_event_evidence(
        event["evidence"],
        evidence_schema,
        identifier_limit=identifier_limit,
        integer_limit=integer_limit,
    )
    document = _deep_thaw(event)
    if require_digest:
        digest = event["event_sha256"]
        if not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None:
            raise ArtifactSchemaError("event_sha256 is not a lowercase SHA-256")
        preimage = dict(document)
        del preimage["event_sha256"]
        expected = hash_canonical(
            (str(schema["hash_domain"]) + "\0").encode("ascii"), preimage
        )
        if digest != expected:
            raise ArtifactSchemaError("event_sha256 is stale or self-inconsistent")
    try:
        canonicalize(document)
    except CanonicalizationError as error:
        raise _error("health event is not valid I-JSON", error)
    return deepcopy(document)
