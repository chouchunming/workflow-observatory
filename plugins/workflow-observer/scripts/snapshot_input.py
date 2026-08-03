"""Canonical adapter-neutral acquisition input for workflow learning."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
import stat
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from canonical_json import CanonicalizationError, canonicalize, hash_canonical
from episode_schema import EpisodeSchemaError, canonical_episode_projection
from policy_artifacts import PolicyError, PolicySet, validate_policy_documents
from store_config import AdapterSemantics
from wiki_observations import (
    InvalidationEvidence,
    ObservationCollection,
    ObservationError,
    ObservationPaths,
    RecordDocument,
    ReferenceEvidence,
    TAXONOMY,
    _validate_scalar,
    collect_record_documents,
)


_UTC_INSTANT_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
_WORKSPACE_ID_RE = re.compile(r"^[0-9a-f]{12}$")
_GENERATION_RE = re.compile(r"^[a-z0-9][a-z0-9._:@+\-]{0,199}$")
_REFERENCE_KIND_RE = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[A-Za-z]:[\\/]")
_MANIFEST_DOMAIN = b"workflow-observatory:snapshot-input-manifest:v1\0"
_STORE_IDENTITY_DOMAIN = b"workflow-observatory:store-identity:v1\0"
_ADAPTER_IMPLEMENTATION_VERSION = "workflow-observer-snapshot-adapter@1"
SNAPSHOT_ANALYZER_FILES = (
    "scripts/episode_schema.py",
    "scripts/policy_artifacts.py",
    "scripts/snapshot_input.py",
    "scripts/store_config.py",
    "scripts/wiki_observations.py",
)
_DOCUMENT_IDENTITIES = {
    "episode_projection": "canonical_projection_contract",
    "producer_capabilities": "producer_capability_registry",
    "workflow_generation_mapping": "workflow_generation_mapping",
    "metric_semantics": "metric_semantics_registry",
    "quantile_policy": "quantile_policy",
    "decision_support_policy": "decision_support_policy",
    "lifecycle_health_policy": "lifecycle_health_policy",
    "candidate_emission_policy": "candidate_emission_policy",
}


class SnapshotInputError(ObservationError):
    """A snapshot acquisition error normalized by the observation CLI."""


@dataclass(frozen=True)
class SnapshotQuery:
    interval: dict
    lifecycle_as_of: str
    project: str | None
    workspace: str | None
    workspace_id: str | None
    task_type: str | None


@dataclass(frozen=True)
class SnapshotInput:
    adapter: dict[str, str]
    store_identity: str | None
    semantic_bundle: dict

    @property
    def canonical_representation(self) -> dict:
        return {
            "adapter": deepcopy(self.adapter),
            "store_identity": self.store_identity,
            "semantic_bundle": deepcopy(self.semantic_bundle),
        }

    @property
    def manifest_bytes(self) -> bytes:
        return canonicalize(self.canonical_representation)


def _snapshot_error(
    message: str,
    *,
    kind: str = "validation",
    cause: BaseException | None = None,
) -> SnapshotInputError:
    error = SnapshotInputError(kind, message)
    if cause is not None:
        error.__cause__ = cause
    return error


def _utc_instant(value: object, label: str) -> datetime:
    if not isinstance(value, str) or _UTC_INSTANT_RE.fullmatch(value) is None:
        raise _snapshot_error(
            f"{label} must be a second-precision UTC Z instant"
        )
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise _snapshot_error(
            f"{label} must be a second-precision UTC Z instant", cause=error
        ) from error


def _canonical_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _localized_midnight(value: date, zone: ZoneInfo) -> datetime:
    wall = datetime.combine(value, time.min)
    first = wall.replace(tzinfo=zone, fold=0)
    second = wall.replace(tzinfo=zone, fold=1)
    if first.utcoffset() != second.utcoffset():
        raise _snapshot_error(
            "requested timezone has a skipped or ambiguous civil midnight"
        )
    absolute = first.astimezone(timezone.utc)
    round_trip = absolute.astimezone(zone)
    if (
        round_trip.date() != value
        or round_trip.hour != 0
        or round_trip.minute != 0
        or round_trip.second != 0
        or round_trip.microsecond != 0
    ):
        raise _snapshot_error(
            "requested timezone has a skipped or ambiguous civil midnight"
        )
    return absolute


def canonical_interval(
    since: date,
    until_inclusive: date,
    timezone_name: str,
) -> dict:
    """Convert inclusive civil dates into one fixed UTC half-open interval."""

    if type(since) is not date or type(until_inclusive) is not date:
        raise _snapshot_error("snapshot dates must be date values")
    if until_inclusive < since:
        raise _snapshot_error("snapshot end date cannot precede its start date")
    if (
        not isinstance(timezone_name, str)
        or not timezone_name
        or len(timezone_name) > 200
        or any(ord(character) < 0x20 for character in timezone_name)
    ):
        raise _snapshot_error("snapshot timezone must be a bounded IANA name")
    try:
        canonicalize(timezone_name)
    except (CanonicalizationError, UnicodeError) as error:
        raise _snapshot_error(
            "snapshot timezone must be valid UTF-8 I-JSON text", cause=error
        ) from error
    try:
        zone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError, UnicodeError) as error:
        raise _snapshot_error(
            "unknown IANA timezone", cause=error
        ) from error
    try:
        end_date = until_inclusive + timedelta(days=1)
    except OverflowError as error:
        raise _snapshot_error("snapshot end date is outside the supported range") from error
    start = _localized_midnight(since, zone)
    end = _localized_midnight(end_date, zone)
    return {
        "basis": "started_at",
        "since_inclusive": _canonical_utc(start),
        "until_exclusive": _canonical_utc(end),
        "requested_timezone": timezone_name,
        "requested_dates": {
            "since": since.isoformat(),
            "until_inclusive": until_inclusive.isoformat(),
        },
    }


def _validate_interval(interval: object) -> dict:
    if not isinstance(interval, dict) or set(interval) != {
        "basis",
        "since_inclusive",
        "until_exclusive",
        "requested_timezone",
        "requested_dates",
    }:
        raise _snapshot_error("snapshot interval does not have its exact fields")
    requested = interval.get("requested_dates")
    if not isinstance(requested, dict) or set(requested) != {
        "since", "until_inclusive"
    }:
        raise _snapshot_error("snapshot requested_dates does not have its exact fields")
    try:
        since = date.fromisoformat(requested["since"])
        until = date.fromisoformat(requested["until_inclusive"])
    except (TypeError, ValueError) as error:
        raise _snapshot_error("snapshot requested dates are invalid", cause=error) from error
    rebuilt = canonical_interval(since, until, interval.get("requested_timezone"))
    if interval != rebuilt:
        raise _snapshot_error("snapshot interval is not canonical")
    return rebuilt


def _bounded_filter(value: object, label: str) -> str | None:
    if value is None:
        return None
    try:
        return _validate_scalar(value, f"snapshot {label} filter")
    except ObservationError as error:
        raise _snapshot_error(str(error), cause=error) from error


def validate_snapshot_query(query: SnapshotQuery) -> None:
    """Validate a query before storage access or acquisition."""

    if not isinstance(query, SnapshotQuery):
        raise _snapshot_error("snapshot query has the wrong type")
    interval = _validate_interval(query.interval)
    lifecycle_as_of = _utc_instant(query.lifecycle_as_of, "lifecycle_as_of")
    until_exclusive = _utc_instant(
        interval["until_exclusive"], "interval until_exclusive"
    )
    if lifecycle_as_of < until_exclusive:
        raise _snapshot_error(
            "lifecycle_as_of cannot precede interval until_exclusive"
        )
    _bounded_filter(query.project, "project")
    _bounded_filter(query.workspace, "workspace")
    if query.workspace_id is not None and (
        not isinstance(query.workspace_id, str)
        or _WORKSPACE_ID_RE.fullmatch(query.workspace_id) is None
    ):
        raise _snapshot_error(
            "snapshot workspace_id filter must be 12 lowercase hex"
        )
    if query.task_type is not None and (
        not isinstance(query.task_type, str) or query.task_type not in TAXONOMY
    ):
        raise _snapshot_error("snapshot task_type filter is invalid")


def _reference_identity(value: object) -> str:
    components = value.split("/") if isinstance(value, str) else ()
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 1024
        or "\0" in value
        or "\\" in value
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        or value.startswith(("/", "\\", "//"))
        or _WINDOWS_ABSOLUTE_RE.match(value) is not None
        or Path(value).is_absolute()
        or any(component in {"", ".", ".."} for component in components)
    ):
        raise _snapshot_error(
            "reference evidence identity must be a bounded relative identity"
        )
    try:
        value.encode("utf-8", errors="strict")
        canonicalize(value)
    except (UnicodeEncodeError, CanonicalizationError) as error:
        raise _snapshot_error(
            "reference evidence identity must be a bounded relative identity",
            cause=error,
        ) from error
    return value


def canonical_reference_manifest(
    evidence: Iterable[ReferenceEvidence],
) -> list[dict[str, str]]:
    """Return unique path-free reference evidence in canonical byte order."""

    rows: dict[tuple[str, str], str] = {}
    try:
        iterator = iter(evidence)
    except TypeError as error:
        raise _snapshot_error("reference evidence must be iterable", cause=error) from error
    for item in iterator:
        if not isinstance(item, ReferenceEvidence):
            raise _snapshot_error("reference evidence has the wrong type")
        if (
            not isinstance(item.kind, str)
            or _REFERENCE_KIND_RE.fullmatch(item.kind) is None
        ):
            raise _snapshot_error("reference evidence kind is invalid")
        identity = _reference_identity(item.identity)
        if (
            not isinstance(item.sha256, str)
            or _LOWER_SHA256_RE.fullmatch(item.sha256) is None
        ):
            raise _snapshot_error("reference evidence sha256 is invalid")
        key = (item.kind, identity)
        previous = rows.get(key)
        if previous is not None and previous != item.sha256:
            raise _snapshot_error(
                "conflicting reference identity has multiple content hashes"
            )
        rows[key] = item.sha256
    return [
        {"kind": kind, "identity": identity, "sha256": rows[(kind, identity)]}
        for kind, identity in sorted(
            rows, key=lambda key: (key[0].encode("utf-8"), key[1].encode("utf-8"))
        )
    ]


def derive_store_identity(
    paths: ObservationPaths,
    semantics: AdapterSemantics,
) -> str | None:
    """Derive the selected store identity without hashing its local path."""

    if not isinstance(paths, ObservationPaths):
        raise _snapshot_error("observation paths have the wrong type")
    if not isinstance(semantics, AdapterSemantics):
        raise _snapshot_error("adapter semantics have the wrong type")
    try:
        metadata = os.stat(paths.root, follow_symlinks=False)
    except OSError as error:
        raise _snapshot_error(
            f"could not inspect snapshot store root: {error}",
            kind="io",
            cause=error,
        ) from error
    if not stat.S_ISDIR(metadata.st_mode):
        raise _snapshot_error("snapshot store root must be a directory")
    if metadata.st_dev == 0 or metadata.st_ino == 0:
        return None
    material = (
        _STORE_IDENTITY_DOMAIN
        + semantics.name.encode("utf-8")
        + b"\0"
        + str(metadata.st_dev).encode("ascii")
        + b"\0"
        + str(metadata.st_ino).encode("ascii")
    )
    return hashlib.sha256(material).hexdigest()


def _validate_policy_set(
    policy_set: PolicySet,
    semantics: AdapterSemantics,
) -> tuple[dict[str, dict[str, object]], dict[str, dict[str, str]]]:
    if not isinstance(policy_set, PolicySet):
        raise _snapshot_error("snapshot policy_set has the wrong type")
    documents = policy_set.documents
    identities = policy_set.core_identity()
    validate_policy_documents(
        documents, allow_reviewed_generation_mapping=True
    )
    for document_name, identity_name in _DOCUMENT_IDENTITIES.items():
        document = documents[document_name]
        identity = identities.get(identity_name)
        if not isinstance(identity, Mapping) or set(identity) != {
            "version", "sha256"
        }:
            raise _snapshot_error(
                f"snapshot {identity_name} identity does not have exact fields"
            )
        if (
            not isinstance(document, Mapping)
            or not isinstance(identity["version"], str)
            or not isinstance(identity["sha256"], str)
            or identity.get("version") != document.get("version")
            or identity.get("sha256")
            != "sha256:" + hashlib.sha256(canonicalize(document)).hexdigest()
        ):
            raise _snapshot_error(
                f"snapshot policy identity does not bind {document_name}"
            )
    expected_identity_names = set(_DOCUMENT_IDENTITIES.values()) | {
        "analyzer_artifact", "canonicalizer_artifact"
    }
    if set(identities) != expected_identity_names:
        raise _snapshot_error("snapshot policy identity set is incomplete")
    artifact_versions = {
        "analyzer_artifact": "workflow-learning-analyzer@0.2.0",
        "canonicalizer_artifact": "rfc8785-jcs@1",
    }
    for name, expected_version in artifact_versions.items():
        identity = identities[name]
        if (
            not isinstance(identity, Mapping)
            or set(identity) != {"version", "sha256"}
            or identity["version"] != expected_version
            or not isinstance(identity["sha256"], str)
            or not identity["sha256"].startswith("sha256:")
            or _LOWER_SHA256_RE.fullmatch(identity["sha256"][7:]) is None
        ):
            raise _snapshot_error(
                f"snapshot {name} identity version or digest is invalid"
            )
    projection = documents["episode_projection"]
    if projection.get("version") != semantics.projection_version:
        raise _snapshot_error(
            "selected adapter projection version does not match policy"
        )
    return documents, identities


def _generation_mapping(documents: Mapping[str, Mapping]) -> dict[str, str]:
    document = documents["workflow_generation_mapping"]
    mapping = document.get("mapping")
    if not isinstance(mapping, Mapping):
        raise _snapshot_error("workflow generation mapping must be an object")
    result: dict[str, str] = {}
    for run_id, generation in mapping.items():
        if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
            raise _snapshot_error("workflow generation mapping run_id is invalid")
        if (
            not isinstance(generation, str)
            or _GENERATION_RE.fullmatch(generation) is None
            or generation in {"unknown", "unavailable"}
        ):
            raise _snapshot_error(
                "workflow generation mapping value is invalid"
            )
        result[run_id] = generation
    return result


def _mapped_projection(
    projection: dict,
    generation_mapping: Mapping[str, str],
) -> dict:
    generation = generation_mapping.get(projection["run_id"])
    if generation is None:
        return projection
    current = projection.get("workflow_generation")
    if not isinstance(current, dict) or set(current) != {"availability", "value"}:
        raise _snapshot_error("canonical workflow generation projection is invalid")
    if current == {"availability": "unavailable", "value": None}:
        enriched = dict(projection)
        enriched["workflow_generation"] = {
            "availability": "observed",
            "value": generation,
        }
        return enriched
    if current != {"availability": "observed", "value": generation}:
        raise _snapshot_error(
            "workflow generation mapping conflicts with observed generation"
        )
    return projection


def _valid_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _LOWER_SHA256_RE.fullmatch(value) is None:
        raise _snapshot_error(f"{label} sha256 is invalid")
    return value


def _matches_query(projection: Mapping, query: SnapshotQuery) -> bool:
    started_at = _utc_instant(projection.get("started_at"), "Episode started_at")
    since = _utc_instant(
        query.interval["since_inclusive"], "interval since_inclusive"
    )
    until = _utc_instant(
        query.interval["until_exclusive"], "interval until_exclusive"
    )
    if not since <= started_at < until:
        return False
    return all(
        expected is None or projection.get(name) == expected
        for name, expected in (
            ("project", query.project),
            ("workspace", query.workspace),
            ("workspace_id", query.workspace_id),
            ("task_type", query.task_type),
        )
    )


def _query_document(query: SnapshotQuery) -> dict:
    return {
        "interval": deepcopy(query.interval),
        "lifecycle_as_of": query.lifecycle_as_of,
        "project": query.project,
        "workspace": query.workspace,
        "workspace_id": query.workspace_id,
        "task_type": query.task_type,
    }


def _acquire_snapshot_input(
    paths: ObservationPaths,
    semantics: AdapterSemantics,
    query: SnapshotQuery,
    policy_set: PolicySet,
) -> SnapshotInput:
    if not isinstance(semantics, AdapterSemantics):
        raise _snapshot_error("adapter semantics have the wrong type")
    validate_snapshot_query(query)
    documents, identities = _validate_policy_set(policy_set, semantics)
    generation_mapping = _generation_mapping(documents)
    store_identity = derive_store_identity(paths, semantics)
    collection = collect_record_documents(paths, semantics, strict_layout=True)
    if not isinstance(collection, ObservationCollection):
        raise _snapshot_error("observation collection has the wrong type")

    seen_physical: set[str] = set()
    all_run_ids: set[str] = set()
    projected: list[tuple[RecordDocument, dict]] = []
    for document in collection.records:
        if not isinstance(document, RecordDocument):
            raise _snapshot_error("observation collection record has the wrong type")
        if document.run_id in seen_physical:
            raise _snapshot_error(
                f"duplicate physical Episode for run_id {document.run_id}"
            )
        seen_physical.add(document.run_id)
        all_run_ids.add(document.run_id)
        projection = canonical_episode_projection(
            document.metadata, document.body, documents
        )
        if projection.get("run_id") != document.run_id:
            raise _snapshot_error("canonical Episode run_id conflicts with source")
        projection = _mapped_projection(projection, generation_mapping)
        _valid_sha256(document.source_sha256, "Episode source")
        projected.append((document, projection))

    selected = [
        (document, projection)
        for document, projection in projected
        if _matches_query(projection, query)
    ]
    selected.sort(key=lambda pair: pair[1]["run_id"].encode("utf-8"))
    selected_ids = {projection["run_id"] for _document, projection in selected}

    episodes = [
        {**deepcopy(projection), "source_sha256": document.source_sha256}
        for document, projection in selected
    ]
    references = canonical_reference_manifest(
        evidence
        for document, _projection in selected
        for evidence in document.references
    )

    invalidations = []
    invalidated_ids: set[str] = set()
    for evidence in collection.invalidations:
        if not isinstance(evidence, InvalidationEvidence):
            raise _snapshot_error("invalidation evidence has the wrong type")
        if evidence.run_id not in all_run_ids:
            raise _snapshot_error("invalidation points to no physical Episode")
        _utc_instant(evidence.timestamp, "invalidation timestamp")
        _valid_sha256(evidence.source_sha256, "invalidation source")
        if evidence.run_id not in selected_ids:
            continue
        if evidence.run_id in invalidated_ids:
            raise _snapshot_error("duplicate selected invalidation evidence")
        invalidated_ids.add(evidence.run_id)
        invalidations.append({
            "run_id": evidence.run_id,
            "source_sha256": evidence.source_sha256,
            "timestamp": evidence.timestamp,
        })
    invalidations.sort(key=lambda row: row["run_id"].encode("utf-8"))

    draft_count = sum(episode["status"] == "draft" for episode in episodes)
    final_count = len(episodes) - draft_count
    record_counts = {
        "selected_episode_n": len(episodes),
        "draft_episode_n": draft_count,
        "final_episode_n": final_count,
        "selected_invalidation_n": len(invalidations),
    }
    if draft_count + final_count != len(episodes):
        raise _snapshot_error("snapshot record counts are inconsistent")

    projection_document = documents["episode_projection"]
    capabilities = projection_document.get("schema_capabilities")
    if not isinstance(capabilities, Mapping):
        raise _snapshot_error("projection schema capabilities are invalid")
    bundle = {
        "schema_version": 1,
        "projection_version": semantics.projection_version,
        "query": _query_document(query),
        "lifecycle_as_of": query.lifecycle_as_of,
        "policy_set": deepcopy(identities),
        "schema_capabilities": deepcopy(dict(capabilities)),
        "record_counts": record_counts,
        "episodes": episodes,
        "invalidations": invalidations,
        "reference_manifest": references,
    }
    bundle["input_manifest_sha256"] = hash_canonical(_MANIFEST_DOMAIN, bundle)

    analyzer_sha256 = identities["analyzer_artifact"]["sha256"][7:]
    adapter = {
        "name": semantics.name,
        "implementation_version": _ADAPTER_IMPLEMENTATION_VERSION,
        "implementation_sha256": analyzer_sha256,
    }
    result = SnapshotInput(adapter, store_identity, bundle)
    result.manifest_bytes
    return result


def acquire_snapshot_input(
    paths: ObservationPaths,
    semantics: AdapterSemantics,
    query: SnapshotQuery,
    policy_set: PolicySet,
) -> SnapshotInput:
    """Validate, project, and hash one canonical selection under one adapter."""

    try:
        return _acquire_snapshot_input(paths, semantics, query, policy_set)
    except SnapshotInputError:
        raise
    except ObservationError as error:
        raise _snapshot_error(str(error), kind=error.kind, cause=error) from error
    except (PolicyError, EpisodeSchemaError, CanonicalizationError) as error:
        raise _snapshot_error(str(error), cause=error) from error
    except OSError as error:
        raise _snapshot_error(str(error), kind="io", cause=error) from error
