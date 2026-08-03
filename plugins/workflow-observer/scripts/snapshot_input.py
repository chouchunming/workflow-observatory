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
from types import MappingProxyType
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from canonical_json import CanonicalizationError, canonicalize, hash_canonical
from episode_schema import (
    EpisodeSchemaError,
    _validate_summary as _validate_decision_summary,
    canonical_episode_projection,
)
from policy_artifacts import PolicyError, PolicySet, validate_policy_documents
from store_config import AdapterSemantics
from wiki_observations import (
    FINAL_STATUSES,
    InvalidationEvidence,
    ObservationCollection,
    ObservationError,
    ObservationPaths,
    RecordDocument,
    ReferenceEvidence,
    StoreRootEvidence,
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
_TASK_REFERENCE_RE = re.compile(r"^\[\[[A-Za-z0-9][A-Za-z0-9._-]*\]\]$")
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
_CANONICAL_EPISODE_KEYS = {
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
    "source_sha256",
}
_POLICY_IDENTITY_VERSIONS = {
    "canonical_projection_contract": "episode-projection@2",
    "producer_capability_registry": "producer-capabilities@1",
    "workflow_generation_mapping": "workflow-generation-mapping@1",
    "metric_semantics_registry": "metric-semantics@1",
    "quantile_policy": "linear-rational-quantile@1",
    "decision_support_policy": "decision-pattern-support@1",
    "lifecycle_health_policy": "draft-staleness@1",
    "candidate_emission_policy": "candidate-emission@1",
    "analyzer_artifact": "workflow-learning-analyzer@0.2.0",
    "canonicalizer_artifact": "rfc8785-jcs@1",
}
_METRIC_NAMES = (
    "elapsed_seconds",
    "verification",
    "review_rounds",
    "defects_found",
    "rework_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cost_amount",
    "test_failures",
    "timeout_count",
)
_SCHEMA_CAPABILITIES = {
    "1": {
        "metrics": {
            name: name in {
                "elapsed_seconds",
                "verification",
                "review_rounds",
                "defects_found",
                "rework_count",
            }
            for name in _METRIC_NAMES
        },
        "decisions": False,
    },
    "2": {
        "metrics": {name: True for name in _METRIC_NAMES},
        "decisions": True,
    },
}
_INTEGER_METRICS = {
    "elapsed_seconds",
    "review_rounds",
    "defects_found",
    "rework_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "test_failures",
    "timeout_count",
}
_DECISION_ENUMERATIONS = {
    "phase": {"implementation", "planning", "recovery", "review", "verification"},
    "actor_role": {
        "coordinator",
        "implementer",
        "planner",
        "reviewer",
        "tester",
    },
    "decision_type": {
        "change-scope",
        "reject",
        "resume",
        "retry",
        "rollback",
        "split-task",
        "stop",
    },
    "reason_code": {
        "api-design",
        "complexity-threshold",
        "dependency",
        "integrity-risk",
        "test-failure",
        "timeout",
        "user-direction",
        "verification-failure",
    },
    "result": {"inconclusive", "rejected", "superseded", "supported"},
}
_DECISION_KEYS = {
    "sequence",
    "phase",
    "actor_role",
    "decision_type",
    "reason_code",
    "result",
    "summary",
}
_REVISION_RE = re.compile(r"^(?:[0-9a-f]{7,40}|unknown)$")
_COST_RE = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_MAX_SAFE_INTEGER = (2**53) - 1


class SnapshotInputError(ObservationError):
    """A snapshot acquisition error normalized by the observation CLI."""


def _deep_freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({
            key: _deep_freeze(item) for key, item in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _deep_thaw(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class SnapshotQuery:
    interval: dict
    lifecycle_as_of: str
    project: str | None
    workspace: str | None
    workspace_id: str | None
    task_type: str | None

    def __post_init__(self) -> None:
        interval = deepcopy(object.__getattribute__(self, "interval"))
        object.__setattr__(
            self,
            "interval",
            _deep_freeze(_validate_interval(interval)),
        )

    def __getattribute__(self, name: str):
        value = object.__getattribute__(self, name)
        if name == "interval":
            return _deep_thaw(value)
        return value


@dataclass(frozen=True)
class SnapshotInput:
    adapter: dict[str, str]
    store_identity: str | None
    semantic_bundle: dict

    def __post_init__(self) -> None:
        adapter, semantic_bundle, manifest_bytes = (
            _validated_snapshot_input_constructor(
                object.__getattribute__(self, "adapter"),
                object.__getattribute__(self, "store_identity"),
                object.__getattribute__(self, "semantic_bundle"),
            )
        )
        object.__setattr__(self, "adapter", _deep_freeze(adapter))
        object.__setattr__(
            self,
            "semantic_bundle",
            _deep_freeze(semantic_bundle),
        )
        object.__setattr__(self, "_manifest_bytes", manifest_bytes)

    def __getattribute__(self, name: str):
        value = object.__getattribute__(self, name)
        if name in {"adapter", "semantic_bundle"}:
            return _deep_thaw(value)
        return value

    @property
    def canonical_representation(self) -> dict:
        return {
            "adapter": deepcopy(self.adapter),
            "store_identity": self.store_identity,
            "semantic_bundle": deepcopy(self.semantic_bundle),
        }

    @property
    def manifest_bytes(self) -> bytes:
        return object.__getattribute__(self, "_manifest_bytes")


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


def _exact_mapping(
    value: object,
    expected_keys: set[str],
    label: str,
) -> dict:
    thawed = _deep_thaw(value)
    if not isinstance(thawed, dict) or set(thawed) != expected_keys:
        raise _snapshot_error(f"{label} does not have its exact fields")
    return thawed


def _snapshot_scalar(value: object, label: str) -> str:
    try:
        return _validate_scalar(value, label)
    except ObservationError as error:
        raise _snapshot_error(str(error), cause=error) from error


def _validated_schema_capabilities(value: object) -> dict:
    capabilities = _exact_mapping(
        value,
        set(_SCHEMA_CAPABILITIES),
        "snapshot schema_capabilities",
    )
    for version, expected in _SCHEMA_CAPABILITIES.items():
        row = _exact_mapping(
            capabilities[version],
            {"metrics", "decisions"},
            f"snapshot schema {version} capabilities",
        )
        metrics = _exact_mapping(
            row["metrics"],
            set(_METRIC_NAMES),
            f"snapshot schema {version} metric capabilities",
        )
        for name, expected_value in expected["metrics"].items():
            if (
                type(metrics[name]) is not bool
                or metrics[name] is not expected_value
            ):
                raise _snapshot_error(
                    f"snapshot schema {version} metric capability is invalid"
                )
        if (
            type(row["decisions"]) is not bool
            or row["decisions"] is not expected["decisions"]
        ):
            raise _snapshot_error(
                f"snapshot schema {version} decision capability is invalid"
            )
    return capabilities


def _validated_metric(
    raw_metric: object,
    name: str,
    *,
    supported: bool,
    status: str,
) -> dict:
    metric = _exact_mapping(
        raw_metric,
        {"availability", "value", "unit"},
        f"snapshot Episode metric {name}",
    )
    unavailable = {
        "availability": "unsupported_by_schema",
        "value": None,
        "unit": None,
    }
    not_recorded = {
        "availability": "not_recorded",
        "value": None,
        "unit": None,
    }
    if not supported:
        if metric != unavailable:
            raise _snapshot_error(
                f"snapshot Episode metric {name} conflicts with its schema"
            )
        return metric
    if status == "draft":
        if metric != not_recorded:
            raise _snapshot_error(
                f"draft snapshot Episode metric {name} must be not_recorded"
            )
        return metric
    if metric == not_recorded:
        if name in {"elapsed_seconds", "verification"}:
            raise _snapshot_error(
                f"final snapshot Episode metric {name} must be observed"
            )
        return metric
    if metric["availability"] != "observed":
        raise _snapshot_error(
            f"snapshot Episode metric {name} availability is invalid"
        )
    if name in _INTEGER_METRICS:
        if (
            type(metric["value"]) is not int
            or metric["value"] < 0
            or metric["value"] > _MAX_SAFE_INTEGER
            or metric["unit"] is not None
        ):
            raise _snapshot_error(
                f"snapshot Episode metric {name} value is invalid"
            )
    elif name == "verification":
        if (
            not isinstance(metric["value"], str)
            or metric["value"] not in {"pass", "fail", "not-run", "unknown"}
            or metric["unit"] is not None
        ):
            raise _snapshot_error(
                "snapshot Episode verification metric is invalid"
            )
    elif name == "cost_amount":
        if (
            not isinstance(metric["value"], str)
            or _COST_RE.fullmatch(metric["value"]) is None
            or not isinstance(metric["unit"], str)
            or _CURRENCY_RE.fullmatch(metric["unit"]) is None
        ):
            raise _snapshot_error(
                "snapshot Episode cost_amount metric is invalid"
            )
    else:
        raise _snapshot_error(f"snapshot Episode metric {name} is unknown")
    return metric


def _validated_decisions(
    value: object,
    *,
    supported: bool,
    status: str,
) -> list[dict]:
    if not isinstance(value, list):
        raise _snapshot_error("snapshot Episode decisions must be a list")
    if (not supported or status == "draft") and value:
        raise _snapshot_error(
            "snapshot Episode decisions conflict with its schema or status"
        )
    if len(value) > 12:
        raise _snapshot_error("snapshot Episode decisions exceed their bound")
    validated = []
    for index, raw_decision in enumerate(value, start=1):
        decision = _exact_mapping(
            raw_decision,
            _DECISION_KEYS,
            f"snapshot Episode decision {index}",
        )
        if type(decision["sequence"]) is not int or decision["sequence"] != index:
            raise _snapshot_error(
                "snapshot Episode decision sequence is not canonical"
            )
        for name, allowed in _DECISION_ENUMERATIONS.items():
            if (
                not isinstance(decision[name], str)
                or decision[name] not in allowed
            ):
                raise _snapshot_error(
                    f"snapshot Episode decision {name} is invalid"
                )
        try:
            _validate_decision_summary(decision["summary"], 200)
        except EpisodeSchemaError as error:
            raise _snapshot_error(str(error), cause=error) from error
        validated.append(decision)
    return validated


def _validated_episode(
    raw_episode: object,
    capabilities: Mapping[str, Mapping],
    query: SnapshotQuery,
) -> dict:
    episode = _exact_mapping(
        raw_episode,
        _CANONICAL_EPISODE_KEYS,
        "snapshot Episode",
    )
    run_id = episode["run_id"]
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise _snapshot_error("snapshot Episode run_id is invalid")
    schema_version = episode["episode_schema_version"]
    if type(schema_version) is not int or str(schema_version) not in capabilities:
        raise _snapshot_error("snapshot Episode schema version is invalid")

    started = _utc_instant(episode["started_at"], "snapshot Episode started_at")
    status = episode["status"]
    if not isinstance(status, str) or status not in FINAL_STATUSES | {"draft"}:
        raise _snapshot_error("snapshot Episode status is invalid")
    if status == "draft":
        if episode["finished_at"] is not None:
            raise _snapshot_error(
                "draft snapshot Episode finished_at must be null"
            )
        finished = None
    else:
        finished = _utc_instant(
            episode["finished_at"],
            "snapshot Episode finished_at",
        )
        if finished < started:
            raise _snapshot_error(
                "snapshot Episode finished_at precedes started_at"
            )

    for name in ("project", "workspace"):
        _snapshot_scalar(episode[name], f"snapshot Episode {name}")
    if (
        not isinstance(episode["workspace_id"], str)
        or _WORKSPACE_ID_RE.fullmatch(episode["workspace_id"]) is None
    ):
        raise _snapshot_error("snapshot Episode workspace_id is invalid")
    if (
        not isinstance(episode["revision"], str)
        or _REVISION_RE.fullmatch(episode["revision"]) is None
    ):
        raise _snapshot_error("snapshot Episode revision is invalid")
    if (
        not isinstance(episode["working_tree"], str)
        or episode["working_tree"] not in {"clean", "dirty", "unknown"}
    ):
        raise _snapshot_error("snapshot Episode working_tree is invalid")
    if episode["agent_surface"] != "codex":
        raise _snapshot_error("snapshot Episode agent_surface is invalid")
    task_type = episode["task_type"]
    workflow_variant = episode["workflow_variant"]
    if (
        not isinstance(task_type, str)
        or task_type not in TAXONOMY
        or not isinstance(workflow_variant, str)
        or workflow_variant not in TAXONOMY[task_type]
    ):
        raise _snapshot_error("snapshot Episode taxonomy is invalid")

    generation = _exact_mapping(
        episode["workflow_generation"],
        {"availability", "value"},
        "snapshot Episode workflow_generation",
    )
    if generation["availability"] == "unavailable":
        if generation["value"] is not None:
            raise _snapshot_error(
                "unavailable snapshot Episode generation must be null"
            )
    elif generation["availability"] == "observed":
        value = generation["value"]
        if (
            not isinstance(value, str)
            or _GENERATION_RE.fullmatch(value) is None
            or value in {"unknown", "unavailable"}
        ):
            raise _snapshot_error(
                "observed snapshot Episode generation is invalid"
            )
    else:
        raise _snapshot_error(
            "snapshot Episode generation availability is invalid"
        )

    schema_capability = capabilities[str(schema_version)]
    metrics = _exact_mapping(
        episode["metrics"],
        set(_METRIC_NAMES),
        "snapshot Episode metrics",
    )
    validated_metrics = {
        name: _validated_metric(
            metrics[name],
            name,
            supported=schema_capability["metrics"][name],
            status=status,
        )
        for name in _METRIC_NAMES
    }
    if finished is not None:
        elapsed = validated_metrics["elapsed_seconds"]["value"]
        if elapsed != int((finished - started).total_seconds()):
            raise _snapshot_error(
                "snapshot Episode elapsed_seconds conflicts with its lifecycle"
            )
        if (
            status == "success"
            and validated_metrics["verification"]["value"] == "fail"
        ):
            raise _snapshot_error(
                "successful snapshot Episode cannot have failed verification"
            )

    episode["decisions"] = _validated_decisions(
        episode["decisions"],
        supported=schema_capability["decisions"],
        status=status,
    )
    if episode["runtime_provenance"] is not None:
        raise _snapshot_error(
            "snapshot Episode runtime_provenance must be null"
        )
    _valid_sha256(episode["source_sha256"], "snapshot Episode source")
    if not _matches_query(episode, query):
        raise _snapshot_error("snapshot Episode does not match its query")
    episode["metrics"] = validated_metrics
    episode["workflow_generation"] = generation
    return episode


def _validated_reference_manifest(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise _snapshot_error("snapshot reference_manifest must be a list")
    evidence = []
    for raw_reference in value:
        reference = _exact_mapping(
            raw_reference,
            {"kind", "identity", "sha256"},
            "snapshot reference",
        )
        kind = reference["kind"]
        identity = reference["identity"]
        if not isinstance(kind, str) or kind not in {
            "source",
            "task",
            "supersession-target",
        }:
            raise _snapshot_error("snapshot reference kind is invalid")
        if not isinstance(identity, str):
            raise _snapshot_error("snapshot reference identity is invalid")
        if kind == "source" and not identity.startswith("raw/"):
            raise _snapshot_error("snapshot source identity must start with raw/")
        if (
            kind == "task"
            and _TASK_REFERENCE_RE.fullmatch(identity) is None
        ):
            raise _snapshot_error("snapshot task identity is invalid")
        if (
            kind == "supersession-target"
            and _RUN_ID_RE.fullmatch(identity) is None
        ):
            raise _snapshot_error(
                "snapshot supersession identity is invalid"
            )
        evidence.append(ReferenceEvidence(
            kind,
            _reference_identity(identity),
            _valid_sha256(reference["sha256"], "snapshot reference"),
        ))
    normalized = canonical_reference_manifest(evidence)
    if value != normalized:
        raise _snapshot_error("snapshot reference_manifest is not canonical")
    return normalized


def _validated_snapshot_input_constructor(
    adapter: object,
    store_identity: object,
    semantic_bundle: object,
) -> tuple[dict, dict, bytes]:
    adapter_copy = _exact_mapping(
        adapter,
        {"name", "implementation_version", "implementation_sha256"},
        "snapshot adapter",
    )
    if (
        adapter_copy["name"] not in {"portable", "llmwiki"}
        or adapter_copy["implementation_version"]
        != _ADAPTER_IMPLEMENTATION_VERSION
        or not isinstance(adapter_copy["implementation_sha256"], str)
        or _LOWER_SHA256_RE.fullmatch(
            adapter_copy["implementation_sha256"]
        )
        is None
    ):
        raise _snapshot_error("snapshot adapter is invalid")
    if store_identity is not None and (
        not isinstance(store_identity, str)
        or _LOWER_SHA256_RE.fullmatch(store_identity) is None
    ):
        raise _snapshot_error("snapshot store identity is invalid")

    bundle_copy = _exact_mapping(
        semantic_bundle,
        {
            "schema_version",
            "projection_version",
            "query",
            "lifecycle_as_of",
            "policy_set",
            "schema_capabilities",
            "record_counts",
            "episodes",
            "invalidations",
            "reference_manifest",
            "input_manifest_sha256",
        },
        "snapshot semantic bundle",
    )
    if (
        type(bundle_copy["schema_version"]) is not int
        or bundle_copy["schema_version"] != 1
        or bundle_copy["projection_version"] != "episode-projection@2"
    ):
        raise _snapshot_error("snapshot semantic bundle version is invalid")

    query_row = _exact_mapping(
        bundle_copy["query"],
        {
            "interval",
            "lifecycle_as_of",
            "project",
            "workspace",
            "workspace_id",
            "task_type",
        },
        "snapshot query",
    )
    query = SnapshotQuery(**query_row)
    validate_snapshot_query(query)
    if bundle_copy["lifecycle_as_of"] != query.lifecycle_as_of:
        raise _snapshot_error("snapshot lifecycle_as_of does not match its query")

    policy_set = bundle_copy["policy_set"]
    policy_set = _exact_mapping(
        policy_set,
        set(_POLICY_IDENTITY_VERSIONS),
        "snapshot policy identity set",
    )
    for name, raw_identity in policy_set.items():
        identity = _exact_mapping(
            raw_identity,
            {"version", "sha256"},
            f"snapshot {name} identity",
        )
        if (
            identity["version"] != _POLICY_IDENTITY_VERSIONS[name]
            or not isinstance(identity["sha256"], str)
            or not identity["sha256"].startswith("sha256:")
            or _LOWER_SHA256_RE.fullmatch(identity["sha256"][7:]) is None
        ):
            raise _snapshot_error(f"snapshot {name} identity is invalid")
    bundle_copy["policy_set"] = policy_set
    if (
        adapter_copy["implementation_sha256"]
        != policy_set["analyzer_artifact"]["sha256"][7:]
    ):
        raise _snapshot_error(
            "snapshot adapter is not bound to its analyzer artifact"
        )

    capabilities = _validated_schema_capabilities(
        bundle_copy["schema_capabilities"]
    )
    bundle_copy["schema_capabilities"] = capabilities

    episodes = bundle_copy["episodes"]
    invalidations = bundle_copy["invalidations"]
    if not isinstance(episodes, list):
        raise _snapshot_error("snapshot episodes must be a list")
    if not isinstance(invalidations, list):
        raise _snapshot_error("snapshot invalidations must be a list")
    validated_episodes = [
        _validated_episode(raw_episode, capabilities, query)
        for raw_episode in episodes
    ]
    episode_run_ids = [episode["run_id"] for episode in validated_episodes]
    if (
        episode_run_ids
        != sorted(episode_run_ids, key=lambda item: item.encode("utf-8"))
        or len(episode_run_ids) != len(set(episode_run_ids))
    ):
        raise _snapshot_error(
            "snapshot Episodes must have unique canonically sorted run_ids"
        )
    episode_ids = set(episode_run_ids)
    episodes_by_id = {
        episode["run_id"]: episode for episode in validated_episodes
    }
    bundle_copy["episodes"] = validated_episodes

    validated_invalidations = []
    for raw_invalidation in invalidations:
        invalidation = _exact_mapping(
            raw_invalidation,
            {"run_id", "source_sha256", "timestamp"},
            "snapshot invalidation",
        )
        if (
            not isinstance(invalidation["run_id"], str)
            or _RUN_ID_RE.fullmatch(invalidation["run_id"]) is None
        ):
            raise _snapshot_error("snapshot invalidation run_id is invalid")
        _valid_sha256(
            invalidation["source_sha256"], "snapshot invalidation source"
        )
        _utc_instant(invalidation["timestamp"], "snapshot invalidation timestamp")
        if invalidation["run_id"] not in episode_ids:
            raise _snapshot_error(
                "snapshot invalidation points to no selected Episode"
            )
        if episodes_by_id[invalidation["run_id"]]["status"] == "draft":
            raise _snapshot_error(
                "snapshot invalidation cannot target a draft Episode"
            )
        validated_invalidations.append(invalidation)
    invalidation_run_ids = [
        invalidation["run_id"] for invalidation in validated_invalidations
    ]
    if (
        invalidation_run_ids
        != sorted(invalidation_run_ids, key=lambda item: item.encode("utf-8"))
        or len(invalidation_run_ids) != len(set(invalidation_run_ids))
    ):
        raise _snapshot_error(
            "snapshot invalidations must have unique canonically sorted run_ids"
        )
    bundle_copy["invalidations"] = validated_invalidations
    bundle_copy["reference_manifest"] = _validated_reference_manifest(
        bundle_copy["reference_manifest"]
    )

    counts = _exact_mapping(
        bundle_copy["record_counts"],
        {
            "selected_episode_n",
            "draft_episode_n",
            "final_episode_n",
            "selected_invalidation_n",
        },
        "snapshot record_counts",
    )
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise _snapshot_error("snapshot record_counts are invalid")
    if (
        counts["selected_episode_n"] != len(validated_episodes)
        or counts["selected_invalidation_n"] != len(validated_invalidations)
        or counts["draft_episode_n"]
        != sum(episode["status"] == "draft" for episode in validated_episodes)
        or counts["final_episode_n"]
        != sum(episode["status"] != "draft" for episode in validated_episodes)
    ):
        raise _snapshot_error("snapshot record_counts are inconsistent")
    bundle_copy["record_counts"] = counts

    actual_digest = bundle_copy["input_manifest_sha256"]
    if (
        not isinstance(actual_digest, str)
        or _LOWER_SHA256_RE.fullmatch(actual_digest) is None
    ):
        raise _snapshot_error("snapshot input manifest digest is invalid")
    digest_material = dict(bundle_copy)
    digest_material.pop("input_manifest_sha256")
    try:
        expected_digest = hash_canonical(_MANIFEST_DOMAIN, digest_material)
    except CanonicalizationError as error:
        raise _snapshot_error(
            "snapshot semantic bundle is not canonical I-JSON",
            cause=error,
        ) from error
    if actual_digest != expected_digest:
        raise _snapshot_error("snapshot input manifest digest is stale")

    representation = {
        "adapter": adapter_copy,
        "store_identity": store_identity,
        "semantic_bundle": bundle_copy,
    }
    try:
        manifest_bytes = canonicalize(representation)
    except CanonicalizationError as error:
        raise _snapshot_error(
            "snapshot manifest is not canonical I-JSON",
            cause=error,
        ) from error
    return adapter_copy, bundle_copy, manifest_bytes


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
    return _derive_store_identity_from_evidence(
        StoreRootEvidence(metadata.st_dev, metadata.st_ino),
        semantics,
    )


def _derive_store_identity_from_evidence(
    root_evidence: StoreRootEvidence,
    semantics: AdapterSemantics,
) -> str | None:
    if not isinstance(root_evidence, StoreRootEvidence):
        raise _snapshot_error("snapshot store root evidence is missing")
    if not isinstance(semantics, AdapterSemantics):
        raise _snapshot_error("adapter semantics have the wrong type")
    if (
        type(root_evidence.device) is not int
        or type(root_evidence.inode) is not int
        or root_evidence.device < 0
        or root_evidence.inode < 0
    ):
        raise _snapshot_error("snapshot store root evidence is invalid")
    if root_evidence.device == 0 or root_evidence.inode == 0:
        return None
    material = (
        _STORE_IDENTITY_DOMAIN
        + semantics.name.encode("utf-8")
        + b"\0"
        + str(root_evidence.device).encode("ascii")
        + b"\0"
        + str(root_evidence.inode).encode("ascii")
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
    collection = collect_record_documents(paths, semantics, strict_layout=True)
    if not isinstance(collection, ObservationCollection):
        raise _snapshot_error("observation collection has the wrong type")
    store_identity = _derive_store_identity_from_evidence(
        collection.root_evidence,
        semantics,
    )

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
