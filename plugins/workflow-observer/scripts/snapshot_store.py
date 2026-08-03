"""Stable publication boundary for deterministic workflow learning snapshots."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
from fractions import Fraction
import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
import time

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)
from learning_snapshot import (
    LearningSnapshotError,
    build_snapshot_core,
    candidate_id,
)
from policy_artifacts import (
    PolicyError,
    PolicySet,
    validate_relative_posix_artifact_path,
    validate_policy_documents,
)
from snapshot_input import (
    SnapshotInput,
    SnapshotQuery,
    validate_snapshot_query,
)
from wiki_observations import ObservationError, _validate_scalar


_SNAPSHOT_CORE_DOMAIN = b"workflow-observatory:learning-snapshot-core:v1\0"
_ADAPTER_IMPLEMENTATION_VERSION = "workflow-observer-snapshot-adapter@1"
_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PREFIXED_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_RUN_ID_RE = re.compile(r"^obs-[0-9]{8}-[0-9]{6}-[0-9a-f]{6}$")
_WORKSPACE_ID_RE = re.compile(r"^[0-9a-f]{12}$")
_TASK_REFERENCE_RE = re.compile(r"^\[\[[A-Za-z0-9][A-Za-z0-9._-]*\]\]$")
_UTC_INSTANT_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
)
_MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
_ARTIFACT_KEYS = {
    "artifact_type",
    "authoritative",
    "generated_at",
    "store_identity",
    "adapter",
    "snapshot_id",
    "core",
    "artifact_sha256",
}
_ADAPTER_KEYS = {
    "name",
    "implementation_version",
    "implementation_sha256",
}
_CORE_KEYS = {
    "schema_version",
    "analyzer_version",
    "query",
    "lifecycle_health_policy",
    "analysis_policy_set",
    "input_manifest",
    "exclusion_ledger",
    "cohorts",
    "decision_patterns",
    "candidates",
}
_POLICY_IDENTITY_NAMES = {
    "analyzer_artifact",
    "candidate_emission_policy",
    "canonical_projection_contract",
    "canonicalizer_artifact",
    "decision_support_policy",
    "lifecycle_health_policy",
    "metric_semantics_registry",
    "producer_capability_registry",
    "quantile_policy",
    "workflow_generation_mapping",
}
_COHORT_IDENTITY_KEYS = {
    "collection",
    "legacy_collection_id",
    "project",
    "workspace",
    "workspace_id",
    "task_type",
    "workflow_variant",
    "workflow_generation",
}
_MISSINGNESS_KEYS = {
    "eligible_episode_n",
    "not_applicable_n",
    "not_recorded_n",
    "observed_n",
    "unsupported_by_schema_n",
}
_METRIC_NAMES = {
    "cache_read_tokens",
    "cost_amount",
    "defects_found",
    "elapsed_seconds",
    "input_tokens",
    "output_tokens",
    "review_rounds",
    "rework_count",
    "test_failures",
    "timeout_count",
    "verification",
}
_METRIC_SEMANTICS = {
    "cache_read_tokens": (
        "measured-token-count@1", "nonnegative-integer", "integer-quantiles"
    ),
    "cost_amount": (
        "measured-cost@1", "normalized-decimal-string", "missingness-only"
    ),
    "defects_found": (
        "confirmed-defect@1", "nonnegative-integer", "integer-quantiles"
    ),
    "elapsed_seconds": (
        "wall-clock-elapsed@1", "nonnegative-integer", "integer-quantiles"
    ),
    "input_tokens": (
        "measured-token-count@1", "nonnegative-integer", "integer-quantiles"
    ),
    "output_tokens": (
        "measured-token-count@1", "nonnegative-integer", "integer-quantiles"
    ),
    "review_rounds": (
        "formal-review-cycle@1", "nonnegative-integer", "integer-quantiles"
    ),
    "rework_count": (
        "confirmed-rework@1", "nonnegative-integer", "integer-quantiles"
    ),
    "test_failures": (
        "confirmed-test-failure@1", "nonnegative-integer", "integer-quantiles"
    ),
    "timeout_count": (
        "confirmed-timeout@1", "nonnegative-integer", "integer-quantiles"
    ),
    "verification": (
        "verification-result@1", "enum", "category-counts"
    ),
}
_POLICY_VERSIONS = {
    "analyzer_artifact": "workflow-learning-analyzer@0.2.0",
    "candidate_emission_policy": "candidate-emission@1",
    "canonical_projection_contract": "episode-projection@2",
    "canonicalizer_artifact": "rfc8785-jcs@1",
    "decision_support_policy": "decision-pattern-support@1",
    "lifecycle_health_policy": "draft-staleness@1",
    "metric_semantics_registry": "metric-semantics@1",
    "producer_capability_registry": "producer-capabilities@1",
    "quantile_policy": "linear-rational-quantile@1",
    "workflow_generation_mapping": "workflow-generation-mapping@1",
}
_POLICY_DOCUMENT_IDENTITIES = {
    "episode_projection": "canonical_projection_contract",
    "producer_capabilities": "producer_capability_registry",
    "workflow_generation_mapping": "workflow_generation_mapping",
    "metric_semantics": "metric_semantics_registry",
    "quantile_policy": "quantile_policy",
    "decision_support_policy": "decision_support_policy",
    "lifecycle_health_policy": "lifecycle_health_policy",
    "candidate_emission_policy": "candidate_emission_policy",
}
_DECISION_ENUMERATIONS = {
    "phase": {"implementation", "planning", "recovery", "review", "verification"},
    "actor_role": {"coordinator", "implementer", "planner", "reviewer", "tester"},
    "decision_type": {
        "change-scope", "reject", "resume", "retry", "rollback", "split-task", "stop"
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


def _candidate_rule(
    candidate_class: str,
    source_kind: str,
    source: str | None,
    evidence: set[str],
    policies: set[str],
    counts: set[str] | None = None,
) -> tuple[str, str, str | None, frozenset[str], frozenset[str], frozenset[str] | None]:
    return (
        candidate_class,
        source_kind,
        source,
        frozenset(evidence),
        frozenset(policies),
        None if counts is None else frozenset(counts),
    )


_METRIC_DISTRIBUTION_POLICIES = {
    "candidate_emission_policy",
    "canonical_projection_contract",
    "metric_semantics_registry",
    "quantile_policy",
}
_CANDIDATE_RULES = {
    "cache-read-token-distribution": _candidate_rule(
        "efficiency", "metric", "cache_read_tokens",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "decision-adjacent-pair": _candidate_rule(
        "decision-pattern", "decision", "contiguous-adjacent-pair",
        {"counts", "pattern"},
        {"candidate_emission_policy", "canonical_projection_contract", "decision_support_policy"},
        {"event_count", "episode_count_with_event"},
    ),
    "decision-single-event": _candidate_rule(
        "decision-pattern", "decision", "single-event",
        {"counts", "pattern"},
        {"candidate_emission_policy", "canonical_projection_contract", "decision_support_policy"},
        {"event_count", "episode_count_with_event"},
    ),
    "defect-observed": _candidate_rule(
        "quality", "metric", "defects_found",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "elapsed-time-distribution": _candidate_rule(
        "efficiency", "metric", "elapsed_seconds",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "generation-unavailable": _candidate_rule(
        "lifecycle-health", "lifecycle", "generation_unavailable_episode_n",
        {"counts"},
        {"candidate_emission_policy", "canonical_projection_contract", "producer_capability_registry", "workflow_generation_mapping"},
        {"generation_unavailable_episode_n"},
    ),
    "input-token-distribution": _candidate_rule(
        "efficiency", "metric", "input_tokens",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "invalidated-episodes": _candidate_rule(
        "lifecycle-health", "lifecycle", "invalidated_episode_n", {"counts"},
        {"candidate_emission_policy", "canonical_projection_contract"},
        {"invalidated_episode_n"},
    ),
    "metric-missingness": _candidate_rule(
        "lifecycle-health", "metric", None, {"missingness"},
        {"candidate_emission_policy", "canonical_projection_contract", "metric_semantics_registry"},
    ),
    "non-success-outcomes": _candidate_rule(
        "outcome-reliability", "outcome", "non_success_outcome_n", {"counts"},
        {"candidate_emission_policy", "canonical_projection_contract"},
        {"non_success_outcome_n"},
    ),
    "output-token-distribution": _candidate_rule(
        "efficiency", "metric", "output_tokens",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "review-round-distribution": _candidate_rule(
        "efficiency", "metric", "review_rounds",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "rework-observed": _candidate_rule(
        "quality", "metric", "rework_count",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "schema-adoption-gap": _candidate_rule(
        "lifecycle-health", "metric", None, {"missingness"},
        {"candidate_emission_policy", "canonical_projection_contract", "metric_semantics_registry"},
    ),
    "stale-drafts": _candidate_rule(
        "lifecycle-health", "lifecycle", "stale_draft_n", {"counts"},
        {"candidate_emission_policy", "canonical_projection_contract", "lifecycle_health_policy"},
        {"stale_draft_n", "as_of", "stale_after_seconds"},
    ),
    "test-failure-observed": _candidate_rule(
        "quality", "metric", "test_failures",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "timeout-observed": _candidate_rule(
        "outcome-reliability", "metric", "timeout_count",
        {"missingness", "observed_values", "quantiles"},
        _METRIC_DISTRIBUTION_POLICIES,
    ),
    "verification-non-pass": _candidate_rule(
        "quality", "metric", "verification", {"category_counts", "missingness"},
        {"candidate_emission_policy", "canonical_projection_contract", "metric_semantics_registry"},
    ),
}


class SnapshotPublicationError(ObservationError):
    """A normalized stable-read, artifact, or publication error."""


class _SnapshotTargetChanged(SnapshotPublicationError):
    pass


@dataclass(frozen=True)
class PublishedSnapshot:
    snapshot_id: str
    path: Path
    artifact: Mapping
    created: bool


def validate_learning_artifact(
    artifact: Mapping, policy_set: PolicySet
) -> None:
    if not isinstance(artifact, Mapping) or set(artifact) != _ARTIFACT_KEYS:
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact has wrong fields"
        )
    if artifact["artifact_type"] != "learning-snapshot":
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact type is invalid"
        )
    if artifact["authoritative"] is not False:
        raise SnapshotPublicationError(
            "validation", "learning snapshot cannot be authoritative"
        )
    _utc_instant(artifact["generated_at"], "generated_at")
    store_identity = artifact["store_identity"]
    if store_identity is not None and not _lower_sha256(store_identity):
        raise SnapshotPublicationError(
            "validation", "learning snapshot store identity is invalid"
        )
    adapter = artifact["adapter"]
    if not isinstance(adapter, Mapping) or set(adapter) != _ADAPTER_KEYS:
        raise SnapshotPublicationError(
            "validation", "learning snapshot adapter identity is invalid"
        )
    if (
        adapter["name"] not in {"portable", "llmwiki"}
        or adapter["implementation_version"]
        != _ADAPTER_IMPLEMENTATION_VERSION
        or not _lower_sha256(adapter["implementation_sha256"])
    ):
        raise SnapshotPublicationError(
            "validation", "learning snapshot adapter identity is invalid"
        )
    core = artifact["core"]
    if not isinstance(core, Mapping) or set(core) != _CORE_KEYS:
        raise SnapshotPublicationError(
            "validation", "learning snapshot core has wrong fields"
        )
    if not _lower_sha256(artifact["snapshot_id"]):
        raise SnapshotPublicationError(
            "validation", "learning snapshot identity is invalid"
        )
    if not _lower_sha256(artifact["artifact_sha256"]):
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact digest is invalid"
        )
    try:
        recomputed_snapshot_id = hash_canonical(_SNAPSHOT_CORE_DOMAIN, dict(core))
        without_digest = dict(artifact)
        del without_digest["artifact_sha256"]
        recomputed_artifact_sha256 = hashlib.sha256(
            canonicalize(without_digest)
        ).hexdigest()
    except CanonicalizationError as error:
        raise SnapshotPublicationError(
            "validation", f"learning snapshot is not canonicalizable: {error}"
        ) from error
    if artifact["snapshot_id"] != recomputed_snapshot_id:
        raise SnapshotPublicationError(
            "validation", "snapshot identity mismatch"
        )
    if artifact["artifact_sha256"] != recomputed_artifact_sha256:
        raise SnapshotPublicationError(
            "validation", "artifact digest mismatch"
        )
    _validate_snapshot_core(core, adapter, policy_set)


def validate_learning_artifact_bytes(
    raw: bytes,
    policy_set: PolicySet,
    *,
    expected_snapshot_id: str | None = None,
) -> Mapping:
    if not isinstance(raw, bytes):
        raise SnapshotPublicationError(
            "validation", "learning snapshot bytes must be bytes"
        )
    if len(raw) > _MAX_ARTIFACT_BYTES:
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact is too large"
        )
    try:
        parsed = strict_json_loads(raw)
        canonical = canonicalize(parsed)
    except CanonicalizationError as error:
        raise SnapshotPublicationError("validation", str(error)) from error
    if raw != canonical:
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact is not canonical JCS"
        )
    if not isinstance(parsed, Mapping):
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact must be an object"
        )
    validate_learning_artifact(parsed, policy_set)
    recomputed_snapshot_id = hash_canonical(
        _SNAPSHOT_CORE_DOMAIN, parsed["core"]
    )
    if (
        expected_snapshot_id is not None
        and expected_snapshot_id != recomputed_snapshot_id
    ):
        raise SnapshotPublicationError(
            "validation", "snapshot filename mismatch"
        )
    return deepcopy(dict(parsed))


def _core_error(label: str) -> None:
    raise SnapshotPublicationError(
        "validation", f"learning snapshot core structure is invalid: {label}"
    )


def _exact_mapping(value: object, keys: set[str], label: str) -> Mapping:
    if not isinstance(value, Mapping) or set(value) != keys:
        _core_error(f"{label} fields")
    return value


def _array(value: object, label: str) -> list:
    if not isinstance(value, list):
        _core_error(f"{label} must be an array")
    return value


def _text(
    value: object,
    label: str,
    *,
    nullable: bool = False,
    maximum: int = 200,
) -> str | None:
    if nullable and value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(ord(character) < 0x20 for character in value)
        or value.startswith(("/", "~/"))
        or re.match(r"^[A-Za-z]:[\\/]", value) is not None
    ):
        _core_error(f"{label} text")
    try:
        _validate_scalar(value, label)
    except ObservationError as error:
        raise SnapshotPublicationError(
            "validation",
            f"learning snapshot core structure is invalid: {error}",
        ) from error
    return value


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= (2**53) - 1:
        _core_error(f"{label} count")
    return value


def _policy_identity(value: object, label: str) -> Mapping:
    row = _exact_mapping(value, {"version", "sha256"}, label)
    _text(row["version"], f"{label} version", maximum=200)
    if (
        not isinstance(row["sha256"], str)
        or _PREFIXED_SHA256_RE.fullmatch(row["sha256"]) is None
    ):
        _core_error(f"{label} digest")
    return row


def _validate_policy_identities(value: object) -> Mapping:
    identities = _exact_mapping(
        value, _POLICY_IDENTITY_NAMES, "analysis_policy_set"
    )
    for name, row in identities.items():
        validated = _policy_identity(row, f"analysis policy {name}")
        if validated["version"] != _POLICY_VERSIONS[name]:
            _core_error(f"analysis policy {name} version")
    return identities


def _validate_query(value: object) -> Mapping:
    query = _exact_mapping(
        value,
        {
            "interval",
            "lifecycle_as_of",
            "project",
            "workspace",
            "workspace_id",
            "task_type",
        },
        "query",
    )
    try:
        validated = SnapshotQuery(
            interval=deepcopy(query["interval"]),
            lifecycle_as_of=query["lifecycle_as_of"],
            project=query["project"],
            workspace=query["workspace"],
            workspace_id=query["workspace_id"],
            task_type=query["task_type"],
        )
        validate_snapshot_query(validated)
    except ObservationError as error:
        _core_error(f"query: {error}")
    return query


def _validate_input_manifest(value: object) -> None:
    manifest = _exact_mapping(
        value,
        {
            "input_manifest_sha256",
            "episodes",
            "invalidations",
            "reference_manifest",
        },
        "input_manifest",
    )
    if not _lower_sha256(manifest["input_manifest_sha256"]):
        _core_error("input manifest digest")
    seen_runs = set()
    for raw in _array(manifest["episodes"], "input manifest episodes"):
        row = _exact_mapping(raw, {"run_id", "source_sha256"}, "episode evidence")
        if (
            not isinstance(row["run_id"], str)
            or _RUN_ID_RE.fullmatch(row["run_id"]) is None
            or row["run_id"] in seen_runs
            or not _lower_sha256(row["source_sha256"])
        ):
            _core_error("episode evidence identity")
        seen_runs.add(row["run_id"])
    episode_rows = manifest["episodes"]
    if [row["run_id"] for row in episode_rows] != sorted(seen_runs):
        _core_error("episode evidence order")
    invalidation_keys = []
    for raw in _array(
        manifest["invalidations"], "input manifest invalidations"
    ):
        row = _exact_mapping(
            raw, {"run_id", "source_sha256", "timestamp"}, "invalidation evidence"
        )
        if (
            not isinstance(row["run_id"], str)
            or _RUN_ID_RE.fullmatch(row["run_id"]) is None
            or not _lower_sha256(row["source_sha256"])
        ):
            _core_error("invalidation evidence identity")
        _utc_instant(row["timestamp"], "invalidation timestamp")
        invalidation_keys.append(row["run_id"])
    if invalidation_keys != sorted(set(invalidation_keys)):
        _core_error("invalidation evidence order")
    reference_keys = []
    for raw in _array(
        manifest["reference_manifest"], "input manifest references"
    ):
        row = _exact_mapping(
            raw, {"kind", "identity", "sha256"}, "reference evidence"
        )
        if row["kind"] not in {"source", "task", "supersession-target"}:
            _core_error("reference evidence kind")
        identity = row["identity"]
        if row["kind"] == "source":
            try:
                normalized = validate_relative_posix_artifact_path(identity)
            except PolicyError as error:
                _core_error(f"reference evidence identity: {error}")
            if normalized != identity or not identity.startswith("raw/"):
                _core_error("reference source identity")
        elif row["kind"] == "task":
            if (
                not isinstance(identity, str)
                or _TASK_REFERENCE_RE.fullmatch(identity) is None
            ):
                _core_error("reference task identity")
        elif (
            not isinstance(identity, str)
            or _RUN_ID_RE.fullmatch(identity) is None
        ):
            _core_error("reference supersession identity")
        if not _lower_sha256(row["sha256"]):
            _core_error("reference evidence digest")
        reference_keys.append((row["kind"], identity))
    if reference_keys != sorted(set(reference_keys)):
        _core_error("reference evidence order")


def _validate_cohort_identity(value: object, label: str) -> Mapping:
    cohort = _exact_mapping(value, _COHORT_IDENTITY_KEYS, label)
    if cohort["collection"] not in {
        "workflow-generation", "legacy-generation-unavailable"
    }:
        _core_error(f"{label} collection")
    for name in ("project", "workspace", "task_type", "workflow_variant"):
        _text(cohort[name], f"{label} {name}", nullable=True, maximum=200)
    workspace_id = cohort["workspace_id"]
    if workspace_id is not None and (
        not isinstance(workspace_id, str)
        or _WORKSPACE_ID_RE.fullmatch(workspace_id) is None
    ):
        _core_error(f"{label} workspace_id")
    _text(
        cohort["workflow_generation"],
        f"{label} workflow_generation",
        nullable=True,
        maximum=200,
    )
    legacy = cohort["legacy_collection_id"]
    if legacy is not None and (
        not isinstance(legacy, str) or _RUN_ID_RE.fullmatch(legacy) is None
    ):
        _core_error(f"{label} legacy collection")
    if (
        cohort["collection"] == "workflow-generation"
        and (cohort["workflow_generation"] is None or legacy is not None)
    ) or (
        cohort["collection"] == "legacy-generation-unavailable"
        and (cohort["workflow_generation"] is not None or legacy is None)
    ):
        _core_error(f"{label} collection identity")
    return cohort


def _cohort_order_key(cohort: Mapping) -> tuple[bytes, ...]:
    terminal_identity = (
        cohort["workflow_generation"]
        if cohort["collection"] == "workflow-generation"
        else cohort["legacy_collection_id"]
    )
    values = (
        cohort["collection"],
        cohort["project"],
        cohort["workspace"],
        cohort["workspace_id"],
        cohort["task_type"],
        cohort["workflow_variant"],
        terminal_identity,
    )
    return tuple(
        b"\x00" if value is None else b"\x01" + value.encode("utf-8")
        for value in values
    )


def _validate_missingness(value: object, label: str) -> None:
    row = _exact_mapping(value, _MISSINGNESS_KEYS, label)
    for name, count in row.items():
        _nonnegative(count, f"{label} {name}")


def _validate_metric(value: object) -> None:
    metric = _exact_mapping(
        value,
        {
            "metric",
            "semantics_id",
            "value_type",
            "aggregation",
            "missingness",
            "observed_values",
            "category_counts",
            "quantiles",
        },
        "metric",
    )
    if metric["metric"] not in _METRIC_NAMES:
        _core_error("metric identity")
    if (
        metric["semantics_id"],
        metric["value_type"],
        metric["aggregation"],
    ) != _METRIC_SEMANTICS[metric["metric"]]:
        _core_error("metric semantics")
    _validate_missingness(metric["missingness"], "metric missingness")
    observed = metric["observed_values"]
    if observed is not None:
        for item in _array(observed, "metric observed_values"):
            if type(item) is int:
                _nonnegative(item, "metric observed value")
            else:
                _text(item, "metric observed value", maximum=200)
    categories = metric["category_counts"]
    if categories is not None:
        categories = _exact_mapping(
            categories, {"fail", "not-run", "pass", "unknown"}, "category counts"
        )
        for name, count in categories.items():
            _nonnegative(count, f"category {name}")
    quantiles = metric["quantiles"]
    if quantiles is not None:
        quantiles = _exact_mapping(
            quantiles, {"p25", "p50", "p75"}, "metric quantiles"
        )
        for name, value in quantiles.items():
            _text(value, f"quantile {name}", nullable=True, maximum=200)


def _validate_cohort(value: object) -> None:
    keys = _COHORT_IDENTITY_KEYS | {
        "comparative_inference_eligible",
        "comparative_inference_exclusions",
        "evidence_strength",
        "outcome_episode_n",
        "outcome_counts",
        "draft_episode_n",
        "active_draft_n",
        "stale_draft_n",
        "superseded_episode_n",
        "invalidated_episode_n",
        "generation_unavailable_episode_n",
        "metrics",
    }
    cohort = _exact_mapping(value, keys, "cohort")
    _validate_cohort_identity(
        {name: cohort[name] for name in _COHORT_IDENTITY_KEYS}, "cohort identity"
    )
    if type(cohort["comparative_inference_eligible"]) is not bool:
        _core_error("cohort comparative eligibility")
    exclusions = _array(
        cohort["comparative_inference_exclusions"], "cohort exclusions"
    )
    if any(item != "generation-unavailable" for item in exclusions):
        _core_error("cohort exclusion")
    if cohort["evidence_strength"] not in {"descriptive", "recurring"}:
        _core_error("cohort evidence strength")
    for name in (
        "outcome_episode_n",
        "draft_episode_n",
        "active_draft_n",
        "stale_draft_n",
        "superseded_episode_n",
        "invalidated_episode_n",
        "generation_unavailable_episode_n",
    ):
        _nonnegative(cohort[name], f"cohort {name}")
    outcomes = _exact_mapping(
        cohort["outcome_counts"],
        {"failed", "partial", "rolled-back", "success"},
        "cohort outcome_counts",
    )
    for name, count in outcomes.items():
        _nonnegative(count, f"cohort outcome {name}")
    metrics = _array(cohort["metrics"], "cohort metrics")
    for metric in metrics:
        _validate_metric(metric)
    if {metric["metric"] for metric in metrics} != _METRIC_NAMES:
        _core_error("cohort metric set")
    if [metric["metric"] for metric in metrics] != sorted(_METRIC_NAMES):
        _core_error("cohort metric order")


def _validate_event(value: object, label: str) -> None:
    event = _exact_mapping(
        value,
        {"phase", "actor_role", "decision_type", "reason_code", "result"},
        label,
    )
    for name, item in event.items():
        if item not in _DECISION_ENUMERATIONS[name]:
            _core_error(f"{label} {name}")


def _validate_pattern(value: object) -> Mapping:
    row = _exact_mapping(
        value,
        {
            "cohort",
            "pattern_kind",
            "pattern",
            "event_count",
            "episode_count_with_event",
            "eligible_episode_n",
            "support_fraction",
            "evidence_strength",
        },
        "decision pattern",
    )
    _validate_cohort_identity(row["cohort"], "decision pattern cohort")
    if row["pattern_kind"] not in {
        "single-event", "contiguous-adjacent-pair"
    }:
        _core_error("decision pattern kind")
    events = _array(row["pattern"], "decision pattern events")
    expected_length = (
        1 if row["pattern_kind"] == "single-event" else 2
    )
    if len(events) != expected_length:
        _core_error("decision pattern kind and length binding")
    for event in events:
        _validate_event(event, "decision pattern event")
    event_count = _nonnegative(
        row["event_count"], "decision pattern event_count"
    )
    supporting_n = _nonnegative(
        row["episode_count_with_event"],
        "decision pattern episode_count_with_event",
    )
    eligible_n = _nonnegative(
        row["eligible_episode_n"], "decision pattern eligible_episode_n"
    )
    if not 1 <= supporting_n <= event_count or supporting_n > eligible_n:
        _core_error("decision pattern count binding")
    support = _exact_mapping(
        row["support_fraction"], {"numerator", "denominator"}, "support fraction"
    )
    numerator = _nonnegative(support["numerator"], "support numerator")
    denominator = _nonnegative(support["denominator"], "support denominator")
    if numerator != supporting_n or denominator != eligible_n:
        _core_error("decision pattern support binding")
    if row["evidence_strength"] not in {"descriptive", "recurring"}:
        _core_error("decision pattern evidence strength")
    return row


def _validate_candidate(value: object, identities: Mapping) -> Mapping:
    row = _exact_mapping(
        value,
        {
            "candidate_id",
            "candidate_type",
            "class",
            "cohort",
            "source",
            "policy_identities",
            "denominators",
            "evidence",
            "evidence_strength",
        },
        "candidate",
    )
    if not _lower_sha256(row["candidate_id"]):
        _core_error("candidate identity")
    _text(row["candidate_type"], "candidate type", maximum=200)
    rule = _CANDIDATE_RULES.get(row["candidate_type"])
    if rule is None:
        _core_error("candidate type")
    (
        expected_class,
        expected_source_kind,
        expected_source,
        expected_evidence,
        expected_policies,
        expected_counts,
    ) = rule
    if row["class"] != expected_class:
        _core_error("candidate class")
    _validate_cohort_identity(row["cohort"], "candidate cohort")
    source = _exact_mapping(
        row["source"], {"kind", "identity", "semantics_id"}, "candidate source"
    )
    if source["kind"] != expected_source_kind:
        _core_error("candidate source kind")
    if expected_source is None:
        if source["identity"] not in _METRIC_NAMES:
            _core_error("candidate source identity")
    elif source["identity"] != expected_source:
        _core_error("candidate source identity")
    expected_semantics = (
        _METRIC_SEMANTICS[source["identity"]][0]
        if source["kind"] == "metric"
        else "decision-pattern-support@1"
        if source["kind"] == "decision"
        else None
    )
    if source["semantics_id"] != expected_semantics:
        _core_error("candidate source semantics")
    policies = row["policy_identities"]
    if not isinstance(policies, Mapping) or set(policies) != set(expected_policies):
        _core_error("candidate policy identity set")
    for name, identity in policies.items():
        _policy_identity(identity, f"candidate policy {name}")
        if identity != identities[name]:
            _core_error(f"candidate policy {name} binding")
    denominators = _exact_mapping(
        row["denominators"],
        {"eligible_episode_n", "outcome_episode_n", "supporting_episode_n"},
        "candidate denominators",
    )
    _nonnegative(denominators["eligible_episode_n"], "eligible episodes")
    _nonnegative(denominators["outcome_episode_n"], "outcome episodes")
    if source["kind"] == "decision":
        _nonnegative(denominators["supporting_episode_n"], "supporting episodes")
    elif denominators["supporting_episode_n"] is not None:
        _core_error("candidate supporting denominator")
    evidence = _exact_mapping(
        row["evidence"],
        {
            "counts",
            "missingness",
            "observed_values",
            "category_counts",
            "quantiles",
            "pattern",
        },
        "candidate evidence",
    )
    present_evidence = {
        name for name, item in evidence.items() if item is not None
    }
    if present_evidence != set(expected_evidence):
        _core_error("candidate evidence field set")
    counts = evidence["counts"]
    if counts is not None:
        if (
            expected_counts is None
            or not isinstance(counts, Mapping)
            or set(counts) != set(expected_counts)
        ):
            _core_error("candidate counts")
        for name, count in counts.items():
            if name == "as_of":
                _utc_instant(count, "candidate as_of")
            else:
                _nonnegative(count, f"candidate count {name}")
    if evidence["missingness"] is not None:
        _validate_missingness(evidence["missingness"], "candidate missingness")
    if evidence["observed_values"] is not None:
        for item in _array(
            evidence["observed_values"], "candidate observed_values"
        ):
            if type(item) is int:
                _nonnegative(item, "candidate observed value")
            else:
                _text(item, "candidate observed value", maximum=200)
    if evidence["category_counts"] is not None:
        categories = _exact_mapping(
            evidence["category_counts"],
            {"fail", "not-run", "pass", "unknown"},
            "candidate category counts",
        )
        for name, count in categories.items():
            _nonnegative(count, f"candidate category {name}")
    if evidence["quantiles"] is not None:
        quantiles = _exact_mapping(
            evidence["quantiles"], {"p25", "p50", "p75"}, "candidate quantiles"
        )
        for name, quantile in quantiles.items():
            _text(quantile, f"candidate quantile {name}", nullable=True, maximum=200)
    if evidence["pattern"] is not None:
        pattern = _array(evidence["pattern"], "candidate pattern")
        expected_length = 2 if source["identity"] == "contiguous-adjacent-pair" else 1
        if len(pattern) != expected_length:
            _core_error("candidate pattern length")
        for event in pattern:
            _validate_event(event, "candidate pattern event")
    expected_strength = "recurring" if source["kind"] == "decision" else "descriptive"
    if row["evidence_strength"] != expected_strength:
        _core_error("candidate evidence strength")
    without_id = dict(row)
    del without_id["candidate_id"]
    try:
        expected_id = candidate_id(without_id)
    except LearningSnapshotError as error:
        _core_error(f"candidate identity: {error}")
    if row["candidate_id"] != expected_id:
        _core_error("candidate identity mismatch")
    return row


def _validated_policy_documents(
    policy_set: PolicySet, artifact_identities: Mapping
) -> Mapping:
    if not isinstance(policy_set, PolicySet):
        raise SnapshotPublicationError(
            "validation", "policy_set must be an immutable PolicySet"
        )
    try:
        policy_identities = policy_set.core_identity()
    except (KeyError, TypeError, ValueError) as error:
        raise SnapshotPublicationError(
            "validation", f"analysis policy set is invalid: {error}"
        ) from error
    if policy_identities != artifact_identities:
        raise SnapshotPublicationError(
            "validation", "analysis policy set does not match learning snapshot"
        )
    documents = policy_set.documents
    try:
        validate_policy_documents(
            documents, allow_reviewed_generation_mapping=True
        )
        for document_name, identity_name in _POLICY_DOCUMENT_IDENTITIES.items():
            document = documents[document_name]
            expected_identity = {
                "version": document["version"],
                "sha256": "sha256:" + hashlib.sha256(
                    canonicalize(document)
                ).hexdigest(),
            }
            if policy_identities[identity_name] != expected_identity:
                raise PolicyError(
                    f"analysis policy identity does not bind {document_name}"
                )
    except (CanonicalizationError, KeyError, PolicyError) as error:
        raise SnapshotPublicationError(
            "validation", f"analysis policy set is invalid: {error}"
        ) from error
    return documents


def _decision_pattern_strength(
    pattern: Mapping, cohort: Mapping, decision_policy: Mapping
) -> str:
    recurring = (
        pattern["episode_count_with_event"]
        >= decision_policy["decision_min_episode_support"]
        and Fraction(
            pattern["support_fraction"]["numerator"],
            pattern["support_fraction"]["denominator"],
        ) >= Fraction(decision_policy["decision_min_support_ratio"])
        and cohort["comparative_inference_eligible"]
        and cohort["outcome_episode_n"]
        >= decision_policy["decision_recurring_minimum_outcome_episodes"]
    )
    return "recurring" if recurring else "descriptive"


def _validate_snapshot_core(
    core: Mapping, adapter: Mapping, policy_set: PolicySet
) -> None:
    if (
        type(core["schema_version"]) is not int
        or core["schema_version"] != 1
        or core["analyzer_version"] != "workflow-learning-analyzer@0.2.0"
    ):
        _core_error("core version")
    query = _validate_query(core["query"])
    identities = _validate_policy_identities(core["analysis_policy_set"])
    documents = _validated_policy_documents(policy_set, identities)
    decision_policy = documents["decision_support_policy"]
    analyzer = identities["analyzer_artifact"]
    if (
        analyzer["version"] != core["analyzer_version"]
        or analyzer["sha256"]
        != "sha256:" + adapter["implementation_sha256"]
    ):
        raise SnapshotPublicationError(
            "validation", "learning snapshot adapter does not bind analyzer artifact"
        )
    lifecycle = _exact_mapping(
        core["lifecycle_health_policy"],
        {"policy_id", "policy_sha256", "as_of", "stale_after_seconds"},
        "lifecycle health policy",
    )
    _text(lifecycle["policy_id"], "lifecycle policy id", maximum=200)
    if (
        not isinstance(lifecycle["policy_sha256"], str)
        or _PREFIXED_SHA256_RE.fullmatch(lifecycle["policy_sha256"]) is None
        or {
            "version": lifecycle["policy_id"],
            "sha256": lifecycle["policy_sha256"],
        } != identities["lifecycle_health_policy"]
    ):
        _core_error("lifecycle policy identity")
    _utc_instant(lifecycle["as_of"], "lifecycle as_of")
    if lifecycle["as_of"] != query["lifecycle_as_of"]:
        _core_error("lifecycle as_of binding")
    _nonnegative(lifecycle["stale_after_seconds"], "lifecycle stale seconds")
    _validate_input_manifest(core["input_manifest"])
    ledger_keys = []
    for raw in _array(core["exclusion_ledger"], "exclusion ledger"):
        row = _exact_mapping(
            raw, {"run_id", "reason", "excluded_from"}, "exclusion ledger row"
        )
        if (
            not isinstance(row["run_id"], str)
            or _RUN_ID_RE.fullmatch(row["run_id"]) is None
            or row["reason"] not in {
                "draft", "generation-unavailable", "invalidated", "superseded"
            }
            or row["excluded_from"] not in {
                "outcome-analysis", "comparative-inference"
            }
        ):
            _core_error("exclusion ledger row")
        ledger_keys.append((row["run_id"], row["reason"], row["excluded_from"]))
    if ledger_keys != sorted(set(ledger_keys)):
        _core_error("exclusion ledger order")
    cohort_rows = _array(core["cohorts"], "cohorts")
    cohort_keys = []
    cohort_order_keys = []
    cohort_index = {}
    for cohort in cohort_rows:
        _validate_cohort(cohort)
        cohort_key = canonicalize({
            name: cohort[name] for name in _COHORT_IDENTITY_KEYS
        })
        cohort_keys.append(cohort_key)
        cohort_order_keys.append(_cohort_order_key(cohort))
        cohort_index[cohort_key] = cohort
    if len(cohort_keys) != len(set(cohort_keys)):
        _core_error("duplicate cohort identity")
    if cohort_order_keys != sorted(cohort_order_keys):
        _core_error("cohort order")
    pattern_rows = _array(core["decision_patterns"], "decision patterns")
    pattern_keys = []
    pattern_index = {}
    recurring_pattern_keys = set()
    for raw_pattern in pattern_rows:
        pattern = _validate_pattern(raw_pattern)
        cohort_key = canonicalize(pattern["cohort"])
        if cohort_key not in cohort_index:
            _core_error("decision pattern cohort binding")
        pattern_key = canonicalize([
            pattern["cohort"], pattern["pattern_kind"], pattern["pattern"]
        ])
        expected_strength = _decision_pattern_strength(
            pattern, cohort_index[cohort_key], decision_policy
        )
        if pattern["evidence_strength"] != expected_strength:
            _core_error("decision pattern evidence strength")
        if expected_strength == "recurring":
            recurring_pattern_keys.add(pattern_key)
        pattern_keys.append(pattern_key)
        pattern_index[pattern_key] = pattern
    if pattern_keys != sorted(set(pattern_keys)):
        _core_error("decision pattern order")
    candidate_rows = [
        _validate_candidate(candidate, identities)
        for candidate in _array(core["candidates"], "candidates")
    ]
    candidate_ids = [candidate["candidate_id"] for candidate in candidate_rows]
    if len(candidate_ids) != len(set(candidate_ids)):
        _core_error("duplicate candidate identity")
    if candidate_ids != sorted(candidate_ids):
        _core_error("candidate order")
    decision_candidate_counts = {}
    for candidate in candidate_rows:
        if candidate["source"]["kind"] != "decision":
            continue
        pattern_key = canonicalize([
            candidate["cohort"],
            candidate["source"]["identity"],
            candidate["evidence"]["pattern"],
        ])
        pattern = pattern_index.get(pattern_key)
        if pattern is None or pattern_key not in recurring_pattern_keys:
            _core_error("decision candidate recurring pattern binding")
        cohort = cohort_index[canonicalize(candidate["cohort"])]
        if candidate["evidence"]["counts"] != {
            "event_count": pattern["event_count"],
            "episode_count_with_event": pattern["episode_count_with_event"],
        } or candidate["denominators"] != {
            "eligible_episode_n": pattern["eligible_episode_n"],
            "outcome_episode_n": cohort["outcome_episode_n"],
            "supporting_episode_n": pattern["episode_count_with_event"],
        }:
            _core_error("decision candidate evidence binding")
        decision_candidate_counts[pattern_key] = (
            decision_candidate_counts.get(pattern_key, 0) + 1
        )
    if (
        set(decision_candidate_counts) != recurring_pattern_keys
        or any(count != 1 for count in decision_candidate_counts.values())
    ):
        _core_error("decision candidate recurring pattern coverage")


def read_learning_artifact(
    snapshot_dir: Path,
    expected_snapshot_id: str,
    policy_set: PolicySet,
) -> Mapping:
    _validate_snapshot_id(expected_snapshot_id)
    directory_fd = _open_directory(Path(snapshot_dir), required_mode=0o700)
    try:
        return _read_learning_artifact_from_fd(
            directory_fd, expected_snapshot_id, policy_set
        )
    finally:
        os.close(directory_fd)


def _validate_snapshot_id(expected_snapshot_id: object) -> None:
    if not isinstance(expected_snapshot_id, str) or not _lower_sha256(
        expected_snapshot_id
    ):
        raise SnapshotPublicationError(
            "validation", "snapshot filename identity is invalid"
        )


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory_identity(metadata: os.stat_result) -> tuple[int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_mode


def _read_learning_artifact_from_fd(
    directory_fd: int,
    expected_snapshot_id: str,
    policy_set: PolicySet,
) -> Mapping:
    _validate_snapshot_id(expected_snapshot_id)
    target_name = f"{expected_snapshot_id}.json"
    try:
        before = os.stat(
            target_name, dir_fd=directory_fd, follow_symlinks=False
        )
    except OSError as error:
        raise SnapshotPublicationError(
            "validation", f"unsafe snapshot target: {error}"
        ) from error
    if not stat.S_ISREG(before.st_mode):
        raise SnapshotPublicationError(
            "validation", "unsafe snapshot target: target is not regular"
        )
    if stat.S_IMODE(before.st_mode) != 0o600:
        raise SnapshotPublicationError(
            "validation", "snapshot target must be mode-0600"
        )
    if before.st_size > _MAX_ARTIFACT_BYTES:
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact is too large"
        )
    flags = os.O_RDONLY | getattr(os, "O_NONBLOCK", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        file_fd = os.open(target_name, flags, dir_fd=directory_fd)
    except OSError as error:
        raise SnapshotPublicationError(
            "validation", f"unsafe snapshot target: {error}"
        ) from error
    try:
        opened = os.fstat(file_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or _stat_identity(before) != _stat_identity(opened)
        ):
            raise _SnapshotTargetChanged(
                "state", "snapshot target changed during read"
            )
        chunks = []
        remaining = _MAX_ARTIFACT_BYTES + 1
        while remaining:
            try:
                chunk = os.read(file_fd, min(remaining, 64 * 1024))
            except OSError as error:
                raise SnapshotPublicationError(
                    "io", f"could not read snapshot target: {error}"
                ) from error
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        after_fd = os.fstat(file_fd)
        try:
            after_entry = os.stat(
                target_name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as error:
            raise _SnapshotTargetChanged(
                "state", "snapshot target changed during read"
            ) from error
        identities = {
            _stat_identity(before),
            _stat_identity(opened),
            _stat_identity(after_fd),
            _stat_identity(after_entry),
        }
        if len(identities) != 1:
            raise _SnapshotTargetChanged(
                "state", "snapshot target changed during read"
            )
        if len(content) > _MAX_ARTIFACT_BYTES:
            raise SnapshotPublicationError(
                "validation", "learning snapshot artifact is too large"
            )
        if len(content) != opened.st_size:
            raise _SnapshotTargetChanged(
                "state", "snapshot target changed during read"
            )
    finally:
        os.close(file_fd)
    return validate_learning_artifact_bytes(
        content,
        policy_set,
        expected_snapshot_id=expected_snapshot_id,
    )


def create_learning_snapshot(
    *,
    acquire: Callable[[], SnapshotInput],
    query: SnapshotQuery,
    policy_set: PolicySet,
    home: Path,
    generated_at: str,
) -> PublishedSnapshot:
    if not callable(acquire):
        raise SnapshotPublicationError(
            "validation", "snapshot acquire callback is required"
        )
    validate_snapshot_query(query)
    _utc_instant(generated_at, "generated_at")
    expected_query = {
        "interval": deepcopy(query.interval),
        "lifecycle_as_of": query.lifecycle_as_of,
        "project": query.project,
        "workspace": query.workspace,
        "workspace_id": query.workspace_id,
        "task_type": query.task_type,
    }
    manifest_a = acquire()
    if not isinstance(manifest_a, SnapshotInput):
        raise SnapshotPublicationError(
            "validation", "snapshot acquisition returned the wrong type"
        )
    if manifest_a.semantic_bundle["query"] != expected_query:
        raise SnapshotPublicationError(
            "validation", "snapshot acquisition query does not match"
        )
    try:
        core = build_snapshot_core(manifest_a, policy_set)
    except LearningSnapshotError as error:
        raise SnapshotPublicationError("validation", str(error)) from error
    snapshot_id = hash_canonical(_SNAPSHOT_CORE_DOMAIN, core)
    artifact_without_digest = {
        "artifact_type": "learning-snapshot",
        "authoritative": False,
        "generated_at": generated_at,
        "store_identity": manifest_a.store_identity,
        "adapter": deepcopy(manifest_a.adapter),
        "snapshot_id": snapshot_id,
        "core": core,
    }
    artifact = {
        **artifact_without_digest,
        "artifact_sha256": hashlib.sha256(
            canonicalize(artifact_without_digest)
        ).hexdigest(),
    }
    validate_learning_artifact(artifact, policy_set)
    artifact_bytes = canonicalize(artifact)
    if len(artifact_bytes) > _MAX_ARTIFACT_BYTES:
        raise SnapshotPublicationError(
            "validation", "learning snapshot artifact is too large"
        )

    manifest_b = acquire()
    if not isinstance(manifest_b, SnapshotInput):
        raise SnapshotPublicationError(
            "validation", "snapshot acquisition returned the wrong type"
        )
    if (
        manifest_a.manifest_bytes != manifest_b.manifest_bytes
        or manifest_a.adapter != manifest_b.adapter
        or manifest_a.store_identity != manifest_b.store_identity
    ):
        raise SnapshotPublicationError(
            "state", "snapshot input changed during analysis"
        )

    snapshot_dir, directory_fd = _open_snapshot_directory(home)
    target_name = f"{snapshot_id}.json"
    target = snapshot_dir / target_name
    temporary_name = None
    created = False
    returned_artifact: Mapping = artifact
    try:
        temporary_name, temporary_fd = _create_temporary(directory_fd)
        try:
            _write_all(temporary_fd, artifact_bytes)
            os.fsync(temporary_fd)
        finally:
            os.close(temporary_fd)
        try:
            os.link(
                temporary_name,
                target_name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise SnapshotPublicationError(
                    "io", f"could not publish snapshot without clobber: {error}"
                ) from error
        else:
            created = True
            os.fsync(directory_fd)
        _remove_temporary(directory_fd, temporary_name)
        temporary_name = None
        if created:
            returned_artifact = _read_learning_artifact_from_fd(
                directory_fd, snapshot_id, policy_set
            )
        else:
            returned_artifact = _reuse_existing_artifact(
                directory_fd, snapshot_id, core, policy_set
            )
    finally:
        try:
            if temporary_name is not None:
                _remove_temporary(directory_fd, temporary_name)
        finally:
            os.close(directory_fd)
    return PublishedSnapshot(
        snapshot_id=snapshot_id,
        path=target,
        artifact=deepcopy(dict(returned_artifact)),
        created=created,
    )


def _reuse_existing_artifact(
    directory_fd: int,
    snapshot_id: str,
    core: Mapping,
    policy_set: PolicySet,
) -> Mapping:
    deadline = time.monotonic() + 5
    while True:
        try:
            existing = _read_learning_artifact_from_fd(
                directory_fd, snapshot_id, policy_set
            )
            break
        except _SnapshotTargetChanged:
            if time.monotonic() >= deadline:
                raise SnapshotPublicationError(
                    "state", "snapshot target did not become stable"
                )
            time.sleep(0.005)
    if (
        existing["snapshot_id"] != snapshot_id
        or canonicalize(existing["core"]) != canonicalize(core)
    ):
        raise SnapshotPublicationError(
            "state", "snapshot identity collision"
        )
    return existing


def _lower_sha256(value: object) -> bool:
    return isinstance(value, str) and _LOWER_SHA256_RE.fullmatch(value) is not None


def _utc_instant(value: object, label: str) -> datetime:
    if not isinstance(value, str) or _UTC_INSTANT_RE.fullmatch(value) is None:
        raise SnapshotPublicationError(
            "validation", f"{label} must be a second-precision UTC Z instant"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise SnapshotPublicationError(
            "validation", f"{label} must be a second-precision UTC Z instant"
        ) from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise SnapshotPublicationError("validation", f"{label} is not canonical")
    return parsed


def _open_snapshot_directory(home: Path) -> tuple[Path, int]:
    try:
        candidate = Path(home)
    except TypeError as error:
        raise SnapshotPublicationError(
            "validation", "workflow observatory home must be a path"
        ) from error
    if not candidate.is_absolute():
        raise SnapshotPublicationError(
            "validation", "workflow observatory home must be absolute"
        )
    current_fd = _open_directory(candidate)
    current = candidate
    try:
        for component in ("learning", "snapshots"):
            try:
                os.stat(
                    component, dir_fd=current_fd, follow_symlinks=False
                )
            except FileNotFoundError:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=current_fd)
                except FileExistsError:
                    pass
                except OSError as error:
                    raise SnapshotPublicationError(
                        "io", f"could not create snapshot directory: {error}"
                    ) from error
                else:
                    try:
                        os.fsync(current_fd)
                    except OSError as error:
                        raise SnapshotPublicationError(
                            "io", f"could not synchronize snapshot parent: {error}"
                        ) from error
            except OSError as error:
                raise SnapshotPublicationError(
                    "io", f"could not inspect snapshot directory: {error}"
                ) from error
            next_fd = _open_child_directory(
                current_fd, component, required_mode=0o700
            )
            os.close(current_fd)
            current_fd = next_fd
            current = current / component
        return current, current_fd
    except Exception:
        os.close(current_fd)
        raise


def _open_directory(path: Path, *, required_mode: int | None = None) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = None
    try:
        before = os.stat(path, follow_symlinks=False)
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        after = os.stat(path, follow_symlinks=False)
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise SnapshotPublicationError(
            "io", f"could not open snapshot directory: {error}"
        ) from error
    if (
        not stat.S_ISDIR(opened.st_mode)
        or (
            required_mode is not None
            and stat.S_IMODE(opened.st_mode) != required_mode
        )
        or len({
            _directory_identity(before),
            _directory_identity(opened),
            _directory_identity(after),
        }) != 1
    ):
        os.close(descriptor)
        raise SnapshotPublicationError(
            "validation", "snapshot directory changed while opening"
        )
    return descriptor


def _open_child_directory(
    parent_fd: int, component: str, *, required_mode: int
) -> int:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = None
    try:
        before = os.stat(
            component, dir_fd=parent_fd, follow_symlinks=False
        )
        descriptor = os.open(component, flags, dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        after = os.stat(
            component, dir_fd=parent_fd, follow_symlinks=False
        )
    except OSError as error:
        if descriptor is not None:
            os.close(descriptor)
        raise SnapshotPublicationError(
            "io", f"could not open snapshot directory component: {error}"
        ) from error
    if (
        not stat.S_ISDIR(opened.st_mode)
        or stat.S_IMODE(opened.st_mode) != required_mode
        or len({
            _directory_identity(before),
            _directory_identity(opened),
            _directory_identity(after),
        }) != 1
    ):
        os.close(descriptor)
        raise SnapshotPublicationError(
            "validation",
            "snapshot directory component must be stable mode-0700",
        )
    return descriptor


def _create_temporary(directory_fd: int) -> tuple[str, int]:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    for _attempt in range(16):
        name = f".snapshot-{secrets.token_hex(16)}.tmp"
        try:
            descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
        except FileExistsError:
            continue
        except OSError as error:
            raise SnapshotPublicationError(
                "io", f"could not create snapshot temporary file: {error}"
            ) from error
        try:
            os.fchmod(descriptor, 0o600)
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise SnapshotPublicationError(
                    "validation", "snapshot temporary file is not regular"
                )
        except Exception:
            os.close(descriptor)
            try:
                os.unlink(name, dir_fd=directory_fd)
            except OSError:
                pass
            raise
        return name, descriptor
    raise SnapshotPublicationError(
        "state", "could not allocate a unique snapshot temporary file"
    )


def _write_all(descriptor: int, content: bytes) -> None:
    remaining = memoryview(content)
    while remaining:
        try:
            written = os.write(descriptor, remaining)
        except OSError as error:
            raise SnapshotPublicationError(
                "io", f"could not write snapshot artifact: {error}"
            ) from error
        if written <= 0:
            raise SnapshotPublicationError(
                "io", "could not write complete snapshot artifact"
            )
        remaining = remaining[written:]


def _remove_temporary(directory_fd: int, name: str) -> None:
    try:
        os.unlink(name, dir_fd=directory_fd)
    except FileNotFoundError:
        pass
    except OSError as error:
        raise SnapshotPublicationError(
            "io", f"could not remove snapshot temporary file: {error}"
        ) from error
    try:
        os.fsync(directory_fd)
    except OSError as error:
        raise SnapshotPublicationError(
            "io", f"could not synchronize snapshot directory: {error}"
        ) from error
