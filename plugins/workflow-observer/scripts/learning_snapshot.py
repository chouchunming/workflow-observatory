from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import re

from canonical_json import CanonicalizationError, canonicalize, hash_canonical
from policy_artifacts import PolicyError, PolicySet, validate_policy_documents
from snapshot_input import SnapshotInput


_ANALYZER_VERSION = "workflow-learning-analyzer@0.2.0"
_INPUT_MANIFEST_DOMAIN = b"workflow-observatory:snapshot-input-manifest:v1\0"
_MAX_SAFE_INTEGER = (2**53) - 1
_LOWER_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_POLICY_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")
_UTC_INSTANT_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)
_GENERATION_RE = re.compile(r"[a-z0-9][a-z0-9._:@+\-]{0,199}")
_RUNTIME_GENERATION_RE = re.compile(r"[a-z0-9][a-z0-9._:@+\-]{0,199}")
_CURRENCY_RE = re.compile(r"[A-Z]{3}")
_DECIMAL_RE = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?")
_OUTCOME_STATUSES = ("failed", "partial", "rolled-back", "success")
_EXCLUSION_REASONS = {
    "draft",
    "generation-unavailable",
    "heterogeneous-runtime-provenance",
    "invalidated",
    "superseded",
}
_EPISODE_KEYS = {
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
_BUNDLE_KEYS = {
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
}
_VERIFICATION_CATEGORIES = ("fail", "not-run", "pass", "unknown")


class LearningSnapshotError(ValueError):
    pass


def _error(message: str, cause: BaseException | None = None) -> LearningSnapshotError:
    result = LearningSnapshotError(message)
    if cause is not None:
        result.__cause__ = cause
    return result


def _utf8(value: str) -> bytes:
    try:
        return value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise _error("snapshot text is not valid UTF-8", error) from error


def _bounded_text(
    value: object, label: str, *, maximum_codepoints: int = 200
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum_codepoints
        or any(ord(character) < 0x20 for character in value)
    ):
        raise LearningSnapshotError(f"{label} must be bounded non-empty text")
    _utf8(value)
    return value


def _utc_instant(value: object, label: str) -> datetime:
    if not isinstance(value, str) or _UTC_INSTANT_RE.fullmatch(value) is None:
        raise LearningSnapshotError(
            f"{label} must be a second-precision UTC Z instant"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise _error(
            f"{label} must be a second-precision UTC Z instant", error
        ) from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise LearningSnapshotError(f"{label} is not canonical")
    return parsed


def _sha256(value: object, label: str, *, prefixed: bool = False) -> str:
    pattern = _POLICY_SHA256_RE if prefixed else _LOWER_SHA256_RE
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        qualifier = "prefixed " if prefixed else ""
        raise LearningSnapshotError(f"{label} must be a {qualifier}lowercase SHA-256")
    return value


def _safe_nonnegative_integer(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        raise LearningSnapshotError(f"{label} must be a safe non-negative integer")
    return value


def _fraction_decimal(value: Fraction) -> str:
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise LearningSnapshotError(
            "quantile interpolation does not have a terminating decimal"
        )
    places = max(twos, fives)
    scaled = value.numerator
    scaled *= 2 ** (places - twos)
    scaled *= 5 ** (places - fives)
    sign = "-" if scaled < 0 else ""
    digits = str(abs(scaled))
    if places == 0:
        return sign + digits
    digits = digits.zfill(places + 1)
    whole = digits[:-places]
    fraction = digits[-places:].rstrip("0")
    return sign + whole if not fraction else f"{sign}{whole}.{fraction}"


def linear_rational_quantile(
    values: Sequence[int], numerator: int, denominator: int
) -> str | None:
    """Return one exact linearly interpolated quantile as normalized decimal text."""

    if isinstance(values, (str, bytes, bytearray)) or not isinstance(
        values, Sequence
    ):
        raise LearningSnapshotError("quantile values must be a sequence")
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or denominator <= 0
        or numerator < 0
        or numerator > denominator
    ):
        raise LearningSnapshotError("quantile ratio must be between zero and one")
    ordered = [
        _safe_nonnegative_integer(value, "quantile value") for value in values
    ]
    ordered.sort()
    if not ordered:
        return None
    index = Fraction((len(ordered) - 1) * numerator, denominator)
    lower = index.numerator // index.denominator
    upper = lower if index.denominator == 1 else lower + 1
    if lower == upper:
        result = Fraction(ordered[lower], 1)
    else:
        result = Fraction(ordered[lower], 1) + (
            Fraction(ordered[upper] - ordered[lower], 1) * (index - lower)
        )
    return _fraction_decimal(result)


def _validate_policy_set(policy_set: PolicySet) -> tuple[dict, dict]:
    if not isinstance(policy_set, PolicySet):
        raise LearningSnapshotError("policy_set must be an immutable PolicySet")
    documents = policy_set.documents
    identities = policy_set.core_identity()
    try:
        validate_policy_documents(
            documents, allow_reviewed_generation_mapping=True
        )
        canonicalize(identities)
    except (PolicyError, CanonicalizationError) as error:
        raise _error(f"analysis policy set is invalid: {error}", error) from error
    if identities.get("analyzer_artifact", {}).get("version") != _ANALYZER_VERSION:
        raise LearningSnapshotError("analysis policy has the wrong analyzer version")
    expected_identity_names = {
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
    if set(identities) != expected_identity_names:
        raise LearningSnapshotError("analysis policy identity set is incomplete")
    for name, row in identities.items():
        if not isinstance(row, Mapping) or set(row) != {"version", "sha256"}:
            raise LearningSnapshotError(f"analysis policy identity {name} is invalid")
        _bounded_text(row["version"], f"analysis policy identity {name} version")
        _sha256(row["sha256"], f"analysis policy identity {name}", prefixed=True)
    document_identities = {
        "episode_projection": "canonical_projection_contract",
        "producer_capabilities": "producer_capability_registry",
        "workflow_generation_mapping": "workflow_generation_mapping",
        "metric_semantics": "metric_semantics_registry",
        "quantile_policy": "quantile_policy",
        "decision_support_policy": "decision_support_policy",
        "lifecycle_health_policy": "lifecycle_health_policy",
        "candidate_emission_policy": "candidate_emission_policy",
    }
    for document_name, identity_name in document_identities.items():
        document = documents[document_name]
        expected = {
            "version": document["version"],
            "sha256": "sha256:" + hashlib.sha256(canonicalize(document)).hexdigest(),
        }
        if identities[identity_name] != expected:
            raise LearningSnapshotError(
                f"analysis policy identity does not bind {document_name}"
            )
    if documents["quantile_policy"]["quantiles"] != ["0.25", "0.50", "0.75"]:
        raise LearningSnapshotError("quantile policy is not the approved quartile set")
    return documents, identities


def _validate_input_bundle(
    snapshot_input: SnapshotInput,
    documents: Mapping,
    identities: Mapping,
) -> dict:
    if not isinstance(snapshot_input, SnapshotInput):
        raise LearningSnapshotError("snapshot_input must be a SnapshotInput")
    bundle = snapshot_input.semantic_bundle
    if not isinstance(bundle, dict) or set(bundle) != _BUNDLE_KEYS:
        raise LearningSnapshotError("snapshot input semantic bundle has wrong fields")
    if type(bundle["schema_version"]) is not int or bundle["schema_version"] != 1:
        raise LearningSnapshotError("snapshot input schema version is unsupported")
    if bundle["projection_version"] != "episode-projection@2":
        raise LearningSnapshotError("snapshot input projection version is unsupported")
    if bundle["policy_set"] != identities:
        raise LearningSnapshotError("snapshot input and analysis policy sets differ")
    expected_capabilities = documents["episode_projection"]["schema_capabilities"]
    if bundle["schema_capabilities"] != expected_capabilities:
        raise LearningSnapshotError("snapshot schema capabilities differ from policy")
    if not isinstance(bundle["query"], dict):
        raise LearningSnapshotError("snapshot query must be an object")
    _utc_instant(bundle["lifecycle_as_of"], "snapshot lifecycle_as_of")
    manifest_sha256 = _sha256(
        bundle["input_manifest_sha256"], "snapshot input manifest"
    )
    unhashed = deepcopy(bundle)
    del unhashed["input_manifest_sha256"]
    try:
        expected_manifest = hash_canonical(_INPUT_MANIFEST_DOMAIN, unhashed)
    except CanonicalizationError as error:
        raise _error(f"snapshot input is not canonicalizable: {error}", error) from error
    if manifest_sha256 != expected_manifest:
        raise LearningSnapshotError("snapshot input manifest digest does not match")
    return bundle


def _generation(episode: Mapping) -> str | None:
    value = episode.get("workflow_generation")
    if not isinstance(value, Mapping) or set(value) != {"availability", "value"}:
        raise LearningSnapshotError("Episode workflow_generation is invalid")
    availability = value["availability"]
    generation = value["value"]
    if availability == "unavailable" and generation is None:
        return None
    if (
        availability != "observed"
        or not isinstance(generation, str)
        or _GENERATION_RE.fullmatch(generation) is None
        or generation in {"unknown", "unavailable"}
    ):
        raise LearningSnapshotError("Episode workflow_generation is inconsistent")
    return generation


def _runtime_generation(episode: Mapping) -> str | None:
    value = episode.get("runtime_provenance")
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or _RUNTIME_GENERATION_RE.fullmatch(value) is None
        or value in {"unknown", "unavailable"}
    ):
        raise LearningSnapshotError("Episode runtime_provenance is invalid")
    return value


def _validate_metric_projection(
    name: str,
    metric: object,
    *,
    schema_supported: bool,
) -> Mapping:
    if not isinstance(metric, Mapping) or set(metric) != {
        "availability", "value", "unit"
    }:
        raise LearningSnapshotError(f"Episode metric {name} is invalid")
    availability = metric["availability"]
    if availability not in {
        "observed", "not_recorded", "unsupported_by_schema"
    }:
        raise LearningSnapshotError(f"Episode metric {name} availability is invalid")
    if schema_supported == (availability == "unsupported_by_schema"):
        raise LearningSnapshotError(
            f"Episode metric {name} conflicts with schema capability"
        )
    if availability != "observed" and (
        metric["value"] is not None or metric["unit"] is not None
    ):
        raise LearningSnapshotError(
            f"Episode metric {name} missing value has retained data"
        )
    return metric


def _validate_episode(episode: object, documents: Mapping) -> Mapping:
    if not isinstance(episode, Mapping) or set(episode) != _EPISODE_KEYS:
        raise LearningSnapshotError("snapshot Episode has wrong fields")
    run_id = _bounded_text(episode["run_id"], "Episode run_id")
    _sha256(episode["source_sha256"], f"Episode {run_id} source")
    schema_version = episode["episode_schema_version"]
    if type(schema_version) is not int or schema_version not in {1, 2}:
        raise LearningSnapshotError("Episode schema version is unsupported")
    _utc_instant(episode["started_at"], f"Episode {run_id} started_at")
    status = episode["status"]
    if status not in {*_OUTCOME_STATUSES, "draft", "superseded"}:
        raise LearningSnapshotError("Episode status is unsupported")
    for name in (
        "project", "workspace", "workspace_id", "task_type", "workflow_variant"
    ):
        _bounded_text(episode[name], f"Episode {name}")
    _generation(episode)
    _runtime_generation(episode)
    metrics = episode["metrics"]
    metric_semantics = documents["metric_semantics"]["metrics"]
    if not isinstance(metrics, Mapping) or set(metrics) != set(metric_semantics):
        raise LearningSnapshotError("Episode metrics do not cover policy metrics")
    capabilities = documents["episode_projection"]["schema_capabilities"]
    schema_capabilities = capabilities[str(schema_version)]["metrics"]
    for name in metric_semantics:
        _validate_metric_projection(
            name,
            metrics[name],
            schema_supported=schema_capabilities[name],
        )
    if not isinstance(episode["decisions"], list):
        raise LearningSnapshotError("Episode decisions must be an array")
    return episode


def _validate_episodes(bundle: Mapping, documents: Mapping) -> list[Mapping]:
    episodes = bundle["episodes"]
    if not isinstance(episodes, list):
        raise LearningSnapshotError("snapshot Episodes must be an array")
    validated = [_validate_episode(episode, documents) for episode in episodes]
    run_ids = [episode["run_id"] for episode in validated]
    if len(run_ids) != len(set(run_ids)):
        raise LearningSnapshotError("snapshot Episodes contain duplicate run_id")
    if run_ids != sorted(run_ids, key=_utf8):
        raise LearningSnapshotError("snapshot Episodes are not sorted")
    counts = bundle["record_counts"]
    if not isinstance(counts, Mapping) or set(counts) != {
        "selected_episode_n",
        "draft_episode_n",
        "final_episode_n",
        "selected_invalidation_n",
    }:
        raise LearningSnapshotError("snapshot record_counts are invalid")
    for name, count in counts.items():
        _safe_nonnegative_integer(count, f"snapshot record_counts {name}")
    draft_n = sum(episode["status"] == "draft" for episode in validated)
    if (
        counts["selected_episode_n"] != len(validated)
        or counts["draft_episode_n"] != draft_n
        or counts["final_episode_n"] != len(validated) - draft_n
    ):
        raise LearningSnapshotError("snapshot Episode record counts are inconsistent")
    return validated


def _validate_invalidations(bundle: Mapping, run_ids: set[str]) -> tuple[list[dict], set[str]]:
    invalidations = bundle["invalidations"]
    if not isinstance(invalidations, list):
        raise LearningSnapshotError("snapshot invalidations must be an array")
    rows = []
    seen = set()
    for raw in invalidations:
        if not isinstance(raw, Mapping) or set(raw) != {
            "run_id", "source_sha256", "timestamp"
        }:
            raise LearningSnapshotError("snapshot invalidation row is invalid")
        run_id = _bounded_text(raw["run_id"], "invalidation run_id")
        if run_id not in run_ids or run_id in seen:
            raise LearningSnapshotError("snapshot invalidation target is inconsistent")
        seen.add(run_id)
        _sha256(raw["source_sha256"], "invalidation source")
        _utc_instant(raw["timestamp"], "invalidation timestamp")
        rows.append(dict(raw))
    rows.sort(key=lambda row: _utf8(row["run_id"]))
    if bundle["record_counts"]["selected_invalidation_n"] != len(rows):
        raise LearningSnapshotError("snapshot invalidation count is inconsistent")
    return rows, seen


def _validate_references(bundle: Mapping) -> list[dict]:
    references = bundle["reference_manifest"]
    if not isinstance(references, list):
        raise LearningSnapshotError("snapshot reference manifest must be an array")
    rows = []
    seen = set()
    for raw in references:
        if not isinstance(raw, Mapping) or set(raw) != {
            "kind", "identity", "sha256"
        }:
            raise LearningSnapshotError("snapshot reference row is invalid")
        kind = _bounded_text(raw["kind"], "reference kind")
        identity = _bounded_text(
            raw["identity"], "reference identity", maximum_codepoints=1024
        )
        _sha256(raw["sha256"], "reference source")
        key = (kind, identity)
        if key in seen:
            raise LearningSnapshotError("snapshot reference identity is duplicated")
        seen.add(key)
        rows.append(dict(raw))
    rows.sort(key=lambda row: (_utf8(row["kind"]), _utf8(row["identity"])))
    return rows


def _cohort_group(episode: Mapping) -> tuple:
    generation = _generation(episode)
    dimensions = tuple(
        episode[name]
        for name in (
            "project",
            "workspace",
            "workspace_id",
            "task_type",
            "workflow_variant",
        )
    )
    if generation is None:
        return ("legacy-generation-unavailable", *dimensions, episode["run_id"])
    return ("workflow-generation", *dimensions, generation)


def _group_sort_key(key: tuple) -> tuple[bytes, ...]:
    return tuple(_utf8(value) for value in key)


def _metric_value(name: str, metric: Mapping, semantics: Mapping) -> object:
    value = metric["value"]
    value_type = semantics["value_type"]
    if value_type == "nonnegative-integer":
        return _safe_nonnegative_integer(value, f"metric {name} value")
    if value_type == "enum":
        if name != "verification" or value not in _VERIFICATION_CATEGORIES:
            raise LearningSnapshotError(f"metric {name} enum value is invalid")
        if metric["unit"] is not None:
            raise LearningSnapshotError(f"metric {name} cannot have a unit")
        return value
    if value_type == "normalized-decimal-string":
        if (
            name != "cost_amount"
            or not isinstance(value, str)
            or _DECIMAL_RE.fullmatch(value) is None
            or not isinstance(metric["unit"], str)
            or _CURRENCY_RE.fullmatch(metric["unit"]) is None
        ):
            raise LearningSnapshotError(f"metric {name} decimal value is invalid")
        return value
    raise LearningSnapshotError(f"metric {name} has unsupported value semantics")


def _metric_result(name: str, outcomes: list[Mapping], documents: Mapping) -> dict:
    semantics = documents["metric_semantics"]["metrics"][name]
    counts = {
        "eligible_episode_n": len(outcomes),
        "observed_n": 0,
        "not_recorded_n": 0,
        "unsupported_by_schema_n": 0,
        "not_applicable_n": 0,
    }
    observed = []
    for episode in outcomes:
        metric = episode["metrics"][name]
        availability = metric["availability"]
        if availability == "observed":
            counts["observed_n"] += 1
            observed.append(_metric_value(name, metric, semantics))
        elif availability == "not_recorded":
            counts["not_recorded_n"] += 1
        elif availability == "unsupported_by_schema":
            counts["unsupported_by_schema_n"] += 1
        else:
            raise LearningSnapshotError(
                f"metric {name} did not enter one eligibility bucket"
            )
    if sum(counts[name] for name in (
        "observed_n",
        "not_recorded_n",
        "unsupported_by_schema_n",
        "not_applicable_n",
    )) != counts["eligible_episode_n"]:
        raise LearningSnapshotError(f"metric {name} eligibility partition is invalid")

    aggregation = semantics["aggregation"]
    observed_values = None
    category_counts = None
    quantiles = None
    if aggregation == "integer-quantiles":
        observed_values = sorted(observed)
        quantiles = {
            "p25": linear_rational_quantile(observed_values, 1, 4),
            "p50": linear_rational_quantile(observed_values, 1, 2),
            "p75": linear_rational_quantile(observed_values, 3, 4),
        }
    elif aggregation == "category-counts":
        if name != "verification":
            raise LearningSnapshotError("category-count metric is not supported")
        category_counts = {
            category: observed.count(category)
            for category in _VERIFICATION_CATEGORIES
        }
    elif aggregation != "missingness-only":
        raise LearningSnapshotError(f"metric {name} aggregation is unsupported")
    return {
        "metric": name,
        "semantics_id": semantics["semantics_id"],
        "value_type": semantics["value_type"],
        "aggregation": aggregation,
        "missingness": counts,
        "observed_values": observed_values,
        "category_counts": category_counts,
        "quantiles": quantiles,
    }


def _ledger_row(run_id: str, reason: str, excluded_from: str) -> tuple[str, str, str]:
    if reason not in _EXCLUSION_REASONS:
        raise LearningSnapshotError("exclusion reason is not bounded")
    if excluded_from not in {"outcome-analysis", "comparative-inference"}:
        raise LearningSnapshotError("exclusion scope is not bounded")
    return run_id, reason, excluded_from


def _build_cohort(
    key: tuple,
    episodes: list[Mapping],
    invalidated_ids: set[str],
    documents: Mapping,
    as_of: datetime,
    ledger: set[tuple[str, str, str]],
) -> dict:
    collection = key[0]
    if collection == "workflow-generation":
        project, workspace, workspace_id, task_type, variant, generation = key[1:]
        legacy_collection_id = None
    else:
        project, workspace, workspace_id, task_type, variant, legacy_collection_id = key[1:]
        generation = None

    outcomes = []
    for episode in episodes:
        run_id = episode["run_id"]
        status = episode["status"]
        if status == "draft":
            ledger.add(_ledger_row(run_id, "draft", "outcome-analysis"))
        if status == "superseded":
            ledger.add(_ledger_row(run_id, "superseded", "outcome-analysis"))
        if run_id in invalidated_ids:
            ledger.add(_ledger_row(run_id, "invalidated", "outcome-analysis"))
        if status in _OUTCOME_STATUSES and run_id not in invalidated_ids:
            outcomes.append(episode)

    exclusions = []
    if generation is None:
        exclusions.append("generation-unavailable")
        for episode in episodes:
            ledger.add(_ledger_row(
                episode["run_id"],
                "generation-unavailable",
                "comparative-inference",
            ))
    runtime_generations = {
        runtime
        for runtime in (_runtime_generation(episode) for episode in outcomes)
        if runtime is not None
    }
    if len(runtime_generations) > 1:
        exclusions.append("heterogeneous-runtime-provenance")
        for episode in outcomes:
            ledger.add(_ledger_row(
                episode["run_id"],
                "heterogeneous-runtime-provenance",
                "comparative-inference",
            ))
    exclusions.sort(key=_utf8)
    comparable = not exclusions
    recurring_minimum = documents["decision_support_policy"] \
        ["decision_recurring_minimum_outcome_episodes"]
    lifecycle = documents["lifecycle_health_policy"]
    stale_after = lifecycle["draft_stale_after_seconds"]
    drafts = [episode for episode in episodes if episode["status"] == "draft"]
    stale_drafts = [
        episode
        for episode in drafts
        if (as_of - _utc_instant(
            episode["started_at"], f"Episode {episode['run_id']} started_at"
        )).total_seconds() > stale_after
    ]
    outcome_counts = {
        status: sum(episode["status"] == status for episode in outcomes)
        for status in _OUTCOME_STATUSES
    }
    metric_names = sorted(
        documents["metric_semantics"]["metrics"], key=_utf8
    )
    return {
        "collection": collection,
        "legacy_collection_id": legacy_collection_id,
        "project": project,
        "workspace": workspace,
        "workspace_id": workspace_id,
        "task_type": task_type,
        "workflow_variant": variant,
        "workflow_generation": generation,
        "comparative_inference_eligible": comparable,
        "comparative_inference_exclusions": exclusions,
        "evidence_strength": (
            "recurring"
            if comparable and len(outcomes) >= recurring_minimum
            else "descriptive"
        ),
        "outcome_episode_n": len(outcomes),
        "outcome_counts": outcome_counts,
        "draft_episode_n": len(drafts),
        "active_draft_n": len(drafts) - len(stale_drafts),
        "stale_draft_n": len(stale_drafts),
        "superseded_episode_n": sum(
            episode["status"] == "superseded" for episode in episodes
        ),
        "invalidated_episode_n": sum(
            episode["run_id"] in invalidated_ids for episode in episodes
        ),
        "generation_unavailable_episode_n": (
            len(episodes) if generation is None else 0
        ),
        "metrics": [
            _metric_result(name, outcomes, documents) for name in metric_names
        ],
    }


def build_snapshot_core(
    snapshot_input: SnapshotInput, policy_set: PolicySet
) -> dict:
    """Build the deterministic Task 7 snapshot core from one canonical input."""

    documents, identities = _validate_policy_set(policy_set)
    bundle = _validate_input_bundle(snapshot_input, documents, identities)
    episodes = _validate_episodes(bundle, documents)
    invalidation_rows, invalidated_ids = _validate_invalidations(
        bundle, {episode["run_id"] for episode in episodes}
    )
    reference_rows = _validate_references(bundle)
    as_of = _utc_instant(bundle["lifecycle_as_of"], "snapshot lifecycle_as_of")

    groups: dict[tuple, list[Mapping]] = {}
    for episode in episodes:
        groups.setdefault(_cohort_group(episode), []).append(episode)
    ledger: set[tuple[str, str, str]] = set()
    cohorts = [
        _build_cohort(
            key,
            groups[key],
            invalidated_ids,
            documents,
            as_of,
            ledger,
        )
        for key in sorted(groups, key=_group_sort_key)
    ]
    exclusion_ledger = [
        {
            "run_id": run_id,
            "reason": reason,
            "excluded_from": excluded_from,
        }
        for run_id, reason, excluded_from in sorted(
            ledger,
            key=lambda row: tuple(_utf8(value) for value in row),
        )
    ]
    lifecycle_identity = identities["lifecycle_health_policy"]
    return {
        "schema_version": 1,
        "analyzer_version": _ANALYZER_VERSION,
        "query": deepcopy(bundle["query"]),
        "lifecycle_health_policy": {
            "policy_id": lifecycle_identity["version"],
            "policy_sha256": lifecycle_identity["sha256"],
            "as_of": bundle["lifecycle_as_of"],
            "stale_after_seconds": documents["lifecycle_health_policy"]
                ["draft_stale_after_seconds"],
        },
        "analysis_policy_set": deepcopy(identities),
        "input_manifest": {
            "input_manifest_sha256": bundle["input_manifest_sha256"],
            "episodes": [
                {
                    "run_id": episode["run_id"],
                    "source_sha256": episode["source_sha256"],
                }
                for episode in episodes
            ],
            "invalidations": invalidation_rows,
            "reference_manifest": reference_rows,
        },
        "exclusion_ledger": exclusion_ledger,
        "cohorts": cohorts,
        "decision_patterns": [],
        "candidates": [],
    }
