from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from types import MappingProxyType
import unicodedata

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    strict_json_loads,
)
from wiki_observations import (
    FINAL_STATUSES,
    ObservationError,
    _parse_completion,
    parse_scope_payload,
)


_MAX_SAFE_INTEGER = (2**53) - 1
_COST_PATTERN = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?")
_CURRENCY_PATTERN = re.compile(r"[A-Z]{3}")
_WORKFLOW_GENERATION_PATTERN = re.compile(
    r"[a-z0-9][a-z0-9._:@+\-]{0,199}"
)
_ABSOLUTE_POSIX_PATH_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_./-])/(?!/)[^\s]+"
)
_ABSOLUTE_WINDOWS_PATH_PATTERN = re.compile(
    r"(?i)(?<![A-Za-z0-9_])[A-Z]:[\\/][^\s]+"
)
_ABSOLUTE_NETWORK_PATH_PATTERN = re.compile(
    r"(?:\\\\[^\s]+[\\/][^\s]+|(?<!:)//[^/\s]+/[^\s]+)"
)
_URL_PATTERN = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://\S+")
_CREDENTIAL_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?:[a-z0-9]+[_-])*"
    r"(?:api[_-]?key|(?:secret[_-]?)?access[_-]?key(?:[_-]?id)?|"
    r"access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|"
    r"refresh[_-]?token|session[_-]?token|private[_-]?key|token|password|"
    r"passwd|secret|credentials?)"
    r"(?:[_-][a-z0-9]+)*\s*[:=]\s*\S+"
)
_SUPPLEMENT_KEYS = {"schema_version", "execution", "quality", "decisions"}
_EXECUTION_KEYS = {
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cost_amount",
    "cost_currency",
    "measurement_source",
}
_SUPPLEMENT_QUALITY_KEYS = {"test_failures", "timeout_count"}
_LIFECYCLE_QUALITY_KEYS = {
    "verification",
    "review_rounds",
    "defects_found",
    "rework_count",
}
_EPISODE_QUALITY_KEYS = {
    "elapsed_seconds",
    *_LIFECYCLE_QUALITY_KEYS,
    *_SUPPLEMENT_QUALITY_KEYS,
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
_VERIFICATION_VALUES = {"pass", "fail", "not-run", "unknown"}
_EPISODE_HEADING = "## Episode data"
_EPISODE_BLOCK_PATTERN = re.compile(
    r"(?:^|\n)## Episode data\n\n```json\n([^\n]*)\n```\n?\Z"
)


class EpisodeSchemaError(ValueError):
    pass


@dataclass(frozen=True)
class EpisodeV2Supplement:
    schema_version: int
    execution: Mapping[str, object]
    quality: Mapping[str, object]
    decisions: tuple[Mapping[str, object], ...]


def _error(message: str, cause: Exception | None = None) -> EpisodeSchemaError:
    error = EpisodeSchemaError(message)
    if cause is not None:
        error.__cause__ = cause
    return error


def _strict_json(data: str | bytes, label: str) -> object:
    try:
        return strict_json_loads(data)
    except CanonicalizationError as error:
        raise _error(f"{label}: {error}", error)


def _canonical_bytes(value: object, label: str) -> bytes:
    try:
        return canonicalize(value)
    except CanonicalizationError as error:
        raise _error(f"{label}: {error}", error)


def _exact_mapping(
    value: object,
    keys: set[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise EpisodeSchemaError(f"{label} must have exactly its allowed keys")
    return value


def _projection_document(projection: Mapping) -> Mapping[str, object]:
    if not isinstance(projection, Mapping):
        raise EpisodeSchemaError("projection policy must be a mapping")
    nested = projection.get("episode_projection")
    document = nested if isinstance(nested, Mapping) else projection
    if document.get("version") != "episode-projection@2":
        raise EpisodeSchemaError("episode projection policy version is unsupported")
    if not isinstance(document.get("enumerations"), Mapping):
        raise EpisodeSchemaError("episode projection enumerations are missing")
    if not isinstance(document.get("schema_capabilities"), Mapping):
        raise EpisodeSchemaError("episode projection capabilities are missing")
    return document


def _safe_nonnegative_integer(
    value: object,
    label: str,
    *,
    nullable: bool = False,
) -> int | None:
    if value is None and nullable:
        return None
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        qualifier = " or null" if nullable else ""
        raise EpisodeSchemaError(
            f"{label} must be a safe non-negative integer{qualifier}"
        )
    return value


def _quality_integer_or_unknown(value: object, label: str) -> int | str:
    if value == "unknown":
        return "unknown"
    validated = _safe_nonnegative_integer(value, label)
    assert validated is not None
    return validated


def _validate_execution(
    value: object,
    projection_document: Mapping[str, object],
) -> dict[str, object]:
    execution = _exact_mapping(value, _EXECUTION_KEYS, "execution")
    validated: dict[str, object] = {}
    token_names = ("input_tokens", "output_tokens", "cache_read_tokens")
    for name in token_names:
        validated[name] = _safe_nonnegative_integer(
            execution[name], name, nullable=True
        )

    amount = execution["cost_amount"]
    if amount is not None and (
        not isinstance(amount, str) or _COST_PATTERN.fullmatch(amount) is None
    ):
        raise EpisodeSchemaError("cost_amount must be a normalized decimal string or null")
    currency = execution["cost_currency"]
    if currency is not None and (
        not isinstance(currency, str)
        or _CURRENCY_PATTERN.fullmatch(currency) is None
    ):
        raise EpisodeSchemaError("cost_currency must be three ASCII uppercase letters or null")
    if (amount is None) != (currency is None):
        raise EpisodeSchemaError("cost_currency must be present exactly with cost_amount")

    enumerations = projection_document["enumerations"]
    assert isinstance(enumerations, Mapping)
    measurement_source = execution["measurement_source"]
    allowed_sources = enumerations.get("measurement_source")
    if (
        not isinstance(measurement_source, str)
        or not isinstance(allowed_sources, Sequence)
        or measurement_source not in allowed_sources
    ):
        raise EpisodeSchemaError("measurement_source is not allowed by projection policy")
    has_measurement = amount is not None or any(
        validated[name] is not None for name in token_names
    )
    if has_measurement and measurement_source not in {
        "tool-derived",
        "agent-reported",
    }:
        raise EpisodeSchemaError(
            "measured execution values require an attributed measurement_source"
        )
    if not has_measurement and measurement_source != "unavailable":
        raise EpisodeSchemaError(
            "unavailable execution values require measurement_source unavailable"
        )
    validated.update({
        "cost_amount": amount,
        "cost_currency": currency,
        "measurement_source": measurement_source,
    })
    return validated


def _validate_supplement_quality(value: object) -> dict[str, object]:
    quality = _exact_mapping(
        value,
        _SUPPLEMENT_QUALITY_KEYS,
        "supplement quality",
    )
    return {
        name: _safe_nonnegative_integer(quality[name], name, nullable=True)
        for name in ("test_failures", "timeout_count")
    }


def _validate_summary(value: object, scalar_limit: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EpisodeSchemaError("Decision summary must be a non-empty string")
    if type(scalar_limit) is not int or scalar_limit <= 0:
        raise EpisodeSchemaError("projection scalar limit is invalid")
    if len(value) > scalar_limit:
        raise EpisodeSchemaError(
            f"Decision summary must not exceed {scalar_limit} Unicode code points"
        )
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise EpisodeSchemaError("Decision summary contains a control character")
    if "---" in value:
        raise EpisodeSchemaError("Decision summary contains a frontmatter delimiter")
    if any(pattern.search(value) for pattern in (
        _ABSOLUTE_POSIX_PATH_PATTERN,
        _ABSOLUTE_WINDOWS_PATH_PATTERN,
        _ABSOLUTE_NETWORK_PATH_PATTERN,
        _URL_PATTERN,
        _CREDENTIAL_ASSIGNMENT_PATTERN,
    )):
        raise EpisodeSchemaError(
            "Decision summary contains a path, URL, or credential assignment"
        )
    return value


def _validate_decisions(
    value: object,
    projection_document: Mapping[str, object],
    *,
    require_list: bool,
) -> tuple[Mapping[str, object], ...]:
    if (
        isinstance(value, (str, bytes, bytearray))
        or not isinstance(value, Sequence)
        or (require_list and not isinstance(value, list))
    ):
        raise EpisodeSchemaError("decisions must be a JSON array")
    maximum = projection_document.get("max_decisions")
    if type(maximum) is not int or maximum <= 0:
        raise EpisodeSchemaError("projection Decision bound is invalid")
    if len(value) > maximum:
        raise EpisodeSchemaError("decisions exceed the projection bound")
    enumerations = projection_document["enumerations"]
    assert isinstance(enumerations, Mapping)
    rows: list[Mapping[str, object]] = []
    for index, raw in enumerate(value, start=1):
        decision = _exact_mapping(raw, _DECISION_KEYS, f"decision {index}")
        sequence = _safe_nonnegative_integer(
            decision["sequence"], f"decision {index} sequence"
        )
        if sequence != index:
            raise EpisodeSchemaError("Decision sequence must be contiguous and one-based")
        row: dict[str, object] = {"sequence": sequence}
        for name in (
            "phase",
            "actor_role",
            "decision_type",
            "reason_code",
            "result",
        ):
            allowed = enumerations.get(name)
            item = decision[name]
            if (
                not isinstance(item, str)
                or not isinstance(allowed, Sequence)
                or item not in allowed
            ):
                raise EpisodeSchemaError(
                    f"Decision {name} is not allowed by projection policy"
                )
            row[name] = item
        row["summary"] = _validate_summary(
            decision["summary"],
            projection_document.get("max_scalar_codepoints"),
        )
        rows.append(MappingProxyType(row))
    return tuple(rows)


def parse_v2_supplement(
    text: str,
    projection: Mapping,
) -> EpisodeV2Supplement:
    if not isinstance(text, str):
        raise EpisodeSchemaError("Episode supplement must be UTF-8 text")
    parsed = _strict_json(text, "Episode supplement")
    supplement = _exact_mapping(parsed, _SUPPLEMENT_KEYS, "Episode supplement")
    if type(supplement["schema_version"]) is not int or supplement["schema_version"] != 2:
        raise EpisodeSchemaError("Episode supplement schema_version must be 2")
    projection_document = _projection_document(projection)
    execution = _validate_execution(supplement["execution"], projection_document)
    quality = _validate_supplement_quality(supplement["quality"])
    decisions = _validate_decisions(
        supplement["decisions"],
        projection_document,
        require_list=True,
    )
    return EpisodeV2Supplement(
        schema_version=2,
        execution=MappingProxyType(execution),
        quality=MappingProxyType(quality),
        decisions=decisions,
    )


def _validated_completion_metrics(value: Mapping) -> dict[str, object]:
    metrics = _exact_mapping(
        value,
        _LIFECYCLE_QUALITY_KEYS,
        "completion_metrics",
    )
    verification = metrics["verification"]
    if not isinstance(verification, str) or verification not in _VERIFICATION_VALUES:
        raise EpisodeSchemaError("verification is not a supported lifecycle value")
    return {
        "verification": verification,
        "review_rounds": _quality_integer_or_unknown(
            metrics["review_rounds"], "review_rounds"
        ),
        "defects_found": _quality_integer_or_unknown(
            metrics["defects_found"], "defects_found"
        ),
        "rework_count": _quality_integer_or_unknown(
            metrics["rework_count"], "rework_count"
        ),
    }


def build_episode_v2(
    *,
    elapsed_seconds: int,
    completion_metrics: Mapping,
    supplement: EpisodeV2Supplement,
) -> dict:
    elapsed = _safe_nonnegative_integer(elapsed_seconds, "elapsed_seconds")
    if not isinstance(supplement, EpisodeV2Supplement) or supplement.schema_version != 2:
        raise EpisodeSchemaError("supplement must be a validated EpisodeV2Supplement")
    execution = _exact_mapping(
        supplement.execution,
        _EXECUTION_KEYS,
        "supplement execution",
    )
    supplement_quality = _exact_mapping(
        supplement.quality,
        _SUPPLEMENT_QUALITY_KEYS,
        "supplement quality",
    )
    lifecycle_quality = _validated_completion_metrics(completion_metrics)
    return {
        "schema_version": 2,
        "execution": dict(execution),
        "quality": {
            **dict(supplement_quality),
            "elapsed_seconds": elapsed,
            **lifecycle_quality,
        },
        "decisions": [dict(decision) for decision in supplement.decisions],
    }


def render_episode_block(data: Mapping) -> str:
    episode = _exact_mapping(data, _SUPPLEMENT_KEYS, "Episode data")
    if type(episode["schema_version"]) is not int or episode["schema_version"] != 2:
        raise EpisodeSchemaError("Episode data schema_version must be 2")
    encoded = _canonical_bytes(dict(episode), "Episode data").decode("utf-8")
    return f"{_EPISODE_HEADING}\n\n```json\n{encoded}\n```\n"


def _validate_episode_v2(
    value: object,
    projection_document: Mapping[str, object],
) -> dict[str, object]:
    episode = _exact_mapping(value, _SUPPLEMENT_KEYS, "Episode data")
    if type(episode["schema_version"]) is not int or episode["schema_version"] != 2:
        raise EpisodeSchemaError("Episode data schema_version must be 2")
    execution = _validate_execution(episode["execution"], projection_document)
    quality = _exact_mapping(
        episode["quality"],
        _EPISODE_QUALITY_KEYS,
        "Episode quality",
    )
    validated_quality: dict[str, object] = {
        "elapsed_seconds": _safe_nonnegative_integer(
            quality["elapsed_seconds"], "elapsed_seconds"
        ),
    }
    validated_quality.update(_validated_completion_metrics({
        key: quality[key] for key in _LIFECYCLE_QUALITY_KEYS
    }))
    validated_quality.update(_validate_supplement_quality({
        key: quality[key] for key in _SUPPLEMENT_QUALITY_KEYS
    }))
    decisions = _validate_decisions(
        episode["decisions"],
        projection_document,
        require_list=True,
    )
    return {
        "schema_version": 2,
        "execution": execution,
        "quality": validated_quality,
        "decisions": [dict(decision) for decision in decisions],
    }


def parse_episode_block(
    body: str,
    projection: Mapping,
) -> tuple[str, dict | None]:
    if not isinstance(body, str):
        raise EpisodeSchemaError("observation body must be text")
    headings = list(re.finditer(r"(?m)^## Episode data$", body))
    if not headings:
        if re.search(r"(?m)^## Episode data[\t \r]*$", body):
            raise EpisodeSchemaError(
                "Episode data block must be final and exactly formatted"
            )
        return body, None
    if len(headings) != 1:
        raise EpisodeSchemaError("Episode data block must occur exactly once")
    match = _EPISODE_BLOCK_PATTERN.search(body)
    if match is None:
        raise EpisodeSchemaError("Episode data block must be final and exactly formatted")
    original = match.group(1).encode("utf-8")
    parsed = _strict_json(original, "Episode data")
    canonical = _canonical_bytes(parsed, "Episode data")
    if canonical != original:
        raise EpisodeSchemaError("Episode data JSON must use canonical encoding")
    projection_document = _projection_document(projection)
    episode = _validate_episode_v2(parsed, projection_document)
    return body[:match.start()], episode


def _canonical_utc_instant(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise EpisodeSchemaError(f"{label} must be an aware ISO-8601 instant")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise _error(f"{label} must be an aware ISO-8601 instant", error)
    if parsed.tzinfo is None or parsed.utcoffset() is None or parsed.microsecond != 0:
        raise EpisodeSchemaError(
            f"{label} must be an aware second-precision ISO-8601 instant"
        )
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_human_lifecycle(
    body: str,
    status: str,
) -> tuple[object | None, dict[str, object]]:
    if status == "draft":
        try:
            parse_scope_payload(body)
        except ObservationError as error:
            raise _error(f"draft observation body: {error}", error)
        return None, {}
    marker = "\n## Execution evidence"
    if marker not in body:
        raise EpisodeSchemaError("final observation is missing completion sections")
    scope, completion_tail = body.split(marker, 1)
    try:
        parse_scope_payload(scope.rstrip() + "\n")
        return _parse_completion(
            "## Execution evidence" + completion_tail,
            allow_derived=True,
        )
    except ObservationError as error:
        raise _error(f"final observation body: {error}", error)


def _projected_metric(
    *,
    schema_supported: bool,
    value: object,
    unit: str | None = None,
    enum_unknown_is_observed: bool = False,
) -> dict[str, object]:
    if not schema_supported:
        return {
            "availability": "unsupported_by_schema",
            "value": None,
            "unit": None,
        }
    if value is None or (value == "unknown" and not enum_unknown_is_observed):
        return {"availability": "not_recorded", "value": None, "unit": None}
    return {"availability": "observed", "value": value, "unit": unit}


def _metadata_string(metadata: Mapping, name: str) -> str:
    value = metadata.get(name)
    if not isinstance(value, str) or not value:
        raise EpisodeSchemaError(f"metadata {name} must be a non-empty string")
    return value


def _project_workflow_generation(
    metadata: Mapping,
    schema_version: int,
) -> dict[str, object]:
    if schema_version == 1:
        if "workflow_generation" in metadata:
            raise EpisodeSchemaError(
                "schema-v1 metadata cannot contain workflow_generation"
            )
        return {"availability": "unavailable", "value": None}
    if "workflow_generation" not in metadata:
        return {"availability": "unavailable", "value": None}
    value = metadata["workflow_generation"]
    if (
        not isinstance(value, str)
        or _WORKFLOW_GENERATION_PATTERN.fullmatch(value) is None
        or value in {"unknown", "unavailable"}
    ):
        raise EpisodeSchemaError("workflow_generation metadata is invalid")
    return {"availability": "observed", "value": value}


def canonical_episode_projection(
    metadata: Mapping,
    body: str,
    projection: Mapping,
) -> dict:
    if not isinstance(metadata, Mapping):
        raise EpisodeSchemaError("observation metadata must be a mapping")
    projection_document = _projection_document(projection)
    human_body, episode = parse_episode_block(body, projection)
    status = _metadata_string(metadata, "status")
    if status not in FINAL_STATUSES | {"draft"}:
        raise EpisodeSchemaError("observation status is invalid")
    if status == "draft" and episode is not None:
        raise EpisodeSchemaError("draft observations cannot contain Episode data")

    completion, derived = _parse_human_lifecycle(human_body, status)
    started_at = _canonical_utc_instant(metadata.get("timestamp"), "started_at")
    finished_at = None
    lifecycle_values: dict[str, object] = {}
    if status != "draft":
        assert completion is not None
        finished_at = _canonical_utc_instant(
            derived.get("finished_at"), "finished_at"
        )
        started = datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        finished = datetime.strptime(
            finished_at, "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        elapsed = _safe_nonnegative_integer(
            derived.get("elapsed_seconds"), "elapsed_seconds"
        )
        if elapsed != int((finished - started).total_seconds()) or finished < started:
            raise EpisodeSchemaError(
                "elapsed_seconds must equal the non-negative lifecycle duration"
            )
        lifecycle_values = {
            "elapsed_seconds": elapsed,
            "verification": completion.verification,
            "review_rounds": _quality_integer_or_unknown(
                completion.review_rounds, "review_rounds"
            ),
            "defects_found": _quality_integer_or_unknown(
                completion.defects_found, "defects_found"
            ),
            "rework_count": _quality_integer_or_unknown(
                completion.rework_count, "rework_count"
            ),
        }

    schema_version = 2 if episode is not None else 1
    if episode is not None:
        for name, value in lifecycle_values.items():
            if episode["quality"][name] != value:
                raise EpisodeSchemaError(
                    f"Episode {name} does not match authoritative lifecycle completion"
                )

    capabilities = projection_document["schema_capabilities"]
    assert isinstance(capabilities, Mapping)
    schema_capability = capabilities.get(str(schema_version))
    if not isinstance(schema_capability, Mapping):
        raise EpisodeSchemaError("Episode schema capability is not defined")
    metric_capabilities = schema_capability.get("metrics")
    if not isinstance(metric_capabilities, Mapping):
        raise EpisodeSchemaError("Episode metric capabilities are not defined")

    metric_values = dict(lifecycle_values)
    cost_currency = None
    decisions: list[dict[str, object]] = []
    if episode is not None:
        metric_values.update({
            name: episode["execution"][name]
            for name in (
                "input_tokens",
                "output_tokens",
                "cache_read_tokens",
                "cost_amount",
            )
        })
        metric_values.update({
            name: episode["quality"][name]
            for name in ("test_failures", "timeout_count")
        })
        cost_currency = episode["execution"]["cost_currency"]
        decisions = [dict(decision) for decision in episode["decisions"]]

    metrics = {}
    for name, supported in metric_capabilities.items():
        if not isinstance(name, str) or not isinstance(supported, bool):
            raise EpisodeSchemaError("Episode metric capability is invalid")
        metrics[name] = _projected_metric(
            schema_supported=supported,
            value=metric_values.get(name),
            unit=cost_currency if name == "cost_amount" else None,
            enum_unknown_is_observed=name == "verification",
        )

    workflow_variant = _metadata_string(metadata, "workflow_variant")
    return {
        "run_id": _metadata_string(metadata, "run_id"),
        "episode_schema_version": schema_version,
        "started_at": started_at,
        "finished_at": finished_at,
        "project": _metadata_string(metadata, "project"),
        "workspace": _metadata_string(metadata, "workspace"),
        "workspace_id": _metadata_string(metadata, "workspace_id"),
        "revision": _metadata_string(metadata, "revision"),
        "working_tree": _metadata_string(metadata, "working_tree"),
        "agent_surface": _metadata_string(metadata, "agent_surface"),
        "task_type": _metadata_string(metadata, "task_type"),
        "workflow_variant": workflow_variant,
        "workflow_generation": _project_workflow_generation(
            metadata,
            schema_version,
        ),
        "status": status,
        "metrics": metrics,
        "runtime_provenance": None,
        "decisions": decisions,
    }
