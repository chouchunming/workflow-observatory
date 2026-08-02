from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
from pathlib import PurePosixPath
import re
import stat

from canonical_json import (
    CanonicalizationError,
    canonicalize,
    hash_canonical,
    strict_json_loads,
)


_UTC_INSTANT_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"
)
_PRODUCER_GENERATION_PATTERN = re.compile(
    r"[a-z0-9][a-z0-9._:@+\-]{0,199}"
)
_DRIVE_PATH_PATTERN = re.compile(r"[A-Za-z]:")
_HAS_OPEN_DIR_FD = hasattr(os, "supports_dir_fd") and os.open in os.supports_dir_fd
_HAS_STAT_DIR_FD = hasattr(os, "supports_dir_fd") and os.stat in os.supports_dir_fd
_HAS_STAT_NOFOLLOW = (
    hasattr(os, "supports_follow_symlinks")
    and os.stat in os.supports_follow_symlinks
)
_POLICY_FILES = {
    "episode_projection": (
        "episode_projection.json",
        "canonical_projection_contract",
        "episode-projection@2",
        {
            "schema_version",
            "version",
            "max_decisions",
            "max_scalar_codepoints",
            "enumerations",
            "schema_capabilities",
        },
    ),
    "producer_capabilities": (
        "producer_capabilities.json",
        "producer_capability_registry",
        "producer-capabilities@1",
        {"schema_version", "version", "entries"},
    ),
    "workflow_generation_mapping": (
        "workflow_generation_mapping.json",
        "workflow_generation_mapping",
        "workflow-generation-mapping@1",
        {"schema_version", "version", "mapping"},
    ),
    "metric_semantics": (
        "metric_semantics.json",
        "metric_semantics_registry",
        "metric-semantics@1",
        {"schema_version", "version", "metrics", "not_applicable_rules"},
    ),
    "quantile_policy": (
        "quantile_policy.json",
        "quantile_policy",
        "linear-rational-quantile@1",
        {"schema_version", "version", "quantiles"},
    ),
    "decision_support_policy": (
        "decision_support_policy.json",
        "decision_support_policy",
        "decision-pattern-support@1",
        {
            "schema_version",
            "version",
            "pattern_kinds",
            "event_key_fields",
            "decision_min_episode_support",
            "decision_min_support_ratio",
            "decision_recurring_minimum_outcome_episodes",
        },
    ),
    "lifecycle_health_policy": (
        "lifecycle_health_policy.json",
        "lifecycle_health_policy",
        "draft-staleness@1",
        {"schema_version", "version", "draft_stale_after_seconds"},
    ),
    "candidate_emission_policy": (
        "candidate_emission_policy.json",
        "candidate_emission_policy",
        "candidate-emission@1",
        {
            "schema_version",
            "version",
            "candidate_classes",
            "candidate_order",
            "candidate_ranking",
            "rules",
        },
    ),
}
_METRIC_KEYS = {
    "semantics_id",
    "value_type",
    "aggregation",
    "candidate_type",
}
_CANDIDATE_RULE_KEYS = {
    "candidate_type",
    "class",
    "source_kind",
    "source",
    "predicate",
    "minimum_denominator",
    "cardinality",
    "evidence_fields",
    "policy_identity_keys",
}
_POLICY_IDENTITY_KEYS = {
    item[1] for item in _POLICY_FILES.values()
}


class PolicyError(ValueError):
    pass


@dataclass(frozen=True)
class RegularFileEvidence:
    content: bytes
    sha256: str
    executable: bool
    device: int
    inode: int


@dataclass(frozen=True)
class PolicySet:
    documents: Mapping[str, Mapping[str, object]]
    identities: Mapping[str, Mapping[str, str]]

    def core_identity(self) -> dict[str, dict[str, str]]:
        return {
            name: dict(self.identities[name]) for name in sorted(self.identities)
        }


def _policy_error(message: str, error: Exception | None = None) -> PolicyError:
    result = PolicyError(message)
    if error is not None:
        result.__cause__ = error
    return result


def _canonical_utc_instant(value: object, label: str) -> datetime:
    if not isinstance(value, str) or _UTC_INSTANT_PATTERN.fullmatch(value) is None:
        raise PolicyError(f"{label} must be a second-precision UTC Z instant")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise _policy_error(
            f"{label} must be a second-precision UTC Z instant", error
        )
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise PolicyError(f"{label} is not a canonical UTC instant")
    return parsed


def _producer_generation(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _PRODUCER_GENERATION_PATTERN.fullmatch(value) is None
        or value in {"unknown", "unavailable"}
    ):
        raise PolicyError(f"{label} is not a valid producer generation")
    return value


def validate_relative_posix_artifact_path(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyError("artifact member path must be a non-empty string")
    if "\0" in value or "\\" in value:
        raise PolicyError("artifact member path contains a prohibited character")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise _policy_error("artifact member path is not valid UTF-8", error)
    if encoded.decode("utf-8") != value:
        raise PolicyError("artifact member path is not normalized UTF-8")
    if value.startswith("/") or _DRIVE_PATH_PATTERN.match(value):
        raise PolicyError("artifact member path must be relative")
    components = value.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise PolicyError("artifact member path contains an unsafe component")
    normalized = str(PurePosixPath(*components))
    if normalized != value:
        raise PolicyError("artifact member path is not normalized POSIX spelling")
    return normalized


def validate_effective_boundary(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"type", "from"}:
        raise PolicyError("effective boundary must have exactly type and from")
    boundary_type = value["type"]
    boundary_from = value["from"]
    if boundary_type == "started_at":
        _canonical_utc_instant(boundary_from, "effective boundary from")
    elif boundary_type == "producer_generation":
        _producer_generation(boundary_from, "effective boundary from")
    else:
        raise PolicyError("effective boundary type is not supported")
    return {"type": boundary_type, "from": boundary_from}


def effective_boundary_applies(
    boundary: Mapping,
    *,
    started_at: str,
    producer_generation: str | None,
) -> bool:
    validated = validate_effective_boundary(boundary)
    episode_started_at = _canonical_utc_instant(started_at, "started_at")
    if validated["type"] == "started_at":
        effective_from = _canonical_utc_instant(
            validated["from"], "effective boundary from"
        )
        return episode_started_at >= effective_from
    if producer_generation is None:
        return False
    explicit_generation = _producer_generation(
        producer_generation, "producer_generation"
    )
    return explicit_generation == validated["from"]


def _ensure_descriptor_capabilities() -> None:
    required_flags = ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC")
    if any(not hasattr(os, name) for name in required_flags):
        raise PolicyError("descriptor-bound no-follow reads are unsupported")
    if (
        not _HAS_OPEN_DIR_FD
        or not _HAS_STAT_DIR_FD
        or not _HAS_STAT_NOFOLLOW
    ):
        raise PolicyError("descriptor-bound no-follow reads are unsupported")


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _open_verified_directory(parent_fd: int, component: str) -> int:
    try:
        before = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as error:
        raise _policy_error("artifact directory component cannot be inspected", error)
    if not stat.S_ISDIR(before.st_mode):
        raise PolicyError("artifact directory component is not a directory")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        descriptor = os.open(component, flags, dir_fd=parent_fd)
    except OSError as error:
        raise _policy_error("artifact directory component cannot be opened", error)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISDIR(opened.st_mode) or _stat_identity(before) != _stat_identity(
            opened
        ):
            raise PolicyError("artifact changed during read")
    except Exception:
        os.close(descriptor)
        raise
    return descriptor


def read_regular_file_evidence(
    root: Path,
    relative_posix_path: str,
    *,
    max_bytes: int,
) -> RegularFileEvidence:
    normalized = validate_relative_posix_artifact_path(relative_posix_path)
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0:
        raise PolicyError("maximum artifact bytes must be a non-negative integer")
    _ensure_descriptor_capabilities()
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        current_fd = os.open(os.fspath(root), directory_flags)
    except (OSError, TypeError) as error:
        raise _policy_error("artifact root cannot be opened securely", error)
    try:
        components = normalized.split("/")
        for component in components[:-1]:
            next_fd = _open_verified_directory(current_fd, component)
            os.close(current_fd)
            current_fd = next_fd

        final_component = components[-1]
        try:
            before = os.stat(
                final_component,
                dir_fd=current_fd,
                follow_symlinks=False,
            )
        except OSError as error:
            raise _policy_error("artifact member cannot be inspected", error)
        if not stat.S_ISREG(before.st_mode):
            raise PolicyError("artifact member is not a regular file")

        file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            file_fd = os.open(final_component, file_flags, dir_fd=current_fd)
        except OSError as error:
            raise _policy_error("artifact member cannot be opened securely", error)
        try:
            opened = os.fstat(file_fd)
            if (
                not stat.S_ISREG(opened.st_mode)
                or _stat_identity(before) != _stat_identity(opened)
            ):
                raise PolicyError("artifact changed during read")
            if opened.st_size > max_bytes:
                raise PolicyError("artifact exceeds maximum byte size")
            try:
                content = os.read(file_fd, max_bytes + 1)
            except OSError as error:
                raise _policy_error("artifact member cannot be read", error)
            after = os.fstat(file_fd)
            if _stat_identity(opened) != _stat_identity(after):
                raise PolicyError("artifact changed during read")
            if len(content) > max_bytes:
                raise PolicyError("artifact exceeds maximum byte size")
            if len(content) != opened.st_size:
                raise PolicyError("artifact produced a short read")
            return RegularFileEvidence(
                content=content,
                sha256=hashlib.sha256(content).hexdigest(),
                executable=bool(opened.st_mode & 0o111),
                device=opened.st_dev,
                inode=opened.st_ino,
            )
        finally:
            os.close(file_fd)
    finally:
        os.close(current_fd)


def build_code_manifest(root: Path, relative_paths: Sequence[str]) -> dict:
    rows = []
    seen = set()
    for raw in relative_paths:
        normalized = validate_relative_posix_artifact_path(raw)
        if normalized in seen:
            raise PolicyError("artifact member path is duplicated")
        seen.add(normalized)
        evidence = read_regular_file_evidence(
            root,
            normalized,
            max_bytes=1_048_576,
        )
        rows.append({
            "path": normalized,
            "sha256": evidence.sha256,
            "executable": evidence.executable,
        })
    rows.sort(key=lambda row: row["path"].encode("utf-8"))
    return {"schema_version": 1, "files": rows}


def _require_exact_keys(
    value: object,
    expected: set[str],
    label: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise PolicyError(f"{label} does not have its exact allowed keys")
    return value


def _require_nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyError(f"{label} must be a non-empty string")
    return value


def _require_positive_integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PolicyError(f"{label} must be a positive integer")
    return value


def _require_unique_string_list(value: object, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
        or len(value) != len(set(value))
    ):
        raise PolicyError(f"{label} must be a non-empty unique string list")
    return value


def _validate_projection(document: Mapping[str, object]) -> None:
    _require_positive_integer(document["max_decisions"], "episode_projection.max_decisions")
    _require_positive_integer(
        document["max_scalar_codepoints"],
        "episode_projection.max_scalar_codepoints",
    )
    enumerations = _require_exact_keys(
        document["enumerations"],
        {
            "measurement_source",
            "phase",
            "actor_role",
            "decision_type",
            "reason_code",
            "result",
        },
        "episode_projection.enumerations",
    )
    for name, values in enumerations.items():
        _require_unique_string_list(values, f"episode_projection.enumerations.{name}")
    capabilities = _require_exact_keys(
        document["schema_capabilities"],
        {"1", "2"},
        "episode_projection.schema_capabilities",
    )
    for version, value in capabilities.items():
        row = _require_exact_keys(
            value,
            {"metrics", "decisions"},
            f"episode_projection.schema_capabilities.{version}",
        )
        if not isinstance(row["decisions"], bool):
            raise PolicyError("episode_projection decisions capability must be Boolean")
        if (
            not isinstance(row["metrics"], Mapping)
            or not row["metrics"]
            or any(
                not isinstance(name, str) or not isinstance(supported, bool)
                for name, supported in row["metrics"].items()
            )
        ):
            raise PolicyError("episode_projection metric capabilities are invalid")


def _validate_metric_semantics(document: Mapping[str, object]) -> None:
    metrics = document["metrics"]
    if not isinstance(metrics, Mapping) or not metrics:
        raise PolicyError("metric_semantics.metrics must be a non-empty object")
    for name, raw in metrics.items():
        if not isinstance(name, str) or not name:
            raise PolicyError("metric_semantics metric name is invalid")
        row = _require_exact_keys(raw, _METRIC_KEYS, f"metric_semantics.metrics.{name}")
        _require_nonempty_string(row["semantics_id"], f"metric_semantics.metrics.{name}.semantics_id")
        if (
            not isinstance(row["value_type"], str)
            or row["value_type"]
            not in {
                "nonnegative-integer",
                "enum",
                "normalized-decimal-string",
            }
        ):
            raise PolicyError(f"metric_semantics metric {name} has invalid value_type")
        if (
            not isinstance(row["aggregation"], str)
            or row["aggregation"]
            not in {
                "integer-quantiles",
                "category-counts",
                "missingness-only",
            }
        ):
            raise PolicyError(f"metric_semantics metric {name} has invalid aggregation")
        if row["candidate_type"] is not None:
            _require_nonempty_string(
                row["candidate_type"],
                f"metric_semantics.metrics.{name}.candidate_type",
            )
    if document["not_applicable_rules"] != []:
        raise PolicyError("metric_semantics.not_applicable_rules is not closed")


def _validate_candidate_policy(
    document: Mapping[str, object],
    metrics: Mapping[str, object],
) -> None:
    classes = _require_unique_string_list(
        document["candidate_classes"],
        "candidate_emission_policy.candidate_classes",
    )
    _require_nonempty_string(
        document["candidate_order"],
        "candidate_emission_policy.candidate_order",
    )
    _require_nonempty_string(
        document["candidate_ranking"],
        "candidate_emission_policy.candidate_ranking",
    )
    rules = document["rules"]
    if not isinstance(rules, list) or not rules:
        raise PolicyError("candidate_emission_policy.rules must be a non-empty list")
    candidate_types = []
    indexed_rules = {}
    for index, raw in enumerate(rules):
        row = _require_exact_keys(
            raw,
            _CANDIDATE_RULE_KEYS,
            f"candidate_emission_policy.rules[{index}]",
        )
        for key in (
            "candidate_type",
            "source",
            "predicate",
            "minimum_denominator",
            "cardinality",
        ):
            _require_nonempty_string(
                row[key], f"candidate_emission_policy.rules[{index}].{key}"
            )
        if row["class"] not in classes:
            raise PolicyError("candidate_emission_policy rule class is unknown")
        if row["source_kind"] not in {"metric", "decision", "lifecycle", "outcome"}:
            raise PolicyError("candidate_emission_policy source_kind is unknown")
        if (
            row["source_kind"] == "metric"
            and row["source"] != "any-metric"
            and row["source"] not in metrics
        ):
            raise PolicyError("candidate_emission_policy metric source is unknown")
        _require_unique_string_list(
            row["evidence_fields"],
            f"candidate_emission_policy.rules[{index}].evidence_fields",
        )
        identity_keys = _require_unique_string_list(
            row["policy_identity_keys"],
            f"candidate_emission_policy.rules[{index}].policy_identity_keys",
        )
        if not set(identity_keys) <= _POLICY_IDENTITY_KEYS:
            raise PolicyError("candidate_emission_policy policy identity key is unknown")
        candidate_type = row["candidate_type"]
        candidate_types.append(candidate_type)
        indexed_rules[candidate_type] = row
    if len(candidate_types) != len(set(candidate_types)):
        raise PolicyError("candidate_emission_policy candidate_type is duplicated")
    if candidate_types != sorted(candidate_types, key=lambda item: item.encode("utf-8")):
        raise PolicyError("candidate_emission_policy rules are not sorted")
    for metric_name, metric in metrics.items():
        candidate_type = metric["candidate_type"]
        if candidate_type is None:
            continue
        rule = indexed_rules.get(candidate_type)
        if rule is None or rule["source_kind"] != "metric" or rule["source"] != metric_name:
            raise PolicyError("candidate_emission_policy does not close metric candidates")


def _validate_policy_documents(documents: Mapping[str, Mapping[str, object]]) -> None:
    _validate_projection(documents["episode_projection"])
    producer_entries = documents["producer_capabilities"]["entries"]
    if producer_entries != []:
        raise PolicyError("producer_capabilities entries are not defined in v0.2")
    generation_mapping = documents["workflow_generation_mapping"]["mapping"]
    if generation_mapping != {}:
        raise PolicyError("workflow_generation_mapping entries are not defined in v0.2")
    _validate_metric_semantics(documents["metric_semantics"])
    projection_metrics = documents["episode_projection"]["schema_capabilities"]
    expected_metric_names = set(documents["metric_semantics"]["metrics"])
    for version, capabilities in projection_metrics.items():
        if set(capabilities["metrics"]) != expected_metric_names:
            raise PolicyError(
                f"episode_projection schema {version} does not cover every metric"
            )
    quantiles = _require_unique_string_list(
        documents["quantile_policy"]["quantiles"],
        "quantile_policy.quantiles",
    )
    if any(re.fullmatch(r"0\.[0-9]+", value) is None for value in quantiles):
        raise PolicyError("quantile_policy quantile is not a normalized decimal")
    decision = documents["decision_support_policy"]
    _require_unique_string_list(
        decision["pattern_kinds"], "decision_support_policy.pattern_kinds"
    )
    _require_unique_string_list(
        decision["event_key_fields"], "decision_support_policy.event_key_fields"
    )
    _require_positive_integer(
        decision["decision_min_episode_support"],
        "decision_support_policy.decision_min_episode_support",
    )
    if (
        not isinstance(decision["decision_min_support_ratio"], str)
        or re.fullmatch(
            r"(?:0\.[0-9]*[1-9][0-9]*|1(?:\.0+)?)",
            decision["decision_min_support_ratio"],
        )
        is None
    ):
        raise PolicyError("decision_support_policy minimum ratio is invalid")
    _require_positive_integer(
        decision["decision_recurring_minimum_outcome_episodes"],
        "decision_support_policy.decision_recurring_minimum_outcome_episodes",
    )
    _require_positive_integer(
        documents["lifecycle_health_policy"]["draft_stale_after_seconds"],
        "lifecycle_health_policy.draft_stale_after_seconds",
    )
    _validate_candidate_policy(
        documents["candidate_emission_policy"],
        documents["metric_semantics"]["metrics"],
    )


def _canonical_sha256(value: object) -> str:
    return "sha256:" + hashlib.sha256(canonicalize(value)).hexdigest()


def load_policy_set(
    policy_root: Path,
    analyzer_files: Sequence[str],
    canonicalizer_files: Sequence[str],
) -> PolicySet:
    documents = {}
    for name, (filename, _identity, expected_version, allowed_keys) in _POLICY_FILES.items():
        evidence = read_regular_file_evidence(
            policy_root,
            filename,
            max_bytes=1_048_576,
        )
        try:
            parsed = strict_json_loads(evidence.content)
        except CanonicalizationError as error:
            raise _policy_error(f"{name}: {error}", error)
        document = _require_exact_keys(parsed, allowed_keys, name)
        if document["schema_version"] != 1:
            raise PolicyError(f"{name} schema_version is not supported")
        if document["version"] != expected_version:
            raise PolicyError(f"{name} version is not supported")
        documents[name] = document

    _validate_policy_documents(documents)
    plugin_root = policy_root.parent
    analyzer_manifest = build_code_manifest(plugin_root, analyzer_files)
    canonicalizer_manifest = build_code_manifest(plugin_root, canonicalizer_files)
    identities = {
        "analyzer_artifact": {
            "version": "workflow-learning-analyzer@0.2.0",
            "sha256": _canonical_sha256(analyzer_manifest),
        },
        "canonicalizer_artifact": {
            "version": "rfc8785-jcs@1",
            "sha256": _canonical_sha256(canonicalizer_manifest),
        },
    }
    for name, (_filename, identity_name, _version, _allowed_keys) in _POLICY_FILES.items():
        document = documents[name]
        identities[identity_name] = {
            "version": document["version"],
            "sha256": _canonical_sha256(document),
        }
    return PolicySet(documents=documents, identities=identities)
