"""Local storage-adapter configuration for Workflow Observatory."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Literal, Mapping, Any


class ConfigError(ValueError):
    """Raised when local Workflow Observatory configuration is invalid."""


@dataclass(frozen=True)
class StoreConfig:
    adapter: Literal["portable", "llmwiki"]
    root: Path
    cli_path: Path | None


def _absolute_path(value: Any, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{field} must be a non-empty path string")
    if "\0" in value:
        raise ConfigError(f"{field} contains a null byte")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ConfigError(f"{field} must be absolute")
    return path


def _reject_unknown_keys(config: Mapping[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ConfigError(f"unknown config keys: {', '.join(unknown)}")


def parse_store_config(config: Mapping[str, Any]) -> StoreConfig:
    """Validate and normalize a decoded local configuration object."""
    if not isinstance(config, Mapping):
        raise ConfigError("config must be a JSON object")
    if any(not isinstance(key, str) for key in config):
        raise ConfigError("config keys must be strings")
    if type(config.get("schema_version")) is not int or config["schema_version"] != 1:
        raise ConfigError("schema_version must be 1")

    adapter = config.get("adapter")
    if not isinstance(adapter, str) or adapter not in {"portable", "llmwiki"}:
        raise ConfigError(f"unsupported adapter: {adapter!r}")

    if adapter == "portable":
        _reject_unknown_keys(config, {"schema_version", "adapter", "root"})
        if "root" not in config:
            raise ConfigError("portable adapter requires root")
        root = _absolute_path(config["root"], "root")
        return StoreConfig("portable", root, None)

    _reject_unknown_keys(
        config, {"schema_version", "adapter", "cli_path", "wiki_root"}
    )
    if "cli_path" not in config:
        raise ConfigError("llmwiki adapter requires cli_path")
    if "wiki_root" not in config:
        raise ConfigError("llmwiki adapter requires wiki_root")

    cli_path = _absolute_path(config["cli_path"], "cli_path")
    wiki_root = _absolute_path(config["wiki_root"], "wiki_root")
    if not cli_path.exists():
        raise ConfigError(f"cli_path does not exist: {cli_path}")
    if not wiki_root.exists():
        raise ConfigError(f"wiki_root does not exist: {wiki_root}")

    resolved_cli = cli_path.resolve(strict=True)
    resolved_root = wiki_root.resolve(strict=True)
    if not resolved_cli.is_file():
        raise ConfigError(f"cli_path is not a file: {cli_path}")
    if not resolved_root.is_dir():
        raise ConfigError(f"wiki_root is not a directory: {wiki_root}")
    try:
        resolved_cli.relative_to(resolved_root)
    except ValueError as error:
        raise ConfigError("cli_path escapes wiki_root") from error

    return StoreConfig("llmwiki", resolved_root, resolved_cli)


def load_store_config(home=None, environ=None) -> StoreConfig:
    """Load local configuration, defaulting to the portable adapter."""
    env = dict(os.environ if environ is None else environ)
    base = Path(
        env.get(
            "WORKFLOW_OBSERVATORY_HOME",
            home or Path.home() / ".codex/workflow-observatory",
        )
    ).expanduser()
    path = base / "config.json"
    if not path.exists():
        return StoreConfig("portable", base / "store", None)
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ConfigError(f"invalid config JSON: {error}") from error
    return parse_store_config(decoded)
