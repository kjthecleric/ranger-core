"""Configuration loader — parse YAML, resolve secrets, validate with Pydantic.

Usage::

    config = load_config("pipeline.yaml")
    # config is a validated PipelineConfig instance
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ranger.core.models import PipelineConfig


# Pattern to match secret references like ${env.DB_PASSWORD} or ${secrets.vault.key}
_SECRET_REF_PATTERN = re.compile(r"\$\{(?P<ref>[^}]+)\}")


def load_config(path: str | Path) -> PipelineConfig:
    """Load and validate a pipeline configuration from a YAML file.

    Args:
        path: Path to the YAML config file.

    Returns:
        Validated PipelineConfig instance.

    Raises:
        FileNotFoundError: If the config file doesn't exist.
        ValueError: If the YAML is invalid or doesn't pass Pydantic validation.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected a YAML mapping at the root of {path}")

    # Resolve secret / env var references throughout the config tree
    resolved = _resolve_refs(raw)

    try:
        config = PipelineConfig.model_validate(resolved)
    except ValidationError as exc:
        raise ValueError(f"Invalid pipeline configuration in {path}:\n{exc}") from exc

    return config


def load_config_from_dict(data: dict[str, Any]) -> PipelineConfig:
    """Load and validate a pipeline config from a dictionary (e.g., from the API).

    Args:
        data: Raw config dictionary.

    Returns:
        Validated PipelineConfig instance.
    """
    resolved = _resolve_refs(data)
    return PipelineConfig.model_validate(resolved)


def validate_config(path: str | Path) -> list[str]:
    """Validate a config file without executing it.

    Returns:
        List of validation error messages (empty if valid).
    """
    errors: list[str] = []
    try:
        load_config(path)
    except FileNotFoundError as exc:
        errors.append(str(exc))
    except ValueError as exc:
        errors.append(str(exc))
    return errors


# ---------------------------------------------------------------------------
# Secret / env var resolution
# ---------------------------------------------------------------------------


def _resolve_refs(obj: Any) -> Any:
    """Recursively resolve ${...} references in the config tree.

    Supported reference formats:
        ${env.VAR_NAME}             — environment variable
        ${secrets.vault.key_name}   — Vault secret (placeholder, requires provider)
        ${secrets.aws.secret_name}  — AWS secret (placeholder, requires provider)
    """
    if isinstance(obj, str):
        return _resolve_string(obj)
    if isinstance(obj, dict):
        return {k: _resolve_refs(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_refs(item) for item in obj]
    return obj


def _resolve_string(value: str) -> str:
    """Resolve all ${...} references within a single string value."""

    def _replacer(match: re.Match[str]) -> str:
        ref = match.group("ref")

        # Environment variable: ${env.VAR_NAME}
        if ref.startswith("env."):
            var_name = ref[4:]
            env_val = os.environ.get(var_name)
            if env_val is None:
                raise ValueError(
                    f"Environment variable '{var_name}' referenced in config but not set"
                )
            return env_val

        # Secret provider references: ${secrets.<provider>.<key>}
        # These are resolved at runtime by the secret provider layer.
        # For now, return the reference as-is — the pipeline executor
        # will resolve them via the configured secret provider.
        if ref.startswith("secrets."):
            return match.group(0)  # keep as-is for lazy resolution

        raise ValueError(f"Unknown config reference format: ${{{ref}}}")

    return _SECRET_REF_PATTERN.sub(_replacer, value)
