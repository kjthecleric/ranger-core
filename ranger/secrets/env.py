"""Environment-variable secret provider for Ranger.

Zero-dependency provider that reads secrets from ``os.environ`` with
an optional key prefix.
"""

from __future__ import annotations

import os

import structlog

from ranger.secrets.base import BaseSecretProvider

logger = structlog.get_logger(__name__)


class EnvSecretProvider(BaseSecretProvider):
    """Resolve secrets from environment variables.

    All environment variables whose name starts with *prefix* are considered
    managed secrets.  When ``get_secret(key)`` is called, the provider looks
    up ``{prefix}{key}`` in ``os.environ``.

    Config keys:
        prefix: Environment-variable prefix (default ``"RANGER_"``).
                Set to ``""`` to disable prefixing.

    Usage::

        provider = EnvSecretProvider(prefix="RANGER_")
        # Reads os.environ["RANGER_DB_PASSWORD"]
        password = provider.get_secret("DB_PASSWORD")
    """

    def __init__(self, prefix: str = "RANGER_") -> None:
        self._prefix = prefix
        logger.info("env_secrets_init", prefix=prefix)

    # ------------------------------------------------------------------
    # BaseSecretProvider interface
    # ------------------------------------------------------------------

    def get_secret(self, key: str) -> str:
        """Look up ``{prefix}{key}`` in the environment.

        Args:
            key: Secret key (the prefix is prepended automatically).

        Returns:
            The environment variable value.

        Raises:
            KeyError: If the variable is not set.
        """
        env_key = f"{self._prefix}{key}"
        value = os.environ.get(env_key)

        if value is None:
            raise KeyError(
                f"Environment variable '{env_key}' not set. "
                f"(prefix={self._prefix!r}, key={key!r})"
            )
        return value

    def list_secrets(self) -> list[str]:
        """List all environment variable keys matching the prefix.

        Returns:
            List of key names **without** the prefix.
        """
        results: list[str] = []
        for env_key in sorted(os.environ):
            if env_key.startswith(self._prefix):
                # Strip the prefix to return the logical key
                results.append(env_key[len(self._prefix):])
        return results

    def provider_name(self) -> str:
        return "env"
