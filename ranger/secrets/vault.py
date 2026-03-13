"""HashiCorp Vault secret provider for Ranger.

Supports token-based and AppRole authentication against HashiCorp Vault,
with KV v2 secret engine.
"""

from __future__ import annotations

import os
from typing import Any

import structlog

from ranger.secrets.base import BaseSecretProvider

logger = structlog.get_logger(__name__)


class VaultSecretProvider(BaseSecretProvider):
    """Resolve secrets from HashiCorp Vault (KV v2).

    Supports two auth methods:

    * **Token auth** — supply ``token`` directly or set ``VAULT_TOKEN`` env var.
    * **AppRole auth** — supply ``role_id`` and ``secret_id``.

    Config keys:
        url: Vault server URL (e.g. ``https://vault.example.com:8200``).
        token: Vault token (optional if using AppRole or ``VAULT_TOKEN`` env).
        role_id: AppRole role ID (optional).
        secret_id: AppRole secret ID (optional).
        mount_point: KV v2 mount point (default ``secret``).
        path: Base path prefix for secrets (default empty).

    Key format for :meth:`get_secret`:
        ``"path/to/secret#field"`` — reads the ``field`` key from the KV
        entry at ``{mount_point}/data/{base_path}/{path/to/secret}``.
        If no ``#field`` suffix is given, defaults to ``"value"``.

    Usage::

        vault = VaultSecretProvider(
            url="https://vault.example.com:8200",
            token="s.xxxx",
            mount_point="secret",
        )
        password = vault.get_secret("myapp/db#password")
    """

    def __init__(
        self,
        url: str,
        token: str | None = None,
        role_id: str | None = None,
        secret_id: str | None = None,
        mount_point: str = "secret",
        path: str = "",
    ) -> None:
        try:
            import hvac
        except ImportError as exc:
            raise ImportError(
                "Install hvac for Vault support: pip install ranger-core[vault]"
            ) from exc

        self._mount_point = mount_point
        self._base_path = path.strip("/")
        self._url = url

        # Authenticate
        if role_id and secret_id:
            # AppRole authentication
            self._client = hvac.Client(url=url)
            auth_response = self._client.auth.approle.login(
                role_id=role_id,
                secret_id=secret_id,
            )
            self._client.token = auth_response["auth"]["client_token"]
            logger.info("vault_approle_auth_success", url=url, mount=mount_point)
        else:
            # Token authentication
            resolved_token = token or os.environ.get("VAULT_TOKEN")
            if not resolved_token:
                raise ValueError(
                    "Vault requires either a token (or VAULT_TOKEN env var) "
                    "or role_id + secret_id for AppRole auth."
                )
            self._client = hvac.Client(url=url, token=resolved_token)
            logger.info("vault_token_auth", url=url, mount=mount_point)

    # ------------------------------------------------------------------
    # BaseSecretProvider interface
    # ------------------------------------------------------------------

    def get_secret(self, key: str) -> str:
        """Retrieve a secret from Vault KV v2.

        Args:
            key: Secret path with optional field — ``"path#field"``
                 (defaults field to ``"value"``).

        Returns:
            The secret value as a string.

        Raises:
            KeyError: If the secret path or field does not exist.
        """
        if "#" in key:
            secret_path, field = key.rsplit("#", 1)
        else:
            secret_path, field = key, "value"

        full_path = f"{self._base_path}/{secret_path}" if self._base_path else secret_path

        try:
            response = self._client.secrets.kv.v2.read_secret_version(
                path=full_path,
                mount_point=self._mount_point,
            )
        except Exception as exc:
            raise KeyError(
                f"Failed to read Vault path '{full_path}' "
                f"(mount={self._mount_point}): {exc}"
            ) from exc

        data: dict[str, Any] = response["data"]["data"]

        if field not in data:
            raise KeyError(
                f"Field '{field}' not found in Vault path '{full_path}'. "
                f"Available fields: {list(data.keys())}"
            )

        return str(data[field])

    def list_secrets(self, path: str = "") -> list[str]:
        """List secret keys at the given path.

        Args:
            path: Sub-path under the mount point (empty = root).

        Returns:
            List of key names at the path.
        """
        full_path = f"{self._base_path}/{path}" if self._base_path else path

        try:
            response = self._client.secrets.kv.v2.list_secrets(
                path=full_path,
                mount_point=self._mount_point,
            )
            return response["data"]["keys"]
        except Exception as exc:
            logger.warning("vault_list_failed", path=full_path, error=str(exc))
            return []

    def health_check(self) -> dict[str, Any]:
        """Check Vault server health.

        Returns:
            Dict with ``healthy`` bool and Vault status details.
        """
        try:
            status = self._client.sys.read_health_status(method="GET")
            is_healthy = not (status.get("sealed", True))
            return {
                "healthy": is_healthy,
                "initialized": status.get("initialized", False),
                "sealed": status.get("sealed", True),
                "version": status.get("version", "unknown"),
                "cluster_name": status.get("cluster_name", ""),
            }
        except Exception as exc:
            logger.error("vault_health_check_failed", error=str(exc))
            return {"healthy": False, "error": str(exc)}

    def provider_name(self) -> str:
        return "vault"
