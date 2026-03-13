"""AWS Secrets Manager provider for Ranger.

Uses boto3 to retrieve and list secrets from AWS Secrets Manager.
"""

from __future__ import annotations

import json
from typing import Any

import structlog

from ranger.secrets.base import BaseSecretProvider

logger = structlog.get_logger(__name__)


class AWSSecretsProvider(BaseSecretProvider):
    """Resolve secrets from AWS Secrets Manager.

    Config keys:
        region: AWS region name (e.g. ``us-east-1``).
        secret_name: Specific secret name to scope lookups.
        secret_prefix: Prefix filter for :meth:`list_secrets`.

    Key format for :meth:`get_secret`:
        ``"my-secret-name"`` — returns the full ``SecretString``.
        ``"my-secret-name#field"`` — parses ``SecretString`` as JSON and
        returns the value of ``field``.

    Usage::

        provider = AWSSecretsProvider(region="us-east-1")
        db_pass = provider.get_secret("myapp/db-creds#password")
    """

    def __init__(
        self,
        region: str | None = None,
        secret_name: str | None = None,
        secret_prefix: str | None = None,
    ) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "Install boto3 for AWS Secrets Manager: pip install ranger-core[aws-secrets]"
            ) from exc

        self._client = boto3.client("secretsmanager", region_name=region)
        self._region = region
        self._secret_name = secret_name
        self._secret_prefix = secret_prefix or ""
        logger.info(
            "aws_secrets_init",
            region=region,
            secret_name=secret_name,
            secret_prefix=secret_prefix,
        )

    # ------------------------------------------------------------------
    # BaseSecretProvider interface
    # ------------------------------------------------------------------

    def get_secret(self, key: str) -> str:
        """Retrieve a secret from AWS Secrets Manager.

        Args:
            key: Secret name with optional JSON field — ``"name#field"``.
                 If *secret_name* was set in config, *key* is the JSON field
                 within that secret.

        Returns:
            The secret value as a string.

        Raises:
            KeyError: If the secret or field is not found.
        """
        # If a base secret_name is configured, treat key as a field within it
        if self._secret_name:
            return self._get_field(self._secret_name, key)

        # Otherwise, key may be "secret_name" or "secret_name#field"
        if "#" in key:
            secret_id, field = key.rsplit("#", 1)
            return self._get_field(secret_id, field)

        return self._get_raw(key)

    def list_secrets(self) -> list[str]:
        """List secret names, optionally filtered by prefix.

        Returns:
            List of secret name strings.
        """
        names: list[str] = []
        paginator = self._client.get_paginator("list_secrets")

        filters: list[dict[str, Any]] = []
        if self._secret_prefix:
            filters.append({"Key": "name", "Values": [self._secret_prefix]})

        try:
            for page in paginator.paginate(Filters=filters) if filters else paginator.paginate():
                for entry in page.get("SecretList", []):
                    names.append(entry["Name"])
        except Exception as exc:
            logger.error("aws_list_secrets_failed", error=str(exc))

        return names

    def provider_name(self) -> str:
        return "aws"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_raw(self, secret_id: str) -> str:
        """Fetch the raw SecretString for a secret."""
        try:
            response = self._client.get_secret_value(SecretId=secret_id)
        except Exception as exc:
            raise KeyError(
                f"AWS secret '{secret_id}' not found: {exc}"
            ) from exc

        secret_string = response.get("SecretString")
        if secret_string is None:
            raise KeyError(
                f"AWS secret '{secret_id}' has no SecretString (binary secrets not supported)"
            )
        return secret_string

    def _get_field(self, secret_id: str, field: str) -> str:
        """Fetch a specific JSON field from a secret."""
        raw = self._get_raw(secret_id)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise KeyError(
                f"AWS secret '{secret_id}' is not valid JSON; cannot extract field '{field}'"
            ) from exc

        if field not in data:
            raise KeyError(
                f"Field '{field}' not found in AWS secret '{secret_id}'. "
                f"Available: {list(data.keys())}"
            )
        return str(data[field])
