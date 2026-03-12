"""Abstract secret provider and concrete implementations.

Ranger supports multiple secret backends.  The config resolver uses
these providers to resolve ``${secrets.<provider>.<key>}`` references.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any


class BaseSecretProvider(ABC):
    """Abstract base class for secret providers."""

    @abstractmethod
    def get_secret(self, key: str) -> str:
        """Retrieve a secret value by key.

        Args:
            key: The secret key / path.

        Returns:
            The secret value as a string.

        Raises:
            KeyError: If the secret is not found.
        """
        ...

    @abstractmethod
    def provider_name(self) -> str:
        """Return the provider name (e.g., 'env', 'vault', 'aws')."""
        ...


class EnvSecretProvider(BaseSecretProvider):
    """Resolve secrets from environment variables.

    This is the default, zero-config provider.
    Reference format: ``${secrets.env.MY_VAR}``
    """

    def get_secret(self, key: str) -> str:
        value = os.environ.get(key)
        if value is None:
            raise KeyError(f"Environment variable '{key}' not set")
        return value

    def provider_name(self) -> str:
        return "env"


class VaultSecretProvider(BaseSecretProvider):
    """Resolve secrets from HashiCorp Vault.

    Requires ``hvac`` library and Vault configuration.
    Reference format: ``${secrets.vault.secret/data/myapp#password}``
    """

    def __init__(self, url: str, token: str | None = None, mount_point: str = "secret") -> None:
        try:
            import hvac
        except ImportError as exc:
            raise ImportError("Install hvac: pip install ranger-core[vault]") from exc

        self._client = hvac.Client(url=url, token=token or os.environ.get("VAULT_TOKEN"))
        self._mount_point = mount_point

    def get_secret(self, key: str) -> str:
        # key format: "path#field" e.g., "data/myapp#password"
        if "#" in key:
            path, field = key.rsplit("#", 1)
        else:
            path, field = key, "value"

        response = self._client.secrets.kv.v2.read_secret_version(
            path=path, mount_point=self._mount_point
        )
        data = response["data"]["data"]
        if field not in data:
            raise KeyError(f"Field '{field}' not found in Vault path '{path}'")
        return str(data[field])

    def provider_name(self) -> str:
        return "vault"


class AWSSecretsProvider(BaseSecretProvider):
    """Resolve secrets from AWS Secrets Manager.

    Reference format: ``${secrets.aws.my-secret-name}`` or ``${secrets.aws.my-secret#field}``
    """

    def __init__(self, region_name: str | None = None) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError("Install boto3: pip install ranger-core[aws-secrets]") from exc

        self._client = boto3.client("secretsmanager", region_name=region_name)

    def get_secret(self, key: str) -> str:
        import json

        if "#" in key:
            secret_name, field = key.rsplit("#", 1)
        else:
            secret_name, field = key, None

        response = self._client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"]

        if field:
            data = json.loads(secret_string)
            if field not in data:
                raise KeyError(f"Field '{field}' not found in AWS secret '{secret_name}'")
            return str(data[field])
        return secret_string

    def provider_name(self) -> str:
        return "aws"


class SecretResolver:
    """Resolves secret references using configured providers.

    Usage::

        resolver = SecretResolver()
        resolver.register(EnvSecretProvider())
        resolver.register(VaultSecretProvider(url="..."))
        value = resolver.resolve("vault.data/myapp#password")
    """

    def __init__(self) -> None:
        self._providers: dict[str, BaseSecretProvider] = {}
        # Always register env provider as fallback
        self.register(EnvSecretProvider())

    def register(self, provider: BaseSecretProvider) -> None:
        """Register a secret provider."""
        self._providers[provider.provider_name()] = provider

    def resolve(self, reference: str) -> str:
        """Resolve a secret reference in format '<provider>.<key>'.

        Args:
            reference: Secret reference (e.g., 'vault.data/myapp#password').

        Returns:
            The resolved secret value.

        Raises:
            ValueError: If the provider is not registered.
            KeyError: If the secret is not found.
        """
        parts = reference.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid secret reference format: '{reference}'. Expected '<provider>.<key>'")

        provider_name, key = parts

        if provider_name not in self._providers:
            raise ValueError(
                f"Unknown secret provider '{provider_name}'. "
                f"Available: {list(self._providers.keys())}"
            )

        return self._providers[provider_name].get_secret(key)
