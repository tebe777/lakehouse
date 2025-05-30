"""
Security manager for credential management and authentication.
"""

import os
import logging
from typing import Dict, Optional, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class Credential:
    """Credential data structure."""
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    connection_string: Optional[str] = None
    additional_properties: Optional[Dict[str, str]] = None


class CredentialProvider(ABC):
    """Abstract base class for credential providers."""
    
    @abstractmethod
    def get_credential(self, credential_name: str) -> Credential:
        """Get credential by name."""
        pass
    
    @abstractmethod
    def list_credentials(self) -> list[str]:
        """List available credential names."""
        pass


class EnvironmentCredentialProvider(CredentialProvider):
    """Credential provider using environment variables."""
    
    def __init__(self, prefix: str = "LAKEHOUSE_"):
        self.prefix = prefix
        
    def get_credential(self, credential_name: str) -> Credential:
        """Get credential from environment variables."""
        env_name = f"{self.prefix}{credential_name.upper()}"
        
        # Try different patterns
        username = os.getenv(f"{env_name}_USERNAME")
        password = os.getenv(f"{env_name}_PASSWORD")
        token = os.getenv(f"{env_name}_TOKEN")
        connection_string = os.getenv(f"{env_name}_CONNECTION_STRING")
        
        if not any([username, password, token, connection_string]):
            # Try simple pattern
            value = os.getenv(env_name)
            if value:
                if "://" in value:
                    connection_string = value
                else:
                    token = value
        
        return Credential(
            username=username,
            password=password,
            token=token,
            connection_string=connection_string
        )
    
    def list_credentials(self) -> list[str]:
        """List available credentials from environment."""
        credentials = set()
        for key in os.environ:
            if key.startswith(self.prefix):
                # Extract credential name
                suffix = key[len(self.prefix):]
                if "_" in suffix:
                    cred_name = suffix.split("_")[0]
                else:
                    cred_name = suffix
                credentials.add(cred_name.lower())
        return list(credentials)


class KeyVaultCredentialProvider(CredentialProvider):
    """Credential provider using Azure Key Vault."""
    
    def __init__(self, vault_url: str, credential: Optional[Any] = None):
        self.vault_url = vault_url
        self.credential = credential or DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=self.credential)
        
    def get_credential(self, credential_name: str) -> Credential:
        """Get credential from Key Vault."""
        try:
            # Try to get structured credential
            username_secret = self._get_secret_safe(f"{credential_name}-username")
            password_secret = self._get_secret_safe(f"{credential_name}-password")
            token_secret = self._get_secret_safe(f"{credential_name}-token")
            connection_string_secret = self._get_secret_safe(f"{credential_name}-connection-string")
            
            # If no structured secrets, try simple secret
            if not any([username_secret, password_secret, token_secret, connection_string_secret]):
                simple_secret = self._get_secret_safe(credential_name)
                if simple_secret:
                    if "://" in simple_secret:
                        connection_string_secret = simple_secret
                    else:
                        token_secret = simple_secret
            
            return Credential(
                username=username_secret,
                password=password_secret,
                token=token_secret,
                connection_string=connection_string_secret
            )
            
        except Exception as e:
            logger.error("Failed to retrieve credential from Key Vault", 
                        credential_name=credential_name, error=str(e))
            raise
    
    def _get_secret_safe(self, secret_name: str) -> Optional[str]:
        """Safely get secret from Key Vault."""
        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception:
            return None
    
    def list_credentials(self) -> list[str]:
        """List available credentials from Key Vault."""
        try:
            secrets = self.client.list_properties_of_secrets()
            credential_names = set()
            
            for secret in secrets:
                name = secret.name
                # Extract base credential name
                if "-" in name:
                    base_name = name.split("-")[0]
                    credential_names.add(base_name)
                else:
                    credential_names.add(name)
            
            return list(credential_names)
        except Exception as e:
            logger.error("Failed to list credentials from Key Vault", error=str(e))
            return []


class SecurityManager:
    """Main security manager for credential and authentication management."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.providers: Dict[str, CredentialProvider] = {}
        self._setup_providers()
        
    def _setup_providers(self):
        """Setup credential providers based on configuration."""
        # Environment provider (always available)
        self.providers["environment"] = EnvironmentCredentialProvider(
            prefix=self.config.get("env_prefix", "LAKEHOUSE_")
        )
        
        # Key Vault provider
        if "keyvault" in self.config:
            kv_config = self.config["keyvault"]
            credential = None
            
            if "client_id" in kv_config and "client_secret" in kv_config:
                credential = ClientSecretCredential(
                    tenant_id=kv_config["tenant_id"],
                    client_id=kv_config["client_id"],
                    client_secret=kv_config["client_secret"]
                )
            
            self.providers["keyvault"] = KeyVaultCredentialProvider(
                vault_url=kv_config["vault_url"],
                credential=credential
            )
    
    def get_credential(self, credential_name: str, provider: str = "auto") -> Credential:
        """Get credential using specified or auto-detected provider."""
        if provider == "auto":
            # Try providers in order of preference
            for provider_name in ["keyvault", "environment"]:
                if provider_name in self.providers:
                    try:
                        credential = self.providers[provider_name].get_credential(credential_name)
                        if self._is_valid_credential(credential):
                            logger.info("Retrieved credential", 
                                      credential_name=credential_name, 
                                      provider=provider_name)
                            return credential
                    except Exception as e:
                        logger.warning("Failed to get credential from provider",
                                     credential_name=credential_name,
                                     provider=provider_name,
                                     error=str(e))
            
            raise ValueError(f"No valid credential found for {credential_name}")
        
        if provider not in self.providers:
            raise ValueError(f"Unknown credential provider: {provider}")
        
        return self.providers[provider].get_credential(credential_name)
    
    def _is_valid_credential(self, credential: Credential) -> bool:
        """Check if credential has at least one valid field."""
        return any([
            credential.username and credential.password,
            credential.token,
            credential.connection_string
        ])
    
    def get_spark_config(self, credential_name: str) -> Dict[str, str]:
        """Get Spark configuration for a credential."""
        credential = self.get_credential(credential_name)
        config = {}
        
        if credential.connection_string:
            # Parse connection string for S3/MinIO
            if "s3" in credential_name.lower() or "minio" in credential_name.lower():
                # Extract access key and secret from connection string
                # Format: s3://access_key:secret_key@endpoint/bucket
                if "://" in credential.connection_string:
                    parts = credential.connection_string.split("://")[1]
                    if "@" in parts:
                        auth_part = parts.split("@")[0]
                        if ":" in auth_part:
                            access_key, secret_key = auth_part.split(":", 1)
                            config.update({
                                "spark.hadoop.fs.s3a.access.key": access_key,
                                "spark.hadoop.fs.s3a.secret.key": secret_key
                            })
        
        elif credential.username and credential.password:
            # Use username/password for S3
            config.update({
                "spark.hadoop.fs.s3a.access.key": credential.username,
                "spark.hadoop.fs.s3a.secret.key": credential.password
            })
        
        return config
    
    def get_connection_string(self, credential_name: str, service_type: str) -> str:
        """Get connection string for specific service type."""
        credential = self.get_credential(credential_name)
        
        if credential.connection_string:
            return credential.connection_string
        
        # Build connection string based on service type
        if service_type == "postgresql":
            if credential.username and credential.password:
                host = self.config.get("postgresql", {}).get("host", "localhost")
                port = self.config.get("postgresql", {}).get("port", 5432)
                database = self.config.get("postgresql", {}).get("database", "lakehouse")
                
                return f"postgresql://{credential.username}:{credential.password}@{host}:{port}/{database}"
        
        raise ValueError(f"Cannot build connection string for {service_type}")
    
    def list_all_credentials(self) -> Dict[str, list[str]]:
        """List all available credentials from all providers."""
        all_credentials = {}
        for provider_name, provider in self.providers.items():
            try:
                credentials = provider.list_credentials()
                all_credentials[provider_name] = credentials
            except Exception as e:
                logger.error("Failed to list credentials from provider",
                           provider=provider_name, error=str(e))
                all_credentials[provider_name] = []
        
        return all_credentials 