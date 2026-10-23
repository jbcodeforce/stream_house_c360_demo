from __future__ import annotations

import os
from urllib.parse import urlparse

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _host_owns_cdc(database_url: str | None) -> bool:
    """Whether a managed CDC connector should own Kafka for this DB host.

    Local PostgreSQL (localhost / 127.0.0.1) has no connector, so the backend
    is the only producer → False. Any remote host — RDS included — is assumed
    to have a Debezium/CDC connector publishing cdc.public.*, so the backend
    must stay silent → True. A wrong True merely goes quiet; a wrong False
    double-publishes, so anything that is not explicitly local defaults to the
    safe True. Mirrors the inference in scripts/run_dev.sh.
    """
    if not database_url:
        return False
    return (urlparse(database_url).hostname or "") not in _LOCAL_HOSTS


class Settings(BaseSettings):
    # The dotenv file path itself comes from the ENV_CONFIG_FILE env var
    # (default ".env"); real environment variables still take precedence.
    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_CONFIG_FILE", ".env"), extra="ignore"
    )

    SINK: str = "postgres"

    # When True, a managed CDC connector (Debezium on RDS / remote PostgreSQL)
    # owns publishing to the cdc.public.* topics, so the backend must NOT also
    # dual-write Kafka events — doing so would double-publish. This hard guard
    # overrides ``KAFKA_PRODUCE_ENABLED``.
    #
    # Leave unset (None) to auto-resolve from the DATABASE_URL host on startup:
    # local PostgreSQL ⇒ False (the app produces), any remote host incl. RDS ⇒
    # True (the connector owns Kafka). Set the env var explicitly to override
    # the inference either way.
    CDC_CONNECTOR_ENABLED: bool | None = None

    # When True (and no CDC connector owns Kafka), the backend dual-writes a
    # Debezium-shaped event to cdc.public.* after each successful DB change.
    KAFKA_PRODUCE_ENABLED: bool = False

    # When True, on startup the backend loads the seed CSVs and publishes every
    # record as a Debezium "create" event to the cdc.public.* topics
    # (customers → accounts → transactions). Skipped when a CDC connector owns
    # Kafka (CDC_CONNECTOR_ENABLED) to avoid double-publishing.
    KAFKA_SEED_ON_START: bool = False

    DATABASE_URL: str | None = None
    KAFKA_BOOTSTRAP_SERVERS: str | None = None
    KAFKA_API_KEY: str | None = None
    KAFKA_API_SECRET: str | None = None
    SCHEMA_REGISTRY_URL: str | None = None
    SCHEMA_REGISTRY_API_KEY: str | None = None
    SCHEMA_REGISTRY_API_SECRET: str | None = None
    KAFKA_TOPIC_CUSTOMERS: str = "cdc.public.customers"
    KAFKA_TOPIC_ACCOUNTS: str = "cdc.public.accounts"
    KAFKA_TOPIC_TRANSACTIONS: str = "cdc.public.transactions"

    # Data Products graph page: selects the status/metrics provider.
    # "mock" / "mock-lag" use deterministic fixtures; "live" talks to
    # Confluent Cloud + Flink (Task 3).
    GRAPH_DATA_MODE: str = "mock"

    # Live-mode credentials (unused until Task 3's LiveProvider).
    CONFLUENT_CLOUD_API_KEY: str | None = None
    CONFLUENT_CLOUD_API_SECRET: str | None = None
    FLINK_API_KEY: str | None = None
    FLINK_API_SECRET: str | None = None
    FLINK_ORG_ID: str | None = None
    FLINK_ENV_ID: str | None = None
    FLINK_COMPUTE_POOL_ID: str | None = None
    FLINK_REST_ENDPOINT: str | None = None

    @model_validator(mode="after")
    def _resolve_cdc_connector(self) -> "Settings":
        """Infer CDC ownership from the DB host when not set explicitly."""
        if self.CDC_CONNECTOR_ENABLED is None:
            self.CDC_CONNECTOR_ENABLED = _host_owns_cdc(self.DATABASE_URL)
        return self


settings = Settings()
print(settings.model_dump_json(indent=2))
